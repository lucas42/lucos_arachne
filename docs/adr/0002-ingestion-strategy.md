# ADR-0002: Ingestion strategy — write pattern redesign

**Date:** 2026-04-20
**Status:** Accepted
**Discussion:** https://github.com/lucas42/lucos_arachne/issues/386
**Incident context:** https://github.com/lucas42/lucos/blob/main/docs/incidents/2026-04-20-arachne-sparql-timeouts-tdb2-index-bloat.md

## Context

The ingestor (`ingestor/ingest.py`) refreshes the knowledge graph by:

1. `DROP GRAPH <g>` for each source graph
2. Bulk `INSERT` of the full source data
3. `DROP GRAPH <urn:lucos:inferred>` plus re-run of `compute_inferences()`
4. Repeat on a schedule (≈2 full runs per day, plus on container restart)

This shape was set when the inference refactor landed in #268 and ran cleanly for about 40 days before the 2026-04-20 incident. The incident root-caused a user-visible SPARQL timeout to TDB2 B+tree index bloat: deleting a triple in TDB2 marks the index page tombstoned but never reclaims it without an explicit compaction. With ≈227K live quads and ~80 full refresh cycles, the six indexes had grown to ~14–17GB each (~93GB total) for a working set that should fit in well under 100MB. Container memory saturated, the JVM swapped, and SPARQL latency drifted from 2ms to 200ms+ on a `ASK {}` healthcheck under modest concurrent load.

The immediate fix (online compaction plus a container/heap bump in #387) restores health but does not address the underlying write pattern. We are writing and then deleting the entire dataset twice a day; most of the data does not change between runs; every refresh pays the full cost of tombstoning the previous snapshot, re-materialising the next one, and rebuilding inference from scratch. Without a write-pattern change, bloat simply re-accumulates over the next 40 days.

Two earlier symptom tickets — #321 (SPARQL timeout, closed with a client-side timeout bump) and #343 (healthcheck flapping, closed without root cause) — were downstream of the same accumulation and closed without anyone asking "what has been quietly changing on this component for the past month?". That is itself a failure mode worth flagging: components that have been stable for weeks and then start misbehaving deserve a different investigation reflex to components that break sharply after a change. This ADR's scope is the write pattern, but the review habit is a recurring theme.

## Decision

Reshape the write pattern to match the data's actual rate of change. The dataset is genuinely small (≈227K quads) and the bloat is a write-pattern artefact, not a data-growth artefact. Fix the write pattern; do not change the storage engine.

Three connected changes, in priority order:

1. **Conditional refresh** — implement first.
2. **Diff-based ingestion** — implement second, contingent on (1) being insufficient on its own.
3. **Scheduled compaction** — implement as a permanent belt-and-braces (tracked independently in #389).

Not pursuing: incremental inference, alternative storage engine. Rationale for each is below.

### Conditional refresh (Option 1)

Before calling `replace_graph_in_triplestore` for a source, hash the incoming payload and compare against the hash stored from the last successful ingest of that source. If unchanged, skip the graph entirely — no `DROP`, no `INSERT`, no inference rebuild triggered from that source.

- **Hash input:** the raw response body, as bytes, plus the content-type. This works for the live systems (`lucos_eolas`, `lucos_contacts`, `lucos_media_metadata_api`) and for the cached ontology files. Sources that serialise the same logical graph in different byte orders on different runs will produce false misses (we ingest when we did not need to) — this is no worse than today's behaviour. If measured miss rates turn out to be high, a cheaper semantic normalisation can be layered in later.
- **Hash state:** stored in a dedicated named graph `urn:lucos:ingestor-metadata` in the `raw_arachne` dataset. Each source's graph URI is the subject, predicate `<urn:lucos:ingestor:lastPayloadHash>`, literal `"sha256:..."`. Two consequences:
  - The ingestor container stays stateless: all persistent state lives in Fuseki, which is already on a named Docker volume and backed up.
  - The `urn:lucos:ingestor-metadata` graph must be added to the allow-list in `cleanup_triplestore(all_graph_uris)` so it is not deleted at the end of each run.
- **Ontologies:** the 12 cached ontology files in `ingestor/ontologies/` change only when the repo changes. Hashing the file bytes makes almost all of them no-ops between deploys.
- **Inference rebuild:** `compute_inferences()` runs only if at least one raw graph was actually rewritten in this cycle. If every source hashed identically to its last run, the inferred graph is left alone.

The expected steady-state behaviour is: most runs touch zero graphs and incur no TDB2 writes at all. Runs after a real source change touch one or two graphs. This already captures the bulk of the benefit.

### Diff-based ingestion (Option 2)

For sources that do change, replace `DROP GRAPH <g>; INSERT { ... }` with computed `INSERT` and `DELETE` over the delta only. For a source where one triple has changed, this writes two quads instead of rewriting the entire graph.

This is committed *in principle* but may not be implemented if (1) proves sufficient on its own — see "Sequencing and reversibility" below.

#### Blank-node handling

**Blank nodes are in scope.** Per discussion on #386, `lucos_eolas` emits blank nodes for Festival dates and place metonyms. A naive `set(new) - set(old)` treats every triple touching a blank node as both inserted and deleted on every run — the opposite of what we want.

The ADR commits to: **skolemise blank nodes at ingest time, so the triplestore never holds any.** For each blank node in an incoming source graph, derive a deterministic URI from a canonical hash of the node's structural context (incoming and outgoing triples, with nested blank nodes recursively resolved). Replace the blank node with a URI of the form `urn:lucos:skolem:<hash>` before writing.

This choice has four properties worth naming:

1. The stored form is stable across runs for the same underlying data — two runs over an unchanged `FestivalPeriod` structure produce identical Skolem URIs, so the diff is empty.
2. The diff reduces to plain set operations once Skolemisation is applied to the incoming data; no graph-matching algorithm is needed at diff time.
3. SPARQL queries against the store become more uniform — blank nodes are awkward to bind against explicitly, Skolem URIs are not.
4. It is a one-way transformation. Consumers of `raw_arachne` will see `urn:lucos:skolem:...` URIs in place of blank nodes. This is intentional and matches the RDF spec's recommendation for persisted stores.

The specific canonicalisation algorithm (full URDNA2015 vs. a cheaper tree-shaped approximation) is left to the implementation issue. Our sources' blank node usage is tree-shaped in practice (a festival has dates, dates do not reference festivals back), so the worst-case pathologies of full URDNA2015 do not apply to us and a simpler algorithm is likely to be sufficient.

#### Atomicity and reader consistency

The current pattern exposes readers to a brief window where a source's graph is empty (after `DROP`, before `INSERT`). This has been tolerable at 80 windows over 40 days but is not something we want to preserve by design.

The new pattern:

- Phase 1 — apply all source-graph diffs in a single multi-statement SPARQL Update (`INSERT { ... } WHERE { }; DELETE { ... } WHERE { };` per changed source, separated by `;`). Fuseki executes these as one TDB2 transaction.
- Phase 2 — if any source changed in Phase 1, rebuild the inferred graph (`DROP GRAPH <urn:lucos:inferred>; INSERT { ... }` — which is itself transactional).

Readers during Phase 1 → Phase 2 may briefly see fresh raw data alongside a stale-but-consistent inferred graph. They never see an inconsistent mid-state within raw graphs, and they never see an empty raw graph. This is a strict improvement on current behaviour. Stronger atomicity (one transaction covering raw + inferred) is possible but requires staging the inferred graph in a temporary name and swapping — the additional complexity does not earn its keep.

### Scheduled compaction (Option 3)

Run `POST /$/compact/arachne?deleteOld=true` on a fixed schedule, at lower frequency than current ingestion. This is not the primary answer to the bloat problem — that is Options 2 and 1 — but it is a useful defensive measure. Even with Options 2 and 1 in place, some tombstones will accumulate from changes that do happen. Compaction is cheap (50s from 93GB to 76MB in the incident), fully online, and costs nothing to schedule.

Scope, schedule, and implementation are tracked independently in #389. After Options 2 and 1 ship, the schedule may drop to monthly; until they ship, weekly is appropriate.

### Security considerations

lucas42 confirmed on #386 that the expensive `DROP` + rebuild path is reachable only by cron and container restart; it is not externally triggerable. Loganne webhooks invoke the item-level helpers (`replace_item_in_triplestore`, `merge_items_in_triplestore`, `delete_item_in_triplestore`) which operate on a single subject URI, not whole-graph rewrites.

The redesign does not widen this surface:

- Conditional refresh (Option 1) only affects the cron path; webhook paths are unchanged.
- Diff-based ingestion (Option 2) similarly only affects the cron path. The worst-case cost of a single diff is bounded by the size of the changed source, which is already bounded by what that source is willing to emit.
- Scheduled compaction (Option 3) is internal to the triplestore container and not exposed externally.

Resource exhaustion as a DoS vector against the ingestion pipeline is therefore a non-issue under the current deployment: an attacker would need to already have write access to a source system to cause large diffs, at which point we have bigger problems than TDB2 bloat. If the ingestion endpoint ever gains an externally-triggerable surface (e.g. a "refresh now" webhook on the ingestor itself), this analysis must be revisited — flagging for future ADRs.

## Alternatives considered

### Option 4: Incremental inference (rejected)

Rather than rebuilding `urn:lucos:inferred` wholesale whenever any raw graph changes, incrementally propagate raw-graph changes into the inferred graph.

Rejected because the engineering cost is materially disproportionate to the gain. Incremental inference requires provenance tracking (which inferred triple was derived from which combination of raw triples) and deletion propagation through the derivation graph — this is the classical truth-maintenance problem, well-studied and well-known for being hard to get right. The current `compute_inferences()` implementation is ~80 lines and runs in-memory against 227K raw quads in well under a second. Wholesale rebuild on a triggered basis (Option 1's "only rebuild if at least one source changed") already eliminates the common case where we rebuild for no reason. The uncommon case is cheap enough to brute-force.

Revisit only if the data scale crosses roughly an order of magnitude (≈2M quads), or if a future reasoning addition produces inferred triple counts that dominate rebuild time. Neither is in sight.

### Option 5: Different storage engine (reserved)

Move off Apache Jena / TDB2 to an alternative triplestore without TDB2's tombstone behaviour — Oxigraph, Blazegraph, RDFox, or similar.

Not pursued now for three reasons:

1. **Reasoning integration.** The current reasoning endpoint is Fuseki-native and is what the MCP server (ADR-0001) queries. Alternative engines either lack comparable built-in reasoning (Oxigraph) or require layering a separate reasoner (RDFox is a commercial product with strong reasoning but different operational characteristics; Blazegraph is unmaintained). The cost of reinstating equivalent inferencing on a new engine is high and mostly speculative until estimated properly.
2. **Premature optimisation.** Arachne's profile — low write volume, high read-to-write ratio, inferencing matters, dataset stays sub-million-quad for years — fits Jena/TDB2 well when the store is not being thrashed. Options 2 and 1 test the hypothesis that thrashing is the problem. If the thrashing goes away and TDB2 still struggles, *then* we have evidence for an engine swap.
3. **Scope.** A storage-engine migration is a project, not a fix. It needs its own ADR, migration plan, and likely a period of parallel running. That would be justified if Options 2 and 1 failed to hold — but not before.

Revisit if (a) Options 2 and 1 ship and TDB2 still produces recurring bloat-driven incidents, or (b) a new requirement appears (multi-writer concurrency, remote writes, reasoning features TDB2 does not support) that Jena cannot meet.

## Consequences

### Positive

- **The dataset's write volume becomes proportional to its actual rate of change**, not fixed at "full dataset, twice a day". Steady-state tombstone generation drops to near-zero.
- **Readers stop seeing empty graphs mid-ingest.** Phase 1's single-transaction multi-statement Update means raw graphs are always consistent from a reader's perspective. Phase 2's stale-but-consistent inferred window is a strict improvement on current behaviour.
- **Blank-node usage becomes explicit architecture, not an accident.** Skolemisation at ingest time is a deliberate choice with documented rationale; today's behaviour (blank nodes in the store, re-minted on every ingest) was not a design choice so much as a consequence of nobody noticing.
- **`urn:lucos:inferred` rebuilds only when it has to.** Even before Option 2 lands, Option 1 alone eliminates the "nothing changed, still rebuild inference" case.
- **Scheduled compaction remains in place as a defensive measure** rather than as the primary fix, which is a healthier operational posture (compaction is not compensating for a known-bad pattern, just absorbing incidental churn).

### Negative

- **The ingestor gains a small amount of persistent state** (the per-source hash graph). Living in Fuseki preserves the stateless-container property but introduces a small dependency: if `urn:lucos:ingestor-metadata` is corrupted or deleted, the next ingest will re-write every source graph once (a one-off cost, no permanent damage). `cleanup_triplestore` now needs to know about this graph explicitly.
- **Skolem URIs are permanent in the store.** Consumers that queried arachne expecting blank nodes will need to tolerate Skolem URIs instead. `raw_arachne` consumers today are the MCP server, `searchindex.py`, and the explore UI — none appear to pattern-match on blank-node-ness, but this is worth a look during implementation.
- **Blank-node canonicalisation adds CPU cost on the ingestion path.** For our sources this is negligible (tree-shaped graphs, trivial algorithm), but it is a new thing to reason about if a future source contributes a pathological graph shape.
- **Atomicity guarantees are nuanced, not strict.** Phase 1 is atomic; Phase 2 is not atomic with respect to Phase 1. A reader holding a long-lived query might see fresh raw + stale inferred. No current consumer appears to require strict atomicity here, but the weaker guarantee is now on the record.
- **Two extra implementation issues** (Options 2 and 1) to deliver before the bloat problem is truly closed out. Compaction (#389) bridges the gap but is not a substitute.

### Neutral

- **Option 5 stays reserved.** This ADR does not commit against a future engine migration; it commits against doing it speculatively. If the redesign fails to hold, the path to reconsidering is clear.
- **Inference stays wholesale.** Option 4 is off the table for now, not forever. The rebuild-on-any-change policy is a pragmatic compromise that scales with current data size and will need revisiting at roughly 10x current scale.

## Implementation

Implementation will land as separate issues, each independently deliverable and reviewable. Issues will be filed immediately after this ADR merges.

- **Conditional refresh (Option 1)** — new issue.
- **Diff-based ingestion with blank-node Skolemisation (Option 2)** — new issue, blocked on Option 1 shipping and on an assessment of whether it is still worth building.
- **Scheduled compaction (Option 3)** — already tracked in #389.

## Sequencing and reversibility

Option 1 is a strict addition: a fast path before the existing code. If it breaks, removing the hash check restores current behaviour with no data loss. Hash state in `urn:lucos:ingestor-metadata` can be dropped at any time without affecting correctness — the next ingest will simply re-do every source once.

Option 2 is a more invasive change to `replace_graph_in_triplestore`. The ADR's position is that we should ship Option 1, measure the residual churn, and then decide whether Option 2's complexity is still worth the remaining gain. A likely outcome is that Option 1 alone reduces churn by enough (plausibly >90%, given how infrequently most sources change) that Option 2 becomes optional.

Option 3 (#389) can ship independently of both and is safe to do so before, during, or after the other work.
