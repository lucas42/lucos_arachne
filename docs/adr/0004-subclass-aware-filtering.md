# ADR-0004: Subclass-aware filtering in the search index

**Date:** 2026-05-27
**Status:** Proposed
**Discussion:** https://github.com/lucas42/lucos_arachne/issues/583

## Context

The arachne Typesense `items` collection stores a single-valued `type` field on each document. The ingestor (`ingestor/searchindex.py`, `graph_to_typesense_docs`) populates it by reading the `skos:prefLabel` of the subject's *immediate* `rdf:type` and `break`-ing after the first hit. Each Film therefore has `doc.type = "Film"`; each Book has `doc.type = "Book"`; etc.

The lucos_search_component web component lets callers narrow a search by setting `data-types="X"`, which translates directly into a Typesense `filter_by=type:=[X]` predicate. This works for any leaf type whose prefLabel matches `X` — `data-types="City"` succeeds because there are documents with `type:"City"`.

It fails for parent classes. Surfaced by `lucas42/lucos_media_metadata_manager#304`, which set `data-types="CreativeWork"` on the `theme_tune` and `soundtrack` form fields expecting to filter to creative-work entities (Films, Books, Songs, Musicals, Tv Programs, Games — 97 instances in production). It matches zero documents, because `schema:CreativeWork` is a parent class — no instance has it as its direct `rdf:type`. The same gap blocks `data-types="Place"` (only `City`, `River`, `Country`, … exist as leaves) and would block `data-types="Organization"` if anyone tried.

Verified against the live index:
- `type:=CreativeWork` → 0 results
- `type:=Film` → 6 results

Two paths exist today and neither is right:

- **Enumerate the leaves at the call site** (`data-types="Film,Tv Program,Book,Musical,Song,Game"`). Hardcodes eolas ontology contents into PHP. Any new `CreativeWorkType` added later silently fails to appear in the dropdown. Inverts the eolas/arachne split that exists specifically to prevent this kind of coupling.
- **Skip type filtering** on these fields. Returns tracks and people alongside creative works. Wrong semantics for the form field.

The underlying capability — "filter to X or any subclass of X" — is what `data-types="CreativeWork"` is implicitly asking for. It does not exist in the search infrastructure today. This ADR introduces it.

There is one degenerate case that already works subclass-aware-ish: Languages. `searchindex.py` lines 107–111 special-case any subject whose `rdf:type` is itself an `eolas:LanguageFamily` (e.g. `<…/iso639-5/cel>`), rewriting `doc.type` to the literal string `"Language"`. This pre-dates the cross-cutting question and is structural special-casing, not generalisable. The ADR keeps the special case in place; the new mechanism layers alongside it.

## Decision

Three coupled decisions, all required to make the capability work end-to-end.

### Decision 1 — Subclass-aware semantics

`data-types="X"` (and equivalently the Typesense `types:=[X]` filter introduced below) means **"X exactly OR any subclass of X transitively"**. A leaf type's own label appears in its own subclass set, so existing leaf-only call sites (`data-types="City"`, `data-types="Track"`) continue to work unchanged.

This generalises the existing leaf-only contract — leaf-only is the special case where X happens to have no subclasses with instances.

### Decision 2 — Parent-class prefLabel form

Classes that are referenced only as `rdfs:subClassOf` targets — `schema:CreativeWork`, `schema:Place`, future others — get `skos:prefLabel` literals in **natural English with spaces**, matching the existing eolas convention for meta-types and multi-word concepts. Specifically:

- `<https://schema.org/CreativeWork> skos:prefLabel "Creative Work"@en`
- `<https://schema.org/Place> skos:prefLabel "Place"@en`
- Future similar parent classes follow the same rule.

This is consistent with the labels eolas already emits for `eolas:PlaceType` ("Place Type"), `eolas:CreativeWorkType` ("Creative Work Type"), and `eolas:LanguageFamily` ("Language Family"). Concatenated camelCase forms ("CreativeWork", "LanguageFamily") are not used in lucos prefLabels; URI-local-names are a separate identifier surface and not user-facing.

Consequence: PR `lucas42/lucos_media_metadata_manager#304` needs a one-character edit — `data-types="CreativeWork"` becomes `data-types="Creative Work"`. The diff is otherwise unchanged.

### Decision 3 — New multi-valued `types[]` field; retain single-valued `type` for display

The Typesense `items` collection gains a new field:

```json
{"name": "types", "type": "string[]", "facet": true, "optional": true}
```

The ingestor populates it with the prefLabels of every class in the subject's `rdf:type` hierarchy, transitively closed over `rdfs:subClassOf`, excluding meta-types (anything in OWL/RDFS/RDF-syntax namespaces per `is_meta_type()`). For a Film: `types: ["Film", "Creative Work"]`.

The existing single-valued `type` field is **retained as-is** and continues to store the leaf prefLabel. The `lucos_search_component` filter switches from `type:=[X]` to `types:=[X]`. Display consumers (the MCP server's `search` tool result formatter at `mcp/server.py:228`, the explorer item-page renderer, future facet widgets) continue reading `doc.type` as a single string with no change.

## Cross-estate principle

> **Sources that emit `rdfs:subClassOf` triples must also emit `skos:prefLabel` for the parent class.**
>
> The existing convention is "every domain `rdf:type` emitted by a source must include `skos:prefLabel` and `eolas:hasCategory` for the type" (per `lucos_arachne/CLAUDE.md` and `#371`). This ADR extends it: any class referenced as the *object* of an `rdfs:subClassOf` triple is now also walked by the ingestor and therefore also needs a prefLabel from the source. `eolas:hasCategory` is *not* required on parent classes — `category` continues to be derived from the leaf type only.
>
> OWL/RDFS-namespace classes remain excluded (they hit `is_meta_type()` and the walk stops there).

The arachne ingestor will fail loudly (`ValueError`) if a source emits an `rdfs:subClassOf` triple without a corresponding parent-class label — same failure mode as the existing convention for `rdf:type` labels.

## Alternatives considered

### Upgrade the existing `type` field to multi-valued (rejected)

The simpler-looking schema change: replace `{"name": "type", "type": "string", "facet": true}` with `{"name": "type", "type": "string[]", "facet": true}`. Filtering syntax (`type:=[X]`) stays exactly the same. The new value contains both the leaf and the ancestor labels.

Rejected because the change set is strictly larger than Decision 3 above. Every display consumer of the `type` field — the MCP server's search-result formatter, the explorer's per-item header, lucos_search_component's dropdown row rendering, anything reading `hit.document.type` as a string — would need to pick one element (presumably the first, but order semantics in Typesense's `string[]` returns are not guaranteed). The breaking change ripples across at least three repos. Decision 3's additive `types[]` field requires changes only in the filter call sites that have to change anyway (the search component, and any direct API user who relies on `type:=`). The `feedback_breaking_change_when_callers_must_change_anyway` lens applies in reverse here: a breaking change is only justified when it saves callers from a separate code change, and this one doesn't — the filter switch is happening regardless of which option is picked.

### A class-URI field rather than a class-label field (rejected)

Instead of (or alongside) `types: ["Film", "Creative Work"]`, store `class_uris: ["https://eolas.l42.eu/metadata/creativeworktype/5/", "https://schema.org/CreativeWork"]`. Filtering becomes `class_uris:=https://schema.org/CreativeWork` — semantic, not label-based, immune to prefLabel choices.

Rejected on caller ergonomics. `data-types="https://schema.org/CreativeWork"` in formfields.php is uglier than `data-types="Creative Work"`, requires every caller to know the canonical URI, and forces re-encoding for URL transport. The existing convention is already label-based — `data-types="City"`, `data-types="Language"`. Changing the convention to URIs at this point would be a breaking change across every existing call site for marginal robustness gain. URIs as the filter identifier are the right answer in a green-field design; they are not worth the cost of retrofit here.

### Pre-compute the leaf-list at form-build time (rejected)

`lucos_media_metadata_manager` could query arachne for all `CreativeWorkType` instances at form-render time and pass `data-types="Film,Tv Program,Book,Musical,Song,Game"` dynamically. Avoids the ingestor change entirely.

Rejected because it pushes ontology-walking responsibility onto every form consumer. Each new consumer ends up implementing the same SPARQL/Typesense query, with the same caching and failure-mode concerns. The walking belongs in the ingestor where it can be done once, deterministically, at write time. Also: the convention that `data-types` is a static string in the form definition is a feature — it means a form's behaviour is reviewable from the source code alone.

### Special-case `schema:CreativeWork` and `schema:Place` in the ingestor (rejected)

Extend the existing `LanguageFamily` special-case (`searchindex.py` lines 107–111) to similarly rewrite Film/Book/etc. to `type: "Creative Work"` (and City/River/etc. to `type: "Place"`). Avoids any schema change.

Rejected because it would erase the leaf type from the display value. A Film hit would render as `[Creative Work] The Bridge on the River Kwai` in the MCP search output and in the explorer, losing the more useful "Film" label. The LanguageFamily case works only because users genuinely think of "English" and "Irish" as Languages, not as members of "West Germanic languages" or "Celtic languages"; the family is an internal classification, not a user-meaningful display category. Films and Books are different — the leaf is the right display value. The ADR keeps display and filter as separate concerns precisely so this trade-off doesn't have to be made.

### Compute the closure at query time via Fuseki (rejected)

`lucos_search_component` could route the filter through arachne's SPARQL endpoint to expand `schema:CreativeWork` into its instance set, then pass that as a Typesense `id:=[…]` filter. Avoids the ingestor changes and the schema change.

Rejected on two grounds. First, it introduces a second round-trip per search (SPARQL expansion → Typesense filter), which the search component is designed to avoid (single Typesense call, sub-100ms). Second, it depends on Fuseki's OWL inference actually materialising `rdf:type schema:CreativeWork` for subclass instances — and at present it doesn't (`mcp__arachne__find_entities("https://schema.org/CreativeWork")` returns 0, despite the `rdfs:subClassOf` triples being in the data). Fixing Fuseki's inference is worthwhile separately but is not on the critical path for this capability.

## Sequencing

Three implementation phases, ordered by data dependency. Each ships as a separate PR; the ADR is approved before any of them starts.

**Phase 1 — `lucos_eolas`.** Emit `skos:prefLabel` for parent classes referenced via `rdfs:subClassOf`. Concretely: when `CreativeWorkType.get_rdf()` adds `(uri, rdflib.RDFS.subClassOf, rdflib.SDO.CreativeWork)`, also add `(rdflib.SDO.CreativeWork, rdflib.SKOS.prefLabel, Literal("Creative Work", lang="en"))` plus the Irish translation. Same pattern for `PlaceType.get_rdf()` and `rdflib.SDO.Place`. Must ship before Phase 2 — otherwise the arachne ingestor will `ValueError` on the missing parent-class label as soon as Phase 2's walking code goes live.

**Phase 2 — `lucos_arachne`.** Two changes in the same PR:
- `search/entrypoint.sh`: add `{"name": "types", "type": "string[]", "facet": true, "optional": true}` to the `items` collection's existing missing-field PATCH loop. Additive; Typesense accepts as a schema upgrade with no data migration.
- `ingestor/searchindex.py`: in `graph_to_typesense_docs`, for each non-meta-type `rdf:type` of the subject, walk `rdfs:subClassOf` recursively in the local graph, collecting the prefLabel of each ancestor via `get_label()`. Stop at meta-types (the existing `is_meta_type` filter applies during the walk too). Populate `doc["types"]` with the deduplicated list. Keep `doc["type"]` populated exactly as today (the leaf for display).

Tests in `test_searchindex.py` need new cases: a single-level chain (Film → CreativeWork), a two-level chain to verify recursion, a chain that bottoms out at a meta-type, a subject with multiple immediate types whose ancestor chains share an ancestor (deduplication), and a subject with `rdfs:subClassOf` pointing to a class that lacks a prefLabel (the `ValueError` path).

Also update `lucos_arachne/CLAUDE.md` "RDF Source Convention" section to include the parent-class label requirement.

**Phase 3 — `lucos_search_component`.** In `web-components/lucos-search.js`, change the two existing `filter_by` constructions from `type:=[${component.getAttribute("data-types")}]` to `types:=[${component.getAttribute("data-types")}]`. Same change for the `data-exclude_types` path. Major version bump. The single known external consumer (lucos_media_metadata_manager via npm) will need a `package.json` bump to consume the new version.

**Then:** `lucos_media_metadata_manager#304` updates `data-types="CreativeWork"` → `data-types="Creative Work"`, bumps the `lucos_search_component` dependency to the new major, and ships.

Between Phase 1 and Phase 2 there is no broken state — Phase 1 just adds extra triples to the eolas RDF export that the existing ingestor doesn't read. Between Phase 2 and Phase 3 the new `types[]` field is populated but unused; the existing `type:=` filter on the search component continues to work. Between Phase 3 and the `#304` update the form field is still broken (same as today). Each phase is independently shippable and reversible.

## Consequences

### Positive

- **The capability lucas42 actually wants exists.** `data-types="Creative Work"` filters to all 97 creative-work instances. `data-types="Place"` (if ever wired up) filters to ~1,000+ place instances. The convention generalises to any future parent class without further code change.
- **Existing call sites are unaffected.** `data-types="City"` keeps working because "City" appears in each `<placetype/13/>` instance's `types` array (as the leaf's own label).
- **Display semantics are preserved.** The leaf type continues to render in MCP search results, explorer item pages, search dropdowns. No display consumer needs a code change.
- **The ingestor catches missing labels at ingest time, not at search time.** Same failure mode as the existing label convention — loud, source-pointing `ValueError`. New ontology terms that forget the parent-class label fail in CI, not in production search.
- **The cross-estate principle is documented in one place** — both in this ADR and in `lucos_arachne/CLAUDE.md`. Future source-system maintainers have a single rule to apply when adding new subclass relationships.

### Negative

- **One-time schema change.** Typesense accepts the additive PATCH cleanly, but the change is irreversible without a collection rebuild — the new field, once added, can't be removed without dropping and recreating the collection. The cost is small but worth naming.
- **A small data-coupling that crosses repos.** The eolas-side prefLabel "Creative Work" must match what callers write in their `data-types` attributes. If eolas changes the string later, every caller breaks silently (returns 0 results, no error). Mitigated by: (a) the strings are stable English nouns, unlikely to change; (b) caller-side strings live in source code and are grep-able; (c) the search component could in principle warn when a filter matches zero documents, though that's a separate UX concern not in scope here.
- **The ingestor's subclass walk is recursive and uses the local graph only.** A subClassOf chain that crosses graph boundaries (e.g. eolas emits `<foo> rdfs:subClassOf <bar>` and a different source emits `<bar> rdfs:subClassOf <baz>`) would not be walked transitively in a single ingest payload, because the walker only sees one source's graph at a time. This is consistent with the existing ingestor model (per-source DROP+INSERT, per ADR-0002). In practice all CreativeWorkType / PlaceType subclass chains are emitted entirely by eolas, so the limitation does not bite today. If it ever does, the right fix is to materialise the closure in the triplestore and have the ingestor query Fuseki — which is a much larger change and would be its own ADR.

### Neutral

- **The `LanguageFamily` special case is preserved.** A LanguageFamily-typed subject still gets `doc.type = "Language"` and `doc.category = "Anthropological"`. The new `types[]` field is populated alongside (`types: ["Language", …]`). No change in behaviour for existing Language consumers; new subclass-aware filters can additionally use `types:=[Language]` for the same effect.
- **Fuseki's OWL inference gap is not addressed here.** That `find_entities("https://schema.org/CreativeWork")` returns 0 against the OWL-inferred endpoint is a separate issue — the Typesense path this ADR defines does not depend on Fuseki's reasoner. Worth filing for later, out of scope for this work.

## Implementation

The implementation work is tracked in three sibling issues filed alongside this ADR:

- **Phase 1 producer:** `lucas42/lucos_eolas#<TBD>` — emit prefLabels for `schema:CreativeWork`, `schema:Place`, and any future parent classes referenced via `rdfs:subClassOf`. Blocked by this ADR.
- **Phase 2 consumer:** `lucas42/lucos_arachne#<TBD>` — Typesense schema update plus ingestor walk. Blocked by Phase 1.
- **Phase 3 component:** `lucas42/lucos_search_component` major version bump — filter syntax switch. Blocked by Phase 2.

The downstream consumer PR `lucas42/lucos_media_metadata_manager#304` is blocked by Phase 3.

## References

- `#583` — issue containing the original investigation and the request for this ADR.
- `lucas42/lucos_media_metadata_manager#304` — the consumer PR that surfaced the limitation. Held open pending this work.
- `#371` — the existing RDF Source Convention this ADR extends.
- ADR-0002 in this repo — ingestion strategy. The per-source DROP+INSERT atomicity is what makes the walk-the-local-graph approach safe.
- `lucos_arachne/CLAUDE.md` — current convention text; needs the parent-class extension added as part of Phase 2.
- `lucos_eolas/CLAUDE.md` — downstream-consumer awareness section; the new "emit subClassOf parent labels" rule will be a natural addition here, mirrored from the arachne side.
