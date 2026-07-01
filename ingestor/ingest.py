#!/usr/bin/env python3
"""
Bulk ingests RDF from other systems and adds data to the triplestore and searchindex
"""
import sys, os, time, random, hashlib
from authorised_fetch import fetch_url
from triplestore import (
    live_systems, ontology_cache, ONTOLOGIES_DIR, INFERRED_GRAPH, METADATA_GRAPH,
    replace_graph_in_triplestore, cleanup_triplestore, compute_inferences,
    get_source_hash, set_source_hash, diff_graph_in_triplestore, execute_sparql_update,
    session as triplestore_session,
)
from searchindex import update_searchindex, cleanup_searchindex, update_person_docs_in_searchindex
from loganne import updateLoganne
from schedule_tracker import updateScheduleTracker

try:
	BASE_URL = os.environ["APP_ORIGIN"] + "/"
except KeyError:
	sys.exit("\033[91mAPP_ORIGIN environment variable not set\033[0m")


def run_ingest():
	all_item_ids = set()
	all_track_ids = set()
	has_failures = False
	any_changed = False

	# ── Phase 1: collect diffs for all live sources ──────────────────────────
	#
	# We compute a SPARQL Update fragment for each live source whose content has
	# changed, then execute all fragments in a single HTTP request.  Fuseki runs
	# multi-statement SPARQL Updates in one TDB2 transaction, so readers never
	# see a partially-updated raw graph.
	#
	# Ontologies keep the old replace_graph approach: they almost never change
	# (the hash check means they're almost always skipped), so the atomicity
	# benefit is negligible.

	phase1_fragments: list[str] = []
	# (system, url, content, content_type, new_hash) for sources that need
	# search-index + hash updates after Phase 1 completes
	changed_live: list[tuple] = []

	for system, url in live_systems.items():
		try:
			(content, content_type) = fetch_url(system, url)
			new_hash = "sha256:" + hashlib.sha256((content + content_type).encode("utf-8")).hexdigest()
			if get_source_hash(url) == new_hash:
				print(f"Skipping {system}: content unchanged (hash {new_hash})", flush=True)
				updateScheduleTracker(success=True, system="lucos_arachne", job_name=system)
				continue
			fragment = diff_graph_in_triplestore(url, content, content_type)
			if fragment:
				phase1_fragments.append(fragment)
			changed_live.append((system, url, content, content_type, new_hash))
		except Exception as e:
			has_failures = True
			error_message = f"Ingest of {system} failed: {e}"
			print(error_message, flush=True)
			updateScheduleTracker(success=False, system="lucos_arachne", job_name=system, message=error_message)

	# ── Execute Phase 1 atomically ────────────────────────────────────────────
	if phase1_fragments:
		combined_update = " ;\n".join(phase1_fragments)
		try:
			execute_sparql_update(combined_update)
			print(f"Phase 1 complete: {len(phase1_fragments)} graph(s) updated atomically", flush=True)
			any_changed = True
		except Exception as e:
			has_failures = True
			error_message = f"Phase 1 (atomic SPARQL Update) failed: {e}"
			print(error_message, flush=True)
			# Don't proceed with hash/searchindex updates if Phase 1 failed
			for system, url, _, _, _ in changed_live:
				updateScheduleTracker(
					success=False,
					system="lucos_arachne",
					job_name=system,
					message=error_message,
				)
			changed_live = []

	# ── Post-Phase-1: update search indices and hashes ────────────────────────
	for system, url, content, content_type, new_hash in changed_live:
		try:
			(item_ids, track_ids) = update_searchindex(system, content, content_type)
			all_item_ids |= item_ids
			all_track_ids |= track_ids
			set_source_hash(url, new_hash)
			updateScheduleTracker(success=True, system="lucos_arachne", job_name=system)
		except Exception as e:
			has_failures = True
			error_message = f"Post-ingest update for {system} failed: {e}"
			print(error_message, flush=True)
			updateScheduleTracker(success=False, system="lucos_arachne", job_name=system, message=error_message)

	# ── Ontologies: existing replace_graph approach ───────────────────────────
	for system, (graph_uri, local_file, content_type) in ontology_cache.items():
		try:
			file_path = os.path.join(ONTOLOGIES_DIR, local_file)
			with open(file_path, "r", encoding="utf-8") as f:
				content = f.read()
			new_hash = "sha256:" + hashlib.sha256((content + content_type).encode("utf-8")).hexdigest()
			if get_source_hash(graph_uri) == new_hash:
				print(f"Skipping {system}: content unchanged (hash {new_hash})", flush=True)
				updateScheduleTracker(success=True, system="lucos_arachne", job_name=system)
				continue
			replace_graph_in_triplestore(graph_uri, content, content_type)
			set_source_hash(graph_uri, new_hash)
			any_changed = True
			updateScheduleTracker(success=True, system="lucos_arachne", job_name=system)
		except Exception as e:
			has_failures = True
			error_message = f"Ingest of {system} failed: {e}"
			print(error_message, flush=True)
			updateScheduleTracker(success=False, system="lucos_arachne", job_name=system, message=error_message)

	# ── Phase 2: rebuild inferred graph if any source changed ─────────────────
	if any_changed:
		try:
			compute_inferences()
			updateScheduleTracker(success=True, system="lucos_arachne", job_name="inference")
		except Exception as e:
			has_failures = True
			error_message = f"Inference computation failed: {e}"
			print(error_message, flush=True)
			updateScheduleTracker(success=False, system="lucos_arachne", job_name="inference", message=error_message)
	else:
		print("Skipping inference: no source graphs changed this cycle", flush=True)
		updateScheduleTracker(success=True, system="lucos_arachne", job_name="inference")

	# ── Person-merge step: compute foaf:Person closures and upsert merged docs ──
	# Runs after all source ingests so the triplestore reflects the full current state
	# (including any newly-added owl:sameAs / preferredIdentifier triples).
	try:
		contacts_graph_uri = live_systems.get("lucos_contacts", "")
		person_ids = update_person_docs_in_searchindex(triplestore_session, contacts_graph_uri)
		all_item_ids |= person_ids
	except Exception as e:
		has_failures = True
		error_message = f"Person merge step failed: {e}"
		print(error_message, flush=True)

	all_graph_uris = (
		list(live_systems.values())
		+ [graph_uri for graph_uri, _, _ in ontology_cache.values()]
		+ [INFERRED_GRAPH, METADATA_GRAPH]
	)
	if has_failures:
		print("Skipping cleanup: one or more sources failed to ingest. Stale items will be cleaned up on the next successful run.", flush=True)
	else:
		cleanup_triplestore(all_graph_uris)
		cleanup_searchindex(all_item_ids, all_track_ids)
		# Touch the reconcile marker so server.py's health check knows the graph is
		# fully healed. Only written here — in the clean, full-reconcile branch —
		# never at the unconditional updateScheduleTracker(job_name="ingestor") call
		# below, which fires even on partial reconciles and would clear the check
		# prematurely while gaps remain.
		reconcile_marker = os.path.expanduser("~/last_successful_reconcile")
		with open(reconcile_marker, "a"):
			os.utime(reconcile_marker, None)

	if has_failures and not any_changed:
		human_readable = "Knowledge graph ingest failed — no updates applied"
	elif has_failures:
		human_readable = "Knowledge graph partially updated — some sources failed"
	elif any_changed:
		human_readable = "Knowledge graph updated"
	else:
		human_readable = "Knowledge graph checked — no changes"
	level = "notable" if has_failures else "routine"
	updateLoganne(type="knowledgeIngest", humanReadable=human_readable, level=level, url=BASE_URL)
	updateScheduleTracker(success=True, system="lucos_arachne", job_name="ingestor")


if __name__ == "__main__":
	# Defer the initial ingest to avoid contributing to startup load spikes
	# when multiple containers start simultaneously (thundering herd).
	# Uses a random jitter within the delay window to stagger concurrent starts.
	try:
		startup_delay = int(os.environ.get("INGEST_STARTUP_DELAY", "30"))
	except ValueError:
		startup_delay = 30
	if startup_delay > 0:
		jitter = random.uniform(0, startup_delay)
		print(f"Deferring initial ingest by {jitter:.0f}s (max {startup_delay}s)")
		time.sleep(jitter)

	try:
		run_ingest()
	except Exception as e:
		error_message = f"Ingest failed: {e}"
		updateScheduleTracker(success=False, system="lucos_arachne", job_name="ingestor", message=error_message)
		sys.exit(error_message)
