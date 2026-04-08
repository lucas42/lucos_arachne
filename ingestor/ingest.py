#!/usr/bin/env python3
"""
Bulk ingests RDF from other systems and adds data to the triplestore and searchindex
"""
import sys, os, time, random
from authorised_fetch import fetch_url
from triplestore import live_systems, ontology_cache, ONTOLOGIES_DIR, INFERRED_GRAPH, replace_graph_in_triplestore, cleanup_triplestore, compute_inferences
from searchindex import update_searchindex, cleanup_searchindex
from loganne import updateLoganne
from schedule_tracker import updateScheduleTracker

try:
	BASE_URL = os.environ["APP_ORIGIN"] + "/"
except KeyError:
	sys.exit("\033[91mAPP_ORIGIN environment variable not set\033[0m")

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
		all_item_ids = set()
		all_track_ids = set()
		for system, url in live_systems.items():
			tracker_system = f"lucos_arachne_ingestor_{system}"
			try:
				(content, content_type) = fetch_url(system, url)
				replace_graph_in_triplestore(url, content, content_type)
				(item_ids, track_ids) = update_searchindex(system, content, content_type)
				all_item_ids |= item_ids
				all_track_ids |= track_ids
				updateScheduleTracker(success=True, system=tracker_system)
			except Exception as e:
				error_message = f"Ingest of {system} failed: {e}"
				print(error_message, flush=True)
				updateScheduleTracker(success=False, system=tracker_system, message=error_message)
		for system, (graph_uri, local_file, content_type) in ontology_cache.items():
			tracker_system = f"lucos_arachne_ingestor_{system}"
			try:
				file_path = os.path.join(ONTOLOGIES_DIR, local_file)
				with open(file_path, "r", encoding="utf-8") as f:
					content = f.read()
				replace_graph_in_triplestore(graph_uri, content, content_type)
				updateScheduleTracker(success=True, system=tracker_system)
			except Exception as e:
				error_message = f"Ingest of {system} failed: {e}"
				print(error_message, flush=True)
				updateScheduleTracker(success=False, system=tracker_system, message=error_message)
		tracker_system = "lucos_arachne_ingestor_inference"
		try:
			compute_inferences()
			updateScheduleTracker(success=True, system=tracker_system)
		except Exception as e:
			error_message = f"Inference computation failed: {e}"
			print(error_message, flush=True)
			updateScheduleTracker(success=False, system=tracker_system, message=error_message)
		all_graph_uris = list(live_systems.values()) + [graph_uri for graph_uri, _, _ in ontology_cache.values()] + [INFERRED_GRAPH]
		cleanup_triplestore(all_graph_uris)
		cleanup_searchindex(all_item_ids, all_track_ids)

		updateLoganne(type="knowledgeIngest", humanReadable="Data ingested into knowledge graph", url=BASE_URL)
	except Exception as e:
		error_message = f"Ingest failed: {e}"
		sys.exit(error_message)
