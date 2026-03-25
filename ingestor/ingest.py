#!/usr/bin/env python3
"""
Bulk ingests RDF from other systems and adds data to the triplestore and searchindex
"""
import sys, os, time, random
from authorised_fetch import fetch_url
from triplestore import systems_to_graphs, replace_graph_in_triplestore, cleanup_triplestore
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
		for system, url in systems_to_graphs.items():
			(content, content_type) = fetch_url(system, url)
			replace_graph_in_triplestore(url, content, content_type)
			(item_ids, track_ids) = update_searchindex(system, content, content_type)
			all_item_ids |= item_ids
			all_track_ids |= track_ids
		cleanup_triplestore(systems_to_graphs.values())
		if all_item_ids:
			cleanup_searchindex(all_item_ids, all_track_ids)
		else:
			print("Warning: no items ingested from any source — skipping search index cleanup to avoid accidental data loss")

		updateLoganne(type="knowledgeIngest", humanReadable="Data ingested into knowledge graph", url=BASE_URL)

		updateScheduleTracker(success=True)
	except Exception as e:
		error_message = f"Ingest failed: {e}"
		updateScheduleTracker(success=True, message=error_message)
		sys.exit(error_message)
