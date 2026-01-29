#!/usr/bin/env python3
"""
Bulk ingests RDF from other systems and adds data to the triplestore and searchindex
"""
import sys, os
from authorised_fetch import fetch_url
from triplestore import systems_to_graphs, replace_graph_in_triplestore, cleanup_triplestore
from searchindex import update_searchindex
from loganne import updateLoganne
from schedule_tracker import updateScheduleTracker

try:
	BASE_URL = os.environ["APP_ORIGIN"] + "/"
except KeyError:
	sys.exit("\033[91mAPP_ORIGIN environment variable not set\033[0m")

if __name__ == "__main__":
	try:
		for system, url in systems_to_graphs.items():
			(content, content_type) = fetch_url(system, url)
			replace_graph_in_triplestore(url, content, content_type)
			update_searchindex(system, content, content_type)
		cleanup_triplestore(systems_to_graphs.values())

		updateLoganne(type="knowledgeIngest", humanReadable="Data ingested into knowledge graph", url=BASE_URL)

		updateScheduleTracker(success=True)
	except Exception as e:
		error_message = f"Ingest failed: {e}"
		updateScheduleTracker(success=True, message=error_message)
		sys.exit(error_message)
