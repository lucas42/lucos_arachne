#!/usr/bin/env python3
"""
Bulk ingests RDF from other systems and adds data to the triplestore and searchindex
"""
import sys, os
import requests
from authorised_fetch import fetch_url
from triplestore import systems_to_graphs, replace_graph_in_triplestore, cleanup_triplestore
from searchindex import update_searchindex

SCHEDULE_TRACKER_ENDPOINT = os.environ.get("SCHEDULE_TRACKER_ENDPOINT")
LOGANNE_ENDPOINT = os.environ.get("LOGANNE_ENDPOINT")

session = requests.Session()
session.headers.update({
	"User-Agent": "lucos_arachne_ingestor",
})

if __name__ == "__main__":
	try:
		for system, url in systems_to_graphs.items():
			(content, content_type) = fetch_url(system, url)
			replace_graph_in_triplestore(url, content, content_type)
			update_searchindex(system, content, content_type)
		cleanup_triplestore(systems_to_graphs.values())

		# Loganne
		session.post(
			LOGANNE_ENDPOINT,
			json={
				"type": "knowledgeIngest",
				"source": "lucos_arachne_ingestor",
				"humanReadable": "Data ingested into knowledge graph",
				"url": "https://arachne.l42.eu/",
			},
			headers={"Content-Type": "application/json"},
		)

		# Schedule tracker success
		session.post(
			SCHEDULE_TRACKER_ENDPOINT,
			json={"system": "lucos_arachne_ingestor", "frequency": 3600, "status": "success"},
			headers={"Content-Type": "application/json"},
		)
	except Exception as e:
		error_message = f"Ingest failed: {e}"
		print("Sending error to schedule tracker")
		session.post(
			SCHEDULE_TRACKER_ENDPOINT,
			json={"system": "lucos_arachne_ingestor", "frequency": 3600, "status": "error", "message": error_message},
			headers={"Content-Type": "application/json"},
		)
		sys.exit(error_message)
