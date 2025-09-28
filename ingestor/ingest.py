#!/usr/bin/env python3
"""
Bulk ingests RDF from other systems and adds data to the triplestore and searchindex
"""
import sys, os
import requests
from authorised_fetch import fetch_url
from triplestore import update_triplestore, cleanup_triplestore
from searchindex import update_searchindex

SCHEDULE_TRACKER_ENDPOINT = os.environ.get("SCHEDULE_TRACKER_ENDPOINT")
LOGANNE_ENDPOINT = os.environ.get("LOGANNE_ENDPOINT")

session = requests.Session()
session.headers.update({
	"User-Agent": "lucos_arachne_ingestor",
})

if __name__ == "__main__":
	try:
		urls = {
			"lucos_eolas": "https://eolas.l42.eu/metadata/all/data/",
			"lucos_contacts": "https://contacts.l42.eu/people/all",
			"lucos_media_metadata_api": "https://media-api.l42.eu/v2/export",
			"foaf": "http://xmlns.com/foaf/spec/",
			"time": "https://www.w3.org/2006/time",
			"dbpedia_meanOfTransportation": "https://dbpedia.org/ontology/MeanOfTransportation",
			"skos": "http://www.w3.org/2004/02/skos/core",
			"owl": "https://www.w3.org/2002/07/owl",
			"dc": "http://purl.org/dc/terms/",
			"dcam": "http://purl.org/dc/dcam/",
			"rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns",
			"rdfs": "http://www.w3.org/2000/01/rdf-schema",
		}
		for system, url in urls.items():
			(content, content_type) = fetch_url(system, url)
			update_triplestore(url, content, content_type)
			update_searchindex(system, content, content_type)
		cleanup_triplestore(urls.values())

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
