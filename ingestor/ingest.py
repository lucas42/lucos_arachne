#!/usr/bin/env python3
"""
Ingests RDF from other systems and adds each as its own graph in the triplestore
"""

import os
import sys
import requests
import re

SCHEDULE_TRACKER_ENDPOINT = os.environ.get("SCHEDULE_TRACKER_ENDPOINT")
LOGANNE_ENDPOINT = os.environ.get("LOGANNE_ENDPOINT")
KEY_LUCOS_ARACHNE = os.environ.get("KEY_LUCOS_ARACHNE")

if not KEY_LUCOS_ARACHNE:
	sys.exit(
		"No KEY_LUCOS_ARACHNE environment variable found — won't be able to authenticate against triplestore endpoint"
	)

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

session = requests.Session()
session.headers.update({"User-Agent": "lucos_arachne_ingestor"})

TRIPLESTORE_AUTH = ("lucos_arachne", KEY_LUCOS_ARACHNE)


def run_ingest():
	for system, url in urls.items():
		auth_header = {}
		if system.startswith("lucos_"):
			key_var = f"KEY_{system.upper()}"
			key = os.environ.get(key_var)
			if not key:
				sys.exit(
					f"No {key_var} environment variable found — won't be able to authenticate against ingestion endpoint {url}"
				)
			auth_header = {"Authorization": f"key {key}"}

		print(f"Ingesting data from {url}")

		# Fetch data
		resp = session.get(
			url,
			headers={
				"Accept": "application/rdf+xml, text/turtle, application/ld+json",
				**auth_header,
			},
			allow_redirects=True,
		)
		resp.raise_for_status()
		content = resp.text

		# Schema.org http → https
		content = re.sub(r"http://schema\.org/", "https://schema.org/", content)
		content_type = resp.headers.get("Content-Type", "").split(";")[0]

		# Drop old graph
		session.post(
			"http://triplestore:3030/raw_arachne/update",
			auth=TRIPLESTORE_AUTH,
			data={"update": f"DROP GRAPH <{url}>"},
		)

		# Upload new data
		upload_resp = session.post(
			f"http://triplestore:3030/raw_arachne/data?graph={url}",
			auth=TRIPLESTORE_AUTH,
			headers={"Content-Type": content_type},
			data=content.encode("utf-8"),
		)
		upload_resp.raise_for_status()
		try:
			json_resp = upload_resp.json()
			if "tripleCount" in json_resp:
				print(f"Uploaded {json_resp['tripleCount']} triples to graph <{url}>")
			else:
				print(f"Upload complete for graph <{url}>, but no tripleCount in response")
		except ValueError:
			print(f"Upload complete for graph <{url}>, but response was not JSON")

	# Cleanup graphs not in list
	resp = session.post(
		"http://triplestore:3030/raw_arachne/sparql",
		auth=TRIPLESTORE_AUTH,
		headers={"Accept": "text/csv"},
		data={"query": "SELECT * WHERE {GRAPH ?g{}}"},
	)
	resp.raise_for_status()
	lines = resp.text.strip().splitlines()[1:]  # skip header
	for line in lines:
		graph_uri = line.strip()
		if graph_uri not in urls.values():
			print(f"Deleting unknown graph <{graph_uri}>")
			session.post(
				"http://triplestore:3030/raw_arachne/update",
				auth=TRIPLESTORE_AUTH,
				data={"update": f"DROP GRAPH <{graph_uri}>"},
			)



if __name__ == "__main__":
	try:
		run_ingest()

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
