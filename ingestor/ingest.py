#!/usr/bin/env python3
"""
Ingests RDF from other systems and adds each as its own graph in the triplestore
"""

import os
import sys
import requests
import re
from rdflib import Graph, Namespace, RDF, RDFS, FOAF, Literal
import typesense

# Common namespaces
SKOS = Namespace("http://www.w3.org/2004/02/skos/core#")
DC = Namespace("http://purl.org/dc/terms/")
MO = Namespace("http://purl.org/ontology/mo/")

# Hardcoded type mapping
TYPE_LABELS = {
	"http://xmlns.com/foaf/0.1/Person": "Person",
	"http://purl.org/ontology/mo/Track": "Track",
	"http://www.w3.org/2006/time#DayOfWeek": "Day of Week",
	"http://www.w3.org/2006/time#MonthOfYear": "Month of Year",
	"https://dbpedia.org/ontology/MeanOfTransportation": "Means of Transport",
	"http://www.w3.org/2002/07/owl#ObjectProperty": "Object Property",
	"http://www.w3.org/2002/07/owl#Class": "Class",
	"http://www.w3.org/2000/01/rdf-schema#Class": "Class",
	"http://www.w3.org/2002/07/owl#DatatypeProperty": "Datatype Property",
	"http://www.w3.org/2002/07/owl#Ontology": "Ontology",
}


SCHEDULE_TRACKER_ENDPOINT = os.environ.get("SCHEDULE_TRACKER_ENDPOINT")
LOGANNE_ENDPOINT = os.environ.get("LOGANNE_ENDPOINT")
KEY_LUCOS_ARACHNE = os.environ.get("KEY_LUCOS_ARACHNE")

if not KEY_LUCOS_ARACHNE:
	sys.exit(
		"No KEY_LUCOS_ARACHNE environment variable found — won't be able to authenticate against triplestore endpoint"
	)


session = requests.Session()
session.headers.update({"User-Agent": "lucos_arachne_ingestor"})

TRIPLESTORE_AUTH = ("lucos_arachne", KEY_LUCOS_ARACHNE)

def fetch_url(system, url):
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
	return (content, content_type)

def update_triplestore(graph_url, content, content_type):
	# Drop old graph
	session.post(
		"http://triplestore:3030/raw_arachne/update",
		auth=TRIPLESTORE_AUTH,
		data={"update": f"DROP GRAPH <{graph_url}>"},
	)

	# Upload new data
	upload_resp = session.post(
		f"http://triplestore:3030/raw_arachne/data?graph={graph_url}",
		auth=TRIPLESTORE_AUTH,
		headers={"Content-Type": content_type},
		data=content.encode("utf-8"),
	)
	upload_resp.raise_for_status()
	try:
		json_resp = upload_resp.json()
		if "tripleCount" in json_resp:
			print(f"Uploaded {json_resp['tripleCount']} triples to graph <{graph_url}>")
		else:
			print(f"Upload complete for graph <{graph_url}>, but no tripleCount in response")
	except ValueError:
		print(f"Upload complete for graph <{graph_url}>, but response was not JSON")

def cleanup_triplestore(graph_uris):
	# Cleanup graphs not in list
	# This uses the CSV output because it was ported from bash and that was easier to parse there
	# TODO: Use json output instead
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
		if graph_uri not in graph_uris:
			print(f"Deleting unknown graph <{graph_uri}>")
			session.post(
				"http://triplestore:3030/raw_arachne/update",
				auth=TRIPLESTORE_AUTH,
				data={"update": f"DROP GRAPH <{graph_uri}>"},
			)


def get_type_label(graph, type_uri):
	uri_str = str(type_uri)
	if uri_str in TYPE_LABELS:
		return TYPE_LABELS[uri_str]

	# Try dynamic lookup
	for label in graph.objects(type_uri, SKOS.prefLabel):
		return str(label)

	raise ValueError(f"Unknown type URI encountered: {uri_str}")


def graph_to_typesense_docs(graph: Graph):
	"""
	Convert an RDFLib Graph into a list of documents
	ready for indexing in Typesense.
	"""
	docs = {}

	for subj in set(graph.subjects()):
		doc = {
			"id": str(subj),
			"type": None,
			"pref_label": None,
			"labels": [],
			"description": None,
			"lyrics": None,
		}

		# type (exactly one, from hardcoded mapping only)
		for o in graph.objects(subj, RDF.type):
			doc["type"] = get_type_label(graph, o)
			break

		# pref_label
		for o in graph.objects(subj, SKOS.prefLabel):
			if isinstance(o, Literal):
				doc["pref_label"] = str(o)
				break

		# labels (can be multiple)
		for pred in [RDFS.label, FOAF.name]:
			for obj in graph.objects(subj, pred):
				if isinstance(obj, Literal):
					doc["labels"].append(str(obj))

		# description
		for o in graph.objects(subj, DC.description):
			if isinstance(o, Literal):
				doc["description"] = str(o)
				break

		# lyrics
		for o in graph.objects(subj, MO.lyrics):
			if isinstance(o, Literal):
				doc["lyrics"] = str(o)
				break

		# only include if we have a type and pref_label
		if doc["type"] and doc["pref_label"]:
			docs[doc["id"]] = doc

	return list(docs.values())


typesense_client = typesense.Client({
    "nodes": [{
        "host": "search",
        "port": "8108",
        "protocol": "http",
    }],
    "api_key": KEY_LUCOS_ARACHNE,
    "connection_timeout_seconds": 2
})


def update_searchindex(system, content, content_type):
	if not system.startswith("lucos_"):
		return
	g = Graph()
	g.parse(data=content, format=content_type)
	docs = graph_to_typesense_docs(g)
	results = typesense_client.collections["items"].documents.import_(docs, {"action": "upsert"})
	for result in results:
		if not result["success"]:
			raise Error(f"Error returned from search index upsert: {result["error"]}")
	print(f"Upserted {len(results)} documents to triplestore from {system}")

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
