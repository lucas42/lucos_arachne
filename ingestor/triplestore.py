import os, sys
import requests

KEY_LUCOS_ARACHNE = os.environ.get("KEY_LUCOS_ARACHNE")

if not KEY_LUCOS_ARACHNE:
	sys.exit("No KEY_LUCOS_ARACHNE environment variable found — won't be able to authenticate against triplestore endpoint")

# Live lucos systems: name → fetch URL (also used as graph URI in the triplestore)
live_systems = {
	"lucos_eolas": "https://eolas.l42.eu/metadata/all/data/",
	"lucos_contacts": "https://contacts.l42.eu/people/all",
	"lucos_media_metadata_api": "https://media-api.l42.eu/v2/export",
}

# 3rd party ontologies: name → (graph_uri, local_filename, content_type)
# Files are cached in ingestor/ontologies/ to avoid relying on external URLs being available.
# (FOAF stopped working Nov 2025, MADS returned 403 Dec 2025, W3C Time had connection resets Mar 2026)
ONTOLOGIES_DIR = os.path.join(os.path.dirname(__file__), "ontologies")
ontology_cache = {
	"foaf": ("https://www.w3.org/archive/xmlns.com/foaf/0.1/ontology", "foaf.rdf", "application/rdf+xml"),
	"time": ("https://www.w3.org/2006/time", "time.ttl", "text/turtle"),
	"dbpedia_meanOfTransportation": ("https://dbpedia.org/ontology/MeanOfTransportation", "dbpedia_meanOfTransportation.ttl", "text/turtle"),
	"skos": ("http://www.w3.org/2004/02/skos/core", "skos.rdf", "application/rdf+xml"),
	"owl": ("https://www.w3.org/2002/07/owl", "owl.ttl", "text/turtle"),
	"dc": ("http://purl.org/dc/terms/", "dc.ttl", "text/turtle"),
	"dcam": ("http://purl.org/dc/dcam/", "dcam.ttl", "text/turtle"),
	"rdf": ("http://www.w3.org/1999/02/22-rdf-syntax-ns", "rdf.ttl", "text/turtle"),
	"rdfs": ("http://www.w3.org/2000/01/rdf-schema", "rdfs.ttl", "text/turtle"),
	"loc_iso639-5": ("http://id.loc.gov/vocabulary/iso639-5/iso639-5_Language", "loc_iso639-5.rdf", "application/rdf+xml"),
	"loc_mads": ("https://id.loc.gov/ontologies/madsrdf/v1.rdf", "loc_mads.rdf", "application/rdf+xml"),
}

session = requests.Session()
session.auth = ("lucos_arachne", KEY_LUCOS_ARACHNE)
session.headers.update({"User-Agent": "lucos_arachne_ingestor"})

def add_triples(graph_uri, content, content_type):
	print(f"Uploading to graph <{graph_uri}> with content-type {content_type!r} ({len(content)} bytes)")
	upload_resp = session.post(
		"http://triplestore:3030/raw_arachne/data",
		params={"graph": graph_uri},
		headers={"Content-Type": content_type},
		data=content.encode("utf-8"),
	)
	if not upload_resp.ok:
		print(f"Triplestore upload failed ({upload_resp.status_code}): {upload_resp.text[:500]}")
	upload_resp.raise_for_status()
	try:
		json_resp = upload_resp.json()
		if "tripleCount" in json_resp:
			print(f"Uploaded {json_resp['tripleCount']} triples to graph <{graph_uri}>")
		else:
			print(f"Upload complete for graph <{graph_uri}>, but no tripleCount in response")
	except ValueError:
		print(f"Upload complete for graph <{graph_uri}>, but response was not JSON")

def replace_graph_in_triplestore(graph_uri, content, content_type):
	# Drop entire graph
	drop_resp = session.post(
		"http://triplestore:3030/raw_arachne/update",
		headers={"Content-Type": "application/sparql-update"},
		data=f"DROP GRAPH <{graph_uri}>",
	)
	drop_resp.raise_for_status()
	add_triples(graph_uri, content, content_type)

# Drop triples where the given item is the subject
def delete_item_in_triplestore(item_uri, graph_uri):
	drop_resp = session.post(
		"http://triplestore:3030/raw_arachne/update",
		headers={"Content-Type": "application/sparql-update"},
		data=f"DELETE WHERE {{ GRAPH <{graph_uri}> {{ <{item_uri}> ?p ?o }} }}",
	)
	drop_resp.raise_for_status()


def replace_item_in_triplestore(item_uri, graph_uri, content, content_type):
	delete_item_in_triplestore(item_uri, graph_uri)
	add_triples(graph_uri, content, content_type)


# Cleans up any graphs in the triplestore which aren't in the list provided
def cleanup_triplestore(graph_uris):
	resp = session.post(
		"http://triplestore:3030/raw_arachne/sparql",
		headers={"Accept": "application/json"},
		data={"query": "SELECT * WHERE {GRAPH ?graph{}}"},
	)
	resp.raise_for_status()
	graphlist = resp.json()
	for binding in graphlist['results']['bindings']:
		graph_uri = binding['graph']['value']
		if graph_uri not in graph_uris:
			print(f"Deleting unknown graph <{graph_uri}>")
			session.post(
				"http://triplestore:3030/raw_arachne/update",
				data={"update": f"DROP GRAPH <{graph_uri}>"},
			)
