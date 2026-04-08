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


INFERRED_GRAPH = "urn:lucos:inferred"

def compute_transitive_closures():
	"""
	Computes transitive closures for all owl:TransitiveProperty properties in the
	triplestore and stores the non-direct inferred pairs in the urn:lucos:inferred
	named graph.  Direct pairs stay in their source named graphs and are visible
	via the union default graph on the arachne endpoint.
	"""
	# Find all transitive properties across all named graphs
	resp = session.post(
		"http://triplestore:3030/raw_arachne/sparql",
		headers={"Accept": "application/json"},
		data={"query": "SELECT DISTINCT ?p WHERE { GRAPH ?g { ?p a <http://www.w3.org/2002/07/owl#TransitiveProperty> } }"},
	)
	resp.raise_for_status()
	transitive_props = [b["p"]["value"] for b in resp.json()["results"]["bindings"]]

	if not transitive_props:
		print("No owl:TransitiveProperty properties found — clearing inferred graph")
		replace_graph_in_triplestore(INFERRED_GRAPH, "", "text/turtle")
		return

	print(f"Found {len(transitive_props)} transitive propert{'y' if len(transitive_props) == 1 else 'ies'}: {', '.join('<' + p + '>' for p in transitive_props)}")

	inferred_lines = []

	for prop in transitive_props:
		resp = session.post(
			"http://triplestore:3030/raw_arachne/sparql",
			headers={"Accept": "application/json"},
			data={"query": f"SELECT DISTINCT ?s ?o WHERE {{ GRAPH ?g {{ ?s <{prop}> ?o }} FILTER(?g != <{INFERRED_GRAPH}>) }}"},
		)
		resp.raise_for_status()

		# Build adjacency map: node → set of direct successors
		direct = {}
		for b in resp.json()["results"]["bindings"]:
			s = b["s"]["value"]
			o = b["o"]["value"]
			direct.setdefault(s, set()).add(o)

		direct_pairs = {(s, o) for s, objs in direct.items() for o in objs}
		inferred_for_prop = []

		for start in direct:
			visited = set()
			queue = list(direct.get(start, []))
			while queue:
				node = queue.pop(0)
				if node in visited:
					continue
				visited.add(node)
				if (start, node) not in direct_pairs:
					inferred_for_prop.append((start, node))
				for next_node in direct.get(node, []):
					if next_node not in visited:
						queue.append(next_node)

		print(f"  <{prop}>: {len(direct_pairs)} direct pairs → {len(inferred_for_prop)} inferred")
		for s, o in inferred_for_prop:
			inferred_lines.append(f"<{s}> <{prop}> <{o}> .")

	turtle_content = "\n".join(inferred_lines)
	print(f"Writing {len(inferred_lines)} inferred triple{'s' if len(inferred_lines) != 1 else ''} to <{INFERRED_GRAPH}>")
	replace_graph_in_triplestore(INFERRED_GRAPH, turtle_content, "text/turtle")


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
