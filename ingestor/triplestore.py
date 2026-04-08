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
OWL_TRANSITIVE = "http://www.w3.org/2002/07/owl#TransitiveProperty"
OWL_INVERSE_OF  = "http://www.w3.org/2002/07/owl#inverseOf"

def _sparql_pairs(prop):
	"""Return a set of (subject, object) pairs for prop, excluding the inferred graph."""
	resp = session.post(
		"http://triplestore:3030/raw_arachne/sparql",
		headers={"Accept": "application/json"},
		data={"query": f"SELECT DISTINCT ?s ?o WHERE {{ GRAPH ?g {{ ?s <{prop}> ?o }} FILTER(?g != <{INFERRED_GRAPH}>) }}"},
	)
	resp.raise_for_status()
	return {(b["s"]["value"], b["o"]["value"]) for b in resp.json()["results"]["bindings"]}

def compute_inferences():
	"""
	Computes inferred triples for owl:TransitiveProperty and owl:inverseOf declarations
	found in the triplestore, then writes them all to urn:lucos:inferred in a single
	operation.  Direct triples stay in their source named graphs; the union default graph
	on the arachne endpoint combines both.
	"""
	inferred_lines = []

	# ── Transitive closures ──────────────────────────────────────────────────
	resp = session.post(
		"http://triplestore:3030/raw_arachne/sparql",
		headers={"Accept": "application/json"},
		data={"query": f"SELECT DISTINCT ?p WHERE {{ GRAPH ?g {{ ?p a <{OWL_TRANSITIVE}> }} }}"},
	)
	resp.raise_for_status()
	transitive_props = [b["p"]["value"] for b in resp.json()["results"]["bindings"]]

	if transitive_props:
		print(f"Transitive properties ({len(transitive_props)}): {', '.join('<' + p + '>' for p in transitive_props)}")
		for prop in transitive_props:
			direct_pairs = _sparql_pairs(prop)
			# Build adjacency map: node → set of direct successors
			direct = {}
			for s, o in direct_pairs:
				direct.setdefault(s, set()).add(o)

			inferred_for_prop = []
			for start in direct:
				visited = set()
				queue = list(direct[start])
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

			print(f"  <{prop}>: {len(direct_pairs)} direct → {len(inferred_for_prop)} inferred")
			for s, o in inferred_for_prop:
				inferred_lines.append(f"<{s}> <{prop}> <{o}> .")
	else:
		print("No owl:TransitiveProperty properties found")

	# ── Inverse properties ───────────────────────────────────────────────────
	resp = session.post(
		"http://triplestore:3030/raw_arachne/sparql",
		headers={"Accept": "application/json"},
		data={"query": f"SELECT DISTINCT ?p1 ?p2 WHERE {{ GRAPH ?g {{ ?p1 <{OWL_INVERSE_OF}> ?p2 }} FILTER(?g != <{INFERRED_GRAPH}>) }}"},
	)
	resp.raise_for_status()

	# owl:inverseOf is symmetric — deduplicate (P1,P2) / (P2,P1) into a single canonical pair
	seen = set()
	inverse_pairs = []
	for b in resp.json()["results"]["bindings"]:
		p1, p2 = b["p1"]["value"], b["p2"]["value"]
		canonical = tuple(sorted([p1, p2]))
		if canonical not in seen:
			seen.add(canonical)
			inverse_pairs.append((p1, p2))

	if inverse_pairs:
		print(f"Inverse property pairs ({len(inverse_pairs)}): {', '.join(f'<{a}>/<{b}>' for a, b in inverse_pairs)}")
		for p1, p2 in inverse_pairs:
			pairs_p1 = _sparql_pairs(p1)
			pairs_p2 = _sparql_pairs(p2)
			# Generate P2 inverses for P1 data, and P1 inverses for P2 data
			for src_pairs, src_prop, dst_prop, dst_pairs in [
				(pairs_p1, p1, p2, pairs_p2),
				(pairs_p2, p2, p1, pairs_p1),
			]:
				inferred_for_dir = [(o, s) for s, o in src_pairs if (o, s) not in dst_pairs]
				print(f"  <{src_prop}> → <{dst_prop}>: {len(src_pairs)} direct → {len(inferred_for_dir)} inferred inverses")
				for s, o in inferred_for_dir:
					inferred_lines.append(f"<{s}> <{dst_prop}> <{o}> .")
	else:
		print("No owl:inverseOf pairs found")

	# ── Write inferred graph ─────────────────────────────────────────────────
	turtle_content = "\n".join(inferred_lines)
	print(f"Writing {len(inferred_lines)} total inferred triple{'s' if len(inferred_lines) != 1 else ''} to <{INFERRED_GRAPH}>")
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
