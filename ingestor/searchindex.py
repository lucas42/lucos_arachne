import os, sys
from rdflib import Graph, Namespace, RDF, RDFS, FOAF, SKOS, DC, Literal
import typesense
import urllib.parse

# Namespace not included in rdflib
MO = Namespace("http://purl.org/ontology/mo/")
LOC_NS = Namespace("http://www.loc.gov/mads/rdf/v1#")
EOLAS_NS = Namespace(f"https://eolas.l42.eu/ontology/")

eolas_graph = Graph()
# Keep a cache of type => category mappings
type_category_cache = {}

# RDF/OWL types which shouldn't be indexed in search index
IGNORE_TYPES = [
	"http://www.w3.org/2002/07/owl#ObjectProperty",
	"http://www.w3.org/2002/07/owl#Class",
	"http://www.w3.org/2000/01/rdf-schema#Class",
	"http://www.w3.org/2002/07/owl#DatatypeProperty",
	"http://www.w3.org/2002/07/owl#Ontology",
	"https://eolas.l42.eu/ontology/Category",
]

KEY_LUCOS_ARACHNE = os.environ.get("KEY_LUCOS_ARACHNE")

if not KEY_LUCOS_ARACHNE:
	sys.exit(
		"No KEY_LUCOS_ARACHNE environment variable found — won't be able to authenticate against triplestore endpoint"
	)

def get_label(graph, uri):
	for label in graph.objects(uri, SKOS.prefLabel):
		if label.language is None or label.language == 'en':
			return str(label)

	raise ValueError(f"Unknown URI encountered when looking for label: {uri}")

def get_category(graph, type):
	global type_category_cache
	for category in graph.objects(type, EOLAS_NS.hasCategory):
		label = get_label(eolas_graph, category)
		type_category_cache[type] = label
		return label
	# If there's isn't a mapping in the current graph, check the cache for one from an existing graph
	if type in type_category_cache:
		return type_category_cache[type]
	raise ValueError(f"Can't find category for type {type}")

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
			"category": None,
			"pref_label": None,
			"labels": [],
			"description": None,
			"lyrics": None,
			"lang_family": None,
		}

		# type
		for o in graph.objects(subj, RDF.type):
			if str(o) in IGNORE_TYPES:
				continue
			doc["type"] = get_label(graph, o)
			doc["category"] = get_category(graph, o)
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

		for o in graph.objects(subj, LOC_NS.hasBroaderExternalAuthority):
			doc["lang_family"] = str(o).split('/')[-1]
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


def update_searchindex(system, content, content_type, full_graph):
	if not system.startswith("lucos_"):
		return
	g = Graph()
	g.parse(data=content, format=content_type)

	# Some data in the eolas graph is used for other ingestion (eg category labels)
	if system == "lucos_eolas" and full_graph:
		global eolas_graph
		eolas_graph = g
	docs = graph_to_typesense_docs(g)
	results = typesense_client.collections["items"].documents.import_(docs, {"action": "upsert"})
	for result in results:
		if not result["success"]:
			raise ValueError(f"Error returned from search index upsert: {result["error"]}")
	print(f"Upserted {len(results)} documents to triplestore from {system}")

def delete_doc_in_searchindex(system, doc_id):
	if not system.startswith("lucos_"):
		return

	# Typesense library doesn't do escaping when it's needed.
	escaped_id = urllib.parse.quote_plus(doc_id)
	typesense_client.collections["items"].documents[escaped_id].delete()