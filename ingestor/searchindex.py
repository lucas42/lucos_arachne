import json, os, sys, re
from rdflib import Graph, Namespace, RDF, RDFS, FOAF, SKOS, DC, Literal
from rdflib.namespace import DCTERMS
import typesense
import urllib.parse

# Namespace not included in rdflib
MO = Namespace("http://purl.org/ontology/mo/")
LOC_NS = Namespace("http://www.loc.gov/mads/rdf/v1#")
EOLAS_NS = Namespace(f"https://eolas.l42.eu/ontology/")
MEDIA_MANAGER_ONTOLOGY = Namespace("https://media-metadata.l42.eu/ontology/")
SDO = Namespace("https://schema.org/")

# RDF/OWL types which shouldn't be indexed in search index
IGNORE_TYPES = [
	"http://www.w3.org/2002/07/owl#ObjectProperty",
	"http://www.w3.org/2002/07/owl#Class",
	"http://www.w3.org/2000/01/rdf-schema#Class",
	"http://www.w3.org/2002/07/owl#DatatypeProperty",
	"http://www.w3.org/2002/07/owl#Ontology",
	"http://www.w3.org/2002/07/owl#TransitiveProperty",
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
	for category in graph.objects(type, EOLAS_NS.hasCategory):
		return get_label(graph, category)
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
			
			# If the type itself has a type of LanguageFamily, then the subject is a Language
			if (o, RDF.type, EOLAS_NS.LanguageFamily) in graph:
				doc["type"] = "Language"
				doc["category"] = "Anthropological"
				doc["lang_family"] = str(o).split('/')[-1]
			else:
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


		# only include if we have a type and pref_label
		if doc["type"] and doc["pref_label"]:
			docs[doc["id"]] = doc

	return list(docs.values())


def _extract_search_url_value(uri_str):
	"""Extract the decoded query parameter value from a search URL like
	https://media-metadata.l42.eu/search?p.artist=The%20Beatles -> 'The Beatles'
	"""
	parsed = urllib.parse.urlparse(uri_str)
	params = urllib.parse.parse_qs(parsed.query)
	for key, values in params.items():
		if key.startswith("p.") and values:
			return values[0]
	return None

def _extract_language_code(uri_str):
	"""Extract language code from a URI like
	https://eolas.l42.eu/metadata/language/fr/ -> 'fr'
	"""
	match = re.search(r'/language/([^/]+)/?$', uri_str)
	return match.group(1) if match else None

def _parse_iso8601_duration(value_str):
	"""Parse an ISO 8601 duration like PT180S to integer seconds."""
	match = re.match(r'^PT(\d+)S$', value_str)
	return int(match.group(1)) if match else None

def graph_to_track_docs(graph: Graph):
	"""
	Convert an RDFLib Graph into a list of track documents
	ready for indexing in the Typesense 'tracks' collection.
	Only includes subjects with rdf:type mo:Track.
	"""
	docs = {}

	for subj in set(graph.subjects()):
		# Only include mo:Track subjects
		if (subj, RDF.type, MO.Track) not in graph:
			continue

		doc = {"id": str(subj)}

		# title (skos:prefLabel)
		for o in graph.objects(subj, SKOS.prefLabel):
			if isinstance(o, Literal):
				doc["title"] = str(o)
				break

		if "title" not in doc:
			continue

		# artist (foaf:maker) — search URL values
		artists = []
		for o in graph.objects(subj, FOAF.maker):
			val = _extract_search_url_value(str(o))
			if val:
				artists.append(val)
		if artists:
			doc["artist"] = artists

		# album (onAlbum) — look up album's skos:prefLabel
		albums = []
		for album_uri in graph.objects(subj, MEDIA_MANAGER_ONTOLOGY.onAlbum):
			try:
				album_label = get_label(graph, album_uri)
				albums.append(album_label)
			except ValueError:
				# Album URI not found in graph, skip it
				pass
		if albums:
			doc["album"] = albums

		# genre (mo:genre) — search URL values
		genres = []
		for o in graph.objects(subj, MO.genre):
			val = _extract_search_url_value(str(o))
			if val:
				genres.append(val)
		if genres:
			doc["genre"] = genres

		# composer (mo:composer) — search URL values
		composers = []
		for o in graph.objects(subj, MO.composer):
			val = _extract_search_url_value(str(o))
			if val:
				composers.append(val)
		if composers:
			doc["composer"] = composers

		# producer (mo:producer) — search URL values
		producers = []
		for o in graph.objects(subj, MO.producer):
			val = _extract_search_url_value(str(o))
			if val:
				producers.append(val)
		if producers:
			doc["producer"] = producers

		# language (dc:language) — extract code from URI path
		languages = []
		for o in graph.objects(subj, DCTERMS.language):
			code = _extract_language_code(str(o))
			if code:
				languages.append(code)
		if languages:
			doc["language"] = languages

		# year (dc:date)
		for o in graph.objects(subj, DCTERMS.date):
			if isinstance(o, Literal):
				doc["year"] = str(o)
				break

		# rating (schema:ratingValue)
		for o in graph.objects(subj, SDO.ratingValue):
			if isinstance(o, Literal):
				try:
					doc["rating"] = int(str(o))
				except ValueError:
					pass
				break

		# lyrics (mo:lyrics)
		for o in graph.objects(subj, MO.lyrics):
			if isinstance(o, Literal):
				doc["lyrics"] = str(o)
				break

		# provenance (dc:source) — search URL value
		for o in graph.objects(subj, DCTERMS.source):
			val = _extract_search_url_value(str(o))
			if val:
				doc["provenance"] = val
				break

		# duration (mo:duration) — parse PT{n}S to integer seconds
		for o in graph.objects(subj, MO.duration):
			seconds = _parse_iso8601_duration(str(o))
			if seconds is not None:
				doc["duration"] = seconds
				break

		# offence (custom trigger predicate) — search URL values
		offences = []
		for p, o in graph.predicate_objects(subj):
			if str(p).endswith("/ontology#trigger"):
				val = _extract_search_url_value(str(o))
				if val:
					offences.append(val)
		if offences:
			doc["offence"] = offences

		# comment (schema:comment)
		for o in graph.objects(subj, SDO.comment):
			if isinstance(o, Literal):
				doc["comment"] = str(o)
				break

		# soundtrack (custom soundtrack predicate) — search URL values
		soundtracks = []
		for p, o in graph.predicate_objects(subj):
			if str(p).endswith("/ontology#soundtrack"):
				val = _extract_search_url_value(str(o))
				if val:
					soundtracks.append(val)
		if soundtracks:
			doc["soundtrack"] = soundtracks

		docs[doc["id"]] = doc

	return list(docs.values())


typesense_client = typesense.Client({
    "nodes": [{
        "host": "search",
        "port": "8108",
        "protocol": "http",
    }],
    "api_key": KEY_LUCOS_ARACHNE,
    "connection_timeout_seconds": 30
})


def update_searchindex(system, content, content_type):
	"""
	Upserts documents into the search index from the given system's RDF content.
	Returns a tuple of (item_ids, track_ids) that were upserted.
	"""
	if not system.startswith("lucos_"):
		return (set(), set())
	g = Graph()
	g.parse(data=content, format=content_type)

	item_ids = set()
	docs = graph_to_typesense_docs(g)
	if len(docs) == 0:
		print(f"No docs updated in search index, from {system}", flush=True)
	else:
		results = typesense_client.collections["items"].documents.import_(docs, {"action": "upsert"})
		for result in results:
			if not result["success"]:
				raise ValueError(f"Error returned from search index upsert: {result['error']}")
		print(f"Upserted {len(results)} documents to items collection from {system}")
		item_ids = {doc["id"] for doc in docs}

	# Upsert into tracks collection for track-type subjects
	track_ids = set()
	track_docs = graph_to_track_docs(g)
	if len(track_docs) > 0:
		track_results = typesense_client.collections["tracks"].documents.import_(track_docs, {"action": "upsert"})
		for result in track_results:
			if not result["success"]:
				raise ValueError(f"Error returned from tracks search index upsert: {result['error']}")
		print(f"Upserted {len(track_results)} documents to tracks collection from {system}")
		track_ids = {doc["id"] for doc in track_docs}

	return (item_ids, track_ids)

def _get_all_doc_ids(collection_name):
	"""Export all document IDs from a Typesense collection."""
	ids = set()
	jsonl = typesense_client.collections[collection_name].documents.export({"include_fields": "id"})
	for line in jsonl.strip().split("\n"):
		if line:
			ids.add(json.loads(line)["id"])
	return ids


def cleanup_searchindex(valid_item_ids, valid_track_ids):
	"""
	Remove documents from the search index that are not in the provided sets.
	This cleans up stale docs that were deleted from data sources but not
	removed from the index (e.g. due to a missed webhook).
	Skips cleanup for a collection if no valid IDs were provided, to avoid
	accidentally wiping all data when sources return empty results.
	"""
	if valid_item_ids:
		existing_item_ids = _get_all_doc_ids("items")
		stale_item_ids = existing_item_ids - valid_item_ids
		for doc_id in stale_item_ids:
			escaped_id = urllib.parse.quote_plus(doc_id)
			typesense_client.collections["items"].documents[escaped_id].delete()
		if stale_item_ids:
			print(f"Cleaned up {len(stale_item_ids)} stale documents from items collection")
	else:
		print("Warning: no items ingested — skipping items collection cleanup")

	if valid_track_ids:
		existing_track_ids = _get_all_doc_ids("tracks")
		stale_track_ids = existing_track_ids - valid_track_ids
		for doc_id in stale_track_ids:
			escaped_id = urllib.parse.quote_plus(doc_id)
			typesense_client.collections["tracks"].documents[escaped_id].delete()
		if stale_track_ids:
			print(f"Cleaned up {len(stale_track_ids)} stale documents from tracks collection")
	else:
		print("Warning: no tracks ingested — skipping tracks collection cleanup")


def delete_doc_in_searchindex(system, doc_id):
	if not system.startswith("lucos_"):
		return

	# Typesense library doesn't do escaping when it's needed.
	escaped_id = urllib.parse.quote_plus(doc_id)
	typesense_client.collections["items"].documents[escaped_id].delete()

	# Also try to delete from the tracks collection (may not exist there)
	try:
		typesense_client.collections["tracks"].documents[escaped_id].delete()
	except typesense.exceptions.ObjectNotFound:
		pass
