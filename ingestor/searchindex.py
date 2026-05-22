import json, os, sys, re
from rdflib import Graph, Namespace, RDF, RDFS, FOAF, SKOS, DC, Literal, URIRef
from rdflib.namespace import DCTERMS, OWL
import typesense
import urllib.parse

# Namespace not included in rdflib
MO = Namespace("http://purl.org/ontology/mo/")
LOC_NS = Namespace("http://www.loc.gov/mads/rdf/v1#")
EOLAS_NS = Namespace(f"https://eolas.l42.eu/ontology/")
MMM = Namespace("https://media-api.l42.eu/ontology#")
SDO = Namespace("https://schema.org/")

# --- Person-merge constants ---
# URI strings (used in SPARQL queries and dict keys)
FOAF_PERSON = str(FOAF.Person)              # http://xmlns.com/foaf/0.1/Person
OWL_SAME_AS = str(OWL.sameAs)              # http://www.w3.org/2002/07/owl#sameAs
PREFERRED_IDENTIFIER = f"{EOLAS_NS}preferredIdentifier"
EOLAS_HAS_CATEGORY = f"{EOLAS_NS}hasCategory"
# Triplestore SPARQL endpoint (raw_arachne supports GRAPH-clause queries across all named graphs)
TRIPLESTORE_SPARQL_URL = "http://triplestore:3030/raw_arachne/sparql"

# Namespace prefixes whose types are OWL/RDFS infrastructure, not domain content.
# Any rdf:type whose URI starts with one of these is a meta-type and should not
# be indexed in the search index.  This replaces the old explicit IGNORE_TYPES
# denylist, which was incomplete and required a code change each time a new OWL
# property characteristic was used by a source.
META_NAMESPACES = (
	"http://www.w3.org/2002/07/owl#",
	"http://www.w3.org/2000/01/rdf-schema#",
	"http://www.w3.org/1999/02/22-rdf-syntax-ns#",
)

def is_meta_type(uri: str) -> bool:
	"""Return True if the given type URI should be excluded from search indexing."""
	return uri.startswith(META_NAMESPACES) or uri == "https://eolas.l42.eu/ontology/Category"

KEY_LUCOS_ARACHNE = os.environ.get("KEY_LUCOS_ARACHNE")

if not KEY_LUCOS_ARACHNE:
	sys.exit(
		"No KEY_LUCOS_ARACHNE environment variable found — won't be able to authenticate against search index"
	)

def get_label(graph, uri):
	# Check skos:prefLabel in local graph
	for label in graph.objects(uri, SKOS.prefLabel):
		if label.language is None or label.language == 'en':
			return str(label)

	# Check rdfs:label in local graph
	for label in graph.objects(uri, RDFS.label):
		if label.language is None or label.language == 'en':
			return str(label)

	raise ValueError(
		f"Source RDF does not include a label for <{uri}>. "
		f"The source's RDF export must include type metadata (skos:prefLabel and eolas:hasCategory) "
		f"for every domain rdf:type it emits. "
		f"(OWL/RDFS infrastructure types are excluded — see is_meta_type().) "
		f"See lucas42/lucos_arachne#371."
	)

def get_category(graph, type):
	# Check local graph first
	for category in graph.objects(type, EOLAS_NS.hasCategory):
		return get_label(graph, category)

	raise ValueError(
		f"Source RDF does not include an eolas:hasCategory mapping for type <{type}>. "
		f"The source's RDF export must include type metadata (skos:prefLabel and eolas:hasCategory) "
		f"for every domain rdf:type it emits. "
		f"(OWL/RDFS infrastructure types are excluded — see is_meta_type().) "
		f"See lucas42/lucos_arachne#371."
	)

def graph_to_typesense_docs(graph: Graph):
	"""
	Convert an RDFLib Graph into a list of documents
	ready for indexing in Typesense.
	"""
	docs = {}

	for subj in set(graph.subjects()):
		# foaf:Person instances are handled by the Person-merge step, not here.
		# Indexing them individually would produce one doc per URI, conflicting with
		# the merged closure doc (one doc per connected component).
		if (subj, RDF.type, FOAF.Person) in graph:
			continue

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
			if is_meta_type(str(o)):
				continue
			
			# If the type itself has a type of LanguageFamily, then the subject is a Language
			if (o, RDF.type, EOLAS_NS.LanguageFamily) in graph:
				doc["type"] = "Language"
				doc["category"] = "Anthropological"
				doc["lang_family"] = str(o).split('/')[-1]
			else:
				doc["type"] = get_label(graph, o)
				# Prefer subject-level eolas:hasCategory (e.g. PlaceType instances like Country
				# carry their own per-instance category directly on the subject URI).  Fall back
				# to type-level (e.g. Vehicle subjects inherit category from their TransportMode
				# type, which has eolas:hasCategory on the type URI).
				subject_cats = list(graph.objects(subj, EOLAS_NS.hasCategory))
				if subject_cats:
					doc["category"] = get_label(graph, subject_cats[0])
				else:
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

		# contained_in: label of the eolas:containedIn target (for places).
		# Only set when a containedIn triple exists and its target has a label.
		for contained_in_uri in graph.objects(subj, EOLAS_NS.containedIn):
			try:
				doc["contained_in"] = get_label(graph, contained_in_uri)
			except ValueError:
				pass
			break  # use the first containedIn value only

		# artist: first artist name from foaf:maker search URLs (for tracks, albums, etc.).
		for maker_uri in graph.objects(subj, FOAF.maker):
			artist = _extract_search_url_value(str(maker_uri))
			if artist:
				doc["artist"] = artist
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
		for album_uri in graph.objects(subj, MMM.onAlbum):
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

		# language (mmm:trackLanguage) — extract code from URI path
		languages = []
		for o in graph.objects(subj, MMM.trackLanguage):
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
			if str(p) == "https://media-api.l42.eu/ontology#trigger":
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
			if str(p) == "https://media-api.l42.eu/ontology#soundtrack":
				val = _extract_search_url_value(str(o))
				if val:
					soundtracks.append(val)
		if soundtracks:
			doc["soundtrack"] = soundtracks

		docs[doc["id"]] = doc

	return list(docs.values())


# ---------------------------------------------------------------------------
# Person-merge helpers: owl:sameAs closure walk + preferredIdentifier pick
# ---------------------------------------------------------------------------

def _find_primary_uri(uris: set, pref_id_pairs: dict) -> str:
	"""
	Given a set of Person URIs in a closure and a dict of preferredIdentifier edges
	(source → target), walk the chain to find the terminal URI (the one with no
	outgoing edge to another closure member). That terminal URI is the primary.

	Falls back to lexicographic min when no preferredIdentifier edges exist within
	the closure — guarantees a deterministic, reproducible result.
	"""
	# Check whether any preferredIdentifier edges exist within this closure
	edges_in_closure = {
		(s, t) for s, t in pref_id_pairs.items() if s in uris and t in uris
	}
	if not edges_in_closure:
		return min(uris)

	# Find the root(s): URIs that are NOT the target of any edge within the closure
	targets = {t for _, t in edges_in_closure}
	roots = uris - targets
	# Walk from a root (deterministic: use lexicographic min of roots)
	start = min(roots) if roots else min(uris)

	visited = set()
	current = start
	while True:
		if current in visited:
			# Cycle safety — shouldn't happen for an asymmetric property
			break
		visited.add(current)
		nxt = pref_id_pairs.get(current)
		if nxt is None or nxt not in uris:
			return current
		current = nxt
	return current


def compute_person_closures(session, contacts_graph_uri: str) -> list:
	"""
	Query the triplestore for all foaf:Person URIs and owl:sameAs links between
	them, compute symmetric transitive closures (connected components), and for
	each closure determine:
	  - the primary URI (via preferredIdentifier walk, or lexicographic fallback)
	  - the secondary URIs (rest of the closure)
	  - whether any URI in the closure is from lucos_contacts (is_contact)

	Returns a list of (primary_uri, secondary_uris_sorted_list, is_contact) tuples.
	Single-URI Persons (no sameAs links) are included as single-element closures.
	"""
	# 1. Get all foaf:Person URIs across all named graphs
	resp = session.post(
		TRIPLESTORE_SPARQL_URL,
		headers={"Accept": "application/json"},
		data={"query": f"SELECT DISTINCT ?p WHERE {{ GRAPH ?g {{ ?p a <{FOAF_PERSON}> }} }}"},
	)
	resp.raise_for_status()
	all_persons = {b["p"]["value"] for b in resp.json()["results"]["bindings"]}
	if not all_persons:
		return []

	# 2. Get all owl:sameAs triples where the subject is a known Person.
	# Both directions are present in the triplestore via symmetric materialisation
	# in compute_inferences(), so no manual symmetry handling is needed here.
	resp = session.post(
		TRIPLESTORE_SPARQL_URL,
		headers={"Accept": "application/json"},
		data={"query": (
			f"SELECT DISTINCT ?a ?b WHERE {{"
			f" GRAPH ?g {{ ?a <{OWL_SAME_AS}> ?b }}"
			f" GRAPH ?g2 {{ ?a a <{FOAF_PERSON}> }}"
			f"}}"
		)},
	)
	resp.raise_for_status()
	same_as_pairs = [
		(b["a"]["value"], b["b"]["value"])
		for b in resp.json()["results"]["bindings"]
		if b["b"]["value"] in all_persons  # both ends must be known Persons
	]

	# 3. Build adjacency map for BFS/DFS
	adjacency = {p: set() for p in all_persons}
	for a, b in same_as_pairs:
		adjacency.setdefault(a, set()).add(b)

	# 4. Compute connected components (closures) via BFS
	visited = set()
	closures = []
	for person in sorted(all_persons):
		if person in visited:
			continue
		component = set()
		queue = [person]
		while queue:
			node = queue.pop()
			if node in component:
				continue
			component.add(node)
			for neighbor in adjacency.get(node, set()):
				if neighbor not in component:
					queue.append(neighbor)
		visited |= component
		closures.append(component)

	# 5. Get all preferredIdentifier edges (filtered to known Persons on both ends)
	resp = session.post(
		TRIPLESTORE_SPARQL_URL,
		headers={"Accept": "application/json"},
		data={"query": (
			f"SELECT DISTINCT ?s ?o WHERE {{"
			f" GRAPH ?g {{ ?s <{PREFERRED_IDENTIFIER}> ?o }}"
			f" GRAPH ?g2 {{ ?s a <{FOAF_PERSON}> }}"
			f"}}"
		)},
	)
	resp.raise_for_status()
	pref_id_pairs = {}
	for b in resp.json()["results"]["bindings"]:
		s, o = b["s"]["value"], b["o"]["value"]
		if s in all_persons and o in all_persons:
			pref_id_pairs[s] = o

	# 6. Get all subjects in the contacts source graph (to determine is_contact)
	resp = session.post(
		TRIPLESTORE_SPARQL_URL,
		headers={"Accept": "application/json"},
		data={"query": (
			f"SELECT DISTINCT ?s WHERE {{ GRAPH <{contacts_graph_uri}> {{ ?s ?p ?o }} }}"
		)},
	)
	resp.raise_for_status()
	contacts_uris = {b["s"]["value"] for b in resp.json()["results"]["bindings"]}

	# 7. Build result list
	result = []
	for component in closures:
		primary = _find_primary_uri(component, pref_id_pairs)
		secondary = sorted(component - {primary})
		is_contact = bool(component & contacts_uris)
		result.append((primary, secondary, is_contact))
	return result


def _query_person_type_category(session) -> tuple:
	"""
	Query the triplestore for the type label (rdfs:label of foaf:Person) and the
	category label (via eolas:hasCategory).  Returns (type_label, category_label),
	either of which may be None if not found.
	"""
	resp = session.post(
		TRIPLESTORE_SPARQL_URL,
		headers={"Accept": "application/json"},
		data={"query": (
			f"SELECT ?type_label ?cat_label WHERE {{"
			f" {{"
			f"  GRAPH ?g {{ <{FOAF_PERSON}> <{RDFS.label}> ?type_label }}"
			f" }} UNION {{"
			f"  GRAPH ?g {{ <{FOAF_PERSON}> <{SKOS.prefLabel}> ?type_label }}"
			f" }}"
			f" OPTIONAL {{"
			f"  GRAPH ?g2 {{ <{FOAF_PERSON}> <{EOLAS_HAS_CATEGORY}> ?cat }}"
			f"  GRAPH ?g3 {{ ?cat <{SKOS.prefLabel}> ?cat_label }}"
			f" }}"
			f"}} LIMIT 1"
		)},
	)
	resp.raise_for_status()
	bindings = resp.json()["results"]["bindings"]
	if not bindings:
		return (None, None)
	b = bindings[0]
	type_label = b.get("type_label", {}).get("value")
	cat_label = b.get("cat_label", {}).get("value")
	return (type_label, cat_label)


def _query_person_labels_batch(session, uris: set) -> dict:
	"""
	For a set of Person URIs, query the triplestore for skos:prefLabel, foaf:name,
	and rdfs:label values.

	Returns a dict keyed by URI:
	  {"pref_label": str | None, "names": [str, ...]}
	"""
	if not uris:
		return {}
	values = " ".join(f"<{u}>" for u in sorted(uris))
	resp = session.post(
		TRIPLESTORE_SPARQL_URL,
		headers={"Accept": "application/json"},
		data={"query": (
			f"SELECT ?s ?pred ?label WHERE {{"
			f" GRAPH ?g {{"
			f"  VALUES ?s {{ {values} }}"
			f"  VALUES ?pred {{ <{SKOS.prefLabel}> <{FOAF.name}> <{RDFS.label}> }}"
			f"  ?s ?pred ?label"
			f" }}"
			f"}}"
		)},
	)
	resp.raise_for_status()
	result = {u: {"pref_label": None, "names": []} for u in uris}
	for b in resp.json()["results"]["bindings"]:
		uri = b["s"]["value"]
		pred = b["pred"]["value"]
		label = b["label"]["value"]
		if pred == str(SKOS.prefLabel):
			if result[uri]["pref_label"] is None:
				result[uri]["pref_label"] = label
		else:  # foaf:name or rdfs:label → goes into names
			if label not in result[uri]["names"]:
				result[uri]["names"].append(label)
	return result


def update_person_docs_in_searchindex(session, contacts_graph_uri: str) -> set:
	"""
	Compute foaf:Person closures from the triplestore, upsert one merged search-index
	document per closure, delete secondary-URI docs from the index, and return the
	set of primary URIs (to be included in valid_item_ids for cleanup purposes).

	Each merged doc includes:
	  - id: primary URI
	  - type, category: from foaf:Person's RDF metadata in the triplestore
	  - pref_label: primary URI's skos:prefLabel, or first foaf:name in closure
	  - labels: all foaf:name / rdfs:label values across the closure
	  - secondary_uris: sorted list of non-primary URIs in the closure
	  - is_contact: True iff any URI in the closure was fetched from lucos_contacts
	"""
	closures = compute_person_closures(session, contacts_graph_uri)
	if not closures:
		print("No foaf:Person instances found in triplestore — skipping Person merge step", flush=True)
		return set()

	# Query type/category for foaf:Person once (same for all Persons)
	(type_label, category_label) = _query_person_type_category(session)
	if not type_label or not category_label:
		print(
			"Warning: foaf:Person has no type/category metadata in triplestore — "
			"skipping Person docs",
			flush=True,
		)
		return set()

	# Query labels for all Person URIs in one batch
	all_uris = {uri for primary, secondary, _ in closures for uri in [primary] + secondary}
	labels_by_uri = _query_person_labels_batch(session, all_uris)

	docs_to_upsert = []
	primary_ids = set()
	secondary_ids = set()

	for primary, secondary, is_contact in closures:
		# Determine pref_label: prefer primary's skos:prefLabel, fall back to any foaf:name in closure
		pref_label = labels_by_uri.get(primary, {}).get("pref_label")
		if pref_label is None:
			for uri in [primary] + secondary:
				names = labels_by_uri.get(uri, {}).get("names", [])
				if names:
					pref_label = names[0]
					break

		if pref_label is None:
			print(
				f"Warning: no label found for Person closure with primary <{primary}> — skipping",
				flush=True,
			)
			continue

		# Collect all name-style labels across the closure
		all_names = []
		for uri in [primary] + secondary:
			for name in labels_by_uri.get(uri, {}).get("names", []):
				if name not in all_names:
					all_names.append(name)

		doc = {
			"id": primary,
			"type": type_label,
			"category": category_label,
			"pref_label": pref_label,
			"labels": all_names,
			"secondary_uris": secondary,
			"is_contact": is_contact,
		}
		docs_to_upsert.append(doc)
		primary_ids.add(primary)
		secondary_ids.update(secondary)

	# Upsert merged Person docs
	if docs_to_upsert:
		results = typesense_client.collections["items"].documents.import_(
			docs_to_upsert, {"action": "upsert"}
		)
		for result in results:
			if not result["success"]:
				raise ValueError(f"Error upserting Person doc: {result['error']}")
		print(
			f"Upserted {len(docs_to_upsert)} Person documents to items collection",
			flush=True,
		)

	# Delete secondary URI docs (they are now subsumed by the primary's merged doc)
	for doc_id in sorted(secondary_ids):
		try:
			escaped_id = urllib.parse.quote_plus(doc_id)
			typesense_client.collections["items"].documents[escaped_id].delete()
		except Exception as e:
			# Not indexed individually — that's fine
			print(
				f"Note: could not delete secondary Person doc <{doc_id}>: {e}",
				flush=True,
			)
	if secondary_ids:
		print(
			f"Deleted {len(secondary_ids)} secondary Person URI doc(s) from items collection",
			flush=True,
		)

	return primary_ids


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
