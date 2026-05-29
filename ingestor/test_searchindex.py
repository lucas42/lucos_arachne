"""
Tests for searchindex.py helper functions and graph_to_track_docs().

searchindex.py calls sys.exit() at module load if KEY_LUCOS_ARACHNE is not set,
so we inject a dummy value before importing.
"""
import os
os.environ.setdefault("KEY_LUCOS_ARACHNE", "test-key")

import json
from unittest.mock import MagicMock, patch, call
from rdflib import Graph, Namespace, RDF, RDFS, Literal, URIRef
from rdflib.namespace import SKOS, FOAF
from rdflib.namespace import DCTERMS

import searchindex
from searchindex import (
    _extract_search_url_value,
    _extract_language_code,
    _parse_iso8601_duration,
    graph_to_track_docs,
    graph_to_typesense_docs,
    get_label,
    get_category,
    is_meta_type,
    _find_primary_uri,
    _collect_subclass_labels,
    compute_person_closures,
    update_person_docs_in_searchindex,
    _query_person_type_category,
)

MO = Namespace("http://purl.org/ontology/mo/")
BASE = Namespace("http://example.com/")
MEDIA = Namespace("https://media-metadata.l42.eu/")
MMM = Namespace("https://media-api.l42.eu/ontology#")
EOLAS = Namespace("https://eolas.l42.eu/metadata/")


# --- _extract_search_url_value ---

def test_extract_search_url_value_typical():
    url = "https://media-metadata.l42.eu/search?p.artist=The%20Beatles"
    assert _extract_search_url_value(url) == "The Beatles"

def test_extract_search_url_value_no_p_param():
    url = "https://media-metadata.l42.eu/search?artist=The%20Beatles"
    assert _extract_search_url_value(url) is None

def test_extract_search_url_value_non_url():
    assert _extract_search_url_value("just a plain string") is None

def test_extract_search_url_value_empty_string():
    assert _extract_search_url_value("") is None


# --- _extract_language_code ---

def test_extract_language_code_well_formed():
    assert _extract_language_code("https://eolas.l42.eu/metadata/language/fr/") == "fr"

def test_extract_language_code_no_trailing_slash():
    assert _extract_language_code("https://eolas.l42.eu/metadata/language/en") == "en"

def test_extract_language_code_no_language_segment():
    assert _extract_language_code("https://eolas.l42.eu/metadata/person/alice/") is None

def test_extract_language_code_empty_string():
    assert _extract_language_code("") is None


# --- _parse_iso8601_duration ---

def test_parse_iso8601_duration_seconds_only():
    assert _parse_iso8601_duration("PT180S") == 180

def test_parse_iso8601_duration_minutes_and_seconds():
    # PTmMnS is not currently handled — silently returns None.
    # This is acceptable if media metadata always emits PT{n}S format,
    # but worth making explicit: if that assumption changes, update this parser.
    assert _parse_iso8601_duration("PT3M0S") is None

def test_parse_iso8601_duration_non_duration():
    assert _parse_iso8601_duration("not a duration") is None

def test_parse_iso8601_duration_zero():
    assert _parse_iso8601_duration("PT0S") == 0


# --- graph_to_track_docs ---

def _make_track_graph(track_uri, title, **kwargs):
    """Helper: build a minimal Graph with one mo:Track subject."""
    g = Graph()
    subj = URIRef(track_uri)
    g.add((subj, RDF.type, MO.Track))
    g.add((subj, SKOS.prefLabel, Literal(title)))
    for pred, obj in kwargs.items():
        g.add((subj, pred, obj))
    return g


def test_graph_to_track_docs_minimal():
    g = _make_track_graph("http://example.com/track/1", "My Song")
    docs = graph_to_track_docs(g)
    assert len(docs) == 1
    doc = docs[0]
    assert doc["id"] == "http://example.com/track/1"
    assert doc["title"] == "My Song"
    # Optional fields absent when not in graph
    assert "artist" not in doc
    assert "album" not in doc
    assert "duration" not in doc


def test_graph_to_track_docs_skips_non_track():
    g = Graph()
    subj = URIRef("http://example.com/person/1")
    g.add((subj, SKOS.prefLabel, Literal("Alice")))
    # No rdf:type mo:Track
    docs = graph_to_track_docs(g)
    assert docs == []


def test_graph_to_track_docs_skips_track_without_title():
    g = Graph()
    subj = URIRef("http://example.com/track/2")
    g.add((subj, RDF.type, MO.Track))
    # No skos:prefLabel
    docs = graph_to_track_docs(g)
    assert docs == []


def test_graph_to_track_docs_with_optional_fields():
    g = _make_track_graph(
        "http://example.com/track/3",
        "Full Track",
        **{
            FOAF.maker: URIRef("https://media-metadata.l42.eu/search?p.artist=Radiohead"),
            MMM.onAlbum: URIRef("https://media-metadata.l42.eu/albums/1"),
            MO.duration: Literal("PT253S"),
            MMM.trackLanguage: URIRef("https://eolas.l42.eu/metadata/language/en/"),
            MO.lyrics: Literal("I'm a creep"),
        }
    )
    # Add the album entity with its label to the graph
    album_uri = URIRef("https://media-metadata.l42.eu/albums/1")
    g.add((album_uri, SKOS.prefLabel, Literal("OK Computer")))

    docs = graph_to_track_docs(g)
    assert len(docs) == 1
    doc = docs[0]
    assert doc["title"] == "Full Track"
    assert doc["artist"] == ["Radiohead"]
    assert doc["album"] == ["OK Computer"]
    assert doc["duration"] == 253
    assert doc["language"] == ["en"]
    assert doc["lyrics"] == "I'm a creep"


def test_graph_to_track_docs_skips_missing_album_label():
    # If a track references an album via onAlbum, but the album entity
    # is not in the graph, skip that album gracefully.
    g = _make_track_graph(
        "http://example.com/track/4",
        "Track with Missing Album",
        **{
            MMM.onAlbum: URIRef("https://media-metadata.l42.eu/albums/missing"),
        }
    )
    docs = graph_to_track_docs(g)
    assert len(docs) == 1
    assert "album" not in docs[0]


def test_graph_to_track_docs_populates_album_from_onAlbum_predicate():
    # Verify that album field is populated by looking up album entity's skos:prefLabel
    g = _make_track_graph(
        "http://example.com/track/5",
        "Album Test Track",
        **{
            MMM.onAlbum: URIRef("https://media-metadata.l42.eu/albums/1"),
        }
    )
    # Add album entity with its label
    album_uri = URIRef("https://media-metadata.l42.eu/albums/1")
    g.add((album_uri, SKOS.prefLabel, Literal("Test Album")))

    docs = graph_to_track_docs(g)
    assert len(docs) == 1
    assert docs[0]["album"] == ["Test Album"]


def test_graph_to_track_docs_multiple_albums():
    # Verify that multiple album references are all populated correctly
    g = _make_track_graph(
        "http://example.com/track/6",
        "Multi-Album Track",
        **{
            MMM.onAlbum: URIRef("https://media-metadata.l42.eu/albums/1"),
        }
    )
    # Add second album reference (though this is unusual, test for robustness)
    track_uri = URIRef("http://example.com/track/6")
    g.add((track_uri, MMM.onAlbum, URIRef("https://media-metadata.l42.eu/albums/2")))

    # Add album entities
    g.add((URIRef("https://media-metadata.l42.eu/albums/1"), SKOS.prefLabel, Literal("Album One")))
    g.add((URIRef("https://media-metadata.l42.eu/albums/2"), SKOS.prefLabel, Literal("Album Two")))

    docs = graph_to_track_docs(g)
    assert len(docs) == 1
    assert set(docs[0]["album"]) == {"Album One", "Album Two"}


# ---------------------------------------------------------------------------
# get_label — label lookup (local graph only, no triplestore fallback)
# ---------------------------------------------------------------------------

def test_get_label_finds_skos_prefLabel_in_local_graph():
    """skos:prefLabel in local graph is returned."""
    g = Graph()
    uri = URIRef("http://example.com/Type")
    g.add((uri, SKOS.prefLabel, Literal("My Type")))
    result = get_label(g, uri)
    assert result == "My Type"


def test_get_label_finds_rdfs_label_in_local_graph():
    """rdfs:label in local graph is returned."""
    g = Graph()
    uri = URIRef("http://example.com/Type")
    g.add((uri, RDFS.label, Literal("My RDF Type")))
    result = get_label(g, uri)
    assert result == "My RDF Type"


def test_get_label_raises_with_helpful_message_when_not_in_local_graph():
    """ValueError pointing at the source is raised when type metadata is missing from the local graph."""
    g = Graph()
    uri = URIRef("http://purl.org/ontology/mo/Record")
    try:
        get_label(g, uri)
        assert False, "Expected ValueError"
    except ValueError as e:
        msg = str(e)
        assert "http://purl.org/ontology/mo/Record" in msg
        assert "source" in msg.lower()
        assert "lucos_arachne#371" in msg


# ---------------------------------------------------------------------------
# get_category — category lookup (local graph only, no triplestore fallback)
# ---------------------------------------------------------------------------

EOLAS_NS = Namespace("https://eolas.l42.eu/ontology/")


def test_get_category_finds_category_in_local_graph():
    """Category found via eolas:hasCategory in local graph."""
    g = Graph()
    type_uri = URIRef("http://example.com/Type")
    category_uri = URIRef("https://eolas.l42.eu/ontology/SomeCategory")
    g.add((type_uri, EOLAS_NS.hasCategory, category_uri))
    g.add((category_uri, SKOS.prefLabel, Literal("Some Category")))
    result = get_category(g, type_uri)
    assert result == "Some Category"


def test_get_category_raises_with_helpful_message_when_not_in_local_graph():
    """ValueError pointing at the source is raised when category metadata is missing from the local graph."""
    g = Graph()
    type_uri = URIRef("http://purl.org/ontology/mo/Record")
    try:
        get_category(g, type_uri)
        assert False, "Expected ValueError"
    except ValueError as e:
        msg = str(e)
        assert "http://purl.org/ontology/mo/Record" in msg
        assert "source" in msg.lower()
        assert "lucos_arachne#371" in msg


def test_get_category_raises_when_type_has_no_category():
    """ValueError raised when type has no eolas:hasCategory mapping in local graph."""
    g = Graph()
    type_uri = URIRef("http://example.com/UnknownType")
    try:
        get_category(g, type_uri)
        assert False, "Expected ValueError"
    except ValueError as e:
        assert "http://example.com/UnknownType" in str(e)


# ---------------------------------------------------------------------------
# graph_to_typesense_docs — indexing items with type-level vs subject-level category
# ---------------------------------------------------------------------------

OWL = Namespace("http://www.w3.org/2002/07/owl#")


def _make_item_graph(subj_uri, type_uri, type_label, pref_label):
    """Build a minimal graph with one indexable subject (type-level hasCategory on the type)."""
    g = Graph()
    subj = URIRef(subj_uri)
    type_u = URIRef(type_uri)
    cat_uri = URIRef("https://eolas.l42.eu/ontology/Technological")
    g.add((subj, RDF.type, type_u))
    g.add((subj, SKOS.prefLabel, Literal(pref_label)))
    g.add((type_u, SKOS.prefLabel, Literal(type_label)))
    g.add((type_u, EOLAS_NS.hasCategory, cat_uri))
    g.add((cat_uri, SKOS.prefLabel, Literal("Technological")))
    return g


def test_graph_to_typesense_docs_type_level_category():
    """Subject gets category from type-level eolas:hasCategory (e.g. Vehicle -> TransportMode)."""
    g = _make_item_graph(
        "https://eolas.l42.eu/metadata/vehicle/mallard/",
        "https://eolas.l42.eu/metadata/transportmode/train/",
        "Train",
        "Mallard",
    )
    docs = graph_to_typesense_docs(g)
    assert len(docs) == 1
    doc = docs[0]
    assert doc["pref_label"] == "Mallard"
    assert doc["type"] == "Train"
    assert doc["category"] == "Technological"


def test_graph_to_typesense_docs_subject_level_category():
    """
    Subject gets category from subject-level eolas:hasCategory when it's present on the subject URI.

    This covers PlaceType/CreativeWorkType instances whose category is a per-instance field:
    e.g. Country (type=eolas:PlaceType) emits eolas:hasCategory on the Country URI itself,
    not on eolas:PlaceType.  The ingestor must look at the subject first.
    """
    g = Graph()
    subj = URIRef("https://eolas.l42.eu/metadata/placetype/country/")
    type_uri = URIRef("https://eolas.l42.eu/ontology/PlaceType")
    cat_uri = URIRef("https://eolas.l42.eu/ontology/Anthropogeographical")

    g.add((subj, RDF.type, type_uri))
    g.add((subj, SKOS.prefLabel, Literal("Country")))
    # Category is on the SUBJECT, not the type
    g.add((subj, EOLAS_NS.hasCategory, cat_uri))
    g.add((cat_uri, SKOS.prefLabel, Literal("Anthropogeographical")))
    # Type has a label but no hasCategory triple
    g.add((type_uri, SKOS.prefLabel, Literal("Place Type")))

    docs = graph_to_typesense_docs(g)
    assert len(docs) == 1
    doc = docs[0]
    assert doc["pref_label"] == "Country"
    assert doc["type"] == "Place Type"
    assert doc["category"] == "Anthropogeographical"


def test_graph_to_typesense_docs_subject_level_category_takes_precedence():
    """
    Subject-level hasCategory takes precedence over type-level when both are present.

    This is important for TransportMode instances (e.g. Train) which have both:
    - eolas:hasCategory on the subject URI (from get_rdf())
    - eolas:hasCategory on eolas:TransportMode (from ontology_graph() class constant)
    Both point to the same value, so the outcome is identical either way.
    """
    g = Graph()
    subj = URIRef("https://eolas.l42.eu/metadata/transportmode/train/")
    type_uri = URIRef("https://eolas.l42.eu/ontology/TransportMode")
    cat_uri = URIRef("https://eolas.l42.eu/ontology/Technological")

    g.add((subj, RDF.type, type_uri))
    g.add((subj, SKOS.prefLabel, Literal("Train")))
    # Category on both subject and type (TransportMode has a class-level category constant)
    g.add((subj, EOLAS_NS.hasCategory, cat_uri))
    g.add((type_uri, EOLAS_NS.hasCategory, cat_uri))
    g.add((type_uri, SKOS.prefLabel, Literal("Mode of Transport")))
    g.add((cat_uri, SKOS.prefLabel, Literal("Technological")))

    docs = graph_to_typesense_docs(g)
    assert len(docs) == 1
    doc = docs[0]
    assert doc["type"] == "Mode of Transport"
    assert doc["category"] == "Technological"


def test_graph_to_typesense_docs_skips_subject_without_type():
    """Subjects with no rdf:type (other than OWL class URIs) are not indexed."""
    g = Graph()
    subj = URIRef("https://eolas.l42.eu/metadata/thing/mystery/")
    g.add((subj, SKOS.prefLabel, Literal("Mystery")))
    docs = graph_to_typesense_docs(g)
    assert docs == []


def test_graph_to_typesense_docs_skips_subject_without_pref_label():
    """Subjects with a type but no skos:prefLabel are not indexed."""
    g = Graph()
    subj = URIRef("https://eolas.l42.eu/metadata/thing/nolabel/")
    type_uri = URIRef("https://eolas.l42.eu/ontology/SomeType")
    cat_uri = URIRef("https://eolas.l42.eu/ontology/Technological")
    g.add((subj, RDF.type, type_uri))
    g.add((type_uri, SKOS.prefLabel, Literal("Some Type")))
    g.add((type_uri, EOLAS_NS.hasCategory, cat_uri))
    g.add((cat_uri, SKOS.prefLabel, Literal("Technological")))
    docs = graph_to_typesense_docs(g)
    assert docs == []


# ---------------------------------------------------------------------------
# graph_to_typesense_docs — disambiguation fields (contained_in, artist)
# ---------------------------------------------------------------------------

EOLAS_ONTOLOGY = Namespace("https://eolas.l42.eu/ontology/")


def _make_place_graph(place_uri, place_label, contained_in_uri=None, contained_in_label=None):
    """Build a minimal graph for a Place-type item with optional containedIn."""
    g = Graph()
    subj = URIRef(place_uri)
    type_uri = URIRef("https://eolas.l42.eu/ontology/City")
    cat_uri = URIRef("https://eolas.l42.eu/ontology/Anthropogeographical")
    g.add((subj, RDF.type, type_uri))
    g.add((subj, SKOS.prefLabel, Literal(place_label)))
    g.add((type_uri, SKOS.prefLabel, Literal("City")))
    g.add((type_uri, EOLAS_NS.hasCategory, cat_uri))
    g.add((cat_uri, SKOS.prefLabel, Literal("Anthropogeographical")))
    if contained_in_uri and contained_in_label:
        c_uri = URIRef(contained_in_uri)
        g.add((subj, EOLAS_ONTOLOGY.containedIn, c_uri))
        g.add((c_uri, SKOS.prefLabel, Literal(contained_in_label)))
    return g


def test_graph_to_typesense_docs_contained_in_populated_for_place():
    """contained_in field is set to the label of the containedIn target for places."""
    g = _make_place_graph(
        "https://eolas.l42.eu/metadata/place/springfield-il/",
        "Springfield",
        contained_in_uri="https://eolas.l42.eu/metadata/place/illinois/",
        contained_in_label="Illinois",
    )
    docs = graph_to_typesense_docs(g)
    # There are two subjects (Springfield + Illinois type info); find Springfield doc
    springfield = next(d for d in docs if d["pref_label"] == "Springfield")
    assert springfield["contained_in"] == "Illinois"


def test_graph_to_typesense_docs_contained_in_absent_when_no_containedIn():
    """contained_in is absent when the subject has no eolas:containedIn triple."""
    g = _make_place_graph(
        "https://eolas.l42.eu/metadata/place/london/",
        "London",
    )
    docs = graph_to_typesense_docs(g)
    london = next(d for d in docs if d["pref_label"] == "London")
    assert "contained_in" not in london


def test_graph_to_typesense_docs_contained_in_absent_when_target_has_no_label():
    """contained_in is absent when the containedIn target URI has no label in the graph."""
    g = _make_place_graph(
        "https://eolas.l42.eu/metadata/place/somewhere/",
        "Somewhere",
    )
    # Add containedIn triple but NO label for the target
    subj = URIRef("https://eolas.l42.eu/metadata/place/somewhere/")
    target = URIRef("https://eolas.l42.eu/metadata/place/unlabelled/")
    g.add((subj, EOLAS_ONTOLOGY.containedIn, target))
    docs = graph_to_typesense_docs(g)
    somewhere = next(d for d in docs if d["pref_label"] == "Somewhere")
    assert "contained_in" not in somewhere


def test_graph_to_typesense_docs_artist_populated_from_foaf_maker():
    """artist field is set from the foaf:maker search URL for items with a maker."""
    g = _make_track_graph(
        "http://example.com/track/yesterday",
        "Yesterday",
        **{FOAF.maker: URIRef("https://media-metadata.l42.eu/search?p.artist=The%20Beatles")},
    )
    docs = graph_to_track_docs(g)  # this adds to tracks collection
    # For the items collection, we test graph_to_typesense_docs separately
    # Need a proper items-compatible graph (not mo:Track which gets skipped by items indexer
    # if its type is in IGNORE_TYPES — but mo:Track is not in IGNORE_TYPES so it IS indexed)
    # Re-build using graph_to_typesense_docs on the same graph
    # First add type metadata required by graph_to_typesense_docs
    from rdflib import URIRef as U
    MO_LOCAL = Namespace("http://purl.org/ontology/mo/")
    g2 = Graph()
    subj = URIRef("http://example.com/track/yesterday")
    track_type = MO_LOCAL.Track
    cat_uri = URIRef("https://eolas.l42.eu/ontology/Music")
    g2.add((subj, RDF.type, track_type))
    g2.add((subj, SKOS.prefLabel, Literal("Yesterday")))
    g2.add((subj, FOAF.maker, URIRef("https://media-metadata.l42.eu/search?p.artist=The%20Beatles")))
    g2.add((track_type, SKOS.prefLabel, Literal("Track")))
    g2.add((track_type, EOLAS_NS.hasCategory, cat_uri))
    g2.add((cat_uri, SKOS.prefLabel, Literal("Music")))
    docs2 = graph_to_typesense_docs(g2)
    assert len(docs2) == 1
    assert docs2[0]["artist"] == "The Beatles"


def test_graph_to_typesense_docs_artist_absent_when_no_foaf_maker():
    """artist field is absent when the subject has no foaf:maker triple."""
    g = _make_item_graph(
        "https://eolas.l42.eu/metadata/vehicle/mallard/",
        "https://eolas.l42.eu/metadata/transportmode/train/",
        "Train",
        "Mallard",
    )
    docs = graph_to_typesense_docs(g)
    doc = next(d for d in docs if d["pref_label"] == "Mallard")
    assert "artist" not in doc


# ---------------------------------------------------------------------------
# graph_to_typesense_docs — origin field (lucos_arachne#595)
# ---------------------------------------------------------------------------

def test_graph_to_typesense_docs_origin_eolas():
    """origin is the scheme+host of the entity URI (eolas entities)."""
    g = _make_item_graph(
        "https://eolas.l42.eu/metadata/language/en/",
        "https://eolas.l42.eu/metadata/languagetype/",
        "Language",
        "English",
    )
    docs = graph_to_typesense_docs(g)
    assert len(docs) == 1
    assert docs[0]["origin"] == "https://eolas.l42.eu"


def test_graph_to_typesense_docs_origin_media_metadata():
    """origin reflects the host for non-eolas entities (e.g. media-metadata Albums)."""
    g = _make_item_graph(
        "https://media-metadata.l42.eu/albums/42",
        "https://media-metadata.l42.eu/albumtype/",
        "Album",
        "Abbey Road",
    )
    docs = graph_to_typesense_docs(g)
    assert len(docs) == 1
    assert docs[0]["origin"] == "https://media-metadata.l42.eu"


# ---------------------------------------------------------------------------
# is_meta_type — unit tests for the namespace-based filter
# ---------------------------------------------------------------------------

def test_is_meta_type_owl_uri():
    assert is_meta_type("http://www.w3.org/2002/07/owl#ObjectProperty") is True

def test_is_meta_type_rdfs_uri():
    assert is_meta_type("http://www.w3.org/2000/01/rdf-schema#Class") is True

def test_is_meta_type_rdf_uri():
    assert is_meta_type("http://www.w3.org/1999/02/22-rdf-syntax-ns#Property") is True

def test_is_meta_type_eolas_category():
    assert is_meta_type("https://eolas.l42.eu/ontology/Category") is True

def test_is_meta_type_eolas_language_family():
    assert is_meta_type("https://eolas.l42.eu/ontology/LanguageFamily") is True

def test_is_meta_type_skos_concept():
    assert is_meta_type("http://www.w3.org/2004/02/skos/core#Concept") is True

def test_is_meta_type_skos_concept_scheme():
    assert is_meta_type("http://www.w3.org/2004/02/skos/core#ConceptScheme") is True

def test_is_meta_type_domain_uri_not_matched():
    assert is_meta_type("http://purl.org/ontology/mo/Track") is False

def test_is_meta_type_eolas_domain_uri_not_matched():
    assert is_meta_type("https://eolas.l42.eu/ontology/City") is False


# ---------------------------------------------------------------------------
# graph_to_typesense_docs — namespace-based meta-type filter
# ---------------------------------------------------------------------------

def test_graph_to_typesense_docs_skips_owl_symmetric_property():
    """owl:SymmetricProperty is filtered by namespace (was not in old IGNORE_TYPES)."""
    g = Graph()
    subj = URIRef("http://example.com/prop/symmetricProp")
    g.add((subj, RDF.type, OWL.SymmetricProperty))
    docs = graph_to_typesense_docs(g)
    assert docs == []


def test_graph_to_typesense_docs_skips_owl_functional_property():
    """owl:FunctionalProperty is filtered by namespace (was not in old IGNORE_TYPES)."""
    g = Graph()
    subj = URIRef("http://example.com/prop/functionalProp")
    g.add((subj, RDF.type, OWL.FunctionalProperty))
    docs = graph_to_typesense_docs(g)
    assert docs == []


def test_graph_to_typesense_docs_skips_owl_named_individual():
    """owl:NamedIndividual is filtered by namespace (was not in old IGNORE_TYPES)."""
    g = Graph()
    subj = URIRef("http://example.com/thing/someIndividual")
    g.add((subj, RDF.type, OWL.NamedIndividual))
    docs = graph_to_typesense_docs(g)
    assert docs == []


def test_graph_to_typesense_docs_skips_incident_shape():
    """Subject with rdf:type owl:AsymmetricProperty + owl:ObjectProperty + skos:prefLabel produces no doc.

    This is the exact incident shape from lucos_arachne#543 — a source declared a new
    predicate with both types and a prefLabel, which crashed the ingestor before the fix.
    """
    g = Graph()
    subj = URIRef("http://example.com/prop/asymmetricProp")
    g.add((subj, RDF.type, OWL.AsymmetricProperty))
    g.add((subj, RDF.type, OWL.ObjectProperty))
    g.add((subj, SKOS.prefLabel, Literal("Asymmetric Property")))
    docs = graph_to_typesense_docs(g)
    assert docs == []


def test_graph_to_typesense_docs_skips_eolas_category():
    """eolas:Category is still skipped as a domain meta-type (explicit exception)."""
    EOLAS_ONT = Namespace("https://eolas.l42.eu/ontology/")
    g = Graph()
    subj = URIRef("https://eolas.l42.eu/ontology/Technological")
    g.add((subj, RDF.type, EOLAS_ONT.Category))
    g.add((subj, SKOS.prefLabel, Literal("Technological")))
    docs = graph_to_typesense_docs(g)
    assert docs == []


def test_graph_to_typesense_docs_domain_type_not_filtered():
    """Domain types (e.g. mo:Track) are still indexed — namespace check must not over-match."""
    g = _make_item_graph(
        "https://eolas.l42.eu/metadata/person/ada-lovelace/",
        "https://eolas.l42.eu/ontology/Person",
        "Person",
        "Ada Lovelace",
    )
    docs = graph_to_typesense_docs(g)
    assert len(docs) == 1
    assert docs[0]["pref_label"] == "Ada Lovelace"
    assert docs[0]["type"] == "Person"


# ---------------------------------------------------------------------------
# graph_to_typesense_docs — SKOS namespace filtered as infrastructure (#591)
# ---------------------------------------------------------------------------

def test_graph_to_typesense_docs_skips_skos_concept():
    """skos:Concept subjects (media SKOS vocab concepts) are filtered as infrastructure.

    This is the incident shape from lucas42/lucos_media_metadata_api#271: after
    lucos_media_metadata_api#258 migrated provenance/availability/singalong/dance to
    SKOS concept schemes, the media RDF export emits ~44 skos:Concept subjects.
    Without the SKOS namespace in META_NAMESPACES the doc-builder called get_label()
    on the skos:Concept *type*, which raised because the type has no eolas:hasCategory.
    """
    g = Graph()
    subj = URIRef("https://media-api.l42.eu/vocab/provenance/bandcamp")
    g.add((subj, RDF.type, SKOS.Concept))
    g.add((subj, SKOS.prefLabel, Literal("Bandcamp", lang="en")))
    g.add((subj, SKOS.notation, Literal("bandcamp")))
    docs = graph_to_typesense_docs(g)
    assert docs == []


def test_graph_to_typesense_docs_skips_skos_concept_scheme():
    """skos:ConceptScheme subjects are filtered as infrastructure."""
    g = Graph()
    subj = URIRef("https://media-api.l42.eu/ontology#provenanceScheme")
    g.add((subj, RDF.type, SKOS.ConceptScheme))
    g.add((subj, SKOS.prefLabel, Literal("Provenance Scheme", lang="en")))
    docs = graph_to_typesense_docs(g)
    assert docs == []


def test_graph_to_typesense_docs_mo_track_not_filtered_by_skos_addition():
    """mo:Track subjects are still indexed after the SKOS namespace addition.

    A mo:Track carries a skos:prefLabel for its title, so it's important the
    SKOS namespace filter targets the rdf:type URI, not the predicate namespace.
    """
    g = _make_item_graph(
        "https://media-metadata.l42.eu/tracks/1",
        str(MO.Track),
        "Track",
        "Bohemian Rhapsody",
    )
    docs = graph_to_typesense_docs(g)
    assert len(docs) == 1
    assert docs[0]["pref_label"] == "Bohemian Rhapsody"
    assert docs[0]["type"] == "Track"


def test_graph_to_typesense_docs_skips_foaf_person():
    """foaf:Person subjects are excluded — they are handled by the Person-merge step."""
    g = Graph()
    contact_uri = URIRef("https://contacts.l42.eu/people/1")
    g.add((contact_uri, RDF.type, FOAF.Person))
    g.add((contact_uri, FOAF.name, Literal("Alice")))
    docs = graph_to_typesense_docs(g)
    assert docs == []


# ---------------------------------------------------------------------------
# _collect_subclass_labels — pure-function unit tests
# ---------------------------------------------------------------------------

def test_collect_subclass_labels_no_subclass():
    """A type with no rdfs:subClassOf triple returns an empty list."""
    g = Graph()
    type_uri = URIRef("https://eolas.l42.eu/metadata/creativeworktype/1/")
    g.add((type_uri, SKOS.prefLabel, Literal("Film")))
    labels = _collect_subclass_labels(g, type_uri)
    assert labels == []


def test_collect_subclass_labels_single_level():
    """Single-level chain: Film → Creative Work."""
    g = Graph()
    SDO = Namespace("https://schema.org/")
    film_uri = URIRef("https://eolas.l42.eu/metadata/creativeworktype/1/")
    g.add((film_uri, SKOS.prefLabel, Literal("Film")))
    g.add((film_uri, RDFS.subClassOf, SDO.CreativeWork))
    g.add((SDO.CreativeWork, SKOS.prefLabel, Literal("Creative Work")))
    labels = _collect_subclass_labels(g, film_uri)
    assert labels == ["Creative Work"]


def test_collect_subclass_labels_two_level_chain():
    """Two-level chain: ChildType → MidTier → Creative Work."""
    g = Graph()
    SDO = Namespace("https://schema.org/")
    child_uri = URIRef("https://eolas.l42.eu/metadata/creativeworktype/2/")
    mid_uri = URIRef("https://example.com/MidTier")
    g.add((child_uri, SKOS.prefLabel, Literal("Child Type")))
    g.add((child_uri, RDFS.subClassOf, mid_uri))
    g.add((mid_uri, SKOS.prefLabel, Literal("Mid Tier")))
    g.add((mid_uri, RDFS.subClassOf, SDO.CreativeWork))
    g.add((SDO.CreativeWork, SKOS.prefLabel, Literal("Creative Work")))
    labels = _collect_subclass_labels(g, child_uri)
    assert labels == ["Mid Tier", "Creative Work"]


def test_collect_subclass_labels_stops_at_meta_type():
    """Walk stops when it reaches a meta-type (OWL/RDFS/RDF-syntax namespaces)."""
    from rdflib.namespace import OWL as OWL_NS
    g = Graph()
    SDO = Namespace("https://schema.org/")
    type_uri = URIRef("https://eolas.l42.eu/metadata/creativeworktype/3/")
    g.add((type_uri, SKOS.prefLabel, Literal("Film")))
    g.add((type_uri, RDFS.subClassOf, SDO.CreativeWork))
    g.add((SDO.CreativeWork, SKOS.prefLabel, Literal("Creative Work")))
    # Chain continues to owl:Thing — must be stopped
    g.add((SDO.CreativeWork, RDFS.subClassOf, OWL_NS.Thing))
    labels = _collect_subclass_labels(g, type_uri)
    assert labels == ["Creative Work"]
    assert "Thing" not in labels


def test_collect_subclass_labels_shared_ancestor_deduplication():
    """Multiple subClassOf parents sharing a common ancestor: ancestor label appears once."""
    g = Graph()
    SDO = Namespace("https://schema.org/")
    type_uri = URIRef("https://example.com/SpecialType")
    parent_a = URIRef("https://example.com/ParentA")
    parent_b = URIRef("https://example.com/ParentB")
    shared = SDO.CreativeWork
    g.add((type_uri, SKOS.prefLabel, Literal("Special Type")))
    g.add((type_uri, RDFS.subClassOf, parent_a))
    g.add((type_uri, RDFS.subClassOf, parent_b))
    g.add((parent_a, SKOS.prefLabel, Literal("Parent A")))
    g.add((parent_a, RDFS.subClassOf, shared))
    g.add((parent_b, SKOS.prefLabel, Literal("Parent B")))
    g.add((parent_b, RDFS.subClassOf, shared))
    g.add((shared, SKOS.prefLabel, Literal("Creative Work")))
    labels = _collect_subclass_labels(g, type_uri)
    assert labels.count("Creative Work") == 1  # deduplicated
    assert "Parent A" in labels
    assert "Parent B" in labels


def test_collect_subclass_labels_raises_on_missing_label():
    """Ancestor with no prefLabel raises ValueError pointing at the source."""
    g = Graph()
    SDO = Namespace("https://schema.org/")
    type_uri = URIRef("https://eolas.l42.eu/metadata/creativeworktype/4/")
    g.add((type_uri, SKOS.prefLabel, Literal("Film")))
    g.add((type_uri, RDFS.subClassOf, SDO.CreativeWork))
    # SDO.CreativeWork has no prefLabel in the graph
    try:
        _collect_subclass_labels(g, type_uri)
        assert False, "Expected ValueError"
    except ValueError as e:
        msg = str(e)
        assert "https://schema.org/CreativeWork" in msg
        assert "source" in msg.lower()


# ---------------------------------------------------------------------------
# graph_to_typesense_docs — types[] field population
# ---------------------------------------------------------------------------

def _make_item_graph_with_subclass(subj_uri, type_uri, type_label, parent_uri, parent_label, pref_label):
    """Build a graph with a subject whose type has one rdfs:subClassOf parent."""
    g = Graph()
    subj = URIRef(subj_uri)
    type_u = URIRef(type_uri)
    parent_u = URIRef(parent_uri)
    cat_uri = URIRef("https://eolas.l42.eu/ontology/Dramaturgical")
    g.add((subj, RDF.type, type_u))
    g.add((subj, SKOS.prefLabel, Literal(pref_label)))
    g.add((type_u, SKOS.prefLabel, Literal(type_label)))
    g.add((type_u, EOLAS_NS.hasCategory, cat_uri))
    g.add((cat_uri, SKOS.prefLabel, Literal("Dramaturgical")))
    g.add((type_u, RDFS.subClassOf, parent_u))
    g.add((parent_u, SKOS.prefLabel, Literal(parent_label)))
    return g


def test_graph_to_typesense_docs_types_includes_leaf_and_parent():
    """Single-level chain Film → Creative Work: types contains both labels."""
    SDO = Namespace("https://schema.org/")
    g = _make_item_graph_with_subclass(
        "https://eolas.l42.eu/metadata/creativework/casablanca/",
        "https://eolas.l42.eu/metadata/creativeworktype/film/",
        "Film",
        str(SDO.CreativeWork),
        "Creative Work",
        "Casablanca",
    )
    docs = graph_to_typesense_docs(g)
    casablanca = next(d for d in docs if d["pref_label"] == "Casablanca")
    assert casablanca["type"] == "Film"
    assert casablanca["types"] == ["Film", "Creative Work"]


def test_graph_to_typesense_docs_types_two_level_chain():
    """Two-level chain: types contains leaf, mid-tier, and top ancestor."""
    mid_uri = "https://example.com/MidTier"
    top_uri = "https://schema.org/CreativeWork"
    g = Graph()
    subj = URIRef("https://eolas.l42.eu/metadata/creativework/test/")
    type_u = URIRef("https://eolas.l42.eu/metadata/creativeworktype/sub/")
    mid_u = URIRef(mid_uri)
    top_u = URIRef(top_uri)
    cat_uri = URIRef("https://eolas.l42.eu/ontology/Dramaturgical")
    g.add((subj, RDF.type, type_u))
    g.add((subj, SKOS.prefLabel, Literal("Test Work")))
    g.add((type_u, SKOS.prefLabel, Literal("Sub Type")))
    g.add((type_u, EOLAS_NS.hasCategory, cat_uri))
    g.add((cat_uri, SKOS.prefLabel, Literal("Dramaturgical")))
    g.add((type_u, RDFS.subClassOf, mid_u))
    g.add((mid_u, SKOS.prefLabel, Literal("Mid Tier")))
    g.add((mid_u, RDFS.subClassOf, top_u))
    g.add((top_u, SKOS.prefLabel, Literal("Creative Work")))
    docs = graph_to_typesense_docs(g)
    assert len(docs) == 1
    assert docs[0]["types"] == ["Sub Type", "Mid Tier", "Creative Work"]


def test_graph_to_typesense_docs_types_leaf_only_when_no_subclass():
    """Subject with no rdfs:subClassOf on its type: types == [type]."""
    g = _make_item_graph(
        "https://eolas.l42.eu/metadata/vehicle/mallard/",
        "https://eolas.l42.eu/metadata/transportmode/train/",
        "Train",
        "Mallard",
    )
    docs = graph_to_typesense_docs(g)
    mallard = next(d for d in docs if d["pref_label"] == "Mallard")
    assert mallard["type"] == "Train"
    assert mallard["types"] == ["Train"]


def test_graph_to_typesense_docs_types_language_family_single_element():
    """LanguageFamily special-case: types == ['Language'], no further walk."""
    g = Graph()
    lang_uri = URIRef("https://eolas.l42.eu/metadata/language/fr/")
    family_uri = URIRef("http://id.loc.gov/vocabulary/iso639-5/roa")
    g.add((lang_uri, RDF.type, family_uri))
    g.add((lang_uri, SKOS.prefLabel, Literal("French")))
    g.add((family_uri, RDF.type, EOLAS_NS.LanguageFamily))
    g.add((family_uri, SKOS.prefLabel, Literal("Romance languages")))
    docs = graph_to_typesense_docs(g)
    assert len(docs) == 1
    doc = docs[0]
    assert doc["type"] == "Language"
    assert doc["types"] == ["Language"]


# ---------------------------------------------------------------------------
# _find_primary_uri — pure-function unit tests
# ---------------------------------------------------------------------------

def test_find_primary_uri_single_uri():
    """Single URI with no preferredIdentifier edges — returns that URI (lex fallback)."""
    result = _find_primary_uri({"https://example.com/a"}, {})
    assert result == "https://example.com/a"


def test_find_primary_uri_no_edges_lexicographic_fallback():
    """Multiple URIs, no preferredIdentifier edges — returns lexicographic minimum."""
    uris = {"https://z.example.com/a", "https://a.example.com/b", "https://m.example.com/c"}
    result = _find_primary_uri(uris, {})
    assert result == min(uris)


def test_find_primary_uri_single_edge():
    """A → B: B has no outgoing edge — B is the terminal/primary."""
    a = "https://contacts.l42.eu/people/1"
    b = "https://eolas.l42.eu/metadata/person/alice/"
    result = _find_primary_uri({a, b}, {a: b})
    assert result == b


def test_find_primary_uri_chain():
    """Chain A → B → C: C has no outgoing edge — C is the primary."""
    a = "https://contacts.l42.eu/people/1"
    b = "https://eolas.l42.eu/metadata/person/alice/"
    c = "https://canonical.example.com/person/42"
    result = _find_primary_uri({a, b, c}, {a: b, b: c})
    assert result == c


def test_find_primary_uri_edge_to_non_member_ignored():
    """preferredIdentifier edge to a URI outside the closure does not affect selection."""
    a = "https://contacts.l42.eu/people/1"
    b = "https://eolas.l42.eu/metadata/person/alice/"
    outside = "https://other.example.com/person/99"
    # A → outside (outside not in closure), B has no outgoing edge → lex fallback
    result = _find_primary_uri({a, b}, {a: outside})
    assert result == min(a, b)


# ---------------------------------------------------------------------------
# compute_person_closures — unit tests with mock triplestore session
# ---------------------------------------------------------------------------

CONTACTS_GRAPH = "https://contacts.l42.eu/people/all"
FOAF_PERSON_URI = "http://xmlns.com/foaf/0.1/Person"
OWL_SAME_AS_URI = "http://www.w3.org/2002/07/owl#sameAs"
PREFERRED_ID_URI = "https://eolas.l42.eu/ontology/preferredIdentifier"

CONTACT_URI = "https://contacts.l42.eu/people/1"
EOLAS_URI = "https://eolas.l42.eu/metadata/person/alice/"


def _make_sparql_response(bindings: list) -> MagicMock:
    """Build a mock response object returning the given SPARQL JSON bindings."""
    mock_resp = MagicMock()
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json.return_value = {"results": {"bindings": bindings}}
    return mock_resp


def _sparql_binding(var_values: dict) -> dict:
    """Build one SPARQL result binding from {var_name: uri_str}."""
    return {k: {"value": v, "type": "uri"} for k, v in var_values.items()}


def _make_session_for_closures(persons, same_as_pairs, pref_id_pairs, contacts_subjects):
    """
    Build a mock session whose .post() side_effect returns canned SPARQL responses
    for compute_person_closures' four queries (in order):
      1. All foaf:Person URIs
      2. owl:sameAs pairs between Persons
      3. preferredIdentifier pairs between Persons
      4. Subjects in the contacts graph
    """
    session = MagicMock()
    responses = [
        _make_sparql_response([_sparql_binding({"p": p}) for p in persons]),
        _make_sparql_response([_sparql_binding({"a": a, "b": b}) for a, b in same_as_pairs]),
        _make_sparql_response([_sparql_binding({"s": s, "o": o}) for s, o in pref_id_pairs]),
        _make_sparql_response([_sparql_binding({"s": s}) for s in contacts_subjects]),
    ]
    session.post.side_effect = responses
    return session


def test_compute_person_closures_no_persons():
    """No foaf:Person URIs in triplestore → empty list returned."""
    session = MagicMock()
    session.post.return_value = _make_sparql_response([])
    result = compute_person_closures(session, CONTACTS_GRAPH)
    assert result == []


def test_compute_person_closures_two_linked_persons():
    """Two Persons linked by owl:sameAs → one closure, not two."""
    session = _make_session_for_closures(
        persons=[CONTACT_URI, EOLAS_URI],
        same_as_pairs=[(CONTACT_URI, EOLAS_URI)],  # contacts → eolas
        pref_id_pairs=[(CONTACT_URI, EOLAS_URI)],  # contacts prefers eolas
        contacts_subjects=[CONTACT_URI],
    )
    result = compute_person_closures(session, CONTACTS_GRAPH)
    assert len(result) == 1
    primary, secondary, is_contact = result[0]
    assert primary == EOLAS_URI
    assert secondary == [CONTACT_URI]
    assert is_contact is True


def test_compute_person_closures_no_pref_id_lexicographic_fallback():
    """Two linked Persons, no preferredIdentifier → lexicographic min is primary."""
    session = _make_session_for_closures(
        persons=[CONTACT_URI, EOLAS_URI],
        same_as_pairs=[(CONTACT_URI, EOLAS_URI)],
        pref_id_pairs=[],
        contacts_subjects=[CONTACT_URI],
    )
    result = compute_person_closures(session, CONTACTS_GRAPH)
    assert len(result) == 1
    primary, secondary, is_contact = result[0]
    assert primary == min(CONTACT_URI, EOLAS_URI)
    assert is_contact is True


def test_compute_person_closures_chain_primary():
    """Chain A → B → C: C is the terminal/primary."""
    a = "https://a.example.com/person/1"
    b = "https://b.example.com/person/2"
    c = "https://c.example.com/person/3"
    session = _make_session_for_closures(
        persons=[a, b, c],
        same_as_pairs=[(a, b), (b, c)],
        pref_id_pairs=[(a, b), (b, c)],
        contacts_subjects=[],
    )
    result = compute_person_closures(session, CONTACTS_GRAPH)
    assert len(result) == 1
    primary, secondary, is_contact = result[0]
    assert primary == c
    assert sorted(secondary) == sorted([a, b])
    assert is_contact is False


def test_compute_person_closures_single_contact_no_sameAs():
    """Single contacts Person with no sameAs → one-element closure, is_contact=True."""
    session = _make_session_for_closures(
        persons=[CONTACT_URI],
        same_as_pairs=[],
        pref_id_pairs=[],
        contacts_subjects=[CONTACT_URI],
    )
    result = compute_person_closures(session, CONTACTS_GRAPH)
    assert len(result) == 1
    primary, secondary, is_contact = result[0]
    assert primary == CONTACT_URI
    assert secondary == []
    assert is_contact is True


def test_compute_person_closures_no_contact_uri():
    """Closure with no contacts URI → is_contact=False."""
    session = _make_session_for_closures(
        persons=[EOLAS_URI],
        same_as_pairs=[],
        pref_id_pairs=[],
        contacts_subjects=[],  # no contacts subjects
    )
    result = compute_person_closures(session, CONTACTS_GRAPH)
    assert len(result) == 1
    _, _, is_contact = result[0]
    assert is_contact is False


def test_compute_person_closures_sameAs_symmetric():
    """Both A→B and B→A in triplestore (materialised by compute_inferences) → one closure."""
    session = _make_session_for_closures(
        persons=[CONTACT_URI, EOLAS_URI],
        same_as_pairs=[(CONTACT_URI, EOLAS_URI), (EOLAS_URI, CONTACT_URI)],
        pref_id_pairs=[],
        contacts_subjects=[CONTACT_URI],
    )
    result = compute_person_closures(session, CONTACTS_GRAPH)
    assert len(result) == 1  # must be one merged closure, not two


def test_compute_person_closures_two_separate_persons():
    """Two unlinked Persons → two single-element closures."""
    other_eolas = "https://eolas.l42.eu/metadata/person/bob/"
    session = _make_session_for_closures(
        persons=[CONTACT_URI, other_eolas],
        same_as_pairs=[],  # no links
        pref_id_pairs=[],
        contacts_subjects=[CONTACT_URI],
    )
    result = compute_person_closures(session, CONTACTS_GRAPH)
    assert len(result) == 2


# ---------------------------------------------------------------------------
# update_person_docs_in_searchindex — unit tests
# ---------------------------------------------------------------------------

def _make_full_session(persons, same_as_pairs, pref_id_pairs, contacts_subjects,
                       type_label, cat_label, label_bindings):
    """
    Build a mock session for update_person_docs_in_searchindex, which calls:
      1-4: compute_person_closures (4 queries)
      5:   _query_person_type_category (1 query)
      6:   _query_person_labels_batch (1 query)
    """
    session = MagicMock()
    type_cat_binding = [_sparql_binding({"type_label": type_label, "cat_label": cat_label})]
    responses = [
        _make_sparql_response([_sparql_binding({"p": p}) for p in persons]),
        _make_sparql_response([_sparql_binding({"a": a, "b": b}) for a, b in same_as_pairs]),
        _make_sparql_response([_sparql_binding({"s": s, "o": o}) for s, o in pref_id_pairs]),
        _make_sparql_response([_sparql_binding({"s": s}) for s in contacts_subjects]),
        _make_sparql_response(type_cat_binding),
        _make_sparql_response(label_bindings),
    ]
    session.post.side_effect = responses
    return session


def _run_update_person_docs(session, contacts_graph_uri=CONTACTS_GRAPH):
    """
    Helper: run update_person_docs_in_searchindex with typesense_client fully mocked.
    Returns (upserted_docs_list, mock_typesense_client).
    """
    with patch.object(searchindex, "typesense_client") as mock_ts:
        mock_ts.collections.__getitem__.return_value.documents.import_.return_value = [
            {"success": True}
        ]
        result = update_person_docs_in_searchindex(session, contacts_graph_uri)
    return result, mock_ts


def test_update_person_docs_upserts_merged_doc():
    """Two linked Persons → one merged doc upserted with correct fields."""
    session = _make_full_session(
        persons=[CONTACT_URI, EOLAS_URI],
        same_as_pairs=[(CONTACT_URI, EOLAS_URI)],
        pref_id_pairs=[(CONTACT_URI, EOLAS_URI)],
        contacts_subjects=[CONTACT_URI],
        type_label="Person",
        cat_label="Biographical",
        label_bindings=[
            {"s": {"value": EOLAS_URI, "type": "uri"},
             "pred": {"value": "http://www.w3.org/2004/02/skos/core#prefLabel", "type": "uri"},
             "label": {"value": "Alice", "type": "literal"}},
            {"s": {"value": CONTACT_URI, "type": "uri"},
             "pred": {"value": "http://xmlns.com/foaf/0.1/name", "type": "uri"},
             "label": {"value": "Alice Smith", "type": "literal"}},
        ],
    )
    result, mock_ts = _run_update_person_docs(session)
    docs_col = mock_ts.collections.__getitem__.return_value.documents
    docs_col.import_.assert_called_once()
    upserted_docs = docs_col.import_.call_args[0][0]
    assert len(upserted_docs) == 1
    doc = upserted_docs[0]
    assert doc["id"] == EOLAS_URI
    assert doc["type"] == "Person"
    assert doc["category"] == "Biographical"
    assert doc["pref_label"] == "Alice"
    assert doc["secondary_uris"] == [CONTACT_URI]
    assert doc["is_contact"] is True
    assert EOLAS_URI in result


def test_update_person_docs_types_field_populated():
    """Upserted Person doc must include types=[type_label] (regression for #587).

    The regular update_searchindex() path populates types[] via _collect_subclass_labels().
    update_person_docs_in_searchindex() is a separate code path that must populate
    types[] itself.  Person is a leaf type with no rdfs:subClassOf chain, so the
    correct value is [type_label].
    """
    session = _make_full_session(
        persons=[CONTACT_URI, EOLAS_URI],
        same_as_pairs=[(CONTACT_URI, EOLAS_URI)],
        pref_id_pairs=[(CONTACT_URI, EOLAS_URI)],
        contacts_subjects=[CONTACT_URI],
        type_label="Person",
        cat_label="Biographical",
        label_bindings=[
            {"s": {"value": EOLAS_URI, "type": "uri"},
             "pred": {"value": "http://www.w3.org/2004/02/skos/core#prefLabel", "type": "uri"},
             "label": {"value": "Alice", "type": "literal"}},
        ],
    )
    _, mock_ts = _run_update_person_docs(session)
    docs_col = mock_ts.collections.__getitem__.return_value.documents
    doc = docs_col.import_.call_args[0][0][0]
    assert doc["types"] == ["Person"]


def test_update_person_docs_deletes_secondary_uri():
    """Secondary URI doc is deleted from the items collection."""
    session = _make_full_session(
        persons=[CONTACT_URI, EOLAS_URI],
        same_as_pairs=[(CONTACT_URI, EOLAS_URI)],
        pref_id_pairs=[(CONTACT_URI, EOLAS_URI)],
        contacts_subjects=[CONTACT_URI],
        type_label="Person",
        cat_label="Biographical",
        label_bindings=[
            {"s": {"value": EOLAS_URI, "type": "uri"},
             "pred": {"value": "http://www.w3.org/2004/02/skos/core#prefLabel", "type": "uri"},
             "label": {"value": "Alice", "type": "literal"}},
        ],
    )
    _, mock_ts = _run_update_person_docs(session)
    docs_col = mock_ts.collections.__getitem__.return_value.documents
    # documents[escaped_secondary_uri].delete() must have been called once
    docs_col.__getitem__.return_value.delete.assert_called_once()


def test_update_person_docs_secondary_uris_for_lazy_lookup():
    """Merged doc has secondary_uris set so lazy lookup (id:=X || secondary_uris:=X) works."""
    session = _make_full_session(
        persons=[CONTACT_URI, EOLAS_URI],
        same_as_pairs=[(CONTACT_URI, EOLAS_URI)],
        pref_id_pairs=[(CONTACT_URI, EOLAS_URI)],
        contacts_subjects=[CONTACT_URI],
        type_label="Person",
        cat_label="Biographical",
        label_bindings=[
            {"s": {"value": EOLAS_URI, "type": "uri"},
             "pred": {"value": "http://www.w3.org/2004/02/skos/core#prefLabel", "type": "uri"},
             "label": {"value": "Alice", "type": "literal"}},
        ],
    )
    _, mock_ts = _run_update_person_docs(session)
    docs_col = mock_ts.collections.__getitem__.return_value.documents
    doc = docs_col.import_.call_args[0][0][0]
    # secondary_uris must contain the contacts URI for secondary_uris:=CONTACT_URI filter
    assert CONTACT_URI in doc["secondary_uris"]


def test_update_person_docs_no_pref_id_fallback_to_foaf_name():
    """When primary has no skos:prefLabel, fall back to any foaf:name in the closure."""
    session = _make_full_session(
        persons=[CONTACT_URI, EOLAS_URI],
        same_as_pairs=[(CONTACT_URI, EOLAS_URI)],
        pref_id_pairs=[(CONTACT_URI, EOLAS_URI)],
        contacts_subjects=[CONTACT_URI],
        type_label="Person",
        cat_label="Biographical",
        label_bindings=[
            # No skos:prefLabel for EOLAS_URI — only foaf:name on CONTACT_URI
            {"s": {"value": CONTACT_URI, "type": "uri"},
             "pred": {"value": "http://xmlns.com/foaf/0.1/name", "type": "uri"},
             "label": {"value": "Alice Smith", "type": "literal"}},
        ],
    )
    _, mock_ts = _run_update_person_docs(session)
    docs_col = mock_ts.collections.__getitem__.return_value.documents
    doc = docs_col.import_.call_args[0][0][0]
    assert doc["pref_label"] == "Alice Smith"


def test_update_person_docs_is_contact_false_for_eolas_only():
    """Closure with only an eolas URI (no contacts URI) has is_contact=False."""
    session = _make_full_session(
        persons=[EOLAS_URI],
        same_as_pairs=[],
        pref_id_pairs=[],
        contacts_subjects=[],  # contacts graph has no Person subjects
        type_label="Person",
        cat_label="Biographical",
        label_bindings=[
            {"s": {"value": EOLAS_URI, "type": "uri"},
             "pred": {"value": "http://www.w3.org/2004/02/skos/core#prefLabel", "type": "uri"},
             "label": {"value": "Bob", "type": "literal"}},
        ],
    )
    _, mock_ts = _run_update_person_docs(session)
    docs_col = mock_ts.collections.__getitem__.return_value.documents
    doc = docs_col.import_.call_args[0][0][0]
    assert doc["is_contact"] is False


# --- _query_person_type_category language filtering ---

def _literal_binding(value: str, lang: str | None = None) -> dict:
    """Build a SPARQL JSON binding for a literal, with optional language tag."""
    entry = {"type": "literal", "value": value}
    if lang is not None:
        entry["xml:lang"] = lang
    return entry


def test_query_person_type_category_returns_english_when_irish_comes_first():
    """
    Regression test for #569: when the triplestore returns Irish labels before
    English ones, the function must skip the Irish bindings and return English.
    """
    session = MagicMock()
    session.post.return_value = _make_sparql_response([
        # Irish binding — must be skipped
        {
            "type_label": _literal_binding("Duine", "ga"),
            "cat_label":  _literal_binding("Daoine", "ga"),
        },
        # English binding — must be returned
        {
            "type_label": _literal_binding("Person", "en"),
            "cat_label":  _literal_binding("People", "en"),
        },
    ])
    type_label, cat_label = _query_person_type_category(session)
    assert type_label == "Person"
    assert cat_label == "People"


def test_query_person_type_category_returns_untagged_labels():
    """Untagged literals (no xml:lang) must be accepted — they are language-neutral."""
    session = MagicMock()
    session.post.return_value = _make_sparql_response([
        {
            "type_label": _literal_binding("Person"),       # no lang tag
            "cat_label":  _literal_binding("Biographical"), # no lang tag
        },
    ])
    type_label, cat_label = _query_person_type_category(session)
    assert type_label == "Person"
    assert cat_label == "Biographical"


def test_query_person_type_category_returns_none_when_only_non_english():
    """If only non-English bindings remain after SPARQL (defence-in-depth), return (None, None)."""
    session = MagicMock()
    session.post.return_value = _make_sparql_response([
        {
            "type_label": _literal_binding("Duine", "ga"),
            "cat_label":  _literal_binding("Daoine", "ga"),
        },
    ])
    type_label, cat_label = _query_person_type_category(session)
    assert type_label is None
    assert cat_label is None
