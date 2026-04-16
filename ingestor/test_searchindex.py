"""
Tests for searchindex.py helper functions and graph_to_track_docs().

searchindex.py calls sys.exit() at module load if KEY_LUCOS_ARACHNE is not set,
so we inject a dummy value before importing.
"""
import os
os.environ.setdefault("KEY_LUCOS_ARACHNE", "test-key")

from unittest.mock import MagicMock, patch
from rdflib import Graph, Namespace, RDF, RDFS, Literal, URIRef
from rdflib.namespace import SKOS, FOAF
from rdflib.namespace import DCTERMS

import searchindex
from searchindex import (
    _extract_search_url_value,
    _extract_language_code,
    _parse_iso8601_duration,
    graph_to_track_docs,
    get_label,
    get_category,
)

MO = Namespace("http://purl.org/ontology/mo/")
BASE = Namespace("http://example.com/")
MEDIA = Namespace("https://media-metadata.l42.eu/")
MEDIA_MANAGER_ONTOLOGY = Namespace("https://media-metadata.l42.eu/ontology/")
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
            MEDIA_MANAGER_ONTOLOGY.onAlbum: URIRef("https://media-metadata.l42.eu/albums/1"),
            MO.duration: Literal("PT253S"),
            DCTERMS.language: URIRef("https://eolas.l42.eu/metadata/language/en/"),
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
    # is not in the graph or the triplestore, skip that album gracefully.
    g = _make_track_graph(
        "http://example.com/track/4",
        "Track with Missing Album",
        **{
            MEDIA_MANAGER_ONTOLOGY.onAlbum: URIRef("https://media-metadata.l42.eu/albums/missing"),
        }
    )
    empty_resp = MagicMock()
    empty_resp.raise_for_status = MagicMock()
    empty_resp.json.return_value = {"results": {"bindings": []}}
    with patch.object(searchindex._triplestore_session, "post", return_value=empty_resp):
        docs = graph_to_track_docs(g)
    assert len(docs) == 1
    assert "album" not in docs[0]


def test_graph_to_track_docs_populates_album_from_onAlbum_predicate():
    # Verify that album field is populated by looking up album entity's skos:prefLabel
    g = _make_track_graph(
        "http://example.com/track/5",
        "Album Test Track",
        **{
            MEDIA_MANAGER_ONTOLOGY.onAlbum: URIRef("https://media-metadata.l42.eu/albums/1"),
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
            MEDIA_MANAGER_ONTOLOGY.onAlbum: URIRef("https://media-metadata.l42.eu/albums/1"),
        }
    )
    # Add second album reference (though this is unusual, test for robustness)
    track_uri = URIRef("http://example.com/track/6")
    g.add((track_uri, MEDIA_MANAGER_ONTOLOGY.onAlbum, URIRef("https://media-metadata.l42.eu/albums/2")))

    # Add album entities
    g.add((URIRef("https://media-metadata.l42.eu/albums/1"), SKOS.prefLabel, Literal("Album One")))
    g.add((URIRef("https://media-metadata.l42.eu/albums/2"), SKOS.prefLabel, Literal("Album Two")))

    docs = graph_to_track_docs(g)
    assert len(docs) == 1
    assert set(docs[0]["album"]) == {"Album One", "Album Two"}


# ---------------------------------------------------------------------------
# get_label — label lookup with triplestore fallback
# ---------------------------------------------------------------------------

def _mock_triplestore_response(bindings):
    """Build a mock requests.Response for a SPARQL JSON result."""
    mock_resp = MagicMock()
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json.return_value = {"results": {"bindings": bindings}}
    return mock_resp


def test_get_label_finds_skos_prefLabel_in_local_graph():
    """skos:prefLabel in local graph is returned without querying triplestore."""
    g = Graph()
    uri = URIRef("http://example.com/Type")
    g.add((uri, SKOS.prefLabel, Literal("My Type")))
    with patch.object(searchindex._triplestore_session, "post") as mock_post:
        result = get_label(g, uri)
    assert result == "My Type"
    mock_post.assert_not_called()


def test_get_label_finds_rdfs_label_in_local_graph():
    """rdfs:label in local graph is returned (no triplestore query needed)."""
    g = Graph()
    uri = URIRef("http://example.com/Type")
    g.add((uri, RDFS.label, Literal("My RDF Type")))
    with patch.object(searchindex._triplestore_session, "post") as mock_post:
        result = get_label(g, uri)
    assert result == "My RDF Type"
    mock_post.assert_not_called()


def test_get_label_falls_back_to_triplestore_when_not_in_local_graph():
    """When neither label predicate is in the local graph, queries the triplestore."""
    g = Graph()
    uri = URIRef("http://purl.org/ontology/mo/Record")
    mock_bindings = [
        {"pred": {"value": "http://www.w3.org/2000/01/rdf-schema#label"}, "label": {"value": "record"}},
    ]
    with patch.object(searchindex._triplestore_session, "post", return_value=_mock_triplestore_response(mock_bindings)):
        result = get_label(g, uri)
    assert result == "record"


def test_get_label_triplestore_prefers_skos_prefLabel_over_rdfs_label():
    """When triplestore returns both predicates, skos:prefLabel is preferred."""
    g = Graph()
    uri = URIRef("http://purl.org/ontology/mo/Record")
    mock_bindings = [
        {"pred": {"value": "http://www.w3.org/2000/01/rdf-schema#label"}, "label": {"value": "record"}},
        {"pred": {"value": "http://www.w3.org/2004/02/skos/core#prefLabel"}, "label": {"value": "Album", "xml:lang": "en"}},
    ]
    with patch.object(searchindex._triplestore_session, "post", return_value=_mock_triplestore_response(mock_bindings)):
        result = get_label(g, uri)
    assert result == "Album"


def test_get_label_triplestore_prefers_english_over_no_lang():
    """English-tagged labels are preferred over untagged ones."""
    g = Graph()
    uri = URIRef("http://example.com/Type")
    mock_bindings = [
        {"pred": {"value": "http://www.w3.org/2000/01/rdf-schema#label"}, "label": {"value": "untagged"}},
        {"pred": {"value": "http://www.w3.org/2000/01/rdf-schema#label"}, "label": {"value": "English", "xml:lang": "en"}},
    ]
    with patch.object(searchindex._triplestore_session, "post", return_value=_mock_triplestore_response(mock_bindings)):
        result = get_label(g, uri)
    assert result == "English"


def test_get_label_raises_when_not_in_local_graph_or_triplestore():
    """ValueError is raised when the URI has no label anywhere."""
    g = Graph()
    uri = URIRef("http://example.com/Unknown")
    with patch.object(searchindex._triplestore_session, "post", return_value=_mock_triplestore_response([])):
        try:
            get_label(g, uri)
            assert False, "Expected ValueError"
        except ValueError as e:
            assert "http://example.com/Unknown" in str(e)


# ---------------------------------------------------------------------------
# get_category — category lookup with triplestore fallback
# ---------------------------------------------------------------------------

EOLAS_NS = Namespace("https://eolas.l42.eu/ontology/")


def _mock_category_triplestore_response(category_uri=None):
    """Build a mock SPARQL response for a hasCategory query."""
    mock_resp = MagicMock()
    mock_resp.raise_for_status = MagicMock()
    if category_uri:
        mock_resp.json.return_value = {
            "results": {"bindings": [{"category": {"type": "uri", "value": category_uri}}]}
        }
    else:
        mock_resp.json.return_value = {"results": {"bindings": []}}
    return mock_resp


def test_get_category_finds_category_in_local_graph():
    """Category found via eolas:hasCategory in local graph — no triplestore query."""
    g = Graph()
    type_uri = URIRef("http://example.com/Type")
    category_uri = URIRef("https://eolas.l42.eu/ontology/SomeCategory")
    g.add((type_uri, EOLAS_NS.hasCategory, category_uri))
    g.add((category_uri, SKOS.prefLabel, Literal("Some Category")))
    with patch.object(searchindex._triplestore_session, "post") as mock_post:
        result = get_category(g, type_uri)
    assert result == "Some Category"
    mock_post.assert_not_called()


def test_get_category_falls_back_to_triplestore():
    """Category not in local graph: falls back to SPARQL query for hasCategory."""
    g = Graph()
    type_uri = URIRef("http://purl.org/ontology/mo/Record")
    category_uri = "https://eolas.l42.eu/ontology/MusicCategory"
    label_bindings = [
        {"pred": {"value": "http://www.w3.org/2004/02/skos/core#prefLabel"}, "label": {"value": "Music", "xml:lang": "en"}},
    ]

    call_count = 0
    def mock_post_side_effect(url, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            # First call: hasCategory lookup
            return _mock_category_triplestore_response(category_uri)
        else:
            # Second call: label lookup for the category URI
            return _mock_triplestore_response(label_bindings)

    with patch.object(searchindex._triplestore_session, "post", side_effect=mock_post_side_effect):
        result = get_category(g, type_uri)
    assert result == "Music"
    assert call_count == 2


def test_get_category_raises_when_not_in_local_graph_or_triplestore():
    """ValueError raised when type has no category mapping anywhere."""
    g = Graph()
    type_uri = URIRef("http://example.com/UnknownType")
    with patch.object(searchindex._triplestore_session, "post", return_value=_mock_category_triplestore_response(None)):
        try:
            get_category(g, type_uri)
            assert False, "Expected ValueError"
        except ValueError as e:
            assert "http://example.com/UnknownType" in str(e)
