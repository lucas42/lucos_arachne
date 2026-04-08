"""
Tests for searchindex.py helper functions and graph_to_track_docs().

searchindex.py calls sys.exit() at module load if KEY_LUCOS_ARACHNE is not set,
so we inject a dummy value before importing.
"""
import os
os.environ.setdefault("KEY_LUCOS_ARACHNE", "test-key")

from rdflib import Graph, Namespace, RDF, Literal, URIRef
from rdflib.namespace import SKOS, FOAF
from rdflib.namespace import DCTERMS

from searchindex import (
    _extract_search_url_value,
    _extract_language_code,
    _parse_iso8601_duration,
    graph_to_track_docs,
)

MO = Namespace("http://purl.org/ontology/mo/")
BASE = Namespace("http://example.com/")
MEDIA = Namespace("https://media-metadata.l42.eu/")
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
            DCTERMS.isPartOf: URIRef("https://media-metadata.l42.eu/search?p.album=OK%20Computer"),
            MO.duration: Literal("PT253S"),
            DCTERMS.language: URIRef("https://eolas.l42.eu/metadata/language/en/"),
            MO.lyrics: Literal("I'm a creep"),
        }
    )
    docs = graph_to_track_docs(g)
    assert len(docs) == 1
    doc = docs[0]
    assert doc["title"] == "Full Track"
    assert doc["artist"] == ["Radiohead"]
    assert doc["album"] == ["OK Computer"]
    assert doc["duration"] == 253
    assert doc["language"] == ["en"]
    assert doc["lyrics"] == "I'm a creep"


def test_graph_to_track_docs_skips_musicbrainz_album():
    g = _make_track_graph(
        "http://example.com/track/4",
        "MBZ Track",
        **{
            DCTERMS.isPartOf: URIRef("https://musicbrainz.org/release/abc123"),
        }
    )
    docs = graph_to_track_docs(g)
    assert len(docs) == 1
    assert "album" not in docs[0]
