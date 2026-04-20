"""Tests for triplestore.py helper functions."""
import os
os.environ.setdefault("KEY_LUCOS_ARACHNE", "test-key")

from unittest.mock import MagicMock, patch, call
import triplestore


def _mock_ok_response():
    resp = MagicMock()
    resp.ok = True
    resp.raise_for_status = MagicMock()
    resp.json.return_value = {"tripleCount": 0}
    return resp


# ---------------------------------------------------------------------------
# merge_items_in_triplestore
# ---------------------------------------------------------------------------

def test_merge_sends_single_sparql_update():
    """merge_items_in_triplestore makes exactly one POST to the update endpoint."""
    with patch.object(triplestore.session, "post", return_value=_mock_ok_response()) as mock_post:
        triplestore.merge_items_in_triplestore(
            "https://example.com/old",
            "https://example.com/new",
            "https://example.com/graph",
        )
    assert mock_post.call_count == 1
    url = mock_post.call_args.args[0]
    assert url == "http://triplestore:3030/raw_arachne/update"


def test_merge_sparql_moves_subject_position_triples():
    """SPARQL inserts target subject triples and deletes source subject triples."""
    with patch.object(triplestore.session, "post", return_value=_mock_ok_response()) as mock_post:
        triplestore.merge_items_in_triplestore(
            "https://example.com/old",
            "https://example.com/new",
            "https://example.com/graph",
        )
    sparql = mock_post.call_args.kwargs["data"]
    # INSERT target as subject
    assert "<https://example.com/new> ?p ?o" in sparql
    # DELETE source as subject
    assert "<https://example.com/old> ?p ?o" in sparql
    # Scoped to the named graph
    assert "<https://example.com/graph>" in sparql


def test_merge_sparql_repoints_object_position_triples():
    """SPARQL inserts target as object and deletes source as object across all graphs."""
    with patch.object(triplestore.session, "post", return_value=_mock_ok_response()) as mock_post:
        triplestore.merge_items_in_triplestore(
            "https://example.com/old",
            "https://example.com/new",
            "https://example.com/graph",
        )
    sparql = mock_post.call_args.kwargs["data"]
    # INSERT target as object (variable graph)
    assert "?s ?p <https://example.com/new>" in sparql
    # DELETE source as object (variable graph)
    assert "?s ?p <https://example.com/old>" in sparql
    # Variable graph used for object-position (cross-graph coverage)
    assert "GRAPH ?g" in sparql


def test_merge_raises_on_triplestore_error():
    """raise_for_status propagates errors from the triplestore."""
    mock_resp = MagicMock()
    mock_resp.raise_for_status.side_effect = Exception("500 Internal Server Error")
    with patch.object(triplestore.session, "post", return_value=mock_resp):
        try:
            triplestore.merge_items_in_triplestore(
                "https://example.com/old",
                "https://example.com/new",
                "https://example.com/graph",
            )
            assert False, "Expected exception not raised"
        except Exception as e:
            assert "500" in str(e)


# ---------------------------------------------------------------------------
# ontology_cache — Music Ontology
# ---------------------------------------------------------------------------

def test_music_ontology_in_cache():
    """music_ontology entry is present in ontology_cache with correct graph URI."""
    assert "music_ontology" in triplestore.ontology_cache
    graph_uri, filename, content_type = triplestore.ontology_cache["music_ontology"]
    assert graph_uri == "http://purl.org/ontology/mo/"
    assert filename == "musicontology.n3"
    assert content_type == "text/turtle"


def test_music_ontology_file_exists():
    """The cached Music Ontology file exists in the ontologies directory."""
    graph_uri, filename, content_type = triplestore.ontology_cache["music_ontology"]
    file_path = os.path.join(triplestore.ONTOLOGIES_DIR, filename)
    assert os.path.isfile(file_path), f"Missing ontology file: {file_path}"


def test_music_ontology_file_contains_record_label():
    """The Music Ontology file includes a label for mo:Record."""
    graph_uri, filename, content_type = triplestore.ontology_cache["music_ontology"]
    file_path = os.path.join(triplestore.ONTOLOGIES_DIR, filename)
    with open(file_path, encoding="utf-8") as f:
        content = f.read()
    assert "mo:Record" in content
    assert 'rdfs:label "record"' in content


# ---------------------------------------------------------------------------
# get_source_hash / set_source_hash
# ---------------------------------------------------------------------------

def _sparql_response(bindings):
    resp = MagicMock()
    resp.ok = True
    resp.raise_for_status = MagicMock()
    resp.json.return_value = {"results": {"bindings": bindings}}
    return resp


def test_get_source_hash_returns_none_when_missing():
    """get_source_hash returns None when the metadata graph has no entry."""
    with patch.object(triplestore.session, "post", return_value=_sparql_response([])):
        result = triplestore.get_source_hash("https://example.com/graph")
    assert result is None


def test_get_source_hash_returns_stored_value():
    """get_source_hash returns the hash string stored in the triplestore."""
    binding = {"hash": {"value": "sha256:abc123"}}
    with patch.object(triplestore.session, "post", return_value=_sparql_response([binding])):
        result = triplestore.get_source_hash("https://example.com/graph")
    assert result == "sha256:abc123"


def test_get_source_hash_queries_metadata_graph():
    """get_source_hash queries the correct metadata graph and predicate."""
    with patch.object(triplestore.session, "post", return_value=_sparql_response([])) as mock_post:
        triplestore.get_source_hash("https://example.com/graph")
    query = mock_post.call_args.kwargs["data"]["query"]
    assert triplestore.METADATA_GRAPH in query
    assert triplestore.LAST_PAYLOAD_HASH_PRED in query
    assert "https://example.com/graph" in query


def test_set_source_hash_sends_delete_and_insert():
    """set_source_hash issues a SPARQL DELETE + INSERT update."""
    with patch.object(triplestore.session, "post", return_value=_mock_ok_response()) as mock_post:
        triplestore.set_source_hash("https://example.com/graph", "sha256:deadbeef")
    assert mock_post.call_count == 1
    sparql = mock_post.call_args.kwargs["data"]
    assert "DELETE" in sparql
    assert "INSERT" in sparql
    assert triplestore.METADATA_GRAPH in sparql
    assert "sha256:deadbeef" in sparql
    assert "https://example.com/graph" in sparql


def test_set_source_hash_targets_update_endpoint():
    """set_source_hash posts to the SPARQL update endpoint."""
    with patch.object(triplestore.session, "post", return_value=_mock_ok_response()) as mock_post:
        triplestore.set_source_hash("https://example.com/graph", "sha256:deadbeef")
    url = mock_post.call_args.args[0]
    assert url == "http://triplestore:3030/raw_arachne/update"


# ---------------------------------------------------------------------------
# metadata graph allow-list
# ---------------------------------------------------------------------------

def test_metadata_graph_constant_defined():
    """METADATA_GRAPH constant exists and has the correct URI."""
    assert triplestore.METADATA_GRAPH == "urn:lucos:ingestor-metadata"
