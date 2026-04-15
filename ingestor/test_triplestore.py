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
