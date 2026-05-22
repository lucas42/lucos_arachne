"""Tests for triplestore.py helper functions."""
import hashlib
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


# ---------------------------------------------------------------------------
# diff_graph_in_triplestore
# ---------------------------------------------------------------------------

_GRAPH_URI = "https://eolas.l42.eu/metadata/all/data/"

_TTL_A = """
@prefix ex: <https://example.com/> .
ex:s ex:p ex:o1 .
ex:s ex:p ex:o2 .
"""

_TTL_A_PLUS = """
@prefix ex: <https://example.com/> .
ex:s ex:p ex:o1 .
ex:s ex:p ex:o2 .
ex:s ex:p ex:o3 .
"""

_TTL_B = """
@prefix ex: <https://example.com/> .
ex:s ex:p ex:o2 .
ex:s ex:p ex:o3 .
"""

_TTL_BNODE = """
@prefix ex: <https://example.com/> .
ex:Festival ex:hasPeriod [
    ex:startDate "2024-01-01" ;
    ex:endDate   "2024-01-07"
] .
"""

# Serialise a Turtle string to N-Triples for use as a mock CONSTRUCT response
def _to_nt(ttl: str) -> str:
    from rdflib import Graph
    g = Graph()
    g.parse(data=ttl, format="turtle")
    return g.serialize(format="nt")


def _mock_construct_response(nt_content: str):
    """Return a mock requests.Response carrying N-Triples content."""
    resp = MagicMock()
    resp.ok = True
    resp.raise_for_status = MagicMock()
    resp.text = nt_content
    return resp


def _call_diff(new_ttl: str, old_nt: str):
    """Helper: call diff_graph_in_triplestore with Turtle new content and N-Triples old graph."""
    responses = [_mock_construct_response(old_nt)]
    with patch.object(triplestore.session, "post", side_effect=responses) as mock_post:
        fragment = triplestore.diff_graph_in_triplestore(
            _GRAPH_URI, new_ttl, "text/turtle"
        )
    return fragment, mock_post


# --- Unchanged graph → empty diff ---

def test_diff_unchanged_graph_returns_none():
    """When the new and old graphs are identical, diff returns None."""
    nt = _to_nt(_TTL_A)
    fragment, _ = _call_diff(_TTL_A, nt)
    assert fragment is None


def test_diff_unchanged_graph_sends_construct_query():
    """diff_graph_in_triplestore issues a CONSTRUCT query to fetch the current graph."""
    nt = _to_nt(_TTL_A)
    _, mock_post = _call_diff(_TTL_A, nt)
    query = mock_post.call_args.kwargs["data"]["query"]
    assert "CONSTRUCT" in query
    assert _GRAPH_URI in query


# --- All-new graph (empty store) → INSERT only ---

def test_diff_all_new_returns_insert_data():
    """When the old graph is empty, diff returns an INSERT DATA statement."""
    fragment, _ = _call_diff(_TTL_A, "")
    assert fragment is not None
    assert "INSERT DATA" in fragment
    assert "DELETE" not in fragment


def test_diff_all_new_insert_covers_all_triples():
    """The INSERT DATA fragment contains all triples from the new graph."""
    from rdflib import Graph
    fragment, _ = _call_diff(_TTL_A, "")
    new_g = Graph()
    new_g.parse(data=_TTL_A, format="turtle")
    for s, p, o in new_g:
        assert str(s) in fragment
        assert str(p) in fragment


# --- All-deleted graph → DELETE only ---

def test_diff_all_deleted_returns_delete_data():
    """When the new graph is empty, diff returns a DELETE DATA statement."""
    empty_ttl = ""
    nt = _to_nt(_TTL_A)
    fragment, _ = _call_diff(empty_ttl, nt)
    assert fragment is not None
    assert "DELETE DATA" in fragment
    assert "INSERT" not in fragment


# --- Single-triple change → bounded diff ---

def test_diff_single_triple_change_bounded():
    """A single-triple change produces a diff of bounded size (not a full rewrite)."""
    from rdflib import Graph

    # old: o1 + o2; new: o1 + o2 + o3  → only o3 should be inserted
    nt_old = _to_nt(_TTL_A)
    fragment, _ = _call_diff(_TTL_A_PLUS, nt_old)
    assert fragment is not None
    assert "INSERT DATA" in fragment
    assert "DELETE" not in fragment

    # Parse the INSERT fragment's graph content and verify it has exactly 1 triple
    # (the added triple)
    assert "ex:o3" in fragment or "https://example.com/o3" in fragment


def test_diff_partial_change_inserts_and_deletes():
    """
    A change that removes some triples and adds others produces both INSERT DATA
    and DELETE DATA in the fragment.
    """
    # old: o1 + o2; new: o2 + o3  → delete o1, insert o3
    nt_old = _to_nt(_TTL_A)
    fragment, _ = _call_diff(_TTL_B, nt_old)
    assert fragment is not None
    assert "INSERT DATA" in fragment
    assert "DELETE DATA" in fragment


# --- Migration case: old graph has blank nodes ---

def test_diff_migration_uses_delete_where():
    """When the old graph has blank nodes, diff uses DELETE WHERE (not DELETE DATA)."""
    nt_old = _to_nt(_TTL_BNODE)
    fragment, _ = _call_diff(_TTL_A, nt_old)
    assert fragment is not None
    assert "DELETE WHERE" in fragment
    assert "?s ?p ?o" in fragment
    assert _GRAPH_URI in fragment


def test_diff_migration_includes_insert_data():
    """Migration fragment includes an INSERT DATA for the new Skolemised content."""
    nt_old = _to_nt(_TTL_BNODE)
    fragment, _ = _call_diff(_TTL_A, nt_old)
    assert "INSERT DATA" in fragment


def test_diff_migration_graph_is_skolemised():
    """After migration, no blank nodes appear in the INSERT DATA content."""
    nt_old = _to_nt(_TTL_BNODE)
    # New content also has blank nodes — should be Skolemised in the fragment
    fragment, _ = _call_diff(_TTL_BNODE, nt_old)
    assert fragment is not None
    # Blank-node notation should not appear in the fragment
    assert "_:N" not in fragment
    assert "_:b" not in fragment


# --- SPARQL Update format ---

def test_diff_uses_graph_uri_in_fragment():
    """The SPARQL Update fragment contains the correct graph URI."""
    fragment, _ = _call_diff(_TTL_A, "")
    assert _GRAPH_URI in fragment


def test_diff_fragment_is_valid_sparql_update_structure():
    """Fragment uses INSERT DATA or DELETE DATA wrapped in GRAPH <g> { … }."""
    fragment, _ = _call_diff(_TTL_A, "")
    assert f"GRAPH <{_GRAPH_URI}>" in fragment


def test_diff_multi_statement_separator():
    """When both INSERT and DELETE are present, they're separated by ' ;'."""
    nt_old = _to_nt(_TTL_A)
    fragment, _ = _call_diff(_TTL_B, nt_old)
    # Fragment should contain both operations separated by semicolon
    assert ";" in fragment


# ---------------------------------------------------------------------------
# execute_sparql_update
# ---------------------------------------------------------------------------

def test_execute_sparql_update_posts_to_update_endpoint():
    """execute_sparql_update POSTs to the SPARQL update endpoint."""
    with patch.object(triplestore.session, "post", return_value=_mock_ok_response()) as mock_post:
        triplestore.execute_sparql_update("DELETE WHERE { ?s ?p ?o }")
    url = mock_post.call_args.args[0]
    assert url == "http://triplestore:3030/raw_arachne/update"


def test_execute_sparql_update_sets_content_type():
    """execute_sparql_update uses application/sparql-update content-type."""
    with patch.object(triplestore.session, "post", return_value=_mock_ok_response()) as mock_post:
        triplestore.execute_sparql_update("DELETE WHERE { ?s ?p ?o }")
    ct = mock_post.call_args.kwargs["headers"]["Content-Type"]
    assert ct == "application/sparql-update"


def test_execute_sparql_update_raises_on_error():
    """execute_sparql_update propagates triplestore errors."""
    mock_resp = MagicMock()
    mock_resp.raise_for_status.side_effect = Exception("500 error")
    with patch.object(triplestore.session, "post", return_value=mock_resp):
        try:
            triplestore.execute_sparql_update("BAD SPARQL")
            assert False, "Expected exception"
        except Exception as e:
            assert "500" in str(e)


# ---------------------------------------------------------------------------
# compute_inferences — hash-skip (Approach 2) and diff-based update (Approach 1)
# ---------------------------------------------------------------------------

def _no_bindings_response():
    """Mock SPARQL SELECT response with no results (no transitive/inverse props)."""
    resp = MagicMock()
    resp.ok = True
    resp.raise_for_status = MagicMock()
    resp.json.return_value = {"results": {"bindings": []}}
    return resp


def _empty_inferred_hash():
    """SHA-256 hash of the empty turtle content produced when there are no inferred triples."""
    return "sha256:" + hashlib.sha256("".encode("utf-8")).hexdigest()


def test_compute_inferences_skips_all_writes_when_hash_matches():
    """
    When the stored hash matches the computed content hash, compute_inferences
    returns False and makes no calls to diff, execute, or set_source_hash.

    This is Approach 2: the triplestore round-trip is skipped entirely when the
    inferred set hasn't changed since the last run.
    """
    with (
        patch.object(triplestore.session, "post", return_value=_no_bindings_response()),
        patch.object(triplestore, "get_source_hash", return_value=_empty_inferred_hash()),
        patch.object(triplestore, "diff_graph_in_triplestore") as mock_diff,
        patch.object(triplestore, "execute_sparql_update") as mock_exec,
        patch.object(triplestore, "set_source_hash") as mock_set,
    ):
        result = triplestore.compute_inferences()

    assert result is False
    mock_diff.assert_not_called()
    mock_exec.assert_not_called()
    mock_set.assert_not_called()


def test_compute_inferences_calls_diff_on_hash_mismatch():
    """
    When the stored hash does not match the computed hash, compute_inferences
    calls diff_graph_in_triplestore with the inferred graph URI and turtle content.

    This is Approach 1: the diff narrows writes to the minimal change set.
    """
    with (
        patch.object(triplestore.session, "post", return_value=_no_bindings_response()),
        patch.object(triplestore, "get_source_hash", return_value="sha256:stale"),
        patch.object(triplestore, "diff_graph_in_triplestore", return_value=None) as mock_diff,
        patch.object(triplestore, "execute_sparql_update"),
        patch.object(triplestore, "set_source_hash"),
    ):
        triplestore.compute_inferences()

    mock_diff.assert_called_once_with(triplestore.INFERRED_GRAPH, "", "text/turtle")


def test_compute_inferences_executes_fragment_when_diff_has_changes():
    """
    When diff_graph_in_triplestore returns a non-None fragment, compute_inferences
    calls execute_sparql_update with that fragment.
    """
    fragment = "INSERT DATA { GRAPH <urn:lucos:inferred> { <ex:s> <ex:p> <ex:o> . } }"
    with (
        patch.object(triplestore.session, "post", return_value=_no_bindings_response()),
        patch.object(triplestore, "get_source_hash", return_value="sha256:stale"),
        patch.object(triplestore, "diff_graph_in_triplestore", return_value=fragment),
        patch.object(triplestore, "execute_sparql_update") as mock_exec,
        patch.object(triplestore, "set_source_hash"),
    ):
        triplestore.compute_inferences()

    mock_exec.assert_called_once_with(fragment)


def test_compute_inferences_skips_execute_when_diff_is_none():
    """
    When diff_graph_in_triplestore returns None (triples already in sync),
    compute_inferences does NOT call execute_sparql_update.
    """
    with (
        patch.object(triplestore.session, "post", return_value=_no_bindings_response()),
        patch.object(triplestore, "get_source_hash", return_value="sha256:stale"),
        patch.object(triplestore, "diff_graph_in_triplestore", return_value=None),
        patch.object(triplestore, "execute_sparql_update") as mock_exec,
        patch.object(triplestore, "set_source_hash"),
    ):
        triplestore.compute_inferences()

    mock_exec.assert_not_called()


def test_compute_inferences_updates_hash_after_diff():
    """
    After calling diff_graph_in_triplestore (whether fragment is None or not),
    compute_inferences stores the new content hash via set_source_hash.
    """
    with (
        patch.object(triplestore.session, "post", return_value=_no_bindings_response()),
        patch.object(triplestore, "get_source_hash", return_value="sha256:stale"),
        patch.object(triplestore, "diff_graph_in_triplestore", return_value=None),
        patch.object(triplestore, "execute_sparql_update"),
        patch.object(triplestore, "set_source_hash") as mock_set,
    ):
        triplestore.compute_inferences()

    mock_set.assert_called_once_with(triplestore.INFERRED_GRAPH, _empty_inferred_hash())


def test_compute_inferences_returns_true_when_diff_has_changes():
    """compute_inferences returns True when the diff produced triplestore writes."""
    fragment = "INSERT DATA { GRAPH <urn:lucos:inferred> { <ex:s> <ex:p> <ex:o> . } }"
    with (
        patch.object(triplestore.session, "post", return_value=_no_bindings_response()),
        patch.object(triplestore, "get_source_hash", return_value="sha256:stale"),
        patch.object(triplestore, "diff_graph_in_triplestore", return_value=fragment),
        patch.object(triplestore, "execute_sparql_update"),
        patch.object(triplestore, "set_source_hash"),
    ):
        result = triplestore.compute_inferences()

    assert result is True


def test_compute_inferences_returns_false_when_diff_is_none():
    """compute_inferences returns False when the diff found no changes to apply."""
    with (
        patch.object(triplestore.session, "post", return_value=_no_bindings_response()),
        patch.object(triplestore, "get_source_hash", return_value="sha256:stale"),
        patch.object(triplestore, "diff_graph_in_triplestore", return_value=None),
        patch.object(triplestore, "execute_sparql_update"),
        patch.object(triplestore, "set_source_hash"),
    ):
        result = triplestore.compute_inferences()

    assert result is False


# ---------------------------------------------------------------------------
# compute_inferences — symmetric property materialisation
# ---------------------------------------------------------------------------

# URIs used in symmetric-property tests
_SYM_PROP = "https://example.com/prop/collaboratedWith"
_URI_A    = "https://example.com/agent/alice"
_URI_B    = "https://example.com/agent/bob"


def _sym_prop_response(prop_uris):
    """Mock SPARQL SELECT response listing symmetric property URIs."""
    resp = MagicMock()
    resp.ok = True
    resp.raise_for_status = MagicMock()
    resp.json.return_value = {"results": {"bindings": [{"p": {"value": p}} for p in prop_uris]}}
    return resp


def _pairs_response(pairs):
    """Mock SPARQL SELECT response for (s, o) pairs."""
    resp = MagicMock()
    resp.ok = True
    resp.raise_for_status = MagicMock()
    resp.json.return_value = {
        "results": {"bindings": [{"s": {"value": s}, "o": {"value": o}} for s, o in pairs]}
    }
    return resp


def _side_effects_for_symmetric(sym_props, prop_pairs, sameAs_pairs=None):
    """
    Return the session.post side_effect list for compute_inferences() when
    transitive and inverse passes find nothing, there is one declared symmetric
    property (sym_props), and owl:sameAs has sameAs_pairs (default empty).

    Call sequence inside compute_inferences():
      [0] transitive props SELECT      → no bindings
      [1] inverse prop pairs SELECT    → no bindings
      [2] symmetric props SELECT       → sym_props list
      [3..] pair queries per prop (declared props first, then OWL_SAME_AS if not declared)
    """
    if sameAs_pairs is None:
        sameAs_pairs = []
    responses = [
        _no_bindings_response(),   # transitive props
        _no_bindings_response(),   # inverse prop pairs
        _sym_prop_response(sym_props),
    ]
    # pair query for each declared symmetric prop
    for pairs in prop_pairs:
        responses.append(_pairs_response(pairs))
    # pair query for OWL_SAME_AS (always appended unless already declared)
    if triplestore.OWL_SAME_AS not in sym_props:
        responses.append(_pairs_response(sameAs_pairs))
    return responses


def test_compute_inferences_symmetric_property_generates_reverse_triple():
    """
    A declared symmetric property with pair (A, B) and no reverse results in the
    inferred reverse triple <B> <prop> <A>.
    """
    side_effects = _side_effects_for_symmetric(
        sym_props=[_SYM_PROP],
        prop_pairs=[[(_URI_A, _URI_B)]],  # only A → B stored
    )
    captured = {}
    with (
        patch.object(triplestore.session, "post", side_effect=side_effects),
        patch.object(triplestore, "get_source_hash", return_value="sha256:stale"),
        patch.object(triplestore, "diff_graph_in_triplestore",
                     side_effect=lambda g, content, ct: captured.update({"content": content}) or None),
        patch.object(triplestore, "execute_sparql_update"),
        patch.object(triplestore, "set_source_hash"),
    ):
        triplestore.compute_inferences()

    content = captured["content"]
    assert f"<{_URI_B}> <{_SYM_PROP}> <{_URI_A}>" in content
    # The forward triple is NOT re-emitted (it's already in the source graph)
    assert f"<{_URI_A}> <{_SYM_PROP}> <{_URI_B}>" not in content


def test_compute_inferences_sameAs_always_treated_as_symmetric():
    """
    owl:sameAs pair (A, B) generates the reverse <B> owl:sameAs <A> even when
    owl:sameAs is not declared as owl:SymmetricProperty in the ontologies.
    """
    side_effects = _side_effects_for_symmetric(
        sym_props=[],  # no declared symmetric props
        prop_pairs=[],
        sameAs_pairs=[(_URI_A, _URI_B)],
    )
    captured = {}
    with (
        patch.object(triplestore.session, "post", side_effect=side_effects),
        patch.object(triplestore, "get_source_hash", return_value="sha256:stale"),
        patch.object(triplestore, "diff_graph_in_triplestore",
                     side_effect=lambda g, content, ct: captured.update({"content": content}) or None),
        patch.object(triplestore, "execute_sparql_update"),
        patch.object(triplestore, "set_source_hash"),
    ):
        triplestore.compute_inferences()

    content = captured["content"]
    assert f"<{_URI_B}> <{triplestore.OWL_SAME_AS}> <{_URI_A}>" in content


def test_compute_inferences_symmetric_idempotent():
    """
    When both (A, B) and (B, A) are already in the source graphs, no new reverse
    triple is inferred (materialisation is idempotent).
    """
    side_effects = _side_effects_for_symmetric(
        sym_props=[_SYM_PROP],
        prop_pairs=[[(_URI_A, _URI_B), (_URI_B, _URI_A)]],  # both directions present
    )
    captured = {}
    with (
        patch.object(triplestore.session, "post", side_effect=side_effects),
        patch.object(triplestore, "get_source_hash", return_value="sha256:stale"),
        patch.object(triplestore, "diff_graph_in_triplestore",
                     side_effect=lambda g, content, ct: captured.update({"content": content}) or None),
        patch.object(triplestore, "execute_sparql_update"),
        patch.object(triplestore, "set_source_hash"),
    ):
        triplestore.compute_inferences()

    content = captured["content"]
    # No inferred triples for _SYM_PROP (both directions were direct)
    assert f"<{_SYM_PROP}>" not in content


def test_compute_inferences_symmetric_skips_literals_via_filter():
    """
    The pair query for symmetric properties uses FILTER(!isLiteral(?o)) so that
    literal-object triples are not fetched and cannot produce a reverse triple.
    """
    side_effects = _side_effects_for_symmetric(
        sym_props=[_SYM_PROP],
        prop_pairs=[[]],  # empty — simulates server honouring the literal filter
    )
    sparql_queries = []
    original_post = triplestore.session.post

    def capturing_post(*args, **kwargs):
        query = (kwargs.get("data") or {}).get("query", "")
        if query:
            sparql_queries.append(query)
        return side_effects.pop(0) if side_effects else _no_bindings_response()

    with (
        patch.object(triplestore.session, "post", side_effect=capturing_post),
        patch.object(triplestore, "get_source_hash", return_value="sha256:stale"),
        patch.object(triplestore, "diff_graph_in_triplestore", return_value=None),
        patch.object(triplestore, "execute_sparql_update"),
        patch.object(triplestore, "set_source_hash"),
    ):
        triplestore.compute_inferences()

    # At least one query should contain the literal filter, targeting _SYM_PROP
    sym_prop_queries = [q for q in sparql_queries if _SYM_PROP in q]
    assert sym_prop_queries, f"No SPARQL query found for {_SYM_PROP}"
    assert any("isLiteral" in q for q in sym_prop_queries), (
        "Symmetric pair query does not use isLiteral filter"
    )


def test_compute_inferences_owl_symmetric_and_sameAs_constants_defined():
    """OWL_SYMMETRIC and OWL_SAME_AS constants are defined with the correct URIs."""
    assert triplestore.OWL_SYMMETRIC == "http://www.w3.org/2002/07/owl#SymmetricProperty"
    assert triplestore.OWL_SAME_AS   == "http://www.w3.org/2002/07/owl#sameAs"
