"""Tests for ingest.py — hash-skip behaviour, Phase 1 atomicity, any_changed guard, allow-list."""
import os
import sys
import types
from unittest.mock import MagicMock, patch, call

import pytest

os.environ.setdefault("APP_ORIGIN", "https://arachne.example.com")
os.environ.setdefault("KEY_LUCOS_ARACHNE", "test-key")
# Required by the real loganne client (imported in the real-transport test below)
os.environ.setdefault("SYSTEM", "lucos_arachne")
os.environ.setdefault("LOGANNE_ENDPOINT", "http://stub-loganne/events")

# ---------------------------------------------------------------------------
# Stub out all modules imported by ingest.py at module level
# ---------------------------------------------------------------------------

_GRAPH_URI = "https://eolas.l42.eu/metadata/all/data/"
_CONTENT = "<rdf> example </rdf>"
_CONTENT_TYPE = "application/rdf+xml"

_live_systems_stub = {"lucos_eolas": _GRAPH_URI}
_ontology_cache_stub = {}  # empty for most tests; overridden in specific ones
_ONTOLOGIES_DIR_stub = "/tmp"
_INFERRED_GRAPH_stub = "urn:lucos:inferred"
_METADATA_GRAPH_stub = "urn:lucos:ingestor-metadata"

_DIFF_FRAGMENT_STUB = (
    f"INSERT DATA {{ GRAPH <{_GRAPH_URI}> {{ <http://ex.com/s> <http://ex.com/p> <http://ex.com/o> . }} }}"
)

_fetch_url_mock = MagicMock(return_value=(_CONTENT, _CONTENT_TYPE))
_replace_graph_mock = MagicMock()
_diff_graph_mock = MagicMock(return_value=_DIFF_FRAGMENT_STUB)
_execute_sparql_update_mock = MagicMock()
_update_searchindex_mock = MagicMock(return_value=(set(), set()))
_update_person_docs_mock = MagicMock(return_value=set())
_cleanup_triplestore_mock = MagicMock()
_cleanup_searchindex_mock = MagicMock()
_compute_inferences_mock = MagicMock()
_get_source_hash_mock = MagicMock(return_value=None)
_set_source_hash_mock = MagicMock()
_update_loganne_mock = MagicMock()
_update_schedule_tracker_mock = MagicMock()

for mod_name, attrs in [
    ("authorised_fetch", {"fetch_url": _fetch_url_mock}),
    (
        "triplestore",
        {
            "live_systems": _live_systems_stub,
            "ontology_cache": _ontology_cache_stub,
            "ONTOLOGIES_DIR": _ONTOLOGIES_DIR_stub,
            "INFERRED_GRAPH": _INFERRED_GRAPH_stub,
            "METADATA_GRAPH": _METADATA_GRAPH_stub,
            "replace_graph_in_triplestore": _replace_graph_mock,
            "diff_graph_in_triplestore": _diff_graph_mock,
            "execute_sparql_update": _execute_sparql_update_mock,
            "cleanup_triplestore": _cleanup_triplestore_mock,
            "compute_inferences": _compute_inferences_mock,
            "get_source_hash": _get_source_hash_mock,
            "set_source_hash": _set_source_hash_mock,
            "session": MagicMock(),  # triplestore_session imported by ingest.py
        },
    ),
    (
        "searchindex",
        {
            "update_searchindex": _update_searchindex_mock,
            "cleanup_searchindex": _cleanup_searchindex_mock,
            "update_person_docs_in_searchindex": _update_person_docs_mock,
        },
    ),
    ("loganne", {"updateLoganne": _update_loganne_mock}),
    ("schedule_tracker", {"updateScheduleTracker": _update_schedule_tracker_mock}),
]:
    stub = types.ModuleType(mod_name)
    for attr, val in attrs.items():
        setattr(stub, attr, val)
    sys.modules[mod_name] = stub

_stub_mod_names = ["authorised_fetch", "triplestore", "searchindex", "loganne", "schedule_tracker"]

import ingest

for _mod_name in _stub_mod_names:
    sys.modules.pop(_mod_name, None)


def _reset_mocks():
    for m in [
        _fetch_url_mock, _replace_graph_mock, _diff_graph_mock,
        _execute_sparql_update_mock, _update_searchindex_mock, _update_person_docs_mock,
        _cleanup_triplestore_mock, _cleanup_searchindex_mock,
        _compute_inferences_mock, _get_source_hash_mock, _set_source_hash_mock,
        _update_loganne_mock, _update_schedule_tracker_mock,
    ]:
        m.reset_mock(side_effect=True, return_value=True)
    _fetch_url_mock.return_value = (_CONTENT, _CONTENT_TYPE)
    _update_searchindex_mock.return_value = (set(), set())
    _update_person_docs_mock.return_value = set()
    _get_source_hash_mock.return_value = None
    _diff_graph_mock.return_value = _DIFF_FRAGMENT_STUB


# ---------------------------------------------------------------------------
# Hash match — source is skipped
# ---------------------------------------------------------------------------

def _expected_hash(content, content_type):
    import hashlib
    return "sha256:" + hashlib.sha256((content + content_type).encode("utf-8")).hexdigest()


def test_hash_match_skips_diff_graph():
    """When stored hash matches, diff_graph_in_triplestore is not called."""
    _reset_mocks()
    _get_source_hash_mock.return_value = _expected_hash(_CONTENT, _CONTENT_TYPE)
    ingest.run_ingest()
    _diff_graph_mock.assert_not_called()


def test_hash_match_skips_execute_sparql_update():
    """When stored hash matches, execute_sparql_update is not called."""
    _reset_mocks()
    _get_source_hash_mock.return_value = _expected_hash(_CONTENT, _CONTENT_TYPE)
    ingest.run_ingest()
    _execute_sparql_update_mock.assert_not_called()


def test_hash_match_skips_update_searchindex():
    """When stored hash matches, update_searchindex is not called."""
    _reset_mocks()
    _get_source_hash_mock.return_value = _expected_hash(_CONTENT, _CONTENT_TYPE)
    ingest.run_ingest()
    _update_searchindex_mock.assert_not_called()


def test_hash_match_skips_set_source_hash():
    """When stored hash matches, set_source_hash is not called."""
    _reset_mocks()
    _get_source_hash_mock.return_value = _expected_hash(_CONTENT, _CONTENT_TYPE)
    ingest.run_ingest()
    _set_source_hash_mock.assert_not_called()


# ---------------------------------------------------------------------------
# Hash miss / no prior hash — source is ingested via diff path
# ---------------------------------------------------------------------------

def test_hash_miss_calls_diff_graph():
    """When stored hash differs, diff_graph_in_triplestore is called with the correct args."""
    _reset_mocks()
    _get_source_hash_mock.return_value = "sha256:old"
    ingest.run_ingest()
    _diff_graph_mock.assert_called_once_with(_GRAPH_URI, _CONTENT, _CONTENT_TYPE)


def test_no_prior_hash_calls_diff_graph():
    """When no hash is stored (None), diff_graph_in_triplestore is called."""
    _reset_mocks()
    _get_source_hash_mock.return_value = None
    ingest.run_ingest()
    _diff_graph_mock.assert_called_once()


def test_diff_fragment_passed_to_execute_sparql_update():
    """The fragment returned by diff_graph_in_triplestore is passed to execute_sparql_update."""
    _reset_mocks()
    _get_source_hash_mock.return_value = None
    ingest.run_ingest()
    _execute_sparql_update_mock.assert_called_once()
    sparql = _execute_sparql_update_mock.call_args.args[0]
    assert _DIFF_FRAGMENT_STUB in sparql


def test_hash_written_after_searchindex_update():
    """set_source_hash is called only after update_searchindex succeeds."""
    _reset_mocks()
    _get_source_hash_mock.return_value = None
    call_order = []
    _update_searchindex_mock.side_effect = lambda *a, **kw: (call_order.append("searchindex"), (set(), set()))[1]
    _set_source_hash_mock.side_effect = lambda *a, **kw: call_order.append("set_hash")
    ingest.run_ingest()
    assert call_order.index("searchindex") < call_order.index("set_hash")


def test_hash_not_written_when_searchindex_fails():
    """If update_searchindex raises, set_source_hash is not called."""
    _reset_mocks()
    _get_source_hash_mock.return_value = None
    _update_searchindex_mock.side_effect = Exception("search index down")
    ingest.run_ingest()
    _set_source_hash_mock.assert_not_called()


# ---------------------------------------------------------------------------
# Phase 1 atomicity — single execute_sparql_update call
# ---------------------------------------------------------------------------

def test_phase1_single_execute_call_for_one_source():
    """Phase 1 issues exactly one execute_sparql_update call even for one source."""
    _reset_mocks()
    _get_source_hash_mock.return_value = None
    ingest.run_ingest()
    _execute_sparql_update_mock.assert_called_once()


def test_phase1_no_execute_when_diff_returns_none():
    """When diff_graph_in_triplestore returns None, execute_sparql_update is not called."""
    _reset_mocks()
    _get_source_hash_mock.return_value = None
    _diff_graph_mock.return_value = None
    ingest.run_ingest()
    _execute_sparql_update_mock.assert_not_called()


def test_phase1_execute_before_searchindex():
    """execute_sparql_update is called before update_searchindex."""
    _reset_mocks()
    _get_source_hash_mock.return_value = None
    call_order = []
    _execute_sparql_update_mock.side_effect = lambda *a, **kw: call_order.append("phase1")
    _update_searchindex_mock.side_effect = lambda *a, **kw: (call_order.append("searchindex"), (set(), set()))[1]
    ingest.run_ingest()
    assert call_order.index("phase1") < call_order.index("searchindex")


def test_phase1_failure_prevents_hash_update():
    """If execute_sparql_update raises, set_source_hash is not called."""
    _reset_mocks()
    _get_source_hash_mock.return_value = None
    _execute_sparql_update_mock.side_effect = Exception("Fuseki error")
    ingest.run_ingest()
    _set_source_hash_mock.assert_not_called()


def test_phase1_failure_prevents_searchindex_update():
    """If execute_sparql_update raises, update_searchindex is not called."""
    _reset_mocks()
    _get_source_hash_mock.return_value = None
    _execute_sparql_update_mock.side_effect = Exception("Fuseki error")
    ingest.run_ingest()
    _update_searchindex_mock.assert_not_called()


# ---------------------------------------------------------------------------
# any_changed guard on compute_inferences
# ---------------------------------------------------------------------------

def test_all_unchanged_skips_inference():
    """compute_inferences is not called when all sources hashed identically."""
    _reset_mocks()
    _get_source_hash_mock.return_value = _expected_hash(_CONTENT, _CONTENT_TYPE)
    ingest.run_ingest()
    _compute_inferences_mock.assert_not_called()


def test_changed_source_triggers_inference():
    """compute_inferences is called when at least one source was re-ingested."""
    _reset_mocks()
    _get_source_hash_mock.return_value = None
    ingest.run_ingest()
    _compute_inferences_mock.assert_called_once()


def test_diff_none_but_hash_changed_skips_inference():
    """
    If diff returns None (RDF semantically unchanged despite different bytes),
    no triplestore writes occur and inference is not triggered.
    """
    _reset_mocks()
    _get_source_hash_mock.return_value = "sha256:old"
    _diff_graph_mock.return_value = None
    ingest.run_ingest()
    _compute_inferences_mock.assert_not_called()


def test_phase1_failure_skips_inference():
    """If Phase 1 fails, compute_inferences is not triggered."""
    _reset_mocks()
    _get_source_hash_mock.return_value = None
    _execute_sparql_update_mock.side_effect = Exception("Fuseki error")
    ingest.run_ingest()
    _compute_inferences_mock.assert_not_called()


# ---------------------------------------------------------------------------
# Person-merge step is called during bulk ingest
# ---------------------------------------------------------------------------

def test_person_merge_called_during_ingest():
    """update_person_docs_in_searchindex is called once per successful ingest run."""
    _reset_mocks()
    _get_source_hash_mock.return_value = None
    ingest.run_ingest()
    _update_person_docs_mock.assert_called_once()


def test_person_merge_still_called_when_no_sources_changed():
    """update_person_docs_in_searchindex is called even when all sources are hash-identical.
    Person topology can change via webhook events independently of bulk source data."""
    _reset_mocks()
    _get_source_hash_mock.return_value = _expected_hash(_CONTENT, _CONTENT_TYPE)
    ingest.run_ingest()
    _update_person_docs_mock.assert_called_once()


# ---------------------------------------------------------------------------
# cleanup_triplestore allow-list includes METADATA_GRAPH
# ---------------------------------------------------------------------------

def test_cleanup_allow_list_includes_metadata_graph():
    """cleanup_triplestore is called with METADATA_GRAPH in its allow-list."""
    _reset_mocks()
    _get_source_hash_mock.return_value = None
    ingest.run_ingest()
    allow_list = _cleanup_triplestore_mock.call_args.args[0]
    assert _METADATA_GRAPH_stub in allow_list


def test_cleanup_allow_list_includes_inferred_graph():
    """cleanup_triplestore allow-list also contains the inferred graph."""
    _reset_mocks()
    _get_source_hash_mock.return_value = None
    ingest.run_ingest()
    allow_list = _cleanup_triplestore_mock.call_args.args[0]
    assert _INFERRED_GRAPH_stub in allow_list


# ---------------------------------------------------------------------------
# Ontologies — still use replace_graph_in_triplestore (not diff path)
# ---------------------------------------------------------------------------

def test_ontology_uses_replace_graph_not_diff():
    """Ontologies use replace_graph_in_triplestore, not diff_graph_in_triplestore."""
    _reset_mocks()
    # Patch ontology_cache to have one entry
    ingest.ontology_cache = {
        "test_ont": ("http://example.com/ont", "test.ttl", "text/turtle")
    }
    ingest.live_systems = {}
    old_ONTOLOGIES_DIR = ingest.ONTOLOGIES_DIR
    import tempfile, os
    with tempfile.TemporaryDirectory() as tmpdir:
        ont_file = os.path.join(tmpdir, "test.ttl")
        with open(ont_file, "w") as f:
            f.write("@prefix ex: <http://example.com/> . ex:s ex:p ex:o .")
        ingest.ONTOLOGIES_DIR = tmpdir
        _get_source_hash_mock.return_value = None
        ingest.run_ingest()
    _replace_graph_mock.assert_called_once()
    _diff_graph_mock.assert_not_called()
    # Restore
    ingest.ontology_cache = _ontology_cache_stub
    ingest.live_systems = _live_systems_stub
    ingest.ONTOLOGIES_DIR = old_ONTOLOGIES_DIR


# ---------------------------------------------------------------------------
# Loganne message — differentiated by any_changed / has_failures
# ---------------------------------------------------------------------------

def test_loganne_message_no_changes():
    """When nothing changed, loganne reports a no-op check at routine level."""
    _reset_mocks()
    _get_source_hash_mock.return_value = _expected_hash(_CONTENT, _CONTENT_TYPE)
    ingest.run_ingest()
    _update_loganne_mock.assert_called_once()
    kwargs = _update_loganne_mock.call_args.kwargs
    assert kwargs["humanReadable"] == "Knowledge graph checked — no changes"
    assert kwargs["level"] == "routine"


def test_loganne_message_updated():
    """When sources changed without failures, loganne reports an update at routine level."""
    _reset_mocks()
    _get_source_hash_mock.return_value = None
    ingest.run_ingest()
    _update_loganne_mock.assert_called_once()
    kwargs = _update_loganne_mock.call_args.kwargs
    assert kwargs["humanReadable"] == "Knowledge graph updated"
    assert kwargs["level"] == "routine"


def test_loganne_message_failed_no_changes():
    """When fetch fails and nothing changed, loganne reports a total failure at notable level."""
    _reset_mocks()
    _fetch_url_mock.side_effect = Exception("network error")
    ingest.run_ingest()
    _update_loganne_mock.assert_called_once()
    kwargs = _update_loganne_mock.call_args.kwargs
    assert kwargs["humanReadable"] == "Knowledge graph ingest failed — no updates applied"
    assert kwargs["level"] == "notable"


def test_loganne_message_partial_failure():
    """When some sources changed but post-ingest fails, loganne reports a partial update at notable level."""
    _reset_mocks()
    _get_source_hash_mock.return_value = None
    _update_searchindex_mock.side_effect = Exception("search index down")
    ingest.run_ingest()
    _update_loganne_mock.assert_called_once()
    kwargs = _update_loganne_mock.call_args.kwargs
    assert kwargs["humanReadable"] == "Knowledge graph partially updated — some sources failed"
    assert kwargs["level"] == "notable"


# ---------------------------------------------------------------------------
# Schedule tracker v2 call shape
# ---------------------------------------------------------------------------

def test_schedule_tracker_uses_lucos_arachne_system_on_hash_skip():
    """When a source is hash-skipped, updateScheduleTracker uses system='lucos_arachne'
    and job_name equal to the source name."""
    _reset_mocks()
    _get_source_hash_mock.return_value = _expected_hash(_CONTENT, _CONTENT_TYPE)
    ingest.run_ingest()
    # find the call for the skipped live source
    calls = _update_schedule_tracker_mock.call_args_list
    assert any(
        c.kwargs.get("system") == "lucos_arachne" and c.kwargs.get("job_name") == "lucos_eolas"
        for c in calls
    ), f"Expected system='lucos_arachne', job_name='lucos_eolas' in calls: {calls}"


def test_schedule_tracker_aggregate_ingestor_uses_v2():
    """The end-of-run aggregate call uses system='lucos_arachne', job_name='ingestor'."""
    _reset_mocks()
    _get_source_hash_mock.return_value = _expected_hash(_CONTENT, _CONTENT_TYPE)
    ingest.run_ingest()
    calls = _update_schedule_tracker_mock.call_args_list
    assert any(
        c.kwargs.get("system") == "lucos_arachne" and c.kwargs.get("job_name") == "ingestor"
        for c in calls
    ), f"Expected aggregate ingestor call with system='lucos_arachne', job_name='ingestor' in: {calls}"


def test_schedule_tracker_inference_uses_v2():
    """The inference job uses system='lucos_arachne', job_name='inference'."""
    _reset_mocks()
    _get_source_hash_mock.return_value = _expected_hash(_CONTENT, _CONTENT_TYPE)
    ingest.run_ingest()
    calls = _update_schedule_tracker_mock.call_args_list
    assert any(
        c.kwargs.get("system") == "lucos_arachne" and c.kwargs.get("job_name") == "inference"
        for c in calls
    ), f"Expected inference call with system='lucos_arachne', job_name='inference' in: {calls}"


def test_schedule_tracker_no_synthetic_system_ids():
    """No updateScheduleTracker call should use a synthetic system ID (e.g. 'lucos_arachne_ingestor_*')."""
    _reset_mocks()
    _get_source_hash_mock.return_value = None
    ingest.run_ingest()
    calls = _update_schedule_tracker_mock.call_args_list
    for c in calls:
        system = c.kwargs.get("system", "")
        assert not system.startswith("lucos_arachne_"), \
            f"Found synthetic system ID in schedule_tracker call: {system!r}"


# ---------------------------------------------------------------------------
# Real-transport loganne test (ADR-0011)
# ---------------------------------------------------------------------------

def test_ingest_loganne_real_transport():
    """Drive the real loganne v2 client against a patched HTTP session.

    ingest.py was imported with a stub loganne — this test temporarily
    replaces ingest.updateLoganne with the real function, then patches
    loganne.session.post to capture the HTTP payload without making a real
    network call.

    Because the v2 client validates `level` before any network call (raising
    ValueError for unknown values), this test would fail if `level` were
    dropped from ingest.py or set to an invalid value.
    """
    import loganne as _real_loganne

    captured = []

    def _fake_loganne_post(url, **kwargs):
        captured.append({"url": url, "json": kwargs.get("json", {})})
        resp = MagicMock()
        resp.raise_for_status = lambda: None
        return resp

    _reset_mocks()
    # Hash-match scenario: nothing changed, simplest code path
    _get_source_hash_mock.return_value = _expected_hash(_CONTENT, _CONTENT_TYPE)

    with patch.object(ingest, "updateLoganne", _real_loganne.updateLoganne), \
         patch.object(_real_loganne.session, "post", side_effect=_fake_loganne_post):
        ingest.run_ingest()

    ingest_calls = [c for c in captured if c["json"].get("type") == "knowledgeIngest"]
    assert len(ingest_calls) >= 1, "Expected at least one knowledgeIngest loganne POST"

    payload = ingest_calls[0]["json"]
    assert payload.get("level") == "routine", (
        f"Expected level='routine' in HTTP payload, got: {payload}"
    )
    assert payload.get("humanReadable") == "Knowledge graph checked — no changes"
    assert payload.get("type") == "knowledgeIngest"
    assert "source" in payload
