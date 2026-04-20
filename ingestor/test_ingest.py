"""Tests for ingest.py — hash-skip behaviour, any_changed guard, allow-list."""
import os
import sys
import types
from unittest.mock import MagicMock, patch, call

import pytest

os.environ.setdefault("APP_ORIGIN", "https://arachne.example.com")
os.environ.setdefault("KEY_LUCOS_ARACHNE", "test-key")

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

_fetch_url_mock = MagicMock(return_value=(_CONTENT, _CONTENT_TYPE))
_replace_graph_mock = MagicMock()
_update_searchindex_mock = MagicMock(return_value=(set(), set()))
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
            "cleanup_triplestore": _cleanup_triplestore_mock,
            "compute_inferences": _compute_inferences_mock,
            "get_source_hash": _get_source_hash_mock,
            "set_source_hash": _set_source_hash_mock,
        },
    ),
    (
        "searchindex",
        {
            "update_searchindex": _update_searchindex_mock,
            "cleanup_searchindex": _cleanup_searchindex_mock,
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
        _fetch_url_mock, _replace_graph_mock, _update_searchindex_mock,
        _cleanup_triplestore_mock, _cleanup_searchindex_mock,
        _compute_inferences_mock, _get_source_hash_mock, _set_source_hash_mock,
        _update_loganne_mock, _update_schedule_tracker_mock,
    ]:
        m.reset_mock(side_effect=True, return_value=True)
    _fetch_url_mock.return_value = (_CONTENT, _CONTENT_TYPE)
    _update_searchindex_mock.return_value = (set(), set())
    _get_source_hash_mock.return_value = None


# ---------------------------------------------------------------------------
# Hash match — source is skipped
# ---------------------------------------------------------------------------

def _expected_hash(content, content_type):
    import hashlib
    return "sha256:" + hashlib.sha256((content + content_type).encode("utf-8")).hexdigest()


def test_hash_match_skips_replace_graph():
    """When stored hash matches, replace_graph_in_triplestore is not called."""
    _reset_mocks()
    _get_source_hash_mock.return_value = _expected_hash(_CONTENT, _CONTENT_TYPE)
    ingest.run_ingest()
    _replace_graph_mock.assert_not_called()


def test_hash_match_skips_update_searchindex():
    """When stored hash matches, update_searchindex is not called."""
    _reset_mocks()
    _get_source_hash_mock.return_value = _expected_hash(_CONTENT, _CONTENT_TYPE)
    ingest.run_ingest()
    _update_searchindex_mock.assert_not_called()


def test_hash_match_skips_set_source_hash():
    """When stored hash matches, set_source_hash is not called (nothing to update)."""
    _reset_mocks()
    _get_source_hash_mock.return_value = _expected_hash(_CONTENT, _CONTENT_TYPE)
    ingest.run_ingest()
    _set_source_hash_mock.assert_not_called()


# ---------------------------------------------------------------------------
# Hash miss / no prior hash — source is ingested
# ---------------------------------------------------------------------------

def test_hash_miss_calls_replace_graph():
    """When stored hash differs, replace_graph_in_triplestore is called."""
    _reset_mocks()
    _get_source_hash_mock.return_value = "sha256:old"
    ingest.run_ingest()
    _replace_graph_mock.assert_called_once_with(_GRAPH_URI, _CONTENT, _CONTENT_TYPE)


def test_no_prior_hash_calls_replace_graph():
    """When no hash is stored (None), replace_graph_in_triplestore is called."""
    _reset_mocks()
    _get_source_hash_mock.return_value = None
    ingest.run_ingest()
    _replace_graph_mock.assert_called_once()


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
