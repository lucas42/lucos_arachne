"""Tests for compact.py — TDB2 compaction triggering."""
import os
import sys
import types
from unittest.mock import MagicMock, patch

os.environ.setdefault("APP_ORIGIN", "https://arachne.example.com")
os.environ.setdefault("KEY_LUCOS_ARACHNE", "test-key")

_update_loganne_mock = MagicMock()
_update_schedule_tracker_mock = MagicMock()

for mod_name, attrs in [
    ("loganne", {"updateLoganne": _update_loganne_mock}),
    ("schedule_tracker", {"updateScheduleTracker": _update_schedule_tracker_mock}),
]:
    stub = types.ModuleType(mod_name)
    for attr, val in attrs.items():
        setattr(stub, attr, val)
    sys.modules[mod_name] = stub

_stub_mod_names = ["loganne", "schedule_tracker"]

import compact

for _mod_name in _stub_mod_names:
    sys.modules.pop(_mod_name, None)


def _reset_mocks():
    _update_loganne_mock.reset_mock(side_effect=True, return_value=True)
    _update_schedule_tracker_mock.reset_mock(side_effect=True, return_value=True)


def _ok_response():
    resp = MagicMock()
    resp.ok = True
    resp.raise_for_status = MagicMock()
    return resp


def _error_response():
    resp = MagicMock()
    resp.ok = False
    resp.raise_for_status.side_effect = Exception("503 Service Unavailable")
    return resp


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------

def test_compaction_posts_to_fuseki_admin_endpoint():
    """run_compaction POSTs to the correct Fuseki compact endpoint."""
    _reset_mocks()
    with patch("requests.post", return_value=_ok_response()) as mock_post:
        compact.run_compaction()
    url = mock_post.call_args.args[0]
    assert url == "http://triplestore:3030/$/compact/raw_arachne"


def test_compaction_sets_deleteold_param():
    """run_compaction passes deleteOld=true query parameter."""
    _reset_mocks()
    with patch("requests.post", return_value=_ok_response()) as mock_post:
        compact.run_compaction()
    params = mock_post.call_args.kwargs.get("params", {})
    assert params.get("deleteOld") == "true"


def test_compaction_uses_basic_auth():
    """run_compaction authenticates with basic auth credentials."""
    _reset_mocks()
    with patch("requests.post", return_value=_ok_response()) as mock_post:
        compact.run_compaction()
    auth = mock_post.call_args.kwargs.get("auth")
    assert auth is not None
    assert auth[0] == "lucos_arachne"


def test_compaction_reports_success_to_schedule_tracker():
    """run_compaction calls updateScheduleTracker with success=True on success."""
    _reset_mocks()
    with patch("requests.post", return_value=_ok_response()):
        compact.run_compaction()
    _update_schedule_tracker_mock.assert_called_once_with(
        success=True, system=compact.SYSTEM, frequency=compact.FREQUENCY_SECONDS
    )


def test_compaction_passes_weekly_frequency_to_schedule_tracker():
    """The compaction job is weekly; FREQUENCY_SECONDS must give a >7-day alert
    threshold (server-side multiplier × 3) so a missed Sunday run alerts within
    a sensible window. 3 days × 3 = 9 days is the minimum acceptable."""
    assert compact.FREQUENCY_SECONDS >= 3 * 24 * 60 * 60


def test_compaction_emits_loganne_event():
    """run_compaction emits a tripleStoreCompaction Loganne event on success."""
    _reset_mocks()
    with patch("requests.post", return_value=_ok_response()):
        compact.run_compaction()
    _update_loganne_mock.assert_called_once()
    call_kwargs = _update_loganne_mock.call_args.kwargs
    assert call_kwargs.get("type") == "tripleStoreCompaction"


# ---------------------------------------------------------------------------
# Failure handling
# ---------------------------------------------------------------------------

def test_compaction_raises_on_triplestore_error():
    """run_compaction propagates an exception when Fuseki returns an error."""
    _reset_mocks()
    with patch("requests.post", return_value=_error_response()):
        try:
            compact.run_compaction()
            assert False, "Expected exception not raised"
        except Exception as e:
            assert "503" in str(e)


def test_compaction_does_not_emit_loganne_on_failure():
    """Loganne is not called when compaction fails."""
    _reset_mocks()
    with patch("requests.post", return_value=_error_response()):
        try:
            compact.run_compaction()
        except Exception:
            pass
    _update_loganne_mock.assert_not_called()
