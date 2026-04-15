"""Tests for the webhookController — *Created, *Deleted, and *Merged event handling."""
import io
import json
import os
import sys
import types
from http.server import BaseHTTPRequestHandler
from unittest.mock import MagicMock, patch

import pytest

# Ensure server.py is not cached from a previous test file's stub imports —
# pop it so it's freshly imported with our mocks bound to its module globals.
sys.modules.pop("server", None)

# Stub out non-stdlib modules before importing server
_fetch_url_mock = MagicMock()
_replace_item_mock = MagicMock()
_delete_item_mock = MagicMock()
_merge_items_mock = MagicMock()
_update_searchindex_mock = MagicMock()
_delete_doc_mock = MagicMock()

_live_systems = {
    "lucos_eolas": "https://eolas.l42.eu/metadata/all/data/",
    "lucos_contacts": "https://contacts.l42.eu/people/all",
}

for mod_name, attrs in [
    ("authorised_fetch", {"fetch_url": _fetch_url_mock}),
    (
        "triplestore",
        {
            "live_systems": _live_systems,
            "replace_item_in_triplestore": _replace_item_mock,
            "delete_item_in_triplestore": _delete_item_mock,
            "merge_items_in_triplestore": _merge_items_mock,
        },
    ),
    (
        "searchindex",
        {
            "update_searchindex": _update_searchindex_mock,
            "delete_doc_in_searchindex": _delete_doc_mock,
        },
    ),
]:
    stub = types.ModuleType(mod_name)
    for attr, val in attrs.items():
        setattr(stub, attr, val)
    sys.modules[mod_name] = stub

os.environ.setdefault("PORT", "8080")

_stub_mod_names = ["authorised_fetch", "triplestore", "searchindex"]
from server import WebhookHandler

for _mod_name in _stub_mod_names:
    sys.modules.pop(_mod_name, None)


def _make_request(body: dict, path: str = "/webhook", auth: str | None = None):
    """
    Invoke WebhookHandler.webhookController() directly and return (status_code, response_body).
    Bypasses the actual HTTP server and socket layer.
    """
    raw = json.dumps(body).encode("utf-8")
    handler = WebhookHandler.__new__(WebhookHandler)
    handler.path = path
    handler.post_data = raw
    handler.headers = {"Content-Length": str(len(raw))}
    if auth is not None:
        handler.headers["Authorization"] = auth

    status_holder = []
    out = io.BytesIO()

    def fake_send_response(code, message=None):
        status_holder.append(code)

    def fake_send_header(key, val):
        pass

    def fake_end_headers():
        pass

    def fake_send_error(code, message=None, explain=None):
        status_holder.append(code)

    handler.send_response = fake_send_response
    handler.send_header = fake_send_header
    handler.end_headers = fake_end_headers
    handler.send_error = fake_send_error
    handler.wfile = out

    # Clear mocks before each call
    _fetch_url_mock.reset_mock()
    _replace_item_mock.reset_mock()
    _delete_item_mock.reset_mock()
    _merge_items_mock.reset_mock()
    _update_searchindex_mock.reset_mock()
    _delete_doc_mock.reset_mock()

    handler.webhookController()

    status = status_holder[0] if status_holder else None
    return status, out.getvalue().decode("utf-8")


# ---------------------------------------------------------------------------
# *Created handler
# ---------------------------------------------------------------------------


def test_created_event_fetches_and_replaces():
    _fetch_url_mock.return_value = ("<rdf/>", "application/rdf+xml")
    status, body = _make_request({
        "type": "albumCreated",
        "source": "lucos_eolas",
        "url": "https://eolas.l42.eu/metadata/1",
    })
    assert status == 200
    assert body == "Updated"
    _fetch_url_mock.assert_called_once_with("lucos_eolas", "https://eolas.l42.eu/metadata/1")
    _replace_item_mock.assert_called_once()
    _update_searchindex_mock.assert_called_once()


# ---------------------------------------------------------------------------
# *Deleted handler
# ---------------------------------------------------------------------------


def test_deleted_event_removes_from_triplestore():
    status, body = _make_request({
        "type": "albumDeleted",
        "source": "lucos_eolas",
        "url": "https://eolas.l42.eu/metadata/1",
    })
    assert status == 200
    assert body == "Deleted"
    _delete_item_mock.assert_called_once_with(
        "https://eolas.l42.eu/metadata/1",
        _live_systems["lucos_eolas"],
    )
    _delete_doc_mock.assert_called_once()


# ---------------------------------------------------------------------------
# *Merged handler
# ---------------------------------------------------------------------------


def test_merged_event_merges_in_triplestore():
    status, body = _make_request({
        "type": "albumMerged",
        "source": "lucos_eolas",
        "sourceUri": "https://eolas.l42.eu/metadata/old",
        "targetUri": "https://eolas.l42.eu/metadata/new",
    })
    assert status == 200
    assert body == "Merged"
    _merge_items_mock.assert_called_once_with(
        "https://eolas.l42.eu/metadata/old",
        "https://eolas.l42.eu/metadata/new",
        _live_systems["lucos_eolas"],
    )


def test_merged_event_removes_source_from_searchindex():
    _make_request({
        "type": "albumMerged",
        "source": "lucos_eolas",
        "sourceUri": "https://eolas.l42.eu/metadata/old",
        "targetUri": "https://eolas.l42.eu/metadata/new",
    })
    _fetch_url_mock.assert_not_called()
    _delete_item_mock.assert_not_called()
    _delete_doc_mock.assert_called_once_with("lucos_eolas", "https://eolas.l42.eu/metadata/old")


def test_merged_event_generic_suffix():
    """Any event type ending in 'Merged' is handled, not just 'albumMerged'."""
    status, body = _make_request({
        "type": "personMerged",
        "source": "lucos_contacts",
        "sourceUri": "https://contacts.l42.eu/people/1",
        "targetUri": "https://contacts.l42.eu/people/2",
    })
    assert status == 200
    assert body == "Merged"
    _merge_items_mock.assert_called_once_with(
        "https://contacts.l42.eu/people/1",
        "https://contacts.l42.eu/people/2",
        _live_systems["lucos_contacts"],
    )


def test_merged_event_idempotent_second_call():
    """Calling merge twice for the same URIs should not raise — triplestore handles idempotency."""
    for _ in range(2):
        status, body = _make_request({
            "type": "albumMerged",
            "source": "lucos_eolas",
            "sourceUri": "https://eolas.l42.eu/metadata/old",
            "targetUri": "https://eolas.l42.eu/metadata/new",
        })
        assert status == 200


# ---------------------------------------------------------------------------
# Unknown event type
# ---------------------------------------------------------------------------


def test_unknown_event_type_returns_404():
    status, _ = _make_request({
        "type": "albumReordered",
        "source": "lucos_eolas",
        "url": "https://eolas.l42.eu/metadata/1",
    })
    assert status == 404
