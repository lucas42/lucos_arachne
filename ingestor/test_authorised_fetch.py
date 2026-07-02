"""Tests for authorised_fetch.py — origin allow-list and credential-leak prevention."""
import os
from unittest.mock import MagicMock, patch

import pytest

# Keep a stable reference to the module object so patch.object always hits the
# same __dict__ as fetch_url.__globals__, regardless of what other test files
# do to sys.modules["authorised_fetch"] during collection.
import authorised_fetch as _af_module
from authorised_fetch import _is_trusted_host, fetch_url


# ---------------------------------------------------------------------------
# _is_trusted_host
# ---------------------------------------------------------------------------

class TestIsTrustedHost:
    def test_lucos_production_domain(self):
        assert _is_trusted_host("https://eolas.l42.eu/metadata/all") is True

    def test_other_l42_subdomain(self):
        assert _is_trusted_host("https://contacts.l42.eu/people/all") is True

    def test_localhost(self):
        assert _is_trusted_host("http://localhost/path") is True

    def test_localhost_with_port(self):
        assert _is_trusted_host("http://localhost:8032/path") is True

    def test_loopback_ip(self):
        assert _is_trusted_host("http://127.0.0.1:8032/path") is True

    def test_docker_internal(self):
        assert _is_trusted_host("http://host.docker.internal:8032/path") is True

    def test_external_loc_gov(self):
        assert _is_trusted_host("http://id.loc.gov/vocabulary/iso639-5/cau") is False

    def test_external_schema_org(self):
        assert _is_trusted_host("https://schema.org/MusicAlbum") is False

    def test_external_wikidata(self):
        assert _is_trusted_host("https://www.wikidata.org/wiki/Q42") is False

    def test_subdomain_of_l42_lookalike(self):
        """Hostname that doesn't end in .l42.eu should not be trusted."""
        assert _is_trusted_host("https://notl42.eu/path") is False

    def test_empty_string(self):
        assert _is_trusted_host("") is False


# ---------------------------------------------------------------------------
# fetch_url — auth header allow-list behaviour
# ---------------------------------------------------------------------------

def _mock_response(content_type="text/turtle", text="<> a <> .", status=200, is_redirect=False, location=None):
    resp = MagicMock()
    resp.status_code = status
    resp.text = text
    resp.is_redirect = is_redirect
    resp.is_permanent_redirect = False
    resp.raise_for_status = MagicMock()
    resp.url = "http://placeholder/"
    resp.headers = {"Content-Type": content_type}
    if location:
        resp.headers["Location"] = location
    return resp


@pytest.fixture(autouse=True)
def set_key_env(monkeypatch):
    monkeypatch.setenv("KEY_LUCOS_EOLAS", "test-secret-key")
    monkeypatch.setenv("SYSTEM", "lucos_arachne")


class TestFetchUrlAuthHeader:

    def test_auth_sent_to_trusted_lucos_host(self):
        """Bearer token MUST be sent when fetching from a lucos production endpoint."""
        mock_resp = _mock_response()
        with patch.object(_af_module, "session") as mock_session:
            mock_session.get.return_value = mock_resp
            fetch_url("lucos_eolas", "https://eolas.l42.eu/metadata/1")
        args, kwargs = mock_session.get.call_args
        assert kwargs["headers"].get("Authorization") == "Bearer test-secret-key"

    def test_auth_not_sent_to_external_host(self):
        """Bearer token MUST NOT be sent when the URL resolves to a non-lucos host."""
        mock_resp = _mock_response()
        with patch.object(_af_module, "session") as mock_session:
            mock_session.get.return_value = mock_resp
            fetch_url("lucos_eolas", "http://id.loc.gov/vocabulary/iso639-5/cau")
        args, kwargs = mock_session.get.call_args
        assert "Authorization" not in kwargs["headers"]

    def test_auth_sent_to_localhost(self):
        """Bearer token MUST be sent to localhost (dev environment)."""
        mock_resp = _mock_response()
        with patch.object(_af_module, "session") as mock_session:
            mock_session.get.return_value = mock_resp
            # localhost gets rewritten to host.docker.internal by map_localhost
            fetch_url("lucos_eolas", "http://localhost:8032/metadata/1")
        args, kwargs = mock_session.get.call_args
        assert kwargs["headers"].get("Authorization") == "Bearer test-secret-key"

    def test_no_auth_for_non_lucos_system(self):
        """No auth header for systems not prefixed lucos_."""
        mock_resp = _mock_response()
        with patch.object(_af_module, "session") as mock_session:
            mock_session.get.return_value = mock_resp
            fetch_url("external_system", "https://eolas.l42.eu/metadata/1")
        args, kwargs = mock_session.get.call_args
        assert "Authorization" not in kwargs["headers"]


class TestFetchUrlRedirectAuth:

    def test_auth_not_forwarded_across_redirect_to_external_host(self):
        """
        If a lucos endpoint redirects to an external host, the Bearer token must
        be dropped for the redirect request — credentials must not cross an origin
        boundary.
        """
        redirect_resp = _mock_response(
            is_redirect=True,
            location="http://id.loc.gov/vocabulary/iso639-5/cau",
        )
        redirect_resp.url = "https://eolas.l42.eu/metadata/1"
        final_resp = _mock_response()

        with patch.object(_af_module, "session") as mock_session:
            mock_session.get.side_effect = [redirect_resp, final_resp]
            fetch_url("lucos_eolas", "https://eolas.l42.eu/metadata/1")

        assert mock_session.get.call_count == 2
        args0, kwargs0 = mock_session.get.call_args_list[0]
        args1, kwargs1 = mock_session.get.call_args_list[1]

        # First call (to lucos host): has auth
        assert kwargs0["headers"].get("Authorization") == "Bearer test-secret-key"
        # Redirect call (to id.loc.gov): no auth
        assert "Authorization" not in kwargs1["headers"]

    def test_auth_forwarded_across_redirect_within_trusted_hosts(self):
        """Redirect between two trusted lucos hosts should still carry auth."""
        redirect_resp = _mock_response(
            is_redirect=True,
            location="https://eolas.l42.eu/metadata/canonical/1",
        )
        redirect_resp.url = "https://eolas.l42.eu/metadata/1"
        final_resp = _mock_response()

        with patch.object(_af_module, "session") as mock_session:
            mock_session.get.side_effect = [redirect_resp, final_resp]
            fetch_url("lucos_eolas", "https://eolas.l42.eu/metadata/1")

        args1, kwargs1 = mock_session.get.call_args_list[1]
        assert kwargs1["headers"].get("Authorization") == "Bearer test-secret-key"
