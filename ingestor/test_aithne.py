"""Tests for aithne.py — M2M token minting, caching, and error handling."""
import time
from unittest.mock import MagicMock, patch

import pytest

import aithne as _aithne_module
from aithne import get_aithne_token


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_token_response(access_token="fake.jwt.token", expires_in=3600, status=200):
    resp = MagicMock()
    resp.ok = status < 400
    resp.status_code = status
    resp.text = f"HTTP {status}"
    resp.json.return_value = {"access_token": access_token, "expires_in": expires_in, "token_type": "Bearer"}
    return resp


def _mock_error_response(status=401, body="invalid_client"):
    resp = MagicMock()
    resp.ok = False
    resp.status_code = status
    resp.text = body
    return resp


@pytest.fixture(autouse=True)
def reset_token_cache():
    """Reset the in-memory token cache before each test."""
    _aithne_module._token_cache["token"] = None
    _aithne_module._token_cache["expires_at"] = 0.0
    yield
    _aithne_module._token_cache["token"] = None
    _aithne_module._token_cache["expires_at"] = 0.0


@pytest.fixture(autouse=True)
def set_aithne_env(monkeypatch):
    monkeypatch.setenv("AITHNE_ORIGIN", "http://aithne.test")
    monkeypatch.setenv("AITHNE_CLIENT_ID", "lucos-arachne")
    monkeypatch.setenv("AITHNE_CLIENT_SECRET", "test-secret")


# ---------------------------------------------------------------------------
# Missing AITHNE_CLIENT_SECRET
# ---------------------------------------------------------------------------

class TestMissingClientSecret:
    def test_raises_when_secret_not_set(self, monkeypatch):
        """get_aithne_token must raise when AITHNE_CLIENT_SECRET is absent."""
        monkeypatch.delenv("AITHNE_CLIENT_SECRET", raising=False)
        with pytest.raises(RuntimeError, match="AITHNE_CLIENT_SECRET"):
            get_aithne_token()


# ---------------------------------------------------------------------------
# Successful minting
# ---------------------------------------------------------------------------

class TestSuccessfulMint:
    def test_returns_access_token(self):
        """A successful mint returns the access_token string."""
        with patch.object(_aithne_module, "_requests") as mock_requests:
            mock_requests.post.return_value = _mock_token_response(access_token="h.p.s")
            mock_requests.RequestException = Exception
            token = get_aithne_token()
        assert token == "h.p.s"

    def test_posts_to_correct_endpoint(self):
        """Token request must go to {AITHNE_ORIGIN}/oauth2/token."""
        with patch.object(_aithne_module, "_requests") as mock_requests:
            mock_requests.post.return_value = _mock_token_response()
            mock_requests.RequestException = Exception
            get_aithne_token()
        call_args = mock_requests.post.call_args
        assert call_args[0][0] == "http://aithne.test/oauth2/token"

    def test_sends_client_credentials_grant(self):
        """Request body must include grant_type=client_credentials."""
        with patch.object(_aithne_module, "_requests") as mock_requests:
            mock_requests.post.return_value = _mock_token_response()
            mock_requests.RequestException = Exception
            get_aithne_token()
        data = mock_requests.post.call_args[1]["data"]
        assert data["grant_type"] == "client_credentials"
        assert data["client_id"] == "lucos-arachne"
        assert data["client_secret"] == "test-secret"

    def test_caches_token_on_success(self):
        """A minted token is cached; the second call must not hit the network."""
        with patch.object(_aithne_module, "_requests") as mock_requests:
            mock_requests.post.return_value = _mock_token_response(access_token="cached.token.here")
            mock_requests.RequestException = Exception
            t1 = get_aithne_token()
            t2 = get_aithne_token()
        assert t1 == t2 == "cached.token.here"
        assert mock_requests.post.call_count == 1

    def test_remints_when_cache_nearly_expired(self, monkeypatch):
        """When cached token expires within 60s, a fresh one must be minted."""
        # Seed the cache with a token that expires in 30s (within the 60s buffer)
        _aithne_module._token_cache["token"] = "old.token"
        _aithne_module._token_cache["expires_at"] = time.monotonic() + 30

        with patch.object(_aithne_module, "_requests") as mock_requests:
            mock_requests.post.return_value = _mock_token_response(access_token="new.token")
            mock_requests.RequestException = Exception
            token = get_aithne_token()
        assert token == "new.token"
        assert mock_requests.post.call_count == 1

    def test_uses_cached_token_when_still_valid(self):
        """When cached token expires in >60s, no network call is made."""
        _aithne_module._token_cache["token"] = "still-valid.token"
        _aithne_module._token_cache["expires_at"] = time.monotonic() + 3600

        with patch.object(_aithne_module, "_requests") as mock_requests:
            mock_requests.RequestException = Exception
            token = get_aithne_token()
        assert token == "still-valid.token"
        mock_requests.post.assert_not_called()


# ---------------------------------------------------------------------------
# Error handling — never returns garbage, always raises
# ---------------------------------------------------------------------------

class TestErrorHandling:
    def test_raises_on_http_error(self):
        """A non-2xx aithne response must raise RuntimeError, not return the body."""
        with patch.object(_aithne_module, "_requests") as mock_requests:
            mock_requests.post.return_value = _mock_error_response(status=401, body='{"error":"invalid_client"}')
            mock_requests.RequestException = Exception
            with pytest.raises(RuntimeError, match="401"):
                get_aithne_token()

    def test_raises_on_network_error(self):
        """A network-level failure must raise RuntimeError."""
        with patch.object(_aithne_module, "_requests") as mock_requests:
            mock_requests.RequestException = ConnectionError
            mock_requests.post.side_effect = ConnectionError("connection refused")
            with pytest.raises(RuntimeError, match="network error"):
                get_aithne_token()

    def test_does_not_cache_on_failure(self):
        """After a failed mint the cache must remain empty so the next call retries."""
        with patch.object(_aithne_module, "_requests") as mock_requests:
            mock_requests.post.return_value = _mock_error_response(status=500)
            mock_requests.RequestException = Exception
            with pytest.raises(RuntimeError):
                get_aithne_token()
        assert _aithne_module._token_cache["token"] is None

    def test_raises_on_missing_access_token_field(self):
        """If the response JSON lacks access_token, RuntimeError must be raised."""
        resp = MagicMock()
        resp.ok = True
        resp.status_code = 200
        resp.json.return_value = {"token_type": "Bearer"}  # no access_token
        with patch.object(_aithne_module, "_requests") as mock_requests:
            mock_requests.post.return_value = resp
            mock_requests.RequestException = Exception
            with pytest.raises(RuntimeError):
                get_aithne_token()

    def test_error_includes_status_and_body(self):
        """The RuntimeError message must include the HTTP status and body snippet."""
        body = '{"error":"invalid_scope","error_description":"scope not granted"}'
        with patch.object(_aithne_module, "_requests") as mock_requests:
            mock_requests.post.return_value = _mock_error_response(status=400, body=body)
            mock_requests.RequestException = Exception
            with pytest.raises(RuntimeError) as exc_info:
                get_aithne_token()
        assert "400" in str(exc_info.value)
        assert "invalid_scope" in str(exc_info.value)
