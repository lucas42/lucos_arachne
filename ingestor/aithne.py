"""Aithne M2M token acquisition for the arachne ingestor.

Implements the OAuth2 client_credentials flow (RFC 6749 §4.4) against the
aithne /oauth2/token endpoint.  Tokens are cached in memory until they are
within 60 seconds of their reported expiry, at which point a fresh token is
minted transparently.

Environment variables consumed:
  AITHNE_ORIGIN        — base URL of the aithne instance
                          (default: https://aithne.l42.eu)
  AITHNE_CLIENT_ID     — OAuth2 client_id (machine-key principal slug)
                          (default: lucos-arachne)
  AITHNE_CLIENT_SECRET — raw machine-key secret (required; no default)

Logging:
  On a successful mint: prints the client_id and reported expiry, never
  the token value itself.
  On failure: prints the HTTP status and the first 500 characters of the
  response body to aid diagnosis without leaking any secrets.
"""

import os
import time

import requests as _requests

# Module-level token cache — a single lucos-arachne identity covers all
# ingestor fetches, so one cached token is sufficient.
_token_cache: dict = {"token": None, "expires_at": 0.0}


def get_aithne_token() -> str:
	"""Return a valid aithne JWT for the lucos-arachne identity.

	Reads AITHNE_ORIGIN, AITHNE_CLIENT_ID, and AITHNE_CLIENT_SECRET from the
	environment.  Returns a cached token when the current one has >60 s left;
	otherwise mints a new one.

	Raises RuntimeError if:
	  - AITHNE_CLIENT_SECRET is not set
	  - The network request fails
	  - Aithne returns a non-2xx response
	  - The response body is missing the access_token field

	Callers must not catch RuntimeError and use the exception message as a
	bearer token — they should let it propagate so the caller of fetch_url()
	can log it as an ingest failure.
	"""
	client_secret = os.environ.get("AITHNE_CLIENT_SECRET")
	if not client_secret:
		raise RuntimeError(
			"AITHNE_CLIENT_SECRET is not set — cannot mint aithne JWT for "
			"authorised fetches.  Set it in lucos_creds and add it to the "
			"ingestor service in docker-compose.yml."
		)

	aithne_origin = os.environ.get("AITHNE_ORIGIN", "https://aithne.l42.eu")
	client_id = os.environ.get("AITHNE_CLIENT_ID", "lucos-arachne")

	# Return the cached token if it is still valid with a 60-second buffer.
	now = time.monotonic()
	if _token_cache["token"] and _token_cache["expires_at"] > now + 60:
		return _token_cache["token"]

	# Mint a fresh token.
	token_url = f"{aithne_origin}/oauth2/token"
	try:
		resp = _requests.post(
			token_url,
			data={
				"grant_type": "client_credentials",
				"client_id": client_id,
				"client_secret": client_secret,
			},
			timeout=10,
		)
	except _requests.RequestException as exc:
		raise RuntimeError(
			f"[aithne] Token mint failed (network error) for client_id={client_id!r}: {exc}"
		) from exc

	if not resp.ok:
		body_preview = resp.text[:500]
		raise RuntimeError(
			f"[aithne] Token mint failed: HTTP {resp.status_code} from {token_url} "
			f"for client_id={client_id!r} — {body_preview}"
		)

	try:
		data = resp.json()
		token = data["access_token"]
	except (ValueError, KeyError) as exc:
		raise RuntimeError(
			f"[aithne] Unexpected token response from {token_url}: {exc}"
		) from exc

	expires_in = data.get("expires_in", 3600)
	_token_cache["token"] = token
	_token_cache["expires_at"] = now + expires_in

	print(
		f"[aithne] Minted token for client_id={client_id!r} "
		f"(expires_in={expires_in}s)",
		flush=True,
	)
	return token
