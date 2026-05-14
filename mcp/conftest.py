"""
Session-scoped pytest fixtures for integration tests that require a local
Fuseki triplestore loaded with the shape fixture.

In CI, a Fuseki sidecar is configured in .circleci/config.yml and is available
at localhost:3030.  Locally, tests are skipped gracefully if Fuseki is not
running — start one with:

    docker run -p 3030:3030 -e ADMIN_PASSWORD=admin stain/jena-fuseki
"""
import os
import socket
import time

import pytest
import requests

_FUSEKI_BASE = "http://localhost:3030"
_DATASET = "test"
_ADMIN_AUTH = ("admin", "admin")
_FIXTURE_PATH = os.path.join(os.path.dirname(__file__), "test_fixtures", "shape.ttl")


def _fuseki_listening() -> bool:
    """Return True immediately if something is listening on port 3030."""
    try:
        with socket.create_connection(("localhost", 3030), timeout=1):
            return True
    except OSError:
        return False


def _wait_for_fuseki(timeout_secs: int = 60) -> bool:
    """
    Poll the Fuseki admin API until it responds or the timeout expires.
    Returns True if ready, False on timeout.
    """
    url = f"{_FUSEKI_BASE}/$/datasets"
    deadline = time.monotonic() + timeout_secs
    while time.monotonic() < deadline:
        try:
            r = requests.get(url, auth=_ADMIN_AUTH, timeout=3)
            if r.status_code < 500:
                return True
        except requests.exceptions.ConnectionError:
            pass
        time.sleep(2)
    return False


@pytest.fixture(scope="session")
def fuseki_sparql_url():
    """
    Return the SPARQL endpoint URL of a local Fuseki instance pre-loaded with
    the shape fixture.  Skips the test session if Fuseki is not reachable.
    """
    if not _fuseki_listening():
        pytest.skip(
            "Fuseki is not listening on localhost:3030 — skipping integration tests. "
            "Start Fuseki with: docker run -p 3030:3030 -e ADMIN_PASSWORD=admin stain/jena-fuseki"
        )

    if not _wait_for_fuseki(timeout_secs=60):
        pytest.skip("Fuseki did not become ready within 60 s — skipping integration tests")

    # Create an in-memory dataset (409 = already exists, which is fine)
    r = requests.post(
        f"{_FUSEKI_BASE}/$/datasets",
        auth=_ADMIN_AUTH,
        data={"dbName": _DATASET, "dbType": "mem"},
        timeout=10,
    )
    if r.status_code not in (200, 201, 409):
        r.raise_for_status()

    # Load the fixture into the default graph
    with open(_FIXTURE_PATH, "rb") as f:
        requests.put(
            f"{_FUSEKI_BASE}/{_DATASET}/data",
            data=f,
            headers={"Content-Type": "text/turtle"},
            auth=_ADMIN_AUTH,
            timeout=30,
        ).raise_for_status()

    return f"{_FUSEKI_BASE}/{_DATASET}/sparql"
