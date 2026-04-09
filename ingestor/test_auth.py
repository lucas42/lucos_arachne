"""Unit tests for is_authorised() and _get_valid_keys() — Phase 3: missing header must return False."""
import os
import sys
import types

import pytest

# Stub out all non-stdlib modules server.py imports at collection time
for mod_name in ("authorised_fetch", "triplestore", "searchindex"):
	stub = types.ModuleType(mod_name)
	stub.fetch_url = None
	stub.live_systems = {}
	stub.replace_item_in_triplestore = None
	stub.delete_item_in_triplestore = None
	stub.update_searchindex = None
	stub.delete_doc_in_searchindex = None
	sys.modules[mod_name] = stub

os.environ.setdefault("PORT", "8080")

_stub_mod_names = list(("authorised_fetch", "triplestore", "searchindex"))
from server import _get_valid_keys, is_authorised

# Remove stubs so real modules are available for other test files in the session
for _mod_name in _stub_mod_names:
	sys.modules.pop(_mod_name, None)


@pytest.fixture(autouse=True)
def clear_client_keys():
	yield
	if "CLIENT_KEYS" in os.environ:
		del os.environ["CLIENT_KEYS"]


# _get_valid_keys tests
def test_get_valid_keys_empty_when_not_set():
	os.environ.pop("CLIENT_KEYS", None)
	assert _get_valid_keys() == set()


def test_get_valid_keys_single_pair():
	os.environ["CLIENT_KEYS"] = "svc=mytoken"
	assert _get_valid_keys() == {"mytoken"}


def test_get_valid_keys_multiple_pairs():
	os.environ["CLIENT_KEYS"] = "a=tokenA;b=tokenB"
	assert _get_valid_keys() == {"tokenA", "tokenB"}


# is_authorised tests (Phase 3 behaviour)
def test_no_client_keys_accepts():
	os.environ.pop("CLIENT_KEYS", None)
	assert is_authorised({}) is True


def test_valid_token_accepted():
	os.environ["CLIENT_KEYS"] = "svc=mysecrettoken"
	assert is_authorised({"Authorization": "Bearer mysecrettoken"}) is True


def test_missing_header_rejected():
	os.environ["CLIENT_KEYS"] = "svc=mysecrettoken"
	assert is_authorised({}) is False


def test_invalid_token_rejected():
	os.environ["CLIENT_KEYS"] = "svc=mysecrettoken"
	assert is_authorised({"Authorization": "Bearer wrongtoken"}) is False


def test_no_bearer_prefix_rejected():
	os.environ["CLIENT_KEYS"] = "svc=mysecrettoken"
	assert is_authorised({"Authorization": "mysecrettoken"}) is False


def test_multiple_keys_first_matches():
	os.environ["CLIENT_KEYS"] = "a=tokenA;b=tokenB"
	assert is_authorised({"Authorization": "Bearer tokenA"}) is True


def test_multiple_keys_second_matches():
	os.environ["CLIENT_KEYS"] = "a=tokenA;b=tokenB"
	assert is_authorised({"Authorization": "Bearer tokenB"}) is True


def test_multiple_keys_none_match():
	os.environ["CLIENT_KEYS"] = "a=tokenA;b=tokenB"
	assert is_authorised({"Authorization": "Bearer tokenC"}) is False
