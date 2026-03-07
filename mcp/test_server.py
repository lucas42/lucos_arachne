"""
Tests for the Arachne MCP server.

All tests mock out HTTP calls to the triplestore and Typesense so they
can run without any external services.
"""

import json
from unittest.mock import MagicMock, patch

import pytest

import server


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sparql_response(bindings: list[dict]) -> MagicMock:
    """Build a mock requests.Response containing a SPARQL JSON result."""
    mock = MagicMock()
    mock.raise_for_status = MagicMock()
    mock.json.return_value = {"results": {"bindings": bindings}}
    return mock


def _literal(value: str, datatype: str = None, lang: str = None) -> dict:
    result = {"type": "literal", "value": value}
    if datatype:
        result["datatype"] = datatype
    if lang:
        result["xml:lang"] = lang
    return result


def _uri_binding(value: str) -> dict:
    return {"type": "uri", "value": value}


# ---------------------------------------------------------------------------
# _is_uri
# ---------------------------------------------------------------------------

def test_is_uri_http():
    assert server._is_uri("http://example.com/foo") is True


def test_is_uri_https():
    assert server._is_uri("https://example.com/foo") is True


def test_is_uri_label():
    assert server._is_uri("Person") is False


def test_is_uri_empty():
    assert server._is_uri("") is False


# ---------------------------------------------------------------------------
# _validate_uri_for_sparql
# ---------------------------------------------------------------------------

def test_validate_uri_valid():
    assert server._validate_uri_for_sparql("https://arachne.l42.eu/person/1") is None


def test_validate_uri_with_angle_bracket():
    err = server._validate_uri_for_sparql("https://example.com/foo>bar")
    assert err is not None
    assert "Invalid URI" in err


def test_validate_uri_with_space():
    err = server._validate_uri_for_sparql("https://example.com/foo bar")
    assert err is not None
    assert "Invalid URI" in err


# ---------------------------------------------------------------------------
# _validate_label_for_sparql
# ---------------------------------------------------------------------------

def test_validate_label_valid():
    assert server._validate_label_for_sparql("Person") is None


def test_validate_label_with_double_quote():
    err = server._validate_label_for_sparql('foo"bar')
    assert err is not None
    assert "Invalid label" in err


def test_validate_label_with_backslash():
    err = server._validate_label_for_sparql("foo\\bar")
    assert err is not None
    assert "Invalid label" in err


def test_validate_label_injection_attempt():
    """A classic SPARQL injection payload is rejected."""
    err = server._validate_label_for_sparql('foo" ) } UNION { ?s ?p ?o } #')
    assert err is not None
    assert "Invalid label" in err


# ---------------------------------------------------------------------------
# _resolve_type_uri
# ---------------------------------------------------------------------------

def test_resolve_type_uri_already_a_uri():
    """When given a URI, return it directly without querying the triplestore."""
    with patch("server.requests.get") as mock_get:
        uri, err = server._resolve_type_uri("https://schema.org/Person")
        mock_get.assert_not_called()
    assert uri == "https://schema.org/Person"
    assert err is None


def test_resolve_type_uri_by_label():
    """Resolve a human-readable type name via SPARQL label lookup."""
    sparql_response = _sparql_response([
        {"type": _uri_binding("https://schema.org/Person")},
    ])
    with patch("server.requests.get", return_value=sparql_response) as mock_get:
        uri, err = server._resolve_type_uri("Person")

    assert err is None
    assert uri == "https://schema.org/Person"
    # Should have queried the triplestore
    mock_get.assert_called_once()


def test_resolve_type_uri_not_found():
    """Return an error when no matching type exists."""
    sparql_response = _sparql_response([])
    with patch("server.requests.get", return_value=sparql_response):
        uri, err = server._resolve_type_uri("NonExistentType")

    assert uri is None
    assert "No type found" in err
    assert "NonExistentType" in err


def test_resolve_type_uri_invalid_uri():
    """Return an error for a URI-like string that contains SPARQL-unsafe characters."""
    uri, err = server._resolve_type_uri("http://example.com/foo>bar")
    assert uri is None
    assert "Invalid URI" in err


def test_resolve_type_uri_injection_attempt():
    """Reject a label containing a double-quote to prevent SPARQL injection."""
    with patch("server.requests.get") as mock_get:
        uri, err = server._resolve_type_uri('foo" ) } UNION { ?s ?p ?o } #')
        mock_get.assert_not_called()
    assert uri is None
    assert "Invalid label" in err


# ---------------------------------------------------------------------------
# _resolve_property_uri
# ---------------------------------------------------------------------------

def test_resolve_property_uri_already_a_uri():
    """When given a URI, return it directly without querying the triplestore."""
    with patch("server.requests.get") as mock_get:
        uri, err = server._resolve_property_uri("https://schema.org/birthDate")
        mock_get.assert_not_called()
    assert uri == "https://schema.org/birthDate"
    assert err is None


def test_resolve_property_uri_by_name():
    """Resolve a property name via SPARQL lookup."""
    sparql_response = _sparql_response([
        {"prop": _uri_binding("https://schema.org/birthDate")},
    ])
    with patch("server.requests.get", return_value=sparql_response) as mock_get:
        uri, err = server._resolve_property_uri("birthDate")

    assert err is None
    assert uri == "https://schema.org/birthDate"
    mock_get.assert_called_once()


def test_resolve_property_uri_not_found():
    """Return an error when no matching property exists."""
    sparql_response = _sparql_response([])
    with patch("server.requests.get", return_value=sparql_response):
        uri, err = server._resolve_property_uri("nonExistentProp")

    assert uri is None
    assert "No property found" in err


def test_resolve_property_uri_injection_attempt():
    """Reject a property name containing a double-quote to prevent SPARQL injection."""
    with patch("server.requests.get") as mock_get:
        uri, err = server._resolve_property_uri('foo" ) } UNION { ?s ?p ?o } #')
        mock_get.assert_not_called()
    assert uri is None
    assert "Invalid label" in err


# ---------------------------------------------------------------------------
# find_entities
# ---------------------------------------------------------------------------

def _make_entity_bindings(entities: list[dict]) -> list[dict]:
    """
    Build SPARQL bindings for find_entities results.
    Each entity dict has: s (URI), label (str or None), and optional prop values.
    """
    bindings = []
    for entity in entities:
        row = {
            "s": _uri_binding(entity["s"]),
        }
        if entity.get("label"):
            row["label"] = _literal(entity["label"])
        # prop values are passed as val0, val1, etc.
        for key, value in entity.items():
            if key.startswith("val"):
                row[key] = _literal(value)
        bindings.append(row)
    return bindings


def test_find_entities_no_properties():
    """find_entities returns a list of entities when no properties requested."""
    type_bindings = _sparql_response([
        {"type": _uri_binding("https://schema.org/Person")},
    ])
    entity_bindings = _sparql_response(_make_entity_bindings([
        {"s": "https://arachne.l42.eu/person/1", "label": "Alice"},
        {"s": "https://arachne.l42.eu/person/2", "label": "Bob"},
    ]))

    with patch("server.requests.get", side_effect=[type_bindings, entity_bindings]):
        result = server.find_entities(type="Person")

    assert "Alice" in result
    assert "Bob" in result
    assert "https://arachne.l42.eu/person/1" in result
    assert "https://arachne.l42.eu/person/2" in result


def test_find_entities_with_type_uri_directly():
    """find_entities skips type resolution when given a URI directly."""
    entity_bindings = _sparql_response(_make_entity_bindings([
        {"s": "https://arachne.l42.eu/person/1", "label": "Alice"},
    ]))

    # Only one SPARQL call — the entity query (no type resolution call)
    with patch("server.requests.get", return_value=entity_bindings) as mock_get:
        result = server.find_entities(type="https://schema.org/Person")

    assert mock_get.call_count == 1
    assert "Alice" in result


def test_find_entities_with_property():
    """find_entities includes property values in results."""
    type_response = _sparql_response([
        {"type": _uri_binding("https://schema.org/Person")},
    ])
    prop_response = _sparql_response([
        {"prop": _uri_binding("https://schema.org/birthDate")},
    ])
    entity_bindings = _sparql_response(_make_entity_bindings([
        {"s": "https://arachne.l42.eu/person/1", "label": "Alice", "val0": "1990-03-15"},
        {"s": "https://arachne.l42.eu/person/2", "label": "Bob"},  # no birthday
    ]))

    with patch("server.requests.get", side_effect=[type_response, prop_response, entity_bindings]):
        result = server.find_entities(type="Person", properties=["birthDate"])

    assert "Alice" in result
    assert "1990-03-15" in result
    assert "Bob" in result
    assert "birthDate" in result


def test_find_entities_type_not_found():
    """find_entities returns a helpful error when the type can't be resolved."""
    type_response = _sparql_response([])

    with patch("server.requests.get", return_value=type_response):
        result = server.find_entities(type="Unicorn")

    assert "No type found" in result
    assert "Unicorn" in result


def test_find_entities_no_results():
    """find_entities returns a message when the type exists but has no entities."""
    type_response = _sparql_response([
        {"type": _uri_binding("https://schema.org/Person")},
    ])
    entity_response = _sparql_response([])

    with patch("server.requests.get", side_effect=[type_response, entity_response]):
        result = server.find_entities(type="Person")

    assert "No entities" in result


def test_find_entities_limit():
    """find_entities passes the limit to SPARQL."""
    type_response = _sparql_response([
        {"type": _uri_binding("https://schema.org/Person")},
    ])
    entity_response = _sparql_response(_make_entity_bindings([
        {"s": "https://arachne.l42.eu/person/1", "label": "Alice"},
    ]))

    with patch("server.requests.get", side_effect=[type_response, entity_response]) as mock_get:
        server.find_entities(type="Person", limit=5)

    # The entity query call is the second one
    entity_call = mock_get.call_args_list[1]
    query_param = entity_call[1]["params"]["query"]
    assert "LIMIT 5" in query_param


def test_find_entities_deduplicates_multi_value_properties():
    """When a property has multiple values, all are shown but each only once."""
    type_response = _sparql_response([
        {"type": _uri_binding("https://schema.org/MusicGroup")},
    ])
    prop_response = _sparql_response([
        {"prop": _uri_binding("https://schema.org/member")},
    ])
    # Two rows for same entity — different member values
    entity_bindings = _sparql_response([
        {
            "s": _uri_binding("https://arachne.l42.eu/band/1"),
            "label": _literal("The Beatles"),
            "val0": _literal("John"),
        },
        {
            "s": _uri_binding("https://arachne.l42.eu/band/1"),
            "label": _literal("The Beatles"),
            "val0": _literal("Paul"),
        },
    ])

    with patch("server.requests.get", side_effect=[type_response, prop_response, entity_bindings]):
        result = server.find_entities(type="MusicGroup", properties=["member"])

    # Only one entity entry (de-duplicated by URI)
    assert result.count("The Beatles") == 1
    assert "John" in result
    assert "Paul" in result


def test_find_entities_property_not_found():
    """find_entities returns an error when a property name can't be resolved."""
    type_response = _sparql_response([
        {"type": _uri_binding("https://schema.org/Person")},
    ])
    prop_response = _sparql_response([])  # property not found

    with patch("server.requests.get", side_effect=[type_response, prop_response]):
        result = server.find_entities(type="Person", properties=["nonExistentProp"])

    assert "Could not resolve property" in result
    assert "nonExistentProp" in result


# ---------------------------------------------------------------------------
# count_by_property
# ---------------------------------------------------------------------------

def _count_response(total: int, with_prop: int) -> MagicMock:
    """Build a mock SPARQL response for count_by_property queries."""
    return _sparql_response([
        {
            "total": _literal(str(total), datatype="http://www.w3.org/2001/XMLSchema#integer"),
            "withProp": _literal(str(with_prop), datatype="http://www.w3.org/2001/XMLSchema#integer"),
        }
    ])


def test_count_by_property_basic():
    """count_by_property returns total and matching counts."""
    type_response = _sparql_response([
        {"type": _uri_binding("https://schema.org/MusicRecording")},
    ])
    prop_response = _sparql_response([
        {"prop": _uri_binding("https://schema.org/lyrics")},
    ])
    count_response = _count_response(total=3891, with_prop=1247)

    with patch("server.requests.get", side_effect=[type_response, prop_response, count_response]):
        result = server.count_by_property(type="Track", property="lyrics")

    assert "1,247" in result
    assert "3,891" in result
    assert "Track" in result
    assert "lyrics" in result


def test_count_by_property_with_type_uri_directly():
    """count_by_property skips type resolution when given a URI directly."""
    prop_response = _sparql_response([
        {"prop": _uri_binding("https://schema.org/lyrics")},
    ])
    count_response = _count_response(total=100, with_prop=50)

    # Only two SPARQL calls: property resolution + count query (no type resolution)
    with patch("server.requests.get", side_effect=[prop_response, count_response]) as mock_get:
        result = server.count_by_property(
            type="https://schema.org/MusicRecording",
            property="lyrics",
        )

    assert mock_get.call_count == 2
    assert "50" in result
    assert "100" in result


def test_count_by_property_with_property_uri_directly():
    """count_by_property skips property resolution when given a URI directly."""
    type_response = _sparql_response([
        {"type": _uri_binding("https://schema.org/MusicRecording")},
    ])
    count_response = _count_response(total=500, with_prop=200)

    # Only two SPARQL calls: type resolution + count query (no property resolution)
    with patch("server.requests.get", side_effect=[type_response, count_response]) as mock_get:
        result = server.count_by_property(
            type="Track",
            property="https://schema.org/lyrics",
        )

    assert mock_get.call_count == 2
    assert "200" in result
    assert "500" in result


def test_count_by_property_none_have_property():
    """count_by_property returns a sensible result when no entities have the property."""
    type_response = _sparql_response([
        {"type": _uri_binding("https://schema.org/MusicRecording")},
    ])
    prop_response = _sparql_response([
        {"prop": _uri_binding("https://schema.org/lyrics")},
    ])
    count_response = _count_response(total=3891, with_prop=0)

    with patch("server.requests.get", side_effect=[type_response, prop_response, count_response]):
        result = server.count_by_property(type="Track", property="lyrics")

    assert "0" in result
    assert "3,891" in result


def test_count_by_property_type_not_found():
    """count_by_property returns an error when the type can't be resolved."""
    type_response = _sparql_response([])

    with patch("server.requests.get", return_value=type_response):
        result = server.count_by_property(type="Unicorn", property="lyrics")

    assert "No type found" in result
    assert "Unicorn" in result


def test_count_by_property_property_not_found():
    """count_by_property returns an error when the property can't be resolved."""
    type_response = _sparql_response([
        {"type": _uri_binding("https://schema.org/MusicRecording")},
    ])
    prop_response = _sparql_response([])

    with patch("server.requests.get", side_effect=[type_response, prop_response]):
        result = server.count_by_property(type="Track", property="nonExistentProp")

    assert "No property found" in result
