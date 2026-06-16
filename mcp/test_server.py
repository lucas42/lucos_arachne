"""
Tests for the Arachne MCP server.

All tests mock out HTTP calls to the triplestore and Typesense so they
can run without any external services.
"""

import asyncio
import json
import logging
import time
from unittest.mock import MagicMock, patch

import pytest
import requests
from starlette.testclient import TestClient

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
    """find_entities passes the limit to the inner subquery in SPARQL."""
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
    # LIMIT is applied in the inner subquery, not at the top level
    assert "LIMIT 5" in query_param
    # The outer query must not have its own LIMIT (which would re-limit expanded rows)
    assert query_param.count("LIMIT 5") == 1


def test_find_entities_limit_governs_entity_count_not_rows():
    """When properties have multiple values, limit controls entities not rows."""
    type_response = _sparql_response([
        {"type": _uri_binding("https://schema.org/MusicGroup")},
    ])
    prop_response = _sparql_response([
        {"prop": _uri_binding("https://schema.org/member")},
    ])
    # SPARQL returns 2 rows per entity (2 members each), but limit=2 means 2 entities
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
        {
            "s": _uri_binding("https://arachne.l42.eu/band/2"),
            "label": _literal("Led Zeppelin"),
            "val0": _literal("Robert Plant"),
        },
        {
            "s": _uri_binding("https://arachne.l42.eu/band/2"),
            "label": _literal("Led Zeppelin"),
            "val0": _literal("Jimmy Page"),
        },
    ])

    with patch("server.requests.get", side_effect=[type_response, prop_response, entity_bindings]):
        result = server.find_entities(type="MusicGroup", properties=["member"], limit=2)

    # Both entities should be present — 4 SPARQL rows represent exactly 2 entities
    assert "The Beatles" in result
    assert "Led Zeppelin" in result
    assert result.startswith("Found 2")


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


def test_find_entities_with_uri_filter():
    """find_entities with a URI filter adds a triple pattern to the inner subquery."""
    type_response = _sparql_response([
        {"type": _uri_binding("https://schema.org/State")},
    ])
    prop_response = _sparql_response([
        {"prop": _uri_binding("https://schema.org/containedIn")},
    ])
    entity_response = _sparql_response(_make_entity_bindings([
        {"s": "https://arachne.l42.eu/state/1", "label": "California"},
    ]))

    with patch("server.requests.get", side_effect=[type_response, prop_response, entity_response]) as mock_get:
        result = server.find_entities(
            type="State",
            filters=[{"property": "containedIn", "value": "https://example.org/usa"}],
        )

    assert "California" in result
    # Filter constraint should appear in the SPARQL query
    entity_call = mock_get.call_args_list[2]
    query_param = entity_call[1]["params"]["query"]
    assert "<https://example.org/usa>" in query_param
    assert "containedIn" in query_param or "schema.org/containedIn" in query_param


def test_find_entities_with_literal_filter():
    """find_entities with a literal filter value wraps it in quotes."""
    type_response = _sparql_response([
        {"type": _uri_binding("https://schema.org/Person")},
    ])
    prop_response = _sparql_response([
        {"prop": _uri_binding("https://schema.org/nationality")},
    ])
    entity_response = _sparql_response(_make_entity_bindings([
        {"s": "https://arachne.l42.eu/person/1", "label": "Alice"},
    ]))

    with patch("server.requests.get", side_effect=[type_response, prop_response, entity_response]) as mock_get:
        result = server.find_entities(
            type="Person",
            filters=[{"property": "nationality", "value": "British"}],
        )

    assert "Alice" in result
    entity_call = mock_get.call_args_list[2]
    query_param = entity_call[1]["params"]["query"]
    assert '"British"' in query_param


def test_find_entities_multiple_filters():
    """Multiple filters are all included as required triple patterns."""
    type_response = _sparql_response([
        {"type": _uri_binding("https://schema.org/Festival")},
    ])
    prop1_response = _sparql_response([
        {"prop": _uri_binding("https://schema.org/containedIn")},
    ])
    prop2_response = _sparql_response([
        {"prop": _uri_binding("https://schema.org/genre")},
    ])
    entity_response = _sparql_response(_make_entity_bindings([
        {"s": "https://arachne.l42.eu/festival/1", "label": "Glastonbury"},
    ]))

    with patch("server.requests.get", side_effect=[type_response, prop1_response, prop2_response, entity_response]) as mock_get:
        result = server.find_entities(
            type="Festival",
            filters=[
                {"property": "containedIn", "value": "https://example.org/uk"},
                {"property": "genre", "value": "Music"},
            ],
        )

    assert "Glastonbury" in result
    entity_call = mock_get.call_args_list[3]
    query_param = entity_call[1]["params"]["query"]
    assert "<https://example.org/uk>" in query_param
    assert '"Music"' in query_param


def test_find_entities_filter_property_not_found():
    """find_entities returns an error when a filter property can't be resolved."""
    type_response = _sparql_response([
        {"type": _uri_binding("https://schema.org/Person")},
    ])
    prop_response = _sparql_response([])  # property not found

    with patch("server.requests.get", side_effect=[type_response, prop_response]):
        result = server.find_entities(
            type="Person",
            filters=[{"property": "nonExistentProp", "value": "something"}],
        )

    assert "Could not resolve filter property" in result
    assert "nonExistentProp" in result


def test_find_entities_filter_invalid_uri_value():
    """find_entities rejects filter values that are invalid URIs."""
    type_response = _sparql_response([
        {"type": _uri_binding("https://schema.org/Person")},
    ])
    prop_response = _sparql_response([
        {"prop": _uri_binding("https://schema.org/containedIn")},
    ])

    with patch("server.requests.get", side_effect=[type_response, prop_response]):
        result = server.find_entities(
            type="Person",
            filters=[{"property": "containedIn", "value": "https://evil.com/>inject"}],
        )

    assert "Invalid filter value" in result


# ---------------------------------------------------------------------------
# get_entity
# ---------------------------------------------------------------------------

def _bnode_binding(bnode_id: str) -> dict:
    return {"type": "bnode", "value": bnode_id}


def test_get_entity_basic():
    """get_entity returns URI and literal properties for an entity."""
    entity_response = _sparql_response([
        {"p": _uri_binding("http://www.w3.org/2004/02/skos/core#prefLabel"), "o": _literal("Alice")},
        {"p": _uri_binding("https://schema.org/birthDate"), "o": _literal("1990-03-15")},
    ])

    with patch("server.requests.get", return_value=entity_response):
        result = server.get_entity("https://arachne.l42.eu/person/1")

    assert "Entity: <https://arachne.l42.eu/person/1>" in result
    assert "skos:prefLabel" in result
    assert "Alice" in result
    assert "birthDate" in result
    assert "1990-03-15" in result


def test_get_entity_not_found():
    """get_entity returns a helpful message when the URI has no properties."""
    entity_response = _sparql_response([])

    with patch("server.requests.get", return_value=entity_response):
        result = server.get_entity("https://arachne.l42.eu/person/99")

    assert "No properties found" in result
    assert "https://arachne.l42.eu/person/99" in result


def test_get_entity_invalid_uri():
    """get_entity rejects URIs containing injection characters."""
    result = server.get_entity("https://example.com/foo>bar")
    assert "Invalid URI" in result


def test_get_entity_bnode_single():
    """get_entity resolves a single blank node value and displays its sub-properties."""
    entity_response = _sparql_response([
        {
            "p": _uri_binding("https://arachne.l42.eu/ontology/festivalStartsOn"),
            "o": _bnode_binding("b0"),
            "bp": _uri_binding("https://arachne.l42.eu/ontology/month"),
            "bo": _literal("7"),
        },
        {
            "p": _uri_binding("https://arachne.l42.eu/ontology/festivalStartsOn"),
            "o": _bnode_binding("b0"),
            "bp": _uri_binding("https://arachne.l42.eu/ontology/day"),
            "bo": _literal("15"),
        },
    ])

    with patch("server.requests.get", return_value=entity_response):
        result = server.get_entity("https://arachne.l42.eu/festival/1")

    # The blank node raw ID should not appear in output
    assert "b0" not in result
    # Sub-properties should be present
    assert "month" in result
    assert '"7"' in result
    assert "day" in result
    assert '"15"' in result
    # Should be listed under the parent property header
    assert "festivalStartsOn:" in result


def test_get_entity_multiple_bnodes_same_property():
    """get_entity handles multiple blank node values for the same property."""
    entity_response = _sparql_response([
        {
            "p": _uri_binding("https://arachne.l42.eu/ontology/festivalStartsOn"),
            "o": _bnode_binding("b0"),
            "bp": _uri_binding("https://arachne.l42.eu/ontology/month"),
            "bo": _literal("7"),
        },
        {
            "p": _uri_binding("https://arachne.l42.eu/ontology/festivalStartsOn"),
            "o": _bnode_binding("b1"),
            "bp": _uri_binding("https://arachne.l42.eu/ontology/month"),
            "bo": _literal("8"),
        },
    ])

    with patch("server.requests.get", return_value=entity_response):
        result = server.get_entity("https://arachne.l42.eu/festival/1")

    assert "b0" not in result
    assert "b1" not in result
    assert '"7"' in result
    assert '"8"' in result
    assert "festivalStartsOn:" in result


def test_get_entity_bnode_without_sub_properties():
    """get_entity handles a blank node that has no resolvable sub-properties."""
    entity_response = _sparql_response([
        {
            "p": _uri_binding("https://schema.org/address"),
            "o": _bnode_binding("b0"),
            # No bp/bo — the OPTIONAL returned nothing
        },
    ])

    with patch("server.requests.get", return_value=entity_response):
        result = server.get_entity("https://arachne.l42.eu/person/1")

    # Property should still appear, blank node ID should not
    assert "address" in result
    assert "b0" not in result


# ---------------------------------------------------------------------------
# count_by_property
# ---------------------------------------------------------------------------

def _total_response(total: int) -> MagicMock:
    """Build a mock SPARQL response for the `?total` count query."""
    return _sparql_response([
        {"total": _literal(str(total), datatype="http://www.w3.org/2001/XMLSchema#integer")},
    ])


def _with_prop_response(with_prop: int) -> MagicMock:
    """Build a mock SPARQL response for the `?withProp` count query."""
    return _sparql_response([
        {"withProp": _literal(str(with_prop), datatype="http://www.w3.org/2001/XMLSchema#integer")},
    ])


def test_count_by_property_basic():
    """count_by_property returns total and matching counts."""
    type_response = _sparql_response([
        {"type": _uri_binding("https://schema.org/MusicRecording")},
    ])
    prop_response = _sparql_response([
        {"prop": _uri_binding("https://schema.org/lyrics")},
    ])
    total_response = _total_response(3891)
    with_prop_response = _with_prop_response(1247)

    with patch(
        "server.requests.get",
        side_effect=[type_response, prop_response, total_response, with_prop_response],
    ):
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
    total_response = _total_response(100)
    with_prop_response = _with_prop_response(50)

    # Three SPARQL calls: property resolution + two count queries (no type resolution)
    with patch(
        "server.requests.get",
        side_effect=[prop_response, total_response, with_prop_response],
    ) as mock_get:
        result = server.count_by_property(
            type="https://schema.org/MusicRecording",
            property="lyrics",
        )

    assert mock_get.call_count == 3
    assert "50" in result
    assert "100" in result


def test_count_by_property_with_property_uri_directly():
    """count_by_property skips property resolution when given a URI directly."""
    type_response = _sparql_response([
        {"type": _uri_binding("https://schema.org/MusicRecording")},
    ])
    total_response = _total_response(500)
    with_prop_response = _with_prop_response(200)

    # Three SPARQL calls: type resolution + two count queries (no property resolution)
    with patch(
        "server.requests.get",
        side_effect=[type_response, total_response, with_prop_response],
    ) as mock_get:
        result = server.count_by_property(
            type="Track",
            property="https://schema.org/lyrics",
        )

    assert mock_get.call_count == 3
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
    total_response = _total_response(3891)
    with_prop_response = _with_prop_response(0)

    with patch(
        "server.requests.get",
        side_effect=[type_response, prop_response, total_response, with_prop_response],
    ):
        result = server.count_by_property(type="Track", property="lyrics")

    assert "0" in result
    assert "3,891" in result


def test_count_by_property_query_shape_no_cartesian_product():
    """
    Regression test for #477.

    The original query used a single SELECT with an OPTIONAL block whose
    inner subject (`?sWithProp`) shared no join variable with the outer
    subject (`?s`). Fuseki materialised the Cartesian product of "all
    instances" × "all instances with property", which at production data
    sizes always tripped the 30 s service guard.

    The fix is to issue two separate count queries, each constrained to a
    single subject variable. This test introspects the SPARQL strings
    actually sent so a future refactor that re-introduces the Cartesian
    shape will fail here rather than silently regressing in production.
    """
    type_response = _sparql_response([
        {"type": _uri_binding("https://schema.org/MusicRecording")},
    ])
    prop_response = _sparql_response([
        {"prop": _uri_binding("https://schema.org/lyrics")},
    ])
    total_response = _total_response(3891)
    with_prop_response = _with_prop_response(1247)

    with patch(
        "server.requests.get",
        side_effect=[type_response, prop_response, total_response, with_prop_response],
    ) as mock_get:
        server.count_by_property(type="Track", property="lyrics")

    # Type and property resolution + two count queries.
    assert mock_get.call_count == 4

    # The two count queries are calls 3 and 4 (0-indexed: 2 and 3).
    count_queries = [
        mock_get.call_args_list[2].kwargs["params"]["query"],
        mock_get.call_args_list[3].kwargs["params"]["query"],
    ]

    for q in count_queries:
        # No OPTIONAL — that block was the route to the Cartesian product.
        assert "OPTIONAL" not in q, (
            f"count query reintroduced an OPTIONAL block, which risks the "
            f"Cartesian-product shape of #477:\n{q}"
        )
        # Only one subject variable bound — `?s`. The original bug used a
        # separate `?sWithProp` with no join to `?s`.
        assert "?sWithProp" not in q, (
            f"count query reintroduced a second subject variable; this is "
            f"how the original Cartesian product was constructed:\n{q}"
        )
        # Counts subjects, not values — protects against the under-reporting
        # variant flagged by lucos-architect on #477.
        assert "COUNT(DISTINCT ?s)" in q, (
            f"count query no longer counts distinct subjects — counting "
            f"distinct values under-reports for properties whose values "
            f"are shared between subjects:\n{q}"
        )

    # Both queries are constrained to instances of the requested type.
    for q in count_queries:
        assert "<https://schema.org/MusicRecording>" in q
    # Only the second (withProp) query restricts on the property.
    assert "<https://schema.org/lyrics>" not in count_queries[0]
    assert "<https://schema.org/lyrics>" in count_queries[1]


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


# ---------------------------------------------------------------------------
# get_ontology resource
# ---------------------------------------------------------------------------

def test_get_ontology_returns_markdown():
    """get_ontology returns the contents of the ontology.md file."""
    result = server.get_ontology()
    assert isinstance(result, str)
    assert len(result) > 0


def test_get_ontology_contains_key_sections():
    """get_ontology content includes the main sections described in the issue."""
    result = server.get_ontology()
    # Should describe entity types
    assert "Person" in result
    assert "Track" in result
    # Should include prefix mappings
    assert "foaf:" in result
    assert "skos:" in result


def test_get_ontology_contains_properties():
    """get_ontology content references common properties for entity types."""
    result = server.get_ontology()
    assert "foaf:name" in result or "foaf:birthday" in result


# ---------------------------------------------------------------------------
# get_data_sources resource
# ---------------------------------------------------------------------------

def test_get_data_sources_returns_string():
    """get_data_sources returns a non-empty string."""
    result = server.get_data_sources()
    assert isinstance(result, str)
    assert len(result) > 0


def test_get_data_sources_contains_all_systems():
    """get_data_sources lists every system from SYSTEMS_TO_GRAPHS."""
    result = server.get_data_sources()
    for system in server.SYSTEMS_TO_GRAPHS:
        assert system in result, f"Expected system '{system}' to appear in data sources"


def test_get_data_sources_contains_graph_uris():
    """get_data_sources lists the graph URI for each source."""
    result = server.get_data_sources()
    for graph_uri in server.SYSTEMS_TO_GRAPHS.values():
        assert graph_uri in result, f"Expected graph URI '{graph_uri}' to appear in data sources"


def test_get_data_sources_is_markdown_table():
    """get_data_sources output is formatted as a Markdown table."""
    result = server.get_data_sources()
    # Should contain table separator row (pipe characters and dashes)
    assert "|-----" in result


# ---------------------------------------------------------------------------
# Timeout and 503 error handling
# ---------------------------------------------------------------------------
#
# Each MCP tool that contacts an external service must handle:
#   1. Timeout — named as a tool issue, not infrastructure failure.
#   2. 503 from the triplestore — with a health probe to distinguish a real
#      Fuseki outage (likely_outage=True) from a query-level error (False).
#
# Helpers
# -------

def _timeout() -> requests.exceptions.Timeout:
    """Return a Timeout instance for use as a side_effect."""
    return requests.exceptions.Timeout()


def _503_response() -> MagicMock:
    """Return a mock requests.Response with status_code 503."""
    mock = MagicMock()
    mock.status_code = 503
    return mock


def _healthy_response() -> MagicMock:
    """Return a mock health-probe response indicating Fuseki is up."""
    mock = MagicMock()
    mock.status_code = 200
    return mock


def _unhealthy_response() -> MagicMock:
    """Return a mock health-probe response indicating Fuseki is down (5xx)."""
    mock = MagicMock()
    mock.status_code = 503
    return mock


# ---------------------------------------------------------------------------
# search — Typesense timeout
# ---------------------------------------------------------------------------

def test_search_timeout_returns_friendly_message():
    """search returns a friendly timeout message without exposing internal details."""
    with patch("server.requests.get", side_effect=_timeout()):
        result = server.search("Alice")

    assert "timed out" in result.lower()
    # Should not expose raw exception type or stack information
    assert "Timeout" not in result
    assert "Exception" not in result


# ---------------------------------------------------------------------------
# get_entity — triplestore timeout and 503
# ---------------------------------------------------------------------------

def test_get_entity_timeout():
    """get_entity returns a structured timeout message naming the tool."""
    with patch("server.requests.get", side_effect=_timeout()):
        result = server.get_entity("https://arachne.l42.eu/person/1")

    assert "get_entity" in result
    assert "timed out" in result.lower()
    # Budget value should appear so the caller knows the constraint
    assert str(int(server._BUDGET_QUERY_S)) in result


def test_get_entity_503_likely_outage():
    """get_entity reports a probable outage when the health probe also fails."""
    with patch("server.requests.get", side_effect=[_503_response(), _unhealthy_response()]):
        result = server.get_entity("https://arachne.l42.eu/person/1")

    assert "503" in result
    assert "unavailable" in result.lower()


def test_get_entity_503_query_issue():
    """get_entity reports a query-level error when Fuseki is reachable after 503."""
    with patch("server.requests.get", side_effect=[_503_response(), _healthy_response()]):
        result = server.get_entity("https://arachne.l42.eu/person/1")

    assert "503" in result
    assert "query" in result.lower()


# ---------------------------------------------------------------------------
# list_types — triplestore timeout and 503
# ---------------------------------------------------------------------------

def test_list_types_timeout():
    """list_types returns a structured timeout message naming the tool."""
    with patch("server.requests.get", side_effect=_timeout()):
        result = server.list_types()

    assert "list_types" in result
    assert "timed out" in result.lower()
    assert str(int(server._BUDGET_QUERY_S)) in result


def test_list_types_503_likely_outage():
    """list_types reports a probable outage when the health probe also fails."""
    with patch("server.requests.get", side_effect=[_503_response(), _unhealthy_response()]):
        result = server.list_types()

    assert "503" in result
    assert "unavailable" in result.lower()


def test_list_types_503_query_issue():
    """list_types reports a query-level error when Fuseki is reachable after 503."""
    with patch("server.requests.get", side_effect=[_503_response(), _healthy_response()]):
        result = server.list_types()

    assert "503" in result
    assert "query" in result.lower()


# ---------------------------------------------------------------------------
# find_entities — triplestore timeout and 503
# ---------------------------------------------------------------------------

def test_find_entities_timeout_during_type_resolve():
    """find_entities handles a timeout that occurs during type resolution."""
    with patch("server.requests.get", side_effect=_timeout()):
        result = server.find_entities(type="Person")

    assert "find_entities" in result
    assert "timed out" in result.lower()


def test_find_entities_timeout_during_main_query():
    """find_entities handles a timeout that occurs during the main entity query."""
    type_response = _sparql_response([
        {"type": _uri_binding("https://schema.org/Person")},
    ])
    with patch("server.requests.get", side_effect=[type_response, _timeout()]):
        result = server.find_entities(type="Person")

    assert "find_entities" in result
    assert "timed out" in result.lower()


def test_find_entities_503_likely_outage():
    """find_entities reports a probable outage when the health probe also fails."""
    # The type resolver hits the 503; health probe also fails.
    with patch("server.requests.get", side_effect=[_503_response(), _unhealthy_response()]):
        result = server.find_entities(type="Person")

    assert "503" in result
    assert "unavailable" in result.lower()


def test_find_entities_503_query_issue():
    """find_entities reports a query-level error when Fuseki is reachable after 503."""
    type_response = _sparql_response([
        {"type": _uri_binding("https://schema.org/Person")},
    ])
    # Main entity query hits 503; health probe returns 200.
    with patch("server.requests.get", side_effect=[type_response, _503_response(), _healthy_response()]):
        result = server.find_entities(type="Person")

    assert "503" in result
    assert "query" in result.lower()


# ---------------------------------------------------------------------------
# count_by_property — triplestore timeout and 503
# ---------------------------------------------------------------------------

def test_count_by_property_timeout_during_type_resolve():
    """count_by_property handles a timeout during type resolution."""
    with patch("server.requests.get", side_effect=_timeout()):
        result = server.count_by_property(type="Track", property="lyrics")

    assert "count_by_property" in result
    assert "timed out" in result.lower()


def test_count_by_property_timeout_during_count_query():
    """count_by_property handles a timeout during the count queries."""
    type_response = _sparql_response([
        {"type": _uri_binding("https://schema.org/MusicRecording")},
    ])
    prop_response = _sparql_response([
        {"prop": _uri_binding("https://schema.org/lyrics")},
    ])
    with patch("server.requests.get", side_effect=[type_response, prop_response, _timeout()]):
        result = server.count_by_property(type="Track", property="lyrics")

    assert "count_by_property" in result
    assert "timed out" in result.lower()


def test_count_by_property_503_likely_outage():
    """count_by_property reports a probable outage when health probe also fails."""
    with patch("server.requests.get", side_effect=[_503_response(), _unhealthy_response()]):
        result = server.count_by_property(type="Track", property="lyrics")

    assert "503" in result
    assert "unavailable" in result.lower()


def test_count_by_property_503_query_issue():
    """count_by_property reports a query-level error when Fuseki is reachable after 503."""
    type_response = _sparql_response([
        {"type": _uri_binding("https://schema.org/MusicRecording")},
    ])
    prop_response = _sparql_response([
        {"prop": _uri_binding("https://schema.org/lyrics")},
    ])
    total_response = _total_response(1000)
    # Second count query hits 503; health probe returns 200.
    with patch(
        "server.requests.get",
        side_effect=[type_response, prop_response, total_response, _503_response(), _healthy_response()],
    ):
        result = server.count_by_property(type="Track", property="lyrics")

    assert "503" in result
    assert "query" in result.lower()


# ---------------------------------------------------------------------------
# _check_fuseki_health
# ---------------------------------------------------------------------------

def test_check_fuseki_health_up():
    """_check_fuseki_health returns True when Fuseki responds with 2xx."""
    mock = MagicMock()
    mock.status_code = 200
    with patch("server.requests.get", return_value=mock):
        assert server._check_fuseki_health() is True


def test_check_fuseki_health_down_timeout():
    """_check_fuseki_health returns False when the health probe times out."""
    with patch("server.requests.get", side_effect=_timeout()):
        assert server._check_fuseki_health() is False


def test_check_fuseki_health_down_5xx():
    """_check_fuseki_health returns False when Fuseki returns a 5xx response."""
    mock = MagicMock()
    mock.status_code = 503
    with patch("server.requests.get", return_value=mock):
        assert server._check_fuseki_health() is False


# ---------------------------------------------------------------------------
# Budget constant regression
# ---------------------------------------------------------------------------

def test_budgets_below_fuseki_service_guard():
    """All per-tool budgets must be strictly below Fuseki's 30 s service-loop guard."""
    assert server._BUDGET_RESOLVE_S < 30
    assert server._BUDGET_QUERY_S < 30
    assert server._BUDGET_HEALTH_S < 30


# ---------------------------------------------------------------------------
# _build_probe_checks
# ---------------------------------------------------------------------------

def test_build_probe_checks_empty_cache_all_not_probed():
    """All five checks report ok=False before the first probe cycle completes."""
    original = dict(server._probe_cache)
    server._probe_cache.clear()
    try:
        checks = server._build_probe_checks()
        assert len(checks) == 5
        for key, check in checks.items():
            assert check["ok"] is False, f"{key} should be ok=False before first probe"
            assert "not been probed yet" in check["techDetail"]
    finally:
        server._probe_cache.update(original)


def test_build_probe_checks_reads_cached_ok_result():
    """_build_probe_checks returns ok=True when the cache has a fresh successful result."""
    original = dict(server._probe_cache)
    server._probe_cache.clear()
    server._probe_cache["list_types"] = {
        "ok": True,
        "techDetail": "list_types completed in 0.42s (budget 5s)",
        "timestamp": time.monotonic(),
    }
    try:
        checks = server._build_probe_checks()
        assert checks["mcp_list_types"]["ok"] is True
        assert "0.42s" in checks["mcp_list_types"]["techDetail"]
    finally:
        server._probe_cache.clear()
        server._probe_cache.update(original)


def test_build_probe_checks_reads_cached_fail_result():
    """_build_probe_checks relays ok=False from a timeout or error result."""
    original = dict(server._probe_cache)
    server._probe_cache.clear()
    server._probe_cache["count_by_property"] = {
        "ok": False,
        "techDetail": (
            "MCP tool count_by_property exceeded 5s budget"
            " against production data (took 5.0s before timeout)"
        ),
        "timestamp": time.monotonic(),
    }
    try:
        checks = server._build_probe_checks()
        assert checks["mcp_count_by_property"]["ok"] is False
        assert "exceeded 5s budget" in checks["mcp_count_by_property"]["techDetail"]
        assert "against production data" in checks["mcp_count_by_property"]["techDetail"]
    finally:
        server._probe_cache.clear()
        server._probe_cache.update(original)


def test_build_probe_checks_stale_result_reported_as_failed():
    """_build_probe_checks returns ok=False when the cached result is older than the stale threshold."""
    from probe_parameters import PROBE_STALE_THRESHOLD_S

    original = dict(server._probe_cache)
    server._probe_cache.clear()
    stale_timestamp = time.monotonic() - PROBE_STALE_THRESHOLD_S - 1
    server._probe_cache["search"] = {
        "ok": True,
        "techDetail": "search completed in 0.10s (budget 5s)",
        "timestamp": stale_timestamp,
    }
    try:
        checks = server._build_probe_checks()
        assert checks["mcp_search"]["ok"] is False
        assert "stale" in checks["mcp_search"]["techDetail"]
        assert "probe runner may have stopped" in checks["mcp_search"]["techDetail"]
    finally:
        server._probe_cache.clear()
        server._probe_cache.update(original)


def test_build_probe_checks_returns_all_five_tools():
    """_build_probe_checks always returns exactly five check keys, one per tool."""
    original = dict(server._probe_cache)
    server._probe_cache.clear()
    try:
        checks = server._build_probe_checks()
        expected_keys = {
            "mcp_search",
            "mcp_get_entity",
            "mcp_list_types",
            "mcp_find_entities",
            "mcp_count_by_property",
        }
        assert set(checks.keys()) == expected_keys
    finally:
        server._probe_cache.update(original)


def test_build_probe_checks_disambiguates_budget_exceeded_from_fuseki_outage():
    """
    The techDetail for a budget-exceeded failure explicitly names the tool and
    mentions production data, so on-call SREs don't mistake it for a Fuseki outage.
    """
    original = dict(server._probe_cache)
    server._probe_cache.clear()
    server._probe_cache["list_types"] = {
        "ok": False,
        "techDetail": (
            "MCP tool list_types exceeded 5s budget"
            " against production data (took 5.0s before timeout)"
        ),
        "timestamp": time.monotonic(),
    }
    try:
        checks = server._build_probe_checks()
        detail = checks["mcp_list_types"]["techDetail"]
        assert "list_types" in detail, "tool name must appear in detail for fast triage"
        assert "production data" in detail, "must mention production data to disambiguate from Fuseki outage"
    finally:
        server._probe_cache.clear()
        server._probe_cache.update(original)


# ---------------------------------------------------------------------------
# Probe constants regression
# ---------------------------------------------------------------------------

def test_probe_budget_below_fuseki_service_guard():
    """PROBE_BUDGET_S must be strictly below Fuseki's 30 s service guard."""
    from probe_parameters import PROBE_BUDGET_S
    assert PROBE_BUDGET_S < 30


def test_probe_tool_funcs_covers_all_probe_tools():
    """_PROBE_TOOL_FUNCS must have an entry for every tool in PROBE_TOOLS."""
    from probe_parameters import PROBE_TOOLS
    for tool_name, _ in PROBE_TOOLS:
        assert tool_name in server._PROBE_TOOL_FUNCS, (
            f"_PROBE_TOOL_FUNCS is missing an entry for '{tool_name}' "
            f"— probe runner will skip this tool silently"
        )


# ---------------------------------------------------------------------------
# _verify_aithne_agent_jwt — JWKS failure vs token validation failure
# ---------------------------------------------------------------------------

def _run(coro):
    """Run a coroutine synchronously for use in synchronous test functions."""
    return asyncio.run(coro)


def test_verify_aithne_agent_jwt_jwks_failure_returns_false(caplog):
    """A JWKS fetch failure returns False and logs a WARNING."""
    with patch("server._jwks_client") as mock_client:
        mock_client.get_signing_key_from_jwt.side_effect = Exception("network error")
        with caplog.at_level(logging.WARNING, logger="server"):
            result = _run(server._verify_aithne_agent_jwt("anytoken"))

    assert result is False
    assert any("JWKS key fetch failed" in r.message for r in caplog.records), (
        "Expected a WARNING log mentioning 'JWKS key fetch failed'"
    )


def test_verify_aithne_agent_jwt_jwks_failure_log_includes_exception(caplog):
    """The JWKS failure log message includes the underlying exception detail."""
    with patch("server._jwks_client") as mock_client:
        mock_client.get_signing_key_from_jwt.side_effect = Exception("DNS resolution failed")
        with caplog.at_level(logging.WARNING, logger="server"):
            _run(server._verify_aithne_agent_jwt("anytoken"))

    log_text = " ".join(r.message for r in caplog.records)
    assert "DNS resolution failed" in log_text, (
        "Expected the exception message to appear in the WARNING log"
    )


def test_verify_aithne_agent_jwt_token_validation_failure_returns_false_silently(caplog):
    """A bad token (wrong signature, expired, etc.) returns False with no WARNING logged."""
    mock_signing_key = MagicMock()
    mock_signing_key.key = "fake-key"

    with patch("server._jwks_client") as mock_client:
        mock_client.get_signing_key_from_jwt.return_value = mock_signing_key
        with patch("server.jwt.decode") as mock_decode:
            mock_decode.side_effect = Exception("Signature verification failed")
            with caplog.at_level(logging.WARNING, logger="server"):
                result = _run(server._verify_aithne_agent_jwt("badtoken"))

    assert result is False
    warning_records = [r for r in caplog.records if r.levelno >= logging.WARNING]
    assert len(warning_records) == 0, (
        f"Expected no WARNING logs for token validation failure, got: {warning_records}"
    )


def test_verify_aithne_agent_jwt_kid_control_chars_stripped_from_log(caplog):
    """Control characters injected via the kid claim are stripped from the WARNING log.

    Regression test for #642: a crafted token with a newline (or other control
    character) in the kid value must not produce a spurious extra log line.
    """
    injected_kid = "abc\nINFO 2026-01-01 Fake: User authenticated successfully"
    with patch("server._jwks_client") as mock_client:
        mock_client.get_signing_key_from_jwt.side_effect = Exception(
            f"Unable to find a signing key that matches: {injected_kid}"
        )
        with caplog.at_level(logging.WARNING, logger="server"):
            _run(server._verify_aithne_agent_jwt("anytoken"))

    log_text = " ".join(r.message for r in caplog.records)
    # The injected newline must not appear in any log message
    assert "\n" not in log_text, (
        "Newline from attacker-controlled kid survived into log — log injection not mitigated"
    )
    # The benign prefix of the kid value must survive sanitisation
    assert "abc" in log_text


# ---------------------------------------------------------------------------
# BearerAuthMiddleware — CLIENT_KEYS rejection regression
# ---------------------------------------------------------------------------


@pytest.fixture
def client():
    """HTTP test client for the Starlette app."""
    return TestClient(server.app)


def test_mcp_rejects_client_keys_bearer_token(client):
    """A static bearer token (CLIENT_KEYS format) must be rejected with 401.

    Regression guard for #645: if a future merge conflict accidentally
    re-introduces the CLIENT_KEYS fallback that was removed in #644, this
    test will fail, preventing the regression from reaching production.

    A static string like 'some-static-api-key' is not a JWT, so
    _jwks_client.get_signing_key_from_jwt raises immediately at the
    header-decode step (no network call required).
    """
    response = client.get("/", headers={"Authorization": "Bearer some-static-api-key"})
    assert response.status_code == 401
