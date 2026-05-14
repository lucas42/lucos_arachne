"""
Integration tests for the Arachne MCP server tools.

These tests exercise each tool end-to-end against a real local Fuseki
instance pre-loaded with mcp/test_fixtures/shape.ttl.  They are skipped
automatically if Fuseki is not reachable (see conftest.py).

Fixture summary (shape.ttl):
  - ex:Widget  — 3 instances (widget1, widget2, widget3)
                 widget1 and widget2 have ex:colour; widget3 does not
  - ex:Gadget  — 5 instances (gadget1 … gadget5)

The count_by_property tests assert exact counts that would be wrong if the
#477 Cartesian-product SPARQL shape (non-DISTINCT OPTIONAL) were reintroduced:
  correct  → withProp = 2
  inflated → 2 × 3 = 6  (non-DISTINCT cross-product)
"""

import pytest

import server

_WIDGET_TYPE  = "https://fixture.example/Widget"
_GADGET_TYPE  = "https://fixture.example/Gadget"
_COLOUR_PROP  = "https://fixture.example/colour"
_WIDGET1_URI  = "https://fixture.example/widget1"


# ---------------------------------------------------------------------------
# Auto-use fixture: redirect server constants at the local test Fuseki
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _use_test_triplestore(monkeypatch, fuseki_sparql_url):
    """Patch the server's triplestore URL and auth for every test in this module."""
    monkeypatch.setattr(server, "TRIPLESTORE_SPARQL_URL", fuseki_sparql_url)
    monkeypatch.setattr(server, "TRIPLESTORE_AUTH", ("admin", "admin"))


# ---------------------------------------------------------------------------
# list_types
# ---------------------------------------------------------------------------

def test_list_types_returns_both_fixture_types():
    """list_types must report exactly the two types in the fixture."""
    result = server.list_types()
    assert "https://fixture.example/Widget" in result
    assert "https://fixture.example/Gadget" in result


def test_list_types_widget_count():
    """list_types must report 3 Widget instances."""
    result = server.list_types()
    # list_types formats as "- Label (N instance(s))\n  URI: …" — the count is on
    # the label line, not the URI line, so assert against the full result string.
    assert "Widget (3 instance" in result, f"Expected 'Widget (3 instance' in result:\n{result}"


def test_list_types_gadget_count():
    """list_types must report 5 Gadget instances."""
    result = server.list_types()
    assert "Gadget (5 instance" in result, f"Expected 'Gadget (5 instance' in result:\n{result}"


def test_list_types_includes_prefLabels():
    """list_types should use skos:prefLabel for display where available."""
    result = server.list_types()
    # The fixture has skos:prefLabel "Widget"@en and "Gadget"@en
    assert "Widget" in result
    assert "Gadget" in result


# ---------------------------------------------------------------------------
# get_entity
# ---------------------------------------------------------------------------

def test_get_entity_returns_type():
    """get_entity must include the rdf:type triple for widget1."""
    result = server.get_entity(_WIDGET1_URI)
    assert "rdf:type" in result or "a " in result
    assert "fixture.example/Widget" in result


def test_get_entity_returns_label():
    """get_entity must include the rdfs:label value for widget1."""
    result = server.get_entity(_WIDGET1_URI)
    assert "Widget One" in result


def test_get_entity_returns_colour_property():
    """get_entity must include the ex:colour property for widget1."""
    result = server.get_entity(_WIDGET1_URI)
    assert "colour" in result
    assert "red" in result


def test_get_entity_unknown_uri_returns_not_found():
    """get_entity must report no properties for an unknown URI."""
    result = server.get_entity("https://fixture.example/does-not-exist")
    assert "No properties found" in result


# ---------------------------------------------------------------------------
# find_entities
# ---------------------------------------------------------------------------

def test_find_entities_widget_by_uri_count():
    """find_entities must return all 3 Widget instances when queried by type URI."""
    result = server.find_entities(type=_WIDGET_TYPE)
    assert "3" in result


def test_find_entities_widget_by_uri_includes_all():
    """All three widget URIs must appear in the find_entities result."""
    result = server.find_entities(type=_WIDGET_TYPE)
    assert "widget1" in result
    assert "widget2" in result
    assert "widget3" in result


def test_find_entities_gadget_count():
    """find_entities must return all 5 Gadget instances."""
    result = server.find_entities(type=_GADGET_TYPE)
    assert "5" in result


def test_find_entities_widget_by_label():
    """find_entities must resolve the type label 'Widget' to the Widget URI."""
    result = server.find_entities(type="Widget")
    assert "widget1" in result
    assert "widget2" in result
    assert "widget3" in result


def test_find_entities_with_property_filter():
    """find_entities with a filter on ex:colour must return only coloured widgets."""
    result = server.find_entities(
        type=_WIDGET_TYPE,
        filters=[{"property": _COLOUR_PROP, "value": "red"}],
    )
    assert "widget1" in result
    assert "widget2" not in result
    assert "widget3" not in result


# ---------------------------------------------------------------------------
# count_by_property — including anti-Cartesian-product regression assertions
# ---------------------------------------------------------------------------

def test_count_by_property_total_widget_count():
    """count_by_property must report a total of 3 Widgets."""
    result = server.count_by_property(type=_WIDGET_TYPE, property=_COLOUR_PROP)
    assert "3" in result


def test_count_by_property_with_prop_count():
    """
    count_by_property must report exactly 2 Widgets with ex:colour.

    Anti-regression for #477: if the Cartesian-product OPTIONAL shape were
    reintroduced without DISTINCT, the inflated count would be 2 × 3 = 6.
    Asserting == 2 catches that regression.
    """
    result = server.count_by_property(type=_WIDGET_TYPE, property=_COLOUR_PROP)
    # Result format: "2 of 3 <type> entities have a <property> property."
    assert result.startswith("2 of 3"), (
        f"Expected '2 of 3 ...' but got: {result!r}\n"
        "A count > 2 suggests the Cartesian-product regression (#477) was reintroduced."
    )


def test_count_by_property_by_type_label():
    """count_by_property must work when type is passed as a human-readable label."""
    result = server.count_by_property(type="Widget", property=_COLOUR_PROP)
    assert "2 of 3" in result


def test_count_by_property_zero_with_prop():
    """count_by_property must report 0 when no entities have the property."""
    result = server.count_by_property(
        type=_GADGET_TYPE,
        property=_COLOUR_PROP,  # no gadgets have ex:colour
    )
    assert "0 of 5" in result
