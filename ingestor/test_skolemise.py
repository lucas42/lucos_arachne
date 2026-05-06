"""Tests for skolemise.py — blank-node Skolemisation."""
import logging

import pytest
from rdflib import BNode, Graph, Literal, URIRef
from rdflib.namespace import RDF, XSD

from skolemise import SKOLEM_PREFIX, skolemise_graph

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

EX = "https://example.com/"


def ex(local: str) -> URIRef:
    return URIRef(EX + local)


def _make_festival_graph() -> Graph:
    """
    Build a graph that mimics the lucos_eolas FestivalPeriod blank-node pattern:

        <Festival> <hasPeriod> _:b0 .
        _:b0 <startDate> "2024-01-01" .
        _:b0 <endDate>   "2024-01-07" .
    """
    g = Graph()
    b = BNode()
    g.add((ex("Festival"), ex("hasPeriod"), b))
    g.add((b, ex("startDate"), Literal("2024-01-01", datatype=XSD.date)))
    g.add((b, ex("endDate"), Literal("2024-01-07", datatype=XSD.date)))
    return g


# ---------------------------------------------------------------------------
# No blank nodes — graph is returned unchanged
# ---------------------------------------------------------------------------

def test_no_blank_nodes_returns_identical_triples():
    """A graph with no blank nodes passes through unchanged."""
    g = Graph()
    g.add((ex("s"), ex("p"), ex("o")))
    result = skolemise_graph(g)
    assert set(result) == set(g)


def test_no_blank_nodes_returns_no_skolem_uris():
    """No Skolem URIs are introduced when the input has no blank nodes."""
    g = Graph()
    g.add((ex("s"), ex("p"), Literal("value")))
    result = skolemise_graph(g)
    for s, p, o in result:
        assert not str(s).startswith(SKOLEM_PREFIX)
        assert not str(o).startswith(SKOLEM_PREFIX)


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------

def test_skolemisation_determinism():
    """The same input graph produces the same Skolem URIs on two separate calls."""
    g1 = _make_festival_graph()
    g2 = _make_festival_graph()

    r1 = skolemise_graph(g1)
    r2 = skolemise_graph(g2)

    # Both results must have the same triples
    assert set(r1) == set(r2)

    # All Skolem URIs in both results must be identical
    uris_1 = {str(s) for s, _, _ in r1 if str(s).startswith(SKOLEM_PREFIX)}
    uris_2 = {str(s) for s, _, _ in r2 if str(s).startswith(SKOLEM_PREFIX)}
    assert uris_1 == uris_2
    assert len(uris_1) == 1  # one blank node → one Skolem URI


def test_skolemisation_semantic_equality():
    """
    Two graphs containing the same RDF triples (but with differently-named blank nodes
    due to a fresh parse in rdflib) produce the same Skolem URIs.
    """
    ttl = """
    @prefix ex: <https://example.com/> .
    @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
    ex:Festival ex:hasPeriod [
        ex:startDate "2024-01-01"^^xsd:date ;
        ex:endDate   "2024-01-07"^^xsd:date
    ] .
    """

    g1 = Graph()
    g1.parse(data=ttl, format="turtle")

    g2 = Graph()
    g2.parse(data=ttl, format="turtle")

    r1 = skolemise_graph(g1)
    r2 = skolemise_graph(g2)

    assert set(r1) == set(r2)


# ---------------------------------------------------------------------------
# Structural sensitivity
# ---------------------------------------------------------------------------

def test_skolemisation_structural_sensitivity():
    """
    A change to the blank node's outgoing triples produces a different Skolem URI.
    """
    # Original festival period
    g_orig = _make_festival_graph()
    r_orig = skolemise_graph(g_orig)

    # Modified festival period — end date changed
    g_mod = Graph()
    b = BNode()
    g_mod.add((ex("Festival"), ex("hasPeriod"), b))
    g_mod.add((b, ex("startDate"), Literal("2024-01-01", datatype=XSD.date)))
    g_mod.add((b, ex("endDate"), Literal("2024-01-14", datatype=XSD.date)))  # different
    r_mod = skolemise_graph(g_mod)

    skolem_uris_orig = {str(o) for _, _, o in r_orig if str(o).startswith(SKOLEM_PREFIX)}
    skolem_uris_mod = {str(o) for _, _, o in r_mod if str(o).startswith(SKOLEM_PREFIX)}

    # The Skolem URIs must differ because the structure changed
    assert skolem_uris_orig != skolem_uris_mod


def test_adding_triple_to_bnode_context_changes_skolem_uri():
    """Adding a new triple involving the blank node changes its Skolem URI."""
    g_orig = _make_festival_graph()
    r_orig = skolemise_graph(g_orig)

    # Add a new predicate to the blank node
    g_mod = _make_festival_graph()
    # Retrieve the existing blank node
    bnode = next(o for _, _, o in g_mod.triples((ex("Festival"), ex("hasPeriod"), None)))
    g_mod.add((bnode, ex("label"), Literal("Edinburgh Festival")))
    r_mod = skolemise_graph(g_mod)

    skolem_uris_orig = {str(o) for _, _, o in r_orig if str(o).startswith(SKOLEM_PREFIX)}
    skolem_uris_mod = {str(o) for _, _, o in r_mod if str(o).startswith(SKOLEM_PREFIX)}

    assert skolem_uris_orig != skolem_uris_mod


def test_identical_outgoing_different_parent_gets_different_skolem_uri():
    """
    Two blank nodes in the same graph with identical outgoing structure but
    different parent URIs must receive different Skolem URIs.

    This guards against the hash-collision bug where outgoing-only hashing
    would merge two distinct festival periods that happen to share the same
    dates into a single triplestore node.
    """
    g = Graph()
    b0 = BNode()
    b1 = BNode()

    # Festival1 has period Jan 1–7
    g.add((ex("Festival1"), ex("hasPeriod"), b0))
    g.add((b0, ex("startDate"), Literal("2024-01-01", datatype=XSD.date)))
    g.add((b0, ex("endDate"), Literal("2024-01-07", datatype=XSD.date)))

    # Festival2 has an identical period Jan 1–7 (same dates, different parent)
    g.add((ex("Festival2"), ex("hasPeriod"), b1))
    g.add((b1, ex("startDate"), Literal("2024-01-01", datatype=XSD.date)))
    g.add((b1, ex("endDate"), Literal("2024-01-07", datatype=XSD.date)))

    result = skolemise_graph(g)

    # Both blank nodes must produce Skolem URIs
    skolem_objects = [str(o) for _, _, o in result if str(o).startswith(SKOLEM_PREFIX)]
    skolem_subjects = [str(s) for s, _, _ in result if str(s).startswith(SKOLEM_PREFIX)]
    all_skolem = set(skolem_objects + skolem_subjects)

    assert len(all_skolem) == 2, (
        f"Expected 2 distinct Skolem URIs (one per festival period), got {len(all_skolem)}: {all_skolem}"
    )


# ---------------------------------------------------------------------------
# Nested blank nodes
# ---------------------------------------------------------------------------

def test_nested_blank_nodes_are_skolemised():
    """Blank nodes nested inside other blank nodes are all Skolemised."""
    g = Graph()
    outer = BNode()
    inner = BNode()
    g.add((ex("s"), ex("p"), outer))
    g.add((outer, ex("child"), inner))
    g.add((inner, ex("value"), Literal("leaf")))

    result = skolemise_graph(g)

    for s, _, o in result:
        assert not isinstance(s, BNode), f"Expected no BNode in subject, got {s!r}"
        assert not isinstance(o, BNode), f"Expected no BNode in object, got {o!r}"

    # Both outer and inner blank nodes should have Skolem URIs
    skolem_subjects = {str(s) for s, _, _ in result if str(s).startswith(SKOLEM_PREFIX)}
    assert len(skolem_subjects) == 2


def test_nested_blank_node_hash_is_stable():
    """The same nested bnode structure produces the same Skolem URIs on repeated calls."""
    def _make_nested() -> Graph:
        g = Graph()
        outer = BNode()
        inner = BNode()
        g.add((ex("s"), ex("p"), outer))
        g.add((outer, ex("child"), inner))
        g.add((inner, ex("value"), Literal("leaf")))
        return g

    r1 = skolemise_graph(_make_nested())
    r2 = skolemise_graph(_make_nested())
    assert set(r1) == set(r2)


# ---------------------------------------------------------------------------
# Cycle detection
# ---------------------------------------------------------------------------

def _make_cyclic_graph() -> Graph:
    """
    Construct a blank-node cycle: b0 → b1 → b0.
    """
    g = Graph()
    b0 = BNode("b0")
    b1 = BNode("b1")
    g.add((ex("s"), ex("p"), b0))
    g.add((b0, ex("next"), b1))
    g.add((b1, ex("next"), b0))
    return g


def test_cycle_detection_logs_warning(caplog):
    """Blank-node cycles log a warning."""
    g = _make_cyclic_graph()
    with caplog.at_level(logging.WARNING, logger="skolemise"):
        skolemise_graph(g)
    assert any("cycle" in msg.lower() for msg in caplog.messages)


def test_cycle_detection_does_not_raise():
    """Blank-node cycles are handled gracefully — no exception is raised."""
    g = _make_cyclic_graph()
    result = skolemise_graph(g)  # must not raise
    assert isinstance(result, Graph)


def test_cycle_detection_produces_skolem_uris():
    """Even for cyclic blank nodes, the result contains Skolem URIs (not blank nodes)."""
    g = _make_cyclic_graph()
    result = skolemise_graph(g)
    for s, _, o in result:
        assert not isinstance(s, BNode), f"Expected no BNode, got {s!r}"
        assert not isinstance(o, BNode), f"Expected no BNode, got {o!r}"


def test_cycle_detection_non_deterministic(caplog):
    """
    Cyclic blank nodes produce non-deterministic Skolem URIs — two calls over the
    same cyclic graph yield different URIs (UUID-based, not hash-based).

    This is a probabilistic test: with 128 bits of randomness the probability of a
    false pass is negligible.
    """
    g = _make_cyclic_graph()
    with caplog.at_level(logging.WARNING, logger="skolemise"):
        r1 = skolemise_graph(g)
        r2 = skolemise_graph(g)
    # The triples are structurally the same but the Skolem URIs differ
    # (because they're UUID-based)
    uris_1 = frozenset(str(s) for s, _, _ in r1 if str(s).startswith(SKOLEM_PREFIX))
    uris_2 = frozenset(str(s) for s, _, _ in r2 if str(s).startswith(SKOLEM_PREFIX))
    assert uris_1 != uris_2


# ---------------------------------------------------------------------------
# Triple count preserved
# ---------------------------------------------------------------------------

def test_triple_count_unchanged():
    """Skolemisation never adds or removes triples."""
    g = _make_festival_graph()
    result = skolemise_graph(g)
    assert len(result) == len(g)


def test_triple_count_unchanged_nested():
    g = Graph()
    outer = BNode()
    inner = BNode()
    g.add((ex("s"), ex("p"), outer))
    g.add((outer, ex("child"), inner))
    g.add((inner, ex("value"), Literal("leaf")))
    g.add((ex("s"), ex("q"), Literal("extra")))
    result = skolemise_graph(g)
    assert len(result) == len(g)
