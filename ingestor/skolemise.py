"""
Blank-node Skolemisation for RDF graphs.

Replaces blank nodes with deterministic ``urn:lucos:skolem:<hash>`` URIs using a
tree-shaped approximation:

  - Each blank node's canonical hash is computed as
    ``sha256(sorted([(predicate_uri, n3_string_of_object) for outgoing triples]))``
    where child blank nodes are resolved recursively before hashing.

  - Cycle detection: if the recursion visits the same blank node twice (indicating
    a cycle), a ValueError is raised for that node.  Any blank node whose hash
    computation raises ValueError is assigned a non-deterministic UUID-based Skolem
    URI, and a warning is logged.  This keeps the store consistent but means all
    triples involving cyclic blank nodes will be fully rewritten on every ingestion
    cycle.

Properties of deterministic Skolem URIs (per ADR-0002):
  - Stable across runs for unchanged data: same structural input → same URI.
  - Diff-safe: set-difference over Skolemised triples works without graph-matching.
  - Semantically clean: consumers see ``urn:lucos:skolem:...`` URIs rather than blank
    nodes, which is the RDF spec's recommendation for persisted stores.
"""
import hashlib
import logging
import uuid

from rdflib import BNode, Graph, URIRef

SKOLEM_PREFIX = "urn:lucos:skolem:"

logger = logging.getLogger(__name__)


def _compute_bnode_hash(bnode: BNode, graph: Graph, visited: frozenset) -> str:
    """
    Recursively compute a deterministic hash for *bnode* based on its outgoing
    triples.

    *visited* is the set of blank nodes on the current recursion path — used to
    detect cycles.  Raises ``ValueError`` on a cycle so the caller can fall back
    to a non-deterministic URI.
    """
    if bnode in visited:
        raise ValueError(f"Blank-node cycle detected at {bnode!r}")

    new_visited = visited | {bnode}
    parts: list[tuple[str, str]] = []

    for (_, p, o) in graph.triples((bnode, None, None)):
        pred_str = str(p)
        if isinstance(o, BNode):
            # Recurse — propagates ValueError upward on cycle
            child_hash = _compute_bnode_hash(o, graph, new_visited)
            parts.append((pred_str, child_hash))
        else:
            # n3() gives a canonical string for URIRef and Literal
            parts.append((pred_str, o.n3()))

    parts.sort()
    canonical = repr(parts)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def skolemise_graph(graph: Graph) -> Graph:
    """
    Return a copy of *graph* with all blank nodes replaced by Skolem URIs.

    Blank nodes forming tree-shaped subgraphs receive stable, deterministic
    ``urn:lucos:skolem:<hash>`` URIs.  Blank nodes involved in cycles receive
    non-deterministic (UUID-based) Skolem URIs, and a warning is logged.
    """
    # Collect all blank nodes present in the graph
    bnodes: set[BNode] = set()
    for s, _, o in graph:
        if isinstance(s, BNode):
            bnodes.add(s)
        if isinstance(o, BNode):
            bnodes.add(o)

    if not bnodes:
        return graph

    bnode_to_uri: dict[BNode, URIRef] = {}
    has_cycles = False

    for bnode in bnodes:
        try:
            h = _compute_bnode_hash(bnode, graph, frozenset())
            bnode_to_uri[bnode] = URIRef(SKOLEM_PREFIX + h)
        except ValueError:
            # Cycle detected — use a non-deterministic URI for this bnode
            has_cycles = True
            bnode_to_uri[bnode] = URIRef(SKOLEM_PREFIX + str(uuid.uuid4()))

    if has_cycles:
        logger.warning(
            "Blank-node cycles detected in graph — cyclic blank nodes have been "
            "assigned non-deterministic Skolem URIs.  These triples will be fully "
            "rewritten on every ingestion cycle."
        )

    # Build a new graph with Skolem URIs substituted in place of blank nodes
    new_graph = Graph()
    for s, p, o in graph:
        new_s = bnode_to_uri.get(s, s)
        new_o = bnode_to_uri.get(o, o)
        new_graph.add((new_s, p, new_o))

    return new_graph
