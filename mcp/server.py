"""
Arachne MCP Server

Exposes the lucos_arachne knowledge graph via the Model Context Protocol.
"""

import asyncio
import logging
import os
import re
import threading
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import jwt
import requests
import uvicorn
from jwt import PyJWKClient, PyJWKClientError

# PyJWKClientNetworkError was added in PyJWT 2.4.0; fall back to the base class
# so the except clause still catches network failures on older versions.
try:
    from jwt import PyJWKClientNetworkError
except ImportError:
    PyJWKClientNetworkError = PyJWKClientError
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Mount, Route

from probe_parameters import (
    PROBE_BUDGET_S,
    PROBE_INTERVAL_S,
    PROBE_STALE_THRESHOLD_S,
    PROBE_TOOLS,
)

logger = logging.getLogger(__name__)

RESOURCES_DIR = Path(__file__).parent / "resources"

# Maps lucOS service names to the graph URIs they contribute to the triplestore.
# Mirrors ingestor/triplestore.py — keep in sync if either changes.
SYSTEMS_TO_GRAPHS = {
    "lucos_eolas": "https://eolas.l42.eu/metadata/all/data/",
    "lucos_contacts": "https://contacts.l42.eu/people/all",
    "lucos_media_metadata_api": "https://media-api.l42.eu/v2/export",
    "foaf": "https://www.w3.org/archive/xmlns.com/foaf/0.1/ontology",
    "time": "https://www.w3.org/2006/time",
    "dbpedia_meanOfTransportation": "https://dbpedia.org/ontology/MeanOfTransportation",
    "skos": "http://www.w3.org/2004/02/skos/core",
    "owl": "https://www.w3.org/2002/07/owl",
    "dc": "http://purl.org/dc/terms/",
    "dcam": "http://purl.org/dc/dcam/",
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns",
    "rdfs": "http://www.w3.org/2000/01/rdf-schema",
    "loc_iso639-5": "http://id.loc.gov/vocabulary/iso639-5/iso639-5_Language",
    "loc_mads": "https://id.loc.gov/ontologies/madsrdf/v1.rdf",
}

# The MCP server must bind on all interfaces so nginx can proxy to it.
# FastMCP defaults to 127.0.0.1 (localhost-only), which breaks container networking.
PORT = int(os.environ.get("PORT", "8200"))

TYPESENSE_URL = "http://search:8108"
# KEY_LUCOS_ARACHNE is registered in the search container with full ["*"] permissions
TYPESENSE_API_KEY = os.environ.get("KEY_LUCOS_ARACHNE", "")

TRIPLESTORE_SPARQL_URL = "http://triplestore:3030/arachne/sparql"
TRIPLESTORE_AUTH = ("lucos_arachne", os.environ.get("KEY_LUCOS_ARACHNE", ""))

# Per-tool query budgets — all strictly below Fuseki's 30 s service-loop guard.
_BUDGET_RESOLVE_S = 5   # cheap LIMIT-1 resolver queries
_BUDGET_QUERY_S = 10    # main per-tool queries
_BUDGET_HEALTH_S = 3    # trivial health probe

# FastMCP's DNS rebinding protection defaults to localhost-only allowed hosts.
# Add the service's public hostname so external clients can reach /mcp.
_allowed_hosts = ["127.0.0.1:*", "localhost:*", "[::1]:*"]
_app_origin = os.environ.get("APP_ORIGIN", "")
if _app_origin:
    _hostname = urlparse(_app_origin).hostname
    if _hostname:
        _allowed_hosts.append(_hostname)

mcp = FastMCP(
    name="lucos_arachne",
    instructions=(
        "This server provides structured access to the lucos_arachne knowledge graph. "
        "It queries the Fuseki triplestore (OWL-inferred arachne endpoint) and the "
        "Typesense full-text search index. Use the available tools to explore entities, "
        "types, and relationships in the knowledge graph."
    ),
    stateless_http=True,
    transport_security=TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=_allowed_hosts,
    ),
)


# ---------------------------------------------------------------------------
# Triplestore error types and SPARQL helper
# ---------------------------------------------------------------------------

class _TriplestoreTimeout(Exception):
    """Raised by _run_sparql when a SPARQL query exceeds its per-tool budget."""
    def __init__(self, budget_secs: float):
        self.budget_secs = budget_secs


class _TriplestoreUnavailable(Exception):
    """Raised by _run_sparql when the triplestore returns a 503 response."""
    def __init__(self, likely_outage: bool):
        self.likely_outage = likely_outage


def _check_fuseki_health() -> bool:
    """
    Probe Fuseki with a trivial ASK query.

    Returns True if Fuseki is reachable and returns a 2xx/4xx response,
    False on any connection error, timeout, or 5xx response.
    """
    try:
        resp = requests.get(
            TRIPLESTORE_SPARQL_URL,
            params={"query": "ASK {}", "format": "json"},
            auth=TRIPLESTORE_AUTH,
            timeout=_BUDGET_HEALTH_S,
        )
        return resp.status_code < 500
    except Exception:
        return False


def _run_sparql(query: str, timeout: float) -> dict:
    """
    Execute a SPARQL query against the triplestore and return the parsed JSON.

    Args:
        query: The SPARQL query string.
        timeout: Maximum seconds to wait for a response.

    Raises:
        _TriplestoreTimeout: if the request exceeds *timeout*.
        _TriplestoreUnavailable: if the triplestore returns 503.
    """
    try:
        response = requests.get(
            TRIPLESTORE_SPARQL_URL,
            params={"query": query, "format": "json"},
            auth=TRIPLESTORE_AUTH,
            timeout=timeout,
        )
    except requests.exceptions.Timeout:
        raise _TriplestoreTimeout(timeout)

    if response.status_code == 503:
        likely_outage = not _check_fuseki_health()
        raise _TriplestoreUnavailable(likely_outage)

    response.raise_for_status()
    return response.json()


def _sparql_timeout_error(tool_name: str, budget_secs: float) -> str:
    """Return a structured user-facing message for a SPARQL query timeout."""
    return (
        f"The {tool_name} query timed out after {budget_secs:.0f} s. "
        f"This limit is set by the MCP tool to stay within Fuseki's service guard. "
        f"Try a more specific query or reduce the limit."
    )


def _sparql_503_error(tool_name: str, likely_outage: bool) -> str:
    """Return a structured user-facing message for a SPARQL 503 response."""
    if likely_outage:
        return (
            f"The {tool_name} query returned a service error (503) and "
            f"a health probe suggests Fuseki may be unavailable. "
            f"Please try again later."
        )
    return (
        f"The {tool_name} query returned a service error (503). "
        f"Fuseki appears reachable, so this is likely a query-level issue "
        f"(e.g. too complex for the current dataset size). "
        f"Try a more specific query or reduce the limit."
    )


@mcp.tool()
def search(query: str, filter_by: Optional[str] = None, limit: int = 10) -> str:
    """
    Search the lucos_arachne knowledge graph for entities matching a query.

    Returns a list of matching entities with their type, label, and URI.

    Args:
        query: The search query string.
        filter_by: Optional Typesense filter expression (e.g. 'type:=Track').
        limit: Maximum number of results to return (default 10).
    """
    params = {
        "q": query,
        "query_by": "pref_label,labels,description,lyrics",
        "per_page": limit,
    }
    if filter_by:
        params["filter_by"] = filter_by

    try:
        response = requests.get(
            f"{TYPESENSE_URL}/collections/items/documents/search",
            params=params,
            headers={"X-TYPESENSE-API-KEY": TYPESENSE_API_KEY},
            timeout=10,
        )
    except requests.exceptions.Timeout:
        return (
            "The search query timed out. "
            "This may indicate the search index is under load — try again in a moment."
        )
    response.raise_for_status()
    data = response.json()

    hits = data.get("hits", [])
    if not hits:
        return f"No results found for '{query}'."

    lines = [f"Found {data.get('found', len(hits))} result(s) for '{query}':\n"]
    for hit in hits:
        doc = hit["document"]
        label = doc.get("pref_label") or "(no label)"
        entity_type = doc.get("type") or "(unknown type)"
        uri = doc.get("id", "")
        lines.append(f"- [{entity_type}] {label}\n  URI: {uri}")

    return "\n".join(lines)


KNOWN_PREFIXES = {
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#": "rdf:",
    "http://www.w3.org/2000/01/rdf-schema#": "rdfs:",
    "http://www.w3.org/2002/07/owl#": "owl:",
    "http://www.w3.org/2004/02/skos/core#": "skos:",
    "http://xmlns.com/foaf/0.1/": "foaf:",
    "http://schema.org/": "schema:",
    "http://purl.org/dc/terms/": "dcterms:",
    "http://purl.org/dc/elements/1.1/": "dc:",
    "http://www.w3.org/ns/prov#": "prov:",
}


def shorten_uri(uri: str) -> str:
    """Return a prefixed URI if a known prefix matches, otherwise return the full URI."""
    for ns, prefix in KNOWN_PREFIXES.items():
        if uri.startswith(ns):
            return prefix + uri[len(ns):]
    return uri


def _format_sparql_object(obj: dict) -> str:
    """Format a SPARQL result binding object (uri or literal) as a display string."""
    if obj["type"] == "uri":
        return f"<{shorten_uri(obj['value'])}>"
    elif obj["type"] == "literal":
        lang = obj.get("xml:lang")
        datatype = obj.get("datatype")
        raw = obj["value"]
        if lang:
            return f'"{raw}"@{lang}'
        elif datatype:
            return f'"{raw}"^^{shorten_uri(datatype)}'
        else:
            return f'"{raw}"'
    else:
        return obj["value"]


@mcp.tool()
def get_entity(uri: str) -> str:
    """
    Return all properties and values for a given entity URI.

    Uses the OWL-inferred `arachne` endpoint, so the result includes both directly
    asserted triples and inferred ones. For example, if a group has a `foaf:member`
    assertion pointing to a person, Fuseki's OWL reasoner infers the inverse
    `foaf:memberOf` on the person. On the raw `raw_arachne` endpoint that inverse
    triple would be absent even though the relationship exists in the data.

    Queries for all triples where the given URI is the subject. Properties are shown with
    human-readable prefixed names where possible (e.g. foaf:name, skos:prefLabel).
    Blank node property values are resolved one level deep and displayed inline.

    Args:
        uri: The full URI of the entity to retrieve (e.g. https://arachne.l42.eu/track/123).
    """
    if ">" in uri or any(c.isspace() for c in uri):
        return f"Invalid URI: <{uri}> contains characters not permitted in a SPARQL IRI."

    query = f"""
    SELECT ?p ?o ?bp ?bo WHERE {{
        <{uri}> ?p ?o .
        OPTIONAL {{
            FILTER(isBlank(?o))
            ?o ?bp ?bo .
        }}
    }}
    ORDER BY ?p ?o ?bp
    """

    try:
        data = _run_sparql(query, _BUDGET_QUERY_S)
    except _TriplestoreTimeout as e:
        return _sparql_timeout_error("get_entity", e.budget_secs)
    except _TriplestoreUnavailable as e:
        return _sparql_503_error("get_entity", e.likely_outage)

    bindings = data.get("results", {}).get("bindings", [])
    if not bindings:
        return f"No properties found for entity <{uri}>. The URI may not exist in the triplestore."

    # Group values by property.
    # Values are either strings (uri/literal) or dicts (blank nodes with sub-properties).
    properties: dict[str, list] = {}
    bnode_sub_props: dict[str, dict[str, list[str]]] = {}  # bnode_id -> sub_prop -> [values]

    for binding in bindings:
        prop_uri = binding["p"]["value"]
        prop_label = shorten_uri(prop_uri)

        obj = binding["o"]
        if obj["type"] == "bnode":
            bnode_id = obj["value"]
            if bnode_id not in bnode_sub_props:
                # First occurrence — create a dict to hold sub-properties and add it to
                # the property list. The same dict object is mutated as further rows arrive.
                bnode_dict: dict[str, list[str]] = {}
                bnode_sub_props[bnode_id] = bnode_dict
                properties.setdefault(prop_label, []).append(bnode_dict)

            if "bp" in binding and "bo" in binding:
                sub_prop_label = shorten_uri(binding["bp"]["value"])
                sub_value = _format_sparql_object(binding["bo"])
                bnode_sub_props[bnode_id].setdefault(sub_prop_label, []).append(sub_value)
        else:
            properties.setdefault(prop_label, []).append(_format_sparql_object(obj))

    lines = [f"Entity: <{uri}>\n"]
    for prop, values in sorted(properties.items()):
        if len(values) == 1:
            val = values[0]
            if isinstance(val, dict):
                lines.append(f"  {prop}:")
                for sub_prop, sub_vals in sorted(val.items()):
                    if len(sub_vals) == 1:
                        lines.append(f"    {sub_prop}: {sub_vals[0]}")
                    else:
                        lines.append(f"    {sub_prop}:")
                        for sv in sub_vals:
                            lines.append(f"      - {sv}")
            else:
                lines.append(f"  {prop}: {val}")
        else:
            lines.append(f"  {prop}:")
            for val in values:
                if isinstance(val, dict):
                    lines.append(f"    -")
                    for sub_prop, sub_vals in sorted(val.items()):
                        if len(sub_vals) == 1:
                            lines.append(f"      {sub_prop}: {sub_vals[0]}")
                        else:
                            lines.append(f"      {sub_prop}:")
                            for sv in sub_vals:
                                lines.append(f"        - {sv}")
                else:
                    lines.append(f"    - {val}")

    return "\n".join(lines)


@mcp.tool()
def list_types() -> str:
    """
    List all RDF types in the triplestore with instance counts.

    Uses the OWL-inferred `arachne` endpoint so that instance counts reflect
    class-hierarchy closure. An entity declared as a subtype (e.g. `schema:MusicAlbum`)
    is also counted under its supertypes (e.g. `schema:CreativeWork`) if the ontology
    defines the subclass relationship. On the raw `raw_arachne` endpoint, subclass
    membership is not expanded and supertype counts would under-report.

    Returns a list of types sorted by instance count (descending), with
    human-readable labels where available (skos:prefLabel or rdfs:label),
    falling back to the URI.
    """
    query = """
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT ?type (COUNT(?s) AS ?count) (SAMPLE(?prefLabel) AS ?label) WHERE {
        ?s a ?type .
        OPTIONAL {
            { ?type skos:prefLabel ?prefLabel }
            UNION
            { ?type rdfs:label ?prefLabel }
        }
    }
    GROUP BY ?type
    ORDER BY DESC(?count)
    """

    try:
        data = _run_sparql(query, _BUDGET_QUERY_S)
    except _TriplestoreTimeout as e:
        return _sparql_timeout_error("list_types", e.budget_secs)
    except _TriplestoreUnavailable as e:
        return _sparql_503_error("list_types", e.likely_outage)

    bindings = data.get("results", {}).get("bindings", [])
    if not bindings:
        return "No types found in the triplestore."

    lines = [f"Found {len(bindings)} type(s) in the triplestore:\n"]
    for binding in bindings:
        uri = binding["type"]["value"]
        count = binding["count"]["value"]
        label = binding.get("label", {}).get("value") or uri
        lines.append(f"- {label} ({count} instance(s))\n  URI: {uri}")

    return "\n".join(lines)


def _is_uri(value: str) -> bool:
    """Return True if the value looks like an absolute URI."""
    return value.startswith("http://") or value.startswith("https://")


def _validate_uri_for_sparql(uri: str) -> Optional[str]:
    """
    Validate that a URI is safe to embed in a SPARQL IRI position.
    Returns an error message if invalid, or None if valid.
    """
    if ">" in uri or any(c.isspace() for c in uri):
        return f"Invalid URI: <{uri}> contains characters not permitted in a SPARQL IRI."
    return None


def _validate_label_for_sparql(label: str) -> Optional[str]:
    """
    Validate that a label is safe to embed in a SPARQL string literal position.
    Returns an error message if invalid, or None if valid.

    Double-quote and backslash characters can break out of a SPARQL string literal
    and are therefore rejected.
    """
    if '"' in label or "\\" in label:
        return f"Invalid label: '{label}' contains characters not permitted in a SPARQL string literal."
    return None


def _resolve_type_uri(type_name: str) -> tuple[Optional[str], Optional[str]]:
    """
    Resolve a human-readable type name (or URI) to a triplestore type URI.

    Returns (uri, error_message). On success, error_message is None.
    On failure, uri is None.
    """
    if _is_uri(type_name):
        err = _validate_uri_for_sparql(type_name)
        if err:
            return None, err
        return type_name, None

    # Validate the label is safe to interpolate into a SPARQL string literal
    err = _validate_label_for_sparql(type_name)
    if err:
        return None, err

    # Query triplestore for a type with a matching label
    query = """
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT DISTINCT ?type WHERE {
        ?s a ?type .
        OPTIONAL {
            { ?type skos:prefLabel ?label }
            UNION
            { ?type rdfs:label ?label }
        }
        BIND(COALESCE(?label, "") AS ?resolvedLabel)
        FILTER(
            LCASE(STR(?resolvedLabel)) = LCASE("%s")
            || LCASE(STRAFTER(STR(?type), "#")) = LCASE("%s")
            || LCASE(STRAFTER(STR(?type), "/")) = LCASE("%s")
        )
    }
    LIMIT 1
    """ % (type_name, type_name, type_name)

    data = _run_sparql(query, _BUDGET_RESOLVE_S)

    bindings = data.get("results", {}).get("bindings", [])
    if not bindings:
        return None, f"No type found matching '{type_name}'. Use list_types() to see available types."

    return bindings[0]["type"]["value"], None


def _resolve_property_uri(prop_name: str) -> tuple[Optional[str], Optional[str]]:
    """
    Resolve a human-readable property name (or URI) to a property URI.

    Returns (uri, error_message). On success, error_message is None.
    On failure, uri is None.
    """
    if _is_uri(prop_name):
        err = _validate_uri_for_sparql(prop_name)
        if err:
            return None, err
        return prop_name, None

    # Validate the label is safe to interpolate into a SPARQL string literal
    err = _validate_label_for_sparql(prop_name)
    if err:
        return None, err

    # Query triplestore for a property URI matching the name
    query = """
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT DISTINCT ?prop WHERE {
        ?s ?prop ?o .
        FILTER(
            LCASE(STRAFTER(STR(?prop), "#")) = LCASE("%s")
            || LCASE(STRAFTER(STR(?prop), "/")) = LCASE("%s")
        )
    }
    LIMIT 1
    """ % (prop_name, prop_name)

    data = _run_sparql(query, _BUDGET_RESOLVE_S)

    bindings = data.get("results", {}).get("bindings", [])
    if not bindings:
        return None, f"No property found matching '{prop_name}'."

    return bindings[0]["prop"]["value"], None


@mcp.tool()
def find_entities(
    type: str,
    limit: int = 20,
    properties: Optional[list[str]] = None,
    filters: Optional[list[dict]] = None,
) -> str:
    """
    Find entities of a given type in the knowledge graph, with optional property values
    and filters.

    Uses the OWL-inferred `arachne` endpoint so that type membership includes subclass
    instances: querying for `schema:CreativeWork` returns `schema:MusicAlbum` entities
    too, because the OWL reasoner expands the subclass hierarchy. On the raw
    `raw_arachne` endpoint, only entities with a direct `rdf:type` assertion for the
    requested type would match — subclass instances would be silently excluded.

    Returns a list of matching entities with their URI, label, and any requested
    property values.

    Args:
        type: The type of entity to find — either a human-readable name (e.g. "Person",
              "Track") or a full URI. Use list_types() to see available types.
        limit: Maximum number of results to return (default 20).
        properties: Optional list of property names or URIs to include in results
                    (e.g. ["birthday", "foaf:name"]). If omitted, only the label
                    and URI are returned.
        filters: Optional list of {"property": ..., "value": ...} dicts to constrain
                 results. Property names are resolved the same way as the `properties`
                 parameter. Values that look like URIs (start with http/https) are
                 treated as URI values; everything else is treated as a string literal.
                 Multiple filters are AND-ed together.
                 Example: [{"property": "containedIn", "value": "https://example.org/usa"}]
    """
    try:
        # Resolve the type to a URI
        type_uri, type_err = _resolve_type_uri(type)
        if type_err:
            return type_err

        # Resolve requested property names to URIs
        resolved_props: list[tuple[str, str]] = []  # (prop_name, prop_uri)
        if properties:
            for prop_name in properties:
                prop_uri, prop_err = _resolve_property_uri(prop_name)
                if prop_err:
                    return f"Could not resolve property '{prop_name}': {prop_err}"
                resolved_props.append((prop_name, prop_uri))

        # Resolve and validate filters
        filter_clauses = ""
        if filters:
            for f in filters:
                filter_prop = f.get("property", "")
                filter_value = f.get("value", "")

                filter_prop_uri, filter_prop_err = _resolve_property_uri(filter_prop)
                if filter_prop_err:
                    return f"Could not resolve filter property '{filter_prop}': {filter_prop_err}"

                if _is_uri(filter_value):
                    err = _validate_uri_for_sparql(filter_value)
                    if err:
                        return f"Invalid filter value: {err}"
                    sparql_value = f"<{filter_value}>"
                else:
                    err = _validate_label_for_sparql(filter_value)
                    if err:
                        return f"Invalid filter value: {err}"
                    sparql_value = f'"{filter_value}"'

                filter_clauses += f"\n                ?s <{filter_prop_uri}> {sparql_value} ."

        # Build the SPARQL query
        optional_clauses = ""
        select_vars = "?s ?label"
        for i, (_, prop_uri) in enumerate(resolved_props):
            var = f"?val{i}"
            select_vars += f" {var}"
            optional_clauses += f"\n    OPTIONAL {{ ?s <{prop_uri}> {var} . }}"

        query = f"""
        PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

        SELECT {select_vars} WHERE {{
            {{
                SELECT DISTINCT ?s WHERE {{
                    ?s a <{type_uri}> .{filter_clauses}
                }}
                LIMIT {limit}
            }}
            OPTIONAL {{
                {{ ?s skos:prefLabel ?label }}
                UNION
                {{ ?s rdfs:label ?label }}
            }}{optional_clauses}
        }}
        ORDER BY ?label ?s
        """

        data = _run_sparql(query, _BUDGET_QUERY_S)
    except _TriplestoreTimeout as e:
        return _sparql_timeout_error("find_entities", e.budget_secs)
    except _TriplestoreUnavailable as e:
        return _sparql_503_error("find_entities", e.likely_outage)

    bindings = data.get("results", {}).get("bindings", [])
    if not bindings:
        return f"No entities of type '{type}' found in the triplestore."

    # De-duplicate by subject URI, merging property values across rows
    # (multiple rows can appear when a property has multiple values)
    entities: dict[str, dict] = {}
    for binding in bindings:
        subject_uri = binding["s"]["value"]

        if subject_uri not in entities:
            label_binding = binding.get("label")
            label = label_binding["value"] if label_binding else "(no label)"
            entities[subject_uri] = {"label": label, "props": {}}

        for i, (prop_name, _) in enumerate(resolved_props):
            var = f"val{i}"
            val_binding = binding.get(var)
            if val_binding:
                raw = val_binding["value"]
                if val_binding["type"] == "uri":
                    raw = f"<{shorten_uri(raw)}>"
                entities[subject_uri]["props"].setdefault(prop_name, [])
                if raw not in entities[subject_uri]["props"][prop_name]:
                    entities[subject_uri]["props"][prop_name].append(raw)

    lines = [f"Found {len(entities)} {type} entity/entities (showing up to {limit}):\n"]
    for uri, entity in entities.items():
        label = entity["label"]
        lines.append(f"- {label}\n  URI: {uri}")
        for prop_name, values in entity["props"].items():
            if len(values) == 1:
                lines.append(f"  {prop_name}: {values[0]}")
            else:
                lines.append(f"  {prop_name}:")
                for v in values:
                    lines.append(f"    - {v}")

    return "\n".join(lines)


@mcp.tool()
def count_by_property(type: str, property: str) -> str:
    """
    Count how many entities of a given type have a specific property.

    Uses the OWL-inferred `arachne` endpoint. The type check (`?s a <type>`)
    benefits from OWL closure: subclass instances are included in the total,
    so querying for `schema:CreativeWork` counts `schema:MusicAlbum` instances
    too. The property check (`?s <prop> ?val`) gains little from inference since
    most data properties in this graph are directly asserted — but keeping both
    queries on the same endpoint avoids unnecessary complexity.

    Returns the total number of entities of the type, and how many of those
    have the specified property set.

    Args:
        type: The type of entity to count — either a human-readable name (e.g. "Track")
              or a full URI. Use list_types() to see available types.
        property: The property name or URI to check (e.g. "lyrics",
                  "https://schema.org/lyrics").
    """
    def _run_count(query: str, binding_name: str):
        data = _run_sparql(query, _BUDGET_QUERY_S)
        bindings = data.get("results", {}).get("bindings", [])
        if not bindings:
            return None
        return int(bindings[0].get(binding_name, {}).get("value", 0))

    try:
        # Resolve the type to a URI
        type_uri, type_err = _resolve_type_uri(type)
        if type_err:
            return type_err

        # Resolve the property to a URI
        prop_uri, prop_err = _resolve_property_uri(property)
        if prop_err:
            return prop_err

        # Two separate queries to keep both shapes simple and cheap.
        #
        # A single combined query with an OPTIONAL block is tempting but a trap:
        # if the OPTIONAL doesn't share a bound variable with the outer pattern,
        # Fuseki materialises a Cartesian product (instances × instances-with-prop)
        # and the 30 s service guard fires long before the query returns. See #477.
        total_query = f"""
        SELECT (COUNT(DISTINCT ?s) AS ?total)
        WHERE {{
            ?s a <{type_uri}> .
        }}
        """

        with_prop_query = f"""
        SELECT (COUNT(DISTINCT ?s) AS ?withProp)
        WHERE {{
            ?s a <{type_uri}> ;
               <{prop_uri}> ?val .
        }}
        """

        total = _run_count(total_query, "total")
        if total is None:
            return f"Could not retrieve counts for type '{type}' and property '{property}'."

        with_prop = _run_count(with_prop_query, "withProp")
        if with_prop is None:
            return f"Could not retrieve counts for type '{type}' and property '{property}'."
    except _TriplestoreTimeout as e:
        return _sparql_timeout_error("count_by_property", e.budget_secs)
    except _TriplestoreUnavailable as e:
        return _sparql_503_error("count_by_property", e.likely_outage)

    return f"{with_prop:,} of {total:,} {type} entities have a {property} property."


@mcp.resource(
    "resource://arachne/ontology",
    name="ontology",
    title="Arachne Ontology Summary",
    description=(
        "A human-readable description of the types, properties, and namespaces "
        "used in the arachne triplestore. Read this before making tool calls to "
        "understand what kinds of data are available."
    ),
    mime_type="text/markdown",
)
def get_ontology() -> str:
    """Return the static ontology summary as a Markdown document."""
    return (RESOURCES_DIR / "ontology.md").read_text(encoding="utf-8")


@mcp.resource(
    "resource://arachne/data-sources",
    name="data-sources",
    title="Arachne Data Sources",
    description=(
        "The lucOS services that contribute data to the arachne triplestore, "
        "and the graph URIs they map to. Use this to understand where data comes from."
    ),
    mime_type="text/markdown",
)
def get_data_sources() -> str:
    """Return the systems-to-graphs mapping as a Markdown table."""
    lines = [
        "# Arachne Data Sources",
        "",
        "The following lucOS services (and external ontologies) contribute data "
        "to the arachne triplestore. Each source is loaded into a named graph.",
        "",
        "| Source | Graph URI |",
        "|--------|-----------|",
    ]
    for system, graph_uri in SYSTEMS_TO_GRAPHS.items():
        lines.append(f"| `{system}` | `{graph_uri}` |")
    return "\n".join(lines)


_AITHNE_ORIGIN = os.environ.get("AITHNE_ORIGIN", "https://aithne.l42.eu")
_AITHNE_JWKS_URL = f"{_AITHNE_ORIGIN}/.well-known/jwks.json"
_AITHNE_ISSUER = _AITHNE_ORIGIN
_AITHNE_AUDIENCE = "l42.eu"

class _LKGJWKSClient:
    """PyJWKClient wrapper that serves last-known-good keys on network failure.

    Falls back to the last successfully fetched signing key when a transient
    network error prevents refreshing the JWKS endpoint. Cold-start (no cached
    key yet) fails closed — the token is rejected and the caller treats the
    request as unauthenticated.

    Per local-verification-contract.md §1 ("Serve last-known-good on a failed
    refresh") and lucas42/lucos_aithne#149.
    """

    def __init__(self, uri):
        self._client = PyJWKClient(uri, cache_keys=True, lifespan=300)
        self._last_good_key = None
        self._lock = threading.Lock()

    def get_signing_key_from_jwt(self, token):
        try:
            key = self._client.get_signing_key_from_jwt(token)
            with self._lock:
                self._last_good_key = key
            return key
        except PyJWKClientNetworkError as exc:
            with self._lock:
                fallback = self._last_good_key
            safe_msg = re.sub(r'[\x00-\x1f\x7f]', '', str(exc))
            if fallback is None:
                logger.warning(
                    "JWKS fetch failed at cold start (no cached key — failing closed): %s",
                    safe_msg,
                )
                raise
            logger.warning("JWKS fetch failed (using last-known-good): %s", safe_msg)
            return fallback
        # Any other PyJWKClientError (e.g. kid not found after refresh) propagates.


# Module-level JWKS client shared across all requests. _LKGJWKSClient wraps
# PyJWKClient to serve the last-known-good key when the endpoint is transiently
# unreachable (e.g. during a signing-key rotation coinciding with a brief blip).
_jwks_client = _LKGJWKSClient(_AITHNE_JWKS_URL)


def _set_jwks_client(client):
    """Override the module-level JWKS client. For testing only — do not call in production."""
    global _jwks_client
    _jwks_client = client


def _get_signing_key(token: str):
    """Wrapper around PyJWKClient that sanitises the kid claim in error messages.

    PyJWKClientError includes the attacker-controlled kid value in its message
    (e.g. "Unable to find a signing key that matches: {kid}"). Stripping control
    characters (\\x00–\\x1f and \\x7f) prevents log injection via the WARNING
    emitted by _verify_aithne_agent_jwt when a JWKS lookup fails.
    """
    try:
        return _jwks_client.get_signing_key_from_jwt(token)
    except Exception as e:
        safe_msg = re.sub(r'[\x00-\x1f\x7f]', '', str(e))
        raise type(e)(safe_msg) from None


def _has_arachne_access(scopes: list) -> bool:
    """Return True if the JWT scopes list grants access to arachne.

    ADR-0001 §6: access is granted by named scope, not bare identity. Accepts
    arachne:read for all principals (human and agent alike — /mcp is not
    restricted to agents only; the scope is the gate). Also accepts render-ui
    in the development environment so lucos-ux can snapshot rendered pages
    without a per-service grant.

    os.environ is read on every call (not cached at module load) so that the
    environment can be controlled in tests.
    """
    if "arachne:read" in scopes:
        return True
    if os.environ.get("ENVIRONMENT", "production") == "development" and "render-ui" in scopes:
        return True
    return False


async def _verify_aithne_agent_jwt(token: str) -> bool:
    """Return True if token is a valid aithne-issued JWT with arachne access.

    Validates signature (ES256), issuer, audience, expiry (30 s clock-skew
    tolerance), and requires arachne:read scope (or render-ui in development)
    per ADR-0001 §6: access is granted by named scope, not bare identity.

    JWKS key fetching is run in a thread-pool executor so that a cache miss
    (startup or key rotation) does not block the event loop.
    """
    try:
        signing_key = await asyncio.to_thread(_get_signing_key, token)
    except Exception as e:
        # JWKS fetch or key-lookup failure — infrastructure problem, worth logging
        logger.warning("JWKS key fetch failed: %s", e)
        return False
    try:
        payload = jwt.decode(
            token,
            signing_key.key,
            algorithms=["ES256"],
            issuer=_AITHNE_ISSUER,
            audience=_AITHNE_AUDIENCE,
            leeway=30,  # 30-second clock-skew tolerance per local-verification-contract
        )
        return _has_arachne_access(payload.get("scopes", []))
    except Exception:
        # Token validation failure — expected for bad tokens, no log needed
        return False


class BearerAuthMiddleware(BaseHTTPMiddleware):
    """Validate Authorization: Bearer <token> on all routes except /_info.

    Accepts only aithne-issued agent JWTs carrying arachne:read (per ADR-0001 §6).
    The legacy CLIENT_KEYS fallback was removed in lucos_arachne#640 once all
    consuming agents had been migrated to per-agent aithne principals.
    """

    async def dispatch(self, request: Request, call_next):
        if request.url.path == "/_info":
            return await call_next(request)
        auth = request.headers.get("Authorization", "")
        if auth.startswith("Bearer "):
            token = auth[len("Bearer "):]
            if await _verify_aithne_agent_jwt(token):
                return await call_next(request)
        return Response(
            content="Unauthorized",
            status_code=401,
            headers={"WWW-Authenticate": "Bearer"},
            media_type="text/plain",
        )


# ---------------------------------------------------------------------------
# Production probe runner
# ---------------------------------------------------------------------------
#
# Each MCP tool is exercised periodically against the live production endpoint.
# Results are cached in _probe_cache and surfaced as Tier 2 checks in /_info.
#
# Design constraints (see #503):
#   - /_info must respond within lucos_monitoring's hard 1 s timeout.
#     The probe must therefore run asynchronously; /_info only reads the cache.
#   - The probe enforces a per-tool wall-clock budget (PROBE_BUDGET_S) that is
#     well below Fuseki's 30 s service guard, so scale-drift is caught first.
#   - A timeout in the probe is not a fatal server error — it is recorded in
#     the cache as ok=False.  The MCP server itself stays healthy.
#   - If the probe hasn't produced a fresh result within PROBE_STALE_THRESHOLD_S,
#     the check reports ok=False rather than surfacing a stale ok=True reading.

# Keyed by tool name.  Values: {"ok": bool, "techDetail": str, "timestamp": float}
# Empty until the first probe cycle completes (typically a few seconds after startup).
_probe_cache: dict[str, dict] = {}

# Dispatch table for the probe runner — maps tool name to callable.
# Populated here (after the tool functions are defined above).
_PROBE_TOOL_FUNCS: dict[str, callable] = {
    "search":            search,
    "get_entity":        get_entity,
    "list_types":        list_types,
    "find_entities":     find_entities,
    "count_by_property": count_by_property,
}


def _build_probe_checks() -> dict:
    """
    Build the checks dict for /_info by reading _probe_cache.

    Synchronous and always fast — reads only from an in-memory dict.

    Three states per tool:
      - Not yet probed (server starting up): ok=False, "not yet probed" detail.
      - Stale (probe runner fell behind): ok=False, age reported.
      - Current: relays the cached ok/techDetail verbatim.
    """
    checks: dict[str, dict] = {}
    now = time.monotonic()
    for tool_name, _ in PROBE_TOOLS:
        check_key = f"mcp_{tool_name}"
        if tool_name not in _probe_cache:
            checks[check_key] = {
                "ok": False,
                "techDetail": (
                    f"MCP tool {tool_name} has not been probed yet"
                    f" — server may still be starting up"
                ),
            }
        else:
            entry = _probe_cache[tool_name]
            age = now - entry["timestamp"]
            if age > PROBE_STALE_THRESHOLD_S:
                checks[check_key] = {
                    "ok": False,
                    "techDetail": (
                        f"MCP tool {tool_name} probe result is stale"
                        f" ({age:.0f}s old, threshold {PROBE_STALE_THRESHOLD_S:.0f}s)"
                        f" — the probe runner may have stopped"
                    ),
                }
            else:
                checks[check_key] = {
                    "ok": entry["ok"],
                    "techDetail": entry["techDetail"],
                }
    return checks


async def _run_probe_loop():
    """
    Background task: periodically exercise each MCP tool and cache the result.

    Each tool is called via asyncio.to_thread (the tools are synchronous) and
    wrapped in asyncio.wait_for with PROBE_BUDGET_S.  A stagger delay of
    PROBE_INTERVAL_S / len(PROBE_TOOLS) is inserted between tools so that no
    two tools run concurrently against Fuseki.

    Cancellation (on server shutdown) is handled cleanly via CancelledError.
    """
    stagger_s = PROBE_INTERVAL_S / len(PROBE_TOOLS)

    while True:
        for tool_name, kwargs in PROBE_TOOLS:
            func = _PROBE_TOOL_FUNCS.get(tool_name)
            if func is None:
                # Shouldn't happen — safety guard in case the dispatch table is
                # ever out of sync with PROBE_TOOLS.
                _probe_cache[tool_name] = {
                    "ok": False,
                    "techDetail": (
                        f"MCP tool {tool_name} is not registered in the probe dispatch table"
                    ),
                    "timestamp": time.monotonic(),
                }
                await asyncio.sleep(stagger_s)
                continue

            start = time.monotonic()
            try:
                await asyncio.wait_for(
                    asyncio.to_thread(func, **kwargs),
                    timeout=PROBE_BUDGET_S,
                )
                elapsed = time.monotonic() - start
                _probe_cache[tool_name] = {
                    "ok": True,
                    "techDetail": (
                        f"{tool_name} completed in {elapsed:.2f}s"
                        f" (budget {PROBE_BUDGET_S:.0f}s)"
                    ),
                    "timestamp": time.monotonic(),
                }
            except asyncio.TimeoutError:
                elapsed = time.monotonic() - start
                _probe_cache[tool_name] = {
                    "ok": False,
                    "techDetail": (
                        f"MCP tool {tool_name} exceeded {PROBE_BUDGET_S:.0f}s budget"
                        f" against production data (took {elapsed:.1f}s before timeout)"
                    ),
                    "timestamp": time.monotonic(),
                }
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                _probe_cache[tool_name] = {
                    "ok": False,
                    "techDetail": (
                        f"MCP tool {tool_name} raised an unexpected error: {exc}"
                    ),
                    "timestamp": time.monotonic(),
                }

            await asyncio.sleep(stagger_s)


async def info(request):
    return JSONResponse({
        "system": os.environ.get("SYSTEM", "lucos_arachne"),
        "checks": _build_probe_checks(),
        "metrics": {},
        "ci": {"circle": "gh/lucas42/lucos_arachne"},
        "title": "Arachne MCP",
    })


mcp_asgi_app = mcp.streamable_http_app()


@asynccontextmanager
async def lifespan(app):
    async with mcp_asgi_app.router.lifespan_context(app):
        probe_task = asyncio.create_task(_run_probe_loop())
        try:
            yield
        finally:
            probe_task.cancel()
            try:
                await probe_task
            except asyncio.CancelledError:
                pass


app = Starlette(
    routes=[
        Route("/_info", info),
        Mount("/", app=mcp_asgi_app),
    ],
    lifespan=lifespan,
    middleware=[
        Middleware(BearerAuthMiddleware),
    ],
)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)
