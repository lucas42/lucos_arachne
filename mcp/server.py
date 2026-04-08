"""
Arachne MCP Server

Exposes the lucos_arachne knowledge graph via the Model Context Protocol.
"""

import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

import requests
import uvicorn
from mcp.server.fastmcp import FastMCP
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Mount, Route

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
TRIPLESTORE_AUTH = ("admin", os.environ.get("KEY_LUCOS_ARACHNE", ""))

mcp = FastMCP(
    name="lucos_arachne",
    instructions=(
        "This server provides structured access to the lucos_arachne knowledge graph. "
        "It queries the Fuseki triplestore (OWL-inferred arachne endpoint) and the "
        "Typesense full-text search index. Use the available tools to explore entities, "
        "types, and relationships in the knowledge graph."
    ),
    stateless_http=True,
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

    response = requests.get(
        f"{TYPESENSE_URL}/collections/items/documents/search",
        params=params,
        headers={"X-TYPESENSE-API-KEY": TYPESENSE_API_KEY},
        timeout=10,
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


@mcp.tool()
def get_entity(uri: str) -> str:
    """
    Return all properties and values for a given entity URI.

    Queries the triplestore reasoning endpoint (which includes inferred triples)
    for all triples where the given URI is the subject. Properties are shown with
    human-readable prefixed names where possible (e.g. foaf:name, skos:prefLabel).

    Args:
        uri: The full URI of the entity to retrieve (e.g. https://arachne.l42.eu/track/123).
    """
    if ">" in uri or any(c.isspace() for c in uri):
        return f"Invalid URI: <{uri}> contains characters not permitted in a SPARQL IRI."

    query = f"""
    SELECT ?p ?o WHERE {{
        <{uri}> ?p ?o .
    }}
    ORDER BY ?p ?o
    """

    response = requests.get(
        TRIPLESTORE_SPARQL_URL,
        params={"query": query, "format": "json"},
        auth=TRIPLESTORE_AUTH,
        timeout=30,
    )
    response.raise_for_status()
    data = response.json()

    bindings = data.get("results", {}).get("bindings", [])
    if not bindings:
        return f"No properties found for entity <{uri}>. The URI may not exist in the triplestore."

    # Group values by property
    properties: dict[str, list[str]] = {}
    for binding in bindings:
        prop_uri = binding["p"]["value"]
        prop_label = shorten_uri(prop_uri)

        obj = binding["o"]
        if obj["type"] == "uri":
            value = f"<{shorten_uri(obj['value'])}>"
        elif obj["type"] == "literal":
            lang = obj.get("xml:lang")
            datatype = obj.get("datatype")
            raw = obj["value"]
            if lang:
                value = f'"{raw}"@{lang}'
            elif datatype:
                value = f'"{raw}"^^{shorten_uri(datatype)}'
            else:
                value = f'"{raw}"'
        else:
            value = obj["value"]

        properties.setdefault(prop_label, []).append(value)

    lines = [f"Entity: <{uri}>\n"]
    for prop, values in sorted(properties.items()):
        if len(values) == 1:
            lines.append(f"  {prop}: {values[0]}")
        else:
            lines.append(f"  {prop}:")
            for v in values:
                lines.append(f"    - {v}")

    return "\n".join(lines)


@mcp.tool()
def list_types() -> str:
    """
    List all RDF types in the triplestore with instance counts.

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

    response = requests.get(
        TRIPLESTORE_SPARQL_URL,
        params={"query": query, "format": "json"},
        auth=TRIPLESTORE_AUTH,
        timeout=30,
    )
    response.raise_for_status()
    data = response.json()

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

    response = requests.get(
        TRIPLESTORE_SPARQL_URL,
        params={"query": query, "format": "json"},
        auth=TRIPLESTORE_AUTH,
        timeout=30,
    )
    response.raise_for_status()
    data = response.json()

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

    response = requests.get(
        TRIPLESTORE_SPARQL_URL,
        params={"query": query, "format": "json"},
        auth=TRIPLESTORE_AUTH,
        timeout=30,
    )
    response.raise_for_status()
    data = response.json()

    bindings = data.get("results", {}).get("bindings", [])
    if not bindings:
        return None, f"No property found matching '{prop_name}'."

    return bindings[0]["prop"]["value"], None


@mcp.tool()
def find_entities(
    type: str,
    limit: int = 20,
    properties: Optional[list[str]] = None,
) -> str:
    """
    Find entities of a given type in the knowledge graph, with optional property values.

    Returns a list of matching entities with their URI, label, and any requested
    property values.

    Args:
        type: The type of entity to find — either a human-readable name (e.g. "Person",
              "Track") or a full URI. Use list_types() to see available types.
        limit: Maximum number of results to return (default 20).
        properties: Optional list of property names or URIs to include in results
                    (e.g. ["birthday", "foaf:name"]). If omitted, only the label
                    and URI are returned.
    """
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
        ?s a <{type_uri}> .
        OPTIONAL {{
            {{ ?s skos:prefLabel ?label }}
            UNION
            {{ ?s rdfs:label ?label }}
        }}{optional_clauses}
    }}
    ORDER BY ?label ?s
    LIMIT {limit}
    """

    response = requests.get(
        TRIPLESTORE_SPARQL_URL,
        params={"query": query, "format": "json"},
        auth=TRIPLESTORE_AUTH,
        timeout=30,
    )
    response.raise_for_status()
    data = response.json()

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

    Returns the total number of entities of the type, and how many of those
    have the specified property set.

    Args:
        type: The type of entity to count — either a human-readable name (e.g. "Track")
              or a full URI. Use list_types() to see available types.
        property: The property name or URI to check (e.g. "lyrics",
                  "https://schema.org/lyrics").
    """
    # Resolve the type to a URI
    type_uri, type_err = _resolve_type_uri(type)
    if type_err:
        return type_err

    # Resolve the property to a URI
    prop_uri, prop_err = _resolve_property_uri(property)
    if prop_err:
        return prop_err

    query = f"""
    SELECT
        (COUNT(DISTINCT ?s) AS ?total)
        (COUNT(DISTINCT ?sWithProp) AS ?withProp)
    WHERE {{
        ?s a <{type_uri}> .
        OPTIONAL {{
            ?sWithProp a <{type_uri}> .
            ?sWithProp <{prop_uri}> ?val .
        }}
    }}
    """

    response = requests.get(
        TRIPLESTORE_SPARQL_URL,
        params={"query": query, "format": "json"},
        auth=TRIPLESTORE_AUTH,
        timeout=30,
    )
    response.raise_for_status()
    data = response.json()

    bindings = data.get("results", {}).get("bindings", [])
    if not bindings:
        return f"Could not retrieve counts for type '{type}' and property '{property}'."

    binding = bindings[0]
    total = int(binding.get("total", {}).get("value", 0))
    with_prop = int(binding.get("withProp", {}).get("value", 0))

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


async def info(request):
    return JSONResponse({
        "system": os.environ.get("SYSTEM", "lucos_arachne"),
        "checks": {},
        "metrics": {},
        "ci": {"circle": "gh/lucas42/lucos_arachne"},
        "title": "Arachne MCP",
    })


mcp_asgi_app = mcp.streamable_http_app()


@asynccontextmanager
async def lifespan(app):
    async with mcp_asgi_app.router.lifespan_context(app):
        yield


app = Starlette(
    routes=[
        Route("/_info", info),
        Mount("/", app=mcp_asgi_app),
    ],
    lifespan=lifespan,
)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)
