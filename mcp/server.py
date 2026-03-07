"""
Arachne MCP Server

Exposes the lucos_arachne knowledge graph via the Model Context Protocol.
"""

import os
from typing import Optional

import requests
import uvicorn
from mcp.server.fastmcp import FastMCP
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Mount, Route

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


async def info(request):
    return JSONResponse({
        "system": os.environ.get("SYSTEM", "lucos_arachne"),
        "checks": {},
        "metrics": {},
        "ci": {"circle": "gh/lucas42/lucos_arachne"},
        "title": "Arachne MCP",
    })


app = Starlette(routes=[
    Route("/_info", info),
    Mount("/", app=mcp.streamable_http_app()),
])

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)
