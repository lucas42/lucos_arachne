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
