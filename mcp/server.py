"""
Arachne MCP Server

Exposes the lucos_arachne knowledge graph via the Model Context Protocol.
Currently a skeleton with no tools registered — transport and container
scaffolding only. Tools will be added in follow-up tickets.
"""

import os

import uvicorn
from mcp.server.fastmcp import FastMCP
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Mount, Route

# The MCP server must bind on all interfaces so nginx can proxy to it.
# FastMCP defaults to 127.0.0.1 (localhost-only), which breaks container networking.
PORT = int(os.environ.get("PORT", "8200"))

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
