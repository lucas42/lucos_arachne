# ADR-0001: MCP server for knowledge graph access

**Date:** 2026-03-07
**Status:** Accepted
**Discussion:** https://github.com/lucas42/lucos_arachne/issues/15

## Context

lucos_arachne is a knowledge graph that ingests linked data from across the lucOS estate — contacts, media metadata, eolas (general knowledge), and several standard ontologies — into an Apache Jena Fuseki triplestore with OWL reasoning. It also maintains a Typesense search index for full-text search. The reasoning endpoint applies OWL inferencing over the union of all ingested graphs, producing inferred triples and cross-source connections that do not exist in any individual source system.

Despite this, arachne's only substantial consumer today is `lucos_search_component`, a set of web components providing search widgets. The inferencing capability and cross-source linking — the features that distinguish arachne from a simple search index — are largely unused.

Separately, LLM agents working on lucOS (particularly the architect agent) need to understand data patterns when making architectural decisions. Currently agents can only read source code, which describes the schema but not the data: how many contacts have birthdays, what the distribution of track genres looks like, whether edge cases exist that would affect a proposed solution.

An earlier attempt to solve this through lucos_comhra — a chatbot backed onto arachne's SPARQL endpoint — failed. The core problem: **LLMs cannot reliably generate valid SPARQL.** The syntax is too precise, the URIs are custom to the arachne ontology, and prefix declarations must be exact. Models produce queries that look plausible but fail on execution. This is not a model quality issue that will be solved by better models — it is a fundamental mismatch between SPARQL's precision requirements and LLM text generation characteristics.

## Decision

Build an **MCP (Model Context Protocol) server** as a new container within the lucos_arachne Docker Compose stack. The MCP server provides structured tool access to arachne's knowledge graph, with the server — not the LLM — generating all SPARQL queries.

### Deployment

The MCP server runs as a persistent container (`lucos_arachne_mcp`) in the arachne compose stack. It is routed via the existing nginx `web` container at the path `/mcp/`, following the same path-based routing pattern used by `/explore/`, `/search`, and `/sparql`. The transport is SSE or Streamable HTTP, as appropriate for a persistent containerised service.

This was chosen over a local stdio process in the agent sandbox because:

1. **Reproducibility.** The agent sandbox is a long-running VM provisioned by `lucos_agent_coding_sandbox`. There is an ongoing drift problem between the provisioning config and the actual VM state. A Docker container under version control avoids this entirely.
2. **Availability to other consumers.** A container in the arachne stack is accessible to any service that can reach `arachne.l42.eu`, not just the agent sandbox. This matters if lucos_comhra gets another attempt, or if other tools want to query arachne data.
3. **Network locality.** Inside the compose stack, the MCP server reaches Fuseki and Typesense via internal Docker hostnames (`triplestore:3030`, `search:8108`). No need to expose those ports externally.

### Tool design

The core principle: **the MCP server writes the SPARQL, not the LLM.** Each tool accepts structured parameters and translates them into correct queries using the actual ontology URIs.

| Tool | Parameters | Purpose |
|---|---|---|
| `search` | `query`, `filter_by?`, `limit?` | Full-text search via Typesense |
| `list_types` | *(none)* | All RDF types with instance counts |
| `get_entity` | `uri` | All triples for a given subject |
| `find_entities` | `type`, `limit?`, `properties?` | Find entities of a type, optionally with specific properties |
| `count_by_property` | `type`, `property` | How many entities of a type have a given property? |

A raw `sparql_query` tool is deliberately excluded. If the tools above cannot express a needed query, the correct response is to add a new tool with structured parameters, not to expose raw SPARQL to the LLM.

### Data access

The MCP server queries the `arachne` reasoning endpoint (not `raw_arachne`). This is the endpoint that applies OWL inferencing over the union of all ingested graphs — the whole point is to exploit the inferred triples and cross-source connections. Access is strictly read-only.

### Static resources

The MCP server also exposes MCP resources — static context that helps LLM consumers understand the data model before calling tools:

- **Ontology summary:** types, properties, and prefix mappings with human-readable descriptions
- **Data sources:** the `systems_to_graphs` mapping showing which lucOS services contribute data

### Technology

- Python with the `mcp` SDK (the official MCP Python package)
- `requests` for HTTP calls to Fuseki and Typesense
- No framework beyond MCP itself — this is a thin translation layer

## Alternatives considered

### Direct SPARQL access (with or without a wrapper)

The architect's initial recommendation: give the agent a read-only SPARQL key, a wrapper script, and example queries. This solves the "agent needs data awareness" problem with zero new infrastructure — just HTTP access to the existing SPARQL endpoint.

Rejected because **LLMs cannot reliably generate valid SPARQL.** The syntax is too precise, the URIs are custom to the arachne ontology, and prefix declarations must be exact. Models produce queries that look plausible but fail on execution. This is not a model quality issue that will improve with better models — it is a fundamental mismatch between SPARQL's precision requirements and LLM text generation characteristics. The same problem applies whether the agent calls the endpoint directly, uses a shell wrapper, or has a library of example queries to crib from. An abstraction layer that translates structured parameters into correct SPARQL — rather than asking the LLM to write SPARQL — is necessary, not optional. This is the core insight that motivated the MCP approach.

### MCP server as a local stdio process

Originally proposed by the architect as the deployment model after the SPARQL generation problem ruled out direct access. Run the MCP server locally in the agent sandbox as a stdio process rather than as a deployed container. Simpler MCP transport (stdio vs SSE), zero production impact.

Revised to a container deployment at lucas42's suggestion, because of the sandbox drift problem. The agent sandbox VM accumulates state that diverges from its provisioning config. A tool under active iterative development would make this worse. Docker containers under version control are reproducible by design.

### Separate repository

Create a new `lucos_arachne_mcp` repository for the MCP server.

Rejected because the MCP server is tightly coupled to arachne's data model — the ontology URIs, property mappings, and `systems_to_graphs` structure. Keeping it in the same repo as the ingestor means shared knowledge stays in sync. The setup overhead of a separate repo (CI, Docker Hub, deployment config) is not justified for what is essentially a read-only view layer over existing data.

## Consequences

### Positive

- **Arachne gets a proper consumer** that exploits its inferencing and cross-source linking capabilities, retroactively justifying the infrastructure complexity.
- **Solves the SPARQL generation problem** that killed lucos_comhra. LLMs interact through structured tool parameters; SPARQL is an internal implementation detail they never see.
- **Single access point for estate data.** Agents can query contacts, media, eolas, and their cross-references through one interface instead of integrating with each datastore separately.
- **Low blast radius.** The MCP server is read-only and runs alongside existing containers. If it breaks, consumers lose a tool — nothing else is affected. The triplestore and search index are unchanged.
- **Extensible.** New tools can be added incrementally as new query patterns are needed, without changing the existing tools or the transport layer.

### Negative

- **New container to maintain.** It needs a Dockerfile, CI pipeline, version management, and monitoring. This is modest overhead but not zero.
- **Tight coupling to the ontology.** If the arachne data model changes (new types, renamed properties), the MCP server's type/property resolution logic must be updated. This is mitigated by keeping it in the same repo.
- **MCP protocol maturity.** The MCP ecosystem is still evolving. The SSE transport and SDK may change in ways that require maintenance. This is accepted as a trade-off — the protocol is stable enough for a non-critical tool, and the implementation is thin enough that adapting to protocol changes would be low effort.
- **Authentication not yet designed.** The MCP server authenticates to Fuseki and Typesense using internal credentials, but client authentication (who can call the MCP server?) is not addressed in this decision. This is acceptable for now — the server is behind the reverse proxy and not publicly advertised — but will need addressing before wider use.
