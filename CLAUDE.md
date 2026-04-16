# lucos_arachne

## RDF Source Convention

**RDF sources consumed by arachne must include type metadata for any `rdf:type` they emit.**

Specifically, for every `rdf:type` emitted by a source, the source RDF must also include:
- `<type> skos:prefLabel "..." @en` — the human-readable label for the type (used as the search index `type` field)
- `<type> eolas:hasCategory <category>` — the eolas category URI
- `<category> skos:prefLabel "..." @en` — the human-readable category label

The ingestor (`searchindex.py`) does **not** fall back to the triplestore when this metadata is missing. It raises a `ValueError` with a message pointing at the source system. New entity types added to any source must include this metadata from day one.

See [#371](https://github.com/lucas42/lucos_arachne/issues/371) for the rationale.
