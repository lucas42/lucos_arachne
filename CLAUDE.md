# lucos_arachne

## Ontology source

This repo consumes ontology and entity data from `lucos_eolas`. For conventions on the ontology side — declaring new predicates, adding property characteristics, using `skos:prefLabel` on meta-types — see [`lucos_eolas/CLAUDE.md`](https://github.com/lucas42/lucos_eolas/blob/main/CLAUDE.md).

## RDF Source Convention

**RDF sources consumed by arachne must include type metadata for every domain `rdf:type` they emit.**

Specifically, for every domain `rdf:type` emitted by a source, the source RDF must also include:
- `<type> skos:prefLabel "..." @en` — the human-readable label for the type (used as the search index `type` field)
- `<type> eolas:hasCategory <category>` — the eolas category URI
- `<category> skos:prefLabel "..." @en` — the human-readable category label

The ingestor (`searchindex.py`) does **not** fall back to the triplestore when this metadata is missing. It raises a `ValueError` with a message pointing at the source system. New entity types added to any source must include this metadata from day one.

**OWL/RDFS infrastructure types are excluded.** Any `rdf:type` in the OWL (`owl:`), RDFS (`rdfs:`), or RDF-syntax (`rdf:`) namespaces is classified as a meta-type by `is_meta_type()` in `searchindex.py` and silently skipped — it is never looked up in the source RDF and requires no source-side metadata. Source systems may freely use property characteristics such as `owl:ObjectProperty`, `owl:FunctionalProperty`, or `rdfs:Class` without supplying type metadata for them.

See [#371](https://github.com/lucas42/lucos_arachne/issues/371) for the rationale.
