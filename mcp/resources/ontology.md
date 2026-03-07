# Arachne Knowledge Graph — Ontology Summary

This document describes the types, properties, and namespaces used in the
lucos_arachne triplestore. Use it to understand what kinds of data are
available before making tool calls.

---

## Namespace Prefixes

| Prefix | Namespace URI |
|--------|---------------|
| `rdf:` | `http://www.w3.org/1999/02/22-rdf-syntax-ns#` |
| `rdfs:` | `http://www.w3.org/2000/01/rdf-schema#` |
| `owl:` | `http://www.w3.org/2002/07/owl#` |
| `skos:` | `http://www.w3.org/2004/02/skos/core#` |
| `foaf:` | `http://xmlns.com/foaf/0.1/` |
| `schema:` | `http://schema.org/` |
| `dcterms:` | `http://purl.org/dc/terms/` |
| `dc:` | `http://purl.org/dc/elements/1.1/` |
| `mo:` | `http://purl.org/ontology/mo/` |
| `prov:` | `http://www.w3.org/ns/prov#` |
| `time:` | `http://www.w3.org/2006/time#` |

---

## Entity Types

### Person (`foaf:Person`)

Represents a real person known to the lucos system.

Common properties:
- `foaf:name` — full name (string)
- `skos:prefLabel` — preferred display label (string)
- `foaf:birthday` — date of birth (date literal, format `YYYY-MM-DD`)
- `foaf:interest` — URI of a topic of interest
- `foaf:knows` — URI of another Person

### Track (`mo:Track`)

A music track or recording ingested from the media metadata API.

Common properties:
- `dc:title` — track title (string)
- `skos:prefLabel` — preferred display label (same as title in most cases)
- `schema:lyrics` — full lyrics text (string, often not present)
- `mo:performed_in` — URI of the Album this track appears on
- `foaf:maker` — URI of the Artist who made the track
- `dcterms:language` — URI of the Language the track is in

### Artist (`mo:MusicArtist`)

A music artist or group. May be an individual or a band.

Common properties:
- `foaf:name` — artist name (string)
- `skos:prefLabel` — preferred display label (string)
- `foaf:member` — URI of a Person who is a member (for groups)

### Album (`mo:Record`)

A music album or release.

Common properties:
- `dc:title` — album title (string)
- `skos:prefLabel` — preferred display label (string)
- `foaf:maker` — URI of the Artist who made the album
- `mo:track` — URI of a Track on the album

### Language (`dcterms:LinguisticSystem`)

A human language, referenced from tracks and other entities.

Common properties:
- `skos:prefLabel` — language name (string, e.g. "English", "French")
- `skos:exactMatch` — URI of the equivalent concept in an external vocabulary

### Software (`schema:SoftwareApplication`)

A software system or application known to the lucos ecosystem.

Common properties:
- `skos:prefLabel` — application name (string)
- `schema:url` — URL of the application

---

## Notes on Querying

- The triplestore endpoint used by MCP tools includes OWL reasoning, so
  inferred triples (e.g. subclass memberships) are included in results.
- Use `skos:prefLabel` or `rdfs:label` for human-readable entity names.
- URIs for arachne-native entities follow the pattern
  `https://arachne.l42.eu/<type>/<id>` (e.g. `https://arachne.l42.eu/person/1`).
