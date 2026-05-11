# mcp/CLAUDE.md — MCP server conventions

## SPARQL conventions

### Rule 1: OPTIONAL and UNION blocks must share a bound variable with the surrounding pattern

**Every OPTIONAL or UNION block must share at least one variable that is already bound by the surrounding pattern, unless a cross-product is the intended semantics.**

Without a shared variable, the triplestore must materialise the full Cartesian product of the inner and outer result sets before evaluating any aggregate. At production data sizes this trips Fuseki's 30-second service guard and surfaces as a 503 that looks like a triplestore outage.

When in doubt, prefer two simple queries over one combined query with an OPTIONAL. The savings from a single round-trip are negligible compared to the cost of accidentally requesting a Cartesian product.

**❌ Wrong — `?sWithProp` is not bound by the outer pattern (Cartesian product):**

```sparql
SELECT (COUNT(DISTINCT ?s) AS ?total) (COUNT(DISTINCT ?sWithProp) AS ?withProp)
WHERE {
    ?s a <https://schema.org/Track> .
    OPTIONAL {
        ?sWithProp a <https://schema.org/Track> .
        ?sWithProp <https://schema.org/lyrics> ?val .
    }
}
```

**✅ Correct — split into two queries, each bound to `?s`:**

```sparql
# Query 1: total
SELECT (COUNT(DISTINCT ?s) AS ?total)
WHERE { ?s a <https://schema.org/Track> . }

# Query 2: with property
SELECT (COUNT(DISTINCT ?s) AS ?withProp)
WHERE {
    ?s a <https://schema.org/Track> .
    ?s <https://schema.org/lyrics> ?val .
}
```

See [#477](https://github.com/lucas42/lucos_arachne/issues/477) for the worked example and production incident.

---

### Rule 2: Always `COUNT(DISTINCT ?s)` when the result is about subjects

**When counting entities, count `COUNT(DISTINCT ?s)` — the subject variable — not `COUNT(DISTINCT ?val)` — the property value variable.**

If a property's value is shared across multiple subjects (e.g. many Tracks with `language: "en"`), then counting distinct values under-reports the number of subjects that have the property.

**❌ Wrong — counts distinct values, not distinct subjects:**

```sparql
SELECT (COUNT(DISTINCT ?val) AS ?withProp)
WHERE {
    ?s a <https://schema.org/Track> .
    ?s <https://schema.org/inLanguage> ?val .
}
```

If 3,000 Tracks all have `inLanguage "en"`, this returns 1 (one distinct value) instead of 3,000 (subjects with the property).

**✅ Correct — counts distinct subjects:**

```sparql
SELECT (COUNT(DISTINCT ?s) AS ?withProp)
WHERE {
    ?s a <https://schema.org/Track> .
    ?s <https://schema.org/inLanguage> ?val .
}
```

This returns 3,000 — the number of Tracks that have the property, regardless of how many share the same value.

See [#477](https://github.com/lucas42/lucos_arachne/issues/477) for the architect's analysis.
