"""
Fixed probe parameters for the per-tool production health checks.

These parameters are committed to source control so that changes require
code review.  Drift in probe parameters must not be allowed to mask drift
in the underlying data.

The probe exercises each of the five MCP tools against the live production
triplestore and search index on a regular interval, records wall-clock
latency, and surfaces the results as Tier 2 checks in /_info.  See #503.
"""

# ---------------------------------------------------------------------------
# Timing constants
# ---------------------------------------------------------------------------

# Wall-clock budget per tool probe (seconds).
# Exceeding this limit marks the check as ok=False.
# Set below the Fuseki 30 s service guard so that scale-drift is caught
# before Fuseki's own guard fires.
PROBE_BUDGET_S: float = 5.0

# Interval between complete probe cycles (seconds).
# The five tools are staggered within each cycle (PROBE_BUDGET_S / num_tools
# between each), so no two tools run concurrently against Fuseki.
PROBE_INTERVAL_S: float = 30.0

# If no probe result has been recorded for a tool within this window, the
# check reports ok=False with a stale-result detail rather than the last
# cached (possibly stale) reading.
PROBE_STALE_THRESHOLD_S: float = 300.0  # 5 minutes

# ---------------------------------------------------------------------------
# Probe parameters
# ---------------------------------------------------------------------------

# schema:MusicRecording is an OWL class from the schema.org ontology, which
# is a permanent fixture in arachne's ingested data.  It is used as the
# stable entity and type across multiple probe calls.
_MUSIC_RECORDING_TYPE = "https://schema.org/MusicRecording"

# Ordered list of (tool_name, kwargs) pairs.
# The tool functions are called with these kwargs during each probe cycle.
# The ordering also sets the stagger sequence within each cycle.
PROBE_TOOLS: list[tuple[str, dict]] = [
    # Full-text search against the Typesense index.
    ("search",            {"query": "music"}),

    # Property lookup on a well-known OWL class URI.
    # schema:MusicRecording is defined in the schema.org ontology and will
    # always exist in the triplestore's inferred arachne endpoint.
    ("get_entity",        {"uri": _MUSIC_RECORDING_TYPE}),

    # Type enumeration — no parameters; exercises the broadest SPARQL query.
    # This is the canonical scale-drift query (see issue #321).
    ("list_types",        {}),

    # Entity listing for a high-instance type.
    ("find_entities",     {"type": _MUSIC_RECORDING_TYPE}),

    # Per-property count for a high-cardinality type/property pair.
    # schema:inLanguage is commonly set on tracks, producing a large result set
    # that exercises the Cartesian-product failure path (see issue #477).
    ("count_by_property", {
        "type":     _MUSIC_RECORDING_TYPE,
        "property": "https://schema.org/inLanguage",
    }),
]
