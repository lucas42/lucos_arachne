#!/usr/bin/env python3
"""
One-time cleanup: delete the 19 synthetic-ID rows that were written by
lucos_arachne's v1 schedule_tracker calls.

Run this once after the v2 migration (this PR) has been deployed and the
new v2 calls have run at least once in production.  After that, the old
synthetic-ID rows will keep ageing past their alert thresholds and firing
false heartbeat alerts until they're gone.

Usage (from inside the container or with SCHEDULE_TRACKER_ENDPOINT set):
    pipenv run python cleanup_v1_schedule_tracker.py
"""
import os, sys
import requests

SCHEDULE_TRACKER_ENDPOINT = os.environ.get("SCHEDULE_TRACKER_ENDPOINT", "")

if not SCHEDULE_TRACKER_ENDPOINT:
    sys.exit("\033[91mSCHEDULE_TRACKER_ENDPOINT environment variable not set\033[0m")

# Strip the /v2/report-status path to get the base URL.
BASE_URL = SCHEDULE_TRACKER_ENDPOINT.split("/v2/")[0]

OLD_SYNTHETIC_IDS = [
    "lucos_arachne_compaction",
    "lucos_arachne_ingestor",
    "lucos_arachne_ingestor_lucos_media_metadata_api",
    "lucos_arachne_ingestor_lucos_configy",
    "lucos_arachne_ingestor_lucos_eolas",
    "lucos_arachne_ingestor_lucos_contacts",
    "lucos_arachne_ingestor_foaf",
    "lucos_arachne_ingestor_time",
    "lucos_arachne_ingestor_dbpedia_meanOfTransportation",
    "lucos_arachne_ingestor_skos",
    "lucos_arachne_ingestor_owl",
    "lucos_arachne_ingestor_dc",
    "lucos_arachne_ingestor_dcam",
    "lucos_arachne_ingestor_rdf",
    "lucos_arachne_ingestor_rdfs",
    "lucos_arachne_ingestor_loc_iso639-5",
    "lucos_arachne_ingestor_loc_mads",
    "lucos_arachne_ingestor_music_ontology",
    "lucos_arachne_ingestor_inference",
]

errors = []
for system_id in OLD_SYNTHETIC_IDS:
    url = f"{BASE_URL}/schedule/{system_id}"
    try:
        resp = requests.delete(url, timeout=10)
        if resp.status_code == 404:
            print(f"  SKIP  {system_id} (already gone)")
        else:
            resp.raise_for_status()
            print(f"  OK    {system_id}")
    except Exception as e:
        print(f"  FAIL  {system_id}: {e}")
        errors.append(system_id)

if errors:
    sys.exit(f"\nFailed to delete {len(errors)} row(s): {errors}")
else:
    print(f"\nAll {len(OLD_SYNTHETIC_IDS)} synthetic-ID rows cleaned up.")
