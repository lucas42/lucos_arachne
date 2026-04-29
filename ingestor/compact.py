#!/usr/bin/env python3
"""Trigger a TDB2 compaction on the raw_arachne Fuseki dataset."""
import os, sys
import requests
from loganne import updateLoganne
from schedule_tracker import updateScheduleTracker

KEY_LUCOS_ARACHNE = os.environ.get("KEY_LUCOS_ARACHNE")
if not KEY_LUCOS_ARACHNE:
	sys.exit("No KEY_LUCOS_ARACHNE environment variable found")

try:
	BASE_URL = os.environ["APP_ORIGIN"] + "/"
except KeyError:
	sys.exit("\033[91mAPP_ORIGIN environment variable not set\033[0m")

SYSTEM = "lucos_arachne_compaction"

# Compaction runs weekly via cron (Sundays at 03:30 UTC). Pass the actual
# schedule interval to schedule-tracker; it derives its alert threshold from
# this value.
FREQUENCY_SECONDS = 7 * 24 * 60 * 60


def run_compaction():
	print("Starting TDB2 compaction on raw_arachne...", flush=True)
	resp = requests.post(
		"http://triplestore:3030/$/compact/raw_arachne",
		params={"deleteOld": "true"},
		auth=("lucos_arachne", KEY_LUCOS_ARACHNE),
	)
	resp.raise_for_status()
	print("Compaction complete.", flush=True)
	updateLoganne(type="tripleStoreCompaction", humanReadable="TDB2 triplestore compacted", url=BASE_URL)
	updateScheduleTracker(success=True, system=SYSTEM, frequency=FREQUENCY_SECONDS)


def main():
	try:
		run_compaction()
	except Exception as e:
		error_message = f"Compaction failed: {e}"
		print(error_message, flush=True)
		updateScheduleTracker(success=False, system=SYSTEM, message=error_message, frequency=FREQUENCY_SECONDS)
		sys.exit(error_message)


if __name__ == "__main__":
	main()
