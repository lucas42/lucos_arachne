#!/bin/sh
set -e

supercronic /crontab &
pipenv --quiet run python -u ingest.py &
pipenv --quiet run python -u server.py
