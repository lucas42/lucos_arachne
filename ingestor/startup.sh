#!/bin/sh
set -e

pipenv --quiet run python -u ingest.py &
pipenv --quiet run python -u server.py
