#!/bin/bash

set -e

# Clean up orphaned TDB2 Data-NNNN directories before starting Fuseki.
#
# TDB2 always uses the highest-numbered directory as the active dataset.
# Compaction with deleteOld=true removes the immediate predecessor only,
# so older orphans can accumulate across multiple compaction cycles.
# Removing them here — before Fuseki starts — is safe: no file handles
# are open yet, and the highest-numbered directory is always correct.
DB_DIR="/fuseki/run/databases/arachne"
if [ -d "$DB_DIR" ]; then
    LIVE_DIR=$(find "$DB_DIR" -maxdepth 1 -name 'Data-[0-9]*' -type d 2>/dev/null | sort -t- -k2 -n | tail -1)
    if [ -n "$LIVE_DIR" ]; then
        find "$DB_DIR" -maxdepth 1 -name 'Data-[0-9]*' -type d 2>/dev/null | while read -r dir; do
            if [ "$dir" != "$LIVE_DIR" ]; then
                echo "Removing orphaned TDB2 directory: $dir"
                rm -rf "$dir"
            fi
        done
    fi
fi

USERS=`echo $CLIENT_KEYS | sed -e "s/:[^=]*=/=/g" | sed -e "s/;/\n/g"` envsubst < shiro.ini.template > run/shiro.ini
java $JVM_ARGS -jar jena-fuseki-server.jar --timeout=30000
