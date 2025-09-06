#!/bin/bash
#
# Ingests RDF from other systems and adds each as its own graph in the triplestore
#

set -e

if [ -z "${KEY_LUCOS_ARACHNE}" ]; then
	echo "No KEY_LUCOS_ARACHNE environment variable found — won't be able to authenticate against triplestore endpoint"
	exit 2
fi

# The URLs to ingest from
declare -A urls=( ["lucos_eolas"]="https://eolas.l42.eu/metadata/all/data/")

for system in "${!urls[@]}"; do
	url=${urls[$system]}
	key_var="KEY_${system^^}"
	key=${!key_var}
	if [ -z "${key}" ]; then
		echo "No ${key_var} environment variable found — won't be able to authenticate against ingestion endpoint ${url}"
		exit 2
	fi
	echo "Ingesting data from $url"
	# Fetch the latest version of data
	curl "$url" --header "Accept: text/turtle" --header "Authorization: key ${key}" --silent --show-error --fail --location --output /tmp/data.ttl
	# Delete everything in the triplestore for the given graph
	curl "http://lucos_arachne:${KEY_LUCOS_ARACHNE}@triplestore:3030/arachne/" --data-urlencode "update=DROP GRAPH <${url}>" --silent --show-error --fail > /dev/null
	# Upload the fresh data to the triplestore
	curl "http://lucos_arachne:${KEY_LUCOS_ARACHNE}@triplestore:3030/arachne/data?graph=${url}" --silent --show-error --fail --form "file=@/tmp/data.ttl" | grep tripleCount
	rm /tmp/data.ttl
done