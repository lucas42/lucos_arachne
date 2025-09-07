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
declare -A urls=(
	["lucos_eolas"]="https://eolas.l42.eu/metadata/all/data/"
	["lucos_contacts"]="https://contacts.l42.eu/agents/all"
	["foaf"]="http://xmlns.com/foaf/spec/"
	["time"]="https://www.w3.org/2006/time"
	["dbpedia_meanOfTransportation"]="https://dbpedia.org/ontology/MeanOfTransportation"
)

for system in "${!urls[@]}"; do
	url=${urls[$system]}
	if [[ $system == lucos_* ]]; then
		key_var="KEY_${system^^}"
		key=${!key_var}
		if [ -z "${key}" ]; then
			echo "No ${key_var} environment variable found — won't be able to authenticate against ingestion endpoint ${url}"
			exit 2
		fi
		auth_header="Authorization: key ${key}"
	fi
	echo "Ingesting data from $url"
	tmp_file="/tmp/${system}.rdf"
	# Fetch the latest version of data
	curl "$url" --header "Accept: application/rdf+xml" --header "${auth_header}" --silent --show-error --fail --location --output $tmp_file
	# Delete everything in the triplestore for the given graph
	curl "http://lucos_arachne:${KEY_LUCOS_ARACHNE}@triplestore:3030/arachne/" --data-urlencode "update=DROP GRAPH <${url}>" --silent --show-error --fail > /dev/null
	# Upload the fresh data to the triplestore
	curl "http://lucos_arachne:${KEY_LUCOS_ARACHNE}@triplestore:3030/arachne/data?graph=${url}" --silent --show-error --fail --form "file=@${tmp_file}" | grep tripleCount
	rm $tmp_file
done