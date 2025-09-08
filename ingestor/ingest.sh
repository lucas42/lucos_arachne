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
	["skos"]="http://www.w3.org/2004/02/skos/core#"
	["owl"]="https://www.w3.org/2002/07/owl"
	["dc"]="http://purl.org/dc/terms/"
	["rdf"]="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
	["rdfs"]="http://www.w3.org/2000/01/rdf-schema#"
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
	curl "$url" --header "Accept: application/rdf+xml" --header "${auth_header}" --user-agent "lucos_arachne_ingestor" --silent --show-error --fail --location --output $tmp_file
	# Delete everything in the triplestore for the given graph
	curl "http://lucos_arachne:${KEY_LUCOS_ARACHNE}@triplestore:3030/raw_arachne/update" --user-agent "lucos_arachne_ingestor" --data-urlencode "update=DROP GRAPH <${url}>" --silent --show-error --fail > /dev/null
	# Upload the fresh data to the triplestore
	curl "http://lucos_arachne:${KEY_LUCOS_ARACHNE}@triplestore:3030/raw_arachne/data?graph=${url}" --user-agent "lucos_arachne_ingestor" --silent --show-error --fail --form "file=@${tmp_file}" | grep tripleCount
	rm $tmp_file
done