#!/bin/bash
#
# Ingests RDF from other systems and adds each as its own graph in the triplestore
#

failure() {
	error_message="Command \`$3\` exited with code $2 [line $1]"
	echo "Sending error to schedule tracker"
	curl "$SCHEDULE_TRACKER_ENDPOINT" --data "{
		\"system\": \"lucos_arachne_ingestor\",
		\"frequency\": 3600,
		\"status\": \"error\",
		\"message\": \"${error_message//\"/\\\"}\"
	}" -H "Content-Type: application/json" --silent --show-error --fail
	exit "$2"
}

trap 'failure "$LINENO" "$?" "$BASH_COMMAND"' ERR

if [ -z "${KEY_LUCOS_ARACHNE}" ]; then
	echo "No KEY_LUCOS_ARACHNE environment variable found — won't be able to authenticate against triplestore endpoint"
	exit 2
fi

# The URLs to ingest from
declare -A urls=(
	["lucos_eolas"]="https://eolas.l42.eu/metadata/all/data/"
	["lucos_contacts"]="https://contacts.l42.eu/people/all"
	["foaf"]="http://xmlns.com/foaf/spec/"
	["time"]="https://www.w3.org/2006/time"
	["dbpedia_meanOfTransportation"]="https://dbpedia.org/ontology/MeanOfTransportation"
	["skos"]="http://www.w3.org/2004/02/skos/core#"
	["owl"]="https://www.w3.org/2002/07/owl"
	["dc"]="http://purl.org/dc/terms/"
	["dcam"]="http://purl.org/dc/dcam/"
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
	tmp_file=$(mktemp)
	response_headers=$(mktemp)
	# Fetch the latest version of data
	curl "$url" --header "Accept: application/rdf+xml, text/turtle, application/ld+json" --header "${auth_header}" --user-agent "lucos_arachne_ingestor" --silent --show-error --fail --location --output $tmp_file --dump-header "$response_headers"
	content_type=$(grep -i '^Content-Type:' "$response_headers" | tail -1 | awk '{print $2}' | tr -d '\r')
	# schema.org is referred to using http and https — standardise to https
	sed -i 's~http://schema.org/~https://schema.org/~g' $tmp_file
	# Delete everything in the triplestore for the given graph
	curl "http://lucos_arachne:${KEY_LUCOS_ARACHNE}@triplestore:3030/raw_arachne/update" --user-agent "lucos_arachne_ingestor" --data-urlencode "update=DROP GRAPH <${url}>" --silent --show-error --fail > /dev/null
	# Upload the fresh data to the triplestore
	curl "http://lucos_arachne:${KEY_LUCOS_ARACHNE}@triplestore:3030/raw_arachne/data?graph=${url}" --header "Content-Type: ${content_type}" --user-agent "lucos_arachne_ingestor" --silent --show-error --fail --data "@${tmp_file}" | grep tripleCount
	rm $tmp_file
	rm $response_headers
done

# Check for any graphs in the triplestore which aren't in our list here, and delete them
curl "http://lucos_arachne:${KEY_LUCOS_ARACHNE}@triplestore:3030/raw_arachne/sparql" --header "Accept:text/csv" --user-agent "lucos_arachne_ingestor" --data-urlencode "query=SELECT * WHERE {GRAPH ?g{}}" --silent --show-error --fail | tail -n +2  | while read line; do
	graph_uri=`echo $line | tr -d '\r\n'`
	if ! [[ ${urls[@]} =~ $graph_uri ]] then
		echo "Deleting unknown graph <${graph_uri}>"
		curl "http://lucos_arachne:${KEY_LUCOS_ARACHNE}@triplestore:3030/raw_arachne/update" --user-agent "lucos_arachne_ingestor" --data-urlencode "update=DROP GRAPH <${graph_uri}>" --silent --show-error --fail > /dev/null
	fi
done

curl "$LOGANNE_ENDPOINT" --data '{
	"type":"knowledgeIngest",
	"source":"lucos_arachne_ingestor",
	"humanReadable":"Data ingested into knowledge graph",
	"url":"https://arachne.l42.eu/"
}' -H "Content-Type: application/json" --silent --show-error --fail > /dev/null


curl "$SCHEDULE_TRACKER_ENDPOINT" --data '{
	"system": "lucos_arachne_ingestor",
	"frequency": 3600,
	"status": "success"
}' -H "Content-Type: application/json" --silent --show-error --fail