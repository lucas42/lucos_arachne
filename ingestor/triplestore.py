import os, sys
import requests

KEY_LUCOS_ARACHNE = os.environ.get("KEY_LUCOS_ARACHNE")

if not KEY_LUCOS_ARACHNE:
	sys.exit("No KEY_LUCOS_ARACHNE environment variable found — won't be able to authenticate against triplestore endpoint")


session = requests.Session()
session.auth = ("lucos_arachne", KEY_LUCOS_ARACHNE)
session.headers.update({"User-Agent": "lucos_arachne_ingestor"})

def update_triplestore(graph_url, content, content_type):
	# Drop old graph
	session.post(
		"http://triplestore:3030/raw_arachne/update",
		data={"update": f"DROP GRAPH <{graph_url}>"},
	)

	# Upload new data
	upload_resp = session.post(
		f"http://triplestore:3030/raw_arachne/data?graph={graph_url}",
		headers={"Content-Type": content_type},
		data=content.encode("utf-8"),
	)
	upload_resp.raise_for_status()
	try:
		json_resp = upload_resp.json()
		if "tripleCount" in json_resp:
			print(f"Uploaded {json_resp['tripleCount']} triples to graph <{graph_url}>")
		else:
			print(f"Upload complete for graph <{graph_url}>, but no tripleCount in response")
	except ValueError:
		print(f"Upload complete for graph <{graph_url}>, but response was not JSON")

# Cleans up any graphs in the triplestore which aren't in the list provided
def cleanup_triplestore(graph_uris):
	resp = session.post(
		"http://triplestore:3030/raw_arachne/sparql",
		headers={"Accept": "application/json"},
		data={"query": "SELECT * WHERE {GRAPH ?graph{}}"},
	)
	resp.raise_for_status()
	graphlist = resp.json()
	for binding in graphlist['results']['bindings']:
		graph_uri = binding['graph']['value']
		if graph_uri not in graph_uris:
			print(f"Deleting unknown graph <{graph_uri}>")
			session.post(
				"http://triplestore:3030/raw_arachne/update",
				data={"update": f"DROP GRAPH <{graph_uri}>"},
			)
