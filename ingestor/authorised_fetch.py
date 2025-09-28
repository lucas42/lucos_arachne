import os, sys, re
import requests

session = requests.Session()
session.headers.update({
	"User-Agent": "lucos_arachne_ingestor",
	"Accept": "application/rdf+xml, text/turtle, application/ld+json",
})

def fetch_url(system, url):
	auth_header = {}
	if system.startswith("lucos_"):
		key_var = f"KEY_{system.upper()}"
		key = os.environ.get(key_var)
		if not key:
			sys.exit(
				f"No {key_var} environment variable found — won't be able to authenticate against ingestion endpoint {url}"
			)
		auth_header = {"Authorization": f"key {key}"}

	print(f"Ingesting data from {url}")

	# Fetch data
	resp = session.get(
		url,
		headers={
			**auth_header,
		},
		allow_redirects=True,
	)
	resp.raise_for_status()
	content = resp.text

	# Schema.org http → https
	content = re.sub(r"http://schema\.org/", "https://schema.org/", content)
	content_type = resp.headers.get("Content-Type", "").split(";")[0]
	return (content, content_type)
