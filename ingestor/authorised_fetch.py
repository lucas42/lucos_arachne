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

	# In dev environment, where URLs can be referencing localhost, switch domain to the docker internal domain to allow requests between containers
	def map_localhost(url) -> str:
		if url.startswith("http://localhost:"):
			return url.replace("http://localhost:", "http://host.docker.internal:")
		return url

	url = map_localhost(url)
	print(f"Ingesting data from <{url}>")

	# Fetch data
	resp = session.get(
		url,
		headers={**auth_header},
		allow_redirects=False,
	)

	# Do first redirect manually, so auth header can be sent to next url
	if resp.is_redirect or resp.is_permanent_redirect:
		redirect_url = resp.headers["Location"]
		redirect_url = requests.compat.urljoin(resp.url, redirect_url)
		redirect_url = map_localhost(redirect_url)
		print(f"Following redirect to {redirect_url}")

		resp = session.get(
			redirect_url,
			headers={**auth_header},
			allow_redirects=True,
		)

	resp.raise_for_status()
	content = resp.text

	# Schema.org http → https
	content = re.sub(r"http://schema\.org/", "https://schema.org/", content)
	content_type = resp.headers.get("Content-Type", "").split(";")[0]
	return (content, content_type)
