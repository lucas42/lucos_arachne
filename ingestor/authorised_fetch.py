import os, sys, re
import requests

session = requests.Session()
session.headers.update({
	"User-Agent": os.environ.get("SYSTEM", ""),
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
		auth_header = {"Authorization": f"Bearer {key}"}

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

	# Validate content-type is RDF before returning — uploading non-RDF to Fuseki gives a
	# cryptic 400 and makes diagnosis hard. Fail fast with a clear error instead.
	RDF_CONTENT_TYPES = {"text/turtle", "application/rdf+xml", "application/ld+json", "application/n-triples"}
	if content_type not in RDF_CONTENT_TYPES:
		raise ValueError(
			f"Expected RDF content from <{url}> but got Content-Type {content_type!r}. "
			f"Check auth headers and that the endpoint supports content negotiation for RDF."
		)

	return (content, content_type)
