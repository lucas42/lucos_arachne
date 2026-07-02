import os, re
import requests
from urllib.parse import urlparse
from aithne import get_aithne_token

session = requests.Session()
session.headers.update({
	"User-Agent": os.environ.get("SYSTEM", ""),
	"Accept": "application/rdf+xml, text/turtle, application/ld+json",
})

_TRUSTED_HOSTS = {"localhost", "127.0.0.1", "host.docker.internal"}

def _is_trusted_host(url: str) -> bool:
	"""Return True if the URL's hostname is a trusted lucos host.

	Auth headers must only be sent to:
	- lucos production domains (*.l42.eu)
	- localhost / 127.0.0.1 (dev)
	- host.docker.internal (dev container-to-container)

	Any other hostname (e.g. an external canonical URI like id.loc.gov) is
	untrusted — even if the request originated from a lucos webhook.
	"""
	try:
		hostname = urlparse(url).hostname or ""
	except ValueError:
		return False
	return hostname in _TRUSTED_HOSTS or hostname.endswith(".l42.eu")

def fetch_url(system, url):
	def _build_auth_header(target_url: str) -> dict:
		"""Return an auth header dict for target_url, or {} if credentials must not be sent.

		For trusted lucos services, mints an aithne JWT via the client_credentials
		grant and uses it as the Bearer token.  Raises RuntimeError (propagated to
		the caller as an ingest failure) if token minting fails — never sends a
		non-JWT value that the remote service would have to reject.
		"""
		if not system.startswith("lucos_"):
			return {}
		if not _is_trusted_host(target_url):
			print(f"Skipping auth header for untrusted host: {urlparse(target_url).hostname}")
			return {}
		token = get_aithne_token()
		return {"Authorization": f"Bearer {token}"}

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
		headers={**_build_auth_header(url)},
		allow_redirects=False,
	)

	# Do first redirect manually so we can re-evaluate auth for the redirect target.
	# Credentials must never follow a redirect to an untrusted host.
	if resp.is_redirect or resp.is_permanent_redirect:
		redirect_url = resp.headers["Location"]
		redirect_url = requests.compat.urljoin(resp.url, redirect_url)
		redirect_url = map_localhost(redirect_url)
		print(f"Following redirect to {redirect_url}")

		resp = session.get(
			redirect_url,
			headers={**_build_auth_header(redirect_url)},
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
