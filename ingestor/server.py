#! /usr/local/bin/python3
import json, sys, os, traceback, threading
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, HTTPServer
from authorised_fetch import fetch_url
from triplestore import live_systems, replace_item_in_triplestore, delete_item_in_triplestore, merge_items_in_triplestore, session as triplestore_session
from searchindex import update_searchindex, delete_doc_in_searchindex, update_person_docs_in_searchindex

if not os.environ.get("PORT"):
	sys.exit("\033[91mPORT not set\033[0m")
try:
	port = int(os.environ.get("PORT"))
except ValueError:
	sys.exit("\033[91mPORT isn't an integer\033[0m")

_failed_ingestion_count = 0
_counter_lock = threading.Lock()
_executor = ThreadPoolExecutor(max_workers=10)


def _increment_failure():
	global _failed_ingestion_count
	with _counter_lock:
		_failed_ingestion_count += 1


def _process_event(event):
	"""Process a validated webhook event. Runs in a thread pool worker."""
	try:
		event_type = event["type"]
		if event_type.endswith("Created") or event_type.endswith("Added") or event_type.endswith("Updated") or event_type.endswith("Linked") or event_type.endswith("Unlinked"):
			(content, content_type) = fetch_url(event["source"], event["url"])
			replace_item_in_triplestore(event["url"], live_systems[event["source"]], content, content_type)
			update_searchindex(event["source"], content, content_type)
			# Re-compute foaf:Person closures so that e.g. a contactLinked event whose
			# new RDF includes owl:sameAs produces a merged doc and removes any
			# previously-standalone eolas Person doc.
			contacts_graph_uri = live_systems.get("lucos_contacts", "")
			update_person_docs_in_searchindex(triplestore_session, contacts_graph_uri)
		elif event_type.endswith("Deleted"):
			delete_item_in_triplestore(event["url"], live_systems[event["source"]])
			delete_doc_in_searchindex(event["source"], event["url"])
		elif event_type.endswith("Merged"):
			merge_items_in_triplestore(event["sourceUri"], event["targetUri"], live_systems[event["source"]])
			delete_doc_in_searchindex(event["source"], event["sourceUri"])
			(content, content_type) = fetch_url(event["source"], event["targetUri"])
			replace_item_in_triplestore(event["targetUri"], live_systems[event["source"]], content, content_type)
			update_searchindex(event["source"], content, content_type)
	except Exception:
		traceback.print_exc()
		_increment_failure()


def _get_valid_keys():
	"""Parse CLIENT_KEYS env var (semicolon-separated name=value pairs) into a set of valid tokens."""
	client_keys_str = os.environ.get("CLIENT_KEYS", "")
	if not client_keys_str:
		return set()
	return {pair.split("=", 1)[1] for pair in client_keys_str.split(";") if "=" in pair}

def is_authorised(headers):
	"""Return True if the request has a valid Bearer token, or if CLIENT_KEYS is not configured."""
	valid_keys = _get_valid_keys()
	if not valid_keys:
		return True
	auth_header = headers.get("Authorization", "")
	if not auth_header.startswith("Bearer "):
		return False
	token = auth_header[len("Bearer "):]
	return token in valid_keys

class WebhookHandler(BaseHTTPRequestHandler):
	def do_GET(self):
		if self.path == "/_info":
			self.infoController()
		else:
			self.send_error(404, "Page Not Found")
		self.wfile.flush()
		self.connection.close()

	def do_POST(self):
		self.post_data = self.rfile.read(int(self.headers['Content-Length']))
		if (self.path.startswith("/webhook")):
			self.webhookController()
		else:
			self.send_error(404, "Page Not Found")
		self.wfile.flush()
		self.connection.close()

	def infoController(self):
		body = json.dumps({
			"system": os.environ.get("SYSTEM", "lucos_arachne_ingestor"),
			"checks": {},
			"metrics": {
				"failed_ingestion_count": {
					"value": _failed_ingestion_count,
					"techDetail": "Number of webhook events that failed to ingest since the last restart",
				}
			},
			"ci": {"circle": "gh/lucas42/lucos_arachne"},
		}).encode("utf-8")
		self.send_response(200, "OK")
		self.send_header("Content-Type", "application/json")
		self.end_headers()
		self.wfile.write(body)

	def webhookController(self):
		if not is_authorised(self.headers):
			self.send_response(401, "Unauthorized")
			self.send_header("Content-type", "text/plain")
			self.send_header("WWW-Authenticate", "Bearer")
			self.end_headers()
			self.wfile.write(b"Invalid API Key")
			return
		try:
			event = json.loads(self.post_data)
		except json.decoder.JSONDecodeError as error:
			self.send_error(400, "Invalid json", str(error))
			return
		event_type = event.get("type", "")
		if not any(event_type.endswith(suffix) for suffix in ("Created", "Added", "Updated", "Linked", "Unlinked", "Deleted", "Merged")):
			self.send_error(404, "Webhook type Not Found")
			return
		_executor.submit(_process_event, event)
		self.send_response(202, "Accepted")
		self.send_header("Content-type", "text/plain")
		self.end_headers()
		self.wfile.write(b"Accepted")

if __name__ == "__main__":
	server = HTTPServer(('', port), WebhookHandler)
	print("Server started on port %s" % (port))
	server.serve_forever()
