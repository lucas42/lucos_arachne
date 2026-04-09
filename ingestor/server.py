#! /usr/local/bin/python3
import json, sys, os, traceback
from http.server import BaseHTTPRequestHandler, HTTPServer
from authorised_fetch import fetch_url
from triplestore import live_systems, replace_item_in_triplestore, delete_item_in_triplestore
from searchindex import update_searchindex, delete_doc_in_searchindex

if not os.environ.get("PORT"):
	sys.exit("\033[91mPORT not set\033[0m")
try:
	port = int(os.environ.get("PORT"))
except ValueError:
	sys.exit("\033[91mPORT isn't an integer\033[0m")

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
		try:
			if event["type"].endswith("Created") or event["type"].endswith("Added") or event["type"].endswith("Updated"):
				(content, content_type) = fetch_url(event["source"], event["url"])
				replace_item_in_triplestore(event["url"], live_systems[event["source"]], content, content_type)
				update_searchindex(event["source"], content, content_type)
				self.send_response(200, "OK")
				self.send_header("Content-type", "text/plain")
				self.end_headers()
				self.wfile.write(bytes("Updated", "utf-8"))
			elif event["type"].endswith("Deleted"):
				delete_item_in_triplestore(event["url"], live_systems[event["source"]])
				delete_doc_in_searchindex(event["source"], event["url"])
				self.send_response(200, "OK")
				self.send_header("Content-type", "text/plain")
				self.end_headers()
				self.wfile.write(bytes("Deleted", "utf-8"))
			else:
				self.send_error(404, "Webhook type Not Found")
		except Exception as error:
			traceback.print_exc()
			self.send_error(500, "Error updating datastore: "+str(error))

if __name__ == "__main__":
	server = HTTPServer(('', port), WebhookHandler)
	print("Server started on port %s" % (port))
	server.serve_forever()
