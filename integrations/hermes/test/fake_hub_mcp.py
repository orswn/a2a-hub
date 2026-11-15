"""A minimal MCP server over HTTP that records each request as it arrives on the wire.

It offers the hub tools send and peers. The send input schema has no origin property, like the
hub schema that other harnesses see.
"""

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

SEND_SCHEMA = {
    "type": "object",
    "properties": {"target": {"type": "string"}, "text": {"type": "string"}},
    "required": ["target", "text"],
}
PEERS_SCHEMA = {"type": "object", "properties": {}}


class FakeHubMCP:
    def __init__(self):
        self.calls = []
        self.initialize_headers = []
        self._lock = threading.Lock()
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), self._handler())
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    @property
    def url(self):
        return f"http://127.0.0.1:{self._server.server_address[1]}/mcp"

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *exc):
        self._server.shutdown()
        self._server.server_close()

    def _answer(self, method, params, headers):
        if method == "initialize":
            with self._lock:
                self.initialize_headers.append(headers)
            return {"protocolVersion": "2025-11-25", "capabilities": {"tools": {}},
                    "serverInfo": {"name": "fake-a2a-hub", "version": "0"}}
        if method == "tools/list":
            return {"tools": [
                {"name": "send", "description": "Send a message.", "inputSchema": SEND_SCHEMA},
                {"name": "peers", "description": "List peers.", "inputSchema": PEERS_SCHEMA},
            ]}
        if method == "tools/call":
            with self._lock:
                self.calls.append({"name": params.get("name"), "arguments": params.get("arguments")})
            return {"content": [{"type": "text", "text": "queued"}], "isError": False}
        if method == "ping":
            return {}
        return None

    def _handler(self):
        fake = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def _reply(self, status, body=None):
                data = b"" if body is None else json.dumps(body).encode()
                self.send_response(status)
                if body is not None:
                    self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def do_GET(self):
                self._reply(405)

            def do_DELETE(self):
                self._reply(200)

            def do_POST(self):
                raw = self.rfile.read(int(self.headers.get("Content-Length") or 0))
                message = json.loads(raw)
                if "id" not in message:
                    self._reply(202)
                    return
                headers = {k.lower(): v for k, v in self.headers.items()}
                result = fake._answer(message.get("method"), message.get("params") or {}, headers)
                if result is None:
                    self._reply(200, {"jsonrpc": "2.0", "id": message["id"],
                                      "error": {"code": -32601, "message": "Method not found"}})
                else:
                    self._reply(200, {"jsonrpc": "2.0", "id": message["id"], "result": result})

        return Handler
