import gzip
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

import lyrelog


class Stub:
    """A local ingest: records every request and answers from a script (default: all accepted)."""

    def __init__(self):
        self.requests = []
        self.script = []
        self.lock = threading.Lock()
        self.received = threading.Condition(self.lock)
        stub = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_POST(self):
                raw = self.rfile.read(int(self.headers.get("Content-Length", 0)))
                body = gzip.decompress(raw) if self.headers.get("Content-Encoding") == "gzip" else raw
                lines = [json.loads(line) for line in body.decode().splitlines() if line.strip()]
                with stub.lock:
                    stub.requests.append({"path": self.path, "headers": dict(self.headers), "raw": body, "lines": lines})
                    answer = stub.script.pop(0) if stub.script else None
                    stub.received.notify_all()
                if answer is None:
                    answer = (200, {"accepted": len(lines), "rejected": []}, {})
                status, payload, headers = answer
                data = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
                self.send_response(status)
                for key, value in headers.items():
                    self.send_header(key, value)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"
        self.thread = threading.Thread(target=self.server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True)
        self.thread.start()

    def respond(self, status, payload=None, headers=None):
        self.script.append((status, payload if payload is not None else {}, headers or {}))

    @property
    def events(self):
        return [line for request in self.requests for line in request["lines"]]

    def wait_requests(self, count, timeout=5.0):
        with self.received:
            return self.received.wait_for(lambda: len(self.requests) >= count, timeout)

    def close(self):
        self.server.shutdown()
        self.server.server_close()


@pytest.fixture
def stub():
    server = Stub()
    yield server
    server.close()


@pytest.fixture
def make_client(stub):
    clients = []

    def make(**kwargs):
        kwargs.setdefault("flush_interval", 60)
        client = lyrelog.Client(endpoint=stub.url, api_key="lyr_ing_test", **kwargs)
        clients.append(client)
        return client

    yield make
    for client in clients:
        client.shutdown(timeout=2)


@pytest.fixture(autouse=True)
def no_default_client():
    yield
    lyrelog.shutdown(timeout=2)
