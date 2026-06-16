from __future__ import annotations

from http.server import BaseHTTPRequestHandler
from http.server import ThreadingHTTPServer
from threading import Thread
from typing import Callable


class ViewServer:
    def __init__(self, host: str, port: int, html: Callable[[], str], data: Callable[[], str]) -> None:
        self.url = f"http://{host}:{port}/"
        self.server = self._build_server(host, port, html, data)
        self.thread = Thread(target=self.server.serve_forever, daemon=True)

    def start(self) -> None:
        self.thread.start()

    def stop(self) -> None:
        self.server.shutdown()
        self.server.server_close()

    def _build_server(
        self,
        host: str,
        port: int,
        html: Callable[[], str],
        data: Callable[[], str],
    ) -> ThreadingHTTPServer:
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                if self.path.startswith("/data.json"):
                    self._send("application/json; charset=utf-8", data())
                    return
                self._send("text/html; charset=utf-8", html())

            def log_message(self, _format: str, *_args) -> None:
                return

            def _send(self, content_type: str, body: str) -> None:
                encoded = body.encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", content_type)
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(encoded)))
                self.end_headers()
                self.wfile.write(encoded)

        return ThreadingHTTPServer((host, port), Handler)
