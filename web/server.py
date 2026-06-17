"""
dltrace.web.server — Web dashboard HTTP server.

Serves the single-file HTML dashboard and optionally proxy API
requests to the backend API server.

Architecture:
  - Pure stdlib http.server (zero external deps)
  - Serves app.html on /
  - Proxies /api/* requests to the backend API (if --no-proxy not set)
"""

from __future__ import annotations

import json
import os
import sys
import urllib.request
import urllib.error
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Any

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

_HERE = os.path.dirname(os.path.abspath(__file__))
_DASHBOARD_PATH = os.path.join(_HERE, "app.html")


def _load_dashboard() -> str:
    """Load the HTML dashboard file."""
    if not os.path.exists(_DASHBOARD_PATH):
        return _generate_fallback_html()
    with open(_DASHBOARD_PATH) as f:
        return f.read()


def _generate_fallback_html() -> str:
    """Generate a minimal inline HTML if app.html is missing."""
    return """<!DOCTYPE html>
<html lang="zh-CN">
<head><meta charset="UTF-8"><title>dltrace Dashboard</title>
<style>
  body { font-family: system-ui, sans-serif; padding: 2em; background: #0d1117; color: #c9d1d9; }
  h1 { color: #58a6ff; }
  .card { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 1em; margin: 1em 0; }
  .error { color: #f85149; }
</style></head>
<body>
  <h1>dltrace Dashboard</h1>
  <div class="card error">
    <p>⚠️ app.html not found at <code>web/app.html</code></p>
    <p>Ensure the dashboard file is present, or use the full-featured dashboard
    served from <code>dltrace-api</code> on port 8899.</p>
  </div>
</body></html>"""


class DashboardHandler(BaseHTTPRequestHandler):
    """HTTP request handler for the dashboard."""

    proxy_enabled = True
    api_url = "http://localhost:8899"

    def _send_json(self, data: Any, status: int = 200) -> None:
        body = json.dumps(data, indent=2, default=str).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_html(self, html: str, status: int = 200) -> None:
        body = html.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _proxy_request(self, path: str) -> None:
        """Proxy an API request to the backend."""
        if not self.proxy_enabled:
            self._send_json({"error": "Proxy disabled"}, 503)
            return

        target = f"{self.api_url}{path}"
        try:
            req = urllib.request.Request(target, method="GET")
            with urllib.request.urlopen(req, timeout=5) as resp:
                body = resp.read()
                self.send_response(resp.status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
        except urllib.error.HTTPError as e:
            body = e.read()
            self.send_response(e.code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except urllib.error.URLError:
            self._send_json(
                {"error": f"Cannot reach API backend at {self.api_url}"},
                502,
            )

    def do_GET(self) -> None:
        path = self.path.split("?")[0]

        # Proxy API requests to backend
        if path.startswith("/api/") or path == "/status":
            self._proxy_request(path)
            return

        # Serve dashboard
        if path == "/" or path == "":
            html = _load_dashboard()
            self._send_html(html)
        else:
            self._send_json({"error": "Not found"}, 404)

    def do_OPTIONS(self) -> None:
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "*")
        self.end_headers()

    def log_message(self, fmt: str, *args: Any) -> None:
        if len(args) >= 3:
            print(f"[dltrace-web] {args[0]} {args[1]} {args[2]}")
        elif args:
            print(f"[dltrace-web] {fmt % args}")
        else:
            print(f"[dltrace-web] {fmt}")


def serve_dashboard(
    host: str = "0.0.0.0",
    port: int = 9900,
    api_url: str = "http://localhost:8899",
    no_proxy: bool = False,
) -> None:
    """Start the web dashboard HTTP server."""
    DashboardHandler.proxy_enabled = not no_proxy
    DashboardHandler.api_url = api_url

    server = HTTPServer((host, port), DashboardHandler)
    print(f"🌐 dltrace dashboard:   http://{host}:{port}")
    print(f"   API backend:          {api_url}")
    print(f"   Proxy:                {'enabled' if DashboardHandler.proxy_enabled else 'disabled'}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down...")
        server.shutdown()
