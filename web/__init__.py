"""
dltrace.web — Web dashboard for K38 cluster monitoring.

Serves the single-file HTML dashboard (web/app.html) and optionally
proxies /api/* requests to the backend API server.

Usage:
  python3 web/__init__.py                 # Dev server on 0.0.0.0:9900
  python3 web/__init__.py --port 9901
  python3 web/__init__.py --no-proxy      # Static files only
"""

from __future__ import annotations

import argparse

from .server import serve_dashboard

__all__ = ["serve_dashboard"]


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="dltrace web dashboard server")
    parser.add_argument("--host", default="0.0.0.0", help="Bind address (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=9900, help="Listen port (default: 9900)")
    parser.add_argument("--api-url", default="http://localhost:8899",
                        help="Backend API URL (default: http://localhost:8899)")
    parser.add_argument("--no-proxy", action="store_true",
                        help="Serve static files only, don't proxy API")
    args = parser.parse_args()

    serve_dashboard(host=args.host, port=args.port, api_url=args.api_url, no_proxy=args.no_proxy)


if __name__ == "__main__":
    main()
