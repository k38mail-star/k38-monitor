"""
dltrace.api — REST API server for K38 cluster monitoring.

Provides HTTP endpoints to query real-time and historical metrics
collected by the collector module (dltrace/collector/).

Endpoints:
  GET /status       — Health check
  GET /api/system   — Latest system metrics (CPU, memory, GPU, disk, temp)
  GET /api/nodes    — Node list with status and remote metrics
  GET /api/history  — Historical time-series data

Usage:
  python3 api/__init__.py              # Start on 0.0.0.0:8899
  python3 api/__init__.py --port 9900  # Custom port
"""

from __future__ import annotations

import argparse
import os
import sys

# When run directly (python3 api/__init__.py), add parent to sys.path
# so package-absolute imports work.
_PARENT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PARENT not in sys.path:
    sys.path.insert(0, _PARENT)

from api.server import create_app, run_server  # type: ignore[import-untyped]

__all__ = ["create_app", "run_server"]


def main() -> None:
    """CLI entry point for the API server."""
    parser = argparse.ArgumentParser(description="dltrace API server (port 8899)")
    parser.add_argument("--host", default="0.0.0.0", help="Bind address (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=8899, help="Listen port (default: 8899)")
    parser.add_argument("--reload", action="store_true", help="Enable auto-reload for development")
    args = parser.parse_args()

    run_server(host=args.host, port=args.port, reload=args.reload)


if __name__ == "__main__":
    main()
