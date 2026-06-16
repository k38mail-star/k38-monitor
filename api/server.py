"""
dltrace.api.server — FastAPI-based REST API for K38 cluster monitoring.

Architecture:
  - FastAPI with uvicorn (async, fast)
  - Single shared Collector instance (lazy-init on first request)
  - CORS enabled for the web frontend
  - In-memory request cache (2s TTL) to avoid hammering the collector
"""

from __future__ import annotations

import json
import os
import sys
import time
from typing import Any

# Ensure parent dir is on sys.path so 'collector' is importable.
# We import 'collector' directly rather than 'dltrace.collector' because
# the dltrace.py CLI file at the package root shadows the dltrace/ directory.
_PARENT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PARENT not in sys.path:
    sys.path.insert(0, _PARENT)

try:
    import uvicorn
except ImportError:  # pragma: no cover
    uvicorn = None  # type: ignore[assignment]

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from collector import Collector, CollectorConfig, CollectorReport

# ── Globals ──────────────────────────────────────────────────────
_collector: Collector | None = None
_cache: dict[str, Any] = {"data": None, "ts": 0.0}
_CACHE_TTL = 2.0  # seconds before re-collecting


def _get_collector() -> Collector:
    """Lazy-init singleton collector."""
    global _collector
    if _collector is None:
        config = CollectorConfig()
        _collector = Collector(config=config)
    return _collector


def _get_report() -> CollectorReport:
    """Return a cached or fresh CollectorReport."""
    global _cache
    now = time.time()
    if _cache["data"] is None or (now - _cache["ts"]) > _CACHE_TTL:
        import asyncio

        collector = _get_collector()
        try:
            _cache["data"] = asyncio.run(collector.collect())
        except RuntimeError:
            loop = asyncio.get_event_loop()
            _cache["data"] = loop.run_until_complete(collector.collect())
        _cache["ts"] = time.time()
    return _cache["data"]


def _to_dict(obj: Any) -> dict[str, Any]:
    """Recursively convert dataclass instances to plain dicts."""
    if hasattr(obj, "__dataclass_fields__"):
        return {f: _to_dict(getattr(obj, f)) for f in obj.__dataclass_fields__}
    if isinstance(obj, dict):
        return {str(k): _to_dict(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_to_dict(item) for item in obj]
    if isinstance(obj, ReportEncoder):
        return obj
    return obj


# ── Low-level response builders (shared by FastAPI and fallback) ─


def _handle_status() -> dict[str, Any]:
    return {
        "status": "ok",
        "service": "dltrace-api",
        "version": "0.6.0",
        "ts": time.time(),
    }


def _handle_system() -> tuple[dict[str, Any], int]:
    try:
        report = _get_report()
        return _to_dict(report.system), 200
    except Exception as exc:
        return {"error": f"Failed to collect system metrics: {exc}"}, 500


def _handle_nodes() -> dict[str, Any]:
    try:
        report = _get_report()
        now = time.time()

        local: dict[str, Any] = {
            "name": report.hostname,
            "local": True,
            "status": "online",
            "cpu_pct": getattr(report.system, "cpu_pct", None),
            "mem_pct": getattr(report.system, "mem_pct", None),
            "gpu_pct": getattr(report.system, "gpu_pct", None),
            "gpu_temp": getattr(report.system, "gpu_temp", None),
            "disk_pct": getattr(report.system, "disk_pct", None),
            "uptime": getattr(report.system, "uptime", None),
        }

        remotes: dict[str, dict[str, Any]] = {}
        for name, data in report.nodes.items():
            remotes[name] = {
                "name": name,
                "local": False,
                "status": data.get("status", "unknown"),
                "cpu_pct": data.get("cpu_pct"),
                "mem_pct": data.get("mem_pct"),
                "gpu_pct": data.get("gpu_pct"),
                "gpu_temp": data.get("gpu_temp"),
                "disk_pct": data.get("disk_pct"),
                "uptime": data.get("uptime"),
                "ts": data.get("ts"),
            }

        config = _get_collector().config
        for name in config.node_config:
            if name not in remotes:
                remotes[name] = {
                    "name": name,
                    "local": False,
                    "status": "offline",
                    "cpu_pct": None,
                    "mem_pct": None,
                    "gpu_pct": None,
                    "gpu_temp": None,
                    "disk_pct": None,
                    "uptime": None,
                    "ts": None,
                }

        all_nodes = [local] + list(remotes.values())
        return {
            "nodes": all_nodes,
            "count": len(all_nodes),
            "ts": now,
        }
    except Exception as exc:
        return {"error": f"Failed to collect node data: {exc}"}


def _handle_history() -> dict[str, Any]:
    try:
        report = _get_report()
        return {
            "history": report.history,
            "ts": report.ts,
            "gpu_trends": report.gpu_trends,
        }
    except Exception as exc:
        return {"error": f"Failed to collect history: {exc}"}


# ── FastAPI App Factory ──────────────────────────────────────────


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    app = FastAPI(
        title="dltrace API",
        version="0.6.0",
        description="K38 cluster monitoring API",
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ── Routes ──────────────────────────────────────────────────

    @app.get("/status")
    async def status() -> dict[str, Any]:
        return _handle_status()

    @app.get("/api/system")
    async def api_system() -> JSONResponse:
        data, status_code = _handle_system()
        return JSONResponse(content=data, status_code=status_code)

    @app.get("/api/nodes")
    async def api_nodes() -> dict[str, Any]:
        return _handle_nodes()

    @app.get("/api/history")
    async def api_history() -> dict[str, Any]:
        return _handle_history()

    return app


# ── Server Runner ────────────────────────────────────────────────


def run_server(host: str = "0.0.0.0", port: int = 8899, reload: bool = False) -> None:
    """Run the API server via uvicorn."""
    print(f"🚀 dltrace API server starting on http://{host}:{port}")
    print(f"   GET /status        — Health check")
    print(f"   GET /api/system    — Latest system metrics")
    print(f"   GET /api/nodes     — Node list + status")
    print(f"   GET /api/history   — Historical time-series")

    if uvicorn is None:
        print("⚠️  uvicorn not installed. Falling back to synchronous http.server.")
        _run_fallback(host, port)
        return

    uvicorn.run(
        "dltrace.api.server:create_app",
        host=host,
        port=port,
        reload=reload,
        factory=True,
        log_level="info",
    )


# ── Fallback HTTP Server (no uvicorn/fastapi) ────────────────────


def _run_fallback(host: str = "0.0.0.0", port: int = 8899) -> None:
    """Minimal fallback HTTP server using stdlib http.server + json."""
    from http.server import HTTPServer, BaseHTTPRequestHandler

    _ROUTES: dict[str, tuple[str, ...]] = {
        "/status": (),
        "/api/system": (),
        "/api/nodes": (),
        "/api/history": (),
    }

    class _FallbackHandler(BaseHTTPRequestHandler):
        def _send_json(self, data: Any, status: int = 200) -> None:
            body = json.dumps(data, indent=2, default=str).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:
            path = self.path.split("?")[0]
            if path not in _ROUTES:
                self._send_json({"error": "Not found"}, 404)
                return
            try:
                if path == "/status":
                    data = _handle_status()
                elif path == "/api/system":
                    data, code = _handle_system()
                    self._send_json(data, code)
                    return
                elif path == "/api/nodes":
                    data = _handle_nodes()
                elif path == "/api/history":
                    data = _handle_history()
                else:
                    data = {"error": "Not found"}
                    self._send_json(data, 404)
                    return
                self._send_json(data)
            except Exception as exc:
                self._send_json({"error": str(exc)}, 500)

        def log_message(self, fmt: str, *args: Any) -> None:
            print(f"[dltrace] {args[0]} {args[1]} {args[2]}")

    server = HTTPServer((host, port), _FallbackHandler)
    print(f"   (fallback mode — stdlib http.server)")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down...")
        server.shutdown()
