"""Remote node polling."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any
import urllib.request

REMOTE_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "k38_remote_collect.py"


async def _fetch_http(name: str, target: str, timeout: float) -> tuple[str, dict[str, Any] | None]:
    def _run() -> tuple[str, dict[str, Any] | None]:
        try:
            with urllib.request.urlopen(target, timeout=timeout) as response:
                data = json.loads(response.read().decode())
            if isinstance(data, dict):
                data.pop("nodes", None)
                data.pop("nodes_count", None)
                return name, data
        except Exception:
            pass
        return name, None

    return await asyncio.to_thread(_run)


async def _fetch_ssh(name: str, target: str, ssh_key_path: str, timeout: float) -> tuple[str, dict[str, Any] | None]:
    if not REMOTE_SCRIPT.exists():
        return name, None

    script = REMOTE_SCRIPT.read_text()
    ssh_cmd = [
        "ssh",
        "-i",
        ssh_key_path,
        "-o",
        "ConnectTimeout=3",
        target,
        "python3",
        "-",
    ]
    try:
        proc = await asyncio.create_subprocess_exec(
            *ssh_cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _stderr = await asyncio.wait_for(proc.communicate(script.encode()), timeout=timeout)
        if proc.returncode == 0 and stdout:
            data = json.loads(stdout.decode().strip())
            if isinstance(data, dict):
                data["_source"] = "ssh"
                return name, {"system": data}
    except Exception:
        pass
    return name, None


async def poll_remote_nodes(
    node_config: dict[str, str],
    ssh_key_path: str,
    timeout: float = 10.0,
    workers: int = 4,
    skip: set[str] | None = None,
) -> dict[str, dict[str, Any]]:
    """Fetch remote node snapshots concurrently."""

    skip = skip or set()
    semaphore = asyncio.Semaphore(workers)
    results: dict[str, dict[str, Any]] = {}

    async def _guarded(name: str, target: str) -> None:
        if name in skip:
            return
        async with semaphore:
            if target.startswith("http"):
                _, payload = await _fetch_http(name, target, timeout)
            else:
                _, payload = await _fetch_ssh(name, target, ssh_key_path, timeout)
            if payload:
                results[name] = payload

    await asyncio.gather(*(_guarded(name, target) for name, target in node_config.items()))
    return results


__all__ = ["poll_remote_nodes"]
