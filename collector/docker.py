"""Docker container collection."""

from __future__ import annotations

import subprocess

from .base import DockerReport


def collect_docker() -> DockerReport:
    """Collect docker container status."""

    report = DockerReport(summary="no containers")
    try:
        result = subprocess.run(
            ["docker", "ps", "-a", "--format", "{{.ID}}|{{.Image}}|{{.Status}}|{{.Names}}"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode != 0:
            return report

        containers: list[dict[str, str]] = []
        for line in result.stdout.strip().splitlines():
            if not line.strip():
                continue
            parts = line.split("|", 3)
            if len(parts) < 4:
                continue
            cid, image, status, name = parts[0][:12], parts[1], parts[2], parts[3]
            running = status.lower().startswith("up") or "Up" in status
            containers.append(
                {
                    "id": cid,
                    "image": image[:40],
                    "name": name,
                    "status": status[:60],
                    "state": "running" if running else "stopped",
                }
            )
        report.containers = containers
        running_count = sum(1 for container in containers if container["state"] == "running")
        stopped_count = len(containers) - running_count
        if containers:
            parts = []
            if running_count:
                parts.append(f"{running_count} running")
            if stopped_count:
                parts.append(f"{stopped_count} stopped")
            report.summary = ", ".join(parts) if parts else "no containers"
    except (OSError, subprocess.TimeoutExpired, FileNotFoundError):
        pass
    return report


__all__ = ["collect_docker"]
