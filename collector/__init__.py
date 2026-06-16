"""Collector package entrypoint."""

from __future__ import annotations

import asyncio
import os
import time
from collections import deque
from dataclasses import asdict
from typing import Any

from .base import (
    Alert,
    AlertConfig,
    CollectorBase,
    CollectorConfig,
    CollectorReport,
    DEFAULT_VERSION,
    DiskIOResult,
    DockerReport,
    FileReport,
    JobReport,
    NetworkReport,
    ProcessReport,
    SystemInfo,
    _to_plain,
)
from .types import NetworkLink
from .docker import collect_docker
from .files import FileTracker, collect_file_reports
from .jobs import collect_jobs, collect_top_processes, detect_processes
from .network import collect_network, collect_ping_metrics
from .remote import poll_remote_nodes as _poll_remote_nodes
from .system import collect_system_info

__all__ = [
    "Alert",
    "AlertConfig",
    "Collector",
    "CollectorConfig",
    "CollectorReport",
    "DiskIOResult",
    "DockerReport",
    "FileReport",
    "JobReport",
    "NetworkLink",
    "NetworkReport",
    "ProcessReport",
    "SystemInfo",
    "DownloadTracker",
]


class Collector(CollectorBase):
    """Orchestrate local, network, and remote cluster collection."""

    def __init__(self, config: CollectorConfig | None = None, alert_config: AlertConfig | None = None):
        self.config = config or CollectorConfig()
        self.alert_config = alert_config or AlertConfig()
        super().__init__(self.config, self.alert_config)
        self.trackers: dict[str, FileTracker] = {}
        self._ping_cache: dict[str, Any] = {}
        self._proc_cache: list[ProcessReport] = []
        self._docker_cache = DockerReport()
        self._diskio_cache: list[DiskIOResult] = []
        self._system_cache = SystemInfo()
        self._last_system_ts = 0.0
        self._last_diskio_ts = 0.0
        self._last_proc_ts = 0.0
        self._docker_cycle = 0
        self._ping_cycle = 0
        self._node_failures: dict[str, int] = {}
        self._node_next_retry: dict[str, float] = {}
        self._gpu_trends: dict[str, list[dict[str, float]]] = {}

    def _collect_disk_io(self) -> list[DiskIOResult]:
        """Collect disk I/O throughput."""

        out: list[DiskIOResult] = []
        try:
            if os.name == "posix" and os.uname().sysname == "Darwin":  # type: ignore[attr-defined]
                import subprocess

                result = subprocess.run(["iostat"], capture_output=True, text=True, timeout=5)
                lines = result.stdout.strip().splitlines()
                if len(lines) >= 3:
                    parts = lines[2].split()
                    headers = lines[0].split()
                    disk_names = [item for item in headers if item.startswith("disk")]
                    for index, name in enumerate(disk_names):
                        base = index * 4
                        if base + 2 < len(parts):
                            kb_t = float(parts[base])
                            tps = float(parts[base + 1])
                            out.append(DiskIOResult(device=name, kb_read=round(kb_t * tps, 1), kb_write=0.0))
            else:
                import subprocess

                result = subprocess.run(["iostat", "-x", "1", "2"], capture_output=True, text=True, timeout=5)
                lines = result.stdout.strip().splitlines()
                device_lines = []
                for line in lines:
                    parts = line.split()
                    if len(parts) >= 12 and (
                        parts[0].startswith("sd") or parts[0].startswith("nvme") or parts[0].startswith("nvm")
                    ):
                        device_lines.append(parts)
                if len(device_lines) > 1:
                    device_lines = device_lines[len(device_lines) // 2 :]
                for parts in device_lines:
                    out.append(
                        DiskIOResult(
                            device=parts[0],
                            kb_read=float(parts[5]) if len(parts) > 5 else 0.0,
                            kb_write=float(parts[9]) if len(parts) > 9 else 0.0,
                        )
                    )
        except Exception:
            pass
        return out

    def _update_system_cache(self, now: float) -> SystemInfo:
        if now - self._last_system_ts >= 5.0 or self._last_system_ts == 0:
            self._system_cache = collect_system_info()
            self._last_system_ts = now
            self._record_history(self._system_cache, now)
        return self._system_cache

    def _update_diskio_cache(self, now: float) -> list[DiskIOResult]:
        if now - self._last_diskio_ts >= float(self.config.diskio_interval) or self._last_diskio_ts == 0:
            self._diskio_cache = self._collect_disk_io()
            self._last_diskio_ts = now
        return self._diskio_cache

    def _update_ping_cache(self, now: float) -> dict[str, Any]:
        self._ping_cycle += 1
        if self._ping_cycle % self.config.ping_interval == 0 or not self._ping_cache:
            self._ping_cache = collect_ping_metrics(self._ping_cache, self.config.node_config)
        return self._ping_cache

    def _update_docker_cache(self) -> DockerReport:
        self._docker_cycle += 1
        if self._docker_cycle % self.config.docker_interval == 0 or not self._docker_cache.containers:
            self._docker_cache = collect_docker()
        return self._docker_cache

    def _update_process_cache(self, now: float) -> tuple[list[ProcessReport], list[ProcessReport]]:
        active_procs = detect_processes(self.config.max_processes)
        if now - self._last_proc_ts >= self.config.process_cache_ttl or not self._proc_cache:
            self._proc_cache = collect_top_processes(self.config.max_processes)
            self._last_proc_ts = now
        return active_procs, self._proc_cache

    def _check_alerts(self, system: SystemInfo) -> list[Alert]:
        """Check threshold alerts and persist the current alert set."""

        alerts: list[Alert] = []
        node = os.uname().nodename
        if system.gpu_temp is not None and system.gpu_temp > self.alert_config.gpu_temp_critical:
            alerts.append(
                Alert(
                    metric="gpu_temp",
                    value=float(system.gpu_temp),
                    threshold=self.alert_config.gpu_temp_critical,
                    severity="critical",
                    node=node,
                )
            )
        if system.cpu_pct is not None and system.cpu_pct > self.alert_config.cpu_pct_warning:
            alerts.append(
                Alert(
                    metric="cpu_pct",
                    value=float(system.cpu_pct),
                    threshold=self.alert_config.cpu_pct_warning,
                    severity="warning",
                    node=node,
                )
            )
        disk_pct = system.disk_pct
        if isinstance(disk_pct, str):
            try:
                disk_pct = float(disk_pct.rstrip("%"))
            except (ValueError, AttributeError):
                disk_pct = None
        if disk_pct is not None and disk_pct > self.alert_config.disk_pct_warning:
            alerts.append(
                Alert(
                    metric="disk_pct",
                    value=float(disk_pct),
                    threshold=self.alert_config.disk_pct_warning,
                    severity="warning",
                    node=node,
                )
            )
        payload = {
            "ts": time.time(),
            "ts_str": time.strftime("%H:%M:%S"),
            "alerts": [_to_plain(alert) for alert in alerts],
            "count": len(alerts),
        }
        self.save_alerts(payload)
        return alerts

    async def poll_remote_nodes(self) -> dict[str, dict[str, Any]]:
        """Poll remote nodes with progressive backoff."""

        now = time.time()
        active: dict[str, str] = {}
        for name, target in self.config.node_config.items():
            if self._node_next_retry.get(name, 0.0) > now:
                continue
            active[name] = target

        if not active:
            return {}

        results = await _poll_remote_nodes(
            active,
            self.config.ssh_key_path,
            timeout=self.config.remote_timeout,
            workers=self.config.remote_workers,
        )

        for name in active:
            if name in results:
                self._node_failures.pop(name, None)
                self._node_next_retry.pop(name, None)
                continue
            failures = self._node_failures.get(name, 0) + 1
            self._node_failures[name] = failures
            backoff = min(300.0, 10.0 * (2 ** max(0, failures - 1)))
            self._node_next_retry[name] = now + backoff

        return results

    async def collect(self) -> CollectorReport:
        """Collect one full report."""

        now = time.time()
        hostname = os.uname().nodename

        remote_task = asyncio.create_task(self.poll_remote_nodes())
        system = self._update_system_cache(now)
        active_files, tracked_total = collect_file_reports(
            self.trackers,
            self.config.watch_dirs,
            self.config.max_tracked_files,
            now,
        )
        active_procs, top_cpu_procs = self._update_process_cache(now)
        network = collect_network(hostname)
        docker = self._update_docker_cache()
        diskio = self._update_diskio_cache(now)
        ping = self._update_ping_cache(now)
        jobs = collect_jobs(self.config.job_file)
        nodes = await remote_task
        alerts = self._check_alerts(system)

        if system.gpu_temp is not None or system.gpu_power is not None:
            bucket = self._gpu_trends.setdefault(hostname, [])
            bucket.append(
                {
                    "temp": round(float(system.gpu_temp or 0.0), 1),
                    "power": round(float(system.gpu_power or 0.0), 1),
                }
            )
            if len(bucket) > 12:
                del bucket[: len(bucket) - 12]

        report = CollectorReport(
            version=self.config.version,
            ts=now,
            ts_str=time.strftime("%H:%M:%S"),
            hostname=hostname,
            system=system,
            active_files=active_files,
            active_procs=active_procs,
            network=network,
            docker=docker,
            jobs=jobs,
            diskio=diskio,
            history=self._history_snapshot(),
            alerts=alerts,
            nodes=nodes,
            ping=ping,
            gpu_trends=dict(self._gpu_trends),
            top_cpu_procs=top_cpu_procs,
            files_count=len(active_files),
            procs_count=len(active_procs),
            tracked_total=tracked_total,
            nodes_count=len(nodes) + 1,
        )
        self._maybe_save_history(now)
        return report

    def write_report(self, report: CollectorReport | dict[str, Any] | None = None, path: str | None = None) -> None:
        """Write a report to disk atomically."""

        path = path or self.config.progress_file
        if report is None:
            try:
                report = asyncio.run(self.collect())
            except RuntimeError:
                loop = asyncio.get_event_loop()
                report = loop.run_until_complete(self.collect())
        self._write_json_atomic(path, report)


DownloadTracker = Collector
