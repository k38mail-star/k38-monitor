"""Collector dataclasses and lightweight compatibility helpers."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any


DEFAULT_VERSION = "0.6.0"


def _expand_path(path: str) -> str:
    return os.path.expanduser(path)


@dataclass
class CollectorConfig:
    watch_dirs: list[str] = field(default_factory=lambda: ["/tmp", "~/k38_output"])
    progress_file: str = "/tmp/dltrace.json"
    poll_interval: float = 1.0
    ssh_key_path: str = "~/.ssh/k38_dgx1"
    history_len: int = 60
    max_tracked_files: int = 20
    max_processes: int = 10
    node_config: dict[str, str] = field(
        default_factory=lambda: {
            "三万八": "http://192.168.3.29:8899/api/system",
            "小四": "http://192.168.3.46:8899/api/system",
            "大傻": "jager-dgx@192.168.3.55",
            "二傻": "jager-dgx-2@192.168.3.45",
            "香港ECS": "https://dlt.k38.ai/dashboard/api/system",
        }
    )
    version: str = DEFAULT_VERSION
    history_file: str = "/tmp/dltrace_history.json"
    job_file: str = "/tmp/dltrace_jobs.json"
    remote_timeout: float = 10.0
    remote_workers: int = 4
    history_save_sec: int = 300
    diskio_interval: int = 30
    ping_interval: int = 10
    docker_interval: int = 10
    process_cache_ttl: float = 5.0

    def __post_init__(self) -> None:
        self.watch_dirs = [_expand_path(path) for path in self.watch_dirs]
        self.progress_file = _expand_path(self.progress_file)
        self.ssh_key_path = _expand_path(self.ssh_key_path)
        self.history_file = _expand_path(self.history_file)
        self.job_file = _expand_path(self.job_file)


@dataclass
class AlertConfig:
    gpu_temp_critical: float = 80.0
    cpu_pct_warning: float = 90.0
    disk_pct_warning: float = 90.0
    alert_file: str = "/tmp/dltrace_alerts.json"

    def __post_init__(self) -> None:
        self.alert_file = _expand_path(self.alert_file)


@dataclass
class SystemInfo:
    cpu_pct: float | None = None
    mem_pct: float | None = None
    mem_total_gb: float | None = None
    mem_used_gb: float | None = None
    disk_pct: float | None = None
    disk_total: str | None = None
    disk_used: str | None = None
    gpu_pct: float | None = None
    gpu_temp: float | None = None
    gpu_mem_pct: float | None = None
    gpu_info: str | None = None
    gpu_vram: str | None = None
    gpu_power: float | None = None
    gpu_clk_graphics: float | None = None
    gpu_clk_memory: float | None = None
    fan_rpm: list[int] = field(default_factory=list)
    fan_rpm_avg: int | None = None
    cpu_temp: float | None = None
    load: float | str | None = None
    uptime: int | str | None = None
    disk_free: str | None = None
    load_1m: float | None = None
    uptime_str: str | None = None
    cpu_user: int | None = None
    cpu_idle: int | None = None
    cpu_total: int | None = None
    gpu_mem_used: float | None = None
    gpu_mem_total: float | None = None
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class FileReport:
    name: str
    size_mb: float
    speed_mb: float
    pct: int
    status: str
    tag: str
    url: str | None = None
    path: str | None = None
    growth_count: int = 0
    age_s: float = 0.0
    idle_s: float = 0.0


@dataclass
class ProcessReport:
    pid: int | None = None
    tag: str = ""
    cmd: str = ""
    url: str | None = None
    cpu_pct: float | None = None
    mem_pct: float | None = None
    kind: str | None = None


@dataclass
class NetworkLink:
    up: bool | None = None
    latency: float | None = None
    peer: str | None = None
    nodes: list[str] = field(default_factory=list)

    def __getitem__(self, key: str) -> Any:
        if key == "link200_nodes":
            return self.nodes
        return getattr(self, key)

    def __setitem__(self, key: str, value: Any) -> None:
        if key == "link200_nodes":
            if value is None:
                self.nodes = []
            elif isinstance(value, list):
                self.nodes = [str(item) for item in value]
            else:
                self.nodes = [str(value)]
            return
        setattr(self, key, value)


@dataclass
class NetworkReport:
    link200: NetworkLink = field(default_factory=NetworkLink)
    tb: list[dict[str, Any]] = field(default_factory=list)
    baidu_ms: float | None = None
    ytb_ms: float | None = None
    github_ms: float | None = None
    google_ms: float | None = None
    yahoo_hk_ms: float | None = None
    public_ip: str | None = None
    public_loc: str | None = None
    node_pings: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class DockerReport:
    containers: list[dict[str, Any]] = field(default_factory=list)
    summary: str = ""


@dataclass
class JobReport:
    id: str = ""
    pid: int | None = None
    name: str = ""
    type: str = ""
    status: str = "running"
    elapsed_s: int | None = None
    remaining_s: int | None = None
    pct: float | int | None = None
    started_ts: float | None = None
    auto: bool = False


@dataclass
class DiskIOResult:
    device: str
    kb_read: float = 0.0
    kb_write: float = 0.0


@dataclass
class Alert:
    metric: str
    value: float
    threshold: float
    severity: str
    node: str


@dataclass
class CollectorReport:
    version: str
    ts: float
    ts_str: str
    hostname: str
    system: SystemInfo
    active_files: list[FileReport]
    active_procs: list[ProcessReport]
    network: NetworkReport
    docker: DockerReport
    jobs: list[JobReport]
    diskio: list[DiskIOResult]
    history: dict[str, list[list[float]]]
    gpu_trends: dict[str, list[dict[str, float]]]
    alerts: list[Alert]
    top_cpu_procs: list[ProcessReport]
    nodes: dict[str, dict[str, Any]] = field(default_factory=dict)
    ping: dict[str, Any] = field(default_factory=dict)
    files_count: int = 0
    procs_count: int = 0
    tracked_total: int = 0
    nodes_count: int = 1

