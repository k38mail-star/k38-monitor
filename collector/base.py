"""Shared collector configuration, report types, and helpers."""

from __future__ import annotations

import json
import os
import threading
import time
from collections import deque
from pathlib import Path
from typing import Any

from .types import (
    Alert,
    AlertConfig,
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
)

try:
    import psutil  # type: ignore

    HAS_PSUTIL = True
except ImportError:  # pragma: no cover - optional dependency
    psutil = None  # type: ignore[assignment]
    HAS_PSUTIL = False

DEFAULT_HISTORY_KEYS = ("cpu", "mem", "gpu", "gpu_temp", "cpu_temp", "load")


def _to_plain(value: Any) -> Any:
    if hasattr(value, "__dataclass_fields__"):
        return {key: _to_plain(getattr(value, key)) for key in value.__dataclass_fields__}
    if isinstance(value, dict):
        return {str(key): _to_plain(val) for key, val in value.items()}
    if isinstance(value, list):
        return [_to_plain(item) for item in value]
    if isinstance(value, tuple):
        return [_to_plain(item) for item in value]
    if isinstance(value, deque):
        return [_to_plain(item) for item in list(value)]
    return value


class CollectorBase:
    """Base class with history, persistence, and serialization helpers."""

    def __init__(self, config: CollectorConfig, alert_config: AlertConfig | None = None):
        self.config = config
        self.alert_config = alert_config or AlertConfig()
        self._history_lock = threading.Lock()
        self._history: dict[str, deque] = {
            key: deque(maxlen=self.config.history_len) for key in DEFAULT_HISTORY_KEYS
        }
        self._last_history_save = 0.0
        self._load_history()

    def _load_history(self) -> None:
        path = Path(self.config.history_file)
        if not path.exists():
            return
        try:
            raw = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError, UnicodeDecodeError):
            return
        now = time.time()
        with self._history_lock:
            for key, values in raw.items():
                if key not in self._history or not isinstance(values, list):
                    continue
                for item in values:
                    if not isinstance(item, list) or len(item) != 2:
                        continue
                    ts, value = item
                    if isinstance(ts, (int, float)) and now - float(ts) < self.config.history_len * 3600:
                        self._history[key].append((float(ts), float(value)))

    def _save_history(self) -> None:
        try:
            tmp = f"{self.config.history_file}.tmp"
            payload = {key: list(value) for key, value in self._history.items()}
            Path(tmp).write_text(json.dumps(payload))
            os.replace(tmp, self.config.history_file)
            self._last_history_save = time.time()
        except OSError:
            pass

    def _maybe_save_history(self, now: float) -> None:
        if now - self._last_history_save >= self.config.history_save_sec:
            self._save_history()

    def _history_snapshot(self) -> dict[str, list[list[float]]]:
        with self._history_lock:
            return {key: [list(item) for item in value] for key, value in self._history.items()}

    def _record_history(self, system: SystemInfo, now: float) -> None:
        mapping = {
            "cpu": system.cpu_pct,
            "mem": system.mem_pct,
            "gpu": system.gpu_pct,
            "gpu_temp": system.gpu_temp,
            "cpu_temp": system.cpu_temp,
            "load": system.load_1m if system.load_1m is not None else system.load,
        }
        with self._history_lock:
            for key, value in mapping.items():
                if value is not None:
                    self._history[key].append((now, float(value)))

    def save_alerts(self, payload: dict[str, Any]) -> None:
        try:
            tmp = f"{self.alert_config.alert_file}.tmp"
            Path(tmp).write_text(json.dumps(payload))
            os.replace(tmp, self.alert_config.alert_file)
        except OSError:
            pass

    def check_alerts(self, system: SystemInfo) -> list[Alert]:
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

    def _write_json_atomic(self, path: str, payload: Any) -> None:
        tmp = f"{path}.tmp"
        Path(tmp).write_text(json.dumps(_to_plain(payload), ensure_ascii=False, indent=2))
        os.replace(tmp, path)


__all__ = [
    "Alert",
    "AlertConfig",
    "CollectorBase",
    "CollectorConfig",
    "CollectorReport",
    "DEFAULT_VERSION",
    "DiskIOResult",
    "DockerReport",
    "FileReport",
    "HAS_PSUTIL",
    "JobReport",
    "NetworkReport",
    "ProcessReport",
    "SystemInfo",
    "_to_plain",
    "psutil",
]
