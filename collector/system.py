"""System information collector - cross-platform (macOS/Linux)."""

from __future__ import annotations

import json
import os
import platform
import re
import subprocess
import time
from typing import Any

from .types import SystemInfo

try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    psutil = None  # type: ignore
    HAS_PSUTIL = False


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _collect_gpu_linux() -> dict:
    gpus = {}
    try:
        out = subprocess.check_output(
            ["nvidia-smi", "--query-gpu=index,name,temperature.gpu,utilization.gpu,memory.used,memory.total,clocks.current.graphics,clocks.current.memory",
             "--format=csv,noheader,nounits"],
            timeout=5, text=True
        )
        for line in out.strip().splitlines():
            parts = [p.strip() for p in line.split(",")]
            if len(parts) >= 8:
                gpus[int(parts[0])] = {
                    "name": parts[1], "temp": _safe_float(parts[2]),
                    "util": _safe_float(parts[3]), "mem_used": _safe_float(parts[4]),
                    "mem_total": _safe_float(parts[5]), "clk_gpu": _safe_float(parts[6]),
                    "clk_mem": _safe_float(parts[7]),
                }
    except Exception:
        pass
    return gpus


def _collect_gpu_macos() -> dict:
    gpus = {}
    try:
        out = subprocess.check_output(
            ["system_profiler", "SPDisplaysDataType", "-json"],
            timeout=5, text=True
        )
        data = json.loads(out)
        for item in data.get("SPDisplaysDataType", []):
            idx = len(gpus)
            gpus[idx] = {"name": item.get("sppci_model", "Apple GPU")}
    except Exception:
        pass
    return gpus


def _collect_cpu_temp() -> float | None:
    if platform.system() == "Darwin":
        try:
            out = subprocess.check_output(["pmset", "-g", "therm"], timeout=3, text=True)
            m = re.search(r"CPU_THERMAL_LEVEL\s*=\s*(\d+)", out)
            if m:
                return _safe_float(m.group(1))
        except Exception:
            pass
    return None


def _collect_fan_info() -> list[int]:
    fans = []
    if platform.system() == "Darwin":
        try:
            out = subprocess.check_output(["pmset", "-g", "therm"], timeout=3, text=True)
            for m in re.finditer(r"Fan (\d+)\s*=\s*(\d+)", out):
                fans.append(int(m.group(2)))
        except Exception:
            pass
    return fans


def _collect_linux() -> dict:
    result = {}
    try:
        with open("/proc/uptime") as f:
            parts = f.read().split()
            result["uptime"] = _safe_float(parts[0]) if parts else 0.0
    except Exception:
        pass
    try:
        with open("/proc/loadavg") as f:
            parts = f.read().split()
            result["load_1m"] = _safe_float(parts[0]) if parts else None
            result["load_5m"] = _safe_float(parts[1]) if len(parts) > 1 else None
            result["load_15m"] = _safe_float(parts[2]) if len(parts) > 2 else None
    except Exception:
        pass
    return result


def _collect_macos() -> dict:
    result = {}
    try:
        out = subprocess.check_output(["uptime"], timeout=3, text=True)
        m = re.search(r"load averages: ([\d.]+)\s+([\d.]+)\s+([\d.]+)", out)
        if m:
            result["load_1m"] = _safe_float(m.group(1))
            result["load_5m"] = _safe_float(m.group(2))
            result["load_15m"] = _safe_float(m.group(3))
    except Exception:
        pass
    return result


def collect_system_info() -> SystemInfo:
    """Collect cross-platform system metrics returning a SystemInfo dataclass."""
    is_linux = platform.system() == "Linux"
    is_macos = platform.system() == "Darwin"

    # CPU
    cpu_pct = 0.0
    cpu_count = os.cpu_count() or 1
    if HAS_PSUTIL:
        try:
            cpu_pct = psutil.cpu_percent(interval=0.1)
        except Exception:
            pass
    cpu_user = cpu_idle = cpu_total = None
    if HAS_PSUTIL:
        try:
            times = psutil.cpu_times_percent()
            cpu_user = int(times.user)
            cpu_idle = int(times.idle)
            cpu_total = 100
        except Exception:
            pass

    # Memory
    mem_pct = None
    mem_total_gb = None
    mem_used_gb = None
    if HAS_PSUTIL:
        try:
            mem = psutil.virtual_memory()
            mem_total_gb = round(mem.total / (1024 ** 3), 1)
            mem_used_gb = round(mem.used / (1024 ** 3), 1)
            mem_pct = round(mem.percent, 1)
        except Exception:
            pass

    # Disk
    disk_pct = None
    disk_total_s = None
    disk_used_s = None
    disk_free_s = None
    if HAS_PSUTIL:
        try:
            du = psutil.disk_usage("/")
            disk_total_s = _fmt_bytes(du.total)
            disk_used_s = _fmt_bytes(du.used)
            disk_free_s = _fmt_bytes(du.free)
            disk_pct = du.percent
        except Exception:
            pass

    # Platform-specific
    extra = _collect_linux() if is_linux else (_collect_macos() if is_macos else {})

    # GPU
    gpus = _collect_gpu_linux() if is_linux else (_collect_gpu_macos() if is_macos else {})
    gpu_info = "; ".join(f"{g.get('name','?')}" for g in gpus.values()) if gpus else None
    gpu_temp = next((g.get("temp") for g in gpus.values() if g.get("temp") is not None), None)
    gpu_pct = next((g.get("util") for g in gpus.values() if g.get("util") is not None), None)
    gpu_mem_used = next((g.get("mem_used") for g in gpus.values() if g.get("mem_used") is not None), None)
    gpu_mem_total = next((g.get("mem_total") for g in gpus.values() if g.get("mem_total") is not None), None)
    if gpu_mem_used is not None and gpu_mem_total and gpu_mem_total > 0:
        gpu_mem_pct = round(gpu_mem_used / gpu_mem_total * 100, 1)
    else:
        gpu_mem_pct = None

    # Temperature / fans
    cpu_temp = _collect_cpu_temp()
    fans = _collect_fan_info()
    fan_rpm_avg = round(sum(fans) / len(fans), 0) if fans else None

    # Uptime
    uptime_secs = extra.get("uptime", None)
    if uptime_secs is None and HAS_PSUTIL:
        try:
            uptime_secs = time.time() - psutil.boot_time()
        except Exception:
            uptime_secs = 0.0
    uptime_h = int(uptime_secs // 3600) if uptime_secs else 0
    uptime_m = int((uptime_secs % 3600) // 60) if uptime_secs else 0
    uptime_str = f"{uptime_h}h{uptime_m}m" if uptime_secs else "0h0m"
    load_val = extra.get("load_1m", None)

    return SystemInfo(
        cpu_pct=round(cpu_pct, 1),
        cpu_user=cpu_user,
        cpu_idle=cpu_idle,
        cpu_total=cpu_total,
        mem_pct=mem_pct,
        mem_total_gb=mem_total_gb,
        mem_used_gb=mem_used_gb,
        disk_pct=disk_pct,
        disk_total=disk_total_s,
        disk_used=disk_used_s,
        disk_free=disk_free_s,
        gpu_pct=gpu_pct,
        gpu_temp=gpu_temp,
        gpu_mem_pct=gpu_mem_pct,
        gpu_info=gpu_info,
        gpu_mem_used=gpu_mem_used,
        gpu_mem_total=gpu_mem_total,
        gpu_clk_graphics=None,
        gpu_clk_memory=None,
        fan_rpm=fans,
        fan_rpm_avg=fan_rpm_avg,
        cpu_temp=cpu_temp,
        load=load_val,
        uptime=uptime_h,
        uptime_str=uptime_str,
        load_1m=extra.get("load_1m"),
        extra={},
    )


def _fmt_bytes(b: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if b < 1024:
            return f"{b:.1f}{unit}"
        b /= 1024
    return f"{b:.1f}PB"
