"""Process and job tracking."""

from __future__ import annotations

import json
import os
import re
import subprocess
import time
from pathlib import Path
from typing import Any

from .base import HAS_PSUTIL, JobReport, ProcessReport

try:
    import psutil  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    psutil = None  # type: ignore[assignment]

DL_TOOL_PATTERNS = {
    "wget": r"wget\s",
    "curl": r"curl\s+-[a-zA-Z]*[Oo]\s",
    "pip": r"pip\s+(install|download)\s",
    "pip3": r"pip3\s+(install|download)\s",
    "git": r"git\s+clone\s",
    "hf": r"huggingface(-cli|_hub)\s",
    "aria2": r"aria2c\s",
    "rsync": r"rsync\s",
    "scp": r"scp\s",
    "docker": r"docker\s+pull\s",
}

COMPUTE_PATTERNS = {
    "torchrun": r"torchrun\s",
    "comfy": r"[Cc]omfy[Uu][Ii]",
    "ffmpeg": r"ffmpeg\s",
    "gcc": r"(?:gcc|g\+\+)\s+-c",
    "cargo": r"cargo\s+build",
    "make": r"make\s+-j",
    "docker_build": r"docker\s+build",
    "nvidia": r"nvidia-smi\s+dmon",
    "python_ml": r"python\d*\s+\S*(?:train|infer|generate|run)\.py",
}


def _parse_etime(value: str) -> int:
    parts = value.split(":")
    if "-" in parts[0]:
        days, rest = parts[0].split("-", 1)
        total = int(days) * 86400
        parts = [rest] + parts[1:]
    else:
        total = 0
    if len(parts) == 3:
        total += int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
    elif len(parts) == 2:
        total += int(parts[0]) * 60 + int(parts[1])
    return total


def detect_processes(max_processes: int) -> list[ProcessReport]:
    """Detect download-tool processes."""

    matches: list[ProcessReport] = []
    try:
        if HAS_PSUTIL:
            for proc in psutil.process_iter(attrs=["pid", "cmdline", "name"]):  # type: ignore[name-defined]
                try:
                    cmd = " ".join(proc.info.get("cmdline") or [])
                    if not cmd:
                        continue
                    for tag, pattern in DL_TOOL_PATTERNS.items():
                        if re.search(pattern, cmd):
                            url = None
                            match = re.search(r"(https?://[^\s\"'<>]+)", cmd)
                            if match:
                                url = match.group(1).rstrip("'").rstrip('"')[:120]
                            matches.append(
                                ProcessReport(
                                    pid=int(proc.info["pid"]),
                                    tag=tag,
                                    cmd=cmd[:120],
                                    url=url,
                                )
                            )
                            break
                except (psutil.Error, AttributeError):  # type: ignore[attr-defined]
                    continue
        else:
            if os.path.isdir("/proc"):
                for entry in os.listdir("/proc"):
                    if not entry.isdigit():
                        continue
                    try:
                        raw = Path(f"/proc/{entry}/cmdline").read_bytes()  # type: ignore[name-defined]
                        cmd = raw.replace(b"\x00", b" ").decode("utf-8", errors="replace").strip()
                    except (OSError, PermissionError):
                        continue
                    if not cmd:
                        continue
                    for tag, pattern in DL_TOOL_PATTERNS.items():
                        if re.search(pattern, cmd):
                            url = None
                            match = re.search(r"(https?://[^\s\"'<>]+)", cmd)
                            if match:
                                url = match.group(1).rstrip("'").rstrip('"')[:120]
                            matches.append(
                                ProcessReport(pid=int(entry), tag=tag, cmd=cmd[:120], url=url)
                            )
                            break
            else:
                out = subprocess.check_output(
                    ["ps", "-eo", "pid,command"], timeout=5, text=True, stderr=subprocess.DEVNULL
                )
                for line in out.splitlines()[1:]:
                    line = line.strip()
                    if not line:
                        continue
                    pid_str, cmd = line.split(None, 1)
                    for tag, pattern in DL_TOOL_PATTERNS.items():
                        if re.search(pattern, cmd):
                            url = None
                            match = re.search(r"(https?://[^\s\"'<>]+)", cmd)
                            if match:
                                url = match.group(1).rstrip("'").rstrip('"')[:120]
                            matches.append(
                                ProcessReport(pid=int(pid_str), tag=tag, cmd=cmd[:120], url=url)
                            )
                            break
    except (OSError, subprocess.TimeoutExpired, subprocess.CalledProcessError, ValueError, PermissionError):
        pass
    return matches[:max_processes]


def collect_top_processes(max_processes: int) -> list[ProcessReport]:
    """Collect top CPU processes."""

    try:
        if HAS_PSUTIL:
            procs: list[ProcessReport] = []
            for proc in psutil.process_iter(attrs=["pid", "cpu_percent", "memory_percent", "cmdline"]):  # type: ignore[name-defined]
                try:
                    cpu_pct = float(proc.info.get("cpu_percent") or 0.0)
                    mem_pct = float(proc.info.get("memory_percent") or 0.0)
                    cmdline = " ".join(proc.info.get("cmdline") or [])
                    if not cmdline:
                        cmdline = getattr(proc, "name", lambda: "")() or ""
                    procs.append(
                        ProcessReport(
                            pid=int(proc.info["pid"]),
                            cpu_pct=round(cpu_pct, 1),
                            mem_pct=round(mem_pct, 1),
                            cmd=cmdline[:80],
                        )
                    )
                except (psutil.Error, AttributeError):  # type: ignore[attr-defined]
                    continue
            procs.sort(key=lambda item: item.cpu_pct or 0.0, reverse=True)
            return procs[:max_processes]

        import platform

        is_mac = platform.system() == "Darwin"
        cmd = ["ps", "-eo", "pid,%cpu,%mem,comm", "--sort=-%cpu", "--no-headers"]
        if is_mac:
            cmd = ["ps", "-eo", "pid,%cpu,%mem,command", "-r"]
        out = subprocess.check_output(cmd, timeout=3, text=True)
        lines = [line.strip() for line in out.splitlines() if line.strip()]
        result: list[ProcessReport] = []
        for line in lines[:max_processes]:
            parts = line.split(None, 3)
            if len(parts) >= 4:
                result.append(
                    ProcessReport(
                        pid=int(parts[0]),
                        cpu_pct=float(parts[1]),
                        mem_pct=float(parts[2]),
                        cmd=parts[3][:80],
                    )
                )
        return result
    except Exception:
        return []


def collect_jobs(job_file: str) -> list[JobReport]:
    """Collect running content-production or compute jobs."""

    jobs: list[JobReport] = []
    now = time.time()

    try:
        if os.path.exists(job_file):
            raw = json.loads(Path(job_file).read_text())  # type: ignore[name-defined]
            if isinstance(raw, list):
                for item in raw:
                    if not isinstance(item, dict):
                        continue
                    elapsed = now - float(item.get("started_ts", now))
                    est = float(item.get("estimated_sec") or 0)
                    report = JobReport(
                        id=str(item.get("id", "")),
                        pid=item.get("pid"),
                        name=str(item.get("name", "")),
                        type=str(item.get("type", "content")),
                        status=str(item.get("status", "running")),
                        elapsed_s=round(elapsed),
                        remaining_s=round(max(0, est - elapsed)) if est > 0 else None,
                        pct=min(100, round(elapsed / est * 100, 1)) if est > 0 else item.get("progress"),
                        started_ts=float(item.get("started_ts", now)),
                        auto=bool(item.get("auto", False)),
                    )
                    jobs.append(report)
                return jobs[:10]
    except (OSError, json.JSONDecodeError, UnicodeError, ValueError, TypeError):
        pass

    try:
        if os.name == "posix":
            ps_cmd = ["ps", "-eo", "pid,etime,cmd", "--no-headers"]
            if os.uname().sysname == "Darwin":  # type: ignore[attr-defined]
                ps_cmd = ["ps", "-eo", "pid,etime,command"]
            out = subprocess.check_output(ps_cmd, timeout=3, text=True, stderr=subprocess.DEVNULL)
            lines = out.splitlines()
            if os.uname().sysname == "Darwin":  # type: ignore[attr-defined]
                lines = lines[1:]
            for line in lines[:30]:
                parts = line.strip().split(None, 2)
                if len(parts) < 3:
                    continue
                pid_str, elapsed_str, cmd = parts
                elapsed_sec = _parse_etime(elapsed_str)
                if elapsed_sec < 30:
                    continue
                matched = None
                for tag, pattern in COMPUTE_PATTERNS.items():
                    if re.search(pattern, cmd):
                        matched = tag
                        break
                if not matched:
                    continue
                job_type = "code" if matched in {"gcc", "cargo", "make", "docker_build"} else "content"
                jobs.append(
                    JobReport(
                        id=f"auto-{pid_str}",
                        pid=int(pid_str),
                        name=f"{matched}: {cmd[:50]}",
                        type=job_type,
                        status="running",
                        elapsed_s=elapsed_sec,
                        remaining_s=None,
                        pct=None,
                        started_ts=now - elapsed_sec,
                        auto=True,
                    )
                )
    except Exception:
        pass

    return jobs[:10]


__all__ = ["collect_jobs", "collect_top_processes", "detect_processes"]
