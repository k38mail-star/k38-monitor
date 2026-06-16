"""Network collection helpers."""

from __future__ import annotations

import json
import os
import platform
import re
import subprocess
import urllib.request
from typing import Any

from .base import NetworkReport

PUBLIC_TARGETS = [
    ("baidu_ms", "baidu.com"),
    ("ytb_ms", "www.youtube.com"),
    ("github_ms", "github.com"),
    ("google_ms", "google.com"),
    ("yahoo_hk_ms", "yahoo.com.hk"),
]

NODE_TARGETS = [
    ("三万八", "192.168.3.29"),
    ("小四", "192.168.3.46"),
    ("大傻", "192.168.3.55"),
    ("二傻", "192.168.3.45"),
]


def _parse_ping_output(output: str) -> float | None:
    match = re.search(r"time=([\d.]+)", output)
    if match:
        return round(float(match.group(1)), 1)
    match = re.search(r"min/avg/max[/\w]+ = ([\d.]+)\/([\d.]+)", output)
    if match:
        return round(float(match.group(2)), 1)
    return None


def _ping(host: str) -> float | None:
    try:
        result = subprocess.run(
            ["ping", "-c", "1", host],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0:
            return _parse_ping_output(result.stdout)
    except (OSError, subprocess.TimeoutExpired):
        return None
    return None


def _public_ip() -> tuple[str | None, str | None]:
    urls = [
        "https://api.ip.sb/ip",
        "https://checkip.amazonaws.com",
        "https://ipinfo.io/ip",
    ]
    for url in urls:
        try:
            with urllib.request.urlopen(url, timeout=5) as response:
                ip = response.read().decode().strip()
            if not re.match(r"^\d{1,3}(?:\.\d{1,3}){3}$", ip):
                continue
            location = None
            try:
                with urllib.request.urlopen(f"https://ipinfo.io/{ip}/json", timeout=5) as response:
                    payload = json.loads(response.read().decode())
                parts: list[str] = []
                if payload.get("city"):
                    parts.append(payload["city"])
                if payload.get("region"):
                    parts.append(payload["region"])
                if payload.get("country"):
                    country_map = {
                        "CN": "中国",
                        "US": "美国",
                        "HK": "香港",
                        "JP": "日本",
                        "SG": "新加坡",
                        "GB": "英国",
                        "DE": "德国",
                    }
                    parts.append(country_map.get(payload["country"], payload["country"]))
                if parts:
                    location = ", ".join(parts)
            except Exception:
                pass
            return ip, location
        except Exception:
            continue
    return None, None


def collect_ping_metrics(
    ping_cache: dict[str, Any] | None = None,
    node_config: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Collect public latency and external IP information."""

    result: dict[str, Any] = dict(ping_cache or {})
    for key, host in PUBLIC_TARGETS:
        ping_ok = False
        latency = _ping(host)
        if latency is not None:
            result[key] = latency
            ping_ok = True
        if not ping_ok and key != "baidu_ms":
            try:
                proc = subprocess.run(
                    [
                        "curl",
                        "-s",
                        "-o",
                        "/dev/null",
                        "-w",
                        "%{time_total}",
                        "--connect-timeout",
                        "5",
                        "https://www.youtube.com",
                    ],
                    timeout=10,
                    capture_output=True,
                    text=True,
                )
                if proc.returncode == 0 and proc.stdout.strip():
                    result[key] = round(float(proc.stdout.strip()) * 1000, 1)
            except (OSError, subprocess.TimeoutExpired, ValueError):
                pass

    public_ip, public_loc = _public_ip()
    if public_ip:
        result["public_ip"] = public_ip
    if public_loc:
        result["public_loc"] = public_loc

    node_pings: list[dict[str, Any]] = []
    for node_name, node_ip in NODE_TARGETS:
        try:
            proc = subprocess.run(
                ["ping", "-c", "1", "-W", "2", node_ip],
                capture_output=True,
                text=True,
                timeout=3,
            )
            if proc.returncode == 0:
                for line in proc.stdout.splitlines():
                    if "time=" in line:
                        ms = float(line.split("time=")[1].split()[0])
                        node_pings.append({"node": node_name, "ip": node_ip, "ms": ms})
                        break
        except Exception:
            continue
    result["node_pings"] = node_pings
    return result


def collect_network(hostname: str) -> NetworkReport:
    """Collect fast network topology information."""

    report = NetworkReport()
    if "spark" in hostname:
        peer = None
        try:
            out = subprocess.check_output(["ip", "-brief", "addr"], timeout=2, text=True)
            for line in out.splitlines():
                match = re.match(r"^(enP\S+)\s+UP\s+.*inet\s+(\S+)", line)
                if not match:
                    continue
                local_ip = match.group(2).split("/")[0]
                prefix, suffix = local_ip.rsplit(".", 1)
                peer = f"{prefix}.{102 if int(suffix) == 101 else 101}"
                break
        except Exception:
            peer = None

        peer = peer or ("192.168.100.102" if "9051" in hostname else "192.168.100.101")
        report.link200["link200_nodes"] = "spark-9051 ↔ spark-9797"
        report.link200["peer"] = peer
        latency = _ping(peer)
        if latency is not None:
            report.link200["up"] = True
            report.link200["latency"] = latency
        else:
            report.link200["up"] = False
        return report

    if platform.system() == "Darwin":
        try:
            out = subprocess.check_output(["ifconfig"], timeout=2, text=True, stderr=subprocess.DEVNULL)
            for line in out.splitlines():
                match = re.search(r"inet\s+169\.254\.(\d+)\.(\d+)", line)
                if match:
                    report.tb.append({"ip": f"169.254.{match.group(1)}.{match.group(2)}", "speed": "80Gb/s"})
                    break
        except Exception:
            pass
    return report


__all__ = ["collect_network", "collect_ping_metrics"]
