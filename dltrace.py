#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
dltrace - K38集群监控面板
Zero-dependency real-time cluster monitor for K38 5-node heterogeneous cluster.

Tracks system resources, downloads, network links, and content production tasks.
Pure Python stdlib, zero external dependencies.

Usage:
  dltrace daemon                    Start monitoring daemon
  dltrace web [--ssh HOST] [...]    Start web dashboard
  dltrace watch                     One-shot status dump
  dltrace deploy USER@HOST [...]    Deploy to remote nodes
"""

import argparse
import glob
import json
import os
import re
import subprocess
import sys
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Any
from http.server import HTTPServer, BaseHTTPRequestHandler


# ── 动态节点配置 ──────────────────────────────────────────────
# Logo data URIs for public service matrix
LOGO_DATA = {
    "baidu": "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAABGdBTUEAALGPC/xhBQAAACBjSFJNAAB6JgAAgIQAAPoAAACA6AAAdTAAAOpgAAA6mAAAF3CculE8AAAARGVYSWZNTQAqAAAACAABh2kABAAAAAEAAAAaAAAAAAADoAEAAwAAAAEAAQAAoAIABAAAAAEAAAAQoAMABAAAAAEAAAAQAAAAADRVcfIAAAHJaVRYdFhNTDpjb20uYWRvYmUueG1wAAAAAAA8eDp4bXBtZXRhIHhtbG5zOng9ImFkb2JlOm5zOm1ldGEvIiB4OnhtcHRrPSJYTVAgQ29yZSA2LjAuMCI+CiAgIDxyZGY6UkRGIHhtbG5zOnJkZj0iaHR0cDovL3d3dy53My5vcmcvMTk5OS8wMi8yMi1yZGYtc3ludGF4LW5zIyI+CiAgICAgIDxyZGY6RGVzY3JpcHRpb24gcmRmOmFib3V0PSIiCiAgICAgICAgICAgIHhtbG5zOmV4aWY9Imh0dHA6Ly9ucy5hZG9iZS5jb20vZXhpZi8xLjAvIj4KICAgICAgICAgPGV4aWY6Q29sb3JTcGFjZT4xPC9leGlmOkNvbG9yU3BhY2U+CiAgICAgICAgIDxleGlmOlBpeGVsWERpbWVuc2lvbj42NDwvZXhpZjpQaXhlbFhEaW1lbnNpb24+CiAgICAgICAgIDxleGlmOlBpeGVsWURpbWVuc2lvbj42NDwvZXhpZjpQaXhlbFlEaW1lbnNpb24+CiAgICAgIDwvcmRmOkRlc2NyaXB0aW9uPgogICA8L3JkZjpSREY+CjwveDp4bXBtZXRhPgohBDnEAAACk0lEQVQ4EaVTS0hUURj+7mPunWbU0Sl7OG0SM1PMRKRty1oURRRB2EOidhmkUQNiL4ImahEt2rVpLxSBWFpjgaljhmIa5jjjC3VGRkadO/cx93TOHb2ktYl+OPd1/v873/f9/wUhhKcrQNe/BqvhQS9/FHd9VkhnMGUBKmmTtL5ZISM/1L8dEGAAG6K7RyGlVVFSciBCBodU8vJVkuwsDpPDR6bJQszYkMteeGyK/gEVSppA14DhUQ3fRzQ4nTwmpw1MRPRN2YC4/mVm1kA4YqCiXIJrCwfTBMr3SVAUE6pKUFbqgMfD40OXgtoaGTnutbMZjeRyhhw/PUt8eycsyqGvadLTp7AtK9o7VsnPsEau34yRHXsmiL8lTkwzu2cxmKL0Rsd0SA4OwU8KLp7LBWP06GkC+R4B9edzYRjAl7403C6O3lVoGoEsc1kJRbtElBQ7MPBNRVWljPhiBmcvzGM8rEOgRwwMqnj+pNDKiUQN1FTLcEicpZ5jRNjT9IyBsXEd5WUSGprirDsQRQ7dvWmAZlypz8OlujwMDWs4VCsjNyfrgW3ibp9oFVxrjCMS1XG7MR+TUxlEJg0kkxm8bUtB0wn8TV6Iwrr1gN3GWDyDusvzqKyQ8OJZIaoPOi0W95u98BYIeHjXi/b3Kdy4Fad+WKQtFBug46NiaT51wg3/nUW0vl6B7OQgS0DGpHIEDi4Xj7Z3KZpHHV0LG8DhANJ0gDqDCu41b8Wxo24sUjOTyyYCD7aBSdSpBI56J/wmwTYxkTBxtWEBoX4V+6mR3gIeS0smdEqXSVimQGO0K2dO5qDF77VBGECAsmlijFZWTfSGsj32FYnw0fYyvVE6J3NzBrYXCnQKneBt3njMjPqv3/kXJUADijW0BeUAAAAASUVORK5CYII=",
    "youtube": "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAAXNSR0IArs4c6QAAAERlWElmTU0AKgAAAAgAAYdpAAQAAAABAAAAGgAAAAAAA6ABAAMAAAABAAEAAKACAAQAAAABAAAAEKADAAQAAAABAAAAEAAAAAA0VXHyAAAAw0lEQVQ4EaVTiw2FIAwsL28ANpARGIEV3MAR3UBHcATcgA14LfLRgI0+SAjl6J09qMJ7Dz3j00Mm7jcLCCEx1nFvMn4N1rjdwHsXYrKAJiacZObNnIJ9JOmXxPNHNN2BimX9swSB5LsWsBbAmBoviOJfYRgAlgVgnrFOVWiniBdIiVKmqFp5gX0HGMfDBtlpDOqDrYEfkMbrce72GA8sVWBvM3gy0aih+hpJ5J/p2spYO8hGZWvEcisXgUb2E4h/hQcKPzl8hRVGXE8aAAAAAElFTkSuQmCC",
    "github": "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAB00lEQVR4nIVSTYsTQRB9ryYKiyCJ9KQ3H64egrtnz178DXvxD+jRgzf/gHhaQRYR42UVvAmKB49eRfAmiy7oQcgmk8kvyGampDrTcYi7bkEzNVX9ql6/KmLNnHPdBvlUyV2qhpiSoOrbher92Wx2XL/P+o937gVF7moFXDeSkLIcHs9m9/4psJmm3xXYtktnFRASpaqBfkzyfCfEYGDnnhn4lJbLU3XSv99tw4R4mqabQo5DQhVZnrPX693Q+fwWi+IwoDc2doqi+DKZTA59mmpgCRibTiMB9gNlE2rZ9+JoNDoCYCfa5/iKyAiqMKyY2jGgIh8AzHG2lQocRI0MK6tRAUiK4uV/wHESr1a+ao0SgBPyynkFZLFoGtsossSfMGNg/7wCJblHCcMLzxaSH5d+KHSp026/GQwGl9eBFvPePwd5LWpgWNrqJuSomsKJql6whQHwZDydPjDHe/8Iqg/rbM0ryrInYbdFhiGp+guq70OHJHlXI/Bt9e7IlhwadqVgx/vfqnrVkgQ+qcidLMumlnPO3UzIr7U1/5nl+SCIGguMs2yLwOtqVW+LSNj1qmPDYOGQBxF8qrVara2u94/7/f5qpN1u13Xa7b1ms3l9/f4fJfbNAEJQCagAAAAASUVORK5CYII=",
    "google": "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAABGdBTUEAALGPC/xhBQAAACBjSFJNAAB6JgAAgIQAAPoAAACA6AAAdTAAAOpgAAA6mAAAF3CculE8AAAARGVYSWZNTQAqAAAACAABh2kABAAAAAEAAAAaAAAAAAADoAEAAwAAAAEAAQAAoAIABAAAAAEAAAAQoAMABAAAAAEAAAAQAAAAADRVcfIAAAHJaVRYdFhNTDpjb20uYWRvYmUueG1wAAAAAAA8eDp4bXBtZXRhIHhtbG5zOng9ImFkb2JlOm5zOm1ldGEvIiB4OnhtcHRrPSJYTVAgQ29yZSA2LjAuMCI+CiAgIDxyZGY6UkRGIHhtbG5zOnJkZj0iaHR0cDovL3d3dy53My5vcmcvMTk5OS8wMi8yMi1yZGYtc3ludGF4LW5zIyI+CiAgICAgIDxyZGY6RGVzY3JpcHRpb24gcmRmOmFib3V0PSIiCiAgICAgICAgICAgIHhtbG5zOmV4aWY9Imh0dHA6Ly9ucy5hZG9iZS5jb20vZXhpZi8xLjAvIj4KICAgICAgICAgPGV4aWY6Q29sb3JTcGFjZT4xPC9leGlmOkNvbG9yU3BhY2U+CiAgICAgICAgIDxleGlmOlBpeGVsWERpbWVuc2lvbj4zMjwvZXhpZjpQaXhlbFhEaW1lbnNpb24+CiAgICAgICAgIDxleGlmOlBpeGVsWURpbWVuc2lvbj4zMjwvZXhpZjpQaXhlbFlEaW1lbnNpb24+CiAgICAgIDwvcmRmOkRlc2NyaXB0aW9uPgogICA8L3JkZjpSREY+CjwveDp4bXBtZXRhPgqWsr5jAAACfElEQVQ4EXVTz0tUURT+3o9RZ0YyiSIrpNoESZSYoRmEES5D002Bk1AKZZuC8h+Qglpki2ihLiKERIlq1cIi0yJIEfwBif1QtEVClI7z8713b995k+JYHvju3HPud8797pnzgA2mtd7qum4TMex5XkKglBpi/IKcbaBnu0yqJ2mC2MzGyTm7PstYdXjTdcM07zBgOWMfkXwzAO/bFx5rWHv3I6+6BoHScqE7VNRmWdY9cfwCvK6O+z6kktbKww4kXvRDxVZgBALCgXYcGLl5CF9qRTjSDPI9FmmwbfuZTaeAnHa5KPqAyf2PYYTCCJ5pQE55hV8g/WEY7tfPyCk75vuGYVCA1c7c11KtidDewqD+ee6g/nGyXFOBhLItupTtZ7wIKGVY9t70ZZ3qhY51tawRFXeJtNZxgUOklA/Hy1CY+9ampjLRpZcnYRYCwROHfJmyxFMaLd1JRJMaJrslSLvA+eMBHyxz1NYk+p3kr0XHUJL6rwknxoLLCWApLlkZk6KjsrXyS7DgmniyMMt+ZiyUa6C7OYjeqyH0XAnhQJEJT2kUFZo+gc0cMbl0ivdrRyNuxqtxa3YOfTMvMwSuefwngznA0LSHqXlgW76Bw8VrBToNvmMLee+pvOT2SBd6Pj1F2A7idHEVKncegWmaGF2cwKsJIPW9HpGK7WitkfnQk3xwlf98FqllpD+p0tb9sUe+gqgTQ8CUHsvouQgFbNTuuoi2yjpR5XJyZZCer/YPDFzje+6CQzKyOIWB+XeY+T3HOQH2FezGqT2VqCoqZTntKqVvcJA6/OrrF/lQqGac+L8pJWeidnMjQT7nCIdkkIj9xSDjjYSMfZb9ARmRtLGfbP3kAAAAAElFTkSuQmCC",
    "yahoo": "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAABGdBTUEAALGPC/xhBQAAACBjSFJNAAB6JgAAgIQAAPoAAACA6AAAdTAAAOpgAAA6mAAAF3CculE8AAAARGVYSWZNTQAqAAAACAABh2kABAAAAAEAAAAaAAAAAAADoAEAAwAAAAEAAQAAoAIABAAAAAEAAAAQoAMABAAAAAEAAAAQAAAAADRVcfIAAAHJaVRYdFhNTDpjb20uYWRvYmUueG1wAAAAAAA8eDp4bXBtZXRhIHhtbG5zOng9ImFkb2JlOm5zOm1ldGEvIiB4OnhtcHRrPSJYTVAgQ29yZSA2LjAuMCI+CiAgIDxyZGY6UkRGIHhtbG5zOnJkZj0iaHR0cDovL3d3dy53My5vcmcvMTk5OS8wMi8yMi1yZGYtc3ludGF4LW5zIyI+CiAgICAgIDxyZGY6RGVzY3JpcHRpb24gcmRmOmFib3V0PSIiCiAgICAgICAgICAgIHhtbG5zOmV4aWY9Imh0dHA6Ly9ucy5hZG9iZS5jb20vZXhpZi8xLjAvIj4KICAgICAgICAgPGV4aWY6Q29sb3JTcGFjZT4xPC9leGlmOkNvbG9yU3BhY2U+CiAgICAgICAgIDxleGlmOlBpeGVsWERpbWVuc2lvbj40ODwvZXhpZjpQaXhlbFhEaW1lbnNpb24+CiAgICAgICAgIDxleGlmOlBpeGVsWURpbWVuc2lvbj40ODwvZXhpZjpQaXhlbFlEaW1lbnNpb24+CiAgICAgIDwvcmRmOkRlc2NyaXB0aW9uPgogICA8L3JkZjpSREY+CjwveDp4bXBtZXRhPgrZdbzTAAABsUlEQVQ4EYWTOy9EQRTHf3d22SBRsEuEkKyCxGNVEiIaGolQ+AI+g1Yi0fgYOoXGYyMa5RZCIpZ4RSEeQWIRhffaO87suNm99orTzMx//v9zzpwzx5nu1P0hxTIOdVpTYu4XuK6FHQWhMDgOCDdDiPGwUiRFXBsozkEsDtX14kCcf7zAzXFeLBJiuCTDsgkUG4fhcpiYg6Yum8HmAlymBY/k/eFooiog6zw79wnxXmjstOIvOaeTkr48wzOjLTp68M8qqfWM2/ca5GwLrg+kBmV+XqADU7i6VmgbLJB3VyEn+G8LdJDLQvcIRKos/f4cTlO2Jv860NKyyhpIjBao++vw+uR/v3dbkoGJ3jEsrWmxlOw77K3Z/nui4lW+hd9MlT+eITVvU76/gMcrqXYJ0+p8sOl9eaUl3xzB4QZ8vglW4Q9SfPI5cCX9oSkYmLSU7UVYnS2ml+4LNZDoSnrc2lcgNff8/Dq5+8uU/BdrsjH9310B8+tM8XaW7GqGJ8gM7MwkdEYGJeoFMW1saLcDc3siWYWCpIKbgJoHpRVjQsl4UUwXrg/t1P0lznM1dzLmY98XE4KRGYksiAAAAABJRU5ErkJggg==",
}

# Services: (key, display_name, logo_key, field_name, green_thresh, yellow_thresh)
PUBLIC_SERVICES = [
    ("baidu", "百度", "baidu", "baidu_ms", 50, 200),
    ("youtube", "YouTube", "youtube", "ytb_ms", 200, 500),
    ("github", "GitHub", "github", "github_ms", 50, 200),
    ("google", "Google", "google", "google_ms", 100, 300),
    ("yahoo", "Yahoo", "yahoo", "yahoo_hk_ms", 50, 200),
]

def _load_node_config(hardcoded: dict) -> dict:
    """
    加载HTTP拉取的远程节点配置。
    优先级: 环境变量 DLTRACE_NODES > ~/.dltrace_nodes.json > hardcoded
    环境变量格式: JSON字符串 e.g. '{"三万八":"http://...",...}'
    """
    # 1. 环境变量
    env_val = os.environ.get("DLTRACE_NODES")
    if env_val:
        try:
            parsed = json.loads(env_val)
            if isinstance(parsed, dict) and len(parsed) > 0:
                return parsed
        except (json.JSONDecodeError, TypeError):
            pass

    # 2. 配置文件
    config_path = os.path.expanduser("~/.dltrace_nodes.json")
    if os.path.exists(config_path):
        try:
            with open(config_path) as f:
                parsed = json.load(f)
            if isinstance(parsed, dict) and len(parsed) > 0:
                return parsed
        except (json.JSONDecodeError, OSError):
            pass

    # 3. 硬编码默认值
    return dict(hardcoded)

import socketserver

# 🔒 禁用__pycache__字节码缓存 (根因: pyc缓存导致代码更新后仍跑旧版本)
sys.dont_write_bytecode = True


class ThreadedHTTPServer(socketserver.ThreadingMixIn, HTTPServer):
    """多线程HTTP服务器，带线程上限防止自引用死锁"""
    allow_reuse_address = True
    daemon_threads = True
    _max_workers = 32
    _worker_count = 0
    _worker_lock = threading.Lock()

    def process_request(self, request, client_address):
        """带线程上限: 超过上限时静默丢弃连接"""
        with self._worker_lock:
            if self._worker_count >= self._max_workers:
                try:
                    request.close()
                except OSError:
                    pass
                return
            self._worker_count += 1
        t = threading.Thread(target=self._process_request_thread_wrapper,
                             args=(request, client_address))
        t.daemon = True
        t.start()

    def _process_request_thread_wrapper(self, request, client_address):
        try:
            self.finish_request(request, client_address)
            self.shutdown_request(request)
        except Exception:
            self.handle_error(request, client_address)
            self.shutdown_request(request)
        finally:
            with self._worker_lock:
                self._worker_count -= 1


# ════════════════════════════════════════════
# 配置常量
# ════════════════════════════════════════════

__version__ = "0.4.1"

# Web Authentication
DLTRACE_TOKEN = os.environ.get("DLTRACE_TOKEN", "")

# ── 文件路径 ──
PROGRESS_FILE  = "/tmp/dltrace.json"
PID_FILE       = "/tmp/dltrace.pid"
HISTORY_FILE   = "/tmp/dltrace_history.json"

# ── 网络 ──
DEFAULT_PORT   = 8899
WEB_BIND       = os.environ.get("DLTRACE_BIND", "0.0.0.0")
SSH_TIMEOUT    = 3
PROC_INTERVAL  = 5   # /proc扫描每N轮一次(高负载时降低频率)
PING_INTERVAL  = 10  # ping/出口IP每N轮采一次
POLL_INTERVAL  = 1.0   # 1秒轮询, 捕获绝大多数下载

# ── 限制 ──
MAX_TRACKED_FILES = 20
MAX_PROCESSES     = 10
HISTORY_LEN       = 60
HISTORY_SAVE_SEC  = 300   # 历史文件最长保留5分钟
LOG_MAX_SIZE      = 1_000_000
GPU_TREND: dict[str, deque] = {}  # GPU趋势缓存, keyed by node_key
MAX_TREND  = 12          # GPU趋势最大样本数

# ── 速度窗口 ──
SPEED_WINDOW_SEC  = 60
GROWTH_MIN_COUNT  = 1    # 至少1次增长即显示(内网快文件秒级也能捕获)
STALE_TIMEOUT     = 120  # 无活动超时(秒)
SHOW_DONE_SEC     = 30   # 已完成文件保留显示(秒), 解决秒级下载漏报
DEAD_TIMEOUT      = 30   # 新文件30秒无增长视为死

# 监控目录(自动检测常见下载路径)
DEFAULT_WATCH_DIRS = [
    "/tmp",
    os.path.expanduser("~"),
    os.path.expanduser("~/Downloads"),
    os.path.expanduser("~/downloads"),
    os.path.expanduser("~/k38_output"),
]

# 跳过配置
SKIP_NAMES = {".", "..", "__pycache__", "node_modules", ".git", ".cache",
              ".npm", ".conda", ".local", ".config", "lost+found", "snap"}
SKIP_EXTS = {".pyc", ".pyo", ".log", ".tmp", ".swp", ".lock", ".part", ".aria2"}
SKIP_PATH_PATTERNS = [r"/\.git/", r"/miniforge3/", r"/anaconda3/", r"/snap/"]

# Alert thresholds
ALERT_GPU_TEMP_CRITICAL = 80
alert_cpu_pct_warning = 90
alert_disk_pct_warning = 90
ALERT_FILE = "/tmp/dltrace_alerts.json"

# 传给面板的字段白名单(增改字段只改这里)
NODE_FIELDS = ["hostname", "ts_str", "active_files", "active_procs",
               "files_count", "procs_count", "tracked_total", "ts",
               "version", "system", "ping", "history", "network", "jobs",
               "docker", "diskio"]

# 下载工具进程匹配
DL_TOOL_PATTERNS = {
    "wget":  r"wget\s",
    "curl":  r"curl\s+-[a-zA-Z]*[Oo]\s",
    # "dd":    r"dd\s+if=",
    "pip":   r"pip\s+(install|download)\s",
    "pip3":  r"pip3\s+(install|download)\s",
    "git":   r"git\s+clone\s",
    "hf":    r"huggingface(-cli|_hub)\s",
    "aria2": r"aria2c\s",
    "rsync": r"rsync\s",
    "scp":   r"scp\s",
    "docker": r"docker\s+pull\s",
}

# 文件扩展名 → 友好标签
EXT_TAGS = {
    ".sh": "script",
    ".safetensors": "model",
    ".pth": "model",
    ".pt": "model",
    ".bin": "bin",
    ".tar": "archive",
    ".tar.gz": "archive",
    ".gz": "archive",
    ".zip": "archive",
    ".7z": "archive",
    ".xz": "archive",
    ".bz2": "archive",
    ".mp4": "video",
    ".mov": "video",
    ".avi": "video",
    ".mkv": "video",
    ".jpg": "image",
    ".png": "image",
    ".gif": "image",
    ".webp": "image",
    ".iso": "iso",
    ".deb": "pkg",
    ".rpm": "pkg",
    ".whl": "pkg",
    ".dmg": "pkg",
    ".exe": "exe",
    ".AppImage": "app",
    ".git": "git",
    ".patch": "patch",
    ".diff": "diff",
}


# ════════════════════════════════════════════
# 核心: 文件监控器
# ════════════════════════════════════════════

class FileTracker:
    """追踪单个文件的下载进度"""

    def __init__(self, path: str):
        self.path = path
        self.name = os.path.basename(path)
        self.prev_size = 0
        self.first_size: int | None = None
        self.first_seen = time.time()
        self.last_growth = time.time()
        self.growth_count = 0
        self.speed = 0.0
        self.expected_size = None  # 如果能获取Content-Length
        self.pct = 0
        self.status = "new"
        self.history: list[tuple[float, int]] = []  # [(t, size), ...]

    def poll(self) -> bool:
        """轮询文件状态, 返回是否有增长"""
        now = time.time()
        try:
            if os.path.exists(self.path):
                size = os.path.getsize(self.path)
                mtime = os.path.getmtime(self.path)
            else:
                size = 0
                mtime = 0
        except (OSError, PermissionError):
            return False

        if self.first_size is None:
            self.first_size = size

        self.history.append((now, size))
        # 裁剪历史
        while (len(self.history) > 2 and self.history[0][0] < now - SPEED_WINDOW_SEC) or len(self.history) > 120:
            self.history.pop(0)

        # 速度(最近10秒内插值)
        recent = [(t, s) for t, s in self.history if t > now - 10]
        if len(recent) >= 2:
            dt = recent[-1][0] - recent[0][0]
            ds = recent[-1][1] - recent[0][1]
            self.speed = (ds / dt) / (1024 * 1024) if dt > 0 else 0.0
        else:
            self.speed = 0.0

        # 检测增长(跳过首次发现时的初始增长)
        grew = False
        if size > self.prev_size:
            if mtime > now - DEAD_TIMEOUT or self.growth_count > 0:
                # 文件mtime是近30秒内的, 或者之前已经增长过
                self.last_growth = now
                self.growth_count += 1
                grew = True
        self.prev_size = size

        # 状态机
        age = now - self.last_growth
        if size == 0:
            self.status = "new"
        elif age < 15:
            self.status = "downloading"
        elif age < 30:
            self.status = "finishing"
        elif age < 90:
            self.status = "done"
        else:
            self.status = "stale"

        return grew

    def get_report(self) -> dict:
        """生成报告字典"""
        size_mb = self.prev_size / (1024 * 1024)
        # 百分比: 有expected_size用真实值, 否则按状态标记
        if self.status in ("done", "stale"):
            self.pct = 100
        elif self.expected_size and self.expected_size > 0:
            self.pct = min(100, int(self.prev_size / self.expected_size * 100))
        else:
            # 无总大小时显示为0(未知), 避免误导
            self.pct = 0

        ext = os.path.splitext(self.name)[1].lower()
        combined = self.name.lower()
        tag = EXT_TAGS.get(ext) or EXT_TAGS.get(combined, ext.lstrip(".") if ext else "file")

        return {
            "name": self.name,
            "path": self.path,
            "tag": tag,
            "pct": self.pct,
            "size_mb": round(size_mb, 1),
            "speed_mb": round(self.speed, 2),
            "status": self.status,
            "growth_count": self.growth_count,
            "age_s": round(time.time() - self.first_seen, 1),
            "idle_s": round(time.time() - self.last_growth, 1),
        }


# ════════════════════════════════════════════
# 核心: 下载追踪器
# ════════════════════════════════════════════

class DownloadTracker:
    """在目标机上运行的追踪器, 监控文件变化"""

    _ping_cycle = 0
    _ping_cache: dict = {}
    _proc_cache: list = []
    _proc_cycle = PROC_INTERVAL  # 首次立即扫描

    def __init__(self, watch_dirs=None,
                 progress_file: str = PROGRESS_FILE,
                 poll_interval: float = POLL_INTERVAL):
        self.watch_dirs = watch_dirs or DEFAULT_WATCH_DIRS
        self.progress_file = progress_file
        self.poll_interval = poll_interval
        self.trackers: dict[str, FileTracker] = {}
        self._history: dict[str, deque] = {"cpu": deque(maxlen=HISTORY_LEN), "mem": deque(maxlen=HISTORY_LEN),
                         "gpu": deque(maxlen=HISTORY_LEN), "gpu_temp": deque(maxlen=HISTORY_LEN),
                         "cpu_temp": deque(maxlen=HISTORY_LEN), "load": deque(maxlen=HISTORY_LEN)}
        self._docker_cycle = 0
        self._docker_cache: dict = {}
        self._history_lock = threading.Lock()  # 线程安全
        # 恢复上次会话的历史(防重启清零)
        self._load_history()

    def _load_history(self):
        """从磁盘恢复历史数据(防重启清零)"""
        try:
            if os.path.exists(HISTORY_FILE):
                with open(HISTORY_FILE) as f:
                    saved = json.load(f)
                now = time.time()
                for k, vals in saved.items():
                    if k in self._history:
                        # 过滤掉超过保存期的旧数据
                        recent = [(t, v) for t, v in vals if now - t < HISTORY_SAVE_SEC]
                        with self._history_lock:
                            for item in recent:
                                self._history[k].append(item)
        except (OSError, json.JSONDecodeError, ValueError):
            pass

    def _save_history(self):
        """持久化历史数据到磁盘"""
        try:
            tmp = HISTORY_FILE + ".tmp"
            with open(tmp, "w") as f:
                with self._history_lock:
                    json.dump({k: list(v) for k, v in self._history.items()}, f)
            os.rename(tmp, HISTORY_FILE)
        except OSError:
            pass

    def _should_skip(self, path: str) -> bool:
        """是否跳过此文件"""
        name = os.path.basename(path)
        if name in SKIP_NAMES:
            return True
        ext = os.path.splitext(name)[1].lower()
        if ext in SKIP_EXTS:
            return True
        for pat in SKIP_PATH_PATTERNS:
            if re.search(pat, path):
                return True
        return False

    def _discover_files(self) -> dict[str, str]:
        """扫描监控目录, 发现候选文件(上限保护)"""
        candidates: dict[str, str] = {}
        for watch_dir in self.watch_dirs:
            if len(candidates) >= MAX_TRACKED_FILES * 3:
                break  # 防止目录过大导致内存爆炸
            if not os.path.isdir(watch_dir):
                continue
            try:
                for entry in sorted(os.listdir(watch_dir)):
                    fpath = os.path.join(watch_dir, entry)
                    if self._should_skip(fpath):
                        continue
                    try:
                        if not os.path.isfile(fpath):
                            continue
                        size = os.path.getsize(fpath)
                        # 只关注 > 1MB 的常规文件或 safetensors
                        if size > 1024 * 1024:
                            candidates[fpath] = entry
                    except (OSError, PermissionError):
                        continue
            except (OSError, PermissionError):
                continue
        return candidates

    def _detect_processes(self) -> list[dict]:
        """扫描进程列表, 查找下载工具进程(Linux /proc/ + macOS ps fallback)"""
        procs: list[dict] = []
        try:
            if os.path.isdir("/proc"):
                # Linux: 读取 /proc/*/cmdline
                for entry in os.listdir("/proc"):
                    if not entry.isdigit():
                        continue
                    try:
                        with open(f"/proc/{entry}/cmdline", "rb") as f:
                            raw = f.read()
                        cmd = raw.replace(b"\x00", b" ").decode("utf-8", errors="replace").strip()
                    except (OSError, PermissionError):
                        continue
                    if not cmd:
                        continue
                    for tag, pat in DL_TOOL_PATTERNS.items():
                        if re.search(pat, cmd):
                            cinfo: dict = {
                                "pid": int(entry),
                                "tag": tag,
                                "cmd": cmd[:120],
                            }
                            url_m = re.search(r"(https?://[^\s\"'<>]+)", cmd)
                            if url_m:
                                cinfo["url"] = url_m.group(1).rstrip("'").rstrip('"')[:120]
                            procs.append(cinfo)
                            break
            else:
                # macOS fallback: 使用 ps 命令
                out = subprocess.check_output(
                    ["ps", "-eo", "pid,command"], timeout=5, text=True, stderr=subprocess.DEVNULL
                )
                for line in out.split("\n")[1:]:  # skip header
                    line = line.strip()
                    if not line:
                        continue
                    parts = line.split(None, 1)
                    if len(parts) < 2:
                        continue
                    pid_str, cmd = parts
                    for tag, pat in DL_TOOL_PATTERNS.items():
                        if re.search(pat, cmd):
                            cinfo: dict = {  # type: ignore[no-redef]
                                "pid": int(pid_str),
                                "tag": tag,
                                "cmd": cmd[:120],
                            }
                            url_m = re.search(r"(https?://[^\s\"'<>]+)", cmd)
                            if url_m:
                                cinfo["url"] = url_m.group(1).rstrip("'").rstrip('"')[:120]
                            procs.append(cinfo)
                            break
        except (OSError, subprocess.TimeoutExpired, subprocess.CalledProcessError, ValueError, PermissionError):
            pass
        return procs[:MAX_PROCESSES]

    def _collect_processes(self) -> list[dict]:
        """Collect Top 5 processes by CPU usage. macOS/Linux compatible."""
        try:
            import platform
            is_mac = platform.system() == "Darwin"
            cmd = ["ps", "-eo", "pid,%cpu,%mem,comm", "--sort=-%cpu", "--no-headers"]
            if is_mac:
                cmd = ["ps", "-eo", "pid,%cpu,%mem,command", "-r"]
            out = subprocess.check_output(cmd, timeout=3, text=True)
            lines = [l.strip() for l in out.strip().split("\n") if l.strip()]
            procs = []
            for line in lines[:5]:
                parts = line.split(None, 3)
                if len(parts) >= 4:
                    procs.append({
                        "pid": int(parts[0]),
                        "cpu_pct": float(parts[1]),
                        "mem_pct": float(parts[2]),
                        "cmd": parts[3][:40]
                    })
            return procs
        except Exception:
            return []

    def _check_alerts(self, result: dict) -> list[dict]:
        """Check alerts against thresholds and save to ALERT_FILE.
        Returns the list of active alerts."""
        alerts = []
        now = time.time()
        ts_str = datetime.now().strftime("%H:%M:%S")
        si = result.get("system", {}) or {}

        # GPU temperature critical
        gpu_temp = si.get("gpu_temp")
        if gpu_temp is not None and gpu_temp > ALERT_GPU_TEMP_CRITICAL:
            alerts.append({
                "metric": "gpu_temp",
                "value": gpu_temp,
                "threshold": ALERT_GPU_TEMP_CRITICAL,
                "severity": "critical",
                "node": os.uname().nodename,
            })

        # CPU percentage warning
        cpu_pct = si.get("cpu_pct")
        if cpu_pct is not None and cpu_pct > alert_cpu_pct_warning:
            alerts.append({
                "metric": "cpu_pct",
                "value": cpu_pct,
                "threshold": alert_cpu_pct_warning,
                "severity": "warning",
                "node": os.uname().nodename,
            })

        # Disk percentage warning
        disk_pct = si.get("disk_pct")
        if isinstance(disk_pct, str):
            try:
                disk_pct = float(disk_pct.rstrip("%"))
            except (ValueError, AttributeError):
                disk_pct = None
        if disk_pct is not None and disk_pct > alert_disk_pct_warning:
            alerts.append({
                "metric": "disk_pct",
                "value": disk_pct,
                "threshold": alert_disk_pct_warning,
                "severity": "warning",
                "node": os.uname().nodename,
            })

        alert_payload = {
            "ts": now,
            "ts_str": ts_str,
            "alerts": alerts,
            "count": len(alerts),
        }
        self._save_alerts(alert_payload)
        return alerts

    def _save_alerts(self, payload: dict):
        """Persist alerts to ALERT_FILE atomically."""
        try:
            tmp = ALERT_FILE + ".tmp"
            with open(tmp, "w") as f:
                json.dump(payload, f)
            os.rename(tmp, ALERT_FILE)
        except OSError:
            pass

    # ── 网络采集（200G + TB + Ping） ──

    def _collect_network(self) -> dict:
        """采集200G直连 + Thunderbolt链接"""
        net: dict = {"link200": {}, "tb": []}
        hostname = os.uname().nodename

        if "spark" in hostname:
            # 200G InfiniBand: 读接口配置, 动态检测对端
            peer = None
            local_ib = None
            try:
                out = subprocess.check_output(
                    ["ip", "-brief", "addr"], timeout=2, text=True, stderr=subprocess.DEVNULL
                )
                for line in out.splitlines():
                    # enP2p1s0f1np1 or similar CX-7 port
                    m = re.match(r"^(enP\S+)\s+UP\s+.*inet\s+(\S+)", line)
                    if m:
                        local_ib = m.group(2).split("/")[0]
                        # Peer: same /24 subnet, different last octet
                        parts = (local_ib or "").rsplit(".", 1)
                        if len(parts) == 2:
                            last = int(parts[1])
                            peer_last = 102 if last == 101 else 101
                            peer = f"{parts[0]}.{peer_last}"
                        break
            except Exception:
                pass

            peer = peer or ("192.168.100.102" if "9051" in hostname else "192.168.100.101")
            nodes_nm = "spark-9051 ↔ spark-9797"
            net["link200"]["link200_nodes"] = nodes_nm

            try:
                out = subprocess.run(  # type: ignore[assignment]
                    ["ping", "-c", "1", "-W", "1", peer],
                    capture_output=True, text=True, timeout=2
                )
                if out.returncode == 0:  # type: ignore[attr-defined]
                    m = re.search(r"time=(\d+\.?\d*)\s*ms", out.stdout)  # type: ignore[attr-defined]
                    lat = float(m.group(1)) if m else 0.15
                    net["link200"]["up"] = True
                    net["link200"]["latency"] = lat
                    net["link200"]["peer"] = peer
                else:
                    net["link200"]["up"] = False
                    net["link200"]["peer"] = peer
            except Exception:
                net["link200"] = {"up": False, "peer": peer}

        elif sys.platform == "darwin":
            # Thunderbolt: 检查是否有169.254.x.x地址的活跃接口
            try:
                out = subprocess.check_output(
                    ["ifconfig"], timeout=2, text=True, stderr=subprocess.DEVNULL
                )
                for line in out.splitlines():
                    m = re.search(r"inet\s+169\.254\.(\d+)\.(\d+)", line)
                    if m:
                        net["tb"].append({"ip": f"169.254.{m.group(1)}.{m.group(2)}",
                                           "speed": "80Gb/s"})
                        break
            except Exception:
                pass

        return net

    # ── 任务追踪（内容生产 + 代码编译） ──

    JOB_FILE = "/tmp/dltrace_jobs.json"
    
    # 长时间运行的计算进程模式
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

    def _collect_jobs(self) -> list[dict]:
        """采集任务列表: 优先读 /tmp/dltrace_jobs.json, 否则自动检测计算进程"""
        jobs: list[dict] = []
        now = time.time()

        # 1. 显式任务文件
        try:
            if os.path.exists(self.JOB_FILE):
                with open(self.JOB_FILE) as f:
                    raw = json.load(f)
                if isinstance(raw, list):
                    for j in raw:
                        jtype = j.get("type", "content")
                        # st = j.get("status", "running")
                        # 计算已耗时
                        elapsed = now - j.get("started_ts", now)
                        j["elapsed_s"] = round(elapsed)
                        # 计算剩余
                        est = (j.get("estimated_sec") or 0)
                        if est > 0:
                            j["remaining_s"] = round(max(0, est - elapsed))
                            j["pct"] = min(100, round(elapsed / est * 100, 1))
                        else:
                            j["remaining_s"] = None
                            j["pct"] = j.get("progress")
                        jobs.append(j)
                    return jobs[:10]
        except (OSError, json.JSONDecodeError):
            pass

        # 2. 自动检测计算进程(Linux/macOS, 仅在无显式job文件时启用)
        try:
            if sys.platform == "linux":
                out = subprocess.check_output(
                    ["ps", "-eo", "pid,etime,cmd", "--no-headers"],
                    timeout=3, text=True, stderr=subprocess.DEVNULL
                )
                lines = out.split("\n")
            else:
                # macOS: 无--no-headers, 需要手动跳过表头
                out = subprocess.check_output(
                    ["ps", "-eo", "pid,etime,command"],
                    timeout=3, text=True, stderr=subprocess.DEVNULL
                )
                lines = out.split("\n")[1:]  # skip header
            
            for line in lines[:30]:
                parts = line.strip().split(None, 2)
                if len(parts) < 3:
                    continue
                pid_s, elapsed_s, cmd = parts
                # 解析耗时
                etime_parts = elapsed_s.split(":")
                et_seconds = 0
                if "-" in etime_parts[0]:  # days-HH:MM:SS
                    days, rest = etime_parts[0].split("-", 1)
                    et_seconds = int(days) * 86400
                    etime_parts = [rest] + etime_parts[1:]
                elif len(etime_parts) == 3:
                    et_seconds = int(etime_parts[0])*3600 + int(etime_parts[1])*60 + int(etime_parts[2])
                elif len(etime_parts) == 2:
                    et_seconds = int(etime_parts[0])*60 + int(etime_parts[1])
                
                # 只看运行超过30秒的进程
                if et_seconds < 30:
                    continue

                matched = None
                for tag, pat in self.COMPUTE_PATTERNS.items():
                    if re.search(pat, cmd):
                        matched = tag
                        break
                if not matched:
                    continue

                # 组装任务
                jtype = "code" if matched in ("gcc", "cargo", "make", "docker_build") else "content"
                name = f"{matched}: {cmd[:50]}"
                jobs.append({
                    "id": f"auto-{pid_s}",
                    "pid": int(pid_s),
                    "name": name,
                    "type": jtype,
                    "status": "running",
                    "elapsed_s": et_seconds,
                    "remaining_s": None,
                    "pct": None,
                    "started_ts": now - et_seconds,
                    "auto": True,
                })
        except Exception:
            pass

        return jobs[:10]

    def _collect_docker(self) -> dict:
        """采集Docker容器状态."""
        out = {"containers": [], "summary": ""}
        try:
            r = subprocess.run(["docker", "ps", "-a", "--format", "{{.ID}}|{{.Image}}|{{.Status}}|{{.Names}}"],
                               capture_output=True, text=True, timeout=5)
            if r.returncode != 0:
                return out
            containers: list[dict] = []
            for line in r.stdout.strip().split("\n"):
                if not line.strip():
                    continue
                parts = line.split("|", 3)
                if len(parts) < 4:
                    continue
                cid, image, status, name = parts[0][:12], parts[1], parts[2], parts[3]
                running = "Up" in status or status.lower().startswith("up")
                containers.append({
                    "id": cid, "image": image[:40], "name": name,
                    "status": status[:60], "state": "running" if running else "stopped"
                })
            out["containers"] = containers  # type: ignore[assignment]
            run_c = sum(1 for c in containers if c["state"] == "running")
            stop_c = len(containers) - run_c
            out["summary"] = (f"{run_c} running" if run_c else "") + \
                (f", {stop_c} stopped" if stop_c else "") if containers else "no containers"
        except (OSError, subprocess.TimeoutExpired, FileNotFoundError):
            pass
        return out

    def _collect_disk_io(self) -> list[dict]:
        """采集磁盘IO吞吐量."""
        out = []
        try:
            if sys.platform == "darwin":
                r = subprocess.run(["iostat"], capture_output=True, text=True, timeout=5)
                lines = r.stdout.strip().split("\n")
                if len(lines) >= 3:
                    parts = lines[2].split()
                    # macOS iostat: 每个disk 4列(KB_t tps MB/s), 再+2列(cpu) + 3列(load)
                    # parts[0]=KB_t_0, [1]=tps_0, [2]=MB_s_0, [3]=KB_t_1, [4]=tps_1, [5]=MB_s_1
                    # name由header行提供: disk0, disk4
                    # header行: [disk0, disk4, cpu, load, average]
                    hdr = lines[0].split()
                    disk_names = [hdr[i] for i in range(len(hdr)) if hdr[i].startswith("disk")]
                    for di, name in enumerate(disk_names):
                        base = di * 4
                        if base + 2 < len(parts):
                            # macOS iostat 不区分读写, 用 kb_t * tps 算读速率, 写设为 0
                            kb_t = float(parts[base])
                            tps = float(parts[base + 1])
                            out.append({"device": name,
                                        "kb_read": round(kb_t * tps, 1),
                                        "kb_write": 0})
            else:
                r = subprocess.run(["iostat", "-x", "1", "2"], capture_output=True, text=True, timeout=5)
                lines = r.stdout.strip().split("\n")
                # iostat -x 1 2: first batch is system-average since boot, second is real 1s sample
                # Collect only the second half of device lines (skip first batch)
                device_lines = []
                for line in lines:
                    parts = line.split()
                    if len(parts) >= 12 and (parts[0].startswith("sd") or parts[0].startswith("nvme") or parts[0].startswith("nvm")):
                        device_lines.append(parts)
                # Take only the second half (the real sample, not system-average)
                if len(device_lines) > 1:
                    device_lines = device_lines[len(device_lines)//2:]
                for parts in device_lines:
                    out.append({"device": parts[0],
                                "kb_read": float(parts[5]) if len(parts) > 5 else 0,
                                "kb_write": float(parts[9]) if len(parts) > 9 else 0})
        except (OSError, subprocess.TimeoutExpired, FileNotFoundError):
            pass
        return out

    def _collect_system_info(self) -> dict:
        """收集系统信息: CPU/内存/磁盘/GPU(跨平台Mac+Linux)"""
        info: dict[str, Any] = {}

        # Mac系统
        if sys.platform == "darwin":
            # CPU (top)
            try:
                out = subprocess.check_output(
                    ["top", "-l", "1", "-n", "0"], timeout=3, text=True, stderr=subprocess.DEVNULL
                )
                for line in out.split("\n"):
                    if "CPU usage" in line:
                        m = re.findall(r'[\d.]+', line)
                        if len(m) >= 2:
                            info["cpu_pct"] = float(m[0]) + float(m[1])
                        elif m:
                            info["cpu_pct"] = float(m[0])
                        break
            except (OSError, subprocess.TimeoutExpired, ValueError):
                pass

            # 内存 (vm_stat)
            try:
                out = subprocess.check_output(["vm_stat"], timeout=3, text=True)
                pagesize = 16384
                free = active = wired = 0
                for line in out.split("\n"):
                    if "page size" in line:
                        _m = re.search(r'(\d+)', line)
                        if _m: pagesize = int(_m.group(1))
                    elif "Pages free" in line:
                        _m = re.search(r'(\d+)', line)
                        if _m: free = int(_m.group(1))
                    elif "Pages active" in line:
                        _m = re.search(r'(\d+)', line)
                        if _m: active = int(_m.group(1))
                    elif "Pages wired" in line:
                        _m = re.search(r'(\d+)', line)
                        if _m: wired = int(_m.group(1))
                mem_used = (active + wired) * pagesize / 1e9
                mem_total = (free + active + wired) * pagesize / 1e9
                if mem_total > 0:
                    info["mem_used_gb"] = round(mem_used, 1)
                    info["mem_total_gb"] = round(mem_total, 1)
                    info["mem_pct"] = round(mem_used / mem_total * 100, 1)
            except (OSError, subprocess.TimeoutExpired, ValueError):
                pass

            # 磁盘
            try:
                out = subprocess.check_output(["df", "-h", "/"], timeout=3, text=True)
                lines = out.strip().split("\n")
                if len(lines) >= 2:
                    parts = lines[-1].split()
                    if len(parts) >= 5:
                        info["disk_total"] = parts[1]
                        info["disk_used"] = parts[2]
                        info["disk_pct"] = float(parts[4].rstrip("%"))
            except (OSError, subprocess.TimeoutExpired, ValueError):
                pass

            # GPU
            try:
                gpu = subprocess.check_output(
                    ["system_profiler", "SPDisplaysDataType"], timeout=5, text=True, stderr=subprocess.DEVNULL
                )
                for line in gpu.split("\n"):
                    if "Chipset Model" in line or "Chip" in line:
                        info["gpu_info"] = line.split(":")[-1].strip()
                    elif "VRAM" in line:
                        info["gpu_vram"] = line.split(":")[-1].strip()
            except (OSError, subprocess.TimeoutExpired, ValueError):
                pass

        # Linux系统(大傻/二傻)
        else:
            # CPU负载
            try:
                with open("/proc/loadavg") as f:
                    parts = f.read().strip().split()
                    if parts: info["load_1m"] = float(parts[0])
            except (OSError, subprocess.TimeoutExpired, ValueError): pass
            # CPU使用率
            try:
                with open("/proc/stat") as f:
                    cpu_line = f.readline()
                    fields = cpu_line.split()
                    if len(fields) > 4:
                        user, nice, system, idle = fields[1:5]
                        total = int(user) + int(nice) + int(system) + int(idle)
                        idle_v = int(idle)
                        info["cpu_user"] = int(user)
                        info["cpu_idle"] = idle_v
                        info["cpu_total"] = total
                        info["cpu_pct"] = round((total - idle_v) / total * 100, 1) if total > 0 else 0
            except (OSError, subprocess.TimeoutExpired, ValueError): pass
            # 内存 (LANG=C 强制英文, 避免中文locale问题)
            try:
                env = {"LANG": "C", "LC_ALL": "C"}
                out = subprocess.check_output(["free", "-b"], timeout=3, text=True, env={**os.environ, **env})
                for line in out.split("\n"):
                    parts = line.split()
                    if len(parts) >= 2 and parts[0] == "Mem:":
                        info["mem_total_gb"] = round(int(parts[1]) / 1e9, 1)
                        info["mem_used_gb"] = round(int(parts[2]) / 1e9, 1)
                        info["mem_pct"] = round(int(parts[2]) / int(parts[1]) * 100, 1)
                        break
            except (OSError, subprocess.TimeoutExpired, ValueError, IndexError):
                pass
            # GPU (nvidia-smi)
            try:
                out = subprocess.check_output(
                    ["nvidia-smi", "--query-gpu=utilization.gpu,utilization.memory,memory.used,memory.total,temperature.gpu,power.draw",
                     "--format=csv,noheader,nounits"], timeout=5, text=True
                )
                parts = out.strip().split(",")
                def _sf(s): s=s.strip();return float(s) if s not in ("[N/A]","N/A","[Not Supported]") else None
                if len(parts) >= 5:
                    info["gpu_pct"] = _sf(parts[0])
                    info["gpu_mem_pct"] = _sf(parts[1])
                    gmu = _sf(parts[2]); gmt = _sf(parts[3])
                    if gmu is not None: info["gpu_mem_used"] = gmu
                    if gmt is not None: info["gpu_mem_total"] = gmt
                    info["gpu_temp"] = _sf(parts[4])
                    if len(parts) >= 6:
                        gpu_p = _sf(parts[5])
                    if gpu_p is not None: info["gpu_power"] = gpu_p
            except (OSError, subprocess.TimeoutExpired, ValueError): pass

            # GPU Clock (Linux/nvidia-smi)
            try:
                clk_out = subprocess.check_output(
                    ["nvidia-smi", "--query-gpu=clocks.current.graphics,clocks.current.memory",
                     "--format=csv,noheader,nounits"],
                    timeout=3, text=True
                )
                clk_parts = clk_out.strip().split(",")
                if len(clk_parts) >= 2:
                    try:
                        info["gpu_clk_graphics"] = float(clk_parts[0].strip())
                        info["gpu_clk_memory"] = float(clk_parts[1].strip())
                    except (ValueError, IndexError):
                        pass
            except Exception:
                pass

            # Fan Speed (Linux only)
            try:
                fans = []
                for fpath in glob.glob("/sys/class/hwmon/hwmon*/fan*_input"):
                    try:
                        with open(fpath) as ff:
                            rpm = int(ff.read().strip())
                            fans.append(rpm)
                    except (OSError, ValueError):
                        pass
                if fans:
                    info["fan_rpm"] = fans
                    info["fan_rpm_avg"] = int(sum(fans) / len(fans))
            except Exception:
                pass

            # CPU温度 (Linux thermal zone)
            try:
                tz_dirs = sorted(glob.glob("/sys/class/thermal/thermal_zone*"))
                if tz_dirs:
                    max_t = 0.0
                    for tz in tz_dirs[:8]:
                        # Skip acpitz zones (always report fixed temperature, not CPU)
                        type_path = os.path.join(tz, "type")
                        try:
                            with open(type_path) as tf:
                                tz_type = tf.read().strip()
                            if tz_type == "acpitz":
                                continue
                        except (OSError, PermissionError):
                            pass
                        with open(os.path.join(tz, "temp")) as f:
                            t = int(f.read().strip()) / 1000.0
                            if t > max_t: max_t = t
                    if max_t > 0:
                        info["cpu_temp"] = round(max_t, 1)
            except (OSError, ValueError): pass
            # 磁盘
            try:
                out = subprocess.check_output(["df", "-h", "/"], timeout=3, text=True)
                lines = out.strip().split("\n")
                if len(lines) >= 2:
                    parts = lines[-1].split()
                    if len(parts) >= 5:
                        info["disk_total"] = parts[1]
                        info["disk_used"] = parts[2]
                        info["disk_pct"] = float(parts[4].rstrip("%"))
            except (OSError, subprocess.TimeoutExpired, ValueError): pass
            # 运行时间
            try:
                out = subprocess.check_output(["uptime"], timeout=3, text=True)
                info["uptime_raw"] = out.strip()
                m = re.search(r'up\s+([^,]+)', out)
                if m: info["uptime"] = m.group(1).strip()
                m2 = re.search(r'load\s+average[s]?\s*:\s*([\d.]+)', out)
                if m2: info["load"] = m2.group(1)
            except (OSError, subprocess.TimeoutExpired, ValueError): pass

        return info

    def _collect_ping(self) -> dict:
        """采集网络延迟(Ping) + 出口IP"""
        result: dict[str, Any] = {}
        targets = [("baidu_ms", "baidu.com"), ("ytb_ms", "www.youtube.com"), ("github_ms", "github.com"), ("google_ms", "google.com"), ("yahoo_hk_ms", "yahoo.com.hk")]
        # macOS -W is timeout in seconds (same as Linux), -t is TTL
        to = "-W"
        for key, host in targets:
            ping_ok = False
            try:
                out = subprocess.check_output(
                    ["ping", "-c", "1", to, "2", host],
                    timeout=5, text=True, stderr=subprocess.DEVNULL
                )
                # Linux: 'time=12.34 ms'  macOS: 'round-trip min/avg/max/stddev = 12.3/12.3/12.3/...'
                m = re.search(r'time=([\d.]+)', out)
                if m:
                    result[key] = round(float(m.group(1)), 1)
                    ping_ok = True
                else:
                    # macOS: 'min/avg/max/stddev = X/Y/Z/W' - use avg (second value)
                    m2 = re.search(r'min/avg/max[/\w]+ = ([\d.]+)\/([\d.]+)', out)
                    if m2:
                        result[key] = round(float(m2.group(2)), 1)
                        ping_ok = True
            except (OSError, subprocess.TimeoutExpired, subprocess.CalledProcessError, ValueError):
                pass
            # YouTube often blocks ICMP; fallback to curl HTTP timing
            if not ping_ok and key != "baidu_ms":
                try:
                    p = subprocess.run(
                        ["curl", "-s", "-o", "/dev/null", "-w", "%{time_total}",
                         "--connect-timeout", "5",
                         "https://www.youtube.com"],
                        timeout=10, capture_output=True, text=True
                    )
                    if p.returncode == 0 and p.stdout.strip():
                        result[key] = round(float(p.stdout.strip()) * 1000, 1)  # curl returns seconds, convert to ms
                except (OSError, subprocess.TimeoutExpired, ValueError):
                    pass
        # 出口IP + 地理位置(每100轮查一次, 除非为空)
        cur = self.__class__._ping_cache.get("public_ip", "")
        cur_loc = self.__class__._ping_cache.get("public_loc", "")
        if not cur or self.__class__._ping_cycle % 100 == 0:
            for url in ["https://api.ip.sb/ip", "https://checkip.amazonaws.com", "https://ipinfo.io/ip"]:
                try:
                    out = subprocess.check_output(
                        ["curl", "-s", "--connect-timeout", "3", "--max-time", "5", url],
                        timeout=6, text=True, stderr=subprocess.DEVNULL
                    )
                    ip = out.strip()
                    if ip and re.match(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$', ip):
                        result["public_ip"] = ip
                        # Geo location
                        try:
                            lo = subprocess.check_output(
                                ["curl", "-s", "--connect-timeout", "3", "--max-time", "5",
                                 "https://ipinfo.io/" + ip + "/json"],
                                timeout=6, text=True, stderr=subprocess.DEVNULL
                            )
                            ld = json.loads(lo)
                            parts = []
                            if ld.get("city"): parts.append(ld["city"])
                            if ld.get("region"): parts.append(ld["region"])
                            if ld.get("country"):
                                cc = ld["country"]
                                cn_m = {"CN":"中国","US":"美国","HK":"香港","JP":"日本","SG":"新加坡","GB":"英国","DE":"德国"}
                                parts.append(cn_m.get(cc, cc))
                            if parts:
                                result["public_loc"] = ", ".join(parts)
                        except (OSError, subprocess.TimeoutExpired, ValueError, json.JSONDecodeError):
                            pass
                        break
                except (OSError, subprocess.TimeoutExpired, ValueError):
                    pass
        elif cur:
            result["public_ip"] = cur
            result["public_loc"] = cur_loc
        # 节点延迟矩阵
        node_ping = []
        for node_ip, node_name in [("192.168.3.29","三万八"),("192.168.3.46","小四"),
                                    ("192.168.3.55","大傻"),("192.168.3.45","二傻")]:
            try:
                p = subprocess.run(["ping","-c","1","-W","2",node_ip],
                    capture_output=True, text=True, timeout=3)
                if p.returncode == 0:
                    for line in p.stdout.split("\n"):
                        if "time=" in line:
                            ms = float(line.split("time=")[1].split()[0])
                            node_ping.append({"node":node_name,"ip":node_ip,"ms":ms})
            except:
                pass
        result["node_pings"] = node_ping
        # 合并而不是覆盖缓存(保留之前成功的ping)
        self.__class__._ping_cache.update(result)
        return dict(self.__class__._ping_cache)

    def collect(self) -> dict:
        """一轮采集, 返回完整报告"""
        now = time.time()

        # 1. 扫描文件
        files = self._discover_files()

        # 2. 新文件加入
        for fpath in files:
            if fpath not in self.trackers:
                self.trackers[fpath] = FileTracker(fpath)

        # 3. 更新所有追踪器, 清理无效/过期的
        to_remove: list[str] = []
        for fpath, tracker in list(self.trackers.items()):
            tracker.poll()
            if tracker.status in ("done", "stale") and (now - tracker.last_growth) > STALE_TIMEOUT:
                to_remove.append(fpath)
            elif tracker.growth_count == 0 and (now - tracker.first_seen) > DEAD_TIMEOUT:
                to_remove.append(fpath)
            elif fpath not in files and (now - tracker.last_growth) > 15:
                to_remove.append(fpath)

        for fp in to_remove:
            del self.trackers[fp]

        # 4. 进程(使用缓存, 减少高负载时/proc扫描)
        processes = self.__class__._proc_cache

        # 5. 生成活跃文件报告
        active_files: list[dict] = []
        seen_names: set[str] = set()
        for fpath, tracker in sorted(self.trackers.items(),
                                     key=lambda x: x[1].last_growth, reverse=True):
            if tracker.growth_count < GROWTH_MIN_COUNT and tracker.status not in ("done", "stale"):
                continue  # 样本不够
            # 已完成文件: 只保留最近完成的
            if tracker.status in ("done", "stale") and (now - tracker.last_growth) > SHOW_DONE_SEC:
                continue
            report = tracker.get_report()
            if report["name"] not in seen_names:
                seen_names.add(report["name"])
                active_files.append(report)

        # 6. 进程报告(补充关联信息)
        active_procs: list[dict] = []
        for p in processes:
            entry: dict = {
                "tag": p.get("tag", "?"),
                "cmd": p.get("cmd", "")[:80],
            }
            if "url" in p:
                entry["url"] = p["url"]
            active_procs.append(entry)

        # 7. 构建完整报告
        si = self._collect_system_info()
        # 历史趋势缓冲
        for k in ["cpu", "mem", "gpu", "gpu_temp", "cpu_temp", "load"]:
            v = si.get({"cpu": "cpu_pct", "mem": "mem_pct", "gpu": "gpu_pct",
                        "gpu_temp": "gpu_temp", "cpu_temp": "cpu_temp", "load": "load"}[k])
            if v is not None:
                with self._history_lock:
                    self._history[k].append((now, float(v)))
        # GPU趋势: 从系统信息收集GPU温度/功耗序列
        node_key = os.uname().nodename
        gpu_temp_val = si.get("gpu_temp")
        gpu_power_val = si.get("gpu_power")
        if gpu_temp_val is not None or gpu_power_val is not None:
            if node_key not in GPU_TREND:
                GPU_TREND[node_key] = deque(maxlen=MAX_TREND)
            GPU_TREND[node_key].append({
                "temp": round(gpu_temp_val if gpu_temp_val is not None else 0, 1),
                "power": round(gpu_power_val if gpu_power_val is not None else 0, 1),
            })

        # 线程安全获取历史快照
        hist_snap = {}
        if self._history_lock.acquire(blocking=False):
            try:
                hist_snap = {k: list(v) for k, v in self._history.items()}
            finally:
                self._history_lock.release()

        gpu_trends_data = {}
        for k, v in GPU_TREND.items():
            gpu_trends_data[k] = list(v)

        # Docker: 每10轮采一次(降频)
        self._docker_cycle += 1
        if self._docker_cycle % 10 == 0 or not self._docker_cache:
            self._docker_cache = self._collect_docker()

        report = {
            "version": __version__,
            "ts": now,
            "ts_str": datetime.now().strftime("%H:%M:%S"),
            "hostname": os.uname().nodename,
            "active_files": active_files[:MAX_TRACKED_FILES],
            "active_procs": active_procs,
            "files_count": len(active_files),
            "procs_count": len(active_procs),
            "tracked_total": len(self.trackers),
            "system": si,
            "gpu_trends": gpu_trends_data,
            "docker": self._docker_cache,
            "diskio": self._collect_disk_io(),
            "ping": dict(self.__class__._ping_cache),
            "history": hist_snap,
            "network": self._collect_network(),
            "jobs": self._collect_jobs(),
        }
        # Ping每N轮采一次
        self.__class__._ping_cycle += 1
        if self.__class__._ping_cycle % PING_INTERVAL == 0:
            self._collect_ping()
        # 进程扫描每N轮采一次(减少高负载时/proc扫描开销)
        self.__class__._proc_cycle += 1
        if self.__class__._proc_cycle >= PROC_INTERVAL:
            self.__class__._proc_cache = self._detect_processes()
            self.__class__._proc_cycle = 0
        # Top CPU/MEM processes and alerts
        report["top_cpu_procs"] = self._collect_processes()
        self._check_alerts(report)
        return report

    def write_report(self):
        """写入JSON报告文件(原子写入防断裂)"""
        report = self.collect()
        tmp = self.progress_file + ".tmp"
        try:
            with open(tmp, "w") as f:
                json.dump(report, f, indent=2)
            os.rename(tmp, self.progress_file)
            # 每5轮持久化一次历史(防重启清零)
            if int(time.time()) % 10 < POLL_INTERVAL:
                self._save_history()
        except (OSError, PermissionError) as e:
            sys.stderr.write(f"[dltrace] Write error: {e}\n")

    def run_loop(self):
        """守护进程主循环"""
        pid = os.getpid()
        hostname = os.uname().nodename

        # PID锁: 防止重复启动
        try:
            if os.path.exists(PID_FILE):
                with open(PID_FILE) as f:
                    old_pid = int(f.read().strip())
                try:
                    os.kill(old_pid, 0)  # 检查进程是否存在
                    print(f"[dltrace] ⚠ Already running (PID={old_pid}). Use pkill dltrace to restart.")
                    sys.exit(1)
                except OSError:
                    pass  # 旧进程已死, 继续
            with open(PID_FILE, "w") as f:
                f.write(str(pid))
        except (OSError, ValueError):
            pass

        print(f"[dltrace] Daemon started | PID={pid} | Host={hostname}")
        print(f"[dltrace] Watching: {', '.join(self.watch_dirs)}")
        print(f"[dltrace] Output: {self.progress_file}")
        print(f"[dltrace] Interval: {self.poll_interval}s")

        # 首次采集(立即输出)
        self.write_report()
        last_report_time = time.time()

        while True:
            try:
                time.sleep(self.poll_interval)
                self.write_report()
                # 每30秒打印一次状态
                if time.time() - last_report_time > 30:
                    n = len(self.trackers)
                    alive = sum(1 for t in self.trackers.values()
                                if t.status in ("downloading", "finishing"))
                    print(f"[dltrace] Tracking {n} files ({alive} active)")
                    last_report_time = time.time()
            except KeyboardInterrupt:
                print("\n[dltrace] Shutting down...")
                break
            except Exception as e:
                sys.stderr.write(f"[dltrace] Error: {e}\n")
                time.sleep(self.poll_interval)

        # 清理
        self._save_history()
        try:
            os.remove(PID_FILE)
        except OSError:
            pass


# ════════════════════════════════════════════
# 模式: watch - 终端实时查看
# ════════════════════════════════════════════

def cmd_watch(args: argparse.Namespace):
    """watch 模式: 在终端实时查看下载进度"""
    progress_file = args.file or PROGRESS_FILE

    def clear_screen():
        os.system("clear" if os.name == "posix" else "cls")

    def format_bar(pct: int, width: int = 20) -> str:
        filled = int(pct / 100 * width)
        bar = "█" * filled + "░" * (width - filled)
        return bar

    print(f"[dltrace] Watching {progress_file} (Ctrl+C to quit)")
    print()

    last_render = ""
    while True:
        try:
            if os.path.exists(progress_file):
                with open(progress_file) as f:
                    data = json.load(f)
            else:
                data = {}
        except (json.JSONDecodeError, OSError):
            data = {}

        lines = []
        lines.append(f"dltrace v{data.get('version', '?')}  -  {data.get('hostname', '?')}  -  {data.get('ts_str', '??')}")
        lines.append("─" * 60)

        files = data.get("active_files") or []
        if files:
            for f in files:
                pct = (f.get("pct") or 0)
                bar = format_bar(pct)
                name = f.get("name", "?")
                size = (f.get("size_mb") or 0)
                speed = (f.get("speed_mb") or 0)
                status = f.get("status", "?")
                tag = f.get("tag", "?")
                lines.append(f"  {bar} {pct:3d}%  {size:>7.1f}MB  {speed:>5.2f}MB/s  [{tag}] {name}")
                if status in ("done", "stale"):
                    lines.append("         ✅ 下载完成")
        else:
            lines.append("  (没有活跃的下载活动)")

        procs = data.get("active_procs") or []
        if procs:
            lines.append("")
            lines.append(f"  进程: {' '.join(p['tag'] for p in procs)}")

        new_render = "\n".join(lines)

        if new_render != last_render:
            clear_screen()
            print(new_render)
            last_render = new_render

        try:
            time.sleep(2)
        except KeyboardInterrupt:
            break


# ════════════════════════════════════════════
# 模式: web - HTTP面板
# ════════════════════════════════════════════

def cmd_web(args: argparse.Namespace):
    """web 模式: 启动HTTP面板"""
    port = args.port or DEFAULT_PORT
    progress_file = args.file or PROGRESS_FILE
    ssh_target = args.ssh
    extra_nodes = args.extra_nodes or []

    print(f"[dltrace] Web dashboard starting on http://localhost:{port}")
    print(f"[dltrace] Reading: {progress_file}")
    if ssh_target:
        print(f"[dltrace] Primary: {ssh_target}")
    if extra_nodes:
        print(f"[dltrace] Extra node(s): {', '.join(extra_nodes)}")

    # 绑定到 localhost 限制外部访问(通过SSH隧道或本地访问)
    bind_host = os.environ.get("DLTRACE_BIND", WEB_BIND)
    server = ThreadedHTTPServer((bind_host, port), _make_handler(progress_file, ssh_target, extra_nodes))
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.shutdown()


def _read_remote_data(ssh_target: str, progress_file: str) -> dict:
    """读取远程节点的JSON"""
    try:
        cmd = ["ssh", ssh_target, "cat", progress_file]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
        if result.returncode == 0 and result.stdout:
            return json.loads(result.stdout)
    except (subprocess.TimeoutExpired, OSError, json.JSONDecodeError):
        pass
    return {}


def _make_handler(progress_file: str, ssh_target, extra_nodes=None):
    """创建HTTP请求处理器"""
    extra_nodes = extra_nodes or []
    HTML = _get_dashboard_html()

    class Handler(BaseHTTPRequestHandler):
        _progress_file = progress_file
        _ssh_target = ssh_target
        _extra_nodes = extra_nodes

        def _read_all_data(self) -> dict:
            """读取所有监控节点的数据(并行SSH, 单节点超时不阻塞)"""
            nodes = {}

            # 惰性初始化自身URL(用于跳过自引用HTTP拉取)
            # 当绑定0.0.0.0时getsockname返回(0.0.0.0, port), 无法匹配硬编码url
            # 所以额外检查: host是本机IP/0.0.0.0/127.0.0.1的也跳过
            if not hasattr(self, "_self_port"):
                try:
                    _addr, _port = self.request.getsockname()[:2]
                    self._self_port = _port
                    # 收集本机所有IP(含127.0.0.1)用于自引用检测
                    if not hasattr(self, "_self_ips"):
                        import socket as _sk
                        self._self_ips = set()
                        try:
                            self._self_ips.add(_sk.gethostbyname(_sk.gethostname()))
                        except Exception:
                            pass
                        self._self_ips.add('127.0.0.1')
                        self._self_ips.add('0.0.0.0')
                        self._self_ips.add('localhost')
                        if _addr:
                            self._self_ips.add(_addr)
                except Exception:
                    self._self_port = None
                    self._self_ips = set()

            # 本地节点: 总是包含
            try:
                if os.path.exists(self._progress_file):
                    with open(self._progress_file) as f:
                        primary_data = json.load(f)
                    nodes["localhost"] = primary_data
            except (json.JSONDecodeError, OSError):
                nodes["localhost"] = {"hostname": "localhost", "ts_str": "--:--:--",
                                        "active_files": [], "active_procs": [],
                                        "files_count": 0, "procs_count": 0}

            # 已知所有节点的HTTP API（支持动态配置）
            # 自动排除自身节点的HTTP拉取, 防止循环死锁
            KNOWN_NODES = _load_node_config(
                hardcoded={
                    "三万八":    "http://192.168.3.29:8899/api/v1/metrics",
                    "小四":      "http://192.168.3.46:8899/api/v1/metrics",
                    "大傻":      "http://192.168.3.55:8899/api/v1/metrics",
                    "二傻":      "http://192.168.3.45:8899/api/v1/metrics",
                }
            )
            # 并行HTTP拉取(兼容旧数据格式)
            def _pull_node(name, url):
                try:
                    import urllib.request
                    r = urllib.request.urlopen(url, timeout=8)
                    return name, json.loads(r.read().decode())
                except Exception:
                    return name, {}
            with ThreadPoolExecutor(max_workers=4) as pool:
                import concurrent.futures
                futs = {}
                for n, u in KNOWN_NODES.items():
                    # 跳过自引用: 精确匹配URL, 或IP是本机+端口匹配
                    _skip = False
                    if hasattr(self, '_self_port') and self._self_port:
                        try:
                            _host = u.split('/')[2].split(':')[0]
                            _port = int(u.split(':')[-1].split('/')[0])
                            if _port == self._self_port:
                                if _host in getattr(self, '_self_ips', set()):
                                    _skip = True
                        except (ValueError, IndexError):
                            pass
                    if not _skip and hasattr(self, '_self_url') and self._self_url:
                        if u == self._self_url:
                            _skip = True
                    if _skip:
                        continue
                    futs[pool.submit(_pull_node, n, u)] = n
                try:
                    for future in concurrent.futures.as_completed(futs, timeout=12):
                        name = futs[future]
                        try:
                            result = future.result()  # (name, data_dict)
                            if isinstance(result, tuple) and len(result) == 2:
                                nd_data = result[1]
                            else:
                                nd_data = result
                            if nd_data and name not in nodes:
                                nodes[name] = nd_data
                        except Exception:
                            pass
                except concurrent.futures.TimeoutError:
                    pass  # 总体超时不阻塞

            if not nodes:
                nodes["localhost"] = {"hostname": "localhost", "ts_str": "--:--:--",
                                        "active_files": [], "active_procs": [],
                                        "files_count": 0, "procs_count": 0}
            return nodes

        def _read_data(self) -> dict:
            """读取所有节点数据, 返回兼容格式"""
            nodes = self._read_all_data()
            # 构建无循环引用的nodes副本(JSON可序列化)
            nodes_clean = {}
            for h, nd in nodes.items():
                sys_info = nd.get("system", {}) or {}
                entry = {f: nd.get(f) for f in NODE_FIELDS}
                entry["hostname"] = nd.get("hostname", h)
                entry["system"] = sys_info
                # 标准化docker: 统一为 dict {containers:[], summary:""}
                dk = entry.get("docker")
                if dk is None or (isinstance(dk, list) and len(dk) == 0):
                    entry["docker"] = {"containers": [], "summary": ""}
                elif isinstance(dk, list):
                    entry["docker"] = {"containers": dk, "summary": f"{len(dk)} containers"}
                elif isinstance(dk, dict):
                    if "containers" not in dk:
                        entry["docker"] = {"containers": [], "summary": ""}
                # 标准化diskio: 统一为 list[dict]
                dio = entry.get("diskio")
                if dio is None or (isinstance(dio, dict) and not dio.get("partitions")):
                    entry["diskio"] = []
                elif isinstance(dio, dict) and dio.get("partitions"):
                    entry["diskio"] = dio["partitions"]
                nodes_clean[h] = entry
            # 返回第一个节点的数据(兼容老API), 附加网络信息
            for hostname, data in nodes.items():
                data["nodes"] = nodes_clean  # 用无循环引用的副本
                data["nodes_count"] = len(nodes)
                # 优先使用DGX节点的network数据(有200G链接信息)
                top_nw = data.get("network") or {}
                if not top_nw.get("link200") or not top_nw.get("link200", {}).get("up"):
                    for h, nd in nodes_clean.items():
                        nw = nd.get("network") or {}
                        if nw.get("link200") and nw["link200"].get("up"):
                            data["network"] = nw
                            break
                if "dltrace_connected" not in data:
                    data["dltrace_connected"] = bool(data.get("tracked_total", 0) > 0)
                return data
            return {}

        def log_message(self, fmt, *args):
            pass  # 静默

        def do_GET(self):
            path = self.path.split("?")[0].split("#")[0]
            # Authentication check (skip /health)
            if path != "/health" and DLTRACE_TOKEN:
                auth = self.headers.get("Authorization", "")
                if auth != f"Bearer {DLTRACE_TOKEN}":
                    self.send_response(401)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("WWW-Authenticate", "Bearer")
                    self.end_headers()
                    self.wfile.write(b'{"error":"unauthorized"}')
                    return
            # 健康检查: 不触发聚合查询, 防止递归死锁
            if path == "/health":
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.end_headers()
                self.wfile.write(b"OK")
                return
            data = self._read_data()
            if path == "/api/v1/metrics":
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps(data).encode())
            elif path == "/api/v1/json":
                # 原始JSON
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                try:
                    if self._ssh_target:
                        cmd = ["ssh", self._ssh_target, "cat", self._progress_file]
                        result = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
                        self.wfile.write(result.stdout.encode() if result.stdout else b"{}")
                    else:
                        with open(self._progress_file) as f:
                            self.wfile.write(f.read().encode())
                except (OSError, subprocess.TimeoutExpired, json.JSONDecodeError):
                    self.wfile.write(b"{}")
            else:
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.end_headers()
                                # ── 3区服务端渲染: 系统+网络+下载 ──
                import html as hlib
                ns = data.get("nodes", {})

                def esc(s): return hlib.escape(str(s)) if s is not None else ""
                def bar_cls(s): return "dl-done" if s in ("done","stale") else ("dl-finish" if s=="finishing" else "dl-down")
                NODE_NAMES = {"localhost": "十六万", "192.168.3.55": "大傻 spark-9051", "192.168.3.46": "小四", "192.168.3.45": "二傻 spark-9797", "192.168.3.29": "三万八", "大傻": "大傻 spark-9051", "二傻": "二傻 spark-9797", "小四": "小四", "三万八": "三万八"}
                NODE_KEYS = ["localhost", "192.168.3.29", "192.168.3.46", "192.168.3.55", "192.168.3.45"]
                FIXED_NAMES = {"localhost": "十六万", "192.168.3.29": "三万八", "192.168.3.46": "小四", "192.168.3.55": "大傻", "192.168.3.45": "二傻"}
                def tag_icon(t):
                    tl=(t or "").lower()
                    if tl.find("model")>=0 or tl.find("qwen")>=0 or tl.find("llm")>=0 or tl.find("safetensors")>=0: return "🧠"
                    if tl.find("archive")>=0 or tl.find("tar")>=0 or tl.find("gz")>=0 or tl.find("zip")>=0: return "🗜️"
                    if tl.find("git")>=0 or tl.find("clone")>=0: return "🔀"
                    if tl.find("pip")>=0 or tl.find("python")>=0: return "🐍"
                    if tl.find("docker")>=0 or tl.find("image")>=0: return "🐳"
                    if tl.find("video")>=0 or tl.find("mp4")>=0 or tl.find("movie")>=0: return "🎬"
                    return "📄"

                def _sparkline(data, color="#0ff", w=120, h=28):
                    """SVG趋势迷你图 (Netdata风格)"""
                    if not data or len(data) < 3: return ""
                    vals = [v for _, v in data]
                    vmin, vmax = min(vals), max(vals)
                    if vmax - vmin < 0.1: vmin -= 1; vmax += 1  # 防除零
                    rng = vmax - vmin
                    pts = [(i/(len(vals)-1)*w, h-(v-vmin)/rng*(h-4)-2) for i, v in enumerate(vals)]
                    line = " ".join(f"{x:.1f},{y:.1f}" for x, y in pts)
                    # 渐变填充
                    gid = f"sg_{abs(hash(color))}"
                    return (f'<svg class="sparkline" width="{w}" height="{h}" viewBox="0 0 {w} {h}">'
                            f'<defs><linearGradient id="{gid}" x1="0" y1="0" x2="0" y2="1">'
                            f'<stop offset="0%" stop-color="{color}" stop-opacity="0.3"/>'
                            f'<stop offset="100%" stop-color="{color}" stop-opacity="0.02"/>'
                            f'</linearGradient></defs>'
                            f'<polyline points="{line}" fill="none" stroke="{color}" stroke-width="1.2" '
                            f'stroke-linecap="round" stroke-linejoin="round"/>'
                            f'<polygon points="0,{h} {line} {w},{h}" fill="url(#{gid})"/>'
                            f'</svg>')

                def _sys_gauge(label, value, unit):
                    v = min(max(float(value), 0), 100)
                    # 温度用特殊阈值: <55绿 55-75黄 >75红
                    if label in ("TMP", "TMG", "TMC"):
                        c = "#0f8" if v < 55 else "#fa0" if v < 75 else "#f44"
                    else:
                        c = "#0ff" if v < 70 else "#fa0" if v < 90 else "#f44"
                    r = 22; sw = 5; cx = r + sw; sz = cx * 2
                    circ = 2 * 3.14159 * r; dash = circ * v / 100
                    return (f'<div class="gg"><svg width="{sz}" height="{sz}" viewBox="0 0 {sz} {sz}">'
                            f'<circle cx="{cx}" cy="{cx}" r="{r}" fill="none" stroke="#0a0a1e" stroke-width="{sw}"/>'
                            f'<circle cx="{cx}" cy="{cx}" r="{r}" fill="none" stroke="{c}" stroke-width="{sw}" '
                            f'stroke-dasharray="{dash:.1f} {circ-dash:.1f}" stroke-linecap="round" '
                            f'transform="rotate(-90 {cx} {cx})" style="filter:drop-shadow(0 0 6px {c})"/>'
                            f'<text x="{cx}" y="{cx-1}" text-anchor="middle" fill="#bbc" '
                            f'font-size="10px" font-weight="bold">{v:.0f}<tspan font-size="6px" fill="#556">{esc(unit)}</tspan></text>'
                            f'<text x="{cx}" y="{cx+9}" text-anchor="middle" fill="#445" '
                            f'font-size="6px">{esc(label)}</text></svg></div>')

                # System monitoring
                sys_html = ""
                for h, n in ns.items():
                    friendly = NODE_NAMES.get(h, h.split(".")[0] if "." in h else h)
                    s = n.get("system", {})
                    dots = (n.get("tracked_total") or 0) > 0 or (n.get("ts") or 0) > 1700000000
                    dc = "online" if dots else "offline"; tc = "#0ff" if dots else "#222"
                    card = f'<div class="card sys-card" style="border-top:2px solid {tc}"><div class="card-header"><span class="card-title {dc}"><span class="status-dot {dc}"></span>{esc(friendly)}</span><span class="card-ts">{n.get("ts_str","--:--:--")}</span></div><div style="display:flex;flex-wrap:wrap;justify-content:center;gap:2px">'
                    if s.get("cpu_pct") is not None: card += _sys_gauge("CPU", s["cpu_pct"], "%")
                    elif s.get("load_1m") is not None: card += _sys_gauge("LD", s["load_1m"], "")
                    if s.get("mem_pct") is not None: card += _sys_gauge("MEM", s["mem_pct"], "%")
                    if s.get("gpu_pct") is not None: card += _sys_gauge("GPU", s["gpu_pct"], "%")
                    if s.get("disk_pct") is not None: card += _sys_gauge("DSK", float(s["disk_pct"]), "%")
                    if s.get("gpu_temp") is not None: card += _sys_gauge("TMG", s["gpu_temp"], "\u00b0")
                    if s.get("cpu_temp") is not None: card += _sys_gauge("TMC", s["cpu_temp"], "\u00b0")
                    # 趋势sparkline
                    hist = n.get("history") or {}
                    if hist.get("cpu"): card += _sparkline(hist["cpu"], "#0ff", 100, 24)
                    gpu_info = (s.get("gpu_info") or "")
                    if gpu_info: card += f'<div style="width:100%;text-align:center;margin-top:2px"><span class="gpu-tag" title="{esc(gpu_info)}">{esc(gpu_info[:30])}</span></div>'
                    if s.get("load"): card += f'<div style="width:100%;text-align:center"><span class="sys-load">LD: {esc(s["load"])}</span></div>'
                    if s.get("gpu_clk_graphics") is not None and s.get("gpu_clk_memory") is not None: card += f'<span class="gpu-clk">⚡ {s["gpu_clk_graphics"]}MHz 💾 {s["gpu_clk_memory"]}MHz</span>'
                    if s.get("fan_rpm_avg") is not None: card += f'<span class="fan-speed">❄ {s["fan_rpm_avg"]} RPM</span>'
                    card += "</div></div>"; sys_html += card

                # Network section
                net_html = '<div class="card net-card"><div class="net-top">'
                l200 = (data.get("network") or {}).get("link200", {})
                lkc = "#0f0" if l200.get("up") else "#f44"
                lkt = f"{l200['latency']:.2f}ms" if l200.get("latency") else ("DOWN" if not l200.get("up") else "...")
                nodes_200g = l200.get("link200_nodes", "spark-9051 ↔ spark-9797")
                net_html += f'<div class="net-200g"><span class="net-200g-icon">⚡</span><span class="net-200g-label">200G</span><span class="net-200g-nodes">{nodes_200g}</span><span class="net-200g-lat" style="color:{lkc}">{lkt}</span></div>'
                tbs = (data.get("network") or {}).get("tb", [])
                for tb in tbs: net_html += f'<div class="net-tb-link">🗡 {esc(tb.get("from",""))} ↔ {esc(tb.get("to",""))} <span class="net-tb-speed">{esc(tb.get("speed",""))}</span></div>'
                net_html += '</div>'
                # 5×5 延迟矩阵
                m_keys = list(ns.keys())
                if m_keys:
                    # ====== NEW: Public service ping matrix ======
                    net_html += '<div class="tog ex" onclick="toggleMatrix(this)"><span class="tog-i">\u25b6</span><span class="tog-t">\u516c\u7f51\u5ef6\u8fdf</span><span class="tog-b">5\u00d75</span><span class="leg"><span class="leg-i"><span class="leg-d" style="background:rgba(34,197,94,.5)"></span>&lt;50ms</span><span class="leg-i"><span class="leg-d" style="background:rgba(234,179,8,.5)"></span>50-200ms</span><span class="leg-i"><span class="leg-d" style="background:rgba(239,68,68,.5)"></span>&gt;200ms</span></span></div>'
                    net_html += '<div class="net-matrix-wrap open"><div class="pg-grid">'
                    # Column headers: empty + services + location
                    net_html += '<div></div>'
                    net_html += '<div class="ph"><img src="data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAABGdBTUEAALGPC/xhBQAAACBjSFJNAAB6JgAAgIQAAPoAAACA6AAAdTAAAOpgAAA6mAAAF3CculE8AAAARGVYSWZNTQAqAAAACAABh2kABAAAAAEAAAAaAAAAAAADoAEAAwAAAAEAAQAAoAIABAAAAAEAAAAQoAMABAAAAAEAAAAQAAAAADRVcfIAAAHJaVRYdFhNTDpjb20uYWRvYmUueG1wAAAAAAA8eDp4bXBtZXRhIHhtbG5zOng9ImFkb2JlOm5zOm1ldGEvIiB4OnhtcHRrPSJYTVAgQ29yZSA2LjAuMCI+CiAgIDxyZGY6UkRGIHhtbG5zOnJkZj0iaHR0cDovL3d3dy53My5vcmcvMTk5OS8wMi8yMi1yZGYtc3ludGF4LW5zIyI+CiAgICAgIDxyZGY6RGVzY3JpcHRpb24gcmRmOmFib3V0PSIiCiAgICAgICAgICAgIHhtbG5zOmV4aWY9Imh0dHA6Ly9ucy5hZG9iZS5jb20vZXhpZi8xLjAvIj4KICAgICAgICAgPGV4aWY6Q29sb3JTcGFjZT4xPC9leGlmOkNvbG9yU3BhY2U+CiAgICAgICAgIDxleGlmOlBpeGVsWERpbWVuc2lvbj42NDwvZXhpZjpQaXhlbFhEaW1lbnNpb24+CiAgICAgICAgIDxleGlmOlBpeGVsWURpbWVuc2lvbj42NDwvZXhpZjpQaXhlbFlEaW1lbnNpb24+CiAgICAgIDwvcmRmOkRlc2NyaXB0aW9uPgogICA8L3JkZjpSREY+CjwveDp4bXBtZXRhPgohBDnEAAACk0lEQVQ4EaVTS0hUURj+7mPunWbU0Sl7OG0SM1PMRKRty1oURRRB2EOidhmkUQNiL4ImahEt2rVpLxSBWFpjgaljhmIa5jjjC3VGRkadO/cx93TOHb2ktYl+OPd1/v873/f9/wUhhKcrQNe/BqvhQS9/FHd9VkhnMGUBKmmTtL5ZISM/1L8dEGAAG6K7RyGlVVFSciBCBodU8vJVkuwsDpPDR6bJQszYkMteeGyK/gEVSppA14DhUQ3fRzQ4nTwmpw1MRPRN2YC4/mVm1kA4YqCiXIJrCwfTBMr3SVAUE6pKUFbqgMfD40OXgtoaGTnutbMZjeRyhhw/PUt8eycsyqGvadLTp7AtK9o7VsnPsEau34yRHXsmiL8lTkwzu2cxmKL0Rsd0SA4OwU8KLp7LBWP06GkC+R4B9edzYRjAl7403C6O3lVoGoEsc1kJRbtElBQ7MPBNRVWljPhiBmcvzGM8rEOgRwwMqnj+pNDKiUQN1FTLcEicpZ5jRNjT9IyBsXEd5WUSGprirDsQRQ7dvWmAZlypz8OlujwMDWs4VCsjNyfrgW3ibp9oFVxrjCMS1XG7MR+TUxlEJg0kkxm8bUtB0wn8TV6Iwrr1gN3GWDyDusvzqKyQ8OJZIaoPOi0W95u98BYIeHjXi/b3Kdy4Fad+WKQtFBug46NiaT51wg3/nUW0vl6B7OQgS0DGpHIEDi4Xj7Z3KZpHHV0LG8DhANJ0gDqDCu41b8Wxo24sUjOTyyYCD7aBSdSpBI56J/wmwTYxkTBxtWEBoX4V+6mR3gIeS0smdEqXSVimQGO0K2dO5qDF77VBGECAsmlijFZWTfSGsj32FYnw0fYyvVE6J3NzBrYXCnQKneBt3njMjPqv3/kXJUADijW0BeUAAAAASUVORK5CYII=" alt="百度" class="pgi"><span class="sn">百度</span></div>'
                    net_html += '<div class="ph"><img src="data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAAXNSR0IArs4c6QAAAERlWElmTU0AKgAAAAgAAYdpAAQAAAABAAAAGgAAAAAAA6ABAAMAAAABAAEAAKACAAQAAAABAAAAEKADAAQAAAABAAAAEAAAAAA0VXHyAAAAw0lEQVQ4EaVTiw2FIAwsL28ANpARGIEV3MAR3UBHcATcgA14LfLRgI0+SAjl6J09qMJ7Dz3j00Mm7jcLCCEx1nFvMn4N1rjdwHsXYrKAJiacZObNnIJ9JOmXxPNHNN2BimX9swSB5LsWsBbAmBoviOJfYRgAlgVgnrFOVWiniBdIiVKmqFp5gX0HGMfDBtlpDOqDrYEfkMbrce72GA8sVWBvM3gy0aih+hpJ5J/p2spYO8hGZWvEcisXgUb2E4h/hQcKPzl8hRVGXE8aAAAAAElFTkSuQmCC" alt="YouTube" class="pgi"><span class="sn">YouTube</span></div>'
                    net_html += '<div class="ph"><img src="data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAABRUlEQVR4nIVSW3LCMAxcOS5cAJLS3P9W/JdgX4AMtjorbNC0TKMZ4sTsrh4rUVWFi5wz7qoQVaiI3fX3KILD4eDhEC+w5AytFdKIv4PQGgK+nEjoL5eUiHhLfAoAGFQfWC9wyRlvc1KwifIp7iTHBFJKkFqtPMbnOOK+rigA7jE+fusKlGL/mQhnVCvIjQRazxxUSzzP8z99NJSIJQniLqi6FaygD5nc8LQKQBmGTQHvOrnBT/5jwwVGjdGq7e2E/mEeb9KBwFn1VlmBtqy9r+/rFefz+Q+Rd8uyWNaONTdSSlqa3+axCGoTPY2jnUYkyRO5VCEg2G6zpA6gGzwLTXLh1rvbTq5t4jRNqBRxmxfCc8sxNHe8A+QYrl+cpsla6Ktqgi6j+d+8n47H11B9lex5HyNKrSiuhdvtBs5pv9thbHPp8QOVn7yjFD03pQAAAABJRU5ErkJggg==" alt="GitHub" class="pgi"><span class="sn">GitHub</span></div>'
                    net_html += '<div class="ph"><img src="data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAABGdBTUEAALGPC/xhBQAAACBjSFJNAAB6JgAAgIQAAPoAAACA6AAAdTAAAOpgAAA6mAAAF3CculE8AAAARGVYSWZNTQAqAAAACAABh2kABAAAAAEAAAAaAAAAAAADoAEAAwAAAAEAAQAAoAIABAAAAAEAAAAQoAMABAAAAAEAAAAQAAAAADRVcfIAAAHJaVRYdFhNTDpjb20uYWRvYmUueG1wAAAAAAA8eDp4bXBtZXRhIHhtbG5zOng9ImFkb2JlOm5zOm1ldGEvIiB4OnhtcHRrPSJYTVAgQ29yZSA2LjAuMCI+CiAgIDxyZGY6UkRGIHhtbG5zOnJkZj0iaHR0cDovL3d3dy53My5vcmcvMTk5OS8wMi8yMi1yZGYtc3ludGF4LW5zIyI+CiAgICAgIDxyZGY6RGVzY3JpcHRpb24gcmRmOmFib3V0PSIiCiAgICAgICAgICAgIHhtbG5zOmV4aWY9Imh0dHA6Ly9ucy5hZG9iZS5jb20vZXhpZi8xLjAvIj4KICAgICAgICAgPGV4aWY6Q29sb3JTcGFjZT4xPC9leGlmOkNvbG9yU3BhY2U+CiAgICAgICAgIDxleGlmOlBpeGVsWERpbWVuc2lvbj4zMjwvZXhpZjpQaXhlbFhEaW1lbnNpb24+CiAgICAgICAgIDxleGlmOlBpeGVsWURpbWVuc2lvbj4zMjwvZXhpZjpQaXhlbFlEaW1lbnNpb24+CiAgICAgIDwvcmRmOkRlc2NyaXB0aW9uPgogICA8L3JkZjpSREY+CjwveDp4bXBtZXRhPgqWsr5jAAACfElEQVQ4EXVTz0tUURT+3o9RZ0YyiSIrpNoESZSYoRmEES5D002Bk1AKZZuC8h+Qglpki2ihLiKERIlq1cIi0yJIEfwBif1QtEVClI7z8713b995k+JYHvju3HPud8797pnzgA2mtd7qum4TMex5XkKglBpi/IKcbaBnu0yqJ2mC2MzGyTm7PstYdXjTdcM07zBgOWMfkXwzAO/bFx5rWHv3I6+6BoHScqE7VNRmWdY9cfwCvK6O+z6kktbKww4kXvRDxVZgBALCgXYcGLl5CF9qRTjSDPI9FmmwbfuZTaeAnHa5KPqAyf2PYYTCCJ5pQE55hV8g/WEY7tfPyCk75vuGYVCA1c7c11KtidDewqD+ee6g/nGyXFOBhLItupTtZ7wIKGVY9t70ZZ3qhY51tawRFXeJtNZxgUOklA/Hy1CY+9ampjLRpZcnYRYCwROHfJmyxFMaLd1JRJMaJrslSLvA+eMBHyxz1NYk+p3kr0XHUJL6rwknxoLLCWApLlkZk6KjsrXyS7DgmniyMMt+ZiyUa6C7OYjeqyH0XAnhQJEJT2kUFZo+gc0cMbl0ivdrRyNuxqtxa3YOfTMvMwSuefwngznA0LSHqXlgW76Bw8VrBToNvmMLee+pvOT2SBd6Pj1F2A7idHEVKncegWmaGF2cwKsJIPW9HpGK7WitkfnQk3xwlf98FqllpD+p0tb9sUe+gqgTQ8CUHsvouQgFbNTuuoi2yjpR5XJyZZCer/YPDFzje+6CQzKyOIWB+XeY+T3HOQH2FezGqT2VqCoqZTntKqVvcJA6/OrrF/lQqGac+L8pJWeidnMjQT7nCIdkkIj9xSDjjYSMfZb9ARmRtLGfbP3kAAAAAElFTkSuQmCC" alt="Google" class="pgi"><span class="sn">Google</span></div>'
                    net_html += '<div class="ph"><img src="data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAABGdBTUEAALGPC/xhBQAAACBjSFJNAAB6JgAAgIQAAPoAAACA6AAAdTAAAOpgAAA6mAAAF3CculE8AAAARGVYSWZNTQAqAAAACAABh2kABAAAAAEAAAAaAAAAAAADoAEAAwAAAAEAAQAAoAIABAAAAAEAAAAQoAMABAAAAAEAAAAQAAAAADRVcfIAAAHJaVRYdFhNTDpjb20uYWRvYmUueG1wAAAAAAA8eDp4bXBtZXRhIHhtbG5zOng9ImFkb2JlOm5zOm1ldGEvIiB4OnhtcHRrPSJYTVAgQ29yZSA2LjAuMCI+CiAgIDxyZGY6UkRGIHhtbG5zOnJkZj0iaHR0cDovL3d3dy53My5vcmcvMTk5OS8wMi8yMi1yZGYtc3ludGF4LW5zIyI+CiAgICAgIDxyZGY6RGVzY3JpcHRpb24gcmRmOmFib3V0PSIiCiAgICAgICAgICAgIHhtbG5zOmV4aWY9Imh0dHA6Ly9ucy5hZG9iZS5jb20vZXhpZi8xLjAvIj4KICAgICAgICAgPGV4aWY6Q29sb3JTcGFjZT4xPC9leGlmOkNvbG9yU3BhY2U+CiAgICAgICAgIDxleGlmOlBpeGVsWERpbWVuc2lvbj40ODwvZXhpZjpQaXhlbFhEaW1lbnNpb24+CiAgICAgICAgIDxleGlmOlBpeGVsWURpbWVuc2lvbj40ODwvZXhpZjpQaXhlbFlEaW1lbnNpb24+CiAgICAgIDwvcmRmOkRlc2NyaXB0aW9uPgogICA8L3JkZjpSREY+CjwveDp4bXBtZXRhPgrZdbzTAAABsUlEQVQ4EYWTOy9EQRTHf3d22SBRsEuEkKyCxGNVEiIaGolQ+AI+g1Yi0fgYOoXGYyMa5RZCIpZ4RSEeQWIRhffaO87suNm99orTzMx//v9zzpwzx5nu1P0hxTIOdVpTYu4XuK6FHQWhMDgOCDdDiPGwUiRFXBsozkEsDtX14kCcf7zAzXFeLBJiuCTDsgkUG4fhcpiYg6Yum8HmAlymBY/k/eFooiog6zw79wnxXmjstOIvOaeTkr48wzOjLTp68M8qqfWM2/ca5GwLrg+kBmV+XqADU7i6VmgbLJB3VyEn+G8LdJDLQvcIRKos/f4cTlO2Jv860NKyyhpIjBao++vw+uR/v3dbkoGJ3jEsrWmxlOw77K3Z/nui4lW+hd9MlT+eITVvU76/gMcrqXYJ0+p8sOl9eaUl3xzB4QZ8vglW4Q9SfPI5cCX9oSkYmLSU7UVYnS2ml+4LNZDoSnrc2lcgNff8/Dq5+8uU/BdrsjH9310B8+tM8XaW7GqGJ8gM7MwkdEYGJeoFMW1saLcDc3siWYWCpIKbgJoHpRVjQsl4UUwXrg/t1P0lznM1dzLmY98XE4KRGYksiAAAAABJRU5ErkJggg==" alt="Yahoo" class="pgi"><span class="sn">Yahoo</span></div>'
                    net_html += '<div class="ph"><svg viewBox="0 0 16 16" width="14" height="14"><path d="M8 1C5.24 1 3 3.24 3 6c0 3.75 5 9 5 9s5-5.25 5-9c0-2.76-2.24-5-5-5z" fill="#ef4444"/><circle cx="8" cy="6" r="2" fill="#fff"/></svg><span class="sn">位置</span></div>'
                    # Data rows: one per node
                    for h in m_keys:
                        n = ns[h]
                        fn = NODE_NAMES.get(h, h.split(".")[0] if "." in h else h)
                        sp = fn.find(" ")
                        if sp > 0: fn = fn[:sp]
                        net_html += f'<div class="pr">{esc(fn)}</div>'
                        # Baidu
                        val = (n.get("ping") or {}).get("baidu_ms")
                        if val is not None:
                            c = "rgba(34,197,94,0.15)" if val < 50 else ("rgba(234,179,8,0.15)" if val < 200 else "rgba(239,68,68,0.15)")
                            tc = "rgba(34,197,94,.9)" if val < 50 else ("rgba(234,179,8,.9)" if val < 200 else "rgba(239,68,68,.9)")
                            net_html += f'<div style="background:{c};color:{tc};font-weight:bold">{int(val)}ms</div>'
                        else:
                            net_html += '<div class="na">N/A</div>'
                        # YouTube
                        val = (n.get("ping") or {}).get("ytb_ms")
                        if val is not None:
                            c = "rgba(34,197,94,0.15)" if val < 200 else ("rgba(234,179,8,0.15)" if val < 500 else "rgba(239,68,68,0.15)")
                            tc = "rgba(34,197,94,.9)" if val < 200 else ("rgba(234,179,8,.9)" if val < 500 else "rgba(239,68,68,.9)")
                            net_html += f'<div style="background:{c};color:{tc};font-weight:bold">{int(val)}ms</div>'
                        else:
                            net_html += '<div class="na">N/A</div>'
                        # GitHub
                        val = (n.get("ping") or {}).get("github_ms")
                        if val is not None:
                            c = "rgba(34,197,94,0.15)" if val < 50 else ("rgba(234,179,8,0.15)" if val < 200 else "rgba(239,68,68,0.15)")
                            tc = "rgba(34,197,94,.9)" if val < 50 else ("rgba(234,179,8,.9)" if val < 200 else "rgba(239,68,68,.9)")
                            net_html += f'<div style="background:{c};color:{tc};font-weight:bold">{int(val)}ms</div>'
                        else:
                            net_html += '<div class="na">N/A</div>'
                        # Google
                        val = (n.get("ping") or {}).get("google_ms")
                        if val is not None:
                            c = "rgba(34,197,94,0.15)" if val < 100 else ("rgba(234,179,8,0.15)" if val < 300 else "rgba(239,68,68,0.15)")
                            tc = "rgba(34,197,94,.9)" if val < 100 else ("rgba(234,179,8,.9)" if val < 300 else "rgba(239,68,68,.9)")
                            net_html += f'<div style="background:{c};color:{tc};font-weight:bold">{int(val)}ms</div>'
                        else:
                            net_html += '<div class="na">N/A</div>'
                        # Yahoo
                        val = (n.get("ping") or {}).get("yahoo_hk_ms")
                        if val is not None:
                            c = "rgba(34,197,94,0.15)" if val < 50 else ("rgba(234,179,8,0.15)" if val < 200 else "rgba(239,68,68,0.15)")
                            tc = "rgba(34,197,94,.9)" if val < 50 else ("rgba(234,179,8,.9)" if val < 200 else "rgba(239,68,68,.9)")
                            net_html += f'<div style="background:{c};color:{tc};font-weight:bold">{int(val)}ms</div>'
                        else:
                            net_html += '<div class="na">N/A</div>'
                        # Location
                        loc = (n.get("ping") or {}).get("public_loc", "")
                        net_html += f'<div style="color:#667;font-size:6px">{esc(loc[:12])}</div>'
                    net_html += '</div></div>'
                net_html += '</div>'
                dl_html = ""
                total_files = 0
                for h, n in ns.items():
                    f = n.get("active_files") or []
                    dots = (n.get("tracked_total") or 0) > 0 or (n.get("ts") or 0) > 1700000000
                    title_cls = "online" if dots else "offline"
                    dot_cls = "online" if dots else ("done" if (n.get("files_count") or 0) else "offline")
                    total_files += (n.get("tracked_total") or 0)

                    friendly = NODE_NAMES.get(h, h.split(".")[0] if "." in h else h)
                    card = f'<div class="card"><div class="card-header"><span class="card-title {title_cls}"><span class="status-dot {dot_cls}"></span>{esc(friendly)}</span><span class="card-ts">{n.get("ts_str","--:--:--")}</span></div>'
                    card += f'<div class="stats-row"><span>📦 <b>{n.get("files_count",0)}</b></span><span>📊 <b>{n.get("tracked_total",0)}</b> tracked</span>'
                    if f: card += f'<span>⚡ <b>{len(f)}</b> active</span>'
                    card += '</div>'

                    if f:
                        card += '<ul class="dl-list">'
                        for ff in f:
                            pct = f(f.get("pct") or 0)
                            sz = f(f.get("size_mb") or 0)
                            sp = f(f.get("speed_mb") or 0)
                            nm = (ff.get("name") or "")
                            st = (ff.get("status") or "")
                            ic = tag_icon(ff.get("tag",""))
                            bc = bar_cls(st)
                            pct_cls = "pct-ok" if pct>=100 else ("pct-mid" if pct>50 else "pct-low")
                            card += '<li class="dl-item"><span class="dl-icon">'+ic+'</span><div class="dl-info"><div class="dl-name" title="'+esc(nm)+'">'+esc(nm)+'</div>'
                            speed_txt = f' {sp:.2f} MB/s' if sp > 0 else ''
                            card += f'<div class="dl-bar-wrap"><div class="dl-bar {bc}" style="width:{pct}%"></div></div>'
                            card += f'<div class="dl-meta"><span class="pct {pct_cls}">{pct:.0f}%</span>'
                            card += f'<span style="color:#556">{sz:.1f} MB</span>'
                            card += f'<span class="speed">{speed_txt}</span>'
                            card += f'<span style="color:#445">{st}</span></div></div></li>'
                        card += '</ul>'
                    else:
                        card += '<div class="empty-state"><div class="icon">📭</div><p>无活跃下载</p></div>'
                    card += '</div>'
                    dl_html += card

                node_count = len(ns)
                status_bar = f'<span class="status-dot {"online" if node_count>0 else "offline"}"></span> {node_count} nodes · {total_files} files'

                html = HTML.replace("{{SYS_HTML}}", sys_html)
                html = html.replace("{{NET_HTML}}", net_html)
                html = html.replace("{{DL_HTML}}", dl_html)
                # Job HTML (server-side initial render)
                job_html = '<div class="job-empty"><div class="icon">&#x1f4ad;</div><p>加载中...</p></div>'
                html = html.replace("{{JOB_HTML}}", job_html)
                html = html.replace("{{STATUS_BAR}}", status_bar)
                html = html.replace("{{VERSION}}", __version__)
                html = html.replace("{{TS}}", datetime.now().strftime("%H:%M:%S"))
                html = html.replace("{{NODE_NAMES_JSON}}", json.dumps(NODE_NAMES))
                html = html.replace("{{NODE_KEYS_JSON}}", json.dumps(NODE_KEYS))
                html = html.replace("{{FIXED_NAMES_JSON}}", json.dumps(FIXED_NAMES))
                html = html.replace("{{DATA}}", json.dumps(data, default=str))
                self.wfile.write(html.encode())

    return Handler


def _fmt_size(mb):
    """MB to human readable: if >=1024 show GB."""
    if mb >= 1024:
        return f"{mb/1024:.1f}GB"
    return f"{mb:.0f}MB"


def _get_dashboard_html() -> str:
    """返回HTML模板(工业风)"""
    return r"""<!DOCTYPE html><html lang="zh"><head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>K38 MONITOR | 集群监控面板</title>
<link href="https://fonts.googleapis.com/css2?family=Orbitron:wght@500;700&family=JetBrains+Mono&display=swap" rel="stylesheet">
<meta http-equiv="refresh" content="5">
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:'JetBrains Mono',monospace;overflow-x:hidden;background:#050510;color:#aab}
canvas#bg{position:fixed;top:0;left:0;width:100%;height:100%;z-index:0;opacity:.35;pointer-events:none}
.scanline{position:fixed;top:0;left:0;width:100%;height:100%;pointer-events:none;z-index:2;background:repeating-linear-gradient(0deg,transparent,transparent 2px,rgba(0,0,0,.03) 2px,rgba(0,0,0,.03) 4px)}
#app{position:relative;z-index:1;max-width:1340px;margin:0 auto;padding:12px;margin-left:160px}
.header{text-align:center;padding:4px 0 0}
.header h1{font-family:'Orbitron',sans-serif;font-size:22px;color:#0ff;letter-spacing:4px;text-shadow:0 0 20px #0ff6}
.header .sub{font-size:10px;color:#334;letter-spacing:2px;margin-top:2px}
.topbar{display:flex;justify-content:space-between;align-items:center;padding:2px 0;font-size:9px;color:#445;border-bottom:1px solid #12122a;margin-bottom:6px}
.theme-switch{display:flex;gap:4px}
.theme-btn{font-size:8px;padding:2px 6px;border:1px solid #1a1a3a;border-radius:3px;background:transparent;color:#445;cursor:pointer;font-family:'Orbitron',sans-serif;letter-spacing:1px;transition:.2s}
.theme-btn:hover{color:#aab;border-color:#3a3a5a}
.theme-btn.active{color:#0ff;border-color:#0ff;text-shadow:0 0 6px #0ff4;background:rgba(0,255,255,.05)}
.status-dot{display:inline-block;width:6px;height:6px;border-radius:50%;margin-right:4px;vertical-align:middle}
.status-dot.online{background:#0f0;box-shadow:0 0 4px #0f0;animation:pulse-dot 2s ease-in-out infinite}
@keyframes pulse-dot{0%,100%{opacity:1;box-shadow:0 0 6px #0f0}50%{opacity:0.65;box-shadow:0 0 14px #0f0}}
.status-dot.offline{background:#f44;box-shadow:0 0 4px #f44}
.sys-card{min-height:130px}
.section-label{font-family:'Orbitron',sans-serif;font-size:12px;letter-spacing:3px;margin:10px 0 6px;padding-bottom:2px;text-shadow:0 0 8px #0ff4}
.section-label .icon{margin-right:6px}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:8px;margin:4px 0}
.card{background:#08081a;border:1px solid #1a1a3a;border-radius:6px;padding:8px;box-shadow:0 0 15px #0006;min-height:60px}
.card-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:4px}
.card-title{font-family:'Orbitron',sans-serif;font-size:11px;letter-spacing:1px;display:flex;align-items:center;gap:4px}
.card-title.online{color:#0ff}.card-title.offline{color:#334}.sys-card-placeholder{border-top:2px solid #222!important;opacity:.35}
.card-ts{font-size:8px;color:#335}
.gg{text-align:center;min-width:52px;display:inline-block;margin:2px}
.sparkline{display:inline-block;margin:4px 2px 0;border-radius:4px;background:rgba(0,0,0,.2)}
.gpu-tag{font-size:7px;color:#448;max-width:100px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;display:inline-block}
.sys-load,.sys-up{font-size:7px;color:#445;padding:1px 4px}
.gpu-clk{color:#0f0;font-size:8px;margin-left:6px}
.fan-speed{color:#08f;font-size:8px;margin-left:6px}
.empty-state{text-align:center;padding:10px 0;color:#445;font-size:10px}
.empty-state .icon{font-size:20px}
.dl-list{list-style:none;padding:0;margin:0}
.dl-item{padding:3px 0;border-bottom:1px solid #0a0a1e}
.dl-item:last-child{border-bottom:none}
.dl-row{display:flex;align-items:center;gap:4px}
.dl-name{flex:1;font-size:10px;color:#bbc;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.dl-bar{height:4px;background:#0a0a1a;border-radius:2px;margin:2px 0;overflow:hidden}
.dl-bar-fill{height:100%;border-radius:2px;transition:width .5s}
.dl-bar-fill.dl-down{background:linear-gradient(90deg,#0f8,#0ff);box-shadow:0 0 4px #0ff6}
.dl-bar-fill.dl-finish{background:linear-gradient(90deg,#fa0,#ff0);box-shadow:0 0 4px #fa06}
.dl-bar-fill.dl-done{background:#446}
.dl-meta{font-size:8px;color:#556;display:flex;gap:8px}
.pct{font-weight:bold;font-size:10px}.pct-low{color:#0ff}.pct-mid{color:#fa0}.pct-ok{color:#0f0}
.net-card{text-align:center;padding:6px;min-height:30px}
.net-row{font-size:11px;display:flex;justify-content:center;gap:16px;align-items:center;padding:2px 0}
.net-link{font-size:9px;color:#556;display:inline-flex;align-items:center;gap:4px;margin:0 6px}
.net-top{border-bottom:1px solid rgba(255,170,0,.15);margin-bottom:6px;padding-bottom:4px}
.net-200g{display:flex;align-items:center;gap:6px;font-size:10px;justify-content:center}
.net-200g-icon{color:#fa0;font-size:14px;filter:drop-shadow(0 0 4px #fa06)}
.net-200g-label{font-family:'Orbitron',sans-serif;font-size:9px;color:#fa0;letter-spacing:1px}
.net-200g-nodes{color:#556;font-size:9px}
.net-200g-lat{font-family:'Orbitron',sans-serif;font-size:10px;font-weight:bold}
.net-tb-link{font-size:8px;color:#445;text-align:center;margin:1px 0}
.net-tb-speed{color:#556}
.net-ping-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:5px}
.np-card{background:rgba(10,10,26,.6);border:1px solid rgba(26,26,58,.3);border-radius:4px;padding:5px;text-align:center;min-height:40px}
.np-head{display:flex;align-items:center;justify-content:center;gap:4px;margin-bottom:3px}
.np-dot{width:5px;height:5px;border-radius:50%;flex-shrink:0}
.np-dot.s-green{background:#0f0;box-shadow:0 0 4px #0f0}
.np-dot.s-gray{background:#334}
.np-name{font-size:9px;color:#bbc;font-family:'Orbitron',sans-serif;letter-spacing:1px}
.np-pings{display:flex;justify-content:center;gap:8px;margin:2px 0}
.np-ping{display:flex;align-items:center;gap:3px}
.np-label{font-size:7px;color:#445}
.np-val{font-size:13px;font-weight:bold;font-family:'Orbitron',sans-serif;min-width:22px;text-align:right}
.np-val.c-green{color:#0f0;text-shadow:0 0 6px #0f06}
.np-val.c-yellow{color:#fa0;text-shadow:0 0 6px #fa06}
.np-val.c-red{color:#f44;text-shadow:0 0 6px #f46}
.np-loc{font-size:6px;color:#334;margin-top:2px}
.pg-grid{display:grid;gap:2px;margin-top:3px;padding-top:3px;grid-template-columns:44px repeat(6,1fr)}
.pg-grid>div{padding:3px 2px;font-size:7px;text-align:center;border-radius:2px;display:flex;align-items:center;justify-content:center;min-height:18px}
.ph{color:#445;font-size:7px;font-weight:bold;background:rgba(26,26,58,.3);padding:3px 2px;flex-direction:column;gap:1px}
.ph img.pgi{width:14px;height:14px;border-radius:2px}
.ph .sn{color:#667;font-size:6px;margin-top:1px;max-width:40px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.pr{color:#667;font-size:7px;font-weight:bold;background:rgba(26,26,58,.2);padding:3px 3px;text-align:right;justify-content:flex-end;white-space:nowrap}
.na{color:#334;font-size:6px}
.tog{display:flex;align-items:center;gap:4px;padding:3px 5px;margin-top:3px;border-top:1px solid rgba(26,26,58,.3);cursor:pointer;user-select:none;transition:all .2s;border-radius:3px}
.tog:hover{background:rgba(0,255,255,.04)}
.tog-i{font-size:6px;transition:transform .2s;color:#556;display:inline-block}
.tog-t{font-size:8px;color:#556;letter-spacing:1px}
.tog-b{font-size:6px;color:#445;background:rgba(26,26,58,.3);padding:1px 3px;border-radius:2px}
.tog.ex{background:rgba(0,255,255,.05);border-top-color:rgba(0,255,255,.15)}
.tog.ex .tog-i{color:#0ff;transform:rotate(90deg)}
.tog.ex .tog-t{color:#0ff}
.tog.ex .tog-b{color:#0ff;background:rgba(0,255,255,.1)}
.leg{margin-left:auto;display:flex;gap:5px;align-items:center}
.leg-i{display:flex;align-items:center;gap:2px;font-size:6px;color:#445}
.leg-d{width:4px;height:4px;border-radius:50%;display:inline-block}
.net-matrix-wrap{display:none}
.net-matrix-wrap.open{display:block}
.footer{text-align:center;color:#224;font-size:9px;margin-top:10px}
.footer a{color:#448;text-decoration:none}
@media(max-width:600px){.grid{grid-template-columns:1fr}.header h1{font-size:16px}}
/* Alert banner */
.alert-banner{position:sticky;top:0;z-index:50;padding:6px 12px;margin-bottom:6px;border-radius:4px;font-size:9px;display:none;align-items:center;gap:8px;font-family:'JetBrains Mono',monospace;backdrop-filter:blur(8px);-webkit-backdrop-filter:blur(8px);transition:all 0.3s;cursor:pointer}
.alert-banner.visible{display:flex}
.alert-banner.critical{background:rgba(255,40,40,0.12);border:1px solid rgba(255,40,40,0.35);color:#f66;box-shadow:0 0 20px rgba(255,40,40,0.08)}
.alert-banner.warning{background:rgba(255,170,0,0.10);border:1px solid rgba(255,170,0,0.30);color:#fa0;box-shadow:0 0 20px rgba(255,170,0,0.06)}
.alert-banner .alert-count{font-weight:bold;font-size:11px;margin-right:4px}
.alert-banner .alert-items{flex:1;display:flex;gap:10px;flex-wrap:wrap}
.alert-banner .alert-item{display:flex;align-items:center;gap:4px}
.alert-banner .alert-sev{font-size:7px;padding:1px 4px;border-radius:2px;text-transform:uppercase;font-weight:bold}
.alert-banner .alert-sev.critical{background:rgba(255,40,40,0.2);color:#f44}
.alert-banner .alert-sev.warning{background:rgba(255,170,0,0.2);color:#fa0}
.alert-banner .alert-dismiss{opacity:0.3;cursor:pointer;font-size:12px;padding:2px 4px}
.alert-banner .alert-dismiss:hover{opacity:0.8}
</style>
<style id="theme-s360">
/* Server360 Lite: glass cards, colored section bands */
.theme-s360 .card{background:linear-gradient(135deg,rgba(12,12,30,.88),rgba(8,8,24,.92));border:1px solid rgba(26,26,58,.5);backdrop-filter:blur(6px);-webkit-backdrop-filter:blur(6px);box-shadow:0 4px 20px rgba(0,0,0,.5);transition:.25s}
.theme-s360 .card:hover{box-shadow:0 4px 30px rgba(0,255,255,.08);border-color:rgba(60,60,100,.6)}
.theme-s360 .card-header{border-bottom:1px solid rgba(26,26,58,.4);margin-bottom:6px;padding-bottom:4px}
.theme-s360 .section-label{font-size:10px;color:#667;letter-spacing:4px;text-transform:uppercase;border-bottom:none;margin:12px 0 8px;display:flex;align-items:center;gap:8px}
.theme-s360 .section-label::after{content:'';flex:1;height:1px;background:linear-gradient(90deg,rgba(26,26,58,.6),transparent)}
.theme-s360 .section-label .badge{font-size:7px;padding:1px 6px;border-radius:2px;font-family:'JetBrains Mono',monospace;letter-spacing:1px;margin-left:4px}
.theme-s360 .section-label.sys-label .badge{color:#0ff;border:1px solid rgba(0,255,255,.3);background:rgba(0,255,255,.05)}
.theme-s360 .section-label.net-label .badge{color:#fa0;border:1px solid rgba(255,170,0,.3);background:rgba(255,170,0,.05)}
.theme-s360 .section-label.dl-label .badge{color:#f4f;border:1px solid rgba(255,68,255,.3);background:rgba(255,68,255,.05)}
.theme-s360 .section-label.job-label .badge{color:#fd0;border:1px solid rgba(255,221,0,.3);background:rgba(255,221,0,.05)}
.job-card{border-left:3px solid #fd06;background:rgba(20,18,8,.9);min-height:50px}
.job-card .job-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:4px}
.job-card .job-name{font-size:10px;color:#bbc;flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.job-card .job-type{font-size:7px;padding:1px 5px;border-radius:2px;margin-left:6px;flex-shrink:0}
.job-card .job-type.content{color:#fd0;border:1px solid rgba(255,221,0,.3)}
.job-card .job-type.code{color:#0ff;border:1px solid rgba(0,255,255,.3)}
.job-card .job-progress{height:5px;background:rgba(10,10,26,.6);border-radius:3px;overflow:hidden;margin:4px 0}
.job-card .job-progress-fill{height:100%;border-radius:3px;transition:width .5s;background:linear-gradient(90deg,#fa0,#fd0);box-shadow:0 0 8px #fd06}
.job-card .job-progress-fill.job-done{background:#446;box-shadow:none}
.job-card .job-timers{display:flex;gap:12px;font-size:8px;color:#556}
.job-card .job-timers .job-elapsed{color:#bbc}
.job-card .job-timers .job-eta{color:#fd0;font-weight:bold}
.job-card .job-detail{font-size:7px;color:#445;margin-top:2px}
.job-card .job-node{font-size:7px;color:#334;text-align:right}
.job-empty{text-align:center;padding:20px 0;color:#445;font-size:10px}
.job-empty .icon{font-size:24px;margin-bottom:6px}
.theme-s360 .card-title{font-size:10px;letter-spacing:2px}
.theme-s360 .card-ts{font-size:7px;padding:1px 4px;border-radius:2px;background:rgba(0,255,255,.04);border:1px solid rgba(0,255,255,.1)}
.theme-s360 .gg{filter:drop-shadow(0 0 2px rgba(0,255,255,.1))}
.theme-s360 .dl-item{padding:4px 0;border-bottom:1px solid rgba(255,255,255,.03)}
.theme-s360 .dl-bar{height:3px;background:rgba(10,10,30,.6)}
.theme-s360 .dl-name{font-size:9px}
.theme-s360 .net-card{background:linear-gradient(135deg,rgba(20,18,10,.88),rgba(12,10,8,.92))}
.theme-s360 .net-row{font-size:10px}
.theme-s360 .gpu-tag{font-size:6px;color:#556}.gpu-alert{display:inline-block;font-size:6px;padding:1px 4px;border-radius:2px;background:#f44;color:#fff;animation:gpu-blink 1s infinite;margin-left:4px;vertical-align:middle}
@keyframes gpu-blink{0%,100%{opacity:1}50%{opacity:.3}}
.theme-s360 .sys-load,.theme-s360 .sys-up{font-size:6px;color:#334}
</style>
<style id="theme-netdata">
/* Netdata: left color strip, prominent metrics */
.theme-netdata .card{background:#0e0e20;border:none;border-radius:4px;padding:8px 10px;box-shadow:0 1px 4px rgba(0,0,0,.4);position:relative;overflow:hidden}
.theme-netdata .card::before{content:'';position:absolute;left:0;top:4px;bottom:4px;width:4px;border-radius:2px}
.theme-netdata .card.sys-card::before{background:#0ff;box-shadow:0 0 6px #0ff6}
.theme-netdata .card.net-card::before{background:#fa0;box-shadow:0 0 6px #fa06}
.theme-netdata .card.dl-card::before{background:#f4f;box-shadow:0 0 6px #f4f6}
.theme-netdata .card-header{border-bottom:1px solid rgba(255,255,255,.05);margin-bottom:6px;padding:0 0 4px}
.theme-netdata .card-title{font-size:10px;letter-spacing:1px;color:#0ff}
.theme-netdata .card-ts{font-size:7px;color:#334}
.theme-netdata .section-label{font-size:10px;border-bottom:none;display:flex;align-items:center;gap:6px;color:#556}
.theme-netdata .section-label .swatch{display:inline-block;width:8px;height:8px;border-radius:2px}
.theme-netdata .section-label.sys-label .swatch{background:#0ff;box-shadow:0 0 6px #0ff6}
.theme-netdata .section-label.net-label .swatch{background:#fa0;box-shadow:0 0 6px #fa06}
.theme-netdata .section-label.dl-label .swatch{background:#f4f;box-shadow:0 0 6px #f4f6}
.theme-netdata .gg svg{filter:none!important}
.theme-netdata .dl-bar{height:6px;background:#0a0a18;border-radius:3px}
.theme-netdata .dl-bar-fill{border-radius:3px}
.theme-netdata .dl-name{font-size:10px;font-weight:bold;color:#bbc}
.theme-netdata .dl-meta{font-size:7px;color:#445}
.theme-netdata .pct{font-size:11px}
.theme-netdata .net-card{padding:8px 12px}
.theme-netdata .net-row{font-size:10px;justify-content:flex-start;gap:12px}
.theme-netdata .net-link{font-size:8px}
/* Sidebar - glassmorphism redesign */
.sidebar{position:fixed;left:0;top:0;width:160px;height:100vh;background:linear-gradient(180deg,rgba(8,12,24,0.92),rgba(12,18,32,0.88) 60%,rgba(6,10,20,0.94));backdrop-filter:blur(12px);-webkit-backdrop-filter:blur(12px);border-right:1px solid rgba(0,255,255,0.08);box-shadow:2px 0 30px rgba(0,0,0,0.5),inset -1px 0 0 rgba(0,255,255,0.04);z-index:100;display:flex;flex-direction:column;padding:0;transition:width 0.3s ease,box-shadow 0.3s}
.sidebar:hover{width:180px;box-shadow:4px 0 40px rgba(0,0,0,0.6),inset -1px 0 0 rgba(0,255,255,0.08)}
.sidebar-title{text-align:center;color:#0ff;font-size:11px;letter-spacing:4px;padding:14px 10px 10px;border-bottom:1px solid rgba(0,255,255,0.08);font-family:'Orbitron',sans-serif;text-shadow:0 0 15px rgba(0,255,255,0.2);position:relative}
.sidebar-title::after{content:'';position:absolute;bottom:-1px;left:10%;width:80%;height:1px;background:linear-gradient(90deg,transparent,rgba(0,255,255,0.2),transparent)}
.sidebar-nav{flex:1;overflow-y:auto;padding:6px 0;position:relative}
.nav-item{display:flex;align-items:center;padding:8px 12px;color:#445;font-size:10px;cursor:pointer;transition:all 0.25s cubic-bezier(0.4,0,0.2,1);border-left:2px solid transparent;letter-spacing:1px;margin:1px 6px;border-radius:0 4px 4px 0;position:relative;overflow:hidden}
.nav-item::before{content:'';position:absolute;left:0;top:0;width:100%;height:100%;background:linear-gradient(90deg,rgba(0,255,255,0.06),transparent 60%);opacity:0;transition:opacity 0.3s;pointer-events:none}
.nav-item:hover{color:#0ff;border-left-color:#0ff;background:rgba(0,255,255,0.06);text-shadow:0 0 8px rgba(0,255,255,0.3)}
.nav-item:hover::before{opacity:1}
.nav-item.active{color:#0ff;border-left-color:#0ff;background:rgba(0,255,255,0.08);text-shadow:0 0 10px rgba(0,255,255,0.4);box-shadow:inset 0 0 20px rgba(0,255,255,0.04),0 0 10px rgba(0,255,255,0.05);animation:nav-glow 3s ease-in-out infinite}
@keyframes nav-glow{0%,100%{box-shadow:inset 0 0 20px rgba(0,255,255,0.04),0 0 10px rgba(0,255,255,0.05)}50%{box-shadow:inset 0 0 20px rgba(0,255,255,0.08),0 0 16px rgba(0,255,255,0.08)}}
.sidebar-bottom{margin-top:auto;padding:10px 8px}
/* Theme switch in sidebar */
.stheme-switch{display:flex;gap:4px;padding:2px;border-radius:5px;background:rgba(0,255,255,0.03);border:1px solid rgba(0,255,255,0.05)}
.stheme-btn{flex:1;padding:5px 0;font-size:8px;background:transparent;border:none;color:#445;cursor:pointer;border-radius:3px;text-align:center;font-family:'Orbitron',sans-serif;letter-spacing:1px;transition:all 0.25s cubic-bezier(0.4,0,0.2,1);position:relative}
.stheme-btn:hover{color:#889}
.stheme-btn.active{color:#0ff;background:rgba(0,255,255,0.1);box-shadow:0 0 12px rgba(0,255,255,0.1);text-shadow:0 0 8px #0ff4}
/* Netdata sidebar */
body.theme-netdata .sidebar{background:linear-gradient(180deg,rgba(24,12,8,0.92),rgba(28,16,10,0.88) 60%,rgba(20,10,6,0.94));backdrop-filter:blur(12px);-webkit-backdrop-filter:blur(12px);border-right-color:rgba(255,160,0,0.08);box-shadow:2px 0 30px rgba(0,0,0,0.5),inset -1px 0 0 rgba(255,160,0,0.04)}
body.theme-netdata .sidebar:hover{box-shadow:4px 0 40px rgba(0,0,0,0.6),inset -1px 0 0 rgba(255,160,0,0.08)}
body.theme-netdata .sidebar-title{color:#fa0;border-bottom-color:rgba(255,160,0,0.08);text-shadow:0 0 15px rgba(255,160,0,0.2)}
body.theme-netdata .sidebar-title::after{background:linear-gradient(90deg,transparent,rgba(255,160,0,0.2),transparent)}
body.theme-netdata .nav-item{color:#654}
body.theme-netdata .nav-item::before{background:linear-gradient(90deg,rgba(255,160,0,0.06),transparent 60%)}
body.theme-netdata .nav-item:hover{color:#fa0;border-left-color:#fa0;background:rgba(255,160,0,0.06);text-shadow:0 0 8px rgba(255,160,0,0.3)}
body.theme-netdata .nav-item.active{color:#fa0;border-left-color:#fa0;background:rgba(255,160,0,0.08);text-shadow:0 0 10px rgba(255,160,0,0.4);box-shadow:inset 0 0 20px rgba(255,160,0,0.04),0 0 10px rgba(255,160,0,0.05);animation:nav-glow-nd 3s ease-in-out infinite}
@keyframes nav-glow-nd{0%,100%{box-shadow:inset 0 0 20px rgba(255,160,0,0.04),0 0 10px rgba(255,160,0,0.05)}50%{box-shadow:inset 0 0 20px rgba(255,160,0,0.08),0 0 16px rgba(255,160,0,0.08)}}
body.theme-netdata .stheme-switch{background:rgba(255,160,0,0.03);border-color:rgba(255,160,0,0.05)}
body.theme-netdata .stheme-btn{color:#654}
body.theme-netdata .stheme-btn.active{color:#fa0;background:rgba(255,160,0,0.1);box-shadow:0 0 12px rgba(255,160,0,0.1);text-shadow:0 0 8px #fa04}
/* Topo */
.topo-map{padding:6px 6px;border-bottom:1px solid rgba(0,255,255,0.06);margin-bottom:6px}
.topo-row{display:flex;justify-content:center;gap:14px;margin:1px 0}
.topo-node{width:8px;height:8px;border-radius:50%;margin:0 auto;transition:all 0.3s}
.topo-line{width:2px;height:8px;background:rgba(0,255,255,0.08);margin:0 auto}
.topo-ceo{background:#fd0;box-shadow:0 0 5px #fd0}
.topo-coo{background:#0ff;box-shadow:0 0 5px #0ff}
.topo-cto{background:#f0f;box-shadow:0 0 5px #f0f}
.topo-dgx{background:#0f0;box-shadow:0 0 5px #0f0}
body.theme-netdata .topo-map{border-bottom-color:rgba(255,160,0,0.06)}
body.theme-netdata .topo-line{background:rgba(255,160,0,0.08)}
body.theme-netdata .sidebar-nav .nav-item{margin:1px 4px}
</style>
</head><body>
<canvas id="bg"></canvas><div class="scanline"></div>
<div id="app">
<div class="alert-banner" id="alert-banner" onclick="this.classList.remove('visible')">
  <span class="alert-count" id="alert-count">0</span>
  <span class="alert-label">alert(s)</span>
  <span class="alert-items" id="alert-items"></span>
  <span class="alert-dismiss">✖</span>
</div>
<div class="sidebar">
<div class="sidebar-title">▣ K38</div>
<div class="sidebar-nav">
<div class="nav-item" onclick="navTo(this,'sys-grid')">MONITOR</div>
<div class="nav-item" onclick="navTo(this,'job-grid')">JOBS</div>
<div class="nav-item" onclick="navTo(this,'net-section')">NET</div>
<div class="nav-item" onclick="navTo(this,'dl-grid')">DL</div>
<div class="nav-item" onclick="navTo(this,'svc-grid')">SVC</div>
<div class="nav-item" onclick="navTo(this,'docker-grid')">DOCK</div>
<div class="nav-item" onclick="navTo(this,'diskio-grid')">DISK</div>
</div>
<div class="sidebar-bottom">
<div class="topo-map">
<div class="topo-row"><div class="topo-node topo-ceo" title="十六万"></div></div>
<div class="topo-line"></div>
<div class="topo-row">
<div class="topo-node topo-coo" title="三万八"></div>
<div class="topo-node topo-cto" title="小四"></div>
</div>
<div class="topo-line"></div>
<div class="topo-row">
<div class="topo-node topo-dgx" title="大傻"></div>
<div class="topo-node topo-dgx" title="二傻"></div>
</div>
</div>
<div class="stheme-switch">
<span class="stheme-btn active" data-theme="s360" onclick="switchTheme('s360')">S360</span>
<span class="stheme-btn" data-theme="netdata" onclick="switchTheme('netdata')">ND</span>
</div>
</div>
</div>
<div class="header"><h1>▣ K38 MONITOR</h1><div class="sub">CLUSTER DASHBOARD · {{VERSION}}</div></div>
<div class="topbar">
<div class="theme-switch">
<span class="theme-btn active" data-theme="s360" onclick="switchTheme('s360')">SERVER360</span>
<span class="theme-btn" data-theme="netdata" onclick="switchTheme('netdata')">NETDATA</span>
</div>
<span>{{STATUS_BAR}}<span style="margin-left:8px;color:#224">5s · auto</span></span>
</div>
<div class="section-label sys-label"><span class="icon">&#x2699;</span> SYSTEM MONITORING</div>
<div class="grid" id="sys-grid">{{SYS_HTML}}</div>
<div class="section-label net-label"><span class="icon">&#x1f517;</span> NETWORK LINKS</div>
<div id="net-section">{{NET_HTML}}</div>
<div class="section-label dl-label"><span class="icon">&#x1f4e5;</span> ACTIVE DOWNLOADS</div>
<div class="grid" id="dl-grid">{{DL_HTML}}</div>
<div class="section-label job-label"><span class="icon">&#x23f3;</span> CONTENT PRODUCTION</div>
<div class="grid" id="job-grid">{{JOB_HTML}}</div>
<div class="section-label sys-label" style="font-size:11px;letter-spacing:3px;margin:10px 0 6px;color:#0f8;font-family:'Orbitron',sans-serif">&#x1f4ca; DISK I/O</div>
<div class="grid" id="diskio-grid"></div>
<div class="section-label sys-label" style="font-size:11px;letter-spacing:3px;margin:10px 0 6px;color:#0ff;font-family:'Orbitron',sans-serif">&#x1f433; DOCKER CONTAINERS</div>
<div class="grid" id="docker-grid"></div>
<div class="footer"><a href="https://github.com/kk38/dltrace" target="_blank">dltrace</a> · {{VERSION}} · {{TS}}</div>
</div>
<script>
var POLL=2500,_hist={};
function $(id){return document.getElementById(id)}
function esc(s){if(s==null)return'';var d=document.createElement('div');d.appendChild(document.createTextNode(String(s)));return d.innerHTML}
function pctCls(p){return p>=100?'pct-ok':p>50?'pct-mid':'pct-low'}
function barCls(s){return s==='done'||s==='stale'?'dl-done':s==='finishing'?'dl-finish':'dl-down'}
function tagIcon(t){var tl=(t||'').toLowerCase();if(tl.match(/model|qwen|llm|safetensors/))return'\uD83E\uDDE0';if(tl.match(/archive|tar|gz|zip/))return'\uD83D\uDDDC\uFE0F';if(tl.match(/git|clone/))return'\uD83D\uDD00';if(tl.match(/pip|python/))return'\uD83D\uDC0D';if(tl.match(/docker|image/))return'\uD83D\uDC33';if(tl.match(/video|mp4|movie/))return'\uD83C\uDFAC';return'\uD83D\uDCC4'}
function fmtSize(mb){if(mb>=1024)return(mb/1024).toFixed(1)+'GB';return Math.round(mb)+'MB'}
function fmtKb(kb){if(kb>=1024)return(kb/1024).toFixed(1)+'MB/s';return Math.round(kb)+'KB/s'}
function renderSparkline(vals,color,h){if(!vals||vals.length<2)return'';var mx=Math.max.apply(null,vals),mn=Math.min.apply(null,vals);var rng=mx-mn||1,bw=Math.max(3,Math.min(6,80/vals.length));var bars=vals.map(function(v){var p=Math.max(5,Math.min(100,(v-mn)/rng*100));return'<div style="width:'+bw+'px;height:'+p+'%;background:'+color+';border-radius:1px;opacity:0.5"></div>';}).join('');return'<div style="display:flex;align-items:flex-end;gap:1px;height:'+h+'px;margin-top:3px">'+bars+'</div>';}
function sparkline(data,color,w,h){if(!data||data.length<3)return'';var vals=[],i;for(i=0;i<data.length;i++)vals.push(data[i][1]);var vmin=Math.min.apply(null,vals),vmax=Math.max.apply(null,vals);if(vmax-vmin<.1){vmin-=1;vmax+=1}var rng=vmax-vmin;var pts='';for(i=0;i<vals.length;i++){pts+=(i/(vals.length-1)*w).toFixed(1)+','+(h-(vals[i]-vmin)/rng*(h-4)-2).toFixed(1)+' '}
var gid='sg_'+Math.abs(Math.floor(Math.random()*99999));return'<svg class="sparkline" width="'+w+'" height="'+h+'" viewBox="0 0 '+w+' '+h+'"><defs><linearGradient id="'+gid+'" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stop-color="'+color+'" stop-opacity="0.3"/><stop offset="100%" stop-color="'+color+'" stop-opacity="0.02"/></linearGradient></defs><polyline points="'+pts+'" fill="none" stroke="'+color+'" stroke-width="1.2" stroke-linecap="round" stroke-linejoin="round"/><polygon points="0,'+h+' '+pts+' '+w+','+h+'" fill="url(#'+gid+')"/></svg>';}
function gauge(id,n,v,u,cl){var isT=n=='TMP'||n=='TMG'||n=='TMC';var c=cl||(isT?(v>75?'#f44':v>55?'#fa0':'#0f8'):(v>90?'#f44':v>70?'#fa0':'#0ff'));var p=Math.min(Math.max(v,0),100);var r=22,sw=5,cx=r+sw,cy=r+sw,sz=cx*2,circ=2*Math.PI*r,dash=circ*p/100;
var h='<svg width="'+sz+'" height="'+sz+'" viewBox="0 0 '+sz+' '+sz+'"><circle cx="'+cx+'" cy="'+cy+'" r="'+r+'" fill="none" stroke="#0a0a1e" stroke-width="'+sw+'"/>'
+'<circle cx="'+cx+'" cy="'+cy+'" r="'+r+'" fill="none" stroke="'+c+'" stroke-width="'+sw+'" stroke-dasharray="'+dash+' '+Math.ceil(circ-dash)+'" stroke-linecap="round" transform="rotate(-90 '+cx+' '+cy+')" style="filter:drop-shadow(0 0 6px '+c+')"/>'
+'<text x="'+cx+'" y="'+(cy-1)+'" text-anchor="middle" fill="#bbc" font-size="10px" font-weight="bold">'+Math.round(p)+'<tspan font-size="6px" fill="#556">'+u+'</tspan></text>'
+'<text x="'+cx+'" y="'+(cy+9)+'" text-anchor="middle" fill="#445" font-size="6px">'+esc(n)+'</text></svg>';
if(!_hist[id])_hist[id]=[];_hist[id].push(v);if(_hist[id].length>60)_hist[id].shift();
var sl='';if(_hist[id].length>2){var mn=Math.min.apply(null,_hist[id]),mx=Math.max.apply(null,_hist[id]);if(mn===mx){mn-=1;mx+=1}var sp=110,sh=16,rng=mx-mn,pts=[];_hist[id].forEach(function(vv,ii){var x=ii/(_hist[id].length-1)*(sp-4)+2,y=sh-2-(vv-mn)/rng*(sh-4);pts.push(x.toFixed(1)+','+y.toFixed(1));});
sl='<svg width="'+sp+'" height="'+sh+'" style="display:block;margin:1px auto"><polyline points="'+pts.join(' ')+'" fill="none" stroke="'+c+'" stroke-width="1" opacity=".5"/></svg>';}
return '<div class="gg">'+h+sl+'</div>';}
function sysCard(h,n){if(!n)return'<div class="card sys-card sys-card-placeholder"><div class="card-header"><span class="card-title offline"><span class="status-dot offline"></span>'+esc(FIXED_NAME[h]||h)+'</span><span class="card-ts">--:--:--</span></div><div style="display:flex;flex-wrap:wrap;justify-content:center;gap:2px;color:#334;font-size:9px;text-align:center;width:100%;padding-top:20px">⚠️ 暂无数据</div></div>';}
var s=n.system||{};var nn=typeof NODE_NAMES!='undefined'?NODE_NAMES:{};var fn=nn[h]||(h.indexOf('.')>=0?h.split('.')[0]:h);
var dots=n.tracked_total>0||(n.ts||0)>1700000000;var dc=dots?'online':'offline';
var htm='<div class="card sys-card" style="border-top:2px solid '+(dots?'#0ff':'#222')+'"><div class="card-header"><span class="card-title '+dc+'"><span class="status-dot '+dc+'"></span>'+esc(fn)+'</span><span class="card-ts">'+(n.ts_str||'--:--:--')+'</span></div><div style="display:flex;flex-wrap:wrap;justify-content:center;gap:2px">';
if(s.cpu_pct!==undefined)htm+=gauge('cpu_'+h,'CPU',s.cpu_pct,'%');else if(s.load_1m!==undefined)htm+=gauge('cpu_'+h,'LD',s.load_1m,'');
if(s.mem_pct!==undefined)htm+=gauge('mem_'+h,'MEM',s.mem_pct,'%');
if(s.gpu_pct!==undefined)htm+=gauge('gpu_'+h,'GPU',s.gpu_pct,'%');
if(s.gpu_mem_used!==undefined&&s.gpu_mem_total!==undefined)htm+='<div style="width:100%;text-align:center;font-size:8px;color:#556;margin-top:1px">VRAM: '+fmtSize(s.gpu_mem_used)+' / '+fmtSize(s.gpu_mem_total)+'</div>';
if(s.disk_pct!==undefined)htm+=gauge('dsk_'+h,'DSK',parseFloat(s.disk_pct),'%');
if(s.gpu_temp!==undefined)htm+=gauge('tmp_'+h,'TMG',s.gpu_temp,'\u00b0')+(s.gpu_temp>85?' <span class="gpu-alert">HOT</span>':'');
if(s.cpu_temp!==undefined)htm+=gauge('cpt_'+h,'TMC',s.cpu_temp,'\u00b0');
var hist=n.history||{};if(hist.cpu)htm+=sparkline(hist.cpu,'#0ff',100,24);
// GPU temp/power trend sparkline (div-based)
var gt=n.gpu_trends||{};var gtKeys=Object.keys(gt);for(var gi=0;gi<gtKeys.length;gi++){if(gtKeys[gi]===h){var trend=gt[gtKeys[gi]];if(trend&&trend.length>=2){var temps=trend.map(function(x){return x.temp});var powers=trend.map(function(x){return x.power});
htm+='<div style="width:100%;display:flex;gap:6px;justify-content:center;margin-top:2px">'+
'<div style="text-align:center"><div style="font-size:6px;color:#556">TEMP \u00b0</div>'+renderSparkline(temps,'#f60',28)+'</div>'+
'<div style="text-align:center"><div style="font-size:6px;color:#556">PWR W</div>'+renderSparkline(powers,'#fa0',28)+'</div></div>';}}}
if(s.gpu_info)htm+='<div style="width:100%;text-align:center;margin-top:2px"><span class="gpu-tag" title="'+esc(s.gpu_info)+'">'+esc(s.gpu_info.substring(0,30))+'</span></div>';
if(s.gpu_clk_graphics!==undefined&&s.gpu_clk_memory!==undefined)htm+='<span class="gpu-clk">⚡ '+esc(s.gpu_clk_graphics)+'MHz 💾 '+esc(s.gpu_clk_memory)+'MHz</span>';
if(s.fan_rpm_avg!==undefined)htm+='<span class="fan-speed">❄ '+esc(s.fan_rpm_avg)+' RPM</span>';
if(s.load)htm+='<div style="width:100%;text-align:center"><span class="sys-load">LD: '+esc(s.load)+'</span></div>';
if(s.uptime)htm+='<div style="width:100%;text-align:center"><span class="sys-up">'+esc(s.uptime)+'</span></div>';
htm+='</div></div>';return htm;}
function dlCard(h,n){var f=n.active_files||[];var nn=typeof NODE_NAMES!='undefined'?NODE_NAMES:{};var fn=nn[h]||(h.indexOf('.')>=0?h.split('.')[0]:h);
var dots=n.tracked_total>0||(n.ts||0)>1700000000;var dc=dots?'online':'offline';
var htm='<div class="card dl-card"><div class="card-header"><span class="card-title '+dc+'"><span class="status-dot '+dc+'"></span>'+esc(fn)+'</span><span class="card-ts">\uD83D\uDCE6 '+n.tracked_total+'</span></div>';
if(f.length){htm+='<ul class="dl-list">';
for(var i=0;i<f.length;i++){var ff=f[i];var p=ff.pct||0;var sz=ff.size_mb||0;var sp=ff.speed_mb||0;var nm=ff.name||'';var st=ff.status||'';var ic=tagIcon(ff.tag);var bc=barCls(st);
htm+='<li class="dl-item"><div class="dl-row"><span>'+ic+'</span><span class="dl-name" title="'+esc(nm)+'">'+esc(nm)+'</span><span class="pct '+pctCls(p)+'">'+p+'%</span></div><div class="dl-bar"><div class="dl-bar-fill '+bc+'" style="width:'+p+'%"></div></div><div class="dl-meta"><span>'+sz.toFixed(1)+' MB</span><span class="speed">'+(sp>0?sp.toFixed(2)+' MB/s':'')+'</span><span>'+st+'</span></div></li>';}
htm+='</ul>'}else{htm+='<div class="empty-state"><div class="icon">\uD83D\uDCED</div><p>\u65E0\u6D3B\u8DC3\u4E0B\u8F7D</p></div>';}
htm+='</div>';return htm;}
var LOGOS={"baidu": "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAABGdBTUEAALGPC/xhBQAAACBjSFJNAAB6JgAAgIQAAPoAAACA6AAAdTAAAOpgAAA6mAAAF3CculE8AAAARGVYSWZNTQAqAAAACAABh2kABAAAAAEAAAAaAAAAAAADoAEAAwAAAAEAAQAAoAIABAAAAAEAAAAQoAMABAAAAAEAAAAQAAAAADRVcfIAAAHJaVRYdFhNTDpjb20uYWRvYmUueG1wAAAAAAA8eDp4bXBtZXRhIHhtbG5zOng9ImFkb2JlOm5zOm1ldGEvIiB4OnhtcHRrPSJYTVAgQ29yZSA2LjAuMCI+CiAgIDxyZGY6UkRGIHhtbG5zOnJkZj0iaHR0cDovL3d3dy53My5vcmcvMTk5OS8wMi8yMi1yZGYtc3ludGF4LW5zIyI+CiAgICAgIDxyZGY6RGVzY3JpcHRpb24gcmRmOmFib3V0PSIiCiAgICAgICAgICAgIHhtbG5zOmV4aWY9Imh0dHA6Ly9ucy5hZG9iZS5jb20vZXhpZi8xLjAvIj4KICAgICAgICAgPGV4aWY6Q29sb3JTcGFjZT4xPC9leGlmOkNvbG9yU3BhY2U+CiAgICAgICAgIDxleGlmOlBpeGVsWERpbWVuc2lvbj42NDwvZXhpZjpQaXhlbFhEaW1lbnNpb24+CiAgICAgICAgIDxleGlmOlBpeGVsWURpbWVuc2lvbj42NDwvZXhpZjpQaXhlbFlEaW1lbnNpb24+CiAgICAgIDwvcmRmOkRlc2NyaXB0aW9uPgogICA8L3JkZjpSREY+CjwveDp4bXBtZXRhPgohBDnEAAACk0lEQVQ4EaVTS0hUURj+7mPunWbU0Sl7OG0SM1PMRKRty1oURRRB2EOidhmkUQNiL4ImahEt2rVpLxSBWFpjgaljhmIa5jjjC3VGRkadO/cx93TOHb2ktYl+OPd1/v873/f9/wUhhKcrQNe/BqvhQS9/FHd9VkhnMGUBKmmTtL5ZISM/1L8dEGAAG6K7RyGlVVFSciBCBodU8vJVkuwsDpPDR6bJQszYkMteeGyK/gEVSppA14DhUQ3fRzQ4nTwmpw1MRPRN2YC4/mVm1kA4YqCiXIJrCwfTBMr3SVAUE6pKUFbqgMfD40OXgtoaGTnutbMZjeRyhhw/PUt8eycsyqGvadLTp7AtK9o7VsnPsEau34yRHXsmiL8lTkwzu2cxmKL0Rsd0SA4OwU8KLp7LBWP06GkC+R4B9edzYRjAl7403C6O3lVoGoEsc1kJRbtElBQ7MPBNRVWljPhiBmcvzGM8rEOgRwwMqnj+pNDKiUQN1FTLcEicpZ5jRNjT9IyBsXEd5WUSGprirDsQRQ7dvWmAZlypz8OlujwMDWs4VCsjNyfrgW3ibp9oFVxrjCMS1XG7MR+TUxlEJg0kkxm8bUtB0wn8TV6Iwrr1gN3GWDyDusvzqKyQ8OJZIaoPOi0W95u98BYIeHjXi/b3Kdy4Fad+WKQtFBug46NiaT51wg3/nUW0vl6B7OQgS0DGpHIEDi4Xj7Z3KZpHHV0LG8DhANJ0gDqDCu41b8Wxo24sUjOTyyYCD7aBSdSpBI56J/wmwTYxkTBxtWEBoX4V+6mR3gIeS0smdEqXSVimQGO0K2dO5qDF77VBGECAsmlijFZWTfSGsj32FYnw0fYyvVE6J3NzBrYXCnQKneBt3njMjPqv3/kXJUADijW0BeUAAAAASUVORK5CYII=", "youtube": "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAAXNSR0IArs4c6QAAAERlWElmTU0AKgAAAAgAAYdpAAQAAAABAAAAGgAAAAAAA6ABAAMAAAABAAEAAKACAAQAAAABAAAAEKADAAQAAAABAAAAEAAAAAA0VXHyAAAAw0lEQVQ4EaVTiw2FIAwsL28ANpARGIEV3MAR3UBHcATcgA14LfLRgI0+SAjl6J09qMJ7Dz3j00Mm7jcLCCEx1nFvMn4N1rjdwHsXYrKAJiacZObNnIJ9JOmXxPNHNN2BimX9swSB5LsWsBbAmBoviOJfYRgAlgVgnrFOVWiniBdIiVKmqFp5gX0HGMfDBtlpDOqDrYEfkMbrce72GA8sVWBvM3gy0aih+hpJ5J/p2spYO8hGZWvEcisXgUb2E4h/hQcKPzl8hRVGXE8aAAAAAElFTkSuQmCC", "github": "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAABGdBTUEAALGPC/xhBQAAACBjSFJNAAB6JgAAgIQAAPoAAACA6AAAdTAAAOpgAAA6mAAAF3CculE8AAAARGVYSWZNTQAqAAAACAABh2kABAAAAAEAAAAaAAAAAAADoAEAAwAAAAEAAQAAoAIABAAAAAEAAAAQoAMABAAAAAEAAAAQAAAAADRVcfIAAAHJaVRYdFhNTDpjb20uYWRvYmUueG1wAAAAAAA8eDp4bXBtZXRhIHhtbG5zOng9ImFkb2JlOm5zOm1ldGEvIiB4OnhtcHRrPSJYTVAgQ29yZSA2LjAuMCI+CiAgIDxyZGY6UkRGIHhtbG5zOnJkZj0iaHR0cDovL3d3dy53My5vcmcvMTk5OS8wMi8yMi1yZGYtc3ludGF4LW5zIyI+CiAgICAgIDxyZGY6RGVzY3JpcHRpb24gcmRmOmFib3V0PSIiCiAgICAgICAgICAgIHhtbG5zOmV4aWY9Imh0dHA6Ly9ucy5hZG9iZS5jb20vZXhpZi8xLjAvIj4KICAgICAgICAgPGV4aWY6Q29sb3JTcGFjZT4xPC9leGlmOkNvbG9yU3BhY2U+CiAgICAgICAgIDxleGlmOlBpeGVsWERpbWVuc2lvbj4zMjwvZXhpZjpQaXhlbFhEaW1lbnNpb24+CiAgICAgICAgIDxleGlmOlBpeGVsWURpbWVuc2lvbj4zMjwvZXhpZjpQaXhlbFlEaW1lbnNpb24+CiAgICAgIDwvcmRmOkRlc2NyaXB0aW9uPgogICA8L3JkZjpSREY+CjwveDp4bXBtZXRhPgqWsr5jAAACQElEQVQ4EXVSS2sTURS+50yiJplogzOdvNBNdi6CiojQnY9SHxtRwR+guHDXRTeWoi5dCG4Ltv6GLERc2o1SK9WNUOhGYzPJNBXUCM3MvX5nOtEY0gNn7nl93/3m3ktqxBzHyaeIZgzRHTLmtLQRv0e8GBrzMgiCH8MQGk48x7lGzA+NMSeH64OYiD6w1gvfgqAxqFmDoOi69xXRcwxVBrXRlYlKWqkb+Vxu52ev9076MUHRca5A5hLyAASLWCfgLgi34T3EGUjdQLyM9bgRkmx2DSQb5LquDeYVNOqQ/trvdC5VKpWjZnf3GEVRB2DAM04URc1Wq9XxXLeBTa6CZF0bM5WChMsA1sGuQAKFymo2m9tYxQf2NQkwRDKDkzV1wTKk300KyjC/QBzF+fiPwc5L2HAPAizjek5JhlLfiqK18bh/VchfR/ZLKoJlSEolbatPlEnifRcOw0OQn44HgGUkq5KAmRn/tC8yaWiiaUweiFNgrbxth5B/HYXv8LOHbXuzWCptdrvd/86iVqsdTKfTM5h5As/BFW5vnuTpWkRvIeELbmEFB/QIDfnPua12+5UMep53Hkofw88gxUsnObOPkdZTHL9t5jk0L8J34LdB8kZZ1mcBJ9ZG/Rzi+LzQD7HhA8Hit5Xyfb/BzLMozsNPoLSqtQ6kJ4ZHJPFv2RnWh88KRpKYQIIt33+K9j34TchbBqEjdTHsyKil4J/wbm7htT7b64z5FgqFI2XPu1CtVv9eablczpYmJ6cnYKOQP8GM3/4taKC9AAAAAElFTkSuQmCC", "google": "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAABGdBTUEAALGPC/xhBQAAACBjSFJNAAB6JgAAgIQAAPoAAACA6AAAdTAAAOpgAAA6mAAAF3CculE8AAAARGVYSWZNTQAqAAAACAABh2kABAAAAAEAAAAaAAAAAAADoAEAAwAAAAEAAQAAoAIABAAAAAEAAAAQoAMABAAAAAEAAAAQAAAAADRVcfIAAAHJaVRYdFhNTDpjb20uYWRvYmUueG1wAAAAAAA8eDp4bXBtZXRhIHhtbG5zOng9ImFkb2JlOm5zOm1ldGEvIiB4OnhtcHRrPSJYTVAgQ29yZSA2LjAuMCI+CiAgIDxyZGY6UkRGIHhtbG5zOnJkZj0iaHR0cDovL3d3dy53My5vcmcvMTk5OS8wMi8yMi1yZGYtc3ludGF4LW5zIyI+CiAgICAgIDxyZGY6RGVzY3JpcHRpb24gcmRmOmFib3V0PSIiCiAgICAgICAgICAgIHhtbG5zOmV4aWY9Imh0dHA6Ly9ucy5hZG9iZS5jb20vZXhpZi8xLjAvIj4KICAgICAgICAgPGV4aWY6Q29sb3JTcGFjZT4xPC9leGlmOkNvbG9yU3BhY2U+CiAgICAgICAgIDxleGlmOlBpeGVsWERpbWVuc2lvbj4zMjwvZXhpZjpQaXhlbFhEaW1lbnNpb24+CiAgICAgICAgIDxleGlmOlBpeGVsWURpbWVuc2lvbj4zMjwvZXhpZjpQaXhlbFlEaW1lbnNpb24+CiAgICAgIDwvcmRmOkRlc2NyaXB0aW9uPgogICA8L3JkZjpSREY+CjwveDp4bXBtZXRhPgqWsr5jAAACfElEQVQ4EXVTz0tUURT+3o9RZ0YyiSIrpNoESZSYoRmEES5D002Bk1AKZZuC8h+Qglpki2ihLiKERIlq1cIi0yJIEfwBif1QtEVClI7z8713b995k+JYHvju3HPud8797pnzgA2mtd7qum4TMex5XkKglBpi/IKcbaBnu0yqJ2mC2MzGyTm7PstYdXjTdcM07zBgOWMfkXwzAO/bFx5rWHv3I6+6BoHScqE7VNRmWdY9cfwCvK6O+z6kktbKww4kXvRDxVZgBALCgXYcGLl5CF9qRTjSDPI9FmmwbfuZTaeAnHa5KPqAyf2PYYTCCJ5pQE55hV8g/WEY7tfPyCk75vuGYVCA1c7c11KtidDewqD+ee6g/nGyXFOBhLItupTtZ7wIKGVY9t70ZZ3qhY51tawRFXeJtNZxgUOklA/Hy1CY+9ampjLRpZcnYRYCwROHfJmyxFMaLd1JRJMaJrslSLvA+eMBHyxz1NYk+p3kr0XHUJL6rwknxoLLCWApLlkZk6KjsrXyS7DgmniyMMt+ZiyUa6C7OYjeqyH0XAnhQJEJT2kUFZo+gc0cMbl0ivdrRyNuxqtxa3YOfTMvMwSuefwngznA0LSHqXlgW76Bw8VrBToNvmMLee+pvOT2SBd6Pj1F2A7idHEVKncegWmaGF2cwKsJIPW9HpGK7WitkfnQk3xwlf98FqllpD+p0tb9sUe+gqgTQ8CUHsvouQgFbNTuuoi2yjpR5XJyZZCer/YPDFzje+6CQzKyOIWB+XeY+T3HOQH2FezGqT2VqCoqZTntKqVvcJA6/OrrF/lQqGac+L8pJWeidnMjQT7nCIdkkIj9xSDjjYSMfZb9ARmRtLGfbP3kAAAAAElFTkSuQmCC", "yahoo": "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAABGdBTUEAALGPC/xhBQAAACBjSFJNAAB6JgAAgIQAAPoAAACA6AAAdTAAAOpgAAA6mAAAF3CculE8AAAARGVYSWZNTQAqAAAACAABh2kABAAAAAEAAAAaAAAAAAADoAEAAwAAAAEAAQAAoAIABAAAAAEAAAAQoAMABAAAAAEAAAAQAAAAADRVcfIAAAHJaVRYdFhNTDpjb20uYWRvYmUueG1wAAAAAAA8eDp4bXBtZXRhIHhtbG5zOng9ImFkb2JlOm5zOm1ldGEvIiB4OnhtcHRrPSJYTVAgQ29yZSA2LjAuMCI+CiAgIDxyZGY6UkRGIHhtbG5zOnJkZj0iaHR0cDovL3d3dy53My5vcmcvMTk5OS8wMi8yMi1yZGYtc3ludGF4LW5zIyI+CiAgICAgIDxyZGY6RGVzY3JpcHRpb24gcmRmOmFib3V0PSIiCiAgICAgICAgICAgIHhtbG5zOmV4aWY9Imh0dHA6Ly9ucy5hZG9iZS5jb20vZXhpZi8xLjAvIj4KICAgICAgICAgPGV4aWY6Q29sb3JTcGFjZT4xPC9leGlmOkNvbG9yU3BhY2U+CiAgICAgICAgIDxleGlmOlBpeGVsWERpbWVuc2lvbj40ODwvZXhpZjpQaXhlbFhEaW1lbnNpb24+CiAgICAgICAgIDxleGlmOlBpeGVsWURpbWVuc2lvbj40ODwvZXhpZjpQaXhlbFlEaW1lbnNpb24+CiAgICAgIDwvcmRmOkRlc2NyaXB0aW9uPgogICA8L3JkZjpSREY+CjwveDp4bXBtZXRhPgrZdbzTAAABsUlEQVQ4EYWTOy9EQRTHf3d22SBRsEuEkKyCxGNVEiIaGolQ+AI+g1Yi0fgYOoXGYyMa5RZCIpZ4RSEeQWIRhffaO87suNm99orTzMx//v9zzpwzx5nu1P0hxTIOdVpTYu4XuK6FHQWhMDgOCDdDiPGwUiRFXBsozkEsDtX14kCcf7zAzXFeLBJiuCTDsgkUG4fhcpiYg6Yum8HmAlymBY/k/eFooiog6zw79wnxXmjstOIvOaeTkr48wzOjLTp68M8qqfWM2/ca5GwLrg+kBmV+XqADU7i6VmgbLJB3VyEn+G8LdJDLQvcIRKos/f4cTlO2Jv860NKyyhpIjBao++vw+uR/v3dbkoGJ3jEsrWmxlOw77K3Z/nui4lW+hd9MlT+eITVvU76/gMcrqXYJ0+p8sOl9eaUl3xzB4QZ8vglW4Q9SfPI5cCX9oSkYmLSU7UVYnS2ml+4LNZDoSnrc2lcgNff8/Dq5+8uU/BdrsjH9310B8+tM8XaW7GqGJ8gM7MwkdEYGJeoFMW1saLcDc3siWYWCpIKbgJoHpRVjQsl4UUwXrg/t1P0lznM1dzLmY98XE4KRGYksiAAAAABJRU5ErkJggg=="};
var PUBLIC_SERVICES=[
  ["baidu","百度","baidu_ms",50,200],
  ["youtube","YouTube","ytb_ms",200,500],
  ["github","GitHub","github_ms",50,200],
  ["google","Google","google_ms",100,300],
  ["yahoo","Yahoo","yahoo_hk_ms",50,200],
];
function netCard(c){var ns=c.nodes||{};var lk=c.network||{};var l200=lk.link200||{};var lkc=l200.up?'#0f0':'#f44';var lkt=l200.latency?l200.latency.toFixed(2)+'ms':(l200.up===false?'DOWN':'...');
var htm='<div class="card net-card"><div class="net-top"><div class="net-200g"><span class="net-200g-icon">\u26a1</span><span class="net-200g-label">200G</span><span class="net-200g-nodes">'+esc(l200.link200_nodes||'spark-9051 \u2194 spark-9797')+'</span><span class="net-200g-lat" style="color:'+lkc+'">'+lkt+'</span></div>';
var tbs=lk.tb||[];tbs.forEach(function(tb){htm+='<div class="net-tb-link">\U0001f5e1 '+esc(tb.from)+' \u2194 '+esc(tb.to)+' <span class="net-tb-speed">'+esc(tb.speed)+'</span></div>';});
htm+='</div>';
// Toggle button + legend inline
htm+='<div class="tog ex" onclick="toggleMatrix(this)"><span class="tog-i">\u25b6</span><span class="tog-t">\u516c\u7f51\u5ef6\u8fdf</span><span class="tog-b">5\u00d75</span><span class="leg"><span class="leg-i"><span class="leg-d" style="background:rgba(34,197,94,.5)"></span>&lt;50ms</span><span class="leg-i"><span class="leg-d" style="background:rgba(234,179,8,.5)"></span>50-200ms</span><span class="leg-i"><span class="leg-d" style="background:rgba(239,68,68,.5)"></span>&gt;200ms</span></span></div>';
htm+='<div class="net-matrix-wrap open"><div class="pg-grid">';
// Column headers
var nn=typeof NODE_NAMES!='undefined'?NODE_NAMES:{};
htm+='<div></div>';
for(var si=0;si<PUBLIC_SERVICES.length;si++){
  var sk=PUBLIC_SERVICES[si];
  htm+='<div class="ph"><img src="'+LOGOS[sk[0]]+'" alt="'+sk[1]+'" class="pgi"><span class="sn">'+sk[1]+'</span></div>';
}
// Location column
htm+='<div class="ph"><svg viewBox="0 0 16 16" width="14" height="14"><path d="M8 1C5.24 1 3 3.24 3 6c0 3.75 5 9 5 9s5-5.25 5-9c0-2.76-2.24-5-5-5z" fill="#ef4444"/><circle cx="8" cy="6" r="2" fill="#fff"/></svg><span class="sn">\u4f4d\u7f6e</span></div>';
// Data rows
var mKeys=Object.keys(ns);
for(var ri=0;ri<mKeys.length;ri++){
  var nodeKey=mKeys[ri];var nd=ns[nodeKey];
  var fn=nn[nodeKey]||(nodeKey.indexOf('.')>=0?nodeKey.split('.')[0]:nodeKey);
  var sp=fn.indexOf(' ');if(sp>0)fn=fn.substring(0,sp);
  htm+='<div class="pr">'+esc(fn)+'</div>';
  var pings=(nd.ping||{});
  for(var si=0;si<PUBLIC_SERVICES.length;si++){
    var sk=PUBLIC_SERVICES[si];var val=pings[sk[2]];var g=sk[3];var y=sk[4];
    if(typeof val==='number'){
      var c=val<g?'rgba(34,197,94,0.15)':val<y?'rgba(234,179,8,0.15)':'rgba(239,68,68,0.15)';
      var tc=val<g?'rgba(34,197,94,.9)':val<y?'rgba(234,179,8,.9)':'rgba(239,68,68,.9)';
      htm+='<div style="background:'+c+';color:'+tc+';font-weight:bold">'+Math.round(val)+'ms</div>';
    }else{
      htm+='<div class="na">N/A</div>';
    }
  }
  // Location
  var loc=pings['public_loc']||'';
  htm+='<div style="color:#667;font-size:6px">'+esc(loc.slice(0,12))+'</div>';
}
htm+='</div></div></div>';return htm;}
// Toggle public ping matrix
function toggleMatrix(el){el.classList.toggle('ex');var w=el.nextElementSibling;if(w)w.classList.toggle('open')}
function switchTheme(t){localStorage.setItem('dltrace-theme',t);document.body.className='theme-'+t;document.querySelectorAll('.theme-btn,.stheme-btn').forEach(function(b){b.classList.toggle('active',b.dataset.theme===t)})}
function applyTheme(t){switchTheme(t)}
function navTo(el,id){document.getElementById(id).scrollIntoView({behavior:'smooth'});document.querySelectorAll('.nav-item.active').forEach(function(e){e.classList.remove('active')});el.classList.add('active')}
(function(){var t=localStorage.getItem('dltrace-theme')||'s360';applyTheme(t)})();
// Job cards
function fmtTime(s){if(s==null||s<0)return'--:--:--';var h=Math.floor(s/3600),m=Math.floor((s%3600)/60),ss=Math.floor(s%60);return (h<10?'0':'')+h+':'+(m<10?'0':'')+m+':'+(ss<10?'0':'')+ss;}
function jobCard(n){var js=n.jobs||[];var nn=typeof NODE_NAMES!=='undefined'?NODE_NAMES:{};var fn=nn[n.hostname]||(n.hostname||'').split('.')[0];
if(!js.length)return'';var htm='';
for(var i=0;i<js.length;i++){var j=js[i];var p=j.pct;var done=j.status==='done'||j.status==='error';var el=j.elapsed_s||0;var rem=j.remaining_s;var barW=done?100:(p!=null?Math.min(100,Math.max(0,p)):(rem!=null&&el>0?Math.min(100,(el/(el+rem))*100):0));
htm+='<div class="card job-card"><div class="job-header"><span class="job-name" title="'+esc(j.name||j.id)+'">'+(j.type==='content'?'\u{1F3AC} ':'\u{2699} ')+esc(j.name||j.id)+'</span><span class="job-type '+(j.type||'content')+'">'+(j.type==='content'?'\u5185\u5BB9\u751F\u4EA7':j.type==='code'?'\u4EE3\u7801\u7F16\u8BD1':'')+'</span></div><div class="job-progress"><div class="job-progress-fill'+(done?' job-done':'')+'" style="width:'+barW+'%"></div></div><div class="job-timers"><span>\u23F1 '+fmtTime(el)+'</span>'+(rem!=null?'<span class="job-eta">\u23F3 '+fmtTime(rem)+'</span>':'')+'<span>'+(done?((j.status==='done'?'\u2705 \u5DF2\u5B8C\u6210':'\u274C \u9519\u8BEF')):(p!=null?Math.round(p)+'%':'...'))+'</span></div>';
if(j.detail)htm+='<div class="job-detail">'+esc(j.detail)+'</div>';
if(j.auto&&fn)htm+='<div class="job-node">\u{1F4BB} '+esc(fn)+'</div>';
htm+='</div>';}
return htm;}
// Job render collector
var _jobHtml='';
function collectJobCards(d){var ns=d.nodes||{};var htm='';var keys=Object.keys(ns);var any=false;
for(var i=0;i<keys.length;i++){var h=keys[i];var n=ns[h];var jc=jobCard(n);if(jc){htm+=jc;any=true;}}
if(!any)htm='<div class="job-empty"><div class="icon">\u{1F4AD}</div><p>\u65E0\u6D3B\u8DC3\u4EFB\u52A1</p><p style="font-size:8px;margin-top:4px">\u5199\u5165 /tmp/dltrace_jobs.json \u4EE5\u624B\u52A8\u58F0\u660E\u4EFB\u52A1</p></div>';
_jobHtml=htm;return htm;}
// Docker
function dockerCard(d){var ns=d.nodes||{};var keys=Object.keys(ns);var htm='';for(var i=0;i<keys.length;i++){var h=keys[i];var n=ns[h];var dc=n.docker||n.containers;if(!dc)continue;if(typeof dc==='object'&&!Array.isArray(dc)&&dc.containers){dc=dc.containers}if(!Array.isArray(dc)||!dc.length)continue;var fn=NODE_NAMES[h]||h.split('.')[0];var run=dc.filter(function(c){return c.state==='running'}).length;var stop=dc.length-run;var summary=(run?run+' running':'')+(run&&stop?', ':'')+(stop?stop+' stopped':'')||'n/a';
htm+='<div class="card dl-card" style="padding:8px"><div class="card-header"><span class="card-title online"><span class="status-dot online"></span>'+esc(fn)+'</span><span style="font-size:8px;color:#448">'+summary+'</span></div><div style="display:flex;flex-wrap:wrap;gap:3px;margin-top:3px">';
for(var j=0;j<dc.length;j++){var c=dc[j];var ctx=c.state==='running'?'#0f0;text-shadow:0 0 4px #0f0':'#f44;text-shadow:0 0 4px #f44';var ctn=c.state==='running'?'#1a1a1a':'#080812';htm+='<div style="background:'+ctn+';border:1px solid rgba(255,255,255,0.04);border-radius:3px;padding:3px 6px;font-size:8px"><span style="color:'+ctx+'" title="'+esc(c.state)+'">\u25CF</span> '+esc(c.name||c.id||'').substring(0,25)+(c.image?' <span style="color:#445">'+esc(c.image).substring(0,20)+'</span>':'')+'</div>';}
htm+='</div></div>';}
return htm||'<div class="empty-state"><p style="text-align:center;color:#334;font-size:10px">No Docker data</p></div>';}
// Disk I/O
function diskCard(d){var ns=d.nodes||{};var keys=Object.keys(ns);var htm='';for(var i=0;i<keys.length;i++){var h=keys[i];var n=ns[h];var io=n.diskio;if(!io||!Array.isArray(io)||!io.length)continue;var fn=NODE_NAMES[h]||h.split('.')[0];var totR=0,totW=0;
for(var k=0;k<io.length;k++){totR+=io[k].kb_read||0;totW+=io[k].kb_write||0;}
htm+='<div class="card dl-card" style="padding:8px"><div class="card-header"><span class="card-title online"><span class="status-dot online"></span>'+esc(fn)+'</span><span style="font-size:8px;color:#448">\u2191 '+fmtKb(totR)+'  \u2193 '+fmtKb(totW)+'</span></div><table style="width:100%;margin-top:4px;border-collapse:collapse;font-size:8px"><tr style="color:#556"><td style="padding:2px 4px">Device</td><td style="padding:2px 4px">Activity</td><td style="padding:2px 4px;text-align:right">R/W</td></tr>';
for(var k=0;k<io.length;k++){var dk=io[k];var r=dk.kb_read||0;var w=dk.kb_write||0;var mx=Math.max(1,totR,totW);var rp=r/mx*100;var wp=w/mx*100;
htm+='<tr><td style="padding:1px 4px;color:#aab;white-space:nowrap">'+esc(dk.device||'')+'</td>'
+'<td style="padding:1px 4px"><div style="display:flex;height:8px;border-radius:4px;overflow:hidden;width:50px;background:#1a1e2e">'
+'<div style="width:'+rp+'%;background:#2e6;min-width:1px"></div>'
+'<div style="width:'+wp+'%;background:#48f;min-width:1px"></div>'
+'</div></td>'
+'<td style="padding:1px 4px;text-align:right;white-space:nowrap;color:#889">R'+fmtKb(r)+'&nbsp;W'+fmtKb(w)+'</td></tr>';}
htm+='</table></div>';}
return htm||'<div class="empty-state"><p style="text-align:center;color:#334;font-size:10px">No disk I/O data</p></div>';}
// --- end docker+disk ---
// BG particles
(function(){var cv=document.getElementById("bg");if(!cv)return;var c=cv.getContext("2d");var pts=[];
function rs(){cv.width=window.innerWidth;cv.height=window.innerHeight}rs();window.addEventListener("resize",rs);
for(var i=0;i<60;i++)pts.push({x:Math.random()*cv.width,y:Math.random()*cv.height,vx:(Math.random()-.5)*.3,vy:(Math.random()-.5)*.3,r:Math.random()*1.2+.3});
(function d(){c.clearRect(0,0,cv.width,cv.height);c.strokeStyle="#0ff1";c.lineWidth=.5;
for(var i=0;i<pts.length;i++)for(var j=i+1;j<pts.length;j++){var p=pts[i],q=pts[j],dx=p.x-q.x,dy=p.y-q.y;if(dx*dx+dy*dy<8e3){c.beginPath();c.moveTo(p.x,p.y);c.lineTo(q.x,q.y);c.stroke()}}
for(var p of pts){p.x+=p.vx;p.y+=p.vy;if(p.x<0||p.x>cv.width)p.vx*=-1;if(p.y<0||p.y>cv.height)p.vy*=-1;c.fillStyle="#0ff";c.beginPath();c.arc(p.x,p.y,p.r,0,6.28);c.fill()}requestAnimationFrame(d)})();})();
var NODE_NAMES={{NODE_NAMES_JSON}}
var NODE_KEYS={{NODE_KEYS_JSON}}
var FIXED_NAME={{FIXED_NAMES_JSON}}
function render(d){var ns=d.nodes||{};var gt=d.gpu_trends||{};
var sys='';for(var i=0;i<NODE_KEYS.length;i++){var k=NODE_KEYS[i];var n=ns[k];var gtKey=gt[k];if(gtKey&&n){n.gpu_trends={};n.gpu_trends[k]=gtKey;}sys+=sysCard(k,n);}$('sys-grid').innerHTML=sys;
$('net-section').innerHTML=netCard(d);
var dl='';for(var i=0;i<keys.length;i++)dl+=dlCard(keys[i],ns[keys[i]]);$('dl-grid').innerHTML=dl;
var jb=collectJobCards(d);$('job-grid').innerHTML=jb;
var dc=dockerCard(d);$('docker-grid').innerHTML=dc;
var dk=diskCard(d);$('diskio-grid').innerHTML=dk;
var total=0;for(var k in ns)total+=ns[k].tracked_total||0;
var sb=document.querySelector('.topbar > span:last-child');if(sb)sb.innerHTML='<span class="status-dot online"></span> '+keys.length+' nodes \u00B7 '+total+' files  <span style="margin-left:8px;color:#224">5s \u00B7 auto</span>';
var mr=document.querySelector('meta[http-equiv="refresh"]');if(mr)mr.parentNode.removeChild(mr);}

(function(){var t=localStorage.getItem('dltrace-theme');if(t)switchTheme(t);})();
try{render({{DATA}});}catch(e){}
(function poll(){setTimeout(function(){fetch('/api/v1/metrics').then(function(r){return r.json()}).then(function(d){render(d);poll();}).catch(function(){setTimeout(poll,POLL);});},POLL);})();
setInterval(function(){var on=document.querySelectorAll('.status-dot.online').length;var tot=document.querySelectorAll('.sys-card').length||on;document.title=(on===tot?'\uD83D\uDFE2':'\uD83D\uDD34')+on+'/'+tot+' | K38';},3000);

function renderAlerts(data){var b=$('alert-banner'),c=$('alert-count'),i=$('alert-items');if(!b||!c||!i)return;var alerts=data&&data.alerts?data.alerts:[];if(!alerts||alerts.length===0){b.classList.remove('visible');return;}
c.textContent=alerts.length;var maxSev='warning';var items='';for(var ai=0;ai<Math.min(alerts.length,6);ai++){var a=alerts[ai];if(a.severity==='critical')maxSev='critical';items+='<span class="alert-item"><span class="alert-sev '+a.severity+'">'+a.severity+'</span>'+esc(a.metric)+'='+esc(a.value)+(a.node?' @'+esc(a.node):'')+'</span>';}
b.className='alert-banner visible '+maxSev;i.innerHTML=items;}

try{renderAlerts({{DATA}});}catch(e){}

// Patch poll to also call renderAlerts
var _origPoll=poll;poll=function(){_origPoll();setTimeout(function(){try{fetch('/api/v1/metrics').then(function(r){return r.json()}).then(function(d){renderAlerts(d);});}catch(e){}},500);};
</script></body></html>"""


# ════════════════════════════════════════════
# 模式: deploy - SSH部署守护进程
# ════════════════════════════════════════════

def cmd_deploy(args: argparse.Namespace):
    """deploy 模式: 通过SSH部署dltrace daemon到远程"""
    target = args.deploy
    if not target:
        print("[dltrace] Error: --deploy requires ssh target (user@host)")
        sys.exit(1)

    script_path = os.path.abspath(__file__)
    remote_script = f"~/{os.path.basename(script_path)}"

    print(f"[dltrace] Deploying to {target}...")

    # 上传脚本
    result = subprocess.run(
        ["scp", script_path, f"{target}:{remote_script}"],
        capture_output=True, text=True, timeout=15
    )
    if result.returncode != 0:
        print(f"[dltrace] SCP failed: {result.stderr}")
        sys.exit(1)
    print(f"[dltrace]   Uploaded {remote_script}")

    # 启动守护进程
    start_cmd = f"nohup python3 {remote_script} daemon > ~/dltrace.log 2>&1 &"
    result = subprocess.run(
        ["ssh", target, start_cmd],
        capture_output=True, text=True, timeout=10
    )
    if result.returncode == 0:
        print(f"[dltrace]   Daemon started on {target}")
    else:
        print(f"[dltrace]   Start failed: {result.stderr}")

    # 验证
    time.sleep(2)
    result = subprocess.run(
        ["ssh", target, "cat /tmp/dltrace.json | head -5"],
        capture_output=True, text=True, timeout=5
    )
    if result.stdout and '"ts"' in result.stdout:
        print("[dltrace]   ✅ Daemon running, data available")
    else:
        print("[dltrace]   ⚠️  Daemon may not be producing data yet")
        print(f"[dltrace]   Run: ssh {target} tail -20 ~/dltrace.log")


# ════════════════════════════════════════════
# CLI入口
# ════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        prog="dltrace",
        description="文件级下载进度追踪器 - 实时监控文件增长, 估算下载进度",
        epilog="GitHub: https://github.com/kk38/dltrace",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("-v", "--version", action="version", version=f"dltrace {__version__}")

    # 全局选项
    parser.add_argument("--ssh", dest="ssh", type=str, default=None,
                        help="主SSH目标 (user@host), web/watch模式时通过SSH读取数据")
    parser.add_argument("--add-node", dest="extra_nodes", type=str, default=[], action="append",
                        help="额外监控节点 (user@host), 可多次使用")
    parser.add_argument("-f", "--file", dest="file", type=str, default=None,
                        help=f"JSON进度文件路径 (默认: {PROGRESS_FILE})")
    parser.add_argument("--port", dest="port", type=int, default=DEFAULT_PORT,
                        help=f"Web面板端口 (默认: {DEFAULT_PORT})")

    sub = parser.add_subparsers(dest="mode", required=True)

    # daemon
    p_daemon = sub.add_parser("daemon", help="在目标机上启动守护进程")
    p_daemon.add_argument("--dirs", dest="watch_dirs", type=str, default=None,
                          help="监控目录列表 (逗号分隔)")

    # watch
    sub.add_parser("watch", help="终端实时查看下载进度")

    # web
    sub.add_parser("web", help="启动HTTP面板")

    # deploy
    p_deploy = sub.add_parser("deploy", help="通过SSH部署守护进程")
    p_deploy.add_argument("target", type=str, help="SSH目标 (user@host)")

    args = parser.parse_args()

    if args.mode == "daemon":
        watch_dirs = None
        if args.watch_dirs:
            watch_dirs = [d.strip() for d in args.watch_dirs.split(",")]
        tracker = DownloadTracker(
            watch_dirs=watch_dirs or DEFAULT_WATCH_DIRS,
            progress_file=args.file or PROGRESS_FILE,
        )
        tracker.run_loop()

    elif args.mode == "watch":
        if args.ssh:
            # SSH远程查看
            _ssh_watch(args)
        else:
            cmd_watch(args)

    elif args.mode == "web":
        cmd_web(args)

    elif args.mode == "deploy":
        # 兼容语法: dltrace deploy user@host
        args.deploy = args.target
        cmd_deploy(args)


def _ssh_watch(args: argparse.Namespace):
    """通过SSH远程watch"""
    target = args.ssh
    progress_file = args.file or PROGRESS_FILE

    print(f"[dltrace] Remote watch via {target}")
    print(f"[dltrace] Reading {target}:{progress_file}")
    print("[dltrace] Press Ctrl+C to quit\n")

    last_render = ""
    while True:
        try:
            result = subprocess.run(
                ["ssh", target, "cat", progress_file],
                capture_output=True, text=True, timeout=5
            )
            if result.returncode == 0 and result.stdout:
                data = json.loads(result.stdout)
            else:
                data = {}
        except (subprocess.TimeoutExpired, OSError, json.JSONDecodeError):
            data = {}

        files = data.get("active_files") or []
        ts = data.get("ts_str", "--:--:--")
        host = data.get("hostname", target)

        lines = [f"dltrace - {host} - {ts}", "─" * 60]
        if files:
            for f in files:
                pct = (f.get("pct") or 0)
                bar_len = max(0, pct // 5)
                bar = "█" * bar_len + "░" * (20 - bar_len)
                name = f.get("name", "?")
                size = (f.get("size_mb") or 0)
                speed = (f.get("speed_mb") or 0)
                tag = f.get("tag", "?")
                status = f.get("status", "?")
                size_str = f"{size:.1f}M" if size else "?M"
                speed_str = f"{speed:.2f}M/s" if speed > 0 else ""
                status_str = "✅" if status in ("done", "stale") else "⬇️" if status == "downloading" else "⏳"
                lines.append(f"  {bar} {pct:3d}% {status_str} {size_str:>8} {speed_str:>9} [{tag}] {name}")
        else:
            lines.append("  (无活跃下载)")

        new_render = "\n".join(lines)
        if new_render != last_render:
            os.system("clear" if os.name == "posix" else "cls")
            print(new_render)
            last_render = new_render

        try:
            time.sleep(2)
        except KeyboardInterrupt:
            break


if __name__ == "__main__":
    main()
