"""File discovery and tracking."""

from __future__ import annotations

import os
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from .base import CollectorConfig, FileReport

SKIP_NAMES = {
    ".",
    "..",
    "__pycache__",
    "node_modules",
    ".git",
    ".cache",
    ".npm",
    ".conda",
    ".local",
    ".config",
    "lost+found",
    "snap",
}
SKIP_EXTS = {".pyc", ".pyo", ".log", ".tmp", ".swp", ".lock", ".part", ".aria2"}
SKIP_PATH_PATTERNS = [r"/\.git/", r"/miniforge3/", r"/anaconda3/", r"/snap/"]
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

SPEED_WINDOW_SEC = 60
DEAD_TIMEOUT = 30
STALE_TIMEOUT = 120
SHOW_DONE_SEC = 30
GROWTH_MIN_COUNT = 1


def should_skip(path: str) -> bool:
    """Return True when a path should be ignored."""

    name = os.path.basename(path)
    if name in SKIP_NAMES:
        return True
    ext = os.path.splitext(name)[1].lower()
    if ext in SKIP_EXTS:
        return True
    return any(re.search(pattern, path) for pattern in SKIP_PATH_PATTERNS)


def discover_files(watch_dirs: list[str], max_candidates: int) -> dict[str, str]:
    """Discover candidate files in watch directories."""

    candidates: dict[str, str] = {}
    for watch_dir in watch_dirs:
        if len(candidates) >= max_candidates * 3:
            break
        if not os.path.isdir(watch_dir):
            continue
        try:
            for entry in sorted(os.listdir(watch_dir)):
                fpath = os.path.join(watch_dir, entry)
                if should_skip(fpath):
                    continue
                try:
                    if not os.path.isfile(fpath):
                        continue
                    size = os.path.getsize(fpath)
                    if size > 1024 * 1024:
                        candidates[fpath] = entry
                except (OSError, PermissionError):
                    continue
        except (OSError, PermissionError):
            continue
    return candidates


@dataclass
class FileTracker:
    """Track one file's growth and lifecycle."""

    path: str
    name: str = field(init=False)
    prev_size: int = 0
    first_size: int | None = None
    first_seen: float = field(default_factory=time.time)
    last_growth: float = field(default_factory=time.time)
    growth_count: int = 0
    speed: float = 0.0
    expected_size: int | None = None
    pct: int = 0
    status: str = "new"
    history: list[tuple[float, int]] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.name = os.path.basename(self.path)

    def poll(self, now: float | None = None) -> bool:
        """Poll the file state and return True if it grew."""

        now = now or time.time()
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
        while len(self.history) > 2 and self.history[0][0] < now - SPEED_WINDOW_SEC:
            self.history.pop(0)
        while len(self.history) > 120:
            self.history.pop(0)

        recent = [(ts, val) for ts, val in self.history if ts > now - 10]
        if len(recent) >= 2:
            dt = recent[-1][0] - recent[0][0]
            ds = recent[-1][1] - recent[0][1]
            self.speed = (ds / dt) / (1024 * 1024) if dt > 0 else 0.0
        else:
            self.speed = 0.0

        grew = False
        if size > self.prev_size and (mtime > now - DEAD_TIMEOUT or self.growth_count > 0):
            self.last_growth = now
            self.growth_count += 1
            grew = True
        self.prev_size = size

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

    def to_report(self, now: float | None = None) -> FileReport:
        """Build a report object."""

        now = now or time.time()
        size_mb = self.prev_size / (1024 * 1024)
        if self.status in ("done", "stale"):
            self.pct = 100
        elif self.expected_size and self.expected_size > 0:
            self.pct = min(100, int(self.prev_size / self.expected_size * 100))
        else:
            self.pct = 0

        ext = os.path.splitext(self.name)[1].lower()
        tag = EXT_TAGS.get(ext, ext.lstrip(".") if ext else "file")
        return FileReport(
            name=self.name,
            path=self.path,
            tag=tag,
            pct=self.pct,
            size_mb=round(size_mb, 1),
            speed_mb=round(self.speed, 2),
            status=self.status,
            growth_count=self.growth_count,
            age_s=round(now - self.first_seen, 1),
            idle_s=round(now - self.last_growth, 1),
        )


def collect_file_reports(
    trackers: dict[str, FileTracker],
    watch_dirs: list[str],
    max_tracked_files: int,
    now: float | None = None,
) -> tuple[list[FileReport], int]:
    """Update trackers and return the active file reports plus tracker count."""

    now = now or time.time()
    discovered = discover_files(watch_dirs, max_tracked_files)

    for path in discovered:
        if path not in trackers:
            trackers[path] = FileTracker(path)

    to_remove: list[str] = []
    for path, tracker in list(trackers.items()):
        tracker.poll(now)
        if tracker.status in ("done", "stale") and (now - tracker.last_growth) > STALE_TIMEOUT:
            to_remove.append(path)
        elif tracker.growth_count == 0 and (now - tracker.first_seen) > DEAD_TIMEOUT:
            to_remove.append(path)
        elif path not in discovered and (now - tracker.last_growth) > 15:
            to_remove.append(path)

    for path in to_remove:
        trackers.pop(path, None)

    active: list[FileReport] = []
    seen_names: set[str] = set()
    for _, tracker in sorted(trackers.items(), key=lambda item: item[1].last_growth, reverse=True):
        if tracker.growth_count < GROWTH_MIN_COUNT and tracker.status not in ("done", "stale"):
            continue
        if tracker.status in ("done", "stale") and (now - tracker.last_growth) > SHOW_DONE_SEC:
            continue
        report = tracker.to_report(now)
        if report.name not in seen_names:
            seen_names.add(report.name)
            active.append(report)

    return active[:max_tracked_files], len(trackers)


__all__ = [
    "FileTracker",
    "collect_file_reports",
    "discover_files",
    "should_skip",
]
