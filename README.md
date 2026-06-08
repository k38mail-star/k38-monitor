# K38 MONITOR (dltrace v0.3.0)

**Zero-dependency real-time cluster monitoring dashboard for heterogeneous 5-node clusters.**

Tracks system resources, file downloads, network links, and content production tasks — all from a single industrial-grade web panel.

```
██╗  ██╗██████╗  ██╗   ███╗ ██████╗ ███╗   ██╗██╗████████╗ ██████╗ ██████╗
██║ ██╔╝╚════██╗██║   ██╔██╗██╔═══██╗████╗  ██║██║╚══██╔══╝██╔═══██╗██╔══██╗
████╔╝  █████╔╝██║   ██║╚██╗██║   ██║██╔██╗ ██║██║   ██║   ██║   ██║██████╔╝
██╔═██╗  ╚═══██╗██║   ██║ ██║██║   ██║██║╚██╗██║██║   ██║   ██║   ██║██╔══██╗
██║  ██╗██████╔╝█████╗╚███╔███╗╚██████╔╝██║ ╚████║██║   ██║   ╚██████╔╝██║  ██║
╚═╝  ╚═╝╚═════╝ ╚════╝ ╚══╝╚══╝ ╚═════╝ ╚═╝  ╚═══╝╚═╝   ╚═╝    ╚═════╝ ╚═╝  ╚═╝
```

---

## Features

### System Monitoring
- CPU, Memory, GPU, Disk usage gauges per node
- GPU temperature with color-coded alerts (>85°C triggers red blink)
- 60-point SVG sparkline trend charts with gradient fills
- Uptime and load average display

### Download Tracking
- File-level download progress with real-time growth tracking
- Per-node download cards: speed, size, ETA, progress bar
- Auto-discovers downloads in common directories (/tmp, ~/Downloads, etc.)
- Completed files persist on display for 30 seconds

### Network Monitoring
- 200G InfiniBand link status with ping latency
- Thunderbolt bridge detection for Mac nodes
- Per-node ping reachability cards
- Dynamic node name labels

### Content Production (v0.3.0)
- **Job tracking dashboard** — declarative via `/tmp/dltrace_jobs.json`
- Auto-calculates elapsed time, countdown, and progress bars
- Type labels: Content Production (gold) / Code Compilation (cyan)
- **Auto-detection** of long-running compute processes (torchrun, ComfyUI, ffmpeg, gcc, cargo, etc.)
- Status: running / done / failed

### Security & Reliability
- Zero XSS (all user data HTML-escaped)
- No `eval()`, no `shell=True`, no bare `except:`
- PID lock prevents duplicate daemons
- History persistence across daemon restarts
- Atomic JSON writes via temp file rename
- `sys.dont_write_bytecode = True` — no stale .pyc poisoning

---

## Quick Start

```bash
# Start the daemon on each node
python3 dltrace.py daemon

# Start the web dashboard (from control node)
python3 dltrace.py \
  --ssh user@node1 \
  --add-node user@node2 \
  --add-node user@node3 \
  web

# Open: http://<control-node-ip>:8899/
```

---

## Job Tracking

Write to `/tmp/dltrace_jobs.json` on any monitored node:

```json
[
  {
    "id": "render-001",
    "name": "Wan2.1 Video Generation",
    "type": "content",
    "status": "running",
    "started_ts": 1740000000,
    "estimated_sec": 900,
    "detail": "1280×720 · 81 frames · Dual DGX"
  }
]
```

The dashboard automatically displays elapsed time, countdown timer, and progress bar.

---

## Architecture

```
┌─────────────────────────────────────────────┐
│              K38 MONITOR (Web)               │
│           http://<ip>:8899                   │
│                                              │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐        │
│  │ System  │ │Download │ │ Network │ Jobs   │
│  │ Gauges  │ │ Cards   │ │ Cards   │ Cards  │
│  └─────────┘ └─────────┘ └─────────┘        │
│                                              │
│  ▲ SSH polling (ThreadPoolExecutor)          │
│  │                                           │
├──┼───────────────────────────────────────────┤
│  │                                           │
│  ▼ daemon on each node                       │
│  /tmp/dltrace.json ← filesystem watch        │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐     │
│  │ DGX 大傻 │ │ DGX 二傻 │ │ Mac ...  │     │
│  │ GPU 81°C │ │ GPU 75°C │ │ CPU 12%  │     │
│  └──────────┘ └──────────┘ └──────────┘     │
└─────────────────────────────────────────────┘
```

---

## Version History

| Version | Highlights |
|---------|------------|
| v0.1.x | File download tracking, SSH remote collection, industrial UI |
| v0.2.0 | 0.0.0.0 bind, parallel SSH, PID lock, history, network (200G+TB), GPU alerts, sparklines |
| **v0.3.0** | **Job dashboard, process auto-detection, `__pycache__` root fix, panel rename, code audit** |

---

## Requirements

- Python 3.9+
- **Zero external dependencies** — pure stdlib only
- SSH access from control node to monitored nodes

## License

Internal K38 production tool.

---

**K38 Corporation** — Built for Nasdaq. 🚀
