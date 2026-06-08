# K38 MONITOR (dltrace v0.3.0)

**Zero-dependency real-time cluster monitoring dashboard for heterogeneous clusters.**

[English](#english) | [中文](#中文)

---

## English

Tracks system resources, file downloads, network links, and content production tasks — all from a single industrial-grade web panel.

### Features

**System Monitoring**
- CPU, Memory, GPU, Disk usage gauges per node
- GPU temperature with color-coded alerts (>85°C triggers red blink)
- 60-point SVG sparkline trend charts with gradient fills
- Uptime and load average display

**Download Tracking**
- File-level download progress with real-time growth tracking
- Per-node download cards: speed, size, ETA, progress bar
- Auto-discovers downloads in common directories
- Completed files persist on display for 30 seconds

**Network Monitoring**
- 200G InfiniBand link status with ping latency
- Thunderbolt bridge detection for Mac nodes
- Per-node ping reachability cards

**Content Production (v0.3.0)**
- Job tracking dashboard via `/tmp/dltrace_jobs.json`
- Auto-calculates elapsed time, countdown, and progress bars
- Type labels: Content Production (gold) / Code Compilation (cyan)
- Auto-detection of long-running compute processes

**Security**
- Zero XSS, no `eval()`, no `shell=True`, no bare `except:`
- PID lock, atomic JSON writes, `sys.dont_write_bytecode = True`

### Quick Start

```bash
# Start daemon on each node
python3 dltrace.py daemon

# Start web dashboard
python3 dltrace.py \
  --ssh user@node1 \
  --add-node user@node2 \
  web

# Open: http://<ip>:8899/
```

### Job Tracking

```json
[{
  "id": "render-001",
  "name": "Wan2.1 Video Generation",
  "type": "content",
  "status": "running",
  "started_ts": 1740000000,
  "estimated_sec": 900,
  "detail": "1280×720 · 81 frames · Dual DGX"
}]
```

### Requirements
- Python 3.9+
- Zero external dependencies — pure stdlib
- SSH access from control node to monitored nodes

---

## 中文

K38 人机共生集团 · 5 节点异构集群 · 工业级实时监控面板

```
██╗  ██╗██████╗  ██╗   ███╗ ██████╗ ███╗   ██╗██╗████████╗ ██████╗ ██████╗
██║ ██╔╝╚════██╗██║   ██╔██╗██╔═══██╗████╗  ██║██║╚══██╔══╝██╔═══██╗██╔══██╗
████╔╝  █████╔╝██║   ██║╚██╗██║   ██║██╔██╗ ██║██║   ██║   ██║   ██║██████╔╝
██╔═██╗  ╚═══██╗██║   ██║ ██║██║   ██║██║╚██╗██║██║   ██║   ██║   ██║██╔══██╗
██║  ██╗██████╔╝█████╗╚███╔███╗╚██████╔╝██║ ╚████║██║   ██║   ╚██████╔╝██║  ██║
╚═╝  ╚═╝╚═════╝ ╚════╝ ╚══╝╚══╝ ╚═════╝ ╚═╝  ╚═══╝╚═╝   ╚═╝    ╚═════╝ ╚═╝  ╚═╝
```

### 功能

**系统监控**
- 每节点 CPU / 内存 / GPU / 磁盘仪表盘
- GPU 温度颜色告警（>85°C 红色闪烁）
- 60 点 SVG sparkline 趋势图 + 渐变填充
- 运行时长 + 负载均值

**下载追踪**
- 文件级下载进度，实时增长追踪
- 每个节点独立下载卡片：速度、大小、进度条、预计剩余时间
- 自动发现常见下载目录中的文件
- 完成后保留 30 秒显示

**网络监测**
- 200G InfiniBand 链路状态 + ping 延迟
- Mac 节点 Thunderbolt 桥接检测
- 每节点 ping 可达性

**内容生产任务面板 (v0.3.0)**
- `/tmp/dltrace_jobs.json` 声明式任务追踪
- 自动计算已耗时 + 倒计时 + 进度条
- 类型标签：内容生产（金色）/ 代码编译（青色）
- 自动检测长运行计算进程（torchrun、ComfyUI、ffmpeg、gcc、cargo 等）

**安全与可靠性**
- 零 XSS、零 `eval()`、零 `shell=True`、零 bare `except:`
- PID 锁防重复运行、原子化 JSON 写入
- `sys.dont_write_bytecode = True` 根除 pyc 缓存毒瘤

### 快速启动

```bash
# 各节点启动守护进程
python3 dltrace.py daemon

# 控制节点启动 Web 面板
python3 dltrace.py \
  --ssh user@node1 \
  --add-node user@node2 \
  web

# 浏览器打开: http://<控制节点IP>:8899/
```

### 任务追踪

在任意被监控节点写入 `/tmp/dltrace_jobs.json`：

```json
[{
  "id": "render-001",
  "name": "Wan2.1 视频推理",
  "type": "content",
  "status": "running",
  "started_ts": 1740000000,
  "estimated_sec": 900,
  "detail": "1280×720 · 81帧 · 双DGX"
}]
```

面板自动显示进度条 + 已耗时 + 倒计时。

### 版本历史

| 版本 | 亮点 |
|------|------|
| v0.1.x | 文件下载追踪、SSH 远程采集、工业风 UI |
| v0.2.0 | 0.0.0.0 绑定、并行 SSH、PID 锁、网络监测(200G+TB)、GPU 告警、sparkline |
| **v0.3.0** | **任务面板、进程自动检测、pyc 根因修复、面板更名、代码审计** |

### 部署环境

- Python 3.9+
- **零外部依赖** — 纯 Python 标准库
- 控制节点到被监控节点需 SSH 免密

### 当前部署

| 节点 | 设备 | 状态 |
|------|------|------|
| 十六万 | Mac Studio M3 Ultra 512GB | ✅ Control Node |
| 大傻 | DGX Spark 128GB | ✅ GPU 81°C |
| 二傻 | DGX Spark 128GB | ✅ GPU 75°C |
| 小四 | Mac Studio M4 Max | ✅ Thunderbolt |
| 三万八 | Mac Studio M3 Ultra 96GB | ✅ WiFi |

---

**K38 Corporation** — Built for Nasdaq. 🚀
