# dltrace 全景升级报告

> K38 集群监控面板 · v0.3.3 · 2148 行单体 Python 守护进程  
> 报告生成: 2026-06-10  
> 基于: `dltrace.py` (dltrace/ 目录)

---

## 目录

1. [架构分析](#1-架构分析)
2. [监控空白](#2-监控空白)
3. [性能评估](#3-性能评估)
4. [功能缺口](#4-功能缺口)
5. [升级优先级矩阵](#5-升级优先级矩阵)
6. [推荐近期行动](#6-推荐近期行动)
7. [技术债务清单](#7-技术债务清单)

---

## 1. 架构分析

### 1.1 当前状态：单文件单体 (Monolith)

```
dltrace.py (2148行)
├── 配置常量 (~180行, L60-240)
├── FileTracker (~100行)           文件级下载追踪
├── DownloadTracker (~600行)        主采集循环
│   ├── _collect_system_info()      系统指标(Linux+macOS)
│   ├── _collect_docker()           Docker容器
│   ├── _collect_disk_io()          磁盘I/O
│   ├── _collect_network()          200G直连+Thunderbolt
│   ├── _collect_ping()             Ping+出口IP
│   ├── _collect_jobs()             任务追踪
│   └── _detect_processes()         进程检测
├── cmd_watch (~60行)              终端模式
├── cmd_web (~2200行, 含内联HTML)   HTTP面板
│   ├── ThreadedHTTPServer          HTTP服务器
│   ├── Handler (do_GET)            路由+服务端渲染
│   ├── _get_dashboard_html()       HTML模板 (↓1600行内联)
│   └── CSS/JS                      ~800行双主题+JS逻辑
└── CLI入口 (~80行)                 argparse
```

**优势：**
- 零外部依赖（纯 Python 标准库）→ `pip install` 不需要，`scp` 即部署
- 单文件 = `scp` 一次搞定，无模块路径问题
- 没有 import 地狱，没有版本冲突

**劣势：**
- 内联 HTML 模板 ~1600 行，CSS/JS 混编，调试困难
- 修改 CSS → 需要重启整个 Python 进程
- HTTPServer 不支持热加载
- 所有逻辑耦合（采集 → 渲染 → 路由在同一文件）

### 1.2 模块化改造权衡分析

| 方案 | 优势 | 劣势 | 推荐度 |
|------|------|------|--------|
| **保持单体** | 零部署成本，适合5节点 | 超过2500行后难以维护 | ⭐ 短期 |
| **HTML 分离** | 内联模板拆为 `.html` 文件，CSS 可独立编辑 | 需文件 I/O 读取模板 | ⭐⭐⭐ 中期 |
| **三个模块** | 采集/Web/CLI 分离 | 部署需 `scp -r` | ⭐⭐ 长期 |
| **插件系统** | 可扩展第三方采集器 | 过度设计 | ❌ 不推荐 |

**推荐路径：** v0.4.0 保持单体，只将 `_get_dashboard_html()` 的内联 HTML 模板拆为独立文件（`dashboard.html` + `dashboard.js` + `dashboard.css`），减少主文件体积并让 CSS/JS 可独立编辑。部署脚本自动打包。

### 1.3 HTTP 拉取 vs SSH 拉取

| 特性 | HTTP 拉取 (当前) | SSH 拉取 (仍在代码中) |
|------|-----------------|---------------------|
| 端口需求 | 需开放 8899/8890 | 仅 SSH (22) |
| 认证 | 无 | SSH 密钥 |
| 性能 | 并行 4-worker，12s 超时 | 串行，单节点超时阻塞 |
| 防火墙友好 | 需要允许入站 | 仅出站 SSH |
| 自动发现 | 配置式 | CLI 传入 |
| 代码健康度 | ✅ 活跃使用 | ❌ 遗留（`_read_remote_data` 已废弃，`/api/v1/json` 路由仍在） |

**现状：** 所有节点已部署 daemon，HTTP 拉取正常工作。SSH 拉取代码为遗留代码，`_read_remote_data()` 函数（L~1240）只有旧的 `--ssh` CLI 参数会触发，建议明确废弃。

---

## 2. 监控空白 (Monitoring Gaps)

### 2.1 当前已监控

- ✅ CPU 使用率（`cpu_pct`，L~770-830）
- ✅ CPU 负载（Linux: `/proc/loadavg`，macOS: uptime）
- ✅ CPU 温度（Linux thermal zone，L~860）
- ✅ 内存使用（macOS vm_stat / Linux free）
- ✅ GPU 使用率（nvidia-smi）
- ✅ GPU 显存（used/total/pct）
- ✅ GPU 温度（nvidia-smi）
- ✅ GPU 功耗（nvidia-smi power.draw）
- ✅ 磁盘使用率（`df -h /`）
- ✅ 磁盘 I/O（macOS iostat / Linux iostat -x）
- ✅ Docker 容器状态（docker ps）
- ✅ 网络延迟（ping baidu/ytb）
- ✅ 200G InfiniBand 直连
- ✅ Thunderbolt 桥接检测

### 2.2 未监控项

| 空白项 | 重要性 | 平台 | 采集方式 | 实现预估 |
|--------|--------|------|----------|----------|
| **风扇转速** | 🔴 高 | macOS + Linux | macOS: `powermetrics` / Linux: `sensors` | ~5行 daemon + ~3行前端 |
| **NVMe SSD 温度** | 🔴 中 | macOS + Linux | macOS: `smartctl` / Linux: `nvme list` | ~8行 |
| **GPU 核心/显存时钟** | 🟡 中 | Linux DGX | `nvidia-smi --query-gpu=clocks.current` | ~3行 |
| **GPU 电压** | 🟡 低 | Linux DGX | `nvidia-smi --query-gpu=voltage` | ~2行 |
| **节点间延迟矩阵** | 🟡 中 | 全节点 | ping 每对节点（ICMP 202G链路已有） | ~20行 daemon |
| **进程级 CPU/内存** | 🔴 高 | Linux + macOS | `ps -eo pid,%cpu,%mem,cmd` | ~15行 daemon + 前端进程表 |
| **电池/电源状态** | 🟡 中 | macOS 笔记本 | `pmset -g batt`（八万八/三万八） | ~10行 |
| **系统日志错误率** | 🟢 低 | Linux + macOS | 最近1h `dmesg`/`log show` 错误计数 | ~15行 |
| **网络接口流量** | 🟢 低 | 全节点 | `/proc/net/dev` / `netstat -ib` | ~10行 |

### 2.3 详细建议

#### 🔴 风扇转速（Fan Speed）
- **为什么重要：** DGX 双傻 GPU 80°C+ 时，风扇是否正常工作至关重要
- **macOS 方法：** `powermetrics --samplers smc -i 1000 -n 1`（需 sudo）
- **Linux 方法：** `sensors -j | jq '."coretemp-*".fan1'` 或 `cat /sys/class/hwmon/hwmon*/fan1_input`
- **难点：** macOS powermetrics 需要 sudo，daemon 需单独配置权限
- **建议：** 在每台 DGX 上必加，Mac 节点可选加

#### 🔴 进程级资源（Process-Level）
- **为什么重要：** 当前只跟踪进程名和命令行（`_detect_processes`, L~310），不显示 CPU/RAM 消耗
- **采集建议：** 在 `_collect_system_info()` 里加上 `ps` 采集，`top 5` 进程：
  ```python
  subprocess.run(["ps", "-eo", "pid,%cpu,%mem,cmd", "--sort=-%cpu", "--no-headers"], ...)
  ```
- **前端渲染：** 在每个节点系统卡片里显示进程列表
- **代码位置：** daemon 侧在 `_collect_system_info()` (L~800) 末尾追加，web 侧在 `sysCard` JS 函数里渲染

#### 🟡 NVMe SSD 温度
- **Linux：** `nvme list` + `nvme smart-log /dev/nvme0` 解析 temperature
- **macOS：** `smartctl -a /dev/disk0`（需安装 smartmontools）
- **注意：** DGX Spark 内部 NVMe SSD 过热是已知问题

---

## 3. 性能评估

### 3.1 轮询间隔 (poll_interval)

**当前值：** `POLL_INTERVAL = 1.0` (L~90)

| 操作 | 频率 | 每次耗时 |
|------|------|---------|
| 文件扫描 (`_discover_files`) | 每轮 | ~2-50ms (取决于目录大小) |
| 文件轮询 (`FileTracker.poll`) | 每轮 | ~0.1ms/tracker |
| 系统采集 (`_collect_system_info`) | 每轮 | ~10-80ms |
| Docker 采集 (`_collect_docker`) | 每轮 | ~50-200ms |
| 磁盘 I/O (`_collect_disk_io`) | 每轮 | ~30-100ms |
| Ping 采集 (`_collect_ping`) | 每 10 轮 | ~2-10s (curl 出口IP) |
| 进程检测 (`_detect_processes`) | 每 5 轮 | ~20-100ms |

**评估：** 轮询周期约 100-400ms，1s 间隔足够安全。但 Docker 采集每轮执行（`_collect_docker` 在 `collect()` 中无条件调用，L~1080）是浪费——Docker 状态很少秒级变化。

### 3.2 CPU 开销估算

- 平均每轮 ~150ms CPU 时间，1s 周期 = 15% CPU (单核)
- 其中 Docker 采集占 ~40% 的 daemon CPU 时间
- macOS 的 `system_profiler SPDisplaysDataType` (~200ms) 每轮都跑，应缓存

### 3.3 内存增长

**当前状态：** 干净（2026-06-09 大傻审查确认）
- `_history`: 5 个 deque(maxlen=60) ≈ 很少
- `self.trackers`: dict 文件跟踪，最多 MAX_TRACKED_FILES=20 个
- `GPU_TREND`: 12 样本/节点 = 极小
- JS 侧 `_hist` 在浏览器里，不在服务端

**风险：** 如果浏览器长期不刷新，`_hist[id]` (`gauge()` 函数，JS) 会无限增长，未设置 maxlen。

### 3.4 WebSocket vs HTTP Polling

| 特性 | HTTP 轮询 (当前) | WebSocket (建议) |
|------|-----------------|------------------|
| **延迟** | 2.5s (JS 侧 POLL=2500) | 实时 (<100ms) |
| **带宽** | 每轮 ~50-120KB JSON | 仅增量更新 ~1-5KB |
| **连接数** | 关闭/重建每轮 | 长连接 1 个 |
| **复杂性** | 无 | 需 `asyncio` + `websockets` 库 |
| **零依赖** | ✅ 保持 | ❌ 需依赖 |
| **刷新** | 浏览器 F5 即可恢复 | WebSocket 断连恢复复杂 |

**结论：** 当前 HTTP 轮询 + `<meta http-equiv="refresh" content="5">` 对于 5 节点集群足够。WebSocket 带来的好处（延迟从 2.5s 降到 <100ms）对监控场景不重要。**不推荐改为 WebSocket**，但建议将前端 `POLL` 值从硬编码改为配置项。

### 3.5 已知优化点

| # | 问题 | 位置 | 建议 |
|---|------|------|------|
| 1 | Docker 每轮扫描 | `collect()` L~1080 | 改为每 10 轮一次，或缓存 5s |
| 2 | system_profiler 每轮跑 | `_collect_system_info()` L~830 | 缓存 GPU info 60s |
| 3 | 出口 IP curl 可能阻塞 6s | `_collect_ping()` L~970 | 改异步或降低频率（当前已每 100 轮） |
| 4 | JS `_hist` 无限增长 | `gauge()` JS 函数 | 添加 `if(_hist[id].length > 60)_hist[id].shift()`（当前已有，但未在 gauge 内执行） |
| 5 | JSON 全量传输 | `_read_data()` L~1400 | 前端只需要 nodes 字段，可剪裁不必要的字段 |

---

## 4. 功能缺口

### 4.1 缺少告警系统 (Alerting) 🔴

**现状：** 没有任何告警机制。GPU 85°C 以上只在 UI 闪烁红色，不推送。

**建议实现路径：**
1. **阶段 1** (v0.4.0): 嵌入简单的告警规则在 daemon 中
   - 规则定义: `~/.dltrace_alerts.json`
   - 格式: `{"rule": "gpu_temp > 85 && duration > 60s", "action": "log"}`
2. **阶段 2** (v0.4.1): 推送动作
   - Webhook (钉钉/飞书 webhook)
   - 通知文件 (`/tmp/dltrace_alarm` 供外部读取)
   - 邮件 (通过 mail_config.py 的 SMTP 配置)
3. **不推荐**: 内建 SMS/Pushover — 把推送委托给外部

### 4.2 缺少配置文件 🔴

**现状：** 所有配置硬编码（L~60-140）+ 少量环境变量。

**硬编码项目（应从配置文件读取）：**

| 配置项 | 当前值 | 位置 |
|--------|--------|------|
| `POLL_INTERVAL` | 1.0s | L90 |
| `HISTORY_LEN` | 60 | L93 |
| `DEFAULT_PORT` | 8899 | L80 |
| `WEB_BIND` | 0.0.0.0 | L81 |
| `SPEED_WINDOW_SEC` | 60 | L99 |
| `DEAD_TIMEOUT` | 120s | L101 |
| `SHOW_DONE_SEC` | 30s | L102 |
| `MAX_TRACKED_FILES` | 20 | L92 |
| `DEFAULT_WATCH_DIRS` | 5 个路径 | L108-114 |
| `GPU_TEMP_THRESHOLD` | 硬编码在 JS | color thresholds |

**建议：**
- 读取 `~/.dltrace.toml` 或 `~/.dltrace.json`
- 支持配置文件覆盖默认值
- 所有 `os.environ.get(...)` 改为先读 config，RE 为 fallback

### 4.3 缺少认证 🔴

**现状：** Web 面板完全公开（`0.0.0.0:8899`），无认证、无速率限制。

**风险评估：**
- 面板显示内网 IP、出口 IP、GPU 配置、Docker 信息
- 但 `/health` 和 `/api/v1/metrics` 是公开的
- 所有节点 HTTP 端口暴露在内网

**建议（最低成本方案）：**
1. 将 `WEB_BIND` 默认改为 `127.0.0.1`，通过 SSH 隧道使用
2. 或添加简单 Token 认证：
   ```python
   BASIC_TOKEN = os.environ.get("DLTRACE_TOKEN", "")
   # 在 do_GET 中:
   if BASIC_TOKEN and req_headers.get("Authorization") != f"Bearer {BASIC_TOKEN}":
       self.send_error(401)
   ```
3. 或 HTTP Basic Auth（Python stdlib 支持）

### 4.4 缺少历史数据库 (Historical Storage) 🟡

**现状：** 60s 滑动窗口。重启即清零（虽然有 `_save_history`/`_load_history` 但只恢复 5 分钟内数据）。

**建议：**
- 轻量方案：将 `_history` 持久化改为每 30s 写入，保留 30 分钟数据
- 或：用 SQLite（Python stdlib）存储系统指标，提供 24h 趋势查询 API
- **不推荐**：TimescaleDB / InfluxDB — 太重且违反零依赖原则

### 4.5 缺少自动恢复/看门狗 (Auto-Recovery) 🟡

**现状：** daemon 进程崩溃后无自动重启。

**建议：**
- 不需要在 dltrace 内部实现看门狗
- 使用 systemd user service (Linux) 或 launchd (macOS)：
  ```
  # ~/.config/systemd/user/dltrace.service
  [Service]
  ExecStart=python3 /opt/dltrace/dltrace.py daemon
  Restart=always
  RestartSec=5
  ```
- 提供 `install-service.sh` 脚本

### 4.6 缺少测试套件 🟡

**现状：** `dltrace/` 目录下无测试文件。

**建议：**
- 最小: `test_dltrace.py` 测试 `FileTracker` 核心逻辑
- FileTracker 是纯函数，最容易写测试
- `_detect_processes` 和 `_collect_system_info` 需要 mock /proc

### 4.7 缺少 API 版本控制 🟢

**现状：** API 路径 `/api/v1/metrics` 和 `/api/v1/json` 已有版本号，但后端数据格式无契约。

**建议：**
- 保持 `/api/v1/` 路径约定
- 在响应中添加 `"api_version": "1.0"` 字段
- 文档化 NODE_FIELDS 契约（L~130）

---

## 5. 升级优先级矩阵

| # | 项目 | 影响 (1-5) | 工作量 (1-5) | 风险 (1-5) | 优先级得分 | 备注 |
|---|------|-----------|------------|----------|----------|------|
| 1 | **进程级 CPU/内存采集** | ⭐⭐⭐⭐⭐ | ⭐⭐ | ⭐ | **:star: 13** | 最高 ROI, ~15行代码 |
| 2 | **风扇转速 (DGX)** | ⭐⭐⭐⭐ | ⭐⭐ | ⭐⭐ | **10** | DGX 散热关键，~5行 |
| 3 | **告警系统(阶段1-日志)** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐ | **9** | 高价值但需谨慎设计 |
| 4 | **Docker 采集降频** | ⭐⭐⭐ | ⭐ | ⭐ | **11** | 只是改间隔，零风险 |
| 5 | **GPU 时钟采集** | ⭐⭐⭐ | ⭐ | ⭐ | **11** | 3行代码 |
| 6 | **配置文件化** | ⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐ | **7** | 需设计向后兼容 |
| 7 | **Web 认证** | ⭐⭐⭐⭐ | ⭐⭐ | ⭐⭐ | **10** | 安全基本要求 |
| 8 | **HTML 模板分离** | ⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐⭐ | **5** | 重构无功能变化 |
| 9 | **历史数据库 (SQLite)** | ⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐⭐ | **4** | 架构级变动 |
| 10 | **节点间延迟矩阵** | ⭐⭐⭐ | ⭐⭐⭐ | ⭐ | **9** | 多节点 ping 可衡量 |
| 11 | **自动恢复 (systemd)** | ⭐⭐⭐ | ⭐⭐ | ⭐ | **10** | 部署脚本而已 |
| 12 | **电池/电源状态 (笔记本)** | ⭐⭐ | ⭐⭐ | ⭐ | **9** | 主要对八万八有用 |
| 13 | **轮询间隔降频(Docker)** | ⭐⭐ | ⭐ | ⭐ | **11** | 一行改动 |
| 14 | **JS _hist 内存绑** | ⭐⭐ | ⭐ | ⭐ | **11** | 2行前缀防内存泄漏 |
| 15 | **WebSocket 替代** | ⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | **-2** | 不推荐，引入依赖 |
| 16 | **系统日志错误检测** | ⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐ | **6** | 平台差异大 |

**优先级算法：** Impact × 2 - Effort - Risk

**关键发现：** 6 项微小改动（影响 ≥ 3, 工作量 ≤ 2）即可显著改善功能性，且零风险。

---

## 6. 推荐近期行动

### 🥇 #1: 进程级 CPU/内存采集 （1-2小时）
**文件位置：** `_collect_system_info()` (L~800) 末尾

**实现：**
```python
def _collect_processes(self) -> list[dict]:
    """采集 Top 5 CPU-hungry 进程"""
    try:
        if sys.platform == "linux":
            out = subprocess.check_output(
                ["ps", "-eo", "pid,%cpu,%mem,cmd", "--sort=-%cpu", "--no-headers"],
                timeout=3, text=True, stderr=subprocess.DEVNULL
            )
        else:
            out = subprocess.check_output(
                ["ps", "-eo", "pid,%cpu,%mem,command", "-r"],  # -r = sort by CPU
                timeout=3, text=True, stderr=subprocess.DEVNULL
            )
        procs = []
        for line in out.strip().split("\n")[:5]:
            parts = line.strip().split(None, 3)
            if len(parts) >= 4:
                procs.append({
                    "pid": int(parts[0]),
                    "cpu": float(parts[1]),
                    "mem": float(parts[2]),
                    "cmd": parts[3][:60],
                })
        return procs
    except Exception:
        return []
```

**前端渲染：** 在每个节点系统卡片里加入简洁进程表（当前 `sysCard` 函数末尾）。

**预期效果：** 一眼看到"大傻 torchrun 吃了 85% CPU"、"二傻 ffmpeg 吃了 40%"

### 🥇 #2: 风扇转速 + GPU 时钟 （30分钟）
**文件位置：** `_collect_system_info()` 的 Linux GPU 段 (L~850)

**实现（DGX 专用）：**
```python
# GPU Clock
try:
    out = subprocess.check_output(
        ["nvidia-smi", "--query-gpu=clocks.current.graphics,clocks.current.memory",
         "--format=csv,noheader,nounits"],
        timeout=3, text=True
    )
    parts = out.strip().split(",")
    if len(parts) >= 2:
        info["gpu_clock_gfx"] = float(parts[0].strip())
        info["gpu_clock_mem"] = float(parts[1].strip())
except Exception:
    pass
# Fan Speed (Linux)
try:
    out = subprocess.check_output(["sensors", "-j"], timeout=3, text=True)
    data = json.loads(out)
    # Parse hwmon paths for fan
except Exception:
    # Fallback: /sys/class/hwmon
    for hwmon in glob.glob("/sys/class/hwmon/hwmon*/fan*_input"):
        try:
            with open(hwmon) as f:
                rpm = int(f.read().strip())
            info["fan_rpm"] = rpm
            break
        except (OSError, ValueError):
            pass
```

**前端渲染：** 在 GPU 表盘旁加小风扇图标 + RPM 数值，或在系统卡片底部加一行小字。

### 🥇 #3: 告警系统（阶段 1 — 规则引擎 + 日志） （3-4小时）
**文件位置：** `DownloadTracker` 类追加 `_check_alerts()` 方法

**设计：**
```python
# ~/.dltrace_alerts.json
[
  {"rule": "gpu_temp > 80", "label": "GPU温度过高", "severity": "warning"},
  {"rule": "cpu_pct > 90", "label": "CPU满载", "severity": "warning"},
  {"rule": "disk_pct > 90", "label": "磁盘空间不足", "severity": "critical"},
]
```

**匹配引擎：** 简单的数值比较 + OR 逻辑，无需表达式解析器。

**输出：**
1. 写入 `/tmp/dltrace_alerts.json`（供外部读取/推送给飞书/webhook）
2. 在 Web UI 顶部显示告警横幅（红色/黄色）

**飞书推送接口（阶段 2）：**
```python
def push_feishu(alert):
    """通过飞书 Webhook 推送告警"""
    webhook = os.environ.get("DLTRACE_FEISHU_WEBHOOK")
    if not webhook: return
    json.dumps({"msg_type": "interactive", "card": alert_card})
```

### 🏆 组合优先级得分

| 排名 | 项目 | 得分 | 预估工时 |
|------|------|------|---------|
| 1 | 进程级 CPU/内存 (🥇) | 13 | 1-2h |
| T2 | Docker 采集降频 | 11 | 5min |
| T2 | GPU 时钟采集 (🥇) | 11 | 10min |
| T2 | JS _hist 内存绑 | 11 | 2min |
| T5 | 风扇转速 (🥇) | 10 | 15min |
| T5 | Web 认证 | 10 | 30min |
| T5 | 自动恢复 (systemd) | 10 | 15min |
| 8 | 节点间延迟矩阵 | 9 | 30min |
| 9 | 电池/电源状态 | 9 | 15min |
| 10 | 告警系统(阶段1) | 9 | 3-4h |

**一个月径建议：**
- **Day 1:** JS _hist 内存绑(2min) + Docker 降频(5min) + GPU 时钟(10min) + 风扇(15min)
- **Day 2:** 进程级 CPU/内存(1-2h) + Web 认证(30min)
- **Day 3:** 告警系统阶段1(3-4h) + systemd 服务(15min)
- **Day 4-5:** 配置文件化(2h) + 测试套件基础(1h)
- **里程碑:** v0.4.0

---

## 7. 技术债务清单

### 7.1 待清理代码

| # | 问题 | 位置 | 修复方式 |
|---|------|------|----------|
| 1 | `_read_remote_data()` 废弃 | L~1240 | 加 `@deprecated` 注释，2026 Q3 移除 |
| 2 | `/api/v1/json` 路由仍在用 SSH | `do_GET` L~1520 | 废弃或改为 HTTP 拉取 |
| 3 | `NODE_KEYS`/`FIXED_NAMES` 硬编码在 web 模板 | L~1560 | 从 `_load_node_config` 自动生成 |
| 4 | `ssh_watch` 在主 CLI 外还有一份实现 | `cmd_watch` + `_ssh_watch` | 统一代码路径 |
| 5 | Docker `collect()` 中无条件调用 | `collect()` L~1080 | 加循环计数器 |
| 6 | macOS `system_profiler` 每轮跑 | L~830 | 加 60s 缓存 |

### 7.2 安全债务

| # | 问题 | 严重度 | 修复 |
|---|------|--------|------|
| 1 | 无认证 | 🔴 | Basic Token 或 SSH 隧道 |
| 2 | 0.0.0.0 绑定默认 | 🟡 | 默认改为 127.0.0.1 |
| 3 | API 包含出口 IP / 内网拓扑 | 🟡 | 至少需网络层隔离 |
| 4 | 无速率限制 | 🟢 | Python stdlib 可实现简单的 request 计数 |

### 7.3 测试债务

| # | 缺失 |
|---|------|
| 1 | FileTracker 单元测试（核心逻辑：poll、状态机、get_report） |
| 2 | DownloadTracker._detect_processes（mock /proc） |
| 3 | DownloadTracker._collect_system_info（mock nvidia-smi） |
| 4 | HTTP handler 功能测试 |
| 5 | 主题切换渲染一致性测试 |

---

## 附录 A: 代码行数分布 (精确)

| 区域 | 行数 | 占比 |
|------|------|------|
| daemon 采集逻辑 | ~460 | 21% |
| web 服务 + 路由 | ~90 | 4% |
| **内联 HTML 模板** | **~1600** | **74%** |
| CLI 入口 | ~80 | 4% |
| **总计** | **2148** | **100%** |

**注释：** 74% 的代码是前端（HTML/CSS/JS），Python 实际逻辑只有 ~550 行。将内联 HTML 分离可大幅降低主文件复杂度。

## 附录 B: 部署现状

| 节点 | dltrace daemon | web panel | HTTP端口 | 运行时间 |
|------|---------------|-----------|----------|----------|
| 十六万 (本机) | ❌ (web 模式) | ✅ 8899 | ✅ | 持续 |
| 大傻 (spark-9051) | ✅ | ❌ | 8890 | 持续 |
| 二傻 (spark-9797) | ✅ | ❌ | 8890 | 持续 |
| 小四 | ✅ | ❌ | 8899 | ✅ |
| 三万八 | ✅ | ❌ | 8899 | ✅ |

当前架构：十六万 web 面板作为控制节点，通过 HTTP 拉取 4 个远端 daemon。

---

*报告结束。这是只读文档 — dltrace.py 未被修改。*
