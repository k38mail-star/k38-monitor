# K38 DLT（Deep Link Trace）暴力测试标准流程 v0.2

> **简称：** DLT / K38DLT  
> **来源：** 2026-06-10 04:05 KK总要求制定，v0.2 加了 Chaos/高压/数据盾  
> **针对版本：** DLT v0.4.x（单文件 ~2500 行，Python daemon + web 双进程，5 节点 HTTP pull）  
> **接入版本：** v0.2 新增视频/图像生成管线暴力测试（大傻·二傻·Echo·Wan2.1）  
> **灵感来源：** Google SRE 可靠性测试、Netflix Chaos Monkey、LitmusChaos、Chaos Mesh

---

## 〇、总纲

### 测试金字塔（底层最频繁，顶层最少跑）

```
        ╱  L8 数据盾  ╲       ← 大版本前（改代码逻辑时必须跑）
       ╱   L7 高压  HVP   ╲      ← 大版本前（压极限）
      ╱    L6 混沌 ChaoS    ╲     ← 大版本前（看能不能活）
     ╱     L5 异常场景       ╲    ← 大版本前（手工/自动）
    ╱      L4 交叉互检        ╲   ← 大版本前（全员并行）
   ╱       L3 集群功能         ╲  ← 每次发布
  ╱        L2 单点压力          ╲ ← 每次发布
 ╱         L1 单节点功能         ╲← 每次代码改动
╱          L0 冒烟起点            ╲← 改一行也要跑（5秒）
```

### 核心原则

1. **先基础后复杂** — 从单进程到全集群，从功能到压测到混沌  
2. **数据 + UI 交叉验证** — 一份数据要用 API + 面板两种方式验证  
3. **先隔离再交叉** — 每项单测通过后再做互测  
4. **混沌在前** — 注入故障后还能通过功能测试，才是真正抗打  
5. **"If you haven't tried it, assume it's broken"** — Google SRE  
6. **每次测试保留现场** — 失败时的 JSON/HTML/log 留档，便于追查  

---

## 一、L0 冒烟起点（任何测试前必须通过，5 秒）

```bash
# AST 解析
python3 -c "import ast; ast.parse(open('dltrace.py').read()); print('AST OK')"

# 本地双进程
nohup python3 -B dltrace.py daemon > /dev/null 2>&1 &
nohup python3 -B dltrace.py web --port 8899 > /dev/null 2>&1 &

# HTTP 200 + 数据非空
curl -s -o /dev/null -w '%{http_code}' http://localhost:8899/ && echo ""
curl -s http://localhost:8899/api/v1/metrics | python3 -c "
import json,sys
d=json.load(sys.stdin)
v=d.get('version','?')
h=list(d.get('hosts',{}).keys())
print(f'v={v}, hosts={h}')
print(f'fields={list(d[\"hosts\"][h[0]].keys())[:8]}')
"
```

**检查清单：**
```
[ ] AST 解析通过
[ ] daemon 进程存活（ps aux | grep dltrace | grep daemon）
[ ] web 进程存活
[ ] HTTP 200
[ ] JSON 非空（version + hosts key 存在）
[ ] 至少 1 个 host 有数据
```

---

## 二、L1 单节点功能测试（谁：各节点自己测自己，2 分钟/节点）

### L1.1 daemon 采集 — 逐字段验证（11 项）

逐项 curl API 并校验值是否合理：

| # | 采集项 | JSON 路径 | 预期值域 | 如果异常 |
|---|--------|-----------|---------|---------|
| 1 | CPU 使用率 | sys.cpu_pct | 0.0–100.0 | 检查 _collect_system_info |
| 2 | 内存使用率 | sys.mem_pct | 0.0–100.0 | 同上 |
| 3 | 磁盘使用率 | sys.disk_pct | 0.0–100.0 | 同上 |
| 4 | 磁盘 IO | sys.diskio | list，至少 1 项 | 检查 _collect_disk_io |
| 5 | 网络延迟 | sys.ping | dict，至少 1 目标 | 检查 _collect_ping |
| 6 | Docker 容器 | sys.docker | dict 含 containers / summary | 检查 _collect_docker |
| 7 | GPU 温度 | sys.gpu_temp | 无 GPU 时为 0；有则 0–100°C | 检查 _collect_system_info |
| 8 | GPU 功耗 | sys.gpu_power | 无 GPU 时为 0；有则 5–200W | 同上 |
| 9 | GPU 显存 | sys.gpu_mem_used | >= 0 | 同上 |
| 10 | GPU 时钟 | sys.gpu_clk_graphics | 数字或 "N/A"（DGX 特有） | 检查 daemon 采集 |
| 11 | 进程 Top 5 | sys.processes | list，每项含 cpu/mem/name | 检查 _collect_processes |

### L1.2 web 服务 — HTTP 层（6 项）

```
[ ] 首页 HTTP 200
[ ] HTML 字节 >= 20000
[ ] HTML 含当前版本号
[ ] JS 块语法正确（无 SyntaxError）
[ ] API v1 JSON schema 正确（含 version + hosts）
[ ] 非 API 路径 → 404
```

### L1.3 数据完整性（3 项）

```
[ ] daemon 写入的 /tmp/dltrace.json 与 web API 返回一致（diff 检查）
[ ] hosts 下所有数据字段无 None（None 计数 = 0）
[ ] hosts 下所有数字字段无 inf/nan（异常值计数 = 0）
```

### L1.4 环境自检（5 项，接入 L1 前置）

```
[ ] python3 >= 3.10
[ ] curl 可用
[ ] 8899 端口未被占用
[ ] /tmp/ 可读写
[ ] 磁盘空闲 > 1GB
```

---

## 三、L2 单点压力测试（谁：十六万，3 分钟）

### L2.1 高频轮询
```
[ ] curl 每秒 1 次 × 60 次 → 全部 200，最大响应 < 1000ms，最小 > 0ms
[ ] curl 每 100ms 一次 × 30 次 → 无崩溃、无空 body、无连接拒绝
```

### L2.2 并发请求
```bash
# 10 路并行 × 10 轮
for i in $(seq 1 10); do
  for j in $(seq 1 10); do
    curl -s http://localhost:8899/ > /dev/null &
  done
  wait
done
echo "10x10 并发完成，进程仍在：$(ps aux | grep 'dltrace.*web' | grep -v grep | wc -l)"
```
```
[ ] 全部 200（统计失败次数 = 0）
[ ] web 进程未挂
[ ] daemon 进程未挂
```

### L2.3 长时间运行 + 内存泄漏检测
```bash
# daemon + web 持续运行 5 分钟，每 30 秒记录 RSS
for i in $(seq 1 10); do
  sleep 30
  ps aux | grep 'dltrace.*daemon' | grep -v grep | awk '{print $6}'
done
```
```
[ ] 持续 5 分钟，进程未退出
[ ] daemon RSS 波动 < 10MB（无持续增长）
[ ] web RSS 波动 < 10MB
[ ] 检查 daemon 日志 → 无 Exception stack trace
```

---

## 四、L3 集群功能测试（谁：十六万，5 分钟）

### L3.1 HTTP pull 多节点采集
```bash
for host in localhost 192.168.3.29 192.168.3.46 10.0.0.126 192.168.3.45; do
  data=$(curl -s --max-time 5 "http://${host}:8899/api/v1/metrics")
  echo "$host: $(echo "$data" | python3 -c "
import json,sys
try:
  d=json.load(sys.stdin)
  h=list(d.get('hosts',{}).keys())
  v=d.get('version','?')
  print(f'OK v={v} hosts={h}')
except: print('FAIL')
")"
done
```
```
[ ] 5 节点全部响应
[ ] 每个节点 hostname 正确
[ ] 每个节点版本号一致
[ ] 每个节点至少 8 个字段非空（cpu/mem/disk/network/ping/docker/processes/alerts）
```

### L3.2 面板渲染完整性
```bash
html=$(curl -s http://localhost:8899/)
```
```
[ ] HTML 含 5 个 sys-card（`.sys-card` 类）
[ ] HTML 含延迟矩阵区域（ping/trace）
[ ] HTML 含 Docker 面板
[ ] HTML 含 DISK IO 面板
[ ] HTML 含告警区域（alert zone / alert-banner）
[ ] HTML 含侧边栏导航 + 顶栏角色链接
[ ] HTML 无 403/404/500 字样的错误文本
```

### L3.3 节点过滤 + 状态持久化
```
[ ] 点击顶栏单个节点 → 面板只显示该节点
[ ] 点击「全部」→ 恢复全节点
[ ] 刷新页面 → 过滤状态保持
[ ] URL hash 变化 → 页面状态同步
```

### L3.4 双主题切换
```
[ ] 默认深色主题 → CSS 变量应用正确
[ ] 点击切换 → 暖色主题生效
[ ] 刷新 → 主题保持
[ ] 两边主题按钮状态同步
```

---

## 五、L4 交叉互检（谁：全员并行，10 分钟）

### L4.1 节点间互检（6 项）

| 主动方 | 被动方 | 比对项 | 命令 |
|--------|--------|--------|------|
| 十六万 → 大傻 | GPU 数据 | API 温度 vs nvidia-smi 温度 | `nvidia-smi --query-gpu=temperature.gpu --format=csv,noheader` |
| 十六万 → 二傻 | Docker | API 容器列表 vs `docker ps --format '{{.Names}}'` | 比对容器名集合 |
| 十六万 → 三万八 | 进程 | API Top 5 进程名 vs `ps aux --sort=-%cpu | head -6` | 看 Top1 是否一致 |
| 十六万 → 小四 | 磁盘 | API disk_pct vs `df -h / | tail -1 | awk '{print $5}'` | 解析百分数比对 |
| 十六万 → 所有 | hostname | API 返回 hostname vs `hostname` 命令 | 精确匹配 |
| 十六万 → 所有 | 版本 | API version vs git tag | 精确匹配 |

### L4.2 API 数据 × 面板交叉验证（4 项）

```
[ ] curl API 某节点 cpu_pct = X.X → 面板 sys-card 显示相同的 X.X%
[ ] curl API 某节点 Docker 容器非空 → 面板显示 Docker 卡片
[ ] curl API 某节点 Docker 为空 → 面板 Docker 区域显示空白或无该卡
[ ] 某节点 web 下线 → 面板该节点灰色占位卡（不崩溃）
```

### L4.3 Agent 视觉审查（谁：大傻/二傻/OpenCode/小四）

```bash
# 截图命令
open http://localhost:8899/  # 手动截图或使用 screencapture
```
```
[ ] 大傻：GPU 数据格式正确，时钟/风扇/温度布局正常
[ ] 二傻：Docker 表 + 延迟矩阵缩摆放正
[ ] OpenCode：CSS 对齐、颜色、字体、响应式、边距
[ ] 小四：告警横幅位置、进程表列宽、数据溢出处理
```

---

## 六、L5 异常场景测试（谁：十六万，5 分钟）

### L5.1 进程崩溃恢复（4 项）

```
[ ] kill daemon → web 仍返回 200（读取历史数据正常）
[ ] 重启 daemon → 数据恢复采集（新旧数据对比）
[ ] kill web → daemon 进程不受影响，继续采集
[ ] 重启 web → 全功能恢复（无需重启 daemon）
```

### L5.2 远程节点离线（3 项）

```
[ ] 关闭一个远程节点的 web 服务 → 面板该节点显示离线/灰色占位
[ ] 恢复该节点 web → 数据自动回归面板（无需刷新）
[ ] 连续 3 个节点同时离线 → 面板不崩溃，剩余节点正常显示
```

### L5.3 空数据 / 损坏数据（3 项）

```
[ ] daemon 刚启动时 curl API → 返回默认空结构（不报 500）
[ ] 删除 /tmp/dltrace.json → daemon 自动重建空文件（不崩溃）
[ ] 写入乱码到 /tmp/dltrace.json → daemon 下次 collect 时重建（不崩溃）
```

### L5.4 大下载任务（1 项）
```
[ ] 制造 >100MB 下载任务 → DownloadTracker 正确追踪进度
[ ] 下载完成后 → 面板自动移出追踪列表
```

---

## 七、L6 混沌测试 · ChaoS（谁：十六万，10 分钟）

> **思想来源：** Netflix Chaos Monkey、LitmusChaos、Chaos Mesh  
> **核心理念：** 不要等故障发生，主动注入故障看系统能不能活

### L6.1 进程层混沌（4 项）

| 操作 | 预期行为 | 验证方法 |
|------|---------|---------|
| `kill -9` daemon（SIGKILL） | web 继续服务历史数据，不会整体崩溃 | curl × 3 全部 200 |
| `kill -9` web（SIGKILL） | daemon 继续采集，不会连带退出 | 检查 daemon pid + /tmp/dltrace.json 更新时间戳 |
| 同时 `kill -9` daemon + web | supervisor/Ralph 重启两者 | 等待 10s → 双进程恢复 |
| `pkill -f dltrace`（误操作模拟） | SSH session 不断，端口可重新绑定 | 模拟通配符杀进程后 fuser 释放端口 |

### L6.2 数据层混沌（4 项）

| 操作 | 预期行为 | 验证方法 |
|------|---------|---------|
| 覆盖 JSON 为 `{"broken": true}` | daemon 下次 collect 时重建合法 JSON | 等一轮采集，检查 /tmp/dltrace.json 格式正确 |
| 覆盖 JSON 为 {} 空对象 | daemon 不崩溃，用默认空结构 | 同上 |
| 覆盖 JSON 为 20MB 假数据 | 内存不爆，web 依然响应 200 | 耗时 < 5s，RSS 增幅 < 50MB |
| 删除整个 /tmp/ 目录（不可写模拟） | daemon 捕获异常，log 输出，不崩溃 | 检查 ps 进程仍存活 |

### L6.3 资源层混沌（3 项）

```bash
# CPU 高压 — 压到 90%+ 看面板响应
dd if=/dev/zero of=/dev/null &
PID=$!
sleep 5
curl -s http://localhost:8899/ | wc -c  # 面板还能渲染吗？
kill $PID
```
```
[ ] CPU 90%+ 时：web 仍返回 200，响应时间 < 5000ms
[ ] 磁盘写满模拟（dd 填到 95%）：daemon 写文件不崩溃，采集数据续
[ ] 内存 OOM 边缘：daemon RSS < 200MB，web RSS < 100MB
```

### L6.4 网络层混沌（3 项）

```bash
# 模拟断网
sudo ifconfig en0 down 2>/dev/null
sleep 3
# 本地 daemon 和 web 不受网络影响
curl -s http://localhost:8899/ | wc -c
sudo ifconfig en0 up
```
```
[ ] 断网 → daemon/web 不崩溃（本地环回互访）
[ ] 网络恢复 → 无需重启自动恢复
[ ] 模拟高延迟（tc 加 500ms 延迟）→ 面板渲染不阻塞
```

### L6.5 组合混沌（1 项，大版本前必跑）

> 同时注入 2-3 个故障：杀掉 web + 写坏 JSON + CPU 高压  
> 看系统在多重故障下的行为

```
[ ] 组合故障注入后：恢复进程顺序不影响系统可用性
[ ] 组合故障注入后：无死锁、无僵尸进程、无端口占用冲突
```

---

## 八、L7 高压测试 · HVP（谁：十六万，10 分钟）

> 不是模拟正常使用，是**找到系统崩溃的临界点**

### L7.1 高并发压测（3 项）

```bash
# 100 路并发 × 10 轮 = 1000 次请求
for round in $(seq 1 10); do
  for i in $(seq 1 100); do
    curl -s --max-time 3 http://localhost:8899/api/v1/metrics > /dev/null &
  done
  wait
  echo "Round $round done: alive=$(ps aux | grep 'dltrace.*web' | grep -v grep | wc -l)"
done
```
```
[ ] 100 并发 × 10 轮 = 0 次连接失败
[ ] web 进程不挂
[ ] 平均响应 < 3000ms
[ ] daemon 采集不受影响（/tmp/dltrace.json 仍在更新）
```

### L7.2 长时泄漏检测（2 项）

```bash
# 1 小时泄漏扫描（每 5 分钟记录一次 RSS）
for i in $(seq 1 12); do
  sleep 300
  date +"%H:%M:%S" >> /tmp/dltrace_mem.log
  ps aux | grep 'dltrace.*daemon' | grep -v grep | awk '{print $6}' >> /tmp/dltrace_mem.log
  ps aux | grep 'dltrace.*web' | grep -v grep | awk '{print $6}' >> /tmp/dltrace_mem.log
done
```
```
[ ] 1 小时运行后：daemon RSS 增量 < 20MB
[ ] 1 小时运行后：web RSS 增量 < 20MB
```

### L7.3 大数据量压测（3 项）

```bash
# 模拟 50 节点数据写入 JSON
python3 -c "
import json, random
data = {'version':'v0.4.2','hosts':{}}
for i in range(50):
  data['hosts'][f'node-{i:03d}'] = {
    'cpu_pct': random.uniform(0,100),
    'mem_pct': random.uniform(0,100),
    'disk_pct': random.uniform(0,100),
    'ping': {'baidu': random.randint(10,50)},
    'docker': {'containers': [{'name':f'container-{j}'} for j in range(random.randint(0,10))], 'summary': ''},
    'processes': [{'name':'test','cpu':random.uniform(0,100),'mem':random.uniform(0,100)} for _ in range(5)],
    'gpu_temp': random.uniform(30,80)
  }
json.dump(data, open('/tmp/dltrace_big.json','w'))
"
cp /tmp/dltrace_big.json /tmp/dltrace.json
```
```
[ ] 50 节点数据 → web 响应 < 5000ms，不 OOM
[ ] HTML 渲染页 ≥ 100KB
[ ] JS 动态刷新不卡顿
```

### L7.4 浏览器渲染压测（1 项）

```
[ ] 面板在 Chrome/Firefox/Safari 均无控制台错误
[ ] 面板在 1440×900、1920×1080、2560×1440 三种分辨率下无布局断裂
[ ] 连续自动刷新 60 次（2 秒间隔）→ 无布局错乱、无数据闪烁
```

---

## 九、L8 数据盾（谁：十六万 + 小四，5 分钟）

### L8.1 值域校验（所有节点）

| 字段 | 正常值域 | 连续 3 次异常视为故障 |
|------|---------|---------------------|
| cpu_pct | 0–100 | 0.0 持续 3 轮（采集可能挂了） |
| mem_pct | 0–100 | >98（内存告警）|
| disk_pct | 0–100 | >95（磁盘告警）|
| gpu_temp | 0–100 | >85（GPU 过热预警）|
| ping.baidu | 5–200ms（大陆） | >500ms（网络异常）或 =0（断网）|
| ping.ytb | — | 大陆节点 =0 属于正常（墙）|
| processes | >= 1 | =0（进程采集挂了）|
| version | 统一版本号 | 不一致（有的节点没更新）|

### L8.2 趋势异常检测

```
[ ] 任意节点 cpu_pct 连续 5 轮 = 0.0 → 标记为「采集异常」
[ ] 任意节点 gpu_temp 连续 3 轮 = 0 → 标记为「传感器脱落或 GPU 离线」
[ ] 任意节点 mem_pct 突然跳变 > 30%（如 30%→70% 在一轮内） → 标记为「异常跃变」
[ ] 跨 3 轮 ping 值标准差 > 100ms → 标记为「网络抖动」
```

### L8.3 跨节点一致性比对

```
[ ] 同一采集轮次内，5 节点 version 字段一致（否则有节点没部署）
[ ] 同一采集轮次内，5 节点 hostname 不重复（否则有配置冲突）
[ ] GPU 数据：大傻和二傻的 GPU 型号相同（都是 GH200），差异仅数值合理
```

### L8.4 数据格式安全校验

```python
# 自动扫描脚本片段
import json, math

def datashield(data: dict) -> list[str]:
    issues = []
    for host, hdata in data.get('hosts', {}).items():
        # 硬防 None
        for key, val in hdata.items():
            if val is None:
                issues.append(f"[None] {host}.{key} is None")
        # 硬防 inf/nan
        for key, val in hdata.items():
            if isinstance(val, float) and (math.isinf(val) or math.isnan(val)):
                issues.append(f"[INF/NaN] {host}.{key} = {val}")
        # 嵌套结构完整性
        for field in ['cpu_pct', 'mem_pct', 'disk_pct']:
            if field not in hdata:
                issues.append(f"[MISS] {host}.{field} missing")
    return issues
```

```
[ ] 数据盾扫全 5 节点 → 0 issues
[ ] None 字段数 = 0
[ ] inf/nan 字段数 = 0
[ ] 必填字段全存在
```

---

## 十、L9 代码审计（谁：十六万 + 大傻，5 分钟）

```
[ ] 运行 scripts/k38_code_audit.sh → 零错误
[ ] 无跨字段 KeyError 模式（`.get("A")` 后 `["B"]`）
[ ] 所有 try/except:pass 有注释说明必要性
[ ] `.get()` 结果没有裸用于比较运算（有 None 保护）
[ ] 版本号一致性：__version__ = git tag = 面板显示
[ ] HTML 标签平衡：无未闭合 div/span
[ ] JS 块无未闭合的模板字符串（${} 配对）
```

---

## 十一、自动化工具清单

### 已有脚本
| 脚本 | 用途 | 路径 |
|------|------|------|
| `k38_code_audit.sh` | 代码风格 + KeyError 扫描 | `scripts/k38_code_audit.sh` |

### 待建脚本（建议下次迭代实现）
| 脚本 | 用途 | 预估行数 |
|------|------|---------|
| `k38_stresstest.sh` | 全自动 L2 压力 + L7 高压 | ~80 行 bash |
| `k38_chaos_inject.py` | 混沌注入引擎（杀进程/写坏文件/CPU 压） | ~150 行 python |
| `k38_datashield.py` | 数据盾全节点扫描（L8） | ~100 行 python |
| `k38_crosscheck.py` | 跨节点一致性校验（L4） | ~80 行 python |
| `k38_fulltest.sh` | 一键跑全 9 层（L0→L9） | ~50 行 bash |

---

## 十二、职责矩阵（更新版）

| 角色 | 主测层 | 参与层 | 怎么测 |
|------|--------|--------|--------|
| **十六万**（CEO） | L2 压力、L3 集群、L5 异常、L6 混沌、L7 高压、L8 数据盾、L9 审计 | 所有 | SSH + 自动化脚本 + 手工分析 |
| **大傻**（首席算力） | L1 自身节点 | L4 互检（GPU）、L6 混沌配合、L9 Code Review | SSH 手动/自动 |
| **二傻**（管线主管） | L1 自身节点 | L4 互检（Docker/延迟）、L6 混沌配合 | SSH 手动/自动 |
| **三万八**（COO/产品） | — | L4 交叉（API×面板一致性）、L7.4 浏览器渲染验收 | 浏览器刷面板 |
| **小四**（CTO） | — | L1.3 数据完整、L8 数据盾机制、L9 代码审计扫描 | 脚本 + 代码扫描 |
| **OpenCode**（设计师） | — | L4.3 视觉审查、L3.4 双主题验收 | 浏览器 |

### 交叉矩阵（更新版）

| 测什么 | 十六万 | 大傻 | 二傻 | 三万八 | 小四 | OpenCode |
|--------|--------|------|------|--------|------|----------|
| L0 冒烟 | ✅ 总检 | ✅ 自测 | ✅ 自测 | ✅ 自测 | ✅ 自测 | — |
| L1 单节点 | ✅ 巡检 | ✅ 主测 | ✅ 主测 | ✅ 主测 | ✅ 主测 | — |
| L2 压力 | ✅ 主测 | — | — | — | — | — |
| L3 集群 | ✅ 主测 | — | — | — | — | — |
| L4 交叉 | ✅ 总调 | ✅ GPU/风扇 | ✅ Docker/网络 | ✅ 视觉验收 | ✅ 进程/告警 | ✅ UI/UX |
| L5 异常 | ✅ 主测 | 配合 | 配合 | — | — | — |
| L6 混沌 | ✅ 主测 | ✅ 配合 | ✅ 配合 | — | — | — |
| L7 高压 | ✅ 主测 | — | — | ✅ 浏览器渲染 | — | — |
| L8 数据盾 | ✅ 主测 | — | — | — | ✅ 机制维护 | — |
| L9 审计 | ✅ 主测 | ✅ 审阅 | — | — | ✅ 工具 | — |

---

## 十三、测试周期建议（更新版）

| 层级 | 触发时机 | 谁触发 | 预计耗时 | 自动化程度 |
|------|---------|--------|---------|-----------|
| L0 冒烟 | ✅ **每次代码改动后** | 十六万自检 | 5 秒 | 全自动 |
| L1 功能 | ✅ **每次代码改动后** | 各节点自检 | 2 分钟/节点 | 半自动 |
| L2 压力 | ✅ **每次版本发布前** | 十六万 | 3 分钟 | 可全自动 |
| L3 集群 | ✅ **每次版本发布时** | 十六万 | 5 分钟 | 可全自动 |
| L4 交叉 | 🔴 **大版本发布前** | 十六万 + 全员 | 10 分钟 | 半自动 |
| L5 异常 | 🔴 **大版本发布前** | 十六万 | 5 分钟 | 半自动 |
| L6 混沌 | 🔴 **大版本发布前** | 十六万 + 双傻 | 10 分钟 | 半自动 |
| L7 高压 | 🔴 **大版本发布前** | 十六万 | 10 分钟 | 半自动 |
| L8 数据盾 | 🔴 **大版本发布前 + 改逻辑时** | 十六万 + 小四 | 5 分钟 | 可全自动 |
| L9 审计 | ✅ **每次代码改动后** | 十六万自动 + 大傻审阅 | 5 分钟 | 半自动 |

> 🔴 = 阻塞性（未通过不得发版）  
> ✅ = 建议性（未通过可以继续开发但需标记）

---

## 十四、测试报告模板（v0.2）

```
╔══════════════════════════════════╗
║   K38 DLT 暴力测试报告 v0.2      ║
╚══════════════════════════════════╝

日期：_________
版本：_________
测试人：_________
被测节点：_________

[L0] 冒烟     □ 通过  □ 失败（___项未过）
[L1] 单节点   □ 通过  □ 失败（___/11 采集 + ___/6 web）
[L2] 压力     □ 通过  □ 失败（高频□ 并发□ 长时□）
[L3] 集群     □ 通过  □ 失败（pull□ 渲染□ 过滤□ 主题□）
[L4] 交叉     □ 通过  □ 失败（互检___/6 □ API×面板□ 视觉□）
[L5] 异常     □ 通过  □ 失败（崩溃□ 离线□ 空数据□ 下载□）
[L6] 混沌     □ 通过  □ 失败（进程□ 数据□ 资源□ 网络□ 组合□）
[L7] 高压     □ 通过  □ 失败（并发□ 泄漏□ 大数据□ 渲染□）
[L8] 数据盾   □ 通过  □ 失败（值域□ 趋势□ 跨节点□ 格式□）
[L9] 审计     □ 通过  □ 失败（___/6 项通过）

总体结论：🟢 全通 / 🟡 部分通过 / 🔴 挂
阻塞项（L4-L9 未通过）：无 / 见下

问题列表：
1. ___
2. ___

改进建议：
- ___
```

---

## 十五、v0.1 → v0.2 变更记录

| 变更 | 说明 |
|------|------|
| **新增 L6 混沌（ChaoS）** | 进程层、数据层、资源层、网络层、组合混沌，共 15 项 |
| **新增 L7 高压（HVP）** | 高并发、长时泄漏、大数据、浏览器渲染，共 9 项 |
| **新增 L8 数据盾** | 值域校验、趋势异常、跨节点一致性、格式安全，自动扫 |
| **原 L6（代码审计）→ L9** | 顺延 |
| **测试金字塔图** | 各层频率可视化 |
| **职责矩阵 + 交叉矩阵更新** | 加入 L6/L7/L8/L9 分配 |
| **触发周期表更新** | 标红阻塞性测试层级 |
| **工具清单** | 已有 + 待建脚本完整列表 |

---

---

## 十六、视频/图像生成管线 · GenAI 暴力测试（v0.2 新增）

> **背景：** K38 双 DGX Spark（大傻/二傻）承载视频生成管线（Wan2.1 T2V-14B + JoyAI-Echo），这是 K38 未来核心业务方向。
> **管线结构：**
> - 二傻（spark-9797）→ k38-worker 常驻（k38_worker_ralph.sh），Ralph Loop 智能重试
> - Docker 镜像：`k38/wan:latest`（25.4GB），含 Wan2.1 T2V-14B
> - 双机 200G 直连 NCCL 协同推理
> - JoyAI-Echo：1280×736，241 帧，25fps，多步去噪
> - 产出目录：`~/k38_output/`

---

### G0 视频生产冒烟（5 分钟，谁：十六万）

```bash
# 前提：所有节点 DLT 面板 L0-L3 全通过
# 检查容器是否存活
ssh jager-dgx-2@192.168.3.45 'docker ps | grep echo'
# 检查 worker 进程
ssh jager-dgx-2@192.168.3.45 'systemctl is-active k38-worker'
# 检查 200G 直连
ssh jager-dgx-2@192.168.3.45 'ping -c 2 -W 1 192.168.100.101'
# 检查 GPU 状态
ssh jager-dgx-2@192.168.3.45 'nvidia-smi --query-gpu=temperature.gpu,power.draw,memory.used --format=csv,noheader'
```

```
[ ] echo2 容器 Up
[ ] k38-worker active
[ ] 200G 直连通（延迟 < 0.5ms）
[ ] GPU 温度 < 60°C（空闲状态）
[ ] GPU 显存未被占用（可用 > 60GB）
```

---

### G1 单段视频生成（10 分钟，谁：大傻 + 二傻）

#### G1.1 Echo 单机推理
```bash
# 通过 k38-worker 提交一个简单任务
# 短 prompt，低帧数 41 帧，快速验证管线完整性
ssh jager-dgx-2@192.168.3.45 '
  cat > /tmp/k38_job.json << EOSIG
  {
    "id": "stress-test-g1-$(date +%s)",
    "prompt": "一只金色的猫在草地上打滚，阳光明媚",
    "master": "192.168.100.102",
    "port": 23456,
    "seed": 42,
    "frames": 41,
    "steps": 4,
    "guide": 5.0,
    "size": "640*640"
  }
EOSIG
  mv /tmp/k38_job.json /tmp/k38_job.json
'
# 观察 worker 日志
tail -f
```
```
[ ] k38-worker 正确接收信号文件
[ ] Docker 容器正常启动（docker ps 可见 k38_job_*）
[ ] 推理过程无报错退出
[ ] 产出视频文件生成至 ~/k38_output/（非空.mp4）
[ ] 视频分辨率正确
[ ] 视频时长 > 1 秒
[ ] 文件可被 ffprobe 识别（无损坏）
[ ] 推理完成后容器自动退出（无僵尸容器）
```

#### G1.2 Echo 双机协同推理
```bash
# master=大傻(192.168.100.101)，二傻登录大傻
# 从大傻触发双机推理命令
ssh jager-dgx@192.168.3.55 '
  docker run --gpus all --rm --ipc=host --network host \
    --name k38_job_coop \
    -v ~/.cache/modelscope:/root/.cache/modelscope \
    -v ~/wan21:/workspace \
    -v ~/k38_output:/output \
    -e NCCL_SOCKET_IFNAME=enP2p1s0f1np1 \
    -e GLOO_SOCKET_IFNAME=enP2p1s0f1np1 \
    -e MASTER_ADDR=192.168.100.101 \
    -e MASTER_PORT=23456 \
    -e PROMPT="一只金色的猫在草地上打滚，阳光明媚" \
    -e SIZE="1280*720" \
    -e FRAMES=81 \
    -e STEPS=4 \
    -e SEED=42 \
    -e GUIDE=5.0 \
    k38/wan:latest bash /host/k38_run.sh
'
```
```
[ ] NCCL 初始化成功（log 含 NCCL INFO）
[ ] 双机 GPU 都有负载（nvidia-smi 都显示功率 > 30W）
[ ] 推理过程中 200G 链路有流量（ifconfig 检查）
[ ] 推理完成无中断
[ ] 产出视频无撕裂/花屏（人工肉眼）
[ ] 双机对比：Echo 单机 vs 双机产出视频清晰度可比
```

#### G1.3 标准参数产出一段全量
```bash
# 用 JoyAI-Echo 的标准配置跑一段全量
ssh jager-dgx-2@192.168.3.45 '
  cat > /tmp/k38_job.json << EOSIG
  {
    "id": "stress-test-full-$(date +%s)",
    "prompt": "电影镜头，一个赛博朋克城市夜景，霓虹灯闪烁，雨中街道反射灯光，4K画质",
    "master": "192.168.100.102",
    "port": 23456,
    "seed": 12345,
    "frames": 81,
    "steps": 4,
    "guide": 5.0,
    "size": "1280*720"
  }
EOSIG
  mv /tmp/k38_job.json /tmp/k38_job.json
'
```
```
[ ] 81 帧 1280×720 推理成功
[ ] 耗时在合理范围（< 15 分钟）
[ ] GPU 温度全程 < 85°C
[ ] 视频无闪烁 / 无鬼影 / 场景连贯
[ ] 文件大小合理（81 帧 1280×720 应 > 3MB）
[ ] 可用 ffmpeg 正常播放
```

---

### G2 视频生成压力测试（15 分钟，谁：十六万）

#### G2.1 连续推理不重启
```bash
# 连续提交 3 个任务，每个完成后自动接下一个
for i in 1 2 3; do
  ssh jager-dgx-2@192.168.3.45 "
    python3 -c \"import json; json.dump({
      'id': 'batch-\${i}-\$(date +%s)',
      'prompt': '测试视频 \${i}，不同场景',
      'master': '192.168.100.102',
      'port': 23456,
      'seed': \$((RANDOM)),
      'frames': 41,
      'steps': 4,
      'guide': 5.0,
      'size': '640*640'
    }, open('/tmp/k38_job.json','w'))"
    mv /tmp/k38_job.json /tmp/k38_job.json
  "
  sleep 120  # 等前一个完成
  # 查是否产出新视频
  ssh jager-dgx-2@192.168.3.45 "ls -lt ~/k38_output/ | head -3"
done
```
```
[ ] 每个任务独立完成（无交叉污染）
[ ] 任务间不需要手动清理 GPU 显存
[ ] 连续 3 次推理后 GPU 温度稳定（不持续升高）
[ ] 每个任务产出独立的 .mp4 文件
[ ] 无容器残留（docker ps -a 检查）
```

#### G2.2 长视频压测
```bash
# 81 帧 → 161 帧 → 241 帧 → 321 帧
# 每个跑完检查产出和温度
for frames in 81 161 241; do
  echo "=== Testing ${frames} frames ==="
  # 提交任务…
  # 监控时间 + GPU 温度
  # 等待完成
  echo "${frames}帧: done"
done
```
```
[ ] 81 帧 < 10 分钟
[ ] 161 帧 < 20 分钟
[ ] 241 帧 < 30 分钟
[ ] 241 帧时 GPU 温度峰值 < 85°C
[ ] 241 帧视频时长 > 8 秒（25fps 下 241/25 ≈ 9.6s）
[ ] 帧数增加时显存占用线性增长（不溢出）
```

#### G2.3 高分辨率压测
```bash
# 640×640 → 832×480 → 1280×720 → 1280×768
for size in "640*640" "832*480" "1280*720" "1280*768"; do
  echo "=== Testing ${size} ==="
  # 41 帧固定
  # 监控时间 + 显存
  # 检查产出
  echo "${size}: done"
done
```
```
[ ] 所有分辨率均推理成功
[ ] 1280×720 推理 < 8 分钟（41 帧）
[ ] 无 OOM（out of memory）
[ ] 产出视频无绿屏/花屏
[ ] 从低到高分辨率质量有肉眼可见提升
```

---

### G3 视频生成混沌测试（15 分钟，谁：十六万 + 双傻）

#### G3.1 推理中断恢复
```bash
# 提交一个长任务（241 帧），中途杀掉容器，看 worker 会不会重试
# Ctrl+C 或 docker stop
ssh jager-dgx-2@192.168.3.45 '
  docker stop k38_job_xxx
  sleep 5
  # 看 worker 有没有重新拉起
  docker ps | grep k38_job_
  tail -5 ~/k38_worker.log
'
```
```
[ ] Ralph Loop 检测到失败并重试
[ ] 重试后新容器正常启动
[ ] 重试次数可观测（log 中有记录）
[ ] 重试 3 次内成功
```

#### G3.2 200G 链路断线恢复
```bash
# 推理中模拟断网
ssh jager-dgx-2@192.168.3.45 '
  sudo ip link set enP2p1s0f1np1 down
  sleep 10
  sudo ip link set enP2p1s0f1np1 up
  sleep 3
  ping -c 2 192.168.100.101
'
```
```
[ ] 200G 断开 → Docker 推理报错但 worker 不挂
[ ] 200G 恢复 → 直连 ping 通
[ ] 下次推理使用恢复后的 200G 链路
[ ] 现场可修复（无需重启 worker 服务）
```

#### G3.3 GPU OOM 模拟
```bash
# 故意提交一个超规格参数（大分辨率 + 多帧数）
# 验证 worker 能否优雅处理 OOM 而不是把 GPU 卡死
ssh jager-dgx-2@192.168.3.45 '
  # 1920×1080 + 481 帧，超出 GH200 128GB 能力
  ...
'
```
```
[ ] OOM 后恢复（无残留进程占显存）
[ ] worker 不崩溃，Ralph 循环检测到失败
[ ] 后续正常任务可继续执行
[ ] GPU 显存在 OOM 后完全释放（nvidia-smi 检查）
```

#### G3.4 空 prompt / 恶意输入
```bash
# 空字符串、超长 prompt（5000 字）、纯符号 prompt
for prompt in "" "。。。。。" "a%20-%20b" "$(python3 -c 'print("x"*5000)')"; do
  # 提交
  # 检查产出
  # 清理
done
```
```
[ ] 空 prompt → 模型不崩溃，产出默认/随机内容
[ ] 超长 prompt → 不 OOM，输出不劣化
[ ] 纯符号 → 不崩溃
```

---

### G4 图像生成测试（5 分钟，谁：十六万 + 大傻）

> 如果后续部署了 FLUX / SDXL 或其他图像生成管线

#### G4.1 单图生成
```
[ ] 模型加载成功（首次 < 60s）
[ ] 512×512 生成 < 10s
[ ] 768×768 生成 < 15s
[ ] 图像无失真/无伪影
[ ] 文件格式正确（PNG/WebP）
```

#### G4.2 批量生成
```
[ ] 连续 5 张不同 prompt → 全部成功
[ ] 连续 5 张的种子隔离正确（相同 prompt 不同 seed → 不同图）
[ ] 批量后无显存泄漏（与批次前相比 RSS 增幅 < 200MB）
```

#### G4.3 图片 → 视频（Img2Vid）
```
[ ] 首帧 + prompt → 视频生成
[ ] 输出视频分辨率与输入图片一致
[ ] 场景从首帧合理延续
```

---

### G5 视频质量测试（人工 + 自动化，10 分钟，谁：三万八 + 十六万）

#### G5.1 人工主观评估
```
[ ] 画面无闪烁（flicker）
[ ] 主体一致性（猫不会变成狗）
[ ] 背景合理性（无物体突然消失/出现）
[ ] 运动流畅度（不跳帧）
[ ] 文字 prompt 遵循度（Prompt adherence）
```

#### G5.2 自动化客观指标
```bash
# 用 ffmpeg 分析视频基础质量
ffmpeg -i output.mp4 -vf "select=eq(pict_type\,I),showinfo" -f null - 2>&1 | grep -c "pts_time"
# 检查关键帧数量（太少表示画面变化不够）
ffprobe -v error -select_streams v:0 -show_entries stream=bit_rate,r_frame_rate,duration -of default=noprint_wrappers=1 output.mp4
```
```
[ ] 视频 bitrate > 1 Mbps（合理画质）
[ ] framerate ≈ 设定值（如 25fps）
[ ] 时长 ≈ 帧数/fps（无丢帧）
[ ] 关键帧间隔合理
[ ] 文件不损坏（ffprobe 无 error）
```

---

### G6 管线可靠性测试（20 分钟，谁：十六万 + 二傻）

#### G6.1 长时生产
```
[ ] 连续 1 小时不停机生产（自动循环提交 41 帧任务）
[ ] 总产出 ≥ 5 个完整视频
[ ] GPU 温度稳定在 75-82°C 区间（不过热降频）
[ ] 最后一个视频质量与第一个无退化
[ ] 无显存泄漏（同一任务前后 nvidia-smi 显存占用偏差 < 500MB）
```

#### G6.2 跨版本兼容
```
[ ] 更新 Echo 配置后，旧配置生成的生产任务不报错
[ ] 模型文件不变时，相同 seed + prompt 产出可复现
[ ] 不同 seed 产出视觉上不同（种子隔离有效）
```

---

### GenAI 测试职责矩阵

| 角色 | 负责 | 工具 |
|------|------|------|
| **十六万**（CEO） | G0 冒烟、G2 压力、G3 混沌、G6 长时生产 | SSH + 脚本 |
| **大傻**（首席算力） | G1.2 双机协同、G4 图像、nvidia-smi 监控 | SSH 到大傻 |
| **二傻**（管线主管） | G1.1/G1.3 单机推理、G3.1/G3.2 恢复、G6 主测 | SSH 到二傻 |
| **三万八**（COO/产品） | G5 人工主观评估（看视频质量） | 浏览器 + ffprobe |
| **小四**（CTO） | G5.2 自动化指标、测试脚本维护 | ffmpeg 分析脚本 |

### GenAI 测试触发周期

| 层级 | 触发时机 | 耗时 |
|------|---------|------|
| G0 冒烟 | ✅ 每次 DLT 测试前 | 1 分钟 |
| G1 单段 | ✅ 每次模型/配置更新后 | 10 分钟 |
| G2 压力 | 🔴 每次大版本发布前 | 15 分钟 |
| G3 混沌 | 🔴 每次大版本发布前 | 15 分钟 |
| G4 图像 | ✅ 图像管线就绪后 | 5 分钟 |
| G5 质量 | 🔴 每次发布前 | 10 分钟 |
| G6 可靠 | 🔴 每周或发版前 | 20 分钟 |

---

*本文件为 v0.2 初稿（含 GenAI 视频/图像测试 G0-G6），待 KK总确认后固化到 MEMORY.md 标准工作流。*
