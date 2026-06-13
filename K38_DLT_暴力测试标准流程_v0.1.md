# K38 DLT（Deep Link Trace）暴力测试标准流程 v0.1

> **来源：** 2026-06-10 04:05 KK总要求制定
> **简称：** DLT
> **全称：** K38 Deep Link Trace / K38DLT
> **状态：** 初稿待确认
> **针对版本：** DLT v0.4.x（单文件 ~2500 行，Python daemon + web 双进程，5 节点 HTTP pull）

---

## 一、核心原则

1. **先基础后复杂** — 从单进程到全集群，从功能到压测
2. **数据 + UI 交叉验证** — 一份数据要用 API + 面板两种方式验证
3. **先隔离再交叉** — 每项单测通过后再做互测
4. **必须跑冒烟起点** — 测试前确认 ast.parse 通过 → HTTP 200 → 数据非空

---

## 二、冒烟起点（任何测试前必须通过）

```
[ ] AST 解析通过   → python3 -c "import ast; ast.parse(open('dltrace.py').read()); print('AST OK')"
[ ] 本地 daemon 启动 → nohup python3 -B dltrace.py daemon > /dev/null 2>&1 &
[ ] 本地 web 启动    → nohup python3 -B dltrace.py web --port 8899 > /dev/null 2>&1 &
[ ] HTTP 200         → curl -s -o /dev/null -w '%{http_code}' http://localhost:8899/
[ ] 数据非空         → curl -s http://localhost:8899/api/v1/metrics | python3 -c "import json,sys;d=json.load(sys.stdin);print(f'v={d[\"version\"]}, hosts={len(d.get(\"hosts\",{}))}');print(f'keys={list(d.get(\"hosts\",{}).get(\"localhost\",{}).keys())[:5]}')"
```

---

## 三、测试分层

### 第 0 层：环境自检（谁：十六万，10 秒）
所有节点上线前自检：
```
[ ] python3 版本 >= 3.10
[ ] curl 可用
[ ] 8899 端口未被占用
[ ] /tmp/dltrace.json 可读写
[ ] 磁盘空闲 > 1GB
[ ] 网络连通（ping 其他 4 节点 < 10ms）
```

### 第 1 层：单节点功能测试（谁：各节点自己测自己，每节点 2 分钟）

#### 1.1 daemon 采集（谁：各节点）

| 采集项 | 验证方法 | 预期 |
|--------|---------|------|
| CPU 使用率 | curl /api/v1/metrics → sys.cpu_pct | 0-100%，数值 |
| 内存使用率 | curl /api/v1/metrics → sys.mem_pct | 0-100%，数值 |
| 磁盘使用率 | curl /api/v1/metrics → sys.disk_pct | 0-100%，数值 |
| 磁盘 IO | curl /api/v1/metrics → sys.diskio | 至少 1 个设备 |
| 网络延迟 | curl /api/v1/metrics → sys.ping | 至少 1 个目标 |
| Docker 容器 | curl /api/v1/metrics → sys.docker | 有/无均可，格式正确 |
| GPU 信息 | curl /api/v1/metrics → sys.gpu_temp | 若有 GPU，0-100°C |
| GPU 功耗 | curl /api/v1/metrics → sys.gpu_power | 若有 GPU，5-200W |
| GPU 显存 | curl /api/v1/metrics → sys.gpu_mem_used | 若有 GPU，>= 0 |
| GPU 时钟 | curl /api/v1/metrics → sys.gpu_clk_graphics | 若有 GPU，数字或 N/A |
| 进程列表 | curl /api/v1/metrics → sys.processes | Top 5，每个有 cpu/mem |
| 告警状态 | curl /api/v1/metrics → alerts | 数组，可为空 |

#### 1.2 web 服务（谁：各节点）

| 测试项 | 验证方法 | 预期 |
|--------|---------|------|
| 首页 | `curl -s -o /dev/null -w '%{http_code}' http://localhost:8899/` | 200 |
| HTML 非空 | `curl -s http://localhost:8899/ | wc -c` | >= 20000 字节 |
| 含版本号 | `curl -s http://localhost:8899/ | grep -o 'v[0-9]\+\.[0-9]\+\.[0-9]\+'` | 匹配版本号 |
| JS 无语法错误 | `curl -s http://localhost:8899/ | python3 -c "import re,sys;html=sys.stdin.read();js=re.search(r'<script>(.*?)</script>',html,re.DOTALL);print('JS_OK' if js else 'NO_SCRIPT')"` | JS_OK |
| API v1 格式正确 | `curl -s http://localhost:8899/api/v1/metrics | python3 -c "import json,sys;d=json.load(sys.stdin);print('version' in d and 'hosts' in d)"` | True |
| 非 api 路径 404 | `curl -s -o /dev/null -w '%{http_code}' http://localhost:8899/wrong` | 404 |

#### 1.3 数据完整性（谁：各节点）

```
[ ] 对比 daemon JSON 和 web API → 数据一致
[ ] 无 None 值（检查 hosts 下所有字段，None 字段数 = 0）
[ ] 无浮点异常（检查 inf/nan 字段数 = 0）
```

### 第 2 层：单节点压力测试（谁：十六万，3 分钟）

#### 2.1 高频轮询
```
[ ] curl 每秒 1 次 × 60 次 → 全程 200，最低响应 0ms，最高 < 500ms
[ ] curl 每 100ms 一次 × 30 次 → 无崩溃，无空 body
```

#### 2.2 并发请求
```
[ ] 10 个并行 curl × 10 次 → 全部 200，服务器不挂
[ ] 同时访问 / 和 /api/v1/metrics → 两路都 200
```

#### 2.3 长时间运行
```
[ ] daemon + web 持续运行 5 分钟 → 无进程退出
[ ] 检查 daemon 日志 → 无 Exception stack trace
[ ] 内存稳定（daemon RSS 不持续增长 > 10MB）
```

### 第 3 层：集群功能测试（谁：十六万，5 分钟）

#### 3.1 HTTP pull 多节点采集
```
[ ] 5 节点全部在 /api/v1/metrics → hosts 含 5 个 key
[ ] 每个节点的 hostname 正确
[ ] 每个节点版本号一致
[ ] 每个节点数据非空（cpu/mem/disk/network 都在）
```

#### 3.2 面板渲染完整性
```
[ ] HTML 含每个节点的系统卡（5 个 sys-card）
[ ] HTML 含网络延迟矩阵
[ ] HTML 含 Docker 面板（各节点）
[ ] HTML 含 DISK IO 面板
[ ] HTML 含告警区域
[ ] HTML 含侧边栏 + 顶栏角色链接
```

#### 3.3 节点过滤
```
[ ] 顶栏点击单个节点 → 面板只显示该节点数据
[ ] 顶栏点击「全部」→ 恢复全节点
[ ] 刷新页面 → 过滤状态保持（hash 持久化）
```

#### 3.4 双主题
```
[ ] 默认深色主题 → CSS 变量正确
[ ] 点击切换 → 切换到暖色主题
[ ] 刷新 → 主题保持（localStorage）
[ ] 两边主题按钮状态同步
```

### 第 4 层：交叉测试（谁：全员交叉，10 分钟）

#### 4.1 节点间互检

| 主动方 | 被测方 | 测什么 |
|--------|--------|--------|
| 十六万 | 大傻 | GPU 数据（温度/功耗/时钟）是否与 nvidia-smi 一致 |
| 十六万 | 二傻 | Docker 容器列表是否与 docker ps 一致 |
| 十六万 | 三万八 | 进程列表是否与 ps aux 的 Top 5 一致 |
| 十六万 | 小四 | 磁盘使用率是否与 df -h 一致 |
| 三万八 | 十六万 | 面板 HTML 是否含所有 5 节点 |
| 小四 | 大傻 | API 返回的 hostname 与 SSH hostname 一致 |

#### 4.2 API 数据 × 面板交叉验证
```
[ ] curl API → 某节点 cpu_pct = X.X → 面板对应 sys-card 显示相同数值
[ ] curl API → 某节点有 Docker → 面板显示 Docker 卡片
[ ] curl API → 某节点无 Docker → 面板 Docker 卡片空白或不渲染
[ ] 节点下线 → 面板显示灰色占位卡
```

#### 4.3 多 Agent 视觉审查（谁：大傻/二傻/OpenCode/小四）
```
[ ] 大傻：检查 GPU/时钟/风扇数据格式
[ ] 二傻：检查 Docker/延迟矩阵格式
[ ] OpenCode：CSS/UI/对齐/颜色/字体
[ ] 小四：检查进程/告警规则逻辑
```

### 第 5 层：异常场景测试（谁：十六万，5 分钟）

#### 5.1 daemon 崩溃恢复
```
[ ] kill daemon → web 仍返回 200（历史数据可读）
[ ] 重启 daemon → 数据恢复采集
[ ] kill web → daemon 继续采集（不影响）
[ ] 重启 web → 全功能恢复
```

#### 5.2 远程节点离线
```
[ ] 关闭一个远程节点的 web → 面板不崩溃，显示离线占位
[ ] 恢复该节点 web → 数据自动回归
[ ] 连续 3 个节点离线 → 面板不崩溃
```

#### 5.3 空数据场景
```
[ ] daemon 启动后立即 curl API → 应有默认空结构
[ ] /tmp/dltrace.json 被删 → daemon 重建新文件
[ ] /tmp/dltrace.json 损坏（写入乱码）→ daemon 不崩溃，重建
```

#### 5.4 大文件场景
```
[ ] 制造一个 1GB 下载任务 → 追踪面板正确显示
[ ] 下载完成后 → 面板自动移除
```

### 第 6 层：白盒代码审计（谁：十六万 + 大傻，5 分钟）

```
[ ] 运行 scripts/k38_code_audit.sh → 零错误
[ ] 跨字段 KeyError 扫描 → 无 `.get("A")` 后 `["B"]` 模式
[ ] try/except:pass 数量 → 全部有注释说明必要性
[ ] `.get()` 结果直接用做比较 → 有 None 保护
[ ] 版本号一致性 → 代码 `__version__` = git tag = 面板显示
[ ] HTML 标签平衡 → 无未闭合的 div/span
```

---

## 四、工具清单

### 测试脚本（可自动化）
| 脚本 | 用途 | 位置 |
|------|------|------|
| `k38_code_audit.sh` | 代码风格扫描 | `scripts/k38_code_audit.sh` |
| 待建：`k38_stresstest.sh` | 高频轮询 + 并发请求 | 自动持续 3 分钟 |
| 待建：`k38_datacheck.py` | 全节点数据完整性校验 | 输出异常字段报告 |
| 待建：`k38_crosscheck.py` | 节点间交叉比对 | 报告不一致项 |

### 快捷验证命令
```bash
# 冒烟
curl -s -o /dev/null -w '%{http_code} ' http://localhost:8899/
curl -s http://localhost:8899/ | wc -c
curl -s http://localhost:8899/api/v1/metrics | python3 -m json.tool | head -20

# 全节点无异常
for host in localhost 192.168.3.29 192.168.3.46 192.168.3.55 192.168.3.45; do
  curl -s "http://${host}:8899/api/v1/metrics" | python3 -c "
import json,sys
try:
  d=json.load(sys.stdin)
  v=d.get('version','?')
  h=list(d.get('hosts',{}).keys())
  print(f'OK v={v} hosts={h}')
except Exception as e:
  print(f'FAIL {e}')
" 2>&1
done

# 高频测试（10次并发）
for i in $(seq 1 10); do curl -s http://localhost:8899/ > /dev/null & done; wait
```

---

## 五、测试报告模板

### 暴力测试报告 vX.Y

```
日期：_________
版本：_________
测试人：_________

[冒烟]  AST  □  daemon □  web □  HTTP □  数据非空 □
[L1]   daemon采集 __/11 项通过 □  web服务 __/6 项通过 □  数据完整 __/3 □
[L2]   高频轮询 □  并发 □  长时间 □  
[L3]   HTTP pull □  面板渲染 □  节点过滤 □  双主题 □
[L4]   交叉互检 __/6 项通过 □  API×面板一致 □  视觉审查 □
[L5]   崩溃恢复 □  离线容错 □  空数据 □  大文件 □
[L6]   代码审计 __/6 项通过 □

总体结论：🟢 全通 / 🟡 部分通过 / 🔴 挂
问题列表：
1. ...
2. ...
```

---

## 六、职责矩阵

| 角色 | 负责测什么 | 怎么测 |
|------|-----------|--------|
| **十六万**（CEO） | L0 环境自检、L2 压力、L3 集群、L5 异常、L6 代码审计 | SSH 执行测试脚本 + curl |
| **大傻**（首席算力） | L1 自身节点 + L4 互检（GPU/时钟/风扇） + 视觉审查 | SSH 到各节点手动/自动测 |
| **二傻**（管线主管） | L1 自身节点 + L4 互检（Docker/延迟） + 视觉审查 | SSH 到各节点手动/自动测 |
| **三万八**（COO/产品） | L4 交叉验证（API数据×面板一致性） + 视觉验收 | 浏览器刷面板对比 |
| **小四**（CTO） | L4 交叉验证（进程/告警） + L6 代码审计 + 测试流程文档 | 代码扫描 + SSH |

### 交叉矩阵简表

| 测什么 | 十六万 | 大傻 | 二傻 | 三万八 | 小四 |
|--------|--------|------|------|--------|------|
| L0 环境自检 | ✅ 主测 | ✅ 自测 | ✅ 自测 | ✅ 自测 | ✅ 自测 |
| L1 单节点功能 | ✅ 巡检 | ✅ 主测 | ✅ 主测 | ✅ 主测 | ✅ 主测 |
| L2 压力测试 | ✅ 主测 | — | — | — | — |
| L3 集群功能 | ✅ 主测 | — | — | — | — |
| L4 交叉互检 | ✅ 总调 | ✅ GPU/风扇 | ✅ Docker/网络 | ✅ 视觉 | ✅ 进程/告警/审计 |
| L5 异常场景 | ✅ 主测 | 配合 | 配合 | 配合 | 配合 |
| L6 代码审计 | ✅ 主测 | ✅ Code Review | — | — | ✅ 扫描 |

---

## 七、测试周期建议

| 测试级别 | 触发时机 | 谁触发 | 耗时 |
|---------|---------|--------|------|
| L0 冒烟 | **每次代码改动后** | 十六万自动 | 5 秒 |
| L1 功能 | **每次版本发布前** | 十六万 + 各节点 | 2 分钟/节点 |
| L2 压力 | **每次大版本发布前** | 十六万 | 3 分钟 |
| L3 集群 | **每次版本发布后** | 十六万 | 5 分钟 |
| L4 交叉 | **大版本发布前** | 全员并行 | 10 分钟 |
| L5 异常 | **大版本发布前** | 十六万 | 5 分钟 |
| L6 审计 | **每次代码改动后** | 十六万自动 + 大傻 | 5 分钟 |

---

*本文件为初稿（v0.1），待 KK总确认后固化到 MEMORY.md 和标准工作流中。*
