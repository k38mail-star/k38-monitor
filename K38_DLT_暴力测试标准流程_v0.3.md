# K38 DLT（Deep Link Trace）暴力测试标准流程 v0.3

> **简称：** DLT / K38DLT  
> **针对版本：** DLT v0.5.0 | 新架构：Python daemon 采集 + Node.js Express 后端 + WS推流 + Redis/PG历史  
> **架构增加：** WebSocket实时推流、PostgreSQL历史序列、REST /api/v1/history  
> **架构移除：** Python 单线程 web、sys-card/DISK IO/告警面板（旧CMD区域）  
> **灵感来源：** Google SRE、Netflix Chaos Monkey、LitmusChaos、Chaos Mesh  

---

## 〇、总纲

### 新架构数据流

```
本机 dltrace.py daemon
  ↓ SCP 每15秒 /tmp/dltrace.json
ECS /opt/dltrace/dltrace.json
  ↓ dlt_cache.sh（Docker python）
Redis(dlt:latest, TTL 300s) + PostgreSQL(dlt_snapshots)
  ↓ Node.js ingest API → WebSocket广播
nginx(SSL) → 用户浏览器 https://dlt.k38.ai/
```

### 测试金字塔

```
        ╱  L10 LIVE        ╲      ← 大版本（手动+自动，生产域）
       ╱   L8 数据盾        ╲     ← 改逻辑时跑
      ╱    L7 高压 HVP      ╲    ← 大版本前
     ╱     L6 中断恢复       ╲   ← 大版本前
    ╱      L4 交叉互检       ╲  ← 大版本前
   ╱       L3 ECS全栈        ╲ ← 每次部署
  ╱        L2 并发压力        ╲← 每次部署
 ╱         L1 单节点验证       ╲← 每次代码改动
╱          L0 冒烟起点          ╲← 改一行也要跑
```

---

## 一、L0 冒烟起点（任何测试前必须通过，10 秒）

```bash
# 1. Python daemon AST 解析
python3 -c "import ast; ast.parse(open('dltrace.py').read()); print('AST OK')"

# 2. Node.js server.js 语法检查
node -c server.js && echo "Node.js syntax OK"

# 3. 本机 daemon 进程存活
ps aux | grep 'dltrace.*daemon' | grep -v grep > /dev/null && echo "daemon OK" || echo "daemon MISSING"

# 4. ECS Node.js 在线
curl -s -o /dev/null -w "%{http_code}" https://dlt.k38.ai/ && echo " ECS Node.js OK"

# 5. ECS /api/v1/latest 返回非空 JSON
curl -s https://dlt.k38.ai/api/v1/latest | python3 -c "
import sys,json; d=json.load(sys.stdin)
assert d.get('nodes'), 'nodes missing'
print(f'L0 OK: {len(d[\"nodes\"])} nodes, ts={d.get(\"ts_str\",\"?\")}')
"

# 6. WebSocket 可连接
python3 -c "
import asyncio,json
async def test():
    import websockets
    async with websockets.connect('wss://dlt.k38.ai/ws') as ws:
        m = json.loads(await asyncio.wait_for(ws.recv(), 3))
        assert m['type']=='data', 'WS msg type wrong'
        print(f'WS OK: {len(m[\"payload\"][\"nodes\"])} nodes')
asyncio.run(test())
"
```

**检查清单：**
```
[ ] AST 解析通过
[ ] Node.js 语法通过
[ ] daemon 进程存活
[ ] ECS Node.js HTTP 200
[ ] API latest 返回 nodes 非空
[ ] WebSocket 推送正常
```

---

## 二、L1 单节点验证（谁：各节点自测，2 分钟/节点）

### L1.1 Daemon 采集 — JSON 全字段校验

通过 `/api/v1/latest` 返回的数据校验：

| # | 字段 | 路径 | 预期 | 原因 |
|---|------|------|------|------|
| 1 | version | root | str 非空 | 版本号 |
| 2 | ts_str | root | str 非空 | 时间戳 |
| 3 | hostname | root | str 非空 | 采集节点 |
| 4 | system | root | dict 非空 | 系统级指标 |
| 5 | nodes | root | dict 非空 | 所有节点数据 |
| 6 | cpu_pct | nodes.*.system | 0-100 | CPU 使用率 |
| 7 | mem_pct | nodes.*.system | 0-100 | 内存使用率 |
| 8 | disk_pct | nodes.*.system | 0-100 | 磁盘使用率 |
| 9 | gpu_info | nodes.* | dict/None | GPU 信息 |
| 10 | ping | root | dict 非空 | 延迟数据 |
| 11 | network | root | dict | 网络信息 |
| 12 | docker | root | dict | Docker 状态 |

### L1.2 Web 服务完整性

```
[ ] GET / → 200, >= 17000B
[ ] GET / 含 "v0.5.0"
[ ] GET /api/v1/latest → 200, JSON schema 完整
[ ] GET /api/v1/history?metric=cpu&minutes=15 → 200, count > 0
[ ] GET /api/v1/history/multi?minutes=30 → 200, 含 cpu/ram/temp 数组
[ ] POST /api/v1/ingest → 200 {ok:true}
[ ] GET /nonexistent → 404
[ ] WebSocket /ws → 推送 data 类型消息
```

### L1.3 数据合理性

```
[ ] 所有 cpu_pct / mem_pct / disk_pct 在 0-100 范围
[ ] 无 NaN / Inf
[ ] 无 None 关键字段（cpu_pct, mem_pct 必填）
[ ] 历史数据点 timestamps 单调递增
```

---

## 三、L2 并发压力测试（谁：十六万测试 ECS，3 分钟）

### L2.1 高频轮询
```bash
# 每秒 1 次 × 30 次
for i in $(seq 1 30); do
  code=$(curl -s -o /dev/null -w "%{http_code}" https://dlt.k38.ai/api/v1/latest)
  ms=$(curl -s -o /dev/null -w "%%{time_total}" https://dlt.k38.ai/api/v1/latest)
  echo "$i: $code ${ms}s"
  sleep 1
done
```
```
[ ] 全部 200
[ ] 最大响应 < 2s（考虑公网延迟）
[ ] 无连接拒绝
```

### L2.2 并发请求
```bash
# 50 路并行 × 5 轮
for r in $(seq 1 5); do
  for i in $(seq 1 50); do
    curl -s -o /dev/null -w "%{http_code}\n" https://dlt.k38.ai/api/v1/latest &
  done
  wait
done
```
```
[ ] 全部 200，失败 = 0
[ ] 进程存活（systemctl status dlt-node）
```

### L2.3 混合压力（WebSocket + HTTP 同时）
```bash
# 后台 10 个 WS 连接 + 前台 20 并发 HTTP
```
```
[ ] WS 连接全部收到数据
[ ] HTTP 全部 200
```

---

## 四、L3 ECS 全栈功能测试（谁：十六万，5 分钟）

### L3.1 Redis 缓存
```
[ ] dlt:latest key 存在，TTL 接近 300s
[ ] dlt:latest JSON 可解析，含 nodes 字段
[ ] 写入后立即读取返回最新数据（< 500ms）
```

### L3.2 PostgreSQL 历史
```
[ ] dlt_snapshots 表存在
[ ] 快照数 > 50（持续运行产生的）
[ ] 时间跨度 > 30 分钟
[ ] 点间间隔无 > 60s 的 gap
[ ] 最新快照与 Redis 时间接近（< 60s 偏差）
```

### L3.3 Node.js REST API
```
[ ] /api/v1/latest 响应 < 500ms（ECS 本地）
[ ] /api/v1/history 支持 minutes=15,60,1440
[ ] /api/v1/history/multi 返回多指标
[ ] 非法 minutes（-1 / 9999）→ 400
```

### L3.4 前端面板渲染
```
[ ] 页面加载 < 3s（公网）
[ ] 节点卡片显示正确名称（大傻/二傻/三万八…）
[ ] 趋势图展开 SVG 正确渲染
[ ] 网络延迟行显示 CN/GH/YT/GO/YH
[ ] 高速互联显示 80Gb/s
[ ] Docker 显示容器数
[ ] 右下角 EDIT 按钮可点
[ ] 底部版本号显示 v0.5.0
```

### L3.5 nginx 反代
```
[ ] HTTP→HTTPS 301 跳转
[ ] SSL 证书有效
[ ] /ws 升级协议正确
[ ] /legacy/ 可访问旧版
```

---

## 五、L4 交叉互检（谁：十六万，5 分钟）

### L4.1 数据源一致性
```
[ ] 本机 /tmp/dltrace.json 的 nodes 数量 = ECS Redis dlt:latest 的 nodes 数量
[ ] 本机 ts 与 ECS Redis ts 差异 < 60s
[ ] PG 最新快照 = Redis 最新数据（nodes 数量一致）
```

### L4.2 双通道验证
```
[ ] HTTP API 返回数据 = WS 推流数据（同一时刻对比）
[ ] 同步脚本 SCP 未损坏文件（json.load 成功）
```

### L4.3 节点完整性
```
[ ] 大傻在线
[ ] 二傻在线
[ ] 三万八在线
[ ] 十六万在线（本机 daemon）
[ ] 小四在线
```

---

## 六、L6 中断恢复测试（谁：十六万，5 分钟）

### L6.1 终止 daemon → Node.js 仍服务
```
[ ] 停止本机 daemon → ECS 仍返回最后缓存数据
[ ] 数据不报错（返回 stale 数据而非 error）
[ ] 重启 daemon → 数据恢复推流
```

### L6.2 中断 Redis → PG 回退
```
[ ] Redis 停掉 → /api/v1/latest 读文件回退
[ ] Redis 恢复 → 自动切回
```

### L6.3 重启 NGINX
```
[ ] nginx reload 不丢连接
[ ] WS 连接重连
```

---

## 七、L7 高压 HVP 测试（谁：十六万，5 分钟）

### L7.1 100 并发 × /api/v1/latest
```
[ ] 100% 200
[ ] 0 超时拒绝
[ ] 平均响应 < 3s（公网）
```

### L7.2 100 并发 × /api/v1/history
```
[ ] 100% 200
[ ] JSON schema 全部正确
[ ] PG 连接池无泄漏
```

### L7.3 大负载 Ingest
```
[ ] POST 500KB JSON → 200 {ok:true}
[ ] Redis 写入成功
[ ] WS 广播成功
```

### L7.4 长期稳定性
```
[ ] 持续运行 5 分钟，内存无增长（监控 RSS）
```

---

## 八、L8 数据盾（谁：小四主导，5 分钟）

### L8.1 JSON Schema 严格校验
```
[ ] 所有必填字段存在（12 项）
[ ] 无 NaN / Inf
[ ] 所有数值字段在合理范围
[ ] 无重复节点名
```

### L8.2 历史数据连续性
```
[ ] 时间戳单调递增
[ ] 无重复 ts（精确到秒）
[ ] 数据点间间隔 < 60s
[ ] 最新数据与当前时间 < 60s 偏差
```

### L8.3 跨字段一致性
```
[ ] nodes 下的 cpu_pct 总和≈系统 cpu_pct（聚合合理）
[ ] nodes 数量与 nodes_count 一致
```

---

## 九、L10 LIVE 生产验证（手动的）

### L10.1 用户验收
```
[ ] 手机微信浏览器打开 https://dlt.k38.ai/ → 渲染完整
[ ] 点击"查看历史趋势" → SVG 曲线展开
[ ] 网络行滚动查看延迟数据
[ ] 数据自动刷新（不需手动 F5）
[ ] 版本号显示 v0.5.0
```

---

## 测试记录表

| 层级 | 名称 | 完成 | 失败 | 用时 |
|------|------|------|------|------|
| L0 | 冒烟起点 | | | |
| L1 | 单节点验证 | | | |
| L2 | 并发压力 | | | |
| L3 | ECS全栈 | | | |
| L4 | 交叉互检 | | | |
| L6 | 中断恢复 | | | |
| L7 | 高压HVP | | | |
| L8 | 数据盾 | | | |
| L10 | LIVE生产 | | | |

### 核心原则（同 v0.2）

1. **先基础后复杂**  
2. **数据 + 前端交叉验证**  
3. **先隔离再交叉**  
4. **混沌在前**  
5. **"If you haven't tried it, assume it's broken"**  
6. **每次测试保留现场**
