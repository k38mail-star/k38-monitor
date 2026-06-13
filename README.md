# K38 DLT (Deep Link Trace) v0.5.3

> **Python daemon + Node.js + PostgreSQL + Redis + WebSocket 集群监控面板**

监控5节点集群（十六万/三万八/小四/大傻/二傻），实时采集CPU/GPU/内存/磁盘/网络延迟，WebSocket推流，PG历史趋势，Redis缓存加速。

## 架构

```
┌─────────────┐    SCP 15s    ┌──────────────────┐    WebSocket    ┌──────────┐
│ 本机daemon  │ ────────────→ │ ECS Node.js 3001 │ ──────────────→ │ 前端面板 │
│ dltrace.py  │              │ server.js        │                │ index..  │
│ 采集5节点    │              │                  │                │          │
└─────────────┘              ├──────────────────┤                └──────────┘
                              │ Redis (缓存)      │
                              │ PG dlt_snapshots  │
                              │ Nginx → dlt.k38.ai│
                              └──────────────────┘
```

## 快速部署

```bash
# 本机 daemon
python3 dltrace.py daemon

# 前端面板
python3 dltrace.py web
# 或 Docker: docker run -p 8899:8899 k38/dlt
```

生产环境部署见 [K38_DLT_暴力测试标准流程_v0.3.md](docs/K38_DLT_暴力测试标准流程_v0.3.md)

## v0.5.3 变更

| 版本 | 内容 |
|------|------|
| v0.5.3 | WS节流去重 + 温度热力图 + 移动端紧凑 |
| v0.5.2 | launchd daemon watchdog + crontab防护 |
| v0.5.1 | PG Pool连接池 + 历史采样 ≤200 点 + 1440min 214ms |
| v0.5.3 | Node.js生产部署 + WebSocket + GitGuardian修复 |

## 暴力测试

25/26 项通过，含：
- **500并发** × 30s latest API: 20,595 req / 638 req/s
- **Redis停服** → 自动文件回退
- **PG历史** 1440min: 214ms
- **数据盾** 零字段异常

[测试标准文档](docs/K38_DLT_暴力测试标准流程_v0.3.md)

## 安全

- 无硬编码密码（环境变量 + .env文件）
- SSH密钥认证
- GitGuardian全库扫描清除

---

💰 K38 Deep Link Trace · AI集群监控
