/**
 * DLT Node.js Backend
 * 配合 dltrace.py daemon 采集 → SCP同步 → 本服务提供 WebSocket + REST
 * - WebSocket: /ws 推实时数据
 * - REST: GET /api/v1/history?node=大傻&metric=cpu&minutes=15
 * - 前端: / 面板渲染（EJS模板）
 */

const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');
const { Client } = require('pg');
const Redis = require('ioredis');
const path = require('path');
const fs = require('fs');

// === Config ===
const PORT = process.env.PORT || 3001;
const DATA_FILE = process.env.DATA_FILE || '/opt/dltrace/dltrace.json';
const REDIS_HOST = process.env.REDIS_HOST || '127.0.0.1';
const PG_HOST = process.env.PG_HOST || '127.0.0.1';
const PG_PORT = parseInt(process.env.PG_PORT || '5432');
const PG_DB = process.env.PG_DB || 'dltrace';
const PG_USER = process.env.PG_USER || 'medusa';
const PG_PASS = process.env.PG_PASS || 'k38admin';

// === Redis & PostgreSQL ===
const redis = new Redis({ host: REDIS_HOST, port: 6379, retryStrategy: t => Math.min(t * 100, 3000), maxRetriesPerRequest: 1 });
redis.on('error', e => console.error('[REDIS] error:', e.message));
// 非阻塞连接，失败不影响API
redis.connect().catch(e => console.error('[REDIS] connect failed:', e.message));
const pgClient = new Client({
    host: PG_HOST,
    port: PG_PORT,
    database: PG_DB,
    user: PG_USER,
    password: PG_PASS,
    connectionTimeoutMillis: 3000,
});
pgClient.connect(err => {
    if (err) console.error('[PG] connect error:', err.message);
    else console.log('[PG] connected');
});

// === Express ===
const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// CORS
app.use((req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*');
    next();
});

// === REST API ===

// 最新实时数据
app.get('/api/v1/latest', async (req, res) => {
    try {
        // 先尝试文件（零依赖）
        if (fs.existsSync(DATA_FILE)) {
            try {
                const raw = fs.readFileSync(DATA_FILE, 'utf8');
                const parsed = JSON.parse(raw);
                if (parsed.nodes && Object.keys(parsed.nodes).length > 0) {
                    // 异步尝试Redis带上时间戳
                    redis.get('dlt:latest').then(cached => {
                        if (cached) { /* 有缓存就用，但不阻塞响应 */ }
                    }).catch(() => {});
                    return res.json(parsed);
                }
            } catch(e) { console.error('[latest] file error:', e.message); }
        }
        // 再试Redis
        redis.get('dlt:latest').then(cached => {
            if (cached) res.json(JSON.parse(cached));
            else res.json({ error: 'no data' });
        }).catch(() => {
            res.json({ error: 'no data' });
        });
    } catch (e) {
        console.error('[latest] Error:', e.message);
        res.status(500).json({ error: e.message });
    }
});

// 历史趋势数据
app.get('/api/v1/history', async (req, res) => {
    const node = req.query.node || null;
    const metric = req.query.metric || 'cpu';
    const minutes = parseInt(req.query.minutes || '15');
    if (isNaN(minutes) || minutes < 1 || minutes > 1440) {
        return res.status(400).json({ error: 'minutes must be 1-1440' });
    }
    try {
        const cutoff = new Date(Date.now() - minutes * 60 * 1000).toISOString();
        const result = await pgClient.query(
            'SELECT ts, data FROM dlt_snapshots WHERE ts > $1 ORDER BY ts ASC',
            [cutoff]
        );
        const points = [];
        for (const row of result.rows) {
            const d = row.data;
            const sys = d.system || {};
            let val = null;
            if (metric === 'temp') {
                const gpu = sys.gpu_info || {};
                val = typeof gpu === 'object' ? gpu.temp || null : null;
            } else if (metric === 'cpu') {
                val = sys.cpu_pct != null ? sys.cpu_pct : null;
            } else if (metric === 'ram') {
                val = sys.mem_pct != null ? sys.mem_pct : null;
            }
            // 节点级覆盖
            if (node && d.nodes && d.nodes[node]) {
                const ns = d.nodes[node].system || {};
                if (metric === 'cpu' && ns.cpu_pct != null) val = ns.cpu_pct;
                if (metric === 'ram' && ns.mem_pct != null) val = ns.mem_pct;
            }
            if (val != null) {
                points.push({ ts: row.ts.toISOString(), value: parseFloat(val) });
            }
        }
        res.json({ node, metric, minutes, points, count: points.length });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// 可选多指标同时查询
app.get('/api/v1/history/multi', async (req, res) => {
    const minutes = parseInt(req.query.minutes || '15');
    try {
        const cutoff = new Date(Date.now() - minutes * 60 * 1000).toISOString();
        const result = await pgClient.query(
            'SELECT ts, data FROM dlt_snapshots WHERE ts > $1 ORDER BY ts ASC',
            [cutoff]
        );
        const temps = [], cpus = [], rams = [];
        for (const row of result.rows) {
            const d = row.data;
            const sys = d.system || {};
            const ts = row.ts.toISOString();
            const gpu = sys.gpu_info || {};
            const temp = typeof gpu === 'object' ? gpu.temp : null;
            const cpu = sys.cpu_pct;
            const ram = sys.mem_pct;
            if (temp != null) temps.push({ ts, value: parseFloat(temp) });
            if (cpu != null) cpus.push({ ts, value: parseFloat(cpu) });
            if (ram != null) rams.push({ ts, value: parseFloat(ram) });
        }
        res.json({ minutes, temp: temps, cpu: cpus, ram: rams });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// === WebSocket: 推流实时数据 ===
const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });

wss.on('connection', (ws) => {
    console.log('[WS] client connected');
    // 发送当前数据
    redis.get('dlt:latest').then(data => {
        if (data) ws.send(JSON.stringify({ type: 'data', payload: JSON.parse(data) }));
    }).catch(() => {});
    ws.on('close', () => console.log('[WS] client disconnected'));
});

// 数据更新广播（被同步脚本触发）
function broadcastData(data) {
    const msg = JSON.stringify({ type: 'data', payload: data });
    wss.clients.forEach(client => {
        if (client.readyState === 1) client.send(msg);
    });
}

// 从文件热加载广播（被同步脚本通过HTTP触发）
app.post('/api/v1/ingest', (req, res) => {
    try {
        const data = req.body;
        if (!data || !data.nodes) {
            return res.status(400).json({ error: 'invalid data' });
        }
        // 写入Redis
        redis.set('dlt:latest', JSON.stringify(data), 'EX', 300).catch(e => console.error('[REDIS] set error:', e.message));
        // 广播
        broadcastData(data);
        res.json({ ok: true });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// === 前端面板 (EJS) ===
app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'views'));

app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// === Start ===
server.listen(PORT, '0.0.0.0', () => {
    console.log(`[DLTNode] listening on :${PORT}`);
    console.log(`  WebSocket: ws://localhost:${PORT}/ws`);
    console.log(`  REST:      http://localhost:${PORT}/api/v1/latest`);
    console.log(`  History:   http://localhost:${PORT}/api/v1/history?metric=cpu&minutes=15`);
});
