const express = require('express');
const http = require('http');
const crypto = require('crypto');

const PORT = process.env.PORT || 3003;
const WEBHOOK_SECRET = '19R4e6iRiOckdAmXJhGVMQdd28MAIe438waJQ0KFPi0';

const app = express();

app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, X-Webhook-Signature');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

app.use(express.json({
  verify: (req, res, buf) => {
    req.rawBody = Buffer.from(buf);
  },
}));

const messages = [];
let nextId = 1;

function nowIso() {
  return new Date().toISOString();
}

function timingSafeHexEquals(a, b) {
  const aa = Buffer.from(String(a), 'hex');
  const bb = Buffer.from(String(b), 'hex');
  if (aa.length === 0 || bb.length === 0 || aa.length !== bb.length) return false;
  return crypto.timingSafeEqual(aa, bb);
}

function computeSignature(rawBody) {
  return crypto.createHmac('sha256', WEBHOOK_SECRET).update(rawBody).digest('hex');
}

function verifyWebhookSignature(req) {
  const header = req.get('X-Webhook-Signature');
  if (!header || !req.rawBody) return false;

  const expected = computeSignature(req.rawBody);
  const normalized = header.startsWith('sha256=') ? header.slice(7) : header;
  return timingSafeHexEquals(expected, normalized);
}

function normalizeText(value) {
  if (value == null) return '';
  return String(value);
}

function createMessage({ from, text, thinking = null, reply = null, replyTs = null }) {
  const message = {
    id: nextId++,
    from: normalizeText(from || 'anonymous'),
    text: normalizeText(text),
    thinking: thinking == null ? null : normalizeText(thinking),
    reply: reply == null ? null : normalizeText(reply),
    replyTs: replyTs || null,
    ts: nowIso(),
  };
  messages.push(message);
  return message;
}

function findMessageById(id) {
  const numericId = Number.parseInt(id, 10);
  if (!Number.isFinite(numericId)) return null;
  return messages.find((message) => message.id === numericId) || null;
}

app.get('/', (req, res) => {
  res.json({ ok: true, service: 'K38 Collaboration Channel API' });
});

app.post('/api/channel/send', (req, res) => {
  if (!verifyWebhookSignature(req)) {
    return res.status(401).json({ error: 'invalid signature' });
  }

  const { from, text, thinking } = req.body || {};
  if (!text) {
    return res.status(400).json({ error: 'text is required' });
  }

  const message = createMessage({ from, text, thinking });
  return res.json({ ok: true, message });
});

app.post('/api/channel/post', (req, res) => {
  const { from, text, thinking } = req.body || {};
  if (!text) {
    return res.status(400).json({ error: 'text is required' });
  }

  const message = createMessage({ from, text, thinking });
  return res.json({ ok: true, message });
});

app.post('/api/channel/reply', (req, res) => {
  if (!verifyWebhookSignature(req)) {
    return res.status(401).json({ error: 'invalid signature' });
  }

  const { id, reply, from } = req.body || {};
  if (id == null) {
    return res.status(400).json({ error: 'id is required' });
  }
  if (!reply) {
    return res.status(400).json({ error: 'reply is required' });
  }

  const message = findMessageById(id);
  if (!message) {
    return res.status(404).json({ error: 'message not found' });
  }

  message.reply = normalizeText(reply);
  message.replyTs = nowIso();

  return res.json({ ok: true, message });
});

app.get('/api/channel/messages', (req, res) => {
  return res.json({ ok: true, messages });
});

app.get('/api/channel/poll', (req, res) => {
  const since = req.query.since;
  if (since == null || since === '') {
    return res.json({ ok: true, messages });
  }

  const sinceId = Number.parseInt(since, 10);
  if (!Number.isFinite(sinceId)) {
    return res.status(400).json({ error: 'since must be a number' });
  }

  return res.json({
    ok: true,
    messages: messages.filter((message) => message.id > sinceId),
  });
});

const server = http.createServer(app);

server.listen(PORT, '0.0.0.0', () => {
  console.log(`[K38Chat] listening on :${PORT}`);
});
