'use strict';

const express = require('express');
const cors    = require('cors');
const { v4: uuidv4 } = require('uuid');

const app  = express();
const PORT = process.env.PORT || 3000;

/* ============================================================
   CONFIGURACIÓN
============================================================ */
const TTL_MS        = parseInt(process.env.TTL_HOURS  || '12') * 60 * 60 * 1000;
const MAX_MSGS      = parseInt(process.env.MAX_MSGS   || '500');
const MAX_TOPICS    = parseInt(process.env.MAX_TOPICS || '50');
const SSE_HEARTBEAT = 20000;   // 20 s — Railway corta conexiones idle a los 30 s

/* ============================================================
   CORS
============================================================ */
app.use(cors({
  origin: '*',
  methods: ['GET', 'POST', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization', 'Cache-Control',
                   'X-DDS-Key', 'Filename', 'Priority', 'Firebase']
}));
app.use(express.json({ limit: '64kb' }));
app.use(express.text({ limit: '64kb' }));

/* ============================================================
   ESTADO EN MEMORIA
============================================================ */
// chat:     Map<topicId, { msgs: [], clients: Set }>
// presence: Map<topicId, { users: Map<userId, obj>, clients: Set }>
const chatTopics     = new Map();
const presenceTopics = new Map();

function getChatTopic(id) {
  if (!chatTopics.has(id)) {
    if (chatTopics.size >= MAX_TOPICS) evictOldestChatTopic();
    chatTopics.set(id, { msgs: [], clients: new Set() });
  }
  return chatTopics.get(id);
}

function getPresenceTopic(id) {
  if (!presenceTopics.has(id)) {
    presenceTopics.set(id, { users: new Map(), clients: new Set() });
  }
  return presenceTopics.get(id);
}

function evictOldestChatTopic() {
  let oldestId  = null;
  let oldestTs  = Infinity;
  for (const [id, t] of chatTopics) {
    if (t.clients.size > 0) continue;           // no expulsar topics con clientes
    const last = t.msgs.at(-1)?.ts || 0;
    if (last < oldestTs) { oldestTs = last; oldestId = id; }
  }
  if (oldestId) chatTopics.delete(oldestId);
}

/* ============================================================
   LIMPIEZA PERIÓDICA — cada 5 minutos
============================================================ */
setInterval(() => {
  const now = Date.now();

  // Mensajes expirados
  for (const [id, t] of chatTopics) {
    t.msgs = t.msgs.filter(m => m.ts + TTL_MS > now);
    if (t.msgs.length === 0 && t.clients.size === 0) chatTopics.delete(id);
  }

  // Presencia expirada
  for (const [id, t] of presenceTopics) {
    for (const [uid, u] of t.users) {
      if (now - u.at > (u.ttl || 90) * 1000 + 15000) t.users.delete(uid);
    }
    if (t.users.size === 0 && t.clients.size === 0) presenceTopics.delete(id);
  }
}, 5 * 60 * 1000);

/* ============================================================
   HELPERS
============================================================ */
function isValidTopic(id) {
  return typeof id === 'string' && /^[a-zA-Z0-9_\-]{3,80}$/.test(id);
}

function parseSince(since) {
  if (!since || since === '0') return 0;
  const map = { '12h':'43200000','6h':'21600000','1h':'3600000',
                '30m':'1800000','90s':'90000','30s':'30000' };
  if (map[since]) return Date.now() - parseInt(map[since]);
  const n = parseInt(since, 10);
  // Si es un número grande = timestamp Unix en segundos
  if (!isNaN(n) && n > 1000000000) return n * 1000;
  // Si es un número pequeño = segundos relativos
  if (!isNaN(n) && n > 0) return Date.now() - n * 1000;
  return 0;
}

function makeEvent(id, topic, messageStr, timeMs) {
  return {
    id:      id,
    event:   'message',
    topic:   topic,
    time:    Math.floor((timeMs || Date.now()) / 1000),
    message: messageStr
  };
}

function broadcast(clients, eventData) {
  const payload = 'data: ' + JSON.stringify(eventData) + '\n\n';
  const dead    = [];
  for (const client of clients) {
    try   { client.res.write(payload); }
    catch { dead.push(client); }
  }
  for (const c of dead) clients.delete(c);
}

function setupSSE(res) {
  res.setHeader('Content-Type',      'text/event-stream');
  res.setHeader('Cache-Control',     'no-cache, no-store');
  res.setHeader('Connection',        'keep-alive');
  res.setHeader('X-Accel-Buffering', 'no');
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.flushHeaders();
}

/* ============================================================
   RUTAS — HEALTH Y HOME
============================================================ */
app.get('/health', (_req, res) => {
  const totalMsgs    = [...chatTopics.values()].reduce((s, t) => s + t.msgs.length, 0);
  const totalClients = [...chatTopics.values()].reduce((s, t) => s + t.clients.size, 0);
  res.json({
    status:        'ok',
    version:       '1.0.0',
    uptime:        Math.floor(process.uptime()),
    topics:        chatTopics.size,
    presence:      presenceTopics.size,
    totalMsgs,
    totalClients,
    memoryMB:      (process.memoryUsage().heapUsed / 1048576).toFixed(1),
    ttlHours:      TTL_MS / 3600000
  });
});

app.get('/', (_req, res) => {
  const totalMsgs    = [...chatTopics.values()].reduce((s, t) => s + t.msgs.length, 0);
  const totalClients = [...chatTopics.values()].reduce((s, t) => s + t.clients.size, 0);
  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  res.send(`<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>DDS Chat Server</title>
  <style>
    *{box-sizing:border-box;margin:0;padding:0;}
    body{font-family:"Cascadia Code",Consolas,monospace;background:#0C110F;
         color:#3DDC97;padding:40px 24px;max-width:640px;margin:0 auto;}
    h1{color:#F1F5F2;font-size:16px;letter-spacing:.3em;margin-bottom:28px;
       display:flex;align-items:center;gap:10px;}
    .dot{width:8px;height:8px;border-radius:50%;background:#3DDC97;
         box-shadow:0 0 10px #3DDC97;animation:ping 2s infinite;}
    @keyframes ping{0%{box-shadow:0 0 0 0 rgba(61,220,151,.5);}
                   70%,100%{box-shadow:0 0 0 10px rgba(61,220,151,0);}}
    table{width:100%;border-collapse:collapse;font-size:13px;}
    td{padding:10px 4px;border-bottom:1px solid #1e2b24;}
    td:last-child{color:#F1F5F2;font-weight:700;text-align:right;}
    .green{color:#3DDC97;}.amber{color:#F59E0B;}.gray{color:#6F7C75;}
    .footer{margin-top:32px;font-size:10px;color:#3a4a42;letter-spacing:.1em;}
  </style>
</head>
<body>
  <h1><span class="dot"></span>DDS CHAT SERVER</h1>
  <table>
    <tr><td>Estado</td>
        <td class="green">● EN LÍNEA</td></tr>
    <tr><td>Versión</td>
        <td>v1.0.0</td></tr>
    <tr><td>Topics de chat activos</td>
        <td>${chatTopics.size} / ${MAX_TOPICS}</td></tr>
    <tr><td>Mensajes en memoria</td>
        <td>${totalMsgs}</td></tr>
    <tr><td>Clientes SSE conectados</td>
        <td class="green">${totalClients}</td></tr>
    <tr><td>Topics de presencia</td>
        <td>${presenceTopics.size}</td></tr>
    <tr><td>TTL mensajes</td>
        <td>${TTL_MS / 3600000} horas</td></tr>
    <tr><td>Uptime</td>
        <td>${Math.floor(process.uptime())} s</td></tr>
    <tr><td>Memoria usada</td>
        <td>${(process.memoryUsage().heapUsed/1048576).toFixed(1)} MB</td></tr>
  </table>
  <div class="footer">DDS Chat Pro · Railway · ${new Date().toLocaleString('es-CR')}</div>
</body>
</html>`);
});

/* ============================================================
   RUTAS — CHAT
============================================================ */

// POST /chat/:topicId  — publicar mensaje
app.post('/chat/:topicId', (req, res) => {
  const { topicId } = req.params;
  if (!isValidTopic(topicId))
    return res.status(400).json({ error: 'Topic inválido' });

  let payload;
  try {
    payload = typeof req.body === 'string' ? JSON.parse(req.body) : req.body;
  } catch {
    return res.status(400).json({ error: 'Body inválido' });
  }
  if (!payload || typeof payload !== 'object')
    return res.status(400).json({ error: 'Payload inválido' });

  const topic = getChatTopic(topicId);
  const id    = uuidv4();
  const now   = Date.now();

  // Solo persistir mensajes de tipo 'm' — el resto son efímeros
  if (payload.t === 'm') {
    topic.msgs.push({ id, ts: now, payload: JSON.stringify(payload) });
    if (topic.msgs.length > MAX_MSGS)
      topic.msgs = topic.msgs.slice(-MAX_MSGS);
  }

  broadcast(topic.clients, makeEvent(id, topicId, JSON.stringify(payload), now));
  res.status(200).json({ id, time: Math.floor(now / 1000) });
});

// GET /chat/:topicId/sse  — stream SSE
app.get('/chat/:topicId/sse', (req, res) => {
  const { topicId } = req.params;
  if (!isValidTopic(topicId))
    return res.status(400).json({ error: 'Topic inválido' });

  setupSSE(res);
  const topic   = getChatTopic(topicId);
  const sinceMs = parseSince(req.query.since || '12h');

  // Historial
  for (const m of topic.msgs.filter(m => m.ts >= sinceMs)) {
    res.write('data: ' + JSON.stringify(makeEvent(m.id, topicId, m.payload, m.ts)) + '\n\n');
  }

  const client = { res, connectedAt: Date.now() };
  topic.clients.add(client);

  // Heartbeat — Railway corta conexiones sin actividad a los ~30 s
  const hb = setInterval(() => {
    try   { res.write(': ping\n\n'); }
    catch { cleanup(); }
  }, SSE_HEARTBEAT);

  function cleanup() {
    clearInterval(hb);
    topic.clients.delete(client);
  }
  req.on('close',  cleanup);
  req.on('error',  cleanup);
  res.on('error',  cleanup);
});

// GET /chat/:topicId/poll  — polling fallback (NDJSON)
app.get('/chat/:topicId/poll', (req, res) => {
  const { topicId } = req.params;
  if (!isValidTopic(topicId))
    return res.status(400).json({ error: 'Topic inválido' });

  const topic   = getChatTopic(topicId);
  const sinceMs = parseSince(req.query.since || '12h');

  res.setHeader('Content-Type', 'application/x-ndjson');
  res.setHeader('Access-Control-Allow-Origin', '*');

  for (const m of topic.msgs.filter(m => m.ts >= sinceMs)) {
    res.write(JSON.stringify(makeEvent(m.id, topicId, m.payload, m.ts)) + '\n');
  }
  res.end();
});

/* ============================================================
   RUTAS — PRESENCIA
============================================================ */

// POST /presence/:topicId  — publicar presencia
app.post('/presence/:topicId', (req, res) => {
  const { topicId } = req.params;
  if (!isValidTopic(topicId))
    return res.status(400).json({ error: 'Topic inválido' });

  let payload;
  try {
    payload = typeof req.body === 'string' ? JSON.parse(req.body) : req.body;
  } catch {
    return res.status(400).json({ error: 'Body inválido' });
  }
  if (!payload || payload.t !== 'presence' || !payload.u)
    return res.status(400).json({ error: 'Payload de presencia inválido' });

  const topic = getPresenceTopic(topicId);
  const id    = uuidv4();
  const now   = Date.now();

  if (payload.s === 'offline') {
    topic.users.delete(payload.u);
  } else {
    topic.users.set(payload.u, {
      id:     payload.u,
      name:   (payload.n || payload.u).slice(0, 32),
      status: payload.s || 'online',
      at:     now,
      ttl:    Math.min(payload.ttl || 90, 300)  // máximo 5 min
    });
  }

  broadcast(topic.clients, makeEvent(id, topicId, JSON.stringify(payload), now));
  res.status(200).json({ id, time: Math.floor(now / 1000) });
});

// GET /presence/:topicId/sse  — stream presencia
app.get('/presence/:topicId/sse', (req, res) => {
  const { topicId } = req.params;
  if (!isValidTopic(topicId))
    return res.status(400).json({ error: 'Topic inválido' });

  setupSSE(res);
  const topic = getPresenceTopic(topicId);
  const now   = Date.now();

  // Enviar usuarios activos al conectar
  for (const [, u] of topic.users) {
    if (now - u.at < u.ttl * 1000 + 15000) {
      const ev = { t:'presence', u:u.id, n:u.name, s:u.status,
                   at:u.at, ttl:u.ttl, v:'server' };
      res.write('data: ' + JSON.stringify(
        makeEvent(uuidv4(), topicId, JSON.stringify(ev), u.at)
      ) + '\n\n');
    }
  }

  const client = { res, connectedAt: Date.now() };
  topic.clients.add(client);

  const hb = setInterval(() => {
    try   { res.write(': ping\n\n'); }
    catch { cleanup(); }
  }, SSE_HEARTBEAT);

  function cleanup() {
    clearInterval(hb);
    topic.clients.delete(client);
  }
  req.on('close',  cleanup);
  req.on('error',  cleanup);
  res.on('error',  cleanup);
});

// GET /presence/:topicId/poll  — polling presencia
app.get('/presence/:topicId/poll', (req, res) => {
  const { topicId } = req.params;
  if (!isValidTopic(topicId))
    return res.status(400).json({ error: 'Topic inválido' });

  const topic = getPresenceTopic(topicId);
  const now   = Date.now();

  res.setHeader('Content-Type', 'application/x-ndjson');
  res.setHeader('Access-Control-Allow-Origin', '*');

  for (const [, u] of topic.users) {
    if (now - u.at < u.ttl * 1000 + 15000) {
      const ev = { t:'presence', u:u.id, n:u.name, s:u.status,
                   at:u.at, ttl:u.ttl, v:'server' };
      res.write(JSON.stringify(
        makeEvent(uuidv4(), topicId, JSON.stringify(ev), u.at)
      ) + '\n');
    }
  }
  res.end();
});

/* ============================================================
   404 catch-all
============================================================ */
app.use((_req, res) => res.status(404).json({ error: 'Ruta no encontrada' }));

/* ============================================================
   ARRANQUE
============================================================ */
app.listen(PORT, () => {
  console.log(`[DDS Chat] Puerto      : ${PORT}`);
  console.log(`[DDS Chat] TTL         : ${TTL_MS / 3600000} h`);
  console.log(`[DDS Chat] Max topics  : ${MAX_TOPICS}`);
  console.log(`[DDS Chat] Max msgs    : ${MAX_MSGS}`);
  console.log(`[DDS Chat] Health      : http://localhost:${PORT}/health`);
});