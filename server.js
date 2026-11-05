// 毒と炎のカード決闘 — 自前サーバー
// ゲーム本体 card-duel.html はそのまま配信し、claude.ai の db / room / user と同じ形の
// 仕組みを WebSocket で提供する（ブラウザ側は shim.js が受け持つ）。
'use strict';
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = Number(process.env.PORT) || 3000;
const ADMIN_KEY = process.env.ADMIN_KEY || '';
const ROOT = __dirname;
const DATA_DIR = process.env.DATA_DIR || path.join(ROOT, 'data');
const DB_FILE = path.join(DATA_DIR, 'db.json');
const SEED_ART = path.join(ROOT, 'seed', 'cardart.json');
const MAX_DOC = 300 * 1024;
const MAX_EMIT = 4 * 1024;

// ---------- 保存領域 ----------
/** @type {Map<string, any>} 'players/u_x' -> data */
const docs = new Map();
/** @type {Map<string, string>} uid -> sha256(secret) */
const auth = new Map();

function load() {
  try {
    const j = JSON.parse(fs.readFileSync(DB_FILE, 'utf8'));
    for (const [k, v] of Object.entries(j.docs || {})) docs.set(k, v);
    for (const [k, v] of Object.entries(j.auth || {})) auth.set(k, v);
    console.log(`loaded ${docs.size} docs`);
  } catch (e) { console.log('no saved data, starting fresh'); }
  try {
    const art = JSON.parse(fs.readFileSync(SEED_ART, 'utf8'));
    let n = 0;
    for (const [k, img] of Object.entries(art)) {
      if (!/^[a-z0-9_]{1,40}$/.test(k) || typeof img !== 'string') continue;
      if (!docs.has('cardart/' + k)) { docs.set('cardart/' + k, { img, updated: 0 }); n++; }
    }
    console.log(`seeded ${n} card images`);
  } catch (e) { console.log('no seed/cardart.json'); }
}
let saveTimer = null;
function scheduleSave() {
  if (saveTimer) return;
  saveTimer = setTimeout(() => {
    saveTimer = null;
    // 終わった対戦は1日で消す
    const cut = Date.now() - 24 * 3600 * 1000;
    for (const [k, v] of docs) if (k.startsWith('matches/') && (v.updated || 0) < cut) docs.delete(k);
    try {
      fs.mkdirSync(DATA_DIR, { recursive: true });
      const tmp = DB_FILE + '.tmp';
      fs.writeFileSync(tmp, JSON.stringify({ docs: Object.fromEntries(docs), auth: Object.fromEntries(auth) }));
      fs.renameSync(tmp, DB_FILE);
    } catch (e) { console.error('save failed', e.message); }
  }, 2000);
}

// ---------- 規則 ----------
const SEG = /^[A-Za-z0-9_.\-]{1,64}$/;
function splitPath(p) {
  if (typeof p !== 'string') return null;
  const s = p.split('/');
  return s.every(x => SEG.test(x)) ? s : null;
}
function canWrite(c, segs) {
  if (segs.length !== 2) return false;
  const [col, id] = segs;
  if (col === 'players' || col === 'cards') return id === c.uid || c.admin;
  if (col === 'matches') return true;
  if (col === 'cardart') return c.admin;
  return false;
}
const isObj = v => v && typeof v === 'object' && !Array.isArray(v);

// ---------- 購読 ----------
/** @type {Set<Client>} */
const clients = new Set();
function docSnap(p) { return docs.has(p) ? { exists: true, data: docs.get(p) } : { exists: false, data: null }; }
function colSnap(q) {
  const out = [];
  const pre = q.path + '/';
  for (const [k, v] of docs) {
    if (!k.startsWith(pre) || k.indexOf('/', pre.length) >= 0) continue;
    if (q.where && !q.where.every(([f, op, val]) => op === '==' ? v[f] === val : op === '!=' ? v[f] !== val : true)) continue;
    out.push({ id: k.slice(pre.length), data: v });
    if (q.limit && out.length >= q.limit) break;
  }
  return out;
}
function sendSnap(c, id, q) {
  if (q.kind === 'doc') c.send({ t: 'snap', sub: id, ...docSnap(q.path) });
  else c.send({ t: 'snap', sub: id, docs: colSnap(q) });
}
function changed(p) {
  const col = p.slice(0, p.lastIndexOf('/'));
  for (const c of clients) for (const [id, q] of c.subs) {
    if ((q.kind === 'doc' && q.path === p) || (q.kind === 'col' && q.path === col)) sendSnap(c, id, q);
  }
  scheduleSave();
}

// ---------- 部屋 ----------
/** @type {Map<string, Map<string, Client>>} room -> peer -> client */
const rooms = new Map();
function peersOf(name) {
  const r = rooms.get(name);
  return r ? [...r.values()].map(c => ({ peer: c.peer, presence: c.presence.get(name) || {}, kind: 'viewer', guest: false })) : [];
}
function broadcastPeers(name) {
  const r = rooms.get(name); if (!r) return;
  const peers = peersOf(name);
  for (const c of r.values()) c.send({ t: 'peers', room: name, peers });
}
function joinRoom(c, name) {
  if (!rooms.has(name)) rooms.set(name, new Map());
  rooms.get(name).set(c.peer, c);
  if (!c.presence.has(name)) c.presence.set(name, {});
  broadcastPeers(name);
}
function leaveRoom(c, name) {
  const r = rooms.get(name); if (!r) return;
  r.delete(c.peer); c.presence.delete(name);
  if (!r.size) rooms.delete(name); else broadcastPeers(name);
}

// ---------- 接続 ----------
class Client {
  constructor(ws) {
    this.ws = ws; this.uid = null; this.admin = false;
    this.peer = 'p' + crypto.randomBytes(9).toString('hex');
    this.subs = new Map(); this.presence = new Map();
  }
  send(o) { if (this.ws.readyState === 1) this.ws.send(JSON.stringify(o)); }
}

function handle(c, m) {
  const ok = data => c.send({ t: 'res', id: m.id, ok: true, data });
  const ng = err => c.send({ t: 'res', id: m.id, ok: false, err });
  if (m.op === 'hello') {
    const { uid, secret } = m;
    if (typeof uid !== 'string' || !/^u_[A-Za-z0-9]{10,40}$/.test(uid) || typeof secret !== 'string' || secret.length < 20) return ng('bad_identity');
    const h = crypto.createHash('sha256').update(secret).digest('hex');
    if (auth.has(uid) && auth.get(uid) !== h) return ng('bad_secret');
    if (!auth.has(uid)) { auth.set(uid, h); scheduleSave(); }
    c.uid = uid;
    c.admin = !!ADMIN_KEY && typeof m.admin === 'string' && m.admin.length === ADMIN_KEY.length &&
      crypto.timingSafeEqual(Buffer.from(m.admin), Buffer.from(ADMIN_KEY));
    return ok({ uid, admin: c.admin, peer: c.peer });
  }
  if (!c.uid) return ng('not_ready');
  const segs = m.path !== undefined ? splitPath(m.path) : null;
  switch (m.op) {
    case 'get':
      if (!segs || segs.length !== 2) return ng('bad_path');
      return ok(docSnap(m.path));
    case 'set': case 'update': {
      if (!segs || !canWrite(c, segs)) return ng('permission_denied');
      if (!isObj(m.data)) return ng('bad_data');
      const next = m.op === 'set' ? m.data : { ...(docs.get(m.path) || {}), ...m.data };
      if (m.op === 'update' && !docs.has(m.path)) return ng('not_found');
      if (JSON.stringify(next).length > MAX_DOC) return ng('too_large');
      docs.set(m.path, next); changed(m.path); return ok(true);
    }
    case 'delete':
      if (!segs || !canWrite(c, segs)) return ng('permission_denied');
      docs.delete(m.path); changed(m.path); return ok(true);
    case 'sub': {
      if (!segs || typeof m.sub !== 'string') return ng('bad_path');
      const q = segs.length === 2 ? { kind: 'doc', path: m.path }
        : { kind: 'col', path: m.path, limit: Math.min(1000, Number(m.limit) || 0) || 0,
            where: Array.isArray(m.where) ? m.where.filter(w => Array.isArray(w) && w.length === 3).slice(0, 5) : null };
      c.subs.set(m.sub, q); ok(true); sendSnap(c, m.sub, q); return;
    }
    case 'unsub': c.subs.delete(m.sub); return ok(true);
    case 'qget':
      if (!segs || segs.length !== 1) return ng('bad_path');
      return ok(colSnap({ path: m.path, limit: m.limit, where: m.where }));
    case 'join':
      if (typeof m.room !== 'string' || m.room.length > 80) return ng('bad_room');
      joinRoom(c, m.room); return ok({ peers: peersOf(m.room) });
    case 'leave': leaveRoom(c, m.room); return ok(true);
    case 'presence': {
      const r = rooms.get(m.room);
      if (!r || !r.has(c.peer) || !isObj(m.patch)) return ng('not_joined');
      const next = { ...(c.presence.get(m.room) || {}), ...m.patch };
      if (JSON.stringify(next).length > MAX_EMIT) return ng('too_large');
      c.presence.set(m.room, next); broadcastPeers(m.room); return ok(true);
    }
    case 'emit': {
      const r = rooms.get(m.room);
      if (!r || !r.has(c.peer)) return ng('not_joined');
      if (typeof m.topic !== 'string' || JSON.stringify(m.data ?? null).length > MAX_EMIT) return ng('too_large');
      for (const o of r.values()) if (o !== c) o.send({ t: 'ev', room: m.room, topic: m.topic, peer: c.peer, data: m.data });
      return ok(true);
    }
    default: return ng('unknown_op');
  }
}

// ---------- HTTP ----------
function page() {
  const body = fs.readFileSync(path.join(ROOT, 'card-duel.html'), 'utf8');
  return '<!doctype html><html lang="ja"><head><meta charset="utf-8">' +
    '<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">' +
    '<style>*,*::before,*::after{box-sizing:border-box}body{margin:0}</style>' +
    '<script src="/shim.js"></script></head><body>' + body + '</body></html>';
}
const server = http.createServer((req, res) => {
  const url = req.url.split('?')[0];
  try {
    if (url === '/' || url === '/index.html') {
      res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-cache' });
      return res.end(page());
    }
    if (url === '/shim.js') {
      res.writeHead(200, { 'content-type': 'text/javascript; charset=utf-8', 'cache-control': 'no-cache' });
      return res.end(fs.readFileSync(path.join(ROOT, 'shim.js')));
    }
    if (url === '/healthz') { res.writeHead(200, { 'content-type': 'text/plain' }); return res.end('ok'); }
  } catch (e) { console.error(e); res.writeHead(500); return res.end('error'); }
  res.writeHead(404, { 'content-type': 'text/plain' }); res.end('not found');
});

const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 2 * 1024 * 1024 });
wss.on('connection', ws => {
  const c = new Client(ws); clients.add(c);
  ws.isAlive = true; ws.on('pong', () => { ws.isAlive = true; });
  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    if (!isObj(m)) return;
    try { handle(c, m); } catch (e) { console.error(e); c.send({ t: 'res', id: m.id, ok: false, err: 'server_error' }); }
  });
  ws.on('close', () => {
    clients.delete(c);
    for (const name of [...c.presence.keys()]) leaveRoom(c, name);
  });
});
// 切れた接続を見つけるための ping
setInterval(() => {
  for (const ws of wss.clients) { if (!ws.isAlive) { ws.terminate(); continue; } ws.isAlive = false; ws.ping(); }
}, 20000);

load();
server.listen(PORT, () => console.log(`card-duel server on :${PORT}${ADMIN_KEY ? '' : ' (ADMIN_KEY 未設定: 絵柄の登録は無効)'}`));
process.on('SIGTERM', () => { if (saveTimer) { clearTimeout(saveTimer); saveTimer = null; } try { fs.mkdirSync(DATA_DIR, { recursive: true }); fs.writeFileSync(DB_FILE, JSON.stringify({ docs: Object.fromEntries(docs), auth: Object.fromEntries(auth) })); } catch (e) {} process.exit(0); });
