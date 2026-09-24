const express = require('express');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 3000;
const DATA_DIR = path.join(__dirname, 'data');
const DATA_FILE = path.join(DATA_DIR, 'store.json');

const RPS = [
  { slug: 'athena', name: 'Athe' },
  { slug: 'aless', name: 'Aless' },
  { slug: 'ortiz', name: 'Ortiz' },
  { slug: 'silvana', name: 'Silvana' },
  { slug: 'dulce', name: 'Dulce' },
  { slug: 'bryan', name: 'Bryan' },
  { slug: 'gabo', name: 'Gabo' },
  { slug: 'matt', name: 'Matt' },
  { slug: 'ara_martz', name: 'Ara Martz' },
];
const RP_SLUGS = new Set(RPS.map((r) => r.slug));
const MAX_LOG = 500;

function defaultState() {
  const rps = {};
  RPS.forEach((r) => { rps[r.slug] = { name: r.name, tickets: 0 }; });
  return { rps, event: { externalSales: 0 }, log: [] };
}

function normalizeState(parsed) {
  if (!parsed || typeof parsed !== 'object') return defaultState();
  if (!parsed.rps) parsed.rps = {};
  RPS.forEach((r) => {
    if (!parsed.rps[r.slug]) parsed.rps[r.slug] = { name: r.name, tickets: 0 };
  });
  if (!parsed.event) parsed.event = { externalSales: 0 };
  if (!Array.isArray(parsed.log)) parsed.log = [];
  return parsed;
}

// With DATABASE_URL (Postgres) data survives restarts. Without it, a local
// file is used, which is only suitable for development: hosts with an
// ephemeral disk (e.g. Render free tier) wipe it on every restart.
const DATABASE_URL = process.env.DATABASE_URL || '';
let pool = null;
if (DATABASE_URL) {
  const { Pool } = require('pg');
  let useSsl = false;
  try { useSsl = new URL(DATABASE_URL).hostname.includes('.'); } catch (e) {}
  pool = new Pool({ connectionString: DATABASE_URL, ssl: useSsl ? { rejectUnauthorized: false } : false });
} else {
  console.warn('ATENCION: sin DATABASE_URL los datos se guardan en un archivo y se pierden al reiniciar el servidor.');
}

async function loadState() {
  let parsed = null;
  if (pool) {
    await pool.query('CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value JSONB NOT NULL)');
    const r = await pool.query("SELECT value FROM kv WHERE key = 'state'");
    if (r.rows.length) parsed = r.rows[0].value;
  } else {
    try { parsed = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8')); } catch (e) {}
  }
  return normalizeState(parsed);
}

let saving = false;
let dirty = false;
async function persist() {
  if (saving) { dirty = true; return; }
  saving = true;
  try {
    do {
      dirty = false;
      if (pool) {
        await pool.query(
          "INSERT INTO kv (key, value) VALUES ('state', $1::jsonb) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
          [JSON.stringify(state)]
        );
      } else {
        fs.mkdirSync(DATA_DIR, { recursive: true });
        fs.writeFileSync(DATA_FILE, JSON.stringify(state, null, 2));
      }
    } while (dirty);
  } catch (e) {
    console.error('Error guardando estado:', e.message);
  }
  saving = false;
}
function saveState() { persist(); }

let state = defaultState();

function publicState() {
  return {
    rps: RPS.map((r) => ({
      slug: r.slug,
      name: state.rps[r.slug].name,
      tickets: state.rps[r.slug].tickets,
    })),
    event: state.event,
    log: state.log.slice(-30).slice().reverse(),
  };
}

const ADMIN_TOKEN = process.env.ADMIN_TOKEN || '';
function requireAdmin(req, res, next) {
  if (!ADMIN_TOKEN) return res.status(503).json({ error: 'admin_no_configurado' });
  if (req.headers['x-admin-token'] !== ADMIN_TOKEN) return res.status(401).json({ error: 'no_autorizado' });
  next();
}

const sseClients = [];
function broadcast() {
  const payload = `data: ${JSON.stringify(publicState())}\n\n`;
  sseClients.forEach((res) => { try { res.write(payload); } catch (e) {} });
}

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

app.get('/api/state', (req, res) => {
  res.json(publicState());
});

app.get('/api/stream', (req, res) => {
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    Connection: 'keep-alive',
  });
  res.write(`data: ${JSON.stringify(publicState())}\n\n`);
  sseClients.push(res);
  const keepAlive = setInterval(() => { try { res.write(':ping\n\n'); } catch (e) {} }, 25000);
  req.on('close', () => {
    clearInterval(keepAlive);
    const idx = sseClients.indexOf(res);
    if (idx !== -1) sseClients.splice(idx, 1);
  });
});

app.post('/api/sale', (req, res) => {
  const { slug, delta } = req.body || {};
  if (!RP_SLUGS.has(slug)) return res.status(400).json({ error: 'rp_desconocido' });
  const d = Number(delta);
  if (!Number.isInteger(d) || d === 0) return res.status(400).json({ error: 'delta_invalido' });

  const cur = state.rps[slug].tickets;
  const next = Math.max(0, cur + d);
  const applied = next - cur;
  if (applied !== 0) {
    state.rps[slug].tickets = next;
    state.log.push({
      id: Date.now().toString(36) + Math.random().toString(36).slice(2, 8),
      rpSlug: slug,
      name: state.rps[slug].name,
      qty: applied,
      ts: Date.now(),
    });
    if (state.log.length > MAX_LOG) state.log = state.log.slice(-MAX_LOG);
    saveState();
    broadcast();
  }
  res.json(publicState());
});

app.post('/api/admin/check', requireAdmin, (req, res) => {
  res.json({ ok: true });
});

// Sets an RP's ticket count directly (e.g. to correct a mistake) instead of applying a +/- delta.
app.post('/api/admin/set-tickets', requireAdmin, (req, res) => {
  const { slug, tickets } = req.body || {};
  if (!RP_SLUGS.has(slug)) return res.status(400).json({ error: 'rp_desconocido' });
  const t = Number(tickets);
  if (!Number.isInteger(t) || t < 0) return res.status(400).json({ error: 'cantidad_invalida' });

  const cur = state.rps[slug].tickets;
  const applied = t - cur;
  state.rps[slug].tickets = t;
  if (applied !== 0) {
    state.log.push({
      id: Date.now().toString(36) + Math.random().toString(36).slice(2, 8),
      rpSlug: slug,
      name: state.rps[slug].name,
      qty: applied,
      ts: Date.now(),
    });
    if (state.log.length > MAX_LOG) state.log = state.log.slice(-MAX_LOG);
  }
  saveState();
  broadcast();
  res.json(publicState());
});

app.post('/api/admin/set-external-sales', requireAdmin, (req, res) => {
  const { externalSales } = req.body || {};
  const v = Number(externalSales);
  if (!Number.isInteger(v) || v < 0) return res.status(400).json({ error: 'cantidad_invalida' });
  state.event.externalSales = v;
  saveState();
  broadcast();
  res.json(publicState());
});

app.get('/api/export', (req, res) => {
  res.json(state);
});

loadState().then((loaded) => {
  state = loaded;
  saveState();
  app.listen(PORT, () => {
    console.log(`Omnia Midnight escuchando en el puerto ${PORT} (${pool ? 'base de datos Postgres' : 'archivo local'})`);
  });
}).catch((e) => {
  console.error('No se pudo cargar el estado inicial:', e.message);
  process.exit(1);
});
