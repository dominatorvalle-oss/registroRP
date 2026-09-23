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

function loadState() {
  try {
    const raw = fs.readFileSync(DATA_FILE, 'utf8');
    const parsed = JSON.parse(raw);
    // fill in any RP that might be missing from an older save
    RPS.forEach((r) => {
      if (!parsed.rps[r.slug]) parsed.rps[r.slug] = { name: r.name, tickets: 0 };
    });
    if (!parsed.event) parsed.event = { externalSales: 0 };
    if (!Array.isArray(parsed.log)) parsed.log = [];
    return parsed;
  } catch (e) {
    return defaultState();
  }
}

function saveState() {
  fs.mkdirSync(DATA_DIR, { recursive: true });
  fs.writeFileSync(DATA_FILE, JSON.stringify(state, null, 2));
}

let state = loadState();
saveState();

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

// Reserved for a future admin view: sets an RP's ticket count directly
// (e.g. to correct a mistake) instead of applying a +/- delta.
app.post('/api/admin/set-tickets', (req, res) => {
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

app.get('/api/export', (req, res) => {
  res.json(state);
});

app.listen(PORT, () => {
  console.log(`Omnia Midnight escuchando en el puerto ${PORT}`);
});
