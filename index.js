/**
 * Vyavahr WhatsApp gateway: bills, payment reminders and CA document requests from each shop's (or CA firm's) own
 * WhatsApp number.
 *
 *   Vyavahr (Vercel + Supabase) ──writes──▶ wa.outbox ◀──claims one at a time per number── this gateway ──▶ WhatsApp
 *   Settings > WhatsApp (shop) / Firm (CA) ──writes a command into wa.sessions──▶ this gateway ──QR / pairing code──▶ wa.sessions
 *
 * - Baileys (WhatsApp Web protocol, no browser). Each linked number is one socket, about 40-80 MB of memory, so a
 *   free 512 MB instance holds roughly 5 numbers; MAX_SESSIONS caps it (the rest wait, oldest first) until the
 *   instance is made bigger.
 * - Each number's WhatsApp login lives in Postgres (wa.auth, keyed by the shop or firm), so restarts and deploys
 *   never need a new scan.
 * - One message at a time per number, with a randomised gap and a "typing" moment; the number is checked on WhatsApp
 *   first. Outbox rows are claimed with SKIP LOCKED.
 * - A Postgres advisory lock keeps exactly one copy active (two copies would knock each other's sessions off).
 *
 * Env: DATABASE_URL (Supabase SESSION pooler, port 5432), PORT (Render sets it), MAX_SESSIONS (default 5), GAP_MS.
 * HTTP: GET /health only (pg_cron pings it every 10 minutes so Render's free instance never sleeps).
 */
const http = require("node:http");
const fs = require("node:fs");
const path = require("node:path");
try {
  for (const line of fs.readFileSync(path.join(__dirname, ".env"), "utf8").split(/\r?\n/)) {
    const m = line.match(/^\s*([A-Z0-9_]+)\s*=\s*(.*?)\s*$/);
    if (m && !(m[1] in process.env)) process.env[m[1]] = m[2].replace(/^["']|["']$/g, "");
  }
} catch {}
const { Pool } = require("pg");
const pino = require("pino");
const QRCode = require("qrcode");
const {
  default: makeWASocket,
  BufferJSON,
  Browsers,
  DisconnectReason,
  fetchLatestBaileysVersion,
  initAuthCreds,
  makeCacheableSignalKeyStore,
  proto,
} = require("baileys");

const VERSION = "1.0.0";
const LOCK_ID = 727272045;
const GAP_MS = Math.max(2000, Number(process.env.GAP_MS) || 6000);
const MAX_SESSIONS = Math.max(1, Number(process.env.MAX_SESSIONS) || 5);
const LINK_WINDOW_MS = 5 * 60_000;
const logger = pino({ level: process.env.LOG_LEVEL || "warn" });
const log = (...a) => console.log(new Date().toISOString(), ...a);
const short = (id) => String(id).slice(0, 8);

if (!process.env.DATABASE_URL) {
  console.error("DATABASE_URL is not set");
  process.exit(1);
}
const pool = new Pool({
  connectionString: process.env.DATABASE_URL.replace(/[?&]sslmode=[^&]*/g, ""),
  ssl: /localhost|127\.0\.0\.1/.test(process.env.DATABASE_URL) ? undefined : { rejectUnauthorized: false },
  max: 6,
  idleTimeoutMillis: 30_000,
});
pool.on("error", (e) => log("[pg] pool error", e.message));
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function setState(owner, patch) {
  const keys = Object.keys(patch);
  if (!keys.length) return;
  const cols = keys.map((k, i) => `${k} = $${i + 2}`).join(", ");
  await pool.query(`update wa.sessions set ${cols}, heartbeat_at = now(), updated_at = now() where owner_id = $1`, [owner, ...keys.map((k) => patch[k])])
    .catch((e) => log("[state]", short(owner), e.message));
}

/* ---------------- one number's WhatsApp login in Postgres ---------------- */

async function pgAuth(owner) {
  const read = async (key) => {
    const { rows } = await pool.query(`select value from wa.auth where owner_id = $1 and key = $2`, [owner, key]);
    return rows[0] ? JSON.parse(rows[0].value, BufferJSON.reviver) : null;
  };
  const creds = (await read("creds")) || initAuthCreds();
  return {
    state: {
      creds,
      keys: {
        get: async (type, ids) => {
          const out = {};
          if (!ids.length) return out;
          const { rows } = await pool.query(`select key, value from wa.auth where owner_id = $1 and key = any($2)`, [owner, ids.map((id) => `${type}-${id}`)]);
          for (const r of rows) {
            let v = JSON.parse(r.value, BufferJSON.reviver);
            if (type === "app-state-sync-key" && v) v = proto.Message.AppStateSyncKeyData.fromObject(v);
            out[r.key.slice(type.length + 1)] = v;
          }
          return out;
        },
        set: async (data) => {
          const client = await pool.connect();
          try {
            await client.query("begin");
            for (const type of Object.keys(data)) {
              for (const id of Object.keys(data[type])) {
                const key = `${type}-${id}`;
                const value = data[type][id];
                if (value) await client.query(`insert into wa.auth (owner_id, key, value) values ($1, $2, $3) on conflict (owner_id, key) do update set value = excluded.value`, [owner, key, JSON.stringify(value, BufferJSON.replacer)]);
                else await client.query(`delete from wa.auth where owner_id = $1 and key = $2`, [owner, key]);
              }
            }
            await client.query("commit");
          } catch (e) {
            await client.query("rollback").catch(() => {});
            throw e;
          } finally {
            client.release();
          }
        },
      },
    },
    saveCreds: () => pool.query(`insert into wa.auth (owner_id, key, value) values ($1, 'creds', $2) on conflict (owner_id, key) do update set value = excluded.value`, [owner, JSON.stringify(creds, BufferJSON.replacer)]),
  };
}

async function registered(owner) {
  const { rows } = await pool.query(`select value from wa.auth where owner_id = $1 and key = 'creds'`, [owner]);
  if (!rows[0]) return false;
  try { return Boolean(JSON.parse(rows[0].value, BufferJSON.reviver).registered); } catch { return false; }
}

/* ---------------- sessions: one socket per linked number ---------------- */

const S = new Map(); // owner -> { sock, connected, starting, linkUntil, pairPhone, pairRequested, retry, busyUntil }
let baileysVersion;

function slot(owner) {
  if (!S.has(owner)) S.set(owner, { sock: null, connected: false, starting: false, linkUntil: 0, pairPhone: null, pairRequested: false, retry: 0, nextSendAt: 0 });
  return S.get(owner);
}
const liveCount = () => [...S.values()].filter((x) => x.sock).length;

async function start(owner) {
  const x = slot(owner);
  if (x.starting || x.sock) return;
  if (liveCount() >= MAX_SESSIONS) {
    await setState(owner, { error: "The WhatsApp service is full right now. Your number links as soon as there is room." });
    return;
  }
  x.starting = true;
  try {
    const isReg = await registered(owner);
    if (!isReg && Date.now() > x.linkUntil) {
      await setState(owner, { status: "not_linked", qr_svg: null, pairing_code: null, me: null });
      return;
    }
    const { state, saveCreds } = await pgAuth(owner);
    if (!baileysVersion) { try { baileysVersion = (await fetchLatestBaileysVersion()).version; } catch {} }
    await setState(owner, { status: "connecting", error: null });
    log("[conn]", short(owner), isReg ? "connecting (linked)" : "connecting (linking)");
    const s = makeWASocket({
      version: baileysVersion,
      auth: { creds: state.creds, keys: makeCacheableSignalKeyStore(state.keys, logger) },
      logger,
      browser: Browsers.ubuntu("Vyavahr"),
      markOnlineOnConnect: false,
      syncFullHistory: false,
      shouldSyncHistoryMessage: () => false,
      generateHighQualityLinkPreview: false,
    });
    x.sock = s;
    x.pairRequested = false;
    s.ev.on("creds.update", saveCreds);
    s.ev.on("connection.update", (u) => onConnection(owner, s, u).catch((e) => log("[conn]", short(owner), e.message)));
  } finally {
    x.starting = false;
  }
}

async function onConnection(owner, s, u) {
  const x = slot(owner);
  if (s !== x.sock) return;
  if (u.qr) {
    if (x.pairPhone && !x.pairRequested) {
      x.pairRequested = true;
      try {
        const code = await s.requestPairingCode(x.pairPhone);
        await setState(owner, { status: "qr", pairing_code: code, qr_svg: await QRCode.toString(u.qr, { type: "svg", margin: 1 }) });
        return;
      } catch (e) {
        await setState(owner, { error: `Pairing code failed: ${e.message}` });
      }
    }
    await setState(owner, { status: "qr", qr_svg: await QRCode.toString(u.qr, { type: "svg", margin: 1 }) });
  }
  if (u.connection === "open") {
    x.connected = true;
    x.retry = 0;
    x.linkUntil = 0;
    x.pairPhone = null;
    const me = String(s.user?.id ?? "").split(":")[0].split("@")[0];
    await setState(owner, { status: "connected", me, qr_svg: null, pairing_code: null, error: null, connected_at: new Date() });
    log("[conn]", short(owner), "connected");
  }
  if (u.connection === "close") {
    x.connected = false;
    x.sock = null;
    const code = u.lastDisconnect?.error?.output?.statusCode;
    log("[conn]", short(owner), "closed", code);
    if (code === DisconnectReason.loggedOut) {
      await pool.query(`delete from wa.auth where owner_id = $1`, [owner]);
      await setState(owner, { status: "logged_out", me: null, qr_svg: null, pairing_code: null, error: "WhatsApp was unlinked from the phone. Link it again." });
      return;
    }
    if (code === DisconnectReason.connectionReplaced) {
      await setState(owner, { status: "offline", error: "This number was opened in another WhatsApp Web session." });
      await sleep(60_000);
    }
    x.retry = Math.min(x.retry + 1, 8);
    setTimeout(() => start(owner).catch((e) => log("[start]", e.message)), Math.min(60_000, 2000 * 2 ** x.retry));
  }
}

async function stop(owner) {
  const x = slot(owner);
  try { x.sock?.end(undefined); } catch {}
  x.sock = null;
  x.connected = false;
}

/* ---------------- commands from the app (wa.sessions.command) ---------------- */

async function commands() {
  for (;;) {
    try {
      const { rows } = await pool.query(`
        with c as (select owner_id, command, command_arg from wa.sessions where command is not null for update skip locked)
        update wa.sessions s set command = null, command_arg = null from c where s.owner_id = c.owner_id
        returning c.owner_id, c.command, c.command_arg`);
      for (const c of rows) {
        const x = slot(c.owner_id);
        log("[cmd]", short(c.owner_id), c.command);
        if (c.command === "link" || c.command === "pair") {
          x.linkUntil = Date.now() + LINK_WINDOW_MS;
          x.pairPhone = c.command === "pair" ? String(c.command_arg || "").replace(/\D/g, "") || null : null;
          if (x.sock && !x.connected) await stop(c.owner_id);
          await setState(c.owner_id, { pairing_code: null, qr_svg: null, error: null });
          await start(c.owner_id);
        } else if (c.command === "logout") {
          if (x.sock) await x.sock.logout().catch(() => {});
          await stop(c.owner_id);
          await pool.query(`delete from wa.auth where owner_id = $1`, [c.owner_id]);
          await setState(c.owner_id, { status: "not_linked", me: null, qr_svg: null, pairing_code: null, error: null });
        }
      }
      // linking windows that ran out without a scan stop making QRs
      for (const [owner, x] of S) {
        if (!x.connected && x.sock && x.linkUntil && Date.now() > x.linkUntil && !(await registered(owner))) {
          await stop(owner);
          x.linkUntil = 0;
          await setState(owner, { status: "not_linked", qr_svg: null, pairing_code: null });
        }
      }
    } catch (e) {
      log("[cmd]", e.message);
    }
    await sleep(3000);
  }
}

// every linked number gets a socket (oldest links first, up to MAX_SESSIONS)
async function reconcile() {
  for (;;) {
    try {
      const { rows } = await pool.query(`
        select s.owner_id from wa.sessions s
        where exists (select 1 from wa.auth a where a.owner_id = s.owner_id and a.key = 'creds')
        order by s.connected_at nulls last, s.owner_id`);
      for (const r of rows) {
        const x = slot(r.owner_id);
        if (!x.sock && !x.starting && (await registered(r.owner_id))) await start(r.owner_id);
      }
      await pool.query(`update wa.gateway set heartbeat_at = now(), sessions = $1, version = $2 where id = 1`, [liveCount(), VERSION]);
    } catch (e) {
      log("[reconcile]", e.message);
    }
    await sleep(30_000);
  }
}

/* ---------------- outbox: one message at a time per number ---------------- */

async function claim(owner) {
  const { rows } = await pool.query(`
    update wa.outbox set status = 'sending', attempts = attempts + 1
    where id = (
      select id from wa.outbox
      where owner_id = $1 and ((status = 'queued' and not_before <= now())
         or (status = 'sending' and created_at < now() - interval '10 minutes' and attempts < 3))
      order by not_before, created_at
      limit 1
      for update skip locked
    )
    returning id, "to", body, kind, attempts`, [owner]);
  return rows[0] || null;
}

async function sendOne(sock, m) {
  const digits = String(m.to).replace(/\D/g, "");
  const number = digits.length === 10 ? `91${digits}` : digits;
  const [hit] = await sock.onWhatsApp(`${number}@s.whatsapp.net`);
  if (!hit?.exists) throw Object.assign(new Error("This number is not on WhatsApp."), { permanent: true });
  const jid = hit.jid;
  await sock.presenceSubscribe(jid).catch(() => {});
  await sock.sendPresenceUpdate("composing", jid).catch(() => {});
  await sleep(Math.min(3000, 800 + m.body.length * 10));
  await sock.sendPresenceUpdate("paused", jid).catch(() => {});
  const res = await sock.sendMessage(jid, { text: m.body });
  return res?.key?.id ?? null;
}

async function work() {
  for (;;) {
    let did = false;
    try {
      for (const [owner, x] of S) {
        if (!x.connected || !x.sock || Date.now() < x.nextSendAt) continue;
        const m = await claim(owner);
        if (!m) continue;
        did = true;
        try {
          const waId = await sendOne(x.sock, m);
          await pool.query(`update wa.outbox set status = 'sent', sent_at = now(), wa_id = $2, error = null where id = $1`, [m.id, waId]);
        } catch (e) {
          const permanent = e.permanent || m.attempts >= 3;
          await pool.query(
            permanent
              ? `update wa.outbox set status = 'failed', error = $2 where id = $1`
              : `update wa.outbox set status = 'queued', error = $2, not_before = now() + interval '2 minutes' where id = $1`,
            [m.id, String(e.message || e).slice(0, 500)],
          );
          log("[send]", short(owner), "failed", m.id, e.message);
        }
        // a person's pace for this number: gap .. 1.6 x gap
        x.nextSendAt = Date.now() + GAP_MS * (1 + Math.random() * 0.6);
      }
    } catch (e) {
      log("[worker]", e.message);
    }
    await sleep(did ? 500 : 3000);
  }
}

/* ---------------- keep-awake, single copy, HTTP ---------------- */

async function registerKeepalive() {
  const url = (process.env.RENDER_EXTERNAL_URL || "").replace(/\/$/, "");
  if (!/^https:\/\/[a-z0-9.-]+$/i.test(url)) return;
  try {
    await pool.query("select cron.schedule('vyavahr-wa-gateway-keepalive', '*/10 * * * *', $1)", [`select net.http_get('${url}/health')`]);
    log("[keepalive] pinging", url);
  } catch (e) {
    log("[keepalive] could not register:", e.message);
  }
}

let lockClient = null;
async function acquireLock() {
  for (;;) {
    try {
      lockClient = await pool.connect();
      const { rows } = await lockClient.query("select pg_try_advisory_lock($1) as ok", [LOCK_ID]);
      if (rows[0].ok) return;
      lockClient.release();
      lockClient = null;
      log("[lock] another gateway is running; standing by");
    } catch (e) {
      log("[lock]", e.message);
      lockClient?.release();
      lockClient = null;
    }
    await sleep(15_000);
  }
}

const server = http.createServer((req, res) => {
  if (req.url === "/health" || req.url === "/") {
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({ ok: true, active: Boolean(lockClient), sessions: liveCount(), max: MAX_SESSIONS, version: VERSION }));
    return;
  }
  res.writeHead(404).end();
});
server.listen(Number(process.env.PORT) || 3000, () => log("[http] listening on", Number(process.env.PORT) || 3000));

(async () => {
  await acquireLock();
  log("[lock] acquired; this copy is active, max sessions", MAX_SESSIONS);
  await registerKeepalive();
  reconcile();
  commands();
  work();
})();

async function shutdown() {
  log("[exit] shutting down");
  try {
    for (const [owner] of S) { await stop(owner); await setState(owner, { status: "offline" }); }
    await lockClient?.query("select pg_advisory_unlock($1)", [LOCK_ID]);
    lockClient?.release();
    await pool.end();
  } catch {}
  process.exit(0);
}
process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
