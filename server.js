"use strict";
// Celestial Messenger Control — server + bot + watchdog
const express = require("express"), fs = require("fs"), path = require("path");
const { sendBanatReplyWithTyping } = require("./banat-human");
const { pickBanatReply, rememberBanatReply } = require("./banat-memory");

const PORT = process.env.PORT || 3000;
const DATA = process.env.DATA_DIR || path.join(__dirname, "data");
fs.mkdirSync(DATA, { recursive: true });
const ADMIN = process.env.ADMIN_PASSWORD || null;   // optional: kung walang env, walang login
function safeDecode(v) { try { return decodeURIComponent(v || ""); } catch { return String(v || ""); } }
const CFG_FILE = path.join(DATA, "config.json");
const POOL = JSON.parse(fs.readFileSync(path.join(__dirname, "replies.json"), "utf8"));

const cfg = Object.assign({
  appState: null, savedAt: null, autoReply: false, active: [], startMessage: "sige tumakbo ka na HAHAHAHAHA",
  skipRate: 0.3, senderCooldownS: 20, hourlyMax: 30, threadMax: 6, threadWindowS: 60,
  lock: { on: false, name: "", threadId: null }
}, safeRead(CFG_FILE));
const save = () => fs.writeFileSync(CFG_FILE, JSON.stringify(cfg, null, 2));
function safeRead(f) { try { return JSON.parse(fs.readFileSync(f, "utf8")); } catch { return {}; } }

const S = { api: null, uid: null, online: false, logging: false, stopListen: null,
  started: Date.now(), botSince: null, lastConnected: null, lastDisconnected: null,
  attempts: 0, nextRetry: null, timer: null, lastError: null,
  replies: [], suppressed: 0, diag: null, loginAt: 0, senders: new Map(), threads: new Map(), log: [] };

const H = { probeOk: null, probeAt: null, probeFails: 0, pingOk: null, pingAt: null, sendFails: 0 };
const MEM_LIMIT = Number(process.env.MEMORY_LIMIT_MB || 450);
const memMB = () => Math.round(process.memoryUsage().rss / 1048576);
function diagnose(msg) {
  const m = String(msg || "").toLowerCase();
  if (m.includes("retrieving userid")) return { code: "COOKIE_OR_IP_BLOCK", hint: "Expired ang cookie o na-block ng Facebook ang server IP. Kumuha ng bagong cookie sa browser, i-verify ang account, tapos Save & connect." };
  if (m.includes("checkpoint") || m.includes("956") || m.includes("verify")) return { code: "CHECKPOINT", hint: "May checkpoint ang account. Buksan ang Facebook sa browser at tapusin ang verification, tapos bagong cookie." };
  if (m.includes("not logged in") || m.includes("logged out") || m.includes("login approval")) return { code: "LOGGED_OUT", hint: "Na-logout ang session. Mag-login ulit sa browser at i-paste ang bagong cookie." };
  if (m.includes("rate") || m.includes("1545") || m.includes("spam")) return { code: "RATE_LIMIT", hint: "Na-limit ang account. Bawasan ang bilis, i-Stop muna ang ibang chats, at hayaang mag-cooldown." };
  if (m.includes("send failed")) return { code: "SEND_FAIL", hint: "Paulit-ulit na hindi nakaka-send. Auto reconnect ang gagawin; kung tuloy-tuloy, posibleng restricted ang account." };
  if (/econnreset|etimedout|enotfound|eai_again|network|timeout|socket/.test(m)) return { code: "NETWORK", hint: "Network issue sa server. Awtomatikong magre-retry." };
  if (m.includes("library missing") || m.includes("cannot find module")) return { code: "MISSING_MODULE", hint: "Kulang ang package. I-check ang package.json at mag-redeploy." };
  return { code: "UNKNOWN", hint: "Tingnan ang Activity log para sa detalye." };
}
function setDiag(msg) { S.diag = diagnose(msg); log("debug", `${S.diag.code}: ${S.diag.hint}`); }
process.on("uncaughtException", e => { log("error", "uncaught: " + (e && e.message || e)); setDiag(e && e.message); });
process.on("unhandledRejection", e => { log("error", "unhandled: " + (e && e.message || e)); setDiag(e && e.message); });

function log(who, msg) { S.log.unshift({ t: Date.now(), who, msg: String(msg).slice(0, 300) }); S.log.length = Math.min(S.log.length, 80); console.log(`[${who}] ${msg}`); }

function parseCookie(raw) {
  raw = String(raw || "").trim();
  if (raw.startsWith("[")) return JSON.parse(raw);
  return raw.split(";").map(p => p.trim()).filter(Boolean).map(p => {
    const i = p.indexOf("="); return { key: p.slice(0, i), value: p.slice(i + 1), domain: "facebook.com", path: "/", hostOnly: false };
  });
}

// ---------- Messenger connection + watchdog ----------
function connect() {
  if (S.logging || !cfg.appState) return;
  clearTimeout(S.timer); S.nextRetry = null; S.logging = true; S.loginAt = Date.now();
  let login; try { login = require("@dongdev/fca-unofficial"); } catch (e) { S.logging = false; log("error", "fca library missing: " + e.message); return setDiag("library missing"); }
  login({ appState: cfg.appState }, { online: true, selfListen: false, listenEvents: true, autoMarkRead: false }, (err, api) => {
    S.logging = false;
    if (err) { S.lastError = String(err.message || err.error || err); log("error", "login: " + S.lastError); setDiag(S.lastError); return scheduleRetry(); }
    S.api = api; S.uid = String(api.getCurrentUserID()); S.online = true; S.attempts = 0; S.lastConnected = Date.now(); S.lastError = null; S.diag = null; H.probeFails = 0; H.sendFails = 0;
    log("system", "Messenger connected as " + S.uid);
    S.stopListen = api.listenMqtt((e, ev) => {
      if (e) { log("error", "listen: " + (e.message || e)); setDiag(e.message || e); return drop(); }
      onEvent(ev);
    });
  });
}
function drop() {
  try { S.stopListen && S.stopListen(); } catch {}
  S.api = null; S.online = false; S.lastDisconnected = Date.now(); scheduleRetry();
}
function scheduleRetry() {
  if (!cfg.appState || S.manualOff) return;
  S.attempts++;
  let s = Math.min(300, 8 * 2 ** (S.attempts - 1));
  if (S.attempts > 10) { s = 1800; S.attempts = 0; log("watchdog", "Gave up after 10 tries. Will retry in 30 min"); }
  else log("watchdog", `reconnect #${S.attempts} in ${s}s`);
  S.nextRetry = Date.now() + s * 1000; clearTimeout(S.timer); S.timer = setTimeout(connect, s * 1000);
}
function disconnect() { S.manualOff = true; clearTimeout(S.timer); try { S.stopListen && S.stopListen(); } catch {} S.api = null; S.online = false; S.lastDisconnected = Date.now(); S.nextRetry = null; }

// ---------- Bot ----------
const sendRaw = (api, text, tid, cb, replyTo) => api.sendMessage(text, tid, cb, replyTo || undefined);
function allowed(ev) {
  const now = Date.now();
  S.replies = S.replies.filter(t => now - t < 3600e3);
  if (S.replies.length >= cfg.hourlyMax) return false;
  if (now - (S.senders.get(ev.senderID) || 0) < cfg.senderCooldownS * 1000) return false;
  const th = (S.threads.get(ev.threadID) || []).filter(t => now - t < cfg.threadWindowS * 1000);
  S.threads.set(ev.threadID, th);
  return th.length < cfg.threadMax;
}
const sleep = ms => new Promise(r => setTimeout(r, ms));
const rnd = (a, b) => a + Math.random() * (b - a);
function enforceName(tid) {
  if (!S.api || !cfg.lock.on || !cfg.lock.name) return;
  S.api.setTitle(cfg.lock.name, tid, e => log(e ? "error" : "lock", e ? "setTitle: " + (e.message || e) : "GC name restored"));
}
async function setAllNicknames(tid, nick) {
  if (!S.api) throw new Error("Not connected");
  const info = await new Promise((ok, no) => S.api.getThreadInfo(tid, (e, i) => e ? no(e) : ok(i)));
  const ids = info.participantIDs || []; log("nick", `setting "${nick}" for ${ids.length} members`);
  for (const id of ids) {
    await new Promise(r => S.api.changeNickname(nick, tid, id, e => { if (e) log("error", "nick " + id + ": " + (e.message || e)); r(); }));
    await sleep(rnd(2500, 5000));
  }
  log("nick", "done");
}
async function onEvent(ev) {
  if (ev.type === "event" && ev.logMessageType === "log:thread-name" && cfg.lock.on && ev.threadID === cfg.lock.threadId) {
    if (ev.logMessageData && ev.logMessageData.name !== cfg.lock.name) { await sleep(rnd(2000, 5000)); enforceName(ev.threadID); }
    return;
  }
  if (ev.type !== "message" || !ev.body || String(ev.senderID) === S.uid || !S.online) return;
  const target = cfg.autoReply || cfg.active.includes(ev.threadID);
  if (!target) return;
  if (Math.random() < cfg.skipRate) return;
  if (!allowed(ev)) { S.suppressed++; return; }
  const r = pickBanatReply(ev.threadID, POOL, []);
  const text = typeof r === "string" ? r : (r && (r.text || r.reply)) || POOL[Math.floor(Math.random() * POOL.length)];
  const now = Date.now(); S.replies.push(now); S.senders.set(ev.senderID, now); S.threads.get(ev.threadID).push(now);
  const ok = await sendBanatReplyWithTyping(S.api, text, ev.threadID, null, { trafficSendMessage: sendRaw, incomingText: ev.body });
  if (ok) { H.sendFails = 0; rememberBanatReply(ev.threadID, text, ev.senderID); log("bot", `reply -> ${ev.threadID}`); }
  else { H.sendFails++; setDiag("send failed"); if (H.sendFails >= 5) { H.sendFails = 0; log("watchdog", "5 send fails -> reconnect"); drop(); } }
}
function gcStart(tid) {
  if (!S.online) throw new Error("Connect muna");
  if (cfg.active.includes(tid)) return;
  cfg.active.push(tid);
  if (!S.botSince) S.botSince = Date.now(); save();
  const msg = cfg.startMessage || "on na";
  S.api.sendMessage(msg, tid, e => log(e ? "error" : "bot", e ? "start msg: " + (e.message || e) : "ON -> " + tid));
}
function gcStop(tid) {
  cfg.active = cfg.active.filter(x => x !== tid); if (!cfg.active.length) S.botSince = null; save(); log("bot", "OFF -> " + tid);
}

// ---------- HTTP ----------
const app = express(); app.use(express.json({ limit: "1mb" }));
app.get("/", (q, r) => r.sendFile(path.join(__dirname, "index.html")));
app.get("/health", (q, r) => r.json({ ok: true, online: S.online, uptime: (Date.now() - S.started) / 1000 | 0 }));
app.use("/api", (req, res, next) => !ADMIN || safeDecode(req.get("x-admin")).trim() === String(ADMIN).trim() ? next() : res.status(401).json({ error: "unauthorized" }));
const status = () => ({
  online: S.online, running: S.online && cfg.active.length > 0, active: cfg.active, startMessage: cfg.startMessage, uid: S.uid, serverUptime: (Date.now() - S.started) / 1000 | 0,
  botUptime: S.botSince && cfg.active.length ? (Date.now() - S.botSince) / 1000 | 0 : 0, lastConnected: S.lastConnected, lastDisconnected: S.lastDisconnected,
  autoReply: cfg.autoReply, repliesHour: S.replies.length, suppressed: S.suppressed,
  attempts: S.attempts, nextRetry: S.nextRetry, lastError: S.lastError, diag: S.diag,
  health: { probeOk: H.probeOk, probeAt: H.probeAt, pingOk: H.pingOk, pingAt: H.pingAt, sendFails: H.sendFails, memoryMB: memMB(), memLimit: MEM_LIMIT },
  cookieSaved: !!cfg.appState, lock: cfg.lock, savedAt: cfg.savedAt, log: S.log.slice(0, 40)
});
app.get("/api/status", (q, r) => r.json(status()));
app.post("/api/auto", (q, r) => { cfg.autoReply = !!q.body.on; save(); r.json(status()); });
app.post("/api/gcstart", (q, r) => { try { gcStart(String(q.body.threadId)); r.json(status()); } catch (e) { r.status(400).json({ error: e.message }); } });
function parseTarget(v) {
  v = String(v || "").trim();
  const m = v.match(/[?&]id=(\d{5,20})/) || v.match(/^(\d{5,20})$/) || v.match(/(?:facebook|fb|messenger)\.com\/(?:t\/)?(\d{5,20})\b/);
  return m ? m[1] : null;
}
app.post("/api/pm", (q, r) => {
  const id = parseTarget(q.body.target);
  if (!id) return r.status(400).json({ error: "Kailangan ng numeric FB ID o link na profile.php?id=..." });
  try { gcStart(id); r.json(status()); } catch (e) { r.status(400).json({ error: e.message }); }
});
app.post("/api/gcstop", (q, r) => { gcStop(String(q.body.threadId)); r.json(status()); });
app.post("/api/startmsg", (q, r) => { cfg.startMessage = String(q.body.text || "").slice(0, 500); save(); r.json(status()); });
app.post("/api/reconnect", (q, r) => { S.manualOff = false; if (S.online) drop(); S.attempts = 0; clearTimeout(S.timer); connect(); r.json(status()); });
app.post("/api/disconnect", (q, r) => { disconnect(); r.json(status()); });
app.post("/api/session", (q, r) => {
  try { cfg.appState = parseCookie(q.body.cookie); cfg.savedAt = Date.now(); save(); S.manualOff = false; S.attempts = 0; connect(); r.json(status()); }
  catch (e) { r.status(400).json({ error: "Invalid cookie format" }); }
});
app.post("/api/nicknames", (q, r) => {
  const { threadId, nickname } = q.body; if (!threadId || !nickname) return r.status(400).json({ error: "threadId + nickname required" });
  setAllNicknames(threadId, String(nickname).slice(0, 60)).catch(e => log("error", "nicknames: " + (e.message || e))); r.json({ ok: true });
});
app.post("/api/lockname", (q, r) => {
  const { threadId, name, on } = q.body; cfg.lock = { on: !!on && !!name && !!threadId, name: String(name || "").slice(0, 100), threadId }; save();
  if (cfg.lock.on) enforceName(threadId); log("lock", cfg.lock.on ? "name lock ON" : "name lock OFF"); r.json(status());
});
app.get("/api/threads", (q, r) => {
  if (!S.api) return r.json([]);
  S.api.getThreadList(100, null, ["INBOX"], (e, l) => r.json(e ? [] : l.map(t => ({
    id: String(t.threadID), group: !!t.isGroup,
    name: t.name || (t.isGroup ? "Unnamed group" : "Private chat " + String(t.threadID).slice(-4)),
    members: (t.participantIDs || t.participants || []).length
  }))));
});
setInterval(() => { if (S.api && cfg.lock.on) S.api.getThreadInfo(cfg.lock.threadId, (e, i) => { if (!e && i && i.threadName !== cfg.lock.name) enforceName(cfg.lock.threadId); }); }, 300000);

// ---------- Watchdog / auto ping / auto debug ----------
function probe() {
  if (!S.api) return;
  let done = false;
  const fail = m => { H.probeOk = false; H.probeAt = Date.now(); H.probeFails++; setDiag(m); log("watchdog", `probe failed (${H.probeFails}/3): ${m}`);
    if (H.probeFails >= 3) { H.probeFails = 0; log("watchdog", "probe failed 3x -> reconnect"); drop(); } };
  const t = setTimeout(() => { if (!done) { done = true; fail("probe timeout"); } }, 25000);
  try {
    S.api.getThreadList(1, null, ["INBOX"], e => { if (done) return; done = true; clearTimeout(t);
      if (e) fail(e.message || e); else { H.probeOk = true; H.probeFails = 0; H.probeAt = Date.now(); } });
  } catch (e) { done = true; clearTimeout(t); fail(e.message); }
}
setInterval(probe, 3 * 60 * 1000);
setInterval(() => {
  if (S.logging && Date.now() - S.loginAt > 90000) { S.logging = false; log("watchdog", "login stuck -> retry"); setDiag("timeout"); scheduleRetry(); }
  if (memMB() > MEM_LIMIT) { log("watchdog", `memory ${memMB()}MB > ${MEM_LIMIT}MB -> restart`); setTimeout(() => process.exit(1), 500); }
}, 60000);
async function selfPing() {
  const urls = [`http://127.0.0.1:${PORT}/health`];
  if (process.env.RAILWAY_PUBLIC_DOMAIN) urls.push(`https://${process.env.RAILWAY_PUBLIC_DOMAIN}/health`);
  for (const u of urls) {
    try { const r = await fetch(u, { signal: AbortSignal.timeout(10000) }); H.pingOk = r.ok; }
    catch (e) { H.pingOk = false; log("ping", "failed " + u + ": " + e.message); }
  }
  H.pingAt = Date.now();
}
setInterval(selfPing, 4 * 60 * 1000);
app.listen(PORT, () => { log("system", "server on " + PORT); if (cfg.appState) connect(); setTimeout(selfPing, 15000); });
