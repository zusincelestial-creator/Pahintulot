"use strict";
// Celestial Messenger Control — server + bot + watchdog
const express = require("express"), fs = require("fs"), path = require("path");
const { sendBanatReplyWithTyping } = require("./banat-human");
const { pickBanatReply, rememberBanatReply } = require("./banat-memory");

const PORT = process.env.PORT || 3000;
const DATA = process.env.DATA_DIR || path.join(__dirname, "data");
fs.mkdirSync(DATA, { recursive: true });
const ADMIN_FILE = path.join(DATA, "admin.json");
let ADMIN = process.env.ADMIN_PASSWORD || (safeRead(ADMIN_FILE).password || null);   // env optional; else set on first visit
const CFG_FILE = path.join(DATA, "config.json");
const POOL = JSON.parse(fs.readFileSync(path.join(__dirname, "replies.json"), "utf8"));

const cfg = Object.assign({
  appState: null, savedAt: null, autoReply: false, threadId: null,
  skipRate: 0.3, senderCooldownS: 20, hourlyMax: 30, threadMax: 6, threadWindowS: 60,
  lock: { on: false, name: "", threadId: null }, openers: ["gm mga ka-GC 👋", "uy andito na ako", "kamusta kayo"]
}, safeRead(CFG_FILE));
const save = () => fs.writeFileSync(CFG_FILE, JSON.stringify(cfg, null, 2));
function safeRead(f) { try { return JSON.parse(fs.readFileSync(f, "utf8")); } catch { return {}; } }

const S = { api: null, uid: null, online: false, running: false, logging: false, stopListen: null,
  started: Date.now(), botSince: null, lastConnected: null, lastDisconnected: null,
  attempts: 0, nextRetry: null, timer: null, lastError: null,
  replies: [], suppressed: 0, senders: new Map(), threads: new Map(), log: [] };

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
  clearTimeout(S.timer); S.nextRetry = null; S.logging = true;
  let login; try { login = require("@dongdev/fca-unofficial"); } catch (e) { S.logging = false; return log("error", "fca library missing: " + e.message); }
  login({ appState: cfg.appState }, { online: true, selfListen: false, listenEvents: true, autoMarkRead: false }, (err, api) => {
    S.logging = false;
    if (err) { S.lastError = String(err.message || err.error || err); log("error", "login: " + S.lastError); return scheduleRetry(); }
    S.api = api; S.uid = String(api.getCurrentUserID()); S.online = true; S.attempts = 0; S.lastConnected = Date.now(); S.lastError = null;
    log("system", "Messenger connected as " + S.uid);
    S.stopListen = api.listenMqtt((e, ev) => {
      if (e) { log("error", "listen: " + (e.message || e)); return drop(); }
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
function disconnect() { S.manualOff = true; clearTimeout(S.timer); try { S.stopListen && S.stopListen(); } catch {} S.api = null; S.online = false; S.running = false; S.lastDisconnected = Date.now(); S.nextRetry = null; }

// ---------- Bot ----------
const sendRaw = (api, text, tid, cb, replyTo) => api.sendMessage(text, tid, cb, replyTo || undefined);
function allowed(ev) {                         // anti-spam gate
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
  for (const id of ids) {                        // slow + randomized to stay account-safe
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
  if (ev.type !== "message" || !ev.body || String(ev.senderID) === S.uid || !S.running) return;
  const target = cfg.autoReply || (cfg.threadId && ev.threadID === cfg.threadId);
  if (!target) return;
  if (Math.random() < cfg.skipRate) return;                     // random skip
  if (!allowed(ev)) { S.suppressed++; return; }
  const r = pickBanatReply(ev.threadID, POOL, []);
  const text = typeof r === "string" ? r : (r && (r.text || r.reply)) || POOL[Math.floor(Math.random() * POOL.length)];
  const now = Date.now(); S.replies.push(now); S.senders.set(ev.senderID, now); S.threads.get(ev.threadID).push(now);
  const ok = await sendBanatReplyWithTyping(S.api, text, ev.threadID, null, { trafficSendMessage: sendRaw, incomingText: ev.body });
  if (ok) { rememberBanatReply(ev.threadID, text, ev.senderID); log("bot", `reply -> ${ev.threadID}`); }
}
async function startBot() {
  if (!S.online) return log("error", "Connect first");
  S.running = true; S.botSince = Date.now(); log("bot", "started");
  if (cfg.threadId) {
    const o = cfg.openers[Math.floor(Math.random() * cfg.openers.length)];
    sendBanatReplyWithTyping(S.api, o, cfg.threadId, null, { trafficSendMessage: sendRaw, incomingText: "" });
  }
}

// ---------- HTTP ----------
const app = express(); app.use(express.json({ limit: "1mb" }));
app.get("/", (q, r) => r.sendFile(path.join(__dirname, "index.html")));  // only index.html is public
app.post("/api/setup", (q, r) => {                      // first-run password (only works once)
  const pw = String(q.body.password || ""); if (ADMIN || pw.length < 6) return r.status(400).json({ error: ADMIN ? "Already set" : "Min 6 characters" });
  ADMIN = pw; fs.writeFileSync(ADMIN_FILE, JSON.stringify({ password: pw })); r.json({ ok: true });
});
app.get("/api/needsetup", (q, r) => r.json({ setup: !ADMIN }));
app.use("/api", (req, res, next) => ADMIN && req.get("x-admin") === ADMIN ? next() : res.status(401).json({ error: "unauthorized" }));
const status = () => ({
  online: S.online, running: S.running, uid: S.uid, serverUptime: (Date.now() - S.started) / 1000 | 0,
  botUptime: S.running ? (Date.now() - S.botSince) / 1000 | 0 : 0, lastConnected: S.lastConnected, lastDisconnected: S.lastDisconnected,
  autoReply: cfg.autoReply, threadId: cfg.threadId, repliesHour: S.replies.length, suppressed: S.suppressed,
  attempts: S.attempts, nextRetry: S.nextRetry, lastError: S.lastError, cookieSaved: !!cfg.appState, lock: cfg.lock, savedAt: cfg.savedAt, log: S.log.slice(0, 40)
});
app.get("/api/status", (q, r) => r.json(status()));
app.post("/api/auto", (q, r) => { cfg.autoReply = !!q.body.on; save(); r.json(status()); });
app.post("/api/thread", (q, r) => { cfg.threadId = q.body.threadId || null; save(); r.json(status()); });
app.post("/api/start", async (q, r) => { await startBot(); r.json(status()); });
app.post("/api/stop", (q, r) => { S.running = false; log("bot", "stopped"); r.json(status()); });
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
  S.api.getThreadList(50, null, ["INBOX"], (e, l) => r.json(e ? [] : l.filter(t => t.isGroup).map(t => ({ id: t.threadID, name: t.name || t.threadID }))));
});
setInterval(() => { if (S.api && cfg.lock.on) S.api.getThreadInfo(cfg.lock.threadId, (e, i) => { if (!e && i && i.threadName !== cfg.lock.name) enforceName(cfg.lock.threadId); }); }, 300000);
app.listen(PORT, () => { log("system", "server on " + PORT); if (cfg.appState) connect(); });
