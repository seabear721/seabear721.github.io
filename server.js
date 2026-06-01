const path = require('path');
const crypto = require('crypto');
const express = require('express');
const { io } = require('socket.io-client');

const PORT = process.env.PORT || 4000;
const ROUND_MS = 45_000;
const ROOM_TTL_AFTER_FINISH_MS = 5 * 60_000;
const ROOM_IDLE_MS = 30 * 60_000;
const ROOM_SWEEP_MS = 60_000;
const MAX_PLAYERS_PER_ROOM = 32;
const REMOTE_FETCH_TIMEOUT_MS = 10_000;
const BOT_NAMES = ['Byte', 'Nova', 'Echo', 'Pixel', 'Dash', 'Orbit', 'Miso', 'Kite'];
const GAME_API_URL =
  process.env.GAME_API_URL ||
  'https://buildyourstax.com/wp-json/dev-api/v1/get-game';
const SOCKET_URL =
  process.env.SOCKET_URL || 'https://stax-socket-production.herokuapp.com';
const PLAYER_ID_CHARS =
  '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz~';

const app = express();
const rooms = new Map();
const remoteSessions = new Map();
const autoReadyGameCodes = new Set();
const debugLogs = [];
const debugSettings = {
  socketEvents: false,
  joinAck: false,
  outbound: false,
  payloads: true,
};
const MAX_DEBUG_LOGS = 500;

const AUTO_READY_EVENTS = new Set([
  'game-started',
  'paused-game',
  'player-unreadied',
]);
const AUTO_READY_EVENT_PATTERN = /pause|phase|round[-_]?end|break|intermission|countdown|between[-_]?rounds/i;

// Render (and most hosts) put a proxy in front of us; trust one hop so req.ip
// and rate-limit keys reflect the real client instead of the proxy address.
app.set('trust proxy', 1);

// Cheap junk-traffic ceiling on the whole API surface. Loopback is exempt so the
// locally served control panel is never throttled. Headroom is generous because
// the panel itself polls /sessions + /debug (~70/min per open browser), so a few
// devices behind one NAT IP must comfortably fit. rateLimit() is hoisted below.
app.use('/api', rateLimit({ windowMs: 60_000, max: 300 }));

app.use(express.json({ limit: '64kb' }));
app.use(express.static(path.join(__dirname, 'public')));

function makeId(prefix) {
  return `${prefix}_${crypto.randomBytes(6).toString('hex')}`;
}

function randomRemotePlayerId() {
  const bytes = crypto.randomBytes(21);
  let id = '';
  for (let i = 0; i < 21; i += 1) {
    id += PLAYER_ID_CHARS[bytes[i] % PLAYER_ID_CHARS.length];
  }
  return id;
}

function cleanGameCode(code) {
  return String(code || '').trim().toUpperCase();
}

async function fetchRemoteGame(code) {
  const base = GAME_API_URL.replace(/\/$/, '');
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), REMOTE_FETCH_TIMEOUT_MS);
  let response;
  try {
    response = await fetch(`${base}/${encodeURIComponent(code)}`, {
      signal: controller.signal,
    });
  } catch (err) {
    if (err.name === 'AbortError') throw new Error('Game lookup timed out');
    throw new Error(`Game lookup failed: ${err.message}`);
  } finally {
    clearTimeout(timer);
  }
  if (!response.ok) {
    const text = await response.text().catch(() => '');
    throw new Error(`Game not found (${response.status})${text ? `: ${text}` : ''}`);
  }
  return response.json();
}

function connectRemoteSocket() {
  return io(SOCKET_URL, {
    transports: ['websocket'],
    withCredentials: false,
    reconnection: true,
    reconnectionDelay: 500,
    reconnectionAttempts: 5,
  });
}

function waitForSocketConnect(socket, timeoutMs = 15000) {
  return new Promise((resolve, reject) => {
    if (socket.connected) return resolve();
    const timer = setTimeout(() => {
      cleanup();
      reject(new Error('Socket connection timed out'));
    }, timeoutMs);

    const cleanup = () => {
      clearTimeout(timer);
      socket.off('connect', onConnect);
      socket.off('connect_error', onError);
    };
    const onConnect = () => {
      cleanup();
      resolve();
    };
    const onError = (err) => {
      cleanup();
      reject(err instanceof Error ? err : new Error(String(err)));
    };

    socket.on('connect', onConnect);
    socket.on('connect_error', onError);
  });
}

function emitRemoteJoin(socket, gameObj, userObj) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      reject(new Error('Join acknowledgement timed out'));
    }, 15000);

    socket.emit('player-joining', gameObj, userObj, (ack) => {
      clearTimeout(timer);
      if (ack && ack.error) {
        reject(new Error(typeof ack.error === 'string' ? ack.error : JSON.stringify(ack.error)));
        return;
      }
      socket.io.opts.query = {
        game: gameObj.id,
        player: userObj.id,
        isSolo: !!gameObj.solo,
        isHost: false,
      };
      resolve(ack || {});
    });
  });
}

function shouldAutoReadyForEvent(eventName) {
  return AUTO_READY_EVENTS.has(eventName) || AUTO_READY_EVENT_PATTERN.test(eventName);
}

function safeJson(value) {
  const seen = new WeakSet();
  const json = JSON.stringify(value, (key, item) => {
    if (typeof item === 'function') return '[Function]';
    if (typeof item === 'bigint') return item.toString();
    if (item && typeof item === 'object') {
      if (seen.has(item)) return '[Circular]';
      seen.add(item);
    }
    return item;
  });
  return typeof json === 'undefined' ? null : JSON.parse(json);
}

function summarizePayload(value) {
  if (Array.isArray(value)) return { type: 'array', length: value.length };
  if (value && typeof value === 'object') {
    return {
      type: 'object',
      keys: Object.keys(value).slice(0, 30),
    };
  }
  return { type: typeof value, value };
}

function addDebugLog(kind, details = {}) {
  const entry = {
    id: crypto.randomUUID(),
    ts: now(),
    kind,
    ...details,
  };

  debugLogs.push(entry);
  while (debugLogs.length > MAX_DEBUG_LOGS) debugLogs.shift();
  console.log(`[debug:${kind}]`, details.eventName || details.message || '', details);
  return entry;
}

function logSocketEvent(session, eventName, args) {
  if (!debugSettings.socketEvents) return;
  addDebugLog('socket-event', {
    eventName,
    sessionId: session.sessionId,
    gameCode: session.gameCode,
    userId: session.userId,
    name: session.name,
    payload: debugSettings.payloads ? safeJson(args) : args.map(summarizePayload),
  });
}

function logJoinAck(session, ack) {
  if (!debugSettings.joinAck) return;
  addDebugLog('join-ack', {
    eventName: 'player-joining:ack',
    sessionId: session.sessionId,
    gameCode: session.gameCode,
    userId: session.userId,
    name: session.name,
    payload: debugSettings.payloads ? safeJson(ack) : summarizePayload(ack),
  });
}

function logOutbound(session, eventName, payload) {
  if (!debugSettings.outbound) return;
  addDebugLog('outbound', {
    eventName,
    sessionId: session?.sessionId || null,
    gameCode: session?.gameCode || null,
    userId: session?.userId || null,
    name: session?.name || null,
    payload: debugSettings.payloads ? safeJson(payload) : summarizePayload(payload),
  });
}

function collectPlayerIds(value, ids = new Set(), seen = new WeakSet()) {
  if (!value || typeof value !== 'object' || seen.has(value)) return ids;
  seen.add(value);

  if (
    typeof value.id === 'string' &&
    (
      typeof value.socketID === 'string' ||
      typeof value.ready === 'boolean' ||
      typeof value.score === 'number'
    )
  ) {
    ids.add(value.id);
  }

  if (Array.isArray(value)) {
    for (const item of value) collectPlayerIds(item, ids, seen);
    return ids;
  }

  for (const item of Object.values(value)) collectPlayerIds(item, ids, seen);
  return ids;
}

function rememberPlayerIds(session, ...values) {
  for (const value of values) {
    for (const id of collectPlayerIds(value)) session.knownPlayerIds.add(id);
  }
}

// Like collectPlayerIds, but captures the full player shape (name/score/ready)
// so the panel can render a live scoreboard of *every* player in the game, not
// just the ids. Same heuristic for what counts as a "player" object.
function collectPlayers(value, out = new Map(), seen = new WeakSet()) {
  if (!value || typeof value !== 'object' || seen.has(value)) return out;
  seen.add(value);

  if (
    typeof value.id === 'string' &&
    (
      typeof value.socketID === 'string' ||
      typeof value.ready === 'boolean' ||
      typeof value.score === 'number'
    )
  ) {
    const prev = out.get(value.id) || {};
    out.set(value.id, {
      id: value.id,
      name: typeof value.name === 'string' ? value.name : prev.name,
      score: typeof value.score === 'number' ? value.score : prev.score,
      ready: typeof value.ready === 'boolean' ? value.ready : prev.ready,
    });
  }

  if (Array.isArray(value)) {
    for (const item of value) collectPlayers(item, out, seen);
    return out;
  }

  for (const item of Object.values(value)) collectPlayers(item, out, seen);
  return out;
}

// Merge any players seen in these payloads into the session's roster, keeping the
// most recently observed name/score/ready for each id. Drives the scoreboard.
function rememberPlayers(session, ...values) {
  if (!session.roster) session.roster = new Map();
  const ts = now();
  for (const value of values) {
    for (const p of collectPlayers(value).values()) {
      const prev = session.roster.get(p.id) || {};
      session.roster.set(p.id, {
        id: p.id,
        name: typeof p.name === 'string' ? p.name : (prev.name ?? null),
        score: typeof p.score === 'number' ? p.score : (prev.score ?? null),
        ready: typeof p.ready === 'boolean' ? p.ready : (prev.ready ?? null),
        updatedAt: ts,
      });
    }
  }
}

function readyAllPlayers(gameCode) {
  const normalized = cleanGameCode(gameCode);
  const playerIds = new Set();
  const sessions = [];

  for (const session of remoteSessions.values()) {
    if (session.gameCode !== normalized || !session.socket.connected) continue;
    for (const id of session.knownPlayerIds) playerIds.add(id);
    sessions.push(session);
  }

  if (!sessions.length || !playerIds.size) return 0;

  const primarySession = sessions[0];
  for (const id of playerIds) {
    const payload = { id };
    logOutbound(primarySession, 'player-ready', payload);
    primarySession.socket.emit('player-ready', payload);
  }
  return playerIds.size;
}

function cleanupRemoteSession(session, reason) {
  if (!remoteSessions.has(session.sessionId)) return;
  remoteSessions.delete(session.sessionId);
  if (debugSettings.socketEvents) {
    addDebugLog('socket-event', {
      eventName: 'disconnect',
      sessionId: session.sessionId,
      gameCode: session.gameCode,
      userId: session.userId,
      name: session.name,
      payload: { socketId: session.socket.id, reason },
    });
  }
  if (![...remoteSessions.values()].some((s) => s.gameCode === session.gameCode)) {
    autoReadyGameCodes.delete(session.gameCode);
  }
}

async function rejoinRemoteSession(session) {
  // The transport reconnected, but the game backend only registers a player via
  // the application-level player-joining handshake — replay it so we don't end
  // up as a "ghost": connected socket, but dropped from the room server-side.
  if (!remoteSessions.has(session.sessionId)) return;
  const { socket, gameObj, userObj } = session;
  if (!gameObj || !userObj || !socket.connected) return;

  userObj.socketID = socket.id;
  logOutbound(session, 'player-joining', { reason: 'reconnect', gameId: gameObj.id, userId: userObj.id });

  try {
    const ack = await emitRemoteJoin(socket, gameObj, userObj);
    if (!remoteSessions.has(session.sessionId)) return; // left/cleaned mid-rejoin
    rememberPlayerIds(session, ack);
    rememberPlayers(session, ack);
    logJoinAck(session, ack);
    addDebugLog('rejoin', {
      sessionId: session.sessionId,
      gameCode: session.gameCode,
      userId: session.userId,
      name: session.name,
    });
    if (autoReadyGameCodes.has(session.gameCode)) {
      setTimeout(() => readyAllPlayers(session.gameCode), 100);
    }
  } catch (err) {
    addDebugLog('rejoin-failed', {
      sessionId: session.sessionId,
      gameCode: session.gameCode,
      userId: session.userId,
      name: session.name,
      message: err?.message || String(err),
    });
  }
}

function attachRemoteSocketHandlers(session) {
  session.socket.onAny((eventName, ...args) => {
    logSocketEvent(session, eventName, args);
    rememberPlayerIds(session, ...args);
    rememberPlayers(session, ...args);
    if (!autoReadyGameCodes.has(session.gameCode)) return;
    if (!shouldAutoReadyForEvent(eventName)) return;
    setTimeout(() => readyAllPlayers(session.gameCode), 100);
  });

  session.socket.on('connect', () => {
    if (debugSettings.socketEvents) {
      addDebugLog('socket-event', {
        eventName: 'connect',
        sessionId: session.sessionId,
        gameCode: session.gameCode,
        userId: session.userId,
        name: session.name,
        payload: { socketId: session.socket.id },
      });
    }
    // This handler is attached after the initial join handshake, so it only
    // ever fires on a *re*connect. Replay player-joining (which re-applies
    // autoready on success) so the player isn't a ghost on the game server.
    rejoinRemoteSession(session);
  });

  session.socket.on('connect_error', (err) => {
    if (!debugSettings.socketEvents) return;
    addDebugLog('socket-event', {
      eventName: 'connect_error',
      sessionId: session.sessionId,
      gameCode: session.gameCode,
      userId: session.userId,
      name: session.name,
      payload: { message: err?.message || String(err) },
    });
  });

  session.socket.on('disconnect', (reason) => {
    // socket.active is true when a reconnect is pending (transient drop:
    // transport close, ping timeout, ...). In that case keep the session so
    // rejoinRemoteSession can replay the handshake once 'reconnect' fires, and
    // let 'reconnect_failed' clean up if the attempts are exhausted. Only a
    // terminal disconnect (server- or client-initiated) releases it here.
    if (session.socket.active) {
      if (debugSettings.socketEvents) {
        addDebugLog('socket-event', {
          eventName: 'disconnect-transient',
          sessionId: session.sessionId,
          gameCode: session.gameCode,
          userId: session.userId,
          name: session.name,
          payload: { reason },
        });
      }
      return;
    }
    cleanupRemoteSession(session, reason);
  });

  session.socket.io.on('reconnect_failed', () => {
    session.socket.disconnect();
    cleanupRemoteSession(session, 'reconnect_failed');
  });
}

function makeCode() {
  const alphabet = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let code = '';
  do {
    code = '';
    for (let i = 0; i < 10; i += 1) {
      code += alphabet[crypto.randomInt(alphabet.length)];
    }
  } while (rooms.has(code));
  return code;
}

function now() {
  return Date.now();
}

// Minimal in-memory fixed-window rate limiter — no extra dependency to install
// on the host. Keyed by client IP (req.ip, real client thanks to trust proxy);
// loopback is exempt so the local panel and health checks are never limited.
function rateLimit({ windowMs, max }) {
  const hits = new Map();
  const sweep = setInterval(() => {
    const ts = now();
    for (const [key, rec] of hits) if (rec.resetAt <= ts) hits.delete(key);
  }, windowMs);
  if (typeof sweep.unref === 'function') sweep.unref();

  return (req, res, next) => {
    const addr = req.socket?.remoteAddress || req.ip;
    if (isLoopbackAddress(addr)) return next();
    const key = req.ip || addr || 'unknown';
    const ts = now();
    let rec = hits.get(key);
    if (!rec || rec.resetAt <= ts) {
      rec = { count: 0, resetAt: ts + windowMs };
      hits.set(key, rec);
    }
    rec.count += 1;
    res.setHeader('X-RateLimit-Limit', max);
    res.setHeader('X-RateLimit-Remaining', Math.max(0, max - rec.count));
    if (rec.count > max) {
      const retry = Math.ceil((rec.resetAt - ts) / 1000);
      res.setHeader('Retry-After', retry);
      return res.status(429).json({ error: `Too many requests, retry in ${retry}s` });
    }
    return next();
  };
}

function publicRoom(room) {
  const remainingMs =
    room.status === 'playing' && room.startedAt
      ? Math.max(0, ROUND_MS - (now() - room.startedAt))
      : room.status === 'finished'
        ? 0
        : ROUND_MS;
  return {
    code: room.code,
    status: room.status,
    createdAt: room.createdAt,
    startedAt: room.startedAt,
    remainingMs,
    players: [...room.players.values()]
      .map(({ id, name, score, ready, isBot, controlled, joinedAt }) => ({ id, name, score, ready, isBot, controlled, joinedAt }))
      .sort((a, b) => b.score - a.score || a.joinedAt - b.joinedAt),
    events: room.events.slice(-80),
  };
}

function addEvent(room, type, message) {
  room.events.push({ id: makeId('evt'), ts: now(), type, message });
  if (room.events.length > 150) room.events.shift();
  room.lastActivity = now();
}

function getRoomOr404(code, res) {
  const room = rooms.get(String(code || '').toUpperCase());
  if (!room) {
    res.status(404).json({ error: 'Room not found' });
    return null;
  }
  return room;
}

function createRoom(hostName = 'Host') {
  const room = {
    code: makeCode(),
    status: 'lobby',
    createdAt: now(),
    startedAt: null,
    finishedAt: null,
    lastActivity: now(),
    players: new Map(),
    events: [],
    timers: new Set(),
  };
  rooms.set(room.code, room);
  const host = addPlayer(room, hostName, false);
  addEvent(room, 'room', `${host.name} created room ${room.code}`);
  return { room, host };
}

function addPlayer(room, name, isBot) {
  const cleanName = String(name || '').trim().slice(0, 24) || (isBot ? 'Bot' : 'Player');
  const player = {
    id: makeId(isBot ? 'bot' : 'player'),
    name: cleanName,
    score: 0,
    ready: true,
    isBot,
    controlled: false,
    joinedAt: now(),
  };
  room.players.set(player.id, player);
  addEvent(room, isBot ? 'bot' : 'join', `${player.name} joined`);
  if (isBot && room.status === 'playing') startBotLoop(room, player);
  return player;
}

function startBotLoop(room, bot) {
  const tick = () => {
    if (room.status !== 'playing' || !room.players.has(bot.id)) return;
    bot.score += crypto.randomInt(1, 5);
    addEvent(room, 'score', `${bot.name} scored`);
  };
  const timer = setInterval(tick, crypto.randomInt(900, 1800));
  room.timers.add(timer);
}

function startRoom(room) {
  if (room.status === 'playing') return;
  room.status = 'playing';
  room.startedAt = now();
  addEvent(room, 'start', 'Game started');

  for (const player of room.players.values()) {
    if (player.isBot) startBotLoop(room, player);
  }

  const finishTimer = setTimeout(() => {
    if (room.status !== 'playing') return;
    room.status = 'finished';
    room.finishedAt = now();
    addEvent(room, 'finish', 'Game finished');
    for (const timer of room.timers) clearInterval(timer);
    room.timers.clear();
  }, ROUND_MS);
  room.timers.add(finishTimer);
}

function destroyRoom(room) {
  for (const timer of room.timers) clearInterval(timer);
  room.timers.clear();
  rooms.delete(room.code);
}

function sweepRooms() {
  const ts = now();
  for (const room of rooms.values()) {
    if (room.status === 'finished' && room.finishedAt && ts - room.finishedAt > ROOM_TTL_AFTER_FINISH_MS) {
      destroyRoom(room);
    } else if (room.status === 'lobby' && ts - room.lastActivity > ROOM_IDLE_MS) {
      destroyRoom(room);
    }
  }
}

const sweepTimer = setInterval(sweepRooms, ROOM_SWEEP_MS);
if (typeof sweepTimer.unref === 'function') sweepTimer.unref();

// Tiny liveness probe for an external uptime monitor (UptimeRobot, cron-job.org,
// a Render Cron Job) to ping every few minutes so the free instance never idles
// into a cold start. Deliberately unauthenticated, un-throttled, and cheap.
app.get('/healthz', (req, res) => {
  res.type('text').send('ok');
});

app.get('/api/health', (req, res) => {
  res.json({ ok: true, rooms: rooms.size, remoteSessions: remoteSessions.size });
});

const DEBUG_TOKEN = process.env.DEBUG_TOKEN || '';
const API_TOKEN = process.env.API_TOKEN || '';

function isLoopbackAddress(addr) {
  if (!addr) return false;
  return addr === '::1' || addr === '::ffff:127.0.0.1' || addr.startsWith('127.');
}

function timingSafeStringEqual(a, b) {
  const ab = Buffer.from(String(a));
  const bb = Buffer.from(String(b));
  if (ab.length !== bb.length) return false;
  return crypto.timingSafeEqual(ab, bb);
}

// The debug endpoints expose full socket payloads (potentially other players'
// data) and let callers toggle logging. Restrict them to loopback, or to a
// caller presenting DEBUG_TOKEN if one is configured (e.g. when proxied).
function requireDebugAccess(req, res, next) {
  const addr = req.socket?.remoteAddress || req.ip;
  if (isLoopbackAddress(addr)) return next();
  const provided = req.get('x-debug-token') || req.get('x-api-token') || req.query?.token || '';
  const ok =
    (DEBUG_TOKEN && timingSafeStringEqual(provided, DEBUG_TOKEN)) ||
    (API_TOKEN && timingSafeStringEqual(provided, API_TOKEN));
  if (ok) return next();
  return res.status(403).json({ error: 'Debug endpoints are restricted to localhost or a valid token' });
}

// Control endpoints (joining/leaving/scoring bots, room management). Open by
// default for back-compat — but if API_TOKEN is set, remote callers must present
// it via the x-api-token header (or ?token=). Loopback (the locally served panel,
// and same-box tooling) is always exempt. A startup warning fires when it's unset.
function requireApiToken(req, res, next) {
  if (!API_TOKEN) return next();
  const addr = req.socket?.remoteAddress || req.ip;
  if (isLoopbackAddress(addr)) return next();
  const provided = req.get('x-api-token') || req.query?.token || '';
  if (timingSafeStringEqual(provided, API_TOKEN)) return next();
  return res.status(401).json({ error: 'Missing or invalid API token' });
}

app.use('/api/stax', requireApiToken);
app.use('/api/rooms', requireApiToken);
app.use('/api/stax/debug', requireDebugAccess);

app.get('/api/stax/debug', (req, res) => {
  const since = Number(req.query?.since || 0);
  res.json({
    ok: true,
    settings: debugSettings,
    logs: debugLogs.filter((entry) => entry.ts > since),
    latestTs: debugLogs.length ? debugLogs[debugLogs.length - 1].ts : since,
  });
});

app.post('/api/stax/debug/settings', (req, res) => {
  const nextSettings = req.body?.settings || {};
  for (const key of Object.keys(debugSettings)) {
    if (typeof nextSettings[key] === 'boolean') debugSettings[key] = nextSettings[key];
  }
  addDebugLog('settings', { message: 'Debug settings updated', settings: { ...debugSettings } });
  res.json({ ok: true, settings: debugSettings });
});

app.post('/api/stax/debug/clear', (req, res) => {
  debugLogs.length = 0;
  res.json({ ok: true });
});

// Join one bot to an already-fetched game. Shared by the single-join and
// bulk-join routes so the connect/handshake/session-wiring lives in one place.
// Throws on connect/handshake failure (caller maps to a 502).
async function joinRemoteBot(gameCode, name, gameObj) {
  const socket = connectRemoteSocket();
  const userObj = {
    id: randomRemotePlayerId(),
    name,
    socketID: null,
    score: 0,
    ready: false,
  };

  let joinAck;
  try {
    await waitForSocketConnect(socket);
    userObj.socketID = socket.id;
    if (debugSettings.outbound) {
      addDebugLog('outbound', {
        eventName: 'player-joining',
        gameCode,
        userId: userObj.id,
        name,
        payload: debugSettings.payloads
          ? safeJson({ game: gameObj, user: userObj })
          : { game: summarizePayload(gameObj), user: summarizePayload(userObj) },
      });
    }
    joinAck = await emitRemoteJoin(socket, gameObj, userObj);
  } catch (err) {
    socket.disconnect();
    throw err;
  }

  const sessionId = crypto.randomUUID();
  remoteSessions.set(sessionId, {
    sessionId,
    socket,
    gameCode,
    gameId: gameObj.id,
    userId: userObj.id,
    name,
    joinedAt: now(),
    knownPlayerIds: new Set([userObj.id]),
    roster: new Map(),
    // Retained so a dropped socket can replay the join handshake on reconnect.
    gameObj,
    userObj,
  });
  const session = remoteSessions.get(sessionId);
  rememberPlayerIds(session, gameObj, joinAck);
  // Seed the roster with our own bot plus anyone in the game/ack payloads.
  rememberPlayers(session, userObj, gameObj, joinAck);
  logJoinAck(session, joinAck);
  attachRemoteSocketHandlers(session);

  if (autoReadyGameCodes.has(gameCode)) {
    setTimeout(() => readyAllPlayers(gameCode), 100);
  }

  return { session, joinAck };
}

app.post('/api/stax/join', rateLimit({ windowMs: 60_000, max: 30 }), async (req, res) => {
  const gameCode = cleanGameCode(req.body?.gameCode);
  const name = String(req.body?.name || '').trim().slice(0, 24);
  if (!gameCode) return res.status(400).json({ error: 'gameCode is required' });
  if (!name) return res.status(400).json({ error: 'name is required' });

  let gameObj;
  try {
    gameObj = await fetchRemoteGame(gameCode);
  } catch (err) {
    return res.status(404).json({ error: err.message });
  }

  let result;
  try {
    result = await joinRemoteBot(gameCode, name, gameObj);
  } catch (err) {
    return res.status(502).json({ error: err.message });
  }

  const { session, joinAck } = result;
  res.json({
    ok: true,
    sessionId: session.sessionId,
    gameCode,
    gameId: session.gameId,
    userId: session.userId,
    name,
    joinAckKeys: joinAck && typeof joinAck === 'object' ? Object.keys(joinAck) : [],
  });
});

// Fleet mode: join `count` bots to one game code in a single request. Fetches the
// game once and reuses it; each bot gets a distinct name and its own socket. Joins
// run sequentially so a flood of sockets doesn't open at once. Partial success is
// fine — failures are reported per-bot rather than failing the whole batch.
app.post('/api/stax/join-bulk', rateLimit({ windowMs: 60_000, max: 30 }), async (req, res) => {
  const gameCode = cleanGameCode(req.body?.gameCode);
  const baseName = String(req.body?.name || '').trim().slice(0, 20) || 'Bot';
  const count = Math.min(10, Math.max(1, Number(req.body?.count) || 1));
  if (!gameCode) return res.status(400).json({ error: 'gameCode is required' });

  let gameObj;
  try {
    gameObj = await fetchRemoteGame(gameCode);
  } catch (err) {
    return res.status(404).json({ error: err.message });
  }

  const joined = [];
  const errors = [];
  for (let i = 0; i < count; i += 1) {
    const name = count === 1 ? baseName : `${baseName} ${i + 1}`;
    try {
      const { session } = await joinRemoteBot(gameCode, name, structuredClone(gameObj));
      joined.push(publicSession(session));
    } catch (err) {
      errors.push({ name, error: err.message });
    }
  }

  res.status(joined.length ? 200 : 502).json({
    ok: joined.length > 0,
    gameCode,
    requested: count,
    joinedCount: joined.length,
    joined,
    errors,
  });
});

function publicSession(session) {
  return {
    sessionId: session.sessionId,
    gameCode: session.gameCode,
    gameId: session.gameId,
    userId: session.userId,
    name: session.name,
    joinedAt: session.joinedAt,
    connected: session.socket.connected,
    // socket.active && !connected means a reconnect is currently in flight.
    reconnecting: !!session.socket.active && !session.socket.connected,
    autoReady: autoReadyGameCodes.has(session.gameCode),
    knownPlayers: session.knownPlayerIds.size,
  };
}

app.get('/api/stax/sessions', (req, res) => {
  res.json({
    ok: true,
    count: remoteSessions.size,
    sessions: [...remoteSessions.values()].map(publicSession),
  });
});

// Live scoreboard: every player our bots can see in a game (or across all games
// if no gameCode is given), merged across sessions, newest observation wins.
// "ours" flags the bots this server is driving so the UI can distinguish them.
app.get('/api/stax/roster', (req, res) => {
  const gameCode = req.query?.gameCode ? cleanGameCode(req.query.gameCode) : null;
  const merged = new Map();
  const ourIds = new Set();

  for (const session of remoteSessions.values()) {
    if (gameCode && session.gameCode !== gameCode) continue;
    ourIds.add(session.userId);
    if (!session.roster) continue;
    for (const player of session.roster.values()) {
      const prev = merged.get(player.id);
      if (!prev || (player.updatedAt || 0) >= (prev.updatedAt || 0)) merged.set(player.id, player);
    }
  }

  const players = [...merged.values()]
    .map((p) => ({ ...p, ours: ourIds.has(p.id) }))
    .sort((a, b) => (b.score ?? -Infinity) - (a.score ?? -Infinity));

  res.json({ ok: true, gameCode, count: players.length, players });
});

app.post('/api/stax/leave', (req, res) => {
  const session = remoteSessions.get(req.body?.sessionId);
  if (!session) return res.status(404).json({ error: 'Remote session not found' });
  logOutbound(session, 'disconnect', { reason: 'leave endpoint' });
  session.socket.disconnect();
  remoteSessions.delete(req.body.sessionId);
  res.json({ ok: true });
});

// Bulk leave: drop every bot, or just those in a given gameCode. Mirrors the
// single-leave teardown (the disconnect handler clears autoready when a game empties).
app.post('/api/stax/leave-all', (req, res) => {
  const gameCode = req.body?.gameCode ? cleanGameCode(req.body.gameCode) : null;
  let left = 0;
  for (const session of [...remoteSessions.values()]) {
    if (gameCode && session.gameCode !== gameCode) continue;
    logOutbound(session, 'disconnect', { reason: 'leave-all endpoint' });
    try {
      session.socket.disconnect();
    } catch (err) {
      console.error('[leave-all] failed to close socket', err?.message || err);
    }
    remoteSessions.delete(session.sessionId);
    left += 1;
  }
  res.json({ ok: true, gameCode, left });
});

app.post('/api/stax/score-self', (req, res) => {
  const session = remoteSessions.get(req.body?.sessionId);
  if (!session) return res.status(404).json({ error: 'Remote session not found' });
  if (!session.socket.connected) return res.status(409).json({ error: 'Remote socket is disconnected' });

  const amount = Number(req.body?.amount);
  if (!Number.isFinite(amount)) return res.status(400).json({ error: 'amount must be a number' });

  const payload = { id: session.userId, score: amount };
  logOutbound(session, 'update-score', payload);
  session.socket.emit('update-score', payload);
  res.json({ ok: true, userId: session.userId, score: amount });
});

// Bulk score: set the same score on every connected bot, or just those in a
// given gameCode. Disconnected bots are skipped and counted.
app.post('/api/stax/score-all', (req, res) => {
  const gameCode = req.body?.gameCode ? cleanGameCode(req.body.gameCode) : null;
  const amount = Number(req.body?.amount);
  if (!Number.isFinite(amount)) return res.status(400).json({ error: 'amount must be a number' });

  let updated = 0;
  let skipped = 0;
  for (const session of remoteSessions.values()) {
    if (gameCode && session.gameCode !== gameCode) continue;
    if (!session.socket.connected) { skipped += 1; continue; }
    const payload = { id: session.userId, score: amount };
    logOutbound(session, 'update-score', payload);
    session.socket.emit('update-score', payload);
    updated += 1;
  }
  res.json({ ok: true, gameCode, amount, updated, skipped });
});

app.post('/api/stax/ready-owned', (req, res) => {
  const session = remoteSessions.get(req.body?.sessionId);
  if (!session) return res.status(404).json({ error: 'Remote session not found' });

  const readyCount = readyAllPlayers(session.gameCode);
  res.json({ ok: true, gameCode: session.gameCode, readyCount });
});

app.post('/api/stax/autoready', (req, res) => {
  const session = remoteSessions.get(req.body?.sessionId);
  if (!session) return res.status(404).json({ error: 'Remote session not found' });
  if (typeof req.body?.enabled !== 'boolean') {
    return res.status(400).json({ error: 'enabled must be a boolean' });
  }

  if (req.body.enabled) {
    autoReadyGameCodes.add(session.gameCode);
    readyAllPlayers(session.gameCode);
  } else {
    autoReadyGameCodes.delete(session.gameCode);
  }

  res.json({
    ok: true,
    gameCode: session.gameCode,
    autoReady: autoReadyGameCodes.has(session.gameCode),
  });
});

app.post('/api/rooms', (req, res) => {
  const { name } = req.body || {};
  const { room, host } = createRoom(name);
  res.json({ room: publicRoom(room), player: host });
});

app.get('/api/rooms/:code', (req, res) => {
  const room = getRoomOr404(req.params.code, res);
  if (!room) return;
  res.json({ room: publicRoom(room) });
});

app.post('/api/rooms/:code/join', (req, res) => {
  const room = getRoomOr404(req.params.code, res);
  if (!room) return;
  if (room.status === 'finished') return res.status(409).json({ error: 'That room is finished' });
  if (room.players.size >= MAX_PLAYERS_PER_ROOM) return res.status(409).json({ error: 'Room is full' });
  const player = addPlayer(room, req.body?.name, false);
  res.json({ room: publicRoom(room), player });
});

app.post('/api/rooms/:code/bots', (req, res) => {
  const room = getRoomOr404(req.params.code, res);
  if (!room) return;

  const requested = Math.min(12, Math.max(1, Number(req.body?.count) || 1));
  const roomSpace = Math.max(0, MAX_PLAYERS_PER_ROOM - room.players.size);
  const count = Math.min(requested, roomSpace);
  if (count === 0) return res.status(409).json({ error: 'Room is full' });
  const made = [];
  for (let i = 0; i < count; i += 1) {
    const base = BOT_NAMES[crypto.randomInt(BOT_NAMES.length)];
    made.push(addPlayer(room, `${base}-${crypto.randomInt(10, 99)}`, true));
  }
  res.json({ room: publicRoom(room), bots: made });
});

app.post('/api/rooms/:code/bot-control', (req, res) => {
  const room = getRoomOr404(req.params.code, res);
  if (!room) return;
  if (room.status === 'finished') return res.status(409).json({ error: 'That room is finished' });
  if (room.players.size >= MAX_PLAYERS_PER_ROOM) return res.status(409).json({ error: 'Room is full' });

  const bot = addPlayer(room, req.body?.name || 'Remote Bot', true);
  bot.controlled = true;
  addEvent(room, 'bot', `${bot.name} is controlled remotely`);
  res.json({ room: publicRoom(room), player: bot });
});

app.post('/api/rooms/:code/ready', (req, res) => {
  const room = getRoomOr404(req.params.code, res);
  if (!room) return;
  const player = room.players.get(req.body?.playerId);
  if (!player) return res.status(404).json({ error: 'Player not found' });
  player.ready = !player.ready;
  addEvent(room, 'ready', `${player.name} is ${player.ready ? 'ready' : 'not ready'}`);
  res.json({ room: publicRoom(room), player });
});

app.post('/api/rooms/:code/start', (req, res) => {
  const room = getRoomOr404(req.params.code, res);
  if (!room) return;
  if (room.players.size < 1) return res.status(409).json({ error: 'Need at least one player' });
  startRoom(room);
  res.json({ room: publicRoom(room) });
});

app.post('/api/rooms/:code/score', (req, res) => {
  const room = getRoomOr404(req.params.code, res);
  if (!room) return;
  if (room.status !== 'playing') return res.status(409).json({ error: 'The game is not running' });
  const player = room.players.get(req.body?.playerId);
  if (!player) return res.status(404).json({ error: 'Player not found' });
  player.score += 1;
  addEvent(room, 'score', `${player.name} scored`);
  res.json({ room: publicRoom(room), player });
});

app.post('/api/rooms/:code/leave', (req, res) => {
  const room = getRoomOr404(req.params.code, res);
  if (!room) return;
  const player = room.players.get(req.body?.playerId);
  if (player) {
    room.players.delete(player.id);
    addEvent(room, 'leave', `${player.name} left`);
  }
  res.json({ room: publicRoom(room) });
});

app.use((req, res) => {
  res.status(404).json({ error: 'Not found' });
});

// eslint-disable-next-line no-unused-vars
app.use((err, req, res, next) => {
  console.error('[error]', err);
  if (res.headersSent) return next(err);
  res.status(500).json({ error: 'Internal server error' });
});

const server = app.listen(PORT, () => {
  console.log(`Game + bot server running at http://localhost:${PORT}`);
  if (!API_TOKEN) {
    console.warn(
      '[warn] API_TOKEN is not set — control endpoints (/api/stax, /api/rooms) are OPEN to anyone who can reach this server. Set API_TOKEN in the environment before exposing it publicly.'
    );
  }
});

let shuttingDown = false;
function shutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log(`\n${signal} received, shutting down...`);
  clearInterval(sweepTimer);
  for (const room of rooms.values()) {
    for (const timer of room.timers) clearInterval(timer);
    room.timers.clear();
  }
  for (const session of remoteSessions.values()) {
    try {
      session.socket.disconnect();
    } catch (err) {
      console.error('[shutdown] failed to close remote socket', err?.message || err);
    }
  }
  remoteSessions.clear();
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 5000).unref();
}

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));

// Untrusted remote socket payloads are processed in event handlers; log and
// stay up rather than letting a single bad payload take the whole server down.
process.on('unhandledRejection', (reason) => {
  console.error('[unhandledRejection]', reason);
});
process.on('uncaughtException', (err) => {
  console.error('[uncaughtException]', err);
});