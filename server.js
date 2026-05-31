const path = require('path');
const crypto = require('crypto');
const express = require('express');
const { io } = require('socket.io-client');

const PORT = process.env.PORT || 4000;
const ROUND_MS = 45_000;
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

app.use(express.json());
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
  const response = await fetch(`${base}/${encodeURIComponent(code)}`);
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

function publicRoom(room) {
  const remainingMs = room.startedAt ? Math.max(0, ROUND_MS - (now() - room.startedAt)) : ROUND_MS;
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
    ready: isBot,
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
    addEvent(room, 'finish', 'Game finished');
    for (const timer of room.timers) clearInterval(timer);
    room.timers.clear();
  }, ROUND_MS);
  room.timers.add(finishTimer);
}

app.get('/api/health', (req, res) => {
  res.json({ ok: true, rooms: rooms.size, remoteSessions: remoteSessions.size });
});

app.post('/api/stax/join', async (req, res) => {
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
    joinAck = await emitRemoteJoin(socket, gameObj, userObj);
  } catch (err) {
    socket.disconnect();
    return res.status(502).json({ error: err.message });
  }

  const sessionId = crypto.randomUUID();
  remoteSessions.set(sessionId, {
    socket,
    gameCode,
    gameId: gameObj.id,
    userId: userObj.id,
    name,
    joinedAt: now(),
  });

  socket.on('disconnect', () => {
    remoteSessions.delete(sessionId);
  });

  res.json({
    ok: true,
    sessionId,
    gameCode,
    gameId: gameObj.id,
    userId: userObj.id,
    name,
    joinAckKeys: joinAck && typeof joinAck === 'object' ? Object.keys(joinAck) : [],
  });
});

app.post('/api/stax/leave', (req, res) => {
  const session = remoteSessions.get(req.body?.sessionId);
  if (!session) return res.status(404).json({ error: 'Remote session not found' });
  session.socket.disconnect();
  remoteSessions.delete(req.body.sessionId);
  res.json({ ok: true });
});

app.post('/api/stax/score-self', (req, res) => {
  const session = remoteSessions.get(req.body?.sessionId);
  if (!session) return res.status(404).json({ error: 'Remote session not found' });
  if (!session.socket.connected) return res.status(409).json({ error: 'Remote socket is disconnected' });

  const amount = Number(req.body?.amount);
  if (!Number.isFinite(amount)) return res.status(400).json({ error: 'amount must be a number' });

  session.socket.emit('update-score', { id: session.userId, score: amount });
  res.json({ ok: true, userId: session.userId, score: amount });
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
  const player = addPlayer(room, req.body?.name, false);
  res.json({ room: publicRoom(room), player });
});

app.post('/api/rooms/:code/bots', (req, res) => {
  const room = getRoomOr404(req.params.code, res);
  if (!room) return;

  const count = Math.min(12, Math.max(1, Number(req.body?.count) || 1));
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

  const bot = addPlayer(room, req.body?.name || 'Remote Bot', true);
  bot.controlled = true;
  bot.ready = false;
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

app.listen(PORT, () => {
  console.log(`Game + bot server running at http://localhost:${PORT}`);
});
