const path = require('path');
const crypto = require('crypto');
const express = require('express');

const PORT = process.env.PORT || 4000;
const ROUND_MS = 45_000;
const BOT_NAMES = ['Byte', 'Nova', 'Echo', 'Pixel', 'Dash', 'Orbit', 'Miso', 'Kite'];

const app = express();
const rooms = new Map();

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

function makeId(prefix) {
  return `${prefix}_${crypto.randomBytes(6).toString('hex')}`;
}

function makeCode() {
  const alphabet = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let code = '';
  do {
    code = '';
    for (let i = 0; i < 5; i += 1) {
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
      .map(({ id, name, score, ready, isBot, joinedAt }) => ({ id, name, score, ready, isBot, joinedAt }))
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
  res.json({ ok: true, rooms: rooms.size });
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
  if (!player || player.isBot) return res.status(404).json({ error: 'Player not found' });
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
