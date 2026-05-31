# Lobby Bots

This is a complete little game server plus browser UI. Players can create or join a room, add server-owned bots, start a 45-second round, and compete by clicking `Score +1` while bots score automatically.

## Run locally

```bash
npm install
npm start
```

Then open:

```text
http://localhost:4000
```

## Deploy note

GitHub Pages can host the HTML, CSS, and client JavaScript, but it cannot run `server.js`. To put this online, deploy this folder to a Node host such as Render, Railway, Fly.io, or a VPS. If you later split the frontend onto GitHub Pages, change the `fetch(...)` calls in `public/index.html` to point at your hosted API URL.

## API

- `POST /api/rooms` creates a room.
- `POST /api/rooms/:code/join` joins as a real player.
- `POST /api/rooms/:code/bots` adds safe bots to that room.
- `POST /api/rooms/:code/bot-control` adds one bot and returns its controller player id.
- `POST /api/rooms/:code/start` starts a round.
- `POST /api/rooms/:code/score` scores for the caller's player id.
- `GET /api/rooms/:code` returns the current room state.
