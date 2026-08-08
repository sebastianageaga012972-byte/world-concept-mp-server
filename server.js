// ══════════════════════════════════════════════════════════════════════
// World Concept — Multiplayer Relay Server
// ──────────────────────────────────────────────────────────────────────
// A lightweight WebSocket relay. It does NOT simulate the game — every
// client still runs its own copy of movement.html and its own physics.
// This server's only job is:
//   1. Let a player create a "room" and get a shareable 5-letter code
//      (plus an optional custom server name + server ID — see
//      "Custom server name/ID" below).
//   2. Let other players join that room with the code.
//   3. Relay each player's state (position, facing, animation/pose flags,
//      equipped gear, socketed Infinity Stones, etc.) to everyone else
//      in the same room, as fast as it arrives (no server-side physics,
//      no authority, no anti-cheat — this is a "trust the peer" relay,
//      good for playing with friends, NOT for a public competitive game).
//
// Run locally:   npm install && npm start        (listens on PORT, default 8080)
// Deploy:        see README.md in this same folder for step-by-step
//                 instructions for Render.com (free tier, no credit card).
//
// ── Custom server name/ID ──────────────────────────────────────────────
// A host creating a room can optionally send `serverName` and `serverId`
// alongside `create_room`. These are cosmetic/organizational — the 5-
// letter room code is still what players actually type in to join.
// Uniqueness is checked on the (serverName, serverId) PAIR, not on
// either one alone: two different hosts can both name their room
// "Grasslands" as long as their serverId differs (and vice versa), so
// reusing a popular name doesn't block anyone. Only an exact match of
// BOTH fields against a currently-open room is rejected — closed rooms
// free up their name/ID combo immediately, same as their room code.
//
// ── Room list endpoint ──────────────────────────────────────────────────
// GET /rooms returns a JSON array of currently open rooms (code, name,
// id, player count, capacity) so a client/launcher can show a public
// server browser instead of requiring a manually-typed code.
//
// ── Rate limiting ────────────────────────────────────────────────────────
// Two independent limits, both per-connection:
//   • General message flood limit — a token bucket over ALL inbound
//     messages (state/event ticks included), so one runaway client
//     can't hog CPU or bandwidth relaying to a whole room.
//   • Room-creation cooldown — create_room/join_room specifically are
//     also limited to a small number of attempts per minute, since spun-
//     up rooms cost memory even if immediately abandoned.
// Clients that exceed a limit get a `rate_limited` message (state/event
// messages are just silently dropped instead, since these are frequent
// and transient — no need to reply to every dropped movement tick).
// ══════════════════════════════════════════════════════════════════════

const { WebSocketServer } = require('ws');
const http = require('http');
const crypto = require('crypto');

const PORT = process.env.PORT || 8080;

// ── Rate limiting knobs ──────────────────────────────────────────────
// General token bucket: refills MSG_RATE_PER_SEC tokens/sec, holds at
// most MSG_BUCKET_MAX. Generous enough for normal state-tick traffic
// (movement.html sends state fairly often) while still catching an
// actually broken/malicious client sending as fast as the socket allows.
const MSG_RATE_PER_SEC = 40;
const MSG_BUCKET_MAX = 80;

// Room-creation/join cooldown: at most this many create_room/join_room
// attempts per connection per window.
const ROOM_ACTION_LIMIT = 6;
const ROOM_ACTION_WINDOW_MS = 60000;

// Server name/ID field limits (mirrors the 24-char player name cap
// already used elsewhere in this file).
const SERVER_NAME_MAX_LEN = 32;
const SERVER_ID_MAX_LEN = 32;

// ── Basic HTTP server, mostly so hosting platforms have something to
//    health-check on GET /, and so this can share a port with the
//    WebSocket upgrade. ──
const httpServer = http.createServer((req, res) => {
    if (req.url === '/health') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: true, rooms: rooms.size }));
        return;
    }
    if (req.url === '/rooms') {
        // Public room browser data — no player identities beyond count,
        // just enough to pick a room to join. Room code is included since
        // that's what a client needs to actually connect (nothing more
        // sensitive than what the host already shares to invite people).
        const list = Array.from(rooms.values()).map((room) => ({
            code: room.code,
            serverName: room.serverName,
            serverId: room.serverId,
            players: room.players.size,
            maxPlayers: ROOM_MAX_PLAYERS,
        }));
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ rooms: list }));
        return;
    }
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('World Concept multiplayer relay is running.\n');
});

const wss = new WebSocketServer({ server: httpServer });

// rooms: Map<roomCode, {
//   code, serverName, serverId,
//   players: Map<playerId, { ws, name, lastState }>
// }>
const rooms = new Map();
const ROOM_MAX_PLAYERS = 8;

const ROOM_CODE_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; // no 0/O/1/I ambiguity
function generateRoomCode() {
    let code;
    do {
        code = Array.from({ length: 5 }, () =>
            ROOM_CODE_CHARS[crypto.randomInt(ROOM_CODE_CHARS.length)]
        ).join('');
    } while (rooms.has(code));
    return code;
}

function makePlayerId() {
    return crypto.randomBytes(6).toString('hex');
}

function send(ws, msg) {
    if (ws.readyState === ws.OPEN) {
        ws.send(JSON.stringify(msg));
    }
}

function broadcastToRoom(roomCode, msg, exceptPlayerId = null) {
    const room = rooms.get(roomCode);
    if (!room) return;
    const payload = JSON.stringify(msg);
    for (const [pid, p] of room.players.entries()) {
        if (pid === exceptPlayerId) continue;
        if (p.ws.readyState === p.ws.OPEN) p.ws.send(payload);
    }
}

function roomRosterMsg(roomCode) {
    const room = rooms.get(roomCode);
    const players = room
        ? Array.from(room.players.entries()).map(([pid, p]) => ({ id: pid, name: p.name }))
        : [];
    return { type: 'roster', players };
}

// ── Sanitize a host-supplied server name/ID: trim, cap length, and
//    collapse to a plain string (never trust the type of incoming JSON
//    fields). Empty after trimming just means "not set" — create_room
//    falls back to the room code for display in that case. ──
function sanitizeField(value, maxLen) {
    return (value == null ? '' : String(value)).trim().slice(0, maxLen);
}

// ── True if some OTHER currently-open room already has this exact
//    (serverName, serverId) pair. Matching is case-insensitive on both
//    fields so "Grasslands"/"grasslands" collide but "Grasslands"+"A"
//    and "Grasslands"+"B" do not — only a full pair match blocks
//    creation, per the "same name AND same ID" requirement. Rooms that
//    left either field blank are only compared against other blank-vs-
//    blank rooms the same way; blank isn't a wildcard. ──
function serverIdentityTaken(serverName, serverId, excludeRoomCode = null) {
    const name = serverName.toLowerCase();
    const id = serverId.toLowerCase();
    for (const room of rooms.values()) {
        if (room.code === excludeRoomCode) continue;
        if (room.serverName.toLowerCase() === name && room.serverId.toLowerCase() === id) {
            return true;
        }
    }
    return false;
}

// ── Rate limiting state, keyed per-connection (stored directly on the
//    ws object so it's naturally cleaned up when the socket closes). ──
function initRateLimitState(ws) {
    ws._msgTokens = MSG_BUCKET_MAX;
    ws._msgTokensLastRefill = Date.now();
    ws._roomActionTimestamps = [];
}

// Refill the token bucket based on elapsed time, then try to spend one
// token. Returns true if the message is allowed, false if it should be
// dropped/rejected.
function takeMessageToken(ws) {
    const now = Date.now();
    const elapsedSec = (now - ws._msgTokensLastRefill) / 1000;
    ws._msgTokensLastRefill = now;
    ws._msgTokens = Math.min(MSG_BUCKET_MAX, ws._msgTokens + elapsedSec * MSG_RATE_PER_SEC);
    if (ws._msgTokens < 1) return false;
    ws._msgTokens -= 1;
    return true;
}

// Sliding-window check for create_room/join_room specifically. Returns
// true if this attempt is allowed (and records it); false if the
// connection has already hit ROOM_ACTION_LIMIT within the window.
function takeRoomActionSlot(ws) {
    const now = Date.now();
    ws._roomActionTimestamps = ws._roomActionTimestamps.filter(
        (t) => now - t < ROOM_ACTION_WINDOW_MS
    );
    if (ws._roomActionTimestamps.length >= ROOM_ACTION_LIMIT) return false;
    ws._roomActionTimestamps.push(now);
    return true;
}

wss.on('connection', (ws) => {
    let currentRoom = null;
    let playerId = null;
    let playerName = 'Player';

    ws.isAlive = true;
    ws.on('pong', () => { ws.isAlive = true; });
    initRateLimitState(ws);

    ws.on('message', (raw) => {
        // General flood limit first — applies to every message type,
        // including state/event ticks. Dropped silently (no reply) since
        // this can legitimately fire during normal fast-paced play and
        // sending a rate_limited notice for every dropped tick would
        // itself contribute to the flood.
        if (!takeMessageToken(ws)) return;

        let msg;
        try {
            msg = JSON.parse(raw);
        } catch {
            return; // ignore malformed input
        }

        switch (msg.type) {

            // ── Host creates a new room ──
            case 'create_room': {
                if (!takeRoomActionSlot(ws)) {
                    send(ws, { type: 'rate_limited', action: 'create_room', reason: 'Too many room actions — wait a moment and try again.' });
                    return;
                }

                const serverName = sanitizeField(msg.serverName, SERVER_NAME_MAX_LEN);
                const serverId = sanitizeField(msg.serverId, SERVER_ID_MAX_LEN);

                // Only reject when BOTH the name and the ID exactly match
                // an already-open room — matching just one of the two is
                // fine (see serverIdentityTaken() above), and blank
                // fields simply fall back to displaying the room code.
                if ((serverName || serverId) && serverIdentityTaken(serverName, serverId)) {
                    send(ws, { type: 'create_error', reason: 'A server with that name and ID is already open. Change either one and try again.' });
                    return;
                }

                const code = generateRoomCode();
                playerId = makePlayerId();
                playerName = (msg.name || 'Player').toString().slice(0, 24);
                currentRoom = code;
                rooms.set(code, {
                    code,
                    serverName: serverName || code,
                    serverId: serverId || code,
                    players: new Map([[playerId, { ws, name: playerName, lastState: null }]]),
                });
                send(ws, { type: 'room_created', roomCode: code, playerId, serverName: serverName || code, serverId: serverId || code });
                break;
            }

            // ── A guest joins an existing room by code ──
            case 'join_room': {
                if (!takeRoomActionSlot(ws)) {
                    send(ws, { type: 'rate_limited', action: 'join_room', reason: 'Too many room actions — wait a moment and try again.' });
                    return;
                }

                const code = (msg.roomCode || '').toString().toUpperCase().trim();
                const room = rooms.get(code);
                if (!room) {
                    send(ws, { type: 'join_error', reason: 'Room not found. Check the code and try again.' });
                    return;
                }
                if (room.players.size >= ROOM_MAX_PLAYERS) {
                    send(ws, { type: 'join_error', reason: `Room is full (${ROOM_MAX_PLAYERS} players max).` });
                    return;
                }
                playerId = makePlayerId();
                playerName = (msg.name || 'Player').toString().slice(0, 24);
                currentRoom = code;
                room.players.set(playerId, { ws, name: playerName, lastState: null });

                send(ws, { type: 'joined', roomCode: code, playerId, serverName: room.serverName, serverId: room.serverId });

                // Tell the newcomer who is already here, including each
                // existing player's most recent known state, so they see
                // everyone else's current position/gear immediately
                // instead of waiting for that player's next state tick.
                const existing = Array.from(room.players.entries())
                    .filter(([pid]) => pid !== playerId)
                    .map(([pid, p]) => ({ id: pid, name: p.name, state: p.lastState }));
                send(ws, { type: 'existing_players', players: existing });

                // Tell everyone else a new player joined.
                broadcastToRoom(code, { type: 'player_joined', id: playerId, name: playerName }, playerId);
                break;
            }

            // ── Frequent state updates: position, facing, animation/pose
            //    flags, equipped gear, socketed stones, etc. Forwarded
            //    verbatim to everyone else in the room — this server does
            //    not interpret or validate the shape of `state`. ──
            case 'state': {
                if (!currentRoom || !playerId) return;
                const room = rooms.get(currentRoom);
                if (!room || !room.players.has(playerId)) return;
                room.players.get(playerId).lastState = msg.state;
                broadcastToRoom(currentRoom, { type: 'state', id: playerId, state: msg.state }, playerId);
                break;
            }

            // ── One-off events that aren't part of continuous state:
            //    attack swings, ability activations, damage numbers, chat
            //    pings, etc. Forwarded the same way as state, but never
            //    stored, since these are transient. ──
            case 'event': {
                if (!currentRoom || !playerId) return;
                broadcastToRoom(currentRoom, { type: 'event', id: playerId, event: msg.event }, playerId);
                break;
            }

            case 'leave_room': {
                cleanupPlayer();
                break;
            }
        }
    });

    function cleanupPlayer() {
        if (!currentRoom || !playerId) return;
        const room = rooms.get(currentRoom);
        if (room) {
            room.players.delete(playerId);
            if (room.players.size === 0) {
                rooms.delete(currentRoom);
            } else {
                broadcastToRoom(currentRoom, { type: 'player_left', id: playerId });
            }
        }
        currentRoom = null;
        playerId = null;
    }

    ws.on('close', cleanupPlayer);
    ws.on('error', cleanupPlayer);
});

// ── Drop dead connections (e.g. laptop closed without a clean close
//    frame) so rooms don't accumulate ghost players forever. ──
const heartbeat = setInterval(() => {
    wss.clients.forEach((ws) => {
        if (ws.isAlive === false) return ws.terminate();
        ws.isAlive = false;
        ws.ping();
    });
}, 30000);

wss.on('close', () => clearInterval(heartbeat));

httpServer.listen(PORT, () => {
    console.log(`World Concept multiplayer relay listening on port ${PORT}`);
});