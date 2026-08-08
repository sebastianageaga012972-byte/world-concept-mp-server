// ══════════════════════════════════════════════════════════════════════
// World Concept — Multiplayer Relay Server
// ──────────────────────────────────────────────────────────────────────
// A lightweight WebSocket relay. It does NOT simulate the game — every
// client still runs its own copy of movement.html and its own physics.
// This server's only job is:
//   1. Let a player create a "room" and get a shareable 5-letter code.
//   2. Let other players join that room with the code.
//   3. Relay each player's state (position, facing, animation/pose flags,
//      equipped gear, socketed Infinity Stones, etc.) to everyone else
//      in the same room, as fast as it arrives (no server-side physics,
//      no authority, no anti-cheat — this is a "trust the peer" relay,
//      good for playing with friends, NOT for a public competitive game).
//
// ── IDENTITY / NO-DUPLICATE-PLAYER MODEL ────────────────────────────────
// Every client generates its own stable "clientId" once (see movement.html
// — it's created with crypto.randomUUID() and saved to localStorage), and
// sends that same clientId every time it connects, reconnects, or rejoins
// a room. The server keys each room's player map by THIS clientId, not by
// a fresh per-socket ID. That means:
//   • A dropped WiFi connection that reconnects a second later replaces
//     the player's OLD entry instead of adding a second "ghost" copy —
//     the old socket (if still technically open) is closed immediately.
//   • Rapid double-clicking "Join" / accidental double submit collapses
//     to a single player entry, because the second join just replaces
//     the first under the same clientId instead of creating a new one.
//   • A player can never appear twice in the same room's roster, by
//     construction — the room is a Map<clientId, ...>, so re-adding the
//     same clientId can only ever overwrite, never duplicate.
//
// Run locally:   npm install && npm start        (listens on PORT, default 8080)
//
// Deploy for internet play:  see README.md in this same folder for
//                 step-by-step instructions for Render.com (free tier,
//                 no credit card).
//
// Run for LOCAL/LAN play (no internet required):  see LAN-PLAY.md in this
//                 same folder — run this server on one player's machine,
//                 share that machine's local IP + room code with friends
//                 on the same WiFi/router, and nothing ever leaves the
//                 local network.
// ══════════════════════════════════════════════════════════════════════

const { WebSocketServer } = require('ws');
const http = require('http');
const crypto = require('crypto');

const PORT = process.env.PORT || 8080;

// ── Basic HTTP server, mostly so hosting platforms have something to
//    health-check on GET /, and so this can share a port with the
//    WebSocket upgrade. ──
const httpServer = http.createServer((req, res) => {
    if (req.url === '/health') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: true, rooms: rooms.size }));
        return;
    }
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('World Concept multiplayer relay is running.\n');
});

const wss = new WebSocketServer({ server: httpServer });

// rooms: Map<roomCode, Map<clientId, { ws, name, lastState }>>
// Keyed by the CLIENT'S OWN stable id (see file header) — never by a
// fresh per-connection id — so the same person can never occupy two
// slots in the same room's roster.
const rooms = new Map();

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

// Basic validation for a client-supplied id: expected to be a UUID-ish
// string from crypto.randomUUID() on the client. Reject anything that
// isn't a reasonable, boring token so a malformed/malicious clientId
// can't be used to collide with or spoof another player's slot.
function isValidClientId(id) {
    return typeof id === 'string' && /^[A-Za-z0-9_-]{8,64}$/.test(id);
}

function send(ws, msg) {
    if (ws.readyState === ws.OPEN) {
        ws.send(JSON.stringify(msg));
    }
}

function broadcastToRoom(roomCode, msg, exceptClientId = null) {
    const room = rooms.get(roomCode);
    if (!room) return;
    const payload = JSON.stringify(msg);
    for (const [cid, p] of room.entries()) {
        if (cid === exceptClientId) continue;
        if (p.ws.readyState === p.ws.OPEN) p.ws.send(payload);
    }
}

// ── Replace-or-insert a player into a room under their stable clientId.
//    If that clientId already has an entry (stale reconnect, double
//    join, etc.), the old socket is closed and the entry is overwritten
//    in place — the room can never end up with two entries for the same
//    clientId. Returns true if this was a genuinely new player (so
//    callers know whether to broadcast a "joined" event). ──
function upsertRoomPlayer(roomCode, clientId, ws, name) {
    const room = rooms.get(roomCode);
    if (!room) return false;
    const existing = room.get(clientId);
    const isNew = !existing;
    if (existing && existing.ws !== ws) {
        // Stale connection under the same identity (e.g. a reconnect
        // after a dropped WiFi link, or a duplicate/racing join). Close
        // it cleanly so it can't linger as a ghost socket still relaying
        // stale state into the room.
        try { existing.ws.close(); } catch {}
    }
    room.set(clientId, { ws, name, lastState: existing ? existing.lastState : null });
    return isNew;
}

wss.on('connection', (ws) => {
    let currentRoom = null;
    let myClientId = null;
    let playerName = 'Player';

    ws.isAlive = true;
    ws.on('pong', () => { ws.isAlive = true; });

    ws.on('message', (raw) => {
        let msg;
        try {
            msg = JSON.parse(raw);
        } catch {
            return; // ignore malformed input
        }

        switch (msg.type) {

            // ── Host creates a new room ──
            case 'create_room': {
                if (!isValidClientId(msg.clientId)) {
                    send(ws, { type: 'join_error', reason: 'Invalid client id. Please reload and try again.' });
                    return;
                }
                const code = generateRoomCode();
                myClientId = msg.clientId;
                playerName = (msg.name || 'Player').toString().slice(0, 24);
                currentRoom = code;
                rooms.set(code, new Map());
                upsertRoomPlayer(code, myClientId, ws, playerName);
                send(ws, { type: 'room_created', roomCode: code, playerId: myClientId });
                break;
            }

            // ── A guest joins an existing room by code ──
            case 'join_room': {
                if (!isValidClientId(msg.clientId)) {
                    send(ws, { type: 'join_error', reason: 'Invalid client id. Please reload and try again.' });
                    return;
                }
                const code = (msg.roomCode || '').toString().toUpperCase().trim();
                const room = rooms.get(code);
                if (!room) {
                    send(ws, { type: 'join_error', reason: 'Room not found. Check the code and try again.' });
                    return;
                }
                const alreadyIn = room.has(msg.clientId);
                if (!alreadyIn && room.size >= 8) {
                    send(ws, { type: 'join_error', reason: 'Room is full (8 players max).' });
                    return;
                }
                myClientId = msg.clientId;
                playerName = (msg.name || 'Player').toString().slice(0, 24);
                currentRoom = code;
                const isNew = upsertRoomPlayer(code, myClientId, ws, playerName);

                send(ws, { type: 'joined', roomCode: code, playerId: myClientId });

                // Tell the newcomer who is already here, including each
                // existing player's most recent known state, so they see
                // everyone else's current position/gear immediately
                // instead of waiting for that player's next state tick.
                const existing = Array.from(room.entries())
                    .filter(([cid]) => cid !== myClientId)
                    .map(([cid, p]) => ({ id: cid, name: p.name, state: p.lastState }));
                send(ws, { type: 'existing_players', players: existing });

                // Only announce a fresh "player_joined" event to the rest
                // of the room if this was actually a new player — a
                // reconnect/rejoin under the same clientId just silently
                // takes over the existing slot instead of re-triggering
                // "X joined" for everyone else.
                if (isNew) {
                    broadcastToRoom(code, { type: 'player_joined', id: myClientId, name: playerName }, myClientId);
                } else {
                    broadcastToRoom(code, { type: 'player_reconnected', id: myClientId, name: playerName }, myClientId);
                }
                break;
            }

            // ── Frequent state updates: position, facing, animation/pose
            //    flags, equipped gear, socketed stones, etc. Forwarded
            //    verbatim to everyone else in the room — this server does
            //    not interpret or validate the shape of `state`. ──
            case 'state': {
                if (!currentRoom || !myClientId) return;
                const room = rooms.get(currentRoom);
                if (!room || !room.has(myClientId)) return;
                const entry = room.get(myClientId);
                if (entry.ws !== ws) return; // stale socket, ignore
                entry.lastState = msg.state;
                broadcastToRoom(currentRoom, { type: 'state', id: myClientId, state: msg.state }, myClientId);
                break;
            }

            // ── One-off events that aren't part of continuous state:
            //    attack swings, ability activations, damage numbers, chat
            //    pings, etc. Forwarded the same way as state, but never
            //    stored, since these are transient. ──
            case 'event': {
                if (!currentRoom || !myClientId) return;
                const room = rooms.get(currentRoom);
                if (!room || !room.has(myClientId) || room.get(myClientId).ws !== ws) return;
                broadcastToRoom(currentRoom, { type: 'event', id: myClientId, event: msg.event }, myClientId);
                break;
            }

            case 'leave_room': {
                cleanupPlayer();
                break;
            }
        }
    });

    function cleanupPlayer() {
        if (!currentRoom || !myClientId) return;
        const room = rooms.get(currentRoom);
        if (room) {
            const entry = room.get(myClientId);
            // Only remove the roster entry if IT'S STILL THIS SOCKET.
            // If a newer connection under the same clientId has already
            // replaced this entry (see upsertRoomPlayer), this stale
            // socket's close/error must NOT delete the newer player.
            if (entry && entry.ws === ws) {
                room.delete(myClientId);
                if (room.size === 0) {
                    rooms.delete(currentRoom);
                } else {
                    broadcastToRoom(currentRoom, { type: 'player_left', id: myClientId });
                }
            }
        }
        currentRoom = null;
        myClientId = null;
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

httpServer.listen(PORT, '0.0.0.0', () => {
    console.log(`World Concept multiplayer relay listening on port ${PORT}`);
    console.log(`Health check: http://localhost:${PORT}/health`);
});