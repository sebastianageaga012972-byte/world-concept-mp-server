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
//   4. On Private/LAN mode, print (and expose over HTTP) the machine's
//      own LAN IP on startup, so movement.html's "Private" panel can
//      auto-detect it instead of the player typing it in by hand.
//   5. On Public mode, keep a lightweight directory of open, joinable
//      rooms (name/id/player count only — never state) so movement.html's
//      "Public" panel can show a scrollable, paginated list of servers to
//      join, with "Create Server" alongside it to host a new one.
//
// Run locally:   npm install && npm start        (listens on PORT, default 8080)
// Deploy:        see README.md in this same folder for step-by-step
//                 instructions for Render.com (free tier, no credit card).
// ══════════════════════════════════════════════════════════════════════

const { WebSocketServer } = require('ws');
const http = require('http');
const crypto = require('crypto');
const os = require('os');

const PORT = process.env.PORT || 8080;

// ── LAN IP auto-detection ────────────────────────────────────────────
// Used two ways:
//   • Printed to the terminal on startup (unchanged, still handy for
//     anyone who wants to read it off manually).
//   • Served over GET /lan-ip as JSON, so a client-side "Private" panel
//     running ON THIS SAME MACHINE (the host's own browser tab) can
//     fetch it and auto-fill the connection address with zero typing —
//     see getMpLocalServerUrl() in movement.html. This only ever
//     resolves to an address of the machine running server.js itself;
//     it cannot discover OTHER devices on the network (no browser can
//     scan a LAN), so guests joining from a different device still need
//     to be told this address some other way (e.g. read aloud, or the
//     host shares it) — auto-detect just removes the guesswork of
//     "which of my IPs is the right one" for whoever's running it.
function getLanIps() {
    const nets = os.networkInterfaces();
    const results = [];
    for (const name of Object.keys(nets)) {
        for (const net of nets[name] || []) {
            // Skip internal (127.0.0.1) and non-IPv4 addresses.
            if (net.family === 'IPv4' && !net.internal) {
                results.push(net.address);
            }
        }
    }
    return results;
}

// ── Basic HTTP server, mostly so hosting platforms have something to
//    health-check on GET /, and so this can share a port with the
//    WebSocket upgrade. Also serves /lan-ip for auto-detect (above) and
//    /rooms as a plain HTTP fallback mirror of the WS list_rooms call,
//    handy for debugging the public directory without a WS client. ──
const httpServer = http.createServer((req, res) => {
    if (req.url === '/health') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: true, rooms: rooms.size }));
        return;
    }
    if (req.url === '/lan-ip') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ips: getLanIps(), port: PORT }));
        return;
    }
    if (req.url && req.url.startsWith('/rooms')) {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ rooms: listPublicRooms(0, 100) }));
        return;
    }
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('World Concept multiplayer relay is running.\n');
});

const wss = new WebSocketServer({ server: httpServer });

// rooms: Map<roomCode, {
//   players: Map<playerId, { ws, clientId, name, lastState }>,
//   serverName, serverId, createdAt
// }>
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

// ── Public server directory ──────────────────────────────────────────
// Returns open rooms newest-first, sliced [offset, offset+limit) — this
// slicing is what makes the client's "endless scroll" work: the panel
// asks for another page as the player scrolls near the bottom, rather
// than the server ever needing to push a giant list all at once.
function listPublicRooms(offset, limit) {
    const all = Array.from(rooms.entries())
        .sort((a, b) => b[1].createdAt - a[1].createdAt)
        .map(([code, room]) => ({
            roomCode: code,
            serverName: room.serverName || code,
            serverId: room.serverId || null,
            playerCount: room.players.size,
            maxPlayers: 8,
        }));
    return {
        total: all.length,
        rooms: all.slice(offset, offset + limit),
    };
}

wss.on('connection', (ws) => {
    let currentRoom = null;
    let playerId = null;
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

            // ── Host creates a new room. Optional serverName/serverId are
            //    purely cosmetic labels shown in the Public directory —
            //    if either is left blank it falls back to the room code,
            //    and a create is only rejected if BOTH fields exactly
            //    match an already-open public room. ──
            case 'create_room': {
                const requestedName = (msg.serverName || '').toString().trim().slice(0, 32);
                const requestedId = (msg.serverId || '').toString().trim().slice(0, 32);
                if (requestedName && requestedId) {
                    for (const room of rooms.values()) {
                        if (room.serverName === requestedName && room.serverId === requestedId) {
                            send(ws, { type: 'join_error', reason: 'A server with that name and ID already exists. Pick a different name or ID.' });
                            return;
                        }
                    }
                }
                const code = generateRoomCode();
                playerId = makePlayerId();
                playerName = (msg.name || 'Player').toString().slice(0, 24);
                currentRoom = code;
                const clientId = (msg.clientId || '').toString().slice(0, 64) || null;
                rooms.set(code, {
                    players: new Map([[playerId, { ws, clientId, name: playerName, lastState: null }]]),
                    serverName: requestedName || null,
                    serverId: requestedId || null,
                    createdAt: Date.now(),
                });
                send(ws, {
                    type: 'room_created',
                    roomCode: code,
                    playerId,
                    serverName: requestedName || code,
                    serverId: requestedId || code,
                });
                break;
            }

            // ── A guest joins an existing room by code ──
            case 'join_room': {
                const code = (msg.roomCode || '').toString().toUpperCase().trim();
                const room = rooms.get(code);
                if (!room) {
                    send(ws, { type: 'join_error', reason: 'Room not found. Check the code and try again.' });
                    return;
                }
                if (room.players.size >= 8) {
                    send(ws, { type: 'join_error', reason: 'Room is full (8 players max).' });
                    return;
                }
                playerId = makePlayerId();
                playerName = (msg.name || 'Player').toString().slice(0, 24);
                currentRoom = code;
                const clientId = (msg.clientId || '').toString().slice(0, 64) || null;
                room.players.set(playerId, { ws, clientId, name: playerName, lastState: null });

                send(ws, {
                    type: 'joined',
                    roomCode: code,
                    playerId,
                    serverName: room.serverName || code,
                    serverId: room.serverId || code,
                });

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

            // ── Public directory browse — paginated so the client can
            //    implement an endless-scroll list instead of one big
            //    dump. offset/limit both default sensibly if omitted. ──
            case 'list_rooms': {
                const offset = Number.isFinite(msg.offset) ? Math.max(0, msg.offset | 0) : 0;
                const limit = Number.isFinite(msg.limit) ? Math.min(50, Math.max(1, msg.limit | 0)) : 20;
                const { total, rooms: page } = listPublicRooms(offset, limit);
                send(ws, { type: 'room_list', rooms: page, offset, total });
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
    const ips = getLanIps();
    if (ips.length) {
        console.log('LAN IP(s) for Private/LAN play (share with devices on the same WiFi):');
        for (const ip of ips) console.log(`  ws://${ip}:${PORT}`);
    } else {
        console.log('No LAN IP detected — check your network connection for Private/LAN play.');
    }
});