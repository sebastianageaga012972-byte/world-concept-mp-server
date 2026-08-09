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
// Run locally:   npm install && npm start        (listens on PORT, default 8080)
// Deploy:        see README.md in this same folder for step-by-step
//                 instructions for Render.com (free tier, no credit card).
// ══════════════════════════════════════════════════════════════════════

const { WebSocketServer } = require('ws');
const http = require('http');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const os = require('os');

const PORT = process.env.PORT || 8080;

// movement.html is expected to sit right next to this file. Serving it
// from here (instead of the player double-clicking it on disk) is what
// makes WiFi auto-detect possible at all: browsers permanently block
// file:// pages from probing the network, even localhost, with no
// workaround. Opening the game at http://localhost:8080 instead removes
// that wall — see movement.html's isMpFileProtocol()/detectMpLocalServerUrl().
const MOVEMENT_HTML_PATH = path.join(__dirname, 'movement.html');

// ── Report this machine's own LAN IP(s), so movement.html's auto-detect
//    (detectMpLocalServerUrl()) has something concrete to hand back to
//    the page — the address a guest on the same WiFi could also reach,
//    which is often different from whatever host/IP the page itself was
//    loaded from (e.g. 'localhost' on the host's own machine). ──
function getLanIps() {
    const nets = os.networkInterfaces();
    const ips = [];
    for (const name of Object.keys(nets)) {
        for (const net of nets[name] || []) {
            // Skip loopback and non-IPv4 addresses.
            if (net.family === 'IPv4' && !net.internal) ips.push(net.address);
        }
    }
    return ips;
}

// ── Basic HTTP server: serves the game itself, a LAN-IP probe endpoint
//    for auto-detect, and a health check for hosting platforms. Also
//    shares its port with the WebSocket upgrade. ──
const httpServer = http.createServer((req, res) => {
    // CORS: movement.html may be served from a different origin than this
    // relay (e.g. a static file host), so the auto-detect probe below
    // needs an explicit allow-origin or the browser discards the response
    // before any JS on the page ever sees it.
    res.setHeader('Access-Control-Allow-Origin', '*');

    if (req.url === '/health') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: true, rooms: rooms.size }));
        return;
    }

    // ── Auto-detect probe — movement.html's detectMpLocalServerUrl()
    //    fetches this to confirm a relay is listening and to learn this
    //    machine's real LAN IP + port, so guests connect to an address
    //    that actually works from their device, not just 'localhost'. ──
    if (req.url === '/lan-ip') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: true, ips: getLanIps(), port: PORT }));
        return;
    }

    if (req.url === '/' || req.url === '/movement.html') {
        fs.readFile(MOVEMENT_HTML_PATH, (err, data) => {
            if (err) {
                res.writeHead(500, { 'Content-Type': 'text/plain' });
                res.end('movement.html not found next to server.js.\n');
                return;
            }
            res.writeHead(200, { 'Content-Type': 'text/html' });
            res.end(data);
        });
        return;
    }

    res.writeHead(404, { 'Content-Type': 'text/plain' });
    res.end('Not found. Try / for the game.\n');
});

const wss = new WebSocketServer({ server: httpServer });

// rooms: Map<roomCode, Map<playerId, { ws, name, lastState }>>
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
    for (const [pid, p] of room.entries()) {
        if (pid === exceptPlayerId) continue;
        if (p.ws.readyState === p.ws.OPEN) p.ws.send(payload);
    }
}

function roomRosterMsg(roomCode) {
    const room = rooms.get(roomCode);
    const players = room
        ? Array.from(room.entries()).map(([pid, p]) => ({ id: pid, name: p.name }))
        : [];
    return { type: 'roster', players };
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

            // ── Host creates a new room ──
            case 'create_room': {
                const code = generateRoomCode();
                playerId = makePlayerId();
                playerName = (msg.name || 'Player').toString().slice(0, 24);
                currentRoom = code;
                rooms.set(code, new Map([[playerId, { ws, name: playerName, lastState: null }]]));
                send(ws, { type: 'room_created', roomCode: code, playerId });
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
                if (room.size >= 8) {
                    send(ws, { type: 'join_error', reason: 'Room is full (8 players max).' });
                    return;
                }
                playerId = makePlayerId();
                playerName = (msg.name || 'Player').toString().slice(0, 24);
                currentRoom = code;
                room.set(playerId, { ws, name: playerName, lastState: null });

                send(ws, { type: 'joined', roomCode: code, playerId });

                // Tell the newcomer who is already here, including each
                // existing player's most recent known state, so they see
                // everyone else's current position/gear immediately
                // instead of waiting for that player's next state tick.
                const existing = Array.from(room.entries())
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
                if (!room || !room.has(playerId)) return;
                room.get(playerId).lastState = msg.state;
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
            room.delete(playerId);
            if (room.size === 0) {
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