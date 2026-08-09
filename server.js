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
//   4. Let a dropped connection (WiFi blip, phone signal loss) rejoin
//      the SAME room as the SAME player within a short grace period,
//      instead of permanently booting them the instant the socket dies.
//
// The SAME server code runs both of the client's connection modes —
// which mode a player gets is entirely about WHERE this process runs.
// For Private mode, the client auto-detects its own address in the
// browser and never needs MP_LOCAL_SERVER_URL_FALLBACK at all in the
// normal case — that constant (and MP_PUBLIC_SERVER_URL for Public
// mode) live near the multiplayer script block in movement.html:
//   • Private — run this on one player's own machine (`npm start`),
//     everyone connects over the same WiFi/LAN. No internet connection
//     involved at all. See LAN-PLAY.md in this same folder.
//   • Public  — deploy this somewhere reachable from the internet (see
//     README.md in this same folder for step-by-step Render.com
//     instructions) so players who aren't on the same network can play
//     together.
//
// Public-specific hardening (all the limits/rate-limiting below): a
// Private game is only ever reachable by people already on the host's
// own WiFi, so none of this matters there. Public is reachable by
// anyone on the internet who has (or guesses) a room code, running on
// a single free-tier instance with finite memory/CPU — so this file
// caps how much of that any one visitor (or one bad actor) can consume:
//   • MAX_ROOMS              — total rooms that can exist at once.
//   • MAX_PLAYERS_PER_ROOM   — matches the client's existing 8-player
//                              room-full message.
//   • MAX_ROOMS_PER_IP       — stops one visitor from spinning up
//                              hundreds of rooms and exhausting MAX_ROOMS.
//   • MESSAGE_RATE_LIMIT_*   — caps how many messages/second a single
//                              connection can send before getting
//                              disconnected, so a runaway/malicious
//                              client can't flood the relay (and
//                              therefore every room's other players)
//                              with traffic.
// None of this is a real anti-abuse system (no auth, no captcha, no
// persistent bans) — it's just enough to keep one bad actor from taking
// the free instance down for everyone else, matching "sensible limits
// for a hobby relay," not "production-grade public infrastructure."
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

// ── Public-mode limits — see the file-header comment above for why
// each of these exists. Private/LAN play never comes close to any of
// them in normal use (a handful of friends on one WiFi network), so
// these are sized around "protect the free Public instance from abuse,"
// not around any expected Private-mode usage. ──
const MAX_ROOMS = 200;                 // total concurrent rooms, all players combined
const MAX_PLAYERS_PER_ROOM = 8;        // matches the existing client-side "Room is full" message
const MAX_ROOMS_PER_IP = 5;            // rooms one IP can be HOSTING at once
const MESSAGE_RATE_LIMIT_COUNT = 60;   // max messages...
const MESSAGE_RATE_LIMIT_WINDOW_MS = 1000; // ...per this many ms, per connection
const RECONNECT_GRACE_MS = 20000;      // how long a dropped player's seat is held before it's given up

// ── Best-effort LAN IP lookup, printed at startup purely to make Private
// mode setup easier — this is NOT used anywhere in the relay logic
// itself, just a convenience so whoever runs this doesn't have to dig
// through `ipconfig`/`ifconfig` output to find the address their
// friends need to type into MP_LOCAL_SERVER_URL_FALLBACK. ──
function getLikelyLanIPs(){
    const nets = os.networkInterfaces();
    const out = [];
    for(const name of Object.keys(nets)){
        for(const net of (nets[name] || [])){
            if(net.family === 'IPv4' && !net.internal) out.push(net.address);
        }
    }
    return out;
}

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

// rooms: Map<roomCode, Map<clientId, {
//   ws, name, lastState, disconnectedAt (ms epoch or null),
//   graceTimer (setTimeout handle or null)
// }>>
// Keyed by the STABLE per-device clientId (see MP_CLIENT_ID in
// movement.html), not a fresh random id per connection — that's what
// makes reconnecting into the same seat possible: the same device
// rejoining under the same clientId is recognized as the same player
// rather than treated as a brand new one.
const rooms = new Map();
// hostingIpCounts: Map<ip, count of rooms currently hosted by that ip>
// — only tracked for create_room (hosting), not join_room, so a public
// room being popular and getting many joiners never counts against
// anyone's limit; only spinning up NEW rooms does.
const hostingIpCounts = new Map();
// roomOwnerIp: Map<roomCode, ip> — so hostingIpCounts can be decremented
// correctly when a room is destroyed, regardless of who deleted it.
const roomOwnerIp = new Map();

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
        if (p.ws && p.ws.readyState === p.ws.OPEN) p.ws.send(payload);
    }
}

function getClientIp(req) {
    // Trust X-Forwarded-For's first hop when present (true behind
    // Render's proxy and most hosts that terminate TLS in front of the
    // app) — falls back to the raw socket address for local/LAN runs
    // where there's no proxy in front at all.
    const fwd = req.headers['x-forwarded-for'];
    if (fwd) return fwd.split(',')[0].trim();
    return req.socket.remoteAddress || 'unknown';
}

function destroyRoom(roomCode) {
    const room = rooms.get(roomCode);
    if (room) {
        for (const p of room.values()) {
            if (p.graceTimer) clearTimeout(p.graceTimer);
        }
    }
    rooms.delete(roomCode);
    const ownerIp = roomOwnerIp.get(roomCode);
    if (ownerIp) {
        const n = (hostingIpCounts.get(ownerIp) || 1) - 1;
        if (n <= 0) hostingIpCounts.delete(ownerIp);
        else hostingIpCounts.set(ownerIp, n);
    }
    roomOwnerIp.delete(roomCode);
}

wss.on('connection', (ws, req) => {
    const clientIp = getClientIp(req);
    let currentRoom = null;
    let myClientId = null;
    let playerName = 'Player';

    ws.isAlive = true;
    ws.on('pong', () => { ws.isAlive = true; });

    // ── Per-connection rate limiting ─────────────────────────────────
    // Sliding-window-ish counter: reset every MESSAGE_RATE_LIMIT_WINDOW_MS,
    // disconnect anyone who blows through MESSAGE_RATE_LIMIT_COUNT inside
    // one window. Cheap and approximate on purpose — this only needs to
    // stop a runaway/malicious client from flooding the relay, not
    // provide precise fairness.
    let msgCount = 0;
    let msgWindowStart = Date.now();

    ws.on('message', (raw) => {
        const now = Date.now();
        if (now - msgWindowStart > MESSAGE_RATE_LIMIT_WINDOW_MS) {
            msgWindowStart = now;
            msgCount = 0;
        }
        msgCount++;
        if (msgCount > MESSAGE_RATE_LIMIT_COUNT) {
            // Deliberately no error message back — a client hitting this
            // is either broken or hostile, and this connection is about
            // to be closed either way.
            ws.terminate();
            return;
        }

        let msg;
        try {
            msg = JSON.parse(raw);
        } catch {
            return; // ignore malformed input
        }

        switch (msg.type) {

            // ── Host creates a new room ──
            case 'create_room': {
                if (rooms.size >= MAX_ROOMS) {
                    send(ws, { type: 'join_error', reason: 'The public server is full right now (too many active rooms). Try again in a bit, or use Private mode.' });
                    return;
                }
                const hostedByThisIp = hostingIpCounts.get(clientIp) || 0;
                if (hostedByThisIp >= MAX_ROOMS_PER_IP) {
                    send(ws, { type: 'join_error', reason: 'You already have several rooms open. Close one before hosting another.' });
                    return;
                }
                const code = generateRoomCode();
                myClientId = (msg.clientId || '').toString().slice(0, 64) || crypto.randomBytes(6).toString('hex');
                playerName = (msg.name || 'Player').toString().slice(0, 24);
                currentRoom = code;
                rooms.set(code, new Map([[myClientId, { ws, name: playerName, lastState: null, disconnectedAt: null, graceTimer: null }]]));
                hostingIpCounts.set(clientIp, hostedByThisIp + 1);
                roomOwnerIp.set(code, clientIp);
                send(ws, { type: 'room_created', roomCode: code, playerId: myClientId });
                break;
            }

            // ── A guest joins an existing room by code — also the
            //    RECONNECT path: if msg.clientId already has a seat in
            //    this room (even mid-grace-period after a drop), this
            //    is the same player resuming, not a new join. ──
            case 'join_room': {
                const code = (msg.roomCode || '').toString().toUpperCase().trim();
                const room = rooms.get(code);
                if (!room) {
                    send(ws, { type: 'join_error', reason: 'Room not found. Check the code and try again.' });
                    return;
                }
                const incomingClientId = (msg.clientId || '').toString().slice(0, 64) || crypto.randomBytes(6).toString('hex');
                playerName = (msg.name || 'Player').toString().slice(0, 24);
                currentRoom = code;
                myClientId = incomingClientId;

                const existing = room.get(incomingClientId);
                if (existing) {
                    // ── Reconnect: same clientId already has a seat
                    //    (either still fully connected — a duplicate
                    //    join from the same device, e.g. a double-click
                    //    — or disconnected and inside its grace period).
                    //    Either way, take over that seat rather than
                    //    creating a second one. ──
                    if (existing.graceTimer) { clearTimeout(existing.graceTimer); existing.graceTimer = null; }
                    if (existing.ws && existing.ws !== ws && existing.ws.readyState === existing.ws.OPEN) {
                        // A stale earlier connection for this same
                        // clientId is somehow still open (e.g. the old
                        // tab never actually closed) — close it so
                        // there's only ever one live socket per seat.
                        try { existing.ws.close(); } catch {}
                    }
                    existing.ws = ws;
                    existing.name = playerName;
                    existing.disconnectedAt = null;
                    room.set(incomingClientId, existing);

                    send(ws, { type: 'joined', roomCode: code, playerId: incomingClientId });
                    const existingPlayers = Array.from(room.entries())
                        .filter(([cid]) => cid !== incomingClientId)
                        .map(([cid, p]) => ({ id: cid, name: p.name, state: p.lastState }));
                    send(ws, { type: 'existing_players', players: existingPlayers });
                    broadcastToRoom(code, { type: 'player_reconnected', id: incomingClientId, name: playerName }, incomingClientId);
                    break;
                }

                // ── Genuinely new player ──
                if (room.size >= MAX_PLAYERS_PER_ROOM) {
                    send(ws, { type: 'join_error', reason: 'Room is full (8 players max).' });
                    return;
                }
                room.set(incomingClientId, { ws, name: playerName, lastState: null, disconnectedAt: null, graceTimer: null });

                send(ws, { type: 'joined', roomCode: code, playerId: incomingClientId });

                // Tell the newcomer who is already here, including each
                // existing player's most recent known state, so they see
                // everyone else's current position/gear immediately
                // instead of waiting for that player's next state tick.
                const existingList = Array.from(room.entries())
                    .filter(([cid]) => cid !== incomingClientId)
                    .map(([cid, p]) => ({ id: cid, name: p.name, state: p.lastState }));
                send(ws, { type: 'existing_players', players: existingList });

                // Tell everyone else a new player joined.
                broadcastToRoom(code, { type: 'player_joined', id: incomingClientId, name: playerName }, incomingClientId);
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
                room.get(myClientId).lastState = msg.state;
                broadcastToRoom(currentRoom, { type: 'state', id: myClientId, state: msg.state }, myClientId);
                break;
            }

            // ── One-off events that aren't part of continuous state:
            //    attack swings, ability activations, damage numbers, chat
            //    pings, etc. Forwarded the same way as state, but never
            //    stored, since these are transient. ──
            case 'event': {
                if (!currentRoom || !myClientId) return;
                broadcastToRoom(currentRoom, { type: 'event', id: myClientId, event: msg.event }, myClientId);
                break;
            }

            case 'leave_room': {
                cleanupPlayer(true); // explicit leave — no grace period, drop the seat now
                break;
            }
        }
    });

    // ── A dropped socket (WiFi blip, phone signal loss, tab killed)
    //    does NOT immediately free the seat — it starts a grace-period
    //    timer instead, so a client that reconnects within
    //    RECONNECT_GRACE_MS (see joinGame()'s auto-retry on the client)
    //    resumes the exact same room/seat instead of being treated as a
    //    new player. Only an EXPLICIT leave_room, or the grace period
    //    actually running out, frees the seat for good. ──
    function cleanupPlayer(explicit) {
        if (!currentRoom || !myClientId) return;
        const room = rooms.get(currentRoom);
        if (!room) { currentRoom = null; myClientId = null; return; }
        const seat = room.get(myClientId);
        // If this socket isn't the seat's current live connection
        // anymore (e.g. a reconnect already replaced it), don't tear
        // anything down on its behalf — it's already been superseded.
        if (seat && seat.ws !== ws) { currentRoom = null; myClientId = null; return; }

        // Capture these BEFORE currentRoom/myClientId get reset to null
        // below — cleanupPlayer returns (and nulls both) synchronously,
        // well before the setTimeout callback below ever runs. Reading
        // the outer currentRoom/myClientId variables from inside that
        // callback would see them already null by then (closures
        // capture the VARIABLE, not a snapshot of its value at the time
        // the closure was created) — these consts are what actually
        // fixes that.
        const roomCodeAtDrop = currentRoom;
        const clientIdAtDrop = myClientId;

        if (explicit || !seat) {
            room.delete(clientIdAtDrop);
            if (room.size === 0) destroyRoom(roomCodeAtDrop);
            else broadcastToRoom(roomCodeAtDrop, { type: 'player_left', id: clientIdAtDrop });
        } else {
            // Hold the seat open for RECONNECT_GRACE_MS. Other players
            // aren't told anything yet — from their side, a brief drop
            // that reconnects in time should look like nothing happened,
            // not a leave-then-rejoin flicker.
            seat.disconnectedAt = Date.now();
            seat.graceTimer = setTimeout(() => {
                const stillThere = room.get(clientIdAtDrop);
                // Only actually remove if it's still the same
                // disconnected seat — a reconnect since would have
                // cleared graceTimer and this timer's closure would be
                // stale.
                if (stillThere && stillThere.disconnectedAt) {
                    room.delete(clientIdAtDrop);
                    if (room.size === 0) destroyRoom(roomCodeAtDrop);
                    else broadcastToRoom(roomCodeAtDrop, { type: 'player_left', id: clientIdAtDrop });
                }
            }, RECONNECT_GRACE_MS);
        }
        currentRoom = null;
        myClientId = null;
    }

    ws.on('close', () => cleanupPlayer(false));
    ws.on('error', () => cleanupPlayer(false));
});

// ── Drop dead connections (e.g. laptop closed without a clean close
//    frame) so rooms don't accumulate ghost players forever. Note this
//    triggers the SAME grace-period path as any other drop (via the
//    'close' handler above once .terminate() fires it) — a laptop lid
//    closed and reopened within RECONNECT_GRACE_MS still resumes the
//    same seat like any other reconnect. ──
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
    // ── Print LAN URLs for Private mode ──────────────────────────────
    // Whoever's hosting a Private game copies one of these into
    // MP_LOCAL_SERVER_URL_FALLBACK near the top of the multiplayer script block
    // in movement.html (everyone playing needs the SAME url, pointed at
    // THIS machine). If nothing prints here, this machine either has no
    // active network connection or is on a network that hides peers from
    // each other (some public/guest WiFi) — LAN play needs a normal
    // home/office WiFi or wired connection to work.
    const lanIPs = getLikelyLanIPs();
    if(lanIPs.length){
        console.log('\nFor Private (LAN) play, set MP_LOCAL_SERVER_URL_FALLBACK in movement.html to one of:');
        lanIPs.forEach(ip => console.log(`  ws://${ip}:${PORT}`));
        console.log('(Use whichever one matches the WiFi/network your friends are also on.)\n');
    } else {
        console.log('\nCould not detect a LAN IP on this machine — Private mode needs one to connect to.');
        console.log('Make sure this machine is connected to a WiFi/network your friends are also on.\n');
    }
    console.log(`Public-mode limits: max ${MAX_ROOMS} rooms, ${MAX_PLAYERS_PER_ROOM} players/room, ${MAX_ROOMS_PER_IP} hosted rooms/IP, ${MESSAGE_RATE_LIMIT_COUNT} msgs/${MESSAGE_RATE_LIMIT_WINDOW_MS}ms/connection, ${RECONNECT_GRACE_MS/1000}s reconnect grace.`);
});