const http = require('http');
const fs = require('fs');
const path = require('path');
const { URL } = require('url');
const WebSocket = require('ws');

const port = Number(process.env.PORT) || 8080;
const groups = new Set();
const clientGroups = new Map();
const privateRooms = new Map();
const clientPrivateRooms = new Map();
const requestLimits = new Map();
const maxMessageLength = 1000;
const maxJoinsPerMinute = 10;
const maxMessagesPerMinute = 60;

function securityHeaders(request, response) {
  response.setHeader('X-Content-Type-Options', 'nosniff');
  response.setHeader('X-Frame-Options', 'DENY');
  response.setHeader('Referrer-Policy', 'no-referrer');
  response.setHeader('Permissions-Policy', 'camera=(), microphone=(), geolocation=()');
  response.setHeader('Content-Security-Policy', "default-src 'self'; script-src 'self' 'unsafe-inline' https://unpkg.com; style-src 'self' 'unsafe-inline'; connect-src 'self' ws: wss: https://unpkg.com https://*.peerjs.com; img-src 'self' data:; base-uri 'self'; frame-ancestors 'none'");
  if (request.headers['x-forwarded-proto'] === 'https') response.setHeader('Strict-Transport-Security', 'max-age=31536000; includeSubDomains');
}

function allowedRate(socket, kind, limit) {
  const key = `${socket._socket.remoteAddress}:${kind}`;
  const now = Date.now();
  const entry = requestLimits.get(key);
  if (!entry || now - entry.startedAt >= 60000) {
    requestLimits.set(key, { startedAt: now, count: 1 });
    return true;
  }
  entry.count += 1;
  return entry.count <= limit;
}

const server = http.createServer((request, response) => {
  securityHeaders(request, response);
  const requestPath = new URL(request.url, 'http://localhost').pathname;
  if (requestPath === '/' || requestPath === '/index.html') {
    response.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    response.setHeader('Cache-Control', 'no-store, max-age=0');
    response.end(fs.readFileSync(path.join(__dirname, 'index.html')));
    return;
  }
  response.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
  response.end('Not found');
});

const socketServer = new WebSocket.Server({ server });

function send(socket, message) {
  if (socket.readyState === WebSocket.OPEN) socket.send(JSON.stringify(message));
}

function removeClient(socket) {
  const group = clientGroups.get(socket);
  if (group) {
    group.delete(socket);
    clientGroups.delete(socket);
    if (!group.size) groups.delete(group);
    group.forEach((member) => send(member, { type: 'peer-left', count: group.size }));
  }
  const privateRoom = clientPrivateRooms.get(socket);
  if (privateRoom) {
    privateRoom.delete(socket);
    clientPrivateRooms.delete(socket);
    if (!privateRoom.size) privateRooms.delete(socket.privateCode);
    privateRoom.forEach((member) => send(member, { type: 'private-left', count: privateRoom.size }));
  }
}

socketServer.on('connection', (socket) => {
  socket.on('message', (rawMessage) => {
    let message;
    try { message = JSON.parse(rawMessage); } catch { return; }
		if (!message || typeof message.type !== 'string') return;
    if (message.type === 'join-private' && typeof message.code === 'string') {
      if (clientPrivateRooms.has(socket) || clientGroups.has(socket) || message.code.length > 64) return;
      const code = message.code.trim().toLowerCase();
      const room = privateRooms.get(code) || new Set();
      if (room.size >= 2) { send(socket, { type: 'private-full' }); return; }
      room.add(socket);
      privateRooms.set(code, room);
      clientPrivateRooms.set(socket, room);
      socket.privateCode = code;
      send(socket, { type: 'private-joined', count: room.size });
      room.forEach((member) => { if (member !== socket) send(member, { type: 'private-joined', count: room.size }); });
      return;
    }
    if ((message.type === 'chat' || message.type === 'typing') && message.scope === 'private' && clientPrivateRooms.has(socket)) {
      if (message.type === 'typing') {
        clientPrivateRooms.get(socket).forEach((member) => { if (member !== socket) send(member, { type: 'typing', active: Boolean(message.active) }); });
        return;
      }
      if (!allowedRate(socket, 'messages', maxMessagesPerMinute) || typeof message.text !== 'string' || message.text.length > maxMessageLength) return;
      clientPrivateRooms.get(socket).forEach((member) => { if (member !== socket) send(member, { type: 'chat', scope: 'private', id: message.id, text: message.text }); });
      return;
    }
    if ((message.type === 'chat' || message.type === 'typing') && message.scope === 'group' && clientGroups.has(socket)) {
      if (message.type === 'typing') {
        const group = clientGroups.get(socket);
        group.forEach((member) => { if (member !== socket) send(member, { type: 'typing', active: Boolean(message.active) }); });
        return;
      }
      if (!allowedRate(socket, 'messages', maxMessagesPerMinute) || typeof message.text !== 'string' || message.text.length > maxMessageLength) return;
      const group = clientGroups.get(socket);
      group.forEach((member) => { if (member !== socket) send(member, { type: 'chat', scope: 'group', id: message.id, text: String(message.text || '') }); });
      return;
    }
    if (message.type !== 'join-group' || typeof message.peerId !== 'string') return;
		if (!allowedRate(socket, 'joins', maxJoinsPerMinute) || message.peerId.length > 100 || clientGroups.has(socket)) return;

    socket.peerId = message.peerId;
    let group = [...groups].find((candidate) => candidate.size < 5);
    if (!group) {
      group = new Set();
      groups.add(group);
    }

    const existingPeers = [...group].map((member) => member.peerId);
    group.add(socket);
    clientGroups.set(socket, group);
    send(socket, { type: 'matched', peers: existingPeers, count: group.size });
    group.forEach((member) => {
      if (member !== socket) send(member, { type: 'peer-joined', peerId: socket.peerId, count: group.size });
    });
  });
  socket.on('close', () => removeClient(socket));
  socket.on('error', () => removeClient(socket));
});

server.listen(port, '0.0.0.0', () => {
  console.log(`Hushline running at http://localhost:${port}`);
});
