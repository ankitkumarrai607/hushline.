const http = require('http');
const fs = require('fs');
const path = require('path');
const WebSocket = require('ws');

const port = Number(process.env.PORT) || 8080;
const groups = new Set();
const clientGroups = new Map();
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
  if (request.url === '/' || request.url === '/index.html') {
    response.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
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
  if (!group) return;
  group.delete(socket);
  clientGroups.delete(socket);
  if (!group.size) groups.delete(group);
  group.forEach((member) => send(member, { type: 'peer-left', peerId: socket.peerId }));
}

socketServer.on('connection', (socket) => {
  socket.on('message', (rawMessage) => {
    let message;
    try { message = JSON.parse(rawMessage); } catch { return; }
		if (!message || typeof message.type !== 'string') return;
    if (message.type === 'chat' && message.scope === 'group' && clientGroups.has(socket)) {
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
