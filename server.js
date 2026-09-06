const http = require('http');
const fs = require('fs');
const path = require('path');
const WebSocket = require('ws');

const port = Number(process.env.PORT) || 8080;
const groups = new Set();
const clientGroups = new Map();

const server = http.createServer((request, response) => {
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
    if (message.type === 'chat' && message.scope === 'group' && clientGroups.has(socket)) {
      const group = clientGroups.get(socket);
      group.forEach((member) => { if (member !== socket) send(member, { type: 'chat', scope: 'group', id: message.id, text: String(message.text || '') }); });
      return;
    }
    if (message.type !== 'join-group' || typeof message.peerId !== 'string') return;

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
