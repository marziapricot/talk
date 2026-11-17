const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const page = path.join(__dirname, 'public', 'index.html');

const server = http.createServer((req, res) => {
  fs.readFile(page, (err, data) => {
    if (err) { res.writeHead(500); return res.end('error'); }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(data);
  });
});

const wss = new WebSocketServer({ server });
const waiting = new Map(); // 密語 -> 等待中的 ws（空字串 = 隨機配對）
const board = [];          // 尋人啟事

const send = (ws, obj) => { if (ws && ws.readyState === 1) ws.send(JSON.stringify(obj)); };
const broadcastOnline = () => wss.clients.forEach(c => send(c, { t: 'online', n: wss.clients.size }));

function unqueue(ws) {
  for (const [k, v] of waiting) if (v === ws) waiting.delete(k);
}

function leave(ws) {
  unqueue(ws);
  const p = ws.partner;
  ws.partner = null;
  if (p) { p.partner = null; send(p, { t: 'left' }); }
}

function find(ws, code) {
  leave(ws);
  code = String(code || '').trim().slice(0, 30);
  const other = waiting.get(code);
  if (other && other !== ws && other.readyState === 1) {
    waiting.delete(code);
    ws.partner = other; other.partner = ws;
    send(ws, { t: 'matched', code });
    send(other, { t: 'matched', code });
  } else {
    waiting.set(code, ws);
    send(ws, { t: 'waiting' });
  }
}

wss.on('connection', ws => {
  ws.partner = null;
  broadcastOnline();
  send(ws, { t: 'board', list: board });

  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw); } catch { return; }
    if (m.t === 'find') find(ws, m.code);
    else if (m.t === 'leave') leave(ws);
    else if (m.t === 'msg' && ws.partner) {
      const text = String(m.text || '').slice(0, 500);
      if (text) send(ws.partner, { t: 'msg', text });
    } else if (m.t === 'typing' && ws.partner) send(ws.partner, { t: 'typing' });
    else if (m.t === 'post') {
      const text = String(m.text || '').trim().slice(0, 200);
      if (!text) return;
      board.unshift({ text, time: Date.now() });
      if (board.length > 30) board.pop();
      wss.clients.forEach(c => send(c, { t: 'board', list: board }));
    }
  });

  ws.on('close', () => { leave(ws); broadcastOnline(); });
});

server.listen(PORT, () => console.log('TALK 已啟動：http://localhost:' + PORT));
