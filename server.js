const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer, WebSocket } = require('ws');
const PORT = process.env.PORT || 3000;
const rooms = new Map();
const server = http.createServer((req,res)=>{
  const file = path.join(__dirname, 'index.html');
  res.writeHead(200, {'content-type':'text/html; charset=utf-8','cache-control':'no-store'});
  fs.createReadStream(file).pipe(res);
});
const wss = new WebSocketServer({noServer:true, maxPayload:16*1024});
server.on('upgrade',(req,socket,head)=>{
  if(req.url !== '/ws'){ socket.destroy(); return; }
  wss.handleUpgrade(req,socket,head,ws=>wss.emit('connection',ws,req));
});
function send(ws,obj){if(ws.readyState===WebSocket.OPEN)ws.send(JSON.stringify(obj));}
function code(){let s='';const chars='ABCDEFGHJKLMNPQRSTUVWXYZ23456789';for(let i=0;i<6;i++)s+=chars[Math.floor(Math.random()*chars.length)];return s;}
function leave(ws){if(!ws.room)return;const room=rooms.get(ws.room);if(room){room.delete(ws);for(const peer of room)send(peer,{type:'peer-left'});if(room.size===0)rooms.delete(ws.room);}ws.room=null;}
wss.on('connection',ws=>{
 ws.room=null;ws.on('message',raw=>{let m;try{m=JSON.parse(raw.toString())}catch{return;}
  if(m.action==='create'){
   leave(ws);let id;do{id=code()}while(rooms.has(id));rooms.set(id,new Set([ws]));ws.room=id;send(ws,{type:'created',room:id});return;
  }
  if(m.action==='join'){
   const id=String(m.room||'').toUpperCase();const room=rooms.get(id);
   if(!room){send(ws,{type:'error',message:'رمز الغرفة غير موجود أو انتهت صلاحيته.'});return;}
   if(room.size>=2){send(ws,{type:'error',message:'الغرفة ممتلئة؛ الحد الأقصى لاعبان.'});return;}
   leave(ws);room.add(ws);ws.room=id;send(ws,{type:'joined',room:id});for(const peer of room)if(peer!==ws)send(peer,{type:'peer-joined'});return;
  }
  if(m.action==='data'&&ws.room){const room=rooms.get(ws.room);if(room)for(const peer of room)if(peer!==ws)send(peer,{type:'data',payload:m.payload});}
 });
 ws.on('close',()=>leave(ws));ws.on('error',()=>leave(ws));
});
server.listen(PORT,()=>console.log(`Alkhattaf multiplayer server listening on ${PORT}`));
