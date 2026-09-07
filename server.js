const { WebSocketServer, WebSocket } = require('ws');

const PORT = process.env.PORT || 8080;
const wss = new WebSocketServer({ port: PORT });

// videoId -> { title: string, clients: Set<WebSocket>, lastSoundTime: number, activePrediction: object|null }
const videoRooms = new Map();
const videoArchives = new Map();
const allClients = new Set();

const SOUND_COOLDOWN_MS = 20000;

wss.on('connection', (ws) => {
  allClients.add(ws);
  let currentVideoId = null;

  broadcastGlobalStats();

  ws.on('message', (data) => {
    try {
      const payload = JSON.parse(data);

      if (payload.type === 'JOIN') {
        if (currentVideoId && videoRooms.has(currentVideoId)) {
          const prev = videoRooms.get(currentVideoId);
          prev.clients.delete(ws);
          if (prev.clients.size === 0) videoRooms.delete(currentVideoId);
        }

        currentVideoId = payload.videoId;
        if (!videoRooms.has(currentVideoId)) {
          videoRooms.set(currentVideoId, {
            title: payload.videoTitle || 'YouTube Video',
            clients: new Set(),
            lastSoundTime: 0,
            activePrediction: null
          });
        }
        const room = videoRooms.get(currentVideoId);
        room.clients.add(ws);
        if (payload.videoTitle) room.title = payload.videoTitle;

        if (!videoArchives.has(currentVideoId)) {
          videoArchives.set(currentVideoId, []);
        }
        ws.send(JSON.stringify({
          type: 'ARCHIVE_SYNC',
          messages: videoArchives.get(currentVideoId)
        }));

        // Send active prediction if one is running in this room
        if (room.activePrediction) {
          ws.send(JSON.stringify({
            type: 'PREDICTION_SYNC',
            prediction: sanitizePrediction(room.activePrediction)
          }));
        }

        broadcastGlobalStats();
      }

      if (payload.type === 'CHAT') {
        const messagePayload = {
          type: 'CHAT',
          scope: payload.scope || 'video',
          user: payload.user || 'Guest',
          text: payload.text,
          videoTime: payload.videoTime || 0,
          time: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
        };

        if (payload.scope === 'global') {
          broadcastSet(allClients, messagePayload);
        } else if (currentVideoId && videoRooms.has(currentVideoId)) {
          const archive = videoArchives.get(currentVideoId);
          if (archive) {
            archive.push(messagePayload);
            if (archive.length > 2000) archive.shift();
          }
          broadcastSet(videoRooms.get(currentVideoId).clients, messagePayload);
        }
      }

      // SOUNDBOARD DROP
      if (payload.type === 'SOUND_DROP') {
        if (!currentVideoId || !videoRooms.has(currentVideoId)) return;
        const room = videoRooms.get(currentVideoId);
        const now = Date.now();

        if (now - room.lastSoundTime < SOUND_COOLDOWN_MS) {
          const remaining = Math.ceil((SOUND_COOLDOWN_MS - (now - room.lastSoundTime)) / 1000);
          ws.send(JSON.stringify({
            type: 'SYSTEM',
            text: `⏳ Soundboard cooling down (${remaining}s remaining)`
          }));
          return;
        }

        room.lastSoundTime = now;
        broadcastSet(room.clients, {
          type: 'PLAY_SOUND',
          soundId: payload.soundId,
          user: payload.user
        });
        broadcastSet(room.clients, {
          type: 'SYSTEM',
          text: `🔊 ${payload.user} dropped ${payload.soundLabel}!`
        });
      }

      // PREDICTION MARKET: CREATE
      if (payload.type === 'PREDICTION_CREATE') {
        if (!currentVideoId || !videoRooms.has(currentVideoId)) return;
        const room = videoRooms.get(currentVideoId);
        if (room.activePrediction) {
          ws.send(JSON.stringify({ type: 'SYSTEM', text: 'A prediction is already active in this room.' }));
          return;
        }

        room.activePrediction = {
          creator: payload.user,
          question: payload.question,
          optA: payload.optA || 'YES',
          optB: payload.optB || 'NO',
          poolA: 0,
          poolB: 0,
          bets: new Map(), // ws -> { side, amount, user }
          endsAt: Date.now() + 45000,
          resolved: false
        };

        broadcastSet(room.clients, {
          type: 'PREDICTION_START',
          prediction: sanitizePrediction(room.activePrediction)
        });

        // Auto-lock betting after 45s
        setTimeout(() => {
          if (room.activePrediction && !room.activePrediction.resolved) {
            broadcastSet(room.clients, {
              type: 'PREDICTION_LOCKED',
              prediction: sanitizePrediction(room.activePrediction)
            });
          }
        }, 45000);
      }

      // PREDICTION MARKET: BET
      if (payload.type === 'PREDICTION_BET') {
        if (!currentVideoId || !videoRooms.has(currentVideoId)) return;
        const room = videoRooms.get(currentVideoId);
        const pred = room.activePrediction;
        if (!pred || pred.resolved || Date.now() > pred.endsAt) return;
        if (pred.bets.has(ws)) return; // 1 bet per user

        const amt = Math.max(10, Math.min(100, parseInt(payload.amount, 10) || 10));
        if (payload.side === 'A') pred.poolA += amt;
        else pred.poolB += amt;

        pred.bets.set(ws, { side: payload.side, amount: amt, user: payload.user });

        broadcastSet(room.clients, {
          type: 'PREDICTION_UPDATE',
          poolA: pred.poolA,
          poolB: pred.poolB
        });
      }

      // PREDICTION MARKET: RESOLVE
      if (payload.type === 'PREDICTION_RESOLVE') {
        if (!currentVideoId || !videoRooms.has(currentVideoId)) return;
        const room = videoRooms.get(currentVideoId);
        const pred = room.activePrediction;
        if (!pred || pred.resolved) return;
        if (pred.creator !== payload.user) return; // Only creator resolves

        pred.resolved = true;
        const winningSide = payload.winningSide; // 'A' or 'B'
        const totalPool = pred.poolA + pred.poolB;
        const winningPool = winningSide === 'A' ? pred.poolA : pred.poolB;

        // Calculate and distribute payouts
        for (const [clientWs, bet] of pred.bets.entries()) {
          if (clientWs.readyState === WebSocket.OPEN) {
            if (bet.side === winningSide && winningPool > 0) {
              const share = Math.floor((bet.amount / winningPool) * totalPool);
              clientWs.send(JSON.stringify({
                type: 'PREDICTION_WIN',
                payout: share,
                message: `🎉 You won ${share} Sparks on [${winningSide === 'A' ? pred.optA : pred.optB}]!`
              }));
            }
          }
        }

        broadcastSet(room.clients, {
          type: 'PREDICTION_END',
          winnerLabel: winningSide === 'A' ? pred.optA : pred.optB
        });

        room.activePrediction = null;
      }

      // LIVE EMOTE RAID
      if (payload.type === 'START_RAID') {
        broadcastSet(allClients, {
          type: 'RAID_ALERT',
          targetVideoId: payload.targetVideoId,
          targetTitle: payload.targetTitle,
          initiator: payload.user
        });
      }
    } catch (err) {
      console.error('Socket error:', err);
    }
  });

  ws.on('close', () => {
    allClients.delete(ws);
    if (currentVideoId && videoRooms.has(currentVideoId)) {
      const room = videoRooms.get(currentVideoId);
      room.clients.delete(ws);
      if (room.activePrediction) room.activePrediction.bets.delete(ws);
      if (room.clients.size === 0) videoRooms.delete(currentVideoId);
    }
    broadcastGlobalStats();
  });
});

function sanitizePrediction(pred) {
  return {
    creator: pred.creator,
    question: pred.question,
    optA: pred.optA,
    optB: pred.optB,
    poolA: pred.poolA,
    poolB: pred.poolB,
    endsAt: pred.endsAt,
    locked: Date.now() > pred.endsAt
  };
}

function broadcastGlobalStats() {
  const topVideos = [];
  for (const [id, room] of videoRooms.entries()) {
    topVideos.push({
      videoId: id,
      title: room.title,
      viewers: room.clients.size
    });
  }
  topVideos.sort((a, b) => b.viewers - a.viewers);

  const stats = JSON.stringify({
    type: 'STATS',
    globalCount: allClients.size,
    topVideos: topVideos.slice(0, 10)
  });

  for (const client of allClients) {
    if (client.readyState === WebSocket.OPEN) client.send(stats);
  }
}

function broadcastSet(clientSet, messageObj) {
  if (!clientSet) return;
  const msg = JSON.stringify(messageObj);
  for (const client of clientSet) {
    if (client.readyState === WebSocket.OPEN) client.send(msg);
  }
}

console.log(`LiveChatside server listening on ws://localhost:${PORT}`);