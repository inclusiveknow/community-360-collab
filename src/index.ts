// Community 360° Builder — Collaboration Server v3
// Cloudflare Worker + Durable Object
// Fix: Uses ctx.acceptWebSocket() (hibernation API) so WebSocket
// connections survive DO hibernation and don't get silently dropped.

interface Env {
  ROOM: DurableObjectNamespace;
}

interface SceneItem {
  id: number;
  type: string;
  name: string;
  src: string;
  lon: number; lat: number; depth: number; scale: number; opacity: number;
  rx: number; ry: number; rz: number;
  bb: boolean; loop: boolean; vis: boolean;
  glow: boolean; glowColor: string; pulse: boolean; float: boolean;
  interact: string; meta: string; isolated: boolean;
  txtSize?: number; txtColor?: string; txtBg?: string; txtBgOp?: number; txtTransp?: boolean;
  [key: string]: any;
}

interface ThreadData { fromId: number; toId: number; color: string; width: number; glow: number; }

interface RoomState { items: SceneItem[]; threads: ThreadData[]; envUrl: string; nextId: number; }

interface PeerMeta { name: string; role: "editor" | "viewer"; color: string; joinedAt: number; }

const PEER_COLORS = ["#4ecdc4","#a78bfa","#f472b6","#fb923c","#4ade80","#60a5fa","#facc15","#f87171"];

export class Room {
  state: DurableObjectState;
  scene: RoomState = { items: [], threads: [], envUrl: "", nextId: 1 };
  peerIdCounter = 0;

  constructor(state: DurableObjectState) {
    this.state = state;
    state.blockConcurrencyWhile(async () => {
      const saved = await state.storage.get<RoomState>("scene");
      if (saved) this.scene = saved;
      const ctr = await state.storage.get<number>("peerIdCounter");
      if (ctr) this.peerIdCounter = ctr;
    });
  }

  async saveScene() {
    await this.state.storage.put("scene", this.scene);
  }

  async deferSave() {
    try {
      await this.state.storage.setAlarm(Date.now() + 600);
    } catch {
      await this.state.storage.put("scene", this.scene);
    }
  }

  async alarm() {
    await this.state.storage.put("scene", this.scene);
  }

  // ── Helper: get peer metadata from WebSocket tags ──
  getPeerMeta(ws: WebSocket): PeerMeta | null {
    try {
      const tags = this.state.getTags(ws);
      const metaTag = tags.find((t: string) => t.startsWith("meta:"));
      if (!metaTag) return null;
      return JSON.parse(metaTag.slice(5));
    } catch { return null; }
  }

  getPeerId(ws: WebSocket): string | null {
    try {
      const tags = this.state.getTags(ws);
      const idTag = tags.find((t: string) => t.startsWith("id:"));
      return idTag ? idTag.slice(3) : null;
    } catch { return null; }
  }

  broadcast(message: string, excludeId?: string) {
    for (const ws of this.state.getWebSockets()) {
      const id = this.getPeerId(ws);
      if (id === excludeId) continue;
      try { ws.send(message); } catch { /* hibernation API handles cleanup */ }
    }
  }

  broadcastPeerList() {
    const peers = this.state.getWebSockets().map((ws: WebSocket) => {
      const id = this.getPeerId(ws);
      const meta = this.getPeerMeta(ws);
      return { id, ...meta };
    }).filter(p => p.id && p.name);
    return peers;
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const corsHeaders = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, Upgrade",
    };

    if (request.method === "OPTIONS") return new Response(null, { headers: corsHeaders });

    if (request.method === "GET" && url.pathname.endsWith("/info")) {
      const peers = this.broadcastPeerList();
      return Response.json({ peers, itemCount: this.scene.items.length }, { headers: corsHeaders });
    }

    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("Expected WebSocket", { status: 426 });
    }

    const name = url.searchParams.get("name") || "Anonymous";
    const role = url.searchParams.get("role") === "viewer" ? "viewer" : "editor";
    const peerId = `p${++this.peerIdCounter}`;
    await this.state.storage.put("peerIdCounter", this.peerIdCounter);
    const color = PEER_COLORS[this.peerIdCounter % PEER_COLORS.length];
    const meta: PeerMeta = { name, role, color, joinedAt: Date.now() };

    // Use hibernation API — survives DO sleep, no silent drops
    const pair = new WebSocketPair();
    const [client, server] = [pair[0], pair[1]];
    this.state.acceptWebSocket(server, [`id:${peerId}`, `meta:${JSON.stringify(meta)}`]);

    // Send full scene state to new peer
    server.send(JSON.stringify({
      type: "init", peerId, role, color,
      scene: this.scene,
      peers: this.broadcastPeerList(),
    }));

    // Announce join to others
    this.broadcast(JSON.stringify({ type: "peer-join", peerId, name, role, color }), peerId);

    return new Response(null, { status: 101, webSocket: client });
  }

  // ── Hibernation API message handler ──
  async webSocketMessage(ws: WebSocket, message: string) {
    try {
      const msg = JSON.parse(message);
      const peerId = this.getPeerId(ws);
      const meta = this.getPeerMeta(ws);
      if (!peerId || !meta) return;

      // Viewers can only send presence
      if (meta.role === "viewer" && !["cursor", "camera"].includes(msg.type)) {
        ws.send(JSON.stringify({ type: "error", message: "Viewers cannot edit" }));
        return;
      }

      switch (msg.type) {

        case "add-item": {
          const item = msg.item as SceneItem;
          // Trust client ID — just keep nextId ahead
          if (item.id >= this.scene.nextId) this.scene.nextId = item.id + 1;
          // Handle collision (two editors add simultaneously)
          const exists = this.scene.items.some(i => i.id === item.id);
          if (exists) {
            const newId = this.scene.nextId++;
            ws.send(JSON.stringify({ type: "id-remap", oldId: item.id, newId }));
            item.id = newId;
          }
          this.scene.items.push(item);
          await this.saveScene();
          // Echo back to ALL including sender so sender confirms server ID
          this.broadcast(JSON.stringify({ type: "add-item", item, by: peerId }));
          break;
        }

        case "update-item": {
          const idx = this.scene.items.findIndex(i => i.id === msg.id);
          if (idx >= 0) {
            Object.assign(this.scene.items[idx], msg.changes);
            await this.deferSave();
            this.broadcast(
              JSON.stringify({ type: "update-item", id: msg.id, changes: msg.changes, by: peerId }),
              peerId
            );
          }
          break;
        }

        case "remove-item": {
          this.scene.items = this.scene.items.filter(i => i.id !== msg.id);
          this.scene.threads = this.scene.threads.filter(t => t.fromId !== msg.id && t.toId !== msg.id);
          await this.saveScene();
          this.broadcast(JSON.stringify({ type: "remove-item", id: msg.id, by: peerId }));
          break;
        }

        case "add-thread": {
          this.scene.threads.push(msg.thread);
          await this.saveScene();
          this.broadcast(JSON.stringify({ type: "add-thread", thread: msg.thread, by: peerId }));
          break;
        }

        case "clear-threads": {
          this.scene.threads = [];
          await this.saveScene();
          this.broadcast(JSON.stringify({ type: "clear-threads", by: peerId }));
          break;
        }

        case "set-env": {
          this.scene.envUrl = msg.url;
          await this.saveScene();
          this.broadcast(JSON.stringify({ type: "set-env", url: msg.url, by: peerId }), peerId);
          break;
        }

        case "cursor":
          this.broadcast(JSON.stringify({ type: "cursor", peerId, screenX: msg.screenX, screenY: msg.screenY, lon: msg.lon, lat: msg.lat }), peerId);
          break;

        case "sel-item":
          this.broadcast(JSON.stringify({ type: "sel-item", peerId, id: msg.id ?? null }), peerId);
          break;

        case "camera":
          this.broadcast(JSON.stringify({ type: "camera", peerId, lon: msg.lon, lat: msg.lat }), peerId);
          break;

        case "play-item":
          this.broadcast(JSON.stringify({ type: "play-item", id: msg.id, currentTime: msg.currentTime, by: peerId }), peerId);
          break;

        case "pause-item":
          this.broadcast(JSON.stringify({ type: "pause-item", id: msg.id, by: peerId }), peerId);
          break;

        case "set-playback-mode":
          if (meta.role === "editor" && (msg.mode === "independent" || msg.mode === "host")) {
            this.broadcast(JSON.stringify({ type: "set-playback-mode", mode: msg.mode, by: peerId }), peerId);
          }
          break;
      }
    } catch (e) {
      console.error("webSocketMessage error:", e);
    }
  }

  async webSocketClose(ws: WebSocket) {
    const peerId = this.getPeerId(ws);
    if (peerId) this.broadcast(JSON.stringify({ type: "peer-leave", peerId }), peerId);
  }

  async webSocketError(ws: WebSocket) {
    const peerId = this.getPeerId(ws);
    if (peerId) this.broadcast(JSON.stringify({ type: "peer-leave", peerId }), peerId);
  }
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const corsHeaders = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, Upgrade",
    };

    if (request.method === "OPTIONS") return new Response(null, { headers: corsHeaders });

    const roomMatch = url.pathname.match(/^\/room\/([a-zA-Z0-9_-]+)(\/info)?$/);
    if (roomMatch) {
      const roomId = roomMatch[1];
      const id = env.ROOM.idFromName(roomId);
      const stub = env.ROOM.get(id);
      const response = await stub.fetch(request);
      if (!request.headers.get("Upgrade")) {
        const newRes = new Response(response.body, response);
        for (const [k, v] of Object.entries(corsHeaders)) newRes.headers.set(k, v);
        return newRes;
      }
      return response;
    }

    if (url.pathname === "/new" && request.method === "POST") {
      const roomId = crypto.randomUUID().split("-")[0];
      return Response.json({ roomId, url: `/room/${roomId}` }, { headers: corsHeaders });
    }

    return new Response(
      "Community 360° Builder — Collab Server v3\nPOST /new\nWS /room/:id\nGET /room/:id/info",
      { headers: { ...corsHeaders, "Content-Type": "text/plain" } }
    );
  },
};
