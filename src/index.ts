// ═══════════════════════════════════════════════════════════
// Community 360° Builder — Collaboration Server v2
// Cloudflare Worker + Durable Object (PartyServer pattern)
// ═══════════════════════════════════════════════════════════
//
// Fixes over v1:
//   1. Client ID trusted — server no longer reassigns item.id.
//      The client assigns the ID before sending; server stores
//      and broadcasts it as-is, then confirms back to sender
//      with an 'item-added' ack. This keeps all peers in sync
//      on the same ID from the moment of creation.
//   2. Deferred saves on update-item — positional drag sends
//      30+ msgs/sec. Saving to Durable Object storage on every
//      one hit Cloudflare limits and caused backpressure/drops.
//      Now: update-item broadcasts immediately but saves are
//      debounced (500ms). Structural mutations (add, remove,
//      threads, env) still save immediately.
//
// Deploy: npx wrangler deploy

interface Env {
  ROOM: DurableObjectNamespace;
}

// ─── Types ───
interface Peer {
  ws: WebSocket;
  name: string;
  role: "editor" | "viewer";
  color: string;
  joinedAt: number;
}

interface SceneItem {
  id: number;
  type: string;
  name: string;
  src: string;
  lon: number;
  lat: number;
  depth: number;
  scale: number;
  opacity: number;
  rx: number;
  ry: number;
  rz: number;
  bb: boolean;
  loop: boolean;
  vis: boolean;
  glow: boolean;
  glowColor: string;
  interact: string;
  meta: string;
  isolated: boolean;
  [key: string]: any;
}

interface ThreadData {
  fromId: number;
  toId: number;
  color: string;
  width: number;
  glow: number;
}

interface RoomState {
  items: SceneItem[];
  threads: ThreadData[];
  envUrl: string;
  nextId: number;
}

// Peer colors — visually distinct, accessible
const PEER_COLORS = [
  "#4ecdc4", "#a78bfa", "#f472b6", "#fb923c",
  "#4ade80", "#60a5fa", "#facc15", "#f87171",
];

// ─── Durable Object: Room ───
export class Room {
  state: DurableObjectState;
  peers: Map<string, Peer> = new Map();
  scene: RoomState = { items: [], threads: [], envUrl: "", nextId: 1 };
  peerIdCounter = 0;

  // Debounce handle for deferred saves
  private saveTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(state: DurableObjectState) {
    this.state = state;
    state.blockConcurrencyWhile(async () => {
      const saved = await state.storage.get<RoomState>("scene");
      if (saved) this.scene = saved;
    });
  }

  // Immediate save — for structural mutations
  async saveScene() {
    if (this.saveTimer) {
      clearTimeout(this.saveTimer);
      this.saveTimer = null;
    }
    await this.state.storage.put("scene", this.scene);
  }

  // Deferred save — coalesces rapid writes (drag/move) into one storage op.
  // Uses state.storage.setAlarm() which is reliable in Durable Objects
  // (unlike setTimeout which may not fire if the DO hibernates).
  async deferSave() {
    try {
      // Set alarm 600ms out — if called again before it fires, it resets.
      await this.state.storage.setAlarm(Date.now() + 600);
    } catch {
      // Fallback: save immediately if alarms not available
      await this.state.storage.put("scene", this.scene);
    }
  }

  // Called by Cloudflare when the alarm fires
  async alarm() {
    await this.state.storage.put("scene", this.scene);
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);

    // ── HTTP: Get room info ──
    if (request.method === "GET" && url.pathname.endsWith("/info")) {
      return Response.json({
        peers: Array.from(this.peers.values()).map((p) => ({
          name: p.name,
          role: p.role,
          color: p.color,
        })),
        itemCount: this.scene.items.length,
        threadCount: this.scene.threads.length,
      });
    }

    // ── WebSocket upgrade ──
    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("Expected WebSocket", { status: 426 });
    }

    const pair = new WebSocketPair();
    const [client, server] = [pair[0], pair[1]];

    const name = url.searchParams.get("name") || "Anonymous";
    const role = url.searchParams.get("role") === "viewer" ? "viewer" : "editor";
    const peerId = `p${++this.peerIdCounter}`;
    const color = PEER_COLORS[this.peerIdCounter % PEER_COLORS.length];

    const peer: Peer = { ws: server, name, role, color, joinedAt: Date.now() };
    this.peers.set(peerId, peer);

    server.accept();

    // Send full scene state to new peer
    server.send(
      JSON.stringify({
        type: "init",
        peerId,
        role,
        color,
        scene: this.scene,
        peers: Array.from(this.peers.entries()).map(([id, p]) => ({
          id,
          name: p.name,
          role: p.role,
          color: p.color,
        })),
      })
    );

    // Announce join to others
    this.broadcast(
      JSON.stringify({ type: "peer-join", peerId, name, role, color }),
      peerId
    );

    // ── Message handler ──
    server.addEventListener("message", async (event) => {
      try {
        const msg = JSON.parse(event.data as string);
        const sender = this.peers.get(peerId);
        if (!sender) return;

        // Viewers can only send presence updates
        if (sender.role === "viewer" && !["cursor", "camera"].includes(msg.type)) {
          server.send(JSON.stringify({ type: "error", message: "Viewers cannot edit" }));
          return;
        }

        switch (msg.type) {

          case "add-item": {
            const item = msg.item as SceneItem;

            // FIX 1: Trust the client's ID — don't overwrite it.
            // Client assigns the ID before sending so it can track
            // the item locally. We just make sure nextId stays ahead
            // of whatever IDs clients are generating.
            if (item.id >= this.scene.nextId) {
              this.scene.nextId = item.id + 1;
            }

            // Avoid duplicate IDs if two editors add simultaneously
            const exists = this.scene.items.some((i) => i.id === item.id);
            if (exists) {
              // Assign a safe server ID and tell the sender to remap
              const newId = this.scene.nextId++;
              server.send(JSON.stringify({ type: "id-remap", oldId: item.id, newId }));
              item.id = newId;
            }

            this.scene.items.push(item);
            await this.saveScene(); // structural — save immediately

            // Broadcast to everyone including sender so all peers
            // confirm the same final ID
            this.broadcast(JSON.stringify({ type: "add-item", item, by: peerId }));
            break;
          }

          case "update-item": {
            const idx = this.scene.items.findIndex((i) => i.id === msg.id);
            if (idx >= 0) {
              Object.assign(this.scene.items[idx], msg.changes);
              // FIX 2: Defer save — broadcast immediately, write later
              this.deferSave();
              this.broadcast(
                JSON.stringify({ type: "update-item", id: msg.id, changes: msg.changes, by: peerId }),
                peerId // don't echo back to mover
              );
            }
            break;
          }

          case "remove-item": {
            this.scene.items = this.scene.items.filter((i) => i.id !== msg.id);
            this.scene.threads = this.scene.threads.filter(
              (t) => t.fromId !== msg.id && t.toId !== msg.id
            );
            await this.saveScene();
            this.broadcast(JSON.stringify({ type: "remove-item", id: msg.id, by: peerId }));
            break;
          }

          case "add-thread": {
            this.scene.threads.push(msg.thread as ThreadData);
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

          // ── Presence — relay only, never saved ──
          case "cursor": {
            this.broadcast(
              JSON.stringify({ type: "cursor", peerId, screenX: msg.screenX, screenY: msg.screenY, lon: msg.lon, lat: msg.lat }),
              peerId
            );
            break;
          }

          case "sel-item": {
            // Broadcast which item this peer has selected (null = deselected)
            this.broadcast(
              JSON.stringify({ type: "sel-item", peerId, id: msg.id ?? null }),
              peerId
            );
            break;
          }

          case "camera": {
            this.broadcast(
              JSON.stringify({ type: "camera", peerId, lon: msg.lon, lat: msg.lat, fov: msg.fov }),
              peerId
            );
            break;
          }

          // ── Playback sync (host-controlled mode only) ──
          case "play-item": {
            this.broadcast(
              JSON.stringify({ type: "play-item", id: msg.id, currentTime: msg.currentTime, by: peerId }),
              peerId
            );
            break;
          }

          case "pause-item": {
            this.broadcast(
              JSON.stringify({ type: "pause-item", id: msg.id, by: peerId }),
              peerId
            );
            break;
          }

          // ── Playback mode sync (editors only) ──
          case "set-playback-mode": {
            if (sender.role === "editor" && (msg.mode === "independent" || msg.mode === "host")) {
              this.broadcast(
                JSON.stringify({ type: "set-playback-mode", mode: msg.mode, by: peerId }),
                peerId
              );
            }
            break;
          }
        }
      } catch (e) {
        console.error("Message error:", e);
      }
    });

    // ── Disconnect ──
    server.addEventListener("close", () => {
      this.peers.delete(peerId);
      this.broadcast(JSON.stringify({ type: "peer-leave", peerId }));
    });

    server.addEventListener("error", () => {
      this.peers.delete(peerId);
      this.broadcast(JSON.stringify({ type: "peer-leave", peerId }));
    });

    return new Response(null, { status: 101, webSocket: client });
  }

  broadcast(message: string, exclude?: string) {
    for (const [id, peer] of this.peers) {
      if (id === exclude) continue;
      try {
        peer.ws.send(message);
      } catch {
        this.peers.delete(id);
      }
    }
  }
}

// ─── Worker entry point ───
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname;

    const corsHeaders = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, Upgrade",
    };

    if (request.method === "OPTIONS") {
      return new Response(null, { headers: corsHeaders });
    }

    const roomMatch = path.match(/^\/room\/([a-zA-Z0-9_-]+)(\/info)?$/);
    if (roomMatch) {
      const roomId = roomMatch[1];
      const id = env.ROOM.idFromName(roomId);
      const stub = env.ROOM.get(id);
      const response = await stub.fetch(request);
      if (!request.headers.get("Upgrade")) {
        const newResponse = new Response(response.body, response);
        for (const [k, v] of Object.entries(corsHeaders)) {
          newResponse.headers.set(k, v);
        }
        return newResponse;
      }
      return response;
    }

    if (path === "/new" && request.method === "POST") {
      const roomId = crypto.randomUUID().split("-")[0];
      return Response.json({ roomId, url: `/room/${roomId}` }, { headers: corsHeaders });
    }

    return new Response(
      `Community 360° Builder — Collaboration Server v2\n\nPOST /new → create room\nWS /room/:id → join room\nGET /room/:id/info → room info`,
      { headers: { ...corsHeaders, "Content-Type": "text/plain" } }
    );
  },
};
