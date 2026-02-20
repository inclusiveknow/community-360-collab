// ═══════════════════════════════════════════════════════════
// Community 360° Builder — Collaboration Server
// Cloudflare Worker + Durable Object (PartyServer pattern)
// ═══════════════════════════════════════════════════════════
//
// Deploy: npx wrangler deploy
// Each "room" is a Durable Object that holds the shared scene.
// Clients connect via WebSocket and sync items/threads in real time.

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
  // Chunked upload buffers: uploadId → { meta, chunks, peerId }
  chunkBuffers: Map<string, { meta: SceneItem; chunks: string[]; peerId: string }> = new Map();

  constructor(state: DurableObjectState) {
    this.state = state;
    // Load persisted scene on wake
    state.blockConcurrencyWhile(async () => {
      const saved = await state.storage.get<RoomState>("scene");
      if (saved) this.scene = saved;
    });
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

    // Parse query params for join info
    const name = url.searchParams.get("name") || "Anonymous";
    const role = url.searchParams.get("role") === "viewer" ? "viewer" : "editor";
    const peerId = `p${++this.peerIdCounter}`;
    const color = PEER_COLORS[this.peerIdCounter % PEER_COLORS.length];

    const peer: Peer = { ws: server, name, role, color, joinedAt: Date.now() };
    this.peers.set(peerId, peer);

    server.accept();

    // Send initial state to new peer
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
      JSON.stringify({
        type: "peer-join",
        peerId,
        name,
        role,
        color,
      }),
      peerId
    );

    // ── Message handler ──
    server.addEventListener("message", async (event) => {
      try {
        const msg = JSON.parse(event.data as string);
        const sender = this.peers.get(peerId);
        if (!sender) return;

        // Viewers can only send cursor/camera updates
        if (sender.role === "viewer" && !["cursor", "camera"].includes(msg.type)) {
          server.send(JSON.stringify({ type: "error", message: "Viewers cannot edit" }));
          return;
        }

        switch (msg.type) {
          // ── Scene mutations (editors only) ──
          case "add-item": {
            const item = msg.item as SceneItem;
            item.id = this.scene.nextId++;
            this.scene.items.push(item);
            await this.saveScene();
            this.broadcast(JSON.stringify({ type: "add-item", item, by: peerId }));
            break;
          }

          case "update-item": {
            const idx = this.scene.items.findIndex((i) => i.id === msg.id);
            if (idx >= 0) {
              Object.assign(this.scene.items[idx], msg.changes);
              await this.saveScene();
              this.broadcast(
                JSON.stringify({ type: "update-item", id: msg.id, changes: msg.changes, by: peerId }),
                peerId // don't echo back to sender
              );
            }
            break;
          }

          case "remove-item": {
            this.scene.items = this.scene.items.filter((i) => i.id !== msg.id);
            // Also remove threads referencing this item
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

          // ── Chunked media upload ──────────────────────────────────────
          // Clients split large base64 blobs into 256KB pieces to stay under
          // Cloudflare's 1MB WebSocket message limit. Server buffers here,
          // then broadcasts the assembled add-item once all chunks arrive.
          case "media-chunk-start": {
            // Assign server ID immediately, persist metadata without src.
            // Large base64 blobs can't be stored (128KB DO storage limit) —
            // we relay chunks directly to peers instead.
            const meta = { ...(msg.itemMeta as SceneItem), src: "" };
            meta.id = this.scene.nextId++;
            this.scene.items.push(meta);
            await this.saveScene();
            this.chunkBuffers.set(msg.uploadId, { meta, chunks: [], peerId });

            // Tell sender their canonical server ID so client can remap
            server.send(JSON.stringify({
              type: "chunk-start-ack",
              uploadId: msg.uploadId,
              serverId: meta.id,
            }));
            break;
          }

          case "media-chunk": {
            // Relay each chunk immediately — don't buffer on server.
            // Each chunk is already ≤256KB so outbound relay is safe.
            this.broadcast(
              JSON.stringify({ type: "media-chunk", uploadId: msg.uploadId,
                               index: msg.index, data: msg.data }),
              peerId
            );
            break;
          }

          case "media-chunk-end": {
            const buf = this.chunkBuffers.get(msg.uploadId);
            this.chunkBuffers.delete(msg.uploadId);

            // Relay end signal — peers finalize the item on their side
            this.broadcast(
              JSON.stringify({ type: "media-chunk-end", uploadId: msg.uploadId,
                               totalChunks: msg.totalChunks,
                               serverId: buf ? buf.meta.id : null,
                               by: peerId }),
              peerId
            );

            // Confirm to sender
            server.send(JSON.stringify({
              type: "chunk-complete",
              uploadId: msg.uploadId,
              serverId: buf ? buf.meta.id : null,
            }));
            break;
          }

          // ── Isolation intent — relay only, no persistence ──
          // Each peer runs MediaPipe locally on their own copy of the media.
          case "isolate-item": {
            this.broadcast(
              JSON.stringify({ type: "isolate-item", id: msg.id, isolated: msg.isolated, peerId }),
              peerId
            );
            break;
          }

          // ── Selection presence — relay only ──
          case "sel-item": {
            this.broadcast(
              JSON.stringify({ type: "sel-item", peerId, id: msg.id ?? null }),
              peerId
            );
            break;
          }

          // ── Playback mode sync — relay only ──
          case "set-playback-mode": {
            this.broadcast(
              JSON.stringify({ type: "set-playback-mode", mode: msg.mode, by: peerId }),
              peerId
            );
            break;
          }

          // ── Play/pause sync (host mode) — relay only ──
          case "play-item": {
            this.broadcast(
              JSON.stringify({ type: "play-item", id: msg.id, currentTime: msg.currentTime ?? null, by: peerId }),
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

          // ── Presence (everyone) ──
          case "cursor": {
            this.broadcast(
              JSON.stringify({
                type: "cursor", peerId,
                screenX: msg.screenX, screenY: msg.screenY,
                lon: msg.lon, lat: msg.lat,
              }),
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
        }
      } catch (e) {
        console.error("Message error:", e);
      }
    });

    // ── Disconnect handler ──
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
        // Dead connection, clean up
        this.peers.delete(id);
      }
    }
  }

  async saveScene() {
    // Cloudflare DO storage has a 128KB per-value limit.
    // Strip large base64 src fields before persisting — blob media is
    // re-uploaded by clients on reconnect via the chunk protocol.
    // URL-based src (https://...) is fine and preserved.
    const sceneToSave: RoomState = {
      ...this.scene,
      items: this.scene.items.map(item => {
        const src = item.src || "";
        const isLargeBlobSrc = src.startsWith("data:") && src.length > 1024;
        return isLargeBlobSrc ? { ...item, src: "" } : item;
      }),
    };
    await this.state.storage.put("scene", sceneToSave);
  }
}

// ─── Worker entry point ───
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname;

    // CORS headers for cross-origin access
    const corsHeaders = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, Upgrade",
    };

    if (request.method === "OPTIONS") {
      return new Response(null, { headers: corsHeaders });
    }

    // ── Route: /room/:id — WebSocket or info ──
    const roomMatch = path.match(/^\/room\/([a-zA-Z0-9_-]+)(\/info)?$/);
    if (roomMatch) {
      const roomId = roomMatch[1];
      const id = env.ROOM.idFromName(roomId);
      const stub = env.ROOM.get(id);
      const response = await stub.fetch(request);

      // Add CORS to non-WebSocket responses
      if (!request.headers.get("Upgrade")) {
        const newResponse = new Response(response.body, response);
        for (const [k, v] of Object.entries(corsHeaders)) {
          newResponse.headers.set(k, v);
        }
        return newResponse;
      }
      return response;
    }

    // ── Route: /new — Create a new room, return its ID ──
    if (path === "/new" && request.method === "POST") {
      const roomId = crypto.randomUUID().split("-")[0]; // Short 8-char ID
      return Response.json({ roomId, url: `/room/${roomId}` }, { headers: corsHeaders });
    }

    // ── Default: landing page ──
    return new Response(
      `Community 360° Builder — Collaboration Server\n\nPOST /new → create room\nWS /room/:id → join room\nGET /room/:id/info → room info`,
      { headers: { ...corsHeaders, "Content-Type": "text/plain" } }
    );
  },
};
