import { connect } from "node:net";
import type { Broker, LiveMsg } from "./broker/index.ts";
import { decodeQuorumAppend } from "./wire.ts";
import { ChanError } from "./errors.ts";
import { splitHost } from "./config.ts";

type Waiter = { resolve: (v: unknown) => void; reject: (e: Error) => void };

type Peer = { id: string; write: (line: object) => void; pending: Map<number, Waiter> };

/** Per-connection state stored on the cluster listen socket. */
type ClusterSock = { buf?: string; peerId?: string };

export class Cluster {
  peers = new Map<string, Peer>();
  private seq = 1;
  private subs = new Map<number, (msg: LiveMsg) => void>();
  private server: { stop(closeActiveConnections?: boolean): void } | null = null;
  private dialTimer: ReturnType<typeof setInterval> | null = null;

  constructor(private broker: Broker) {}

  start() {
    const listen = this.broker.cfg.clusterListen;
    if (!listen || this.broker.cfg.members.length === 0) return;
    const { host, port } = splitHost(listen);
    this.server = Bun.listen<ClusterSock>({
      hostname: host,
      port,
      socket: {
        data: (socket, data) => this.onData(socket, data),
        open: (socket) => {
          socket.data = { buf: "" };
        },
        close: (socket) => {
          const id = socket.data?.peerId;
          if (id) this.peers.delete(id);
          this.broker.promoteIfLeader();
        },
        error: () => {},
      },
    });
    this.dialTimer = setInterval(() => this.dial(), 200);
    this.dial();
  }

  /** Close the listen socket. Tests use this so the process can exit. */
  stop() {
    if (this.dialTimer) clearInterval(this.dialTimer);
    this.dialTimer = null;
    this.server?.stop(true);
    this.server = null;
  }

  private dial() {
    const self = this.broker.cfg.nodeId;
    for (const member of this.broker.cfg.members) {
      if (member.id <= self) continue;
      if (this.peers.has(member.id)) continue;
      const { host, port } = splitHost(member.addr);
      const sock = connect({ host, port });
      const peerBuf = { buf: "" };
      sock.on("data", (chunk) =>
        this.readLines(peerBuf, chunk.toString(), (line) =>
          this.dispatch(member.id, line, (obj) => {
            sock.write(`${JSON.stringify(obj)}\n`);
          }),
        ),
      );
      sock.on("connect", () => {
        const write = (line: object) => sock.write(`${JSON.stringify(line)}\n`);
        const peer: Peer = { id: member.id, write, pending: new Map() };
        this.peers.set(member.id, peer);
        write({ v: 1, op: "hello", id: 0, nodeId: self, payload: { v: 1, node: self, snapshot: this.broker.snapshot(), consumed: this.broker.consumed } });
      });
      sock.on("close", () => {
        this.peers.delete(member.id);
        this.broker.promoteIfLeader();
      });
      sock.on("error", () => {
        this.peers.delete(member.id);
        this.broker.promoteIfLeader();
      });
    }
  }

  private onData(socket: { data: ClusterSock; write: (s: string) => number }, data: Buffer | string) {
    const st = socket.data;
    if (st.buf == null) st.buf = "";
    const text = typeof data === "string" ? data : data.toString();
    this.readLines(st as { buf: string }, text, (line) =>
      this.dispatch("", line, (obj) => socket.write(`${JSON.stringify(obj)}\n`), socket),
    );
  }

  private readLines(st: { buf: string }, text: string, onLine: (line: Record<string, unknown>) => void) {
    st.buf += text;
    let idx: number;
    while ((idx = st.buf.indexOf("\n")) >= 0) {
      const line = st.buf.slice(0, idx);
      st.buf = st.buf.slice(idx + 1);
      if (!line.trim()) continue;
      try {
        onLine(JSON.parse(line));
      } catch {
        /* ignore */
      }
    }
  }

  private dispatch(
    fallbackId: string,
    msg: Record<string, unknown>,
    write: (obj: object) => void,
    socket?: { data?: { peerId?: string } },
  ) {
    const op = String(msg.op ?? "");
    if (op === "hello") {
      const payload = (msg.payload ?? {}) as Record<string, unknown>;
      const id = String(msg.nodeId ?? payload.node ?? fallbackId);
      if (id) {
        this.peers.set(id, { id, write: (line) => write(line), pending: new Map() });
        if (socket?.data) socket.data.peerId = id;
        this.broker.promoteIfLeader();
      }
      const snap = (payload.snapshot ?? msg.snapshot) as ReturnType<Broker["snapshot"]> | undefined;
      try {
        this.broker.applySnapshot(snap);
        this.broker.applyConsumed(payload.consumed as Array<{ vhost?: string; queue?: string; id?: string }>);
      } catch {
        /* a peer of the other implementation keeps its own files */
      }
      write({
        v: 1,
        op: "reply",
        id: msg.id ?? 0,
        ok: true,
        from: this.broker.cfg.nodeId,
        nodeId: this.broker.cfg.nodeId,
        payload: { v: 1, node: this.broker.cfg.nodeId, snapshot: this.broker.snapshot(), consumed: this.broker.consumed },
      });
      return;
    }
    if (op === "reply") {
      const id = String(msg.from ?? msg.nodeId ?? "");
      const peer = id ? this.peers.get(id) : [...this.peers.values()][0];
      const waiter = peer?.pending.get(Number(msg.id));
      if (waiter) {
        peer!.pending.delete(Number(msg.id));
        if (msg.ok === false) waiter.reject(new Error(String(msg.error ?? "cluster error")));
        else waiter.resolve(msg.payload);
      }
      const payload = (msg.payload ?? {}) as { snapshot?: ReturnType<Broker["snapshot"]>; consumed?: Array<{ vhost?: string; queue?: string; id?: string }> };
      if (msg.snapshot) this.broker.applySnapshot(msg.snapshot as ReturnType<Broker["snapshot"]>);
      if (payload.snapshot) this.broker.applySnapshot(payload.snapshot);
      this.broker.applyConsumed(payload.consumed);
      return;
    }
    if (op === "apply" || op === "enqueue" || op === "quorum_append" || op === "quorum_drop" || op === "ack" || op === "nack" || op === "get" || op === "purge" || op === "declare_queue" || op === "delete_queue" || op === "unsub") {
      void this.handle(op, msg).then(
        (payload) => write({ op: "reply", id: msg.id, ok: true, payload, from: this.broker.cfg.nodeId }),
        (err) => write({ op: "reply", id: msg.id, ok: false, error: String(err), from: this.broker.cfg.nodeId }),
      );
      return;
    }
    if (op === "sub") {
      const body = (msg.payload ?? msg) as Record<string, unknown>;
      const session = Number(body.session);
      const vhost = String(body.vhost);
      const queue = String(body.queue);
      const q = this.broker.queues.get(this.broker.key(vhost, queue));
      if (!q) {
        write({ op: "reply", id: msg.id, ok: false, error: "NOT_FOUND", from: this.broker.cfg.nodeId });
        return;
      }
      q.consumers.push({
        tag: `remote-${session}`,
        session,
        noAck: !!body.noAck,
        exclusive: !!body.exclusive,
        want: () => true,
        deliver: (m) => {
          write({
            op: "deliver",
            session,
            msg: {
              ...m,
              body: Buffer.from(m.body).toString("base64"),
              propRaw: Buffer.from(m.propRaw).toString("base64"),
            },
          });
        },
      });
      write({ op: "reply", id: msg.id, ok: true, from: this.broker.cfg.nodeId });
      this.broker.pump(q);
      return;
    }
    if (op === "deliver") {
      const session = Number(msg.session);
      const fn = this.subs.get(session);
      const raw = msg.msg as LiveMsg & { body: string; propRaw: string };
      if (fn && raw) {
        fn({
          ...raw,
          body: new Uint8Array(Buffer.from(raw.body, "base64")),
          propRaw: new Uint8Array(Buffer.from(raw.propRaw, "base64")),
        });
      }
    }
  }

  private async handle(op: string, msg: Record<string, unknown>): Promise<unknown> {
    if (op === "apply") {
      this.broker.applyRemote(String(msg.kind), (msg.payload ?? {}) as Record<string, unknown>);
      return true;
    }
    if (op === "unsub") {
      const body = (msg.payload ?? msg) as Record<string, unknown>;
      const q = this.broker.queues.get(this.broker.key(String(body.vhost), String(body.queue)));
      if (q) q.consumers = q.consumers.filter((c) => c.session !== Number(body.session));
      return true;
    }
    if (op === "declare_queue" || op === "delete_queue") {
      this.broker.applyRemote(op === "declare_queue" ? "queue" : "delete_queue", (msg.payload ?? msg) as Record<string, unknown>);
      return true;
    }
    if (op === "quorum_drop") {
      const p = (msg.payload ?? msg) as Record<string, unknown>;
      this.broker.dropLocal(String(p.vhost), String(p.queue), String(p.id ?? p.qid ?? ""));
      return true;
    }
    if (op === "quorum_append" || op === "enqueue") {
      const p = (msg.payload ?? msg) as Record<string, unknown>;
      const decoded = decodeQuorumAppend(p);
      const q = this.broker.queues.get(this.broker.key(decoded.vhost, decoded.queue));
      if (!q) throw new Error("NOT_FOUND");
      const ok = this.broker.enqueueLocal(q, {
        body: decoded.body,
        exchange: decoded.exchange,
        routingKey: decoded.routingKey,
        headers: decoded.headers,
        propRaw: decoded.propRaw,
        persistent: decoded.persistent,
        priority: decoded.priority,
        expiration: decoded.expiration,
        id: decoded.messageId,
      }, 0);
      // A resolved false is "not stored". The reply envelope must be ok:false,
      // or the caller counts the peer as a durable copy.
      if (op === "quorum_append" && !ok) throw new Error("NOT_STORED");
      if (op === "quorum_append") await this.broker.store.whenDurable();
      return ok;
    }
    if (op === "ack") {
      const p = (msg.payload ?? msg) as Record<string, unknown>;
      await this.broker.ack(String(p.vhost), String(p.queue), String(p.id));
      return true;
    }
    if (op === "nack") {
      const p = (msg.payload ?? msg) as Record<string, unknown>;
      await this.broker.nack(String(p.vhost), String(p.queue), String(p.id), !!p.requeue);
      return true;
    }
    if (op === "purge") {
      const p = (msg.payload ?? msg) as Record<string, unknown>;
      return this.broker.purge(String(p.vhost), String(p.queue));
    }
    if (op === "get") {
      const p = (msg.payload ?? msg) as Record<string, unknown>;
      const m = await this.broker.get(String(p.vhost), String(p.queue), !!(p.noAck ?? p.no_ack));
      if (!m) return { empty: true };
      return {
        msg: { ...m, body: Buffer.from(m.body).toString("base64"), propRaw: Buffer.from(m.propRaw).toString("base64") },
      };
    }
    return true;
  }

  peerIds(): string[] {
    return [...this.peers.keys()];
  }

  async call(home: string, op: string, payload: unknown): Promise<unknown> {
    const peer = this.peers.get(home);
    if (!peer) throw new ChanError(541, "INTERNAL_ERROR - queue home is unavailable");
    const id = this.seq++;
    const result = new Promise((resolve, reject) => {
      peer.pending.set(id, { resolve, reject });
      setTimeout(() => {
        if (peer.pending.has(id)) {
          peer.pending.delete(id);
          if (this.peers.get(home) === peer) this.peers.delete(home);
          reject(new ChanError(541, "INTERNAL_ERROR - queue home is unavailable"));
        }
      }, 3000);
    });
    peer.write({ op, id, payload, from: this.broker.cfg.nodeId });
    return result;
  }

  async replicate(kind: string, payload: unknown) {
    await Promise.all(
      [...this.peers.values()].filter((p) => p.id !== this.broker.cfg.nodeId).map((peer) => {
        const id = this.seq++;
        return new Promise((resolve) => {
          peer.pending.set(id, { resolve, reject: () => resolve(null) });
          setTimeout(() => resolve(null), 3000);
          peer.write({ op: "apply", id, kind, payload, from: this.broker.cfg.nodeId });
        });
      }),
    );
  }

  async subscribe(home: string, payload: { vhost: string; queue: string; session: number; noAck: boolean; exclusive: boolean }, onDeliver: (msg: LiveMsg) => void) {
    this.subs.set(payload.session, onDeliver);
    await this.call(home, "sub", payload);
    return payload.session;
  }
}
