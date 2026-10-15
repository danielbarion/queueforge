import { readFileSync } from "node:fs";
import { connect, createServer, type Server, type Socket } from "node:net";
import type { Broker, LiveMsg } from "./broker/index.ts";
import { decodeQuorumAppend, QUORUM_BODY } from "./wire.ts";
import { ChanError } from "./errors.ts";
import { splitHost } from "./config.ts";
import { Consensus } from "./raft/glue.ts";
import { META } from "./raft/node.ts";

type Waiter = {
  resolve: (v: unknown) => void;
  reject: (e: Error) => void;
  deadline: number;
  onTimeout: () => void;
};

type Peer = {
  id: string;
  write: (line: object) => void;
  /** Set when the socket takes binary frames (`encodeFrame`). */
  writeParts?: (parts: Uint8Array[]) => void;
  pending: Map<number, Waiter>;
  token: object;
};

/**
 * Write pressure on one cluster socket. `full` is set when a write is left in
 * the socket's buffer and cleared on `drain`, which runs `wake`. Remote
 * consumers stop taking messages while it is set, so a slow peer holds them in
 * the queue instead of in this process's socket buffer.
 */
type Flow = { full: boolean; wake: Set<() => void> };

function newFlow(): Flow {
  return { full: false, wake: new Set() };
}

function drained(flow: Flow) {
  flow.full = false;
  for (const wake of [...flow.wake]) wake();
}

/** A batch this large goes out before the turn ends, so `flow.full` still trips. */
const LINE_FLUSH = 1024 * 1024;

/**
 * Join the lines written to one socket in a turn into one write.
 *
 * Each cluster message was its own `socket.write`, so every forwarded copy of
 * a fanout publish was a syscall: about half of a four-CPU child's time in the
 * fanout-20 profile. Lines and frames keep their order; a reply still leaves
 * in the turn that produced it.
 */
function lineWriter(sock: Socket, flow: Flow): (part: string | Uint8Array) => void {
  let parts: Array<string | Uint8Array> = [];
  let bytes = 0;
  let binary = false;
  let armed = false;
  const flush = () => {
    armed = false;
    if (!parts.length) return;
    const out = binary ? joinParts(parts) : parts.length === 1 ? parts[0]! : (parts as string[]).join("");
    parts = [];
    bytes = 0;
    binary = false;
    if (sock.destroyed) return;
    if (!sock.write(out)) flow.full = true;
  };
  return (part) => {
    parts.push(part);
    bytes += part.length;
    if (typeof part !== "string") binary = true;
    if (bytes >= LINE_FLUSH) flush();
    else if (!armed) {
      armed = true;
      queueMicrotask(flush);
    }
  };
}

/** One buffer from lines and frame parts; each run of lines is encoded once. */
function joinParts(parts: Array<string | Uint8Array>): Buffer {
  const out: Uint8Array[] = [];
  let text: string[] = [];
  for (const part of parts) {
    if (typeof part === "string") {
      text.push(part);
      continue;
    }
    if (text.length) {
      out.push(Buffer.from(text.join("")));
      text = [];
    }
    out.push(part);
  }
  if (text.length) out.push(Buffer.from(text.join("")));
  return Buffer.concat(out);
}

/** How long a call waits for its reply. */
const WAIT_MS = 3000;
/** Peers with calls in flight, for the shared timeout sweep. */
const waiting = new Set<Peer>();
let sweeper: ReturnType<typeof setInterval> | null = null;

/**
 * Time out every call past its deadline. All share `WAIT_MS`, so a peer's
 * pending map is oldest first and the walk stops at the first live call.
 */
function sweepWaiters() {
  const now = Date.now();
  for (const peer of waiting) {
    for (const [id, waiter] of peer.pending) {
      if (waiter.deadline > now) break;
      peer.pending.delete(id);
      waiter.onTimeout();
    }
    if (peer.pending.size === 0) waiting.delete(peer);
  }
  if (waiting.size === 0 && sweeper) {
    clearInterval(sweeper);
    sweeper = null;
  }
}

/**
 * Wait up to 3 s for the reply. One sweep timer covers every call: a timer
 * per forwarded publish was ~5% of a four-CPU fanout child.
 */
function armWaiter(
  peer: Peer,
  id: number,
  resolve: (v: unknown) => void,
  reject: (e: Error) => void,
  onTimeout: () => void,
) {
  peer.pending.set(id, { resolve, reject, deadline: Date.now() + WAIT_MS, onTimeout });
  waiting.add(peer);
  sweeper ??= setInterval(sweepWaiters, 100);
}

/** A reply that arrives after the timeout finds nothing. */
function takeWaiter(peer: Peer, id: number): Waiter | undefined {
  const waiter = peer.pending.get(id);
  if (!waiter) return undefined;
  peer.pending.delete(id);
  return waiter;
}

/** First byte of a binary frame. A JSON line starts with `{`. */
const FRAME = 0;
/** Marker, then the JSON, body and properties lengths as u32 big-endian. */
const FRAME_HEAD = 13;

/**
 * Encode a message whose payload carries a body and properties as bytes, for a
 * peer that advertised `bin_frames`. The JSON part is the message without them;
 * base64 of each body and its decode were a fifth of both processes in the
 * four-CPU fanout profile.
 */
export function encodeFrame(msg: object, body: Uint8Array, propRaw: Uint8Array): Uint8Array[] {
  const json = Buffer.from(JSON.stringify(msg));
  const head = Buffer.allocUnsafe(FRAME_HEAD);
  head[0] = FRAME;
  head.writeUInt32BE(json.length, 1);
  head.writeUInt32BE(body.length, 5);
  head.writeUInt32BE(propRaw.length, 9);
  return [head, json, body, propRaw];
}

function copyOf(buf: Buffer, start: number, end: number): Uint8Array {
  const out = new Uint8Array(end - start);
  out.set(buf.subarray(start, end));
  return out;
}

/**
 * Cluster socket input: JSON lines and binary frames, in arrival order.
 *
 * Bytes, not text: a chunk decoded on its own could split a UTF-8 character.
 * A frame's body and properties arrive as `payload.body` and
 * `payload.propRaw`, copied out so a stored message does not hold the chunk.
 */
export class FrameReader {
  private rest: Buffer | null = null;
  /** Chunks held while a frame of known length is still incomplete. */
  private parts: Buffer[] = [];
  private held = 0;
  private need = 0;

  push(chunk: Buffer, onMessage: (msg: Record<string, unknown>) => void) {
    if (this.need > 0 && this.held + chunk.length < this.need) {
      this.parts.push(chunk);
      this.held += chunk.length;
      return;
    }
    let buf = chunk;
    if (this.parts.length) buf = Buffer.concat([...this.parts, chunk]);
    else if (this.rest) buf = Buffer.concat([this.rest, chunk]);
    this.parts = [];
    this.rest = null;
    this.held = 0;
    this.need = 0;
    let at = 0;
    while (at < buf.length) {
      if (buf[at] === FRAME) {
        if (buf.length - at < FRAME_HEAD) break;
        const jsonEnd = at + FRAME_HEAD + buf.readUInt32BE(at + 1);
        const bodyEnd = jsonEnd + buf.readUInt32BE(at + 5);
        const end = bodyEnd + buf.readUInt32BE(at + 9);
        if (end > buf.length) {
          this.need = end - at;
          break;
        }
        try {
          const msg = JSON.parse(buf.toString("utf8", at + FRAME_HEAD, jsonEnd)) as Record<string, unknown>;
          const payload = (msg.payload ?? {}) as Record<string, unknown>;
          const body = copyOf(buf, jsonEnd, bodyEnd);
          payload.body = body;
          payload.propRaw = copyOf(buf, bodyEnd, end);
          (payload as { [QUORUM_BODY]?: Uint8Array })[QUORUM_BODY] = body;
          msg.payload = payload;
          onMessage(msg);
        } catch {
          /* ignore, as a bad line is */
        }
        at = end;
        continue;
      }
      const nl = buf.indexOf(10, at);
      if (nl < 0) break;
      if (nl > at) {
        try {
          onMessage(JSON.parse(buf.toString("utf8", at, nl)) as Record<string, unknown>);
        } catch {
          /* ignore */
        }
      }
      at = nl + 1;
    }
    if (at >= buf.length) return;
    const tail = buf.subarray(at);
    if (this.need > 0) {
      this.parts = [tail];
      this.held = tail.length;
    } else {
      // Copied: a short tail must not keep a whole chunk alive.
      this.rest = Buffer.from(tail);
    }
  }
}

/** Per-connection state stored on the cluster listen socket. */
type ClusterSock = { peerId?: string; token?: object };

export class Cluster {
  peers = new Map<string, Peer>();
  private seq = 1;
  private nextDelivery = 1;
  /** `${vhost}\\0${queue}\\0${deliveryId}` → local message id for a remote ack. */
  private remoteAcks = new Map<string, string>();
  /** Same key → the remote consumer session, so `requeue` finds what a closed channel held. */
  private remoteSessions = new Map<string, number>();
  /** `${vhost}\0${queue}\0${id}` → the `remoteAcks` key of a remote get, for a settle by `id`. */
  private remoteGets = new Map<string, string>();
  private subs = new Map<number, (msg: LiveMsg) => void>();
  /**
   * `${vhost}\\0${queue}\\0${session}` → a remote consumer this process homes:
   * its waker on the socket's flow, and its credit refill when it subscribed
   * with `ack_credit`.
   */
  private remoteSubs = new Map<string, { flow?: Flow; wake: () => void; refill?: () => void }>();
  private server: Server | null = null;
  private sockets: Socket[] = [];
  private dialTimer: ReturnType<typeof setInterval> | null = null;

  /** Raft groups, once every voter advertises the `raft` feature. */
  consensus: Consensus;
  /** Tokens of peer sockets that have closed. */
  private closedTokens = new WeakSet<object>();

  constructor(private broker: Broker) {
    const self = () => this.broker.cfg.nodeId;
    // One-way (docs/raft.md, section 1). A peer that is not connected misses it; Raft resends.
    // Raft messages to a member keep to one socket while it is open: a peer
    // reachable on a dialed and an accepted socket would otherwise get a burst
    // split across both, and proposals forwarded to a leader appended out of order.
    const routes = new Map<string, Peer>();
    this.consensus = new Consensus(broker, (to, payload) => {
      let peer = routes.get(to);
      if (!peer || this.closedTokens.has(peer.token)) {
        peer = this.peers.get(to);
        if (peer) routes.set(to, peer);
        else routes.delete(to);
      }
      peer?.write({ v: 1, op: "raft", id: 0, from: self(), nodeId: self(), payload });
    });
  }

  start() {
    this.loadMembers();
    const listen = this.broker.cfg.clusterListen;
    if (!listen || this.broker.cfg.members.length === 0) return;
    const { host, port } = splitHost(listen);
    const server = createServer((socket) => {
      socket.setNoDelay(true);
      this.sockets.push(socket);
      const st: ClusterSock = {};
      const flow = newFlow();
      socket.on("drain", () => drained(flow));
      const send = lineWriter(socket, flow);
      const reader = new FrameReader();
      const wrapped = {
        data: st,
        write: (s: string) => {
          send(s);
          return s.length;
        },
        writeParts: (parts: Uint8Array[]) => {
          for (const part of parts) send(part);
        },
      };
      socket.on("data", (data: Buffer) => this.onData(wrapped, reader, data, flow));
      socket.on("close", () => {
        this.sockets = this.sockets.filter((open) => open !== socket);
        const id = st.peerId;
        const token = st.token;
        if (token) this.closedTokens.add(token);
        if (id && token && this.peers.get(id)?.token === token) this.peers.delete(id);
        this.broker.promoteIfLeader();
      });
      socket.on("error", () => {});
    });
    server.listen(port, host);
    this.server = server;
    this.consensus.start();
    this.dialTimer = setInterval(() => this.dial(), 200);
    this.dial();
  }

  /**
   * Change the member list for the whole cluster. With Raft on it is
   * committed through the meta log first, as RabbitMQ's Khepri does, and
   * fails without a majority. It is then pushed to every member, so one
   * without Raft (PHP) or still connecting has it at once.
   *
   * @returns The installed list. Throws when the commit failed.
   */
  async changeMembers(members: Array<{ id: string; addr: string }>) {
    const before = this.broker.cfg.members;
    const node = this.consensus.node;
    if (node) await node.propose(META, "members", members);
    this.installMembers(members);
    const targets = new Map([...before, ...this.broker.cfg.members].map((m) => [m.id, m]));
    for (const member of targets.values()) {
      if (member.id === this.broker.cfg.nodeId) continue;
      await this.call(member.id, "apply", { kind: "members", body: this.broker.cfg.members }).catch(() => null);
    }
    return this.broker.cfg.members;
  }

  /** Replace the member list from a join, forget, or `members.json`. */
  installMembers(rows: Array<{ id?: string; addr?: string }>) {
    const members = rows
      .filter((row) => row.id && row.addr)
      .map((row) => ({ id: String(row.id), addr: String(row.addr) }));
    if (members.length === 0) return;
    this.broker.cfg.members = members;
    void Bun.write(`${this.broker.cfg.dataDir}/members.json`, JSON.stringify(members));
    this.consensus?.membersChanged();
  }

  private loadMembers() {
    const path = `${this.broker.cfg.dataDir}/members.json`;
    try {
      const rows = JSON.parse(readFileSync(path, "utf8")) as Array<{ id?: string; addr?: string }>;
      if (Array.isArray(rows) && rows.length > 0) this.installMembers(rows);
    } catch {
      void Bun.write(path, JSON.stringify(this.broker.cfg.members));
    }
  }

  /** Close the listen socket. Tests use this so the process can exit. */
  stop() {
    if (this.dialTimer) clearInterval(this.dialTimer);
    this.dialTimer = null;
    for (const socket of this.sockets) socket.destroy();
    this.sockets = [];
    this.server?.close();
    this.server = null;
    this.consensus.stop();
  }

  private dial() {
    const self = this.broker.cfg.nodeId;
    for (const member of this.broker.cfg.members) {
      if (member.id <= self) continue;
      if (this.peers.has(member.id)) continue;
      const { host, port } = splitHost(member.addr);
      const sock = connect({ host, port });
      sock.setNoDelay(true);
      const flow = newFlow();
      sock.on("drain", () => drained(flow));
      const send = lineWriter(sock, flow);
      const write = (line: object) => send(`${JSON.stringify(line)}\n`);
      const writeParts = (parts: Uint8Array[]) => {
        for (const part of parts) send(part);
      };
      const reader = new FrameReader();
      sock.on("data", (chunk: Buffer) => reader.push(chunk, (line) => this.dispatch(member.id, line, write, undefined, flow)));
      const token = {};
      sock.on("connect", () => {
        this.broker.clearQuorumStrikes(member.id);
        const peer: Peer = { id: member.id, write, writeParts, pending: new Map(), token };
        this.peers.set(member.id, peer);
        this.broker.promoteIfLeader();
        write({ v: 1, op: "hello", id: 0, nodeId: self, payload: { v: 1, node: self, snapshot: this.broker.snapshot(), consumed: this.broker.consumed, features: this.consensus.features() } });
      });
      const dropIfCurrent = () => {
        this.closedTokens.add(token);
        if (this.peers.get(member.id)?.token === token) this.peers.delete(member.id);
        this.broker.promoteIfLeader();
      };
      sock.on("close", dropIfCurrent);
      sock.on("error", () => {
        dropIfCurrent();
        this.broker.noteQuorumDown(member.id);
      });
    }
    if (!this.broker.quorumHold) return;
    for (const member of this.broker.cfg.members) {
      if (member.id >= self) continue;
      if (this.broker.heardPeers.has(member.id) || this.broker.downPeers.has(member.id)) continue;
      this.probeLower(member.id, member.addr);
    }
  }

  /** One connect to a lower id. Refusal counts as a strike. Success leaves the real dial to that peer. */
  private probeLower(id: string, addr: string) {
    const { host, port } = splitHost(addr);
    const sock = connect({ host, port });
    sock.setNoDelay(true);
    let settled = false;
    const finish = (down: boolean) => {
      if (settled) return;
      settled = true;
      sock.destroy();
      if (down) this.broker.noteQuorumDown(id);
      else this.broker.clearQuorumStrikes(id);
    };
    sock.on("connect", () => finish(false));
    sock.on("error", () => finish(true));
  }

  private onData(
    socket: { data: ClusterSock; write: (s: string) => number; writeParts: (parts: Uint8Array[]) => void },
    reader: FrameReader,
    data: Buffer,
    flow?: Flow,
  ) {
    reader.push(data, (line) => this.dispatch("", line, (obj) => socket.write(`${JSON.stringify(obj)}\n`), socket, flow));
  }

  private dispatch(
    fallbackId: string,
    msg: Record<string, unknown>,
    write: (obj: object) => void,
    socket?: { data?: { peerId?: string; token?: object }; writeParts?: (parts: Uint8Array[]) => void },
    flow?: Flow,
  ) {
    const op = String(msg.op ?? "");
    if (op === "hello") {
      const payload = (msg.payload ?? {}) as Record<string, unknown>;
      const id = String(msg.nodeId ?? payload.node ?? fallbackId);
      if (id) {
        const token = {};
        this.peers.set(id, { id, write: (line) => write(line), writeParts: socket?.writeParts, pending: new Map(), token });
        if (socket?.data) {
          socket.data.peerId = id;
          socket.data.token = token;
        }
        this.broker.promoteIfLeader();
      }
      const snap = (payload.snapshot ?? msg.snapshot) as ReturnType<Broker["snapshot"]> | undefined;
      try {
        this.broker.applySnapshot(snap);
      } catch {
        /* a peer of the other implementation keeps its own files */
      }
      this.broker.applyConsumed(payload.consumed as Array<{ vhost?: string; queue?: string; id?: string }>);
      this.broker.noteQuorumPeer(id);
      this.broker.retryConsumedDrops();
      write({
        v: 1,
        op: "reply",
        id: msg.id ?? 0,
        ok: true,
        from: this.broker.cfg.nodeId,
        nodeId: this.broker.cfg.nodeId,
        payload: { v: 1, node: this.broker.cfg.nodeId, snapshot: this.broker.snapshot(), consumed: this.broker.consumed, features: this.consensus.features() },
      });
      this.consensus.noteFeatures(id, payload);
      return;
    }
    if (op === "feature") {
      // A member enabled a feature flag; only raft is cluster-wide.
      if ((msg.payload as { name?: unknown } | undefined)?.name === "raft") this.consensus.enableNow();
      return;
    }
    if (op === "raft") {
      this.consensus.step(String(msg.from ?? msg.nodeId ?? fallbackId), (msg.payload ?? {}) as Parameters<Consensus["step"]>[1]);
      return;
    }
    if (op === "reply") {
      const id = String(msg.from ?? msg.nodeId ?? "");
      const peer = id ? this.peers.get(id) : [...this.peers.values()][0];
      const waiter = peer ? takeWaiter(peer, Number(msg.id)) : undefined;
      if (waiter) {
        if (msg.ok === false) waiter.reject(new Error(String(msg.error ?? "cluster error")));
        else waiter.resolve(msg.payload);
      }
      const payload = (msg.payload ?? {}) as { snapshot?: ReturnType<Broker["snapshot"]>; consumed?: Array<{ vhost?: string; queue?: string; id?: string }> };
      try {
        if (msg.snapshot) this.broker.applySnapshot(msg.snapshot as ReturnType<Broker["snapshot"]>);
        if (payload.snapshot) this.broker.applySnapshot(payload.snapshot);
      } catch {
        /* a peer of the other implementation keeps its own files */
      }
      this.broker.applyConsumed(payload.consumed);
      if (Array.isArray(payload.consumed)) this.broker.retryConsumedDrops();
      this.consensus.noteFeatures(String(msg.from ?? msg.nodeId ?? id ?? ""), payload as Record<string, unknown>, Number(msg.id) === 0);
      if (Array.isArray(payload.consumed) || Number(msg.id) === 0) {
        this.broker.noteQuorumPeer(String(msg.from ?? msg.nodeId ?? ""));
      }
      return;
    }
    if (op === "apply" || op === "enqueue" || op === "quorum_append" || op === "quorum_drop" || op === "quorum_forget" || op === "ack" || op === "nack" || op === "get" || op === "purge" || op === "declare" || op === "declare_queue" || op === "delete_queue" || op === "unsub" || op === "credit" || op === "set_credit" || op === "stats" || op === "delete" || op === "settle" || op === "requeue" || op === "set-args" || op === "join" || op === "forget" || op === "place_queue") {
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
      // `credit` is topped up by the subscriber (Rust's `credit` op).
      // `ack_credit` is Bun's: the prefetch, returned here one per ack or nack
      // of its deliveries, so the subscriber sends nothing extra.
      const ackCredit = typeof body.credit !== "number" && typeof body.ack_credit === "number" ? body.ack_credit : null;
      let left: number | null = typeof body.credit === "number" ? body.credit : ackCredit;
      let reserved = 0;
      const noAck = !!(body.noAck ?? body.no_ack);
      const subKey = `${vhost}\0${queue}\0${session}`;
      const wake = () => this.broker.pump(q);
      this.remoteSubs.get(subKey)?.flow?.wake.delete(this.remoteSubs.get(subKey)!.wake);
      flow?.wake.add(wake);
      this.remoteSubs.set(subKey, {
        flow,
        wake,
        refill: ackCredit == null ? undefined : () => {
          if (left != null) left++;
        },
      });
      q.consumers.push({
        tag: `remote-${session}`,
        session,
        noAck,
        exclusive: !!body.exclusive,
        want: () => !flow?.full && (left == null || reserved < left),
        reserve: () => {
          reserved++;
        },
        addCredit: (n: number) => {
          if (left != null) left += n;
        },
        setCredit: (n: number | null) => {
          left = n;
        },
        deliver: (m) => {
          if (reserved > 0) reserved--;
          if (left != null) left = Math.max(0, left - 1);
          const deliveryId = this.nextDelivery++;
          if (!noAck) {
            this.remoteAcks.set(`${vhost}\0${queue}\0${deliveryId}`, m.id);
            this.remoteSessions.set(`${vhost}\0${queue}\0${deliveryId}`, session);
          }
          // One copy of the body: Bun, Rust, and PHP all read `payload.message`.
          write({
            op: "deliver",
            id: msg.id,
            session,
            payload: {
              session,
              delivery_id: deliveryId,
              offset: m.rowId ?? 0,
              settles_on_write: noAck,
              message: {
                exchange: m.exchange,
                routing_key: m.routingKey,
                body_b64: Buffer.from(m.body).toString("base64"),
                propRaw: m.propRaw.length ? Buffer.from(m.propRaw).toString("base64") : undefined,
                persistent: m.persistent,
                priority: m.priority,
                redelivered: !!m.redelivered,
                message_id: m.id,
              },
            },
          });
        },
      });
      write({ op: "reply", id: msg.id, ok: true, from: this.broker.cfg.nodeId });
      this.broker.pump(q);
      return;
    }
    if (op === "deliver") {
      const payload = (msg.payload ?? {}) as Record<string, unknown>;
      const session = Number(msg.session ?? payload.session);
      const fn = this.subs.get(session);
      if (!fn) return;
      const rustMsg = payload.message as Record<string, unknown> | undefined;
      const rustBody = rustMsg ? String(rustMsg.body_b64 ?? rustMsg.body ?? "") : "";
      if (rustMsg && rustBody) {
        fn({
          id: String(payload.delivery_id ?? rustMsg.message_id ?? ""),
          rowId: null,
          body: new Uint8Array(Buffer.from(rustBody, "base64")),
          exchange: String(rustMsg.exchange ?? ""),
          routingKey: String(rustMsg.routing_key ?? rustMsg.routingKey ?? ""),
          headers: [],
          propRaw: typeof rustMsg.propRaw === "string" ? new Uint8Array(Buffer.from(rustMsg.propRaw, "base64")) : new Uint8Array(),
          persistent: rustMsg.persistent !== false,
          priority: Number(rustMsg.priority ?? 0),
          expiresAt: null,
          redelivered: !!rustMsg.redelivered,
        });
        return;
      }
      const raw = msg.msg as (LiveMsg & { body: string; propRaw: string }) | undefined;
      if (raw?.body) {
        fn({
          ...raw,
          body: new Uint8Array(Buffer.from(raw.body, "base64")),
          propRaw: new Uint8Array(Buffer.from(raw.propRaw ?? "", "base64")),
        });
      }
    }
  }

  private async handle(op: string, msg: Record<string, unknown>): Promise<unknown> {
    if (op === "place_queue") {
      const p = (msg.payload ?? msg) as Record<string, unknown>;
      return this.broker.workerPool() ? this.broker.assignHome(String(p.vhost ?? "/"), String(p.name ?? "")) : null;
    }
    if (op === "apply") {
      const payload = (msg.payload ?? {}) as Record<string, unknown>;
      const kind = String(msg.kind || payload.kind || "");
      const body = (payload.body ?? payload) as Record<string, unknown> | Array<{ id?: string; addr?: string }>;
      if (kind === "members" && Array.isArray(body)) {
        this.installMembers(body);
        return true;
      }
      this.broker.applyRemote(kind, (body ?? {}) as Record<string, unknown>);
      return true;
    }
    if (op === "join") {
      const payload = (msg.payload ?? {}) as { id?: string; addr?: string };
      const members = this.broker.cfg.members.filter((member) => member.id !== payload.id);
      members.push({ id: String(payload.id ?? ""), addr: String(payload.addr ?? "") });
      return this.changeMembers(members);
    }
    if (op === "forget") {
      const payload = (msg.payload ?? {}) as { id?: string };
      const id = String(payload.id ?? "");
      if (id === this.broker.cfg.nodeId) throw new Error("a node cannot forget itself");
      if ([...this.broker.queues.values()].some((queue) => queue.home === id)) {
        throw new Error(`member ${id} still homes a classic queue`);
      }
      const members = this.broker.cfg.members.filter((member) => member.id !== id);
      if (members.length === 0) throw new Error("the member list cannot become empty");
      return this.changeMembers(members);
    }
    if (op === "unsub") {
      const body = (msg.payload ?? msg) as Record<string, unknown>;
      const q = this.broker.queues.get(this.broker.key(String(body.vhost), String(body.queue)));
      if (q) q.consumers = q.consumers.filter((c) => c.session !== Number(body.session));
      const subKey = `${String(body.vhost)}\0${String(body.queue)}\0${Number(body.session)}`;
      const sub = this.remoteSubs.get(subKey);
      sub?.flow?.wake.delete(sub.wake);
      this.remoteSubs.delete(subKey);
      return true;
    }
    if (op === "declare" || op === "declare_queue" || op === "delete_queue") {
      const payload = { ...((msg.payload ?? msg) as Record<string, unknown>) };
      if (op === "delete_queue") {
        this.broker.applyRemote("delete_queue", payload);
        return true;
      }
      if (payload.name == null || payload.name === "") payload.name = payload.queue;
      if (payload.home == null || payload.home === "") payload.home = this.broker.cfg.nodeId;
      this.broker.applyRemote("queue", payload);
      return {
        queue: {
          vhost: String(payload.vhost ?? "/"),
          name: String(payload.name ?? ""),
          durable: payload.durable !== false,
          exclusive: payload.exclusive === true,
          auto_delete: payload.auto_delete === true || payload.autoDelete === true,
          home: String(payload.home),
        },
      };
    }
    if (op === "stats") {
      const p = (msg.payload ?? msg) as Record<string, unknown>;
      const q = this.broker.queues.get(this.broker.key(String(p.vhost ?? "/"), String(p.queue ?? p.name ?? "")));
      return {
        messages_ready: q?.ready.length ?? 0,
        consumer_count: q?.consumers.length ?? 0,
      };
    }
    if (op === "quorum_drop") {
      const p = (msg.payload ?? msg) as Record<string, unknown>;
      this.broker.dropLocal(String(p.vhost), String(p.queue), String(p.id ?? p.message_id ?? p.qid ?? ""));
      return true;
    }
    if (op === "quorum_forget") {
      const p = (msg.payload ?? msg) as { items?: Array<{ vhost?: string; queue?: string; id?: string }> };
      this.broker.applyForget(p.items);
      return true;
    }
    if (op === "quorum_append" || op === "enqueue") {
      const p = (msg.payload ?? msg) as Record<string, unknown>;
      const decoded = decodeQuorumAppend(p);
      // `queues` (advertised as enqueue_many): one copy of a fanout for every
      // queue this process homes, so the body crosses the wire once.
      const names = op === "enqueue" && Array.isArray(p.queues) ? (p.queues as unknown[]).map(String) : [decoded.queue];
      let ok = true;
      for (const name of names) {
        const q = this.broker.queues.get(this.broker.key(decoded.vhost, name));
        if (!q) throw new Error("NOT_FOUND");
        const stored = this.broker.enqueueLocal(q, {
          body: decoded.body,
          exchange: decoded.exchange,
          routingKey: decoded.routingKey,
          headers: decoded.headers,
          propRaw: decoded.propRaw,
          persistent: decoded.persistent,
          priority: decoded.priority,
          expiration: decoded.expiration,
          id: names.length > 1 ? undefined : decoded.messageId,
        }, 0);
        if (!stored) ok = false;
      }
      // A resolved false is "not stored". The reply envelope must be ok:false,
      // or the caller counts the peer as a durable copy.
      if (op === "quorum_append" && !ok) throw new Error("NOT_STORED");
      if (decoded.persistent) await this.broker.store.whenDurable();
      return ok;
    }
    if (op === "ack" || op === "nack") {
      const p = (msg.payload ?? msg) as Record<string, unknown>;
      const vhost = String(p.vhost);
      const queue = String(p.queue);
      const delivery = p.delivery_id ?? p.id;
      const mapped = this.remoteAcks.get(`${vhost}\0${queue}\0${delivery}`);
      if (mapped != null) {
        const session = this.remoteSessions.get(`${vhost}\0${queue}\0${delivery}`);
        this.remoteAcks.delete(`${vhost}\0${queue}\0${delivery}`);
        this.remoteSessions.delete(`${vhost}\0${queue}\0${delivery}`);
        this.remoteGets.delete(`${vhost}\0${queue}\0${mapped}`);
        // Before the ack, whose pump is what sends the next message.
        this.remoteSubs.get(`${vhost}\0${queue}\0${session}`)?.refill?.();
      } else {
        const got = this.remoteGets.get(`${vhost}\0${queue}\0${delivery}`);
        if (got != null) {
          this.remoteGets.delete(`${vhost}\0${queue}\0${delivery}`);
          this.remoteAcks.delete(got);
        }
      }
      const id = mapped ?? String(p.id ?? delivery ?? "");
      if (op === "ack") await this.broker.ack(vhost, queue, id);
      else await this.broker.nack(vhost, queue, id, p.requeue !== false);
      return true;
    }
    if (op === "delete") {
      // Rust's queue.delete on the home. Same checks as a local delete.
      const p = (msg.payload ?? msg) as Record<string, unknown>;
      const vhost = String(p.vhost ?? "/");
      const queue = String(p.queue ?? p.name ?? "");
      const q = this.broker.queues.get(this.broker.key(vhost, queue));
      if (!q) return { message_count: 0 };
      if (p.if_unused === true && q.consumers.length > 0) throw new Error(`PRECONDITION_FAILED - queue '${queue}' in vhost '${vhost}' in use`);
      if (p.if_empty === true && q.ready.length > 0) throw new Error(`PRECONDITION_FAILED - queue '${queue}' in vhost '${vhost}' not empty`);
      return { message_count: await this.broker.deleteQueue(vhost, queue) };
    }
    if (op === "settle") {
      // A no-ack delivery to a remote consumer is settled once written.
      const p = (msg.payload ?? msg) as Record<string, unknown>;
      const vhost = String(p.vhost);
      const queue = String(p.queue);
      const key = `${vhost}\0${queue}\0${p.delivery_id ?? p.id}`;
      const id = this.remoteAcks.get(key);
      if (id == null) return true;
      this.remoteAcks.delete(key);
      this.remoteSessions.delete(key);
      await this.broker.ack(vhost, queue, id);
      return true;
    }
    if (op === "requeue") {
      // A remote channel closed: requeue what its sessions still hold.
      const p = (msg.payload ?? msg) as Record<string, unknown>;
      const vhost = String(p.vhost);
      const queue = String(p.queue);
      const sessions = new Set(((p.sessions as unknown[]) ?? []).map(Number));
      const prefix = `${vhost}\0${queue}\0`;
      for (const [key, session] of [...this.remoteSessions]) {
        if (!key.startsWith(prefix) || !sessions.has(session)) continue;
        const id = this.remoteAcks.get(key);
        this.remoteAcks.delete(key);
        this.remoteSessions.delete(key);
        if (id != null) await this.broker.nack(vhost, queue, id, true);
      }
      return true;
    }
    if (op === "set-args") {
      // A Rust policy change for a queue homed here. Bun applies the same
      // policies from its own table, so this only re-reads them.
      this.broker.applyPolicies();
      return true;
    }
    if (op === "credit" || op === "set_credit") {
      const p = (msg.payload ?? msg) as Record<string, unknown>;
      const q = this.broker.queues.get(this.broker.key(String(p.vhost), String(p.queue)));
      const consumer = q?.consumers.find((c) => c.session === Number(p.session));
      if (op === "set_credit") consumer?.setCredit?.(p.credit == null ? null : Number(p.credit));
      else consumer?.addCredit?.(Number(p.credit ?? 0));
      if (q) this.broker.pump(q);
      return true;
    }
    if (op === "purge") {
      const p = (msg.payload ?? msg) as Record<string, unknown>;
      return this.broker.purge(String(p.vhost), String(p.queue));
    }
    if (op === "get") {
      const p = (msg.payload ?? msg) as Record<string, unknown>;
      const vhost = String(p.vhost);
      const queue = String(p.queue);
      const noAck = !!(p.noAck ?? p.no_ack);
      const m = await this.broker.get(vhost, queue, noAck);
      if (!m) return { empty: true };
      // Rust settles a get by `delivery_id`, as a pushed delivery; Bun and
      // PHP by `msg.id`.
      let deliveryId = 0;
      if (!noAck) {
        deliveryId = this.nextDelivery++;
        this.remoteAcks.set(`${vhost}\0${queue}\0${deliveryId}`, m.id);
        this.remoteGets.set(`${vhost}\0${queue}\0${m.id}`, `${vhost}\0${queue}\0${deliveryId}`);
      }
      return {
        delivery_id: deliveryId,
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
      armWaiter(peer, id, resolve, reject, () => {
        reject(new ChanError(541, "INTERNAL_ERROR - queue home is unavailable"));
      });
    });
    peer.write({ op, id, payload, from: this.broker.cfg.nodeId });
    return result;
  }

  /**
   * `call` for a payload with a message body. A peer that reads binary frames
   * gets the bytes as they are; any other (Rust, PHP, an older Bun) gets
   * `body` and `propRaw` in base64, as before.
   */
  async callBytes(home: string, op: string, payload: Record<string, unknown>, body: Uint8Array, propRaw: Uint8Array): Promise<unknown> {
    const peer = this.peers.get(home);
    if (!peer?.writeParts || !this.consensus.binPeers.has(home)) {
      return this.call(home, op, { ...payload, body: Buffer.from(body).toString("base64"), propRaw: Buffer.from(propRaw).toString("base64") });
    }
    const id = this.seq++;
    const result = new Promise((resolve, reject) => {
      armWaiter(peer, id, resolve, reject, () => {
        reject(new ChanError(541, "INTERNAL_ERROR - queue home is unavailable"));
      });
    });
    peer.writeParts(encodeFrame({ op, id, payload, from: this.broker.cfg.nodeId }, body, propRaw));
    return result;
  }

  /** Tell every connected member to enable a feature flag. */
  broadcastFeature(name: string) {
    for (const peer of this.peers.values()) {
      if (peer.id === this.broker.cfg.nodeId) continue;
      peer.write({ v: 1, op: "feature", id: 0, from: this.broker.cfg.nodeId, payload: { name } });
    }
  }

  async replicate(kind: string, payload: unknown) {
    // With Raft on, metadata commits through the meta log (docs/raft.md,
    // section 5) and is still pushed, so a peer applies it before the next
    // heartbeat carries the commit. Applying twice is a no-op.
    const node = this.consensus.node;
    if (node && kind !== "members") {
      await node.propose(META, kind, payload).catch((err) => console.error("raft meta proposal", String(err)));
    }
    await Promise.all(
      [...this.peers.values()].filter((p) => p.id !== this.broker.cfg.nodeId).map((peer) => {
        const id = this.seq++;
        return new Promise((resolve) => {
          armWaiter(peer, id, resolve, () => resolve(null), () => resolve(null));
          // kind and body inside payload too: PHP reads only payload.kind/body.
          peer.write({ op: "apply", id, kind, payload: { kind, body: payload }, from: this.broker.cfg.nodeId });
        });
      }),
    );
  }

  async subscribe(home: string, payload: { vhost: string; queue: string; session: number; noAck: boolean; exclusive: boolean; ack_credit?: number }, onDeliver: (msg: LiveMsg) => void) {
    this.subs.set(payload.session, onDeliver);
    await this.call(home, "sub", payload);
    return payload.session;
  }

  /** Drop the local callback even when the remote home cannot be reached. */
  async unsubscribe(home: string, payload: { vhost: string; queue: string; session: number }): Promise<void> {
    this.subs.delete(payload.session);
    await this.call(home, "unsub", payload);
  }
}
