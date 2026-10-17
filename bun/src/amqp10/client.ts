/**
 * A small AMQP 1.0 client: SASL PLAIN or ANONYMOUS, one session, receiver
 * and sender links. Shovels use it for `amqp10` endpoints on another broker.
 */
import { connect as netConnect, type Socket } from "node:net";
import { connect as tlsConnect } from "node:tls";
import { Decoder, described, Described, encode, field, sym, Sym, ubyte, uint, ushort } from "./types.ts";

const HEADER_AMQP = Uint8Array.of(65, 77, 81, 80, 0, 1, 0, 0);
const HEADER_SASL = Uint8Array.of(65, 77, 81, 80, 3, 1, 0, 0);
const MAX_FRAME = 128 * 1024;
const WINDOW = 0x7fffffff;
const P = { open: 0x10, begin: 0x11, attach: 0x12, flow: 0x13, transfer: 0x14, disposition: 0x15, detach: 0x16, end: 0x17, close: 0x18 };
const SASL = { mechanisms: 0x40, init: 0x41, outcome: 0x44 };
const ACCEPTED = 0x24;

function frame(type: number, channel: number, body: Uint8Array): Uint8Array {
  const out = new Uint8Array(8 + body.length);
  const view = new DataView(out.buffer);
  view.setUint32(0, out.length);
  out[4] = 2;
  out[5] = type;
  view.setUint16(6, channel);
  out.set(body, 8);
  return out;
}

function performative(code: number, fields: unknown[]): Uint8Array {
  while (fields.length && (fields[fields.length - 1] === null || fields[fields.length - 1] === undefined)) fields.pop();
  return encode(described(code, fields));
}

function concat(a: Uint8Array, b: Uint8Array): Uint8Array {
  const out = new Uint8Array(a.length + b.length);
  out.set(a);
  out.set(b, a.length);
  return out;
}

type Receiver = {
  handle: number;
  credit: number;
  received: number;
  deliveryCount: number;
  onMessage: (payload: Uint8Array, deliveryId: number) => void;
  partial: { id: number; chunks: Uint8Array[] } | null;
};
type Sender = { handle: number; credit: number; deliveryCount: number; waiters: Array<() => void> };

/** Parsed `amqp://user:pass@host:port/vhost`. */
export function parseAmqpUri(uri: string): { tls: boolean; host: string; port: number; user: string; pass: string; vhost: string } {
  const u = new URL(uri);
  const tls = u.protocol === "amqps:";
  const vhostPath = u.pathname.replace(/^\//, "");
  return {
    tls,
    host: u.hostname || "127.0.0.1",
    port: Number(u.port) || (tls ? 5671 : 5672),
    user: decodeURIComponent(u.username),
    pass: decodeURIComponent(u.password),
    vhost: vhostPath ? decodeURIComponent(vhostPath) : "/",
  };
}

export class Amqp10Client {
  private sock!: Socket;
  private buf: Uint8Array = new Uint8Array(0);
  private stage: "sasl-header" | "sasl" | "header" | "open" = "sasl-header";
  private nextHandle = 0;
  private nextOutgoingId = 0;
  private nextDeliveryId = 0;
  private receivers = new Map<number, Receiver>();
  private senders = new Map<number, Sender>();
  private settles = new Map<number, (ok: boolean) => void>();
  private attachWaiters = new Map<number, (remote: unknown[]) => void>();
  private opened: { resolve: () => void; reject: (e: Error) => void } | null = null;
  private closedHandlers: Array<(err?: Error) => void> = [];
  closed = false;

  /** Connect, authenticate and open one session. */
  static async connect(uri: string): Promise<Amqp10Client> {
    const c = new Amqp10Client();
    await c.open(parseAmqpUri(uri));
    return c;
  }

  onClose(fn: (err?: Error) => void) {
    this.closedHandlers.push(fn);
  }

  private open(u: ReturnType<typeof parseAmqpUri>): Promise<void> {
    return new Promise((resolve, reject) => {
      this.opened = { resolve, reject };
      const onConnect = () => this.sock.write(HEADER_SASL);
      this.sock = u.tls ? tlsConnect({ host: u.host, port: u.port, rejectUnauthorized: false }, onConnect) : netConnect({ host: u.host, port: u.port }, onConnect);
      this.sock.on("data", (d: Buffer) => {
        try {
          this.feed(new Uint8Array(d), u);
        } catch (err) {
          this.fail(err as Error);
        }
      });
      this.sock.on("error", (err) => this.fail(err));
      this.sock.on("close", () => this.fail(new Error("connection closed")));
    });
  }

  private fail(err: Error) {
    if (this.closed) return;
    this.closed = true;
    this.opened?.reject(err);
    this.opened = null;
    for (const settle of this.settles.values()) settle(false);
    this.settles.clear();
    for (const fn of this.closedHandlers) fn(err);
    try {
      this.sock.destroy();
    } catch {
      /* already gone */
    }
  }

  private send(type: number, body: Uint8Array) {
    if (this.closed) return;
    this.sock.write(frame(type, 0, body));
  }

  private feed(data: Uint8Array, u: ReturnType<typeof parseAmqpUri>) {
    this.buf = this.buf.length ? concat(this.buf, data) : data;
    for (;;) {
      if (this.stage === "sasl-header" || this.stage === "header") {
        if (this.buf.length < 8) return;
        this.buf = this.buf.subarray(8);
        if (this.stage === "sasl-header") this.stage = "sasl";
        else {
          this.stage = "open";
          this.send(0, performative(P.open, [`queueforge-shovel-${crypto.randomUUID()}`, `vhost:${u.vhost}`, uint(MAX_FRAME), ushort(0)]));
          this.send(0, performative(P.begin, [null, uint(0), uint(WINDOW), uint(WINDOW), uint(255)]));
        }
        continue;
      }
      if (this.buf.length < 8) return;
      const size = new DataView(this.buf.buffer, this.buf.byteOffset, 4).getUint32(0);
      if (this.buf.length < size) return;
      const doff = this.buf[4]! * 4;
      const body = this.buf.subarray(doff, size);
      this.buf = this.buf.subarray(size);
      if (body.length === 0) continue;
      const d = new Decoder(body);
      const perf = d.value();
      if (!(perf instanceof Described)) throw new Error("bad frame");
      const code = typeof perf.descriptor === "number" ? perf.descriptor : -1;
      const f = Array.isArray(perf.value) ? perf.value : [];
      if (this.stage === "sasl") {
        if (code === SASL.mechanisms) {
          const offered = field(f, 0);
          const names = (Array.isArray(offered) ? offered : [offered]).map((m) => (m instanceof Sym ? m.s : String(m)));
          const plain = !!u.user && names.includes("PLAIN");
          const response = plain ? new TextEncoder().encode(`\0${u.user}\0${u.pass}`) : new Uint8Array(0);
          this.send(1, performative(SASL.init, [sym(plain ? "PLAIN" : "ANONYMOUS"), response]));
        } else if (code === SASL.outcome) {
          if (Number(field(f, 0)) !== 0) throw new Error("SASL login refused");
          this.stage = "header";
          this.sock.write(HEADER_AMQP);
        }
        continue;
      }
      this.onFrame(code, f, body.subarray(d.at));
    }
  }

  private onFrame(code: number, f: unknown[], payload: Uint8Array) {
    switch (code) {
      case P.begin:
        this.opened?.resolve();
        this.opened = null;
        return;
      case P.attach: {
        const handle = Number(field(f, 1));
        const local = [...this.receivers.values(), ...this.senders.values()].find((l) => l.handle === handle);
        if (!local) return;
        // A refused attach comes back with no source or target, then a detach.
        this.attachWaiters.get(handle)?.(f);
        this.attachWaiters.delete(handle);
        return;
      }
      case P.flow: {
        const handle = field(f, 4);
        if (handle == null) return;
        const s = this.senders.get(Number(handle));
        if (!s) return;
        const deliveryCount = Number(field(f, 5) ?? 0);
        const credit = Number(field(f, 6) ?? 0);
        s.credit = deliveryCount + credit - s.deliveryCount;
        while (s.credit > 0 && s.waiters.length) s.waiters.shift()!();
        return;
      }
      case P.transfer: {
        const r = this.receivers.get(Number(field(f, 0)));
        if (!r) return;
        const more = field(f, 5) === true;
        if (!r.partial) r.partial = { id: Number(field(f, 1)), chunks: [] };
        r.partial.chunks.push(payload.slice());
        if (more) return;
        const { id, chunks } = r.partial;
        r.partial = null;
        const whole = chunks.length === 1 ? chunks[0]! : chunks.reduce((a, b) => concat(a, b));
        r.deliveryCount++;
        r.received++;
        // Keep the peer's credit topped up as messages arrive.
        if (r.received >= r.credit / 2) {
          r.received = 0;
          this.flow(r);
        }
        r.onMessage(whole, id);
        return;
      }
      case P.disposition: {
        const role = field(f, 0);
        if (role !== true) return;
        const first = Number(field(f, 1));
        const last = field(f, 2) == null ? first : Number(field(f, 2));
        const state = field(f, 4);
        const ok = state instanceof Described && state.descriptor === ACCEPTED;
        for (let id = first; id <= last; id++) {
          this.settles.get(id)?.(ok);
          this.settles.delete(id);
        }
        return;
      }
      case P.detach: {
        const handle = Number(field(f, 0));
        const err = field(f, 2);
        this.attachWaiters.get(handle)?.([]);
        this.attachWaiters.delete(handle);
        if (this.receivers.has(handle) || this.senders.has(handle)) {
          this.fail(new Error(`link detached${err instanceof Described ? `: ${String(field(err.value, 1) ?? field(err.value, 0))}` : ""}`));
        }
        return;
      }
      case P.end:
      case P.close:
        this.fail(new Error("closed by peer"));
        return;
      default:
        return;
    }
  }

  private flow(r: Receiver) {
    this.send(0, performative(P.flow, [uint(0), uint(WINDOW), uint(this.nextOutgoingId), uint(WINDOW), uint(r.handle), uint(r.deliveryCount), uint(r.credit)]));
  }

  private attached(handle: number): Promise<void> {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("attach timed out")), 10_000);
      this.attachWaiters.set(handle, (remote) => {
        clearTimeout(timer);
        const sourceOrTarget = field(remote, 5) ?? field(remote, 6);
        if (sourceOrTarget == null) reject(new Error("attach refused"));
        else resolve();
      });
    });
  }

  /** Attach a receiver on `address`; `onMessage` gets each transfer payload. Settle with {@link settle}. */
  async receive(address: string, credit: number, onMessage: (payload: Uint8Array, deliveryId: number) => void): Promise<void> {
    const handle = this.nextHandle++;
    const r: Receiver = { handle, credit: Math.max(1, credit), received: 0, deliveryCount: 0, onMessage, partial: null };
    this.receivers.set(handle, r);
    const done = this.attached(handle);
    // role receiver, sender settle unsettled (0), receiver settle first (0)
    this.send(0, performative(P.attach, [`shovel-in-${handle}`, uint(handle), true, ubyte(0), ubyte(0), described(0x28, [address]), described(0x29, [null])]));
    await done;
    this.flow(r);
  }

  /** Settle one received delivery: accepted, or released for redelivery. */
  settle(deliveryId: number, accepted: boolean) {
    const state = accepted ? described(ACCEPTED, []) : described(0x26, []);
    this.send(0, performative(P.disposition, [true, uint(deliveryId), null, true, state]));
  }

  /** Attach a sender on `address`. Returns a function that sends one payload and resolves once accepted. */
  async sender(address: string): Promise<(payload: Uint8Array) => Promise<boolean>> {
    const handle = this.nextHandle++;
    const s: Sender = { handle, credit: 0, deliveryCount: 0, waiters: [] };
    this.senders.set(handle, s);
    const done = this.attached(handle);
    this.send(0, performative(P.attach, [`shovel-out-${handle}`, uint(handle), false, ubyte(0), ubyte(0), described(0x28, [null]), described(0x29, [address])]));
    await done;
    return async (payload) => {
      if (this.closed) return false;
      if (s.credit <= 0) await new Promise<void>((resolve) => s.waiters.push(resolve));
      if (this.closed) return false;
      s.credit--;
      s.deliveryCount++;
      const id = this.nextDeliveryId++;
      this.nextOutgoingId++;
      const settled = new Promise<boolean>((resolve) => this.settles.set(id, resolve));
      const tag = new Uint8Array(4);
      new DataView(tag.buffer).setUint32(0, id);
      const first = performative(P.transfer, [uint(handle), uint(id), tag, uint(0), false, false]);
      const room = MAX_FRAME - 8 - first.length - 8;
      if (payload.length <= room) this.send(0, concat(first, payload));
      else {
        for (let at = 0; at < payload.length; ) {
          const chunk = payload.subarray(at, at + room);
          at += chunk.length;
          const more = at < payload.length;
          const perf = at === chunk.length ? performative(P.transfer, [uint(handle), uint(id), tag, uint(0), false, more]) : performative(P.transfer, [uint(handle), null, null, null, false, more]);
          this.send(0, concat(perf, chunk));
        }
      }
      return settled;
    };
  }

  close() {
    if (this.closed) return;
    this.send(0, performative(P.close, []));
    this.closed = true;
    try {
      this.sock.end();
    } catch {
      /* already gone */
    }
  }
}

