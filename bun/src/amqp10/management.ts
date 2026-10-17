/**
 * RabbitMQ 4's AMQP 1.0 management node ("HTTP over AMQP", v2 paths).
 *
 * A client attaches a sender and a receiver, both named
 * `management-link-pair`, to `/management`. Each request has the HTTP method
 * as subject and the path as `to`; the reply carries the status code as
 * subject and the request's message-id as correlation-id.
 *
 * Requests run the same broker operations and permission checks as AMQP
 * 0-9-1. An operation the user may not do throws {@link MgmtRefused}; the
 * session layer ends the session with that error, as RabbitMQ does.
 */
import type { Broker } from "../broker/index.ts";
import { ChanError } from "../broker/index.ts";
import type { Field } from "../codec.ts";
import { AMap, Decoder, described, Described, encode, field, Sym, Typed } from "./types.ts";

const EXCHANGE_TYPES = new Set(["direct", "fanout", "topic", "headers", "x-consistent-hash", "x-local-random", "x-delayed-message"]);

/** The request was not allowed: end the session with this condition. */
export class MgmtRefused extends Error {
  constructor(
    readonly condition: string,
    message: string,
  ) {
    super(message);
  }
}

export type MgmtRequest = { messageId: unknown; to: string; subject: string; body: unknown };
export type MgmtReply = { status: string; body?: unknown };
/** Who is asking: the connection's broker, vhost, user, and a token refresh hook. */
export type MgmtContext = { broker: Broker; vhost: string; user: string; refreshToken: (token: string) => Promise<boolean> };

const dec = new TextDecoder();

/** Read message-id, to, subject, and the amqp-value (or data) body of a request. */
export function readRequest(payload: Uint8Array): MgmtRequest {
  const d = new Decoder(payload);
  const out: MgmtRequest = { messageId: null, to: "", subject: "", body: undefined };
  while (d.at < payload.length) {
    const s = d.value();
    if (!(s instanceof Described)) break;
    const code = typeof s.descriptor === "number" ? s.descriptor : -1;
    if (code === 0x73) {
      out.messageId = field(s.value, 0) ?? null;
      const to = field(s.value, 2);
      const subject = field(s.value, 3);
      out.to = typeof to === "string" ? to : "";
      out.subject = typeof subject === "string" ? subject : subject instanceof Sym ? subject.s : "";
    } else if (code === 0x77) out.body = s.value;
    else if (code === 0x75) out.body = s.value;
  }
  return out;
}

/** Encode a reply: properties (subject, correlation-id) and an amqp-value body. */
export function writeReply(req: MgmtRequest, reply: MgmtReply): Uint8Array {
  const props = described(0x73, [null, null, null, reply.status, null, req.messageId ?? null]);
  const head = encode(props);
  if (reply.body === undefined) return head;
  const body = encode(described(0x77, toAmqp(reply.body)));
  const out = new Uint8Array(head.length + body.length);
  out.set(head);
  out.set(body, head.length);
  return out;
}

/** Plain JS objects become AMQP maps with string keys. */
function toAmqp(v: unknown): unknown {
  if (v === null || v === undefined || typeof v !== "object") return v;
  if (v instanceof Uint8Array || v instanceof AMap || v instanceof Described || v instanceof Typed || v instanceof Sym) return v;
  if (Array.isArray(v)) return v.map(toAmqp);
  return new AMap(Object.entries(v as Record<string, unknown>).map(([k, x]) => [k, toAmqp(x)]));
}

/** An AMQP map (string or symbol keys) as a plain object. */
function plain(v: unknown): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  if (v !== null && typeof v === "object" && !(v instanceof AMap) && !Array.isArray(v) && !(v instanceof Uint8Array)) return v as Record<string, unknown>;
  if (!(v instanceof AMap)) return out;
  for (const [k, x] of v.entries) out[k instanceof Sym ? k.s : String(k)] = x instanceof AMap ? plain(x) : x instanceof Sym ? x.s : x;
  return out;
}

const truthy = (v: unknown) => v === true || v === 1 || v === "true";

function segment(s: string): string {
  try {
    return decodeURIComponent(s);
  } catch {
    return s;
  }
}

/** Queue arguments as the broker's argument record. Booleans are 1/0 as from 0-9-1. */
function queueArgs(v: unknown): Record<string, string | number> {
  const out: Record<string, string | number> = {};
  for (const [k, x] of Object.entries(plain(v))) {
    if (typeof x === "number" || typeof x === "string") out[k] = x;
    else if (typeof x === "boolean") out[k] = x ? 1 : 0;
    else if (typeof x === "bigint") out[k] = Number(x);
  }
  return out;
}

/** Binding arguments as 0-9-1 fields. */
function bindArgs(v: unknown): Array<[string, Field]> {
  const out: Array<[string, Field]> = [];
  for (const [k, x] of Object.entries(plain(v))) {
    if (typeof x === "number") out.push([k, Number.isInteger(x) ? { t: "l", v: x } : { t: "S", v: String(x) }]);
    else if (typeof x === "boolean") out.push([k, { t: "t", v: x }]);
    else if (x !== null && x !== undefined) out.push([k, { t: "S", v: x instanceof Uint8Array ? dec.decode(x) : String(x) }]);
  }
  return out;
}

function refuse(ctx: MgmtContext, kind: "configure" | "write" | "read", what: "queue" | "exchange", name: string) {
  if (ctx.broker.can(ctx.user, ctx.vhost, kind, name)) return;
  throw new MgmtRefused("amqp:unauthorized-access", `${kind} access to ${what} '${name}' in vhost '${ctx.vhost}' refused for user '${ctx.user}'`);
}

function queueMap(ctx: MgmtContext, name: string) {
  const b = ctx.broker;
  const q = b.queues.get(b.key(ctx.vhost, name))!;
  const node = q.home || b.cfg.nodeId || "queueforge";
  return {
    name: q.name,
    vhost: q.vhost,
    durable: q.durable,
    auto_delete: q.autoDelete,
    exclusive: q.exclusive,
    type: q.argsParsed.queueType,
    arguments: q.args,
    message_count: q.ready.length,
    consumer_count: q.consumers.length,
    leader: node,
    replicas: [node],
  };
}

/** A broker error as the status RabbitMQ answers with. Refusals end the session. */
function statusOf(err: ChanError): MgmtReply {
  if (err.code === 403) throw new MgmtRefused("amqp:unauthorized-access", err.message);
  const status = err.code === 404 ? "404" : err.code === 406 || err.code === 405 ? "409" : "400";
  return { status, body: err.message };
}

/** Answer one request. */
export async function handle(ctx: MgmtContext, req: MgmtRequest): Promise<MgmtReply> {
  try {
    return await route(ctx, req);
  } catch (err) {
    if (err instanceof ChanError) return statusOf(err);
    throw err;
  }
}

async function route(ctx: MgmtContext, req: MgmtRequest): Promise<MgmtReply> {
  const b = ctx.broker;
  const vhost = ctx.vhost;
  const [path, query = ""] = req.to.split("?", 2) as [string, string?];
  const parts = path.split("/").slice(1).map(segment);
  const method = req.subject.toUpperCase();
  const body = plain(req.body);

  if (parts[0] === "queues" && parts.length >= 2) {
    const name = parts[1]!;
    const exists = () => b.queues.has(b.key(vhost, name));
    if (parts.length === 3 && parts[2] === "messages" && method === "DELETE") {
      refuse(ctx, "read", "queue", name);
      return { status: "200", body: { message_count: await b.purge(vhost, name) } };
    }
    if (parts.length !== 2) return { status: "400", body: `unknown path '${req.to}'` };
    if (method === "GET") {
      if (!exists()) return { status: "404", body: `queue '${name}' in vhost '${vhost}' not found` };
      return { status: "200", body: queueMap(ctx, name) };
    }
    if (method === "PUT") {
      refuse(ctx, "configure", "queue", name);
      const existed = exists();
      await b.declareQueue({
        vhost,
        name,
        passive: false,
        durable: body.durable === undefined ? true : truthy(body.durable),
        exclusive: truthy(body.exclusive),
        autoDelete: truthy(body.auto_delete),
        args: queueArgs(body.arguments),
      });
      return { status: existed ? "200" : "201", body: queueMap(ctx, name) };
    }
    if (method === "DELETE") {
      refuse(ctx, "configure", "queue", name);
      if (!exists()) return { status: "200", body: { message_count: 0 } };
      return { status: "200", body: { message_count: await b.deleteQueue(vhost, name) } };
    }
  }

  if (parts[0] === "exchanges" && parts.length === 2) {
    const name = parts[1]!;
    if (method === "PUT") {
      refuse(ctx, "configure", "exchange", name);
      const type = String(body.type ?? "direct");
      if (!EXCHANGE_TYPES.has(type)) return { status: "400", body: `unknown exchange type '${type}'` };
      const args = plain(body.arguments);
      const str = (k: string) => (args[k] === undefined || args[k] === null ? null : String(args[k]));
      const header = str("hash-header");
      const property = str("hash-property");
      if (header && property) return { status: "400", body: "hash-header and hash-property are mutually exclusive" };
      const existing = b.exchanges.get(b.key(vhost, name));
      if (existing) {
        if (existing.kind !== type || existing.durable !== (body.durable === undefined ? true : truthy(body.durable))) {
          return { status: "409", body: `inequivalent arg for exchange '${name}' in vhost '${vhost}'` };
        }
        return { status: "204" };
      }
      await b.declareExchange(
        vhost,
        name,
        type,
        body.durable === undefined ? true : truthy(body.durable),
        truthy(body.auto_delete),
        truthy(body.internal),
        str("alternate-exchange"),
        str("x-delayed-type"),
        header ? `header:${header}` : property ? `property:${property}` : null,
      );
      return { status: "204" };
    }
    if (method === "DELETE") {
      refuse(ctx, "configure", "exchange", name);
      if (b.exchanges.has(b.key(vhost, name))) await b.deleteExchange(vhost, name);
      return { status: "204" };
    }
    if (method === "GET") {
      const ex = b.exchanges.get(b.key(vhost, name));
      if (!ex) return { status: "404", body: `exchange '${name}' in vhost '${vhost}' not found` };
      return { status: "200", body: { name, type: ex.kind, durable: ex.durable, auto_delete: ex.autoDelete, internal: ex.internal, arguments: {} } };
    }
  }

  if (parts[0] === "bindings") {
    if (method === "POST" && parts.length === 1) {
      const source = String(body.source ?? "");
      const key = String(body.binding_key ?? "");
      const dq = body.destination_queue;
      const de = body.destination_exchange;
      const destination = String(dq ?? de ?? "");
      refuse(ctx, "read", "exchange", source || "amq.default");
      refuse(ctx, "write", dq !== undefined ? "queue" : "exchange", destination);
      const ex = b.exchanges.get(b.key(vhost, source));
      if (ex?.kind === "topic" && !b.topicReadAllowed(ctx.user, vhost, source, key)) {
        throw new MgmtRefused("amqp:unauthorized-access", `read access to topic '${key}' in exchange '${source}' in vhost '${vhost}' refused for user '${ctx.user}'`);
      }
      if (dq !== undefined) await b.bind(vhost, source, destination, key, bindArgs(body.arguments));
      else await b.bindExchange(vhost, source, destination, key);
      return { status: "204" };
    }
    const spec = parts.length === 2 ? parseBinding(path.split("/")[2] ?? "") : parseQuery(query);
    const source = spec.src ?? "";
    const key = spec.key ?? "";
    if (method === "DELETE" && parts.length === 2) {
      const destination = spec.dstq ?? spec.dste ?? "";
      refuse(ctx, "read", "exchange", source || "amq.default");
      refuse(ctx, "write", spec.dstq !== undefined ? "queue" : "exchange", destination);
      if (spec.dstq !== undefined) await b.unbind(vhost, source, destination, key, spec.args ? null : []);
      else await b.unbindExchange(vhost, source, destination, key);
      return { status: "204" };
    }
    if (method === "GET") {
      const list: unknown[] = [];
      for (const x of b.bindings) {
        if (x.vhost !== vhost || (spec.src !== undefined && x.exchange !== spec.src) || (spec.dstq !== undefined && x.queue !== spec.dstq)) continue;
        if (spec.dste !== undefined || (spec.key !== undefined && x.routingKey !== spec.key)) continue;
        list.push({ source: x.exchange, vhost, destination: x.queue, destination_type: "queue", binding_key: x.routingKey, arguments: {} });
      }
      for (const x of b.e2e) {
        if (x.vhost !== vhost || (spec.src !== undefined && x.source !== spec.src) || (spec.dste !== undefined && x.destination !== spec.dste)) continue;
        if (spec.dstq !== undefined || (spec.key !== undefined && x.routingKey !== spec.key)) continue;
        list.push({ source: x.source, vhost, destination: x.destination, destination_type: "exchange", binding_key: x.routingKey, arguments: {} });
      }
      return { status: "200", body: list };
    }
  }

  if (parts[0] === "auth" && parts[1] === "tokens" && method === "PUT") {
    const token = req.body instanceof Uint8Array ? dec.decode(req.body) : typeof req.body === "string" ? req.body : "";
    if (!token || !(await ctx.refreshToken(token))) throw new MgmtRefused("amqp:unauthorized-access", "token refresh failed");
    return { status: "204" };
  }

  return { status: "400", body: `unsupported request ${method} '${req.to}'` };
}

/** `src=x;dstq=q;key=k;args=` with percent-encoded values. */
function parseBinding(s: string): Record<string, string> {
  const out: Record<string, string> = {};
  for (const kv of s.split(";")) {
    const at = kv.indexOf("=");
    if (at < 0) continue;
    const v = segment(kv.slice(at + 1));
    if (kv.slice(0, at) === "args" && v === "") continue;
    out[kv.slice(0, at)] = v;
  }
  return out;
}

function parseQuery(s: string): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [k, v] of new URLSearchParams(s)) out[k] = v;
  return out;
}
