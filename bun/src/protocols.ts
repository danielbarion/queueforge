import { Broker } from "./broker.ts";
import { emptyProps } from "./codec.ts";

async function push(broker: Broker, name: string, body: Uint8Array) {
  try {
    await broker.declareQueue({
      vhost: "/",
      name,
      durable: false,
      exclusive: false,
      autoDelete: false,
      passive: false,
      args: {},
    });
  } catch {
    // already declared
  }
  await broker.publish({
    vhost: "/",
    exchange: "",
    routingKey: name,
    body,
    headers: [],
    propRaw: emptyProps(),
    persistent: false,
    priority: 0,
    expiration: "",
  });
}

async function pull(broker: Broker, name: string): Promise<Uint8Array | null> {
  try {
    const msg = await broker.get("/", name, true);
    return msg ? msg.body : null;
  } catch {
    return null;
  }
}

function mqttMatch(filter: string, topic: string): boolean {
  if (filter === topic || filter === "#") return true;
  const f = filter.split("/");
  const t = topic.split("/");
  for (let i = 0; i < f.length; i++) {
    if (f[i] === "#") return true;
    if (i >= t.length) return false;
    if (f[i] !== "+" && f[i] !== t[i]) return false;
  }
  return f.length === t.length;
}

function mqttStr(buf: Uint8Array, at: number): { text: string; next: number } | null {
  if (at + 2 > buf.length) return null;
  const n = (buf[at]! << 8) | buf[at + 1]!;
  const end = at + 2 + n;
  if (end > buf.length) return null;
  return { text: new TextDecoder().decode(buf.subarray(at + 2, end)), next: end };
}

function mqttEncodeLen(len: number): number[] {
  const out: number[] = [];
  do {
    let byte = len % 128;
    len = Math.floor(len / 128);
    if (len > 0) byte |= 0x80;
    out.push(byte);
  } while (len > 0);
  return out;
}

function mqttPublish(topic: string, payload: Uint8Array): Uint8Array {
  const topicBytes = new TextEncoder().encode(topic);
  const rest = new Uint8Array(2 + topicBytes.length + payload.length);
  rest[0] = topicBytes.length >> 8;
  rest[1] = topicBytes.length & 0xff;
  rest.set(topicBytes, 2);
  rest.set(payload, 2 + topicBytes.length);
  const len = mqttEncodeLen(rest.length);
  const out = new Uint8Array(1 + len.length + rest.length);
  out[0] = 0x30;
  out.set(len, 1);
  out.set(rest, 1 + len.length);
  return out;
}

type MqttSub = { filter: string; write: (frame: Uint8Array) => void };
type StompSub = { destination: string; id: string; write: (frame: string) => void };

const mqttHub: MqttSub[] = [];
const stompHub: StompSub[] = [];
const streams = new Map<string, Uint8Array[]>();

function mqttFanout(topic: string, payload: Uint8Array) {
  const frame = mqttPublish(topic, payload);
  for (const sub of mqttHub) {
    if (mqttMatch(sub.filter, topic)) sub.write(frame);
  }
}

function stompQueue(dest: string): string {
  return dest.replace(/^\/queue\//, "").replace(/^\/topic\//, "");
}

function stompFanout(dest: string, body: string) {
  const queue = stompQueue(dest);
  for (const sub of stompHub) {
    if (sub.destination === dest || stompQueue(sub.destination) === queue) {
      sub.write(`MESSAGE\nsubscription:${sub.id}\ndestination:${dest}\ncontent-length:${body.length}\n\n${body}\0`);
    }
  }
}

export function startMqtt(host: string, port: number, broker: Broker) {
  Bun.listen({
    hostname: host,
    port,
    socket: {
      open(socket) {
        socket.data = { buf: new Uint8Array() };
      },
      async data(socket, data) {
        const state = socket.data as { buf: Uint8Array };
        const merged = new Uint8Array(state.buf.length + data.length);
        merged.set(state.buf);
        merged.set(data, state.buf.length);
        state.buf = merged;
        while (state.buf.length > 2) {
          let value = 0;
          let shift = 0;
          let i = 1;
          let done = false;
          while (i < state.buf.length && shift < 28) {
            const byte = state.buf[i]!;
            value += (byte & 0x7f) << shift;
            i++;
            if ((byte & 0x80) === 0) {
              done = true;
              break;
            }
            shift += 7;
          }
          if (!done || state.buf.length < i + value) break;
          const kind = state.buf[0]! >> 4;
          const body = state.buf.subarray(i, i + value);
          state.buf = state.buf.subarray(i + value);
          if (kind === 1) socket.write(Uint8Array.of(0x20, 0x02, 0x00, 0x00));
          else if (kind === 3) {
            const topic = mqttStr(body, 0);
            if (!topic) continue;
            const payload = body.subarray(topic.next);
            mqttFanout(topic.text, payload);
            try {
              await push(broker, topic.text, payload);
            } catch (err) {
              console.error("mqtt publish", err);
            }
          } else if (kind === 8 && body.length >= 2) {
            const id0 = body[0]!;
            const id1 = body[1]!;
            let at = 2;
            const codes: number[] = [];
            let last = "";
            while (at < body.length) {
              const filter = mqttStr(body, at);
              if (!filter) break;
              at = filter.next + 1;
              last = filter.text;
              mqttHub.push({
                filter: filter.text,
                write: (frame) => {
                  try {
                    socket.write(frame);
                  } catch {
                    /* subscriber already closed */
                  }
                },
              });
              codes.push(0);
            }
            const len = mqttEncodeLen(2 + codes.length);
            const out = new Uint8Array(1 + len.length + 2 + codes.length);
            out[0] = 0x90;
            out.set(len, 1);
            out[1 + len.length] = id0;
            out[2 + len.length] = id1;
            out.set(codes, 3 + len.length);
            socket.write(out);
            if (last) {
              const queued = await pull(broker, last);
              if (queued) socket.write(mqttPublish(last, queued));
            }
          } else if (kind === 12) socket.write(Uint8Array.of(0xd0, 0x00));
          else if (kind === 14) socket.end();
        }
      },
    },
  });
}

export function startStomp(host: string, port: number, broker: Broker) {
  Bun.listen({
    hostname: host,
    port,
    socket: {
      open(socket) {
        socket.data = { buf: new Uint8Array() };
      },
      async data(socket, data) {
        const state = socket.data as { buf: Uint8Array };
        const merged = new Uint8Array(state.buf.length + data.length);
        merged.set(state.buf);
        merged.set(data, state.buf.length);
        state.buf = merged;
        const text = new TextDecoder().decode(state.buf);
        let nul = text.indexOf("\0");
        let consumed = 0;
        while (nul >= 0) {
          const frame = text.slice(consumed, nul);
          consumed = nul + 1;
          const lines = frame.split("\n").map((l) => l.replace(/\r$/, ""));
          const cmd = lines[0] ?? "";
          const headers = new Map<string, string>();
          let bodyAt = lines.length;
          for (let i = 1; i < lines.length; i++) {
            if (lines[i] === "") {
              bodyAt = i + 1;
              break;
            }
            const sep = lines[i]!.indexOf(":");
            if (sep > 0) headers.set(lines[i]!.slice(0, sep), lines[i]!.slice(sep + 1));
          }
          const body = lines.slice(bodyAt).join("\n");
          if (cmd === "CONNECT" || cmd === "STOMP") socket.write("CONNECTED\nversion:1.2\nheart-beat:0,0\n\n\0");
          else if (cmd === "SEND") {
            const dest = headers.get("destination") ?? "";
            const queue = stompQueue(dest);
            const n = Number(headers.get("content-length"));
            const raw = Number.isFinite(n) ? body.slice(0, n) : body;
            stompFanout(dest, raw);
            try {
              await push(broker, queue, new TextEncoder().encode(raw));
            } catch (err) {
              console.error("stomp publish", err);
            }
          } else if (cmd === "SUBSCRIBE") {
            const id = headers.get("id") ?? "0";
            const dest = headers.get("destination") ?? "";
            stompHub.push({
              destination: dest,
              id,
              write: (frame) => {
                try {
                  socket.write(frame);
                } catch {
                  /* subscriber already closed */
                }
              },
            });
            const queued = await pull(broker, stompQueue(dest));
            if (queued) {
              const textBody = new TextDecoder().decode(queued);
              socket.write(`MESSAGE\nsubscription:${id}\ndestination:${dest}\ncontent-length:${textBody.length}\n\n${textBody}\0`);
            }
          } else if (cmd === "DISCONNECT") socket.end();
          nul = text.indexOf("\0", consumed);
        }
        state.buf = new TextEncoder().encode(text.slice(consumed));
      },
    },
  });
}

function u16(n: number): Uint8Array {
  return Uint8Array.of((n >> 8) & 0xff, n & 0xff);
}
function u32(n: number): Uint8Array {
  const b = new Uint8Array(4);
  new DataView(b.buffer).setUint32(0, n);
  return b;
}
function i32(n: number): Uint8Array {
  const b = new Uint8Array(4);
  new DataView(b.buffer).setInt32(0, n);
  return b;
}
function concat(...parts: Uint8Array[]): Uint8Array {
  const n = parts.reduce((s, p) => s + p.length, 0);
  const out = new Uint8Array(n);
  let at = 0;
  for (const p of parts) {
    out.set(p, at);
    at += p.length;
  }
  return out;
}
function streamStr(text: string): Uint8Array {
  const b = new TextEncoder().encode(text);
  return concat(Uint8Array.of((b.length >> 8) & 0xff, b.length & 0xff), b);
}
function streamFrame(payload: Uint8Array): Uint8Array {
  return concat(u32(payload.length), payload);
}
function streamResp(key: number, corr: number, extra: Uint8Array): Uint8Array {
  return streamFrame(concat(u16(key), u16(1), u32(corr), u16(1), extra));
}
function readStr(buf: Uint8Array, at: number): { text: string; next: number } | null {
  if (at + 2 > buf.length) return null;
  const n = (buf[at]! << 8) | buf[at + 1]!;
  if (at + 2 + n > buf.length) return null;
  return { text: new TextDecoder().decode(buf.subarray(at + 2, at + 2 + n)), next: at + 2 + n };
}

export type Amqp10State = { phase: "header" | "sasl" | "open"; sender: string | null; receiver: string | null };

function amqpFrame(ftype: number, body: Uint8Array): Uint8Array {
  const out = new Uint8Array(8 + body.length);
  const view = new DataView(out.buffer);
  view.setUint32(0, out.length);
  out[4] = 2;
  out[5] = ftype;
  out.set(body, 8);
  return out;
}

function amqpPerf(code: number): Uint8Array {
  return amqpFrame(0, Uint8Array.of(0x00, 0x53, code, 0x45));
}

function queueFrom(body: Uint8Array): string | null {
  const marker = new TextEncoder().encode("/queues/");
  for (let i = 0; i + marker.length < body.length; i++) {
    let ok = true;
    for (let j = 0; j < marker.length; j++) if (body[i + j] !== marker[j]) ok = false;
    if (!ok || i < 2 || body[i - 2] !== 0xa1) continue;
    const n = body[i - 1]!;
    return new TextDecoder().decode(body.subarray(i + marker.length, i - 2 + 2 + n));
  }
  return null;
}

function dataSection(body: Uint8Array): Uint8Array | null {
  for (let i = 0; i + 5 < body.length; i++) {
    if (body[i] === 0x00 && body[i + 1] === 0x53 && body[i + 2] === 0x75 && body[i + 3] === 0xa0) {
      const n = body[i + 4]!;
      return body.subarray(i + 5, i + 5 + n);
    }
  }
  return null;
}

function has(body: Uint8Array, bytes: number[]): boolean {
  for (let i = 0; i + bytes.length <= body.length; i++) {
    let ok = true;
    for (let j = 0; j < bytes.length; j++) if (body[i + j] !== bytes[j]) ok = false;
    if (ok) return true;
  }
  return false;
}

export async function driveAmqp10(
  buf: Uint8Array,
  state: Amqp10State,
  write: (frame: Uint8Array) => void,
  broker: Broker,
): Promise<Uint8Array> {
  if (state.phase === "header" && buf.length >= 8 && buf[0] === 65) {
    const sasl = buf[4] === 3;
    buf = buf.subarray(8);
    if (sasl) {
      write(Uint8Array.of(65, 77, 81, 80, 3, 1, 0, 0));
      write(amqpFrame(1, Uint8Array.of(0x00, 0x53, 0x40, 0xc0, 0x0a, 0x01, 0xa3, 0x05, 0x50, 0x4c, 0x41, 0x49, 0x4e)));
      state.phase = "sasl";
    } else {
      write(Uint8Array.of(65, 77, 81, 80, 0, 1, 0, 0));
      state.phase = "open";
    }
  }
  while (buf.length >= 8 && buf[0] !== 65) {
    const size = new DataView(buf.buffer, buf.byteOffset, 4).getUint32(0);
    if (size < 8 || buf.length < size) break;
    const frame = buf.subarray(0, size);
    const body = frame.subarray(8);
    buf = buf.subarray(size);
    if (state.phase === "sasl") {
      if (has(body, [0x00, 0x53, 0x41])) {
        write(amqpFrame(1, Uint8Array.of(0x00, 0x53, 0x44, 0xc0, 0x04, 0x02, 0x50, 0x00, 0x40)));
      }
      continue;
    }
    if (buf.length >= 8 && false) break;
    if (has(body, [0x00, 0x53, 0x10])) write(amqpPerf(0x10));
    else if (has(body, [0x00, 0x53, 0x11])) write(amqpPerf(0x11));
    else if (has(body, [0x00, 0x53, 0x12])) {
      const queue = queueFrom(body);
      if (queue && has(body, [0x41]) && has(body, [0x00, 0x53, 0x28])) state.receiver = queue;
      else if (queue) state.sender = queue;
      write(amqpPerf(0x12));
      if (state.sender && !state.receiver) write(amqpPerf(0x13));
    } else if (dataSection(body) && state.sender) {
      await push(broker, state.sender, dataSection(body)!);
    } else if (has(body, [0x00, 0x53, 0x13]) && state.receiver) {
      const queued = await pull(broker, state.receiver);
      if (queued) {
        const fields = Uint8Array.of(0x52, 0x00, 0x43, 0xa0, 1, 1, 0x43, 0x41);
        const list = concat(Uint8Array.of(0xc0, 1 + fields.length, 5), fields);
        const section = concat(Uint8Array.of(0x00, 0x53, 0x75, 0xa0, queued.length), queued);
        write(amqpFrame(0, concat(Uint8Array.of(0x00, 0x53, 0x14), list, section)));
      }
    } else if (has(body, [0x00, 0x53, 0x18])) write(amqpPerf(0x18));
  }
  if (state.phase === "sasl" && buf.length >= 8 && buf[0] === 65 && buf[4] === 0 && buf[5] === 1) {
    buf = buf.subarray(8);
    write(Uint8Array.of(65, 77, 81, 80, 0, 1, 0, 0));
    state.phase = "open";
    return driveAmqp10(buf, state, write, broker);
  }
  return buf;
}

export function startStream(host: string, port: number, broker: Broker) {
  Bun.listen({
    hostname: host,
    port,
    socket: {
      open(socket) {
        socket.data = { buf: new Uint8Array(), publishers: new Map<number, string>() };
      },
      async data(socket, data) {
        const state = socket.data as { buf: Uint8Array; publishers: Map<number, string> };
        state.buf = concat(state.buf, data instanceof Uint8Array ? data : new Uint8Array(data));
        while (state.buf.length >= 4) {
          const size = new DataView(state.buf.buffer, state.buf.byteOffset, 4).getUint32(0);
          if (state.buf.length < 4 + size) break;
          const frame = state.buf.subarray(4, 4 + size);
          state.buf = state.buf.subarray(4 + size);
          if (frame.length < 4) continue;
          const key = (frame[0]! << 8) | frame[1]!;
          const rest = frame.subarray(4);
          const corr = rest.length >= 4 ? new DataView(rest.buffer, rest.byteOffset, 4).getUint32(0) : 0;
          if (key === 0x0011) {
            const props = concat(u32(2), streamStr("product"), streamStr("RabbitMQ"), streamStr("version"), streamStr("4.3.6"));
            socket.write(streamResp(0x8011, corr, props));
          } else if (key === 0x0012) {
            socket.write(streamResp(0x8012, corr, concat(u32(1), streamStr("PLAIN"))));
          } else if (key === 0x0013) {
            socket.write(streamResp(0x8013, corr, new Uint8Array()));
            socket.write(streamFrame(concat(u16(0x0014), u16(1), u32(1048576), u32(60))));
          } else if (key === 0x0015) {
            socket.write(streamResp(0x8015, corr, u32(0)));
          } else if (key === 0x000d) {
            const name = readStr(rest, 4)?.text ?? "stream";
            if (!streams.has(name)) streams.set(name, []);
            socket.write(streamResp(0x800d, corr, new Uint8Array()));
          } else if (key === 0x0001) {
            const publisher = rest[4] ?? 1;
            const reference = readStr(rest, 5);
            const stream = reference ? readStr(rest, reference.next)?.text ?? "stream" : "stream";
            state.publishers.set(publisher, stream);
            socket.write(streamResp(0x8001, corr, new Uint8Array()));
          } else if (key === 0x0002) {
            const publisher = rest[0] ?? 1;
            const stream = state.publishers.get(publisher) ?? "stream";
            const count = rest.length >= 5 ? new DataView(rest.buffer, rest.byteOffset + 1, 4).getUint32(0) : 0;
            let at = 5;
            const ids: number[] = [];
            const bucket = streams.get(stream) ?? [];
            for (let i = 0; i < count; i++) {
              if (at + 12 > rest.length) break;
              const id = new DataView(rest.buffer, rest.byteOffset + at, 8).getUint32(4);
              at += 8;
              const n = new DataView(rest.buffer, rest.byteOffset + at, 4).getInt32(0);
              const raw = rest.subarray(at, at + 4 + n);
              const payload = rest.subarray(at + 4, at + 4 + n);
              at += 4 + n;
              ids.push(id);
              bucket.push(raw);
              await push(broker, stream, payload);
            }
            streams.set(stream, bucket);
            const idBytes = new Uint8Array(4 + ids.length * 8);
            new DataView(idBytes.buffer).setUint32(0, ids.length);
            ids.forEach((id, i) => new DataView(idBytes.buffer).setUint32(4 + i * 8 + 4, id));
            socket.write(streamFrame(concat(u16(0x0003), u16(1), Uint8Array.of(publisher), idBytes)));
          } else if (key === 0x0007) {
            const subId = rest[4] ?? 1;
            const stream = readStr(rest, 5)?.text ?? "stream";
            socket.write(streamResp(0x8007, corr, new Uint8Array()));
            const queued = streams.get(stream) ?? [];
            if (queued.length) {
              const data = concat(...queued);
              const chunk = concat(
                Uint8Array.of(0x50, 0x00),
                u16(queued.length),
                u32(queued.length),
                new Uint8Array(8),
                Uint8Array.of(0, 0, 0, 0, 0, 0, 0, 1),
                new Uint8Array(8),
                i32(0),
                u32(data.length),
                u32(0),
                Uint8Array.of(0, 0, 0, 0),
                data,
              );
              socket.write(streamFrame(concat(u16(0x0008), u16(1), Uint8Array.of(subId), chunk)));
            }
          } else if (key === 0x0016) {
            socket.write(streamResp(0x8016, corr, new Uint8Array()));
            socket.end();
          } else if (key === 0x0014 || key === 0x0017) {
            /* tune response and heartbeat */
          } else if ((key & 0x8000) === 0) {
            socket.write(streamResp(key | 0x8000, corr, new Uint8Array()));
          }
        }
      },
    },
  });
}
