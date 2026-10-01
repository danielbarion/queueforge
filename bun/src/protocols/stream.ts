/**
 * RabbitMQ stream protocol listener.
 *
 * Commands are answered with the matching response key. Published chunks are
 * kept in memory for a later subscribe on the same process and also pushed
 * to the broker under the stream name.
 */
import type { Broker } from "../broker/index.ts";
import { push } from "./queue.ts";

const streams = new Map<string, Uint8Array[]>();

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
/**
 * Listen for the stream protocol on `host`:`port`.
 *
 * @param host Bind address passed to Bun.listen.
 * @param port TCP port from config.
 * @param broker Broker that receives each published payload. The in-memory
 * chunk list is what a subscribe on this process replays.
 * @returns Nothing. The listener stays open until the process exits.
 */
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
