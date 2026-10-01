/**
 * MQTT 3.1.1 listener.
 *
 * Publishes are fanned out to live subscribers and also stored on a classic queue
 * named after the topic. Subscriptions replay one queued payload when one exists.
 */
import type { Broker } from "../broker/index.ts";
import { pull, push } from "./queue.ts";

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
const mqttHub: MqttSub[] = [];

function mqttFanout(topic: string, payload: Uint8Array) {
  const frame = mqttPublish(topic, payload);
  for (const sub of mqttHub) {
    if (mqttMatch(sub.filter, topic)) sub.write(frame);
  }
}

/**
 * Listen for MQTT on `host`:`port` and bridge publishes through `broker`.
 *
 * @param host Bind address passed to Bun.listen.
 * @param port TCP port. The caller chooses it from config; this function does not retry.
 * @param broker Broker that stores a copy of each publish. A publish error is logged
 * and does not close the socket.
 * @returns Nothing. The listener stays open until the process exits.
 */
export function startMqtt(host: string, port: number, broker: Broker) {
  Bun.listen<{ buf: Uint8Array }>({
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
