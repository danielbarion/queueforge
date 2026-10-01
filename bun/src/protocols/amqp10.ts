/**
 * Minimal AMQP 1.0 header and frame exchange.
 *
 * The 0-9-1 listener calls {@link driveAmqp10} after it sees an AMQP 1.0 protocol header.
 * This is not a second broker: transfers are stored with the same default-exchange push.
 */
import type { Broker } from "../broker.ts";
import { pull, push } from "./queue.ts";

/**
 * Parse progress for one AMQP 1.0 connection.
 *
 * `phase` is `header` until the protocol header is consumed, `sasl` during SASL,
 * then `open`. `sender` and `receiver` are queue names from attach frames.
 */
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

/**
 * Consume `buf` and write any AMQP 1.0 responses.
 *
 * @param buf Bytes not yet parsed. A leading protocol header is handled once.
 * @param state Connection progress. The caller keeps this object across reads.
 * @param write Sends one frame. It must not throw if the socket is still open.
 * @param broker Broker used for message transfers. Attach names become queue names.
 * @returns Bytes left over when the last frame is incomplete. The caller must
 * pass them back on the next read.
 */
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
