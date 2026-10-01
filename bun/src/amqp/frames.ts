/**
 * AMQP frame read and write for one connection.
 *
 * Owns the outbound queue and the inbound byte buffer. Method bodies live
 * in the sibling files; this file only frames them.
 */
import { driveAmqp10 } from "../protocols/index.ts";
import { method, methodFrame, R, writeTable } from "../codec.ts";
import { ChanError } from "../broker.ts";
import { Conn } from "./listen.ts";

/** Every AMQP frame ends with this octet. A mismatch closes the connection. */
const FRAME_END = 0xce;

/**
 * Queue one frame behind any write already in flight.
 *
 * @param frame A complete frame, including its end octet.
 * @returns The write chain. Later frames wait on this promise.
 */
export function send(this: Conn, frame: Uint8Array) {
  return this.sendMany([frame]);
}

/**
 * Queue several frames, in order, behind any write already in flight.
 *
 * @param frames Complete frames. They are not sent until earlier writes finish.
 * @returns The write chain. A closed connection drops the frames.
 */
export function sendMany(this: Conn, frames: Uint8Array[]) {
  this.writeChain = this.writeChain.then(() => {
    if (this.closed) return;
    for (const frame of frames) this.outbound.push(frame);
    this.flush();
  });
  return this.writeChain;
}

/**
 * Write queued frames until the socket accepts a partial write.
 *
 * A short write keeps the unsent tail at the front of the queue. The socket
 * `drain` callback calls this again.
 */
export function flush(this: Conn) {
  while (this.outbound.length) {
    const frame = this.outbound[0]!;
    const n = this.socket.write(frame);
    if (n === frame.length) {
      this.outbound.shift();
      continue;
    }
    if (n > 0) this.outbound[0] = frame.subarray(n);
    return;
  }
  this.socket.flush?.();
}

/**
 * Append inbound bytes and handle every complete frame they contain.
 *
 * @param data The next chunk from the socket. It may hold a partial frame.
 * An AMQP 0-9-1 header switches the connection into frame mode and sends
 * `connection.start`. An AMQP 1.0 header is handed to `driveAmqp10`.
 * A bad frame end closes the connection. A channel error closes channel 0
 * with that code; any other error closes the connection with 541.
 */
export async function push(this: Conn, data: Uint8Array) {
  const next = new Uint8Array(this.buf.length + data.length);
  next.set(this.buf);
  next.set(data, this.buf.length);
  this.buf = next;
  try {
    if (this.stage === "amqp10") {
      this.buf = await driveAmqp10(this.buf, this.amqp10, (frame) => this.socket.write(frame), this.broker);
      return;
    }
    if (this.stage === "header") {
      if (this.buf.length < 8) return;
      const head = this.buf.subarray(0, 8);
      const amqp10 = head[0] === 65 && head[1] === 77 && head[2] === 81 && head[3] === 80 && ((head[4] === 3 && head[5] === 1 && head[6] === 0 && head[7] === 0) || (head[4] === 0 && head[5] === 1 && head[6] === 0 && head[7] === 0));
      if (amqp10) {
        this.stage = "amqp10";
        this.buf = await driveAmqp10(this.buf, this.amqp10, (frame) => this.socket.write(frame), this.broker);
        return;
      }
      const ok = head[0] === 65 && head[1] === 77 && head[2] === 81 && head[3] === 80 && head[4] === 0 && head[5] === 0 && head[6] === 9 && head[7] === 1;
      if (!ok) {
        this.socket.end();
        return;
      }
      this.buf = this.buf.subarray(8);
      this.stage = "frames";
      await this.send(
        methodFrame(
          0,
          method(10, 10, (w) => {
            w.u8(0);
            w.u8(9);
            writeTable(w, [
              ["capabilities", { t: "F", v: [
                ["publisher_confirms", { t: "t", v: true }],
                ["consumer_cancel_notify", { t: "t", v: true }],
                ["basic.nack", { t: "t", v: true }],
              ] }],
            ]);
            w.longstr("PLAIN");
            w.longstr("en_US");
          }),
        ),
      );
    }
    while (this.buf.length >= 7) {
      const r = new R(this.buf);
      const type = r.u8();
      const channel = r.u16();
      const size = r.u32();
      if (this.buf.length < 7 + size + 1) return;
      const payload = this.buf.subarray(7, 7 + size);
      const end = this.buf[7 + size];
      this.buf = this.buf.subarray(8 + size);
      if (end !== FRAME_END) {
        await this.connClose(501, "FRAME_ERROR");
        return;
      }
      if (type === 8) continue;
      if (type === 1) await this.onMethod(channel, payload);
      else if (type === 2) await this.onHeader(channel, payload);
      else if (type === 3) await this.onBody(channel, payload);
      if (this.connClosed) return;
    }
  } catch (err) {
    if (err instanceof ChanError) await this.chanClose(0, err.code, err.message);
    else await this.connClose(541, "INTERNAL_ERROR");
  }
}

Conn.prototype.send = send;
Conn.prototype.sendMany = sendMany;
Conn.prototype.flush = flush;
Conn.prototype.push = push;
