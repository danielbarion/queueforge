import { expect, test } from "bun:test";
import { bodyFrame, FRAME_MAX } from "../src/codec.ts";

/** Parse back body frames: [channel, payload] per frame. */
function frames(buf: Uint8Array): Array<{ channel: number; size: number; payload: Uint8Array }> {
  const out = [];
  let at = 0;
  while (at < buf.length) {
    expect(buf[at]).toBe(3);
    const channel = (buf[at + 1]! << 8) | buf[at + 2]!;
    const size = ((buf[at + 3]! << 24) | (buf[at + 4]! << 16) | (buf[at + 5]! << 8) | buf[at + 6]!) >>> 0;
    expect(buf[at + 7 + size]).toBe(0xce);
    out.push({ channel, size, payload: buf.subarray(at + 7, at + 7 + size) });
    at += size + 8;
  }
  return out;
}

test("a 1 MiB body is split into frames no larger than frame-max", () => {
  const body = new Uint8Array(1024 * 1024).map((_, i) => i % 251);
  const parsed = frames(bodyFrame(5, body));
  expect(parsed.length).toBe(Math.ceil(body.length / (FRAME_MAX - 8)));
  for (const f of parsed) {
    expect(f.channel).toBe(5);
    expect(f.size + 8).toBeLessThanOrEqual(FRAME_MAX);
  }
  const joined = new Uint8Array(body.length);
  let o = 0;
  for (const f of parsed) {
    joined.set(f.payload, o);
    o += f.size;
  }
  expect(joined).toEqual(body);
});

test("a negotiated smaller frame-max is honored, and a small body stays one frame", () => {
  const body = new Uint8Array(10000).fill(7);
  for (const f of frames(bodyFrame(1, body, 4096))) expect(f.size + 8).toBeLessThanOrEqual(4096);
  expect(frames(bodyFrame(1, new Uint8Array(256)))).toHaveLength(1);
  expect(bodyFrame(1, new Uint8Array(0))).toHaveLength(0);
});
