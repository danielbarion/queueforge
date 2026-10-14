import { expect, test } from "bun:test";
import { encodeFrame, FrameReader } from "../src/cluster.ts";
import { QUORUM_BODY } from "../src/wire.ts";

function read(chunks: Array<string | Uint8Array>): Array<Record<string, unknown>> {
  const reader = new FrameReader();
  const out: Array<Record<string, unknown>> = [];
  for (const chunk of chunks) reader.push(Buffer.from(chunk), (msg) => out.push(msg));
  return out;
}

function frame(msg: object, body: Uint8Array, prop: Uint8Array): Buffer {
  return Buffer.concat(encodeFrame(msg, body, prop));
}

test("one cluster chunk yields every line and keeps a partial tail", () => {
  const n = 1000;
  let body = "";
  for (let i = 0; i < n; i++) body += `{"op":"quorum_drop","id":${i}}\n`;
  const all = read([body]);
  expect(all).toHaveLength(n);
  expect(all[0]!.id).toBe(0);
  expect(all[n - 1]!.id).toBe(n - 1);

  expect(read(['{"op":"x"}\n{"op":', '"y"}\n']).map((m) => m.op)).toEqual(["x", "y"]);
  expect(read(['{"op":"x"}\n{"op":', '"z"}\n\n']).map((m) => m.op)).toEqual(["x", "z"]);
  expect(read(['{"op":"x"}\n{"op":'])).toHaveLength(1);
});

test("a character split across chunks is decoded whole", () => {
  const line = Buffer.from('{"op":"x","name":"fila-ção"}\n');
  const cut = line.indexOf(0xa7);
  expect(read([line.subarray(0, cut), line.subarray(cut)])[0]!.name).toBe("fila-ção");
});

test("binary frames carry their bytes between lines, split anywhere", () => {
  const body = new Uint8Array([0, 10, 123, 255, 0, 13]);
  const prop = new Uint8Array([9, 8]);
  const one = frame({ op: "enqueue", id: 7, payload: { vhost: "/", queue: "q" } }, body, prop);
  const stream = Buffer.concat([Buffer.from('{"op":"a"}\n'), one, Buffer.from('{"op":"b"}\n'), one]);
  for (let cut = 0; cut <= stream.length; cut++) {
    const got = read([stream.subarray(0, cut), stream.subarray(cut)]);
    expect(got.map((m) => m.op)).toEqual(["a", "enqueue", "b", "enqueue"]);
    const payload = got[1]!.payload as Record<string | symbol, unknown>;
    expect(payload.body).toEqual(body);
    expect(payload.propRaw).toEqual(prop);
    expect(payload[QUORUM_BODY]).toBe(payload.body);
    expect(payload.queue).toBe("q");
  }
});

test("a large frame in many chunks arrives once, with its own copy of the body", () => {
  const body = new Uint8Array(300_000).map((_, i) => i & 0xff);
  const bytes = frame({ op: "enqueue", id: 1, payload: {} }, body, new Uint8Array());
  const chunks: Buffer[] = [];
  for (let at = 0; at < bytes.length; at += 4096) chunks.push(bytes.subarray(at, at + 4096));
  const got = read([...chunks, '{"op":"after"}\n']);
  expect(got.map((m) => m.op)).toEqual(["enqueue", "after"]);
  const copy = (got[0]!.payload as { body: Uint8Array }).body;
  expect(copy).toEqual(body);
  expect(copy.buffer.byteLength).toBe(body.length);
});
