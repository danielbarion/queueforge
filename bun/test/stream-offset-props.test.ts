import { expect, test } from "bun:test";
import { encodeWithOffset, withOffset } from "../src/broker/stream.ts";
import { readContentHeader, type Field } from "../src/codec.ts";

/** The headers of a property block, through a content header that carries it. */
function headersOf(props: Uint8Array): Array<[string, Field]> {
  const payload = new Uint8Array(12 + props.length);
  payload[1] = 60;
  payload.set(props, 12);
  return readContentHeader(payload).props.headers;
}

/** Property blocks: none, delivery mode only, content type and headers, and every later field. */
const blocks: Array<{ props: Uint8Array; headers: Array<[string, Field]> }> = [
  { props: new Uint8Array(0), headers: [] },
  { props: Uint8Array.of(0x10, 0x00, 0x02), headers: [] },
  // content-type "a/b", headers {k: "v"}, delivery mode 2, priority 5
  { props: Uint8Array.of(0xb8, 0x00, 3, 0x61, 0x2f, 0x62, 0, 0, 0, 7, 1, 0x6b, 0x53, 0, 0, 0, 1, 0x76, 2, 5), headers: [["k", { t: "S", v: "v" }]] },
];

test("the cached offset block matches a fresh encode for every offset", () => {
  const offsets = [0, 1, 255, 256, 65535, 2 ** 31 - 1, 2 ** 32, 2 ** 32 + 5, 2 ** 40 + 12345, Number.MAX_SAFE_INTEGER];
  for (let round = 0; round < 2; round++) {
    for (const { props, headers } of blocks) {
      for (const offset of offsets) {
        const cached = withOffset(props, headers, offset);
        expect(Buffer.compare(cached, encodeWithOffset(props, headers, offset).encoded)).toBe(0);
        const back = headersOf(cached).find(([k]: [string, Field]) => k === "x-stream-offset")?.[1];
        expect(back).toEqual({ t: "l", v: offset });
      }
    }
  }
  // A fresh empty header list for each entry still hits the cache and stays correct.
  const props = Uint8Array.of(0x10, 0x00, 0x02);
  const a = withOffset(props, [], 7);
  const b = withOffset(props, [], 8);
  expect(a).not.toBe(b);
  expect(headersOf(b)).toEqual([["x-stream-offset", { t: "l", v: 8 }]]);
});
