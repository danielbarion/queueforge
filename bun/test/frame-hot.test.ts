import { expect, test } from "bun:test";
import {
  bodyFrame,
  contentHeaderFrame,
  emptyProps,
  encodeDeliver,
  encodeSettle,
  method,
  methodFrame,
} from "../src/codec.ts";

function legacySettle(channel: number, tag: number, nack: boolean): Uint8Array {
  return methodFrame(
    channel,
    method(60, nack ? 120 : 80, (w) => {
      w.u64(tag);
      w.bits(nack ? [false, false] : [false]);
    }),
  );
}

function legacyDeliver(
  channel: number,
  tag: string,
  dtag: number,
  msg: { exchange: string; routingKey: string; redelivered: boolean; propRaw: Uint8Array; body: Uint8Array },
): Uint8Array {
  const frames = [
    methodFrame(
      channel,
      method(60, 60, (w) => {
        w.shortstr(tag);
        w.u64(dtag);
        w.bits([msg.redelivered]);
        w.shortstr(msg.exchange);
        w.shortstr(msg.routingKey);
      }),
    ),
    contentHeaderFrame(channel, msg.body.length, msg.propRaw.length ? msg.propRaw : emptyProps()),
    bodyFrame(channel, msg.body),
  ];
  const n = frames.reduce((sum, frame) => sum + frame.length, 0);
  const out = new Uint8Array(n);
  let o = 0;
  for (const frame of frames) {
    out.set(frame, o);
    o += frame.length;
  }
  return out;
}

test("hot confirm and deliver frames match the generic builder", () => {
  expect(encodeSettle(1, 42, false)).toEqual(legacySettle(1, 42, false));
  expect(encodeSettle(3, 2 ** 32 + 7, true)).toEqual(legacySettle(3, 2 ** 32 + 7, true));
  const msg = {
    exchange: "",
    routingKey: "bench-fan",
    redelivered: false,
    propRaw: Uint8Array.of(0x10, 0x00, 0x02),
    body: new Uint8Array(256).fill(7),
  };
  expect(encodeDeliver(2, "bench-0", 99, msg)).toEqual(legacyDeliver(2, "bench-0", 99, msg));
  const again = { ...msg, exchange: "amq.direct", redelivered: true, propRaw: new Uint8Array() };
  expect(encodeDeliver(1, "ctag", 1, again)).toEqual(legacyDeliver(1, "ctag", 1, again));
  expect(encodeDeliver(1, "t", 1, { ...msg, routingKey: "x".repeat(300) })).toBeNull();
});
