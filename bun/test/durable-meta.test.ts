import { expect, test } from "bun:test";
import { durableMeta } from "../src/broker/publish.ts";
import type { Field } from "../src/codec.ts";

/** What durableMeta must equal after JSON.parse: the plain JSON.stringify shape. */
function reference(src: { id?: string; exchange: string; routingKey: string; headers: Array<[string, Field]>; propRaw: Uint8Array; persistent: boolean; priority: number; redelivered?: boolean }, expiresAt: number | null) {
  return JSON.parse(
    JSON.stringify({
      id: src.id,
      exchange: src.exchange,
      routingKey: src.routingKey,
      headers: src.headers,
      propRaw: Buffer.from(src.propRaw).toString("base64"),
      persistent: src.persistent,
      priority: src.priority,
      expiresAt,
      redelivered: !!src.redelivered,
    }),
  );
}

test("the cached meta string, with and without a quorum id, parses to the stringify shape", () => {
  const props = [Uint8Array.of(0x10, 0, 2), Uint8Array.of(0x90, 0, 2, 5), new Uint8Array(0)];
  const names = ["", "amq.direct", "orders-qq", 'quo"te', "back\\slash", "tab\tkey", "ünï"];
  let n = 0;
  for (const exchange of names)
    for (const routingKey of names)
      for (const propRaw of props)
        for (const id of [undefined, "1760780000000-abcdef12-7", `q-${n}`, 'bad"id'])
          for (const redelivered of [false, true]) {
            const src = { id, exchange, routingKey, headers: [] as Array<[string, Field]>, propRaw, persistent: true, priority: n % 3, redelivered };
            // Twice: the second call is the cached string.
            for (let k = 0; k < 2; k++) expect(JSON.parse(durableMeta(src, null))).toEqual(reference(src, null));
            n++;
          }
  // Headers and an expiry take the plain stringify path.
  const src = { id: "x-1", exchange: "e", routingKey: "k", headers: [["h", { t: "S", v: "v" }]] as unknown as Array<[string, Field]>, propRaw: props[0]!, persistent: true, priority: 0 };
  expect(JSON.parse(durableMeta(src, 123))).toEqual(reference(src, 123));
});
