import { describe, expect, test } from "bun:test";
import { decodeQuorumAppend, encodeQuorumAppend } from "../src/wire.ts";

describe("quorum wire v1", () => {
  test("round trip keeps the body without a data directory", () => {
    const encoded = encodeQuorumAppend({
      vhost: "/",
      queue: "orders",
      messageId: "m1",
      body: new TextEncoder().encode("mixed-body"),
      exchange: "",
      routingKey: "orders",
      persistent: true,
    });
    expect(encoded.v).toBe(1);
    const decoded = decodeQuorumAppend(encoded);
    expect(new TextDecoder().decode(decoded.body)).toBe("mixed-body");
    expect(decoded.messageId).toBe("m1");
    const fromRust = decodeQuorumAppend({
      v: 1,
      vhost: "/",
      queue: "orders",
      message_id: "m1",
      body_b64: Buffer.from("mixed-body").toString("base64"),
      persistent: true,
      routing_key: "orders",
      exchange: "",
    });
    expect(new TextDecoder().decode(fromRust.body)).toBe("mixed-body");
    expect(fromRust.messageId).toBe("m1");
  });
});
