import { describe, expect, test } from "bun:test";
import { decodeQuorumAppend, encodeQuorumAppend, quorumAppendKey } from "../src/wire.ts";

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

  test("the live key matches the decoded fields for every accepted shape", () => {
    const shapes: Array<Record<string, unknown>> = [
      encodeQuorumAppend({ vhost: "/", queue: "q", messageId: "m1", body: new Uint8Array([1]), exchange: "", routingKey: "q", persistent: true }),
      { vhost: "v", queue: "q", message: { message_id: "rust-1", body_b64: "AQ==" } },
      { vhost: "v", queue: "q", qid: "old-qid", body: "AQ==" },
      { vhost: "v", queue: "q", id: "old-id" },
      { queue: "q" },
      {},
    ];
    for (const payload of shapes) {
      const d = decodeQuorumAppend(payload);
      expect(quorumAppendKey(payload)).toBe(`${d.vhost}\0${d.queue}\0${d.messageId}`);
    }
  });
});
