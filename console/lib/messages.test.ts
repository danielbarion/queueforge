import { expect, test } from "bun:test";
import { deadLetterDetails, inspectionBody, messageBytes, parseMessages, payloadText, replayAvailability } from "./messages";
import type { BrokerKind } from "../stores/broker";
const row = { payload: "aGVsbG8=", payload_encoding: "base64", payload_bytes: 5, exchange: "", routing_key: "q", redelivered: false, properties: {} };
const snapshot = () => parseMessages([row])![0]!;

test("invalid inspection data is unknown rather than an empty or fabricated result", () => {
  for (const body of [null, {}, { items: [] }, [null], [{ ...row, payload: 42 }], [{ ...row, payload_encoding: "auto" }], [{ ...row, payload_encoding: ["base64"] }], [{ ...row, properties: null }], [{ ...row, exchange: undefined }], [{ ...row, payload_bytes: -1 }], [{ ...row, payload: "!!!!" }]]) expect(parseMessages(body)).toBeNull();
  expect(parseMessages([])).toEqual([]);
  const { properties: _, ...noProperties } = row;
  const missing = parseMessages([noProperties])![0]!;
  expect(missing.propertiesAvailable).toBe(false);
  expect(replayAvailability("rabbitmq", false, missing).allowed).toBe(false);
});

test("binary payload roundtrips exactly; preview never changes the saved bytes", () => {
  const bytes = new Uint8Array([0, 255, 128, 10, 13]);
  const payload = Buffer.from(bytes).toString("base64");
  const message = parseMessages([{ ...row, payload }])![0]!;
  expect(messageBytes(message)).toEqual(bytes);
  expect(payloadText(message)).toBe(payload);
  expect(message.payload).toBe(payload);
  expect(message.complete).toBe(true);
});

test("UTF-8 byte counts, JSON preview and truncation use original payload", () => {
  const payload = '{"value":"á"}';
  const message = parseMessages([{ ...row, payload, payload_encoding: "string", payload_bytes: new TextEncoder().encode(payload).length }])![0]!;
  expect(payloadText(message)).toBe('{\n  "value": "á"\n}');
  expect(message.payload).toBe(payload);
  expect(message.complete).toBe(true);
  const truncated = parseMessages([{ ...row, payload_bytes: 100 }])![0]!;
  expect(truncated.complete).toBe(false);
  expect(replayAvailability("rabbitmq", false, truncated).allowed).toBe(false);
});

test("every broker gets both requeue contracts and bounded count", () => {
  for (const kind of ["rust", "bun", "php", "rabbitmq"] as BrokerKind[]) {
    expect(inspectionBody(kind, 2)).toEqual({ count: 2, ackmode: "ack_requeue_true", requeue: true, encoding: "base64" });
    expect(inspectionBody(kind, 99).count).toBe(20);
    expect(inspectionBody(kind, NaN).count).toBe(1);
    expect(inspectionBody(kind, -1).count).toBe(1);
  }
});

test("copy policy requires full snapshot and faithful properties; demo never writes", () => {
  for (const kind of ["rust", "bun", "php", "rabbitmq"] as BrokerKind[]) expect(replayAvailability(kind, true, snapshot()).allowed).toBe(false);
  for (const kind of ["rust", "bun", "php"] as BrokerKind[]) expect(replayAvailability(kind, false, snapshot()).allowed).toBe(false);
  expect(replayAvailability("rabbitmq", false, snapshot()).allowed).toBe(true);
  expect(replayAvailability("rabbitmq", false, snapshot()).reason).toContain("original remains queued");
});

test("dead letter data uses standard headers and retains unknown fields", () => {
  const message = snapshot();
  expect(deadLetterDetails(message)).toEqual([]);
  message.properties = { headers: { "x-death": [{ queue: "dlq", reason: "rejected", count: 2, exchange: "source" }, { count: "99", queue: {} }, null] } };
  expect(deadLetterDetails(message)).toEqual([{ queue: "dlq", reason: "rejected", count: 2, exchange: "source", routingKeys: null }, { queue: null, reason: null, count: null, exchange: null, routingKeys: null }]);
  message.propertiesAvailable = false;
  expect(deadLetterDetails(message)).toEqual([]);
});

test("dead-letter history preserves original routing keys without guessing missing values", () => {
  const message = snapshot(); message.properties = { headers: { "x-death": [{ "routing-keys": ["original", "cc"] }, { "routing-keys": [42] }] } };
  expect(deadLetterDetails(message)[0]?.routingKeys).toEqual(["original", "cc"]); expect(deadLetterDetails(message)[1]?.routingKeys).toBeNull();
});
