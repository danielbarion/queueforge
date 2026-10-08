import { describe, expect, test } from "bun:test";
import { brokerPath, cookiePair, managementOrigin, sumConfirmBeforeFsync } from "./broker-http";
import { diffActivity, nextAlerts, parseBindings, parseQueues, ratesFrom, type AlertRules, type QueueSnap } from "./live-diff";
import { EMPTY_ALERT_MEMORY } from "./live-diff";

describe("management origin", () => {
  test("keeps http and https origins", () => {
    expect(managementOrigin("http://10.0.0.12:15672/api")).toBe("http://10.0.0.12:15672");
    expect(managementOrigin("https://broker.internal")).toBe("https://broker.internal");
  });

  test("rejects other schemes and embedded passwords", () => {
    expect(managementOrigin("file:///tmp/broker")).toBeNull();
    expect(managementOrigin("http://admin:secret@127.0.0.1:15672")).toBeNull();
  });
});

describe("broker path", () => {
  test("allows health and api paths", () => {
    expect(brokerPath("/healthz")).toBe("/healthz");
    expect(brokerPath("/api/queues/%2F?page_size=500")).toBe("/api/queues/%2F?page_size=500");
  });

  test("rejects login, traversal, and foreign urls", () => {
    expect(brokerPath("/api/login")).toBeNull();
    expect(brokerPath("/api/../metrics")).toBeNull();
    expect(brokerPath("https://evil.example/api/overview")).toBeNull();
    // The bypasses a raw string check let through.
    expect(brokerPath("/api/%2e%2e/latest/meta-data/")).toBeNull();
    expect(brokerPath("/api/login/")).toBeNull();
    expect(brokerPath("/api//login")).toBeNull();
    expect(brokerPath("/API/LOGIN")).toBeNull();
    expect(brokerPath("/api/./login")).toBeNull();
  });
});

describe("session cookie", () => {
  test("keeps the name and value", () => {
    expect(cookiePair("queueforge_session_15672=abc; HttpOnly; SameSite=Lax; Path=/")).toBe("queueforge_session_15672=abc");
  });
});

describe("confirm before fsync", () => {
  test("sums every series", () => {
    const text = ["queueforge_confirm_before_fsync_total 2", 'queueforge_confirm_before_fsync_total{node="n1"} 3', "other 9"].join("\n");
    expect(sumConfirmBeforeFsync(text)).toBe(5);
  });
});

const orders: QueueSnap = { name: "orders", type: "classic", messages: 4, messagesReady: 3, messagesUnacked: 1, consumers: 1 };

describe("poll diff", () => {
  test("rates are per second and ignore a counter that went backwards", () => {
    const prev = { connections: 1, publish: 100, deliver: 80, ack: 70, version: "0.1.0" };
    const next = { connections: 1, publish: 300, deliver: 80, ack: 10, version: "0.1.0" };
    expect(ratesFrom(prev, next, 2000)).toEqual({ publish: 100, deliver: 0, ack: null });
  });

  test("activity records connections, new queues, ready moves, and removals", () => {
    const events = diffActivity([orders], [{ ...orders, messagesReady: 8 }], 2, 4, 10);
    expect(events.map((event) => event.text)).toEqual(["Connections 2 → 4", "orders ready 3 → 8"]);
    const gone = diffActivity([orders], [], 1, 1, 11);
    expect(gone.map((event) => event.text)).toEqual(["Queue orders disappeared"]);
  });

  test("bindings from a raw list and a page both keep this queue", () => {
    expect(parseBindings([{ source: "amq.direct", destination: "orders", routing_key: "orders" }], "orders")).toEqual([
      { source: "amq.direct", routingKey: "orders" },
    ]);
    expect(parseBindings({ items: [{ source: "amq.topic", destination: "other", routing_key: "a" }] }, "orders")).toEqual([]);
  });

  test("queue pages expose the type", () => {
    expect(parseQueues({ items: [{ name: "orders", type: "quorum", messages_ready: 1, messages_unacknowledged: 2, consumers: 0 }] })).toEqual([
      { name: "orders", type: "quorum", messages: 3, messagesReady: 1, messagesUnacked: 2, consumers: 0 },
    ]);
  });
});

const rules: AlertRules = {
  readyEnabled: true,
  readyOver: 10,
  unackedEnabled: true,
  unackedOver: 5,
  fsyncEnabled: true,
  cooldownMs: 60_000,
};

describe("alerts", () => {
  test("fires on the rising edge and not again while it stays over", () => {
    const first = nextAlerts(rules, EMPTY_ALERT_MEMORY, { ready: 11, unacked: 0, fsync: 0, at: 1_000 }, false);
    expect(first.fired).toEqual(["Ready is 11, over 10"]);
    const second = nextAlerts(rules, first.memory, { ready: 12, unacked: 0, fsync: 0, at: 2_000 }, false);
    expect(second.fired).toEqual([]);
  });

  test("a stale sample does not fire or move the edge", () => {
    const stale = nextAlerts(rules, EMPTY_ALERT_MEMORY, { ready: 11, unacked: 0, fsync: 1, at: 1_000 }, true);
    expect(stale.fired).toEqual([]);
    expect(stale.memory).toEqual(EMPTY_ALERT_MEMORY);
  });

  test("a dip and return inside the cooldown stays quiet", () => {
    const first = nextAlerts(rules, EMPTY_ALERT_MEMORY, { ready: 11, unacked: 0, fsync: 0, at: 1_000 }, false);
    const clear = nextAlerts(rules, first.memory, { ready: 0, unacked: 0, fsync: 0, at: 2_000 }, false);
    const again = nextAlerts(rules, clear.memory, { ready: 11, unacked: 0, fsync: 0, at: 3_000 }, false);
    expect(again.fired).toEqual([]);
  });
});
