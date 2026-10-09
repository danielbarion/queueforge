import { expect, test } from "bun:test";
import { discoverDeadLetterQueues, queueCandidate, rowsOf } from "./dead-letter";
const binding = { source: "failures", destination: "inbox", destination_type: "queue", routing_key: "failed" };

test("dead-letter discovery requires configuration rather than suggestive names or matching policies", () => {
  expect(discoverDeadLetterQueues([{ name: "orders.dead-letter" }, { name: "inbox" }], [binding], [])).toEqual([]);
  expect(discoverDeadLetterQueues([{ name: "orders" }, { name: "inbox" }], [binding], [{ name: "broad", pattern: ".*", definition: { "dead-letter-exchange": "failures" } }])).toEqual([]);
});

test("dead-letter candidates preserve source routes, counts and missing values", () => {
  const rows = discoverDeadLetterQueues([{ name: "orders", arguments: { "x-dead-letter-exchange": "failures", "x-dead-letter-routing-key": "failed" } }, { name: "inbox", type: "classic", messages_ready: 0 }], [binding, binding], []);
  expect(rows).toHaveLength(1);
  expect(rows[0]).toEqual({ name: "inbox", type: "classic", vhost: "/", ready: 0, unacked: null, sources: [{ queue: "orders", exchange: "failures", routingKey: "failed", bindingKey: "failed", via: "arguments", policy: null }] });
  expect(queueCandidate({ name: "unknown", messages_ready: -1, messages_unacknowledged: "0" })?.ready).toBeNull();
  expect(queueCandidate({ name: "unknown" })?.unacked).toBeNull();
  expect(queueCandidate({})).toBeNull();
});

test("dead-letter discovery resolves effective and explicitly applied policies", () => {
  const policies = [{ name: "applied", definition: { "dead-letter-exchange": "failures", "dead-letter-routing-key": "policy-key" } }];
  let rows = discoverDeadLetterQueues([{ name: "orders", policy: "applied" }, { name: "inbox" }], [binding], policies);
  expect(rows[0]?.sources[0]).toMatchObject({ via: "applied policy", policy: "applied", routingKey: "policy-key" });
  rows = discoverDeadLetterQueues([{ name: "orders", effective_policy_definition: { "dead-letter-exchange": "failures" } }, { name: "inbox" }], [binding], []);
  expect(rows[0]?.sources[0]).toMatchObject({ via: "effective policy", routingKey: null });
  rows = discoverDeadLetterQueues([{ name: "orders", arguments: { "x-dead-letter-exchange": "other" }, effective_policy_definition: { "dead-letter-exchange": "failures" } }, { name: "inbox" }], [binding], policies);
  expect(rows).toEqual([]);
});

test("dead-letter discovery keeps vhosts separate and ignores exchange destinations", () => {
  const queues = [{ name: "orders", vhost: "/a", arguments: { "x-dead-letter-exchange": "failures" } }, { name: "inbox", vhost: "/b" }, { name: "inbox", vhost: "/a" }];
  expect(discoverDeadLetterQueues(queues, [{ ...binding, vhost: "/b" }], [])).toEqual([]);
  expect(discoverDeadLetterQueues(queues, [{ ...binding, vhost: "/a", destination_type: "exchange" }], [])).toEqual([]);
  expect(discoverDeadLetterQueues(queues, [{ ...binding, vhost: "/a" }], [])[0]?.vhost).toBe("/a");
});

test("default-exchange dead letters require an explicit destination key", () => {
  expect(discoverDeadLetterQueues([{ name: "orders", arguments: { "x-dead-letter-exchange": "" } }, { name: "inbox" }], [], [])).toEqual([]);
  const rows = discoverDeadLetterQueues([{ name: "orders", arguments: { "x-dead-letter-exchange": "", "x-dead-letter-routing-key": "inbox" } }, { name: "inbox" }], [], []);
  expect(rows[0]?.name).toBe("inbox"); expect(rows[0]?.sources[0]?.exchange).toBe("");
});

test("management lists remain unknown when missing rather than becoming empty", () => {
  expect(rowsOf(null)).toBeNull(); expect(rowsOf({})).toBeNull(); expect(rowsOf({ items: [] })).toEqual([]);
  expect(rowsOf([{ name: "queue" }])).toEqual([{ name: "queue" }]);
});
