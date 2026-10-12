import { expect, test } from "bun:test";
import type { Broker, LiveMsg } from "../src/broker/index.ts";
import { cancel } from "../src/broker/delivery.ts";
import { Cluster } from "../src/cluster.ts";

for (const scenario of [
  { type: "classic", home: "remote", leader: false, destination: "remote" },
  { type: "classic", home: "local", leader: false, destination: null },
  { type: "quorum", home: "remote", leader: false, destination: "leader" },
  { type: "quorum", home: "remote", leader: true, destination: null },
  { type: "stream", home: "remote", leader: false, destination: null },
]) {
  test(`cancel ${scenario.type} ${scenario.home} leader=${scenario.leader} unsubscribes only remote owners`, async () => {
    const calls: unknown[][] = [];
    const follower = scenario.type === "quorum" && !scenario.leader;
    const queue = {
      consumers: follower ? [] : [{ tag: "consumer", session: 42 }],
      argsParsed: { queueType: scenario.type }, home: scenario.home, autoDelete: false,
    };
    const remoteQuorum = new Map(follower ? [["/\0q\0consumer", 42]] : []);
    const broker = {
      queues: new Map([["/q", queue]]), key: (vhost: string, name: string) => vhost + name,
      prom: { consumers: 1 }, emitEvent() {}, remoteQuorum,
      isQuorumLeader: () => scenario.leader, quorumLeader: () => "leader",
      isLocalHome: (home: string) => home === "local", removeStreamReader() {},
      cluster: { unsubscribe: async (...args: unknown[]) => { calls.push(args); } },
    } as unknown as Broker;
    await cancel.call(broker, "/", "q", "consumer");
    expect(calls).toEqual(scenario.destination ? [[scenario.destination, { vhost: "/", queue: "q", session: 42 }]] : []);
    expect(queue.consumers).toHaveLength(0);
    expect(remoteQuorum.size).toBe(0);
  });
}

type Consumer = { deliver: (message: LiveMsg) => void; want: () => boolean };
type Flow = { full: boolean; wake: Set<() => void> };
type Probe = {
  subs: Map<number, (message: LiveMsg) => void>;
  remoteAcks: Map<string, string>;
  remoteSessions: Map<string, number>;
  dispatch: (peer: string, message: Record<string, unknown>, write: (value: object) => void, socket?: unknown, flow?: Flow) => void;
};
function fixture() {
  const queue = { consumers: [] as Consumer[] };
  const settled: string[] = [];
  let pumps = 0;
  const broker = {
    cfg: { nodeId: "self" }, queues: new Map([["/q", queue]]),
    key: (vhost: string, name: string) => vhost + name, pump() { pumps++; },
    ack(_vhost: string, _queue: string, id: string) { settled.push(id); },
    nack(_vhost: string, _queue: string, id: string) { settled.push(id); },
  } as unknown as Broker;
  const cluster = new Cluster(broker);
  return { cluster, probe: cluster as unknown as Probe, queue, settled, pumps: () => pumps };
}
const subscription = { vhost: "/", queue: "q", session: 42, noAck: false, exclusive: false };

test("unsubscribe removes the local callback and sends the existing unsub RPC", async () => {
  const { cluster, probe } = fixture();
  const calls: unknown[][] = [];
  cluster.call = async (...args) => { calls.push(args); return true; };
  await cluster.subscribe("home", subscription, () => {});
  expect(probe.subs.size).toBe(1);
  await cluster.unsubscribe("home", subscription);
  expect(probe.subs.size).toBe(0);
  expect(calls[1]).toEqual(["home", "unsub", subscription]);
});

test("unsubscribe removes the callback even when the remote RPC fails", async () => {
  const { cluster, probe } = fixture();
  cluster.call = async () => true;
  await cluster.subscribe("home", subscription, () => {});
  cluster.call = async () => { throw new Error("offline"); };
  await expect(cluster.unsubscribe("home", subscription)).rejects.toThrow("offline");
  expect(probe.subs.size).toBe(0);
});

for (const noAck of [true, false]) {
  test(`remote ${noAck ? "autoAck" : "manualAck"} deliveries retain only required settlement state`, () => {
    const { probe, queue } = fixture();
    const sent: object[] = [];
    probe.dispatch("peer", { op: "sub", id: 1, payload: { ...subscription, noAck } }, value => sent.push(value));
    queue.consumers[0]!.deliver({
      id: "message", body: Uint8Array.of(1), propRaw: Uint8Array.of(0, 0),
      exchange: "", routingKey: "q", persistent: false, priority: 0, redelivered: false,
    } as LiveMsg);
    expect([...probe.remoteAcks.entries()]).toEqual(noAck ? [] : [["/\0q\0" + 1, "message"]]);
    expect([...probe.remoteSessions.entries()]).toEqual(noAck ? [] : [["/\0q\0" + 1, 42]]);
    const delivery = sent.at(-1) as { payload: { settles_on_write: boolean; delivery_id: number } };
    expect(delivery.payload.settles_on_write).toBe(noAck);
    expect(delivery.payload.delivery_id).toBe(1);
  });
}

const message = (id: string) => ({
  id, body: Uint8Array.of(1, 2, 3), propRaw: Uint8Array.of(0x80, 0, 4, 116, 101, 120, 116),
  exchange: "", routingKey: "q", persistent: true, priority: 0, redelivered: false,
} as LiveMsg);

test("ack_credit holds a remote consumer to its prefetch and refills on ack", async () => {
  const { probe, queue, settled } = fixture();
  probe.dispatch("peer", { op: "sub", id: 1, payload: { ...subscription, ack_credit: 2 } }, () => {});
  const consumer = queue.consumers[0]!;
  consumer.deliver(message("a"));
  consumer.deliver(message("b"));
  expect(consumer.want()).toBe(false);
  probe.dispatch("peer", { op: "ack", id: 2, payload: { vhost: "/", queue: "q", delivery_id: 1 } }, () => {});
  await Bun.sleep(0);
  expect(settled).toEqual(["a"]);
  expect(consumer.want()).toBe(true);
});

test("a Rust credit subscriber is not refilled by acks", async () => {
  const { probe, queue } = fixture();
  probe.dispatch("peer", { op: "sub", id: 1, payload: { ...subscription, credit: 1 } }, () => {});
  const consumer = queue.consumers[0]!;
  consumer.deliver(message("a"));
  probe.dispatch("peer", { op: "ack", id: 2, payload: { vhost: "/", queue: "q", delivery_id: 1 } }, () => {});
  await Bun.sleep(0);
  expect(consumer.want()).toBe(false);
});

test("a full cluster socket stops remote deliveries until it drains", () => {
  const { probe, queue, pumps } = fixture();
  const flow: Flow = { full: false, wake: new Set() };
  probe.dispatch("peer", { op: "sub", id: 1, payload: subscription }, () => {}, undefined, flow);
  const consumer = queue.consumers[0]!;
  expect(consumer.want()).toBe(true);
  flow.full = true;
  expect(consumer.want()).toBe(false);
  const before = pumps();
  flow.full = false;
  for (const wake of flow.wake) wake();
  expect(pumps()).toBe(before + 1);
  probe.dispatch("peer", { op: "unsub", id: 2, payload: subscription }, () => {}, undefined, flow);
  expect(flow.wake.size).toBe(0);
});

test("a remote delivery carries one body and keeps its properties", () => {
  const { probe, queue } = fixture();
  const sent: Array<Record<string, unknown>> = [];
  probe.dispatch("peer", { op: "sub", id: 1, payload: subscription }, (value) => sent.push(value as Record<string, unknown>));
  queue.consumers[0]!.deliver(message("a"));
  const delivery = sent.at(-1)!;
  expect(delivery.msg).toBeUndefined();
  const received: LiveMsg[] = [];
  probe.subs.set(42, (m) => received.push(m));
  probe.dispatch("home", delivery, () => {});
  expect([...received[0]!.body]).toEqual([1, 2, 3]);
  expect([...received[0]!.propRaw]).toEqual([0x80, 0, 4, 116, 101, 120, 116]);
});
