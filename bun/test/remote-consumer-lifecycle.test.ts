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

type Consumer = { deliver: (message: LiveMsg) => void };
type Probe = {
  subs: Map<number, (message: LiveMsg) => void>;
  remoteAcks: Map<string, string>;
  remoteSessions: Map<string, number>;
  dispatch: (peer: string, message: Record<string, unknown>, write: (value: object) => void) => void;
};
function fixture() {
  const queue = { consumers: [] as Consumer[] };
  const broker = {
    cfg: { nodeId: "self" }, queues: new Map([["/q", queue]]),
    key: (vhost: string, name: string) => vhost + name, pump() {},
  } as unknown as Broker;
  const cluster = new Cluster(broker);
  return { cluster, probe: cluster as unknown as Probe, queue };
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
