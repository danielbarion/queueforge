import { expect, test } from "bun:test";
import { Conn } from "../src/amqp/listen.ts";
import "../src/amqp/index.ts";
import type { Broker } from "../src/broker/index.ts";
import { method, writeTable } from "../src/codec.ts";

for (const scenario of [
  { type: "classic", home: "local", leader: false, pumps: 3, streams: 0 },
  { type: "classic", home: "remote", leader: false, pumps: 0, streams: 0 },
  { type: "quorum", home: "remote", leader: true, pumps: 3, streams: 0 },
  { type: "quorum", home: "local", leader: false, pumps: 0, streams: 0 },
  { type: "stream", home: "remote", leader: false, pumps: 0, streams: 3 },
]) {
  test(`output wakes resume ${scenario.type} ${scenario.home} leader=${scenario.leader} without resubscribing`, async () => {
    let subscriptions = 0, pumps = 0, streams = 0, parkedFlushes = 0;
    const queue = { home: scenario.home, argsParsed: { queueType: scenario.type } };
    const broker = {
      queues: new Map([["/q", queue]]),
      key: (vhost: string, name: string) => vhost + name,
      nextSession: () => 42,
      noteMgmtConsumer() {},
      consume: async () => {},
      kick: async () => { subscriptions++; },
      isLocalHome: (home: string) => home === "local",
      isQuorumLeader: () => scenario.leader,
      consumerTimeoutFor: () => 0,
      pump: () => { pumps++; },
      pumpStream: () => { streams++; },
    } as unknown as Broker;
    const conn = new Conn({ write: bytes => bytes.length, end() {} }, broker);
    conn.own = () => {};
    conn.need = () => {};
    conn.flushParked = () => { parkedFlushes++; };
    const ch = conn.ch(1);
    ch.prefetch = 10;
    const payload = method(60, 20, w => { w.u16(0); w.shortstr("q"); w.shortstr("consumer"); w.u8(0); writeTable(w, []); });
    await conn.consume(1, ch, payload);
    expect(subscriptions).toBe(1);
    const wake = conn.outputWakers.get("1\0consumer")!;
    wake(); wake(); wake();
    expect(subscriptions).toBe(1);
    expect(parkedFlushes).toBe(3);
    expect(pumps).toBe(scenario.pumps);
    expect(streams).toBe(scenario.streams);
    broker.queues.clear();
    wake();
    expect(subscriptions).toBe(1);
    expect(pumps).toBe(scenario.pumps);
    expect(streams).toBe(scenario.streams);
  });
}

for (const limit of ["bytes", "operations"] as const) {
  test(`parked delivery ${limit} limit closes admission and preserves manual requeue`, async () => {
    let deliver!: Parameters<Broker["consume"]>[2]["deliver"];
    const requeued: string[] = [];
    const target = { home: "remote", argsParsed: { queueType: "classic" } };
    const broker = {
      queues: new Map([["/q", target]]), key: (vhost: string, name: string) => vhost + name,
      nextSession: () => 1, noteMgmtConsumer() {}, kick: async () => {},
      consume: async (_vhost: string, _queue: string, consumer: Parameters<Broker["consume"]>[2]) => { deliver = consumer.deliver; },
      nack: (_vhost: string, _queue: string, id: string) => { requeued.push(id); },
    } as unknown as Broker;
    const conn = new Conn({ write: bytes => bytes.length, end() {} }, broker);
    conn.own = () => {}; conn.need = () => {};
    let closeCode = 0;
    conn.connClose = async code => { closeCode = code; conn.connClosed = true; conn.closed = true; };
    const ch = conn.ch(1);
    await conn.consume(1, ch, method(60, 20, w => { w.u16(0); w.shortstr("q"); w.shortstr("c"); w.u8(0); writeTable(w, []); }));
    conn.outboundBytes = 2 * 1024 * 1024;
    if (limit === "bytes") conn.parkedBytes = 64 * 1024 * 1024;
    else conn.parkedOperations = 65536;
    const message = { id: "held", body: Uint8Array.of(1), propRaw: Uint8Array.of(0, 0) } as Parameters<typeof deliver>[0];
    deliver(message);
    expect(closeCode).toBe(506);
    expect(ch.parked).toHaveLength(1);
    deliver({ ...message, id: "late" });
    expect(ch.parked).toHaveLength(1);
    await conn.requeueChannel(ch);
    expect(requeued).toEqual(["late", "held"]);
    expect(ch.parked).toHaveLength(0);
  });
}
