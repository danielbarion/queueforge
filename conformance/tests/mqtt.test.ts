import { afterAll, expect, test } from "bun:test";
import mqtt, { type IClientOptions, type MqttClient } from "mqtt";
import { channel, eventually, sleep, uniq } from "../lib.ts";

const PORT = Number(process.env.QF_MQTT_PORT ?? 1883);
const WS = process.env.QF_MQTT_WS ?? "";
const open: MqttClient[] = [];

async function client(opts: IClientOptions = {}, url = `mqtt://127.0.0.1:${PORT}`): Promise<MqttClient> {
  const c = mqtt.connect(url, {
    username: "admin",
    password: "devpassword12",
    reconnectPeriod: 0,
    connectTimeout: 4000,
    clientId: uniq("mqtt"),
    ...opts,
  });
  open.push(c);
  await new Promise<void>((resolve, reject) => {
    c.once("connect", () => resolve());
    c.once("error", reject);
    c.once("close", () => reject(new Error("closed before connect")));
  });
  c.on("error", () => {});
  return c;
}

function received(c: MqttClient) {
  const got: Array<{ topic: string; payload: string; qos: number; retain: boolean; props?: Record<string, unknown> }> = [];
  c.on("message", (topic, payload, packet) => {
    got.push({
      topic,
      payload: payload.toString(),
      qos: packet.qos,
      retain: packet.retain,
      props: packet.properties as Record<string, unknown> | undefined,
    });
  });
  return got;
}

afterAll(() => {
  for (const c of open) c.end(true);
});

test("MQTT 3.1.1 :: a wrong password is refused", async () => {
  let refused = false;
  try {
    await client({ password: "wrong-password" });
  } catch {
    refused = true;
  }
  expect(refused).toBe(true);
});

test("MQTT 3.1.1 :: QoS 0 publish reaches a wildcard subscriber", async () => {
  const sub = await client();
  const got = received(sub);
  const base = uniq("t");
  await sub.subscribeAsync(`${base}/+/temp`, { qos: 0 });
  const pub = await client();
  await pub.publishAsync(`${base}/kitchen/temp`, "21", { qos: 0 });
  expect(await eventually(() => got.length === 1, 3000)).toBe(true);
  expect(got[0]).toMatchObject({ topic: `${base}/kitchen/temp`, payload: "21" });
});

test("MQTT 3.1.1 :: QoS 1 publish is acknowledged and delivered at QoS 1", async () => {
  const sub = await client();
  const got = received(sub);
  const topic = `${uniq("q1")}/x`;
  await sub.subscribeAsync(topic, { qos: 1 });
  const pub = await client();
  await pub.publishAsync(topic, "once", { qos: 1 });
  expect(await eventually(() => got.length === 1, 3000)).toBe(true);
  expect(got[0]?.qos).toBe(1);
});

test("MQTT 3.1.1 :: a retained message reaches a later subscriber", async () => {
  const topic = `${uniq("ret")}/state`;
  const pub = await client();
  await pub.publishAsync(topic, "on", { qos: 1, retain: true });
  await sleep(200);
  const sub = await client();
  const got = received(sub);
  await sub.subscribeAsync(topic, { qos: 1 });
  expect(await eventually(() => got.length === 1, 3000)).toBe(true);
  expect(got[0]).toMatchObject({ payload: "on", retain: true });
  // An empty retained payload clears it.
  await pub.publishAsync(topic, "", { qos: 1, retain: true });
});

test("MQTT 3.1.1 :: a will message is published when a client drops", async () => {
  const topic = `${uniq("will")}/gone`;
  const watcher = await client();
  const got = received(watcher);
  await watcher.subscribeAsync(topic, { qos: 1 });
  const dying = await client({ will: { topic, payload: Buffer.from("offline"), qos: 1, retain: false } });
  (dying as unknown as { stream: { destroy: () => void } }).stream.destroy();
  expect(await eventually(() => got.length === 1, 4000)).toBe(true);
  expect(got[0]?.payload).toBe("offline");
});

test("MQTT 3.1.1 :: a persistent session keeps QoS 1 messages while offline", async () => {
  const clientId = uniq("persist");
  const topic = `${uniq("sess")}/inbox`;
  const first = await client({ clientId, clean: false });
  await first.subscribeAsync(topic, { qos: 1 });
  await first.endAsync();
  const pub = await client();
  await pub.publishAsync(topic, "while-away", { qos: 1 });
  await sleep(300);
  const again = mqtt.connect(`mqtt://127.0.0.1:${PORT}`, {
    username: "admin",
    password: "devpassword12",
    reconnectPeriod: 0,
    clientId,
    clean: false,
  });
  open.push(again);
  const got = received(again);
  expect(await eventually(() => got.length === 1, 4000)).toBe(true);
  expect(got[0]?.payload).toBe("while-away");
  again.end(true);
  // Clear the session.
  const clear = await client({ clientId, clean: true });
  await clear.endAsync();
});

test("MQTT 3.1.1 :: an MQTT publish reaches an AMQP queue bound to amq.topic", async () => {
  const ch = await channel();
  const q = uniq("mqtt-amqp");
  await ch.assertQueue(q, { durable: true });
  const base = uniq("bridge");
  await ch.bindQueue(q, "amq.topic", `${base}.*`);
  const pub = await client();
  await pub.publishAsync(`${base}/door`, "open", { qos: 1 });
  let msg = null as Awaited<ReturnType<typeof ch.get>> | null;
  await eventually(async () => {
    msg = await ch.get(q, { noAck: true });
    return !!msg;
  }, 3000);
  expect(msg && msg.content.toString()).toBe("open");
  expect(msg && msg.fields.routingKey).toBe(`${base}.door`);
  await ch.deleteQueue(q);
});

test("MQTT 5.0 :: user properties and the content type travel with the message", async () => {
  const sub = await client({ protocolVersion: 5 });
  const got = received(sub);
  const topic = `${uniq("v5")}/props`;
  await sub.subscribeAsync(topic, { qos: 1 });
  const pub = await client({ protocolVersion: 5 });
  await pub.publishAsync(topic, "{}", { qos: 1, properties: { userProperties: { origin: "test" }, contentType: "application/json" } });
  expect(await eventually(() => got.length === 1, 3000)).toBe(true);
  expect((got[0]?.props?.userProperties as Record<string, string>)?.origin).toBe("test");
  expect(got[0]?.props?.contentType).toBe("application/json");
});

test("MQTT 5.0 :: a v5 refusal carries a reason code", async () => {
  let code = -1;
  try {
    await client({ protocolVersion: 5, password: "wrong-password" });
  } catch (err) {
    code = Number((err as { code?: number }).code ?? -1);
  }
  // 0x86 bad user name or password, or 0x87 not authorized.
  expect([0x86, 0x87]).toContain(code);
});

test("MQTT 5.0 :: response topic and correlation data reach the subscriber", async () => {
  const sub = await client({ protocolVersion: 5 });
  const got = received(sub);
  const topic = `${uniq("v5")}/req`;
  await sub.subscribeAsync(topic, { qos: 1 });
  const pub = await client({ protocolVersion: 5 });
  await pub.publishAsync(topic, "ping", {
    qos: 1,
    properties: { responseTopic: "replies/abc", correlationData: Buffer.from("corr-1"), payloadFormatIndicator: true },
  });
  expect(await eventually(() => got.length === 1, 3000)).toBe(true);
  expect(got[0]?.props?.responseTopic).toBe("replies/abc");
  expect(Buffer.from(got[0]?.props?.correlationData as Uint8Array).toString()).toBe("corr-1");
});

test("MQTT 5.0 :: an expired message is not delivered to a later subscriber", async () => {
  const topic = `${uniq("v5")}/expiry`;
  const pub = await client({ protocolVersion: 5 });
  await pub.publishAsync(topic, "short", { qos: 1, retain: true, properties: { messageExpiryInterval: 1 } });
  await pub.publishAsync(`${topic}/long`, "long", { qos: 1, retain: true, properties: { messageExpiryInterval: 60 } });
  await sleep(2200);
  const sub = await client({ protocolVersion: 5 });
  const got = received(sub);
  // RabbitMQ sends retained messages to exact-topic subscriptions only.
  await sub.subscribeAsync(`${topic}/long`, { qos: 1 });
  await sub.subscribeAsync(topic, { qos: 1 });
  await sleep(800);
  expect(got.map((m) => m.payload)).toEqual(["long"]);
  // The remaining expiry interval travels with the delivery.
  const left = Number(got[0]?.props?.messageExpiryInterval ?? 0);
  expect(left).toBeGreaterThan(0);
  expect(left).toBeLessThanOrEqual(60);
  await pub.publishAsync(topic, "", { qos: 1, retain: true });
  await pub.publishAsync(`${topic}/long`, "", { qos: 1, retain: true });
});

test("MQTT 5.0 :: a topic alias set by the publisher resolves on later publishes", async () => {
  const sub = await client({ protocolVersion: 5 });
  const got = received(sub);
  const topic = `${uniq("v5")}/alias`;
  await sub.subscribeAsync(topic, { qos: 1 });
  const pub = await client({ protocolVersion: 5 });
  await pub.publishAsync(topic, "first", { qos: 1, properties: { topicAlias: 1 } });
  // Empty topic: the alias names it.
  await pub.publishAsync("", "second", { qos: 1, properties: { topicAlias: 1 } });
  expect(await eventually(() => got.length === 2, 3000)).toBe(true);
  expect(got.map((m) => [m.topic, m.payload])).toEqual([
    [topic, "first"],
    [topic, "second"],
  ]);
});

test("MQTT 5.0 :: no-local keeps a client's own publishes from coming back", async () => {
  const c = await client({ protocolVersion: 5 });
  const got = received(c);
  const topic = `${uniq("v5")}/nolocal`;
  await c.subscribeAsync(topic, { qos: 1, nl: true });
  await c.publishAsync(topic, "mine", { qos: 1 });
  const other = await client({ protocolVersion: 5 });
  await other.publishAsync(topic, "theirs", { qos: 1 });
  expect(await eventually(() => got.length >= 1, 3000)).toBe(true);
  await sleep(400);
  expect(got.map((m) => m.payload)).toEqual(["theirs"]);
});

test("MQTT 5.0 :: retain-as-published keeps the retain flag on forwarded messages", async () => {
  const topic = `${uniq("v5")}/rap`;
  const keep = await client({ protocolVersion: 5 });
  const kept = received(keep);
  await keep.subscribeAsync(topic, { qos: 1, rap: true });
  const drop = await client({ protocolVersion: 5 });
  const dropped = received(drop);
  await drop.subscribeAsync(topic, { qos: 1, rap: false });
  const pub = await client({ protocolVersion: 5 });
  await pub.publishAsync(topic, "flagged", { qos: 1, retain: true });
  expect(await eventually(() => kept.length === 1 && dropped.length === 1, 3000)).toBe(true);
  expect(kept[0]?.retain).toBe(true);
  expect(dropped[0]?.retain).toBe(false);
  await pub.publishAsync(topic, "", { qos: 1, retain: true });
});

test("MQTT 5.0 :: retain handling 2 sends no retained messages on subscribe", async () => {
  const topic = `${uniq("v5")}/rh`;
  const pub = await client({ protocolVersion: 5 });
  await pub.publishAsync(topic, "stored", { qos: 1, retain: true });
  const none = await client({ protocolVersion: 5 });
  const gotNone = received(none);
  await none.subscribeAsync(topic, { qos: 1, rh: 2 });
  const some = await client({ protocolVersion: 5 });
  const gotSome = received(some);
  await some.subscribeAsync(topic, { qos: 1, rh: 0 });
  expect(await eventually(() => gotSome.length === 1, 3000)).toBe(true);
  await sleep(300);
  expect(gotNone.length).toBe(0);
  await pub.publishAsync(topic, "", { qos: 1, retain: true });
});

test("MQTT 5.0 :: a subscription identifier comes back on matching deliveries", async () => {
  const sub = await client({ protocolVersion: 5 });
  const got = received(sub);
  const topic = `${uniq("v5")}/subid`;
  await sub.subscribeAsync(topic, { qos: 1, properties: { subscriptionIdentifier: 42 } });
  const pub = await client({ protocolVersion: 5 });
  await pub.publishAsync(topic, "tagged", { qos: 1 });
  expect(await eventually(() => got.length === 1, 3000)).toBe(true);
  const ids = got[0]?.props?.subscriptionIdentifier;
  expect(Array.isArray(ids) ? ids : [ids]).toContain(42);
});

test("MQTT 5.0 :: a session with an expiry interval keeps QoS 1 messages across a reconnect", async () => {
  const clientId = uniq("v5-session");
  const topic = `${uniq("v5")}/sess`;
  const first = await client({ protocolVersion: 5, clientId, clean: true, properties: { sessionExpiryInterval: 60 } });
  await first.subscribeAsync(topic, { qos: 1 });
  first.end(true);
  await sleep(300);
  const pub = await client({ protocolVersion: 5 });
  await pub.publishAsync(topic, "while-away", { qos: 1 });
  await sleep(300);
  const again = mqtt.connect(`mqtt://127.0.0.1:${PORT}`, {
    username: "admin",
    password: "devpassword12",
    reconnectPeriod: 0,
    protocolVersion: 5,
    clientId,
    clean: false,
    properties: { sessionExpiryInterval: 60 },
  });
  open.push(again);
  const got = received(again);
  const present = await new Promise<boolean>((resolve) => again.once("connect", (ack) => resolve(!!ack.sessionPresent)));
  expect(present).toBe(true);
  expect(await eventually(() => got.length === 1, 3000)).toBe(true);
  expect(got[0]?.payload).toBe("while-away");
});

test("MQTT 5.0 :: a will delay interval holds the will until it expires", async () => {
  const topic = `${uniq("v5")}/willdelay`;
  const watcher = await client({ protocolVersion: 5 });
  const got = received(watcher);
  await watcher.subscribeAsync(topic, { qos: 1 });
  const dying = await client({
    protocolVersion: 5,
    properties: { sessionExpiryInterval: 10 },
    will: { topic, payload: Buffer.from("gone"), qos: 1, retain: false, properties: { willDelayInterval: 2 } },
  });
  (dying as unknown as { stream: { destroy: () => void } }).stream.destroy();
  await sleep(800);
  expect(got.length).toBe(0);
  expect(await eventually(() => got.length === 1, 5000)).toBe(true);
  expect(got[0]?.payload).toBe("gone");
}, 10000);

/** MQTT 3.1.1 packets for the WebSocket test. mqtt.js has no WebSocket transport on Bun. */
function mqttString(text: string): number[] {
  const b = [...new TextEncoder().encode(text)];
  return [b.length >> 8, b.length & 0xff, ...b];
}
function mqttPacket(type: number, body: number[]): Uint8Array {
  const len: number[] = [];
  let n = body.length;
  do {
    let byte = n % 128;
    n = Math.floor(n / 128);
    if (n > 0) byte |= 0x80;
    len.push(byte);
  } while (n > 0);
  return Uint8Array.from([type, ...len, ...body]);
}

test("WebSockets :: MQTT over WebSocket delivers a message", async () => {
  expect(WS).not.toBe("");
  const ws = new WebSocket(WS, ["mqtt"]);
  ws.binaryType = "arraybuffer";
  const packets: Uint8Array[] = [];
  ws.onmessage = (ev) => packets.push(new Uint8Array(ev.data as ArrayBuffer));
  await new Promise<void>((resolve, reject) => {
    ws.onopen = () => resolve();
    ws.onerror = () => reject(new Error("ws error"));
  });
  const flags = 0x80 | 0x40 | 0x02;
  ws.send(mqttPacket(0x10, [...mqttString("MQTT"), 4, flags, 0, 60, ...mqttString(uniq("ws")), ...mqttString("admin"), ...mqttString("devpassword12")]));
  expect(await eventually(() => packets.some((p) => p[0] === 0x20 && p[3] === 0), 3000)).toBe(true);
  const topic = `${uniq("ws")}/x`;
  ws.send(mqttPacket(0x82, [0, 1, ...mqttString(topic), 0]));
  expect(await eventually(() => packets.some((p) => p[0] === 0x90), 3000)).toBe(true);
  const pub = await client();
  await pub.publishAsync(topic, "over-ws", { qos: 0 });
  const isPublish = (p: Uint8Array) => (p[0]! >> 4) === 3;
  expect(await eventually(() => packets.some(isPublish), 3000)).toBe(true);
  const text = new TextDecoder().decode(packets.find(isPublish)!);
  expect(text.endsWith("over-ws")).toBe(true);
  ws.close();
});
