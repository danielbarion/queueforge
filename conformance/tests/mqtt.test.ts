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
