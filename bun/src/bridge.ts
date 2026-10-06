/**
 * Shovel and federation links that dial another AMQP broker.
 */
import amqp from "amqplib";
import type { Broker } from "./broker/index.ts";

/**
 * Move messages from `srcQueue` on `srcUri` to `destQueue` on `destUri`.
 *
 * @param srcUri AMQP URI of the source broker, including the vhost.
 * @param destUri AMQP URI of the destination broker.
 * @param srcQueue Queue to consume.
 * @param destQueue Queue to publish into. It is declared durable when missing.
 * @returns Nothing. The consume loop stays up until the process exits or a broker closes the socket.
 */
export async function runShovel(srcUri: string, destUri: string, srcQueue: string, destQueue: string): Promise<void> {
  const src = await amqp.connect(srcUri);
  const dest = await amqp.connect(destUri);
  const srcCh = await src.createChannel();
  const destCh = await dest.createChannel();
  await srcCh.assertQueue(srcQueue, { durable: true });
  await destCh.assertQueue(destQueue, { durable: true });
  await srcCh.consume(srcQueue, (msg) => {
    if (!msg) return;
    destCh.sendToQueue(destQueue, msg.content, { persistent: true });
    srcCh.ack(msg);
  });
}

/**
 * Consume one upstream exchange and republish each body on `broker`.
 *
 * @param uri Upstream AMQP URI.
 * @param downstream Vhost that receives the republished bodies.
 * @param exchange Upstream exchange name. The link binds `#`.
 * @param broker Broker that routes the body on `downstream`.
 * @returns Nothing. The consume loop stays up until the upstream closes.
 */
export async function runFederation(uri: string, downstream: string, exchange: string, broker: Broker): Promise<void> {
  const conn = await amqp.connect(uri);
  const ch = await conn.createChannel();
  const queue = `qf-fed-${exchange}`;
  await ch.assertExchange(exchange, "topic", { durable: true });
  await ch.assertQueue(queue, { durable: true });
  await ch.bindQueue(queue, exchange, "#");
  await ch.consume(queue, (msg) => {
    if (!msg) return;
    const pending = broker.publish({
      vhost: downstream,
      exchange,
      routingKey: msg.fields.routingKey,
      body: new Uint8Array(msg.content),
      headers: [],
      propRaw: new Uint8Array(),
      persistent: true,
      priority: 0,
      expiration: "",
    });
    const done = typeof pending === "string" ? Promise.resolve(pending) : pending;
    void done.then(() => ch.ack(msg), () => ch.nack(msg, false, true));
  });
}

/** Exchange name from a policy pattern. A pattern with regex operators returns null. */
export function exchangeFromPattern(pattern: string): string | null {
  if (!pattern.startsWith("^") || !pattern.endsWith("$")) return null;
  const name = pattern.slice(1, -1).replaceAll("\\.", ".");
  if (!name || /[*+?(\[|]/.test(name)) return null;
  return name;
}
