/**
 * Default-exchange publish and get used by MQTT, STOMP, streams, and AMQP 1.0.
 *
 * Each protocol stores payloads on the `/` vhost under the topic or queue name.
 */
import { Broker } from "../broker/index.ts";
import { emptyProps } from "../codec.ts";

/**
 * Declare `name` on `/` if needed and publish `body` to the default exchange.
 *
 * @param broker Broker that owns the vhost.
 * @param name Queue and routing key. A declare that fails because the queue
 * already exists is ignored.
 * @param body Payload bytes. They are stored as a non-persistent message.
 */
export async function push(broker: Broker, name: string, body: Uint8Array) {
  try {
    await broker.declareQueue({
      vhost: "/",
      name,
      durable: false,
      exclusive: false,
      autoDelete: false,
      passive: false,
      args: {},
    });
  } catch {
    // already declared
  }
  await broker.publish({
    vhost: "/",
    exchange: "",
    routingKey: name,
    body,
    headers: [],
    propRaw: emptyProps(),
    persistent: false,
    priority: 0,
    expiration: "",
  });
}

/**
 * Take one message from `name` with no-ack.
 *
 * @param broker Broker that owns the vhost.
 * @param name Queue to read. A missing queue returns null instead of throwing.
 * @returns The body, or null when the queue is empty or absent.
 */
export async function pull(broker: Broker, name: string): Promise<Uint8Array | null> {
  try {
    const msg = await broker.get("/", name, true);
    return msg ? msg.body : null;
  } catch {
    return null;
  }
}
