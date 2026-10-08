/**
 * Dynamic shovels, as RabbitMQ's `/api/parameters/shovel/{vhost}/{name}`.
 *
 * A shovel moves messages from a source queue to a destination queue. A URI
 * with no host (`amqp://` or `amqp:///vhost`) means this broker, as in
 * RabbitMQ; those run in process with no socket. Each message is removed from
 * the source only after the destination accepted it. Definitions are stored,
 * so a shovel starts again with the broker.
 */
import { runShovel } from "../bridge.ts";
import { Broker } from "./class.ts";

export type ShovelDef = {
  vhost: string;
  name: string;
  srcUri: string;
  srcQueue: string;
  destUri: string;
  destQueue: string;
};

/** The vhost a local `amqp://` URI names, or null for a remote URI. */
export function localVhost(uri: string, fallback: string): string | null {
  const m = /^amqps?:\/\/([^/]*)(?:\/(.*))?$/.exec(uri.trim());
  if (!m) return null;
  if (m[1] !== "") return null;
  return m[2] ? decodeURIComponent(m[2]) : fallback;
}

/** Start one shovel. A running shovel with the same vhost and name is stopped first. */
export function startShovel(this: Broker, def: ShovelDef): void {
  this.stopShovel(def.vhost, def.name);
  const srcVhost = localVhost(def.srcUri, def.vhost);
  const destVhost = localVhost(def.destUri, def.vhost);
  if (srcVhost == null || destVhost == null) {
    let stopped = false;
    this.shovels.set(`${def.vhost}\0${def.name}`, {
      def,
      stop: () => {
        stopped = true;
      },
    });
    void runShovel(def.srcUri, def.destUri, def.srcQueue, def.destQueue).catch((err) => {
      if (!stopped) console.error("shovel", def.name, err);
    });
    return;
  }
  let busy = false;
  const move = async () => {
    if (busy) return;
    busy = true;
    try {
      for (let i = 0; i < 256; i++) {
        const msg = await this.get(srcVhost, def.srcQueue, false);
        if (!msg) break;
        const result = await this.publish({
          vhost: destVhost,
          exchange: "",
          routingKey: def.destQueue,
          body: msg.body,
          headers: msg.headers,
          propRaw: msg.propRaw,
          persistent: msg.persistent,
          priority: msg.priority,
          expiration: "",
          confirm: true,
        });
        if (result === "ack") await this.ack(srcVhost, def.srcQueue, msg.id);
        else {
          await this.nack(srcVhost, def.srcQueue, msg.id, true);
          break;
        }
      }
    } catch {
      /* a queue is missing; try again on the next tick */
    } finally {
      busy = false;
    }
  };
  const timer = setInterval(() => void move(), 25);
  this.shovels.set(`${def.vhost}\0${def.name}`, { def, stop: () => clearInterval(timer) });
}

/** Stop one shovel. Returns false when none was running. */
export function stopShovel(this: Broker, vhost: string, name: string): boolean {
  const key = `${vhost}\0${name}`;
  const running = this.shovels.get(key);
  if (!running) return false;
  running.stop();
  this.shovels.delete(key);
  return true;
}

/** Store and start a shovel. */
export function putShovel(this: Broker, def: ShovelDef): void {
  this.store.putParameter("shovel", def.vhost, def.name, JSON.stringify(def));
  this.startShovel(def);
}

/** Stop and forget a shovel. Returns false when it did not exist. */
export function deleteShovel(this: Broker, vhost: string, name: string): boolean {
  const stored = this.store.deleteParameter("shovel", vhost, name);
  return this.stopShovel(vhost, name) || stored;
}

/** Start every stored shovel. Called once the queues are loaded. */
export function resumeShovels(this: Broker): void {
  for (const row of this.store.listParameters("shovel")) {
    try {
      this.startShovel(JSON.parse(row.value) as ShovelDef);
    } catch {
      /* a corrupt row is skipped */
    }
  }
}

Broker.prototype.startShovel = startShovel;
Broker.prototype.stopShovel = stopShovel;
Broker.prototype.putShovel = putShovel;
Broker.prototype.deleteShovel = deleteShovel;
Broker.prototype.resumeShovels = resumeShovels;

declare module "./class.ts" {
  interface Broker {
    startShovel: typeof startShovel;
    stopShovel: typeof stopShovel;
    putShovel: typeof putShovel;
    deleteShovel: typeof deleteShovel;
    resumeShovels: typeof resumeShovels;
  }
}
