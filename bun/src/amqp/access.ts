/**
 * Per-connection access checks: permission patterns and exclusive ownership.
 *
 * RabbitMQ checks configure, write, and read on the named resource, and
 * refuses a second connection's use of an exclusive queue with 405. Results
 * are cached on the connection until the broker's permission table changes.
 */
import { ChanError } from "../broker/index.ts";
import { Conn } from "./listen.ts";

export type Access = "configure" | "write" | "read";

/**
 * Test one permission for this connection's user and vhost.
 *
 * @param kind Which pattern to test.
 * @param resource Queue or exchange name. The default exchange is `amq.default`.
 */
export function allowed(this: Conn, kind: Access, resource: string): boolean {
  const perms = this.broker.perms;
  if (this.permRef !== perms || this.permLen !== perms.length) {
    this.permRef = perms;
    this.permLen = perms.length;
    this.permCache.clear();
  }
  const key = `${kind}\0${resource}`;
  const hit = this.permCache.get(key);
  if (hit !== undefined) return hit;
  const ok = this.broker.can(this.user, this.vhost, kind, resource);
  if (this.permCache.size > 4096) this.permCache.clear();
  this.permCache.set(key, ok);
  return ok;
}

/** Throw RabbitMQ's 403 when the permission is missing. */
export function need(this: Conn, kind: Access, what: "queue" | "exchange", resource: string): void {
  if (this.allowed(kind, resource)) return;
  throw new ChanError(
    403,
    `ACCESS_REFUSED - ${kind} access to ${what} '${resource}' in vhost '${this.vhost}' refused for user '${this.user}'`,
  );
}

/** Throw 405 when another connection owns this exclusive queue. A missing queue is left to the caller. */
export function own(this: Conn, queue: string): void {
  const q = this.broker.queues.get(this.broker.key(this.vhost, queue));
  if (q?.owner != null && q.owner !== this.id) {
    throw new ChanError(
      405,
      `RESOURCE_LOCKED - cannot obtain exclusive access to locked queue '${queue}' in vhost '${this.vhost}'`,
    );
  }
}

/** Delete the exclusive queues this connection declared. Called once the connection is gone. */
export async function dropExclusive(this: Conn): Promise<void> {
  for (const q of [...this.broker.queues.values()]) {
    if (q.owner !== this.id) continue;
    try {
      await this.broker.deleteQueue(q.vhost, q.name);
    } catch {
      /* already deleted */
    }
  }
}

Conn.prototype.allowed = allowed;
Conn.prototype.need = need;
Conn.prototype.own = own;
Conn.prototype.dropExclusive = dropExclusive;

declare module "./listen.ts" {
  interface Conn {
    allowed: typeof allowed;
    need: typeof need;
    own: typeof own;
    dropExclusive: typeof dropExclusive;
  }
}
