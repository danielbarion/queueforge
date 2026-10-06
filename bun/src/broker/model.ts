/**
 * Live queue, policy, prometheus, and management row types.
 */
import type { Field } from "../codec.ts";
import type { QueueRow } from "../store.ts";

export type LiveMsg = {
  id: string;
  rowId: number | null;
  body: Uint8Array;
  exchange: string;
  routingKey: string;
  headers: Array<[string, Field]>;
  propRaw: Uint8Array;
  persistent: boolean;
  priority: number;
  expiresAt: number | null;
  /** Byte size when `body` is not held in memory. Ready accounting uses this. */
  bodyBytes?: number;
  redelivered: boolean;
  deliveries?: number;
  /** While `hold` is set, deliver and get leave this body at the head. */
  confirmGate?: { hold: boolean };
  /** True after a claim drop reached every peer. The ack then skips a second drop. */
  quorumDropped?: boolean;
  /** Durable quorum fields live in the row until deliver reloads them. */
  slim?: boolean;
};

const READY_CHUNK = 128;

/** Shared empty payload for a durable quorum body that already sits in the store. */
export const EMPTY_BODY = new Uint8Array(0);

/** Shared header list for a slim quorum record. Deliver replaces it from the row. */
export const EMPTY_HEADERS: Array<[string, Field]> = [];

function accountedBytes(msg: LiveMsg): number {
  return msg.bodyBytes ?? msg.body.length;
}

/**
 * Ready messages in arrival order.
 *
 * `shift` drops the head without sliding the whole backlog. `bytes` is the
 * body total, so a max-length check does not walk the queue.
 */
export class ReadyQueue {
  private chunks: LiveMsg[][] = [];
  private headChunk = 0;
  private head = 0;
  length = 0;
  bytes = 0;
  /** Messages whose `expiresAt` is set. Expire can skip the scan when this is 0. */
  expiring = 0;

  push(msg: LiveMsg): void {
    // A drained queue leaves the cursor past the last chunk. Appending to that
    // chunk would store the message behind the cursor, so the next shift misses it.
    if (this.headChunk >= this.chunks.length) {
      this.chunks = [[msg]];
      this.headChunk = 0;
      this.head = 0;
      this.length = 0;
      this.bytes = 0;
      this.expiring = 0;
      this.noteAdd(msg);
      return;
    }
    let tail = this.chunks[this.chunks.length - 1]!;
    if (tail.length >= READY_CHUNK) {
      tail = [];
      this.chunks.push(tail);
    }
    tail.push(msg);
    this.noteAdd(msg);
  }

  shift(): LiveMsg | undefined {
    while (this.headChunk < this.chunks.length) {
      const chunk = this.chunks[this.headChunk]!;
      if (this.head < chunk.length) {
        const msg = chunk[this.head]!;
        this.head++;
        if (this.head === chunk.length) {
          this.headChunk++;
          this.head = 0;
          if (this.headChunk > 8) this.compact();
        }
        this.noteRemove(msg);
        return msg;
      }
      this.headChunk++;
      this.head = 0;
    }
    return undefined;
  }

  /** Put `msg` back at the head. Requeue is rare next to publish. */
  unshift(msg: LiveMsg): void {
    this.compact();
    if (this.chunks.length === 0) this.chunks.push([]);
    if (this.head > 0) {
      this.head--;
      this.chunks[0]![this.head] = msg;
    } else {
      this.chunks[0]!.unshift(msg);
    }
    this.noteAdd(msg);
  }

  at(index: number): LiveMsg | undefined {
    let left = index;
    for (let c = this.headChunk; c < this.chunks.length; c++) {
      const chunk = this.chunks[c]!;
      const start = c === this.headChunk ? this.head : 0;
      const count = chunk.length - start;
      if (left < count) return chunk[start + left];
      left -= count;
    }
    return undefined;
  }

  /** Insert at a ready index. Priority queues are not the benchmark path. */
  insertAt(index: number, msg: LiveMsg): void {
    const items = this.toArray();
    items.splice(index, 0, msg);
    this.rebuild(items);
  }

  /** Remove messages matching `pred` and return them. The rest stay in order. */
  extract(pred: (msg: LiveMsg) => boolean): LiveMsg[] {
    const keep: LiveMsg[] = [];
    const taken: LiveMsg[] = [];
    for (const msg of this) {
      if (pred(msg)) taken.push(msg);
      else keep.push(msg);
    }
    this.rebuild(keep);
    return taken;
  }

  rebuild(items: LiveMsg[]): void {
    this.chunks = [];
    this.headChunk = 0;
    this.head = 0;
    this.length = 0;
    this.bytes = 0;
    this.expiring = 0;
    for (const msg of items) this.push(msg);
  }

  clear(): void {
    this.chunks = [];
    this.headChunk = 0;
    this.head = 0;
    this.length = 0;
    this.bytes = 0;
    this.expiring = 0;
  }

  toArray(): LiveMsg[] {
    return [...this];
  }

  *[Symbol.iterator](): IterableIterator<LiveMsg> {
    for (let c = this.headChunk; c < this.chunks.length; c++) {
      const chunk = this.chunks[c]!;
      const start = c === this.headChunk ? this.head : 0;
      for (let i = start; i < chunk.length; i++) yield chunk[i]!;
    }
  }

  private compact(): void {
    if (this.headChunk === 0) return;
    this.chunks = this.chunks.slice(this.headChunk);
    this.headChunk = 0;
  }

  private noteAdd(msg: LiveMsg): void {
    this.length++;
    this.bytes += accountedBytes(msg);
    if (msg.expiresAt != null) this.expiring++;
  }

  private noteRemove(msg: LiveMsg): void {
    this.length--;
    this.bytes -= accountedBytes(msg);
    if (msg.expiresAt != null) this.expiring--;
  }
}

export type Consumer = {
  tag: string;
  session: number;
  noAck: boolean;
  exclusive: boolean;
  priority?: number;
  want: () => boolean;
  /**
   * Count one quorum claim against prefetch before the follower drop finishes.
   * `deliver` runs only after that drop, so credit has to move here or pump
   * keeps taking messages while `want` still looks open.
   */
  reserve?: () => void;
  deliver: (msg: LiveMsg) => void;
  onCancel?: () => void;
  /** Add AMQP prefetch credit for a consumer registered by a remote member. */
  addCredit?: (n: number) => void;
  /** Replace remaining credit. Null means unlimited. */
  setCredit?: (n: number | null) => void;
};

export type QArgs = {
  messageTtl: number | null;
  expiresMs: number | null;
  maxLength: number | null;
  maxLengthBytes: number | null;
  overflow: "drop-head" | "reject-publish" | "reject-publish-dlx";
  dlxStrategy: "at-most-once" | "at-least-once";
  dlx: string | null;
  dlxKey: string | null;
  maxPriority: number | null;
  singleActive: boolean;
  deliveryLimit: number | null;
  queueType: "classic" | "quorum";
};

export type QueueLive = QueueRow & {
  argsParsed: QArgs;
  declaredArgs: Record<string, string | number>;
  ready: ReadyQueue;
  /** Follower copies in append order. Delete by id does not rebuild the log. */
  replicas: Map<string, LiveMsg>;
  unacked: Map<string, LiveMsg>;
  consumers: Consumer[];
  rr: number;
  lastUsed: number;
};

export type Policy = {
  vhost: string;
  name: string;
  pattern: string;
  applyTo: "queues" | "exchanges" | "all";
  priority: number;
  messageTtl: number | null;
  expiresMs: number | null;
  dlx: string | null;
  dlxKey: string | null;
  maxLength: number | null;
  maxLengthBytes: number | null;
  overflow: "drop-head" | "reject-publish" | "reject-publish-dlx" | null;
  dlxStrategy: "at-most-once" | "at-least-once" | null;
  deliveryLimit: number | null;
  alternate: string | null;
};

export const BUILTIN: Array<[string, string, boolean]> = [
  ["", "default", true],
  ["amq.direct", "direct", false],
  ["amq.fanout", "fanout", false],
  ["amq.topic", "topic", false],
];

export type Prom = {
  connections: number;
  connectionsOpened: number;
  connectionsClosed: number;
  channels: number;
  channelsOpened: number;
  channelsClosed: number;
  queuesDeclared: number;
  queuesCreated: number;
  queuesDeleted: number;
  consumers: number;
  received: number;
  receivedConfirm: number;
  confirmed: number;
  routed: number;
  unroutableDropped: number;
  unroutableReturned: number;
  delivered: number;
  deliveredConsumeManual: number;
  deliveredConsumeAuto: number;
  deliveredGetManual: number;
  deliveredGetAuto: number;
  getEmpty: number;
  acknowledged: number;
  redelivered: number;
  dlxExpired: number;
  dlxRejected: number;
  dlxMaxlen: number;
  dlxDeliveryLimit: number;
};

/**
 * Zero every management counter.
 *
 * @returns A new counter object with every field set to 0. The caller owns it. Reusing the same object for a second broker mixes the two processes' counts.
 */
export function emptyProm(): Prom {
  return {
    connections: 0,
    connectionsOpened: 0,
    connectionsClosed: 0,
    channels: 0,
    channelsOpened: 0,
    channelsClosed: 0,
    queuesDeclared: 0,
    queuesCreated: 0,
    queuesDeleted: 0,
    consumers: 0,
    received: 0,
    receivedConfirm: 0,
    confirmed: 0,
    routed: 0,
    unroutableDropped: 0,
    unroutableReturned: 0,
    delivered: 0,
    deliveredConsumeManual: 0,
    deliveredConsumeAuto: 0,
    deliveredGetManual: 0,
    deliveredGetAuto: 0,
    getEmpty: 0,
    acknowledged: 0,
    redelivered: 0,
    dlxExpired: 0,
    dlxRejected: 0,
    dlxMaxlen: 0,
    dlxDeliveryLimit: 0,
  };
}

export type MgmtConnection = {
  name: string;
  user: string;
  vhost: string;
  peer_host: string;
  peer_port: number;
  channels: number;
  connected_at: number;
  close: () => void;
};

export type MgmtChannel = {
  name: string;
  connection: string;
  user: string;
  vhost: string;
  number: number;
  peer_host: string;
  peer_port: number;
};

export type MgmtConsumer = {
  consumer_tag: string;
  connection: string;
  channel: number;
  queue: string;
  vhost: string;
};

export type TopicPerm = {
  user: string;
  vhost: string;
  exchange: string;
  write: string;
  read: string;
};
