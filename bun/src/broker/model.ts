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
  redelivered: boolean;
  deliveries?: number;
};

export type Consumer = {
  tag: string;
  session: number;
  noAck: boolean;
  exclusive: boolean;
  priority?: number;
  want: () => boolean;
  deliver: (msg: LiveMsg) => void;
  onCancel?: () => void;
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
  ready: LiveMsg[];
  replicas: LiveMsg[];
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
