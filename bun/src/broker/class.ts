/**
 * Process-wide broker state.
 *
 * Methods live in the sibling files and are installed on this prototype.
 * enqueueQuorum still calls durableMajority from quorum-confirm.ts.
 */
import type { Cluster } from "../cluster.ts";
import type { Config } from "../config.ts";
import { Store, type BindRow, type ExRow } from "../store.ts";
import { ConsumedSet } from "./consumed-set.ts";
import type { AlarmListener, LiveMsg, MgmtChannel, MgmtConnection, MgmtConsumer, Policy, QueueLive, TopicPerm } from "./model.ts";
import { emptyProm } from "./model.ts";

/**
 * Queues, exchanges, and the confirm path for one Bun process.
 *
 * Construct this with the parsed config and the open store. Call load before serving.
 */
/** One group commit's quorum publishes: their gates, queues and ids, in order, and the shared ack. */
export type QuorumAcks = {
  of: Promise<void>;
  confirms: number;
  gates: Array<{ hold: boolean }>;
  queues: QueueLive[];
  ids: string[];
  ack: Promise<"ack" | "nack">;
};

export class Broker {
  ready = false;
  prom = emptyProm();
  exchanges = new Map<string, ExRow>();
  queues = new Map<string, QueueLive>();
  bindings: BindRow[] = [];
  policies: Policy[] = [];
  e2e: Array<{ vhost: string; source: string; destination: string; routingKey: string }> = [];
  sessions = new Map<number, { vhost: string; queue: string }>();
  remoteQuorum = new Map<string, number>();
  cluster: Cluster | null = null;
  users = new Map<string, { hash: string; tags: string[] }>();
  /** Logins from the OAuth 2.0 and LDAP backends, by login name. Users in `users` never look here. */
  principals = new Map<string, import("../auth/backends.ts").Principal>();
  oauth: import("../auth/backends.ts").OauthBackend | null = null;
  ldap: import("../auth/backends.ts").LdapBackend | null = null;
  perms: Array<{ user: string; vhost: string; configure: string; write: string; read: string }> = [];
  vhosts = new Set<string>();
  sessionsNext = 1;
  connSeq = 0;
  mgmtConnections = new Map<string, MgmtConnection>();
  mgmtChannels = new Map<string, MgmtChannel>();
  mgmtConsumers: MgmtConsumer[] = [];
  topicPerms: TopicPerm[] = [];
  /** At-least-once dead letters sent to a remote home and not yet accepted. */
  dlxPending = 0;
  userConnLimit = new Map<string, number>();
  userChanLimit = new Map<string, number>();
  vhostConnLimit = new Map<string, number>();
  vhostQueueLimit = new Map<string, number>();
  operatorPolicies: Policy[] = [];
  transientNonexcl = false;
  memAlarm = false;
  diskAlarm = false;
  /** True while a memory or disk alarm holds publishes. */
  blocked = false;
  /** The shared ack of the current group commit on the publish fast path (see `localClassicFast`). */
  durableAcks: { of: Promise<void>; confirms: number; ack: Promise<"ack"> } | null = null;
  /** The same for quorum publishes this process alone makes durable (see `localQuorumFast`). */
  quorumAcks: QuorumAcks | null = null;
  alarmListeners = new Set<AlarmListener>();
  unblockWaiters: Array<() => void> = [];
  /** Stream queue logs, by vhost and name. Loaded on first use. */
  streams = new Map<string, import("./stream.ts").StreamState>();
  /** Running federation links by type, vhost, name and upstream (see federation.ts). */
  fedLinks = new Map<string, import("./federation.ts").FedLink>();
  /** Set once the broker is serving; federation links start only then. */
  federationOn = false;
  /** Running shovels by vhost and name. */
  shovels = new Map<string, import("./shovel.ts").ShovelRun>();
  /** Vhosts with firehose tracing on. */
  tracedVhosts = new Set<string>();
  /** Direct reply-to addresses, each delivering to one requester channel. */
  replySinks = new Map<string, (msg: { routingKey: string; body: Uint8Array; propRaw: Uint8Array }) => boolean>();
  readonly startedAt = Date.now();

  constructor(
    public cfg: Config,
    public store: Store,
  ) {}

  /** Quorum appends still waiting on each peer. A peer past the extra-copy cap is skipped. */
  appendInflight = new Map<string, number>();

  /** Quorum ids already delivered. A peer hello reads `consumed`. */
  consumedIds = new ConsumedSet();

  /**
   * Recovered quorum bodies sit in `replicas` until every other member's hello
   * has been applied, or that member has refused five cluster dials. `/readyz`
   * stays false for that wait. A peer that accepts a connection stays required.
   */
  quorumHold = false;

  /** Member ids whose hello or hello-reply has been applied during `quorumHold`. */
  heardPeers = new Set<string>();

  /** Drops every member accepted, waiting for the next forget round (`queueForget`). */
  forgetQueue: Array<{ vhost: string; queue: string; id: string }> = [];
  forgetTimer: ReturnType<typeof setTimeout> | null = null;
  forgetRound: Promise<void> | null = null;

  /** The running pass of `retryConsumedDrops`, and whether another must follow it. */
  dropRetry: Promise<void> | null = null;
  dropRetryAgain = false;

  /** Member ids that refused five dials during `quorumHold`. */
  downPeers = new Set<string>();

  /** Refused dials since the last successful connect, per member id. */
  downStrikes = new Map<string, number>();

  /** Snapshot of delivered quorum ids. Built when a peer connects, not on the publish path. */
  get consumed(): Array<{ vhost: string; queue: string; id: string }> {
    return this.consumedIds.list();
  }

}
