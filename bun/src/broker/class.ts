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
import type { LiveMsg, MgmtChannel, MgmtConnection, MgmtConsumer, Policy, QueueLive, TopicPerm } from "./model.ts";
import { emptyProm } from "./model.ts";

/**
 * Queues, exchanges, and the confirm path for one Bun process.
 *
 * Construct this with the parsed config and the open store. Call load before serving.
 */
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
  perms: Array<{ user: string; vhost: string; configure: string; write: string; read: string }> = [];
  vhosts = new Set<string>();
  sessionsNext = 1;
  connSeq = 0;
  mgmtConnections = new Map<string, MgmtConnection>();
  mgmtChannels = new Map<string, MgmtChannel>();
  mgmtConsumers: MgmtConsumer[] = [];
  topicPerms: TopicPerm[] = [];
  userConnLimit = new Map<string, number>();
  userChanLimit = new Map<string, number>();
  vhostConnLimit = new Map<string, number>();
  vhostQueueLimit = new Map<string, number>();
  operatorPolicies: Policy[] = [];
  transientNonexcl = false;
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

  /** Member ids that refused five dials during `quorumHold`. */
  downPeers = new Set<string>();

  /** Refused dials since the last successful connect, per member id. */
  downStrikes = new Map<string, number>();

  /** Snapshot of delivered quorum ids. Built when a peer connects, not on the publish path. */
  get consumed(): Array<{ vhost: string; queue: string; id: string }> {
    return this.consumedIds.list();
  }

}
