/**
 * Process-wide broker state.
 *
 * Methods live in the sibling files and are installed on this prototype.
 * enqueueQuorum still calls durableMajority from quorum-confirm.ts.
 */
import type { Cluster } from "../cluster.ts";
import type { Config } from "../config.ts";
import { Store, type BindRow, type ExRow } from "../store.ts";
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

  consumed: Array<{ vhost: string; queue: string; id: string }> = [];

}
