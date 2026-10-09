// Feature support audited from source and focused tests through 2026-10-09.
// "p" means partial; each note identifies a remaining limitation. RabbitMQ
// notes identify plugins; core features have no plugin note.
//
// Historical full conformance on 2026-10-08: RabbitMQ 133/134 (delayed
// exchange requires a plugin), Rust 134/134, Bun 134/134.
// PHP on 2026-10-09: 126/126 real-client tests across 11 non-auth files;
// external authentication checked separately with focused fixtures. Cluster
// and queue-home gates passed 7/7 and 3/3. These results do not establish
// advanced mixed-implementation Raft parity or optional client-certificate TLS.

export type Level = "y" | "p" | "n";
export type Cell = Level | readonly [Level, string];
export type Broker = "mq" | "rust" | "bun" | "php";

export type Feature = {
  name: string;
  detail: string;
  code?: readonly string[];
  cells: Record<Broker, Cell>;
};

export type FeatureGroup = { title: string; rows: Feature[] };

export const BROKERS: { key: Broker; name: string }[] = [
  { key: "mq", name: "RabbitMQ 4.3" },
  { key: "rust", name: "Rust" },
  { key: "bun", name: "Bun" },
  { key: "php", name: "PHP" },
];

export const AUDITED = "2026-10-09";

export const featureGroups: FeatureGroup[] = [
  {
    title: "Protocols",
    rows: [
      {
        name: "AMQP 0-9-1",
        detail: "The client protocol every engine is built around",
        cells: {
          mq: "y",
          rust: "y",
          bun: "y",
          php: "y",
        },
      },
      {
        name: "AMQP 1.0",
        detail: "Native in RabbitMQ 4, with v2 addresses, on port 5672",
        cells: {
          mq: "y",
          rust: ["y", "bridged over loopback 0-9-1"],
          bun: "y",
          php: "y",
        },
      },
      {
        name: "MQTT 3.1.1",
        detail: "Publish and subscribe for devices",
        cells: {
          mq: ["y", "plugin"],
          rust: ["p", "retained in memory"],
          bun: "y",
          php: "y",
        },
      },
      {
        name: "MQTT 5.0",
        detail: "Properties, session expiry, reason codes",
        cells: { mq: ["y", "plugin"], rust: ["p", "properties and reason codes"], bun: ["p", "properties and reason codes"], php: ["p", "properties and reason codes; session expiry incomplete"] },
      },
      {
        name: "STOMP",
        detail: "Text frames, versions 1.0 to 1.2",
        cells: {
          mq: ["y", "plugin"],
          rust: "y",
          bun: "y",
          php: "y",
        },
      },
      {
        name: "WebSockets",
        detail: "Web MQTT and Web STOMP",
        cells: { mq: ["y", "plugin"], rust: ["y", "/ws on the management port"], bun: ["y", "/ws on the management port"], php: ["y", "/ws on the management port"] },
      },
      {
        name: "Stream protocol",
        detail: "The binary stream protocol, port 5552",
        cells: {
          mq: ["y", "plugin"],
          rust: "y",
          bun: "y",
          php: "y",
        },
      },
      {
        name: "TLS",
        detail: "Encrypted client listeners",
        cells: {
          mq: "y",
          rust: ["y", "AMQP, HTTPS, MQTT, STOMP, streams"],
          bun: ["y", "AMQP, HTTPS, MQTT, STOMP, streams"],
          php: ["p", "all client listeners; configured client CA requires a certificate"],
        },
      },
    ],
  },
  {
    title: "Exchanges",
    rows: [
      {
        name: "Core exchange types",
        detail: "Routing by key, pattern or header",
        code: ["direct", "fanout", "topic", "headers"],
        cells: { mq: "y", rust: "y", bun: "y", php: "y" },
      },
      {
        name: "Default exchange",
        detail: "The routing key names the queue",
        cells: { mq: "y", rust: "y", bun: "y", php: "y" },
      },
      {
        name: "Exchange-to-exchange bindings",
        detail: "One exchange routes into another",
        cells: {
          mq: "y",
          rust: "y",
          bun: "y",
          php: "y",
        },
      },
      {
        name: "Alternate exchange",
        detail: "Unroutable messages go somewhere instead",
        code: ["alternate-exchange"],
        cells: { mq: "y", rust: "y", bun: "y", php: "y" },
      },
      {
        name: "Internal exchanges",
        detail: "Clients cannot publish to them directly",
        cells: {
          mq: "y",
          rust: "y",
          bun: "y",
          php: "y",
        },
      },
      {
        name: "Passive declare, delete if-unused",
        detail: "Check without creating; refuse to drop a bound exchange",
        cells: {
          mq: "y",
          rust: "y",
          bun: "y",
          php: "y",
        },
      },
      {
        name: "Consistent-hash exchange",
        detail: "Spread keys across queues by weight",
        code: ["x-consistent-hash"],
        cells: { mq: ["y", "plugin"], rust: ["p", "routing key only"], bun: ["p", "routing key only"], php: ["p", "routing key only"] },
      },
      {
        name: "Delayed messages",
        detail: "Hold a message for a set time before routing",
        code: ["x-delayed-message"],
        cells: { mq: ["p", "community plugin"], rust: ["p", "delays held in memory"], bun: ["p", "delays held in memory"], php: ["p", "delays held in memory"] },
      },
      {
        name: "Local random exchange",
        detail: "Deliver to a queue on the publishing node",
        code: ["x-local-random"],
        cells: { mq: "y", rust: "y", bun: "y", php: "y" },
      },
    ],
  },
  {
    title: "Queues",
    rows: [
      {
        name: "Durable classic queues",
        detail: "Queue and messages survive a restart",
        cells: {
          mq: "y",
          rust: "y",
          bun: "y",
          php: "y",
        },
      },
      {
        name: "Quorum queues",
        detail: "Replicated; confirm after a durable majority",
        code: ["x-queue-type=quorum"],
        cells: {
          mq: "y",
          rust: ["y", "a Raft group per queue; shared with Bun"],
          bun: ["y", "a Raft group per queue; shared with Rust"],
          php: ["p", "Raft wired; mixed majority and failover gates pending"],
        },
      },
      {
        name: "Stream queues",
        detail: "Append-only log, consume from an offset",
        code: ["x-queue-type=stream"],
        cells: { mq: "y", rust: ["y", "Raft-replicated; shared with Bun"], bun: ["y", "Raft-replicated; shared with Rust"], php: ["p", "durable offsets; advanced Raft replication gates pending"] },
      },
      {
        name: "Super streams",
        detail: "A stream partitioned across several logs",
        cells: { mq: "y", rust: "y", bun: "y", php: "y" },
      },
      {
        name: "Exclusive, auto-delete, server-named",
        detail: "Queues that live as long as their user",
        cells: {
          mq: "y",
          rust: "y",
          bun: "y",
          php: "y",
        },
      },
      {
        name: "Queue message TTL",
        detail: "Expire messages after a time",
        code: ["x-message-ttl"],
        cells: { mq: "y", rust: "y", bun: "y", php: "y" },
      },
      {
        name: "Per-message TTL",
        detail: "The expiration property on one message",
        cells: { mq: "y", rust: "y", bun: "y", php: "y" },
      },
      {
        name: "Queue expiry",
        detail: "Delete a queue that sits unused",
        code: ["x-expires"],
        cells: { mq: "y", rust: "y", bun: "y", php: "y" },
      },
      {
        name: "Length limits",
        detail: "Cap a queue by count or bytes",
        code: ["x-max-length", "x-max-length-bytes"],
        cells: { mq: "y", rust: "y", bun: "y", php: "y" },
      },
      {
        name: "Overflow behaviour",
        detail: "What a full queue does",
        code: ["drop-head", "reject-publish", "reject-publish-dlx"],
        cells: { mq: "y", rust: "y", bun: "y", php: "y" },
      },
      {
        name: "Dead-letter exchanges",
        detail: "Rejected and expired messages are rerouted",
        cells: {
          mq: "y",
          rust: "y",
          bun: "y",
          php: "y",
        },
      },
      {
        name: "At-least-once dead-lettering",
        detail: "The message is kept until the DLX accepts it",
        code: ["x-dead-letter-strategy"],
        cells: {
          mq: ["y", "quorum queues"],
          rust: "y",
          bun: "y",
          php: ["y", "source retained until destination commit; quorum removal replicated"],
        },
      },
      {
        name: "Priority queues",
        detail: "Higher priority messages are delivered first",
        code: ["x-max-priority"],
        cells: { mq: "y", rust: "y", bun: "y", php: "y" },
      },
      {
        name: "Single active consumer",
        detail: "One consumer at a time, with failover",
        code: ["x-single-active-consumer"],
        cells: {
          mq: "y",
          rust: "y",
          bun: "y",
          php: "y",
        },
      },
      {
        name: "Consumer priorities",
        detail: "Prefer some consumers over others",
        code: ["x-priority"],
        cells: { mq: "y", rust: "y", bun: "y", php: "y" },
      },
      {
        name: "Delivery limit",
        detail: "Dead-letter a message redelivered too often",
        code: ["x-delivery-limit"],
        cells: {
          mq: ["y", "quorum queues"],
          rust: "y",
          bun: "y",
          php: "y",
        },
      },
      {
        name: "Queue leader locator",
        detail: "Choose which node a new queue lives on",
        cells: { mq: "y", rust: "y", bun: "y", php: "y" },
      },
    ],
  },
  {
    title: "Messaging",
    rows: [
      {
        name: "Publisher confirms",
        detail: "The broker acks each publish it has taken",
        cells: { mq: "y", rust: "y", bun: "y", php: "y" },
      },
      {
        name: "Confirm after fsync",
        detail: "A durable confirm means the message is on disk",
        cells: {
          mq: ["p", "classic confirms before flush"],
          rust: "y",
          bun: "y",
          php: "y",
        },
      },
      {
        name: "Acks, nacks, rejects",
        detail: "Including multiple and requeue",
        cells: { mq: "y", rust: "y", bun: "y", php: "y" },
      },
      {
        name: "Prefetch",
        detail: "basic.qos, per consumer and per channel",
        cells: {
          mq: "y",
          rust: "y",
          bun: "y",
          php: "y",
        },
      },
      {
        name: "basic.get",
        detail: "Pull one message",
        cells: { mq: "y", rust: "y", bun: "y", php: "y" },
      },
      {
        name: "Mandatory and basic.return",
        detail: "Unroutable publishes come back",
        cells: { mq: "y", rust: "y", bun: "y", php: "y" },
      },
      {
        name: "Transactions",
        detail: "tx.select, commit, rollback",
        cells: {
          mq: "y",
          rust: "y",
          bun: "y",
          php: "y",
        },
      },
      {
        name: "basic.recover",
        detail: "Redeliver unacked messages",
        cells: {
          mq: "y",
          rust: "y",
          bun: "y",
          php: "y",
        },
      },
      {
        name: "Consumer cancel notification",
        detail: "The client hears when its queue goes away",
        cells: { mq: "y", rust: "y", bun: "y", php: "y" },
      },
      {
        name: "connection.blocked",
        detail: "Tell publishers when an alarm is raised",
        cells: { mq: "y", rust: "y", bun: "y", php: "y" },
      },
      {
        name: "Sender-selected distribution",
        detail: "Extra destinations in the CC and BCC headers",
        cells: { mq: "y", rust: "y", bun: "y", php: "y" },
      },
      {
        name: "Direct reply-to",
        detail: "RPC replies without a reply queue",
        code: ["amq.rabbitmq.reply-to"],
        cells: { mq: "y", rust: "y", bun: "y", php: "y" },
      },
      {
        name: "Validated user-id",
        detail: "The user-id property must match the login",
        cells: { mq: "y", rust: "y", bun: "y", php: "y" },
      },
    ],
  },
  {
    title: "Security",
    rows: [
      {
        name: "Users, vhosts, permissions",
        detail: "Configure, write and read patterns per vhost",
        cells: {
          mq: "y",
          rust: "y",
          bun: "y",
          php: "y",
        },
      },
      {
        name: "Topic permissions",
        detail: "Limit routing keys per user",
        cells: {
          mq: "y",
          rust: "y",
          bun: "y",
          php: "y",
        },
      },
      {
        name: "Password hashing",
        detail: "Salted SHA-256, RabbitMQ compatible",
        cells: { mq: "y", rust: "y", bun: "y", php: "y" },
      },
      {
        name: "Vhost and user limits",
        detail: "Caps on connections, channels and queues",
        cells: {
          mq: "y",
          rust: "y",
          bun: "y",
          php: "y",
        },
      },
      {
        name: "x.509 client certificates",
        detail: "The EXTERNAL SASL mechanism",
        cells: { mq: ["y", "plugin"], rust: ["p", "AMQP 0-9-1 only"], bun: ["p", "AMQP 0-9-1 only"], php: ["p", "AMQP 0-9-1 EXTERNAL; configured CA requires a certificate"] },
      },
      {
        name: "LDAP",
        detail: "Users and permissions from a directory",
        cells: { mq: ["y", "plugin"], rust: ["p", "bind and an admin group, no access queries"], bun: ["p", "bind and an admin group, no access queries"], php: ["p", "bind and an admin group, no access queries"] },
      },
      {
        name: "OAuth 2.0",
        detail: "JWT tokens instead of passwords",
        cells: { mq: ["y", "plugin"], rust: ["p", "RS256 via JWKS, no token refresh"], bun: ["p", "RS256 via JWKS, no token refresh"], php: ["p", "RS256 via JWKS and scopes; no token refresh"] },
      },
    ],
  },
  {
    title: "Operations",
    rows: [
      {
        name: "Management UI",
        detail: "The web console",
        cells: {
          mq: ["y", "plugin"],
          rust: ["y", "embedded"],
          bun: ["y", "shared build"],
          php: ["y", "shared build"],
        },
      },
      {
        name: "Management HTTP API",
        detail: "RabbitMQ-style /api routes",
        cells: {
          mq: ["y", "plugin"],
          rust: ["y", "a subset"],
          bun: ["y", "a subset"],
          php: ["y", "a subset, including connection and channel lists"],
        },
      },
      {
        name: "Prometheus metrics",
        detail: "rabbitmq_* metric families",
        cells: { mq: ["y", "plugin"], rust: "y", bun: "y", php: "y" },
      },
      {
        name: "Health and readiness",
        detail: "GET /healthz and /readyz",
        cells: { mq: ["y", "CLI and API checks"], rust: "y", bun: "y", php: "y" },
      },
      {
        name: "Definitions import and export",
        detail: "Topology as JSON",
        cells: {
          mq: "y",
          rust: "y",
          bun: "y",
          php: ["y", "users, vhosts, topology, permissions, policies and limits"],
        },
      },
      {
        name: "Policies",
        detail: "Queue settings applied by pattern",
        cells: { mq: "y", rust: "y", bun: "y", php: "y" },
      },
      {
        name: "Operator policies",
        detail: "Limits that users cannot override",
        cells: {
          mq: "y",
          rust: "y",
          bun: "y",
          php: "y",
        },
      },
      {
        name: "Shovel",
        detail: "Move messages from one broker to another",
        cells: {
          mq: ["y", "plugin"],
          rust: ["p", "not stored"],
          bun: "y",
          php: ["p", "local and remote AMQP 0-9-1; no AMQP 1.0 or stream sources"],
        },
      },
      {
        name: "Federation",
        detail: "Link exchanges across brokers",
        cells: {
          mq: ["y", "plugin"],
          rust: ["p", "exchanges, in memory"],
          bun: ["p", "exchanges, in memory"],
          php: ["p", "local and remote AMQP 0-9-1 exchanges; hop limit"],
        },
      },
      {
        name: "Memory and disk alarms",
        detail: "Stop publishers before resources run out",
        cells: {
          mq: "y",
          rust: "y",
          bun: "y",
          php: ["p", "disk only, no memory watermark"],
        },
      },
      {
        name: "Command-line tool",
        detail: "rabbitmqctl and friends",
        cells: { mq: "y", rust: ["y", "queueforge-ctl"], bun: ["y", "queueforge-ctl"], php: "y" },
      },
      {
        name: "Message tracing",
        detail: "The firehose tracer",
        cells: { mq: "y", rust: "y", bun: "y", php: "y" },
      },
      {
        name: "Event exchange",
        detail: "Broker events published as messages",
        cells: { mq: ["y", "plugin"], rust: "y", bun: "y", php: "y" },
      },
      {
        name: "Feature flags",
        detail: "Turn on new behaviour across a cluster",
        cells: {
          mq: "y",
          rust: ["y", "raft and four RabbitMQ flags"],
          bun: ["y", "raft and four RabbitMQ flags"],
          php: ["p", "enable/disable controls; cluster rollout not verified"],
        },
      },
    ],
  },
  {
    title: "Clustering",
    rows: [
      {
        name: "Multi-node cluster",
        detail: "Several processes, one broker",
        cells: { mq: "y", rust: "y", bun: "y", php: "y" },
      },
      {
        name: "Runtime membership",
        detail: "Add and remove nodes without a restart",
        cells: { mq: "y", rust: "y", bun: "y", php: "y" },
      },
      {
        name: "Mixed implementations",
        detail: "Rust, Bun and PHP members in one list",
        cells: {
          mq: ["n", "one implementation"],
          rust: "y",
          bun: "y",
          php: ["p", "legacy links checked; mixed Raft gates pending"],
        },
      },
      {
        name: "Raft consensus",
        detail: "Elections and terms for replicated state",
        cells: { mq: "y", rust: ["y", "meta, and a group per queue"], bun: ["y", "meta, and a group per queue"], php: ["p", "metadata and queue groups wired; mixed gates pending"] },
      },
      {
        name: "Replicated metadata store",
        detail: "Khepri in RabbitMQ 4.3",
        cells: {
          mq: "y",
          rust: ["y", "Raft log, membership included"],
          bun: ["y", "Raft log, membership included"],
          php: ["p", "committed metadata commands; mixed Raft gates pending"],
        },
      },
      {
        name: "Peer discovery",
        detail: "Classic config and DNS, both in the RabbitMQ core",
        cells: { mq: "y", rust: ["y", "config, env and DNS"], bun: ["y", "config, env and DNS"], php: ["y", "config, env and startup DNS"] },
      },
      {
        name: "Cloud peer discovery",
        detail: "Kubernetes, Consul, etcd, AWS",
        cells: { mq: ["y", "plugin"], rust: "n", bun: "n", php: "n" },
      },
      {
        name: "Rolling upgrades",
        detail: "Upgrade one node at a time",
        cells: { mq: "y", rust: ["y", "enable raft after the last node"], bun: ["y", "enable raft after the last node"], php: ["p", "capability negotiation; mixed Raft upgrades unverified"] },
      },
      {
        name: "Uses every core",
        detail: "One broker process scales across CPUs",
        cells: {
          mq: "y",
          rust: "y",
          bun: ["y", "one child per core"],
          php: ["p", "off with TLS"],
        },
      },
    ],
  },
];

export function level(cell: Cell): Level {
  return typeof cell === "string" ? cell : cell[0];
}

export function note(cell: Cell): string | undefined {
  return typeof cell === "string" ? undefined : cell[1];
}

/** A row RabbitMQ covers without a plugin. Only these are counted in the tallies. */
export function isStandard(row: Feature): boolean {
  return !(note(row.cells.mq) ?? "").includes("plugin");
}

export function standardCount(): number {
  return featureGroups.reduce((sum, group) => sum + group.rows.filter(isStandard).length, 0);
}

export function tally(broker: Broker) {
  const cells = featureGroups.flatMap((group) =>
    group.rows.filter(isStandard).map((row) => level(row.cells[broker])),
  );
  return {
    y: cells.filter((value) => value === "y").length,
    p: cells.filter((value) => value === "p").length,
    total: cells.length,
  };
}
