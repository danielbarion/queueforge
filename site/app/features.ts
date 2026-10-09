// Feature support, read from each broker's source on 2026-10-07 and updated
// on 2026-10-08 after the Raft and PHP work, then again for the Bun work
// (per-queue Raft groups, replicated streams, stored limits and parameters,
// TLS on every listener and core, feature flags an operator enables). "p" is
// partial: the note says what is missing. RabbitMQ notes name the plugin
// a feature needs; core features carry no note.
//
// Rows that conformance/ covers were then checked against RabbitMQ 4.3 and
// each broker (`cd conformance && bun run.ts`). A green cell on such a row
// means every test for it passed. On 2026-10-08: RabbitMQ 133/134 (the
// delayed exchange is a plugin), Rust 134/134, Bun 134/134, PHP 60/134.

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

export const AUDITED = "2026-10-08";

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
          php: ["n", "shim; 0 of 12 tests pass"],
        },
      },
      {
        name: "MQTT 3.1.1",
        detail: "Publish and subscribe for devices",
        cells: {
          mq: ["y", "plugin"],
          rust: ["p", "retained in memory"],
          bun: "y",
          php: ["p", "QoS 0, no auth"],
        },
      },
      {
        name: "MQTT 5.0",
        detail: "Properties, session expiry, reason codes",
        cells: { mq: ["y", "plugin"], rust: ["p", "properties and reason codes"], bun: ["p", "properties and reason codes"], php: "n" },
      },
      {
        name: "STOMP",
        detail: "Text frames, versions 1.0 to 1.2",
        cells: {
          mq: ["y", "plugin"],
          rust: "y",
          bun: "y",
          php: ["p", "no ACK or auth"],
        },
      },
      {
        name: "WebSockets",
        detail: "Web MQTT and Web STOMP",
        cells: { mq: ["y", "plugin"], rust: ["y", "/ws on the management port"], bun: ["y", "/ws on the management port"], php: "n" },
      },
      {
        name: "Stream protocol",
        detail: "The binary stream protocol, port 5552",
        cells: {
          mq: ["y", "plugin"],
          rust: "y",
          bun: "y",
          php: ["n", "in memory; 0 of 9 tests pass"],
        },
      },
      {
        name: "TLS",
        detail: "Encrypted client listeners",
        cells: {
          mq: "y",
          rust: ["y", "AMQP, HTTPS, MQTT, STOMP, streams"],
          bun: ["y", "AMQP, HTTPS, MQTT, STOMP, streams"],
          php: ["p", "AMQP only"],
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
          php: ["p", "not stored"],
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
        cells: { mq: ["y", "plugin"], rust: ["p", "routing key only"], bun: ["p", "routing key only"], php: "n" },
      },
      {
        name: "Delayed messages",
        detail: "Hold a message for a set time before routing",
        code: ["x-delayed-message"],
        cells: { mq: ["p", "community plugin"], rust: ["p", "delays held in memory"], bun: ["p", "delays held in memory"], php: "n" },
      },
      {
        name: "Local random exchange",
        detail: "Deliver to a queue on the publishing node",
        code: ["x-local-random"],
        cells: { mq: "y", rust: "y", bun: "y", php: "n" },
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
          php: ["p", "arguments lost on restart"],
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
          php: ["p", "majority ack, no Raft"],
        },
      },
      {
        name: "Stream queues",
        detail: "Append-only log, consume from an offset",
        code: ["x-queue-type=stream"],
        cells: { mq: "y", rust: ["y", "Raft-replicated; shared with Bun"], bun: ["y", "Raft-replicated; shared with Rust"], php: ["p", "1 of 5 tests pass"] },
      },
      {
        name: "Super streams",
        detail: "A stream partitioned across several logs",
        cells: { mq: "y", rust: "y", bun: "y", php: "n" },
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
        cells: { mq: "y", rust: "y", bun: "y", php: ["p", "1 of 2 tests pass"] },
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
          php: ["p", "no x-death header"],
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
          php: ["p", "no redelivery"],
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
          php: ["p", "switches on full prefetch"],
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
        cells: { mq: "y", rust: "y", bun: "y", php: "n" },
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
          php: ["p", "one vhost; 0 of 3 tests pass"],
        },
      },
      {
        name: "Topic permissions",
        detail: "Limit routing keys per user",
        cells: {
          mq: "y",
          rust: "y",
          bun: "y",
          php: ["n", "0 of 2 tests pass"],
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
          php: ["n", "0 of 2 tests pass"],
        },
      },
      {
        name: "x.509 client certificates",
        detail: "The EXTERNAL SASL mechanism",
        cells: { mq: ["y", "plugin"], rust: ["p", "AMQP 0-9-1 only"], bun: ["p", "AMQP 0-9-1 only"], php: ["n", "only the refusal test passes"] },
      },
      {
        name: "LDAP",
        detail: "Users and permissions from a directory",
        cells: { mq: ["y", "plugin"], rust: ["p", "bind and an admin group, no access queries"], bun: ["p", "bind and an admin group, no access queries"], php: ["n", "only the refusal test passes"] },
      },
      {
        name: "OAuth 2.0",
        detail: "JWT tokens instead of passwords",
        cells: { mq: ["y", "plugin"], rust: ["p", "RS256 via JWKS, no token refresh"], bun: ["p", "RS256 via JWKS, no token refresh"], php: ["n", "only the refusal test passes"] },
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
          php: ["p", "no connection list"],
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
          php: ["p", "no users or policies"],
        },
      },
      {
        name: "Policies",
        detail: "Queue settings applied by pattern",
        cells: { mq: "y", rust: "y", bun: "y", php: ["p", "not stored"] },
      },
      {
        name: "Operator policies",
        detail: "Limits that users cannot override",
        cells: {
          mq: "y",
          rust: "y",
          bun: "y",
          php: ["n", "does not cap values"],
        },
      },
      {
        name: "Shovel",
        detail: "Move messages from one broker to another",
        cells: {
          mq: ["y", "plugin"],
          rust: ["p", "not stored"],
          bun: "y",
          php: ["n", "stored, never runs"],
        },
      },
      {
        name: "Federation",
        detail: "Link exchanges across brokers",
        cells: {
          mq: ["y", "plugin"],
          rust: ["p", "exchanges, in memory"],
          bun: ["p", "exchanges, in memory"],
          php: ["n", "stored, never runs"],
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
        cells: { mq: "y", rust: ["y", "queueforge-ctl"], bun: ["y", "queueforge-ctl"], php: ["p", "4 of 6 tests pass"] },
      },
      {
        name: "Message tracing",
        detail: "The firehose tracer",
        cells: { mq: "y", rust: "y", bun: "y", php: "n" },
      },
      {
        name: "Event exchange",
        detail: "Broker events published as messages",
        cells: { mq: ["y", "plugin"], rust: "y", bun: "y", php: "n" },
      },
      {
        name: "Feature flags",
        detail: "Turn on new behaviour across a cluster",
        cells: {
          mq: "y",
          rust: ["y", "raft and four RabbitMQ flags"],
          bun: ["y", "raft and four RabbitMQ flags"],
          php: ["p", "fixed list"],
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
          php: ["p", "keeps the cluster on version 1"],
        },
      },
      {
        name: "Raft consensus",
        detail: "Elections and terms for replicated state",
        cells: { mq: "y", rust: ["y", "meta, and a group per queue"], bun: ["y", "meta, and a group per queue"], php: ["p", "core only, not wired in"] },
      },
      {
        name: "Replicated metadata store",
        detail: "Khepri in RabbitMQ 4.3",
        cells: {
          mq: "y",
          rust: ["y", "Raft log, membership included"],
          bun: ["y", "Raft log, membership included"],
          php: ["n", "merged snapshots"],
        },
      },
      {
        name: "Peer discovery",
        detail: "Classic config and DNS, both in the RabbitMQ core",
        cells: { mq: "y", rust: ["y", "config, env and DNS"], bun: ["y", "config, env and DNS"], php: "n" },
      },
      {
        name: "Cloud peer discovery",
        detail: "Kubernetes, Consul, etcd, AWS",
        cells: { mq: ["y", "plugin"], rust: "n", bun: "n", php: "n" },
      },
      {
        name: "Rolling upgrades",
        detail: "Upgrade one node at a time",
        cells: { mq: "y", rust: ["y", "enable raft after the last node"], bun: ["y", "enable raft after the last node"], php: "n" },
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
