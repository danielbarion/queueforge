// Feature support, read from each broker's source on 2026-10-07. "p" is
// partial: the note says what is missing. RabbitMQ notes name the plugin
// a feature needs; core features carry no note.

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

export const AUDITED = "2026-10-07";

export const featureGroups: FeatureGroup[] = [
  {
    title: "Protocols",
    rows: [
      {
        name: "AMQP 0-9-1",
        detail: "The client protocol every engine is built around",
        cells: {
          mq: "y",
          rust: ["p", "no queue.purge"],
          bun: ["p", "no global qos"],
          php: ["p", "no tx.rollback"],
        },
      },
      {
        name: "AMQP 1.0",
        detail: "Native in RabbitMQ 4. A shim on port 5672 here",
        cells: {
          mq: "y",
          rust: ["p", "shim, small bodies"],
          bun: ["p", "shim, default exchange"],
          php: ["p", "shim, no credit"],
        },
      },
      {
        name: "MQTT 3.1.1",
        detail: "Publish and subscribe for devices",
        cells: {
          mq: ["y", "plugin"],
          rust: ["p", "QoS 0, no auth"],
          bun: ["p", "QoS 0, no auth"],
          php: ["p", "QoS 0, no auth"],
        },
      },
      {
        name: "MQTT 5.0",
        detail: "Properties, session expiry, reason codes",
        cells: { mq: ["y", "plugin"], rust: "n", bun: "n", php: "n" },
      },
      {
        name: "STOMP",
        detail: "Text frames, versions 1.0 to 1.2",
        cells: {
          mq: ["y", "plugin"],
          rust: ["p", "no ACK or auth"],
          bun: ["p", "no ACK or auth"],
          php: ["p", "no ACK or auth"],
        },
      },
      {
        name: "WebSockets",
        detail: "Web MQTT and Web STOMP",
        cells: { mq: ["y", "plugin"], rust: "n", bun: "n", php: "n" },
      },
      {
        name: "Stream protocol",
        detail: "The binary stream protocol, port 5552",
        cells: {
          mq: ["y", "plugin"],
          rust: ["p", "in memory, no offsets"],
          bun: ["p", "in memory, no offsets"],
          php: ["p", "in memory, no offsets"],
        },
      },
      {
        name: "TLS",
        detail: "Encrypted client listeners",
        cells: {
          mq: "y",
          rust: ["p", "AMQP and HTTPS"],
          bun: ["n", "parsed, unused"],
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
          rust: ["p", "not stored"],
          bun: ["p", "not stored"],
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
          rust: ["p", "blocks e2e hops"],
          bun: ["p", "blocks e2e hops"],
          php: "y",
        },
      },
      {
        name: "Passive declare, delete if-unused",
        detail: "Check without creating; refuse to drop a bound exchange",
        cells: {
          mq: "y",
          rust: "y",
          bun: ["p", "passive does not check"],
          php: ["p", "if-unused ignored"],
        },
      },
      {
        name: "Consistent-hash exchange",
        detail: "Spread keys across queues by weight",
        code: ["x-consistent-hash"],
        cells: { mq: ["y", "plugin"], rust: "n", bun: "n", php: "n" },
      },
      {
        name: "Delayed messages",
        detail: "Hold a message for a set time before routing",
        code: ["x-delayed-message"],
        cells: { mq: ["p", "community plugin"], rust: "n", bun: "n", php: "n" },
      },
      {
        name: "Local random exchange",
        detail: "Deliver to a queue on the publishing node",
        code: ["x-local-random"],
        cells: { mq: "y", rust: "n", bun: "n", php: "n" },
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
          rust: ["p", "majority ack, no Raft"],
          bun: ["p", "majority ack, no Raft"],
          php: ["p", "majority ack, no Raft"],
        },
      },
      {
        name: "Stream queues",
        detail: "Append-only log, consume from an offset",
        code: ["x-queue-type=stream"],
        cells: { mq: "y", rust: "n", bun: "n", php: "n" },
      },
      {
        name: "Super streams",
        detail: "A stream partitioned across several logs",
        cells: { mq: "y", rust: "n", bun: "n", php: "n" },
      },
      {
        name: "Exclusive, auto-delete, server-named",
        detail: "Queues that live as long as their user",
        cells: {
          mq: "y",
          rust: "y",
          bun: ["p", "server-named only"],
          php: ["p", "server-named only"],
        },
      },
      {
        name: "Queue message TTL",
        detail: "Expire messages after a time",
        code: ["x-message-ttl"],
        cells: { mq: "y", rust: ["p", "0 refused"], bun: "y", php: "y" },
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
        cells: { mq: "y", rust: "y", bun: ["p", "reset on consume only"], php: "y" },
      },
      {
        name: "Length limits",
        detail: "Cap a queue by count or bytes",
        code: ["x-max-length", "x-max-length-bytes"],
        cells: { mq: "y", rust: ["p", "0 refused"], bun: "y", php: "y" },
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
          bun: ["p", "x-death count stays 1"],
          php: ["p", "x-death count stays 1"],
        },
      },
      {
        name: "At-least-once dead-lettering",
        detail: "The message is kept until the DLX accepts it",
        code: ["x-dead-letter-strategy"],
        cells: {
          mq: ["y", "quorum queues"],
          rust: "y",
          bun: ["p", "remote DLX not retried"],
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
          rust: ["p", "switches on full prefetch"],
          bun: ["p", "not sticky"],
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
          rust: ["p", "nack requeue only"],
          bun: "y",
          php: "y",
        },
      },
      {
        name: "Queue leader locator",
        detail: "Choose which node a new queue lives on",
        cells: { mq: "y", rust: "n", bun: "n", php: "n" },
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
        cells: { mq: "y", rust: "y", bun: "y", php: ["p", "unacked lost on drop"] },
      },
      {
        name: "Prefetch",
        detail: "basic.qos, per consumer and per channel",
        cells: {
          mq: "y",
          rust: ["p", "global flag ignored"],
          bun: ["p", "global flag ignored"],
          php: ["p", "global flag ignored"],
        },
      },
      {
        name: "basic.get",
        detail: "Pull one message",
        cells: { mq: "y", rust: "y", bun: ["p", "message-count is 0"], php: "y" },
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
          rust: ["p", "nack not in tx"],
          bun: "y",
          php: ["p", "no rollback"],
        },
      },
      {
        name: "basic.recover",
        detail: "Redeliver unacked messages",
        cells: {
          mq: "y",
          rust: ["p", "requeue only"],
          bun: ["p", "requeue only"],
          php: ["p", "requeue only"],
        },
      },
      {
        name: "Consumer cancel notification",
        detail: "The client hears when its queue goes away",
        cells: { mq: "y", rust: "y", bun: "y", php: "n" },
      },
      {
        name: "connection.blocked",
        detail: "Tell publishers when an alarm is raised",
        cells: { mq: "y", rust: "n", bun: "n", php: "n" },
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
        cells: { mq: "y", rust: "n", bun: "n", php: "n" },
      },
      {
        name: "Validated user-id",
        detail: "The user-id property must match the login",
        cells: { mq: "y", rust: "n", bun: "n", php: "n" },
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
          bun: ["p", "patterns not checked"],
          php: ["p", "patterns not checked"],
        },
      },
      {
        name: "Topic permissions",
        detail: "Limit routing keys per user",
        cells: {
          mq: "y",
          rust: ["p", "write only"],
          bun: ["p", "write only"],
          php: ["p", "write only"],
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
          rust: ["p", "not stored"],
          bun: ["p", "not stored"],
          php: ["p", "not stored"],
        },
      },
      {
        name: "x.509 client certificates",
        detail: "The EXTERNAL SASL mechanism",
        cells: { mq: ["y", "plugin"], rust: "n", bun: "n", php: "n" },
      },
      {
        name: "LDAP",
        detail: "Users and permissions from a directory",
        cells: { mq: ["y", "plugin"], rust: "n", bun: "n", php: "n" },
      },
      {
        name: "OAuth 2.0",
        detail: "JWT tokens instead of passwords",
        cells: { mq: ["y", "plugin"], rust: "n", bun: "n", php: "n" },
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
          rust: ["p", "no parameters"],
          bun: ["p", "drops e2e bindings"],
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
          rust: ["p", "not stored"],
          bun: ["p", "not stored"],
          php: ["p", "does not cap values"],
        },
      },
      {
        name: "Shovel",
        detail: "Move messages from one broker to another",
        cells: {
          mq: ["y", "plugin"],
          rust: ["p", "not stored"],
          bun: ["p", "not stored"],
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
          rust: ["p", "refuses publish"],
          bun: ["p", "reported only"],
          php: "n",
        },
      },
      {
        name: "Command-line tool",
        detail: "rabbitmqctl and friends",
        cells: { mq: "y", rust: "n", bun: "n", php: "n" },
      },
      {
        name: "Message tracing",
        detail: "The firehose tracer",
        cells: { mq: "y", rust: "n", bun: "n", php: "n" },
      },
      {
        name: "Event exchange",
        detail: "Broker events published as messages",
        cells: { mq: ["y", "plugin"], rust: "n", bun: "n", php: "n" },
      },
      {
        name: "Feature flags",
        detail: "Turn on new behaviour across a cluster",
        cells: {
          mq: "y",
          rust: ["p", "two fixed flags"],
          bun: ["p", "two fixed flags"],
          php: ["p", "not read"],
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
          rust: ["p", "classic homes differ"],
          bun: ["p", "classic homes differ"],
          php: ["p", "classic homes differ"],
        },
      },
      {
        name: "Raft consensus",
        detail: "Elections and terms for replicated state",
        cells: { mq: "y", rust: "n", bun: "n", php: "n" },
      },
      {
        name: "Replicated metadata store",
        detail: "Khepri in RabbitMQ 4.3",
        cells: {
          mq: "y",
          rust: ["p", "push, no consensus"],
          bun: ["p", "push, no consensus"],
          php: ["n", "merged snapshots"],
        },
      },
      {
        name: "Peer discovery",
        detail: "Kubernetes, Consul, etcd, DNS, AWS",
        cells: { mq: "y", rust: "n", bun: "n", php: "n" },
      },
      {
        name: "Rolling upgrades",
        detail: "Upgrade one node at a time",
        cells: { mq: "y", rust: "n", bun: "n", php: "n" },
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

export function tally(broker: Broker) {
  const cells = featureGroups.flatMap((group) => group.rows.map((row) => level(row.cells[broker])));
  return {
    y: cells.filter((value) => value === "y").length,
    p: cells.filter((value) => value === "p").length,
    total: cells.length,
  };
}
