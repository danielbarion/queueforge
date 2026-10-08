import type { Metadata } from "next";
import { CopyCommand } from "../copy-command";

export const metadata: Metadata = {
  title: "About",
  description:
    "QueueForge is an open source AMQP 0-9-1 broker. Rust, Bun and PHP processes share one client protocol and can sit in the same cluster.",
};

const RUST = `cd rust
cargo build --release -p queueforge-broker
./target/release/queueforge \\
  --config configs/queueforge.example.toml \\
  --dev-bootstrap`;

const BUN = `cd bun
bun install
bun run start -- \\
  --config config.example.toml --dev-bootstrap`;

const PHP = `cd php
php bin/queueforge \\
  --config config.example.toml --dev-bootstrap`;

const BINDS = [
  ["amqp", "0.0.0.0:5672"],
  ["management", "0.0.0.0:15672"],
  ["metrics", "127.0.0.1:15692"],
  ["health", "GET /healthz"],
  ["ready", "GET /readyz"],
  ["confirm", "after the fsync"],
  ["cluster", "protocol v1"],
  ["php amqp", "127.0.0.1:5675"],
] as const;

export default function AboutPage() {
  return (
    <main id="content">
      <section className="page-intro">
        <div className="wrap">
          <p className="kicker">About</p>
          <h1>Three brokers. The same wire.</h1>
          <p className="lede">
            QueueForge is an open source AMQP 0-9-1 broker. A Rust process, a Bun process and a
            PHP process expose the same client-visible behavior. You run them yourself. There is
            no account and no hosted service.
          </p>
        </div>
      </section>

      <section className="rule">
        <div className="wrap">
          <h2>Same protocol. Three engines.</h2>
          <p className="section-lead">
            Each process has its own data directory, and its own AMQP, management, metrics, and
            cluster ports. <code>--dev-bootstrap</code> creates the first user. The password is in
            the README, not on this page.
          </p>
          <div className="engines">
            <article className="panel engine tone-rust">
              <div>
                <h3>
                  <span className="dot" aria-hidden="true" />
                  Rust
                </h3>
                <span className="store">write-ahead log</span>
                <p>
                  Tokio. Metadata in redb. Messages in a write-ahead log. Management HTTP and the
                  React UI are compiled into the binary.
                </p>
              </div>
              <CopyCommand text={RUST} />
            </article>
            <article className="panel engine tone-bun">
              <div>
                <h3>
                  <span className="dot" aria-hidden="true" />
                  Bun
                </h3>
                <span className="store">SQLite</span>
                <p>
                  AMQP on a Bun TCP listener. More than one granted core starts one child per core.
                  The parent hands each connection to the child that owns the queue. Messages stay in
                  that child&apos;s SQLite. The public metrics port adds up{" "}
                  <code>queueforge_confirm_before_fsync_total</code> across the children. On the
                  2026-10-06 16-queue run that counter was 0 on every cell. The other metric lines,
                  and the management page, are the first child only.
                </p>
              </div>
              <CopyCommand text={BUN} />
            </article>
            <article className="panel engine tone-php">
              <div>
                <h3>
                  <span className="dot" aria-hidden="true" />
                  PHP
                </h3>
                <span className="store">append-only log</span>
                <p>
                  One process on one core. More cores start one child per core, and the parent hands
                  each connection to a child. Messages stay on that child. The example config listens
                  on 5675, so it can run alongside the other two.
                </p>
              </div>
              <CopyCommand text={PHP} />
            </article>
          </div>
        </div>
      </section>

      <section className="rule">
        <div className="wrap">
          <h2>One member list. Any binary.</h2>
          <p className="section-lead">
            Leave <code>[cluster].members</code> empty for a single node. For several processes,
            put the same list on every node, including itself. All three speak cluster protocol
            version 1, so a member list can mix Rust, Bun and PHP. A classic queue is homed on
            one node by hash and operations reaching any other node are forwarded there; a quorum
            queue confirms only once a durable majority has the message.
          </p>
          <ol className="members">
            <li className="tone-rust">
              <span>node a</span>Rust
            </li>
            <li className="tone-bun">
              <span>node b</span>Bun
            </li>
            <li className="tone-php">
              <span>node c</span>PHP
            </li>
          </ol>
          <div className="pair">
            <div className="panel">
              <h3>Classic</h3>
              <p>
                One home node, a hash of the vhost and the queue name. Peers forward operations
                there. The messages stay in that node&apos;s local engine. A durable confirm
                returns when the interval fsync covers the append. Rust hashes differently from
                Bun and PHP, so a mixed list does not agree on classic homes.
              </p>
            </div>
            <div className="panel">
              <h3>Quorum</h3>
              <p>
                A durable quorum queue confirms a persistent publish after a majority of the
                members hold the body. Each member writes that body into its own store before it
                acks the peer. The client can publish to whichever member is up. The live leader is
                the lowest reachable member id. Quorum queues are homed where they are declared,
                so mixing binaries does not affect them.
              </p>
            </div>
          </div>
        </div>
      </section>

      <section className="rule">
        <div className="wrap wire">
          <div>
            <h2>What the client already speaks.</h2>
            <p className="section-lead">
              Single-node v0.1 is the AMQP 0-9-1 model: exchanges, queues, bindings,
              acknowledgements, durability. Not a new client protocol.
            </p>
            <dl className="spec">
              <div>
                <dt>Exchanges</dt>
                <dd>direct, fanout, topic, headers, and the default exchange</dd>
              </div>
              <div>
                <dt>Queues</dt>
                <dd>durable classic, quorum, priority, TTL, dead-letter, max length</dd>
              </div>
              <div>
                <dt>Client</dt>
                <dd>
                  publisher confirms, manual acks, prefetch. Prefetch 0 is unlimited, matching
                  RabbitMQ.
                </dd>
              </div>
              <div>
                <dt>Operations</dt>
                <dd>
                  management HTTP, Prometheus, definitions import and export. The Rust process
                  embeds the UI and serves TLS with rustls.
                </dd>
              </div>
            </dl>
          </div>
          <aside className="panel plate" aria-label="Default binds">
            <p className="panel-title">Example config binds</p>
            <dl>
              {BINDS.map(([term, value]) => (
                <div key={term}>
                  <dt>{term}</dt>
                  <dd>{value}</dd>
                </div>
              ))}
            </dl>
          </aside>
        </div>
      </section>
    </main>
  );
}
