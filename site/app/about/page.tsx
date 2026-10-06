import type { Metadata } from "next";
import { CopyCommand } from "../copy-command";

export const metadata: Metadata = {
  title: "About",
  description:
    "QueueForge is an open source AMQP 0-9-1 broker. Rust, Bun and PHP processes share one client protocol and can sit in the same cluster.",
};

const RUST = `cd rust
cargo build --release -p queueforge-broker
./target/release/queueforge --config configs/queueforge.example.toml --dev-bootstrap`;

const BUN = `cd bun
bun install
bun run start -- --config config.example.toml --dev-bootstrap`;

const BINDS = [
  ["amqp", "0.0.0.0:5672"],
  ["management", "0.0.0.0:15672"],
  ["metrics", "127.0.0.1:15692"],
  ["health", "GET /healthz"],
  ["ready", "GET /readyz"],
  ["confirm", "after the fsync"],
  ["cluster", "protocol v1"],
] as const;

export default function AboutPage() {
  return (
    <main id="content">
      <section className="wrap page-intro">
        <p className="kicker">About</p>
        <h1>Two brokers. The same wire.</h1>
        <p className="lede">
          QueueForge is an open source AMQP 0-9-1 broker. A Rust process and a Bun process expose
          the same client-visible behavior. You run them yourself. There is no account and no
          hosted service.
        </p>
      </section>

      <section className="rule">
        <div className="wrap">
          <h2>Same protocol. Two engines.</h2>
          <p className="section-lead">
            Each process has its own data directory, and its own AMQP, management, metrics, and
            cluster ports. <code>--dev-bootstrap</code> creates the first user. The password is in
            the README, not on this page.
          </p>
          <div className="split">
            <article className="proc">
              <h3>Rust</h3>
              <p>
                Tokio. Metadata in redb. Messages in a write-ahead log. Management HTTP and the
                React UI are compiled into the binary.
              </p>
              <CopyCommand text={RUST} />
            </article>
            <article className="proc">
              <h3>Bun</h3>
              <p>
                AMQP on a Bun TCP listener. Messages in SQLite. Management HTTP is Elysia. Same
                ports, unless the config says otherwise.
              </p>
              <CopyCommand text={BUN} />
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
            version 1, so a member list can mix Rust, Bun and PHP.
          </p>
          <ol className="members">
            <li>
              <span>node a</span>Rust
            </li>
            <li>
              <span>node b</span>Bun
            </li>
            <li>
              <span>node c</span>Rust
            </li>
          </ol>
          <div className="pair">
            <div>
              <h3>Classic</h3>
              <p>
                One home node, a hash of the vhost and the queue name. Peers forward operations
                there. The messages stay in that node&apos;s local engine. A durable confirm
                returns when the interval fsync covers the append.
              </p>
            </div>
            <div>
              <h3>Quorum</h3>
              <p>
                A durable quorum queue confirms a persistent publish after a majority of the
                members hold the body. Each member writes that body into its own store before it
                acks the peer. The client can publish to whichever member is up. The live leader is
                the lowest reachable member id.
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
          <aside className="plate" aria-label="Default binds">
            <p>Example config binds</p>
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
