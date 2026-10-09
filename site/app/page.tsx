import Link from "next/link";
import { Bars } from "./bars";
import { formatRate, formatTimes, GITHUB, paced, scale } from "./bench";
import { FeatureTease } from "./feature-table";

export default function Page() {
  const rabbit = paced[0].rate;
  const figures = paced.slice(1);
  return (
    <main id="content">
      <section className="hero">
        <div className="wrap hero-layout">
          <div className="hero-copy">
            <p className="kicker">Open source message brokers</p>
            <h1>One protocol.<br /><em>Three engines.</em></h1>
            <p className="lede">
              RabbitMQ features, implemented in Rust, Bun and PHP. Compare their
              compatibility, explore the tradeoffs, and benchmark them side by side.
            </p>
            <div className="actions">
              <Link className="btn" href="/features">Explore compatibility <span aria-hidden="true">↗</span></Link>
              <a className="btn ghost" href={GITHUB}>View on GitHub</a>
            </div>
            <ul className="protocols" aria-label="Supported client protocols">
              {["AMQP 0-9-1", "AMQP 1.0", "MQTT", "STOMP", "Streams"].map((protocol) => <li key={protocol}>{protocol}</li>)}
            </ul>
          </div>
          <div className="wire-map" aria-label="One AMQP client protocol with three different broker storage engines">
            <div className="wire-map-heading"><span className="status-dot" aria-hidden="true" /><span>Built to speak the same language</span><span className="wire-version">AMQP</span></div>
            <div className="wire-client"><span className="wire-symbol" aria-hidden="true">&gt;_</span><div><strong>Your application</strong><span>Use your existing AMQP client</span></div></div>
            <div className="wire-connector" aria-hidden="true"><span>one client protocol</span></div>
            <div className="wire-engines">
              {[{ name: "Rust", store: "Write-ahead log", tone: "rust", number: "01" }, { name: "Bun", store: "SQLite", tone: "bun", number: "02" }, { name: "PHP", store: "Append-only log", tone: "php", number: "03" }].map((engine) => (
                <div key={engine.name} className={`wire-engine tone-${engine.tone}`}><span className="wire-number">{engine.number}</span><strong><span className="dot" aria-hidden="true" />{engine.name}</strong><span>{engine.store}</span></div>
              ))}
            </div>
            <div className="wire-map-foot"><span>Different stacks. Measured together.</span><Link href="/benchmark">See the results <span aria-hidden="true">↗</span></Link></div>
          </div>
        </div>
      </section>

      <section className="score-section">
        <div className="wrap">
          <div className="panel scoreboard">
            <div>
              <p className="panel-title">The benchmark · one core</p>
              <ul className="figures" aria-label="Kept rate versus RabbitMQ">
                {figures.map((row) => (
                  <li key={row.name} className={`tone-${row.tone}`}>
                    <span>{row.name}</span>
                    <strong>{formatTimes(row.rate, rabbit)}×</strong>
                  </li>
                ))}
              </ul>
              <p className="note">
                Versus RabbitMQ 4.3, on one core. QueueForge&apos;s durable confirm returns after
                the covering fsync. RabbitMQ 4.3 classic confirms before its flush.
              </p>
            </div>
            <div className="chart">
              <p className="chart-title">Kept messages/s</p>
              <Bars rows={paced} unit="/s" base={rabbit} />
              <p className="caption">
                128 confirms in flight. Durable 256-byte body. Mac client through the published
                port, 1 CPU / 512 MiB, measured 2026-10-09. A step counts only when confirms and
                acks both reach 95% of the offer.
              </p>
            </div>
          </div>
        </div>
      </section>

      <section className="band">
        <div className="wrap split-proof">
          <div>
            <p className="kicker">At four cores</p>
            <h2>Bun, Rust and PHP follow the cores when the queues are separate.</h2>
            <p className="section-lead">
              Unpaced, 16 queues, 4 CPU / 4 GiB, measured 2026-10-09. Rust is{" "}
              {formatTimes(scale[1].rate, scale[0].rate)}× RabbitMQ. Bun, one process per core, is{" "}
              {formatTimes(scale[2].rate, scale[0].rate)}× RabbitMQ and{" "}
              {formatTimes(scale[2].rate, scale[1].rate)}× Rust. Confirms and deliveries both stay
              near {formatRate(scale[2].rate)}/s. PHP, one process per core, is{" "}
              {formatTimes(scale[3].rate, scale[0].rate)}× RabbitMQ.
            </p>
            <p className="note">
              This is a different run from the chart above: a full window, inside the Docker
              network, measured for 8 seconds. Not the paced score. One queue still lives on one
              process, so one connection does not speed up.
            </p>
          </div>
          <div className="panel chart">
            <p className="chart-title">16 queues, confirm/s</p>
            <Bars rows={scale} unit="/s" base={scale[0].rate} />
          </div>
        </div>
      </section>

      <FeatureTease />

      <section className="rule">
        <div className="wrap about-tease">
          <div>
            <p className="kicker">The project</p>
            <h2>Three processes. One protocol.</h2>
          </div>
          <div>
            <p>
              The Rust broker keeps messages in a write-ahead log. The Bun broker keeps them in
              SQLite. The PHP broker keeps them in an append-only log. A client speaks AMQP 0-9-1
              to any of them, and the same member list can mix the binaries: Rust and Bun members
              run one Raft log together for metadata and quorum queues. The storage layers
              differ on purpose; what a client, a peer, or the management UI can observe does not.
            </p>
            <p>
              <Link href="/about">About QueueForge</Link>
            </p>
          </div>
        </div>
      </section>
    </main>
  );
}
