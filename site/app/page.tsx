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
        <div className="wrap">
          <p className="kicker">Open source · AMQP 0-9-1 and 1.0 · MQTT · STOMP · Streams</p>
          <h1>A RabbitMQ-compatible broker in Rust, Bun, and PHP.</h1>
          <p className="lede">
            Three processes, one client protocol. You run QueueForge yourself. There is no account
            and no hosted broker.
          </p>
          <div className="actions">
            <a className="btn" href={GITHUB}>
              View on GitHub
            </a>
            <Link className="btn ghost" href="/benchmark">
              See the benchmark
            </Link>
          </div>
        </div>
      </section>

      <section className="score-section">
        <div className="wrap">
          <div className="panel scoreboard">
            <div>
              <p className="panel-title">Kept rate versus RabbitMQ 4.3</p>
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
                port, 1 CPU / 512 MiB, measured 2026-10-08. A step counts only when confirms and
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
              Unpaced, 16 queues, 4 CPU / 4 GiB, measured 2026-10-08. Rust is{" "}
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
