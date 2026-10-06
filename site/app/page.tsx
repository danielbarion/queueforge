import Link from "next/link";
import { Bars } from "./bars";
import { formatTimes, GITHUB, paced, scale } from "./bench";

export default function Page() {
  const rabbit = paced[0].rate;
  return (
    <main id="content">
      <section className="hero wrap">
        <div>
          <p className="kicker">Open source · AMQP 0-9-1</p>
          <h1>An AMQP broker in Rust, Bun, and PHP.</h1>
          <p className="lede">
            Three processes, one client protocol. You run QueueForge yourself. There is no account
            and no hosted broker.
          </p>
          <div className="figures" aria-label="Kept rate versus RabbitMQ">
            <div>
              <span>Rust</span>
              <strong>{formatTimes(paced[1].rate, rabbit)}×</strong>
            </div>
            <div>
              <span>Bun</span>
              <strong>{formatTimes(paced[2].rate, rabbit)}×</strong>
            </div>
            <div>
              <span>PHP</span>
              <strong>{formatTimes(paced[3].rate, rabbit)}×</strong>
            </div>
          </div>
          <p className="note">
            Versus RabbitMQ 4.3, on one core. QueueForge&apos;s durable confirm returns after the
            covering fsync. RabbitMQ 4.3 classic confirms before its flush.
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
        <div className="chart">
          <p className="chart-title">Kept messages/s</p>
          <Bars rows={paced} unit="/s" />
          <p className="caption">
            128 confirms in flight. Durable 256-byte body. Mac client through the published port.
            RabbitMQ, Rust and Bun on 2026-10-05; PHP on 2026-10-06. A step counts only when
            confirms and acks both reach 95% of the offer.
          </p>
        </div>
      </section>

      <section className="band">
        <div className="wrap split-proof">
          <div>
            <p className="kicker">At four cores</p>
            <h2>Rust follows the cores. Bun stays on one.</h2>
            <p className="section-lead">
              Unpaced, 16 queues, 4 CPU / 4 GiB. Rust is {formatTimes(scale[1].rate, scale[0].rate)}×
              RabbitMQ on this shape. Bun stays near one core, just above RabbitMQ. PHP, measured
              the next day on the same shape, is {formatTimes(scale[3].rate, scale[0].rate)}× and
              stays near one core. Giving the same CPU count more RAM did not raise the rate.
            </p>
            <p className="note">
              This is a different run from the chart above: a full window, inside the Docker
              network, measured for 8 seconds. Not the paced score.
            </p>
          </div>
          <div className="chart">
            <p className="chart-title">16 queues, confirm/s</p>
            <Bars rows={scale} unit="/s" />
          </div>
        </div>
      </section>

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
              to any of them, and the same member list can mix the binaries.
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
