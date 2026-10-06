import type { Metadata } from "next";
import { Bars } from "../bars";
import { BENCH_MD, formatRate, formatTimes, loadRows, paced, scale } from "../bench";

export const metadata: Metadata = {
  title: "Benchmark",
  description:
    "QueueForge versus RabbitMQ 4.3. Paced kept rates, and an unpaced load sweep across five container sizes.",
};

export default function BenchmarkPage() {
  const rabbit = paced[0].rate;
  return (
    <main id="content">
      <section className="wrap page-intro">
        <p className="kicker">Benchmark · 2026-10-05</p>
        <h1>Measured against RabbitMQ 4.3.</h1>
        <p className="lede">
          QueueForge waits for the covering fsync. RabbitMQ 4.3 classic confirms before its flush.
          These are not the same moment. The rates below are not a slogan.
        </p>
      </section>

      <section className="rule">
        <div className="wrap split-proof">
          <div>
            <h2>
              {formatTimes(paced[1].rate, rabbit)}× Rust. {formatTimes(paced[2].rate, rabbit)}× Bun.
            </h2>
            <p className="section-lead">
              Paced messages/s that stayed kept. Durable 256-byte body, 128 confirms in flight, 1
              CPU / 512 MiB, Mac client through the published port, 2026-10-05T09:03:42Z. A step
              counts only when confirms and acks both reach 95% of the offer.
            </p>
          </div>
          <div className="chart">
            <p className="chart-title">Kept messages/s</p>
            <Bars rows={paced} unit="/s" />
          </div>
        </div>
      </section>

      <section className="band">
        <div className="wrap split-proof">
          <div>
            <h2>Sixteen queues, four cores.</h2>
            <p className="section-lead">
              Unpaced confirm/s inside the Docker network. 4 CPU / 4 GiB, durable classic, 256-byte
              body, measure 8 s. Rust and Bun are from 2026-10-05T21:29:06Z. PHP is from
              2026-10-06T12:05:24Z, same client and size. Rust is{" "}
              {formatTimes(scale[1].rate, scale[0].rate)}× RabbitMQ here. Bun is{" "}
              {formatTimes(scale[2].rate, scale[0].rate)}×. PHP is{" "}
              {formatTimes(scale[3].rate, scale[0].rate)}× and does not gain from the extra cores.
              Same CPU count with more RAM did not raise the rate. PHP was not in the paced ladder
              above. The login column has no PHP cell.
            </p>
          </div>
          <div className="chart">
            <p className="chart-title">16 queues, confirm/s</p>
            <Bars rows={scale} unit="/s" />
          </div>
        </div>
      </section>

      <section className="rule">
        <div className="wrap">
          <h2>Every size.</h2>
          <p className="section-lead">
            One row per broker and container. 1 conn and 16 queues are confirm/s. 16 conn is
            consume/s on one shared queue, the rate that does not grow it. Connections are a
            separate login sweep: the last hold that stayed up and passed the probe. Message rates
            and that hold were not measured together. There is no 1 CPU / 1 GiB login cell, and no
            PHP login cell.
          </p>
          <div className="scroll">
            <table>
              <thead>
                <tr>
                  <th scope="col">App</th>
                  <th scope="col">Size</th>
                  <th scope="col">1 conn msg/s</th>
                  <th scope="col">16 conn msg/s</th>
                  <th scope="col">16 queues msg/s</th>
                  <th scope="col">Connections</th>
                  <th scope="col">MiB</th>
                  <th scope="col">Login memory</th>
                </tr>
              </thead>
              <tbody>
                {loadRows.map((row) => (
                  <tr key={`${row.app}-${row.size}`}>
                    <td>{row.app}</td>
                    <td>{row.size}</td>
                    <td>{formatRate(row.one)}</td>
                    <td>{formatRate(row.shared)}</td>
                    <td>{formatRate(row.spread)}</td>
                    <td>{row.connections}</td>
                    <td>{row.mib}</td>
                    <td>{row.login}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <p className="caption">
            Bun at 4 CPU held 160,000 connections. Attempts of 320,000 were cut by the login
            window at 171,961 (4 GiB) and 174,334 (8 GiB). Rust at 4 CPU / 8 GiB held 100,000, and
            the probe was refused. 95,000 passed. RabbitMQ at 4 CPU / 8 GiB held 61,562. Larger
            asks stop at 65,527 sockets. Rust confirm/s on the shared queue, small size to large,
            was 104,653.0, 106,388.5, 163,821.6, 224,816.2, and 239,222.9. The source table is{" "}
            <a href={BENCH_MD}>BENCHMARK.md</a>.
          </p>
        </div>
      </section>
    </main>
  );
}
