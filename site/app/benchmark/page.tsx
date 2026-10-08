import type { Metadata } from "next";
import { Bars } from "../bars";
import {
  BENCH_MD,
  formatRate,
  formatTimes,
  loadRows,
  paced,
  scale,
  type LoadRow,
  type Tone,
} from "../bench";

const TONES: Record<string, Tone> = { RabbitMQ: "mq", Rust: "rust", Bun: "bun", PHP: "php" };
const RATES = ["one", "shared", "spread"] as const;

/** A rate that can be compared with the others in its column. */
function comparable(row: LoadRow, key: (typeof RATES)[number]) {
  const rate = row[key];
  return rate !== null && !row.caveats?.[key] ? rate : null;
}

function groups(rows: LoadRow[]) {
  const sizes = [...new Set(rows.map((row) => row.size))];
  return sizes.map((size) => {
    const members = rows.filter((row) => row.size === size);
    const best = Object.fromEntries(
      RATES.map((key) => [
        key,
        Math.max(...members.map((row) => comparable(row, key) ?? -Infinity)),
      ]),
    ) as Record<(typeof RATES)[number], number>;
    return { size, members, best };
  });
}

function RateCell({ row, field, best }: { row: LoadRow; field: (typeof RATES)[number]; best: number }) {
  const rate = row[field];
  const caveat = row.caveats?.[field];
  if (rate === null || caveat === "failed") {
    return (
      <td className="na">
        no delivery<sup>a</sup>
      </td>
    );
  }
  if (caveat === "split") {
    return (
      <td className="caveat">
        {formatRate(rate)}
        <sup>b</sup>
      </td>
    );
  }
  return <td className={rate === best ? "best" : undefined}>{formatRate(rate)}</td>;
}

export const metadata: Metadata = {
  title: "Benchmark",
  description:
    "QueueForge versus RabbitMQ 4.3. Paced kept rates, and an unpaced load sweep across five container sizes.",
};

export default function BenchmarkPage() {
  const rabbit = paced[0].rate;
  return (
    <main id="content">
      <section className="page-intro">
        <div className="wrap">
          <p className="kicker">Benchmark · 2026-10-08</p>
          <h1>Measured against RabbitMQ 4.3.</h1>
          <p className="lede">
            QueueForge waits for the covering fsync. RabbitMQ 4.3 classic confirms before its
            flush. These are not the same moment. The rates below are not a slogan.
          </p>
        </div>
      </section>

      <section className="score-section">
        <div className="wrap split-proof">
          <div>
            <h2>
              {formatTimes(paced[1].rate, rabbit)}× Rust. {formatTimes(paced[2].rate, rabbit)}× Bun.{" "}
              {formatTimes(paced[3].rate, rabbit)}× PHP.
            </h2>
            <p className="section-lead">
              Paced messages/s that stayed kept. Durable 256-byte body, 128 confirms in flight, 1
              CPU / 512 MiB, Mac client through the published port, measured 2026-10-08 from
              12:50Z (Rust rerun 13:43Z on a quiet host). Same client and scenarios as the
              2026-10-05 run. A step counts only when confirms and acks both reach 95% of the offer.
            </p>
          </div>
          <div className="panel chart">
            <p className="chart-title">Kept messages/s</p>
            <Bars rows={paced} unit="/s" base={rabbit} />
          </div>
        </div>
      </section>

      <section className="band">
        <div className="wrap split-proof">
          <div>
            <h2>Sixteen queues, four cores.</h2>
            <p className="section-lead">
              Unpaced confirm/s inside the Docker network. 4 CPU / 4 GiB, durable classic, 256-byte
              body, measure 8 s, from 2026-10-08T13:00:41Z. Rust is{" "}
              {formatTimes(scale[1].rate, scale[0].rate)}× RabbitMQ here. Bun is{" "}
              {formatTimes(scale[2].rate, scale[0].rate)}× RabbitMQ and{" "}
              {formatTimes(scale[2].rate, scale[1].rate)}× Rust, and consume/s matches confirm/s.
              PHP, one process per core with each queue on its home process, is{" "}
              {formatTimes(scale[3].rate, scale[0].rate)}× RabbitMQ. PHP rows are the 13:31Z rerun
              after the handoff fix.
            </p>
          </div>
          <div className="panel chart">
            <p className="chart-title">16 queues, confirm/s</p>
            <Bars rows={scale} unit="/s" base={scale[0].rate} />
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
            and that hold were not measured together. There is no 1 CPU / 1 GiB login cell. PHP
            above 1 CPU has no login hold yet.
          </p>
          <div className="panel table-panel bench-table">
            <div className="scroll">
              <table>
                <thead>
                  <tr>
                    <th scope="col">App</th>
                    <th scope="col">1 conn msg/s</th>
                    <th scope="col">16 conn msg/s</th>
                    <th scope="col">16 queues msg/s</th>
                    <th scope="col">Connections</th>
                    <th scope="col">MiB</th>
                    <th scope="col">Login memory</th>
                  </tr>
                </thead>
                {groups(loadRows).map(({ size, members, best }) => (
                  <tbody key={size}>
                    <tr className="group">
                      <th scope="rowgroup" colSpan={7}>
                        {size}
                      </th>
                    </tr>
                    {members.map((row) => (
                      <tr key={`${row.app}-${row.size}`}>
                        <td>
                          <span className={`app tone-${TONES[row.app]}`}>
                            <span className="dot" aria-hidden="true" />
                            {row.app}
                          </span>
                        </td>
                        {RATES.map((key) => (
                          <RateCell key={key} row={row} field={key} best={best[key]} />
                        ))}
                        <td className={row.connections === "n/a" ? "na" : undefined}>
                          {row.connections}
                        </td>
                        <td>{row.mib}</td>
                        <td className={row.login === "n/a" ? "na" : undefined}>{row.login}</td>
                      </tr>
                    ))}
                  </tbody>
                ))}
              </table>
            </div>
          </div>
          <p className="caption">
            Highlighted: the highest comparable message rate for that container size, per column.
          </p>
          <p className="caption">
            PHP with more than one CPU moves each connection to the process that is its
            queue&apos;s home. That move passes the socket over a Unix datagram, and PHP&apos;s
            receive cut every datagram to 8192 bytes, so a connection that arrived with a deep
            publish window was lost and reset. With the buffer fixed, every PHP cell is one
            queue on one process and is scored like the others.
          </p>
          <p className="caption">
            Bun at 4 CPU held 160,000 connections. Attempts of 320,000 were cut by the login
            window at 171,961 (4 GiB) and 174,334 (8 GiB). Rust at 4 CPU / 8 GiB held 100,000, and
            the probe was refused. 95,000 passed. RabbitMQ at 4 CPU / 8 GiB held 61,562. Larger
            asks stop at 65,527 sockets. Rust confirm/s on the shared queue, small size to large,
            was 106,618.1, 105,064.2, 192,043.2, 225,623.5, and 242,289.1; consume/s is the rate in the table. The source table is{" "}
            <a href={BENCH_MD}>BENCHMARK.md</a>.
          </p>
        </div>
      </section>
    </main>
  );
}
