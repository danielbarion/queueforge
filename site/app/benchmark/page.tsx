import type { Metadata } from "next";
import { Bars } from "../bars";
import {
  BENCH_MD,
  formatRate,
  formatTimes,
  loadRows,
  paced,
  scale,
  scenarioRows,
  type LoadRow,
  type Tone,
} from "../bench";
import { ScenarioTable } from "../scenario-table";

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
    "QueueForge versus RabbitMQ 4.3. Paced kept rates, 16 application scenarios with RabbitMQ PerfTest, and an unpaced load sweep across five container sizes.",
};

export default function BenchmarkPage() {
  const rabbit = paced[0].rate;
  return (
    <main id="content">
      <section className="page-intro">
        <div className="wrap">
          <p className="kicker">Benchmark · 2026-10-09</p>
          <h1>Measured against RabbitMQ 4.3.</h1>
          <p className="lede">
            QueueForge waits for the covering fsync. RabbitMQ 4.3 classic confirms before its
            flush. These are not the same moment. The rates below are not a slogan, and the
            scenarios where QueueForge loses are in the table too.
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
              CPU / 512 MiB, Mac client through the published port, measured 2026-10-09. Same
              client and scenarios as the 2026-10-05 run. A step counts only when confirms and acks
              both reach 95% of the offer.
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
              body, measure 8 s, 2026-10-09. Rust is{" "}
              {formatTimes(scale[1].rate, scale[0].rate)}× RabbitMQ here. Bun is{" "}
              {formatTimes(scale[2].rate, scale[0].rate)}× RabbitMQ and{" "}
              {formatTimes(scale[2].rate, scale[1].rate)}× Rust, and consume/s matches confirm/s.
              PHP, one process per core with each queue on its home process, is{" "}
              {formatTimes(scale[3].rate, scale[0].rate)}× RabbitMQ.
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
          <h2>Application scenarios.</h2>
          <p className="section-lead">
            RabbitMQ&apos;s own load tool, PerfTest 2.25, against each broker in turn: a fresh
            container per cell, the client on three other CPUs, 25 s with the first 5 s dropped.
            Rates are delivered messages per second unless the row says otherwise; latency rows are
            the p99 from publish to delivery at a fixed offered rate. A failed cell names what
            stopped it. Highlighted: the best broker in that row.
          </p>
          <h3 className="table-heading">1 CPU / 1 GiB</h3>
          <ScenarioTable rows={scenarioRows.filter((row) => row.size === "1 CPU / 1 GiB")} />
          <h3 className="table-heading">4 CPU / 4 GiB</h3>
          <ScenarioTable rows={scenarioRows.filter((row) => row.size === "4 CPU / 4 GiB")} />
          <p className="caption">
            RabbitMQ confirms before its flush; QueueForge confirms after the fsync. Large bodies
            are where that costs QueueForge the most. Backlog is fill / drain msg/s for 300,000
            durable 1 KiB messages. Memory peaks and every caveat are in{" "}
            <a href={BENCH_MD}>BENCHMARK.md</a>.
          </p>
        </div>
      </section>

      <section className="band">
        <div className="wrap">
          <h2>Every size.</h2>
          <p className="section-lead">
            One row per broker and container. 1 conn and 16 queues are confirm/s. 16 conn is
            consume/s on one shared queue, the rate that does not grow it. Connections are a
            separate login sweep: the last hold that stayed up and passed the probe. Message rates
            and that hold were not measured together. There is no 1 CPU / 1 GiB login cell.
          </p>
          <div className="panel table-panel bench-table">
            <div className="scroll" tabIndex={0} role="region" aria-label="Benchmark results">
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
            A PHP cell marked no delivery stopped on 404 NOT_FOUND for its queue: with more than
            one CPU, a PHP connection can reach a process that has not seen the declare yet.
          </p>
          <p className="caption">
            Bun at 4 CPU held 247,316 connections (4 GiB) and 260,000 (8 GiB). Rust at 4 CPU / 8
            GiB held 95,000, RabbitMQ 64,370; RabbitMQ asks stop near 65,527 sockets. PHP held
            1,000 to 2,500. Rust confirm/s on the shared queue, small size to large, was 59,836.4,
            116,388.6, 189,525.0, 244,256.8, and 239,698.4; consume/s is the rate in the table. The
            source table is <a href={BENCH_MD}>BENCHMARK.md</a>.
          </p>
        </div>
      </section>
    </main>
  );
}
