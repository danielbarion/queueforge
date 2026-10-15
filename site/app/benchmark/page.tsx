import type { Metadata } from "next";
import { Bars } from "../bars";
import {
  BENCH_MD,
  diskCeiling,
  formatMemory,
  formatRate,
  formatTimes,
  loadRows,
  MEASURED,
  paced,
  parseMemory,
  scale,
  scenarioAverages,
  scenarioRows,
  type Bar,
  type LoadRow,
  type Tone,
} from "../bench";
import { DiskCeilingTable, IoTable, MemoryTable, ScenarioTable } from "../scenario-table";
import { SizeCharts } from "../size-chart";

const TONES: Record<string, Tone> = { RabbitMQ: "mq", Rust: "rust", Bun: "bun", PHP: "php" };
const RATES = ["one", "shared", "spread"] as const;

/** A harness size string with a consistent unit, or the text as it was ("n/a"). */
function memoryText(text: string) {
  const mib = parseMemory(text);
  return mib == null ? text : formatMemory(mib);
}

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
        {formatRate(rate)} msg/s
        <sup>b</sup>
      </td>
    );
  }
  return <td className={rate === best ? "best" : undefined}>{formatRate(rate)} msg/s</td>;
}

export const metadata: Metadata = {
  title: "Benchmark",
  description:
    "QueueForge versus RabbitMQ 4.3. Paced kept rates, 16 application scenarios with RabbitMQ PerfTest, and an unpaced load sweep across five container sizes.",
};

export default function BenchmarkPage() {
  const rabbit = paced[0].rate;
  const averages = scenarioAverages(scenarioRows);
  const ioMeasured = scenarioRows.some((row) => row.io && Object.values(row.io).some((io) => io.disk != null));
  const score = Object.fromEntries(averages.map((row) => [row.tone, row])) as Record<Tone, (typeof averages)[number]>;
  const averageBars: Bar[] = averages.map((row) => ({
    name: row.name,
    rate: row.mean,
    tone: row.tone,
    note: row.tone === "mq" ? `${row.used} scenarios` : `${row.used} of ${row.possible}`,
  }));
  return (
    <main id="content">
      <section className="page-intro">
        <div className="wrap">
          <p className="kicker">Benchmark · {MEASURED}</p>
          <h1>Measured against RabbitMQ 4.3.</h1>
          <p className="lede">
            QueueForge waits for the covering fsync. A RabbitMQ 4.3 classic confirm follows the
            batch write, or a consumer ack if that happens first, and classic does not fsync before
            the confirm. The figure below is the average across scenarios, not one of them. The
            rows where QueueForge loses stay in the table. Rust, Bun and PHP are the 2026-10-15
            run; RabbitMQ is the 2026-10-12 run, with the same image.
          </p>
        </div>
      </section>

      <section className="score-section">
        <div className="wrap split-proof">
          <div>
            <h2>
              {formatTimes(score.rust.mean, 1)}× Rust. {formatTimes(score.bun.mean, 1)}× Bun.{" "}
              {formatTimes(score.php.mean, 1)}× PHP.
            </h2>
            <p className="section-lead">
              Geometric mean of each finished application scenario divided by RabbitMQ. A lower
              latency counts as faster. A missing cell is left out, not scored as zero. RabbitMQ
              has a number on {score.mq.used} scenarios; Rust is scored on {score.rust.used}, Bun on{" "}
              {score.bun.used} and PHP on {score.php.used} of them. The durable 256-byte paced score, 128 confirms in flight, is separate:{" "}
              {formatTimes(paced[1].rate, rabbit)}× Rust, {formatTimes(paced[2].rate, rabbit)}× Bun,{" "}
              {formatTimes(paced[3].rate, rabbit)}× PHP.
            </p>
          </div>
          <div className="panel chart">
            <p className="chart-title">Average, × RabbitMQ</p>
            <Bars rows={averageBars} unit="×" />
          </div>
        </div>
      </section>

      <section className="band">
        <div className="wrap">
          <h2>Across container sizes.</h2>
          <p className="section-lead">
            Unpaced message rate at each container in the load sweep. 16 connections is consume/s,
            the rate that does not grow the queue. A gap is a cell that did not deliver, not a zero.
            The numbers are the same ones in the table further down. Bun&apos;s points are the median of
            three 8 s measurements, run in rounds over the whole sweep, for Rust, Bun and PHP; Bun&apos;s
            4 CPU / 8 GiB one-connection point ranged from 68k to 324k msg/s across those three, so
            one sample could be off by 2× or more. RabbitMQ is one 8 s measurement from 2026-10-12. The
            application scenarios below repeat every cell.
          </p>
          <SizeCharts rows={loadRows} />
        </div>
      </section>

      <section className="band">
        <div className="wrap split-proof">
          <div>
            <h2>Sixteen queues, four cores.</h2>
            <p className="section-lead">
              Unpaced confirm/s inside the Docker network. 4 CPU / 4 GiB, durable classic, 256-byte
              body, measure 8 s, {MEASURED}. Rust is{" "}
              {formatTimes(scale[1].rate, scale[0].rate)}× RabbitMQ here. Bun is{" "}
              {formatTimes(scale[2].rate, scale[0].rate)}× RabbitMQ and{" "}
              {formatTimes(scale[2].rate, scale[1].rate)}× Rust, and consume/s matches confirm/s.
              PHP, which runs one process unless told otherwise, is{" "}
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
            container per cell, the client on three other CPUs, 25 s with the first 5 s dropped,
            two repeats per cell. Rates are delivered messages per second unless the row says
            otherwise; latency rows are the p99 from publish to delivery at a fixed offered rate. A
            failed cell names what stopped it. Highlighted: the best broker in that row.
          </p>
          <h3 className="table-heading">1 CPU / 1 GiB</h3>
          <ScenarioTable rows={scenarioRows.filter((row) => row.size === "1 CPU / 1 GiB")} />
          <h3 className="table-heading">4 CPU / 4 GiB</h3>
          <ScenarioTable rows={scenarioRows.filter((row) => row.size === "4 CPU / 4 GiB")} />
          <p className="caption">
            RabbitMQ classic confirms after the batch write, or after a consumer ack if that
            happens first, and does not fsync before the confirm. QueueForge confirms after the
            fsync. Large bodies are where that costs QueueForge the most. The two quorum-queue rows
            repeat the large-message cells on the queue type that RabbitMQ also fsyncs before the
            confirm, which is the like-for-like durable comparison. Backlog is the fill and
            drain rate for 300,000 durable 1 KiB messages. Each score is the median of its runs;
            the range across runs is in BENCHMARK.md. Memory for the same cells is below. Every
            caveat and raw result is in{" "}
            <a href={BENCH_MD}>BENCHMARK.md</a>.
          </p>
        </div>
      </section>

      <section className="rule">
        <div className="wrap">
          <h2>Memory.</h2>
          <p className="section-lead">
            The peak memory of each scenario cell above, sampled once per second. With repeats,
            each value is the median of the runs&apos; peaks. It is the total Linux charges to the
            container: the broker&apos;s processes plus the page cache, the kernel&apos;s copy of the
            files the broker has written (its message log).
          </p>
          <p className="caption">
            The cache grows with how much the broker has written, not with what it still holds, and
            the kernel reclaims it as soon as anything needs the memory. A durable broker that
            writes fast can sit at its 1 GiB limit without running short, which is why the rows are
            not ranked.
          </p>
          <MemoryTable rows={scenarioRows} label="Peak container memory" />
        </div>
      </section>

      {(diskCeiling.length > 0 || ioMeasured) && (
        <section className="rule">
          <div className="wrap">
            <h2>Disk.</h2>
            <p className="section-lead">
              What the disk under the brokers can do, measured on its own: <code>dd</code> writes
              with an <code>fdatasync</code> after every block, in a 1 CPU / 1 GiB container on an
              anonymous volume, as the broker containers write. A durable confirm waits on such a
              sync, so the rate depends on how much each sync covers. The last row is one sync at
              the end, about what a broker that does not sync before the confirm can reach.
            </p>
            <DiskCeilingTable rows={diskCeiling} />
            <p className="caption">
              Docker Desktop&apos;s disk is a file on the host&apos;s SSD, so these rates move with
              the host; each row is the median of three runs, with their range.
            </p>
            <IoTable rows={scenarioRows} label="Disk writes and fsyncs" />
            {ioMeasured && (
              <p className="caption">
                Per scenario cell, over its measured window: what the broker container wrote to the
                disk, and the fsyncs its metrics page counted with their mean duration. RabbitMQ
                counts every file sync of the node; QueueForge counts its durable-log commits.
              </p>
            )}
          </div>
        </section>
      )}

      <section className="band">
        <div className="wrap">
          <h2>Every size.</h2>
          <p className="section-lead">
            One row per broker and container. 1 conn and 16 queues are confirm/s. 16 conn is
            consume/s on one shared queue, the rate that does not grow it. Connections are a
            separate login sweep: the last hold that stayed up and passed the probe. Message rates
            and that hold were not measured together. Memory is the container&apos;s, page cache
            included, at the end of the 16-queue measure; login memory is <code>docker stats</code>{" "}
            usage at that hold. There is no 1 CPU / 1 GiB login cell.
          </p>
          <div className="panel table-panel bench-table">
            <div className="scroll" tabIndex={0} role="region" aria-label="Benchmark results">
              <table>
                <thead>
                  <tr>
                    <th scope="col">App</th>
                    <th scope="col">1 conn</th>
                    <th scope="col">16 conn</th>
                    <th scope="col">16 queues</th>
                    <th scope="col">Connections</th>
                    <th scope="col">Memory</th>
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
                        <td>{memoryText(row.mib)}</td>
                        <td className={row.login === "n/a" ? "na" : undefined}>{memoryText(row.login)}</td>
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
            A cell marked no delivery moved no messages in its measure window. PHP runs one process
            unless <code>QUEUEFORGE_CORES</code> is set, so its rates do not grow with the container.
          </p>
          <p className="caption">
            Bun at 4 CPU held 259,195 connections (4 GiB) and 260,000 (8 GiB). Rust at 4 CPU / 8
            GiB held 95,000, RabbitMQ 62,065; RabbitMQ asks stop near 65,527 sockets. PHP held
            28,000 at 1 CPU and 40,000 at 2 and 4 CPU; at 4 CPU / 8 GiB its 40,000 logins took longer
            than the login window, so it is listed at 20,000. Rust confirm/s on the shared queue,
            small size to large, was 100,552.1, 105,525.9, 183,290.0, 245,078.6, and 216,175.0;
            consume/s is the rate in the table. The source table is{" "}
            <a href={BENCH_MD}>BENCHMARK.md</a>.
          </p>
        </div>
      </section>
    </main>
  );
}
