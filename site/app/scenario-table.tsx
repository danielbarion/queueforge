import { type ScenarioRow, type Tone } from "./bench";

const COLUMNS: Array<{ tone: Tone; name: string }> = [
  { tone: "mq", name: "RabbitMQ" },
  { tone: "rust", name: "Rust" },
  { tone: "bun", name: "Bun" },
  { tone: "php", name: "PHP" },
];

const UNIT: Record<ScenarioRow["kind"], string> = {
  rate: "msg/s",
  bytes: "MB/s",
  latency: "p99 ms",
};

const MEMORY_TITLE = "Peak memory of the broker's own processes (anonymous memory), sampled once per second.";
const CACHE_TITLE =
  "Peak memory of the whole container, which adds the page cache of the files the broker writes. The kernel gives that cache back under pressure.";

function mib(value: number | null) {
  return value == null ? "–" : value.toLocaleString("en-US", { maximumFractionDigits: 0 });
}

/** The best value in a row: highest rate, or lowest p99 latency. */
function best(row: ScenarioRow): number | null {
  const vals = COLUMNS.map((c) => row.values[c.tone]).filter((v): v is number => v != null);
  if (!vals.length) return null;
  return row.kind === "latency" ? Math.min(...vals) : Math.max(...vals);
}

/** One table of PerfTest scenarios for one container size. */
export function ScenarioTable({ rows }: { rows: ScenarioRow[] }) {
  return (
    <div className="panel table-panel bench-table scenario-table">
      <div className="scroll">
        <table>
          <colgroup>
            <col className="scenario-column" />
            <col className="unit-column" />
            {COLUMNS.map((c) => [
              <col className="score-column" key={`${c.tone}-score`} />,
              <col className="memory-column" key={`${c.tone}-memory`} />,
              <col className="memory-column" key={`${c.tone}-cache`} />,
            ])}
          </colgroup>
          <thead>
            <tr>
              <th scope="col" rowSpan={2}>Scenario</th>
              <th scope="col" rowSpan={2}>Unit</th>
              {COLUMNS.map((c) => (
                <th scope="colgroup" colSpan={3} key={c.tone} className="broker-group">
                  <span className={`app tone-${c.tone}`}>
                    <span className="dot" aria-hidden="true" />
                    {c.name}
                  </span>
                </th>
              ))}
            </tr>
            <tr className="sub">
              {COLUMNS.map((c) => [
                <th scope="col" key={`${c.tone}-score`} className="broker-group">Score</th>,
                <th scope="col" key={`${c.tone}-memory`} title={MEMORY_TITLE}>Memory MiB</th>,
                <th scope="col" key={`${c.tone}-cache`} title={CACHE_TITLE}>With cache MiB</th>,
              ])}
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => {
              const top = best(row);
              return (
                <tr key={`${row.size}-${row.id}`}>
                  <th scope="row" className="scenario">
                    <span className="scenario-title">{row.title}</span>
                    <span className="scenario-real">{row.real}</span>
                  </th>
                  <td className="na">{UNIT[row.kind]}</td>
                  {COLUMNS.map((c) => {
                    const v = row.values[c.tone];
                    const spread = row.spread[c.tone];
                    const memory = row.memory[c.tone];
                    return [
                      <td key={`${c.tone}-score`} className={["broker-group", v == null ? "na" : v === top ? "best" : ""].join(" ").trim()}>
                        <span>{row.text[c.tone]}</span>
                        {spread ? <span className="scenario-spread">{spread}</span> : null}
                      </td>,
                      <td key={`${c.tone}-memory`} className="memory" title={MEMORY_TITLE}>{mib(memory.anonymous)}</td>,
                      <td key={`${c.tone}-cache`} className="memory cache" title={CACHE_TITLE}>{mib(memory.cgroup)}</td>,
                    ];
                  })}
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
