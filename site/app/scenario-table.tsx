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

function memoryText(memory: ScenarioRow["memory"][Tone]) {
  const format = (value: number | null) => value == null ? "–" : value.toLocaleString("en-US", { maximumFractionDigits: 0 });
  return `${format(memory.anonymous)} / ${format(memory.cgroup)} MiB`;
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
            <col className="score-column" />
            {COLUMNS.map((c) => <col className="broker-column" key={c.tone} />)}
          </colgroup>
          <thead>
            <tr>
              <th scope="col">Scenario</th>
              <th scope="col">Score</th>
              {COLUMNS.map((c) => (
                <th scope="col" key={c.tone}>
                  <span className={`app tone-${c.tone}`}>
                    <span className="dot" aria-hidden="true" />
                    {c.name}
                  </span>
                </th>
              ))}
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
                    const text = row.text[c.tone];
                    return (
                      <td key={c.tone} className={v == null ? "na" : v === top ? "best" : undefined}>
                        <span>{text}</span>
                        <span className="scenario-memory" title="Anonymous / total cgroup memory peaks; total includes file cache. Sampled once per second.">
                          {memoryText(row.memory[c.tone])}
                        </span>
                      </td>
                    );
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
