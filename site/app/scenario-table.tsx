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
              <th scope="col">Unit</th>
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
                  <td className="unit">{row.id === "backlog" ? "fill / drain msg/s" : UNIT[row.kind]}</td>
                  {COLUMNS.map((c) => {
                    const v = row.values[c.tone];
                    return (
                      <td key={c.tone} className={v == null ? "na" : v === top ? "best" : undefined}>
                        {row.text[c.tone]}
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

/**
 * Peak container memory per scenario and broker, grouped by container size.
 * Not ranked: the total includes page cache, which is the kernel's to reclaim.
 */
export function MemoryTable({ rows, label }: { rows: ScenarioRow[]; label: string }) {
  const sizes = [...new Set(rows.map((row) => row.size))];
  const format = (value: number | null) => value == null ? "–" : `${value.toLocaleString("en-US", { maximumFractionDigits: 0 })} MiB`;
  return (
    <div className="panel table-panel bench-table memory-table">
      <div className="scroll" tabIndex={0} role="region" aria-label={label}>
        <table>
          <colgroup>
            <col className="scenario-column" />
            {COLUMNS.map((c) => <col className="broker-column" key={c.tone} />)}
          </colgroup>
          <thead>
            <tr>
              <th scope="col">Scenario</th>
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
          {sizes.map((size) => (
            <tbody key={size}>
              <tr className="group">
                <th scope="rowgroup" colSpan={COLUMNS.length + 1}>{size}</th>
              </tr>
              {rows.filter((row) => row.size === size).map((row) => {
                const values = COLUMNS.map((c) => row.memory[c.tone]);
                return (
                  <tr key={`${size}-${row.id}`}>
                    <th scope="row" className="memory-scenario">{row.title}</th>
                    {COLUMNS.map((c, i) => (
                      <td key={c.tone} className={values[i] == null ? "na" : undefined}>
                        {format(values[i])}
                      </td>
                    ))}
                  </tr>
                );
              })}
            </tbody>
          ))}
        </table>
      </div>
    </div>
  );
}
