import { formatMemory, formatScore, type DiskCeiling, type ScenarioRow, type Tone } from "./bench";

const COLUMNS: Array<{ tone: Tone; name: string }> = [
  { tone: "mq", name: "RabbitMQ" },
  { tone: "rust", name: "Rust" },
  { tone: "bun", name: "Bun" },
  { tone: "php", name: "PHP" },
];

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
          <tbody>
            {rows.map((row) => {
              const top = best(row);
              return (
                <tr key={`${row.size}-${row.id}`}>
                  <th scope="row" className="scenario">
                    <span className="scenario-title">{row.title}</span>
                    <span className="scenario-real">{row.real}</span>
                  </th>
                  {COLUMNS.map((c) => {
                    const v = row.values[c.tone];
                    return (
                      <td key={c.tone} className={v == null ? "na" : v === top ? "best" : undefined}>
                        {v == null ? row.text[c.tone] : formatScore(row.kind, row.text[c.tone])}
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
  const format = (value: number | null) => value == null ? "–" : formatMemory(value);
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

function fixed(value: number, digits: number) {
  return value.toLocaleString("en-US", { minimumFractionDigits: digits, maximumFractionDigits: digits });
}

/**
 * Disk writes and fsyncs over each cell's measured window, grouped by container
 * size. Rows appear only once a run has measured them.
 */
export function IoTable({ rows, label }: { rows: ScenarioRow[]; label: string }) {
  const measured = rows.filter((row) => row.io && COLUMNS.some((c) => row.io![c.tone].disk != null));
  if (!measured.length) return null;
  const sizes = [...new Set(measured.map((row) => row.size))];
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
              {measured.filter((row) => row.size === size).map((row) => (
                <tr key={`${size}-${row.id}`}>
                  <th scope="row" className="memory-scenario">{row.title}</th>
                  {COLUMNS.map((c) => {
                    const io = row.io![c.tone];
                    if (io.disk == null) return <td key={c.tone} className="na">–</td>;
                    return (
                      <td key={c.tone}>
                        {fixed(io.disk, 1)} MB/s
                        {io.syncs != null && (
                          <span className="io-syncs">
                            {fixed(io.syncs, 0)} fsync/s{io.syncMs != null ? `, ${fixed(io.syncMs, 2)} ms each` : ""}
                          </span>
                        )}
                      </td>
                    );
                  })}
                </tr>
              ))}
            </tbody>
          ))}
        </table>
      </div>
    </div>
  );
}

/** The disk's write+fdatasync ceiling per amount written between syncs. */
export function DiskCeilingTable({ rows }: { rows: DiskCeiling[] }) {
  if (!rows.length) return null;
  return (
    <div className="panel table-panel bench-table disk-table">
      <div className="scroll" tabIndex={0} role="region" aria-label="Disk write ceiling">
        <table>
          <thead>
            <tr>
              <th scope="col">Written per fsync</th>
              <th scope="col">Median</th>
              <th scope="col">Range</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={row.label}>
                <th scope="row" className="memory-scenario">{row.label}</th>
                <td>{fixed(row.mbS, 1)} MB/s</td>
                <td>{fixed(row.min, 1)}–{fixed(row.max, 1)} MB/s</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
