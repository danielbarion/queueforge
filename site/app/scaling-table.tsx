import { formatTimes, type Tone } from "./bench";
import { scaling, type ScalingPoint } from "./scaling-data";

const BROKERS: Array<{ tone: "rust" | "bun" | "php"; name: string }> = [
  { tone: "rust", name: "Rust" },
  { tone: "bun", name: "Bun" },
  { tone: "php", name: "PHP" },
];

function whole(rate: number) {
  return Math.round(rate).toLocaleString("en-US");
}

/**
 * The growing load per broker: rows are CPU counts, cells the median rate
 * and its efficiency against 1 CPU, X(N) / (N X(1)). A failed repeat is
 * counted under the rate; a cell with none passed shows its count only.
 */
export function ScalingTable({ scenario }: { scenario: "weak-t" | "weak" }) {
  const shown = BROKERS.filter((b) => scaling[b.tone][scenario]?.length);
  const sizes = [...new Set(shown.flatMap((b) => scaling[b.tone][scenario]!.map((p) => p.cpus)))].sort((a, b) => a - b);
  const at = (tone: Tone, cpus: number): ScalingPoint | undefined =>
    scaling[tone as "rust" | "bun" | "php"][scenario]?.find((p) => p.cpus === cpus);
  return (
    <div className="panel table-panel bench-table scaling-table">
      <div className="scroll">
        <table>
          <thead>
            <tr>
              <th scope="col">CPUs</th>
              {shown.map((b) => (
                <th scope="col" key={b.tone}>
                  <span className={`app tone-${b.tone}`}>
                    <span className="dot" aria-hidden="true" />
                    {b.name}
                  </span>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {sizes.map((cpus) => {
              const rates = shown.map((b) => at(b.tone, cpus)?.rate ?? null);
              const top = Math.max(...rates.filter((r): r is number => r != null));
              return (
                <tr key={cpus}>
                  <th scope="row">{cpus}</th>
                  {shown.map((b, i) => {
                    const p = at(b.tone, cpus);
                    const one = at(b.tone, 1)?.rate;
                    if (!p || p.rate == null) {
                      return <td key={b.tone} className="na">{p ? `${p.failed} of ${p.repeats} failed` : "–"}</td>;
                    }
                    return (
                      <td key={b.tone} className={rates[i] === top ? "best" : undefined}>
                        {whole(p.rate)} msg/s
                        {one && cpus > 1 ? <span className="scaling-eff">{formatTimes(p.rate, cpus * one)} of linear</span> : null}
                        {p.failed ? <span className="scaling-eff">{p.failed} of {p.repeats} failed</span> : null}
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
