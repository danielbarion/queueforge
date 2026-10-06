import { formatRate, type Bar } from "./bench";

export function Bars({ rows, unit }: { rows: Bar[]; unit: string }) {
  const max = Math.max(...rows.map((row) => row.rate));
  return (
    <ul className="bars">
      {rows.map((row) => (
        <li key={row.name}>
          <span className="bar-name">{row.name}</span>
          <span className="bar-track" aria-hidden="true">
            <span className={`bar-fill ${row.tone}`} style={{ width: `${(row.rate / max) * 100}%` }} />
          </span>
          <span className="bar-value">
            {formatRate(row.rate)}
            <span className="bar-unit"> {unit}</span>
          </span>
        </li>
      ))}
    </ul>
  );
}
