import { formatRate, formatTimes, type Bar } from "./bench";

export function Bars({ rows, unit, base }: { rows: Bar[]; unit: string; base?: number }) {
  const max = Math.max(...rows.map((row) => row.rate));
  return (
    <ul className="bars">
      {rows.map((row) => (
        <li key={row.name} className={`tone-${row.tone}`}>
          <span className="bar-name">
            <span className="dot" aria-hidden="true" />
            {row.name}
          </span>
          <span className="bar-track" aria-hidden="true">
            <span className="bar-fill" style={{ width: `${(row.rate / max) * 100}%` }} />
          </span>
          <span className="bar-value">
            {formatRate(row.rate)}
            <span className="bar-unit"> {unit}</span>
            {base ? <span className="bar-times">{formatTimes(row.rate, base)}×</span> : null}
          </span>
        </li>
      ))}
    </ul>
  );
}
