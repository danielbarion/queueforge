import { formatRate, type LoadRow, type RateKey, type Tone } from "./bench";

const APPS: { app: string; tone: Tone }[] = [
  { app: "RabbitMQ", tone: "mq" },
  { app: "Rust", tone: "rust" },
  { app: "Bun", tone: "bun" },
  { app: "PHP", tone: "php" },
];

const SHAPES: { key: RateKey; title: string; unit: string }[] = [
  { key: "one", title: "1 connection", unit: "confirm/s" },
  { key: "shared", title: "16 connections, one queue", unit: "consume/s" },
  { key: "spread", title: "16 queues", unit: "confirm/s" },
];

const WIDTH = 480;
const HEIGHT = 210;
const PAD = { l: 46, r: 10, t: 14, b: 40 };

/** A failed cell is not a rate. A split cell is not the same queue. */
function rateOf(row: LoadRow, key: RateKey): number | null {
  if (row.caveats?.[key]) return null;
  const rate = row[key];
  return rate == null ? null : rate;
}

/** 1–2–5 ceiling so the top gridline sits above the highest point. */
function niceMax(value: number) {
  if (value <= 0) return 1;
  const exp = 10 ** Math.floor(Math.log10(value));
  const fraction = value / exp;
  const nice = fraction <= 1 ? 1 : fraction <= 2 ? 2 : fraction <= 5 ? 5 : 10;
  return nice * exp;
}

function compact(value: number) {
  if (value >= 1000) return `${Math.round(value / 1000)}k`;
  return String(Math.round(value));
}

function SizePanel({ rows, shape, sizes }: { rows: LoadRow[]; shape: (typeof SHAPES)[number]; sizes: string[] }) {
  const innerW = WIDTH - PAD.l - PAD.r;
  const innerH = HEIGHT - PAD.t - PAD.b;
  const xAt = (index: number) => PAD.l + (sizes.length === 1 ? innerW / 2 : (index / (sizes.length - 1)) * innerW);
  const series = APPS.map((app) => ({
    ...app,
    points: sizes.map((size, index) => {
      const row = rows.find((item) => item.app === app.app && item.size === size);
      const value = row ? rateOf(row, shape.key) : null;
      return { index, value };
    }),
  }));
  const peak = Math.max(0, ...series.flatMap((line) => line.points.map((point) => point.value ?? 0)));
  const max = niceMax(peak);
  const yAt = (value: number) => PAD.t + (1 - value / max) * innerH;
  const ticks = [max, max / 2, 0];

  return (
    <figure className="size-chart">
      <figcaption>
        <span className="size-chart-title">{shape.title}</span>
        <span className="size-chart-unit">{shape.unit}</span>
      </figcaption>
      <svg viewBox={`0 0 ${WIDTH} ${HEIGHT}`} role="img" aria-label={`${shape.title}, ${shape.unit}, across container sizes`}>
        {ticks.map((tick) => (
          <g key={tick}>
            <line className="grid" x1={PAD.l} x2={WIDTH - PAD.r} y1={yAt(tick)} y2={yAt(tick)} />
            <text x={PAD.l - 8} y={yAt(tick) + 3} textAnchor="end">
              {compact(tick)}
            </text>
          </g>
        ))}
        {sizes.map((size, index) => {
          const [cpu, mem] = size.split(" / ");
          const x = xAt(index);
          return (
            <text key={size} x={x} y={HEIGHT - 22} textAnchor="middle">
              <tspan x={x} dy="0">{cpu}</tspan>
              <tspan x={x} dy="13">{mem}</tspan>
            </text>
          );
        })}
        {series.map((line) => {
          const segments: { index: number; value: number }[][] = [];
          let current: { index: number; value: number }[] = [];
          for (const point of line.points) {
            if (point.value == null) {
              if (current.length) segments.push(current);
              current = [];
            } else {
              current.push({ index: point.index, value: point.value });
            }
          }
          if (current.length) segments.push(current);
          return (
            <g key={line.app} className={`tone-${line.tone}`}>
              {segments.map((segment) => (
                <polyline
                  key={segment.map((point) => point.index).join("-")}
                  className="series"
                  points={segment.map((point) => `${xAt(point.index).toFixed(1)},${yAt(point.value).toFixed(1)}`).join(" ")}
                />
              ))}
              {line.points.map((point) =>
                point.value == null ? null : (
                  <circle key={point.index} className="series-dot" cx={xAt(point.index)} cy={yAt(point.value)} r="3.5" />
                ),
              )}
            </g>
          );
        })}
      </svg>
      <table className="sr-only">
        <caption>
          {shape.title}, {shape.unit}
        </caption>
        <thead>
          <tr>
            <th>App</th>
            {sizes.map((size) => (
              <th key={size}>{size}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {series.map((line) => (
            <tr key={line.app}>
              <th>{line.app}</th>
              {line.points.map((point) => (
                <td key={point.index}>{point.value == null ? "no delivery" : formatRate(point.value)}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </figure>
  );
}

/** Message rate at each container size. One panel per load shape. */
export function SizeCharts({ rows }: { rows: LoadRow[] }) {
  const sizes = [...new Set(rows.map((row) => row.size))];
  return (
    <div>
      <ul className="chart-legend">
        {APPS.map((app) => (
          <li key={app.app} className={`tone-${app.tone}`}>
            <span className="dot" aria-hidden="true" />
            {app.app}
          </li>
        ))}
      </ul>
      <div className="size-charts">
        {SHAPES.map((shape) => (
          <SizePanel key={shape.key} rows={rows} shape={shape} sizes={sizes} />
        ))}
      </div>
    </div>
  );
}
