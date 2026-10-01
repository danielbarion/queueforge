import { chartScaleSplit, chartWindowSec, type ChartPoint, type ChartSeriesName } from "../rates";

export type LiveMagnitudes = {
  publish: number;
  deliver: number;
  ack: number;
  ready: number;
  unacked: number;
};

type Props = {
  points: ChartPoint[];
  magnitudes: LiveMagnitudes;
};

const STYLE: Record<ChartSeriesName, { color: string; label: string }> = {
  publish: { color: "#3ee0c5", label: "Publish" },
  deliver: { color: "#9ad7ff", label: "Deliver" },
  ack: { color: "#ff6b8a", label: "Ack" },
  ready: { color: "#7aa2ff", label: "Ready" },
  unacked: { color: "#e7c15a", label: "Unacked" },
};

function magnitudeOf(name: ChartSeriesName, magnitudes: LiveMagnitudes): number {
  switch (name) {
    case "publish":
      return magnitudes.publish;
    case "deliver":
      return magnitudes.deliver;
    case "ack":
      return magnitudes.ack;
    case "ready":
      return magnitudes.ready;
    case "unacked":
      return magnitudes.unacked;
  }
}

function pointValue(name: ChartSeriesName, point: ChartPoint): number {
  switch (name) {
    case "publish":
      return point.publishPerSec;
    case "deliver":
      return point.deliverPerSec;
    case "ack":
      return point.ackPerSec;
    case "ready":
      return point.ready;
    case "unacked":
      return point.unacked;
  }
}

function formatMagnitude(name: ChartSeriesName, value: number): string {
  if (name === "ready" || name === "unacked") return Math.round(value).toLocaleString();
  return `${value.toFixed(1)}/s`;
}

function formatCeiling(value: number, rate: boolean): string {
  if (rate) return value >= 100 ? Math.round(value).toLocaleString() : value.toFixed(1);
  return Math.round(value).toLocaleString();
}

export default function Spark({ points, magnitudes }: Props) {
  const split = chartScaleSplit(points);
  const seconds = chartWindowSec(points);
  const windowLabel =
    points.length < 2 ? "window collecting samples" : `window ${seconds < 10 ? seconds.toFixed(1) : Math.round(seconds)}s · ${points.length} samples`;
  return (
    <div className="rate-board">
      <div className="rate-legend" aria-label="Series key">
        {split.series.map((series) => (
          <span className="rate-key" key={series.name}>
            <span className="rate-swatch" style={{ background: STYLE[series.name].color }} />
            <span>{STYLE[series.name].label}</span>
            <strong>{formatMagnitude(series.name, magnitudeOf(series.name, magnitudes))}</strong>
          </span>
        ))}
      </div>
      <div className="muted rate-window">{windowLabel}</div>
      <ScalePane
        title="Rate scale"
        ceiling={`0–${formatCeiling(split.rateMax, true)} /s`}
        max={split.rateMax}
        points={points}
        names={split.series.filter((series) => series.scale === "rate").map((series) => series.name)}
      />
      <ScalePane
        title="Depth scale"
        ceiling={`0–${formatCeiling(split.depthMax, false)}`}
        max={split.depthMax}
        points={points}
        names={split.series.filter((series) => series.scale === "depth").map((series) => series.name)}
      />
    </div>
  );
}

function ScalePane({
  title,
  ceiling,
  max,
  points,
  names,
}: {
  title: string;
  ceiling: string;
  max: number;
  points: ChartPoint[];
  names: ChartSeriesName[];
}) {
  const w = 640;
  const h = 72;
  const x = (i: number) => (points.length <= 1 ? 0 : (i / (points.length - 1)) * w);
  const y = (value: number) => h - 8 - (value / max) * (h - 16);
  const line = (name: ChartSeriesName) =>
    points.map((point, i) => `${i === 0 ? "M" : "L"}${x(i).toFixed(1)},${y(pointValue(name, point)).toFixed(1)}`).join(" ");
  return (
    <div className="rate-pane">
      <div className="rate-pane-head">
        <span>{title}</span>
        <span>{ceiling}</span>
      </div>
      <svg className="spark" viewBox={`0 0 ${w} ${h}`} role="img" aria-label={`${title} ${names.join(", ")}`}>
        <line x1="0" y1={h - 8} x2={w} y2={h - 8} stroke="#243044" strokeWidth="1" />
        {names.map((name) => (
          <path key={name} d={line(name)} fill="none" stroke={STYLE[name].color} strokeWidth="2" />
        ))}
      </svg>
    </div>
  );
}
