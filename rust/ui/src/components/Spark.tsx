import type { ChartPoint } from "../rates";

type Props = { points: ChartPoint[] };

export default function Spark({ points }: Props) {
  const w = 640;
  const h = 92;
  const maxRate = Math.max(1, ...points.map((p) => Math.max(p.publishPerSec, p.deliverPerSec, p.ackPerSec)));
  const maxDepth = Math.max(1, ...points.map((p) => Math.max(p.ready, p.unacked)));
  const x = (i: number) => (points.length <= 1 ? 0 : (i / (points.length - 1)) * w);
  const line = (pick: (p: ChartPoint) => number, max: number) =>
    points.map((p, i) => `${i === 0 ? "M" : "L"}${x(i).toFixed(1)},${(h - 8 - (pick(p) / max) * (h - 16)).toFixed(1)}`).join(" ");
  return (
    <svg className="spark" viewBox={`0 0 ${w} ${h}`} role="img" aria-label="Recent publish, deliver, ack, and depth">
      <path d={line((p) => p.ready, maxDepth)} fill="none" stroke="#7aa2ff" strokeWidth="2" />
      <path d={line((p) => p.unacked, maxDepth)} fill="none" stroke="#e7c15a" strokeWidth="2" />
      <path d={line((p) => p.publishPerSec, maxRate)} fill="none" stroke="#3ee0c5" strokeWidth="2" />
      <path d={line((p) => p.deliverPerSec, maxRate)} fill="none" stroke="#9ad7ff" strokeWidth="1.5" />
      <path d={line((p) => p.ackPerSec, maxRate)} fill="none" stroke="#ff6b8a" strokeWidth="1.5" />
    </svg>
  );
}
