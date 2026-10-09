"use client";

import { useId, useState } from "react";
import { Card, CardBody } from "@/components/ui/Card";
import { useBrokerStore } from "@/stores/broker";
import { formatRate, useLiveStore } from "@/stores/live";

const SERIES = [
  { key: "publish", label: "Published", color: "var(--color-primary)", dash: undefined },
  { key: "deliver", label: "Delivered", color: "var(--color-secondary)", dash: "7 3" },
  { key: "ack", label: "Acknowledged", color: "var(--color-accent)", dash: "2 3" },
] as const;
const clock = (at: number) => new Date(at).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
const valid = (v: number | null): v is number => v !== null && Number.isFinite(v) && v >= 0;

export function Throughput() {
  const demo = useBrokerStore((state) => state.demo);
  const history = useLiveStore((state) => state.history);
  const rates = useLiveStore((state) => state.rates);
  const stale = useLiveStore((state) => state.stale);
  const authed = useLiveStore((state) => state.authed);
  const [minutes, setMinutes] = useState(1);
  const id = useId();
  const end = history.at(-1)?.at ?? null;
  const start = (end ?? 0) - minutes * 60_000;
  const samples = history.filter((s) => s.at >= start);
  const values = samples.flatMap((s) => SERIES.flatMap(({ key }) => valid(s[key]) ? [s[key]] : []));
  const max = Math.max(1, ...values) * 1.1;
  const x = (at: number) => 50 + ((at - start) / (minutes * 60_000)) * 658;
  const y = (value: number) => 15 + (1 - value / max) * 183;
  const path = (key: typeof SERIES[number]["key"]) => {
    let drawing = false;
    return samples.map((s) => {
      if (!valid(s[key])) { drawing = false; return ""; }
      const command = `${drawing ? "L" : "M"}${x(s.at).toFixed(2)},${y(s[key]).toFixed(2)}`;
      drawing = true;
      return command;
    }).join(" ");
  };
  return <Card><CardBody>
    <div className="flex flex-wrap items-start justify-between gap-3">
      <div><h2 className="text-base font-semibold">Message throughput</h2><p className="mt-1 text-xs text-muted">{demo ? "Simulated messages per second · sample data" : "Messages per second · measured between broker samples"}</p></div>
      <div className="join" role="group" aria-label="Throughput time range">{[1, 5].map((range) => <button key={range} type="button" className={`btn btn-sm join-item ${minutes === range ? "btn-primary" : "btn-ghost"}`} aria-pressed={minutes === range} onClick={() => setMinutes(range)}>{range} min</button>)}</div>
    </div>
    <dl className="grid grid-cols-3 gap-3">{SERIES.map(({ key, label, color }) => <div key={key}><dt className="text-xs text-muted"><span aria-hidden="true" className="mr-2 inline-block size-2 rounded-full" style={{ backgroundColor: color }} />{label}</dt><dd className="mt-1 font-mono text-xl" style={{ color }}>{formatRate(rates[key])}<span className="ml-1 text-xs text-muted">/s</span></dd></div>)}</dl>
    {stale && <p role="status" className="text-xs text-warning">Updates paused. Showing the last received samples{end === null ? "." : ` through ${clock(end)}.`}</p>}
    {values.length === 0 ? <div className="flex min-h-48 items-center justify-center rounded-box border border-dashed border-base-300 p-6 text-center text-sm text-muted">{authed === false ? "Log in to receive live throughput samples." : "Waiting for two samples with message counters. Rates appear after the next poll."}</div> : <>
      <svg className="w-full overflow-visible" viewBox="0 0 720 230" role="img" aria-labelledby={`${id}-title ${id}-description`}>
        <title id={`${id}-title`}>Message throughput over the last {minutes} minute{minutes === 1 ? "" : "s"}</title>
        <desc id={`${id}-description`}>Published messages use a solid line, delivered messages a dashed line, and acknowledgments a dotted line. Missing counters and counter resets leave gaps. Exact timestamps and rates are in the sample table below.</desc>
        {[0, 0.5, 1].map((fraction) => <g key={fraction}><line x1={50} x2={708} y1={y(max * fraction)} y2={y(max * fraction)} stroke="var(--color-base-300)" /><text x={42} y={y(max * fraction) + 4} textAnchor="end" fill="var(--color-muted)" fontSize={11}>{formatRate(max * fraction)}</text></g>)}
        {SERIES.map(({ key, label, color, dash }) => <g key={key}><path d={path(key)} fill="none" stroke={color} strokeWidth={2} strokeDasharray={dash} />{samples.filter((s) => valid(s[key])).map((s) => <circle key={s.at} cx={x(s.at)} cy={y(s[key]!)} r={2.5} fill={color}><title>{clock(s.at)}: {label} {formatRate(s[key])}/s</title></circle>)}</g>)}
        <text x={50} y={223} fill="var(--color-muted)" fontSize={11}>{clock(start)}</text><text x={708} y={223} textAnchor="end" fill="var(--color-muted)" fontSize={11}>{clock(end!)}</text>
      </svg>
      <details className="text-xs text-muted"><summary className="cursor-pointer">View sample values ({samples.length})</summary><div className="mt-3 max-h-56 overflow-auto"><table className="table table-xs"><caption className="sr-only">Throughput samples, messages per second; a dash means unavailable.</caption><thead><tr><th scope="col">Time</th>{SERIES.map(({ key, label }) => <th key={key} scope="col">{label}/s</th>)}</tr></thead><tbody>{samples.map((s) => <tr key={s.at}><th scope="row"><time dateTime={new Date(s.at).toISOString()}>{clock(s.at)}</time></th>{SERIES.map(({ key }) => <td key={key} className="font-mono">{formatRate(s[key])}</td>)}</tr>)}</tbody></table></div></details>
    </>}
  </CardBody></Card>;
}
