export function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div className="qf-stat stat rounded-box border border-base-300 bg-base-200 px-5 py-5">
      <div className="stat-title text-[11px] font-semibold uppercase tracking-wider text-muted">{label}</div>
      <div className="stat-value mt-2 font-mono text-3xl text-base-content">{value}</div>
    </div>
  );
}
