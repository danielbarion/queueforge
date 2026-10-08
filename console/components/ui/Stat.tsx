export function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div className="stat rounded-box border border-base-300 bg-base-200 px-4 py-3">
      <div className="stat-title text-[11px] font-semibold uppercase tracking-wider text-muted">{label}</div>
      <div className="stat-value font-mono text-2xl text-base-content">{value}</div>
    </div>
  );
}
