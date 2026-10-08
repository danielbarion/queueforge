import type { ReactNode } from "react";

export function Badge({
  children,
  tone = "neutral",
}: {
  children: ReactNode;
  tone?: "neutral" | "primary" | "warning" | "error";
}) {
  const toneClass = {
    neutral: "badge-ghost",
    primary: "badge-primary",
    warning: "badge-warning",
    error: "badge-error",
  }[tone];
  return <span className={`badge badge-sm font-semibold uppercase tracking-wider ${toneClass}`}>{children}</span>;
}
