import type { ReactNode } from "react";

export function Card({ children, className = "" }: { children: ReactNode; className?: string }) {
  return <section className={`qf-card card min-w-0 border border-base-300 bg-base-200 shadow-none ${className}`}>{children}</section>;
}

export function CardBody({ children, className = "" }: { children: ReactNode; className?: string }) {
  return <div className={`card-body flex min-w-0 flex-col gap-5 p-5 sm:p-6 ${className}`}>{children}</div>;
}
