import type { ReactNode } from "react";

export function Card({ children, className = "" }: { children: ReactNode; className?: string }) {
  return <section className={`card border border-base-300 bg-base-200 shadow-none ${className}`}>{children}</section>;
}

export function CardBody({ children, className = "" }: { children: ReactNode; className?: string }) {
  return <div className={`card-body flex flex-col gap-5 p-6 ${className}`}>{children}</div>;
}
