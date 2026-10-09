import type { LucideIcon } from "lucide-react";

export function PageHeader({ title, icon: Icon, detail }: { title: string; icon: LucideIcon; detail: string }) {
  return (
    <header className="qf-page-header mb-8 flex items-start justify-between gap-4">
      <div>
        <h1 className="flex items-center gap-3 text-3xl font-semibold tracking-tight">
          <Icon className="size-6 text-primary" aria-hidden="true" />
          {title}
        </h1>
        <p className="mt-2 max-w-xl text-sm leading-relaxed text-pretty text-muted">{detail}</p>
      </div>
    </header>
  );
}
