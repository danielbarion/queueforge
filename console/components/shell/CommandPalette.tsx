"use client";

import { useEffect, useId, useState } from "react";
import { useRouter } from "next/navigation";
import { Search, X } from "lucide-react";
import { Modal } from "@/components/ui/Modal";
import { DEMO_PROFILES } from "@/lib/demo";
import { NAV } from "@/lib/nav";
import { useBrokerStore } from "@/stores/broker";
import { useLiveStore } from "@/stores/live";

export function CommandPalette() {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const [index, setIndex] = useState(0);
  const id = useId(); const router = useRouter();
  const brokerState = useBrokerStore(); const queues = useLiveStore((s) => s.queues);
  useEffect(() => {
    const show = () => { if (document.querySelector('[aria-modal="true"]')) return; setQuery(""); setIndex(0); setOpen(true); };
    const key = (event: KeyboardEvent) => { if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === "k") { event.preventDefault(); show(); } };
    window.addEventListener("qf-command-palette", show); document.addEventListener("keydown", key);
    return () => { window.removeEventListener("qf-command-palette", show); document.removeEventListener("keydown", key); };
  }, []);
  const actions = [
    ...NAV.flatMap((group) => group.items).map((item) => ({ key: item.href, label: item.label, detail: "Page", run: () => router.push(item.href) })),
    ...queues.map((q) => ({ key: `queue-${q.name}`, label: q.name, detail: "Queue on /", run: () => router.push(`/queues/${encodeURIComponent(q.name)}`) })),
    ...(brokerState.demo ? DEMO_PROFILES : brokerState.targets).map((target) => ({ key: target.id, label: target.name, detail: `Switch broker · ${target.kind}`, run: () => { if (brokerState.demo) brokerState.setDemo(true, target.kind); else brokerState.select(target.id); router.push("/"); } })),
  ].filter((a) => `${a.label} ${a.detail}`.toLowerCase().includes(query.toLowerCase()));
  const active = Math.min(index, Math.max(0, actions.length - 1));
  useEffect(() => { if (open) document.getElementById(`${id}-option-${active}`)?.scrollIntoView({ block: "nearest" }); }, [active, open, query, id]);
  function run(at: number) { actions[at]?.run(); setOpen(false); }
  if (!open) return null;
  return <Modal labelId={id} onClose={() => setOpen(false)}><div className="flex items-center justify-between gap-3 border-b border-base-300 p-4"><h2 id={id} className="font-semibold">Jump to a page, queue or broker</h2><button className="btn btn-ghost btn-sm btn-square" aria-label="Close search" onClick={() => setOpen(false)}><X className="size-4" /></button></div><div className="p-4"><label className="input input-bordered flex w-full items-center gap-2"><Search className="size-4" /><input autoFocus role="combobox" aria-label="Search console" aria-expanded="true" aria-controls={`${id}-results`} aria-activedescendant={actions.length ? `${id}-option-${active}` : undefined} autoComplete="off" value={query} onChange={(event) => { setQuery(event.target.value); setIndex(0); }} onKeyDown={(event) => { if (event.key === "ArrowDown" || event.key === "ArrowUp") { event.preventDefault(); setIndex(actions.length ? (active + (event.key === "ArrowDown" ? 1 : -1) + actions.length) % actions.length : 0); } if (event.key === "Enter" && actions.length) { event.preventDefault(); run(active); } }} /></label></div><ul id={`${id}-results`} role="listbox" aria-label="Search results" className="qf-palette-results">{actions.map((action, at) => <li id={`${id}-option-${at}`} key={action.key} role="option" aria-selected={at === active}><button tabIndex={-1} className={`qf-command ${at === active ? "is-selected" : ""}`} onMouseEnter={() => setIndex(at)} onClick={() => run(at)}><span>{action.label}</span><span className="text-xs text-muted">{action.detail}</span></button></li>)}{actions.length === 0 && <li className="p-5 text-sm text-muted" role="presentation">No matches.</li>}</ul><p className="border-t border-base-300 p-4 text-xs text-muted">↑ ↓ navigate · Enter open · Escape close · Ctrl / ⌘ K search</p></Modal>;
}
