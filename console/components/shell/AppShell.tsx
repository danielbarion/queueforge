"use client";

import { Menu, RefreshCw, Search, Server } from "lucide-react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { useEffect, useRef, useState, type ReactNode } from "react";
import { Poller } from "@/components/live/Poller";
import { Toasts } from "@/components/live/Toasts";
import { CommandPalette } from "@/components/shell/CommandPalette";
import { DEMO_PROFILES } from "@/lib/demo";
import { Sidebar } from "@/components/shell/Sidebar";
import { NAV } from "@/lib/nav";
import { selectedBroker, useBrokerStore } from "@/stores/broker";
import { useLiveStore } from "@/stores/live";
import { resolvedTheme, useThemeStore } from "@/stores/theme";

export function AppShell({ children }: { children: ReactNode }) {
  const preference = useThemeStore((state) => state.preference);
  const [systemDark, setSystemDark] = useState(true);
  const [open, setOpen] = useState(false);
  const menuButton = useRef<HTMLButtonElement>(null);
  const pathname = usePathname();
  const broker = useBrokerStore(selectedBroker);
  const targets = useBrokerStore((state) => state.targets);
  const select = useBrokerStore((state) => state.select);
  const demo = useBrokerStore((state) => state.demo);
  const setDemo = useBrokerStore((state) => state.setDemo);
  const available = demo ? DEMO_PROFILES : targets;
  const health = useLiveStore((state) => state.health);
  const stale = useLiveStore((state) => state.stale);
  const nudge = useLiveStore((state) => state.nudge);
  const page = NAV.flatMap((group) => group.items).find((item) => item.href === "/" ? pathname === "/" : pathname.startsWith(item.href));
  const status = !broker ? "No broker connected" : stale ? "Data is stale" : health === null ? "Connecting" : health ? "Broker online" : "Broker offline";

  useEffect(() => {
    const media = window.matchMedia("(prefers-color-scheme: dark)");
    const apply = () => setSystemDark(media.matches);
    apply();
    media.addEventListener("change", apply);
    return () => media.removeEventListener("change", apply);
  }, []);

  useEffect(() => {
    document.documentElement.dataset.theme = resolvedTheme(preference, systemDark);
  }, [preference, systemDark]);

  useEffect(() => {
    if (!open) return;
    const previous = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    const previousFocus = document.activeElement as HTMLElement | null;
    const navigation = document.getElementById("console-navigation");
    const focusable = () => Array.from(navigation?.querySelectorAll<HTMLElement>('a[href], button:not([disabled]), select') ?? []);
    focusable()[0]?.focus();
    const onKey = (event: KeyboardEvent) => {
      if (event.key === "Escape") setOpen(false);
      if (event.key !== "Tab") return;
      const items = focusable();
      const first = items[0];
      const last = items[items.length - 1];
      if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last?.focus(); }
      else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first?.focus(); }
    };
    const media = window.matchMedia("(min-width: 1024px)");
    const closeOnDesktop = () => { if (media.matches) setOpen(false); };
    media.addEventListener("change", closeOnDesktop);
    document.addEventListener("keydown", onKey);
    return () => {
      document.body.style.overflow = previous;
      document.removeEventListener("keydown", onKey);
      media.removeEventListener("change", closeOnDesktop);
      if (previousFocus && previousFocus !== document.body) previousFocus.focus();
      else menuButton.current?.focus();
    };
  }, [open]);

  return (
    <div className="qf-shell flex min-h-screen bg-base-100">
      <a href="#console-content" className="qf-skip">Skip to content</a>
      <CommandPalette />
      <Poller />
      <Toasts />
      <Sidebar open={open} onClose={() => setOpen(false)} />
      <div className="min-w-0 flex-1" inert={open}>
        <div className="qf-toolbar">
          <div className="flex min-w-0 items-center gap-3">
            <button ref={menuButton} type="button" className="btn btn-ghost btn-sm lg:hidden" onClick={() => setOpen(true)} aria-label="Open navigation" aria-expanded={open} aria-controls="console-navigation"><Menu className="size-5" /></button>
            <span className="hidden text-xs text-faint sm:inline">Console</span><span className="hidden text-faint sm:inline" aria-hidden="true">/</span><span className="truncate text-sm font-medium">{page?.label ?? "Workspace"}</span>
          </div>
          <div className="flex min-w-0 items-center gap-3">
            <button className="btn btn-ghost btn-sm" aria-label="Search console, Ctrl or Command K" onClick={() => window.dispatchEvent(new Event("qf-command-palette"))}><Search className="size-4" /><span className="hidden sm:inline">Search</span><kbd className="hidden text-xs text-faint md:inline">⌘ K</kbd></button>
            {broker ? <label className="qf-broker-picker"><Server className="size-4 shrink-0 text-primary" aria-hidden="true" /><select aria-label="Active broker" value={broker.id} onChange={(event) => { const target = available.find((t) => t.id === event.target.value); if (demo && target) setDemo(true, target.kind); else select(event.target.value); }}>{available.map((target) => <option key={target.id} value={target.id}>{target.name}</option>)}</select></label> : <Link href="/brokers" className="btn btn-ghost btn-sm"><Server className="size-4" aria-hidden="true" />Add broker</Link>}
            <span className={`qf-live-status ${!broker ? "" : stale ? "is-stale" : health === false ? "is-offline" : health ? "is-online" : ""}`} title={status}><i aria-hidden="true" /><span className="hidden xl:inline">{status}</span><span className="sr-only xl:hidden">{status}</span></span>
            <button type="button" className="btn btn-ghost btn-sm btn-square" onClick={nudge} disabled={!broker} aria-label="Refresh broker data" title="Refresh broker data"><RefreshCw className="size-4" /></button>
          </div>
        </div>
        {demo && <div className="qf-demo-banner" role="status"><div><strong>Demo mode</strong><span>Sample data · read-only · no broker connections</span></div><button className="btn btn-sm btn-ghost" onClick={() => setDemo(false)}>Exit demo</button></div>}
        <main id="console-content" tabIndex={-1} className="qf-content mx-auto w-full max-w-7xl px-5 py-8 sm:px-8 lg:px-10">{children}</main>
      </div>
    </div>
  );
}
