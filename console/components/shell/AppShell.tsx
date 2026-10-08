"use client";

import { Menu } from "lucide-react";
import { useEffect, useState, type ReactNode } from "react";
import { Poller } from "@/components/live/Poller";
import { Toasts } from "@/components/live/Toasts";
import { Sidebar } from "@/components/shell/Sidebar";
import { resolvedTheme, useThemeStore } from "@/stores/theme";

export function AppShell({ children }: { children: ReactNode }) {
  const preference = useThemeStore((state) => state.preference);
  const [systemDark, setSystemDark] = useState(true);
  const [open, setOpen] = useState(false);

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

  return (
    <div className="flex min-h-screen bg-base-100">
      <Poller />
      <Toasts />
      <Sidebar open={open} onClose={() => setOpen(false)} />
      <div className="min-w-0 flex-1">
        <div className="flex items-center border-b border-base-300 px-4 py-3 lg:hidden">
          <button type="button" className="btn btn-ghost btn-sm" onClick={() => setOpen(true)} aria-label="Open navigation">
            <Menu className="size-4" />
          </button>
        </div>
        <main className="mx-auto w-full max-w-6xl px-5 py-6">{children}</main>
      </div>
    </div>
  );
}
