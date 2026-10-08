"use client";

import { Monitor, Moon, Sun } from "lucide-react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { useEffect, useRef, useState } from "react";
import { NAV } from "@/lib/nav";
import { useThemeStore, type ThemePreference } from "@/stores/theme";
import { Badge } from "@/components/ui/Badge";

const THEMES: { value: ThemePreference; label: string; icon: typeof Sun }[] = [
  { value: "system", label: "System", icon: Monitor },
  { value: "dark", label: "Dark mode", icon: Moon },
  { value: "light", label: "Light mode", icon: Sun },
];

export function Sidebar({ open, onClose }: { open: boolean; onClose: () => void }) {
  const pathname = usePathname();
  const preference = useThemeStore((state) => state.preference);
  const setPreference = useThemeStore((state) => state.setPreference);
  const [menuOpen, setMenuOpen] = useState(false);
  const menuRef = useRef<HTMLDivElement>(null);
  const current = THEMES.find((item) => item.value === preference) ?? THEMES[0];
  const CurrentIcon = current.icon;

  useEffect(() => {
    if (!menuOpen) return;
    function onPointer(event: MouseEvent) {
      if (!menuRef.current?.contains(event.target as Node)) setMenuOpen(false);
    }
    function onKey(event: KeyboardEvent) {
      if (event.key === "Escape") setMenuOpen(false);
    }
    document.addEventListener("mousedown", onPointer);
    document.addEventListener("keydown", onKey);
    return () => {
      document.removeEventListener("mousedown", onPointer);
      document.removeEventListener("keydown", onKey);
    };
  }, [menuOpen]);

  return (
    <>
      {open && (
        <button type="button" aria-label="Close navigation" className="fixed inset-0 z-40 bg-black/50 lg:hidden" onClick={onClose} />
      )}
      <aside
        className={`fixed inset-y-0 left-0 z-50 flex w-60 shrink-0 flex-col border-r border-base-300 bg-base-200 transition-transform lg:static lg:translate-x-0 ${
          open ? "translate-x-0" : "-translate-x-full"
        }`}
      >
        <div className="flex items-center gap-2 px-5 py-5">
          <span className="text-lg font-bold tracking-tight">QueueForge</span>
          <Badge tone="primary">dash</Badge>
        </div>
        <nav className="flex-1 overflow-y-auto px-3 pb-4">
          {NAV.map((group) => (
            <div key={group.section ?? "root"} className="mb-4">
              {group.section && (
                <div className="px-2 pb-2 pt-1 text-[10px] font-semibold uppercase tracking-wider text-faint">{group.section}</div>
              )}
              <div className="flex flex-col gap-0.5">
                {group.items.map((item) => {
                  const active = item.href === "/" ? pathname === "/" : pathname.startsWith(item.href);
                  const Icon = item.icon;
                  return (
                    <Link
                      key={item.href}
                      href={item.href}
                      onClick={onClose}
                      className={`relative flex items-center gap-3 rounded-lg px-3 py-2 text-sm font-medium ${
                        active ? "bg-base-300 text-base-content" : "text-muted hover:bg-base-300/60 hover:text-base-content"
                      }`}
                    >
                      {active && <span className="absolute inset-y-1.5 left-0 w-0.5 rounded-full bg-primary" />}
                      <Icon className="size-[18px] shrink-0" aria-hidden="true" />
                      <span className="truncate">{item.label}</span>
                    </Link>
                  );
                })}
              </div>
            </div>
          ))}
        </nav>
        <div ref={menuRef} className="relative mx-2 mb-3">
          {menuOpen && (
            <div className="absolute bottom-full left-0 right-0 z-10 mb-1 rounded-lg border border-base-300 bg-base-100 p-1" role="menu">
              {THEMES.map((item) => {
                const Icon = item.icon;
                const selected = item.value === preference;
                return (
                  <button
                    key={item.value}
                    type="button"
                    role="menuitemradio"
                    aria-checked={selected}
                    className={`flex w-full items-center gap-3 rounded-md px-3 py-2 text-left text-sm ${
                      selected ? "bg-base-300 text-base-content" : "text-muted hover:bg-base-300/60 hover:text-base-content"
                    }`}
                    onClick={() => {
                      setPreference(item.value);
                      setMenuOpen(false);
                    }}
                  >
                    <Icon className="size-4 shrink-0" aria-hidden="true" />
                    {item.label}
                  </button>
                );
              })}
            </div>
          )}
          <button
            type="button"
            className="flex w-full items-center gap-3 rounded-lg px-3 py-2 text-sm text-muted hover:bg-base-300/60 hover:text-base-content"
            aria-haspopup="menu"
            aria-expanded={menuOpen}
            onClick={() => setMenuOpen((open) => !open)}
          >
            <CurrentIcon className="size-4 shrink-0" aria-hidden="true" />
            {current.label}
          </button>
        </div>
      </aside>
    </>
  );
}
