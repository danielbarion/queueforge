"use client";

import { useEffect, useRef, type ReactNode } from "react";
import { createPortal } from "react-dom";

export function Modal({ children, labelId, onClose, drawer = false }: { children: ReactNode; labelId: string; onClose: () => void; drawer?: boolean }) {
  const ref = useRef<HTMLDivElement>(null);
  const close = useRef(onClose);
  close.current = onClose;
  useEffect(() => {
    const previous = document.activeElement as HTMLElement | null;
    const shell = document.querySelector<HTMLElement>(".qf-shell");
    const inert = shell?.inert ?? false;
    const overflow = document.body.style.overflow;
    if (shell) shell.inert = true;
    document.body.style.overflow = "hidden";
    const items = () => Array.from(ref.current?.querySelectorAll<HTMLElement>('button:not(:disabled), input:not(:disabled), select:not(:disabled), textarea:not(:disabled), a[href], [tabindex="0"]') ?? []).filter((el) => el.getClientRects().length > 0);
    (ref.current?.querySelector<HTMLElement>('[role="combobox"]') ?? items()[0] ?? ref.current)?.focus();
    const key = (event: KeyboardEvent) => {
      if (event.key === "Escape") { event.preventDefault(); close.current(); }
      if (event.key !== "Tab") return;
      const all = items(); const first = all[0]; const last = all.at(-1);
      if (!first) { event.preventDefault(); ref.current?.focus(); }
      else if (event.shiftKey && (document.activeElement === first || document.activeElement === ref.current)) { event.preventDefault(); last?.focus(); }
      else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first.focus(); }
    };
    document.addEventListener("keydown", key);
    return () => {
      document.removeEventListener("keydown", key);
      if (shell) shell.inert = inert;
      document.body.style.overflow = overflow;
      if (previous?.isConnected) previous.focus();
    };
  }, []);
  if (typeof document === "undefined") return null;
  return createPortal(<div className={`qf-modal-layer ${drawer ? "is-drawer" : ""}`} onMouseDown={(event) => { if (event.target === event.currentTarget) close.current(); }}><div ref={ref} role="dialog" aria-modal="true" aria-labelledby={labelId} tabIndex={-1} className={drawer ? "qf-inspector" : "qf-palette"}>{children}</div></div>, document.body);
}
