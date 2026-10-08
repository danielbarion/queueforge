"use client";

import { useEffect } from "react";
import { useAlertStore } from "@/stores/alerts";

export function Toasts() {
  const toasts = useAlertStore((state) => state.toasts);
  const dismiss = useAlertStore((state) => state.dismiss);

  useEffect(() => {
    if (toasts.length === 0) return;
    const timer = setTimeout(() => dismiss(toasts[toasts.length - 1].id), 6000);
    return () => clearTimeout(timer);
  }, [toasts, dismiss]);

  if (toasts.length === 0) return null;
  return (
    <div className="fixed bottom-4 right-4 z-50 flex w-80 flex-col gap-2">
      {toasts.map((toast) => (
        <button key={toast.id} type="button" className="rounded-box border border-base-300 bg-base-200 px-4 py-3 text-left text-sm shadow-none" onClick={() => dismiss(toast.id)}>
          {toast.text}
        </button>
      ))}
    </div>
  );
}
