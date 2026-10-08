"use client";

import { create } from "zustand";
import { persist } from "zustand/middleware";
import { EMPTY_ALERT_MEMORY, nextAlerts, type AlertMemory, type AlertRules } from "@/lib/live-diff";

export type Toast = { id: string; text: string };

type AlertState = AlertRules & {
  memory: AlertMemory;
  toasts: Toast[];
  setReady: (enabled: boolean, over: number) => void;
  setUnacked: (enabled: boolean, over: number) => void;
  setFsync: (enabled: boolean) => void;
  evaluate: (sample: { ready: number; unacked: number; fsync: number | null; at: number }, stale: boolean) => void;
  push: (text: string) => void;
  dismiss: (id: string) => void;
};

export const useAlertStore = create<AlertState>()(
  persist(
    (set, get) => ({
      readyEnabled: false,
      readyOver: 1000,
      unackedEnabled: false,
      unackedOver: 100,
      fsyncEnabled: false,
      cooldownMs: 60_000,
      memory: EMPTY_ALERT_MEMORY,
      toasts: [],
      setReady: (enabled, over) =>
        set((state) => ({ readyEnabled: enabled, readyOver: Number.isFinite(over) && over >= 0 ? over : state.readyOver })),
      setUnacked: (enabled, over) =>
        set((state) => ({ unackedEnabled: enabled, unackedOver: Number.isFinite(over) && over >= 0 ? over : state.unackedOver })),
      setFsync: (enabled) => set({ fsyncEnabled: enabled }),
      evaluate: (sample, stale) => {
        const state = get();
        const rules: AlertRules = {
          readyEnabled: state.readyEnabled,
          readyOver: state.readyOver,
          unackedEnabled: state.unackedEnabled,
          unackedOver: state.unackedOver,
          fsyncEnabled: state.fsyncEnabled,
          cooldownMs: state.cooldownMs,
        };
        const result = nextAlerts(rules, state.memory, sample, stale);
        const toasts = result.fired.map((text) => ({ id: crypto.randomUUID(), text }));
        set({ memory: result.memory, toasts: [...toasts, ...state.toasts].slice(0, 4) });
      },
      push: (text) => set((state) => ({ toasts: [{ id: crypto.randomUUID(), text }, ...state.toasts].slice(0, 4) })),
      dismiss: (id) => set((state) => ({ toasts: state.toasts.filter((toast) => toast.id !== id) })),
    }),
    {
      name: "queueforge-console-alerts",
      partialize: (state) => ({
        readyEnabled: state.readyEnabled,
        readyOver: state.readyOver,
        unackedEnabled: state.unackedEnabled,
        unackedOver: state.unackedOver,
        fsyncEnabled: state.fsyncEnabled,
        cooldownMs: state.cooldownMs,
      }),
    },
  ),
);
