"use client";

import { create } from "zustand";
import { persist } from "zustand/middleware";
import { DEMO_PROFILES } from "@/lib/demo";

export type BrokerKind = "rust" | "bun" | "php" | "rabbitmq";

export type BrokerTarget = {
  id: string;
  name: string;
  kind: BrokerKind;
  url: string;
};

type BrokerState = {
  targets: BrokerTarget[];
  demo: boolean;
  demoKind: BrokerKind;
  setDemo: (value: boolean, kind?: BrokerKind) => void;
  selectedId: string;
  select: (id: string) => void;
  add: (broker: Omit<BrokerTarget, "id">) => void;
  remove: (id: string) => void;
};

export const useBrokerStore = create<BrokerState>()(
  persist(
    (set) => ({
      targets: [],
      demo: false,
      demoKind: "bun",
      setDemo: (demo, demoKind) => set((state) => ({ demo, demoKind: demoKind ?? state.demoKind })),
      selectedId: "",
      select: (id) => set({ selectedId: id, demo: false }),
      add: (broker) =>
        set((state) => {
          const next = { ...broker, id: crypto.randomUUID() };
          return { targets: [...state.targets, next], selectedId: next.id, demo: false };
        }),
      remove: (id) =>
        set((state) => {
          const targets = state.targets.filter((target) => target.id !== id);
          const selectedId = state.selectedId === id ? (targets[0]?.id ?? "") : state.selectedId;
          return { targets, selectedId };
        }),
    }),
    { name: "queueforge-console-brokers", partialize: ({ targets, selectedId }) => ({ targets, selectedId }) },
  ),
);

export function selectedBroker(state: BrokerState): BrokerTarget | null {
  if (state.demo) return DEMO_PROFILES.find((target) => target.kind === state.demoKind) ?? DEMO_PROFILES[1];
  return state.targets.find((target) => target.id === state.selectedId) ?? null;
}
