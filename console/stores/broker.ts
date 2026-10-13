"use client";

import { create } from "zustand";
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
  add: (broker: BrokerTarget) => void;
  remove: (id: string) => void;
  hydrate: (targets: BrokerTarget[], selectedId: string) => void;
};

export const useBrokerStore = create<BrokerState>()((set) => ({
  targets: [],
  demo: false,
  demoKind: "bun",
  setDemo: (demo, demoKind) => set((state) => ({ demo, demoKind: demoKind ?? state.demoKind })),
  selectedId: "",
  select: (id) => {
    set({ selectedId: id, demo: false });
    void fetch(`/api/brokers/targets/${id}/select`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: "{}",
    });
  },
  add: (broker) => set({ targets: [...useBrokerStore.getState().targets, broker], selectedId: broker.id, demo: false }),
  remove: (id) => {
    const state = useBrokerStore.getState();
    const targets = state.targets.filter((target) => target.id !== id);
    const selectedId = state.selectedId === id ? (targets[0]?.id ?? "") : state.selectedId;
    set({ targets, selectedId });
    void fetch(`/api/brokers/targets/${id}`, { method: "DELETE" });
  },
  hydrate: (targets, selectedId) => set({ targets, selectedId }),
}));

export function selectedBroker(state: BrokerState): BrokerTarget | null {
  if (state.demo) return DEMO_PROFILES.find((target) => target.kind === state.demoKind) ?? DEMO_PROFILES[1];
  return state.targets.find((target) => target.id === state.selectedId) ?? null;
}
