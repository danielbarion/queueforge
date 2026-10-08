"use client";

import { create } from "zustand";
import { persist } from "zustand/middleware";

export type BrokerKind = "rust" | "bun" | "php" | "rabbitmq";

export type BrokerTarget = {
  id: string;
  name: string;
  kind: BrokerKind;
  url: string;
};

type BrokerState = {
  targets: BrokerTarget[];
  selectedId: string;
  select: (id: string) => void;
  add: (broker: Omit<BrokerTarget, "id">) => void;
  remove: (id: string) => void;
};

export const useBrokerStore = create<BrokerState>()(
  persist(
    (set) => ({
      targets: [],
      selectedId: "",
      select: (id) => set({ selectedId: id }),
      add: (broker) =>
        set((state) => {
          const next = { ...broker, id: crypto.randomUUID() };
          return { targets: [...state.targets, next], selectedId: next.id };
        }),
      remove: (id) =>
        set((state) => {
          const targets = state.targets.filter((target) => target.id !== id);
          const selectedId = state.selectedId === id ? (targets[0]?.id ?? "") : state.selectedId;
          return { targets, selectedId };
        }),
    }),
    { name: "queueforge-console-brokers" },
  ),
);

export function selectedBroker(state: BrokerState): BrokerTarget | null {
  return state.targets.find((target) => target.id === state.selectedId) ?? null;
}
