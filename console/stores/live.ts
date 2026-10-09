"use client";

import { create } from "zustand";
import {
  diffActivity,
  parseOverview,
  parseQueues,
  ratesFrom,
  totals,
  type ActivityEvent,
  type OverviewSnap,
  type QueueSnap,
  type Rates,
} from "@/lib/live-diff";
import { sumConfirmBeforeFsync } from "@/lib/broker-http";
import { useBrokerStore } from "@/stores/broker";
import { useAlertStore } from "@/stores/alerts";

const EMPTY_RATES: Rates = { publish: null, deliver: null, ack: null };
const ACTIVITY_LIMIT = 100;
export const RATE_HISTORY_MS = 5 * 60 * 1000;
const HISTORY_LIMIT = 2048;
export type RateSample = Rates & { at: number };
type CounterAvailability = Record<keyof Rates, boolean>;
const NO_COUNTERS: CounterAvailability = { publish: false, deliver: false, ack: false };

export type LiveState = {
  brokerId: string;
  authed: boolean | null;
  health: boolean | null;
  ready: boolean | null;
  stale: boolean;
  error: string | null;
  sampledAt: number | null;
  overview: OverviewSnap | null;
  queues: QueueSnap[];
  rates: Rates;
  history: RateSample[];
  counterAvailability: CounterAvailability;
  activity: ActivityEvent[];
  nonce: number;
  reset: (brokerId: string) => void;
  setAuthed: (authed: boolean) => void;
  markStale: (error: string) => void;
  applyPublic: (health: boolean, ready: boolean) => void;
  applySample: (overviewBody: unknown, queueBody: unknown, metrics: string | null, at: number) => void;
  nudge: () => void;
};

export const useLiveStore = create<LiveState>((set, get) => ({
  brokerId: "",
  authed: null,
  health: null,
  ready: null,
  stale: false,
  error: null,
  sampledAt: null,
  overview: null,
  queues: [],
  rates: EMPTY_RATES,
  history: [],
  counterAvailability: NO_COUNTERS,
  activity: [],
  nonce: 0,
  reset: (brokerId) =>
    set({
      brokerId,
      authed: null,
      health: null,
      ready: null,
      stale: false,
      error: null,
      sampledAt: null,
      overview: null,
      queues: [],
      rates: EMPTY_RATES,
      history: [],
      counterAvailability: NO_COUNTERS,
      activity: [],
    }),
  setAuthed: (authed) => set({ authed }),
  markStale: (error) => set({ stale: true, error }),
  applyPublic: (health, ready) => set({ health, ready, error: null }),
  applySample: (overviewBody, queueBody, metrics, at) => {
    const overview = parseOverview(overviewBody);
    if (!overview) {
      set({ stale: true, error: "The overview was not a QueueForge payload." });
      return;
    }
    const queues = parseQueues(queueBody);
    const state = get();
    const stats = (overviewBody as { message_stats?: Record<string, unknown> }).message_stats;
    const counterAvailability = Object.fromEntries(
      (["publish", "deliver", "ack"] as const).map((key) => [key, typeof stats?.[key] === "number" && Number.isFinite(stats[key])]),
    ) as CounterAvailability;
    const measured = state.overview && state.sampledAt !== null && !state.stale
      ? ratesFrom(state.overview, overview, at - state.sampledAt)
      : EMPTY_RATES;
    const rates = Object.fromEntries(
      (["publish", "deliver", "ack"] as const).map((key) => [key, counterAvailability[key] && state.counterAvailability[key] ? measured[key] : null]),
    ) as Rates;
    const history = [...state.history.filter((sample) => sample.at >= at - RATE_HISTORY_MS && sample.at < at), { at, ...rates }].slice(-HISTORY_LIMIT);
    const events =
      state.overview === null
        ? []
        : diffActivity(state.queues, queues, state.overview.connections, overview.connections, at);
    const counts = totals(queues);
    if (!useBrokerStore.getState().demo) useAlertStore.getState().evaluate(
      { ready: counts.ready, unacked: counts.unacked, fsync: metrics === null ? null : sumConfirmBeforeFsync(metrics), at },
      false,
    );
    set({
      overview,
      queues,
      rates,
      history,
      counterAvailability,
      health: true,
      stale: false,
      error: null,
      sampledAt: at,
      activity: [...events, ...state.activity].slice(0, ACTIVITY_LIMIT),
    });
  },
  nudge: () => set((state) => ({ nonce: state.nonce + 1 })),
}));

export function formatCount(value: number): string {
  return new Intl.NumberFormat("en-US").format(value);
}

export function formatRate(value: number | null): string {
  if (value === null || !Number.isFinite(value)) return "—";
  if (Math.abs(value) >= 100) return formatCount(Math.round(value));
  return value.toFixed(1);
}
