"use client";

import { useEffect } from "react";
import { isDemo } from "@/lib/demo";
import { callBroker, sessionBroker } from "@/lib/client";
import { selectedBroker, useBrokerStore } from "@/stores/broker";
import { useLiveStore } from "@/stores/live";

const QUEUE_PATH = "/api/queues/%2F?page_size=500";

export function Poller() {
  const broker = useBrokerStore(selectedBroker);
  const nonce = useLiveStore((state) => state.nonce);
  const reset = useLiveStore((state) => state.reset);
  const setAuthed = useLiveStore((state) => state.setAuthed);
  const markStale = useLiveStore((state) => state.markStale);
  const applyPublic = useLiveStore((state) => state.applyPublic);
  const applySample = useLiveStore((state) => state.applySample);
  const id = broker?.id ?? "";
  const url = broker?.url ?? "";

  useEffect(() => {
    reset(id);
    if (isDemo(url)) {
      const at = Date.now();
      useLiveStore.setState({ history: Array.from({ length: 100 }, (_, i) => ({ at: at - (100 - i) * 3000, publish: 120, deliver: 112, ack: 108 })) });
    }
  }, [id, url, reset]);

  useEffect(() => {
    if (!url) return;
    let stop = false;
    let running = false;

    async function tick() {
      if (stop || running || document.hidden) return;
      running = true;
      try {
        const authed = await sessionBroker(url);
        if (stop) return;
        setAuthed(authed);
        const health = await callBroker(url, "GET", "/healthz");
        const ready = await callBroker(url, "GET", "/readyz");
        if (stop) return;
        // RabbitMQ has no /healthz or /readyz; a 404 there is not a failure.
        const healthy = health.status === 200 || health.status === 404;
        applyPublic(healthy, ready.status === 200 || (ready.status === 404 && healthy));
        if (!healthy) {
          markStale("Health did not answer.");
          return;
        }
        if (!authed) return;
        const overview = await callBroker(url, "GET", "/api/overview");
        if (overview.status === 401) {
          setAuthed(false);
          markStale("The session ended. Log in again.");
          return;
        }
        if (overview.status !== 200) {
          markStale("Overview did not answer.");
          return;
        }
        const queues = await callBroker(url, "GET", QUEUE_PATH);
        if (queues.status !== 200) {
          markStale("The queue list did not answer.");
          return;
        }
        const metrics = await callBroker(url, "GET", "/metrics");
        if (stop) return;
        applySample(overview.body, queues.body, metrics.status === 200 ? metrics.text : null, Date.now());
      } catch (error) {
        if (!stop) markStale(error instanceof Error ? error.message : "Could not reach that address.");
      } finally {
        running = false;
      }
    }

    void tick();
    const timer = setInterval(() => void tick(), 3000);
    return () => {
      stop = true;
      clearInterval(timer);
    };
  }, [id, url, nonce, setAuthed, markStale, applyPublic, applySample]);

  return null;
}
