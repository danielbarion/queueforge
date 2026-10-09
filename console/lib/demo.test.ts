import { expect, test } from "bun:test";
import { demoCall, DEMO_PROFILES } from "./demo";
import { callBroker, loginBroker, logoutBroker, sessionBroker } from "./client";
import { parseMessages } from "./messages";
import { selectedBroker, useBrokerStore } from "../stores/broker";

test("demo snapshots preserve payload bytes and reject mutation endpoints", () => {
  for (const target of DEMO_PROFILES) {
    const response = demoCall(target.url, "POST", "/api/queues/%2F/orders.dead-letter/get", { encoding: "base64" });
    const messages = parseMessages(response.body);
    expect(messages?.[0]?.complete).toBe(true);
    expect(demoCall(target.url, "DELETE", "/api/queues/%2F/orders").status).toBe(403);
    expect(demoCall(target.url, "POST", "/api/exchanges/%2F/orders/publish").status).toBe(403);
  }
});

test("demo isolates real profiles and performs no network calls, including obsolete real-target calls", async () => {
  const state = useBrokerStore.getState(); const original = globalThis.fetch;
  let calls = 0;
  globalThis.fetch = (async () => { calls++; throw new Error("Unexpected network"); }) as typeof fetch;
  const real = { id: "saved", name: "Saved", kind: "rust" as const, url: "http://broker.test:15672" };
  try {
    useBrokerStore.setState({ targets: [real], selectedId: real.id, demo: true, demoKind: "bun" });
    expect(selectedBroker(useBrokerStore.getState())?.url).toBe("demo://bun");
    expect((await callBroker("demo://bun", "GET", "/api/overview")).status).toBe(200);
    expect(await sessionBroker("demo://bun")).toBe(true);
    expect(await loginBroker("demo://bun", "", "")).toBe("Demo operator");
    await expect(callBroker(real.url, "DELETE", "/api/queues/%2F/orders")).rejects.toThrow("Exit demo");
    await expect(loginBroker(real.url, "", "")).rejects.toThrow("Exit demo");
    expect(await sessionBroker(real.url)).toBe(false); await logoutBroker(real.url);
    expect(calls).toBe(0);
    useBrokerStore.getState().setDemo(false);
    expect(selectedBroker(useBrokerStore.getState())).toEqual(real);
    expect(useBrokerStore.getState().targets).toEqual([real]);
  } finally { globalThis.fetch = original; useBrokerStore.setState(state); }
});
