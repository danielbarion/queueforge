import { useBrokerStore } from "../stores/broker";
import { demoCall, isDemo } from "./demo";

export type BrokerCall = {
  status: number;
  body: unknown;
  text: string;
};

export async function callBroker(url: string, method: "GET" | "POST" | "PUT" | "DELETE", path: string, body?: unknown): Promise<BrokerCall> {
  if (isDemo(url)) return demoCall(url, method, path, body);
  if (useBrokerStore.getState().demo) throw new Error("Exit demo mode to contact a configured broker.");
  const response = await fetch("/api/brokers/call", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ url, method, path, body }),
  });
  const payload = (await response.json()) as BrokerCall & { error?: string };
  if (!response.ok) throw new Error(payload.error ?? "Could not reach that address.");
  return { status: payload.status, body: payload.body, text: payload.text ?? "" };
}

export async function loginBroker(url: string, username: string, password: string): Promise<string> {
  if (isDemo(url)) return "Demo operator";
  if (useBrokerStore.getState().demo) throw new Error("Exit demo mode to log in to a broker.");
  const response = await fetch("/api/brokers/login", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ url, username, password }),
  });
  const payload = (await response.json()) as { name?: string; error?: string };
  if (!response.ok) throw new Error(payload.error ?? "The broker refused the login.");
  return payload.name ?? username;
}

export async function logoutBroker(url: string): Promise<void> {
  if (isDemo(url) || useBrokerStore.getState().demo) return;
  await fetch("/api/brokers/logout", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ url }),
  });
}

export async function sessionBroker(url: string): Promise<boolean> {
  if (isDemo(url)) return true;
  if (useBrokerStore.getState().demo) return false;
  const response = await fetch(`/api/brokers/session?url=${encodeURIComponent(url)}`);
  if (!response.ok) return false;
  const payload = (await response.json()) as { loggedIn?: boolean };
  return payload.loggedIn === true;
}
