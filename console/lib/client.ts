export type BrokerCall = {
  status: number;
  body: unknown;
  text: string;
};

export async function callBroker(url: string, method: "GET" | "POST" | "PUT" | "DELETE", path: string, body?: unknown): Promise<BrokerCall> {
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
  await fetch("/api/brokers/logout", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ url }),
  });
}

export async function sessionBroker(url: string): Promise<boolean> {
  const response = await fetch(`/api/brokers/session?url=${encodeURIComponent(url)}`);
  if (!response.ok) return false;
  const payload = (await response.json()) as { loggedIn?: boolean };
  return payload.loggedIn === true;
}
