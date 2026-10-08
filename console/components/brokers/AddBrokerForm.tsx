"use client";

import { FormEvent, useState } from "react";
import { Field } from "@/components/ui/Field";
import { useBrokerStore, type BrokerKind } from "@/stores/broker";

export function AddBrokerForm() {
  const add = useBrokerStore((state) => state.add);
  const [name, setName] = useState("");
  const [url, setUrl] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [pending, setPending] = useState(false);

  async function onSubmit(event: FormEvent) {
    event.preventDefault();
    const trimmedName = name.trim();
    if (!trimmedName) {
      setError("Name the broker.");
      return;
    }
    setPending(true);
    setError(null);
    try {
      const response = await fetch("/api/brokers/identify", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ url }),
      });
      const json = (await response.json()) as { kind?: BrokerKind; url?: string; error?: string };
      if (!response.ok || !json.kind || !json.url) {
        setError(json.error ?? "Could not tell which broker that is.");
        return;
      }
      add({ name: trimmedName, kind: json.kind, url: json.url });
      setName("");
      setUrl("");
    } catch {
      setError("Could not reach that address.");
    } finally {
      setPending(false);
    }
  }

  return (
    <form onSubmit={onSubmit} className="flex flex-col gap-5">
      <Field label="Name">
        <input className="input input-bordered w-full bg-base-100" value={name} onChange={(event) => setName(event.target.value)} placeholder="Orders" />
      </Field>
      <Field label="Management URL">
        <input
          className="input input-bordered w-full bg-base-100 font-mono text-sm"
          value={url}
          onChange={(event) => setUrl(event.target.value)}
          placeholder="http://10.0.0.12:15672"
          inputMode="url"
        />
      </Field>
      <p className="text-sm text-muted">The console asks the broker which implementation it is. You do not pick Rust, Bun, or PHP.</p>
      {error && <p className="text-sm text-error">{error}</p>}
      <button type="submit" className="btn btn-primary mt-1 w-fit" disabled={pending}>
        {pending ? "Checking…" : "Add broker"}
      </button>
    </form>
  );
}
