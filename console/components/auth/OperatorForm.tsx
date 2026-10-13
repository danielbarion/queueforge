"use client";

import { FormEvent, useState } from "react";
import { useRouter } from "next/navigation";
import { Field } from "@/components/ui/Field";
import { safeNext } from "@/lib/operator";

export function OperatorForm({ next }: { next: string }) {
  const router = useRouter();
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [pending, setPending] = useState(false);

  async function onSubmit(event: FormEvent) {
    event.preventDefault();
    setPending(true);
    setError(null);
    try {
      const response = await fetch("/api/operator/login", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ username: username.trim(), password }),
      });
      const payload = (await response.json().catch(() => ({}))) as { error?: string };
      if (!response.ok) {
        setError(payload.error ?? "Those credentials were refused.");
        return;
      }
      setPassword("");
      router.replace(safeNext(next));
      router.refresh();
    } catch {
      setError("Could not reach the console.");
    } finally {
      setPending(false);
    }
  }

  return (
    <form onSubmit={onSubmit} className="flex flex-col gap-5">
      <Field label="User">
        <input className="input input-bordered w-full bg-base-100" value={username} onChange={(event) => setUsername(event.target.value)} autoComplete="username" autoFocus />
      </Field>
      <Field label="Password">
        <input className="input input-bordered w-full bg-base-100" type="password" value={password} onChange={(event) => setPassword(event.target.value)} autoComplete="current-password" />
      </Field>
      {error && <p className="text-sm text-error">{error}</p>}
      <button type="submit" className="btn btn-primary w-fit" disabled={pending}>
        {pending ? "Checking…" : "Log in"}
      </button>
    </form>
  );
}
