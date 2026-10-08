"use client";

import { FormEvent, useState } from "react";
import { Field } from "@/components/ui/Field";
import { loginBroker } from "@/lib/client";
import { useLiveStore } from "@/stores/live";

export function LoginForm({ url }: { url: string }) {
  const nudge = useLiveStore((state) => state.nudge);
  const setAuthed = useLiveStore((state) => state.setAuthed);
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [pending, setPending] = useState(false);

  async function onSubmit(event: FormEvent) {
    event.preventDefault();
    setPending(true);
    setError(null);
    try {
      await loginBroker(url, username.trim(), password);
      setPassword("");
      setAuthed(true);
      nudge();
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "The broker refused the login.");
    } finally {
      setPending(false);
    }
  }

  return (
    <form onSubmit={onSubmit} className="flex max-w-md flex-col gap-5">
      <Field label="User">
        <input className="input input-bordered w-full bg-base-100" value={username} onChange={(event) => setUsername(event.target.value)} autoComplete="username" />
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
