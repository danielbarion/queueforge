import { FormEvent, useState } from "react";
import { ApiError, login, type Whoami } from "../api";

type Props = {
  onLoggedIn: (user: Whoami) => void;
};

export default function LoginPage({ onLoggedIn }: Props) {
  const [username, setUsername] = useState("admin");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  async function onSubmit(e: FormEvent) {
    e.preventDefault();
    setError(null);
    setBusy(true);
    try {
      const me = await login(username.trim(), password);
      onLoggedIn(me);
    } catch (err) {
      if (err instanceof ApiError) {
        if (err.status === 401) setError("Invalid username or password");
        else if (err.status === 403)
          setError("User is not allowed to use management");
        else if (err.status === 429)
          setError("Too many failed login attempts — try again later");
        else setError(err.message);
      } else {
        setError("Network error — is the management server running?");
      }
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="centered">
      <form className="card login-card" onSubmit={onSubmit}>
        <h1>QueueForge</h1>
        <p className="muted">Sign in to the management console</p>
        {error && <div className="error">{error}</div>}
        <label>
          Username
          <input
            autoComplete="username"
            value={username}
            onChange={(e) => setUsername(e.target.value)}
            disabled={busy}
            required
          />
        </label>
        <label>
          Password
          <input
            type="password"
            autoComplete="current-password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            disabled={busy}
            required
          />
        </label>
        <button type="submit" disabled={busy}>
          {busy ? "Signing in…" : "Sign in"}
        </button>
      </form>
    </div>
  );
}
