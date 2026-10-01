import { useCallback, useEffect, useState } from "react";
import { Link } from "react-router-dom";
import {
  ApiError,
  createUser,
  deleteUser,
  listUsers,
  type UserItem,
  type Whoami,
} from "../api";
import Layout from "../components/Layout";

type Props = { user: Whoami; onLoggedOut: () => void };

export default function UsersPage({ user, onLoggedOut }: Props) {
  const [items, setItems] = useState<UserItem[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [name, setName] = useState("");
  const [password, setPassword] = useState("");
  const [tags, setTags] = useState("management");

  const load = useCallback(async () => {
    setError(null);
    try {
      const items = await listUsers();
      setItems(items);
    } catch (err) {
      if (err instanceof ApiError && err.status === 401) {
        onLoggedOut();
        return;
      }
      setError(err instanceof Error ? err.message : "Failed to load users");
    }
  }, [onLoggedOut]);

  useEffect(() => {
    void load();
  }, [load]);

  async function onCreate(e: React.FormEvent) {
    e.preventDefault();
    try {
      await createUser(
        name.trim(),
        password,
        tags
          .split(",")
          .map((t) => t.trim())
          .filter(Boolean),
      );
      setName("");
      setPassword("");
      await load();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Create failed");
    }
  }

  return (
    <Layout
      user={user}
      onLoggedOut={onLoggedOut}
      title="Users"
      actions={
        <button type="button" className="btn-secondary" onClick={() => void load()}>
          Refresh
        </button>
      }
    >
      {error && <div className="error">{error}</div>}

      <form className="inline-form card" onSubmit={(e) => void onCreate(e)}>
        <strong>Create user</strong>
        <input
          placeholder="username"
          value={name}
          onChange={(e) => setName(e.target.value)}
        />
        <input
          type="password"
          placeholder="password"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
        />
        <input
          placeholder="tags (comma-separated)"
          value={tags}
          onChange={(e) => setTags(e.target.value)}
        />
        <button type="submit" className="btn-primary">
          Create
        </button>
      </form>

      <div className="table-wrap card">
        <table>
          <thead>
            <tr>
              <th>Name</th>
              <th>Tags</th>
              <th />
            </tr>
          </thead>
          <tbody>
            {items.map((u) => (
              <tr key={u.name}>
                <td>
                  <Link to={`/users/${encodeURIComponent(u.name)}`}><code>{u.name}</code></Link>
                </td>
                <td className="muted">{u.tags.join(", ") || "—"}</td>
                <td className="row-actions">
                  {u.name !== user.name && (
                    <button
                      type="button"
                      className="btn-danger"
                      onClick={() =>
                        void deleteUser(u.name)
                          .then(load)
                          .catch((e) => setError(String(e.message ?? e)))
                      }
                    >
                      Delete
                    </button>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </Layout>
  );
}
