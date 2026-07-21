import { NavLink } from "react-router-dom";
import type { Whoami } from "../api";
import { logout } from "../api";

const NAV = [
  { to: "/", label: "Overview", end: true },
  { to: "/queues", label: "Queues" },
  { to: "/exchanges", label: "Exchanges" },
  { to: "/bindings", label: "Bindings" },
  { to: "/users", label: "Users" },
  { to: "/connections", label: "Connections" },
  { to: "/publish", label: "Publish / Get" },
  { to: "/definitions", label: "Definitions" },
];

type Props = {
  user: Whoami;
  onLoggedOut: () => void;
  children: React.ReactNode;
  title: string;
  actions?: React.ReactNode;
};

export default function Layout({ user, onLoggedOut, children, title, actions }: Props) {
  async function onLogout() {
    try {
      await logout();
    } catch {
      /* ignore */
    }
    onLoggedOut();
  }

  return (
    <div className="layout">
      <header className="topbar">
        <div className="brand">QueueForge</div>
        <nav className="nav">
          {NAV.map((item) => (
            <NavLink
              key={item.to}
              to={item.to}
              end={item.end}
              className={({ isActive }) => (isActive ? "nav-link active" : "nav-link")}
            >
              {item.label}
            </NavLink>
          ))}
        </nav>
        <div className="topbar-right">
          <span className="muted">
            {user.name}
            {user.tags.length > 0 && (
              <span className="tag-list"> · {user.tags.join(", ")}</span>
            )}
          </span>
          <button type="button" className="btn-secondary" onClick={() => void onLogout()}>
            Log out
          </button>
        </div>
      </header>
      <main className="content">
        <div className="page-header">
          <h1>{title}</h1>
          <div className="page-actions">{actions}</div>
        </div>
        {children}
      </main>
    </div>
  );
}
