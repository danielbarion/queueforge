import { NavLink } from "react-router-dom";
import type { Whoami } from "../api";
import { logout } from "../api";

const OPERATE = [
  { to: "/", label: "Overview", end: true },
  { to: "/connections", label: "Connections" },
  { to: "/channels", label: "Channels" },
  { to: "/exchanges", label: "Exchanges" },
  { to: "/queues", label: "Queues" },
];

const ADMIN = [
  { to: "/users", label: "Users" },
  { to: "/vhosts", label: "Virtual Hosts" },
  { to: "/feature-flags", label: "Feature Flags" },
  { to: "/deprecated-features", label: "Deprecated Features" },
  { to: "/policies", label: "Policies" },
  { to: "/limits", label: "Limits" },
  { to: "/cluster", label: "Cluster" },
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
      /* the session is already gone */
    }
    onLoggedOut();
  }

  return (
    <div className="layout">
      <aside className="rail">
        <div className="brand">
          <strong>QueueForge</strong>
          <span>operations console</span>
        </div>
        <div className="rail-scroll">
          <div>
            <div className="rail-label">Operate</div>
            <div className="rail-group">
              {OPERATE.map((item) => (
                <NavLink key={item.to} to={item.to} end={item.end} className={({ isActive }) => (isActive ? "nav-link active" : "nav-link")}>
                  {item.label}
                </NavLink>
              ))}
            </div>
          </div>
          <div>
            <div className="rail-label">Admin</div>
            <div className="rail-group">
              {ADMIN.map((item) => (
                <NavLink key={item.to} to={item.to} className={({ isActive }) => (isActive ? "nav-link active" : "nav-link")}>
                  {item.label}
                </NavLink>
              ))}
            </div>
          </div>
        </div>
        <div className="rail-foot">
          <span className="muted">{user.name}</span>
          <button type="button" className="btn-secondary" onClick={() => void onLogout()}>Log out</button>
        </div>
      </aside>
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
