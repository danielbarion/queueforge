import { useCallback, useEffect, useState } from "react";
import { Navigate, Route, Routes, useNavigate } from "react-router-dom";
import { whoami, type Whoami } from "./api";
import LoginPage from "./pages/Login";
import OverviewPage from "./pages/Overview";
import QueuesPage from "./pages/Queues";
import ExchangesPage from "./pages/Exchanges";
import BindingsPage from "./pages/Bindings";
import UsersPage from "./pages/Users";
import ConnectionsPage from "./pages/Connections";
import DefinitionsPage from "./pages/Definitions";
import PublishGetPage from "./pages/PublishGet";

export default function App() {
  const [user, setUser] = useState<Whoami | null | undefined>(undefined);
  const navigate = useNavigate();

  const refreshSession = useCallback(async () => {
    try {
      const me = await whoami();
      setUser(me);
      return me;
    } catch {
      setUser(null);
      return null;
    }
  }, []);

  useEffect(() => {
    void refreshSession();
  }, [refreshSession]);

  const onLoggedIn = (me: Whoami) => {
    setUser(me);
    navigate("/", { replace: true });
  };

  const onLoggedOut = () => {
    setUser(null);
    navigate("/login", { replace: true });
  };

  if (user === undefined) {
    return (
      <div className="centered">
        <p className="muted">Loading…</p>
      </div>
    );
  }

  const guard = (el: React.ReactNode) =>
    user ? el : <Navigate to="/login" replace />;

  return (
    <Routes>
      <Route
        path="/login"
        element={
          user ? <Navigate to="/" replace /> : <LoginPage onLoggedIn={onLoggedIn} />
        }
      />
      <Route
        path="/"
        element={guard(<OverviewPage user={user!} onLoggedOut={onLoggedOut} />)}
      />
      <Route
        path="/queues"
        element={guard(<QueuesPage user={user!} onLoggedOut={onLoggedOut} />)}
      />
      <Route
        path="/exchanges"
        element={guard(<ExchangesPage user={user!} onLoggedOut={onLoggedOut} />)}
      />
      <Route
        path="/bindings"
        element={guard(<BindingsPage user={user!} onLoggedOut={onLoggedOut} />)}
      />
      <Route
        path="/users"
        element={guard(<UsersPage user={user!} onLoggedOut={onLoggedOut} />)}
      />
      <Route
        path="/connections"
        element={guard(<ConnectionsPage user={user!} onLoggedOut={onLoggedOut} />)}
      />
      <Route
        path="/publish"
        element={guard(<PublishGetPage user={user!} onLoggedOut={onLoggedOut} />)}
      />
      <Route
        path="/definitions"
        element={guard(<DefinitionsPage user={user!} onLoggedOut={onLoggedOut} />)}
      />
      <Route path="*" element={<Navigate to={user ? "/" : "/login"} replace />} />
    </Routes>
  );
}
