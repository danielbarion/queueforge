import { useState } from "react";
import {
  ApiError,
  exportDefinitions,
  importDefinitions,
  type Whoami,
} from "../api";
import Layout from "../components/Layout";

type Props = { user: Whoami; onLoggedOut: () => void };

export default function DefinitionsPage({ user, onLoggedOut }: Props) {
  const [jsonText, setJsonText] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [status, setStatus] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  async function onExport() {
    setError(null);
    setStatus(null);
    setBusy(true);
    try {
      const data = await exportDefinitions();
      setJsonText(JSON.stringify(data, null, 2));
      setStatus("Exported definitions into the editor.");
    } catch (err) {
      if (err instanceof ApiError && err.status === 401) {
        onLoggedOut();
        return;
      }
      setError(err instanceof Error ? err.message : "Export failed");
    } finally {
      setBusy(false);
    }
  }

  async function onImport() {
    setError(null);
    setStatus(null);
    let parsed: unknown;
    try {
      parsed = JSON.parse(jsonText);
    } catch {
      setError("Editor content is not valid JSON.");
      return;
    }
    if (!window.confirm("Import definitions? Existing topology may be merged/upserted.")) {
      return;
    }
    setBusy(true);
    try {
      await importDefinitions(parsed);
      setStatus("Import completed.");
    } catch (err) {
      if (err instanceof ApiError && err.status === 401) {
        onLoggedOut();
        return;
      }
      setError(err instanceof Error ? err.message : "Import failed");
    } finally {
      setBusy(false);
    }
  }

  function onDownload() {
    if (!jsonText.trim()) return;
    const blob = new Blob([jsonText], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "queueforge-definitions.json";
    a.click();
    URL.revokeObjectURL(url);
  }

  return (
    <Layout
      user={user}
      onLoggedOut={onLoggedOut}
      title="Definitions"
      actions={
        <div className="row-actions">
          <button
            type="button"
            className="btn-secondary"
            disabled={busy}
            onClick={() => void onExport()}
          >
            Export
          </button>
          <button
            type="button"
            className="btn-secondary"
            disabled={busy || !jsonText.trim()}
            onClick={onDownload}
          >
            Download
          </button>
          <button
            type="button"
            className="btn-primary"
            disabled={busy || !jsonText.trim()}
            onClick={() => void onImport()}
          >
            Import
          </button>
        </div>
      }
    >
      {error && <div className="error">{error}</div>}
      {status && <div className="ok">{status}</div>}
      <p className="muted">
        Export broker topology (users, vhosts, exchanges, queues, bindings) as JSON,
        edit if needed, then import. Import requires the administrator tag.
      </p>
      <textarea
        className="definitions-editor"
        spellCheck={false}
        placeholder='Click "Export" or paste definitions JSON…'
        value={jsonText}
        onChange={(e) => setJsonText(e.target.value)}
      />
    </Layout>
  );
}
