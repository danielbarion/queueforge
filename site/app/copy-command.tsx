"use client";

import { useState } from "react";

export function CopyCommand({ text, label = "shell" }: { text: string; label?: string }) {
  const [state, setState] = useState("Copy");

  function flash(next: string) {
    setState(next);
    window.setTimeout(() => setState("Copy"), 1600);
  }

  return (
    <div className="cmd">
      <div className="cmd-head">
        <span>{label}</span>
        <button
          className="copy"
          type="button"
          onClick={() => {
            const write = navigator.clipboard?.writeText;
            if (!write) {
              flash("Failed");
              return;
            }
            write.call(navigator.clipboard, text).then(
              () => flash("Copied"),
              () => flash("Failed"),
            );
          }}
        >
          {state}
        </button>
      </div>
      <pre>
        <code>{text}</code>
      </pre>
    </div>
  );
}
