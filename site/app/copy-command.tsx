"use client";

import { useState } from "react";

export function CopyCommand({ text }: { text: string }) {
  const [label, setLabel] = useState("Copy");

  function flash(next: string) {
    setLabel(next);
    window.setTimeout(() => setLabel("Copy"), 1600);
  }

  return (
    <pre className="cmd">
      <code>{text}</code>
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
        {label}
      </button>
    </pre>
  );
}
