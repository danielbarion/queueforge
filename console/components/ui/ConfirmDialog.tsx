"use client";

export function ConfirmDialog({
  title,
  body,
  action,
  pending,
  onConfirm,
  onClose,
}: {
  title: string;
  body: string;
  action: string;
  pending: boolean;
  onConfirm: () => void;
  onClose: () => void;
}) {
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4" role="dialog" aria-modal="true">
      <div className="w-full max-w-md rounded-box border border-base-300 bg-base-200 p-6">
        <h2 className="text-base font-semibold">{title}</h2>
        <p className="mt-2 text-sm leading-relaxed text-muted">{body}</p>
        <div className="mt-5 flex gap-2">
          <button type="button" className="btn btn-error" disabled={pending} onClick={onConfirm}>
            {pending ? "Working…" : action}
          </button>
          <button type="button" className="btn btn-ghost" disabled={pending} onClick={onClose}>
            Cancel
          </button>
        </div>
      </div>
    </div>
  );
}
