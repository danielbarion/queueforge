"use client";

import { Shield } from "lucide-react";
import { GatedList, text } from "@/components/pages/ApiList";
import { PageHeader } from "@/components/ui/PageHeader";
import { callBroker } from "@/lib/client";
import { selectedBroker, useBrokerStore } from "@/stores/broker";

export default function PoliciesPage() {
  const broker = useBrokerStore(selectedBroker);
  return (
    <>
      <PageHeader title="Policies" icon={Shield} detail="User policies, then operator policies. The highest priority match applies to each queue." />
      <GatedList
        path="/api/policies"
        empty="No policies."
        rowKey={(row) => `${text(row.vhost)}\0${text(row.name)}`}
        columns={[
          { label: "Vhost", cell: (row) => text(row.vhost) },
          { label: "Name", cell: (row) => text(row.name) },
          { label: "Pattern", cell: (row) => <code>{text(row.pattern)}</code> },
          { label: "Applies to", cell: (row) => text(row["apply-to"]) },
          { label: "Priority", cell: (row) => text(row.priority) },
          { label: "Definition", cell: (row) => <code>{text(row.definition)}</code> },
        ]}
        action={(row, reload) => (
          <button
            type="button"
            className="btn btn-ghost btn-xs"
            onClick={async () => {
              if (!broker || !window.confirm(`Delete policy ${text(row.name)}?`)) return;
              await callBroker(broker.url, "DELETE", `/api/policies/${encodeURIComponent(text(row.vhost))}/${encodeURIComponent(text(row.name))}`);
              reload();
            }}
          >
            Delete
          </button>
        )}
      />
      <h2 className="mt-8 mb-3 text-sm font-semibold">Operator policies</h2>
      <GatedList
        path="/api/operator-policies"
        empty="No operator policies."
        rowKey={(row) => `${text(row.vhost)}\0${text(row.name)}`}
        columns={[
          { label: "Vhost", cell: (row) => text(row.vhost) },
          { label: "Name", cell: (row) => text(row.name) },
          { label: "Pattern", cell: (row) => <code>{text(row.pattern)}</code> },
          { label: "Definition", cell: (row) => <code>{text(row.definition)}</code> },
        ]}
      />
    </>
  );
}
