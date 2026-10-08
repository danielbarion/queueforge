"use client";

import { Users } from "lucide-react";
import { GatedList, text } from "@/components/pages/ApiList";
import { PageHeader } from "@/components/ui/PageHeader";
import { callBroker } from "@/lib/client";
import { selectedBroker, useBrokerStore } from "@/stores/broker";

export default function UsersPage() {
  const broker = useBrokerStore(selectedBroker);
  return (
    <>
      <PageHeader title="Users" icon={Users} detail="Users and their tags, then the permissions each holds per vhost." />
      <GatedList
        path="/api/users"
        empty="No users."
        rowKey={(row) => text(row.name)}
        columns={[
          { label: "Name", cell: (row) => text(row.name) },
          { label: "Tags", cell: (row) => text(row.tags) },
        ]}
        action={(row, reload) => (
          <button
            type="button"
            className="btn btn-ghost btn-xs"
            onClick={async () => {
              if (!broker || !window.confirm(`Delete user ${text(row.name)}?`)) return;
              await callBroker(broker.url, "DELETE", `/api/users/${encodeURIComponent(text(row.name))}`);
              reload();
            }}
          >
            Delete
          </button>
        )}
      />
      <h2 className="mt-8 mb-3 text-sm font-semibold">Permissions</h2>
      <GatedList
        path="/api/permissions"
        empty="No permissions."
        rowKey={(row) => `${text(row.user)}\0${text(row.vhost)}`}
        columns={[
          { label: "User", cell: (row) => text(row.user) },
          { label: "Vhost", cell: (row) => text(row.vhost) },
          { label: "Configure", cell: (row) => <code>{text(row.configure)}</code> },
          { label: "Write", cell: (row) => <code>{text(row.write)}</code> },
          { label: "Read", cell: (row) => <code>{text(row.read)}</code> },
        ]}
      />
    </>
  );
}
