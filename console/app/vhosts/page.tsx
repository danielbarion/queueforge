"use client";

import { Network } from "lucide-react";
import { GatedList, text } from "@/components/pages/ApiList";
import { PageHeader } from "@/components/ui/PageHeader";

export default function VhostsPage() {
  return (
    <>
      <PageHeader title="Vhosts" icon={Network} detail="Virtual hosts, with their message counts and limits." />
      <GatedList
        path="/api/vhosts"
        empty="No vhosts."
        rowKey={(row) => text(row.name)}
        columns={[
          { label: "Name", cell: (row) => text(row.name) },
          { label: "Messages", cell: (row) => text(row.messages ?? 0) },
          { label: "Ready", cell: (row) => text(row.messages_ready ?? 0) },
          { label: "Unacked", cell: (row) => text(row.messages_unacknowledged ?? 0) },
          { label: "Tracing", cell: (row) => (row.tracing === true ? "on" : "off") },
        ]}
      />
      <h2 className="mt-8 mb-3 text-sm font-semibold">Limits</h2>
      <GatedList
        path="/api/vhost-limits"
        empty="No vhost limits are set."
        rowKey={(row) => text(row.vhost)}
        columns={[
          { label: "Vhost", cell: (row) => text(row.vhost) },
          { label: "Limits", cell: (row) => <code>{text(row.value)}</code> },
        ]}
      />
    </>
  );
}
