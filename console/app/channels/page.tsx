"use client";

import { Radio } from "lucide-react";
import { GatedList, text } from "@/components/pages/ApiList";
import { PageHeader } from "@/components/ui/PageHeader";

export default function ChannelsPage() {
  return (
    <>
      <PageHeader title="Channels" icon={Radio} detail="Open channels on every connection, with their prefetch and unacked counts." />
      <GatedList
        path="/api/channels"
        empty="No channels are open."
        rowKey={(row) => text(row.name)}
        columns={[
          { label: "Channel", cell: (row) => text(row.name) },
          { label: "User", cell: (row) => text(row.user) },
          { label: "Vhost", cell: (row) => text(row.vhost) },
          { label: "Prefetch", cell: (row) => text(row.prefetch_count) },
          { label: "Unacked", cell: (row) => text(row.messages_unacknowledged) },
          { label: "Confirm", cell: (row) => (row.confirm === true ? "yes" : "no") },
        ]}
      />
    </>
  );
}
