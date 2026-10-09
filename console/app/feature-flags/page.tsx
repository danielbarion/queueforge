"use client";

import { Flag } from "lucide-react";
import { GatedList, text } from "@/components/pages/ApiList";
import { PageHeader } from "@/components/ui/PageHeader";

export default function FeatureFlagsPage() {
  return (
    <>
      <PageHeader
        title="Feature flags"
        icon={Flag}
        detail="Flags this broker supports. A new QueueForge cluster turns raft on by itself; after an upgrade an administrator enables it with PUT /api/feature-flags/raft/enable."
      />
      <GatedList
        path="/api/feature-flags"
        empty="No feature flags."
        rowKey={(row) => text(row.name)}
        columns={[
          { label: "Name", cell: (row) => text(row.name) },
          { label: "State", cell: (row) => <span className={row.state === "enabled" ? "text-success" : "text-muted"}>{text(row.state)}</span> },
          { label: "Stability", cell: (row) => text(row.stability) },
          { label: "Description", cell: (row) => text(row.desc) },
        ]}
      />
    </>
  );
}
