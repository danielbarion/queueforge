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
        detail="Flags this broker supports. On a QueueForge cluster, raft turns on by itself once every member advertises it."
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
