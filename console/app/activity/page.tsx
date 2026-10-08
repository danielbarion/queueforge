"use client";

import { Activity } from "lucide-react";
import { Card, CardBody } from "@/components/ui/Card";
import { PageHeader } from "@/components/ui/PageHeader";
import { useLiveStore } from "@/stores/live";

export default function ActivityPage() {
  const activity = useLiveStore((state) => state.activity);
  return (
    <>
      <PageHeader title="Activity" icon={Activity} detail="Changes since this broker was selected. Switching the broker in use clears the list." />
      <Card>
        <CardBody>
          {activity.length === 0 ? (
            <p className="text-sm text-muted">Nothing has changed yet.</p>
          ) : (
            <ul className="flex flex-col gap-2">
              {activity.map((event) => (
                <li key={event.id} className="text-sm">
                  <span className="mr-2 font-mono text-xs text-faint">{new Date(event.at).toLocaleTimeString()}</span>
                  {event.text}
                </li>
              ))}
            </ul>
          )}
        </CardBody>
      </Card>
    </>
  );
}
