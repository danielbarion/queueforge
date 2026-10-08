"use client";

import type { ReactNode } from "react";
import { LoginForm } from "@/components/auth/LoginForm";
import { Card, CardBody } from "@/components/ui/Card";
import { selectedBroker, useBrokerStore } from "@/stores/broker";
import { useLiveStore } from "@/stores/live";

export function BrokerGate({ children }: { children: ReactNode }) {
  const broker = useBrokerStore(selectedBroker);
  const authed = useLiveStore((state) => state.authed);
  if (!broker) return <p className="text-sm text-muted">Add a broker first.</p>;
  if (authed === null) return <p className="text-sm text-muted">Checking the session…</p>;
  if (!authed) {
    return (
      <Card>
        <CardBody>
          <LoginForm url={broker.url} />
        </CardBody>
      </Card>
    );
  }
  return children;
}
