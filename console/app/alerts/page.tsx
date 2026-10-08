"use client";

import { Bell } from "lucide-react";
import { Field } from "@/components/ui/Field";
import { Card, CardBody } from "@/components/ui/Card";
import { PageHeader } from "@/components/ui/PageHeader";
import { useAlertStore } from "@/stores/alerts";

export default function AlertsPage() {
  const readyEnabled = useAlertStore((state) => state.readyEnabled);
  const readyOver = useAlertStore((state) => state.readyOver);
  const unackedEnabled = useAlertStore((state) => state.unackedEnabled);
  const unackedOver = useAlertStore((state) => state.unackedOver);
  const fsyncEnabled = useAlertStore((state) => state.fsyncEnabled);
  const setReady = useAlertStore((state) => state.setReady);
  const setUnacked = useAlertStore((state) => state.setUnacked);
  const setFsync = useAlertStore((state) => state.setFsync);

  return (
    <>
      <PageHeader
        title="Alerts"
        icon={Bell}
        detail="Checked in this browser on each poll of the broker in use. A stale poll does not raise a new alert. Nothing is sent onward."
      />
      <Card>
        <CardBody>
          <div className="flex max-w-md flex-col gap-5">
            <label className="flex items-center gap-3 text-sm">
              <input type="checkbox" className="checkbox checkbox-primary" checked={readyEnabled} onChange={(event) => setReady(event.target.checked, readyOver)} />
              Ready messages over the threshold
            </label>
            <Field label="Ready threshold">
              <input
                className="input input-bordered w-full bg-base-100"
                inputMode="numeric"
                value={String(readyOver)}
                onChange={(event) => setReady(readyEnabled, Number(event.target.value))}
              />
            </Field>
            <label className="flex items-center gap-3 text-sm">
              <input type="checkbox" className="checkbox checkbox-primary" checked={unackedEnabled} onChange={(event) => setUnacked(event.target.checked, unackedOver)} />
              Unacked messages over the threshold
            </label>
            <Field label="Unacked threshold">
              <input
                className="input input-bordered w-full bg-base-100"
                inputMode="numeric"
                value={String(unackedOver)}
                onChange={(event) => setUnacked(unackedEnabled, Number(event.target.value))}
              />
            </Field>
            <label className="flex items-center gap-3 text-sm">
              <input type="checkbox" className="checkbox checkbox-primary" checked={fsyncEnabled} onChange={(event) => setFsync(event.target.checked)} />
              Confirm-before-fsync leaves zero
            </label>
          </div>
        </CardBody>
      </Card>
    </>
  );
}
