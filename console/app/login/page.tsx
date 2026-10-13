import { Card, CardBody } from "@/components/ui/Card";
import { OperatorForm } from "@/components/auth/OperatorForm";
import { operatorConfigured, safeNext } from "@/lib/operator";

export default async function LoginPage({ searchParams }: { searchParams: Promise<{ next?: string }> }) {
  const next = safeNext((await searchParams).next ?? null);
  const configured = operatorConfigured();
  return (
    <main className="grid min-h-dvh place-items-center bg-base-100 px-5 py-16">
      <Card className="w-full max-w-md">
        <CardBody>
          <div>
            <p className="text-[10px] font-semibold uppercase tracking-[0.18em] text-faint">QueueForge</p>
            <h1 className="mt-2 text-2xl font-semibold tracking-tight">Operator login</h1>
            <p className="mt-2 text-sm text-muted">This console is not open. Broker logins are a second step, after this one.</p>
          </div>
          {configured ? <OperatorForm next={next} /> : <p className="text-sm text-error">Set CONSOLE_USER and CONSOLE_PASSWORD before starting the console. The password must be at least 12 characters.</p>}
        </CardBody>
      </Card>
    </main>
  );
}
