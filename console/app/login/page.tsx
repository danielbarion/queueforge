import { Card, CardBody } from "@/components/ui/Card";
import { OperatorForm } from "@/components/auth/OperatorForm";
import { databaseConfigured, safeNext } from "@/lib/operator";

export default async function LoginPage({ searchParams }: { searchParams: Promise<{ next?: string }> }) {
  const next = safeNext((await searchParams).next ?? null);
  const configured = databaseConfigured();
  return (
    <main className="grid min-h-dvh place-items-center bg-base-100 px-5 py-16">
      <Card className="w-full max-w-md">
        <CardBody>
          <div>
            <p className="text-[10px] font-semibold uppercase tracking-[0.18em] text-faint">QueueForge</p>
            <h1 className="mt-2 text-2xl font-semibold tracking-tight">Login</h1>
          </div>
          {configured ? <OperatorForm next={next} /> : <p className="text-sm text-error">The console database is not configured. Set DATABASE_URL, CONSOLE_USER, and CONSOLE_PASSWORD before starting.</p>}
        </CardBody>
      </Card>
    </main>
  );
}
