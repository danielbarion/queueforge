import type { Metadata } from "next";
import { IBM_Plex_Mono, IBM_Plex_Sans } from "next/font/google";
import { headers } from "next/headers";
import { redirect } from "next/navigation";
import { currentSession } from "@/lib/accounts";
import { databaseConfigured } from "@/lib/operator";
import { AppShell } from "@/components/shell/AppShell";
import "./globals.css";

const sans = IBM_Plex_Sans({
  subsets: ["latin"],
  weight: ["400", "500", "600"],
  variable: "--font-plex-sans",
});

const mono = IBM_Plex_Mono({
  subsets: ["latin"],
  weight: ["400", "500"],
  variable: "--font-plex-mono",
});

export const metadata: Metadata = {
  title: "QueueForge console",
  description: "Operator console for the Rust, Bun, and PHP brokers.",
};

export default async function RootLayout({ children }: LayoutProps<"/">) {
  const headerStore = await headers();
  const gate = headerStore.get("x-console-gate");
  if (gate !== "login") {
    const session = databaseConfigured() ? await currentSession(headerStore.get("cookie")) : null;
    if (!session) redirect("/login");
  }
  return (
    <html lang="en" data-theme="queueforge" className={`${sans.variable} ${mono.variable}`}>
      <body>
        {gate === "login" ? children : <AppShell>{children}</AppShell>}
      </body>
    </html>
  );
}
