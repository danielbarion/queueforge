import type { Metadata } from "next";
import { IBM_Plex_Mono, IBM_Plex_Sans } from "next/font/google";
import { headers } from "next/headers";
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
  const gate = (await headers()).get("x-console-gate");
  return (
    <html lang="en" data-theme="queueforge" className={`${sans.variable} ${mono.variable}`}>
      <body>
        {gate === "login" ? children : <AppShell>{children}</AppShell>}
      </body>
    </html>
  );
}
