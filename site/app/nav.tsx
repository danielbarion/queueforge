"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { GITHUB } from "./bench";

const LINKS = [
  { href: "/about", label: "About" },
  { href: "/benchmark", label: "Benchmark" },
];

export function Nav() {
  const path = usePathname();
  return (
    <header>
      <div className="wrap bar">
        <Link className="brand" href="/">
          <span className="mark" aria-hidden="true">
            <i />
            <i />
            <i />
          </span>
          QueueForge
        </Link>
        <nav aria-label="Site">
          {LINKS.map((link) => (
            <Link key={link.href} href={link.href} aria-current={path === link.href ? "page" : undefined}>
              {link.label}
            </Link>
          ))}
          <a href={GITHUB}>GitHub</a>
        </nav>
      </div>
    </header>
  );
}
