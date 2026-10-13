import { NextResponse } from "next/server";
import type { NextRequest } from "next/server";
import { databaseConfigured, OPERATOR_COOKIE, sessionIdShape } from "@/lib/operator";

function loginPage(request: NextRequest) {
  const headers = new Headers(request.headers);
  headers.set("x-console-gate", "login");
  return NextResponse.next({ request: { headers } });
}

/**
 * The cookie only decides the redirect. Postgres decides whether the session
 * is real; pages and APIs check that themselves.
 */
export function proxy(request: NextRequest) {
  const { pathname } = request.nextUrl;
  if (pathname === "/api/operator/login" || pathname === "/api/operator/logout") return NextResponse.next();
  if (!databaseConfigured()) {
    if (pathname === "/login") return loginPage(request);
    if (pathname.startsWith("/api/")) {
      return NextResponse.json({ error: "The console database is not configured." }, { status: 503 });
    }
    return NextResponse.redirect(new URL("/login", request.url));
  }
  const present = sessionIdShape(request.cookies.get(OPERATOR_COOKIE)?.value);
  if (pathname === "/login") {
    if (present) return NextResponse.redirect(new URL("/", request.url));
    return loginPage(request);
  }
  if (present) return NextResponse.next();
  if (pathname.startsWith("/api/")) return NextResponse.json({ error: "Log in." }, { status: 401 });
  const login = new URL("/login", request.url);
  login.searchParams.set("next", pathname);
  return NextResponse.redirect(login);
}

export const config = {
  matcher: ["/((?!_next/static|_next/image|favicon.ico).*)"],
};
