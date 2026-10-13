import { NextResponse } from "next/server";
import type { NextRequest } from "next/server";
import { OPERATOR_COOKIE, operatorConfigured, sessionValid } from "@/lib/operator";

function loginPage(request: NextRequest) {
  const headers = new Headers(request.headers);
  headers.set("x-console-gate", "login");
  return NextResponse.next({ request: { headers } });
}

/**
 * Every page and API requires the operator session. Broker login is separate
 * and only happens after this gate.
 */
export async function proxy(request: NextRequest) {
  const { pathname } = request.nextUrl;
  if (pathname === "/api/operator/login" || pathname === "/api/operator/logout") return NextResponse.next();
  if (!operatorConfigured()) {
    if (pathname === "/login") return loginPage(request);
    if (pathname.startsWith("/api/")) {
      return NextResponse.json({ error: "Operator login is not configured." }, { status: 503 });
    }
    return NextResponse.redirect(new URL("/login", request.url));
  }
  const valid = await sessionValid(request.cookies.get(OPERATOR_COOKIE)?.value);
  if (pathname === "/login") {
    if (valid) return NextResponse.redirect(new URL("/", request.url));
    return loginPage(request);
  }
  if (valid) return NextResponse.next();
  if (pathname.startsWith("/api/")) return NextResponse.json({ error: "Log in." }, { status: 401 });
  const login = new URL("/login", request.url);
  login.searchParams.set("next", pathname);
  return NextResponse.redirect(login);
}

export const config = {
  matcher: ["/((?!_next/static|_next/image|favicon.ico).*)"],
};
