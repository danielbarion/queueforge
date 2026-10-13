/**
 * The operator cookie is an opaque session id. The session row lives in
 * Postgres. This module only shapes the cookie; it does not decide that the
 * session is real.
 */

export const OPERATOR_COOKIE = "qfc_op";
const MAX_AGE_SECONDS = 60 * 60 * 12;

export function databaseConfigured(): boolean {
  return Boolean(process.env.DATABASE_URL);
}

export function sessionIdShape(token: string | null | undefined): boolean {
  return typeof token === "string" && /^[A-Za-z0-9_-]{32,}$/.test(token);
}

export function operatorCookie(token: string, secure: boolean): string {
  return `${OPERATOR_COOKIE}=${token}; Path=/; HttpOnly; SameSite=Strict; Max-Age=${MAX_AGE_SECONDS}${secure ? "; Secure" : ""}`;
}

export function clearOperatorCookie(secure: boolean): string {
  return `${OPERATOR_COOKIE}=; Path=/; HttpOnly; SameSite=Strict; Max-Age=0${secure ? "; Secure" : ""}`;
}

export function requestIsSecure(request: Request): boolean {
  const forwarded = request.headers.get("x-forwarded-proto");
  if (forwarded) return forwarded.split(",")[0]!.trim() === "https";
  return new URL(request.url).protocol === "https:";
}

/** A same-origin path. Anything else becomes the overview, so login cannot redirect off the console. */
export function safeNext(value: string | null): string {
  if (!value || !value.startsWith("/") || value.startsWith("//") || value.includes("\\") || value.includes("://")) return "/";
  return value;
}
