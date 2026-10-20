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

/**
 * Public registration. Read through a dynamic key so Next does not inline the
 * value at build time; Coolify can flip it without rebuilding the image.
 * Unset, empty, and every value other than 1/true/yes/on leave it closed.
 */
export function registrationEnabled(): boolean {
  const raw = process.env["CONSOLE_REGISTRATION"];
  if (!raw) return false;
  return ["1", "true", "yes", "on"].includes(raw.trim().toLowerCase());
}

const USERNAME = /^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/;

/** Why this registration must be refused, or null when the fields are acceptable. */
export function registrationRejection(username: string, password: string): string | null {
  if (!USERNAME.test(username)) return "Use a username of up to 64 letters, numbers, dots, underscores, or hyphens.";
  if (password.length < 8) return "Use a password of at least 8 characters.";
  return null;
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
