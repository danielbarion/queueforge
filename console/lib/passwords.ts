import { randomBytes, scryptSync, timingSafeEqual } from "node:crypto";

const PARAMS = { N: 16384, r: 8, p: 1 } as const;

/** Stored as `scrypt$<salt>$<hash>`, both base64url. */
export function hashPassword(password: string): string {
  const salt = randomBytes(16);
  const hash = scryptSync(password, salt, 32, PARAMS);
  return `scrypt$${salt.toString("base64url")}$${hash.toString("base64url")}`;
}

export function verifyPassword(password: string, stored: string): boolean {
  const [kind, salt, hash] = stored.split("$");
  if (kind !== "scrypt" || !salt || !hash) return false;
  const expected = Buffer.from(hash, "base64url");
  const actual = scryptSync(password, Buffer.from(salt, "base64url"), expected.length, PARAMS);
  if (actual.length !== expected.length) return false;
  return timingSafeEqual(actual, expected);
}
