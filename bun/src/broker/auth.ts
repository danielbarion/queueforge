/**
 * RabbitMQ password-hash create and check used by broker login.
 *
 * New passwords are base64(4-byte salt || SHA-256). SHA-512 hashes still verify.
 */
import { createHash, randomBytes, timingSafeEqual } from "node:crypto";

/** Minimum password length in Unicode code points. */
export const MIN_PASSWORD_LEN = 8;

/** Maximum password length in bytes. */
export const MAX_PASSWORD_BYTES = 1024;

/**
 * Reject a password that is empty, shorter than 8 characters, or longer than 1024 bytes.
 *
 * @param password Plain password.
 * @returns An error string, or null when the password meets the policy.
 */
export function passwordPolicyError(password: string): string | null {
  if (password.length === 0) return "password must not be empty";
  if (Buffer.byteLength(password, "utf8") > MAX_PASSWORD_BYTES) {
    return `password must be at most ${MAX_PASSWORD_BYTES} bytes`;
  }
  if ([...password].length < MIN_PASSWORD_LEN) {
    return `password must be at least ${MIN_PASSWORD_LEN} characters`;
  }
  return null;
}

/**
 * Hash a password as the RabbitMQ SHA-256 password-hash.
 *
 * @param password Plain password. The caller enforces the length policy.
 * @param salt Four salt bytes. Omitted salts are random.
 * @returns Base64 of the 4-byte salt followed by SHA-256(salt || password).
 */
export function hashRabbitPassword(password: string, salt?: Uint8Array): string {
  const used = salt ?? randomBytes(4);
  if (used.byteLength !== 4) throw new Error("rabbit password salt must be 4 bytes");
  const digest = createHash("sha256").update(used).update(password, "utf8").digest();
  return Buffer.concat([Buffer.from(used), digest]).toString("base64");
}

/**
 * Compare a plaintext password with a RabbitMQ password_hash value.
 *
 * @param password Password the client sent.
 * @param encoded Base64 of salt plus SHA-256 or SHA-512. Any other length returns false.
 * @returns True when the digest matches.
 */
export function rabbitPasswordHashMatches(password: string, encoded: string): boolean {
  const raw = Buffer.from(encoded, "base64");
  if (raw.length !== 36 && raw.length !== 68) return false;
  const salt = raw.subarray(0, 4);
  const digest = raw.subarray(4);
  const algo = digest.length === 32 ? "sha256" : "sha512";
  const computed = createHash(algo).update(salt).update(password, "utf8").digest();
  return timingSafeEqual(computed, digest);
}
