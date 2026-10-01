/**
 * RabbitMQ password-hash check used by broker login.
 */
import { createHash, timingSafeEqual } from "node:crypto";

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
