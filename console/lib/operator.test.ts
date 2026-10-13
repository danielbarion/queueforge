import { describe, expect, test } from "bun:test";
import { hashPassword, verifyPassword } from "./passwords";
import { databaseConfigured, safeNext, sessionIdShape } from "./operator";

describe("operator login", () => {
  test("the console is closed until a database is configured", () => {
    const previous = process.env.DATABASE_URL;
    delete process.env.DATABASE_URL;
    expect(databaseConfigured()).toBe(false);
    if (previous === undefined) delete process.env.DATABASE_URL;
    else process.env.DATABASE_URL = previous;
  });

  test("a password hash verifies only itself", () => {
    const stored = hashPassword("correct-horse-12");
    expect(verifyPassword("correct-horse-12", stored)).toBe(true);
    expect(verifyPassword("correct-horse-13", stored)).toBe(false);
    expect(verifyPassword("correct-horse-12", "scrypt$not-salt$not-hash")).toBe(false);
  });

  test("a session id is the opaque cookie, not a signed token", () => {
    expect(sessionIdShape("a".repeat(32))).toBe(true);
    expect(sessionIdShape("short")).toBe(false);
    expect(sessionIdShape("body.signature-that-is-long-enough")).toBe(false);
  });

  test("the post-login path stays on this console", () => {
    expect(safeNext("/queues")).toBe("/queues");
    expect(safeNext("https://evil.example")).toBe("/");
    expect(safeNext("//evil.example")).toBe("/");
    expect(safeNext(null)).toBe("/");
  });
});
