import { describe, expect, test } from "bun:test";
import { hashPassword, verifyPassword } from "./passwords";
import { databaseConfigured, registrationEnabled, registrationRejection, safeNext, sessionIdShape } from "./operator";

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

  test("registration stays closed unless the environment opens it", () => {
    const previous = process.env.CONSOLE_REGISTRATION;
    try {
      delete process.env.CONSOLE_REGISTRATION;
      expect(registrationEnabled()).toBe(false);
      for (const closed of ["", "false", "0", "no", "off", "enabled"]) {
        process.env.CONSOLE_REGISTRATION = closed;
        expect(registrationEnabled()).toBe(false);
      }
      for (const open of ["1", "true", "TRUE", " yes ", "on"]) {
        process.env.CONSOLE_REGISTRATION = open;
        expect(registrationEnabled()).toBe(true);
      }
    } finally {
      if (previous === undefined) delete process.env.CONSOLE_REGISTRATION;
      else process.env.CONSOLE_REGISTRATION = previous;
    }
  });

  test("a registration needs a plain username and an 8 character password", () => {
    expect(registrationRejection("ada", "eightchr")).toBeNull();
    expect(registrationRejection("ada", "sevench")).toMatch(/8 characters/);
    expect(registrationRejection("", "correct-horse")).toMatch(/username/);
    expect(registrationRejection("bad name", "correct-horse")).toMatch(/username/);
    expect(registrationRejection("a".repeat(65), "correct-horse")).toMatch(/username/);
  });

  test("the post-login path stays on this console", () => {
    expect(safeNext("/queues")).toBe("/queues");
    expect(safeNext("https://evil.example")).toBe("/");
    expect(safeNext("//evil.example")).toBe("/");
    expect(safeNext(null)).toBe("/");
  });
});
