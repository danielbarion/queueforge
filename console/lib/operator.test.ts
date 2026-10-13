import { afterEach, describe, expect, test } from "bun:test";
import { credentialsMatch, issueSession, operatorConfigured, safeNext, sessionValid } from "./operator";

describe("operator login", () => {
  const previous = { user: process.env.CONSOLE_USER, password: process.env.CONSOLE_PASSWORD };
  afterEach(() => {
    if (previous.user === undefined) delete process.env.CONSOLE_USER;
    else process.env.CONSOLE_USER = previous.user;
    if (previous.password === undefined) delete process.env.CONSOLE_PASSWORD;
    else process.env.CONSOLE_PASSWORD = previous.password;
  });
  test("a short password is not a configuration", () => {
    process.env.CONSOLE_USER = "operator";
    process.env.CONSOLE_PASSWORD = "short";
    expect(operatorConfigured()).toBe(false);
  });

  test("the password compare accepts only the configured pair", async () => {
    process.env.CONSOLE_USER = "operator";
    process.env.CONSOLE_PASSWORD = "correct-horse-12";
    expect(await credentialsMatch("operator", "correct-horse-12")).toBe(true);
    expect(await credentialsMatch("operator", "correct-horse-13")).toBe(false);
    expect(await credentialsMatch("someone", "correct-horse-12")).toBe(false);
    expect(await credentialsMatch("operator", "correct-horse-1")).toBe(false);
  });

  test("a session lasts until its expiry and fails if the password changes", async () => {
    process.env.CONSOLE_USER = "operator";
    process.env.CONSOLE_PASSWORD = "correct-horse-12";
    const now = 1_700_000_000_000;
    const token = await issueSession(now);
    expect(await sessionValid(token, now + 1000)).toBe(true);
    expect(await sessionValid(token, now + 13 * 60 * 60 * 1000)).toBe(false);
    expect(await sessionValid(`${token}x`, now + 1000)).toBe(false);
    process.env.CONSOLE_PASSWORD = "correct-horse-99";
    expect(await sessionValid(token, now + 1000)).toBe(false);
  });

  test("the post-login path stays on this console", () => {
    expect(safeNext("/queues")).toBe("/queues");
    expect(safeNext("https://evil.example")).toBe("/");
    expect(safeNext("//evil.example")).toBe("/");
    expect(safeNext(null)).toBe("/");
  });
});
