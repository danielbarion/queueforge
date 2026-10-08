import { describe, expect, test } from "bun:test";
import { consoleSid, crossSite, forbiddenHost, getCredential, newSid, setCredential, sidCookie } from "./guard";

function req(headers: Record<string, string>, method = "POST") {
  return new Request("http://127.0.0.1:3200/api/brokers/call", { method, headers });
}

describe("cross-site guard", () => {
  test("allows this console's own JSON fetch", () => {
    expect(crossSite(req({ origin: "http://127.0.0.1:3200", host: "127.0.0.1:3200", "content-type": "application/json", "sec-fetch-site": "same-origin" }))).toBeNull();
  });

  test("refuses another site, by Origin or by Sec-Fetch-Site", () => {
    expect(crossSite(req({ origin: "https://evil.example", host: "127.0.0.1:3200", "content-type": "application/json" }))).not.toBeNull();
    expect(crossSite(req({ host: "127.0.0.1:3200", "content-type": "application/json", "sec-fetch-site": "cross-site" }))).not.toBeNull();
  });

  test("refuses a form post that is not JSON", () => {
    expect(crossSite(req({ origin: "http://127.0.0.1:3200", host: "127.0.0.1:3200", "content-type": "text/plain" }))).not.toBeNull();
  });
});

describe("console session", () => {
  test("credentials belong to one browser", () => {
    const mine = newSid();
    const theirs = newSid();
    setCredential(mine, "http://broker:15672", { kind: "basic", value: "Basic eDp5" });
    expect(getCredential(mine, "http://broker:15672")?.value).toBe("Basic eDp5");
    expect(getCredential(theirs, "http://broker:15672")).toBeNull();
    expect(getCredential(null, "http://broker:15672")).toBeNull();
  });

  test("the cookie is HttpOnly and SameSite=Strict, and round-trips", () => {
    const sid = newSid();
    const cookie = sidCookie(sid, false);
    expect(cookie).toContain("HttpOnly");
    expect(cookie).toContain("SameSite=Strict");
    expect(consoleSid(req({ cookie: `other=1; ${cookie.split(";")[0]}` }, "GET"))).toBe(sid);
    expect(consoleSid(req({ cookie: "qfc_sid=short" }, "GET"))).toBeNull();
  });
});

test("link-local and metadata hosts are never a broker", () => {
  expect(forbiddenHost("http://169.254.169.254")).toBe(true);
  expect(forbiddenHost("http://metadata.google.internal")).toBe(true);
  expect(forbiddenHost("http://127.0.0.1:15672")).toBe(false);
});
