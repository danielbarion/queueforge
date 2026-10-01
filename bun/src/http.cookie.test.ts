import { describe, expect, test } from "bun:test";
import { cookieNameFromHost } from "./http/index.ts";

describe("cookieNameFromHost", () => {
  test("different management ports get different cookie names", () => {
    expect(cookieNameFromHost("127.0.0.1:36673")).toBe("queueforge_session_36673");
    expect(cookieNameFromHost("127.0.0.1:36674")).toBe("queueforge_session_36674");
    expect(cookieNameFromHost("127.0.0.1:36673")).not.toBe(cookieNameFromHost("127.0.0.1:36674"));
    expect(cookieNameFromHost(null)).toBe("queueforge_session");
  });
});
