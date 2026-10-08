import { expect, test } from "bun:test";
import { dnsMembers } from "../src/config.ts";

// DNS discovery (docs/raft.md, section 10): every address is a member, and this node always is.
test("dns discovery lists the resolved addresses and this node", async () => {
  const self = { id: "127.0.0.1:25998", addr: "127.0.0.1:25998" };
  const members = await dnsMembers("localhost", 25998, self);
  expect(members.some((m) => m.id === self.id)).toBe(true);
  expect(members.every((m) => m.id === m.addr)).toBe(true);
  const none = await dnsMembers("no-such-host.invalid", 1, self);
  expect(none).toEqual([self]);
});
