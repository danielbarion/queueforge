import { expect, test } from "bun:test";
import { join } from "node:path";
import { channel, confirmChannel, eventually, MGMT, uniq } from "../lib.ts";

const CTL = join(import.meta.dir, "../../ctl/queueforge-ctl.ts");

/** Run queueforge-ctl against the broker under test. */
function ctl(...args: string[]): { code: number; out: string; err: string } {
  const res = Bun.spawnSync(["bun", CTL, "--url", MGMT, "--user", "admin", "--password", "devpassword12", ...args]);
  return { code: res.exitCode ?? -1, out: res.stdout.toString(), err: res.stderr.toString() };
}

const lines = (out: string) => out.trim().split("\n").filter(Boolean);

test("Command-line tool :: vhosts are added, listed and deleted", () => {
  const vhost = uniq("ctl-vh");
  expect(ctl("add_vhost", vhost).code).toBe(0);
  expect(lines(ctl("list_vhosts").out)).toContain(vhost);
  expect(ctl("delete_vhost", vhost).code).toBe(0);
  expect(lines(ctl("list_vhosts").out)).not.toContain(vhost);
});

test("Command-line tool :: users and permissions are set and listed", () => {
  const user = uniq("ctl-user");
  expect(ctl("add_user", user, "pw-ctl-123").code).toBe(0);
  expect(ctl("set_permissions", user, "^x", "^y", "^z").code).toBe(0);
  const perms = lines(ctl("list_permissions").out).map((l) => l.split("\t"));
  expect(perms).toContainEqual([user, "^x", "^y", "^z"]);
  expect(ctl("delete_user", user).code).toBe(0);
});

test("Command-line tool :: a policy is set, listed and cleared", () => {
  const name = uniq("ctl-pol");
  expect(ctl("set_policy", name, "^ctl-nothing$", '{"max-length": 5}', "--apply-to", "queues").code).toBe(0);
  expect(lines(ctl("list_policies").out).some((l) => l.startsWith(`${name}\t`))).toBe(true);
  expect(ctl("clear_policy", name).code).toBe(0);
  expect(lines(ctl("list_policies").out).some((l) => l.startsWith(`${name}\t`))).toBe(false);
});

test("Command-line tool :: list_queues shows depth and purge_queue empties a queue", async () => {
  const ch = await confirmChannel();
  const q = uniq("ctl-q");
  await ch.assertQueue(q, { durable: true });
  for (let i = 0; i < 3; i++) ch.sendToQueue(q, Buffer.from(String(i)));
  await ch.waitForConfirms();
  // RabbitMQ refreshes management queue counts every 5 seconds.
  expect(await eventually(() => lines(ctl("list_queues").out).includes(`${q}\t3`), 8000)).toBe(true);
  expect(ctl("purge_queue", q).code).toBe(0);
  expect((await ch.checkQueue(q)).messageCount).toBe(0);
  expect(ctl("delete_queue", q).code).toBe(0);
});

test("Command-line tool :: export_definitions writes the queues", async () => {
  const ch = await channel();
  const q = uniq("ctl-def");
  await ch.assertQueue(q, { durable: true });
  const res = ctl("export_definitions", "-");
  expect(res.code).toBe(0);
  const defs = JSON.parse(res.out) as { queues: Array<{ name: string }> };
  expect(defs.queues.some((d) => d.name === q)).toBe(true);
  await ch.deleteQueue(q);
});

test("Command-line tool :: a refused request exits 1", () => {
  expect(ctl("delete_queue", uniq("ctl-missing")).code).toBe(1);
});
