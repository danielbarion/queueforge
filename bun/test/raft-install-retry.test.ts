import { expect, spyOn, test } from "bun:test";
import { mkdtempSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { META, RaftNode, type RaftHooks } from "../src/raft/node.ts";

const pause = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
async function until(check: () => boolean) {
  const deadline = Date.now() + 3000;
  while (!check()) {
    if (Date.now() >= deadline) throw new Error("Raft runtime did not reach expected state");
    await pause(10);
  }
}

function restoredNode(hooks: RaftHooks) {
  const dir = mkdtempSync(join(tmpdir(), "qf-raft-install-retry-"));
  mkdirSync(join(dir, META));
  writeFileSync(join(dir, META, "snapshot.json"), JSON.stringify({ index: 1, term: 1, voters: ["a"], state: { restored: true } }));
  writeFileSync(join(dir, META, "state.json"), JSON.stringify({ term: 1, vote: null }));
  const node = new RaftNode("a", dir, ["a"], hooks);
  return { node, close: () => { node.stop(); rmSync(dir, { recursive: true, force: true }); } };
}

test("restored snapshot retries before leader announcement, command application and confirmation", async () => {
  const errors = spyOn(console, "error").mockImplementation(() => {});
  let attempts = 0, accept = false, confirmed = false;
  const events: string[] = [];
  const f = restoredNode({
    send: () => {}, state: () => ({}),
    install: async (group, state) => {
      expect(group).toBe(META); expect(state).toEqual({ restored: true });
      attempts++;
      if (!accept) throw new Error("temporary metadata store failure");
      events.push("installed");
    },
    apply: async (group, entry) => { if (group === META) events.push(`apply:${entry.kind}`); },
    leaderChanged: (group, leader) => { if (group === META && leader) events.push("leader"); },
  });
  try {
    await until(() => attempts > 0);
    f.node.expedite(META);
    const proposal = f.node.propose(META, "command", { value: 42 }).then(() => { confirmed = true; events.push("confirmed"); });
    await until(() => attempts >= 2);
    expect(f.node.leader(META)).toBeNull();
    expect(events).toEqual([]); expect(confirmed).toBe(false);
    accept = true;
    await proposal;
    await until(() => f.node.leader(META) === "a");
    expect(events[0]).toBe("installed");
    expect(events.indexOf("apply:command")).toBeGreaterThan(0);
    expect(events.indexOf("confirmed")).toBeGreaterThan(events.indexOf("apply:command"));
    expect(events.indexOf("leader")).toBeGreaterThan(0);
    expect(confirmed).toBe(true);
  } finally { f.close(); errors.mockRestore(); }
});

test("permanent restored snapshot failure remains unannounced and stop cancels retries", async () => {
  const errors = spyOn(console, "error").mockImplementation(() => {});
  let attempts = 0, applied = 0, announced = 0;
  const f = restoredNode({
    send: () => {}, state: () => ({}),
    install: async () => { attempts++; throw new Error("invalid persisted snapshot"); },
    apply: async () => { applied++; },
    leaderChanged: (_group, leader) => { if (leader) announced++; },
  });
  try {
    f.node.expedite(META);
    await until(() => attempts >= 3);
    expect(f.node.leader(META)).toBeNull();
    expect(applied).toBe(0); expect(announced).toBe(0);
    f.node.stop();
    const stoppedAttempts = attempts;
    await pause(500);
    expect(attempts).toBe(stoppedAttempts);
    expect(applied).toBe(0); expect(announced).toBe(0);
  } finally { f.close(); errors.mockRestore(); }
});
