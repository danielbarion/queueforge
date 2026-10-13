import { expect, test } from "bun:test";
import { fstatSync, mkdtempSync, openSync, closeSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Disk, META, RaftNode } from "../src/raft/node.ts";
import type { Entry } from "../src/raft/core.ts";

const entry = (i: number): Entry => ({ i, term: 1, kind: "x", data: { i } });

function scratch() {
  const dir = mkdtempSync(join(tmpdir(), "qf-raft-disk-"));
  return { dir, done: () => rmSync(dir, { recursive: true, force: true }) };
}

const synced = (disk: Disk) => new Promise<boolean>((resolve) => disk.syncAside(resolve));

test("a background sync reports the entries written before it", async () => {
  const s = scratch();
  try {
    const disk = new Disk(s.dir);
    disk.writeEntries([entry(1), entry(2)]);
    expect(disk.dirty).toBe(true);
    const done = synced(disk);
    expect(disk.syncing).toBe(true);
    expect(disk.dirty).toBe(false);
    disk.writeEntries([entry(3)]);
    expect(disk.dirty).toBe(true);
    expect(await done).toBe(true);
    expect(disk.syncing).toBe(false);
    // Entry 3 came after the sync started: it still needs one.
    expect(disk.dirty).toBe(true);
    disk.close();
  } finally {
    s.done();
  }
});

test("a background sync that outlives a rewrite proves nothing", async () => {
  const s = scratch();
  try {
    const disk = new Disk(s.dir);
    disk.writeEntries([entry(1), entry(2)]);
    const done = synced(disk);
    disk.rewrite([entry(1)]);
    expect(await done).toBe(false);
    expect(readFileSync(join(s.dir, "log.jsonl"), "utf8").trim().split("\n")).toHaveLength(1);
    disk.close();
  } finally {
    s.done();
  }
});

test("closing under a background sync keeps its fd open until the sync returns", async () => {
  const s = scratch();
  try {
    const disk = new Disk(s.dir);
    disk.writeEntries([entry(1)]);
    const fd = (disk as unknown as { log: number }).log;
    const done = synced(disk);
    disk.close();
    // The number is still this log file, so no other open can be handed it.
    expect(() => fstatSync(fd)).not.toThrow();
    await done;
    expect(() => fstatSync(fd)).toThrow();
    // A later open may reuse the number freely.
    closeSync(openSync(join(s.dir, "other"), "w"));
  } finally {
    s.done();
  }
});

test("an inline sync during a background one still syncs", async () => {
  const s = scratch();
  try {
    const disk = new Disk(s.dir);
    disk.writeEntries([entry(1)]);
    expect(disk.sync()).toBe(true);
    expect(disk.sync()).toBe(false);
    disk.writeEntries([entry(2)]);
    const done = synced(disk);
    // Nothing was written since the background sync started, but it has not landed.
    expect(disk.dirty).toBe(false);
    expect(disk.sync()).toBe(true);
    await done;
    expect(disk.sync()).toBe(false);
    disk.close();
  } finally {
    s.done();
  }
});

test("a leader commits only after its background fsync lands", async () => {
  const s = scratch();
  const applied: number[] = [];
  const node = new RaftNode("a", s.dir, ["a"], {
    send: () => {}, state: () => ({}), install: async () => {}, leaderChanged: () => {},
    apply: async (_group, e) => { if (e.kind === "cmd") applied.push((e.data as { v: number }).v); },
  });
  try {
    node.expedite(META);
    const deadline = Date.now() + 3000;
    while (node.leader(META) !== "a" && Date.now() < deadline) await new Promise((r) => setTimeout(r, 10));
    expect(node.leader(META)).toBe("a");
    const g = (node as unknown as { groups: Map<string, { disk: Disk; core: { commit: number; lastIndex(): number } }> }).groups.get(META)!;
    // Hold the fsync until released: the entry is written but not durable.
    let release: (() => void) | null = null;
    const real = g.disk.syncAside.bind(g.disk);
    g.disk.syncAside = (done) => { release = () => real(done); };
    const proposal = node.propose(META, "cmd", { v: 7 });
    await new Promise((r) => setTimeout(r, 100));
    expect(release).not.toBeNull();
    expect(g.core.commit).toBeLessThan(g.core.lastIndex());
    expect(applied).toEqual([]);
    release!();
    await proposal;
    expect(applied).toEqual([7]);
  } finally {
    node.stop();
    s.done();
  }
});
