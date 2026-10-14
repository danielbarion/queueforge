import { expect, test } from "bun:test";
import { appendFileSync, existsSync, mkdtempSync, readFileSync, readdirSync, rmSync, statSync, truncateSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { COMPACT_BYTES, Disk, entryBodyBytes, META, RaftNode, soloQueueGroup, type RaftHooks } from "../src/raft/node.ts";
import type { Entry } from "../src/raft/core.ts";
import { decodeQuorumAppend, encodeQuorumAppend, rawQuorumBody } from "../src/wire.ts";

function scratch() {
  const dir = mkdtempSync(join(tmpdir(), "qf-raft-bin-"));
  return { dir, done: () => rmSync(dir, { recursive: true, force: true }) };
}

const body = (n: number) => new Uint8Array(1024).map((_, k) => (k * 7 + n) & 0xff);

function enq(i: number): Entry {
  return {
    i,
    term: 2,
    kind: "enq",
    data: encodeQuorumAppend({
      vhost: "/",
      queue: "orders",
      messageId: `m${i}`,
      body: body(i),
      exchange: "x",
      routingKey: "rk",
      persistent: true,
      propRaw: new Uint8Array([0x10, 0, 2]),
      headers: [["h", { t: "S", v: "v" }]],
      priority: 3,
    }),
  };
}

const other = (i: number): Entry => ({ i, term: 2, kind: "drop", data: { vhost: "/", queue: "orders", ids: [`m${i - 1}`] } });

test("a one-voter log keeps quorum bodies as bytes and loads the same entries", () => {
  const s = scratch();
  try {
    const disk = new Disk(s.dir, true);
    const written = [enq(1), other(2), enq(3)];
    disk.writeEntries(written.slice(0, 2));
    disk.writeEntries(written.slice(2));
    disk.sync();
    disk.close();
    expect(existsSync(join(s.dir, "log.jsonl"))).toBe(false);
    // The body is not base64 in the file: about one byte per body byte.
    expect(statSync(join(s.dir, "log.bin")).size).toBeLessThan(2 * 1024 + 900);
    const { log } = new Disk(s.dir, true).load();
    expect(log.map((e) => e.i)).toEqual([1, 2, 3]);
    for (const [k, e] of log.entries()) {
      // Byte-identical JSON: what a peer, a snapshot or a JSON log would see.
      expect(JSON.stringify(e)).toBe(JSON.stringify(written[k]));
    }
    expect(rawQuorumBody(log[0]!.data)).toEqual(body(1));
    const d = decodeQuorumAppend(log[2]!.data as Record<string, unknown>);
    expect(d.body).toEqual(body(3));
    expect(d.propRaw).toEqual(new Uint8Array([0x10, 0, 2]));
    expect(d.priority).toBe(3);
  } finally {
    s.done();
  }
});

test("a torn or corrupt record ends the log there, and later appends are read", () => {
  const s = scratch();
  try {
    const disk = new Disk(s.dir, true);
    disk.writeEntries([enq(1), enq(2)]);
    disk.close();
    const path = join(s.dir, "log.bin");
    const full = statSync(path).size;
    // A crash mid-write of entry 3.
    appendFileSync(path, Buffer.from(readFileSync(path).subarray(0, 40)));
    let loaded = new Disk(s.dir, true);
    expect(loaded.load().log.map((e) => e.i)).toEqual([1, 2]);
    expect(statSync(path).size).toBe(full);
    loaded.writeEntries([enq(3)]);
    loaded.close();
    loaded = new Disk(s.dir, true);
    expect(loaded.load().log.map((e) => e.i)).toEqual([1, 2, 3]);
    loaded.close();
    // A flipped byte in entry 2's body: entries from there on are not trusted.
    const bytes = readFileSync(path);
    bytes[Math.floor(full / 2) + 600] ^= 0xff;
    writeFileSync(path, bytes);
    expect(new Disk(s.dir, true).load().log.map((e) => e.i)).toEqual([1]);
  } finally {
    s.done();
  }
});

test("a later record for an index replaces the tail, as a follower's truncate does", () => {
  const s = scratch();
  try {
    const disk = new Disk(s.dir, true);
    disk.writeEntries([enq(1), enq(2), enq(3)]);
    const replaced = { ...enq(2), term: 3 };
    disk.writeEntries([replaced]);
    disk.close();
    const { log } = new Disk(s.dir, true).load();
    expect(log.map((e) => [e.i, e.term])).toEqual([[1, 2], [2, 3]]);
  } finally {
    s.done();
  }
});

test("a group written as JSON lines moves to log.bin on load", () => {
  const s = scratch();
  try {
    const lines = new Disk(s.dir, false);
    lines.writeEntries([enq(1), other(2)]);
    lines.close();
    expect(existsSync(join(s.dir, "log.jsonl"))).toBe(true);
    const disk = new Disk(s.dir, true);
    const first = disk.load().log;
    expect(first.map((e) => e.i)).toEqual([1, 2]);
    expect(existsSync(join(s.dir, "log.jsonl"))).toBe(false);
    expect(existsSync(join(s.dir, "log.bin"))).toBe(true);
    disk.writeEntries([enq(3)]);
    disk.close();
    const again = new Disk(s.dir, true).load().log;
    expect(again.map((e) => e.i)).toEqual([1, 2, 3]);
    expect(JSON.stringify(again[0])).toBe(JSON.stringify(enq(1)));
  } finally {
    s.done();
  }
});

test("a crash between the move and removing the lines loads the same log", () => {
  const s = scratch();
  try {
    const lines = new Disk(s.dir, false);
    lines.writeEntries([enq(1), other(2)]);
    lines.close();
    const jsonl = readFileSync(join(s.dir, "log.jsonl"));
    new Disk(s.dir, true).load();
    // Put the lines back, as if the process died before removing them.
    writeFileSync(join(s.dir, "log.jsonl"), jsonl);
    expect(new Disk(s.dir, true).load().log.map((e) => e.i)).toEqual([1, 2]);
  } finally {
    s.done();
  }
});

test("rewrite replaces the binary log, and multi-voter groups keep JSON lines", () => {
  const s = scratch();
  try {
    const disk = new Disk(s.dir, true);
    disk.writeEntries([enq(1), enq(2), enq(3)]);
    disk.rewrite([enq(3)]);
    disk.writeEntries([enq(4)]);
    disk.close();
    expect(new Disk(s.dir, true).load().log.map((e) => e.i)).toEqual([3, 4]);
    const t = scratch();
    try {
      const shared = new Disk(t.dir, false);
      shared.writeEntries([enq(1)]);
      shared.close();
      expect(existsSync(join(t.dir, "log.bin"))).toBe(false);
      const line = readFileSync(join(t.dir, "log.jsonl"), "utf8").trim();
      expect(line).toBe(JSON.stringify(enq(1)));
      expect(JSON.parse(line).data.body_b64).toBe(Buffer.from(body(1)).toString("base64"));
    } finally {
      t.done();
    }
  } finally {
    s.done();
  }
});

test("an empty log file and a header-only fragment load as empty", () => {
  const s = scratch();
  try {
    writeFileSync(join(s.dir, "log.bin"), Buffer.alloc(0));
    expect(new Disk(s.dir, true).load().log).toEqual([]);
    writeFileSync(join(s.dir, "log.bin"), Buffer.from([0x51, 0x46, 0x52]));
    expect(new Disk(s.dir, true).load().log).toEqual([]);
    expect(statSync(join(s.dir, "log.bin")).size).toBe(0);
    truncateSync(join(s.dir, "log.bin"), 0);
  } finally {
    s.done();
  }
});

test("a one-voter queue group writes log.bin and replays it after a restart; meta keeps JSON lines", async () => {
  const s = scratch();
  const group = soloQueueGroup("a", "/", "orders");
  const applied: string[] = [];
  const hooks: RaftHooks = {
    send: () => {}, state: () => ({}), install: async () => {}, leaderChanged: () => {},
    apply: async (g, e) => { if (g === group && e.kind === "enq") applied.push(decodeQuorumAppend(e.data as Record<string, unknown>).messageId); },
  };
  const waitLeader = async (node: RaftNode, name: string) => {
    const deadline = Date.now() + 3000;
    while (node.leader(name) !== "a" && Date.now() < deadline) await new Promise((r) => setTimeout(r, 10));
    expect(node.leader(name)).toBe("a");
  };
  try {
    let node = new RaftNode("a", s.dir, ["a"], hooks);
    node.addGroup(group);
    node.expedite(group);
    node.expedite(META);
    await waitLeader(node, group);
    await waitLeader(node, META);
    await node.propose(group, "enq", enq(1).data);
    await node.propose(group, "enq", enq(2).data);
    await node.propose(META, "cmd", { v: 1 });
    node.stop();
    const dirs = readdirSync(s.dir).filter((d) => d.startsWith("q-"));
    expect(dirs).toHaveLength(1);
    const qdir = join(s.dir, dirs[0]!);
    expect(existsSync(join(qdir, "log.bin"))).toBe(true);
    expect(existsSync(join(qdir, "log.jsonl"))).toBe(false);
    expect(existsSync(join(s.dir, META, "log.jsonl"))).toBe(true);
    expect(existsSync(join(s.dir, META, "log.bin"))).toBe(false);
    applied.length = 0;
    node = new RaftNode("a", s.dir, ["a"], hooks);
    node.expedite(group);
    await waitLeader(node, group);
    expect(applied).toEqual(["m1", "m2"]);
    node.stop();
  } finally {
    s.done();
  }
});

test("a group of large bodies compacts by bytes, long before 10,000 entries", async () => {
  const s = scratch();
  const group = soloQueueGroup("a", "/", "big");
  const hooks: RaftHooks = {
    send: () => {}, state: () => ({ messages: [] }), install: async () => {}, leaderChanged: () => {},
    apply: async () => {},
  };
  const big = new Uint8Array(4 * 1024 * 1024).fill(7);
  try {
    const node = new RaftNode("a", s.dir, ["a"], hooks);
    node.addGroup(group);
    node.expedite(group);
    const deadline = Date.now() + 3000;
    while (node.leader(group) !== "a" && Date.now() < deadline) await new Promise((r) => setTimeout(r, 10));
    const count = COMPACT_BYTES / big.length + 2;
    for (let i = 1; i <= count; i++) {
      const data = encodeQuorumAppend({
        vhost: "/", queue: "big", messageId: `m${i}`, body: big, exchange: "", routingKey: "big",
        persistent: true, propRaw: new Uint8Array(), headers: [], priority: 0,
      });
      await node.propose(group, "enq", data);
    }
    const dir = readdirSync(s.dir).find((d) => d.startsWith("q-"))!;
    expect(existsSync(join(s.dir, dir, "snapshot.json"))).toBe(true);
    // The log was rewritten from the snapshot on: well under the bodies written.
    expect(statSync(join(s.dir, dir, "log.bin")).size).toBeLessThan(COMPACT_BYTES);
    node.stop();
  } finally {
    s.done();
  }
});

test("entry body bytes count raw and base64 bodies", () => {
  expect(entryBodyBytes(enq(1))).toBe(1024);
  expect(entryBodyBytes({ i: 1, term: 1, kind: "enq", data: { body_b64: "AAAA" } })).toBe(4);
  expect(entryBodyBytes({ i: 1, term: 1, kind: "enq", data: { message: { body_b64: "AAAAAAAA" } } })).toBe(8);
  expect(entryBodyBytes(other(2))).toBe(0);
});
