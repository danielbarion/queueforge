import { expect, test } from "bun:test";
import { RaftCore, type Msg } from "../src/raft/core.ts";

/** The same in-memory cluster the Rust `cluster::raft` tests drive. */
class Sim {
  nodes = new Map<string, RaftCore>();
  down = new Set<string>();
  now = 0;
  applied = new Map<string, unknown[]>();

  constructor(ids: string[]) {
    for (const [i, id] of ids.entries()) {
      this.nodes.set(id, new RaftCore(id, "meta", [...ids], 7 + i * 1000));
    }
  }

  run(ms: number) {
    const end = this.now + ms;
    while (this.now < end) {
      this.now += 10;
      for (const [id, node] of this.nodes) {
        if (!this.down.has(id)) node.tick(this.now);
      }
      this.flush();
    }
  }

  flush() {
    for (;;) {
      let moved = false;
      for (const [id, node] of this.nodes) {
        node.takeDirty();
        const out = node.takeOutbox();
        const done = node.takeCommitted();
        const values = this.applied.get(id) ?? [];
        for (const entry of done) {
          if (entry.kind !== "noop" && entry.kind !== "config") values.push(entry.data);
        }
        this.applied.set(id, values);
        for (const [to, msg] of out) {
          if (this.down.has(id) || this.down.has(to)) continue;
          const dst = this.nodes.get(to);
          if (dst) {
            dst.step(id, msg as Msg, this.now);
            moved = true;
          }
        }
      }
      if (!moved) break;
    }
  }

  leaders() {
    return [...this.nodes.values()]
      .filter((node) => node.role === "leader" && !this.down.has(node.id))
      .map((node) => ({ term: node.term, id: node.id }))
      .sort((a, b) => a.term - b.term || a.id.localeCompare(b.id))
      .map((row) => row.id);
  }

  leader() {
    const live = this.leaders();
    expect(live).toHaveLength(1);
    return live[0]!;
  }

  propose(data: unknown) {
    const id = this.leader();
    const index = this.nodes.get(id)!.propose("x", data, this.now);
    expect(index).not.toBeNull();
    this.flush();
    return index;
  }

  values(id: string) {
    return this.applied.get(id) ?? [];
  }
}

test("elects one leader and replicates", () => {
  const sim = new Sim(["a", "b", "c"]);
  sim.run(3000);
  sim.propose(1);
  sim.propose(2);
  sim.run(500);
  for (const id of ["a", "b", "c"]) expect(sim.values(id)).toEqual([1, 2]);
});

test("a new leader keeps committed entries after the old leader fails", () => {
  const sim = new Sim(["a", "b", "c"]);
  sim.run(3000);
  sim.propose("before");
  sim.run(300);
  const old = sim.leader();
  sim.down.add(old);
  sim.run(4000);
  const next = sim.leader();
  expect(next).not.toBe(old);
  sim.propose("after");
  sim.run(300);
  sim.down.clear();
  sim.run(1000);
  for (const id of ["a", "b", "c"]) expect(sim.values(id)).toEqual(["before", "after"]);
});

test("a minority cannot commit", () => {
  const sim = new Sim(["a", "b", "c"]);
  sim.run(3000);
  const leader = sim.leader();
  for (const id of ["a", "b", "c"]) if (id !== leader) sim.down.add(id);
  sim.nodes.get(leader)!.propose("x", "lost", sim.now);
  sim.run(500);
  expect(sim.values(leader)).toEqual([]);
  sim.down.clear();
  sim.down.add(leader);
  sim.run(4000);
  sim.propose("kept");
  sim.down.clear();
  sim.run(2000);
  for (const id of ["a", "b", "c"]) expect(sim.values(id)).toEqual(["kept"]);
});

test("a partitioned node rejoins without disrupting the leader", () => {
  const sim = new Sim(["a", "b", "c"]);
  sim.run(3000);
  const leader = sim.leader();
  const term = sim.nodes.get(leader)!.term;
  const other = ["a", "b", "c"].find((id) => id !== leader)!;
  sim.down.add(other);
  sim.run(6000);
  sim.down.clear();
  sim.run(1000);
  expect(sim.leader()).toBe(leader);
  expect(sim.nodes.get(leader)!.term).toBe(term);
});

test("a lagging follower is caught up by snapshot", () => {
  const sim = new Sim(["a", "b", "c"]);
  sim.run(3000);
  const leader = sim.leader();
  const lag = ["a", "b", "c"].find((id) => id !== leader)!;
  sim.down.add(lag);
  for (let i = 0; i < 20; i++) sim.propose(i);
  sim.run(300);
  const node = sim.nodes.get(leader)!;
  const snap = node.compact({ count: 20 });
  expect(snap).not.toBeNull();
  node.snapshotSource = () => ({ count: 20 });
  expect(snap!.index).toBeGreaterThanOrEqual(20);
  sim.down.clear();
  sim.run(1000);
  expect(sim.nodes.get(lag)!.takeInstalled()).not.toBeNull();
  expect(sim.nodes.get(lag)!.commit).toBeGreaterThanOrEqual(snap!.index);
  sim.propose("next");
  sim.run(300);
  expect(sim.values(lag).at(-1)).toBe("next");
});

test("membership changes one voter at a time", () => {
  const sim = new Sim(["a", "b", "c"]);
  sim.nodes.set("d", new RaftCore("d", "meta", ["a", "b", "c"], 99));
  sim.run(3000);
  const leader = sim.leader();
  const wanted = ["a", "b", "c", "d"];
  for (let i = 0; i < 4; i++) {
    sim.nodes.get(leader)!.reconfigure(wanted, sim.now);
    sim.run(300);
  }
  for (const id of wanted) expect(sim.nodes.get(id)!.voters()).toEqual(wanted);
  sim.propose("four");
  sim.run(300);
  expect(sim.values("d")).toEqual(["four"]);
});

test("a single voter commits alone", () => {
  const sim = new Sim(["solo"]);
  sim.run(2500);
  sim.propose(1);
  expect(sim.values("solo")).toEqual([1]);
});
