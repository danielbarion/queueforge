import { Database } from "bun:sqlite";
import { mkdirSync } from "node:fs";
import { dirname } from "node:path";
import type { Field } from "./codec.ts";

export type UserRow = { name: string; hash: string; tags: string[] };
export type PermRow = { user: string; vhost: string; configure: string; write: string; read: string };
export type ExRow = {
  vhost: string;
  name: string;
  kind: string;
  durable: boolean;
  autoDelete: boolean;
  internal: boolean;
  alternate: string | null;
};
export type QueueRow = {
  vhost: string;
  name: string;
  durable: boolean;
  exclusive: boolean;
  autoDelete: boolean;
  args: Record<string, string | number>;
  home: string | null;
};
export type BindRow = {
  vhost: string;
  exchange: string;
  queue: string;
  routingKey: string;
  args: Array<[string, Field]>;
};
export type MsgRow = {
  id: number;
  vhost: string;
  queue: string;
  body: Uint8Array;
  meta: string;
};

export type FsyncMode = "never" | "every_n_ms" | "always";

export class Store {
  db: Database;
  private readonly mode: FsyncMode;
  private readonly intervalMs: number;
  private dirty = false;
  private waiters: Array<() => void> = [];
  private timer: ReturnType<typeof setInterval> | null = null;
  private nextId = 1;
  private pending: Array<{ id: number; vhost: string; queue: string; body: Uint8Array; meta: string }> = [];
  /** How many times the interval path raised synchronous=FULL. */
  fullFlushCount = 0;
  constructor(path: string, mode: FsyncMode | boolean = "every_n_ms", intervalMs = 100) {
    mkdirSync(dirname(path), { recursive: true });
    this.db = new Database(path, { create: true });
    this.mode = mode === true ? "always" : mode === false ? "never" : mode;
    this.intervalMs = Math.max(1, intervalMs);
    this.db.exec("PRAGMA journal_mode=WAL");
    this.db.exec(this.mode === "always" ? "PRAGMA synchronous=FULL" : "PRAGMA synchronous=OFF");
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS users (name TEXT PRIMARY KEY, hash TEXT NOT NULL, tags TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS vhosts (name TEXT PRIMARY KEY);
      CREATE TABLE IF NOT EXISTS permissions (
        user TEXT, vhost TEXT, configure TEXT, write TEXT, read TEXT,
        PRIMARY KEY (user, vhost)
      );
      CREATE TABLE IF NOT EXISTS exchanges (
        vhost TEXT, name TEXT, kind TEXT, durable INT, auto_delete INT, internal INT, alternate TEXT,
        PRIMARY KEY (vhost, name)
      );
      CREATE TABLE IF NOT EXISTS queues (
        vhost TEXT, name TEXT, durable INT, exclusive INT, auto_delete INT, args TEXT, home TEXT,
        PRIMARY KEY (vhost, name)
      );
      CREATE TABLE IF NOT EXISTS bindings (
        vhost TEXT, exchange TEXT, queue TEXT, routing_key TEXT, args TEXT,
        PRIMARY KEY (vhost, exchange, queue, routing_key, args)
      );
      CREATE TABLE IF NOT EXISTS policies (
        vhost TEXT, name TEXT, body TEXT, PRIMARY KEY (vhost, name)
      );
      CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        vhost TEXT, queue TEXT, body BLOB, meta TEXT
      );
    `);
    const max = this.db.query("SELECT COALESCE(MAX(id), 0) AS m FROM messages").get() as { m: number };
    this.nextId = Number(max.m) + 1;
  }

  putPolicy(p: unknown) {
    const row = p as { vhost: string; name: string };
    this.db
      .query("INSERT INTO policies (vhost, name, body) VALUES (?, ?, ?) ON CONFLICT(vhost, name) DO UPDATE SET body=excluded.body")
      .run(row.vhost, row.name, JSON.stringify(p));
  }
  listPolicies(): unknown[] {
    return this.db
      .query("SELECT body FROM policies")
      .all()
      .map((r) => JSON.parse(String((r as { body: string }).body)));
  }
  deletePolicy(vhost: string, name: string) {
    this.db.query("DELETE FROM policies WHERE vhost=? AND name=?").run(vhost, name);
  }
  listUsers(): UserRow[] {
    return this.db
      .query("SELECT name, hash, tags FROM users")
      .all()
      .map((r) => ({ name: String(r.name), hash: String(r.hash), tags: JSON.parse(String(r.tags)) }));
  }
  getUser(name: string): UserRow | null {
    const r = this.db.query("SELECT name, hash, tags FROM users WHERE name=?").get(name) as
      | { name: string; hash: string; tags: string }
      | null;
    return r ? { name: r.name, hash: r.hash, tags: JSON.parse(r.tags) } : null;
  }
  putUser(u: UserRow) {
    this.db
      .query("INSERT INTO users (name, hash, tags) VALUES (?, ?, ?) ON CONFLICT(name) DO UPDATE SET hash=excluded.hash, tags=excluded.tags")
      .run(u.name, u.hash, JSON.stringify(u.tags));
  }
  deleteUser(name: string) {
    this.db.query("DELETE FROM permissions WHERE user=?").run(name);
    this.db.query("DELETE FROM users WHERE name=?").run(name);
  }
  listVhosts(): string[] {
    return this.db.query("SELECT name FROM vhosts").all().map((r) => String(r.name));
  }
  ensureVhost(name: string) {
    this.db.query("INSERT OR IGNORE INTO vhosts (name) VALUES (?)").run(name);
  }
  deleteVhost(name: string) {
    this.db.query("DELETE FROM vhosts WHERE name=?").run(name);
  }
  listPerms(): PermRow[] {
    return this.db.query("SELECT user, vhost, configure, write, read FROM permissions").all() as PermRow[];
  }
  putPerm(p: PermRow) {
    this.db
      .query(
        `INSERT INTO permissions (user, vhost, configure, write, read) VALUES (?, ?, ?, ?, ?)
         ON CONFLICT(user, vhost) DO UPDATE SET configure=excluded.configure, write=excluded.write, read=excluded.read`,
      )
      .run(p.user, p.vhost, p.configure, p.write, p.read);
  }
  deletePerm(user: string, vhost: string) {
    this.db.query("DELETE FROM permissions WHERE user=? AND vhost=?").run(user, vhost);
  }
  putExchange(e: ExRow) {
    this.db
      .query(
        `INSERT INTO exchanges (vhost, name, kind, durable, auto_delete, internal, alternate)
         VALUES (?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(vhost, name) DO UPDATE SET kind=excluded.kind, durable=excluded.durable, auto_delete=excluded.auto_delete, internal=excluded.internal, alternate=excluded.alternate`,
      )
      .run(e.vhost, e.name, e.kind, e.durable ? 1 : 0, e.autoDelete ? 1 : 0, e.internal ? 1 : 0, e.alternate);
  }
  deleteExchange(vhost: string, name: string) {
    this.db.query("DELETE FROM exchanges WHERE vhost=? AND name=?").run(vhost, name);
    this.db.query("DELETE FROM bindings WHERE vhost=? AND exchange=?").run(vhost, name);
  }
  listExchanges(): ExRow[] {
    return this.db
      .query("SELECT vhost, name, kind, durable, auto_delete, internal, alternate FROM exchanges")
      .all()
      .map((r) => ({
        vhost: String(r.vhost),
        name: String(r.name),
        kind: String(r.kind),
        durable: !!r.durable,
        autoDelete: !!r.auto_delete,
        internal: !!r.internal,
        alternate: r.alternate == null ? null : String(r.alternate),
      }));
  }
  putQueue(q: QueueRow) {
    const write = () => {
      this.db
        .query(
          `INSERT INTO queues (vhost, name, durable, exclusive, auto_delete, args, home)
           VALUES (?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(vhost, name) DO UPDATE SET durable=excluded.durable, exclusive=excluded.exclusive, auto_delete=excluded.auto_delete, args=excluded.args, home=excluded.home`,
        )
        .run(q.vhost, q.name, q.durable ? 1 : 0, q.exclusive ? 1 : 0, q.autoDelete ? 1 : 0, JSON.stringify(q.args), q.home);
    };
    if (q.durable) this.durableWrite(write);
    else write();
  }
  deleteQueue(vhost: string, name: string) {
    this.db.query("DELETE FROM queues WHERE vhost=? AND name=?").run(vhost, name);
    this.db.query("DELETE FROM bindings WHERE vhost=? AND queue=?").run(vhost, name);
    this.db.query("DELETE FROM messages WHERE vhost=? AND queue=?").run(vhost, name);
  }
  listQueues(): QueueRow[] {
    return this.db
      .query("SELECT vhost, name, durable, exclusive, auto_delete, args, home FROM queues")
      .all()
      .map((r) => ({
        vhost: String(r.vhost),
        name: String(r.name),
        durable: !!r.durable,
        exclusive: !!r.exclusive,
        autoDelete: !!r.auto_delete,
        args: JSON.parse(String(r.args || "{}")),
        home: r.home == null ? null : String(r.home),
      }));
  }
  putBinding(b: BindRow) {
    this.db
      .query(
        `INSERT OR REPLACE INTO bindings (vhost, exchange, queue, routing_key, args) VALUES (?, ?, ?, ?, ?)`,
      )
      .run(b.vhost, b.exchange, b.queue, b.routingKey, JSON.stringify(b.args));
  }
  deleteBinding(vhost: string, exchange: string, queue: string, routingKey: string, argsJson: string | null) {
    if (argsJson == null) {
      this.db.query("DELETE FROM bindings WHERE vhost=? AND exchange=? AND queue=? AND routing_key=?").run(vhost, exchange, queue, routingKey);
    } else {
      this.db
        .query("DELETE FROM bindings WHERE vhost=? AND exchange=? AND queue=? AND routing_key=? AND args=?")
        .run(vhost, exchange, queue, routingKey, argsJson);
    }
  }
  listBindings(): BindRow[] {
    return this.db
      .query("SELECT vhost, exchange, queue, routing_key, args FROM bindings")
      .all()
      .map((r) => ({
        vhost: String(r.vhost),
        exchange: String(r.exchange),
        queue: String(r.queue),
        routingKey: String(r.routing_key),
        args: JSON.parse(String(r.args || "[]")),
      }));
  }
  insertMessage(vhost: string, queue: string, body: Uint8Array, meta: string): number {
    if (this.mode === "every_n_ms") {
      const id = this.nextId++;
      this.pending.push({ id, vhost, queue, body, meta });
      this.dirty = true;
      this.arm();
      return id;
    }
    let id = 0;
    this.durableWrite(() => {
      const res = this.db
        .query("INSERT INTO messages (vhost, queue, body, meta) VALUES (?, ?, ?, ?)")
        .run(vhost, queue, body, meta);
      id = Number(res.lastInsertRowid);
    });
    if (id >= this.nextId) this.nextId = id + 1;
    return id;
  }

  /** Resolves after the durable write is covered by an fsync. */
  whenDurable(): Promise<void> {
    if (this.mode !== "every_n_ms") return Promise.resolve();
    if (!this.dirty && this.waiters.length === 0) return Promise.resolve();
    return new Promise((resolve) => {
      this.waiters.push(resolve);
      this.arm();
    });
  }

  /** Test hook: stop the group-commit timer. */
  close() {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
    this.flushGroup();
    this.db.close();
  }

  private arm() {
    if (this.timer || this.mode !== "every_n_ms") return;
    this.timer = setInterval(() => this.flushGroup(), this.intervalMs);
  }

  private flushGroup() {
    this.flushPending();
    if (!this.dirty && this.waiters.length === 0) return;
    if (this.mode === "every_n_ms") {
      this.fullFlushCount++;
      this.db.exec("PRAGMA synchronous=FULL");
      this.db.exec("PRAGMA wal_checkpoint(FULL)");
      this.db.exec("PRAGMA synchronous=OFF");
    }
    this.dirty = false;
    const pending = this.waiters;
    this.waiters = [];
    for (const resolve of pending) resolve();
  }

  private durableWrite(fn: () => void) {
    if (this.mode === "always") {
      fn();
      return;
    }
    fn();
    if (this.mode === "every_n_ms") {
      this.dirty = true;
      this.arm();
    }
  }
  private flushPending() {
    if (this.pending.length === 0) return;
    const rows = this.pending;
    this.pending = [];
    const insert = this.db.query(
      "INSERT INTO messages (id, vhost, queue, body, meta) VALUES (?, ?, ?, ?, ?)",
    );
    const write = this.db.transaction((batch: typeof rows) => {
      for (const row of batch) insert.run(row.id, row.vhost, row.queue, row.body, row.meta);
    });
    write(rows);
  }

  deleteMessage(id: number) {
    const idx = this.pending.findIndex((row) => row.id === id);
    if (idx >= 0) {
      this.pending.splice(idx, 1);
      return;
    }
    this.db.query("DELETE FROM messages WHERE id=?").run(id);
  }
  listMessages(): MsgRow[] {
    return this.db.query("SELECT id, vhost, queue, body, meta FROM messages ORDER BY id").all() as MsgRow[];
  }
}
