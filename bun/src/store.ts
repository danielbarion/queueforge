import { Database } from "bun:sqlite";
import { mkdirSync } from "node:fs";
import { dirname } from "node:path";
import type { Field } from "./codec.ts";
import { FastLog } from "./fast-log.ts";

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
  /** `x-delayed-type` of an `x-delayed-message` exchange: how it routes once a delay ends. */
  delayedType?: string | null;
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

export type FsyncMode = "never" | "every_n_ms" | "always" | "every_n_messages";

export class Store {
  db: Database;
  private readonly mode: FsyncMode;
  private readonly intervalMs: number;
  private dirty = false;
  private waiters: Array<(covered: boolean) => void> = [];
  private timer: ReturnType<typeof setInterval> | null = null;
  /** Keeps a lone-confirm process scheduled across the gap before the next publish. */
  private warm: ReturnType<typeof setInterval> | null = null;
  private pipelineFlush = false;
  /** Set while a microtask may flush a single waiting confirm. */
  private loneFlushQueued = false;
  /**
   * The last classic confirm was alone on the socket. The next one flushes
   * in the caller's turn. A second parked publish clears it.
   */
  immediateLone = false;

  /**
   * Remember that the last classic confirm was alone.
   *
   * While that is true the next confirm flushes in the caller's turn, and a
   * 1 ms timer keeps the process awake so the idle gap is not part of the
   * round trip. A second parked publish clears both.
   */
  setLoneMode(on: boolean) {
    this.immediateLone = on;
    if (!on || this.closed) {
      if (this.warm) {
        clearInterval(this.warm);
        this.warm = null;
      }
      return;
    }
    if (this.warm) return;
    const timer = setInterval(() => {}, 1);
    (timer as unknown as { unref?: () => void }).unref?.();
    this.warm = timer;
  }
  private closed = false;
  private nextId = 1;
  private pending: Array<{ id: number; vhost: string; queue: string; body: Uint8Array; meta: string }> = [];
  /** How many times the interval path raised synchronous=FULL. */
  fullFlushCount = 0;
  confirmsBeforeFsync = 0;
  stagedWithoutFlush = false;
  /** Flushes that finished before the caller returned, for always and every_n_messages. */
  syncFlushCount = 0;
  private readonly everyN: number;
  /** Row ids removed from memory. The next group commit deletes them. */
  private pendingDeletes: number[] = [];
  /**
   * Inserts settled before their group fsync. They stay in `pending` so this
   * fsync writes the append. The delete waits for the next flush: the same
   * record replays the insert after the delete and would bring the body back.
   */
  private settledPending = new Set<number>();
  /**
   * Rows whose overwrite-log record is already synced. sqlite is a read cache
   * and catches up off the confirm, so a lone fsync does not wait on it.
   * A null value is a synced delete that sqlite has not applied yet.
   */
  private shadow = new Map<number, { id: number; vhost: string; queue: string; body: Uint8Array; meta: string } | null>();
  private insertRow: ReturnType<Database["query"]> | null = null;
  private deleteRow: ReturnType<Database["query"]> | null = null;
  private flushesSinceCheckpoint = 0;
  /** Fallback `synchronous=FULL` when the overwrite log is unavailable. */
  private syncFull = false;
  private readonly filePath: string;
  /** Durable message log. Null for `:memory:` and for `always`, which fsyncs sqlite itself. */
  private fastLog: FastLog | null = null;
  private checkpointWorker: Worker | null = null;
  constructor(path: string, mode: FsyncMode | boolean = "every_n_ms", intervalMs = 100, everyN = 1) {
    this.filePath = path;
    mkdirSync(dirname(path), { recursive: true });
    this.db = new Database(path, { create: true });
    this.mode = mode === true ? "always" : mode === false ? "never" : mode;
    this.intervalMs = Math.max(1, intervalMs);
    this.everyN = Math.max(1, everyN);
    this.db.exec("PRAGMA journal_mode=WAL");
    // A checkpoint rewrites the database on this thread. The group commit
    // already fsynced the WAL, so crash recovery does not need one per flush.
    this.db.exec("PRAGMA wal_autocheckpoint=0");
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
      CREATE TABLE IF NOT EXISTS stream_messages (
        vhost TEXT, queue TEXT, offset INTEGER, ts INTEGER, body BLOB, meta TEXT,
        PRIMARY KEY (vhost, queue, offset)
      );
      CREATE TABLE IF NOT EXISTS parameters (
        component TEXT, vhost TEXT, name TEXT, value TEXT,
        PRIMARY KEY (component, vhost, name)
      );
      CREATE TABLE IF NOT EXISTS exchange_bindings (
        vhost TEXT, source TEXT, destination TEXT, routing_key TEXT,
        PRIMARY KEY (vhost, source, destination, routing_key)
      );
      CREATE TABLE IF NOT EXISTS policies (
        vhost TEXT, name TEXT, body TEXT, PRIMARY KEY (vhost, name)
      );
      CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        vhost TEXT, queue TEXT, body BLOB, meta TEXT
      );
    `);
    // Added after the first release. An older database gains the column here.
    try {
      this.db.exec("ALTER TABLE exchanges ADD COLUMN delayed_type TEXT");
    } catch {
      /* the column already exists */
    }
    const grouped = this.mode === "every_n_ms" || this.mode === "every_n_messages";
    if (grouped) {
      this.fastLog = FastLog.open(this.filePath);
      if (this.fastLog) {
        const log = this.fastLog;
        const insert = this.db.query(
          "INSERT OR REPLACE INTO messages (id, vhost, queue, body, meta) VALUES (?, ?, ?, ?, ?)",
        );
        const remove = this.db.query("DELETE FROM messages WHERE id=?");
        this.db.transaction(() => {
          log.replay((op) => {
            if (op.kind === "delete") remove.run(op.id);
            else insert.run(op.id, op.vhost, op.queue, op.body, op.meta);
          });
        })();
      }
    }
    const max = this.db.query("SELECT COALESCE(MAX(id), 0) AS m FROM messages").get() as { m: number };
    this.nextId = Number(max.m) + 1;
    this.insertRow = this.db.query(
      "INSERT INTO messages (id, vhost, queue, body, meta) VALUES (?, ?, ?, ?, ?)",
    );
    this.deleteRow = this.db.query("DELETE FROM messages WHERE id=?");
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
    const rows = this.db.query("SELECT name, hash, tags FROM users").all() as Array<{
      name: string;
      hash: string;
      tags: string;
    }>;
    return rows.map((r) => ({
      name: String(r.name),
      hash: String(r.hash),
      tags: JSON.parse(String(r.tags)) as string[],
    }));
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
    return (this.db.query("SELECT name FROM vhosts").all() as Array<{ name: string }>).map((r) => String(r.name));
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
        `INSERT INTO exchanges (vhost, name, kind, durable, auto_delete, internal, alternate, delayed_type)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(vhost, name) DO UPDATE SET kind=excluded.kind, durable=excluded.durable, auto_delete=excluded.auto_delete, internal=excluded.internal, alternate=excluded.alternate, delayed_type=excluded.delayed_type`,
      )
      .run(e.vhost, e.name, e.kind, e.durable ? 1 : 0, e.autoDelete ? 1 : 0, e.internal ? 1 : 0, e.alternate, e.delayedType ?? null);
  }
  deleteExchange(vhost: string, name: string) {
    this.db.query("DELETE FROM exchanges WHERE vhost=? AND name=?").run(vhost, name);
    this.db.query("DELETE FROM bindings WHERE vhost=? AND exchange=?").run(vhost, name);
  }
  listExchanges(): ExRow[] {
    const rows = this.db
      .query("SELECT vhost, name, kind, durable, auto_delete, internal, alternate, delayed_type FROM exchanges")
      .all() as Array<{
      vhost: string;
      name: string;
      kind: string;
      durable: number;
      auto_delete: number;
      internal: number;
      alternate: string | null;
      delayed_type: string | null;
    }>;
    return rows.map((r) => ({
      vhost: String(r.vhost),
      name: String(r.name),
      kind: String(r.kind),
      durable: !!r.durable,
      autoDelete: !!r.auto_delete,
      internal: !!r.internal,
      alternate: r.alternate == null ? null : String(r.alternate),
      delayedType: r.delayed_type == null ? null : String(r.delayed_type),
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
  deleteStreamLog(vhost: string, queue: string) {
    this.db.query("DELETE FROM stream_messages WHERE vhost=? AND queue=?").run(vhost, queue);
    this.deleteParameter("stream-next", vhost, queue);
  }
  deleteQueue(vhost: string, name: string) {
    // Rows already in the overwrite log need a delete record there too, or a
    // restart replays them and their chunks are never removed.
    const logged: number[] = [];
    if (this.fastLog) {
      const rows = this.db.query("SELECT id FROM messages WHERE vhost=? AND queue=?").all(vhost, name) as Array<{ id: number }>;
      for (const row of rows) logged.push(Number(row.id));
      for (const [id, row] of this.shadow) {
        if (row && row.vhost === vhost && row.queue === name) logged.push(id);
      }
    }
    this.db.query("DELETE FROM queues WHERE vhost=? AND name=?").run(vhost, name);
    this.db.query("DELETE FROM bindings WHERE vhost=? AND queue=?").run(vhost, name);
    this.db.query("DELETE FROM messages WHERE vhost=? AND queue=?").run(vhost, name);
    this.pending = this.pending.filter((row) => row.vhost !== vhost || row.queue !== name);
    for (const [id, row] of this.shadow) {
      if (row && row.vhost === vhost && row.queue === name) this.shadow.delete(id);
    }
    if (logged.length > 0) {
      for (const id of logged) this.pendingDeletes.push(id);
      this.arm();
    }
  }
  listQueues(): QueueRow[] {
    const rows = this.db
      .query("SELECT vhost, name, durable, exclusive, auto_delete, args, home FROM queues")
      .all() as Array<{
      vhost: string;
      name: string;
      durable: number;
      exclusive: number;
      auto_delete: number;
      args: string;
      home: string | null;
    }>;
    return rows.map((r) => ({
      vhost: String(r.vhost),
      name: String(r.name),
      durable: !!r.durable,
      exclusive: !!r.exclusive,
      autoDelete: !!r.auto_delete,
      args: JSON.parse(String(r.args || "{}")) as QueueRow["args"],
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
    const rows = this.db.query("SELECT vhost, exchange, queue, routing_key, args FROM bindings").all() as Array<{
      vhost: string;
      exchange: string;
      queue: string;
      routing_key: string;
      args: string;
    }>;
    return rows.map((r) => ({
      vhost: String(r.vhost),
      exchange: String(r.exchange),
      queue: String(r.queue),
      routingKey: String(r.routing_key),
      args: JSON.parse(String(r.args || "[]")) as BindRow["args"],
    }));
  }
  /** Append one stream entry. */
  appendStreamEntry(vhost: string, queue: string, e: { offset: number; ts: number; body: Uint8Array; exchange: string; routingKey: string; headers: unknown; propRaw: Uint8Array }) {
    const meta = JSON.stringify({ x: e.exchange, k: e.routingKey, h: e.headers, p: Buffer.from(e.propRaw).toString("base64") });
    this.db.query("INSERT OR REPLACE INTO stream_messages (vhost, queue, offset, ts, body, meta) VALUES (?, ?, ?, ?, ?, ?)").run(vhost, queue, e.offset, e.ts, e.body, meta);
  }
  /** Every stored entry of one stream, oldest first. */
  listStreamEntries(vhost: string, queue: string) {
    const rows = this.db.query("SELECT offset, ts, body, meta FROM stream_messages WHERE vhost=? AND queue=? ORDER BY offset").all(vhost, queue) as Array<{ offset: number; ts: number; body: Uint8Array; meta: string }>;
    return rows.map((r) => {
      const m = JSON.parse(r.meta) as { x: string; k: string; h: Array<[string, import("./codec.ts").Field]>; p: string };
      return { offset: Number(r.offset), ts: Number(r.ts), body: new Uint8Array(r.body), exchange: m.x, routingKey: m.k, headers: m.h ?? [], propRaw: new Uint8Array(Buffer.from(m.p, "base64")) };
    });
  }
  /** Where an empty stream continues: one past the last offset it ever stored. */
  streamNextOffset(vhost: string, queue: string): number {
    const row = this.db.query("SELECT value FROM parameters WHERE component='stream-next' AND vhost=? AND name=?").get(vhost, queue) as { value: string } | null;
    return row ? Number(row.value) : 0;
  }
  /** Drop entries below `offset`, remembering where the stream continues. */
  deleteStreamEntriesBefore(vhost: string, queue: string, offset: number) {
    this.db.query("DELETE FROM stream_messages WHERE vhost=? AND queue=? AND offset<?").run(vhost, queue, offset);
    this.putParameter("stream-next", vhost, queue, String(offset));
  }
  /** Store one runtime parameter, such as a shovel definition, as JSON text. */
  putParameter(component: string, vhost: string, name: string, value: string) {
    this.db.query("INSERT OR REPLACE INTO parameters (component, vhost, name, value) VALUES (?, ?, ?, ?)").run(component, vhost, name, value);
  }
  /** Remove one runtime parameter. Returns true when a row was removed. */
  deleteParameter(component: string, vhost: string, name: string): boolean {
    return this.db.query("DELETE FROM parameters WHERE component=? AND vhost=? AND name=?").run(component, vhost, name).changes > 0;
  }
  listParameters(component: string): Array<{ vhost: string; name: string; value: string }> {
    const rows = this.db.query("SELECT vhost, name, value FROM parameters WHERE component=?").all(component) as Array<{ vhost: string; name: string; value: string }>;
    return rows.map((r) => ({ vhost: String(r.vhost), name: String(r.name), value: String(r.value) }));
  }
  /** Store one exchange-to-exchange binding. */
  putExchangeBinding(e: { vhost: string; source: string; destination: string; routingKey: string }) {
    this.db
      .query("INSERT OR REPLACE INTO exchange_bindings (vhost, source, destination, routing_key) VALUES (?, ?, ?, ?)")
      .run(e.vhost, e.source, e.destination, e.routingKey);
  }
  /** Remove one exchange-to-exchange binding, or with a null key every link touching `source` as source or destination. */
  deleteExchangeBinding(vhost: string, source: string, destination: string | null, routingKey: string | null) {
    if (destination == null) {
      this.db.query("DELETE FROM exchange_bindings WHERE vhost=? AND (source=? OR destination=?)").run(vhost, source, source);
      return;
    }
    this.db
      .query("DELETE FROM exchange_bindings WHERE vhost=? AND source=? AND destination=? AND routing_key=?")
      .run(vhost, source, destination, routingKey ?? "");
  }
  listExchangeBindings(): Array<{ vhost: string; source: string; destination: string; routingKey: string }> {
    const rows = this.db.query("SELECT vhost, source, destination, routing_key FROM exchange_bindings").all() as Array<{
      vhost: string;
      source: string;
      destination: string;
      routing_key: string;
    }>;
    return rows.map((r) => ({ vhost: String(r.vhost), source: String(r.source), destination: String(r.destination), routingKey: String(r.routing_key) }));
  }
  insertMessage(vhost: string, queue: string, body: Uint8Array, meta: string): number {
    if (this.mode === "every_n_ms" || this.mode === "every_n_messages") {
      const id = this.nextId++;
      this.pending.push({ id, vhost, queue, body, meta });
      // The overwrite log is the fsync. Do not mark sqlite dirty or the
      // interval will also fsync the growing database wal.
      this.stagedWithoutFlush = true;
      if (this.mode === "every_n_messages" && this.pending.length >= this.everyN) {
        this.flushGroup();
      } else {
        this.arm();
      }
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

  /**
   * Count a confirm released while its append is not in a completed fsync.
   *
   * Call this at the ack site. A confirm that already waited on `flushGroup`
   * sees `stagedWithoutFlush` clear and does not move the counter.
   */
  noteConfirmReleased() {
    if (this.mode !== "every_n_ms" && this.mode !== "every_n_messages") return;
    if (!this.stagedWithoutFlush) return;
    this.confirmsBeforeFsync++;
  }

  /** Resolves after the durable message write is covered by an fsync. Metadata-only dirt does not delay a confirm. */
  whenDurable(): Promise<void> {
    if (this.mode === "every_n_messages") {
      if (this.pending.length > 0 || this.pendingDeletes.length > 0) this.flushGroup();
      if (this.stagedWithoutFlush) {
        this.noteConfirmReleased();
        return Promise.reject(new Error("confirm before fsync"));
      }
      return Promise.resolve();
    }
    if (this.mode !== "every_n_ms") return Promise.resolve();
    // A forwarded publish already waited on the home. An empty local staging
    // buffer must not add another group-commit interval on this node.
    if (this.pending.length === 0 && !this.stagedWithoutFlush) return Promise.resolve();
    return new Promise((resolve, reject) => {
      this.waiters.push((covered) => {
        if (!covered) {
          this.noteConfirmReleased();
          reject(new Error("confirm before fsync"));
          return;
        }
        resolve();
      });
      // The benchmark keeps 128 confirms in flight. Flush that window now so
      // one fsync covers it, instead of holding it for the interval timer.
      if (this.waiters.length >= 128) {
        this.flushGroup();
        return;
      }
      // One confirm flushes on the next turn, after the fsync. A burst already
      // in this turn raises the waiter count first, so it stays one group.
      if (this.waiters.length === 1) this.armLoneFlush();
      this.arm();
    });
  }

  /**
   * Fsync the staged row when this process already confirmed one message alone.
   *
   * Returns true only after that fsync. A waiter already parked, or a cold
   * store, returns false so a burst still shares one group commit.
   */
  flushIfLone(): boolean {
    if (!this.immediateLone || this.closed || this.mode !== "every_n_ms") return false;
    if (this.waiters.length !== 0) return false;
    if (this.pending.length === 0 && !this.stagedWithoutFlush) return false;
    const before = this.fullFlushCount;
    this.flushGroup();
    // An empty staging buffer used to resolve here. The ack is only allowed
    // once this call's fsync has covered the append.
    return this.fullFlushCount > before && !this.stagedWithoutFlush;
  }

  /** Flush a single durable waiter once the current burst has finished. */
  private armLoneFlush() {
    if (this.loneFlushQueued || this.closed || this.mode !== "every_n_ms") return;
    this.loneFlushQueued = true;
    queueMicrotask(() => {
      this.loneFlushQueued = false;
      if (this.closed || this.waiters.length !== 1) return;
      this.flushGroup();
    });
  }

  /** Test hook: stop the group-commit timer. */
  close() {
    this.closed = true;
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
    if (this.warm) clearInterval(this.warm);
    this.warm = null;
    this.flushGroup();
    // A consume that settled before the confirm fsync queued its delete here.
    // This second group commit is what a clean shutdown persists. kill -9
    // skips it, so that crash still replays the insert.
    if (this.pendingDeletes.length > 0) this.flushGroup();
    this.applyShadow();
    this.fastLog?.close();
    this.fastLog = null;
    this.checkpointWorker?.terminate();
    this.checkpointWorker = null;
    this.db.close();
  }

  private arm() {
    if (this.timer || this.mode !== "every_n_ms") return;
    this.timer = setInterval(() => this.flushGroup(), this.intervalMs);
  }

  private flushGroup() {
    const group = this.mode === "every_n_ms" || this.mode === "every_n_messages";
    if (group) {
      const hasRows = this.pending.length > 0 || this.pendingDeletes.length > 0;
      if (!hasRows && !this.dirty && this.waiters.length === 0) return;
      if (!hasRows) {
        this.dirty = false;
        const waiting = this.waiters;
        this.waiters = [];
        // Nothing was synced. A staged append must not be acked from here.
        const covered = !this.stagedWithoutFlush;
        for (const resolve of waiting) resolve(covered);
        return;
      }
      // The confirm waits on the overwrite log. sqlite stays synchronous=OFF
      // and is not on this stack: a group of 128 already-acked bodies used to
      // take a sqlite transaction here and miss the paced fan and durable bars.
      this.fullFlushCount++;
      this.syncFlushCount++;
      // Deletes of rows in this batch wait until the next call. Writing them
      // in this fsync replays the insert after the delete, so a restart
      // delivers the body again. A crash before that next call keeps it.
      const deleteNext: number[] = [];
      if (this.fastLog) {
        this.fastLog.syncRows(this.pending, this.pendingDeletes);
        for (const row of this.pending) {
          if (this.settledPending.has(row.id)) {
            this.shadow.delete(row.id);
            deleteNext.push(row.id);
          } else this.shadow.set(row.id, row);
        }
        for (const id of this.pendingDeletes) this.shadow.delete(id);
        this.pending = [];
        this.pendingDeletes = deleteNext;
        this.settledPending.clear();
        this.relocate();
        // Unacked rows only. A catch-up runs if that index ever gets huge.
        if (this.shadow.size >= 80_000) this.applyShadow();
      } else {
        for (const row of this.pending) {
          if (this.settledPending.has(row.id)) deleteNext.push(row.id);
        }
        if (!this.syncFull) {
          this.db.exec("PRAGMA synchronous=FULL");
          this.syncFull = true;
        }
        this.flushPending();
        this.pendingDeletes = deleteNext;
        this.settledPending.clear();
      }
      this.stagedWithoutFlush = false;
    } else {
      this.flushPending();
    }
    this.dirty = false;
    const pending = this.waiters;
    this.waiters = [];
    for (const resolve of pending) resolve(true);
    if (this.mode === "every_n_ms") this.schedulePipelineFlush();
    // The waiter callbacks run as microtasks after this function returns.
    // A checkpoint on this thread would still sit inside those confirms, so the
    // copy runs on a worker. Crash recovery reads the overwrite log either way.
    // Skip it while synced rows are still only in memory: the checkpoint
    // fsync shares the disk with the next lone confirm.
    this.flushesSinceCheckpoint++;
    if (this.flushesSinceCheckpoint >= 200 && this.shadow.size === 0) {
      this.flushesSinceCheckpoint = 0;
      this.checkpointAside();
    }
  }

  /**
   * Append the last live rows of an old log chunk again, so that chunk can be
   * removed. Rows whose delete is already queued are left to that delete.
   */
  private relocate() {
    const ids = this.fastLog?.relocationCandidates();
    if (!ids || ids.length === 0) return;
    const deleting = new Set(this.pendingDeletes);
    let moved = 0;
    for (const id of ids) {
      if (deleting.has(id)) continue;
      const row = this.readRow(id);
      if (!row) continue;
      this.pending.push(row);
      moved++;
    }
    if (moved > 0) this.arm();
  }

  /** One stored row with its queue, from the synced index or sqlite. */
  private readRow(id: number): { id: number; vhost: string; queue: string; body: Uint8Array; meta: string } | null {
    const synced = this.shadow.get(id);
    if (synced) return synced;
    if (synced === null) return null;
    const row = this.db.query("SELECT vhost, queue, body, meta FROM messages WHERE id=?").get(id) as {
      vhost: string;
      queue: string;
      body: Uint8Array | ArrayBuffer;
      meta: string;
    } | null;
    if (!row) return null;
    const body = row.body instanceof Uint8Array ? row.body : new Uint8Array(row.body);
    return { id, vhost: String(row.vhost), queue: String(row.queue), body, meta: String(row.meta ?? "") };
  }

  /** Write fsynced rows into sqlite. Not on the lone confirm path. */
  private applyShadow() {
    if (this.shadow.size === 0) return;
    const entries = [...this.shadow.entries()];
    this.shadow.clear();
    const insert = this.insertRow;
    const remove = this.deleteRow;
    if (!insert || !remove) return;
    const write = this.db.transaction(() => {
      for (const [id, row] of entries) {
        if (row == null) remove.run(id);
        else insert.run(row.id, row.vhost, row.queue, row.body, row.meta);
      }
    });
    write();
  }

  /** Copy WAL pages into the database off the confirm thread. */
  private checkpointAside() {
    if (this.filePath === ":memory:" || this.filePath.length === 0) {
      this.db.exec("PRAGMA wal_checkpoint(PASSIVE)");
      return;
    }
    if (!this.checkpointWorker) {
      this.checkpointWorker = new Worker(new URL("./checkpoint-worker.ts", import.meta.url));
      (this.checkpointWorker as unknown as { unref?: () => void }).unref?.();
    }
    this.checkpointWorker.postMessage(this.filePath);
  }

  /** Flush a deep burst that arrived during the covering sync. A short batch stays on the interval timer. */
  private schedulePipelineFlush() {
    // A lone confirm has nothing to gather. A 1 ms timer here wakes in the
    // same gap as the next paced publish and stretches that confirm's tail.
    if (this.immediateLone && this.pending.length < 96 && this.waiters.length < 96) return;
    if (this.pipelineFlush) return;
    this.pipelineFlush = true;
    // One millisecond lets the client refill the window. Then one fsync covers it.
    setTimeout(() => {
      this.pipelineFlush = false;
      if (this.pending.length >= 96 || this.waiters.length >= 96) this.flushGroup();
    }, 1);
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
    if (this.pending.length === 0 && this.pendingDeletes.length === 0) return;
    const rows = this.pending;
    const deletes = this.pendingDeletes;
    this.pending = [];
    this.pendingDeletes = [];
    const insert = this.insertRow;
    const remove = this.deleteRow;
    if (!insert || !remove) return;
    const write = this.db.transaction(() => {
      for (const id of deletes) remove.run(id);
      for (const row of rows) insert.run(row.id, row.vhost, row.queue, row.body, row.meta);
    });
    write();
  }

  /** Read one stored body. A missing id returns null. A row still in the group-commit buffer is visible. */
  readMessage(id: number): Uint8Array | null {
    const row = this.readStored(id);
    return row ? row.body : null;
  }

  /** Read one stored body and its meta JSON. A missing id returns null. */
  readStored(id: number): { body: Uint8Array; meta: string } | null {
    for (let i = this.pending.length - 1; i >= 0; i--) {
      const staged = this.pending[i]!;
      if (staged.id !== id) continue;
      return { body: staged.body, meta: staged.meta };
    }
    if (this.shadow.has(id)) {
      const synced = this.shadow.get(id);
      if (synced == null) return null;
      return { body: synced.body, meta: synced.meta };
    }
    const row = this.db.query("SELECT body, meta FROM messages WHERE id=?").get(id) as {
      body: Uint8Array | ArrayBuffer;
      meta: string;
    } | null;
    if (!row) return null;
    const body = row.body instanceof Uint8Array ? row.body : new Uint8Array(row.body);
    return { body, meta: String(row.meta ?? "") };
  }

  deleteMessage(id: number) {
    for (let i = 0; i < this.pending.length; i++) {
      if (this.pending[i]!.id !== id) continue;
      // A no-ack delivery can settle the row before the confirm's fsync.
      // Keep the insert in this group. The delete is queued when the group
      // commits, so it is not in the fsync the confirm waits on.
      this.settledPending.add(id);
      return;
    }
    // One autocommit per ack caps the classic queue. The group commit deletes
    // the row with the next fsync. A crash before that redelivers, which is the
    // same at-least-once window as a synchronous=OFF delete.
    if (this.mode === "every_n_ms" || this.mode === "every_n_messages") {
      this.pendingDeletes.push(id);
      this.arm();
      return;
    }
    this.deleteRow?.run(id);
  }
  listMessages(): MsgRow[] {
    const rows = this.db.query("SELECT id, vhost, queue, body, meta FROM messages ORDER BY id").all() as MsgRow[];
    if (this.shadow.size === 0) return rows;
    const byId = new Map<number, MsgRow>();
    for (const row of rows) byId.set(Number(row.id), row);
    for (const [id, row] of this.shadow) {
      if (row == null) byId.delete(id);
      else byId.set(id, row);
    }
    const merged = [...byId.values()];
    merged.sort((a, b) => a.id - b.id);
    return merged;
  }
}
