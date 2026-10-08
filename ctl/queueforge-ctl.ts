#!/usr/bin/env bun
/**
 * queueforge-ctl: one command-line tool for all three QueueForge brokers.
 *
 * It speaks the RabbitMQ management HTTP API with Basic auth, so the same
 * commands also work against RabbitMQ itself. Command names follow
 * rabbitmqctl; output is tab-separated, one row per line, like rabbitmqctl.
 *
 *   bun ctl/queueforge-ctl.ts [--url URL] [--user U] [--password P] [-p VHOST] <command> [args]
 *
 * The connection can also come from QUEUEFORGE_CTL_URL, QUEUEFORGE_CTL_USER
 * and QUEUEFORGE_CTL_PASSWORD. Exit status is 0 on success, 1 when the broker
 * refused the request, and 2 for a usage error.
 */
import { readFileSync, writeFileSync } from "node:fs";

type Opts = { url: string; user: string; password: string; vhost: string; priority: number; applyTo: string };

class UsageError extends Error {}

const HELP = `usage: queueforge-ctl [--url URL] [--user U] [--password P] [-p VHOST] <command> [args]

status                                   broker overview
list_vhosts | list_users | list_permissions | list_policies | list_operator_policies
list_queues | list_exchanges | list_bindings | list_connections | list_consumers
add_vhost NAME | delete_vhost NAME
add_user NAME PASSWORD | delete_user NAME | change_password NAME PASSWORD
set_user_tags NAME [TAG...]
set_permissions USER CONF WRITE READ | clear_permissions USER
set_topic_permissions USER EXCHANGE WRITE READ
set_policy NAME PATTERN DEFINITION [--priority N] [--apply-to queues|exchanges|all]
clear_policy NAME | set_operator_policy NAME PATTERN DEFINITION | clear_operator_policy NAME
set_vhost_limits DEFINITION | set_user_limits USER DEFINITION
purge_queue NAME | delete_queue NAME
trace_on | trace_off
export_definitions FILE | import_definitions FILE`;

function parseArgs(argv: string[]): { opts: Opts; cmd: string; args: string[] } {
  const opts: Opts = {
    url: process.env.QUEUEFORGE_CTL_URL ?? "http://localhost:15672",
    user: process.env.QUEUEFORGE_CTL_USER ?? "admin",
    password: process.env.QUEUEFORGE_CTL_PASSWORD ?? "",
    vhost: "/",
    priority: 0,
    applyTo: "all",
  };
  const rest: string[] = [];
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i]!;
    const value = () => {
      const v = argv[++i];
      if (v === undefined) throw new UsageError(`${a} needs a value`);
      return v;
    };
    if (a === "--url") opts.url = value();
    else if (a === "--user" || a === "-u") opts.user = value();
    else if (a === "--password") opts.password = value();
    else if (a === "-p" || a === "--vhost") opts.vhost = value();
    else if (a === "--priority") opts.priority = Number(value());
    else if (a === "--apply-to") opts.applyTo = value();
    else if (a === "-h" || a === "--help") rest.unshift("help");
    else rest.push(a);
  }
  const [cmd = "help", ...args] = rest;
  return { opts, cmd, args };
}

async function api(opts: Opts, method: string, path: string, body?: unknown): Promise<unknown> {
  const res = await fetch(`${opts.url.replace(/\/$/, "")}${path}`, {
    method,
    headers: {
      authorization: `Basic ${btoa(`${opts.user}:${opts.password}`)}`,
      "content-type": "application/json",
    },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const text = await res.text();
  if (!res.ok) throw new Error(`${method} ${path}: ${res.status} ${text.slice(0, 300)}`);
  if (!text) return null;
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}

/** Lists come back as arrays from RabbitMQ and as `{ items }` from older QueueForge builds. */
function rows(value: unknown): Array<Record<string, unknown>> {
  if (Array.isArray(value)) return value as Array<Record<string, unknown>>;
  const items = (value as { items?: unknown } | null)?.items;
  return Array.isArray(items) ? (items as Array<Record<string, unknown>>) : [];
}

function print(list: Array<Record<string, unknown>>, columns: string[]) {
  for (const row of list) console.log(columns.map((c) => cell(row[c])).join("\t"));
}

function cell(v: unknown): string {
  if (v == null) return "";
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
}

function need(args: string[], n: number, usage: string) {
  if (args.length < n) throw new UsageError(`usage: ${usage}`);
}

const enc = encodeURIComponent;

function definition(text: string): Record<string, unknown> {
  try {
    const parsed = JSON.parse(text) as unknown;
    if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) return parsed as Record<string, unknown>;
  } catch {
    /* reported below */
  }
  throw new UsageError(`definition must be a JSON object: ${text}`);
}

export async function run(argv: string[]): Promise<number> {
  const { opts, cmd, args } = parseArgs(argv);
  const v = enc(opts.vhost);
  switch (cmd) {
    case "help":
      console.log(HELP);
      return 0;
    case "status": {
      const o = (await api(opts, "GET", "/api/overview")) as Record<string, unknown>;
      const totals = (o.object_totals ?? {}) as Record<string, unknown>;
      console.log(`product\t${cell(o.product_name)} ${cell(o.product_version ?? o.rabbitmq_version)}`);
      for (const k of ["connections", "channels", "queues", "exchanges", "consumers"]) console.log(`${k}\t${cell(totals[k])}`);
      return 0;
    }
    case "list_vhosts":
      print(rows(await api(opts, "GET", "/api/vhosts")), ["name"]);
      return 0;
    case "list_users":
      print(rows(await api(opts, "GET", "/api/users")), ["name", "tags"]);
      return 0;
    case "list_permissions":
      print(rows(await api(opts, "GET", "/api/permissions")).filter((p) => p.vhost === opts.vhost), ["user", "configure", "write", "read"]);
      return 0;
    case "list_policies":
      print(rows(await api(opts, "GET", `/api/policies/${v}`)), ["name", "pattern", "apply-to", "definition", "priority"]);
      return 0;
    case "list_operator_policies":
      print(rows(await api(opts, "GET", `/api/operator-policies/${v}`)), ["name", "pattern", "apply-to", "definition", "priority"]);
      return 0;
    case "list_queues":
      print(rows(await api(opts, "GET", `/api/queues/${v}`)), ["name", "messages"]);
      return 0;
    case "list_exchanges":
      print(rows(await api(opts, "GET", `/api/exchanges/${v}`)), ["name", "type"]);
      return 0;
    case "list_bindings":
      print(rows(await api(opts, "GET", `/api/bindings/${v}`)), ["source", "destination", "destination_type", "routing_key"]);
      return 0;
    case "list_connections":
      print(rows(await api(opts, "GET", "/api/connections")), ["name", "user", "vhost"]);
      return 0;
    case "list_consumers":
      print(rows(await api(opts, "GET", `/api/consumers/${v}`)), ["consumer_tag", "queue"]);
      return 0;
    case "add_vhost":
      need(args, 1, "add_vhost NAME");
      await api(opts, "PUT", `/api/vhosts/${enc(args[0]!)}`, {});
      return 0;
    case "delete_vhost":
      need(args, 1, "delete_vhost NAME");
      await api(opts, "DELETE", `/api/vhosts/${enc(args[0]!)}`);
      return 0;
    case "add_user":
      need(args, 2, "add_user NAME PASSWORD");
      await api(opts, "PUT", `/api/users/${enc(args[0]!)}`, { password: args[1], tags: "" });
      return 0;
    case "delete_user":
      need(args, 1, "delete_user NAME");
      await api(opts, "DELETE", `/api/users/${enc(args[0]!)}`);
      return 0;
    case "change_password": {
      need(args, 2, "change_password NAME PASSWORD");
      const users = rows(await api(opts, "GET", "/api/users"));
      const current = users.find((u) => u.name === args[0]);
      const tags = Array.isArray(current?.tags) ? (current!.tags as string[]).join(",") : cell(current?.tags);
      await api(opts, "PUT", `/api/users/${enc(args[0]!)}`, { password: args[1], tags });
      return 0;
    }
    case "set_user_tags": {
      need(args, 1, "set_user_tags NAME [TAG...]");
      // A PUT without a password clears it on RabbitMQ, so the stored hash is sent back.
      const users = rows(await api(opts, "GET", "/api/users"));
      const current = users.find((u) => u.name === args[0]);
      const body: Record<string, unknown> = { tags: args.slice(1).join(",") };
      if (current?.password_hash) {
        body.password_hash = current.password_hash;
        if (current.hashing_algorithm) body.hashing_algorithm = current.hashing_algorithm;
      }
      await api(opts, "PUT", `/api/users/${enc(args[0]!)}`, body);
      return 0;
    }
    case "set_permissions":
      need(args, 4, "set_permissions USER CONF WRITE READ");
      await api(opts, "PUT", `/api/permissions/${v}/${enc(args[0]!)}`, { configure: args[1], write: args[2], read: args[3] });
      return 0;
    case "clear_permissions":
      need(args, 1, "clear_permissions USER");
      await api(opts, "DELETE", `/api/permissions/${v}/${enc(args[0]!)}`);
      return 0;
    case "set_topic_permissions":
      need(args, 4, "set_topic_permissions USER EXCHANGE WRITE READ");
      await api(opts, "PUT", `/api/topic-permissions/${v}/${enc(args[0]!)}`, { exchange: args[1], write: args[2], read: args[3] });
      return 0;
    case "set_policy":
    case "set_operator_policy": {
      need(args, 3, `${cmd} NAME PATTERN DEFINITION`);
      const kind = cmd === "set_policy" ? "policies" : "operator-policies";
      await api(opts, "PUT", `/api/${kind}/${v}/${enc(args[0]!)}`, {
        pattern: args[1],
        definition: definition(args[2]!),
        priority: opts.priority,
        "apply-to": cmd === "set_operator_policy" && opts.applyTo === "all" ? "queues" : opts.applyTo,
      });
      return 0;
    }
    case "clear_policy":
    case "clear_operator_policy": {
      need(args, 1, `${cmd} NAME`);
      const kind = cmd === "clear_policy" ? "policies" : "operator-policies";
      await api(opts, "DELETE", `/api/${kind}/${v}/${enc(args[0]!)}`);
      return 0;
    }
    case "set_vhost_limits": {
      need(args, 1, "set_vhost_limits DEFINITION");
      for (const [name, value] of Object.entries(definition(args[0]!))) {
        await api(opts, "PUT", `/api/vhost-limits/${v}/${enc(name)}`, { value });
      }
      return 0;
    }
    case "set_user_limits": {
      need(args, 2, "set_user_limits USER DEFINITION");
      for (const [name, value] of Object.entries(definition(args[1]!))) {
        await api(opts, "PUT", `/api/user-limits/${enc(args[0]!)}/${enc(name)}`, { value });
      }
      return 0;
    }
    case "purge_queue":
      need(args, 1, "purge_queue NAME");
      await api(opts, "DELETE", `/api/queues/${v}/${enc(args[0]!)}/contents`);
      return 0;
    case "delete_queue":
      need(args, 1, "delete_queue NAME");
      await api(opts, "DELETE", `/api/queues/${v}/${enc(args[0]!)}`);
      return 0;
    case "trace_on":
    case "trace_off":
      await api(opts, "PUT", `/api/vhosts/${v}`, { tracing: cmd === "trace_on" });
      return 0;
    case "export_definitions": {
      need(args, 1, "export_definitions FILE");
      const defs = await api(opts, "GET", "/api/definitions");
      if (args[0] === "-") console.log(JSON.stringify(defs, null, 2));
      else writeFileSync(args[0]!, `${JSON.stringify(defs, null, 2)}\n`);
      return 0;
    }
    case "import_definitions":
      need(args, 1, "import_definitions FILE");
      await api(opts, "POST", "/api/definitions", JSON.parse(readFileSync(args[0]!, "utf8")));
      return 0;
    default:
      throw new UsageError(`unknown command '${cmd}'\n\n${HELP}`);
  }
}

if (import.meta.main) {
  try {
    process.exit(await run(process.argv.slice(2)));
  } catch (err) {
    console.error(err instanceof Error ? err.message : String(err));
    process.exit(err instanceof UsageError ? 2 : 1);
  }
}
