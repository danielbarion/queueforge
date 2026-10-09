/**
 * Runtime and global parameters, as RabbitMQ's `/api/parameters` and
 * `/api/global-parameters`, and stored topic permissions.
 *
 * Two components already have their own tables: `vhost-limits` maps to the
 * vhost limits and `shovel` to the dynamic shovels. Any other component is
 * kept as JSON under `rt:<component>` in the parameters table, so a
 * definitions file round-trips even when this broker does nothing with it.
 */
import { Broker } from "./class.ts";
import type { TopicPerm } from "./model.ts";
import type { ShovelDef } from "./shovel.ts";

export type RuntimeParam = { component: string; vhost: string; name: string; value: unknown };

const GENERIC = "rt:";
const GLOBAL = "global";
const TOPIC_PERMS = "topic-permissions";

function shovelValue(def: ShovelDef) {
  return { "src-uri": def.srcUri, "src-queue": def.srcQueue, "dest-uri": def.destUri, "dest-queue": def.destQueue };
}

function parseJson(text: string): unknown {
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}

/**
 * List runtime parameters.
 *
 * @param component When set, only that component. Omitted includes every component.
 * @param vhost When set, only that vhost.
 * @returns RabbitMQ rows: `{ component, vhost, name, value }`.
 */
export function listRuntimeParams(this: Broker, component?: string, vhost?: string): RuntimeParam[] {
  const rows: RuntimeParam[] = [];
  for (const row of this.listVhostLimits()) {
    const value: Record<string, number> = {};
    if (row["max-connections"] != null) value["max-connections"] = row["max-connections"];
    if (row["max-queues"] != null) value["max-queues"] = row["max-queues"];
    rows.push({ component: "vhost-limits", vhost: row.vhost, name: "limits", value });
  }
  for (const row of this.store.listParameters("shovel")) {
    const def = parseJson(row.value) as ShovelDef;
    rows.push({ component: "shovel", vhost: row.vhost, name: row.name, value: shovelValue(def) });
  }
  for (const c of this.store.listParameterComponents()) {
    if (!c.startsWith(GENERIC)) continue;
    for (const row of this.store.listParameters(c)) {
      rows.push({ component: c.slice(GENERIC.length), vhost: row.vhost, name: row.name, value: parseJson(row.value) });
    }
  }
  return rows
    .filter((r) => (component == null || r.component === component) && (vhost == null || r.vhost === vhost))
    .sort((a, b) => a.component.localeCompare(b.component) || a.vhost.localeCompare(b.vhost) || a.name.localeCompare(b.name));
}

/**
 * Set one runtime parameter.
 *
 * @param p Component, vhost, name and the value object.
 * @returns Nothing. A `shovel` without `src-queue` and `dest-queue` throws.
 */
export function putRuntimeParam(this: Broker, p: RuntimeParam) {
  const value = (p.value ?? {}) as Record<string, unknown>;
  if (p.component === "vhost-limits") {
    this.applyLimitsRow("vhost_limits", { vhost: p.vhost, ...value });
  } else if (p.component === "shovel") {
    const srcQueue = value["src-queue"];
    const destQueue = value["dest-queue"];
    if (typeof srcQueue !== "string" || typeof destQueue !== "string" || !srcQueue || !destQueue) {
      throw new Error("src-queue and dest-queue are required");
    }
    this.putShovel({
      vhost: p.vhost,
      name: p.name,
      srcUri: String(value["src-uri"] || "amqp://"),
      srcQueue,
      destUri: String(value["dest-uri"] || "amqp://"),
      destQueue,
    });
  } else {
    this.store.putParameter(GENERIC + p.component, p.vhost, p.name, JSON.stringify(p.value ?? null));
  }
}

/**
 * Remove one runtime parameter.
 *
 * @returns True when it existed.
 */
export function deleteRuntimeParam(this: Broker, component: string, vhost: string, name: string): boolean {
  if (component === "vhost-limits") {
    const had = this.vhostConnLimit.has(vhost) || this.vhostQueueLimit.has(vhost);
    this.setVhostLimit(vhost, null, null);
    return had;
  }
  if (component === "shovel") return this.deleteShovel(vhost, name);
  return this.store.deleteParameter(GENERIC + component, vhost, name);
}

/**
 * List global parameters. `cluster_name` is always present.
 *
 * @returns `{ name, value }` rows sorted by name.
 */
export function listGlobalParams(this: Broker): Array<{ name: string; value: unknown }> {
  const rows = this.store.listParameters(GLOBAL).map((row) => ({ name: row.name, value: parseJson(row.value) }));
  if (!rows.some((row) => row.name === "cluster_name")) rows.push({ name: "cluster_name", value: this.cfg.nodeId || "queueforge" });
  return rows.sort((a, b) => a.name.localeCompare(b.name));
}

export function putGlobalParam(this: Broker, name: string, value: unknown) {
  this.store.putParameter(GLOBAL, "", name, JSON.stringify(value ?? null));
}

export function deleteGlobalParam(this: Broker, name: string): boolean {
  return this.store.deleteParameter(GLOBAL, "", name);
}

/** The cluster name: the `cluster_name` global parameter, else the node id. */
export function clusterName(this: Broker): string {
  const row = this.listGlobalParams().find((r) => r.name === "cluster_name");
  return typeof row?.value === "string" ? row.value : this.cfg.nodeId || "queueforge";
}

export function storeTopicPerm(this: Broker, perm: TopicPerm) {
  this.store.putParameter(TOPIC_PERMS, perm.vhost, `${perm.user}\0${perm.exchange}`, JSON.stringify(perm));
}

export function unstoreTopicPerm(this: Broker, user: string, vhost: string, exchange: string) {
  this.store.deleteParameter(TOPIC_PERMS, vhost, `${user}\0${exchange}`);
}

/** Load stored topic permissions. A row that does not parse is skipped. */
export function loadTopicPerms(this: Broker) {
  for (const row of this.store.listParameters(TOPIC_PERMS)) {
    const perm = parseJson(row.value) as TopicPerm;
    if (perm && typeof perm.user === "string" && typeof perm.exchange === "string") {
      this.topicPerms = this.topicPerms.filter(
        (p) => !(p.user === perm.user && p.vhost === perm.vhost && p.exchange === perm.exchange),
      );
      this.topicPerms.push(perm);
    }
  }
}

Broker.prototype.listRuntimeParams = listRuntimeParams;
Broker.prototype.putRuntimeParam = putRuntimeParam;
Broker.prototype.deleteRuntimeParam = deleteRuntimeParam;
Broker.prototype.listGlobalParams = listGlobalParams;
Broker.prototype.putGlobalParam = putGlobalParam;
Broker.prototype.deleteGlobalParam = deleteGlobalParam;
Broker.prototype.clusterName = clusterName;
Broker.prototype.storeTopicPerm = storeTopicPerm;
Broker.prototype.unstoreTopicPerm = unstoreTopicPerm;
Broker.prototype.loadTopicPerms = loadTopicPerms;

declare module "./class.ts" {
  interface Broker {
    listRuntimeParams: typeof listRuntimeParams;
    putRuntimeParam: typeof putRuntimeParam;
    deleteRuntimeParam: typeof deleteRuntimeParam;
    listGlobalParams: typeof listGlobalParams;
    putGlobalParam: typeof putGlobalParam;
    deleteGlobalParam: typeof deleteGlobalParam;
    clusterName: typeof clusterName;
    storeTopicPerm: typeof storeTopicPerm;
    unstoreTopicPerm: typeof unstoreTopicPerm;
    loadTopicPerms: typeof loadTopicPerms;
  }
}
