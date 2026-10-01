/**
 * Policy and operator-policy updates on the live broker.
 *
 * These functions are the Broker methods. Loading this file installs them.
 */
import { Broker } from "./class.ts";
import { fieldEq, fieldStr, replaceHeaderTable, writeTable, type Field } from "../codec.ts";
import type { Config } from "../config.ts";
import { ChanError } from "../errors.ts";
import { Store, type BindRow, type ExRow, type QueueRow } from "../store.ts";
import { encodeQuorumAppend } from "../wire.ts";
import { durableMajority, type MemberCopy } from "../quorum-confirm.ts";
import { rabbitPasswordHashMatches } from "./auth.ts";
import { parseArgs, deathHeaders, propsWithDeath, argsFromFields } from "./args.ts";
import { topicMatches, headersMatch, fnv1a, headerList, overflowOf, liveFrom, pickConsumer, queueHome } from "./routing.ts";
import { matchOne, policyItem, policyFromBody, fillPolicyArgs } from "./policy-data.ts";
import { BUILTIN, emptyProm, type Consumer, type LiveMsg, type MgmtChannel, type MgmtConnection, type MgmtConsumer, type Policy, type Prom, type QArgs, type QueueLive, type TopicPerm } from "./model.ts";


/**
 * Find the user policy for one queue or exchange.
 *
 * @param vhost Vhost the policy must belong to.
 * @param name Resource name tested against each pattern.
 * @param entity `queues` or `exchanges`. A policy for `all` still matches.
 * @returns The highest-priority match, or null. Operator policies are not included.
 */
export function matchPolicy(this: Broker, vhost: string, name: string, entity: "queues" | "exchanges"): Policy | null {
  return matchOne(this.policies, vhost, name, entity);
}

/**
 * Merge user and operator policies into queue arguments.
 *
 * @param vhost Vhost of the queue.
 * @param name Queue name used to match policies.
 * @param args Arguments the client declared. A present non-empty key is kept.
 * @returns The merged map. The operator policy is applied after the user policy. Neither map is stored here.
 */
export function argsWithPolicy(this: Broker, vhost: string, name: string, args: Record<string, string | number>): Record<string, string | number> {
  const user = this.matchPolicy(vhost, name, "queues");
  const operator = this.matchOperatorPolicy(vhost, name, "queues");
  let out = fillPolicyArgs(args, user);
  out = fillPolicyArgs(args, operator, out);
  return out;
}

/**
 * Find the operator policy for one queue or exchange.
 *
 * @param vhost Vhost the policy must belong to.
 * @param name Resource name tested against each pattern.
 * @param entity `queues` or `exchanges`.
 * @returns The highest-priority operator match, or null.
 */
export function matchOperatorPolicy(this: Broker, vhost: string, name: string, entity: "queues" | "exchanges"): Policy | null {
  return matchOne(this.operatorPolicies, vhost, name, entity);
}

/**
 * Name the user and operator policies that match a queue.
 *
 * @param vhost Vhost of the queue.
 * @param name Queue name.
 * @returns The two policy names. Either is null when nothing matches. Exchange policies are not considered.
 */
export function policyNames(this: Broker, vhost: string, name: string): { policy: string | null; operator_policy: string | null } {
  return {
    policy: this.matchPolicy(vhost, name, "queues")?.name ?? null,
    operator_policy: this.matchOperatorPolicy(vhost, name, "queues")?.name ?? null,
  };
}

/**
 * Replace one operator policy and apply it.
 *
 * @param p Policy to store. An invalid pattern throws. The row is not written to the store.
 * @returns Nothing. A policy with the same vhost and name is replaced, then every queue is updated.
 */
export function upsertOperatorPolicy(this: Broker, p: Policy) {
  try {
    new RegExp(p.pattern);
  } catch {
    throw new Error("invalid policy pattern");
  }
  this.operatorPolicies = this.operatorPolicies.filter((x) => !(x.vhost === p.vhost && x.name === p.name));
  this.operatorPolicies.push(p);
  this.applyPolicies();
}

/**
 * Remove one operator policy.
 *
 * @param vhost Vhost of the policy.
 * @param name Policy name.
 * @returns True when a row was removed. False leaves the queues unchanged.
 */
export function deleteOperatorPolicy(this: Broker, vhost: string, name: string): boolean {
  const before = this.operatorPolicies.length;
  this.operatorPolicies = this.operatorPolicies.filter((p) => !(p.vhost === vhost && p.name === name));
  if (this.operatorPolicies.length === before) return false;
  this.applyPolicies();
  return true;
}

/**
 * Remove one user policy from memory and the store.
 *
 * @param vhost Vhost of the policy.
 * @param name Policy name.
 * @returns True when a row was removed and the queues were updated. False means the name was absent.
 */
export function deletePolicy(this: Broker, vhost: string, name: string): boolean {
  const before = this.policies.length;
  this.policies = this.policies.filter((p) => !(p.vhost === vhost && p.name === name));
  if (this.policies.length === before) return false;
  this.store.deletePolicy(vhost, name);
  this.applyPolicies();
  return true;
}

/**
 * Copy the permission table.
 *
 * @returns A new array of the current permission rows. Mutating the returned array does not change the broker. Mutating a row object does.
 */
export function listPerms(this: Broker) {
  return [...this.perms];
}

/**
 * Replace one user policy, store it, and apply it.
 *
 * @param p Policy to store. An invalid pattern throws before the table changes.
 * @returns Nothing. A policy with the same vhost and name is replaced.
 */
export function upsertPolicy(this: Broker, p: Policy) {
  try {
    new RegExp(p.pattern);
  } catch {
    throw new Error("invalid policy pattern");
  }
  this.policies = this.policies.filter((x) => !(x.vhost === p.vhost && x.name === p.name));
  this.policies.push(p);
  this.store.putPolicy(p);
  this.applyPolicies();
}

/**
 * Recompute every queue's arguments from the current policies.
 *
 * @returns Nothing. A queue that was declared quorum stays quorum when the merged arguments omit `x-queue-type`. A durable queue is written back to the store.
 */
export function applyPolicies(this: Broker) {
  for (const q of this.queues.values()) {
    const merged = this.argsWithPolicy(q.vhost, q.name, q.declaredArgs);
    if (merged["x-queue-type"] == null && q.argsParsed.queueType === "quorum") merged["x-queue-type"] = "quorum";
    q.args = merged;
    q.argsParsed = parseArgs(merged);
    if (q.argsParsed.queueType === "quorum" && q.argsParsed.deliveryLimit == null) q.argsParsed.deliveryLimit = 20;
    if (q.durable) this.store.putQueue({ ...q, args: merged });
  }
}

Broker.prototype.matchPolicy = matchPolicy;
Broker.prototype.argsWithPolicy = argsWithPolicy;
Broker.prototype.matchOperatorPolicy = matchOperatorPolicy;
Broker.prototype.policyNames = policyNames;
Broker.prototype.upsertOperatorPolicy = upsertOperatorPolicy;
Broker.prototype.deleteOperatorPolicy = deleteOperatorPolicy;
Broker.prototype.deletePolicy = deletePolicy;
Broker.prototype.listPerms = listPerms;
Broker.prototype.upsertPolicy = upsertPolicy;
Broker.prototype.applyPolicies = applyPolicies;

declare module "./class.ts" {
  interface Broker {
    matchPolicy: typeof matchPolicy;
    argsWithPolicy: typeof argsWithPolicy;
    matchOperatorPolicy: typeof matchOperatorPolicy;
    policyNames: typeof policyNames;
    upsertOperatorPolicy: typeof upsertOperatorPolicy;
    deleteOperatorPolicy: typeof deleteOperatorPolicy;
    deletePolicy: typeof deletePolicy;
    listPerms: typeof listPerms;
    upsertPolicy: typeof upsertPolicy;
    applyPolicies: typeof applyPolicies;
  }
}
