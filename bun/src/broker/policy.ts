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


/** Broker.matchPolicy. The parameters and return value are unchanged from the previous class method. */
export function matchPolicy(this: Broker, vhost: string, name: string, entity: "queues" | "exchanges"): Policy | null {
  return matchOne(this.policies, vhost, name, entity);
}

/** Broker.argsWithPolicy. The parameters and return value are unchanged from the previous class method. */
export function argsWithPolicy(this: Broker, vhost: string, name: string, args: Record<string, string | number>): Record<string, string | number> {
  const user = this.matchPolicy(vhost, name, "queues");
  const operator = this.matchOperatorPolicy(vhost, name, "queues");
  let out = fillPolicyArgs(args, user);
  out = fillPolicyArgs(args, operator, out);
  return out;
}

/** Broker.matchOperatorPolicy. The parameters and return value are unchanged from the previous class method. */
export function matchOperatorPolicy(this: Broker, vhost: string, name: string, entity: "queues" | "exchanges"): Policy | null {
  return matchOne(this.operatorPolicies, vhost, name, entity);
}

/** Broker.policyNames. The parameters and return value are unchanged from the previous class method. */
export function policyNames(this: Broker, vhost: string, name: string): { policy: string | null; operator_policy: string | null } {
  return {
    policy: this.matchPolicy(vhost, name, "queues")?.name ?? null,
    operator_policy: this.matchOperatorPolicy(vhost, name, "queues")?.name ?? null,
  };
}

/** Broker.upsertOperatorPolicy. The parameters and return value are unchanged from the previous class method. */
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

/** Broker.deleteOperatorPolicy. The parameters and return value are unchanged from the previous class method. */
export function deleteOperatorPolicy(this: Broker, vhost: string, name: string): boolean {
  const before = this.operatorPolicies.length;
  this.operatorPolicies = this.operatorPolicies.filter((p) => !(p.vhost === vhost && p.name === name));
  if (this.operatorPolicies.length === before) return false;
  this.applyPolicies();
  return true;
}

/** Broker.deletePolicy. The parameters and return value are unchanged from the previous class method. */
export function deletePolicy(this: Broker, vhost: string, name: string): boolean {
  const before = this.policies.length;
  this.policies = this.policies.filter((p) => !(p.vhost === vhost && p.name === name));
  if (this.policies.length === before) return false;
  this.store.deletePolicy(vhost, name);
  this.applyPolicies();
  return true;
}

/** Broker.listPerms. The parameters and return value are unchanged from the previous class method. */
export function listPerms(this: Broker) {
  return [...this.perms];
}

/** Broker.upsertPolicy. The parameters and return value are unchanged from the previous class method. */
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

/** Broker.applyPolicies. The parameters and return value are unchanged from the previous class method. */
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
