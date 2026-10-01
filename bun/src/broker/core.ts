/**
 * Queue map key.
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
 * Build the in-memory map key for a vhost resource.
 *
 * @param vhost Vhost name. A slash is a normal character, not a separator.
 * @param name Queue or exchange name.
 * @returns `vhost`, a NUL, then `name`. Two resources that differ only by that NUL would collide. Callers must use this helper for every map lookup.
 */
export function key(this: Broker, vhost: string, name: string) {
  return `${vhost}\0${name}`;
}

/** Register a connection that finished connection.open. Returns its management id. */

Broker.prototype.key = key;

declare module "./class.ts" {
  interface Broker {
    key: typeof key;
  }
}
