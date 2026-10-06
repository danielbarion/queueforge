/** Where one cluster member holds a quorum publish. */
export type MemberCopy = "memory" | "durable";

/**
 * In-flight appends allowed on a peer the majority does not need.
 * The peers required for a majority are always contacted.
 */
export const EXTRA_APPEND_CAP = 32;

/**
 * Choose which peers receive this quorum append.
 *
 * @param peers Reachable peers, not including this node.
 * @param inflight Appends already waiting on each peer.
 * @param needed How many peer copies are still required once the local copy is durable.
 * @param cap In-flight limit for a peer past `needed`. Defaults to `EXTRA_APPEND_CAP`.
 * @returns Peer ids to contact. The first `needed`, lowest in-flight first, are always included.
 *          A further peer is included only while its in-flight count is below `cap`.
 *          A skipped peer is not a durable copy.
 */
export function selectQuorumPeers(
  peers: readonly string[],
  inflight: ReadonlyMap<string, number>,
  needed: number,
  cap = EXTRA_APPEND_CAP,
): string[] {
  const ranked = [...peers].sort((left, right) => {
    const load = (inflight.get(left) ?? 0) - (inflight.get(right) ?? 0);
    if (load !== 0) return load;
    if (left < right) return -1;
    if (left > right) return 1;
    return 0;
  });
  const chosen: string[] = [];
  for (let i = 0; i < ranked.length; i++) {
    const id = ranked[i]!;
    if (i < needed || (inflight.get(id) ?? 0) < cap) chosen.push(id);
  }
  return chosen;
}

/**
 * Confirm a quorum publish only after a durable majority.
 *
 * @param members Static `[cluster].members` length. One member confirms after its own durable copy.
 * @param copies One entry per member that was asked to store the body. `"memory"` does not count.
 * @returns True when durable copies are a majority of `members`.
 */
export function durableMajority(members: number, copies: MemberCopy[]): boolean {
  const need = Math.floor(Math.max(members, 1) / 2) + 1;
  let durable = 0;
  for (const copy of copies) {
    if (copy === "durable") durable += 1;
  }
  return durable >= need;
}
