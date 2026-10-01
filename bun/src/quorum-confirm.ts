/** Where one cluster member holds a quorum publish. */
export type MemberCopy = "memory" | "durable";

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
