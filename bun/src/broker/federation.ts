/**
 * Federation links kept in memory. They are stored and not executed.
 */
type FedLink = { upstream: string; downstream: string; pattern: string };
/** Links recorded by addFederationPolicy. Publish reads this list. */
export const fedLinks: FedLink[] = [];
/** Upstreams recorded by addFederationUpstream. */
export const fedUpstreams: { downstream: string; upstream: string }[] = [];

/**
 * Record a federation upstream. Nothing is dialed.
 *
 * @param downstream Local node name stored on the pair.
 * @param upstream Upstream name stored as given. A duplicate pair is appended again.
 * @returns Nothing. `addFederationPolicy` only sees upstreams added before it runs.
 */
export function addFederationUpstream(downstream: string, upstream: string) {
  fedUpstreams.push({ downstream, upstream });
}

/**
 * Record a federation link for upstreams already added for this downstream.
 *
 * @param downstream Must equal an upstream's `downstream`. No match stores nothing.
 * @param pattern Pattern copied onto each matching upstream. A later `addFederationUpstream` does not backfill this pattern.
 * @returns Nothing. Publish reads `fedLinks`; this function does not start a link.
 */
export function addFederationPolicy(downstream: string, pattern: string) {
  for (const up of fedUpstreams) {
    if (up.downstream === downstream) fedLinks.push({ upstream: up.upstream, downstream, pattern });
  }
}
