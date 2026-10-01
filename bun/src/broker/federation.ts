/**
 * Federation links kept in memory. They are stored and not executed.
 */
type FedLink = { upstream: string; downstream: string; pattern: string };
/** Links recorded by addFederationPolicy. Publish reads this list. */
export const fedLinks: FedLink[] = [];
/** Upstreams recorded by addFederationUpstream. */
export const fedUpstreams: { downstream: string; upstream: string }[] = [];

export function addFederationUpstream(downstream: string, upstream: string) {
  fedUpstreams.push({ downstream, upstream });
}

export function addFederationPolicy(downstream: string, pattern: string) {
  for (const up of fedUpstreams) {
    if (up.downstream === downstream) fedLinks.push({ upstream: up.upstream, downstream, pattern });
  }
}
