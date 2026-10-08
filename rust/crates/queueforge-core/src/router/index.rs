//! Binding index lookup, topic patterns, and header matching.

use std::collections::HashSet;

use compact_str::CompactString;

use super::{BindingIndex, BindingKey};
use crate::domain::ExchangeType;

impl BindingIndex {
    /// Add `key` to the index for exchange `kind`. Direct, fanout, topic, and headers use different buckets. A duplicate key replaces the previous destination for that identity.
    pub(super) fn insert(&mut self, key: BindingKey, kind: ExchangeType) {
        let vhost = key.vhost.clone();
        let exchange = key.exchange.clone();
        self.kinds.insert((vhost.clone(), exchange.clone()), kind);
        if !self.bindings.insert(key) {
            return;
        }
        self.rebuild_exchange(vhost.as_str(), exchange.as_str(), kind);
    }

    /// Remove `key` from the index for `kind`. Returns whether it was present. The exchange bucket is rebuilt when a topic or headers pattern changes the lookup set.
    pub(super) fn remove(&mut self, key: &BindingKey, kind: ExchangeType) -> bool {
        if !self.bindings.remove(key) {
            return false;
        }
        self.kinds
            .insert((key.vhost.clone(), key.exchange.clone()), kind);
        self.rebuild_exchange(key.vhost.as_str(), key.exchange.as_str(), kind);
        true
    }

    /// Project one exchange from the binding set that remains.
    ///
    /// Direct, fanout, and topic destinations are queue sets. A queue stays
    /// while any sibling key with the same routing projection is still present.
    pub(super) fn rebuild_exchange(&mut self, vhost: &str, exchange: &str, kind: ExchangeType) {
        match kind {
            ExchangeType::Direct | ExchangeType::Default => {
                self.direct
                    .retain(|(vh, ex, _), _| !(vh.as_str() == vhost && ex.as_str() == exchange));
                for b in &self.bindings {
                    if b.vhost.as_str() == vhost && b.exchange.as_str() == exchange {
                        self.direct
                            .entry((b.vhost.clone(), b.exchange.clone(), b.routing_key.clone()))
                            .or_default()
                            .insert(b.queue.clone());
                    }
                }
            }
            ExchangeType::Fanout => {
                self.fanout
                    .retain(|(vh, ex), _| !(vh.as_str() == vhost && ex.as_str() == exchange));
                for b in &self.bindings {
                    if b.vhost.as_str() == vhost && b.exchange.as_str() == exchange {
                        self.fanout
                            .entry((b.vhost.clone(), b.exchange.clone()))
                            .or_default()
                            .insert(b.queue.clone());
                    }
                }
            }
            ExchangeType::Topic => {
                self.topic
                    .retain(|(vh, ex, _), _| !(vh.as_str() == vhost && ex.as_str() == exchange));
                for b in &self.bindings {
                    if b.vhost.as_str() == vhost && b.exchange.as_str() == exchange {
                        let bucket = topic_bucket(b.routing_key.as_str());
                        self.topic
                            .entry((b.vhost.clone(), b.exchange.clone(), bucket))
                            .or_default()
                            .push((b.routing_key.clone(), b.queue.clone()));
                    }
                }
            }
            // Consistent-hash weights and local-random members keep each
            // binding's key, as headers bindings do.
            ExchangeType::Headers | ExchangeType::ConsistentHash | ExchangeType::LocalRandom | ExchangeType::Delayed => {
                self.headers
                    .retain(|(vh, ex), _| !(vh.as_str() == vhost && ex.as_str() == exchange));
                for b in &self.bindings {
                    if b.vhost.as_str() == vhost && b.exchange.as_str() == exchange {
                        self.headers
                            .entry((b.vhost.clone(), b.exchange.clone()))
                            .or_default()
                            .push((b.routing_key.clone(), b.queue.clone(), b.args.clone()));
                    }
                }
            }
        }
    }

    /// Drop every binding bucket for `exchange` on `vhost`. Bindings on other exchanges stay.
    pub(super) fn remove_exchange(&mut self, vhost: &str, exchange: &str) {
        self.bindings
            .retain(|b| !(b.vhost.as_str() == vhost && b.exchange.as_str() == exchange));
        let kind = self
            .kinds
            .remove(&(CompactString::from(vhost), CompactString::from(exchange)));
        if let Some(kind) = kind {
            self.rebuild_exchange(vhost, exchange, kind);
        }
    }

    /// Drop every binding whose destination queue is `queue` on `vhost`. Returns nothing. Source exchanges that lost a binding are rebuilt.
    pub(super) fn remove_queue(&mut self, vhost: &str, queue: &str) {
        let doomed: Vec<BindingKey> = self
            .bindings
            .iter()
            .filter(|b| b.vhost.as_str() == vhost && b.queue.as_str() == queue)
            .cloned()
            .collect();
        let mut touched = Vec::new();
        for b in &doomed {
            self.bindings.remove(b);
            let pair = (b.vhost.clone(), b.exchange.clone());
            if !touched.contains(&pair) {
                touched.push(pair);
            }
        }
        for (vh, ex) in touched {
            if let Some(kind) = self.kinds.get(&(vh.clone(), ex.clone())).copied() {
                self.rebuild_exchange(vh.as_str(), ex.as_str(), kind);
            }
        }
    }

    /// Route a publish to destination queue names (not including default exchange).
    pub fn route_named(
        &self,
        vhost: &str,
        exchange: &str,
        kind: ExchangeType,
        routing_key: &str,
        headers: &[(CompactString, crate::domain::HeaderArg)],
    ) -> Vec<CompactString> {
        let mut out: HashSet<CompactString> = HashSet::new();
        match kind {
            ExchangeType::Default => {
                // Caller should use the default-exchange special path.
            }
            ExchangeType::Direct => {
                if let Some(set) = self.direct.get(&(
                    CompactString::from(vhost),
                    CompactString::from(exchange),
                    CompactString::from(routing_key),
                )) {
                    out.extend(set.iter().cloned());
                }
            }
            ExchangeType::Fanout => {
                if let Some(set) = self
                    .fanout
                    .get(&(CompactString::from(vhost), CompactString::from(exchange)))
                {
                    out.extend(set.iter().cloned());
                }
            }
            ExchangeType::Topic => {
                let (names, _examined) = self.topic_destinations(vhost, exchange, routing_key);
                out.extend(names);
            }
            ExchangeType::Headers => {
                if let Some(list) = self
                    .headers
                    .get(&(CompactString::from(vhost), CompactString::from(exchange)))
                {
                    for (_routing_key, queue, args) in list {
                        if headers_match(args, headers) {
                            out.insert(queue.clone());
                        }
                    }
                }
            }
            ExchangeType::ConsistentHash => {
                if let Some(list) = self
                    .headers
                    .get(&(CompactString::from(vhost), CompactString::from(exchange)))
                {
                    // Each binding key is a weight: that many buckets for its queue.
                    let mut buckets: Vec<&CompactString> = Vec::new();
                    for (weight, queue, _) in list {
                        let n = weight.parse::<u32>().unwrap_or(0).min(1000);
                        for _ in 0..n {
                            buckets.push(queue);
                        }
                    }
                    if !buckets.is_empty() {
                        let at = jump_hash(fnv1a(routing_key.as_bytes()), buckets.len());
                        out.insert(buckets[at].clone());
                    }
                }
            }
            ExchangeType::LocalRandom => {
                if let Some(list) = self
                    .headers
                    .get(&(CompactString::from(vhost), CompactString::from(exchange)))
                {
                    let mut queues: Vec<&CompactString> = list.iter().map(|(_, q, _)| q).collect();
                    queues.sort();
                    queues.dedup();
                    if !queues.is_empty() {
                        out.insert(queues[next_random() as usize % queues.len()].clone());
                    }
                }
            }
            ExchangeType::Delayed => {
                // Routed by `routing_kind`; a delayed exchange never reaches here.
            }
        }
        let mut v: Vec<CompactString> = out.into_iter().collect();
        v.sort();
        v
    }

    /// Topic destinations plus how many patterns were examined.
    ///
    /// Only the bucket for the routing key's first word and the wildcard
    /// bucket are read.
    pub fn topic_destinations(
        &self,
        vhost: &str,
        exchange: &str,
        routing_key: &str,
    ) -> (Vec<CompactString>, usize) {
        let vh = CompactString::from(vhost);
        let ex = CompactString::from(exchange);
        let mut examined = 0usize;
        let mut out = Vec::new();
        for bucket in [topic_lookup_bucket(routing_key), CompactString::from("*")] {
            if let Some(list) = self.topic.get(&(vh.clone(), ex.clone(), bucket)) {
                for (pattern, queue) in list {
                    examined += 1;
                    if topic_matches(pattern.as_str(), routing_key) {
                        out.push(queue.clone());
                    }
                }
            }
        }
        out.sort();
        out.dedup();
        (out, examined)
    }

    /// Empty index at the given epoch.
    pub fn empty(epoch: u64) -> Self {
        Self {
            epoch,
            ..Default::default()
        }
    }

    /// Number of user bindings.
    pub fn len(&self) -> usize {
        self.bindings.len()
    }

    /// Whether there are no user bindings.
    pub fn is_empty(&self) -> bool {
        self.bindings.is_empty()
    }

    /// Whether a binding exists.
    pub fn contains(&self, key: &BindingKey) -> bool {
        self.bindings.contains(key)
    }

    /// List all bindings (unordered).
    pub fn list(&self) -> Vec<BindingKey> {
        self.bindings.iter().cloned().collect()
    }

    /// Bindings for a specific exchange in a vhost.
    pub fn list_for_exchange(&self, vhost: &str, exchange: &str) -> Vec<BindingKey> {
        self.bindings
            .iter()
            .filter(|b| b.vhost.as_str() == vhost && b.exchange.as_str() == exchange)
            .cloned()
            .collect()
    }

    /// Count bindings for an exchange (for `if_unused` delete).
    pub fn count_for_exchange(&self, vhost: &str, exchange: &str) -> usize {
        self.bindings
            .iter()
            .filter(|b| b.vhost.as_str() == vhost && b.exchange.as_str() == exchange)
            .count()
    }

    /// Count bindings for a queue.
    pub fn count_for_queue(&self, vhost: &str, queue: &str) -> usize {
        self.bindings
            .iter()
            .filter(|b| b.vhost.as_str() == vhost && b.queue.as_str() == queue)
            .count()
    }
}

/// Bucket a topic pattern by its first literal word. `pattern` is the binding key. Returns that word, or `#` when the pattern can match any first word. Lookup uses the same bucket so a publish does not scan every pattern.
pub(super) fn topic_bucket(pattern: &str) -> CompactString {
    match pattern.split('.').next() {
        Some("*") | Some("#") | None => CompactString::from("*"),
        Some("") => CompactString::from("*"),
        Some(word) => CompactString::from(word),
    }
}

/// Bucket a published routing key by its first word. `routing_key` is the publish key. Returns that word. Patterns stored under `#` are still consulted.
pub(super) fn topic_lookup_bucket(routing_key: &str) -> CompactString {
    match routing_key.split('.').next() {
        Some(word) if !word.is_empty() => CompactString::from(word),
        _ => CompactString::from(""),
    }
}

/// `x-match=all` (default) requires every other argument. `x-match=any` requires one.
pub fn headers_match(
    args: &[(CompactString, crate::domain::HeaderArg)],
    headers: &[(CompactString, crate::domain::HeaderArg)],
) -> bool {
    let any = args.iter().any(|(k, v)| {
        k.as_str() == "x-match" && matches!(v, crate::domain::HeaderArg::Str(s) if s == "any")
    });
    let checks: Vec<_> = args
        .iter()
        .filter(|(k, _)| k.as_str() != "x-match")
        .collect();
    if checks.is_empty() {
        return !any;
    }
    if any {
        checks
            .iter()
            .any(|(k, v)| headers.iter().any(|(hk, hv)| hk == *k && hv == v))
    } else {
        checks
            .iter()
            .all(|(k, v)| headers.iter().any(|(hk, hv)| hk == *k && hv == v))
    }
}

/// AMQP topic matching: `*` = one word, `#` = zero or more words (dot-separated).
pub fn topic_matches(pattern: &str, routing_key: &str) -> bool {
    let pat: Vec<&str> = if pattern.is_empty() {
        Vec::new()
    } else {
        pattern.split('.').collect()
    };
    let key: Vec<&str> = if routing_key.is_empty() {
        Vec::new()
    } else {
        routing_key.split('.').collect()
    };
    topic_match_parts(&pat, &key)
}

/// Match topic `pat` parts against `key` parts. Returns true when every pattern token matches. `#` consumes the rest of the key. An empty pattern matches only an empty key.
pub(super) fn topic_match_parts(pat: &[&str], key: &[&str]) -> bool {
    match (pat.first().copied(), key.first().copied()) {
        (None, None) => true,
        (Some("#"), _) => {
            let rest = &pat[1..];
            // Trailing `#` matches the rest of the key (including empty).
            if rest.is_empty() {
                return true;
            }
            // `#` matches zero or more words, then the remainder of the pattern.
            for i in 0..=key.len() {
                if topic_match_parts(rest, &key[i..]) {
                    return true;
                }
            }
            false
        }
        (Some("*"), _) => {
            if key.is_empty() {
                false
            } else {
                topic_match_parts(&pat[1..], &key[1..])
            }
        }
        (Some(p), Some(k)) if p == k => topic_match_parts(&pat[1..], &key[1..]),
        _ => false,
    }
}

/// FNV-1a over `bytes`.
fn fnv1a(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    h
}

/// Lamping and Veach's jump consistent hash: a stable bucket for `key`.
/// Adding a bucket moves only the keys that land on it.
fn jump_hash(mut key: u64, buckets: usize) -> usize {
    let mut b: i64 = -1;
    let mut j: i64 = 0;
    while j < buckets as i64 {
        b = j;
        key = key.wrapping_mul(2_862_933_555_777_941_757).wrapping_add(1);
        j = (((b + 1) as f64) * ((1u64 << 31) as f64 / (((key >> 33) + 1) as f64))) as i64;
    }
    b.max(0) as usize
}

/// A cheap per-process random number for `x-local-random`.
fn next_random() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static STATE: AtomicU64 = AtomicU64::new(0x9e37_79b9_7f4a_7c15);
    let mut x = STATE.fetch_add(0x9e37_79b9_7f4a_7c15, Ordering::Relaxed);
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}
