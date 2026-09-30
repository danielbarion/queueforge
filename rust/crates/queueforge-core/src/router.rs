//! Exchange routing: binding index, topic matching, multi-destination lookup.
//!
//! Binding tables are versioned and swapped via [`arc_swap::ArcSwap`] for
//! lock-free publish-path reads. Mutations take a short write mutex, rebuild,
//! and swap.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use arc_swap::ArcSwap;
use compact_str::CompactString;
use tracing::debug;

use crate::domain::{Binding, Exchange, ExchangeType, DEFAULT_EXCHANGE_NAME};
use crate::error::{Error, Result};
use crate::policy::{apply_queue_policy, effective_alternate, select_policy, Policy, PolicyTarget};
use crate::queue::{QueueArgs, QueueKey};

/// Unique identity of a binding row, including header arguments.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BindingKey {
    /// Virtual host.
    pub vhost: CompactString,
    /// Exchange name.
    pub exchange: CompactString,
    /// Queue name.
    pub queue: CompactString,
    /// Routing key / topic pattern.
    pub routing_key: CompactString,
    /// Header arguments. Empty for direct, fanout, and topic rows.
    pub args: Vec<(CompactString, crate::domain::HeaderArg)>,
}

impl BindingKey {
    /// Build from parts.
    pub fn new(
        vhost: impl Into<CompactString>,
        exchange: impl Into<CompactString>,
        queue: impl Into<CompactString>,
        routing_key: impl Into<CompactString>,
    ) -> Self {
        Self {
            vhost: vhost.into(),
            exchange: exchange.into(),
            queue: queue.into(),
            routing_key: routing_key.into(),
            args: Vec::new(),
        }
    }

    /// From a domain [`Binding`].
    pub fn from_binding(b: &Binding) -> Self {
        Self {
            vhost: b.vhost.clone(),
            exchange: b.exchange.clone(),
            queue: b.queue.clone(),
            routing_key: b.routing_key.clone(),
            args: b.args.clone(),
        }
    }

    /// Domain [`Binding`] snapshot.
    pub fn to_binding(&self) -> Binding {
        Binding {
            vhost: self.vhost.clone(),
            exchange: self.exchange.clone(),
            queue: self.queue.clone(),
            routing_key: self.routing_key.clone(),
            args: self.args.clone(),
        }
    }
}

/// Immutable snapshot used on the publish hot path.
#[derive(Debug, Clone, Default)]
pub struct BindingIndex {
    /// Epoch when this snapshot was built.
    pub epoch: u64,
    /// All user bindings (not implicit default-exchange rows).
    bindings: HashSet<BindingKey>,
    /// `(vhost, exchange, routing_key) → queues` for direct (and default unused).
    direct: HashMap<(CompactString, CompactString, CompactString), HashSet<CompactString>>,
    /// `(vhost, exchange) → queues` for fanout.
    fanout: HashMap<(CompactString, CompactString), HashSet<CompactString>>,
    /// `(vhost, exchange, bucket) → (pattern, queue)` for topic.
    ///
    /// Bucket is the pattern's first literal word, or `"*"` when the pattern
    /// starts with `*` or `#`. A lookup only reads the key's first word and
    /// the wildcard bucket, so unrelated patterns are not examined.
    topic:
        HashMap<(CompactString, CompactString, CompactString), Vec<(CompactString, CompactString)>>,
    /// `(vhost, exchange) → (queue, header args)` for headers exchanges.
    headers: HashMap<(CompactString, CompactString), Vec<HeaderBinding>>,
    /// Exchange kind used to rebuild a projection after a sibling unbind.
    kinds: HashMap<(CompactString, CompactString), ExchangeType>,
}

/// One headers-exchange binding: routing key, destination queue, and match arguments.
type HeaderBinding = (
    CompactString,
    CompactString,
    Vec<(CompactString, crate::domain::HeaderArg)>,
);

impl BindingIndex {
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

    fn insert(&mut self, key: BindingKey, kind: ExchangeType) {
        let vhost = key.vhost.clone();
        let exchange = key.exchange.clone();
        self.kinds.insert((vhost.clone(), exchange.clone()), kind);
        if !self.bindings.insert(key) {
            return;
        }
        self.rebuild_exchange(vhost.as_str(), exchange.as_str(), kind);
    }

    fn remove(&mut self, key: &BindingKey, kind: ExchangeType) -> bool {
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
    fn rebuild_exchange(&mut self, vhost: &str, exchange: &str, kind: ExchangeType) {
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
            ExchangeType::Headers => {
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

    fn remove_exchange(&mut self, vhost: &str, exchange: &str) {
        self.bindings
            .retain(|b| !(b.vhost.as_str() == vhost && b.exchange.as_str() == exchange));
        let kind = self
            .kinds
            .remove(&(CompactString::from(vhost), CompactString::from(exchange)));
        if let Some(kind) = kind {
            self.rebuild_exchange(vhost, exchange, kind);
        }
    }

    fn remove_queue(&mut self, vhost: &str, queue: &str) {
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
        }
        let mut v: Vec<CompactString> = out.into_iter().collect();
        v.sort();
        v
    }
}

impl BindingIndex {
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
}

fn topic_bucket(pattern: &str) -> CompactString {
    match pattern.split('.').next() {
        Some("*") | Some("#") | None => CompactString::from("*"),
        Some("") => CompactString::from("*"),
        Some(word) => CompactString::from(word),
    }
}

fn topic_lookup_bucket(routing_key: &str) -> CompactString {
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

fn topic_match_parts(pat: &[&str], key: &[&str]) -> bool {
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct ExchangeEdge {
    vhost: CompactString,
    source: CompactString,
    destination: CompactString,
    routing_key: CompactString,
}

/// Live exchange table + binding index for routing.
///
/// Mutations serialize on `write`; readers load `Arc` snapshots without blocking.
pub struct ExchangeRouter {
    exchanges: ArcSwap<HashMap<(CompactString, CompactString), Exchange>>,
    index: ArcSwap<BindingIndex>,
    policies: ArcSwap<Vec<Policy>>,
    exchange_bindings: ArcSwap<Vec<ExchangeEdge>>,
    epoch: AtomicU64,
    write: Mutex<()>,
}

impl Default for ExchangeRouter {
    fn default() -> Self {
        Self::new()
    }
}

impl ExchangeRouter {
    /// Empty router (no exchanges, no bindings).
    pub fn new() -> Self {
        Self {
            exchanges: ArcSwap::from_pointee(HashMap::new()),
            index: ArcSwap::from_pointee(BindingIndex::empty(0)),
            policies: ArcSwap::from_pointee(Vec::new()),
            exchange_bindings: ArcSwap::from_pointee(Vec::new()),
            epoch: AtomicU64::new(0),
            write: Mutex::new(()),
        }
    }

    /// Shared handle for connection tasks.
    pub fn shared() -> Arc<Self> {
        Arc::new(Self::new())
    }

    /// Current binding-index epoch.
    pub fn epoch(&self) -> u64 {
        self.index.load().epoch
    }

    /// Load a snapshot of the binding index (lock-free).
    pub fn index(&self) -> arc_swap::Guard<Arc<BindingIndex>> {
        self.index.load()
    }

    /// Get an exchange definition (lock-free).
    pub fn get_exchange(&self, vhost: &str, name: &str) -> Option<Exchange> {
        let map = self.exchanges.load();
        map.get(&(CompactString::from(vhost), CompactString::from(name)))
            .cloned()
    }

    /// List exchanges in a vhost.
    pub fn list_exchanges(&self, vhost: &str) -> Vec<Exchange> {
        let map = self.exchanges.load();
        let mut out: Vec<Exchange> = map
            .values()
            .filter(|e| e.vhost.as_str() == vhost)
            .cloned()
            .collect();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }

    /// Insert or replace an exchange definition in the live table.
    pub fn put_exchange(&self, exchange: Exchange) {
        let _g = self.write.lock().expect("router write lock poisoned");
        let mut map = (**self.exchanges.load()).clone();
        map.insert((exchange.vhost.clone(), exchange.name.clone()), exchange);
        self.exchanges.store(Arc::new(map));
    }

    /// Remove an exchange and all of its bindings. Returns the removed exchange.
    pub fn delete_exchange(&self, vhost: &str, name: &str) -> Result<Exchange> {
        let _g = self.write.lock().expect("router write lock poisoned");
        let mut map = (**self.exchanges.load()).clone();
        let key = (CompactString::from(vhost), CompactString::from(name));
        let Some(ex) = map.remove(&key) else {
            return Err(Error::NotFound(format!("exchange {vhost}/{name}")));
        };
        self.exchanges.store(Arc::new(map));

        let epoch = self.epoch.fetch_add(1, Ordering::AcqRel) + 1;
        let mut idx = (**self.index.load()).clone();
        idx.epoch = epoch;
        idx.remove_exchange(vhost, name);
        self.index.store(Arc::new(idx));
        debug!(vhost, exchange = name, "router: exchange deleted");
        Ok(ex)
    }

    /// Bind a queue to an exchange. Idempotent if the same binding already exists.
    ///
    /// Rejects binding to the default exchange `""` (implicit only).
    pub fn bind(&self, binding: Binding) -> Result<bool> {
        if binding.exchange.as_str() == DEFAULT_EXCHANGE_NAME {
            return Err(Error::PreconditionFailed(
                "cannot bind to the default exchange".into(),
            ));
        }
        let _g = self.write.lock().expect("router write lock poisoned");
        let map = self.exchanges.load();
        let ex_key = (binding.vhost.clone(), binding.exchange.clone());
        let Some(ex) = map.get(&ex_key) else {
            return Err(Error::NotFound(format!(
                "exchange {}/{}",
                binding.vhost, binding.exchange
            )));
        };
        let kind = ex.kind;
        let key = BindingKey::from_binding(&binding);
        if self.index.load().contains(&key) {
            return Ok(false);
        }
        let epoch = self.epoch.fetch_add(1, Ordering::AcqRel) + 1;
        let mut idx = (**self.index.load()).clone();
        idx.epoch = epoch;
        idx.insert(key, kind);
        self.index.store(Arc::new(idx));
        debug!(
            vhost = %binding.vhost,
            exchange = %binding.exchange,
            queue = %binding.queue,
            rk = %binding.routing_key,
            "router: bound"
        );
        Ok(true)
    }

    /// Remove a binding. Returns `true` if it existed.
    pub fn unbind(&self, binding: &Binding) -> Result<bool> {
        if binding.exchange.as_str() == DEFAULT_EXCHANGE_NAME {
            return Err(Error::PreconditionFailed(
                "cannot unbind from the default exchange".into(),
            ));
        }
        let _g = self.write.lock().expect("router write lock poisoned");
        let map = self.exchanges.load();
        let ex_key = (binding.vhost.clone(), binding.exchange.clone());
        let kind = map
            .get(&ex_key)
            .map(|e| e.kind)
            .unwrap_or(ExchangeType::Direct);
        let key = BindingKey::from_binding(binding);
        if !self.index.load().contains(&key) {
            // RabbitMQ still returns unbind-ok for missing binding; we match that
            // by returning Ok(false) without error.
            return Ok(false);
        }
        let epoch = self.epoch.fetch_add(1, Ordering::AcqRel) + 1;
        let mut idx = (**self.index.load()).clone();
        idx.epoch = epoch;
        let removed = idx.remove(&key, kind);
        self.index.store(Arc::new(idx));
        Ok(removed)
    }

    /// Drop all bindings targeting a queue (on queue delete).
    pub fn remove_queue_bindings(&self, vhost: &str, queue: &str) {
        let _g = self.write.lock().expect("router write lock poisoned");
        let epoch = self.epoch.fetch_add(1, Ordering::AcqRel) + 1;
        let mut idx = (**self.index.load()).clone();
        idx.epoch = epoch;
        idx.remove_queue(vhost, queue);
        self.index.store(Arc::new(idx));
    }

    /// Bind one exchange to another. The publish routing key is matched on the source.
    pub fn bind_exchange(
        &self,
        vhost: &str,
        source: &str,
        destination: &str,
        routing_key: &str,
    ) -> Result<bool> {
        if source == DEFAULT_EXCHANGE_NAME || destination == DEFAULT_EXCHANGE_NAME {
            return Err(Error::PreconditionFailed(
                "cannot bind the default exchange".into(),
            ));
        }
        if self.get_exchange(vhost, source).is_none() || self.get_exchange(vhost, destination).is_none() {
            return Err(Error::NotFound(format!(
                "exchange {vhost}/{source} -> {destination}"
            )));
        }
        let _g = self.write.lock().expect("router write lock poisoned");
        let mut list = (**self.exchange_bindings.load()).clone();
        let edge = ExchangeEdge {
            vhost: CompactString::from(vhost),
            source: CompactString::from(source),
            destination: CompactString::from(destination),
            routing_key: CompactString::from(routing_key),
        };
        if list.iter().any(|e| e == &edge) {
            return Ok(false);
        }
        list.push(edge);
        self.exchange_bindings.store(Arc::new(list));
        Ok(true)
    }

    /// Remove an exchange-to-exchange binding. Missing rows still succeed.
    pub fn unbind_exchange(&self, vhost: &str, source: &str, destination: &str, routing_key: &str) -> bool {
        let _g = self.write.lock().expect("router write lock poisoned");
        let mut list = (**self.exchange_bindings.load()).clone();
        let before = list.len();
        list.retain(|e| {
            !(e.vhost.as_str() == vhost
                && e.source.as_str() == source
                && e.destination.as_str() == destination
                && e.routing_key.as_str() == routing_key)
        });
        let removed = list.len() != before;
        self.exchange_bindings.store(Arc::new(list));
        removed
    }

    fn next_exchanges(&self, vhost: &str, source: &str, routing_key: &str) -> Vec<String> {
        let kind = self.get_exchange(vhost, source).map(|ex| ex.kind);
        let edges = self.exchange_bindings.load();
        edges
            .iter()
            .filter(|e| e.vhost.as_str() == vhost && e.source.as_str() == source)
            .filter(|e| match kind {
                Some(ExchangeType::Fanout) => true,
                Some(ExchangeType::Topic) => topic_matches(e.routing_key.as_str(), routing_key),
                _ => e.routing_key.as_str() == routing_key,
            })
            .map(|e| e.destination.to_string())
            .collect()
    }

    /// Resolve destination queue keys for a publish.
    ///
    /// - Default exchange `""`: routing key is the queue name (single dest if present is
    ///   **not** checked here — callers still need the queue registry).
    /// - Named exchanges: use the binding index; missing exchange → `NotFound`.
    /// - Internal exchanges (except default) refuse publish.
    ///
    /// Follow `alternate-exchange` while the result has no queue. A cycle stops
    /// and returns no destinations. The message body is not modified.
    pub fn route_publish(
        &self,
        vhost: &str,
        exchange: &str,
        routing_key: &str,
        headers: &[(CompactString, crate::domain::HeaderArg)],
    ) -> Result<RouteResult> {
        let mut pending = vec![exchange.to_string()];
        let mut seen = HashSet::new();
        let mut destinations = Vec::new();
        let mut kind = ExchangeType::Direct;
        let mut exchange_name = CompactString::from(exchange);
        while let Some(current) = pending.pop() {
            if !seen.insert(current.clone()) {
                continue;
            }
            let result = self.route_with_headers(vhost, &current, routing_key, headers)?;
            kind = result.kind;
            exchange_name = result.exchange_name;
            let empty = result.destinations.is_empty();
            destinations.extend(result.destinations);
            pending.extend(self.next_exchanges(vhost, &current, routing_key));
            if empty {
                let declared = self
                    .get_exchange(vhost, &current)
                    .and_then(|ex| ex.alternate.clone());
                if let Some(alt) = self.alternate_for(vhost, &current, declared.as_deref()) {
                    pending.push(alt.to_string());
                }
            }
        }
        let mut uniq = Vec::new();
        for key in destinations {
            if !uniq.iter().any(|existing: &QueueKey| existing == &key) {
                uniq.push(key);
            }
        }
        Ok(RouteResult {
            exchange_name,
            kind,
            destinations: uniq,
        })
    }

    /// Insert or replace a policy. Rejects a pattern that is not a valid regex.
    pub fn upsert_policy(&self, policy: Policy) -> Result<()> {
        if regex::Regex::new(&policy.pattern).is_err() {
            return Err(Error::PreconditionFailed(format!(
                "invalid policy pattern '{}'",
                policy.pattern
            )));
        }
        let _g = self.write.lock().expect("router write lock poisoned");
        let mut list = (**self.policies.load()).clone();
        if let Some(slot) = list
            .iter()
            .position(|p| p.vhost == policy.vhost && p.name == policy.name)
        {
            list[slot] = policy;
        } else {
            list.push(policy);
        }
        self.policies.store(Arc::new(list));
        Ok(())
    }

    /// Remove a policy. Returns whether it existed.
    pub fn delete_policy(&self, vhost: &str, name: &str) -> bool {
        let _g = self.write.lock().expect("router write lock poisoned");
        let mut list = (**self.policies.load()).clone();
        let before = list.len();
        list.retain(|p| !(p.vhost.as_str() == vhost && p.name.as_str() == name));
        let removed = list.len() != before;
        self.policies.store(Arc::new(list));
        removed
    }

    /// Policies in one vhost, or every vhost when `vhost` is `None`.
    pub fn list_policies(&self, vhost: Option<&str>) -> Vec<Policy> {
        let list = self.policies.load();
        let mut out: Vec<Policy> = list
            .iter()
            .filter(|p| vhost.map(|v| p.vhost.as_str() == v).unwrap_or(true))
            .cloned()
            .collect();
        out.sort_by(|a, b| (&a.vhost, &a.name).cmp(&(&b.vhost, &b.name)));
        out
    }

    /// Queue arguments after the matching policy fills keys the declare left unset.
    pub fn queue_args_with_policy(&self, vhost: &str, name: &str, declared: &QueueArgs) -> QueueArgs {
        let list = self.policies.load();
        apply_queue_policy(
            declared,
            select_policy(list.as_slice(), vhost, name, PolicyTarget::Queues),
        )
    }

    /// Alternate exchange after policy fill. A non-empty declare argument wins.
    pub fn alternate_for(&self, vhost: &str, exchange: &str, declared: Option<&str>) -> Option<CompactString> {
        let list = self.policies.load();
        effective_alternate(
            declared,
            select_policy(list.as_slice(), vhost, exchange, PolicyTarget::Exchanges),
        )
    }

    /// Route a publish with no application headers.
    pub fn route(&self, vhost: &str, exchange: &str, routing_key: &str) -> Result<RouteResult> {
        self.route_with_headers(vhost, exchange, routing_key, &[])
    }

    /// Route one exchange using the routing key and header arguments.
    pub fn route_with_headers(
        &self,
        vhost: &str,
        exchange: &str,
        routing_key: &str,
        headers: &[(CompactString, crate::domain::HeaderArg)],
    ) -> Result<RouteResult> {
        if exchange == DEFAULT_EXCHANGE_NAME {
            // Implicit direct: RK = queue name. Existence checked by caller.
            return Ok(RouteResult {
                exchange_name: CompactString::from(DEFAULT_EXCHANGE_NAME),
                kind: ExchangeType::Default,
                destinations: vec![QueueKey::new(vhost, routing_key)],
            });
        }

        let map = self.exchanges.load();
        let Some(ex) = map.get(&(CompactString::from(vhost), CompactString::from(exchange))) else {
            return Err(Error::NotFound(format!("exchange {vhost}/{exchange}")));
        };
        if ex.internal {
            return Err(Error::PreconditionFailed(format!(
                "exchange {exchange} is internal"
            )));
        }

        let names = self
            .index
            .load()
            .route_named(vhost, exchange, ex.kind, routing_key, headers);
        let destinations = names.into_iter().map(|q| QueueKey::new(vhost, q)).collect();
        Ok(RouteResult {
            exchange_name: CompactString::from(exchange),
            kind: ex.kind,
            destinations,
        })
    }
}

/// Result of routing a single publish.
#[derive(Debug, Clone)]
pub struct RouteResult {
    /// Exchange name the message was published to.
    pub exchange_name: CompactString,
    /// Exchange type used for routing.
    pub kind: ExchangeType,
    /// Destination queues (may be empty → unroutable).
    pub destinations: Vec<QueueKey>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::ExchangeType;

    #[test]
    fn headers_unbind_keeps_the_sibling_binding() {
        use crate::domain::HeaderArg;
        let r = ExchangeRouter::new();
        r.put_exchange(Exchange {
            vhost: "/".into(),
            name: "h".into(),
            kind: ExchangeType::Headers,
            durable: false,
            auto_delete: false,
            internal: false,
            alternate: None,
        });
        let mut color = Binding::new("/", "h", "q2", "");
        color.args = vec![(CompactString::from("color"), HeaderArg::Str("blue".into()))];
        let mut size = Binding::new("/", "h", "q2", "other");
        size.args = vec![(CompactString::from("size"), HeaderArg::Str("l".into()))];
        r.bind(color.clone()).unwrap();
        r.bind(size).unwrap();
        r.unbind(&color).unwrap();
        let routed = r
            .route_publish(
                "/",
                "h",
                "ignored",
                &[(CompactString::from("size"), HeaderArg::Str("l".into()))],
            )
            .unwrap();
        let names: Vec<_> = routed
            .destinations
            .iter()
            .map(|k| k.name.as_str())
            .collect();
        assert_eq!(names, vec!["q2"]);
    }

    fn arg_binding(
        exchange: &str,
        queue: &str,
        routing_key: &str,
        name: &str,
        value: &str,
    ) -> Binding {
        let mut binding = Binding::new("/", exchange, queue, routing_key);
        binding.args = vec![(
            CompactString::from(name),
            crate::domain::HeaderArg::Str(value.into()),
        )];
        binding
    }

    fn put_kind(router: &ExchangeRouter, name: &str, kind: ExchangeType) {
        router.put_exchange(Exchange {
            vhost: "/".into(),
            name: name.into(),
            kind,
            durable: false,
            auto_delete: false,
            internal: false,
            alternate: None,
        });
    }

    #[test]
    fn unbind_one_direct_arg_variant_keeps_the_sibling_routed() {
        let router = ExchangeRouter::new();
        put_kind(&router, "d", ExchangeType::Direct);
        let color = arg_binding("d", "q", "rk", "color", "blue");
        let size = arg_binding("d", "q", "rk", "size", "l");
        router.bind(color.clone()).unwrap();
        router.bind(size).unwrap();
        router.unbind(&color).unwrap();
        let routed = router.route_publish("/", "d", "rk", &[]).unwrap();
        let names: Vec<_> = routed
            .destinations
            .iter()
            .map(|k| k.name.as_str())
            .collect();
        assert_eq!(names, vec!["q"]);
    }

    #[test]
    fn unbind_one_fanout_arg_variant_keeps_the_sibling_routed() {
        let router = ExchangeRouter::new();
        put_kind(&router, "f", ExchangeType::Fanout);
        let color = arg_binding("f", "q", "", "color", "blue");
        let size = arg_binding("f", "q", "", "size", "l");
        router.bind(color.clone()).unwrap();
        router.bind(size).unwrap();
        router.unbind(&color).unwrap();
        let routed = router.route_publish("/", "f", "ignored", &[]).unwrap();
        let names: Vec<_> = routed
            .destinations
            .iter()
            .map(|k| k.name.as_str())
            .collect();
        assert_eq!(names, vec!["q"]);
    }

    #[test]
    fn unbind_one_topic_arg_variant_keeps_the_sibling_routed() {
        let router = ExchangeRouter::new();
        put_kind(&router, "t", ExchangeType::Topic);
        let color = arg_binding("t", "q", "orders.*", "color", "blue");
        let size = arg_binding("t", "q", "orders.*", "size", "l");
        router.bind(color.clone()).unwrap();
        router.bind(size).unwrap();
        router.unbind(&color).unwrap();
        let routed = router.route_publish("/", "t", "orders.new", &[]).unwrap();
        let names: Vec<_> = routed
            .destinations
            .iter()
            .map(|k| k.name.as_str())
            .collect();
        assert_eq!(names, vec!["q"]);
    }

    #[test]
    fn unbind_one_headers_arg_variant_keeps_the_sibling_routed() {
        let router = ExchangeRouter::new();
        put_kind(&router, "h", ExchangeType::Headers);
        let color = arg_binding("h", "q", "", "color", "blue");
        let size = arg_binding("h", "q", "", "size", "l");
        router.bind(color.clone()).unwrap();
        router.bind(size).unwrap();
        router.unbind(&color).unwrap();
        let routed = router
            .route_publish(
                "/",
                "h",
                "",
                &[(
                    CompactString::from("size"),
                    crate::domain::HeaderArg::Str("l".into()),
                )],
            )
            .unwrap();
        let names: Vec<_> = routed
            .destinations
            .iter()
            .map(|k| k.name.as_str())
            .collect();
        assert_eq!(names, vec!["q"]);
        let gone = router
            .route_publish(
                "/",
                "h",
                "",
                &[(
                    CompactString::from("color"),
                    crate::domain::HeaderArg::Str("blue".into()),
                )],
            )
            .unwrap();
        assert!(gone.destinations.is_empty());
    }

    #[test]
    fn topic_lookup_skips_unrelated_first_words() {
        let mut idx = BindingIndex::empty(1);
        for i in 0..1000 {
            let key = BindingKey::new("/", "t", format!("q{i}"), format!("other.{i}"));
            idx.insert(key, ExchangeType::Topic);
        }
        idx.insert(
            BindingKey::new("/", "t", "hit", "want.x"),
            ExchangeType::Topic,
        );
        let (names, examined) = idx.topic_destinations("/", "t", "want.x");
        assert_eq!(names, vec![CompactString::from("hit")]);
        assert!(
            examined < 10,
            "unrelated bindings were examined: {examined}"
        );
    }

    #[test]
    fn topic_star_and_hash() {
        assert!(topic_matches("a.*", "a.b"));
        assert!(!topic_matches("a.*", "a.b.c"));
        assert!(topic_matches("a.#", "a.b.c"));
        assert!(topic_matches("a.#", "a"));
        assert!(topic_matches("#", "x.y.z"));
        assert!(topic_matches("#", ""));
        assert!(topic_matches("*.orange.*", "quick.orange.rabbit"));
        assert!(!topic_matches("*.orange.*", "quick.orange.male.rabbit"));
        assert!(topic_matches("lazy.#", "lazy.pink.rabbit"));
        assert!(topic_matches("*.*.rabbit", "quick.orange.rabbit"));
        assert!(!topic_matches("a.b", "a.c"));
        assert!(topic_matches("a.b", "a.b"));
    }

    #[test]
    fn direct_and_fanout_routing() {
        let r = ExchangeRouter::new();
        r.put_exchange(Exchange::new("/", "d", ExchangeType::Direct));
        r.put_exchange(Exchange::new("/", "f", ExchangeType::Fanout));

        r.bind(Binding::new("/", "d", "q1", "rk1")).unwrap();
        r.bind(Binding::new("/", "d", "q2", "rk1")).unwrap();
        r.bind(Binding::new("/", "d", "q3", "other")).unwrap();
        r.bind(Binding::new("/", "f", "qa", "")).unwrap();
        r.bind(Binding::new("/", "f", "qb", "ignored")).unwrap();

        let d = r.route("/", "d", "rk1").unwrap();
        assert_eq!(d.destinations.len(), 2);
        let names: Vec<_> = d.destinations.iter().map(|k| k.name.as_str()).collect();
        assert!(names.contains(&"q1"));
        assert!(names.contains(&"q2"));

        let f = r.route("/", "f", "anything").unwrap();
        assert_eq!(f.destinations.len(), 2);

        let empty = r.route("/", "d", "nope").unwrap();
        assert!(empty.destinations.is_empty());
    }

    #[test]
    fn topic_routing_dedupes_queue() {
        let r = ExchangeRouter::new();
        r.put_exchange(Exchange::new("/", "t", ExchangeType::Topic));
        r.bind(Binding::new("/", "t", "q1", "a.*")).unwrap();
        r.bind(Binding::new("/", "t", "q1", "a.#")).unwrap();
        r.bind(Binding::new("/", "t", "q2", "a.b")).unwrap();

        let res = r.route("/", "t", "a.b").unwrap();
        let names: Vec<_> = res.destinations.iter().map(|k| k.name.as_str()).collect();
        assert_eq!(names, vec!["q1", "q2"]);
    }

    #[test]
    fn cannot_bind_default_exchange() {
        let r = ExchangeRouter::new();
        r.put_exchange(Exchange {
            vhost: "/".into(),
            name: "".into(),
            kind: ExchangeType::Default,
            durable: true,
            auto_delete: false,
            internal: true,
            alternate: None,
        });
        let err = r.bind(Binding::new("/", "", "q", "q")).unwrap_err();
        assert!(matches!(err, Error::PreconditionFailed(_)));
    }

    #[test]
    fn unbind_and_delete_exchange() {
        let r = ExchangeRouter::new();
        r.put_exchange(Exchange::new("/", "x", ExchangeType::Direct));
        r.bind(Binding::new("/", "x", "q", "k")).unwrap();
        assert!(r.unbind(&Binding::new("/", "x", "q", "k")).unwrap());
        assert!(!r.unbind(&Binding::new("/", "x", "q", "k")).unwrap());
        r.bind(Binding::new("/", "x", "q", "k")).unwrap();
        r.delete_exchange("/", "x").unwrap();
        assert!(r.route("/", "x", "k").is_err());
    }
}
