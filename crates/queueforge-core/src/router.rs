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
use crate::queue::QueueKey;

/// Unique identity of a binding row (ignores args).
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
        }
    }

    /// From a domain [`Binding`].
    pub fn from_binding(b: &Binding) -> Self {
        Self {
            vhost: b.vhost.clone(),
            exchange: b.exchange.clone(),
            queue: b.queue.clone(),
            routing_key: b.routing_key.clone(),
        }
    }

    /// Domain [`Binding`] snapshot.
    pub fn to_binding(&self) -> Binding {
        Binding {
            vhost: self.vhost.clone(),
            exchange: self.exchange.clone(),
            queue: self.queue.clone(),
            routing_key: self.routing_key.clone(),
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
    /// `(vhost, exchange) → (pattern, queue)` for topic.
    topic: HashMap<(CompactString, CompactString), Vec<(CompactString, CompactString)>>,
}

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
        if !self.bindings.insert(key.clone()) {
            return;
        }
        match kind {
            ExchangeType::Direct | ExchangeType::Default => {
                self.direct
                    .entry((
                        key.vhost.clone(),
                        key.exchange.clone(),
                        key.routing_key.clone(),
                    ))
                    .or_default()
                    .insert(key.queue);
            }
            ExchangeType::Fanout => {
                self.fanout
                    .entry((key.vhost.clone(), key.exchange.clone()))
                    .or_default()
                    .insert(key.queue);
            }
            ExchangeType::Topic => {
                self.topic
                    .entry((key.vhost.clone(), key.exchange.clone()))
                    .or_default()
                    .push((key.routing_key, key.queue));
            }
        }
    }

    fn remove(&mut self, key: &BindingKey, kind: ExchangeType) -> bool {
        if !self.bindings.remove(key) {
            return false;
        }
        match kind {
            ExchangeType::Direct | ExchangeType::Default => {
                let map_key = (
                    key.vhost.clone(),
                    key.exchange.clone(),
                    key.routing_key.clone(),
                );
                if let Some(set) = self.direct.get_mut(&map_key) {
                    set.remove(&key.queue);
                    if set.is_empty() {
                        self.direct.remove(&map_key);
                    }
                }
            }
            ExchangeType::Fanout => {
                let map_key = (key.vhost.clone(), key.exchange.clone());
                if let Some(set) = self.fanout.get_mut(&map_key) {
                    set.remove(&key.queue);
                    if set.is_empty() {
                        self.fanout.remove(&map_key);
                    }
                }
            }
            ExchangeType::Topic => {
                let map_key = (key.vhost.clone(), key.exchange.clone());
                if let Some(list) = self.topic.get_mut(&map_key) {
                    list.retain(|(rk, q)| {
                        !(rk.as_str() == key.routing_key.as_str()
                            && q.as_str() == key.queue.as_str())
                    });
                    if list.is_empty() {
                        self.topic.remove(&map_key);
                    }
                }
            }
        }
        true
    }

    fn remove_exchange(&mut self, vhost: &str, exchange: &str) {
        let doomed: Vec<BindingKey> = self
            .bindings
            .iter()
            .filter(|b| b.vhost.as_str() == vhost && b.exchange.as_str() == exchange)
            .cloned()
            .collect();
        for b in doomed {
            self.bindings.remove(&b);
        }
        self.direct
            .retain(|(vh, ex, _), _| !(vh.as_str() == vhost && ex.as_str() == exchange));
        self.fanout
            .retain(|(vh, ex), _| !(vh.as_str() == vhost && ex.as_str() == exchange));
        self.topic
            .retain(|(vh, ex), _| !(vh.as_str() == vhost && ex.as_str() == exchange));
    }

    fn remove_queue(&mut self, vhost: &str, queue: &str) {
        let doomed: Vec<BindingKey> = self
            .bindings
            .iter()
            .filter(|b| b.vhost.as_str() == vhost && b.queue.as_str() == queue)
            .cloned()
            .collect();
        for b in &doomed {
            self.bindings.remove(b);
        }
        for set in self.direct.values_mut() {
            set.remove(queue);
        }
        self.direct.retain(|_, set| !set.is_empty());
        for set in self.fanout.values_mut() {
            set.remove(queue);
        }
        self.fanout.retain(|_, set| !set.is_empty());
        for list in self.topic.values_mut() {
            list.retain(|(_, q)| q.as_str() != queue);
        }
        self.topic.retain(|_, list| !list.is_empty());
        let _ = vhost;
    }

    /// Route a publish to destination queue names (not including default exchange).
    pub fn route_named(
        &self,
        vhost: &str,
        exchange: &str,
        kind: ExchangeType,
        routing_key: &str,
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
                if let Some(list) = self
                    .topic
                    .get(&(CompactString::from(vhost), CompactString::from(exchange)))
                {
                    for (pattern, queue) in list {
                        if topic_matches(pattern.as_str(), routing_key) {
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

/// Live exchange table + binding index for routing.
///
/// Mutations serialize on `write`; readers load `Arc` snapshots without blocking.
pub struct ExchangeRouter {
    exchanges: ArcSwap<HashMap<(CompactString, CompactString), Exchange>>,
    index: ArcSwap<BindingIndex>,
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

    /// Resolve destination queue keys for a publish.
    ///
    /// - Default exchange `""`: routing key is the queue name (single dest if present is
    ///   **not** checked here — callers still need the queue registry).
    /// - Named exchanges: use the binding index; missing exchange → `NotFound`.
    /// - Internal exchanges (except default) refuse publish.
    pub fn route(&self, vhost: &str, exchange: &str, routing_key: &str) -> Result<RouteResult> {
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
            .route_named(vhost, exchange, ex.kind, routing_key);
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
