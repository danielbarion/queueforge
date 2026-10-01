//! Exchange routing: binding index, topic matching, multi-destination lookup.
//! Index lookup, bind, route, and policy each live in a sibling module.
//!
//! Binding tables are versioned and swapped via [`arc_swap::ArcSwap`] for
//! lock-free publish-path reads. Mutations take a short write mutex, rebuild,
//! and swap.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};

use arc_swap::ArcSwap;
use compact_str::CompactString;

use crate::domain::{Binding, Exchange, ExchangeType};
use crate::error::Error;
use crate::policy::Policy;
use crate::queue::QueueKey;

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
    operator_policies: ArcSwap<Vec<Policy>>,
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
            operator_policies: ArcSwap::from_pointee(Vec::new()),
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

mod bind;
mod index;
mod policy;
mod route;

pub use index::{headers_match, topic_matches};

#[cfg(test)]
mod tests;
