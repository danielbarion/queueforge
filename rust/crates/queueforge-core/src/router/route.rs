//! Publish routing, including exchange-to-exchange edges.

use std::collections::HashSet;

use compact_str::CompactString;

use super::index::topic_matches;
use super::{ExchangeRouter, RouteResult};
use crate::domain::{ExchangeType, DEFAULT_EXCHANGE_NAME};
use crate::error::{Error, Result};
use crate::queue::QueueKey;

impl ExchangeRouter {
    /// List exchange-to-exchange destinations from `source` on `vhost` for `routing_key`. Returns destination exchange names. Cycles are the caller's problem; this list is one hop.
    pub(super) fn next_exchanges(
        &self,
        vhost: &str,
        source: &str,
        routing_key: &str,
    ) -> Vec<String> {
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
    /// - Internal exchanges route like any other; the AMQP publish path refuses a
    ///   client publish to one before routing.
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
        let names = self
            .index
            .load()
            .route_named(vhost, exchange, ex.routing_kind(), routing_key, headers);
        let destinations = names.into_iter().map(|q| QueueKey::new(vhost, q)).collect();
        Ok(RouteResult {
            exchange_name: CompactString::from(exchange),
            kind: ex.kind,
            destinations,
        })
    }
}
