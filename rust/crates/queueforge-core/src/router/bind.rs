//! Exchange declare and binding mutations on the router.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use compact_str::CompactString;
use tracing::debug;

use super::{BindingKey, ExchangeEdge, ExchangeRouter};
use crate::domain::{Binding, Exchange, ExchangeType, DEFAULT_EXCHANGE_NAME};
use crate::error::{Error, Result};

impl ExchangeRouter {
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
        if self.get_exchange(vhost, source).is_none()
            || self.get_exchange(vhost, destination).is_none()
        {
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
    pub fn unbind_exchange(
        &self,
        vhost: &str,
        source: &str,
        destination: &str,
        routing_key: &str,
    ) -> bool {
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
}
