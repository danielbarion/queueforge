//! Binding rows. Header arguments stay part of the binding identity.

use queueforge_core::{Binding, DEFAULT_EXCHANGE_NAME};
use redb::ReadableTable;
use tracing::debug;

use super::binding_args_key;
use super::MetadataStore;
use crate::error::{Result, StoreError};
use crate::tables::{BINDINGS, EXCHANGES, QUEUES};

impl MetadataStore {
    /// Create a durable binding row. Fails if the binding already exists.
    ///
    /// Rejects bindings to the default exchange `""`. The exchange and queue
    /// must already exist.
    pub fn create_binding(&self, binding: &Binding) -> Result<()> {
        if binding.exchange.as_str() == DEFAULT_EXCHANGE_NAME {
            return Err(StoreError::BuiltinExchange {
                vhost: binding.vhost.to_string(),
                name: DEFAULT_EXCHANGE_NAME.to_string(),
            });
        }
        let txn = self.write_txn()?;
        {
            let exchanges = txn.open_table(EXCHANGES)?;
            if exchanges
                .get((binding.vhost.as_str(), binding.exchange.as_str()))?
                .is_none()
            {
                return Err(StoreError::ExchangeNotFound {
                    vhost: binding.vhost.to_string(),
                    name: binding.exchange.to_string(),
                });
            }
        }
        {
            let queues = txn.open_table(QUEUES)?;
            if queues
                .get((binding.vhost.as_str(), binding.queue.as_str()))?
                .is_none()
            {
                return Err(StoreError::QueueNotFound {
                    vhost: binding.vhost.to_string(),
                    name: binding.queue.to_string(),
                });
            }
        }
        {
            let mut bindings = txn.open_table(BINDINGS)?;
            let args_key = binding_args_key(&binding.args);
            let key = (
                binding.vhost.as_str(),
                binding.exchange.as_str(),
                binding.queue.as_str(),
                binding.routing_key.as_str(),
                args_key.as_str(),
            );
            if bindings.get(key)?.is_some() {
                return Err(StoreError::BindingExists {
                    vhost: binding.vhost.to_string(),
                    exchange: binding.exchange.to_string(),
                    queue: binding.queue.to_string(),
                    routing_key: binding.routing_key.to_string(),
                });
            }
            let bytes = serde_json::to_vec(binding)?;
            bindings.insert(key, bytes.as_slice())?;
        }
        txn.commit()?;
        debug!(
            vhost = %binding.vhost,
            exchange = %binding.exchange,
            queue = %binding.queue,
            rk = %binding.routing_key,
            "created binding"
        );
        Ok(())
    }

    /// Upsert a binding (create or replace). Rejects default-exchange bindings.
    pub fn put_binding(&self, binding: &Binding) -> Result<()> {
        if binding.exchange.as_str() == DEFAULT_EXCHANGE_NAME {
            return Err(StoreError::BuiltinExchange {
                vhost: binding.vhost.to_string(),
                name: DEFAULT_EXCHANGE_NAME.to_string(),
            });
        }
        let txn = self.write_txn()?;
        {
            let exchanges = txn.open_table(EXCHANGES)?;
            if exchanges
                .get((binding.vhost.as_str(), binding.exchange.as_str()))?
                .is_none()
            {
                return Err(StoreError::ExchangeNotFound {
                    vhost: binding.vhost.to_string(),
                    name: binding.exchange.to_string(),
                });
            }
        }
        {
            let queues = txn.open_table(QUEUES)?;
            if queues
                .get((binding.vhost.as_str(), binding.queue.as_str()))?
                .is_none()
            {
                return Err(StoreError::QueueNotFound {
                    vhost: binding.vhost.to_string(),
                    name: binding.queue.to_string(),
                });
            }
        }
        {
            let mut bindings = txn.open_table(BINDINGS)?;
            let args_key = binding_args_key(&binding.args);
            let key = (
                binding.vhost.as_str(),
                binding.exchange.as_str(),
                binding.queue.as_str(),
                binding.routing_key.as_str(),
                args_key.as_str(),
            );
            let bytes = serde_json::to_vec(binding)?;
            bindings.insert(key, bytes.as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Fetch a binding by composite key.
    pub fn get_binding(
        &self,
        vhost: &str,
        exchange: &str,
        queue: &str,
        routing_key: &str,
        args_key: &str,
    ) -> Result<Option<Binding>> {
        let txn = self.read_txn()?;
        let table = txn.open_table(BINDINGS)?;
        match table.get((vhost, exchange, queue, routing_key, args_key))? {
            Some(guard) => Ok(Some(serde_json::from_slice(guard.value())?)),
            None => Ok(None),
        }
    }

    /// List all bindings in a vhost.
    pub fn list_bindings(&self, vhost: &str) -> Result<Vec<Binding>> {
        let txn = self.read_txn()?;
        let table = txn.open_table(BINDINGS)?;
        let mut out = Vec::new();
        for item in table.range((vhost, "", "", "", "")..)? {
            let (key, value) = item?;
            let (vh, _, _, _, _) = key.value();
            if vh != vhost {
                break;
            }
            out.push(serde_json::from_slice(value.value())?);
        }
        Ok(out)
    }

    /// List bindings for a specific exchange in a vhost.
    pub fn list_bindings_for_exchange(&self, vhost: &str, exchange: &str) -> Result<Vec<Binding>> {
        let txn = self.read_txn()?;
        let table = txn.open_table(BINDINGS)?;
        let mut out = Vec::new();
        for item in table.range((vhost, exchange, "", "", "")..)? {
            let (key, value) = item?;
            let (vh, ex, _, _, _) = key.value();
            if vh != vhost || ex != exchange {
                break;
            }
            out.push(serde_json::from_slice(value.value())?);
        }
        Ok(out)
    }

    /// Delete a binding. Returns `true` if it existed.
    pub fn delete_binding(
        &self,
        vhost: &str,
        exchange: &str,
        queue: &str,
        routing_key: &str,
        args_key: &str,
    ) -> Result<bool> {
        let txn = self.write_txn()?;
        let removed = {
            let mut bindings = txn.open_table(BINDINGS)?;
            let guard = bindings.remove((vhost, exchange, queue, routing_key, args_key))?;
            guard.is_some()
        };
        txn.commit()?;
        Ok(removed)
    }
}
