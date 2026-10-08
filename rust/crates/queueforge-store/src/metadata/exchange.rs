//! Exchange rows, including the builtin-exchange guard.

use queueforge_core::{Exchange, ExchangeRouter};
use redb::ReadableTable;
use tracing::debug;

use super::schema::reject_builtin_name;
use super::MetadataStore;
use crate::error::{Result, StoreError};
use crate::tables::{EXCHANGES, VHOSTS};

impl MetadataStore {
    /// Build a live [`ExchangeRouter`] from durable exchanges and bindings.
    ///
    /// Implicit default-exchange bindings are **not** loaded (they are not
    /// stored); the router treats `""` as special on the publish path.
    pub fn bootstrap_router(&self) -> Result<ExchangeRouter> {
        let router = ExchangeRouter::new();
        for vh in self.list_vhosts()? {
            for ex in self.list_exchanges(vh.name.as_str())? {
                router.put_exchange(ex);
            }
            for b in self.list_bindings(vh.name.as_str())? {
                // Ignore duplicate / missing-exchange errors: durable rows are
                // authoritative; router put_exchange ran first.
                match router.bind(b) {
                    Ok(_) => {}
                    Err(e) => {
                        debug!(error = %e, "skipping binding during router bootstrap");
                    }
                }
            }
        }
        for vh in self.list_vhosts()? {
            for (source, destination, routing_key) in self.list_exchange_bindings(vh.name.as_str())? {
                if let Err(e) = router.bind_exchange(vh.name.as_str(), &source, &destination, &routing_key) {
                    debug!(error = %e, "skipping exchange binding during router bootstrap");
                }
            }
        }
        for policy in self.list_policies()? {
            let _ = router.upsert_policy(policy);
        }
        Ok(router)
    }

    /// Create an exchange. Fails if an exchange with the same key exists.
    ///
    /// Builtin exchange names (`""`, `amq.direct`, `amq.fanout`, `amq.topic`)
    /// cannot be created through this API.
    pub fn create_exchange(&self, exchange: &Exchange) -> Result<()> {
        reject_builtin_name(exchange.vhost.as_str(), exchange.name.as_str())?;

        let txn = self.write_txn()?;
        {
            let vhosts = txn.open_table(VHOSTS)?;
            let missing = vhosts.get(exchange.vhost.as_str())?.is_none();
            if missing {
                return Err(StoreError::VhostNotFound(exchange.vhost.to_string()));
            }
        }
        {
            let mut exchanges = txn.open_table(EXCHANGES)?;
            let key = (exchange.vhost.as_str(), exchange.name.as_str());
            let exists = exchanges.get(key)?.is_some();
            if exists {
                return Err(StoreError::ExchangeExists {
                    vhost: exchange.vhost.to_string(),
                    name: exchange.name.to_string(),
                });
            }
            let bytes = serde_json::to_vec(exchange)?;
            exchanges.insert(key, bytes.as_slice())?;
        }
        txn.commit()?;
        debug!(
            vhost = %exchange.vhost,
            exchange = %exchange.name,
            "created exchange"
        );
        Ok(())
    }

    /// Upsert an exchange (create or replace).
    ///
    /// Builtin exchange names cannot be modified through this API.
    pub fn put_exchange(&self, exchange: &Exchange) -> Result<()> {
        reject_builtin_name(exchange.vhost.as_str(), exchange.name.as_str())?;

        let txn = self.write_txn()?;
        {
            let vhosts = txn.open_table(VHOSTS)?;
            let missing = vhosts.get(exchange.vhost.as_str())?.is_none();
            if missing {
                return Err(StoreError::VhostNotFound(exchange.vhost.to_string()));
            }
        }
        {
            let mut exchanges = txn.open_table(EXCHANGES)?;
            let key = (exchange.vhost.as_str(), exchange.name.as_str());
            let bytes = serde_json::to_vec(exchange)?;
            exchanges.insert(key, bytes.as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Fetch an exchange by vhost and name.
    pub fn get_exchange(&self, vhost: &str, name: &str) -> Result<Option<Exchange>> {
        let txn = self.read_txn()?;
        let table = txn.open_table(EXCHANGES)?;
        match table.get((vhost, name))? {
            Some(guard) => Ok(Some(serde_json::from_slice(guard.value())?)),
            None => Ok(None),
        }
    }

    /// List exchanges in a vhost (including builtins).
    ///
    /// Uses a composite-key range starting at `(vhost, "")` and stops when the
    /// vhost prefix diverges.
    pub fn list_exchanges(&self, vhost: &str) -> Result<Vec<Exchange>> {
        let txn = self.read_txn()?;
        let table = txn.open_table(EXCHANGES)?;
        let mut out = Vec::new();
        for item in table.range((vhost, "")..)? {
            let (key, value) = item?;
            let (vh, _) = key.value();
            if vh != vhost {
                break;
            }
            out.push(serde_json::from_slice(value.value())?);
        }
        Ok(out)
    }

    /// Delete an exchange and cascade its bindings. Builtin exchanges cannot be deleted.
    ///
    /// Returns `true` if the exchange existed and was removed.
    pub fn delete_exchange(&self, vhost: &str, name: &str) -> Result<bool> {
        reject_builtin_name(vhost, name)?;
        let txn = self.write_txn()?;
        let removed = {
            let mut exchanges = txn.open_table(EXCHANGES)?;
            let guard = exchanges.remove((vhost, name))?;
            guard.is_some()
        };
        if removed {
            Self::delete_bindings_for_exchange(&txn, vhost, name)?;
            Self::delete_exchange_bindings_touching(&txn, vhost, name)?;
        }
        txn.commit()?;
        Ok(removed)
    }
}
