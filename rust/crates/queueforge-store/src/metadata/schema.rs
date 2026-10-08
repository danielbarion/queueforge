//! Schema bootstrap, builtin exchanges, and cascading deletes inside one write transaction.

use queueforge_core::{Binding, Exchange, Vhost, BUILTIN_EXCHANGE_NAMES, DEFAULT_VHOST};
use redb::{ReadableTable, TableHandle};
use tracing::{debug, info};

use super::binding_args_key;
use super::MetadataStore;
use crate::error::{Result, StoreError};
use crate::tables::{
    BINDINGS, EXCHANGES, LEGACY_BINDINGS, PERMISSIONS, POLICIES, QUEUES, SCHEMA_VERSION,
    SCHEMA_VERSION_KEY, SCHEMA_VERSION_V1, USERS, VHOSTS,
};

impl MetadataStore {
    /// Create schema v1 on an empty database, or refuse a version this process does not know. Returns the store error. Builtin exchanges for `/` are written only on first create.
    pub(super) fn init_schema(&self) -> Result<()> {
        let txn = self.write_txn()?;

        let existing_version = {
            let version_table = txn.open_table(SCHEMA_VERSION)?;
            let guard = version_table.get(SCHEMA_VERSION_KEY)?;
            guard.map(|v| v.value())
        };

        match existing_version {
            None => {
                {
                    let mut version_table = txn.open_table(SCHEMA_VERSION)?;
                    version_table.insert(SCHEMA_VERSION_KEY, SCHEMA_VERSION_V1)?;
                }
                {
                    let mut vhosts = txn.open_table(VHOSTS)?;
                    let default = Vhost::new(DEFAULT_VHOST);
                    let bytes = serde_json::to_vec(&default)?;
                    vhosts.insert(DEFAULT_VHOST, bytes.as_slice())?;
                }
                Self::write_builtin_exchanges(&txn, DEFAULT_VHOST)?;
                {
                    let _queues = txn.open_table(QUEUES)?;
                    let _users = txn.open_table(USERS)?;
                    let _permissions = txn.open_table(PERMISSIONS)?;
                    let _bindings = txn.open_table(BINDINGS)?;
                    let _e2e = txn.open_table(crate::tables::EXCHANGE_BINDINGS)?;
                    let _params = txn.open_table(crate::tables::PARAMETERS)?;
                }
                info!(
                    data_dir = %self.data_dir.display(),
                    "bootstrapped metadata schema v1 with default vhost and builtin exchanges"
                );
            }
            Some(version) => {
                if version != SCHEMA_VERSION_V1 {
                    return Err(StoreError::UnsupportedSchema(version));
                }
                let vhost_names = {
                    let vhosts = txn.open_table(VHOSTS)?;
                    let mut names = Vec::new();
                    for item in vhosts.iter()? {
                        let (k, _) = item?;
                        names.push(k.value().to_string());
                    }
                    names
                };
                {
                    let _exchanges = txn.open_table(EXCHANGES)?;
                    let _queues = txn.open_table(QUEUES)?;
                    // Ensure auth/bindings tables exist on stores created before
                    // those features (schema v1 is compatible; tables are
                    // created empty on first open).
                    let _users = txn.open_table(USERS)?;
                    let _permissions = txn.open_table(PERMISSIONS)?;
                    let _bindings = txn.open_table(BINDINGS)?;
                    let _e2e = txn.open_table(crate::tables::EXCHANGE_BINDINGS)?;
                    let _params = txn.open_table(crate::tables::PARAMETERS)?;
                }
                Self::migrate_legacy_bindings(&txn)?;
                for name in &vhost_names {
                    Self::ensure_builtin_exchanges(&txn, name)?;
                }
                debug!(
                    data_dir = %self.data_dir.display(),
                    version,
                    vhosts = vhost_names.len(),
                    "opened existing metadata store"
                );
            }
        }

        {
            let _policies = txn.open_table(POLICIES)?;
        }

        txn.commit()?;
        Ok(())
    }

    /// Unconditionally write canonical builtin exchange rows for `vhost`.
    pub(super) fn write_builtin_exchanges(txn: &redb::WriteTransaction, vhost: &str) -> Result<()> {
        let mut exchanges = txn.open_table(EXCHANGES)?;
        for ex in Exchange::builtins_for(vhost) {
            let key = (ex.vhost.as_str(), ex.name.as_str());
            let bytes = serde_json::to_vec(&ex)?;
            exchanges.insert(key, bytes.as_slice())?;
        }
        Ok(())
    }

    /// Insert missing builtins and repair any whose stored attributes differ
    /// from the canonical definition.
    pub(super) fn ensure_builtin_exchanges(
        txn: &redb::WriteTransaction,
        vhost: &str,
    ) -> Result<()> {
        let mut exchanges = txn.open_table(EXCHANGES)?;
        for ex in Exchange::builtins_for(vhost) {
            let key = (ex.vhost.as_str(), ex.name.as_str());
            let needs_write = {
                let guard = exchanges.get(key)?;
                match guard {
                    None => true,
                    Some(g) => {
                        let existing: Exchange = serde_json::from_slice(g.value())?;
                        existing != ex
                    }
                }
            };
            if needs_write {
                let bytes = serde_json::to_vec(&ex)?;
                exchanges.insert(key, bytes.as_slice())?;
                debug!(
                    vhost,
                    exchange = %ex.name,
                    "restored or repaired builtin exchange"
                );
            }
        }
        Ok(())
    }

    /// Remove all exchanges for `vhost`, propagating iteration/storage errors.
    pub(super) fn delete_exchanges_for_vhost(
        txn: &redb::WriteTransaction,
        vhost: &str,
    ) -> Result<()> {
        let mut exchanges = txn.open_table(EXCHANGES)?;
        // Range from (vhost, "") covers this vhost and later keys; retain only
        // rows whose vhost differs so this vhost is fully removed in one pass.
        exchanges.retain_in((vhost, "").., |k, _| {
            let (vh, _) = k;
            vh != vhost
        })?;
        Ok(())
    }

    /// Remove all queues for `vhost`, propagating storage errors.
    pub(super) fn delete_queues_for_vhost(txn: &redb::WriteTransaction, vhost: &str) -> Result<()> {
        let mut queues = txn.open_table(QUEUES)?;
        queues.retain_in((vhost, "").., |k, _| {
            let (vh, _) = k;
            vh != vhost
        })?;
        Ok(())
    }

    /// Remove all permission rows for `user`.
    pub(super) fn delete_permissions_for_user(
        txn: &redb::WriteTransaction,
        user: &str,
    ) -> Result<()> {
        let mut perms = txn.open_table(PERMISSIONS)?;
        perms.retain_in((user, "").., |k, _| {
            let (u, _) = k;
            u != user
        })?;
        Ok(())
    }

    /// Remove all permission rows for `vhost` (any user).
    pub(super) fn delete_permissions_for_vhost(
        txn: &redb::WriteTransaction,
        vhost: &str,
    ) -> Result<()> {
        let mut perms = txn.open_table(PERMISSIONS)?;
        perms.retain(|k, _| {
            let (_, vh) = k;
            vh != vhost
        })?;
        Ok(())
    }

    /// Copy pre-`bindings_v2` rows, preserving arguments, then drop the old table.
    pub(super) fn migrate_legacy_bindings(txn: &redb::WriteTransaction) -> Result<()> {
        let has_legacy = txn.list_tables()?.any(|table| table.name() == "bindings");
        if !has_legacy {
            return Ok(());
        }
        let rows = {
            let old = txn.open_table(LEGACY_BINDINGS)?;
            let mut rows = Vec::new();
            for item in old.iter()? {
                let (_, value) = item?;
                rows.push(serde_json::from_slice::<Binding>(value.value())?);
            }
            rows
        };
        {
            let mut bindings = txn.open_table(BINDINGS)?;
            for binding in &rows {
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
        }
        txn.delete_table(LEGACY_BINDINGS)?;
        info!(rows = rows.len(), "migrated legacy bindings table");
        Ok(())
    }

    /// Remove all bindings for `vhost`.
    pub(super) fn delete_bindings_for_vhost(
        txn: &redb::WriteTransaction,
        vhost: &str,
    ) -> Result<()> {
        let mut bindings = txn.open_table(BINDINGS)?;
        bindings.retain_in((vhost, "", "", "", "").., |k, _| {
            let (vh, _, _, _, _) = k;
            vh != vhost
        })?;
        Ok(())
    }

    /// Remove all bindings for `(vhost, exchange)`.
    pub(super) fn delete_bindings_for_exchange(
        txn: &redb::WriteTransaction,
        vhost: &str,
        exchange: &str,
    ) -> Result<()> {
        let mut bindings = txn.open_table(BINDINGS)?;
        bindings.retain_in((vhost, exchange, "", "", "").., |k, _| {
            let (vh, ex, _, _, _) = k;
            !(vh == vhost && ex == exchange)
        })?;
        Ok(())
    }

    /// Remove all bindings targeting `(vhost, queue)`.
    pub(super) fn delete_bindings_for_queue(
        txn: &redb::WriteTransaction,
        vhost: &str,
        queue: &str,
    ) -> Result<()> {
        let mut bindings = txn.open_table(BINDINGS)?;
        // Full scan of vhost prefix: composite key is not ordered by queue.
        bindings.retain_in((vhost, "", "", "", "").., |k, _| {
            let (vh, _, q, _, _) = k;
            if vh != vhost {
                return true;
            }
            q != queue
        })?;
        Ok(())
    }
}

/// Reject mutations of server-defined builtin exchange names.
pub(super) fn reject_builtin_name(vhost: &str, name: &str) -> Result<()> {
    if BUILTIN_EXCHANGE_NAMES.contains(&name) {
        return Err(StoreError::BuiltinExchange {
            vhost: vhost.to_string(),
            name: name.to_string(),
        });
    }
    Ok(())
}
