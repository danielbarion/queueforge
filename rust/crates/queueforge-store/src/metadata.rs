//! redb-backed metadata store for vhosts, users, permissions, exchanges, queues,
//! and bindings.

use std::cell::Cell;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use queueforge_core::{
    Binding, Exchange, ExchangeRouter, Permission, Policy, Queue, User, Vhost,
    BUILTIN_EXCHANGE_NAMES, DEFAULT_EXCHANGE_NAME, DEFAULT_VHOST,
};
use redb::{Database, ReadableTable, TableHandle};
use tracing::{debug, info};

use crate::error::{Result, StoreError};
use crate::tables::{
    BINDINGS, EXCHANGES, LEGACY_BINDINGS, PERMISSIONS, POLICIES, QUEUES, SCHEMA_VERSION, SCHEMA_VERSION_KEY,
    SCHEMA_VERSION_V1, USERS, VHOSTS,
};

/// Filename of the redb metadata database inside the data directory.
pub const METADATA_DB_FILE: &str = "metadata.db";

pub use queueforge_core::binding_args_key;

thread_local! {
    static IN_BLOCKING_HELPER: Cell<bool> = const { Cell::new(false) };
}

static TRACK_STORE_SCHEDULER: AtomicBool = AtomicBool::new(false);
static STORE_CALLS_ON_WORKER: AtomicU64 = AtomicU64::new(0);
static STORE_CALLS_OFF_WORKER: AtomicU64 = AtomicU64::new(0);

/// Counts of metadata transactions observed while tracking is armed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoreSchedulerStats {
    /// Transactions opened on a Tokio async task.
    pub on_worker: u64,
    /// Transactions opened off an async task (blocking pool, tests, startup).
    pub off_worker: u64,
}

/// Arm or disarm counting of which scheduler opens metadata transactions.
///
/// Turning tracking on resets both counters. A call on a Tokio task counts as
/// `on_worker`. `spawn_blocking` (and other non-task threads) count as
/// `off_worker`.
pub fn track_store_scheduler(enabled: bool) {
    STORE_CALLS_ON_WORKER.store(0, Ordering::Relaxed);
    STORE_CALLS_OFF_WORKER.store(0, Ordering::Relaxed);
    TRACK_STORE_SCHEDULER.store(enabled, Ordering::Relaxed);
}

/// Snapshot the counters. Meaningful only while [`track_store_scheduler`] is on.
pub fn store_scheduler_stats() -> StoreSchedulerStats {
    StoreSchedulerStats {
        on_worker: STORE_CALLS_ON_WORKER.load(Ordering::Relaxed),
        off_worker: STORE_CALLS_OFF_WORKER.load(Ordering::Relaxed),
    }
}

fn note_store_scheduler() {
    if !TRACK_STORE_SCHEDULER.load(Ordering::Relaxed) {
        return;
    }
    let via_helper = IN_BLOCKING_HELPER.with(|flag| flag.get());
    if via_helper {
        STORE_CALLS_OFF_WORKER.fetch_add(1, Ordering::Relaxed);
    } else if tokio::task::try_id().is_some() {
        STORE_CALLS_ON_WORKER.fetch_add(1, Ordering::Relaxed);
    } else {
        STORE_CALLS_OFF_WORKER.fetch_add(1, Ordering::Relaxed);
    }
}

/// Persistent metadata store (vhosts, users, permissions, exchanges, queues, bindings).
///
/// On first open of an empty data directory, creates schema v1 and bootstraps
/// vhost `/` with builtin exchanges `""`, `amq.direct`, `amq.fanout`, `amq.topic`.
/// Users are **not** created here; see `queueforge-auth` bootstrap.
///
/// Builtin exchanges are immutable via the public CRUD API; only bootstrap and
/// open-time repair paths may write them.
pub struct MetadataStore {
    db: Database,
    data_dir: PathBuf,
}

impl MetadataStore {
    /// Open (or create) the metadata database under `data_dir`.
    ///
    /// Creates `data_dir` if it does not exist. Bootstraps defaults when the
    /// database has no schema version yet.
    pub fn open(data_dir: impl AsRef<Path>) -> Result<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&data_dir)?;

        let db_path = data_dir.join(METADATA_DB_FILE);
        let db = Database::create(&db_path)?;

        let store = Self { db, data_dir };
        store.init_schema()?;
        Ok(store)
    }

    /// Root data directory this store was opened with.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Flush and close the underlying redb database.
    ///
    /// Prefer this at process shutdown over bare `drop` so the close is explicit
    /// in logs. redb's `Drop` already flushes; this is the ordered-shutdown step.
    pub fn close(self) {
        info!(
            data_dir = %self.data_dir.display(),
            "closing metadata store (redb)"
        );
        // `Database::drop` flushes and releases the file lock.
        drop(self);
    }

    /// Run a metadata operation on Tokio's blocking pool.
    ///
    /// redb transactions must not run on a worker thread. Callers that already
    /// hold `Arc<MetadataStore>` (connection tasks, management handlers) use this
    /// so a write lock cannot stall the runtime. Sync methods stay for tests and
    /// the startup path.
    pub async fn blocking<T, F>(this: std::sync::Arc<Self>, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Self) -> Result<T> + Send + 'static,
    {
        match tokio::task::spawn_blocking(move || {
            IN_BLOCKING_HELPER.with(|flag| flag.set(true));
            let result = f(&this);
            IN_BLOCKING_HELPER.with(|flag| flag.set(false));
            result
        })
        .await
        {
            Ok(result) => result,
            Err(e) => Err(StoreError::Database(format!("metadata task panicked: {e}"))),
        }
    }

    fn read_txn(&self) -> Result<redb::ReadTransaction> {
        note_store_scheduler();
        Ok(self.db.begin_read()?)
    }

    fn write_txn(&self) -> Result<redb::WriteTransaction> {
        note_store_scheduler();
        Ok(self.db.begin_write()?)
    }

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
        for policy in self.list_policies()? {
            let _ = router.upsert_policy(policy);
        }
        Ok(router)
    }

    /// Insert or replace a policy row.
    pub fn put_policy(&self, policy: &Policy) -> Result<()> {
        let txn = self.write_txn()?;
        {
            let mut table = txn.open_table(POLICIES)?;
            let bytes = serde_json::to_vec(policy)?;
            table.insert((policy.vhost.as_str(), policy.name.as_str()), bytes.as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Delete a policy row. Returns whether a row was removed.
    pub fn delete_policy(&self, vhost: &str, name: &str) -> Result<bool> {
        let txn = self.write_txn()?;
        let removed = {
            let mut table = txn.open_table(POLICIES)?;
            let gone = table.remove((vhost, name))?.is_some();
            gone
        };
        txn.commit()?;
        Ok(removed)
    }

    /// Every stored policy.
    pub fn list_policies(&self) -> Result<Vec<Policy>> {
        let txn = self.read_txn()?;
        let table = txn.open_table(POLICIES)?;
        let mut out = Vec::new();
        for item in table.iter()? {
            let (_, value) = item?;
            out.push(serde_json::from_slice(value.value())?);
        }
        Ok(out)
    }

    /// Current schema version stored in the database.
    pub fn schema_version(&self) -> Result<u32> {
        let txn = self.read_txn()?;
        let table = txn.open_table(SCHEMA_VERSION)?;
        let version = table
            .get(SCHEMA_VERSION_KEY)?
            .map(|v| v.value())
            .unwrap_or(0);
        Ok(version)
    }

    // ── Vhosts ──────────────────────────────────────────────────────────

    /// Create a vhost and its builtin exchanges.
    ///
    /// Returns [`StoreError::VhostExists`] if the name is already present.
    pub fn create_vhost(&self, name: &str) -> Result<Vhost> {
        let txn = self.write_txn()?;
        {
            let mut vhosts = txn.open_table(VHOSTS)?;
            let exists = vhosts.get(name)?.is_some();
            if exists {
                return Err(StoreError::VhostExists(name.to_string()));
            }
            let vhost = Vhost::new(name);
            let bytes = serde_json::to_vec(&vhost)?;
            vhosts.insert(name, bytes.as_slice())?;
        }
        Self::write_builtin_exchanges(&txn, name)?;
        txn.commit()?;
        debug!(vhost = name, "created vhost with builtin exchanges");
        Ok(Vhost::new(name))
    }

    /// Fetch a vhost by name.
    pub fn get_vhost(&self, name: &str) -> Result<Option<Vhost>> {
        let txn = self.read_txn()?;
        let table = txn.open_table(VHOSTS)?;
        match table.get(name)? {
            Some(guard) => Ok(Some(serde_json::from_slice(guard.value())?)),
            None => Ok(None),
        }
    }

    /// List all vhosts, ordered by name (redb key order).
    pub fn list_vhosts(&self) -> Result<Vec<Vhost>> {
        let txn = self.read_txn()?;
        let table = txn.open_table(VHOSTS)?;
        let mut out = Vec::new();
        for item in table.iter()? {
            let (_, value) = item?;
            out.push(serde_json::from_slice(value.value())?);
        }
        Ok(out)
    }

    /// Delete a vhost and all of its exchanges, queues, bindings, and permissions.
    ///
    /// Returns `true` if the vhost existed and was removed.
    pub fn delete_vhost(&self, name: &str) -> Result<bool> {
        let txn = self.write_txn()?;
        let existed = {
            let mut vhosts = txn.open_table(VHOSTS)?;
            let removed = vhosts.remove(name)?;
            removed.is_some()
        };
        if existed {
            Self::delete_bindings_for_vhost(&txn, name)?;
            Self::delete_exchanges_for_vhost(&txn, name)?;
            Self::delete_queues_for_vhost(&txn, name)?;
            Self::delete_permissions_for_vhost(&txn, name)?;
        }
        txn.commit()?;
        if existed {
            debug!(vhost = name, "deleted vhost");
        }
        Ok(existed)
    }

    // ── Users ───────────────────────────────────────────────────────────

    /// Create a user. Fails if a user with the same name exists.
    pub fn create_user(&self, user: &User) -> Result<()> {
        let txn = self.write_txn()?;
        {
            let mut users = txn.open_table(USERS)?;
            let exists = users.get(user.name.as_str())?.is_some();
            if exists {
                return Err(StoreError::UserExists(user.name.to_string()));
            }
            let bytes = serde_json::to_vec(user)?;
            users.insert(user.name.as_str(), bytes.as_slice())?;
        }
        txn.commit()?;
        debug!(user = %user.name, "created user");
        Ok(())
    }

    /// Atomically create a user and an initial permission row in one write txn.
    ///
    /// Used by admin bootstrap so a crash cannot leave an administrator without
    /// permissions. Fails if the user already exists or the permission's vhost
    /// is missing. `permission.user` must equal `user.name`.
    pub fn create_user_with_permission(&self, user: &User, permission: &Permission) -> Result<()> {
        if permission.user.as_str() != user.name.as_str() {
            return Err(StoreError::UserNotFound(permission.user.to_string()));
        }
        let txn = self.write_txn()?;
        {
            let mut users = txn.open_table(USERS)?;
            let exists = users.get(user.name.as_str())?.is_some();
            if exists {
                return Err(StoreError::UserExists(user.name.to_string()));
            }
            let bytes = serde_json::to_vec(user)?;
            users.insert(user.name.as_str(), bytes.as_slice())?;
        }
        {
            let vhosts = txn.open_table(VHOSTS)?;
            if vhosts.get(permission.vhost.as_str())?.is_none() {
                return Err(StoreError::VhostNotFound(permission.vhost.to_string()));
            }
        }
        {
            let mut perms = txn.open_table(PERMISSIONS)?;
            let key = (permission.user.as_str(), permission.vhost.as_str());
            let bytes = serde_json::to_vec(permission)?;
            perms.insert(key, bytes.as_slice())?;
        }
        txn.commit()?;
        debug!(
            user = %user.name,
            vhost = %permission.vhost,
            "created user with permission (atomic)"
        );
        Ok(())
    }

    /// Upsert a user (create or replace).
    pub fn put_user(&self, user: &User) -> Result<()> {
        let txn = self.write_txn()?;
        {
            let mut users = txn.open_table(USERS)?;
            let bytes = serde_json::to_vec(user)?;
            users.insert(user.name.as_str(), bytes.as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Fetch a user by name.
    pub fn get_user(&self, name: &str) -> Result<Option<User>> {
        let txn = self.read_txn()?;
        let table = txn.open_table(USERS)?;
        match table.get(name)? {
            Some(guard) => Ok(Some(serde_json::from_slice(guard.value())?)),
            None => Ok(None),
        }
    }

    /// List all users, ordered by name (redb key order).
    pub fn list_users(&self) -> Result<Vec<User>> {
        let txn = self.read_txn()?;
        let table = txn.open_table(USERS)?;
        let mut out = Vec::new();
        for item in table.iter()? {
            let (_, value) = item?;
            out.push(serde_json::from_slice(value.value())?);
        }
        Ok(out)
    }

    /// Delete a user and all of their permissions.
    ///
    /// Returns `true` if the user existed and was removed.
    pub fn delete_user(&self, name: &str) -> Result<bool> {
        let txn = self.write_txn()?;
        let existed = {
            let mut users = txn.open_table(USERS)?;
            let removed = users.remove(name)?;
            removed.is_some()
        };
        if existed {
            Self::delete_permissions_for_user(&txn, name)?;
        }
        txn.commit()?;
        if existed {
            debug!(user = name, "deleted user");
        }
        Ok(existed)
    }

    /// Number of users currently stored.
    pub fn user_count(&self) -> Result<usize> {
        let txn = self.read_txn()?;
        let table = txn.open_table(USERS)?;
        // redb Table does not expose len(); iterate.
        let mut n = 0usize;
        for item in table.iter()? {
            let _ = item?;
            n += 1;
        }
        Ok(n)
    }

    // ── Permissions ─────────────────────────────────────────────────────

    /// Set (create or replace) permissions for `(user, vhost)`.
    ///
    /// The user and vhost must already exist.
    pub fn put_permission(&self, permission: &Permission) -> Result<()> {
        let txn = self.write_txn()?;
        {
            let users = txn.open_table(USERS)?;
            if users.get(permission.user.as_str())?.is_none() {
                return Err(StoreError::UserNotFound(permission.user.to_string()));
            }
        }
        {
            let vhosts = txn.open_table(VHOSTS)?;
            if vhosts.get(permission.vhost.as_str())?.is_none() {
                return Err(StoreError::VhostNotFound(permission.vhost.to_string()));
            }
        }
        {
            let mut perms = txn.open_table(PERMISSIONS)?;
            let key = (permission.user.as_str(), permission.vhost.as_str());
            let bytes = serde_json::to_vec(permission)?;
            perms.insert(key, bytes.as_slice())?;
        }
        txn.commit()?;
        debug!(
            user = %permission.user,
            vhost = %permission.vhost,
            "set permissions"
        );
        Ok(())
    }

    /// Fetch permissions for `(user, vhost)`.
    pub fn get_permission(&self, user: &str, vhost: &str) -> Result<Option<Permission>> {
        let txn = self.read_txn()?;
        let table = txn.open_table(PERMISSIONS)?;
        match table.get((user, vhost))? {
            Some(guard) => Ok(Some(serde_json::from_slice(guard.value())?)),
            None => Ok(None),
        }
    }

    /// List all permission entries.
    pub fn list_permissions(&self) -> Result<Vec<Permission>> {
        let txn = self.read_txn()?;
        let table = txn.open_table(PERMISSIONS)?;
        let mut out = Vec::new();
        for item in table.iter()? {
            let (_, value) = item?;
            out.push(serde_json::from_slice(value.value())?);
        }
        Ok(out)
    }

    /// List permissions for a single user.
    pub fn list_permissions_for_user(&self, user: &str) -> Result<Vec<Permission>> {
        let txn = self.read_txn()?;
        let table = txn.open_table(PERMISSIONS)?;
        let mut out = Vec::new();
        for item in table.range((user, "")..)? {
            let (key, value) = item?;
            let (u, _) = key.value();
            if u != user {
                break;
            }
            out.push(serde_json::from_slice(value.value())?);
        }
        Ok(out)
    }

    /// Delete permissions for `(user, vhost)`.
    ///
    /// Returns `true` if an entry existed and was removed.
    pub fn delete_permission(&self, user: &str, vhost: &str) -> Result<bool> {
        let txn = self.write_txn()?;
        let removed = {
            let mut perms = txn.open_table(PERMISSIONS)?;
            let guard = perms.remove((user, vhost))?;
            guard.is_some()
        };
        txn.commit()?;
        Ok(removed)
    }

    // ── Exchanges ───────────────────────────────────────────────────────

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
        }
        txn.commit()?;
        Ok(removed)
    }

    // ── Queues ──────────────────────────────────────────────────────────

    /// Create a queue. Fails if a queue with the same key exists.
    pub fn create_queue(&self, queue: &Queue) -> Result<()> {
        let txn = self.write_txn()?;
        {
            let vhosts = txn.open_table(VHOSTS)?;
            let missing = vhosts.get(queue.vhost.as_str())?.is_none();
            if missing {
                return Err(StoreError::VhostNotFound(queue.vhost.to_string()));
            }
        }
        {
            let mut queues = txn.open_table(QUEUES)?;
            let key = (queue.vhost.as_str(), queue.name.as_str());
            let exists = queues.get(key)?.is_some();
            if exists {
                return Err(StoreError::QueueExists {
                    vhost: queue.vhost.to_string(),
                    name: queue.name.to_string(),
                });
            }
            let bytes = serde_json::to_vec(queue)?;
            queues.insert(key, bytes.as_slice())?;
        }
        txn.commit()?;
        debug!(vhost = %queue.vhost, queue = %queue.name, "created queue");
        Ok(())
    }

    /// Upsert a queue (create or replace).
    pub fn put_queue(&self, queue: &Queue) -> Result<()> {
        let txn = self.write_txn()?;
        {
            let vhosts = txn.open_table(VHOSTS)?;
            let missing = vhosts.get(queue.vhost.as_str())?.is_none();
            if missing {
                return Err(StoreError::VhostNotFound(queue.vhost.to_string()));
            }
        }
        {
            let mut queues = txn.open_table(QUEUES)?;
            let key = (queue.vhost.as_str(), queue.name.as_str());
            let bytes = serde_json::to_vec(queue)?;
            queues.insert(key, bytes.as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Fetch a queue by vhost and name.
    pub fn get_queue(&self, vhost: &str, name: &str) -> Result<Option<Queue>> {
        let txn = self.read_txn()?;
        let table = txn.open_table(QUEUES)?;
        match table.get((vhost, name))? {
            Some(guard) => Ok(Some(serde_json::from_slice(guard.value())?)),
            None => Ok(None),
        }
    }

    /// List queues in a vhost.
    ///
    /// Uses a composite-key range starting at `(vhost, "")` and stops when the
    /// vhost prefix diverges.
    pub fn list_queues(&self, vhost: &str) -> Result<Vec<Queue>> {
        let txn = self.read_txn()?;
        let table = txn.open_table(QUEUES)?;
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

    /// Delete a queue and cascade its bindings. Returns `true` if it existed.
    pub fn delete_queue(&self, vhost: &str, name: &str) -> Result<bool> {
        let txn = self.write_txn()?;
        let removed = {
            let mut queues = txn.open_table(QUEUES)?;
            let guard = queues.remove((vhost, name))?;
            guard.is_some()
        };
        if removed {
            Self::delete_bindings_for_queue(&txn, vhost, name)?;
        }
        txn.commit()?;
        Ok(removed)
    }

    // ── Bindings ────────────────────────────────────────────────────────

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

    // ── Internals ───────────────────────────────────────────────────────

    fn init_schema(&self) -> Result<()> {
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
    fn write_builtin_exchanges(txn: &redb::WriteTransaction, vhost: &str) -> Result<()> {
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
    fn ensure_builtin_exchanges(txn: &redb::WriteTransaction, vhost: &str) -> Result<()> {
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
    fn delete_exchanges_for_vhost(txn: &redb::WriteTransaction, vhost: &str) -> Result<()> {
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
    fn delete_queues_for_vhost(txn: &redb::WriteTransaction, vhost: &str) -> Result<()> {
        let mut queues = txn.open_table(QUEUES)?;
        queues.retain_in((vhost, "").., |k, _| {
            let (vh, _) = k;
            vh != vhost
        })?;
        Ok(())
    }

    /// Remove all permission rows for `user`.
    fn delete_permissions_for_user(txn: &redb::WriteTransaction, user: &str) -> Result<()> {
        let mut perms = txn.open_table(PERMISSIONS)?;
        perms.retain_in((user, "").., |k, _| {
            let (u, _) = k;
            u != user
        })?;
        Ok(())
    }

    /// Remove all permission rows for `vhost` (any user).
    fn delete_permissions_for_vhost(txn: &redb::WriteTransaction, vhost: &str) -> Result<()> {
        let mut perms = txn.open_table(PERMISSIONS)?;
        perms.retain(|k, _| {
            let (_, vh) = k;
            vh != vhost
        })?;
        Ok(())
    }

    /// Copy pre-`bindings_v2` rows, preserving arguments, then drop the old table.
    fn migrate_legacy_bindings(txn: &redb::WriteTransaction) -> Result<()> {
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
    fn delete_bindings_for_vhost(txn: &redb::WriteTransaction, vhost: &str) -> Result<()> {
        let mut bindings = txn.open_table(BINDINGS)?;
        bindings.retain_in((vhost, "", "", "", "").., |k, _| {
            let (vh, _, _, _, _) = k;
            vh != vhost
        })?;
        Ok(())
    }

    /// Remove all bindings for `(vhost, exchange)`.
    fn delete_bindings_for_exchange(
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
    fn delete_bindings_for_queue(
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
fn reject_builtin_name(vhost: &str, name: &str) -> Result<()> {
    if BUILTIN_EXCHANGE_NAMES.contains(&name) {
        return Err(StoreError::BuiltinExchange {
            vhost: vhost.to_string(),
            name: name.to_string(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use queueforge_core::{ExchangeType, HeaderArg, DEFAULT_EXCHANGE_NAME};
    use tempfile::TempDir;

    fn open_tmp() -> (TempDir, MetadataStore) {
        let dir = TempDir::new().unwrap();
        let store = MetadataStore::open(dir.path()).unwrap();
        (dir, store)
    }

    #[test]
    fn bootstrap_creates_default_vhost_and_builtins() {
        let (_dir, store) = open_tmp();
        assert_eq!(store.schema_version().unwrap(), SCHEMA_VERSION_V1);

        let vhosts = store.list_vhosts().unwrap();
        assert_eq!(vhosts.len(), 1);
        assert_eq!(vhosts[0].name, DEFAULT_VHOST);

        let exchanges = store.list_exchanges(DEFAULT_VHOST).unwrap();
        assert_eq!(exchanges.len(), 4);

        let default_ex = store
            .get_exchange(DEFAULT_VHOST, DEFAULT_EXCHANGE_NAME)
            .unwrap()
            .expect("default exchange");
        assert_eq!(default_ex.kind, ExchangeType::Default);
        assert!(default_ex.durable);
        assert!(!default_ex.auto_delete);
        assert!(default_ex.internal);

        for name in ["amq.direct", "amq.fanout", "amq.topic"] {
            let ex = store
                .get_exchange(DEFAULT_VHOST, name)
                .unwrap()
                .unwrap_or_else(|| panic!("missing {name}"));
            assert!(ex.durable);
            assert!(!ex.auto_delete);
            assert!(!ex.internal);
        }

        assert_eq!(
            store
                .get_exchange(DEFAULT_VHOST, "amq.direct")
                .unwrap()
                .unwrap()
                .kind,
            ExchangeType::Direct
        );
        assert_eq!(
            store
                .get_exchange(DEFAULT_VHOST, "amq.fanout")
                .unwrap()
                .unwrap()
                .kind,
            ExchangeType::Fanout
        );
        assert_eq!(
            store
                .get_exchange(DEFAULT_VHOST, "amq.topic")
                .unwrap()
                .unwrap()
                .kind,
            ExchangeType::Topic
        );
    }

    #[test]
    fn reopen_preserves_data_and_does_not_duplicate_builtins() {
        let dir = TempDir::new().unwrap();
        {
            let store = MetadataStore::open(dir.path()).unwrap();
            store.create_vhost("app").unwrap();
            let mut q = Queue::new("app", "jobs");
            q.durable = true;
            store.create_queue(&q).unwrap();
        }
        let store = MetadataStore::open(dir.path()).unwrap();
        assert_eq!(store.list_vhosts().unwrap().len(), 2);
        assert_eq!(store.list_exchanges("app").unwrap().len(), 4);
        assert_eq!(store.list_queues("app").unwrap().len(), 1);
        let q = store.get_queue("app", "jobs").unwrap().unwrap();
        assert!(q.durable);
    }

    #[test]
    fn vhost_crud() {
        let (_dir, store) = open_tmp();

        let vh = store.create_vhost("tenant-a").unwrap();
        assert_eq!(vh.name, "tenant-a");
        assert!(store.get_vhost("tenant-a").unwrap().is_some());

        let err = store.create_vhost("tenant-a").unwrap_err();
        assert!(matches!(err, StoreError::VhostExists(_)));

        assert_eq!(store.list_exchanges("tenant-a").unwrap().len(), 4);

        assert!(store.delete_vhost("tenant-a").unwrap());
        assert!(store.get_vhost("tenant-a").unwrap().is_none());
        assert!(store.list_exchanges("tenant-a").unwrap().is_empty());
        assert!(!store.delete_vhost("tenant-a").unwrap());
    }

    #[test]
    fn exchange_crud() {
        let (_dir, store) = open_tmp();
        store.create_vhost("vh").unwrap();

        let ex = Exchange::new("vh", "orders", ExchangeType::Direct);
        store.create_exchange(&ex).unwrap();
        assert!(store.get_exchange("vh", "orders").unwrap().is_some());

        let err = store.create_exchange(&ex).unwrap_err();
        assert!(matches!(err, StoreError::ExchangeExists { .. }));

        let mut updated = ex.clone();
        updated.auto_delete = true;
        store.put_exchange(&updated).unwrap();
        assert!(
            store
                .get_exchange("vh", "orders")
                .unwrap()
                .unwrap()
                .auto_delete
        );

        assert!(store.delete_exchange("vh", "orders").unwrap());
        assert!(store.get_exchange("vh", "orders").unwrap().is_none());

        let err = store.delete_exchange("vh", "amq.direct").unwrap_err();
        assert!(matches!(err, StoreError::BuiltinExchange { .. }));

        let orphan = Exchange::new("nope", "x", ExchangeType::Fanout);
        let err = store.create_exchange(&orphan).unwrap_err();
        assert!(matches!(err, StoreError::VhostNotFound(_)));
    }

    #[test]
    fn cannot_mutate_or_delete_builtin_exchanges() {
        let (_dir, store) = open_tmp();

        let before = store
            .get_exchange(DEFAULT_VHOST, "amq.direct")
            .unwrap()
            .unwrap();

        // put_exchange must not clobber builtins
        let mut evil = before.clone();
        evil.kind = ExchangeType::Fanout;
        evil.durable = false;
        evil.internal = true;
        let err = store.put_exchange(&evil).unwrap_err();
        assert!(matches!(err, StoreError::BuiltinExchange { .. }));
        assert_eq!(
            store
                .get_exchange(DEFAULT_VHOST, "amq.direct")
                .unwrap()
                .unwrap(),
            before
        );

        // create_exchange on builtin name rejected
        let err = store
            .create_exchange(&Exchange::new(
                DEFAULT_VHOST,
                "amq.topic",
                ExchangeType::Direct,
            ))
            .unwrap_err();
        assert!(matches!(err, StoreError::BuiltinExchange { .. }));

        // default exchange "" delete rejected; value unchanged
        let default_before = store
            .get_exchange(DEFAULT_VHOST, DEFAULT_EXCHANGE_NAME)
            .unwrap()
            .unwrap();
        let err = store
            .delete_exchange(DEFAULT_VHOST, DEFAULT_EXCHANGE_NAME)
            .unwrap_err();
        assert!(matches!(err, StoreError::BuiltinExchange { .. }));
        assert_eq!(
            store
                .get_exchange(DEFAULT_VHOST, DEFAULT_EXCHANGE_NAME)
                .unwrap()
                .unwrap(),
            default_before
        );

        // put on default name also rejected
        let mut evil_default = default_before.clone();
        evil_default.internal = false;
        let err = store.put_exchange(&evil_default).unwrap_err();
        assert!(matches!(err, StoreError::BuiltinExchange { .. }));
    }

    #[test]
    fn repairs_corrupted_builtin_attributes_on_reopen() {
        let dir = TempDir::new().unwrap();
        {
            let store = MetadataStore::open(dir.path()).unwrap();
            drop(store);
            // Corrupt amq.fanout attributes via raw redb write.
            let db = Database::create(dir.path().join(METADATA_DB_FILE)).unwrap();
            let txn = db.begin_write().unwrap();
            {
                let mut exchanges = txn.open_table(EXCHANGES).unwrap();
                let mut bad = Exchange::new(DEFAULT_VHOST, "amq.fanout", ExchangeType::Direct);
                bad.durable = false;
                bad.auto_delete = true;
                let bytes = serde_json::to_vec(&bad).unwrap();
                exchanges
                    .insert((DEFAULT_VHOST, "amq.fanout"), bytes.as_slice())
                    .unwrap();
            }
            txn.commit().unwrap();
        }
        let store = MetadataStore::open(dir.path()).unwrap();
        let fixed = store
            .get_exchange(DEFAULT_VHOST, "amq.fanout")
            .unwrap()
            .unwrap();
        assert_eq!(fixed.kind, ExchangeType::Fanout);
        assert!(fixed.durable);
        assert!(!fixed.auto_delete);
        assert!(!fixed.internal);
    }

    #[test]
    fn unsupported_schema_version_fails_closed() {
        let dir = TempDir::new().unwrap();
        {
            // Bootstrap normally then bump schema version.
            let store = MetadataStore::open(dir.path()).unwrap();
            drop(store);
            let db = Database::create(dir.path().join(METADATA_DB_FILE)).unwrap();
            let txn = db.begin_write().unwrap();
            {
                let mut version = txn.open_table(SCHEMA_VERSION).unwrap();
                version.insert(SCHEMA_VERSION_KEY, 99u32).unwrap();
            }
            txn.commit().unwrap();
        }
        let err = match MetadataStore::open(dir.path()) {
            Ok(_) => panic!("expected UnsupportedSchema"),
            Err(e) => e,
        };
        assert!(matches!(err, StoreError::UnsupportedSchema(99)));
    }

    #[test]
    fn queue_crud() {
        let (_dir, store) = open_tmp();

        let mut q = Queue::new(DEFAULT_VHOST, "q1");
        q.durable = true;
        q.exclusive = false;
        q.auto_delete = false;
        store.create_queue(&q).unwrap();

        let got = store.get_queue(DEFAULT_VHOST, "q1").unwrap().unwrap();
        assert!(got.durable);

        let err = store.create_queue(&q).unwrap_err();
        assert!(matches!(err, StoreError::QueueExists { .. }));

        q.auto_delete = true;
        store.put_queue(&q).unwrap();
        assert!(
            store
                .get_queue(DEFAULT_VHOST, "q1")
                .unwrap()
                .unwrap()
                .auto_delete
        );

        let listed = store.list_queues(DEFAULT_VHOST).unwrap();
        assert_eq!(listed.len(), 1);

        assert!(store.delete_queue(DEFAULT_VHOST, "q1").unwrap());
        assert!(store.list_queues(DEFAULT_VHOST).unwrap().is_empty());
        assert!(!store.delete_queue(DEFAULT_VHOST, "q1").unwrap());
    }

    #[test]
    fn delete_vhost_cascades_queues_exchanges_and_permissions() {
        use queueforge_core::{Permission, User, UserTag};

        let (_dir, store) = open_tmp();
        store.create_vhost("gone").unwrap();
        store
            .create_exchange(&Exchange::new("gone", "ex", ExchangeType::Topic))
            .unwrap();
        store.create_queue(&Queue::new("gone", "q")).unwrap();

        // Sibling vhost data must survive cascade.
        store.create_vhost("keep").unwrap();
        store.create_queue(&Queue::new("keep", "q")).unwrap();

        let user = User::new("carol", "$argon2id$placeholder", vec![UserTag::Management]);
        store.create_user(&user).unwrap();
        store
            .put_permission(&Permission::full_access("carol", "gone"))
            .unwrap();
        store
            .put_permission(&Permission::full_access("carol", "keep"))
            .unwrap();

        assert!(store.delete_vhost("gone").unwrap());
        assert!(store.list_exchanges("gone").unwrap().is_empty());
        assert!(store.list_queues("gone").unwrap().is_empty());
        assert!(store.get_permission("carol", "gone").unwrap().is_none());
        // Sibling vhost perms + topology intact.
        assert!(store.get_permission("carol", "keep").unwrap().is_some());
        assert_eq!(store.list_exchanges("keep").unwrap().len(), 4);
        assert_eq!(store.list_queues("keep").unwrap().len(), 1);
        // Default vhost untouched.
        assert_eq!(store.list_exchanges(DEFAULT_VHOST).unwrap().len(), 4);
    }

    #[test]
    fn list_is_scoped_to_vhost_via_range() {
        let (_dir, store) = open_tmp();
        store.create_vhost("a").unwrap();
        store.create_vhost("b").unwrap();
        store
            .create_exchange(&Exchange::new("a", "only-a", ExchangeType::Direct))
            .unwrap();
        store
            .create_exchange(&Exchange::new("b", "only-b", ExchangeType::Topic))
            .unwrap();
        store.create_queue(&Queue::new("a", "qa")).unwrap();
        store.create_queue(&Queue::new("b", "qb")).unwrap();

        let a_ex = store.list_exchanges("a").unwrap();
        assert!(a_ex.iter().any(|e| e.name == "only-a"));
        assert!(a_ex.iter().all(|e| e.vhost == "a"));
        assert!(!a_ex.iter().any(|e| e.name == "only-b"));

        let b_q = store.list_queues("b").unwrap();
        assert_eq!(b_q.len(), 1);
        assert_eq!(b_q[0].name, "qb");
    }

    #[test]
    fn creates_data_dir_if_missing() {
        let dir = TempDir::new().unwrap();
        let nested = dir.path().join("a").join("b").join("data");
        assert!(!nested.exists());
        let store = MetadataStore::open(&nested).unwrap();
        assert!(nested.join(METADATA_DB_FILE).exists());
        assert_eq!(store.list_vhosts().unwrap().len(), 1);
    }

    #[test]
    fn user_and_permission_crud() {
        use queueforge_core::{Permission, User, UserTag};

        let (_dir, store) = open_tmp();
        assert_eq!(store.user_count().unwrap(), 0);

        let user = User::new("alice", "$argon2id$placeholder", vec![UserTag::Management]);
        store.create_user(&user).unwrap();
        assert!(store.get_user("alice").unwrap().is_some());
        assert_eq!(store.user_count().unwrap(), 1);

        let err = store.create_user(&user).unwrap_err();
        assert!(matches!(err, StoreError::UserExists(_)));

        let perm = Permission::new("alice", DEFAULT_VHOST, ".*", "^q\\.", ".*");
        store.put_permission(&perm).unwrap();
        let got = store
            .get_permission("alice", DEFAULT_VHOST)
            .unwrap()
            .unwrap();
        assert_eq!(got.write, "^q\\.");

        // Unknown user / vhost rejected.
        let orphan = Permission::new("nobody", DEFAULT_VHOST, ".*", ".*", ".*");
        assert!(matches!(
            store.put_permission(&orphan).unwrap_err(),
            StoreError::UserNotFound(_)
        ));
        let bad_vh = Permission::new("alice", "missing", ".*", ".*", ".*");
        assert!(matches!(
            store.put_permission(&bad_vh).unwrap_err(),
            StoreError::VhostNotFound(_)
        ));

        assert!(store.delete_permission("alice", DEFAULT_VHOST).unwrap());
        assert!(store
            .get_permission("alice", DEFAULT_VHOST)
            .unwrap()
            .is_none());

        // Re-add permission then delete user cascades.
        store.put_permission(&perm).unwrap();
        assert!(store.delete_user("alice").unwrap());
        assert!(store.get_user("alice").unwrap().is_none());
        assert!(store
            .get_permission("alice", DEFAULT_VHOST)
            .unwrap()
            .is_none());
    }

    #[test]
    fn create_user_with_permission_is_atomic() {
        use queueforge_core::{Permission, User, UserTag};

        let (_dir, store) = open_tmp();
        let user = User::new(
            "boot",
            "$argon2id$placeholder",
            vec![UserTag::Administrator],
        );
        let perm = Permission::full_access("boot", DEFAULT_VHOST);
        store.create_user_with_permission(&user, &perm).unwrap();
        assert!(store.get_user("boot").unwrap().is_some());
        assert_eq!(
            store
                .get_permission("boot", DEFAULT_VHOST)
                .unwrap()
                .unwrap()
                .configure,
            ".*"
        );

        // Missing vhost rolls back (no orphan user).
        let user2 = User::new("orphan", "$argon2id$placeholder", vec![]);
        let bad = Permission::full_access("orphan", "no-such-vhost");
        let err = store.create_user_with_permission(&user2, &bad).unwrap_err();
        assert!(matches!(err, StoreError::VhostNotFound(_)));
        assert!(store.get_user("orphan").unwrap().is_none());
    }

    #[test]
    fn restores_missing_builtins_on_reopen() {
        let dir = TempDir::new().unwrap();
        {
            let store = MetadataStore::open(dir.path()).unwrap();
            drop(store);
            let db = Database::create(dir.path().join(METADATA_DB_FILE)).unwrap();
            let txn = db.begin_write().unwrap();
            {
                let mut exchanges = txn.open_table(EXCHANGES).unwrap();
                exchanges.remove((DEFAULT_VHOST, "amq.topic")).unwrap();
            }
            txn.commit().unwrap();
        }
        let store = MetadataStore::open(dir.path()).unwrap();
        let ex = store
            .get_exchange(DEFAULT_VHOST, "amq.topic")
            .unwrap()
            .expect("builtin restored");
        assert_eq!(ex.kind, ExchangeType::Topic);
    }

    #[test]
    fn same_routing_key_headers_bindings_stay_distinct_across_reopen() {
        use compact_str::CompactString;

        let dir = TempDir::new().unwrap();
        let color_args = vec![(CompactString::from("color"), HeaderArg::Str("blue".into()))];
        let size_args = vec![(CompactString::from("size"), HeaderArg::Str("l".into()))];
        {
            let store = MetadataStore::open(dir.path()).unwrap();
            store
                .create_exchange(&Exchange::new("/", "hdr-dur", ExchangeType::Headers))
                .unwrap();
            let mut queue = Queue::new("/", "q2");
            queue.durable = true;
            store.create_queue(&queue).unwrap();

            let mut color = Binding::new("/", "hdr-dur", "q2", "");
            color.args = color_args.clone();
            let mut size = Binding::new("/", "hdr-dur", "q2", "");
            size.args = size_args.clone();
            store.put_binding(&color).unwrap();
            store.put_binding(&size).unwrap();

            let stored = store.list_bindings_for_exchange("/", "hdr-dur").unwrap();
            assert_eq!(stored.len(), 2, "args must keep sibling rows apart");

            let color_key = binding_args_key(&color.args);
            assert!(store
                .delete_binding("/", "hdr-dur", "q2", "", &color_key)
                .unwrap());
        }

        let store = MetadataStore::open(dir.path()).unwrap();
        let left = store.list_bindings_for_exchange("/", "hdr-dur").unwrap();
        assert_eq!(left.len(), 1);
        assert_eq!(left[0].args, size_args);
        let router = store.bootstrap_router().unwrap();
        let size_hit = router
            .route_publish(
                "/",
                "hdr-dur",
                "",
                &[(CompactString::from("size"), HeaderArg::Str("l".into()))],
            )
            .unwrap();
        assert_eq!(size_hit.destinations.len(), 1);
        let color_hit = router
            .route_publish(
                "/",
                "hdr-dur",
                "",
                &[(CompactString::from("color"), HeaderArg::Str("blue".into()))],
            )
            .unwrap();
        assert!(color_hit.destinations.is_empty());
    }

    #[test]
    fn legacy_bindings_table_is_copied_on_reopen() {
        use compact_str::CompactString;
        use redb::{TableDefinition, TableHandle};

        let dir = TempDir::new().unwrap();
        {
            let store = MetadataStore::open(dir.path()).unwrap();
            store
                .create_exchange(&Exchange::new("/", "hdr-dur", ExchangeType::Headers))
                .unwrap();
            let mut queue = Queue::new("/", "q2");
            queue.durable = true;
            store.create_queue(&queue).unwrap();
            drop(store);

            let db = Database::create(dir.path().join(METADATA_DB_FILE)).unwrap();
            let txn = db.begin_write().unwrap();
            {
                let mut binding = Binding::new("/", "hdr-dur", "q2", "");
                binding.args = vec![(CompactString::from("color"), HeaderArg::Str("blue".into()))];
                let bytes = serde_json::to_vec(&binding).unwrap();
                let mut old = txn
                    .open_table(TableDefinition::<(&str, &str, &str, &str), &[u8]>::new(
                        "bindings",
                    ))
                    .unwrap();
                old.insert(("/", "hdr-dur", "q2", ""), bytes.as_slice())
                    .unwrap();
            }
            txn.commit().unwrap();
        }

        let store = MetadataStore::open(dir.path()).unwrap();
        let router = store.bootstrap_router().unwrap();
        let hit = router
            .route_publish(
                "/",
                "hdr-dur",
                "",
                &[(CompactString::from("color"), HeaderArg::Str("blue".into()))],
            )
            .unwrap();
        assert_eq!(hit.destinations.len(), 1);
        assert!(
            !store
                .db
                .begin_read()
                .unwrap()
                .list_tables()
                .unwrap()
                .any(|table| table.name() == "bindings"),
            "legacy table must be consumed"
        );
    }
}
