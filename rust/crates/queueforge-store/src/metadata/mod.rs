//! redb-backed metadata store for vhosts, users, permissions, exchanges, queues,
//! and bindings.
//! Policy, access, exchange, queue, binding, and schema helpers live in sibling modules.

use std::cell::Cell;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use queueforge_core::{Binding, Exchange, Queue, DEFAULT_VHOST};
use redb::Database;
use tracing::info;

use crate::error::{Result, StoreError};
use crate::tables::{EXCHANGES, SCHEMA_VERSION, SCHEMA_VERSION_KEY, SCHEMA_VERSION_V1};

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

    // ── Users ───────────────────────────────────────────────────────────

    // ── Permissions ─────────────────────────────────────────────────────

    // ── Exchanges ───────────────────────────────────────────────────────

    // ── Queues ──────────────────────────────────────────────────────────

    // ── Bindings ────────────────────────────────────────────────────────

    // ── Internals ───────────────────────────────────────────────────────
}

mod access;
mod binding;
mod e2e;
mod parameters;
mod exchange;
mod policy;
mod queue;
mod schema;

#[cfg(test)]
mod tests;
