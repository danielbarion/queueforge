//! Disk free-space budget for durable publishes.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tracing::warn;

use crate::sysinfo::disk_free_bytes;

/// Shared disk free-space budget backed by `statvfs` on the data directory.
///
/// Probe failures are **fail-closed** for durable publishes: free space is
/// treated as 0 so publishers are blocked rather than risking disk full.
/// The queue actor reads the cache. A blocking thread refreshes it.
#[derive(Debug)]
pub struct DiskBudget {
    data_dir: PathBuf,
    /// Minimum free bytes required to accept durable publishes (`0` = disabled).
    limit_bytes: u64,
    last_free_bytes: AtomicU64,
}

impl DiskBudget {
    /// Create a budget for `data_dir` with the given free-space floor.
    pub fn new(data_dir: impl Into<PathBuf>, limit_bytes: u64) -> Self {
        let data_dir = data_dir.into();
        let initial = disk_free_bytes(&data_dir).unwrap_or(0);
        Self {
            data_dir,
            limit_bytes,
            last_free_bytes: AtomicU64::new(initial),
        }
    }

    /// Shared handle.
    pub fn shared(data_dir: impl Into<PathBuf>, limit_bytes: u64) -> Arc<Self> {
        Arc::new(Self::new(data_dir, limit_bytes))
    }

    /// Data directory being monitored.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Configured free-space floor.
    pub fn limit_bytes(&self) -> u64 {
        self.limit_bytes
    }

    /// Last observed free bytes (updated by [`Self::refresh`] / publish checks).
    pub fn last_free_bytes(&self) -> u64 {
        self.last_free_bytes.load(Ordering::Relaxed)
    }

    /// Probe free space; on error treat as **0** (fail closed), update gauge/cache.
    pub fn refresh(&self) -> u64 {
        let free = match disk_free_bytes(&self.data_dir) {
            Ok(n) => n,
            Err(e) => {
                warn!(
                    path = %self.data_dir.display(),
                    error = %e,
                    "failed to probe disk free space; treating as 0 (fail closed)"
                );
                0
            }
        };
        self.last_free_bytes.store(free, Ordering::Relaxed);
        metrics::gauge!("queueforge_disk_free_bytes").set(free as f64);
        crate::prom::disk_space_available(free);
        crate::prom::disk_alarm(self.limit_bytes > 0 && free < self.limit_bytes);
        free
    }

    /// Whether a durable (WAL) publish is allowed under the free-space limit.
    ///
    /// `limit_bytes == 0` disables the check. This reads the cached sample only.
    /// A publish must not call `statvfs`: the broker refreshes the cache on a
    /// blocking thread. A stale cache fails the same way as a failed probe once
    /// the background refresh stores 0.
    pub fn allows_durable_write(&self) -> bool {
        if self.limit_bytes == 0 {
            return true;
        }
        self.last_free_bytes.load(Ordering::Relaxed) >= self.limit_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_limit_always_allows() {
        let b = DiskBudget::new(".", 0);
        assert!(b.allows_durable_write());
    }

    #[test]
    fn huge_limit_blocks() {
        let b = DiskBudget::new(".", u64::MAX);
        assert!(!b.allows_durable_write());
    }

    #[test]
    fn refresh_updates_cache() {
        let b = DiskBudget::new(".", 1);
        let free = b.refresh();
        assert_eq!(b.last_free_bytes(), free);
        assert!(free > 0);
    }

    #[test]
    fn allows_reuses_the_cached_sample_inside_the_probe_interval() {
        let b = DiskBudget::new(".", 1);
        assert!(b.allows_durable_write());
        b.last_free_bytes.store(0, Ordering::Relaxed);
        // A live statvfs would still see free space. The cache must win.
        assert!(!b.allows_durable_write());
        b.refresh();
        assert!(b.allows_durable_write());
    }
}
