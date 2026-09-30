//! Broker recovery: exclusive purge, WAL replay, spawn durable actors.
//!
//! Normative steps from DESIGN-queueforge.md recovery algorithm:
//! 1. Metadata already open (caller).
//! 2. Process state Recovering → `/readyz` 503 (caller sets ReadyFlag).
//! 3. Drop exclusive durable queues (definition + WAL).
//! 4. For each remaining durable queue: replay WAL → ready + redelivered.
//! 5. Spawn actors (via [`QueueRegistry::restore_recovered`]).
//! 6. Implicit default-exchange bindings are routing-key lookups (no-op index).
//! 7. User bindings already loaded via [`MetadataStore::bootstrap_router`].
//! 8. Ready (caller).

use std::collections::VecDeque;
use std::sync::Arc;

use queueforge_core::{
    DurabilityPolicy, Queue, QueueActorBootstrap, QueueMessage, QueueOffset, QueueRegistry, QueueType,
};
use tracing::{error, info, warn};

use crate::error::{Result, StoreError};
use crate::metadata::MetadataStore;
use crate::wal::{open_queue_wal, QueueWal, DEFAULT_SEGMENT_MAX_BYTES};

/// Configuration for recovery / WAL open.
#[derive(Debug, Clone)]
pub struct RecoveryConfig {
    /// Max segment size (bytes).
    pub wal_segment_max_bytes: u64,
    /// Group-commit policy for restored actors.
    pub durability_policy: DurabilityPolicy,
    /// When set, durable queues homed on another node are not spawned here.
    pub local_node: Option<String>,
}

impl Default for RecoveryConfig {
    fn default() -> Self {
        Self {
            wal_segment_max_bytes: DEFAULT_SEGMENT_MAX_BYTES,
            durability_policy: DurabilityPolicy::default(),
            local_node: None,
        }
    }
}

/// Summary counters from a recovery pass.
#[derive(Debug, Clone, Default)]
pub struct RecoveryReport {
    /// Exclusive durable queues purged.
    pub exclusive_purged: u64,
    /// Durable queues successfully restored.
    pub queues_restored: u64,
    /// Queues marked corrupt and skipped (not spawned).
    pub queues_corrupt: u64,
    /// Total messages rebuilt into ready.
    pub messages_recovered: u64,
}

/// Run the durable recovery algorithm and populate `registry`.
///
/// Caller must leave `/readyz` at 503 until this returns successfully.
pub async fn recover_durable_queues(
    store: &MetadataStore,
    registry: &QueueRegistry,
    cfg: &RecoveryConfig,
) -> Result<RecoveryReport> {
    let mut report = RecoveryReport::default();
    let data_dir = store.data_dir();

    // Gather all queue definitions across vhosts.
    let mut all_queues: Vec<Queue> = Vec::new();
    for vh in store.list_vhosts()? {
        all_queues.extend(store.list_queues(vh.name.as_str())?);
    }

    // Step 3: drop exclusive durable queues (no live owner after restart).
    let mut remaining = Vec::new();
    for q in all_queues {
        if q.exclusive {
            info!(
                vhost = %q.vhost,
                queue = %q.name,
                "purging exclusive durable queue on recovery"
            );
            let _ = store.delete_queue(q.vhost.as_str(), q.name.as_str())?;
            if let Err(e) = QueueWal::delete_dir(data_dir, q.vhost.as_str(), q.name.as_str()) {
                warn!(
                    vhost = %q.vhost,
                    queue = %q.name,
                    error = %e,
                    "failed to delete exclusive queue WAL dir"
                );
            }
            report.exclusive_purged = report.exclusive_purged.saturating_add(1);
            metrics::counter!("queueforge_exclusive_queues_purged_on_recovery_total").increment(1);
            continue;
        }
        if q.durable {
            let quorum = q.args.queue_type == Some(QueueType::Quorum);
            if !quorum {
            if let (Some(local), Some(home)) = (cfg.local_node.as_deref(), q.home.as_deref()) {
                if home != local {
                    info!(
                        vhost = %q.vhost,
                        queue = %q.name,
                        home,
                        "skipping durable queue homed on a peer"
                    );
                    continue;
                }
            }
            }
            remaining.push(q);
        }
        // Transient definitions should not be in redb; ignore if present.
    }

    // Step 4–5: replay WAL and spawn actors.
    for q in remaining {
        match recover_one_queue(store, registry, &q, cfg).await {
            Ok(n) => {
                report.queues_restored = report.queues_restored.saturating_add(1);
                report.messages_recovered = report.messages_recovered.saturating_add(n);
            }
            Err(StoreError::WalCorrupt { path, reason }) => {
                error!(
                    vhost = %q.vhost,
                    queue = %q.name,
                    %path,
                    %reason,
                    "queue WAL corrupt — not spawning actor (operator restore required)"
                );
                report.queues_corrupt = report.queues_corrupt.saturating_add(1);
                metrics::counter!(
                    "queueforge_wal_corrupt_queues_total",
                    "vhost" => q.vhost.to_string(),
                    "queue" => q.name.to_string()
                )
                .increment(1);
                // Leave definition in meta but do not spawn — unavailable until delete/recreate.
            }
            Err(e) => {
                error!(
                    vhost = %q.vhost,
                    queue = %q.name,
                    error = %e,
                    "queue recovery failed"
                );
                return Err(e);
            }
        }
    }

    info!(
        exclusive_purged = report.exclusive_purged,
        queues_restored = report.queues_restored,
        queues_corrupt = report.queues_corrupt,
        messages_recovered = report.messages_recovered,
        "durable recovery complete"
    );
    Ok(report)
}

async fn recover_one_queue(
    store: &MetadataStore,
    registry: &QueueRegistry,
    q: &Queue,
    cfg: &RecoveryConfig,
) -> Result<u64> {
    let data_dir = store.data_dir();
    let mut wal = open_queue_wal(
        data_dir,
        q.vhost.as_str(),
        q.name.as_str(),
        cfg.wal_segment_max_bytes,
    )?;

    let recovered = wal.recover_messages()?;
    let mut ready: VecDeque<QueueMessage> = VecDeque::with_capacity(recovered.messages.len());
    for (offset, mut msg) in recovered.messages {
        // Invariant: after crash, no live consumers → every non-acked message
        // is ready with redelivered=true.
        msg.redelivered = true;
        ready.push_back(QueueMessage {
            offset: QueueOffset(offset),
            message: Arc::new(msg),
            expires_at: None,
        });
    }
    let n = ready.len() as u64;

    let bootstrap = QueueActorBootstrap::new_empty(true)
        .with_recovered(ready, recovered.next_offset)
        .with_log(Box::new(wal), cfg.durability_policy);

    registry
        .restore_recovered(q, bootstrap)
        .await
        .map_err(|e| StoreError::Database(e.to_string()))?;

    Ok(n)
}

/// Convenience: open factory for runtime durable declares after recovery.
pub use crate::wal::WalFactory;

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use bytes::Bytes;
    use compact_str::CompactString;
    use queueforge_core::{
        DurabilityPolicy, DurableQueueLog, FsyncPolicy, MemoryTracker, Message, Queue,
        QueueMetaStore, QueueOffset, QueueRegistry,
    };
    use tempfile::TempDir;

    use crate::metadata::MetadataStore;
    use crate::wal::{open_queue_wal, queue_dir};

    fn sample(body: &[u8]) -> Message {
        Message {
            exchange: CompactString::from(""),
            routing_key: CompactString::from("q"),
            body: Bytes::copy_from_slice(body),
            persistent: true,
            redelivered: false,
            content_type: None,
            content_encoding: None,
            correlation_id: None,
            message_id: None,
            reply_to: None,
            expiration: None,
            app_id: None,
            user_id: None,
            type_: None,
            priority: None,
            timestamp: None,
            expires_unix_ms: None,
            headers: Default::default(),
        }
    }

    #[tokio::test]
    async fn exclusive_durable_purged_on_recovery() {
        let dir = TempDir::new().unwrap();
        let store = MetadataStore::open(dir.path()).unwrap();
        let q = Queue {
            vhost: CompactString::from("/"),
            name: CompactString::from("ex-only"),
            durable: true,
            exclusive: true,
            auto_delete: false,
            args: Default::default(),
            home: None,
        };
        store.create_queue(&q).unwrap();
        {
            let mut wal = open_queue_wal(dir.path(), "/", "ex-only", 1024 * 1024).unwrap();
            DurableQueueLog::append_enqueue(&mut wal, QueueOffset(1), &sample(b"gone")).unwrap();
            DurableQueueLog::fsync(&mut wal).unwrap();
        }
        assert!(queue_dir(dir.path(), "/", "ex-only").exists());

        let store = Arc::new(store);
        let policy = DurabilityPolicy::from_parts(FsyncPolicy::Always, 100, 1);
        let factory = Arc::new(WalFactory::new(store.data_dir(), 4 * 1024 * 1024));
        let reg = QueueRegistry::shared_with_durability(
            Arc::clone(&store) as Arc<dyn QueueMetaStore>,
            MemoryTracker::shared(),
            factory,
            policy,
        );
        let report = recover_durable_queues(
            store.as_ref(),
            reg.as_ref(),
            &RecoveryConfig {
                wal_segment_max_bytes: 4 * 1024 * 1024,
                durability_policy: policy,
                local_node: None,
            },
        )
        .await
        .unwrap();

        assert_eq!(report.exclusive_purged, 1);
        assert_eq!(report.queues_restored, 0);
        assert!(store.get_queue("/", "ex-only").unwrap().is_none());
        assert!(
            !queue_dir(dir.path(), "/", "ex-only").exists(),
            "WAL dir for exclusive queue must be deleted"
        );
        assert!(reg
            .get(&queueforge_core::QueueKey::new("/", "ex-only"))
            .is_none());
    }

    #[tokio::test]
    async fn non_exclusive_durable_restored_with_message() {
        let dir = TempDir::new().unwrap();
        let store = MetadataStore::open(dir.path()).unwrap();
        let q = Queue {
            vhost: CompactString::from("/"),
            name: CompactString::from("keep"),
            durable: true,
            exclusive: false,
            auto_delete: false,
            args: Default::default(),
            home: None,
        };
        store.create_queue(&q).unwrap();
        {
            let mut wal = open_queue_wal(dir.path(), "/", "keep", 1024 * 1024).unwrap();
            DurableQueueLog::append_enqueue(&mut wal, QueueOffset(1), &sample(b"stay")).unwrap();
            DurableQueueLog::fsync(&mut wal).unwrap();
        }

        let store = Arc::new(store);
        let policy = DurabilityPolicy::from_parts(FsyncPolicy::Always, 100, 1);
        let factory = Arc::new(WalFactory::new(store.data_dir(), 4 * 1024 * 1024));
        let reg = QueueRegistry::shared_with_durability(
            Arc::clone(&store) as Arc<dyn QueueMetaStore>,
            MemoryTracker::shared(),
            factory,
            policy,
        );
        let report = recover_durable_queues(
            store.as_ref(),
            reg.as_ref(),
            &RecoveryConfig {
                wal_segment_max_bytes: 4 * 1024 * 1024,
                durability_policy: policy,
                local_node: None,
            },
        )
        .await
        .unwrap();

        assert_eq!(report.queues_restored, 1);
        assert_eq!(report.messages_recovered, 1);
        assert!(reg
            .get(&queueforge_core::QueueKey::new("/", "keep"))
            .is_some());
    }
}
