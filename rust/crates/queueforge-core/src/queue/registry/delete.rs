//! Queue delete, including if-unused and if-empty checks.

use std::sync::Arc;

use tracing::{debug, info};

use super::super::cmd::QueueStats;
use super::shutdown::{query_stats, shutdown_actor};
use super::{QueueKey, QueueRegistry};
use crate::error::{Error, Result};

impl QueueRegistry {
    /// Delete a queue: optional unused/empty checks, shutdown actor, drop metadata.
    ///
    /// Returns the number of messages that were in the queue (0 until publish).
    pub async fn delete(&self, key: &QueueKey, if_unused: bool, if_empty: bool) -> Result<u32> {
        // Serialize with create so we never delete meta for a recreated queue.
        let _lifecycle = self.lifecycle.lock().await;

        let handle = {
            let guard = self.entries.read().expect("queue registry lock poisoned");
            guard.get(key).map(|e| e.handle.clone())
        };
        let Some(handle) = handle else {
            // Best-effort metadata cleanup for durable leftovers.
            // The map lock is released before this await (std RwLockGuard is !Send).
            let vhost = key.vhost.to_string();
            let name = key.name.to_string();
            let _ = self
                .meta_blocking(move |meta| meta.delete_queue(&vhost, &name))
                .await?;
            return Err(Error::NotFound(format!("queue {key}")));
        };

        let stats = if handle.is_available() {
            query_stats(&handle.tx).await.unwrap_or_default()
        } else {
            QueueStats::default()
        };

        if if_unused && stats.consumer_count > 0 {
            return Err(Error::PreconditionFailed(format!(
                "queue {key} is in use (consumers={})",
                stats.consumer_count
            )));
        }
        if if_empty && (stats.messages_ready > 0 || stats.messages_unacked > 0) {
            return Err(Error::PreconditionFailed(format!(
                "queue {key} is not empty"
            )));
        }

        // Remove only our handle; concurrent recreate under lifecycle is impossible
        // while we hold the lock, but ptr_eq still guards against logic bugs.
        let removed = {
            let mut guard = self.entries.write().expect("queue registry lock poisoned");
            match guard.get(key) {
                Some(entry) if Arc::ptr_eq(&entry.handle.info, &handle.info) => {
                    guard.remove(key);
                    true
                }
                Some(_) => false,
                None => false,
            }
        };

        if !removed {
            return Err(Error::NotFound(format!("queue {key}")));
        }

        metrics::gauge!("queueforge_queues").decrement(1.0);

        if handle.is_available() {
            let _ = shutdown_actor(&handle.tx).await;
        }

        // Safe: lifecycle held, map entry gone, no concurrent declare can recreate yet.
        let vhost = key.vhost.to_string();
        let name = key.name.to_string();
        let _ = self
            .meta_blocking(move |meta| meta.delete_queue(&vhost, &name))
            .await?;

        if handle.info.durable {
            if let Some(factory) = &self.durable_factory {
                if let Err(e) = factory.delete(key.vhost.as_str(), key.name.as_str()) {
                    debug!(
                        vhost = %key.vhost,
                        queue = %key.name,
                        error = %e,
                        "WAL delete after queue delete failed (best-effort)"
                    );
                }
            }
        }

        info!(vhost = %key.vhost, queue = %key.name, "queue deleted");
        Ok(stats.messages_ready)
    }
}
