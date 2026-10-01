//! Registry lookup: handles by key, exclusive owners, and auto-delete queues.

use std::sync::Arc;

use tokio::sync::mpsc;

use super::super::cmd::QueueCmd;
use super::{QueueDeclareOpts, QueueEntry, QueueHandle, QueueInfo, QueueKey, QueueRegistry};

impl QueueRegistry {
    /// Register a handle whose actor is not spawned by this registry.
    ///
    /// Used for a cluster proxy mailbox. Returns `false` when a handle already
    /// exists for the same queue.
    pub fn insert_external(&self, handle: QueueHandle) -> bool {
        let key = handle.info.key.clone();
        let mut guard = self.entries.write().expect("queue registry lock poisoned");
        if guard.contains_key(&key) {
            return false;
        }
        guard.insert(key, QueueEntry { handle });
        true
    }

    /// Build a proxy mailbox for a queue homed on a peer.
    ///
    /// Returns the command receiver the proxy task must drain. `None` when a
    /// handle is already registered.
    pub fn begin_proxy(
        &self,
        key: QueueKey,
        opts: QueueDeclareOpts,
        capacity: usize,
    ) -> Option<(QueueHandle, mpsc::Receiver<QueueCmd>)> {
        if self.get(&key).is_some() {
            return None;
        }
        let info = Arc::new(QueueInfo::new(key, &opts));
        let (tx, rx) = mpsc::channel(capacity.max(1));
        let handle = QueueHandle {
            tx,
            info: Arc::clone(&info),
        };
        if !self.insert_external(handle.clone()) {
            return None;
        }
        Some((handle, rx))
    }

    /// Lookup a live queue handle.
    pub fn get(&self, key: &QueueKey) -> Option<QueueHandle> {
        let guard = self.entries.read().expect("queue registry lock poisoned");
        guard.get(key).map(|e| e.handle.clone())
    }

    /// Number of queues currently registered (including unavailable).
    pub fn len(&self) -> usize {
        self.entries
            .read()
            .expect("queue registry lock poisoned")
            .len()
    }

    /// Whether the registry has no queues.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// List keys currently registered.
    pub fn list_keys(&self) -> Vec<QueueKey> {
        self.entries
            .read()
            .expect("queue registry lock poisoned")
            .keys()
            .cloned()
            .collect()
    }

    /// Queues owned exclusively by `owner` (connection id / peer string).
    pub fn list_exclusive_owned_by(&self, owner: &str) -> Vec<QueueHandle> {
        self.entries
            .read()
            .expect("queue registry lock poisoned")
            .values()
            .filter(|e| {
                e.handle.info.exclusive
                    && e.handle
                        .info
                        .exclusive_owner
                        .as_ref()
                        .map(|o| o.as_str() == owner)
                        .unwrap_or(false)
            })
            .map(|e| e.handle.clone())
            .collect()
    }

    /// Queues in `vhost` with `auto_delete` set.
    pub fn list_auto_delete_in_vhost(&self, vhost: &str) -> Vec<QueueHandle> {
        self.entries
            .read()
            .expect("queue registry lock poisoned")
            .values()
            .filter(|e| e.handle.info.auto_delete && e.handle.info.key.vhost.as_str() == vhost)
            .map(|e| e.handle.clone())
            .collect()
    }
}
