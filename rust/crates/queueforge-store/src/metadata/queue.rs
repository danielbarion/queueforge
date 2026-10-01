//! Queue rows.

use queueforge_core::Queue;
use redb::ReadableTable;
use tracing::debug;

use super::MetadataStore;
use crate::error::{Result, StoreError};
use crate::tables::{QUEUES, VHOSTS};

impl MetadataStore {
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
}
