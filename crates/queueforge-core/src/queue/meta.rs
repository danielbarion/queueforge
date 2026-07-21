//! Metadata persistence port used by [`super::QueueRegistry`].
//!
//! Implemented for `queueforge_store::MetadataStore` in that crate so
//! `queueforge-core` does not depend on the store (avoids a dependency cycle).

use crate::domain::Queue;
use crate::error::Result;

/// Persist / load queue definitions (durable definitions survive restart).
pub trait QueueMetaStore: Send + Sync {
    /// Insert a queue definition. Fails if it already exists.
    fn create_queue(&self, queue: &Queue) -> Result<()>;

    /// Upsert a queue definition.
    fn put_queue(&self, queue: &Queue) -> Result<()>;

    /// Fetch a queue by vhost + name.
    fn get_queue(&self, vhost: &str, name: &str) -> Result<Option<Queue>>;

    /// Delete a queue definition. Returns whether it existed.
    fn delete_queue(&self, vhost: &str, name: &str) -> Result<bool>;
}

/// No-op metadata store (unit tests / pure in-memory mode).
#[derive(Debug, Default)]
pub struct NoopMetaStore;

impl QueueMetaStore for NoopMetaStore {
    fn create_queue(&self, _queue: &Queue) -> Result<()> {
        Ok(())
    }

    fn put_queue(&self, _queue: &Queue) -> Result<()> {
        Ok(())
    }

    fn get_queue(&self, _vhost: &str, _name: &str) -> Result<Option<Queue>> {
        Ok(None)
    }

    fn delete_queue(&self, _vhost: &str, _name: &str) -> Result<bool> {
        Ok(false)
    }
}
