//! [`queueforge_core::QueueMetaStore`] implementation for [`MetadataStore`].

use queueforge_core::{Error as CoreError, Queue, QueueMetaStore, Result as CoreResult};

use crate::error::StoreError;
use crate::metadata::MetadataStore;

impl QueueMetaStore for MetadataStore {
    fn create_queue(&self, queue: &Queue) -> CoreResult<()> {
        MetadataStore::create_queue(self, queue).map_err(store_to_core)
    }

    fn put_queue(&self, queue: &Queue) -> CoreResult<()> {
        MetadataStore::put_queue(self, queue).map_err(store_to_core)
    }

    fn get_queue(&self, vhost: &str, name: &str) -> CoreResult<Option<Queue>> {
        MetadataStore::get_queue(self, vhost, name).map_err(store_to_core)
    }

    fn delete_queue(&self, vhost: &str, name: &str) -> CoreResult<bool> {
        MetadataStore::delete_queue(self, vhost, name).map_err(store_to_core)
    }
}

fn store_to_core(err: StoreError) -> CoreError {
    match err {
        StoreError::QueueExists { vhost, name } => {
            CoreError::AlreadyExists(format!("queue {vhost}/{name}"))
        }
        StoreError::QueueNotFound { vhost, name } => {
            CoreError::NotFound(format!("queue {vhost}/{name}"))
        }
        StoreError::VhostNotFound(v) => CoreError::NotFound(format!("vhost {v}")),
        other => CoreError::Store(other.to_string()),
    }
}
