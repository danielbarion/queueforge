//! Persistence layer for QueueForge.
//!
//! Provides a redb-backed [`MetadataStore`] for vhosts, users, permissions,
//! exchanges, queues, and bindings (schema v1), plus a segmented message
//! [`wal`] with group-commit recovery.

#![deny(missing_docs)]

/// Store error types.
pub mod error;
/// Metadata store (vhosts / exchanges / queues).
pub mod metadata;
/// [`queueforge_core::QueueMetaStore`] impl for [`MetadataStore`].
pub mod queue_meta;
/// Durable queue recovery algorithm.
pub mod recovery;
/// redb table definitions.
pub mod tables;
/// Segmented write-ahead log for durable messages.
pub mod wal;

pub use error::{Result, StoreError};
pub use metadata::{
    binding_args_key, store_scheduler_stats, track_store_scheduler, MetadataStore,
    StoreSchedulerStats, METADATA_DB_FILE,
};
pub use recovery::{recover_durable_queues, RecoveryConfig, RecoveryReport, WalFactory};
pub use tables::SCHEMA_VERSION_V1;
pub use wal::{open_queue_wal, queue_dir, QueueWal, DEFAULT_SEGMENT_MAX_BYTES};
