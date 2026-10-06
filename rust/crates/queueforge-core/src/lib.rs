//! Core domain types and shared utilities for QueueForge.
//!
//! This crate holds protocol-agnostic models (vhosts, exchanges, queues,
//! users, permissions, bindings, messages), the per-queue actor runtime +
//! [`QueueRegistry`], and shared error types. Concrete persistence lives in
//! `queueforge-store`; the AMQP surface lives in `queueforge-broker`.

#![deny(missing_docs)]

/// Configuration types shared across crates.
pub mod config;

/// Disk free-space budget for durable publishes.
pub mod disk;

/// Protocol-agnostic domain types (vhosts, exchanges, queues, users).
pub mod domain;

/// Domain error types.
pub mod error;

/// Process-wide memory watermark counters and soft/hard alarms.
pub mod memory;

/// Per-queue actors and the queue registry.
pub mod queue;

/// Management policies (TTL, dead-letter, max-length, alternate-exchange).
pub mod policy;

/// Exchange federation between vhosts in one process.
pub mod federation;

/// Exchange routing: binding index, topic matching, multi-destination lookup.
pub mod router;

/// Prometheus names shared with RabbitMQ, using the `queueforge_` prefix.
pub mod prom;

/// Lightweight system probes (RAM, disk free).
pub mod sysinfo;

pub use config::{
    ClusterConfig, ClusterMember, Config, DataConfig, FsyncPolicy, LimitsConfig, ListenersConfig,
    LoggingConfig, ManagementConfig, MemoryConfig, TlsConfig, DEFAULT_DISK_FREE_LIMIT_BYTES,
    DEFAULT_FSYNC_INTERVAL_MS, DEFAULT_HIGH_WATERMARK_RELATIVE, DEFAULT_MAX_CONNECTIONS,
    DEFAULT_MAX_MESSAGE_BYTES, DEFAULT_PREFETCH, DEFAULT_SOFT_WATERMARK_RELATIVE,
    DEFAULT_WAL_SEGMENT_MAX_BYTES,
};
pub use disk::DiskBudget;
pub use domain::{
    binding_args_key, binding_properties_key, Binding, Exchange, ExchangeType, HeaderArg,
    Permission, Queue, User, UserTag, Vhost, BUILTIN_EXCHANGE_NAMES, DEFAULT_EXCHANGE_NAME,
    DEFAULT_VHOST,
};
pub use error::{Error, Result};
pub use memory::MemoryTracker;
pub use policy::{apply_queue_policy, effective_alternate, select_policy, Policy, PolicyTarget};
pub use queue::{
    death_hop_count, effective_priority, generate_server_queue_name, is_cycle_destination,
    prepare_dead_letter, should_drop_for_cycle, AppHeaderValue, ArgValue, ConsumerDeliveryId,
    ConsumerSessionId, DeadLetterStrategy, DeathEntry, DeathReason, DeclareResult, DlxFailAction,
    DlxOutcome, DlxPublishResult, DlxRouter, DurabilityPolicy, DurableLogFactory, DurableQueueLog,
    EnqueueCompletion, Message, MessageHeaders, NoopMetaStore, OpenedDurableLog, OverflowPolicy,
    PrepareDeath, QueueActorBootstrap, QueueActorState, QueueArgs, QueueCmd, QueueDeclareOpts,
    QueueDelivery, QueueHandle, QueueInfo, QueueKey, QueueMessage, QueueMetaStore, QueueOffset,
    QueueRegistry, QueueStats, QueueType, Ready, SharedDurableLogFactory, ShutdownReport,
    DEFAULT_MAILBOX_CAPACITY, DEFAULT_MAX_DEATH_HOPS,
};
pub use router::{topic_matches, BindingIndex, BindingKey, ExchangeRouter, RouteResult};
pub use sysinfo::{disk_free_bytes, system_total_memory_bytes};
