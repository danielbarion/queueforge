//! [`QueueRegistry`]: `(vhost, name) → QueueHandle` map with actor spawn/supervision.
//! Declare, delete, lookup, and shutdown live in sibling modules.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, RwLock};

use compact_str::CompactString;
use tokio::sync::{mpsc, oneshot, Mutex as AsyncMutex};

/// Outcome of [`QueueRegistry::shutdown_all`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ShutdownReport {
    /// Queues that were present when drain started.
    pub queues: usize,
    /// Actors whose final WAL fsync failed.
    pub fsync_errors: usize,
    /// Actors that did not reply within the global budget.
    pub timeouts: usize,
    /// Mailboxes already closed (actor gone).
    pub mailbox_closed: usize,
}

impl ShutdownReport {
    /// True when every actor answered and no durable fsync failed.
    pub fn is_clean(&self) -> bool {
        self.fsync_errors == 0 && self.timeouts == 0
    }
}

use super::actor::DEFAULT_MAILBOX_CAPACITY;
use super::args::QueueArgs;
use super::cmd::QueueCmd;
use super::dlx::DlxRouter;
use super::durable::{DurabilityPolicy, SharedDurableLogFactory};
use super::meta::QueueMetaStore;
use crate::disk::DiskBudget;
use crate::domain::Queue;
use crate::error::{Error, Result};
use crate::memory::MemoryTracker;

/// Process-wide counter for unique server-generated queue names.
static SERVER_QUEUE_SEQ: AtomicU64 = AtomicU64::new(1);

/// Runtime state of a queue actor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum QueueActorState {
    /// Actor is running and accepting commands.
    Running = 0,
    /// Actor panicked; registry keeps the entry but does **not** auto-restart.
    Unavailable = 1,
}

impl QueueActorState {
    fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::Running,
            _ => Self::Unavailable,
        }
    }
}

/// Composite key for a queue in a vhost.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QueueKey {
    /// Virtual host name.
    pub vhost: CompactString,
    /// Queue name.
    pub name: CompactString,
}

impl QueueKey {
    /// Build a key from vhost + name.
    pub fn new(vhost: impl Into<CompactString>, name: impl Into<CompactString>) -> Self {
        Self {
            vhost: vhost.into(),
            name: name.into(),
        }
    }
}

impl std::fmt::Display for QueueKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.vhost, self.name)
    }
}

/// Shared metadata for a live (or unavailable) queue.
#[derive(Debug)]
pub struct QueueInfo {
    /// Queue key.
    pub key: QueueKey,
    /// Survives broker restart when true.
    pub durable: bool,
    /// Restricted to a single connection when true.
    pub exclusive: bool,
    /// Deleted when last consumer cancels.
    pub auto_delete: bool,
    /// Connection id that owns an exclusive queue (if any).
    ///
    /// Until the connection-manager lands, the broker passes the peer
    /// `SocketAddr` string as a per-TCP-connection stand-in.
    pub exclusive_owner: Option<CompactString>,
    /// Arguments from the declare, before policy fill.
    pub declared_args: QueueArgs,
    /// Effective arguments (declare plus policy).
    pub args: std::sync::Mutex<QueueArgs>,
    state: AtomicU8,
    /// Bytes this queue currently holds in the process-wide [`crate::MemoryTracker`].
    ///
    /// Released on normal message lifecycle and as a safety net when the actor
    /// panics or exits without an ordered shutdown (see actor `MemoryGuard`).
    pub(crate) reserved_bytes: AtomicU64,
}

impl QueueInfo {
    /// Create queue info from declare options (also used by actor tests).
    pub(crate) fn new(key: QueueKey, opts: &QueueDeclareOpts) -> Self {
        Self {
            key,
            durable: opts.durable,
            exclusive: opts.exclusive,
            auto_delete: opts.auto_delete,
            exclusive_owner: opts.exclusive_owner.clone(),
            declared_args: opts
                .declared_args
                .clone()
                .unwrap_or_else(|| opts.args.clone()),
            args: std::sync::Mutex::new(opts.args.clone()),
            state: AtomicU8::new(QueueActorState::Running as u8),
            reserved_bytes: AtomicU64::new(0),
        }
    }

    /// Current actor state.
    pub fn state(&self) -> QueueActorState {
        QueueActorState::from_u8(self.state.load(Ordering::Acquire))
    }

    /// Whether the actor is accepting work.
    pub fn is_available(&self) -> bool {
        self.state() == QueueActorState::Running
    }

    /// Mark the actor unavailable after panic isolation.
    pub fn mark_unavailable(&self) {
        self.state
            .store(QueueActorState::Unavailable as u8, Ordering::Release);
    }

    /// Bytes currently reserved by this queue in the global memory tracker.
    pub fn reserved_bytes(&self) -> u64 {
        self.reserved_bytes.load(Ordering::Relaxed)
    }

    /// Domain [`Queue`] snapshot (for persistence / management).
    pub fn to_domain(&self) -> Queue {
        Queue {
            vhost: self.key.vhost.clone(),
            name: self.key.name.clone(),
            durable: self.durable,
            exclusive: self.exclusive,
            auto_delete: self.auto_delete,
            args: self.args.lock().unwrap_or_else(|e| e.into_inner()).clone(),
            home: None,
        }
    }
}

/// Handle returned to connection tasks: mailbox sender + shared info.
#[derive(Debug, Clone)]
pub struct QueueHandle {
    /// Bounded command mailbox.
    pub tx: mpsc::Sender<QueueCmd>,
    /// Shared queue metadata / runtime state.
    pub info: Arc<QueueInfo>,
}

impl QueueHandle {
    /// Whether the actor is still running.
    pub fn is_available(&self) -> bool {
        self.info.is_available()
    }
}

/// Options for [`QueueRegistry::declare`].
#[derive(Debug, Clone, Default)]
pub struct QueueDeclareOpts {
    /// Survive broker restart.
    pub durable: bool,
    /// Exclusive to one connection.
    pub exclusive: bool,
    /// Auto-delete when unused.
    pub auto_delete: bool,
    /// Passive declare (must already exist).
    pub passive: bool,
    /// Declaring connection id (required when `exclusive`).
    pub exclusive_owner: Option<CompactString>,
    /// Closed declare-arguments set (already validated at the protocol edge).
    pub args: QueueArgs,
    /// Declare arguments before policy merge. `None` means "same as `args`".
    pub declared_args: Option<QueueArgs>,
    /// Cluster node that should own the actor. `None` keeps single-node behavior.
    pub home: Option<CompactString>,
}

/// Result of a successful declare (name may be server-generated).
#[derive(Debug, Clone)]
pub struct DeclareResult {
    /// Handle for the queue actor.
    pub handle: QueueHandle,
    /// Message count (0 until publish lands).
    pub message_count: u32,
    /// Consumer count.
    pub consumer_count: u32,
}

struct QueueEntry {
    handle: QueueHandle,
}

/// Maps `(vhost, queue_name) → QueueHandle` and owns actor lifecycle.
///
/// Create/delete/passive-restore mutations are serialized with [`Self::lifecycle`]
/// so durable metadata and registry membership stay one logical transaction
/// (no live durable actor without a meta row; no orphan meta from race loss).
pub struct QueueRegistry {
    entries: RwLock<HashMap<QueueKey, QueueEntry>>,
    /// Serializes create / delete / passive-restore so meta + map stay consistent.
    lifecycle: AsyncMutex<()>,
    meta: Arc<dyn QueueMetaStore>,
    /// Shared memory watermark counters (updated on enqueue/dequeue).
    pub memory: Arc<MemoryTracker>,
    /// Optional disk free budget for durable WAL publishes.
    pub disk: Option<Arc<DiskBudget>>,
    mailbox_capacity: usize,
    /// Opens per-queue WALs for durable queues (`None` = memory-only / tests).
    durable_factory: Option<SharedDurableLogFactory>,
    /// Group-commit policy applied to newly opened durable logs.
    durability_policy: DurabilityPolicy,
    /// When set, this registry must not spawn queues homed on a peer.
    local_node: Option<String>,
    /// Dead-letter router (set after construction to break Arc cycles).
    dlx: RwLock<Option<Arc<DlxRouter>>>,
    /// Fan-in of per-actor `x-expires` notifications.
    expired_tx: mpsc::UnboundedSender<QueueKey>,
    /// Receiver side held until [`Self::take_expired_rx`] is called.
    expired_rx: AsyncMutex<Option<mpsc::UnboundedReceiver<QueueKey>>>,
}

impl QueueRegistry {
    /// Create a registry backed by `meta` (use [`super::meta::NoopMetaStore`] in unit tests).
    pub fn new(meta: Arc<dyn QueueMetaStore>, memory: Arc<MemoryTracker>) -> Self {
        let (expired_tx, expired_rx) = mpsc::unbounded_channel();
        Self {
            entries: RwLock::new(HashMap::new()),
            lifecycle: AsyncMutex::new(()),
            meta,
            memory,
            disk: None,
            mailbox_capacity: DEFAULT_MAILBOX_CAPACITY,
            durable_factory: None,
            durability_policy: DurabilityPolicy::default(),
            local_node: None,
            dlx: RwLock::new(None),
            expired_tx,
            expired_rx: AsyncMutex::new(Some(expired_rx)),
        }
    }

    /// Attach a durable log factory and group-commit policy (production path).
    pub fn with_durability(
        mut self,
        factory: SharedDurableLogFactory,
        policy: DurabilityPolicy,
    ) -> Self {
        self.durable_factory = Some(factory);
        self.durability_policy = policy;
        self
    }

    /// Attach a disk free-space budget (durable publish gate).
    pub fn with_disk_budget(mut self, disk: Arc<DiskBudget>) -> Self {
        self.disk = Some(disk);
        self
    }

    /// Record this process's cluster node id so peer-homed queues are not spawned.
    pub fn with_local_node(mut self, node_id: impl Into<String>) -> Self {
        self.local_node = Some(node_id.into());
        self
    }

    /// Override mailbox capacity (tests / config).
    pub fn with_mailbox_capacity(mut self, capacity: usize) -> Self {
        self.mailbox_capacity = capacity.max(1);
        self
    }

    /// Install the dead-letter router (typically after the exchange router is ready).
    pub fn set_dlx(&self, dlx: Arc<DlxRouter>) {
        *self.dlx.write().expect("dlx lock poisoned") = Some(dlx);
    }

    /// Take the `x-expires` notification receiver (at most once).
    ///
    /// The broker should spawn a task that deletes queues as they expire.
    pub async fn take_expired_rx(&self) -> Option<mpsc::UnboundedReceiver<QueueKey>> {
        self.expired_rx.lock().await.take()
    }

    /// Shared handle for cloning into connection tasks.
    pub fn shared(meta: Arc<dyn QueueMetaStore>, memory: Arc<MemoryTracker>) -> Arc<Self> {
        Arc::new(Self::new(meta, memory))
    }

    /// Shared registry with durable WAL factory.
    pub fn shared_with_durability(
        meta: Arc<dyn QueueMetaStore>,
        memory: Arc<MemoryTracker>,
        factory: SharedDurableLogFactory,
        policy: DurabilityPolicy,
    ) -> Arc<Self> {
        Arc::new(Self::new(meta, memory).with_durability(factory, policy))
    }

    /// Shared registry with durability + disk budget.
    pub fn shared_with_durability_and_disk(
        meta: Arc<dyn QueueMetaStore>,
        memory: Arc<MemoryTracker>,
        factory: SharedDurableLogFactory,
        policy: DurabilityPolicy,
        disk: Arc<DiskBudget>,
    ) -> Arc<Self> {
        Arc::new(
            Self::new(meta, memory)
                .with_durability(factory, policy)
                .with_disk_budget(disk),
        )
    }
}

mod declare;
mod declare_sync;
mod delete;
mod lookup;
mod shutdown;

pub use declare::generate_server_queue_name;

#[cfg(test)]
mod tests;
