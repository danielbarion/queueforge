//! Durable log port used by the queue actor for WAL append / fsync / ack watermark.
//!
//! Implemented by `queueforge_store::QueueWal` so core does not depend on the store.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;

use crate::config::FsyncPolicy;
use crate::error::Result;
use crate::queue::args::QueueArgs;
use crate::queue::cmd::{Message, QueueMessage, QueueOffset};
use crate::queue::dlx::DlxRouter;
use crate::queue::QueueKey;

/// Result of opening a durable log: always includes a recovery pass.
pub struct OpenedDurableLog {
    /// Open WAL handle.
    pub log: Box<dyn DurableQueueLog>,
    /// Messages rebuilt into ready (`redelivered=true` when recovered from disk).
    pub ready: VecDeque<QueueMessage>,
    /// Next offset to assign (from WAL meta / last recovered offset).
    pub next_offset: u64,
}

/// Factory that opens / deletes per-queue durable logs under the data directory.
pub trait DurableLogFactory: Send + Sync {
    /// Open (or create) the durable log and **always** run recovery so ready /
    /// `next_offset` match on-disk segments. Corrupt WALs must return `Err`.
    fn open(&self, vhost: &str, queue: &str) -> Result<OpenedDurableLog>;

    /// Delete on-disk WAL segments + meta for a queue (best-effort).
    fn delete(&self, vhost: &str, queue: &str) -> Result<()>;
}

/// Per-queue segmented WAL interface owned by the queue actor.
///
/// Append is buffered; durability is completed when [`Self::fsync`] covers the
/// message offset (group commit). Ack advances an in-memory watermark that is
/// persisted on fsync / compact.
pub trait DurableQueueLog: Send {
    /// Append an enqueue record (OS page cache; not necessarily fsynced).
    fn append_enqueue(&mut self, offset: QueueOffset, msg: &Message) -> Result<()>;

    /// Note that `offset` has been acknowledged. Advances `ack_watermark` when
    /// contiguous. Sets [`Self::meta_dirty`] when the watermark moves.
    fn acknowledge(&mut self, offset: QueueOffset) -> Result<()>;

    /// Fsync data segments then meta. Returns the highest offset known durable.
    ///
    /// Waiters with `offset <= returned` may be completed successfully.
    /// Clears [`Self::meta_dirty`].
    fn fsync(&mut self) -> Result<QueueOffset>;

    /// Highest offset covered by a completed fsync (0 if none).
    fn durable_offset(&self) -> QueueOffset;

    /// Current ack watermark (all offsets ≤ this are fully consumed).
    fn ack_watermark(&self) -> QueueOffset;

    /// Whether `queue-meta.json` has unpersisted watermark / offset changes.
    fn meta_dirty(&self) -> bool;

    /// Compact: delete segments whose max offset is `≤ ack_watermark`.
    fn compact(&mut self) -> Result<()>;
}

/// Fsync / group-commit settings for a durable queue actor.
#[derive(Debug, Clone, Copy)]
pub struct DurabilityPolicy {
    /// Fsync policy.
    pub policy: FsyncPolicy,
    /// Interval for [`FsyncPolicy::EveryNMs`].
    pub interval: Duration,
    /// Batch size for [`FsyncPolicy::EveryNMessages`].
    pub every_n_messages: u64,
}

impl Default for DurabilityPolicy {
    fn default() -> Self {
        Self {
            policy: FsyncPolicy::EveryNMs,
            interval: Duration::from_millis(100),
            every_n_messages: 1,
        }
    }
}

impl DurabilityPolicy {
    /// Build from data-config fields.
    pub fn from_parts(
        policy: FsyncPolicy,
        fsync_interval_ms: u64,
        fsync_every_n_messages: u64,
    ) -> Self {
        Self {
            policy,
            interval: Duration::from_millis(fsync_interval_ms.max(1)),
            every_n_messages: fsync_every_n_messages.max(1),
        }
    }
}

/// Initial state for a queue actor after WAL recovery (or empty for new queues).
pub struct QueueActorBootstrap {
    /// Messages rebuilt into ready (already marked redelivered when recovered).
    pub ready: VecDeque<QueueMessage>,
    /// Next offset to assign on enqueue.
    pub next_offset: u64,
    /// Durable log (only for durable queues that need WAL).
    pub durable_log: Option<Box<dyn DurableQueueLog>>,
    /// Whether the queue definition is durable (controls WAL on persistent msgs).
    pub durable: bool,
    /// Group-commit policy (ignored when `durable_log` is `None`).
    pub durability_policy: DurabilityPolicy,
    /// Validated declare arguments (TTL / DLX / max-length).
    pub args: QueueArgs,
    /// Shared dead-letter router (`None` in pure unit tests).
    pub dlx: Option<Arc<DlxRouter>>,
    /// Notify registry when `x-expires` fires.
    pub expired_tx: Option<mpsc::UnboundedSender<QueueKey>>,
}

impl Default for QueueActorBootstrap {
    fn default() -> Self {
        Self::new_empty(false)
    }
}

impl std::fmt::Debug for QueueActorBootstrap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueueActorBootstrap")
            .field("ready_len", &self.ready.len())
            .field("next_offset", &self.next_offset)
            .field("has_wal", &self.durable_log.is_some())
            .field("durable", &self.durable)
            .field("durability_policy", &self.durability_policy)
            .field("args", &self.args)
            .field("has_dlx", &self.dlx.is_some())
            .finish()
    }
}

impl QueueActorBootstrap {
    /// Transient / empty durable queue defaults (`next_offset = 1`).
    pub fn new_empty(durable: bool) -> Self {
        Self {
            ready: VecDeque::new(),
            next_offset: 1,
            durable_log: None,
            durable,
            durability_policy: DurabilityPolicy::default(),
            args: QueueArgs::default(),
            dlx: None,
            expired_tx: None,
        }
    }

    /// Attach a durable log + policy.
    pub fn with_log(mut self, log: Box<dyn DurableQueueLog>, policy: DurabilityPolicy) -> Self {
        self.durable_log = Some(log);
        self.durable = true;
        self.durability_policy = policy;
        self
    }

    /// Attach declare args / DLX hooks.
    pub fn with_args(mut self, args: QueueArgs) -> Self {
        self.args = args;
        self
    }

    /// Attach dead-letter router.
    pub fn with_dlx(mut self, dlx: Arc<DlxRouter>) -> Self {
        self.dlx = Some(dlx);
        self
    }

    /// Attach x-expires notification channel.
    pub fn with_expired_tx(mut self, tx: mpsc::UnboundedSender<QueueKey>) -> Self {
        self.expired_tx = Some(tx);
        self
    }

    /// Seed ready messages and next offset from recovery.
    pub fn with_recovered(mut self, ready: VecDeque<QueueMessage>, next_offset: u64) -> Self {
        self.ready = ready;
        self.next_offset = next_offset.max(1);
        self
    }
}

/// Shared factory handle (optional — unit tests omit durability).
pub type SharedDurableLogFactory = Arc<dyn DurableLogFactory>;
