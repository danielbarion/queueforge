//! Commands sent on a queue actor's bounded mailbox.

use std::sync::Arc;
use tokio::time::Instant;

use bytes::Bytes;
use compact_str::CompactString;

use super::args::QueueArgs;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

/// One AMQP field-table value stored on a message.
///
/// Floats are stored as IEEE bits so the type stays `Eq`. Nested tables and
/// arrays round-trip. Broker `x-death` fields are not stored here; they live
/// on [`MessageHeaders`] death fields and overwrite same-named client keys
/// when the message is delivered.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AppHeaderValue {
    /// AMQP void.
    Null,
    /// Boolean.
    Bool(bool),
    /// Integer field (any width that fits).
    I64(i64),
    /// `f64::to_bits`.
    F64Bits(u64),
    /// UTF-8 string.
    Str(String),
    /// Raw byte array.
    Bytes(Vec<u8>),
    /// Field array.
    Array(Vec<AppHeaderValue>),
    /// Nested field table, insertion order preserved.
    Table(Vec<(String, AppHeaderValue)>),
}

use super::dlx::{DeathEntry, DeathReason};
use crate::error::Result;

/// Queue-scoped identity for unacked deliveries (never an AMQP delivery tag).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConsumerDeliveryId(pub u64);

/// Opaque consumer session handle assigned by the connection task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConsumerSessionId(pub u64);

/// Monotonic per-queue offset assigned at enqueue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct QueueOffset(pub u64);

/// Application headers carried with a message.
///
/// Client field-table entries live in [`Self::app`]. The dead-letter chain
/// (`x-death` and first-death fields) is stored separately and wins on deliver.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageHeaders {
    /// `x-death` array (oldest first; RabbitMQ appends).
    pub deaths: Vec<DeathEntry>,
    /// `x-first-death-reason`.
    pub first_death_reason: Option<DeathReason>,
    /// `x-first-death-queue`.
    pub first_death_queue: Option<CompactString>,
    /// `x-first-death-exchange`.
    pub first_death_exchange: Option<CompactString>,
    /// Client application headers, excluding broker death keys.
    pub app: Vec<(CompactString, AppHeaderValue)>,
}

impl MessageHeaders {
    /// Whether any death / first-death fields are set.
    pub fn is_empty(&self) -> bool {
        self.deaths.is_empty()
            && self.first_death_reason.is_none()
            && self.first_death_queue.is_none()
            && self.first_death_exchange.is_none()
            && self.app.is_empty()
    }

    /// Approximate encoded size of application headers.
    pub fn app_bytes(&self) -> u64 {
        self.app.iter().fold(0u64, |n, (k, v)| {
            n.saturating_add(k.len() as u64)
                .saturating_add(app_header_bytes(v))
        })
    }
}

fn app_header_bytes(v: &AppHeaderValue) -> u64 {
    match v {
        AppHeaderValue::Null | AppHeaderValue::Bool(_) => 1,
        AppHeaderValue::I64(_) | AppHeaderValue::F64Bits(_) => 8,
        AppHeaderValue::Str(s) => s.len() as u64,
        AppHeaderValue::Bytes(b) => b.len() as u64,
        AppHeaderValue::Array(items) => items
            .iter()
            .fold(0u64, |n, i| n.saturating_add(app_header_bytes(i))),
        AppHeaderValue::Table(entries) => entries.iter().fold(0u64, |n, (k, i)| {
            n.saturating_add(k.len() as u64)
                .saturating_add(app_header_bytes(i))
        }),
    }
}

/// Internal message envelope.
#[derive(Debug, Clone)]
pub struct Message {
    /// Exchange the message was published to.
    pub exchange: CompactString,
    /// Routing key.
    pub routing_key: CompactString,
    /// Payload bytes.
    pub body: Bytes,
    /// `delivery_mode == 2`.
    pub persistent: bool,
    /// Previously delivered and requeued.
    pub redelivered: bool,
    /// Optional content-type (passed through for deliver headers).
    pub content_type: Option<CompactString>,
    /// Optional content-encoding.
    pub content_encoding: Option<CompactString>,
    /// Optional correlation id.
    pub correlation_id: Option<CompactString>,
    /// Optional message id.
    pub message_id: Option<CompactString>,
    /// Optional reply-to.
    pub reply_to: Option<CompactString>,
    /// Optional AMQP expiration property (relative ms as string).
    pub expiration: Option<CompactString>,
    /// Optional app id.
    pub app_id: Option<CompactString>,
    /// Optional user id.
    pub user_id: Option<CompactString>,
    /// Optional type name.
    pub type_: Option<CompactString>,
    /// Optional priority.
    pub priority: Option<u8>,
    /// Optional timestamp (posix seconds).
    pub timestamp: Option<u64>,
    /// Absolute expiry as unix milliseconds, when known.
    ///
    /// Set at enqueue from the earlier of queue TTL and the per-message
    /// expiration property. Recovery restores the remaining time from this
    /// value. `None` on older records means "grant a fresh TTL".
    pub expires_unix_ms: Option<u64>,
    /// Application headers (`x-death`, …).
    pub headers: MessageHeaders,
}

impl Message {
    /// Empty message used only as a carrier for a server consumer cancel.
    pub fn blank() -> Self {
        Self {
            exchange: CompactString::default(),
            routing_key: CompactString::default(),
            body: Bytes::new(),
            persistent: false,
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
            headers: MessageHeaders::default(),
        }
    }

    /// Approximate tracked size: body + fixed slot overhead + properties length.
    ///
    /// Matches the design watermark table (conservative vs RSS). Optional string
    /// properties contribute their UTF-8 length; small scalar flags add 1–8 bytes.
    pub fn tracked_bytes(&self) -> u64 {
        const OVERHEAD: u64 = 128;
        OVERHEAD
            .saturating_add(self.body.len() as u64)
            .saturating_add(self.properties_bytes())
    }

    /// Approximate bytes for AMQP basic properties carried on this message.
    fn properties_bytes(&self) -> u64 {
        let mut n: u64 = 0;
        for s in [
            self.content_type.as_deref(),
            self.content_encoding.as_deref(),
            self.correlation_id.as_deref(),
            self.message_id.as_deref(),
            self.reply_to.as_deref(),
            self.expiration.as_deref(),
            self.app_id.as_deref(),
            self.user_id.as_deref(),
            self.type_.as_deref(),
        ]
        .into_iter()
        .flatten()
        {
            n = n.saturating_add(s.len() as u64);
        }
        n = n.saturating_add(self.headers.app_bytes());
        if self.priority.is_some() {
            n = n.saturating_add(1);
        }
        if self.timestamp.is_some() {
            n = n.saturating_add(8);
        }
        // delivery_mode / redelivered flags already covered by fixed overhead.
        n
    }

    /// Payload body length only (for max-length-bytes accounting).
    pub fn body_bytes(&self) -> u64 {
        self.body.len() as u64
    }
}

/// Message as held by a queue (offset + payload + ready-state expiry).
#[derive(Debug, Clone)]
pub struct QueueMessage {
    /// Monotonic enqueue offset.
    pub offset: QueueOffset,
    /// Shared message body/metadata.
    pub message: Arc<Message>,
    /// Absolute ready-state expiry (from per-msg / queue TTL at enqueue).
    ///
    /// `None` = no TTL. Not applied while unacked (message is not in ready).
    pub expires_at: Option<Instant>,
}

impl QueueMessage {
    /// Construct without a ready-state expiry (caller may set later).
    pub fn new(offset: QueueOffset, message: Arc<Message>) -> Self {
        Self {
            offset,
            message,
            expires_at: None,
        }
    }
}

/// Completion signal for enqueue (transient completes immediately; durable awaits WAL).
#[derive(Debug)]
pub struct EnqueueCompletion {
    /// Assigned queue offset.
    pub offset: QueueOffset,
    /// Resolved when the message is safe w.r.t. durability policy.
    pub durable_done: oneshot::Receiver<Result<()>>,
}

/// Push notification from a queue actor to a consumer's connection.
#[derive(Debug, Clone)]
pub struct QueueDelivery {
    /// Consumer session that should receive the message.
    pub session: ConsumerSessionId,
    /// Queue-scoped delivery id for the channel ledger / ack path.
    pub delivery_id: ConsumerDeliveryId,
    /// Message payload.
    pub message: QueueMessage,
    /// When true, the connection settles the delivery after the frame is written.
    ///
    /// The actor keeps the message unacked until then, so a crash before the
    /// write requeues it.
    pub settles_on_write: bool,
    /// Queue is gone. The connection sends `basic.cancel` and does not deliver.
    pub server_cancel: bool,
}

/// Where a stream consumer starts reading (`x-stream-offset`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamStart {
    /// The oldest entry still kept.
    First,
    /// The newest entry.
    Last,
    /// Only entries published after the consumer starts. The default.
    Next,
    /// A given offset, clamped to what is kept.
    Offset(u64),
    /// The first entry at or after this Unix time in milliseconds.
    TimestampMs(u64),
    /// The first entry no older than this many milliseconds.
    AgeMs(u64),
}

/// Commands handled by a per-queue actor.
#[derive(Debug)]
pub enum QueueCmd {
    /// Set where the next consumer registered with `session` starts, for a
    /// stream queue. Other queue types ignore it.
    StreamStart {
        /// Session the following `RegisterConsumer` will use.
        session: ConsumerSessionId,
        /// Starting point.
        start: StreamStart,
    },
    /// Enqueue a message into the ready set.
    Enqueue {
        /// Message to enqueue.
        msg: Arc<Message>,
        /// Completion reply.
        reply: oneshot::Sender<Result<EnqueueCompletion>>,
    },
    /// Fsync the queue log through `offset`, then reply. Classic confirms do not send this.
    FlushDurable {
        /// Enqueue offset that must be inside the fsynced log before the reply.
        offset: QueueOffset,
        /// Completes after that offset is in the fsynced log, or immediately when there is no log.
        reply: oneshot::Sender<Result<()>>,
    },
    /// Pull one ready message into unacked for a consumer (legacy / get-style).
    ///
    /// Prefer credit-based push via [`QueueCmd::RegisterConsumer`]. When the
    /// queue is empty, replies `None`.
    Deliver {
        /// Consumer session (must be registered unless used for basic.get path).
        consumer: ConsumerSessionId,
        /// Optional delivery.
        reply: oneshot::Sender<Option<(ConsumerDeliveryId, QueueMessage)>>,
    },
    /// basic.get: take one message (or empty). When `no_ack`, message is not
    /// entered into the unacked map.
    Get {
        /// When true, remove without unacked tracking.
        no_ack: bool,
        /// Optional delivery + remaining ready count after take.
        reply: oneshot::Sender<Option<(ConsumerDeliveryId, QueueMessage, u32)>>,
    },
    /// Ack by queue-scoped delivery id. Channel expands `multiple` into
    /// individual acks; `multiple_to` is unused in v1.
    Ack {
        /// Delivery id.
        id: ConsumerDeliveryId,
        /// Optional range upper bound (unused; channel expands).
        multiple_to: Option<ConsumerDeliveryId>,
    },
    /// Finish an auto-ack delivery after the connection wrote it to the socket.
    SettleDelivered {
        /// Delivery id returned in [`QueueDelivery`].
        id: ConsumerDeliveryId,
    },
    /// Nack / reject by queue-scoped delivery id.
    Nack {
        /// Delivery id.
        id: ConsumerDeliveryId,
        /// Requeue when true (redelivered=true on next deliver).
        requeue: bool,
    },
    /// Ack a quorum delivery and report its shared id when the message left the log.
    AckReport {
        /// Delivery id.
        id: ConsumerDeliveryId,
        /// `Some(message_id)` when the message was removed.
        reply: oneshot::Sender<Option<CompactString>>,
    },
    /// Nack a quorum delivery. `Some(message_id)` when it was discarded or dead-lettered.
    NackReport {
        /// Delivery id.
        id: ConsumerDeliveryId,
        /// Requeue when true.
        requeue: bool,
        /// `Some(message_id)` when the message left the log.
        reply: oneshot::Sender<Option<CompactString>>,
    },
    /// Drop one replicated quorum message from ready and unacked on this node.
    Forget {
        /// Shared id assigned at publish.
        message_id: CompactString,
    },
    /// Register a consumer with push delivery + prefetch credit.
    RegisterConsumer {
        /// Session id assigned by the connection task.
        session: ConsumerSessionId,
        /// Auto-ack deliveries (no unacked map / no ledger credit needed).
        no_ack: bool,
        /// Exclusive consumer (at most one on this queue).
        exclusive: bool,
        /// `basic.consume` argument `x-priority`. Higher is served first.
        priority: i32,
        /// Initial prefetch credit: `None` = unlimited, `Some(0)` = zero (hold),
        /// `Some(n)` = n deliveries before more credit is needed.
        initial_credit: Option<u32>,
        /// Push channel for deliveries.
        deliver_tx: mpsc::Sender<QueueDelivery>,
        /// Reply once registered.
        reply: oneshot::Sender<Result<()>>,
    },
    /// Add prefetch credit after channel-side acks free capacity.
    AddCredit {
        /// Consumer session.
        session: ConsumerSessionId,
        /// Credits to add (`0` ignored; unlimited sessions ignore).
        credit: u32,
    },
    /// Replace absolute prefetch credit for a session (`None` = unlimited).
    /// Used when `basic.qos` changes after consume.
    SetCredit {
        /// Consumer session.
        session: ConsumerSessionId,
        /// New absolute credit budget (`None` = unlimited).
        credit: Option<u32>,
    },
    /// Unregister a consumer; requeue its unacked when `requeue` is true.
    UnregisterConsumer {
        /// Session to remove.
        session: ConsumerSessionId,
        /// Requeue unacked messages for this session.
        requeue: bool,
        /// Fired after unregister completes.
        reply: oneshot::Sender<()>,
    },
    /// Requeue all unacked for the given sessions (channel close bulk).
    RequeueUnacked {
        /// Sessions whose unacked should return to ready.
        sessions: Vec<ConsumerSessionId>,
        /// Fired when done.
        reply: oneshot::Sender<()>,
    },
    /// Purge ready messages; reply is the count removed.
    Purge {
        /// Reply with number of messages purged.
        reply: oneshot::Sender<u32>,
    },
    /// Snapshot of ready / unacked / consumer counts.
    Stats {
        /// Reply with current stats.
        reply: oneshot::Sender<QueueStats>,
    },
    /// The first and last offsets a stream holds, as the stream protocol's
    /// `first_chunk_id` and `committed_chunk_id`. Other queues, and an empty
    /// stream, reply `None`.
    StreamOffsets {
        /// Reply with `(first, last)` visible offsets.
        reply: oneshot::Sender<Option<(u64, u64)>>,
    },
    /// Graceful actor shutdown.
    Shutdown {
        /// Fired after the actor stops accepting work.
        ///
        /// `Ok(())` when the final WAL fsync (if any) succeeded; `Err` if the
        /// durable fsync failed (callers should treat process shutdown as unclean).
        reply: oneshot::Sender<crate::error::Result<()>>,
    },
    /// Replace effective queue arguments after a policy write.
    SetArgs {
        /// Merged declare arguments and policy.
        args: QueueArgs,
    },
    /// Mark the queue as "used" (redeclare / passive declare resets `x-expires`).
    Touch {
        /// Fired after the touch is recorded.
        reply: oneshot::Sender<()>,
    },
    /// Async DLX completion (posted by a background task; never awaits foreign actors
    /// on the queue actor task itself — avoids A↔B cross-actor deadlock).
    DlxResolved {
        /// Source message that left ready/unacked for dead-lettering.
        qm: QueueMessage,
        /// `Ok` when at least one destination accepted or the hop/cycle guard dropped.
        /// `Err` only when **no** destination accepted (caller may requeue or drop).
        outcome: std::result::Result<super::dlx::DlxOutcome, crate::error::Error>,
        /// What to do when `outcome` is `Err`.
        on_fail: DlxFailAction,
    },
    /// Test-only: panic the actor to exercise supervision.
    #[cfg(test)]
    TestPanic,
    /// Test-only: how many durable appends are still waiting for the parked log.
    #[cfg(test)]
    TestDeferredAppends {
        /// Count observed inside the command, before the handler returns.
        reply: oneshot::Sender<usize>,
    },
}

/// Policy applied when an async DLX attempt fails completely.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DlxFailAction {
    /// Drop the source message (overflow / TTL paths must make progress).
    Drop,
    /// Return the original message to ready (nack path; no silent loss).
    Requeue,
}

/// Lightweight queue counters.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QueueStats {
    /// Messages ready for delivery.
    pub messages_ready: u32,
    /// Messages awaiting ack.
    pub messages_unacked: u32,
    /// Active consumers.
    pub consumer_count: u32,
    /// `x-max-priority` when the queue is a priority queue.
    pub max_priority: Option<u8>,
    /// Ready counts per priority band (`index = priority`); `None` for FIFO.
    pub ready_by_priority: Option<Vec<u32>>,
}
