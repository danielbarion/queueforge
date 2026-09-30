//! Per-queue actor loop: owns ready/unacked state, consumer credit, TTL heap,
//! overflow policies, optional WAL, and dead-letter routing.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::Instant;

use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, trace, warn};

use super::args::{DeadLetterStrategy, OverflowPolicy, QueueArgs};
use super::cmd::{
    ConsumerDeliveryId, ConsumerSessionId, DlxFailAction, EnqueueCompletion, Message, QueueCmd,
    QueueDelivery, QueueMessage, QueueOffset, QueueStats,
};
use super::dlx::{DeathReason, DlxOutcome, DlxRouter};
use super::durable::{DurabilityPolicy, DurableQueueLog, QueueActorBootstrap};
use super::ready::Ready;
use super::{QueueInfo, QueueKey};
use crate::config::FsyncPolicy;
use crate::disk::DiskBudget;
use crate::error::Error;
use crate::memory::MemoryTracker;

/// Default bounded mailbox capacity (connection → queue actor).
pub const DEFAULT_MAILBOX_CAPACITY: usize = 1024;

/// Min-heap key for ready-message TTL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TtlKey {
    at: Instant,
    offset: QueueOffset,
}

impl PartialOrd for TtlKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TtlKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse ordering via BinaryHeap<Reverse<_>>: Ord on TtlKey is natural
        // (earlier Instant is smaller).
        match self.at.cmp(&other.at) {
            std::cmp::Ordering::Equal => self.offset.cmp(&other.offset),
            o => o,
        }
    }
}

struct UnackedEntry {
    message: QueueMessage,
    session: Option<ConsumerSessionId>,
}

struct ConsumerState {
    no_ack: bool,
    /// Remaining credit. `None` means unlimited.
    credit: Option<u32>,
    /// `x-priority`. Higher values are served first.
    priority: i32,
    deliver_tx: mpsc::Sender<QueueDelivery>,
}

struct FsyncWaiter {
    offset: QueueOffset,
    tx: oneshot::Sender<Result<(), Error>>,
}

struct QueueState {
    key: QueueKey,
    /// Ready set: FIFO or multi-lane priority (`Ready::Fifo` vs `Ready::Priority`).
    ready: Ready,
    /// Approximate ready payload bytes (body only).
    ready_bytes: u64,
    /// Min-heap of ready TTL deadlines (lazy deletion of stale offsets).
    ttl_heap: BinaryHeap<Reverse<TtlKey>>,
    unacked: HashMap<ConsumerDeliveryId, UnackedEntry>,
    consumers: HashMap<ConsumerSessionId, ConsumerState>,
    /// Round-robin cursor over consumer session ids.
    rr_order: Vec<ConsumerSessionId>,
    rr_idx: usize,
    next_offset: u64,
    next_delivery_id: u64,
    memory: Arc<MemoryTracker>,
    /// Per-queue reserved bytes (shared with registry / panic release).
    info: Arc<QueueInfo>,
    /// Optional free-space budget (durable WAL publishes).
    disk: Option<Arc<DiskBudget>>,
    exclusive_consumer: Option<ConsumerSessionId>,
    /// Queue is durable (definitions survive restart).
    durable: bool,
    /// Optional segmented WAL for durable+persistent messages.
    wal: Option<Box<dyn DurableQueueLog>>,
    /// True while an interval fsync owns the log outside this command loop.
    wal_parked: bool,
    /// Appends accepted while the interval fsync holds the log.
    deferred_appends: Vec<(QueueOffset, Arc<Message>)>,
    /// Acks accepted while the interval fsync holds the log.
    deferred_acks: Vec<QueueOffset>,
    durability_policy: DurabilityPolicy,
    /// Waiters for `durable_done` pending group commit.
    fsync_waiters: Vec<FsyncWaiter>,
    /// Appends since last fsync (for EveryNMessages).
    unsynced_appends: u64,
    /// Offsets enqueued to WAL that are not yet acked (for watermark continuity).
    outstanding_offsets: HashSet<u64>,
    /// Declared queue arguments (TTL / DLX / max-length).
    args: QueueArgs,
    /// Times each offset has been requeued. Used by `x-delivery-limit`.
    redeliveries: HashMap<u64, u32>,
    /// Dead-letter router (shared; weak registry inside).
    dlx: Option<Arc<DlxRouter>>,
    /// Last time the queue was "used" for `x-expires` (consumer/get/redeclare).
    last_used: Instant,
    /// Ack or enqueue needs fsync before the next command is fully durable.
    /// The actor loop runs the syscall off the runtime.
    pending_fsync: bool,
    /// Notify registry when `x-expires` fires.
    expired_tx: Option<mpsc::UnboundedSender<QueueKey>>,
    /// Self-command channel for async DLX completions (unbounded; avoids mailbox deadlock).
    internal_tx: mpsc::UnboundedSender<QueueCmd>,
}

impl QueueState {
    fn from_bootstrap(
        key: QueueKey,
        memory: Arc<MemoryTracker>,
        disk: Option<Arc<DiskBudget>>,
        info: Arc<QueueInfo>,
        boot: QueueActorBootstrap,
        internal_tx: mpsc::UnboundedSender<QueueCmd>,
    ) -> Self {
        let now = Instant::now();
        let mut state = Self {
            key,
            // Zero overhead when x-max-priority unset: Ready::Fifo.
            ready: Ready::from_max_priority(boot.args.max_priority),
            ready_bytes: 0,
            ttl_heap: BinaryHeap::new(),
            unacked: HashMap::new(),
            consumers: HashMap::new(),
            rr_order: Vec::new(),
            rr_idx: 0,
            next_offset: boot.next_offset.max(1),
            next_delivery_id: 1,
            memory,
            info,
            disk,
            exclusive_consumer: None,
            durable: boot.durable,
            wal: boot.durable_log,
            wal_parked: false,
            deferred_appends: Vec::new(),
            deferred_acks: Vec::new(),
            durability_policy: boot.durability_policy,
            fsync_waiters: Vec::new(),
            unsynced_appends: 0,
            outstanding_offsets: HashSet::new(),
            args: boot.args,
            redeliveries: HashMap::new(),
            dlx: boot.dlx,
            last_used: now,
            pending_fsync: false,
            expired_tx: boot.expired_tx,
            internal_tx,
        };
        // Recovered messages with `expires_unix_ms` keep the remaining deadline.
        // Older records (field absent) still receive a fresh TTL.
        //
        // Recovery re-lanes: bootstrap ready is offset-ordered; push_back into
        // multi-lane Ready preserves FIFO within each effective priority.
        let recovered = boot.ready;
        for mut qm in recovered {
            state.outstanding_offsets.insert(qm.offset.0);
            state.mem_reserve_uncond(qm.message.tracked_bytes());
            state.ready_bytes = state.ready_bytes.saturating_add(qm.message.body_bytes());
            let fresh = compute_expires_at(qm.message.as_ref(), &state.args, now);
            qm.expires_at = expiry_instant(qm.message.expires_unix_ms, fresh, now);
            if let Some(at) = qm.expires_at {
                state.ttl_heap.push(Reverse(TtlKey {
                    at,
                    offset: qm.offset,
                }));
            }
            state.ready.push_back(qm);
        }
        state.update_gauges();
        state
    }

    fn stats(&self) -> QueueStats {
        QueueStats {
            messages_ready: self.ready.len() as u32,
            messages_unacked: self.unacked.len() as u32,
            consumer_count: self.consumers.len() as u32,
            max_priority: self.ready.max_priority(),
            ready_by_priority: self.ready.counts_by_priority(),
        }
    }

    fn mint_offset(&mut self) -> QueueOffset {
        let off = QueueOffset(self.next_offset);
        self.next_offset = self.next_offset.saturating_add(1);
        off
    }

    fn mint_delivery_id(&mut self) -> ConsumerDeliveryId {
        let id = ConsumerDeliveryId(self.next_delivery_id);
        self.next_delivery_id = self.next_delivery_id.saturating_add(1);
        id
    }

    /// Reserve `n` bytes under the hard watermark (global + per-queue).
    fn mem_try_reserve(&self, n: u64) -> bool {
        if !self.memory.try_add(n) {
            return false;
        }
        self.info
            .reserved_bytes
            .fetch_add(n, std::sync::atomic::Ordering::Relaxed);
        true
    }

    /// Unconditional reserve (recovery / bootstrap).
    fn mem_reserve_uncond(&self, n: u64) {
        if n == 0 {
            return;
        }
        self.memory.add(n);
        self.info
            .reserved_bytes
            .fetch_add(n, std::sync::atomic::Ordering::Relaxed);
    }

    /// Release `n` bytes from global + per-queue accounting.
    fn mem_release(&self, n: u64) {
        if n == 0 {
            return;
        }
        let _ = self.info.reserved_bytes.fetch_update(
            std::sync::atomic::Ordering::Relaxed,
            std::sync::atomic::Ordering::Relaxed,
            |cur| Some(cur.saturating_sub(n)),
        );
        self.memory.sub(n);
    }

    /// Whether this enqueue must go through the WAL.
    fn needs_wal(&self, msg: &Message) -> bool {
        self.durable && msg.persistent && (self.wal.is_some() || self.wal_parked)
    }

    /// Assign an offset for enqueue; WAL append happens before the counter advances.
    fn assign_offset_for_enqueue(
        &mut self,
        msg: &Arc<Message>,
        needs_wal: bool,
    ) -> Result<QueueOffset, Error> {
        if needs_wal {
            let offset = QueueOffset(self.next_offset);
            if let Some(wal) = self.wal.as_mut() {
                wal.append_enqueue(offset, msg).map_err(|e| {
                    error!(
                        vhost = %self.key.vhost,
                        queue = %self.key.name,
                        error = %e,
                        "WAL append failed"
                    );
                    e
                })?;
                self.unsynced_appends = self.unsynced_appends.saturating_add(1);
            } else {
                // The interval fsync holds the log. The confirm does not wait for it.
                self.deferred_appends.push((offset, Arc::clone(msg)));
            }
            self.next_offset = self.next_offset.saturating_add(1);
            return Ok(offset);
        }

        let offset = self.mint_offset();
        if self.wal.is_some() {
            // Durable queue, non-persistent message: never on disk, but still
            // occupies an offset — auto-ack so watermark is not blocked.
            if let Some(wal) = self.wal.as_mut() {
                let _ = wal.acknowledge(offset);
            }
            // Persist watermark under Always; timer covers EveryNMs.
            self.maybe_flush_after_ack();
        }
        Ok(offset)
    }

    fn would_exceed_limits(&self, extra_msgs: u64, extra_bytes: u64) -> bool {
        if let Some(max) = self.args.max_length {
            if (self.ready.len() as u64).saturating_add(extra_msgs) > max {
                return true;
            }
        }
        if let Some(max_b) = self.args.max_length_bytes {
            if self.ready_bytes.saturating_add(extra_bytes) > max_b {
                return true;
            }
        }
        false
    }

    fn push_ready(&mut self, qm: QueueMessage) {
        self.ready_bytes = self.ready_bytes.saturating_add(qm.message.body_bytes());
        if let Some(at) = qm.expires_at {
            self.ttl_heap.push(Reverse(TtlKey {
                at,
                offset: qm.offset,
            }));
        }
        self.ready.push_back(qm);
    }

    /// Deliver head: highest-priority ready (or FIFO front).
    fn pop_ready_front(&mut self) -> Option<QueueMessage> {
        let qm = self.ready.pop_front()?;
        self.ready_bytes = self.ready_bytes.saturating_sub(qm.message.body_bytes());
        Some(qm)
    }

    /// Overflow drop-head: lowest-priority oldest (or FIFO front).
    fn pop_ready_drop_head(&mut self) -> Option<QueueMessage> {
        let qm = self.ready.pop_drop_head()?;
        self.ready_bytes = self.ready_bytes.saturating_sub(qm.message.body_bytes());
        Some(qm)
    }

    fn remove_ready_offset(&mut self, offset: QueueOffset) -> Option<QueueMessage> {
        let qm = self.ready.remove_offset(offset)?;
        self.ready_bytes = self.ready_bytes.saturating_sub(qm.message.body_bytes());
        Some(qm)
    }

    /// Drop a ready message without DLX (memory + WAL watermark).
    fn drop_ready_message(&mut self, qm: QueueMessage) {
        self.mem_release(qm.message.tracked_bytes());
        self.note_acked_offset(qm.offset);
    }

    /// Hand off DLX to a background task; never await foreign queue actors here.
    ///
    /// Message is already out of ready/unacked. Memory remains charged until
    /// [`QueueCmd::DlxResolved`]. Overflow/expire use [`DlxFailAction::Drop`] so
    /// failure cannot re-enter a tight requeue loop.
    fn schedule_dead_letter(
        &mut self,
        qm: QueueMessage,
        reason: DeathReason,
        on_fail: DlxFailAction,
    ) {
        let Some(dlx_name) = self.args.dead_letter_exchange.clone() else {
            self.drop_ready_message(qm);
            return;
        };
        let Some(dlx) = self.dlx.clone() else {
            self.drop_ready_message(qm);
            return;
        };
        let rk = self
            .args
            .dead_letter_routing_key
            .clone()
            .unwrap_or_else(|| qm.message.routing_key.clone());
        let max_hops = self.args.max_death_hops;
        let msg = Arc::clone(&qm.message);
        let key = self.key.clone();
        let internal_tx = self.internal_tx.clone();

        tokio::spawn(async move {
            let outcome = dlx
                .dead_letter(&key, dlx_name.as_str(), rk.as_str(), reason, msg, max_hops)
                .await;
            let _ = internal_tx.send(QueueCmd::DlxResolved {
                qm,
                outcome,
                on_fail,
            });
        });
    }

    fn apply_dlx_resolved(
        &mut self,
        qm: QueueMessage,
        outcome: Result<DlxOutcome, Error>,
        on_fail: DlxFailAction,
    ) {
        match outcome {
            Ok(DlxOutcome::Published)
            | Ok(DlxOutcome::PartialPublished)
            | Ok(DlxOutcome::DroppedCycle) => {
                self.mem_release(qm.message.tracked_bytes());
                self.note_acked_offset(qm.offset);
            }
            Err(e) => match on_fail {
                DlxFailAction::Drop => {
                    warn!(
                        vhost = %self.key.vhost,
                        queue = %self.key.name,
                        error = %e,
                        "DLX failed; dropping source message to make progress"
                    );
                    metrics::counter!("queueforge_dlx_failure_drop_total").increment(1);
                    self.drop_ready_message(qm);
                }
                DlxFailAction::Requeue => {
                    warn!(
                        vhost = %self.key.vhost,
                        queue = %self.key.name,
                        error = %e,
                        "DLX failed; requeueing message to ready"
                    );
                    // Clear past-due expiry so we do not immediately re-expire
                    // into another failing DLX loop before a consumer sees it.
                    let mut qm = qm;
                    if qm.expires_at.is_some_and(|at| at <= Instant::now()) {
                        qm.expires_at = None;
                    }
                    self.push_ready(qm);
                    self.drain_ready();
                }
            },
        }
        self.update_gauges();
    }

    /// Make room for one new message under max-length policies.
    ///
    /// Drop-head schedules async DLX and does **not** wait; the head leaves ready
    /// immediately so the loop always makes progress (no DLX-fail requeue spin).
    fn enforce_overflow_for_incoming(&mut self, incoming_bytes: u64) -> Result<(), Error> {
        loop {
            if !self.would_exceed_limits(1, incoming_bytes) {
                return Ok(());
            }
            match self.args.overflow {
                OverflowPolicy::RejectPublish | OverflowPolicy::RejectPublishDlx => {
                    return Err(Error::PreconditionFailed(format!(
                        "message rejected as queue {} length limit is reached",
                        self.key
                    )));
                }
                OverflowPolicy::DropHead => {
                    // Priority: drop lowest-priority oldest; FIFO: classic head.
                    let Some(head) = self.pop_ready_drop_head() else {
                        return Err(Error::PreconditionFailed(format!(
                            "message rejected as queue {} length limit is reached",
                            self.key
                        )));
                    };
                    // Progress-safe: message is out of ready before DLX runs.
                    self.schedule_dead_letter(head, DeathReason::Maxlen, self.dlx_on_fail());
                    crate::prom::dead_lettered("maxlen");
                }
            }
        }
    }

    fn dlx_on_fail(&self) -> DlxFailAction {
        match self.args.dead_letter_strategy {
            DeadLetterStrategy::AtLeastOnce => DlxFailAction::Requeue,
            DeadLetterStrategy::AtMostOnce => DlxFailAction::Drop,
        }
    }

    fn publish_rejected_to_dlx(&self, msg: Arc<Message>) {
        let Some(dlx_name) = self.args.dead_letter_exchange.clone() else {
            return;
        };
        let Some(dlx) = self.dlx.clone() else {
            return;
        };
        let rk = self
            .args
            .dead_letter_routing_key
            .clone()
            .unwrap_or_else(|| msg.routing_key.clone());
        let max_hops = self.args.max_death_hops;
        let key = self.key.clone();
        tokio::spawn(async move {
            let _ = dlx
                .dead_letter(&key, dlx_name.as_str(), rk.as_str(), DeathReason::Maxlen, msg, max_hops)
                .await;
        });
    }

    fn enqueue(&mut self, msg: Arc<Message>) -> Result<EnqueueCompletion, Error> {
        let body_bytes = msg.body_bytes();
        if let Err(err) = self.enforce_overflow_for_incoming(body_bytes) {
            if self.args.overflow == OverflowPolicy::RejectPublishDlx {
                self.publish_rejected_to_dlx(Arc::clone(&msg));
            }
            return Err(err);
        }

        let needs_wal = self.needs_wal(&msg);

        // Global memory hard watermark: refuse before WAL/offset mutation.
        let tracked = msg.tracked_bytes();
        if !self.mem_try_reserve(tracked) {
            metrics::counter!("queueforge_publish_blocked_total", "reason" => "memory")
                .increment(1);
            return Err(Error::Resource(
                "memory high watermark exceeded".to_string(),
            ));
        }

        // Disk free budget only applies to durable+persistent (WAL) path.
        if needs_wal {
            if let Some(disk) = self.disk.as_ref() {
                if !disk.allows_durable_write() {
                    self.mem_release(tracked);
                    metrics::counter!("queueforge_publish_blocked_total", "reason" => "disk")
                        .increment(1);
                    return Err(Error::Resource(format!(
                        "disk free below limit ({} bytes)",
                        disk.limit_bytes()
                    )));
                }
            }
        }

        // Stamp an absolute deadline before the WAL append so recovery can
        // restore the remaining TTL instead of granting a fresh one.
        let now = Instant::now();
        let (expires_at, expires_unix_ms) = expiry_stamp(msg.as_ref(), &self.args, now);
        let msg = if msg.expires_unix_ms != expires_unix_ms {
            let mut owned = (*msg).clone();
            owned.expires_unix_ms = expires_unix_ms;
            Arc::new(owned)
        } else {
            msg
        };

        // Issue 4: mint offset only after a successful WAL append so a failed
        // append cannot burn an offset and stall the ack watermark forever.
        let offset = match self.assign_offset_for_enqueue(&msg, needs_wal) {
            Ok(o) => o,
            Err(e) => {
                self.mem_release(tracked);
                return Err(e);
            }
        };

        let qm = QueueMessage {
            offset,
            message: msg,
            expires_at,
        };
        self.push_ready(qm);

        if needs_wal {
            self.outstanding_offsets.insert(offset.0);
        }

        let (tx, rx) = oneshot::channel();
        if needs_wal {
            match self.durability_policy.policy {
                FsyncPolicy::Never => {
                    // Buffered write only — treat as durable immediately.
                    let _ = tx.send(Ok(()));
                }
                FsyncPolicy::Always => {
                    // The actor loop fsyncs off the runtime, then completes this waiter.
                    self.fsync_waiters.push(FsyncWaiter { offset, tx });
                    self.pending_fsync = true;
                }
                FsyncPolicy::EveryNMs => {
                    // The interval timer still fsyncs. The confirm does not wait for it.
                    metrics::counter!("queueforge_confirm_before_fsync_total").increment(1);
                    let _ = tx.send(Ok(()));
                }
                FsyncPolicy::EveryNMessages => {
                    self.fsync_waiters.push(FsyncWaiter { offset, tx });
                    if self.unsynced_appends >= self.durability_policy.every_n_messages {
                        self.pending_fsync = true;
                    }
                }
            }
        } else {
            // Transient queue or non-persistent message: complete immediately.
            let _ = tx.send(Ok(()));
        }

        self.drain_ready();
        self.update_gauges();

        Ok(EnqueueCompletion {
            offset,
            durable_done: rx,
        })
    }

    /// Fsync on the blocking pool, then complete durability waiters.
    ///
    /// The actor keeps exclusive ownership of the log: the handle is moved into
    /// the blocking task and put back before any other command runs.
    async fn fsync_now(&mut self) -> Result<(), Error> {
        self.pending_fsync = false;
        let Some(mut wal) = self.wal.take() else {
            self.complete_waiters_up_to(QueueOffset(u64::MAX), Ok(()));
            return Ok(());
        };
        let start = std::time::Instant::now();
        let joined = tokio::task::spawn_blocking(move || {
            let result = wal.fsync();
            let compact_err = if result.is_ok() {
                wal.compact().err().map(|e| e.to_string())
            } else {
                None
            };
            (wal, result, compact_err)
        })
        .await;

        let (wal, result, compact_err) = match joined {
            Ok(parts) => parts,
            Err(e) => {
                return Err(Error::Store(format!("wal fsync task panicked: {e}")));
            }
        };
        self.wal = Some(wal);
        metrics::histogram!("queueforge_wal_fsync_seconds").record(start.elapsed().as_secs_f64());

        if let Some(err) = compact_err {
            warn!(
                vhost = %self.key.vhost,
                queue = %self.key.name,
                error = %err,
                "WAL compact failed"
            );
        }

        match result {
            Ok(synced) => {
                self.unsynced_appends = 0;
                self.complete_waiters_up_to(synced, Ok(()));
                Ok(())
            }
            Err(e) => {
                error!(
                    vhost = %self.key.vhost,
                    queue = %self.key.name,
                    error = %e,
                    "WAL fsync failed"
                );
                self.complete_waiters_up_to(
                    QueueOffset(u64::MAX),
                    Err(Error::Store(e.to_string())),
                );
                Err(e)
            }
        }
    }

    fn complete_waiters_up_to(&mut self, synced: QueueOffset, result: Result<(), Error>) {
        let mut remaining = Vec::new();
        for w in self.fsync_waiters.drain(..) {
            if w.offset.0 <= synced.0 {
                let send_val = match &result {
                    Ok(()) => Ok(()),
                    Err(e) => Err(Error::Store(e.to_string())),
                };
                let _ = w.tx.send(send_val);
            } else if result.is_err() {
                let _ = w.tx.send(Err(Error::Store("wal fsync failed".into())));
            } else {
                remaining.push(w);
            }
        }
        self.fsync_waiters = remaining;
    }

    fn update_gauges(&self) {
        metrics::gauge!(
            "queueforge_messages_ready",
            "queue" => self.key.name.to_string(),
            "vhost" => self.key.vhost.to_string()
        )
        .set(self.ready.len() as f64);
        metrics::gauge!(
            "queueforge_messages_unacked",
            "queue" => self.key.name.to_string(),
            "vhost" => self.key.vhost.to_string()
        )
        .set(self.unacked.len() as f64);
        crate::prom::queue_depth(
            self.key.vhost.as_str(),
            self.key.name.as_str(),
            self.ready.len() as u64,
            self.unacked.len() as u64,
            self.consumers.len() as u64,
        );
    }

    fn has_credit(c: &ConsumerState) -> bool {
        match c.credit {
            None => true,
            Some(n) => n > 0,
        }
    }

    fn take_credit(c: &mut ConsumerState) {
        if let Some(n) = c.credit.as_mut() {
            *n = n.saturating_sub(1);
        }
    }

    /// Deliver ready messages to consumers with available credit (round-robin).
    fn drain_ready(&mut self) {
        if self.consumers.is_empty() || self.ready.is_empty() {
            return;
        }
        let max_iters = self
            .ready
            .len()
            .saturating_mul(self.consumers.len().max(1) + 1);
        for _ in 0..max_iters {
            if self.ready.is_empty() || self.rr_order.is_empty() {
                break;
            }
            let Some(session) = self.pick_next_consumer() else {
                break;
            };
            if !self.try_deliver_to(session) && !self.consumers.values().any(Self::has_credit) {
                break;
            }
        }
        self.update_gauges();
    }

    fn pick_next_consumer(&mut self) -> Option<ConsumerSessionId> {
        if self.rr_order.is_empty() {
            return None;
        }
        if self.args.single_active {
            let session = self
                .rr_order
                .iter()
                .enumerate()
                .filter_map(|(i, session)| {
                    let c = self.consumers.get(session)?;
                    Self::has_credit(c).then_some((std::cmp::Reverse(c.priority), i))
                })
                .min()
                .map(|(_, i)| self.rr_order[i])?;
            return Some(session);
        }
        let best = self
            .rr_order
            .iter()
            .filter_map(|session| {
                let c = self.consumers.get(session)?;
                Self::has_credit(c).then_some(c.priority)
            })
            .max()?;
        let n = self.rr_order.len();
        for i in 0..n {
            let idx = (self.rr_idx + i) % n;
            let session = self.rr_order[idx];
            if let Some(c) = self.consumers.get(&session) {
                if c.priority == best && Self::has_credit(c) {
                    self.rr_idx = (idx + 1) % n;
                    return Some(session);
                }
            }
        }
        None
    }

    /// Returns true if a message was delivered.
    fn try_deliver_to(&mut self, session: ConsumerSessionId) -> bool {
        let no_ack = match self.consumers.get(&session) {
            Some(c) if Self::has_credit(c) => c.no_ack,
            _ => return false,
        };

        let Some(qm) = self.pop_ready_front() else {
            return false;
        };

        let tx = match self.consumers.get(&session) {
            Some(c) => c.deliver_tx.clone(),
            None => {
                self.push_ready_front(qm);
                return false;
            }
        };

        let delivery_id = self.mint_delivery_id();
        self.unacked.insert(
            delivery_id,
            UnackedEntry {
                message: qm.clone(),
                session: Some(session),
            },
        );

        if let Some(c) = self.consumers.get_mut(&session) {
            Self::take_credit(c);
        }

        let redelivered = qm.message.redelivered;
        let delivery = QueueDelivery {
            session,
            delivery_id,
            message: qm,
            settles_on_write: no_ack,
            server_cancel: false,
        };

        match tx.try_send(delivery) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                if let Some(entry) = self.unacked.remove(&delivery_id) {
                    self.push_ready_front(entry.message);
                }
                if let Some(c) = self.consumers.get_mut(&session) {
                    if let Some(n) = c.credit.as_mut() {
                        *n = n.saturating_add(1);
                    }
                }
                return false;
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                if let Some(entry) = self.unacked.remove(&delivery_id) {
                    let mut msg = entry.message;
                    let mut inner = (*msg.message).clone();
                    inner.redelivered = true;
                    msg.message = Arc::new(inner);
                    self.push_ready_front(msg);
                }
                self.remove_consumer(session, false);
                return false;
            }
        }

        metrics::counter!(
            "queueforge_deliver_total",
            "vhost" => self.key.vhost.to_string(),
            "queue" => self.key.name.to_string()
        )
        .increment(1);
        crate::prom::message_delivered_consume(no_ack);
        if redelivered {
            crate::prom::message_redelivered();
        }

        true
    }

    fn push_ready_front(&mut self, qm: QueueMessage) {
        self.ready_bytes = self.ready_bytes.saturating_add(qm.message.body_bytes());
        if let Some(at) = qm.expires_at {
            self.ttl_heap.push(Reverse(TtlKey {
                at,
                offset: qm.offset,
            }));
        }
        self.ready.push_front(qm);
    }

    fn note_acked_offset(&mut self, offset: QueueOffset) {
        if !self.outstanding_offsets.remove(&offset.0) {
            // Non-WAL message (transient or non-persistent).
            return;
        }
        if let Some(wal) = self.wal.as_mut() {
            if let Err(e) = wal.acknowledge(offset) {
                warn!(
                    vhost = %self.key.vhost,
                    queue = %self.key.name,
                    offset = offset.0,
                    error = %e,
                    "WAL acknowledge failed"
                );
            }
        } else if self.wal_parked {
            self.deferred_acks.push(offset);
        }
        self.maybe_flush_after_ack();
    }

    /// Issue 2: persist ack watermark without waiting for the next publish.
    fn maybe_flush_after_ack(&mut self) {
        let dirty = self.wal.as_ref().map(|w| w.meta_dirty()).unwrap_or(false);
        if !dirty {
            return;
        }
        match self.durability_policy.policy {
            FsyncPolicy::Always | FsyncPolicy::EveryNMessages => {
                self.pending_fsync = true;
            }
            FsyncPolicy::EveryNMs | FsyncPolicy::Never => {}
        }
    }

    fn needs_group_commit_flush(&self) -> bool {
        if !self.fsync_waiters.is_empty()
            || self.unsynced_appends > 0
            || !self.deferred_appends.is_empty()
            || !self.deferred_acks.is_empty()
        {
            return true;
        }
        self.wal.as_ref().map(|w| w.meta_dirty()).unwrap_or(false)
    }

    /// Start the interval fsync on the blocking pool and return the actor to commands.
    fn begin_interval_fsync(
        &mut self,
    ) -> Option<
        tokio::task::JoinHandle<(
            Box<dyn DurableQueueLog>,
            Result<QueueOffset, String>,
            Option<String>,
        )>,
    > {
        let mut wal = self.wal.take()?;
        self.wal_parked = true;
        Some(tokio::task::spawn_blocking(move || {
            let result = wal.fsync().map(|offset| offset).map_err(|e| e.to_string());
            let compact_err = if result.is_ok() {
                wal.compact().err().map(|e| e.to_string())
            } else {
                None
            };
            (wal, result, compact_err)
        }))
    }

    fn finish_interval_fsync(
        &mut self,
        joined: Result<
            (
                Box<dyn DurableQueueLog>,
                Result<QueueOffset, String>,
                Option<String>,
            ),
            tokio::task::JoinError,
        >,
        started: std::time::Instant,
    ) {
        let (wal, result, compact_err) = match joined {
            Ok(parts) => parts,
            Err(e) => {
                self.wal_parked = false;
                error!(
                    vhost = %self.key.vhost,
                    queue = %self.key.name,
                    error = %e,
                    "WAL fsync task panicked"
                );
                self.complete_waiters_up_to(
                    QueueOffset(u64::MAX),
                    Err(Error::Store(format!("wal fsync task panicked: {e}"))),
                );
                return;
            }
        };
        self.wal = Some(wal);
        self.wal_parked = false;
        metrics::histogram!("queueforge_wal_fsync_seconds").record(started.elapsed().as_secs_f64());
        if let Some(err) = compact_err {
            warn!(
                vhost = %self.key.vhost,
                queue = %self.key.name,
                error = %err,
                "WAL compact failed"
            );
        }
        match result {
            Ok(synced) => {
                self.unsynced_appends = 0;
                self.complete_waiters_up_to(synced, Ok(()));
            }
            Err(e) => {
                error!(
                    vhost = %self.key.vhost,
                    queue = %self.key.name,
                    error = %e,
                    "WAL fsync failed"
                );
                self.complete_waiters_up_to(
                    QueueOffset(u64::MAX),
                    Err(Error::Store(e.clone())),
                );
            }
        }
        self.flush_deferred_wal();
    }

    fn flush_deferred_wal(&mut self) {
        let acks = std::mem::take(&mut self.deferred_acks);
        let appends = std::mem::take(&mut self.deferred_appends);
        let added = {
            let Some(wal) = self.wal.as_mut() else {
                self.deferred_acks = acks;
                self.deferred_appends = appends;
                return;
            };
            for offset in acks {
                if let Err(e) = wal.acknowledge(offset) {
                    warn!(
                        vhost = %self.key.vhost,
                        queue = %self.key.name,
                        offset = offset.0,
                        error = %e,
                        "deferred WAL acknowledge failed"
                    );
                }
            }
            let mut added = 0u64;
            for (offset, msg) in appends {
                if let Err(e) = wal.append_enqueue(offset, msg.as_ref()) {
                    error!(
                        vhost = %self.key.vhost,
                        queue = %self.key.name,
                        error = %e,
                        "deferred WAL append failed"
                    );
                } else {
                    added = added.saturating_add(1);
                }
            }
            added
        };
        self.unsynced_appends = self.unsynced_appends.saturating_add(added);
    }

    fn deliver_pull(
        &mut self,
        consumer: ConsumerSessionId,
    ) -> Option<(ConsumerDeliveryId, QueueMessage)> {
        let qm = self.pop_ready_front()?;
        let redelivered = qm.message.redelivered;
        let no_ack = self.consumers.get(&consumer).is_some_and(|c| c.no_ack);
        let delivery_id = self.mint_delivery_id();
        self.unacked.insert(
            delivery_id,
            UnackedEntry {
                message: qm.clone(),
                session: Some(consumer),
            },
        );
        self.update_gauges();
        metrics::counter!(
            "queueforge_deliver_total",
            "vhost" => self.key.vhost.to_string(),
            "queue" => self.key.name.to_string()
        )
        .increment(1);
        crate::prom::message_delivered_consume(no_ack);
        if redelivered {
            crate::prom::message_redelivered();
        }
        Some((delivery_id, qm))
    }

    fn get(&mut self, no_ack: bool) -> Option<(ConsumerDeliveryId, QueueMessage, u32)> {
        // basic.get counts as queue use for x-expires.
        self.touch_used();
        let qm = self.pop_ready_front()?;
        let redelivered = qm.message.redelivered;
        let delivery_id = self.mint_delivery_id();
        if no_ack {
            self.mem_release(qm.message.tracked_bytes());
            self.note_acked_offset(qm.offset);
        } else {
            self.unacked.insert(
                delivery_id,
                UnackedEntry {
                    message: qm.clone(),
                    session: None,
                },
            );
        }
        let remaining = self.ready.len() as u32;
        self.update_gauges();
        metrics::counter!(
            "queueforge_deliver_total",
            "vhost" => self.key.vhost.to_string(),
            "queue" => self.key.name.to_string()
        )
        .increment(1);
        crate::prom::message_delivered_get(no_ack);
        if redelivered {
            crate::prom::message_redelivered();
        }
        Some((delivery_id, qm, remaining))
    }

    fn ack(&mut self, id: ConsumerDeliveryId) {
        let _ = self.ack_report(id);
    }

    fn ack_report(&mut self, id: ConsumerDeliveryId) -> Option<compact_str::CompactString> {
        let entry = self.unacked.remove(&id)?;
        let message_id = entry.message.message.message_id.clone();
        self.mem_release(entry.message.message.tracked_bytes());
        self.note_acked_offset(entry.message.offset);
        metrics::counter!(
            "queueforge_ack_total",
            "vhost" => self.key.vhost.to_string(),
            "queue" => self.key.name.to_string()
        )
        .increment(1);
        crate::prom::message_acked();
        self.update_gauges();
        message_id
    }

    fn forget_message(&mut self, message_id: &str) {
        let mut kept = Vec::new();
        while let Some(qm) = self.pop_ready_front() {
            if qm.message.message_id.as_deref() == Some(message_id) {
                self.mem_release(qm.message.tracked_bytes());
                self.note_acked_offset(qm.offset);
            } else {
                kept.push(qm);
            }
        }
        for qm in kept {
            self.push_ready(qm);
        }
        let doomed: Vec<_> = self
            .unacked
            .iter()
            .filter(|(_, entry)| entry.message.message.message_id.as_deref() == Some(message_id))
            .map(|(id, _)| *id)
            .collect();
        for id in doomed {
            let _ = self.ack_report(id);
        }
        self.update_gauges();
    }

    fn nack(&mut self, id: ConsumerDeliveryId, requeue: bool) {
        let _ = self.nack_report(id, requeue);
    }

    /// `Some(message_id)` when the message left this node's log.
    fn nack_report(&mut self, id: ConsumerDeliveryId, requeue: bool) -> Option<compact_str::CompactString> {
        let entry = self.unacked.remove(&id)?;
        let message_id = entry.message.message.message_id.clone();
        if requeue {
            let mut qm = entry.message;
            if let Some(limit) = self.args.delivery_limit {
                let count = self.redeliveries.entry(qm.offset.0).or_insert(0);
                *count = count.saturating_add(1);
                if *count >= limit {
                    self.schedule_dead_letter(qm, DeathReason::Rejected, self.dlx_on_fail());
                    crate::prom::dead_lettered("delivery_limit");
                    self.update_gauges();
                    return message_id;
                }
            }
            let mut inner = (*qm.message).clone();
            inner.redelivered = true;
            qm.message = Arc::new(inner);
            self.push_ready(qm);
            self.drain_ready();
            self.update_gauges();
            return None;
        }
        self.schedule_dead_letter(entry.message, DeathReason::Rejected, self.dlx_on_fail());
        crate::prom::dead_lettered("rejected");
        self.update_gauges();
        message_id
    }

    fn register_consumer(
        &mut self,
        session: ConsumerSessionId,
        no_ack: bool,
        exclusive: bool,
        initial_credit: Option<u32>,
        deliver_tx: mpsc::Sender<QueueDelivery>,
        priority: i32,
    ) -> Result<(), Error> {
        if exclusive && (self.exclusive_consumer.is_some() || !self.consumers.is_empty()) {
            return Err(Error::ResourceLocked(format!(
                "queue {} already has consumers",
                self.key
            )));
        }
        if self.exclusive_consumer.is_some() {
            return Err(Error::ResourceLocked(format!(
                "queue {} has an exclusive consumer",
                self.key
            )));
        }
        if self.consumers.contains_key(&session) {
            return Err(Error::PreconditionFailed(format!(
                "consumer session {} already registered on {}",
                session.0, self.key
            )));
        }

        let credit = if no_ack { None } else { initial_credit };

        self.consumers.insert(
            session,
            ConsumerState {
                no_ack,
                credit,
                priority,
                deliver_tx,
            },
        );
        self.rr_order.push(session);
        if exclusive {
            self.exclusive_consumer = Some(session);
        }
        self.touch_used();
        self.drain_ready();
        crate::prom::consumer_opened();
        Ok(())
    }

    fn add_credit(&mut self, session: ConsumerSessionId, credit: u32) {
        if credit == 0 {
            return;
        }
        if let Some(c) = self.consumers.get_mut(&session) {
            if let Some(n) = c.credit.as_mut() {
                *n = n.saturating_add(credit);
            }
            self.drain_ready();
        }
    }

    fn set_credit(&mut self, session: ConsumerSessionId, credit: Option<u32>) {
        if let Some(c) = self.consumers.get_mut(&session) {
            if c.no_ack {
                return;
            }
            c.credit = credit;
            self.drain_ready();
        }
    }

    fn remove_consumer(&mut self, session: ConsumerSessionId, requeue: bool) {
        if self.consumers.remove(&session).is_some() {
            crate::prom::consumer_closed();
        }
        self.rr_order.retain(|s| *s != session);
        if self.rr_idx >= self.rr_order.len() {
            self.rr_idx = 0;
        }
        if self.exclusive_consumer == Some(session) {
            self.exclusive_consumer = None;
        }
        if requeue {
            self.requeue_sessions(&[session]);
        }
        // Becoming unused starts the x-expires clock.
        if self.consumers.is_empty() {
            self.touch_used();
        }
    }

    fn requeue_sessions(&mut self, sessions: &[ConsumerSessionId]) {
        let session_set: std::collections::HashSet<_> = sessions.iter().copied().collect();
        let mut to_requeue = Vec::new();
        let keys: Vec<_> = self.unacked.keys().copied().collect();
        for id in keys {
            if let Some(entry) = self.unacked.get(&id) {
                match entry.session {
                    Some(s) if session_set.contains(&s) => {
                        if let Some(entry) = self.unacked.remove(&id) {
                            to_requeue.push(entry.message);
                        }
                    }
                    _ => {}
                }
            }
        }
        for mut qm in to_requeue {
            let mut inner = (*qm.message).clone();
            inner.redelivered = true;
            qm.message = Arc::new(inner);
            self.push_ready(qm);
        }
        self.update_gauges();
    }

    fn requeue_all_unacked(&mut self) {
        let ids: Vec<_> = self.unacked.keys().copied().collect();
        for id in ids {
            self.nack(id, true);
        }
    }

    fn purge(&mut self) -> u32 {
        let n = self.ready.len() as u32;
        while let Some(qm) = self.pop_ready_front() {
            self.mem_release(qm.message.tracked_bytes());
            // Purged messages are treated as acked for watermark (dropped).
            self.note_acked_offset(qm.offset);
        }
        self.ttl_heap.clear();
        self.update_gauges();
        n
    }

    fn touch_used(&mut self) {
        self.last_used = Instant::now();
    }

    /// Next absolute deadline for the actor sleep (TTL or x-expires).
    fn next_deadline(&mut self) -> Option<Instant> {
        self.purge_stale_ttl_keys();
        let ttl = self.ttl_heap.peek().map(|Reverse(k)| k.at);
        let expires = self.queue_expires_at();
        match (ttl, expires) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        }
    }

    fn queue_expires_at(&self) -> Option<Instant> {
        let ttl = self.args.expires()?;
        // Unused only when no consumers (RabbitMQ-like).
        if !self.consumers.is_empty() {
            return None;
        }
        Some(self.last_used + ttl)
    }

    fn purge_stale_ttl_keys(&mut self) {
        while let Some(Reverse(k)) = self.ttl_heap.peek().copied() {
            let still_ready = self.ready.has_ttl_entry(k.offset, Some(k.at));
            if still_ready {
                break;
            }
            self.ttl_heap.pop();
        }
    }

    /// Expire all ready messages whose deadline has passed.
    ///
    /// Progress-safe: each due message leaves ready before async DLX; on DLX
    /// failure the completion path **drops** (never requeues a still-due TTL).
    fn expire_due_messages(&mut self) {
        let now = Instant::now();
        loop {
            self.purge_stale_ttl_keys();
            let Some(Reverse(k)) = self.ttl_heap.peek().copied() else {
                break;
            };
            if k.at > now {
                break;
            }
            self.ttl_heap.pop();
            if let Some(qm) = self.remove_ready_offset(k.offset) {
                self.schedule_dead_letter(qm, DeathReason::Expired, self.dlx_on_fail());
                crate::prom::dead_lettered("expired");
            }
        }
        self.drain_ready();
        self.update_gauges();
    }

    /// If `x-expires` elapsed while unused, notify registry and return true.
    fn check_queue_expired(&self) -> bool {
        matches!(self.queue_expires_at(), Some(at) if Instant::now() >= at)
    }

    fn notify_queue_expired(&self) {
        if let Some(tx) = &self.expired_tx {
            let _ = tx.send(self.key.clone());
        }
    }

    /// Final fsync + free in-memory state. Returns fsync error if durable WAL
    /// could not be flushed (Issue 3: surface failure to process shutdown).
    async fn shutdown_flush(&mut self) -> Result<(), Error> {
        for (session, consumer) in &self.consumers {
            let _ = consumer
                .deliver_tx
                .send(QueueDelivery {
                    session: *session,
                    delivery_id: ConsumerDeliveryId(0),
                    message: QueueMessage::new(QueueOffset(0), Arc::new(Message::blank())),
                    settles_on_write: false,
                    server_cancel: true,
                })
                .await;
        }
        let fsync_result = if self.wal.is_some() {
            self.fsync_now().await
        } else {
            Ok(())
        };
        // Fail any remaining waiters (durable_done paths still in flight).
        for w in self.fsync_waiters.drain(..) {
            let _ = w.tx.send(Err(Error::Unavailable(format!(
                "queue {}/{} shutting down",
                self.key.vhost, self.key.name
            ))));
        }
        while let Some(qm) = self.pop_ready_front() {
            self.mem_release(qm.message.tracked_bytes());
        }
        let unacked: Vec<_> = self.unacked.drain().map(|(_, e)| e).collect();
        for entry in unacked {
            self.mem_release(entry.message.message.tracked_bytes());
        }
        self.ttl_heap.clear();
        fsync_result
    }
}

/// `(actor wake instant, absolute unix ms)` for a newly enqueued message.
fn expiry_stamp(msg: &Message, args: &QueueArgs, now: Instant) -> (Option<Instant>, Option<u64>) {
    if let Some(ms) = msg.expires_unix_ms {
        let instant = expiry_instant(Some(ms), None, now);
        return (instant, Some(ms));
    }
    let expires_at = compute_expires_at(msg, args, now);
    let unix_ms = expires_at.and_then(|at| {
        let left = at.saturating_duration_since(now);
        SystemTime::now()
            .checked_add(left)
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .map(|d| d.as_millis() as u64)
    });
    (expires_at, unix_ms)
}

/// Restore a wall-clock deadline. A deadline already in the past is due now.
fn expiry_instant(unix_ms: Option<u64>, fresh: Option<Instant>, now: Instant) -> Option<Instant> {
    match unix_ms {
        Some(ms) => {
            let deadline = UNIX_EPOCH + Duration::from_millis(ms);
            match deadline.duration_since(SystemTime::now()) {
                Ok(left) => Some(now + left),
                Err(_) => Some(now),
            }
        }
        None => fresh,
    }
}

/// Effective ready-state TTL = min(per-msg expiration ms, queue x-message-ttl).
fn compute_expires_at(msg: &Message, args: &QueueArgs, now: Instant) -> Option<Instant> {
    let mut ttl_ms: Option<u64> = None;
    if let Some(d) = args.message_ttl() {
        ttl_ms = Some(d.as_millis().min(u128::from(u64::MAX)) as u64);
    }
    if let Some(ref exp) = msg.expiration {
        if let Ok(ms) = exp.parse::<u64>() {
            ttl_ms = Some(match ttl_ms {
                Some(t) => t.min(ms),
                None => ms,
            });
        }
    }
    ttl_ms.map(|ms| now + Duration::from_millis(ms))
}

/// RAII guard: on drop, release any remaining per-queue reservation to the
/// global [`MemoryTracker`]. Normal lifecycle paths zero the counter first so
/// this is a no-op; panic / abrupt exit reclaims the watermark contribution.
struct MemoryGuard {
    memory: Arc<MemoryTracker>,
    reserved: Arc<QueueInfo>,
}

impl Drop for MemoryGuard {
    fn drop(&mut self) {
        let residual = self
            .reserved
            .reserved_bytes
            .swap(0, std::sync::atomic::Ordering::AcqRel);
        if residual > 0 {
            self.memory.sub(residual);
        }
    }
}

/// Run the queue actor until [`QueueCmd::Shutdown`] or the mailbox closes.
pub async fn run(
    key: QueueKey,
    mut rx: mpsc::Receiver<QueueCmd>,
    memory: Arc<MemoryTracker>,
    disk: Option<Arc<DiskBudget>>,
    info: Arc<QueueInfo>,
    bootstrap: QueueActorBootstrap,
) {
    debug!(
        vhost = %key.vhost,
        queue = %key.name,
        durable = bootstrap.durable,
        ready = bootstrap.ready.len(),
        next_offset = bootstrap.next_offset,
        "queue actor started"
    );
    let policy = bootstrap.durability_policy;
    // Releases any residual reservation if the actor panics or exits without
    // ordered Shutdown (individual mem_release zeros the counter first).
    let _memory_guard = MemoryGuard {
        memory: Arc::clone(&memory),
        reserved: Arc::clone(&info),
    };
    // Unbounded self-channel so async DLX tasks can post completions without
    // contending on the bounded external mailbox (deadlock-safe).
    let (internal_tx, mut internal_rx) = mpsc::unbounded_channel();
    let mut state =
        QueueState::from_bootstrap(key.clone(), memory, disk, info, bootstrap, internal_tx);

    // Group-commit timer only when durable WAL + every_n_ms.
    let use_timer = state.wal.is_some() && policy.policy == FsyncPolicy::EveryNMs;
    let mut interval = if use_timer {
        let mut i = tokio::time::interval(policy.interval);
        i.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // Skip the immediate first tick.
        i.tick().await;
        Some(i)
    } else {
        None
    };
    let mut interval_fsync: Option<(
        std::time::Instant,
        tokio::task::JoinHandle<(
            Box<dyn DurableQueueLog>,
            Result<QueueOffset, String>,
            Option<String>,
        )>,
    )> = None;

    loop {
        // Expire any already-due messages before sleeping.
        state.expire_due_messages();
        if state.check_queue_expired() {
            debug!(
                vhost = %key.vhost,
                queue = %key.name,
                "queue x-expires elapsed; notifying registry"
            );
            state.notify_queue_expired();
            // Soft-stop: still process cmds until registry Shutdown, but stop
            // arming further expires sleeps by clearing the arg.
            state.args.expires_ms = None;
        }

        let deadline = state.next_deadline();

        let cmd = tokio::select! {
            biased;
            // The in-flight fsync join is first. A ready mailbox must not park
            // the log forever; a confirm still returns while the sync blocks.
            joined = async {
                if let Some((_, handle)) = interval_fsync.as_mut() {
                    (&mut *handle).await
                } else {
                    std::future::pending().await
                }
            }, if interval_fsync.is_some() => {
                if let Some((started, _)) = interval_fsync.take() {
                    state.finish_interval_fsync(joined, started);
                }
                continue;
            }
            // Prefer external cmds, but always drain internal DLX completions.
            cmd = rx.recv() => cmd,
            cmd = internal_rx.recv() => cmd,
            _ = async {
                if let Some(d) = deadline {
                    tokio::time::sleep_until(d).await;
                } else {
                    std::future::pending::<()>().await;
                }
            }, if deadline.is_some() => {
                continue;
            }
            _ = async {
                if let Some(interval) = interval.as_mut() {
                    interval.tick().await;
                } else {
                    std::future::pending::<()>().await;
                }
            }, if use_timer && interval_fsync.is_none() => {
                if state.needs_group_commit_flush() {
                    if let Some(handle) = state.begin_interval_fsync() {
                        interval_fsync = Some((std::time::Instant::now(), handle));
                    }
                }
                continue;
            }
        };

        let Some(cmd) = cmd else {
            break;
        };

        match cmd {
            QueueCmd::Enqueue { msg, reply } => {
                let res = state.enqueue(msg);
                let _ = reply.send(res);
            }
            QueueCmd::Deliver { consumer, reply } => {
                let _ = reply.send(state.deliver_pull(consumer));
            }
            QueueCmd::Get { no_ack, reply } => {
                let _ = reply.send(state.get(no_ack));
            }
            QueueCmd::Ack { id, .. } => {
                state.ack(id);
            }
            QueueCmd::AckReport { id, reply } => {
                let _ = reply.send(state.ack_report(id));
            }
            QueueCmd::SettleDelivered { id } => {
                state.ack(id);
            }
            QueueCmd::Nack { id, requeue } => {
                state.nack(id, requeue);
            }
            QueueCmd::NackReport { id, requeue, reply } => {
                let _ = reply.send(state.nack_report(id, requeue));
            }
            QueueCmd::Forget { message_id } => {
                state.forget_message(message_id.as_str());
            }
            QueueCmd::SetArgs { args } => {
                state.args = args;
            }
            QueueCmd::DlxResolved {
                qm,
                outcome,
                on_fail,
            } => {
                state.apply_dlx_resolved(qm, outcome, on_fail);
            }
            QueueCmd::RegisterConsumer {
                session,
                no_ack,
                exclusive,
                initial_credit,
                deliver_tx,
                priority,
                reply,
            } => {
                let res = state.register_consumer(
                    session,
                    no_ack,
                    exclusive,
                    initial_credit,
                    deliver_tx,
                    priority,
                );
                let _ = reply.send(res);
            }
            QueueCmd::AddCredit { session, credit } => {
                state.add_credit(session, credit);
            }
            QueueCmd::SetCredit { session, credit } => {
                state.set_credit(session, credit);
            }
            QueueCmd::UnregisterConsumer {
                session,
                requeue,
                reply,
            } => {
                state.remove_consumer(session, requeue);
                let _ = reply.send(());
            }
            QueueCmd::RequeueUnacked { sessions, reply } => {
                if sessions.is_empty() {
                    state.requeue_all_unacked();
                } else {
                    state.requeue_sessions(&sessions);
                }
                let _ = reply.send(());
            }
            QueueCmd::Purge { reply } => {
                let _ = reply.send(state.purge());
            }
            QueueCmd::Stats { reply } => {
                let _ = reply.send(state.stats());
            }
            QueueCmd::Touch { reply } => {
                state.touch_used();
                let _ = reply.send(());
            }
            QueueCmd::Shutdown { reply } => {
                debug!(vhost = %key.vhost, queue = %key.name, "queue actor shutting down");
                if let Some((started, handle)) = interval_fsync.take() {
                    let joined = handle.await;
                    state.finish_interval_fsync(joined, started);
                }
                let res = state.shutdown_flush().await;
                if let Err(ref e) = res {
                    error!(
                        vhost = %key.vhost,
                        queue = %key.name,
                        error = %e,
                        "queue shutdown fsync failed"
                    );
                    metrics::counter!("queueforge_shutdown_fsync_errors_total").increment(1);
                }
                let _ = reply.send(res);
                break;
            }
            #[cfg(test)]
            QueueCmd::TestPanic => {
                panic!("queue actor test panic: {}/{}", key.vhost, key.name);
            }
            #[cfg(test)]
            QueueCmd::TestDeferredAppends { reply } => {
                let _ = reply.send(state.deferred_appends.len());
            }
        }
        if state.pending_fsync {
            let _ = state.fsync_now().await;
        }
        trace!(vhost = %key.vhost, queue = %key.name, "queue actor handled command");
    }

    // Drain remaining commands so waiters are not stuck.
    rx.close();
    while let Some(cmd) = rx.recv().await {
        match cmd {
            QueueCmd::Enqueue { reply, .. } => {
                let _ = reply.send(Err(Error::Unavailable(format!(
                    "queue {}/{} shutting down",
                    key.vhost, key.name
                ))));
            }
            QueueCmd::Deliver { reply, .. } => {
                let _ = reply.send(None);
            }
            QueueCmd::Get { reply, .. } => {
                let _ = reply.send(None);
            }
            QueueCmd::RegisterConsumer { reply, .. } => {
                let _ = reply.send(Err(Error::Unavailable(format!(
                    "queue {}/{} shutting down",
                    key.vhost, key.name
                ))));
            }
            QueueCmd::UnregisterConsumer { reply, .. } => {
                let _ = reply.send(());
            }
            QueueCmd::RequeueUnacked { reply, .. } => {
                let _ = reply.send(());
            }
            QueueCmd::Purge { reply } => {
                let _ = reply.send(0);
            }
            QueueCmd::Stats { reply } => {
                let _ = reply.send(state.stats());
            }
            QueueCmd::Touch { reply } => {
                let _ = reply.send(());
            }
            QueueCmd::Shutdown { reply } => {
                // Actor already flushed; late Shutdown is success.
                let _ = reply.send(Ok(()));
            }
            QueueCmd::Ack { .. }
            | QueueCmd::AckReport { .. }
            | QueueCmd::Nack { .. }
            | QueueCmd::NackReport { .. }
            | QueueCmd::Forget { .. }
            | QueueCmd::SettleDelivered { .. }
            | QueueCmd::AddCredit { .. }
            | QueueCmd::SetCredit { .. }
            | QueueCmd::SetArgs { .. }
            | QueueCmd::DlxResolved { .. } => {}
            #[cfg(test)]
            QueueCmd::TestPanic | QueueCmd::TestDeferredAppends { .. } => {}
        }
    }

    warn!(vhost = %key.vhost, queue = %key.name, "queue actor stopped");
}

/// Convenience: run with empty bootstrap (tests / transient).
#[cfg(test)]
async fn run_simple(key: QueueKey, rx: mpsc::Receiver<QueueCmd>, memory: Arc<MemoryTracker>) {
    let info = Arc::new(QueueInfo::new(
        key.clone(),
        &crate::queue::QueueDeclareOpts::default(),
    ));
    run(
        key,
        rx,
        memory,
        None,
        info,
        QueueActorBootstrap::new_empty(false),
    )
    .await;
}

/// Run with custom args (unit tests for TTL / overflow).
#[cfg(test)]
async fn run_with_args(
    key: QueueKey,
    rx: mpsc::Receiver<QueueCmd>,
    memory: Arc<MemoryTracker>,
    args: QueueArgs,
) {
    let info = Arc::new(QueueInfo::new(
        key.clone(),
        &crate::queue::QueueDeclareOpts::default(),
    ));
    let mut boot = QueueActorBootstrap::new_empty(false);
    boot.args = args;
    run(key, rx, memory, None, info, boot).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::MemoryTracker;
    use bytes::Bytes;
    use compact_str::CompactString;
    use tokio::sync::oneshot;

    fn sample_msg(body: &[u8]) -> Arc<Message> {
        Arc::new(Message {
            exchange: CompactString::from(""),
            routing_key: CompactString::from("q"),
            body: Bytes::copy_from_slice(body),
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
            headers: Default::default(),
        })
    }

    fn sample_msg_exp(body: &[u8], exp_ms: &str) -> Arc<Message> {
        let mut m = (*sample_msg(body)).clone();
        m.expiration = Some(CompactString::from(exp_ms));
        Arc::new(m)
    }

    fn sample_msg_prio(body: &[u8], priority: u8) -> Arc<Message> {
        let mut m = (*sample_msg(body)).clone();
        m.priority = Some(priority);
        Arc::new(m)
    }

    async fn spawn_actor(name: &str) -> (mpsc::Sender<QueueCmd>, tokio::task::JoinHandle<()>) {
        let key = QueueKey::new("/", name);
        let memory = MemoryTracker::shared();
        let (tx, rx) = mpsc::channel(16);
        let actor = tokio::spawn(run_simple(key, rx, Arc::clone(&memory)));
        (tx, actor)
    }

    async fn spawn_actor_args(
        name: &str,
        args: QueueArgs,
    ) -> (mpsc::Sender<QueueCmd>, tokio::task::JoinHandle<()>) {
        let key = QueueKey::new("/", name);
        let memory = MemoryTracker::shared();
        let (tx, rx) = mpsc::channel(16);
        let actor = tokio::spawn(run_with_args(key, rx, Arc::clone(&memory), args));
        (tx, actor)
    }

    async fn enqueue(tx: &mpsc::Sender<QueueCmd>, msg: Arc<Message>) -> EnqueueCompletion {
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(QueueCmd::Enqueue {
            msg,
            reply: reply_tx,
        })
        .await
        .unwrap();
        reply_rx.await.unwrap().unwrap()
    }

    async fn stats(tx: &mpsc::Sender<QueueCmd>) -> QueueStats {
        let (stats_tx, stats_rx) = oneshot::channel();
        tx.send(QueueCmd::Stats { reply: stats_tx }).await.unwrap();
        stats_rx.await.unwrap()
    }

    async fn shutdown(tx: mpsc::Sender<QueueCmd>, actor: tokio::task::JoinHandle<()>) {
        let (shut_tx, shut_rx) = oneshot::channel();
        tx.send(QueueCmd::Shutdown { reply: shut_tx })
            .await
            .unwrap();
        shut_rx.await.unwrap().expect("shutdown ok");
        actor.await.unwrap();
    }

    struct SlowLog {
        delay: Duration,
        entered: Arc<std::sync::atomic::AtomicBool>,
        released: Arc<std::sync::atomic::AtomicBool>,
        appends: Arc<std::sync::atomic::AtomicU64>,
    }

    impl DurableQueueLog for SlowLog {
        fn append_enqueue(
            &mut self,
            _offset: QueueOffset,
            _msg: &Message,
        ) -> crate::error::Result<()> {
            self.appends
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
        fn acknowledge(&mut self, _offset: QueueOffset) -> crate::error::Result<()> {
            Ok(())
        }
        fn fsync(&mut self) -> crate::error::Result<QueueOffset> {
            self.entered
                .store(true, std::sync::atomic::Ordering::SeqCst);
            std::thread::sleep(self.delay);
            self.released
                .store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(QueueOffset(u64::MAX))
        }
        fn durable_offset(&self) -> QueueOffset {
            QueueOffset(0)
        }
        fn ack_watermark(&self) -> QueueOffset {
            QueueOffset(0)
        }
        fn meta_dirty(&self) -> bool {
            false
        }
        fn compact(&mut self) -> crate::error::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn slow_durable_fsync_does_not_block_other_queue() {
        let entered = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let key = QueueKey::new("/", "slow");
        let memory = MemoryTracker::shared();
        let info = Arc::new(QueueInfo::new(
            key.clone(),
            &crate::queue::QueueDeclareOpts {
                durable: true,
                ..crate::queue::QueueDeclareOpts::default()
            },
        ));
        let mut boot = QueueActorBootstrap::new_empty(true).with_log(
            Box::new(SlowLog {
                delay: Duration::from_millis(300),
                entered: Arc::clone(&entered),
                released: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                appends: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            }),
            DurabilityPolicy {
                policy: FsyncPolicy::Always,
                interval: Duration::from_millis(100),
                every_n_messages: 1,
            },
        );
        boot.durable = true;
        let (tx_slow, rx_slow) = mpsc::channel(8);
        let slow = tokio::spawn(run(key, rx_slow, Arc::clone(&memory), None, info, boot));

        let (tx_fast, fast_actor) = spawn_actor("fast").await;
        let mut msg = (*sample_msg(b"durable")).clone();
        msg.persistent = true;
        let slow_done = enqueue(&tx_slow, Arc::new(msg)).await;
        let started = std::time::Instant::now();
        while !entered.load(std::sync::atomic::Ordering::SeqCst) {
            if started.elapsed() > Duration::from_secs(2) {
                panic!("slow fsync did not start");
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let fast_done = enqueue(&tx_fast, sample_msg(b"transient")).await;
        let fast_at = std::time::Instant::now();
        fast_done.durable_done.await.unwrap().unwrap();
        assert!(
            fast_at.elapsed() < Duration::from_millis(150),
            "transient enqueue waited on the other queue's fsync"
        );
        slow_done.durable_done.await.unwrap().unwrap();
        shutdown(tx_slow, slow).await;
        shutdown(tx_fast, fast_actor).await;
    }

    /// Interval fsync still runs. The confirm returns after the buffered append.
    #[tokio::test]
    async fn every_n_ms_confirm_returns_before_the_interval_fsync() {
        let entered = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let key = QueueKey::new("/", "group");
        let memory = MemoryTracker::shared();
        let info = Arc::new(QueueInfo::new(
            key.clone(),
            &crate::queue::QueueDeclareOpts {
                durable: true,
                ..crate::queue::QueueDeclareOpts::default()
            },
        ));
        let mut boot = QueueActorBootstrap::new_empty(true).with_log(
            Box::new(SlowLog {
                delay: Duration::from_millis(80),
                entered: Arc::clone(&entered),
                released: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                appends: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            }),
            DurabilityPolicy {
                policy: FsyncPolicy::EveryNMs,
                interval: Duration::from_millis(40),
                every_n_messages: 1,
            },
        );
        boot.durable = true;
        let (tx, rx) = mpsc::channel(8);
        let actor = tokio::spawn(run(key, rx, memory, None, info, boot));

        let mut msg = (*sample_msg(b"durable")).clone();
        msg.persistent = true;
        let done = enqueue(&tx, Arc::new(msg)).await;
        let started = std::time::Instant::now();
        done.durable_done.await.unwrap().unwrap();
        assert!(
            started.elapsed() < Duration::from_millis(30),
            "publisher confirm waited for the group-commit fsync"
        );
        assert!(
            !entered.load(std::sync::atomic::Ordering::SeqCst),
            "interval fsync ran before the confirm returned"
        );

        let wait = std::time::Instant::now();
        while !entered.load(std::sync::atomic::Ordering::SeqCst) {
            if wait.elapsed() > Duration::from_secs(2) {
                panic!("group-commit timer did not fsync");
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        shutdown(tx, actor).await;
    }

    /// A confirm issued while the interval fsync is blocked does not wait for it.
    #[tokio::test]
    async fn interval_fsync_does_not_queue_the_next_confirm() {
        let entered = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let released = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let appends = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let key = QueueKey::new("/", "inflight");
        let memory = MemoryTracker::shared();
        let info = Arc::new(QueueInfo::new(
            key.clone(),
            &crate::queue::QueueDeclareOpts {
                durable: true,
                ..crate::queue::QueueDeclareOpts::default()
            },
        ));
        let mut boot = QueueActorBootstrap::new_empty(true).with_log(
            Box::new(SlowLog {
                delay: Duration::from_millis(400),
                entered: Arc::clone(&entered),
                released: Arc::clone(&released),
                appends: Arc::clone(&appends),
            }),
            DurabilityPolicy {
                policy: FsyncPolicy::EveryNMs,
                interval: Duration::from_millis(30),
                every_n_messages: 1,
            },
        );
        boot.durable = true;
        let (tx, rx) = mpsc::channel(8);
        let actor = tokio::spawn(run(key, rx, memory, None, info, boot));

        let mut first = (*sample_msg(b"first")).clone();
        first.persistent = true;
        let first_done = enqueue(&tx, Arc::new(first)).await;
        first_done.durable_done.await.unwrap().unwrap();
        let wait = std::time::Instant::now();
        while !entered.load(std::sync::atomic::Ordering::SeqCst) {
            if wait.elapsed() > Duration::from_secs(2) {
                panic!("interval fsync did not start");
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(
            !released.load(std::sync::atomic::Ordering::SeqCst),
            "fsync returned before the second publish"
        );

        let mut second = (*sample_msg(b"second")).clone();
        second.persistent = true;
        let started = std::time::Instant::now();
        let second_done = enqueue(&tx, Arc::new(second)).await;
        second_done.durable_done.await.unwrap().unwrap();
        assert!(
            started.elapsed() < Duration::from_millis(80),
            "confirm waited for the in-flight interval fsync"
        );
        assert!(
            !released.load(std::sync::atomic::Ordering::SeqCst),
            "interval fsync returned before the confirm"
        );
        assert_eq!(
            appends.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "the second durable enqueue was written before the blocked fsync returned"
        );

        // Keep a command queued for the whole fsync. The join has to flush the
        // deferred append anyway, and the following command must observe that.
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flood_tx = tx.clone();
        loop {
            let (reply_tx, _reply_rx) = oneshot::channel();
            if flood_tx
                .try_send(QueueCmd::Stats { reply: reply_tx })
                .is_err()
            {
                break;
            }
        }
        let flood_stop = Arc::clone(&stop);
        let flood = std::thread::spawn(move || {
            while !flood_stop.load(std::sync::atomic::Ordering::SeqCst) {
                let (reply_tx, _reply_rx) = oneshot::channel();
                if flood_tx
                    .blocking_send(QueueCmd::Stats { reply: reply_tx })
                    .is_err()
                {
                    break;
                }
            }
        });
        let flushed = std::time::Instant::now();
        while appends.load(std::sync::atomic::Ordering::SeqCst) < 2 {
            if flushed.elapsed() > Duration::from_secs(2) {
                panic!("commands in the mailbox prevented the deferred append from flushing");
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(
            released.load(std::sync::atomic::Ordering::SeqCst),
            "deferred append flushed before the interval fsync returned"
        );
        stop.store(true, std::sync::atomic::Ordering::SeqCst);
        tokio::task::spawn_blocking(move || flood.join())
            .await
            .unwrap()
            .unwrap();
        let (snap_tx, snap_rx) = oneshot::channel();
        tx.send(QueueCmd::TestDeferredAppends { reply: snap_tx })
            .await
            .unwrap();
        let still_deferred = snap_rx.await.unwrap();
        assert_eq!(
            still_deferred, 0,
            "following command ran while the deferred append was still unflushed"
        );
        shutdown(tx, actor).await;
    }

    #[tokio::test]
    async fn recovered_past_deadline_is_due_immediately() {
        let (tx, actor) = spawn_actor("ttl").await;
        let mut msg = (*sample_msg(b"old")).clone();
        msg.expires_unix_ms = Some(1);
        let _ = enqueue(&tx, Arc::new(msg)).await;
        let stats = stats(&tx).await;
        assert_eq!(
            stats.messages_ready, 0,
            "a deadline already in the past must not sit in ready"
        );
        shutdown(tx, actor).await;
    }

    #[tokio::test]
    async fn enqueue_deliver_ack_roundtrip() {
        let (tx, actor) = spawn_actor("t").await;

        let completion = enqueue(&tx, sample_msg(b"hi")).await;
        assert!(completion.durable_done.await.unwrap().is_ok());

        let (dtx, mut drx) = mpsc::channel(64);
        let (reg_tx, reg_rx) = oneshot::channel();
        tx.send(QueueCmd::RegisterConsumer {
            session: ConsumerSessionId(1),
            no_ack: false,
            exclusive: false,
            priority: 0,
            initial_credit: Some(1),
            deliver_tx: dtx,
            reply: reg_tx,
        })
        .await
        .unwrap();
        reg_rx.await.unwrap().unwrap();

        let delivery = tokio::time::timeout(std::time::Duration::from_secs(1), drx.recv())
            .await
            .expect("timeout")
            .expect("delivery");
        assert_eq!(delivery.message.message.body.as_ref(), b"hi");

        tx.send(QueueCmd::Ack {
            id: delivery.delivery_id,
            multiple_to: None,
        })
        .await
        .unwrap();

        let s = stats(&tx).await;
        assert_eq!(s.messages_ready, 0);
        assert_eq!(s.messages_unacked, 0);
        assert_eq!(s.consumer_count, 1);

        let (shut_tx, shut_rx) = oneshot::channel();
        tx.send(QueueCmd::Shutdown { reply: shut_tx })
            .await
            .unwrap();
        shut_rx.await.unwrap().expect("shutdown ok");
        actor.await.unwrap();
    }

    #[tokio::test]
    async fn nack_requeue_sets_redelivered() {
        let (tx, actor) = spawn_actor("t2").await;
        let _ = enqueue(&tx, sample_msg(b"x")).await;

        let (dtx, mut drx) = mpsc::channel(64);
        let (reg_tx, reg_rx) = oneshot::channel();
        tx.send(QueueCmd::RegisterConsumer {
            session: ConsumerSessionId(7),
            no_ack: false,
            exclusive: false,
            priority: 0,
            initial_credit: Some(10),
            deliver_tx: dtx,
            reply: reg_tx,
        })
        .await
        .unwrap();
        reg_rx.await.unwrap().unwrap();

        let d1 = drx.recv().await.unwrap();
        assert!(!d1.message.message.redelivered);

        tx.send(QueueCmd::Nack {
            id: d1.delivery_id,
            requeue: true,
        })
        .await
        .unwrap();
        tx.send(QueueCmd::AddCredit {
            session: ConsumerSessionId(7),
            credit: 1,
        })
        .await
        .unwrap();

        let d2 = tokio::time::timeout(std::time::Duration::from_secs(1), drx.recv())
            .await
            .expect("timeout")
            .expect("redelivery");
        assert!(d2.message.message.redelivered);

        let (shut_tx, shut_rx) = oneshot::channel();
        tx.send(QueueCmd::Shutdown { reply: shut_tx })
            .await
            .unwrap();
        shut_rx.await.unwrap().expect("shutdown ok");
        actor.await.unwrap();
    }

    #[tokio::test]
    async fn zero_credit_holds_ready_until_add_credit() {
        let (tx, actor) = spawn_actor("t3").await;
        let _ = enqueue(&tx, sample_msg(b"held")).await;

        let (dtx, mut drx) = mpsc::channel(64);
        let (reg_tx, reg_rx) = oneshot::channel();
        tx.send(QueueCmd::RegisterConsumer {
            session: ConsumerSessionId(3),
            no_ack: false,
            exclusive: false,
            priority: 0,
            initial_credit: Some(0),
            deliver_tx: dtx,
            reply: reg_tx,
        })
        .await
        .unwrap();
        reg_rx.await.unwrap().unwrap();

        let no_delivery =
            tokio::time::timeout(std::time::Duration::from_millis(50), drx.recv()).await;
        assert!(no_delivery.is_err(), "zero credit must not deliver");

        let s = stats(&tx).await;
        assert_eq!(s.messages_ready, 1);
        assert_eq!(s.messages_unacked, 0);

        tx.send(QueueCmd::AddCredit {
            session: ConsumerSessionId(3),
            credit: 1,
        })
        .await
        .unwrap();

        let d = tokio::time::timeout(std::time::Duration::from_secs(1), drx.recv())
            .await
            .expect("timeout")
            .expect("delivery after credit");
        assert_eq!(d.message.message.body.as_ref(), b"held");

        let (shut_tx, shut_rx) = oneshot::channel();
        tx.send(QueueCmd::Shutdown { reply: shut_tx })
            .await
            .unwrap();
        shut_rx.await.unwrap().expect("shutdown ok");
        actor.await.unwrap();
    }

    #[tokio::test]
    async fn unlimited_credit_delivers() {
        let (tx, actor) = spawn_actor("t4").await;
        let _ = enqueue(&tx, sample_msg(b"go")).await;

        let (dtx, mut drx) = mpsc::channel(64);
        let (reg_tx, reg_rx) = oneshot::channel();
        tx.send(QueueCmd::RegisterConsumer {
            session: ConsumerSessionId(4),
            no_ack: false,
            exclusive: false,
            priority: 0,
            initial_credit: None,
            deliver_tx: dtx,
            reply: reg_tx,
        })
        .await
        .unwrap();
        reg_rx.await.unwrap().unwrap();

        let d = tokio::time::timeout(std::time::Duration::from_secs(1), drx.recv())
            .await
            .expect("timeout")
            .expect("delivery");
        assert_eq!(d.message.message.body.as_ref(), b"go");

        shutdown(tx, actor).await;
    }

    #[tokio::test(start_paused = true)]
    async fn message_ttl_expires_ready_only() {
        let args = QueueArgs {
            message_ttl_ms: Some(100),
            ..QueueArgs::default()
        };
        let (tx, actor) = spawn_actor_args("ttl", args).await;
        let _ = enqueue(&tx, sample_msg(b"die")).await;
        assert_eq!(stats(&tx).await.messages_ready, 1);

        tokio::time::advance(Duration::from_millis(150)).await;
        // Yield so the actor can process the timer.
        for _ in 0..10 {
            tokio::task::yield_now().await;
            if stats(&tx).await.messages_ready == 0 {
                break;
            }
            tokio::time::advance(Duration::from_millis(10)).await;
        }
        assert_eq!(stats(&tx).await.messages_ready, 0);

        shutdown(tx, actor).await;
    }

    #[tokio::test(start_paused = true)]
    async fn per_message_expiration_property() {
        let (tx, actor) = spawn_actor("exp").await;
        let _ = enqueue(&tx, sample_msg_exp(b"soon", "50")).await;
        assert_eq!(stats(&tx).await.messages_ready, 1);

        tokio::time::advance(Duration::from_millis(80)).await;
        for _ in 0..10 {
            tokio::task::yield_now().await;
            if stats(&tx).await.messages_ready == 0 {
                break;
            }
            tokio::time::advance(Duration::from_millis(10)).await;
        }
        assert_eq!(stats(&tx).await.messages_ready, 0);

        shutdown(tx, actor).await;
    }

    #[tokio::test(start_paused = true)]
    async fn ttl_does_not_expire_unacked() {
        let args = QueueArgs {
            message_ttl_ms: Some(50),
            ..QueueArgs::default()
        };
        let (tx, actor) = spawn_actor_args("unack-ttl", args).await;
        let _ = enqueue(&tx, sample_msg(b"held")).await;

        let (dtx, mut drx) = mpsc::channel(64);
        let (reg_tx, reg_rx) = oneshot::channel();
        tx.send(QueueCmd::RegisterConsumer {
            session: ConsumerSessionId(1),
            no_ack: false,
            exclusive: false,
            priority: 0,
            initial_credit: Some(1),
            deliver_tx: dtx,
            reply: reg_tx,
        })
        .await
        .unwrap();
        reg_rx.await.unwrap().unwrap();
        let d = drx.recv().await.unwrap();
        assert_eq!(stats(&tx).await.messages_unacked, 1);

        // Past TTL while unacked — must stay unacked.
        tokio::time::advance(Duration::from_millis(200)).await;
        for _ in 0..5 {
            tokio::task::yield_now().await;
        }
        let s = stats(&tx).await;
        assert_eq!(s.messages_unacked, 1);
        assert_eq!(s.messages_ready, 0);

        // Ack cleans up.
        tx.send(QueueCmd::Ack {
            id: d.delivery_id,
            multiple_to: None,
        })
        .await
        .unwrap();
        assert_eq!(stats(&tx).await.messages_unacked, 0);

        shutdown(tx, actor).await;
    }

    #[tokio::test]
    async fn max_length_drop_head() {
        let args = QueueArgs {
            max_length: Some(2),
            overflow: OverflowPolicy::DropHead,
            ..QueueArgs::default()
        };
        let (tx, actor) = spawn_actor_args("ml", args).await;
        let _ = enqueue(&tx, sample_msg(b"a")).await;
        let _ = enqueue(&tx, sample_msg(b"b")).await;
        let _ = enqueue(&tx, sample_msg(b"c")).await; // drops "a"
        assert_eq!(stats(&tx).await.messages_ready, 2);

        // Remaining should be b, c (FIFO after drop-head).
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(QueueCmd::Get {
            no_ack: true,
            reply: reply_tx,
        })
        .await
        .unwrap();
        let (_, qm, _) = reply_rx.await.unwrap().unwrap();
        assert_eq!(qm.message.body.as_ref(), b"b");

        shutdown(tx, actor).await;
    }

    #[tokio::test]
    async fn priority_p9_before_p0() {
        let args = QueueArgs {
            max_priority: Some(9),
            ..QueueArgs::default()
        };
        let (tx, actor) = spawn_actor_args("prio", args).await;
        // Enqueue low then high — deliver must prefer p=9.
        let _ = enqueue(&tx, sample_msg_prio(b"p0", 0)).await;
        let _ = enqueue(&tx, sample_msg_prio(b"p9", 9)).await;
        let _ = enqueue(&tx, sample_msg_prio(b"p5", 5)).await;

        let s = stats(&tx).await;
        assert_eq!(s.messages_ready, 3);
        assert_eq!(s.max_priority, Some(9));
        let bands = s.ready_by_priority.expect("priority bands");
        assert_eq!(bands.len(), 10);
        assert_eq!(bands[0], 1);
        assert_eq!(bands[5], 1);
        assert_eq!(bands[9], 1);

        async fn get_body(tx: &mpsc::Sender<QueueCmd>) -> Vec<u8> {
            let (reply_tx, reply_rx) = oneshot::channel();
            tx.send(QueueCmd::Get {
                no_ack: true,
                reply: reply_tx,
            })
            .await
            .unwrap();
            reply_rx.await.unwrap().unwrap().1.message.body.to_vec()
        }

        assert_eq!(get_body(&tx).await, b"p9");
        assert_eq!(get_body(&tx).await, b"p5");
        assert_eq!(get_body(&tx).await, b"p0");

        shutdown(tx, actor).await;
    }

    #[tokio::test]
    async fn priority_drop_head_from_lowest() {
        let args = QueueArgs {
            max_priority: Some(9),
            max_length: Some(2),
            overflow: OverflowPolicy::DropHead,
            ..QueueArgs::default()
        };
        let (tx, actor) = spawn_actor_args("prio-drop", args).await;
        let _ = enqueue(&tx, sample_msg_prio(b"low", 0)).await;
        let _ = enqueue(&tx, sample_msg_prio(b"high", 9)).await;
        // Exceeds max-length=2 → drop lowest-priority oldest ("low").
        let _ = enqueue(&tx, sample_msg_prio(b"mid", 5)).await;
        assert_eq!(stats(&tx).await.messages_ready, 2);

        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(QueueCmd::Get {
            no_ack: true,
            reply: reply_tx,
        })
        .await
        .unwrap();
        let (_, qm, _) = reply_rx.await.unwrap().unwrap();
        assert_eq!(
            qm.message.body.as_ref(),
            b"high",
            "deliver still prefers highest remaining priority"
        );

        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(QueueCmd::Get {
            no_ack: true,
            reply: reply_tx,
        })
        .await
        .unwrap();
        let (_, qm, _) = reply_rx.await.unwrap().unwrap();
        assert_eq!(qm.message.body.as_ref(), b"mid");

        shutdown(tx, actor).await;
    }

    #[tokio::test]
    async fn priority_recovery_relanes_by_offset() {
        // Simulate WAL recovery: flat offset-ordered ready + max_priority args.
        let args = QueueArgs {
            max_priority: Some(9),
            ..QueueArgs::default()
        };
        let mut boot = QueueActorBootstrap::new_empty(false);
        boot.args = args;
        let mut ready = std::collections::VecDeque::new();
        ready.push_back(QueueMessage::new(
            QueueOffset(1),
            sample_msg_prio(b"first-low", 0),
        ));
        ready.push_back(QueueMessage::new(
            QueueOffset(2),
            sample_msg_prio(b"high", 9),
        ));
        ready.push_back(QueueMessage::new(
            QueueOffset(3),
            sample_msg_prio(b"second-low", 0),
        ));
        boot.ready = ready;
        boot.next_offset = 4;

        let key = QueueKey::new("/", "rec-prio");
        let memory = MemoryTracker::shared();
        let (tx, rx) = mpsc::channel(16);
        let info = Arc::new(QueueInfo::new(
            key.clone(),
            &crate::queue::QueueDeclareOpts::default(),
        ));
        let actor = tokio::spawn(run(key, rx, Arc::clone(&memory), None, info, boot));

        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(QueueCmd::Get {
            no_ack: true,
            reply: reply_tx,
        })
        .await
        .unwrap();
        assert_eq!(
            reply_rx.await.unwrap().unwrap().1.message.body.as_ref(),
            b"high"
        );

        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(QueueCmd::Get {
            no_ack: true,
            reply: reply_tx,
        })
        .await
        .unwrap();
        assert_eq!(
            reply_rx.await.unwrap().unwrap().1.message.body.as_ref(),
            b"first-low",
            "same priority preserves offset FIFO after re-lane"
        );

        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(QueueCmd::Get {
            no_ack: true,
            reply: reply_tx,
        })
        .await
        .unwrap();
        assert_eq!(
            reply_rx.await.unwrap().unwrap().1.message.body.as_ref(),
            b"second-low"
        );

        shutdown(tx, actor).await;
    }

    #[tokio::test]
    async fn max_length_reject_publish() {
        let args = QueueArgs {
            max_length: Some(1),
            overflow: OverflowPolicy::RejectPublish,
            ..QueueArgs::default()
        };
        let (tx, actor) = spawn_actor_args("rj", args).await;
        let _ = enqueue(&tx, sample_msg(b"a")).await;

        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(QueueCmd::Enqueue {
            msg: sample_msg(b"b"),
            reply: reply_tx,
        })
        .await
        .unwrap();
        let err = reply_rx.await.unwrap().unwrap_err();
        assert!(matches!(err, Error::PreconditionFailed(_)));
        assert_eq!(stats(&tx).await.messages_ready, 1);

        shutdown(tx, actor).await;
    }

    #[tokio::test]
    async fn max_length_bytes_drop_head() {
        let args = QueueArgs {
            max_length_bytes: Some(4),
            overflow: OverflowPolicy::DropHead,
            ..QueueArgs::default()
        };
        let (tx, actor) = spawn_actor_args("mlb", args).await;
        let _ = enqueue(&tx, sample_msg(b"12")).await; // 2 bytes
        let _ = enqueue(&tx, sample_msg(b"34")).await; // 2 bytes → total 4
        let _ = enqueue(&tx, sample_msg(b"56")).await; // need room → drop first
        assert_eq!(stats(&tx).await.messages_ready, 2);

        shutdown(tx, actor).await;
    }

    #[tokio::test]
    async fn nack_no_requeue_without_dlx_drops() {
        let (tx, actor) = spawn_actor("nack-drop").await;
        let _ = enqueue(&tx, sample_msg(b"x")).await;

        let (dtx, mut drx) = mpsc::channel(64);
        let (reg_tx, reg_rx) = oneshot::channel();
        tx.send(QueueCmd::RegisterConsumer {
            session: ConsumerSessionId(1),
            no_ack: false,
            exclusive: false,
            priority: 0,
            initial_credit: Some(1),
            deliver_tx: dtx,
            reply: reg_tx,
        })
        .await
        .unwrap();
        reg_rx.await.unwrap().unwrap();
        let d = drx.recv().await.unwrap();

        tx.send(QueueCmd::Nack {
            id: d.delivery_id,
            requeue: false,
        })
        .await
        .unwrap();
        // Let actor process.
        tokio::task::yield_now().await;
        let s = stats(&tx).await;
        assert_eq!(s.messages_ready, 0);
        assert_eq!(s.messages_unacked, 0);

        shutdown(tx, actor).await;
    }

    /// Overflow + configured DLX with no routes must not hang the actor (Issue 2).
    #[tokio::test]
    async fn overflow_dlx_fail_does_not_spin() {
        use crate::domain::{Exchange, ExchangeType};
        use crate::queue::dlx::DlxRouter;
        use crate::queue::meta::NoopMetaStore;
        use crate::queue::registry::QueueRegistry;
        use crate::router::ExchangeRouter;
        use std::time::Duration as StdDuration;

        let reg = Arc::new(QueueRegistry::new(
            Arc::new(NoopMetaStore),
            MemoryTracker::shared(),
        ));
        let router = Arc::new(ExchangeRouter::new());
        // DLX exchange exists but has zero bindings → publish Err.
        router.put_exchange(Exchange::new("/", "dlx", ExchangeType::Fanout));
        let dlx = Arc::new(DlxRouter::new(router, Arc::downgrade(&reg)));

        let args = QueueArgs {
            max_length: Some(1),
            overflow: OverflowPolicy::DropHead,
            dead_letter_exchange: Some(CompactString::from("dlx")),
            ..QueueArgs::default()
        };
        let mut boot = QueueActorBootstrap::new_empty(false);
        boot.args = args;
        boot.dlx = Some(dlx);

        let key = QueueKey::new("/", "ov-dlx");
        let memory = MemoryTracker::shared();
        let (tx, rx) = mpsc::channel(16);
        let info = Arc::new(QueueInfo::new(
            key.clone(),
            &crate::queue::QueueDeclareOpts::default(),
        ));
        let actor = tokio::spawn(run(key, rx, Arc::clone(&memory), None, info, boot));

        // Fill queue.
        let _ = enqueue(&tx, sample_msg(b"a")).await;
        // Second publish forces drop-head + failing DLX; must complete promptly.
        let done = tokio::time::timeout(StdDuration::from_secs(2), async {
            enqueue(&tx, sample_msg(b"b")).await
        })
        .await
        .expect("enqueue must not hang on DLX failure");
        let _ = done;
        // Head dropped (or pending free); at most 1 ready.
        let s = stats(&tx).await;
        assert!(s.messages_ready <= 1);

        // Actor still services stats after overflow path.
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert_eq!(stats(&tx).await.messages_ready, 1);

        shutdown(tx, actor).await;
    }

    /// TTL expiry with failing DLX must not livelock (Issue 3).
    #[tokio::test(start_paused = true)]
    async fn expire_dlx_fail_makes_progress() {
        use crate::domain::{Exchange, ExchangeType};
        use crate::queue::dlx::DlxRouter;
        use crate::queue::meta::NoopMetaStore;
        use crate::queue::registry::QueueRegistry;
        use crate::router::ExchangeRouter;

        let reg = Arc::new(QueueRegistry::new(
            Arc::new(NoopMetaStore),
            MemoryTracker::shared(),
        ));
        let router = Arc::new(ExchangeRouter::new());
        router.put_exchange(Exchange::new("/", "dlx", ExchangeType::Fanout));
        let dlx = Arc::new(DlxRouter::new(router, Arc::downgrade(&reg)));

        let args = QueueArgs {
            message_ttl_ms: Some(50),
            dead_letter_exchange: Some(CompactString::from("dlx")),
            ..QueueArgs::default()
        };
        let mut boot = QueueActorBootstrap::new_empty(false);
        boot.args = args;
        boot.dlx = Some(dlx);

        let key = QueueKey::new("/", "ttl-dlx");
        let memory = MemoryTracker::shared();
        let (tx, rx) = mpsc::channel(16);
        let info = Arc::new(QueueInfo::new(
            key.clone(),
            &crate::queue::QueueDeclareOpts::default(),
        ));
        let actor = tokio::spawn(run(key, rx, Arc::clone(&memory), None, info, boot));

        let _ = enqueue(&tx, sample_msg(b"die")).await;
        assert_eq!(stats(&tx).await.messages_ready, 1);

        tokio::time::advance(Duration::from_millis(100)).await;
        for _ in 0..20 {
            tokio::task::yield_now().await;
            if stats(&tx).await.messages_ready == 0 {
                break;
            }
            tokio::time::advance(Duration::from_millis(10)).await;
        }
        assert_eq!(
            stats(&tx).await.messages_ready,
            0,
            "expired message must leave ready even when DLX fails"
        );

        shutdown(tx, actor).await;
    }

    /// A↔B mutual DLX under overflow must not deadlock actors (Issue 1).
    #[tokio::test]
    async fn mutual_dlx_overflow_no_deadlock() {
        use crate::domain::{Binding, Exchange, ExchangeType};
        use crate::queue::dlx::DlxRouter;
        use crate::queue::meta::NoopMetaStore;
        use crate::queue::registry::{QueueDeclareOpts, QueueRegistry};
        use crate::router::ExchangeRouter;
        use std::time::Duration as StdDuration;

        let reg = Arc::new(
            QueueRegistry::new(Arc::new(NoopMetaStore), MemoryTracker::shared())
                .with_mailbox_capacity(16),
        );
        let router = Arc::new(ExchangeRouter::new());
        router.put_exchange(Exchange::new("/", "to-a", ExchangeType::Fanout));
        router.put_exchange(Exchange::new("/", "to-b", ExchangeType::Fanout));
        reg.set_dlx(Arc::new(DlxRouter::new(
            Arc::clone(&router),
            Arc::downgrade(&reg),
        )));

        let a = reg
            .declare(
                "/",
                "qa",
                QueueDeclareOpts {
                    args: QueueArgs {
                        max_length: Some(1),
                        overflow: OverflowPolicy::DropHead,
                        dead_letter_exchange: Some(CompactString::from("to-b")),
                        ..QueueArgs::default()
                    },
                    ..QueueDeclareOpts::default()
                },
            )
            .await
            .unwrap();
        let b = reg
            .declare(
                "/",
                "qb",
                QueueDeclareOpts {
                    args: QueueArgs {
                        max_length: Some(1),
                        overflow: OverflowPolicy::DropHead,
                        dead_letter_exchange: Some(CompactString::from("to-a")),
                        ..QueueArgs::default()
                    },
                    ..QueueDeclareOpts::default()
                },
            )
            .await
            .unwrap();

        router.bind(Binding::new("/", "to-a", "qa", "")).unwrap();
        router.bind(Binding::new("/", "to-b", "qb", "")).unwrap();

        // Fill both queues.
        for (h, body) in [(&a.handle, b"a1"), (&b.handle, b"b1")] {
            let (rtx, rrx) = oneshot::channel();
            h.tx.send(QueueCmd::Enqueue {
                msg: sample_msg(body),
                reply: rtx,
            })
            .await
            .unwrap();
            let _ = rrx.await.unwrap().unwrap();
        }

        // Concurrent overflow enqueues that force A↔B DLX handoff.
        let ha = a.handle.clone();
        let hb = b.handle.clone();
        let t1 = tokio::spawn(async move {
            for i in 0..5u8 {
                let (rtx, rrx) = oneshot::channel();
                ha.tx
                    .send(QueueCmd::Enqueue {
                        msg: sample_msg(&[b'A', i]),
                        reply: rtx,
                    })
                    .await
                    .unwrap();
                let _ = rrx.await;
            }
        });
        let t2 = tokio::spawn(async move {
            for i in 0..5u8 {
                let (rtx, rrx) = oneshot::channel();
                hb.tx
                    .send(QueueCmd::Enqueue {
                        msg: sample_msg(&[b'B', i]),
                        reply: rtx,
                    })
                    .await
                    .unwrap();
                let _ = rrx.await;
            }
        });

        tokio::time::timeout(StdDuration::from_secs(5), async {
            let _ = t1.await;
            let _ = t2.await;
        })
        .await
        .expect("mutual DLX must not deadlock queue actors");

        // Still responsive.
        let (stx, srx) = oneshot::channel();
        a.handle
            .tx
            .send(QueueCmd::Stats { reply: stx })
            .await
            .unwrap();
        let _ = srx.await.unwrap();
    }
}
