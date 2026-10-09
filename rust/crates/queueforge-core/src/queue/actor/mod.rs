//! Per-queue actor loop: owns ready/unacked state, consumer credit, TTL heap,
//! overflow policies, optional WAL, and dead-letter routing.
//! The select loop stays here. Enqueue, fsync, dead-letter, recovery, delivery, and settle live in sibling modules.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::Arc;
use tokio::time::Instant;

use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, trace, warn};

use super::args::QueueArgs;
use super::cmd::{
    ConsumerDeliveryId, ConsumerSessionId, Message, QueueCmd, QueueDelivery, QueueMessage,
    QueueOffset,
};
use super::dlx::DlxRouter;
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
    /// Ready set: classic deque, quorum index, or priority lanes.
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
    /// The previous confirm was alone through its fsync. The next single waiter
    /// skips the 1 ms quiet window. A second command already queued clears it.
    immediate_lone: bool,
    /// Notify registry when `x-expires` fires.
    expired_tx: Option<mpsc::UnboundedSender<QueueKey>>,
    /// Self-command channel for async DLX completions (unbounded; avoids mailbox deadlock).
    internal_tx: mpsc::UnboundedSender<QueueCmd>,
    /// Ready and unacked gauges. Created on the first update so a burst does not
    /// rebuild label strings per message.
    depth_gauges: Option<DepthGauges>,
}

mod dead_letter;
mod deliver;
mod enqueue;
mod expiry;
mod fsync;
mod recovery;
mod settle;

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

/// Prometheus handles for this queue. Label strings are allocated once.
struct DepthGauges {
    ready: metrics::Gauge,
    unacked: metrics::Gauge,
    rabbit_ready: metrics::Gauge,
    rabbit_unacked: metrics::Gauge,
    rabbit_total: metrics::Gauge,
    rabbit_consumers: metrics::Gauge,
}

/// Take one command already sitting in a mailbox, external first.
fn take_ready_cmd(
    rx: &mut mpsc::Receiver<QueueCmd>,
    internal_rx: &mut mpsc::UnboundedReceiver<QueueCmd>,
) -> Option<QueueCmd> {
    match rx.try_recv() {
        Ok(cmd) => Some(cmd),
        Err(_) => match internal_rx.try_recv() {
            Ok(cmd) => Some(cmd),
            Err(_) => None,
        },
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
    // The bool is true when this sync took the lone waiter and completed it
    // from the blocking thread. Batch syncs leave it false.
    let mut interval_fsync: Option<(
        std::time::Instant,
        tokio::task::JoinHandle<(
            Box<dyn DurableQueueLog>,
            Result<QueueOffset, String>,
            Option<String>,
        )>,
        bool,
    )> = None;
    // The select below prefers the mailbox over the timer. A publisher that
    // keeps one command queued would otherwise never reach the interval arm,
    // so a full window and an elapsed interval start the sync before select.
    let mut last_group_commit = tokio::time::Instant::now();
    // Set after an interval sync. A deep confirm window then flushes on its own
    // clock instead of waiting for the next tick.
    let mut follow_up = false;
    // Last persistent enqueue. The first lone waiter syncs once this is 1 ms
    // old. After that confirm stayed alone, the next one syncs immediately.
    let mut last_enqueue_at: Option<Instant> = None;
    // A command pulled out of the mailbox so a lone flush does not run ahead of it.
    let mut held: Option<QueueCmd> = None;
    // A lone fsync overlapped a deep burst. Cover the remainder on the next pass.
    let mut flush_remainder = false;

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

        // 128 waiters share one fsync. The interval still fires while the mailbox
        // stays full, so a deferred append is not stuck behind a command flood.
        // A lone publish and a paused burst wait for the coalesce arm: that arm
        // runs only when the mailbox was empty. Pulling commands aside livelocks
        // against a flooded mailbox and never joins the in-flight fsync.
        if use_timer && interval_fsync.is_none() && state.needs_group_commit_flush() {
            let due = last_group_commit.elapsed() >= policy.interval;
            let waiting = state.fsync_waiters.len();
            if due || waiting >= 128 || flush_remainder {
                flush_remainder = false;
                if let Some(handle) = state.begin_interval_fsync() {
                    last_group_commit = tokio::time::Instant::now();
                    if let Some(interval) = interval.as_mut() {
                        interval.reset();
                    }
                    interval_fsync = Some((std::time::Instant::now(), handle, false));
                }
            }
        }

        let from_held = held.take();
        let cmd = if let Some(cmd) = from_held {
            Some(cmd)
        } else {
            let deadline = state.next_deadline();
            let waiting_now = state.fsync_waiters.len();
            let coalesce = waiting_now == 1 || (follow_up && state.pipeline_follow_up_ready());
            let coalesce_deadline = if use_timer && interval_fsync.is_none() && coalesce {
                if waiting_now == 1 && state.immediate_lone {
                    Some(Instant::now())
                } else {
                    last_enqueue_at.map(|t| t + std::time::Duration::from_millis(1))
                }
            } else {
                None
            };

            tokio::select! {
                biased;
                // The in-flight fsync join is first. A ready mailbox must not park
                // the log forever; a confirm still returns while the sync blocks.
                joined = async {
                    if let Some((_, handle, _)) = interval_fsync.as_mut() {
                        (&mut *handle).await
                    } else {
                        std::future::pending().await
                    }
                }, if interval_fsync.is_some() => {
                    let mut lone_flush = false;
                    if let Some((started, _, lone)) = interval_fsync.take() {
                        lone_flush = lone;
                        state.finish_interval_fsync(joined, started);
                    }
                    // Deferred appends landed after the interval fsync. Sync them
                    // before the next command, so a quorum flush cannot confirm early.
                    if state.pending_fsync {
                        let _ = state.fsync_now().await;
                    }
                    follow_up = true;
                    let alone = state.fsync_waiters.is_empty() && state.deferred_appends.is_empty();
                    state.immediate_lone = lone_flush && alone;
                    // One message of a 96-burst rode this fsync. The rest are
                    // still short of the follow-up mark and must not wait out
                    // the interval.
                    if lone_flush && state.fsync_waiters.len() >= QueueState::LONE_BURST_REMAINDER
                    {
                        flush_remainder = true;
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
                // Mailbox stayed empty. The first lone waiter waits 1 ms so a
                // burst can share the fsync. A learned lone waiter does not.
                // Yield before deciding: a ready arm that continues without
                // awaiting livelocks the current-thread tests.
                _ = async {
                    if let Some(t) = coalesce_deadline {
                        tokio::time::sleep_until(t).await;
                    } else {
                        std::future::pending::<()>().await;
                    }
                }, if coalesce_deadline.is_some() && interval_fsync.is_none() => {
                    tokio::task::yield_now().await;
                    if let Some(cmd) = take_ready_cmd(&mut rx, &mut internal_rx) {
                        state.immediate_lone = false;
                        held = Some(cmd);
                        continue;
                    }
                    let quiet = last_enqueue_at
                        .map(|t| t.elapsed() >= std::time::Duration::from_millis(1))
                        .unwrap_or(false);
                    let waiting = state.fsync_waiters.len();
                    let learned = waiting == 1 && state.immediate_lone;
                    let first_lone = waiting == 1 && quiet;
                    let paused_burst = follow_up && state.pipeline_follow_up_ready() && quiet;
                    if state.needs_group_commit_flush() && (learned || first_lone) {
                        if let Some(handle) = state.begin_lone_fsync() {
                            last_group_commit = tokio::time::Instant::now();
                            if let Some(interval) = interval.as_mut() {
                                interval.reset();
                            }
                            interval_fsync = Some((std::time::Instant::now(), handle, true));
                        } else {
                            // No log to park. A zero deadline would spin this arm.
                            state.immediate_lone = false;
                        }
                    } else if paused_burst && state.needs_group_commit_flush() {
                        if let Some(handle) = state.begin_interval_fsync() {
                            last_group_commit = tokio::time::Instant::now();
                            if let Some(interval) = interval.as_mut() {
                                interval.reset();
                            }
                            interval_fsync = Some((std::time::Instant::now(), handle, false));
                        }
                    }
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
                            last_group_commit = tokio::time::Instant::now();
                            interval_fsync = Some((std::time::Instant::now(), handle, false));
                        }
                    }
                    continue;
                }
            }
        };

        let Some(cmd) = cmd else {
            break;
        };

        match cmd {
            QueueCmd::StreamStart { .. } => {}
            QueueCmd::StreamApply { reply, .. } | QueueCmd::StreamInstall { reply, .. } => {
                let _ = reply.send(Err(Error::PreconditionFailed(
                    "replicated stream command on a non-stream queue".into(),
                )));
            }
            QueueCmd::StreamDump { reply } => {
                let _ = reply.send(Default::default());
            }
            QueueCmd::Enqueue { msg, reply } => {
                if msg.persistent {
                    last_enqueue_at = Some(Instant::now());
                }
                let res = state.enqueue(msg);
                let _ = reply.send(res);
                // Steady-state lone confirm: fsync as soon as the append is in,
                // unless the next command is already queued. That command is a
                // pipeline and has to share the batch.
                if state.immediate_lone
                    && use_timer
                    && interval_fsync.is_none()
                    && state.fsync_waiters.len() == 1
                {
                    if let Some(cmd) = take_ready_cmd(&mut rx, &mut internal_rx) {
                        state.immediate_lone = false;
                        held = Some(cmd);
                    } else if let Some(handle) = state.begin_lone_fsync() {
                        last_group_commit = tokio::time::Instant::now();
                        if let Some(interval) = interval.as_mut() {
                            interval.reset();
                        }
                        interval_fsync = Some((std::time::Instant::now(), handle, true));
                    }
                }
            }
            QueueCmd::FlushDurable { offset, reply } => {
                if state.wal.is_none() && !state.wal_parked {
                    let _ = reply.send(Ok(()));
                } else {
                    state.fsync_waiters.push(FsyncWaiter { offset, tx: reply });
                    if !state.wal_parked {
                        state.pending_fsync = true;
                    }
                }
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
            QueueCmd::ResetReplicas { reply } => {
                // Wait for any parked log before persisting snapshot removals.
                if let Some((started, handle, _)) = interval_fsync.take() {
                    state.finish_interval_fsync(handle.await, started);
                }
                state.purge();
                let unacked: Vec<_> = state.unacked.keys().copied().collect();
                for id in unacked {
                    state.ack(id);
                }
                let result = state.fsync_now().await;
                let _ = reply.send(result);
            }
            QueueCmd::StreamOffsets { reply } => {
                let _ = reply.send(None);
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
                if let Some((started, handle, _)) = interval_fsync.take() {
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
            QueueCmd::FlushDurable { reply, .. } => {
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
            QueueCmd::StreamOffsets { reply } => {
                let _ = reply.send(None);
            }
            QueueCmd::StreamApply { reply, .. }
            | QueueCmd::StreamInstall { reply, .. }
            | QueueCmd::ResetReplicas { reply } => {
                let _ = reply.send(Err(Error::Unavailable(
                    "queue actor is shutting down".into(),
                )));
            }
            QueueCmd::StreamDump { reply } => {
                let _ = reply.send(Default::default());
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
            QueueCmd::StreamStart { .. }
            | QueueCmd::Ack { .. }
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
mod tests;
