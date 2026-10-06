//! Queue recovery on actor start: restored ready messages, deadlines, and expiry stamps.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::mpsc;
use tokio::time::Instant;

use super::super::args::QueueArgs;
use super::super::cmd::{Message, QueueCmd};
use super::super::durable::QueueActorBootstrap;
use super::super::ready::Ready;
use super::super::{QueueInfo, QueueKey};
use super::{QueueState, TtlKey};
use crate::disk::DiskBudget;
use crate::memory::MemoryTracker;

impl QueueState {
    /// Build the actor state from `boot`. `key` names the queue, `memory` and `disk` are the process budgets, `info` is the shared reservation counter, and `internal_tx` receives async dead-letter completions. Returns the state with recovered ready messages re-laned in offset order. A recovered `expires_unix_ms` already in the past is due immediately; a missing stamp gets a fresh TTL.
    pub(super) fn from_bootstrap(
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
            // Classic stays a deque. Quorum indexes by id so a drop does not slide the tail.
            ready: Ready::for_queue(&boot.args),
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
            immediate_lone: false,
            expired_tx: boot.expired_tx,
            internal_tx,
            depth_gauges: None,
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
}

/// `(actor wake instant, absolute unix ms)` for a newly enqueued message.
///
/// Stamp a new enqueue. `msg` and `args` supply per-message expiration and `x-message-ttl`; `now` is the actor clock. Returns the wake `Instant` and the absolute unix millis stored on the message. An existing `expires_unix_ms` is kept so recovery does not grant a new TTL.
pub(super) fn expiry_stamp(
    msg: &Message,
    args: &QueueArgs,
    now: Instant,
) -> (Option<Instant>, Option<u64>) {
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
///
/// Turn a stored deadline into an actor `Instant`. `unix_ms` is the absolute stamp, `fresh` is used when there is no stamp, and `now` is the actor clock. Returns the wake time. A deadline already in the past returns `now`.
pub(super) fn expiry_instant(
    unix_ms: Option<u64>,
    fresh: Option<Instant>,
    now: Instant,
) -> Option<Instant> {
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
///
/// Effective ready TTL. `msg` may carry an expiration property, `args` may carry `x-message-ttl`, and `now` is the actor clock. Returns the earlier deadline, or `None` when neither is set.
pub(super) fn compute_expires_at(msg: &Message, args: &QueueArgs, now: Instant) -> Option<Instant> {
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
