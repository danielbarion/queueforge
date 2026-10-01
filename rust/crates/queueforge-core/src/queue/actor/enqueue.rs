//! Enqueue path: limits, memory and disk checks, WAL append, and the completion oneshot.

use std::cmp::Reverse;
use std::sync::Arc;

use tokio::sync::oneshot;
use tokio::time::Instant;
use tracing::error;

use super::super::args::OverflowPolicy;
use super::super::cmd::{EnqueueCompletion, Message, QueueMessage, QueueOffset};
use super::super::dlx::DeathReason;
use super::recovery::expiry_stamp;
use super::{FsyncWaiter, QueueState, TtlKey};
use crate::config::FsyncPolicy;
use crate::error::Error;

impl QueueState {
    /// Allocate the next queue offset. Returns that offset and advances the counter. A failed WAL append must not call this; the offset would be burned and the ack watermark would stall.
    pub(super) fn mint_offset(&mut self) -> QueueOffset {
        let off = QueueOffset(self.next_offset);
        self.next_offset = self.next_offset.saturating_add(1);
        off
    }

    /// Reserve `n` bytes under the hard watermark (global + per-queue).
    ///
    /// Reserve `n` tracked bytes under the hard watermark. Returns `true` when both the process and per-queue counters accepted them. A `false` result means the caller must not append or enqueue.
    pub(super) fn mem_try_reserve(&self, n: u64) -> bool {
        if !self.memory.try_add(n) {
            return false;
        }
        self.info
            .reserved_bytes
            .fetch_add(n, std::sync::atomic::Ordering::Relaxed);
        true
    }

    /// Unconditional reserve (recovery / bootstrap).
    ///
    /// Charge `n` tracked bytes without the hard-watermark check. Used only for recovery, which must restore bytes that were already accepted before restart.
    pub(super) fn mem_reserve_uncond(&self, n: u64) {
        if n == 0 {
            return;
        }
        self.memory.add(n);
        self.info
            .reserved_bytes
            .fetch_add(n, std::sync::atomic::Ordering::Relaxed);
    }

    /// Release `n` bytes from global + per-queue accounting.
    ///
    /// Release `n` tracked bytes from the process and per-queue counters. `n` of zero is a no-op. Releasing more than reserved saturates at zero.
    pub(super) fn mem_release(&self, n: u64) {
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
    ///
    /// Report whether `msg` must be appended to the WAL. Returns true for a persistent message on a durable queue that has a log, including while an interval fsync has the log parked.
    pub(super) fn needs_wal(&self, msg: &Message) -> bool {
        self.durable && msg.persistent && (self.wal.is_some() || self.wal_parked)
    }

    /// Assign an offset for enqueue; WAL append happens before the counter advances.
    ///
    /// Assign the enqueue offset for `msg`. `needs_wal` selects the durable path. Returns the offset after a successful append, or the WAL error. On the durable path the counter advances only after the append; a parked log records a deferred append instead of writing.
    pub(super) fn assign_offset_for_enqueue(
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

    /// Report whether `extra_msgs` messages and `extra_bytes` body bytes would pass `x-max-length` or `x-max-length-bytes`. Returns true when either limit would be crossed.
    pub(super) fn would_exceed_limits(&self, extra_msgs: u64, extra_bytes: u64) -> bool {
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

    /// Place `qm` at the ready tail and arm its TTL heap entry. Body bytes are added to the ready total.
    pub(super) fn push_ready(&mut self, qm: QueueMessage) {
        self.ready_bytes = self.ready_bytes.saturating_add(qm.message.body_bytes());
        if let Some(at) = qm.expires_at {
            self.ttl_heap.push(Reverse(TtlKey {
                at,
                offset: qm.offset,
            }));
        }
        self.ready.push_back(qm);
    }

    /// Make room for one new message under max-length policies.
    ///
    /// Drop-head schedules async DLX and does **not** wait; the head leaves ready
    /// immediately so the loop always makes progress (no DLX-fail requeue spin).
    ///
    /// Make room for one incoming message of `incoming_bytes` body bytes. Returns `Ok` when the queue is within limits. Reject-publish returns the precondition error; drop-head removes the overflow head before dead-letter and does not wait for that dead-letter.
    pub(super) fn enforce_overflow_for_incoming(
        &mut self,
        incoming_bytes: u64,
    ) -> Result<(), Error> {
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

    /// Accept `msg` onto the ready set. Returns the offset and the `durable_done` oneshot, or the limit, memory, disk, or WAL error. `EveryNMs` completes `durable_done` before the interval fsync. `Always` and a full `EveryNMessages` batch set `pending_fsync` so the select loop syncs before the next command.
    pub(super) fn enqueue(&mut self, msg: Arc<Message>) -> Result<EnqueueCompletion, Error> {
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
}
