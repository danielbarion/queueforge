//! WAL fsync, interval group commit, deferred appends, and shutdown flush.

use std::sync::Arc;

use tracing::{error, warn};

use super::super::cmd::{ConsumerDeliveryId, Message, QueueDelivery, QueueMessage, QueueOffset};
use super::super::durable::DurableQueueLog;
use super::QueueState;
use crate::config::FsyncPolicy;
use crate::error::Error;

impl QueueState {
    /// Fsync on the blocking pool, then complete durability waiters.
    ///
    /// The actor keeps exclusive ownership of the log: the handle is moved into
    /// the blocking task and put back before any other command runs.
    ///
    /// Fsync the WAL on the blocking pool and complete durability waiters covered by the synced offset. Returns the fsync error. The log is taken out of the actor for the blocking task and put back before any other command runs. Waiters for offsets still only in `deferred_appends` are not completed here.
    pub(super) async fn fsync_now(&mut self) -> Result<(), Error> {
        self.pending_fsync = false;
        let Some(mut wal) = self.wal.take() else {
            self.complete_waiters_up_to(QueueOffset(u64::MAX), Ok(()), false);
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
                self.complete_waiters_up_to(synced, Ok(()), false);
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
                    false,
                );
                Err(e)
            }
        }
    }

    /// Complete waiters covered by `synced`.
    ///
    /// When `hold_deferred` is set, an offset still sitting in `deferred_appends`
    /// stays waiting: that body was not in the log this fsync wrote.
    ///
    /// Complete durability waiters at or before `synced` with `result`. When `hold_deferred` is set, an offset still in `deferred_appends` stays waiting because that body was not in the log this fsync wrote. A fsync error fails every waiter.
    pub(super) fn complete_waiters_up_to(
        &mut self,
        synced: QueueOffset,
        result: Result<(), Error>,
        hold_deferred: bool,
    ) {
        let mut remaining = Vec::new();
        for w in self.fsync_waiters.drain(..) {
            let still_deferred = hold_deferred
                && self
                    .deferred_appends
                    .iter()
                    .any(|(offset, _)| offset.0 == w.offset.0);
            if still_deferred {
                remaining.push(w);
                continue;
            }
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

    /// Record `offset` as acked for the WAL watermark. A non-WAL message returns immediately. While the log is parked the ack is deferred. A dirty watermark under `Always` or `EveryNMessages` sets `pending_fsync`.
    pub(super) fn note_acked_offset(&mut self, offset: QueueOffset) {
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
    ///
    /// Request an fsync when the WAL meta watermark is dirty and the policy is `Always` or `EveryNMessages`. `EveryNMs` and `Never` leave the interval timer or the buffer alone.
    pub(super) fn maybe_flush_after_ack(&mut self) {
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

    /// Report whether the interval arm should fsync. Returns true when waiters, unsynced appends, deferred appends, deferred acks, or a dirty watermark exist.
    pub(super) fn needs_group_commit_flush(&self) -> bool {
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
    ///
    /// Park the log and start its fsync on the blocking pool. Returns the join handle, or `None` when the queue has no log. The select loop must not use the log until `finish_interval_fsync`.
    pub(super) fn begin_interval_fsync(
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

    /// Install the log from `joined` and complete waiters. `started` is the interval-arm clock used for the fsync histogram. Deferred appends are written after the waiters that this fsync did not cover are held back, and `pending_fsync` is set when those bodies still need a sync.
    pub(super) fn finish_interval_fsync(
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
                    false,
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
                // Bodies still in `deferred_appends` were not in this fsync.
                self.complete_waiters_up_to(synced, Ok(()), true);
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
                    false,
                );
            }
        }
        self.flush_deferred_wal();
        if !self.fsync_waiters.is_empty() || self.unsynced_appends > 0 {
            self.pending_fsync = true;
        }
    }

    /// Write acks and appends accepted while the interval fsync held the log. A missing log puts the batches back. Append errors are logged and those bodies are not counted as unsynced.
    pub(super) fn flush_deferred_wal(&mut self) {
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

    /// Final fsync + free in-memory state. Returns fsync error if durable WAL
    /// could not be flushed (Issue 3: surface failure to process shutdown).
    pub(super) async fn shutdown_flush(&mut self) -> Result<(), Error> {
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
