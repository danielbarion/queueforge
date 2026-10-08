//! Actor supervision and registry shutdown, including the WAL fsync budget.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, info, warn};

use super::super::actor;
use super::super::cmd::{QueueCmd, QueueStats};
use super::super::durable::QueueActorBootstrap;
use super::{QueueInfo, QueueKey, QueueRegistry, ShutdownReport};
use crate::disk::DiskBudget;
use crate::memory::MemoryTracker;

impl QueueRegistry {
    /// Graceful process drain: fan-out `Shutdown` to every live queue actor
    /// (WAL fsync + watermark flush for durable queues), wait under a single
    /// global budget, then clear the registry map.
    ///
    /// Returns a [`ShutdownReport`]. Callers should treat
    /// [`ShutdownReport::is_clean`] == false as an unclean process exit
    /// (fsync errors and/or timeouts).
    pub async fn shutdown_all(&self) -> ShutdownReport {
        self.shutdown_all_with_timeout(Duration::from_secs(10))
            .await
    }

    /// Like [`Self::shutdown_all`] with an explicit global wait budget.
    pub async fn shutdown_all_with_timeout(&self, budget: Duration) -> ShutdownReport {
        let keys = self.list_keys();
        info!(
            queues = keys.len(),
            budget_ms = budget.as_millis() as u64,
            "draining queue actors (Shutdown + fsync, parallel)"
        );

        let mailboxes: Vec<(QueueKey, mpsc::Sender<QueueCmd>)> = {
            let guard = self.entries.read().expect("queue registry lock poisoned");
            keys.iter()
                .filter_map(|k| guard.get(k).map(|e| (k.clone(), e.handle.tx.clone())))
                .collect()
        };

        let mut report = ShutdownReport {
            queues: mailboxes.len(),
            ..ShutdownReport::default()
        };

        // Fan-out Shutdown to all mailboxes first (Issue 6: not N×serial waits).
        let mut pending: Vec<(QueueKey, oneshot::Receiver<crate::error::Result<()>>)> = Vec::new();
        for (key, tx) in mailboxes {
            let (reply_tx, reply_rx) = oneshot::channel();
            if tx
                .send(QueueCmd::Shutdown { reply: reply_tx })
                .await
                .is_err()
            {
                debug!(
                    vhost = %key.vhost,
                    queue = %key.name,
                    "queue actor mailbox closed during shutdown"
                );
                report.mailbox_closed += 1;
                continue;
            }
            pending.push((key, reply_rx));
        }

        // Join all replies under one global timeout barrier.
        let mut join_set = tokio::task::JoinSet::new();
        for (key, reply_rx) in pending {
            join_set.spawn(async move {
                let res = reply_rx.await;
                (key, res)
            });
        }

        let deadline = tokio::time::Instant::now() + budget;
        while !join_set.is_empty() {
            let now = tokio::time::Instant::now();
            if now >= deadline {
                let left = join_set.len();
                report.timeouts += left;
                warn!(
                    remaining = left,
                    "queue actor drain budget exhausted; aborting remaining waits"
                );
                join_set.abort_all();
                // Drain aborted join results so JoinSet drops cleanly.
                while join_set.join_next().await.is_some() {}
                break;
            }
            let remaining = deadline - now;
            match tokio::time::timeout(remaining, join_set.join_next()).await {
                Ok(Some(Ok((key, Ok(Ok(())))))) => {
                    debug!(
                        vhost = %key.vhost,
                        queue = %key.name,
                        "queue actor shut down"
                    );
                }
                Ok(Some(Ok((key, Ok(Err(e)))))) => {
                    report.fsync_errors += 1;
                    error!(
                        vhost = %key.vhost,
                        queue = %key.name,
                        error = %e,
                        "queue actor final fsync failed during shutdown"
                    );
                }
                Ok(Some(Ok((key, Err(_))))) => {
                    warn!(
                        vhost = %key.vhost,
                        queue = %key.name,
                        "queue actor dropped shutdown reply"
                    );
                }
                Ok(Some(Err(join_err))) => {
                    if join_err.is_cancelled() {
                        report.timeouts += 1;
                    } else {
                        warn!(error = %join_err, "queue shutdown join error");
                    }
                }
                Ok(None) => break,
                Err(_elapsed) => {
                    let left = join_set.len();
                    report.timeouts += left;
                    warn!(
                        remaining = left,
                        "queue actor drain timed out; aborting remaining waits"
                    );
                    join_set.abort_all();
                    while join_set.join_next().await.is_some() {}
                    break;
                }
            }
        }

        self.entries
            .write()
            .expect("queue registry lock poisoned")
            .clear();

        if report.is_clean() {
            info!(queues = report.queues, "all queue actors drained");
        } else {
            // Issue 4: do not claim "all drained" when timeouts / fsync failed.
            warn!(
                queues = report.queues,
                fsync_errors = report.fsync_errors,
                timeouts = report.timeouts,
                mailbox_closed = report.mailbox_closed,
                "queue actor drain incomplete"
            );
        }
        report
    }
}

/// Spawn the queue actor for `key` on `boot` and mark `info` unavailable if that task panics. `tx` is not used; the mailbox is created inside. Returns the command sender and the join handle. A panic does not restart the actor.
pub(super) fn spawn_supervised_actor(
    key: QueueKey,
    info: Arc<QueueInfo>,
    mailbox_capacity: usize,
    memory: Arc<MemoryTracker>,
    disk: Option<Arc<DiskBudget>>,
    bootstrap: QueueActorBootstrap,
) -> mpsc::Sender<QueueCmd> {
    let (tx, rx) = mpsc::channel(mailbox_capacity);
    let actor_key = key.clone();
    let info_for_actor = Arc::clone(&info);
    let memory_for_actor = Arc::clone(&memory);
    let stream = bootstrap.args.queue_type == Some(crate::queue::QueueType::Stream);
    let join = tokio::spawn(async move {
        if stream {
            crate::queue::stream::run_stream(actor_key, rx, info_for_actor, bootstrap).await;
            return;
        }
        actor::run(
            actor_key,
            rx,
            memory_for_actor,
            disk,
            info_for_actor,
            bootstrap,
        )
        .await;
    });

    // Supervisor: on panic mark Unavailable + metric; do NOT restart empty.
    // Memory release is handled by the actor's MemoryGuard Drop during unwind.
    let sup_key = key;
    let info_for_sup = Arc::clone(&info);
    let memory_for_sup = Arc::clone(&memory);
    tokio::spawn(async move {
        match join.await {
            Ok(()) => {
                debug!(vhost = %sup_key.vhost, queue = %sup_key.name, "queue actor exited cleanly");
            }
            Err(join_err) if join_err.is_panic() => {
                error!(
                    vhost = %sup_key.vhost,
                    queue = %sup_key.name,
                    "queue actor panicked; marking unavailable (no auto-restart)"
                );
                info_for_sup.mark_unavailable();
                // Safety net if Drop did not run (e.g. abort) — release residual.
                let residual = info_for_sup.reserved_bytes.swap(0, Ordering::AcqRel);
                if residual > 0 {
                    memory_for_sup.sub(residual);
                    warn_reserved_release(&sup_key, residual);
                }
                metrics::counter!("queueforge_queue_actor_panics_total").increment(1);
            }
            Err(join_err) => {
                debug!(
                    vhost = %sup_key.vhost,
                    queue = %sup_key.name,
                    error = %join_err,
                    "queue actor join error (cancelled?)"
                );
                let residual = info_for_sup.reserved_bytes.swap(0, Ordering::AcqRel);
                if residual > 0 {
                    memory_for_sup.sub(residual);
                }
            }
        }
    });

    tx
}

/// Log when `key` still holds `residual` reserved bytes at shutdown. A zero residual is not logged.
pub(super) fn warn_reserved_release(key: &QueueKey, residual: u64) {
    tracing::warn!(
        vhost = %key.vhost,
        queue = %key.name,
        residual,
        "released residual memory reservation after queue actor panic"
    );
}

/// Send shutdown to `tx` and report whether the actor acknowledged it. Returns false when the mailbox is closed.
pub(super) async fn shutdown_actor(tx: &mpsc::Sender<QueueCmd>) -> bool {
    let (reply_tx, reply_rx) = oneshot::channel();
    if tx
        .send(QueueCmd::Shutdown { reply: reply_tx })
        .await
        .is_err()
    {
        return false;
    }
    // Treat Ok(Ok) and Ok(Err(fsync)) as "actor stopped"; only missing reply fails.
    reply_rx.await.is_ok()
}

/// Ask `tx` for queue stats. Returns `None` when the actor is gone. The caller treats `None` as an empty or unavailable queue.
pub(super) async fn query_stats(tx: &mpsc::Sender<QueueCmd>) -> Option<QueueStats> {
    let (reply_tx, reply_rx) = oneshot::channel();
    if tx.send(QueueCmd::Stats { reply: reply_tx }).await.is_err() {
        return None;
    }
    reply_rx.await.ok()
}
