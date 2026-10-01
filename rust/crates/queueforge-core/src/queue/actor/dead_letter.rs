//! Dead-letter handoff, overflow rejection, and TTL expiry of ready messages.

use std::cmp::Reverse;
use std::sync::Arc;

use tokio::time::Instant;
use tracing::warn;

use super::super::args::DeadLetterStrategy;
use super::super::cmd::{DlxFailAction, Message, QueueCmd, QueueMessage};
use super::super::dlx::{DeathReason, DlxOutcome};
use super::QueueState;
use crate::error::Error;

impl QueueState {
    /// Drop a ready message without DLX (memory + WAL watermark).
    ///
    /// Release `qm` without dead-letter. Memory is returned and the offset is acked so the watermark can advance.
    pub(super) fn drop_ready_message(&mut self, qm: QueueMessage) {
        self.mem_release(qm.message.tracked_bytes());
        self.note_acked_offset(qm.offset);
    }

    /// Hand off DLX to a background task; never await foreign queue actors here.
    ///
    /// Message is already out of ready/unacked. Memory remains charged until
    /// [`QueueCmd::DlxResolved`]. Overflow/expire use [`DlxFailAction::Drop`] so
    /// failure cannot re-enter a tight requeue loop.
    ///
    /// Hand `qm` to the dead-letter router for `reason`. `on_fail` is applied when the router task posts `DlxResolved`. No dead-letter exchange drops the message in place. This never awaits another queue actor.
    pub(super) fn schedule_dead_letter(
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

    /// Finish a dead-letter attempt for `qm`. `outcome` is the router result and `on_fail` chooses drop or requeue. A published or cycle-dropped message is acked. Requeue clears a deadline that is already due so the message is not expired again immediately.
    pub(super) fn apply_dlx_resolved(
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

    /// Return the dead-letter failure action for this queue. At-least-once requeues; at-most-once drops.
    pub(super) fn dlx_on_fail(&self) -> DlxFailAction {
        match self.args.dead_letter_strategy {
            DeadLetterStrategy::AtLeastOnce => DlxFailAction::Requeue,
            DeadLetterStrategy::AtMostOnce => DlxFailAction::Drop,
        }
    }

    /// Publish `msg` to the dead-letter exchange after reject-publish-dlx overflow. No exchange or router returns without publishing. The spawn is detached; the enqueue error is already returned to the publisher.
    pub(super) fn publish_rejected_to_dlx(&self, msg: Arc<Message>) {
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
                .dead_letter(
                    &key,
                    dlx_name.as_str(),
                    rk.as_str(),
                    DeathReason::Maxlen,
                    msg,
                    max_hops,
                )
                .await;
        });
    }

    /// Expire all ready messages whose deadline has passed.
    ///
    /// Progress-safe: each due message leaves ready before async DLX; on DLX
    /// failure the completion path **drops** (never requeues a still-due TTL).
    ///
    /// Dead-letter every ready message whose deadline has passed. Each message leaves the ready set before the dead-letter task runs. Failure follows `dlx_on_fail` and must not spin on a still-due deadline.
    pub(super) fn expire_due_messages(&mut self) {
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
}
