//! Ack, nack, consumer registration, requeue, and purge.

use std::sync::Arc;

use tokio::sync::mpsc;

use super::super::cmd::{ConsumerDeliveryId, ConsumerSessionId, QueueDelivery};
use super::super::dlx::DeathReason;
use super::{ConsumerState, QueueState};
use crate::error::Error;

impl QueueState {
    /// Ack delivery `id` and ignore a missing id. Memory is released and the WAL offset is noted.
    pub(super) fn ack(&mut self, id: ConsumerDeliveryId) {
        let _ = self.ack_report(id);
    }

    /// Ack delivery `id`. Returns the message id when the delivery was unacked, or `None` when `id` is unknown. Unknown ids must not release memory.
    pub(super) fn ack_report(
        &mut self,
        id: ConsumerDeliveryId,
    ) -> Option<compact_str::CompactString> {
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

    /// Drop every ready or unacked copy whose message id equals `message_id`. Each dropped copy is acked for the watermark. A message id that matches nothing leaves the queue unchanged.
    pub(super) fn forget_message(&mut self, message_id: &str) {
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

    /// Nack delivery `id`. `requeue` returns it to ready with the redelivered flag, or dead-letters it when false or when `x-delivery-limit` is reached.
    pub(super) fn nack(&mut self, id: ConsumerDeliveryId, requeue: bool) {
        let _ = self.nack_report(id, requeue);
    }

    /// `Some(message_id)` when the message left this node's log.
    ///
    /// Nack delivery `id` and return the message id when the message left this node's log. `requeue` keeps it ready and returns `None` unless the delivery limit dead-letters it. An unknown `id` returns `None` and changes nothing.
    pub(super) fn nack_report(
        &mut self,
        id: ConsumerDeliveryId,
        requeue: bool,
    ) -> Option<compact_str::CompactString> {
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

    /// Register `session`. `no_ack` forces unlimited credit, `exclusive` refuses a queue that already has consumers, `initial_credit` is the prefetch, `deliver_tx` receives pushes, and `priority` orders single-active and multi-priority choice. Returns a lock or precondition error when the session cannot be added. On success, ready messages are drained.
    pub(super) fn register_consumer(
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

    /// Add `credit` units to `session`. Zero credit returns immediately. A missing session is ignored. Ready messages are drained after a real increase.
    pub(super) fn add_credit(&mut self, session: ConsumerSessionId, credit: u32) {
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

    /// Replace prefetch on `session` with `credit`. A no-ack consumer is left unlimited. A missing session is ignored.
    pub(super) fn set_credit(&mut self, session: ConsumerSessionId, credit: Option<u32>) {
        if let Some(c) = self.consumers.get_mut(&session) {
            if c.no_ack {
                return;
            }
            c.credit = credit;
            self.drain_ready();
        }
    }

    /// Remove `session`. `requeue` returns that session's unacked messages to ready. Removing the last consumer starts the `x-expires` clock.
    pub(super) fn remove_consumer(&mut self, session: ConsumerSessionId, requeue: bool) {
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

    /// Return unacked messages for `sessions` to the ready head with the redelivered flag set. Messages for other sessions stay unacked.
    pub(super) fn requeue_sessions(&mut self, sessions: &[ConsumerSessionId]) {
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

    /// Nack every unacked delivery with requeue. Delivery-limit dead-letter still applies.
    pub(super) fn requeue_all_unacked(&mut self) {
        let ids: Vec<_> = self.unacked.keys().copied().collect();
        for id in ids {
            self.nack(id, true);
        }
    }

    /// Drop every ready message and ack each offset. Returns the number removed. Unacked messages stay.
    pub(super) fn purge(&mut self) -> u32 {
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
}
