//! Ready-queue pops, consumer credit, and push or pull delivery.

use std::cmp::Reverse;
use std::sync::Arc;

use tokio::sync::mpsc;

use super::super::cmd::{
    ConsumerDeliveryId, ConsumerSessionId, QueueDelivery, QueueMessage, QueueOffset, QueueStats,
};
use super::{ConsumerState, QueueState, TtlKey, UnackedEntry};

impl QueueState {
    /// Return ready, unacked, consumer, and per-priority counts. The values are the actor's in-memory view at this command.
    pub(super) fn stats(&self) -> QueueStats {
        QueueStats {
            messages_ready: self.ready.len() as u32,
            messages_unacked: self.unacked.len() as u32,
            consumer_count: self.consumers.len() as u32,
            max_priority: self.ready.max_priority(),
            ready_by_priority: self.ready.counts_by_priority(),
        }
    }

    /// Allocate the next consumer delivery id. Returns that id. Ids are not reused for the life of the actor.
    pub(super) fn mint_delivery_id(&mut self) -> ConsumerDeliveryId {
        let id = ConsumerDeliveryId(self.next_delivery_id);
        self.next_delivery_id = self.next_delivery_id.saturating_add(1);
        id
    }

    /// Deliver head: highest-priority ready (or FIFO front).
    ///
    /// Remove the highest-priority ready message, or the FIFO head. Returns it, or `None` when ready is empty. Ready bytes decrease by the body size.
    pub(super) fn pop_ready_front(&mut self) -> Option<QueueMessage> {
        let qm = self.ready.pop_front()?;
        self.ready_bytes = self.ready_bytes.saturating_sub(qm.message.body_bytes());
        Some(qm)
    }

    /// Overflow drop-head: lowest-priority oldest (or FIFO front).
    ///
    /// Remove the overflow drop-head: lowest-priority oldest, or the FIFO head. Returns it, or `None` when ready is empty.
    pub(super) fn pop_ready_drop_head(&mut self) -> Option<QueueMessage> {
        let qm = self.ready.pop_drop_head()?;
        self.ready_bytes = self.ready_bytes.saturating_sub(qm.message.body_bytes());
        Some(qm)
    }

    /// Remove the ready message at `offset`. Returns it, or `None` when that offset is not ready. Ready bytes decrease by the body size.
    pub(super) fn remove_ready_offset(&mut self, offset: QueueOffset) -> Option<QueueMessage> {
        let qm = self.ready.remove_offset(offset)?;
        self.ready_bytes = self.ready_bytes.saturating_sub(qm.message.body_bytes());
        Some(qm)
    }

    /// Publish ready, unacked, and consumer gauges for this queue. Labels are the vhost and queue name.
    pub(super) fn update_gauges(&self) {
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

    /// Report whether `c` can take a delivery. Unlimited credit (`None`) and a positive count return true.
    pub(super) fn has_credit(c: &ConsumerState) -> bool {
        match c.credit {
            None => true,
            Some(n) => n > 0,
        }
    }

    /// Consume one credit unit on `c`. Unlimited credit is left unchanged.
    pub(super) fn take_credit(c: &mut ConsumerState) {
        if let Some(n) = c.credit.as_mut() {
            *n = n.saturating_sub(1);
        }
    }

    /// Deliver ready messages to consumers with available credit (round-robin).
    ///
    /// Deliver ready messages to consumers that have credit, in priority then round-robin order. Stops when ready is empty or no consumer has credit.
    pub(super) fn drain_ready(&mut self) {
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

    /// Choose the next consumer session for a push delivery. Returns `None` when no consumer has credit. Single-active mode picks the highest priority; otherwise round-robin stays inside the highest priority.
    pub(super) fn pick_next_consumer(&mut self) -> Option<ConsumerSessionId> {
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
    pub(super) fn try_deliver_to(&mut self, session: ConsumerSessionId) -> bool {
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

    /// Place `qm` at the ready head and arm its TTL entry. Used when a delivery is undone. Body bytes are added back.
    pub(super) fn push_ready_front(&mut self, qm: QueueMessage) {
        self.ready_bytes = self.ready_bytes.saturating_add(qm.message.body_bytes());
        if let Some(at) = qm.expires_at {
            self.ttl_heap.push(Reverse(TtlKey {
                at,
                offset: qm.offset,
            }));
        }
        self.ready.push_front(qm);
    }

    /// Basic.deliver for an explicit `consumer` pull. Returns the delivery id and message, or `None` when ready is empty. The message stays unacked until ack, nack, or settle.
    pub(super) fn deliver_pull(
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

    /// Basic.get. `no_ack` drops and acks the message immediately; otherwise it stays unacked with no consumer session. Returns the delivery id, message, and remaining ready count, or `None` when ready is empty. Counts as queue use for `x-expires`.
    pub(super) fn get(&mut self, no_ack: bool) -> Option<(ConsumerDeliveryId, QueueMessage, u32)> {
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
}
