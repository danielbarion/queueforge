//! Queue x-expires and the ready-message TTL heap the select loop sleeps on.

use std::cmp::Reverse;

use tokio::time::Instant;

use super::QueueState;

impl QueueState {
    /// Mark the queue used now. This resets the `x-expires` idle clock.
    pub(super) fn touch_used(&mut self) {
        self.last_used = Instant::now();
    }

    /// Next absolute deadline for the actor sleep (TTL or x-expires).
    ///
    /// Return the next actor wake time, the earlier of the next ready TTL and `x-expires`. Returns `None` when neither is armed. Stale TTL heap keys are discarded first.
    pub(super) fn next_deadline(&mut self) -> Option<Instant> {
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

    /// Return when `x-expires` elapses, or `None` when the argument is unset or a consumer is connected. The clock starts at the last use.
    pub(super) fn queue_expires_at(&self) -> Option<Instant> {
        let ttl = self.args.expires()?;
        // Unused only when no consumers (RabbitMQ-like).
        if !self.consumers.is_empty() {
            return None;
        }
        Some(self.last_used + ttl)
    }

    /// Pop TTL heap entries whose offset is no longer ready at that deadline. The first still-ready entry stops the scan.
    pub(super) fn purge_stale_ttl_keys(&mut self) {
        while let Some(Reverse(k)) = self.ttl_heap.peek().copied() {
            let still_ready = self.ready.has_ttl_entry(k.offset, Some(k.at));
            if still_ready {
                break;
            }
            self.ttl_heap.pop();
        }
    }

    /// If `x-expires` elapsed while unused, notify registry and return true.
    ///
    /// Return whether `x-expires` has elapsed while the queue has no consumers.
    pub(super) fn check_queue_expired(&self) -> bool {
        matches!(self.queue_expires_at(), Some(at) if Instant::now() >= at)
    }

    /// Tell the registry this queue's `x-expires` elapsed. A missing notifier is ignored. The registry still has to shut the actor down.
    pub(super) fn notify_queue_expired(&self) {
        if let Some(tx) = &self.expired_tx {
            let _ = tx.send(self.key.clone());
        }
    }
}
