//! Ready-set storage: plain FIFO or multi-lane priority queues.
//!
//! Queues without `x-max-priority` use [`Ready::Fifo`] (single `VecDeque`,
//! zero lane overhead). Priority queues use [`Ready::Priority`]: an array of
//! deques of length `max_priority + 1`. Deliver scans high→low; drop-head
//! scans low→high (lowest-priority oldest first, RabbitMQ-like).

use std::collections::VecDeque;

use super::cmd::{Message, QueueMessage, QueueOffset};

/// Effective priority: `min(properties.priority.unwrap_or(0), max_priority)`.
#[inline]
pub fn effective_priority(msg: &Message, max_priority: u8) -> u8 {
    msg.priority.unwrap_or(0).min(max_priority)
}

/// Ready message storage for a queue actor.
#[derive(Debug)]
pub enum Ready {
    /// Non-priority queue: classic FIFO.
    Fifo(VecDeque<QueueMessage>),
    /// Priority queue: one lane per priority level `0..=max_priority`.
    Priority {
        /// Lanes indexed by effective priority; higher index = higher priority.
        lanes: Box<[VecDeque<QueueMessage>]>,
        /// Cached total ready count across all lanes.
        len: usize,
        /// `x-max-priority` (lanes.len() == max_priority + 1).
        max_priority: u8,
    },
}

impl Ready {
    /// Build ready storage from declare args (`None` / zero → FIFO).
    pub fn from_max_priority(max_priority: Option<u8>) -> Self {
        match max_priority {
            Some(max) if max > 0 => {
                let n = usize::from(max) + 1;
                let lanes = (0..n)
                    .map(|_| VecDeque::new())
                    .collect::<Vec<_>>()
                    .into_boxed_slice();
                Self::Priority {
                    lanes,
                    len: 0,
                    max_priority: max,
                }
            }
            _ => Self::Fifo(VecDeque::new()),
        }
    }

    /// Total ready messages.
    #[inline]
    pub fn len(&self) -> usize {
        match self {
            Self::Fifo(q) => q.len(),
            Self::Priority { len, .. } => *len,
        }
    }

    /// Whether the ready set is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// `x-max-priority` when this is a priority ready set.
    #[inline]
    pub fn max_priority(&self) -> Option<u8> {
        match self {
            Self::Fifo(_) => None,
            Self::Priority { max_priority, .. } => Some(*max_priority),
        }
    }

    /// Per-priority ready counts (`None` for FIFO).
    pub fn counts_by_priority(&self) -> Option<Vec<u32>> {
        match self {
            Self::Fifo(_) => None,
            Self::Priority { lanes, .. } => Some(lanes.iter().map(|l| l.len() as u32).collect()),
        }
    }

    /// Append to the back of the appropriate structure (enqueue / requeue).
    pub fn push_back(&mut self, qm: QueueMessage) {
        match self {
            Self::Fifo(q) => q.push_back(qm),
            Self::Priority {
                lanes,
                len,
                max_priority,
            } => {
                let p = effective_priority(qm.message.as_ref(), *max_priority) as usize;
                lanes[p].push_back(qm);
                *len = len.saturating_add(1);
            }
        }
    }

    /// Prepend so the message is next among its priority band (failed push delivery).
    pub fn push_front(&mut self, qm: QueueMessage) {
        match self {
            Self::Fifo(q) => q.push_front(qm),
            Self::Priority {
                lanes,
                len,
                max_priority,
            } => {
                let p = effective_priority(qm.message.as_ref(), *max_priority) as usize;
                lanes[p].push_front(qm);
                *len = len.saturating_add(1);
            }
        }
    }

    /// Pop the next message for delivery (highest priority, then FIFO within lane).
    pub fn pop_front(&mut self) -> Option<QueueMessage> {
        match self {
            Self::Fifo(q) => q.pop_front(),
            Self::Priority { lanes, len, .. } => {
                for lane in lanes.iter_mut().rev() {
                    if let Some(qm) = lane.pop_front() {
                        *len = len.saturating_sub(1);
                        return Some(qm);
                    }
                }
                None
            }
        }
    }

    /// Pop the drop-head victim (lowest-priority oldest ready message).
    ///
    /// For FIFO this is the queue head. For priority queues, scan lanes from
    /// priority 0 upward and take the front of the first non-empty lane.
    pub fn pop_drop_head(&mut self) -> Option<QueueMessage> {
        match self {
            Self::Fifo(q) => q.pop_front(),
            Self::Priority { lanes, len, .. } => {
                for lane in lanes.iter_mut() {
                    if let Some(qm) = lane.pop_front() {
                        *len = len.saturating_sub(1);
                        return Some(qm);
                    }
                }
                None
            }
        }
    }

    /// Remove a specific offset from ready (TTL expiry). Linear scan.
    pub fn remove_offset(&mut self, offset: QueueOffset) -> Option<QueueMessage> {
        match self {
            Self::Fifo(q) => {
                let pos = q.iter().position(|m| m.offset == offset)?;
                q.remove(pos)
            }
            Self::Priority { lanes, len, .. } => {
                for lane in lanes.iter_mut() {
                    if let Some(pos) = lane.iter().position(|m| m.offset == offset) {
                        let qm = lane.remove(pos).expect("position valid");
                        *len = len.saturating_sub(1);
                        return Some(qm);
                    }
                }
                None
            }
        }
    }

    /// Whether any ready message still matches `(offset, expires_at)` (TTL heap GC).
    pub fn has_ttl_entry(
        &self,
        offset: QueueOffset,
        expires_at: Option<tokio::time::Instant>,
    ) -> bool {
        match self {
            Self::Fifo(q) => q
                .iter()
                .any(|m| m.offset == offset && m.expires_at == expires_at),
            Self::Priority { lanes, .. } => lanes.iter().any(|lane| {
                lane.iter()
                    .any(|m| m.offset == offset && m.expires_at == expires_at)
            }),
        }
    }

    /// Drain all ready messages (purge / shutdown). Order is undefined across lanes.
    pub fn drain_all(&mut self) -> Vec<QueueMessage> {
        match self {
            Self::Fifo(q) => q.drain(..).collect(),
            Self::Priority { lanes, len, .. } => {
                let mut out = Vec::with_capacity(*len);
                for lane in lanes.iter_mut() {
                    out.extend(lane.drain(..));
                }
                *len = 0;
                out
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use compact_str::CompactString;
    use std::sync::Arc;

    fn msg(body: &[u8], priority: Option<u8>) -> Arc<Message> {
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
            priority,
            timestamp: None,
            expires_unix_ms: None,
            headers: Default::default(),
        })
    }

    fn qm(offset: u64, body: &[u8], priority: Option<u8>) -> QueueMessage {
        QueueMessage::new(QueueOffset(offset), msg(body, priority))
    }

    #[test]
    fn fifo_zero_overhead_path() {
        let mut r = Ready::from_max_priority(None);
        assert!(matches!(r, Ready::Fifo(_)));
        r.push_back(qm(1, b"a", Some(9)));
        r.push_back(qm(2, b"b", Some(0)));
        // Priority ignored on FIFO — order is enqueue order.
        assert_eq!(r.pop_front().unwrap().message.body.as_ref(), b"a");
        assert_eq!(r.pop_front().unwrap().message.body.as_ref(), b"b");
    }

    #[test]
    fn priority_high_before_low() {
        let mut r = Ready::from_max_priority(Some(9));
        r.push_back(qm(1, b"low", Some(0)));
        r.push_back(qm(2, b"high", Some(9)));
        r.push_back(qm(3, b"mid", Some(5)));
        assert_eq!(r.pop_front().unwrap().message.body.as_ref(), b"high");
        assert_eq!(r.pop_front().unwrap().message.body.as_ref(), b"mid");
        assert_eq!(r.pop_front().unwrap().message.body.as_ref(), b"low");
    }

    #[test]
    fn priority_clamped_to_max() {
        let mut r = Ready::from_max_priority(Some(5));
        r.push_back(qm(1, b"a", Some(255))); // → lane 5
        r.push_back(qm(2, b"b", Some(5)));
        // Same effective priority → FIFO by enqueue.
        assert_eq!(r.pop_front().unwrap().message.body.as_ref(), b"a");
        assert_eq!(r.pop_front().unwrap().message.body.as_ref(), b"b");
    }

    #[test]
    fn drop_head_lowest_priority_oldest() {
        let mut r = Ready::from_max_priority(Some(9));
        r.push_back(qm(1, b"low-old", Some(0)));
        r.push_back(qm(2, b"high", Some(9)));
        r.push_back(qm(3, b"low-new", Some(0)));
        let dropped = r.pop_drop_head().unwrap();
        assert_eq!(dropped.message.body.as_ref(), b"low-old");
        // Deliver still prefers high.
        assert_eq!(r.pop_front().unwrap().message.body.as_ref(), b"high");
        assert_eq!(r.pop_front().unwrap().message.body.as_ref(), b"low-new");
    }

    #[test]
    fn missing_priority_is_zero() {
        assert_eq!(effective_priority(msg(b"x", None).as_ref(), 9), 0);
        assert_eq!(effective_priority(msg(b"x", Some(3)).as_ref(), 9), 3);
        assert_eq!(effective_priority(msg(b"x", Some(99)).as_ref(), 9), 9);
    }
}
