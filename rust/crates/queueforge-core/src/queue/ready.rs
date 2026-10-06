//! Ready-set storage: plain FIFO, an indexed quorum FIFO, or priority lanes.
//!
//! Classic queues use [`Ready::Fifo`] (single `VecDeque`). Quorum queues without
//! `x-max-priority` use [`Ready::Indexed`], so a drop by message id does not
//! slide the messages behind it. Priority queues use [`Ready::Priority`]: an
//! array of deques of length `max_priority + 1`. Deliver scans high→low;
//! drop-head scans low→high (lowest-priority oldest first, RabbitMQ-like).

use std::collections::{HashMap, VecDeque};

use compact_str::CompactString;

use super::args::{QueueArgs, QueueType};
use super::cmd::{Message, QueueMessage, QueueOffset};

/// Quorum ready set. A drop by message id does not slide the messages behind it.
///
/// Classic queues stay on [`Ready::Fifo`]. This exists because a quorum follower
/// holds every copy until the leader delivers it, and a prefetch window drops
/// those copies while newer appends are still in the same deque.
#[derive(Debug)]
struct IndexedFifo {
    order: VecDeque<u64>,
    live: HashMap<u64, QueueMessage>,
    by_id: HashMap<CompactString, Vec<u64>>,
    tombstones: usize,
}

impl IndexedFifo {
    fn new() -> Self {
        Self {
            order: VecDeque::new(),
            live: HashMap::new(),
            by_id: HashMap::new(),
            tombstones: 0,
        }
    }

    fn len(&self) -> usize {
        self.live.len()
    }

    fn index_id(&mut self, qm: &QueueMessage) {
        if let Some(id) = qm.message.message_id.clone() {
            self.by_id.entry(id).or_default().push(qm.offset.0);
        }
    }

    fn unindex_id(&mut self, qm: &QueueMessage) {
        let Some(id) = qm.message.message_id.as_ref() else {
            return;
        };
        let Some(slots) = self.by_id.get_mut(id) else {
            return;
        };
        if let Some(pos) = slots.iter().position(|offset| *offset == qm.offset.0) {
            slots.swap_remove(pos);
        }
        if slots.is_empty() {
            self.by_id.remove(id);
        }
    }

    fn push_back(&mut self, qm: QueueMessage) {
        let offset = qm.offset.0;
        self.index_id(&qm);
        self.order.push_back(offset);
        self.live.insert(offset, qm);
    }

    fn push_front(&mut self, qm: QueueMessage) {
        let offset = qm.offset.0;
        self.index_id(&qm);
        self.order.push_front(offset);
        self.live.insert(offset, qm);
    }

    fn take_live(&mut self, offset: u64) -> Option<QueueMessage> {
        let qm = self.live.remove(&offset)?;
        self.unindex_id(&qm);
        Some(qm)
    }

    fn pop_front(&mut self) -> Option<QueueMessage> {
        while let Some(offset) = self.order.pop_front() {
            if let Some(qm) = self.take_live(offset) {
                return Some(qm);
            }
            self.tombstones = self.tombstones.saturating_sub(1);
        }
        self.tombstones = 0;
        None
    }

    fn remove_offset(&mut self, offset: u64) -> Option<QueueMessage> {
        let qm = self.take_live(offset)?;
        self.tombstones = self.tombstones.saturating_add(1);
        self.maybe_compact();
        Some(qm)
    }

    fn remove_message_id(&mut self, message_id: &str) -> Vec<QueueMessage> {
        let Some(offsets) = self.by_id.remove(message_id) else {
            return Vec::new();
        };
        let mut out = Vec::with_capacity(offsets.len());
        for offset in offsets {
            if let Some(qm) = self.live.remove(&offset) {
                self.tombstones = self.tombstones.saturating_add(1);
                out.push(qm);
            }
        }
        self.maybe_compact();
        out
    }

    fn maybe_compact(&mut self) {
        if self.tombstones > self.live.len().max(32) {
            let live = &self.live;
            self.order.retain(|offset| live.contains_key(offset));
            self.tombstones = 0;
        }
    }

    fn has_ttl(&self, offset: u64, expires_at: Option<tokio::time::Instant>) -> bool {
        self.live
            .get(&offset)
            .is_some_and(|message| message.expires_at == expires_at)
    }

    fn drain_all(&mut self) -> Vec<QueueMessage> {
        self.order.clear();
        self.by_id.clear();
        self.tombstones = 0;
        self.live.drain().map(|(_, message)| message).collect()
    }
}

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
    /// Quorum FIFO. Drops by id leave holes instead of sliding the tail.
    Indexed(IndexedFifo),
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
    /// Classic queues stay a deque. A quorum queue without priority uses the indexed set.
    pub fn for_queue(args: &QueueArgs) -> Self {
        if args.queue_type == Some(QueueType::Quorum) && args.max_priority.unwrap_or(0) == 0 {
            return Self::Indexed(IndexedFifo::new());
        }
        Self::from_max_priority(args.max_priority)
    }

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
            Self::Indexed(q) => q.len(),
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
            Self::Fifo(_) | Self::Indexed(_) => None,
            Self::Priority { max_priority, .. } => Some(*max_priority),
        }
    }

    /// Per-priority ready counts (`None` for FIFO).
    pub fn counts_by_priority(&self) -> Option<Vec<u32>> {
        match self {
            Self::Fifo(_) | Self::Indexed(_) => None,
            Self::Priority { lanes, .. } => Some(lanes.iter().map(|l| l.len() as u32).collect()),
        }
    }

    /// Append to the back of the appropriate structure (enqueue / requeue).
    pub fn push_back(&mut self, qm: QueueMessage) {
        match self {
            Self::Fifo(q) => q.push_back(qm),
            Self::Indexed(q) => q.push_back(qm),
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
            Self::Indexed(q) => q.push_front(qm),
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
            Self::Indexed(q) => q.pop_front(),
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
            Self::Indexed(q) => q.pop_front(),
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
            Self::Indexed(q) => q.remove_offset(offset.0),
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

    /// Remove every ready body whose message id is `message_id`.
    ///
    /// The head is the usual quorum drop, and that case does not walk the
    /// queue. A later id is removed in place. The other bodies stay in order.
    pub fn remove_message_id(&mut self, message_id: &str) -> Vec<QueueMessage> {
        match self {
            Self::Fifo(q) => remove_id_from_deque(q, message_id),
            Self::Indexed(q) => q.remove_message_id(message_id),
            Self::Priority { lanes, len, .. } => {
                let mut out = Vec::new();
                for lane in lanes.iter_mut() {
                    let dropped = remove_id_from_deque(lane, message_id);
                    *len = len.saturating_sub(dropped.len());
                    out.extend(dropped);
                }
                out
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
            Self::Indexed(q) => q.has_ttl(offset.0, expires_at),
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
            Self::Indexed(q) => q.drain_all(),
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

/// Take every body in `q` whose id is `message_id`. Head matches return without a scan.
fn remove_id_from_deque(q: &mut VecDeque<QueueMessage>, message_id: &str) -> Vec<QueueMessage> {
    let mut out = Vec::new();
    while q.front().and_then(|m| m.message.message_id.as_deref()) == Some(message_id) {
        if let Some(qm) = q.pop_front() {
            out.push(qm);
        }
    }
    let mut i = 0;
    while i < q.len() {
        let matches = q.get(i).and_then(|m| m.message.message_id.as_deref()) == Some(message_id);
        if matches {
            if let Some(qm) = q.remove(i) {
                out.push(qm);
            }
        } else {
            i += 1;
        }
    }
    out
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

    fn qm_id(offset: u64, body: &[u8], id: &str) -> QueueMessage {
        qm_prio(offset, body, id, None)
    }

    fn qm_prio(offset: u64, body: &[u8], id: &str, priority: Option<u8>) -> QueueMessage {
        let mut message = (*msg(body, priority)).clone();
        message.message_id = Some(CompactString::from(id));
        QueueMessage::new(QueueOffset(offset), Arc::new(message))
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
    fn remove_message_id_keeps_fifo_order() {
        let mut ready = Ready::from_max_priority(None);
        for n in 0..64 {
            ready.push_back(qm_id(n, &[n as u8], &format!("id-{n}")));
        }
        let head = ready.remove_message_id("id-0");
        assert_eq!(head.len(), 1);
        assert_eq!(head[0].offset.0, 0);
        let dropped = ready.remove_message_id("id-40");
        assert_eq!(dropped.len(), 1);
        assert_eq!(dropped[0].offset.0, 40);
        assert_eq!(ready.len(), 62);
        assert!(ready.remove_message_id("id-40").is_empty());
        let mut seen = Vec::new();
        while let Some(message) = ready.pop_front() {
            seen.push(message.offset.0);
        }
        let expected: Vec<u64> = (1..64).filter(|n| *n != 40).collect();
        assert_eq!(seen, expected);
    }

    #[test]
    fn quorum_remove_does_not_slide_the_tail() {
        let mut args = QueueArgs::default();
        args.queue_type = Some(QueueType::Quorum);
        let mut ready = Ready::for_queue(&args);
        assert!(matches!(ready, Ready::Indexed(_)));
        for n in 0..128 {
            ready.push_back(qm_id(n, &[n as u8], &format!("id-{n}")));
        }
        // Drop a body from the middle, the way an out-of-order quorum drop does.
        let dropped = ready.remove_message_id("id-90");
        assert_eq!(dropped.len(), 1);
        assert_eq!(dropped[0].offset.0, 90);
        assert_eq!(ready.len(), 127);
        assert_eq!(ready.pop_front().unwrap().offset.0, 0);
        let dropped_head = ready.remove_message_id("id-1");
        assert_eq!(dropped_head[0].offset.0, 1);
        assert_eq!(ready.pop_front().unwrap().offset.0, 2);
    }

    #[test]
    fn classic_for_queue_stays_a_deque() {
        let ready = Ready::for_queue(&QueueArgs::default());
        assert!(matches!(ready, Ready::Fifo(_)));
        let mut args = QueueArgs::default();
        args.queue_type = Some(QueueType::Quorum);
        args.max_priority = Some(2);
        let ready = Ready::for_queue(&args);
        assert!(matches!(ready, Ready::Priority { .. }));
    }

    #[test]
    fn quorum_compact_keeps_survivor_order() {
        let mut args = QueueArgs::default();
        args.queue_type = Some(QueueType::Quorum);
        let mut ready = Ready::for_queue(&args);
        for n in 0..4000 {
            ready.push_back(qm_id(n, b"x", &format!("id-{n}")));
        }
        for n in (0..4000).filter(|n| n % 2 == 1) {
            let dropped = ready.remove_message_id(&format!("id-{n}"));
            assert_eq!(dropped.len(), 1);
            assert_eq!(dropped[0].offset.0, n);
        }
        // Tombstones now equal the survivors. One more drop compacts.
        let extra = ready.remove_message_id("id-0");
        assert_eq!(extra.len(), 1);
        assert_eq!(ready.len(), 1999);
        let mut seen = Vec::new();
        while let Some(message) = ready.pop_front() {
            seen.push(message.offset.0);
        }
        let expected: Vec<u64> = (2..4000).filter(|n| n % 2 == 0).collect();
        assert_eq!(seen, expected);
        assert!(ready.remove_message_id("id-2").is_empty());
    }

    #[test]
    fn remove_message_id_on_a_priority_lane() {
        let mut ready = Ready::from_max_priority(Some(2));
        ready.push_back(qm_prio(1, b"low-old", "low-old", Some(0)));
        ready.push_back(qm_prio(2, b"high", "high", Some(2)));
        ready.push_back(qm_prio(3, b"low-new", "low-new", Some(0)));
        let dropped = ready.remove_message_id("low-old");
        assert_eq!(dropped.len(), 1);
        assert_eq!(dropped[0].offset.0, 1);
        assert_eq!(ready.pop_front().unwrap().message.body.as_ref(), b"high");
        assert_eq!(ready.pop_front().unwrap().message.body.as_ref(), b"low-new");
        assert!(ready.pop_front().is_none());
    }

    #[test]
    fn missing_priority_is_zero() {
        assert_eq!(effective_priority(msg(b"x", None).as_ref(), 9), 0);
        assert_eq!(effective_priority(msg(b"x", Some(3)).as_ref(), 9), 3);
        assert_eq!(effective_priority(msg(b"x", Some(99)).as_ref(), 9), 9);
    }
}
