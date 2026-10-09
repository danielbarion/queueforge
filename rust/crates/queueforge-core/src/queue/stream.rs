//! Stream queues (`x-queue-type: stream`), served over AMQP as RabbitMQ 4 does.
//!
//! A stream is an append-only log. Publishing appends; consuming never
//! removes. Each consumer reads from its own offset, set by
//! `x-stream-offset`, with prefetch credit returned on ack. The log is the
//! queue's write-ahead log: entries are never acknowledged except when
//! retention (`x-max-length-bytes`, `x-max-age`) drops them, so recovery
//! returns the whole stream. Each delivery carries `x-stream-offset`.
//!
//! On a Raft cluster that runs queue groups, a stream is replicated: a
//! client append is committed as an `sappend` in the stream's group, and
//! every member appends committed entries in log order, so all assign the
//! same offsets (docs/raft.md, section 6). The entry's Raft index is kept
//! in a header this actor never delivers, so a replay after a restart is
//! skipped.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use compact_str::CompactString;
use tokio::sync::{mpsc, oneshot};
use tracing::warn;

use super::cmd::{
    AppHeaderValue, ConsumerDeliveryId, ConsumerSessionId, EnqueueCompletion, Message, QueueCmd,
    QueueDelivery, QueueMessage, QueueOffset, QueueStats, StreamCopy, StreamEntryCopy,
    StreamReplicator, StreamStart,
};
use super::durable::{DurableQueueLog, QueueActorBootstrap};
use super::registry::{QueueInfo, QueueKey};
use crate::error::Error;

/// Header holding the Raft index of a replicated entry. Never delivered.
const RAFT_HEADER: &str = "x-qf-raft";

fn raft_index_of(message: &Message) -> u64 {
    message
        .headers
        .app
        .iter()
        .find(|(k, _)| k == RAFT_HEADER)
        .and_then(|(_, v)| match v {
            AppHeaderValue::I64(n) => u64::try_from(*n).ok(),
            _ => None,
        })
        .unwrap_or(0)
}

fn without_raft_header(message: &Message) -> Message {
    let mut out = message.clone();
    out.headers.app.retain(|(k, _)| k != RAFT_HEADER);
    out
}

struct Entry {
    offset: u64,
    at_ms: u64,
    message: Arc<Message>,
}

struct Reader {
    cursor: u64,
    credit: Option<u32>,
    tx: mpsc::Sender<QueueDelivery>,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

struct Stream {
    log: VecDeque<Entry>,
    next: u64,
    bytes: u64,
    readers: HashMap<ConsumerSessionId, Reader>,
    starts: HashMap<ConsumerSessionId, StreamStart>,
    wal: Option<Box<dyn DurableQueueLog>>,
    max_bytes: Option<u64>,
    max_age: Option<Duration>,
    next_delivery: u64,
    /// Raft index of the last applied replicated append.
    raft_index: u64,
}

/// Stream offsets start at 0, as RabbitMQ's do. The WAL numbers entries
/// from 1, so the visible offset is one less than the stored one.
fn visible(stored: u64) -> u64 {
    stored.saturating_sub(1)
}

/// The stored offset for a visible one.
fn stored(visible: u64) -> u64 {
    visible.saturating_add(1)
}

impl Stream {
    fn first(&self) -> u64 {
        self.log.front().map(|e| e.offset).unwrap_or(self.next)
    }

    fn start_at(&self, start: StreamStart) -> u64 {
        match start {
            StreamStart::First => self.first(),
            StreamStart::Last => self.log.back().map(|e| e.offset).unwrap_or(self.next),
            StreamStart::Next => self.next,
            StreamStart::Offset(n) => stored(n).clamp(self.first(), self.next),
            StreamStart::TimestampMs(ts) => self
                .log
                .iter()
                .find(|e| e.at_ms >= ts)
                .map(|e| e.offset)
                .unwrap_or(self.next),
            StreamStart::AgeMs(age) => {
                let since = now_ms().saturating_sub(age);
                self.log
                    .iter()
                    .find(|e| e.at_ms >= since)
                    .map(|e| e.offset)
                    .unwrap_or(self.next)
            }
        }
    }

    fn append(&mut self, msg: Arc<Message>, at_ms: u64) -> Result<u64, Error> {
        let offset = self.next;
        if let Some(wal) = self.wal.as_mut() {
            wal.append_enqueue(QueueOffset(offset), &msg)?;
            wal.fsync()?;
        }
        self.next += 1;
        self.bytes = self.bytes.saturating_add(msg.body.len() as u64);
        self.log.push_back(Entry {
            offset,
            at_ms,
            message: msg,
        });
        self.trim();
        Ok(offset)
    }

    /// Append one committed replicated entry, unless it is a replay.
    fn apply(&mut self, index: u64, at_ms: u64, msg: Arc<Message>) -> Result<(), Error> {
        if index <= self.raft_index {
            return Ok(());
        }
        let mut message = (*msg).clone();
        message.headers.app.retain(|(k, _)| k != RAFT_HEADER);
        message.headers.app.push((
            CompactString::from(RAFT_HEADER),
            AppHeaderValue::I64(index as i64),
        ));
        self.append(Arc::new(message), at_ms)?;
        self.raft_index = index;
        Ok(())
    }

    fn dump(&self) -> StreamCopy {
        StreamCopy {
            first: visible(self.first()),
            next: visible(self.next),
            raft_index: self.raft_index,
            entries: self
                .log
                .iter()
                .map(|e| StreamEntryCopy {
                    offset: visible(e.offset),
                    at_ms: e.at_ms,
                    message: Arc::new(without_raft_header(&e.message)),
                })
                .collect(),
        }
    }

    /// Replace every entry with a leader's copy.
    fn install(&mut self, copy: StreamCopy) -> Result<(), Error> {
        // Stage the new view. WAL errors leave the applied index and visible state unchanged.
        let count = copy.entries.len();
        let mut log = VecDeque::new();
        let mut bytes = 0u64;
        for (i, e) in copy.entries.into_iter().enumerate() {
            let mut message = without_raft_header(&e.message);
            if i + 1 == count {
                message.headers.app.push((
                    CompactString::from(RAFT_HEADER),
                    AppHeaderValue::I64(copy.raft_index as i64),
                ));
            }
            let message = Arc::new(message);
            bytes = bytes.saturating_add(message.body.len() as u64);
            log.push_back(Entry {
                offset: stored(e.offset),
                at_ms: e.at_ms,
                message,
            });
        }
        if let Some(wal) = self.wal.as_mut() {
            // Retained offsets must not be acknowledged: that watermark would hide them on restart.
            for old in &self.log {
                if !log.iter().any(|entry| entry.offset == old.offset) {
                    wal.acknowledge(QueueOffset(old.offset))?;
                }
            }
            for e in &log {
                wal.append_enqueue(QueueOffset(e.offset), &e.message)?;
            }
            wal.fsync()?;
        }
        self.log = log;
        self.bytes = bytes;
        self.next = stored(copy.next).max(self.log.back().map(|e| e.offset + 1).unwrap_or(0));
        self.raft_index = copy.raft_index;
        let (first, next) = (self.first(), self.next);
        for reader in self.readers.values_mut() {
            reader.cursor = reader.cursor.clamp(first, next);
        }
        Ok(())
    }

    /// Drop the oldest entries past the byte or age limit. The newest stays.
    fn trim(&mut self) {
        let now = now_ms();
        while self.log.len() > 1 {
            let front = self.log.front().expect("non-empty");
            let too_big = self.max_bytes.is_some_and(|max| self.bytes > max);
            let too_old = self
                .max_age
                .is_some_and(|age| now.saturating_sub(front.at_ms) > age.as_millis() as u64);
            if !too_big && !too_old {
                break;
            }
            let e = self.log.pop_front().expect("non-empty");
            self.bytes = self.bytes.saturating_sub(e.message.body.len() as u64);
            if let Some(wal) = self.wal.as_mut() {
                let _ = wal.acknowledge(QueueOffset(e.offset));
            }
        }
    }

    fn pump(&mut self) {
        let first = self.first();
        let mut gone = Vec::new();
        for (session, reader) in self.readers.iter_mut() {
            if reader.cursor < first {
                reader.cursor = first;
            }
            while reader.cursor < self.next && reader.credit.is_none_or(|c| c > 0) {
                let at = (reader.cursor - first) as usize;
                let Some(entry) = self.log.get(at) else { break };
                let mut message = (*entry.message).clone();
                message
                    .headers
                    .app
                    .retain(|(k, _)| k != "x-stream-offset" && k != RAFT_HEADER);
                message.headers.app.push((
                    CompactString::from("x-stream-offset"),
                    AppHeaderValue::I64(visible(entry.offset) as i64),
                ));
                self.next_delivery += 1;
                let delivery = QueueDelivery {
                    session: *session,
                    delivery_id: ConsumerDeliveryId(self.next_delivery),
                    message: QueueMessage {
                        offset: QueueOffset(entry.offset),
                        message: Arc::new(message),
                        expires_at: None,
                    },
                    settles_on_write: false,
                    server_cancel: false,
                };
                match reader.tx.try_send(delivery) {
                    Ok(()) => {
                        reader.cursor += 1;
                        if let Some(c) = reader.credit.as_mut() {
                            *c -= 1;
                        }
                    }
                    Err(mpsc::error::TrySendError::Full(_)) => break,
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        gone.push(*session);
                        break;
                    }
                }
            }
        }
        for session in gone {
            self.readers.remove(&session);
        }
    }

    /// First and last visible offsets, or `None` while the stream is empty.
    fn offsets(&self) -> Option<(u64, u64)> {
        let first = self.log.front()?.offset;
        let last = self.log.back()?.offset;
        Some((visible(first), visible(last)))
    }

    fn stats(&self) -> QueueStats {
        QueueStats {
            messages_ready: self.log.len() as u32,
            messages_unacked: 0,
            consumer_count: self.readers.len() as u32,
            max_priority: None,
            ready_by_priority: None,
        }
    }
}

/// Run a stream queue until shutdown or until the mailbox closes.
pub async fn run_stream(
    key: QueueKey,
    mut rx: mpsc::Receiver<QueueCmd>,
    info: Arc<QueueInfo>,
    boot: QueueActorBootstrap,
) {
    let _ = info;
    let replicator = boot.replicator.clone();
    let mut s = Stream {
        log: VecDeque::new(),
        next: boot.next_offset,
        bytes: 0,
        readers: HashMap::new(),
        starts: HashMap::new(),
        wal: boot.durable_log,
        max_bytes: boot.args.max_length_bytes,
        max_age: boot.args.max_age_ms.map(Duration::from_millis),
        next_delivery: 0,
        raft_index: 0,
    };
    // Recovered entries keep their offsets. Arrival times are not stored, so
    // they count as now for x-max-age.
    for qm in boot.ready {
        s.bytes = s.bytes.saturating_add(qm.message.body.len() as u64);
        s.next = s.next.max(qm.offset.0 + 1);
        s.raft_index = s.raft_index.max(raft_index_of(&qm.message));
        s.log.push_back(Entry {
            offset: qm.offset.0,
            at_ms: now_ms(),
            message: qm.message,
        });
    }
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    loop {
        let cmd = tokio::select! {
            cmd = rx.recv() => match cmd { Some(c) => c, None => break },
            _ = tick.tick() => {
                s.trim();
                s.pump();
                continue;
            }
        };
        match cmd {
            QueueCmd::Enqueue { msg, reply } => {
                // A replicated stream appends what its group commits, in log
                // order. The publish is confirmed once committed and applied.
                let rep: Option<Arc<dyn StreamReplicator>> = replicator
                    .as_ref()
                    .and_then(|slot| slot.read().unwrap_or_else(|e| e.into_inner()).clone());
                if let Some(rep) = rep.filter(|r| r.replicated(&key)) {
                    let (tx, rx) = oneshot::channel();
                    let commit = rep.append(key.clone(), msg);
                    tokio::spawn(async move {
                        let _ = tx.send(commit.await);
                    });
                    let _ = reply.send(Ok(EnqueueCompletion {
                        offset: QueueOffset(0),
                        durable_done: rx,
                    }));
                    continue;
                }
                let done = s.append(msg, now_ms()).map(|offset| {
                    let (tx, rx) = oneshot::channel();
                    let _ = tx.send(Ok(()));
                    EnqueueCompletion {
                        offset: QueueOffset(offset),
                        durable_done: rx,
                    }
                });
                let _ = reply.send(done);
                s.pump();
            }
            QueueCmd::ResetReplicas { reply } => {
                let _ = reply.send(Err(Error::PreconditionFailed(
                    "cannot reset quorum replicas on a stream".into(),
                )));
            }
            QueueCmd::FlushDurable { reply, .. } => {
                let _ = reply.send(Ok(()));
            }
            QueueCmd::StreamStart { session, start } => {
                s.starts.insert(session, start);
            }
            QueueCmd::StreamApply {
                index,
                at_ms,
                msg,
                reply,
            } => {
                let _ = reply.send(s.apply(index, at_ms, msg));
                s.pump();
            }
            QueueCmd::StreamDump { reply } => {
                let _ = reply.send(s.dump());
            }
            QueueCmd::StreamInstall { copy, reply } => {
                let _ = reply.send(s.install(copy));
                s.pump();
            }
            QueueCmd::RegisterConsumer {
                session,
                initial_credit,
                deliver_tx,
                reply,
                ..
            } => {
                let start = s.starts.remove(&session).unwrap_or(StreamStart::Next);
                let cursor = s.start_at(start);
                s.readers.insert(
                    session,
                    Reader {
                        cursor,
                        credit: initial_credit,
                        tx: deliver_tx,
                    },
                );
                let _ = reply.send(Ok(()));
                s.pump();
            }
            QueueCmd::AddCredit { session, credit } => {
                if let Some(r) = s.readers.get_mut(&session) {
                    if let Some(c) = r.credit.as_mut() {
                        *c = c.saturating_add(credit);
                    }
                }
                s.pump();
            }
            QueueCmd::SetCredit { session, credit } => {
                if let Some(r) = s.readers.get_mut(&session) {
                    r.credit = credit;
                }
                s.pump();
            }
            QueueCmd::UnregisterConsumer { session, reply, .. } => {
                s.readers.remove(&session);
                let _ = reply.send(());
            }
            // A stream ack or nack only frees credit, which the channel returns
            // with AddCredit. The entry stays in the log.
            QueueCmd::Ack { .. }
            | QueueCmd::Nack { .. }
            | QueueCmd::SettleDelivered { .. }
            | QueueCmd::Forget { .. } => {}
            QueueCmd::AckReport { reply, .. } | QueueCmd::NackReport { reply, .. } => {
                let _ = reply.send(None);
            }
            QueueCmd::Deliver { reply, .. } => {
                let _ = reply.send(None);
            }
            // RabbitMQ does not support basic.get on a stream; it is always empty here.
            QueueCmd::Get { reply, .. } => {
                let _ = reply.send(None);
            }
            QueueCmd::RequeueUnacked { reply, .. } => {
                let _ = reply.send(());
            }
            QueueCmd::Purge { reply } => {
                let _ = reply.send(0);
            }
            QueueCmd::Stats { reply } => {
                let _ = reply.send(s.stats());
            }
            QueueCmd::StreamOffsets { reply } => {
                let _ = reply.send(s.offsets());
            }
            QueueCmd::Touch { reply } => {
                let _ = reply.send(());
            }
            QueueCmd::SetArgs { args } => {
                s.max_bytes = args.max_length_bytes;
                s.max_age = args.max_age_ms.map(Duration::from_millis);
                s.trim();
            }
            QueueCmd::DlxResolved { .. } => {}
            QueueCmd::Shutdown { reply } => {
                if let Some(wal) = s.wal.as_mut() {
                    let _ = wal.fsync();
                }
                let _ = reply.send(Ok(()));
                break;
            }
            #[cfg(test)]
            QueueCmd::TestPanic => panic!("stream actor test panic"),
            #[cfg(test)]
            QueueCmd::TestDeferredAppends { reply } => {
                let _ = reply.send(0);
            }
        }
    }
    rx.close();
    while let Some(cmd) = rx.recv().await {
        match cmd {
            QueueCmd::Enqueue { reply, .. } => {
                let _ = reply.send(Err(Error::Unavailable(format!(
                    "stream {key} shutting down"
                ))));
            }
            QueueCmd::RegisterConsumer { reply, .. } => {
                let _ = reply.send(Err(Error::Unavailable(format!(
                    "stream {key} shutting down"
                ))));
            }
            QueueCmd::Shutdown { reply } => {
                let _ = reply.send(Ok(()));
            }
            QueueCmd::Stats { reply } => {
                let _ = reply.send(s.stats());
            }
            QueueCmd::StreamOffsets { reply } => {
                let _ = reply.send(s.offsets());
            }
            QueueCmd::StreamApply { reply, .. }
            | QueueCmd::StreamInstall { reply, .. }
            | QueueCmd::ResetReplicas { reply } => {
                let _ = reply.send(Err(Error::Unavailable(
                    "stream actor is shutting down".into(),
                )));
            }
            QueueCmd::StreamDump { reply } => {
                let _ = reply.send(s.dump());
            }
            _ => {}
        }
    }
    warn!(vhost = %key.vhost, queue = %key.name, "stream actor stopped");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Mutex,
    };

    #[derive(Default)]
    struct LogState {
        fail_append: AtomicBool,
        fail_fsync: AtomicBool,
        acknowledged: Mutex<Vec<u64>>,
    }
    struct TestLog(Arc<LogState>);
    impl DurableQueueLog for TestLog {
        fn append_enqueue(&mut self, _: QueueOffset, _: &Message) -> Result<(), Error> {
            if self.0.fail_append.load(Ordering::Relaxed) {
                Err(Error::Store("injected append failure".into()))
            } else {
                Ok(())
            }
        }
        fn acknowledge(&mut self, offset: QueueOffset) -> Result<(), Error> {
            self.0.acknowledged.lock().unwrap().push(offset.0);
            Ok(())
        }
        fn fsync(&mut self) -> Result<QueueOffset, Error> {
            if self.0.fail_fsync.load(Ordering::Relaxed) {
                Err(Error::Store("injected fsync failure".into()))
            } else {
                Ok(QueueOffset(u64::MAX))
            }
        }
        fn durable_offset(&self) -> QueueOffset {
            QueueOffset(0)
        }
        fn ack_watermark(&self) -> QueueOffset {
            QueueOffset(0)
        }
        fn meta_dirty(&self) -> bool {
            false
        }
        fn compact(&mut self) -> Result<(), Error> {
            Ok(())
        }
    }
    fn stream(state: Arc<LogState>) -> Stream {
        Stream {
            log: VecDeque::new(),
            next: 1,
            bytes: 0,
            readers: HashMap::new(),
            starts: HashMap::new(),
            wal: Some(Box::new(TestLog(state))),
            max_bytes: None,
            max_age: None,
            next_delivery: 0,
            raft_index: 0,
        }
    }
    #[test]
    fn failed_replicated_append_can_be_retried_without_skipping_an_offset() {
        let state = Arc::new(LogState::default());
        let mut s = stream(state.clone());
        for failure in [&state.fail_append, &state.fail_fsync] {
            failure.store(true, Ordering::Relaxed);
            assert!(s.apply(7, 100, Arc::new(Message::blank())).is_err());
            assert_eq!((s.raft_index, s.next, s.log.len()), (0, 1, 0));
            failure.store(false, Ordering::Relaxed);
        }
        s.apply(7, 100, Arc::new(Message::blank())).unwrap();
        s.apply(7, 100, Arc::new(Message::blank())).unwrap();
        s.apply(8, 101, Arc::new(Message::blank())).unwrap();
        assert_eq!((s.raft_index, s.next, s.log.len()), (8, 3, 2));
        assert_eq!(
            s.log.iter().map(|e| e.offset).collect::<Vec<_>>(),
            vec![1, 2]
        );
    }
    #[test]
    fn snapshot_keeps_retained_offsets_unacknowledged_and_does_not_advance_on_error() {
        let state = Arc::new(LogState::default());
        let mut s = stream(state.clone());
        s.apply(7, 100, Arc::new(Message::blank())).unwrap();
        s.apply(8, 101, Arc::new(Message::blank())).unwrap();
        let mut copy = s.dump();
        copy.entries.remove(0);
        copy.first = 1;
        copy.raft_index = 9;
        state.fail_fsync.store(true, Ordering::Relaxed);
        assert!(s.install(copy.clone()).is_err());
        assert_eq!((s.raft_index, s.next, s.log.len()), (8, 3, 2));
        assert!(!state.acknowledged.lock().unwrap().contains(&2));
        state.fail_fsync.store(false, Ordering::Relaxed);
        s.install(copy).unwrap();
        assert_eq!((s.raft_index, s.next, s.log.len()), (9, 3, 1));
        assert_eq!(s.log.front().unwrap().offset, 2);
        assert!(!state.acknowledged.lock().unwrap().contains(&2));
    }
}
