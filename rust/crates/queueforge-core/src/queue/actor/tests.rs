//! Queue actor tests for enqueue, delivery, TTL, overflow, and fsync timing.

use std::time::Duration;

use super::super::args::OverflowPolicy;
use super::super::cmd::{EnqueueCompletion, QueueStats};
use super::*;
use crate::memory::MemoryTracker;
use bytes::Bytes;
use compact_str::CompactString;
use tokio::sync::oneshot;

fn sample_msg(body: &[u8]) -> Arc<Message> {
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
        priority: None,
        timestamp: None,
        expires_unix_ms: None,
        headers: Default::default(),
    })
}

fn sample_msg_exp(body: &[u8], exp_ms: &str) -> Arc<Message> {
    let mut m = (*sample_msg(body)).clone();
    m.expiration = Some(CompactString::from(exp_ms));
    Arc::new(m)
}

fn sample_msg_prio(body: &[u8], priority: u8) -> Arc<Message> {
    let mut m = (*sample_msg(body)).clone();
    m.priority = Some(priority);
    Arc::new(m)
}

async fn spawn_actor(name: &str) -> (mpsc::Sender<QueueCmd>, tokio::task::JoinHandle<()>) {
    let key = QueueKey::new("/", name);
    let memory = MemoryTracker::shared();
    let (tx, rx) = mpsc::channel(16);
    let actor = tokio::spawn(run_simple(key, rx, Arc::clone(&memory)));
    (tx, actor)
}

async fn spawn_actor_args(
    name: &str,
    args: QueueArgs,
) -> (mpsc::Sender<QueueCmd>, tokio::task::JoinHandle<()>) {
    let key = QueueKey::new("/", name);
    let memory = MemoryTracker::shared();
    let (tx, rx) = mpsc::channel(16);
    let actor = tokio::spawn(run_with_args(key, rx, Arc::clone(&memory), args));
    (tx, actor)
}

async fn enqueue(tx: &mpsc::Sender<QueueCmd>, msg: Arc<Message>) -> EnqueueCompletion {
    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(QueueCmd::Enqueue {
        msg,
        reply: reply_tx,
    })
    .await
    .unwrap();
    reply_rx.await.unwrap().unwrap()
}

async fn stats(tx: &mpsc::Sender<QueueCmd>) -> QueueStats {
    let (stats_tx, stats_rx) = oneshot::channel();
    tx.send(QueueCmd::Stats { reply: stats_tx }).await.unwrap();
    stats_rx.await.unwrap()
}

async fn shutdown(tx: mpsc::Sender<QueueCmd>, actor: tokio::task::JoinHandle<()>) {
    let (shut_tx, shut_rx) = oneshot::channel();
    tx.send(QueueCmd::Shutdown { reply: shut_tx })
        .await
        .unwrap();
    shut_rx.await.unwrap().expect("shutdown ok");
    actor.await.unwrap();
}

struct SlowLog {
    delay: Duration,
    entered: Arc<std::sync::atomic::AtomicBool>,
    released: Arc<std::sync::atomic::AtomicBool>,
    appends: Arc<std::sync::atomic::AtomicU64>,
}

impl DurableQueueLog for SlowLog {
    fn append_enqueue(&mut self, _offset: QueueOffset, _msg: &Message) -> crate::error::Result<()> {
        self.appends
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
    fn acknowledge(&mut self, _offset: QueueOffset) -> crate::error::Result<()> {
        Ok(())
    }
    fn fsync(&mut self) -> crate::error::Result<QueueOffset> {
        self.entered
            .store(true, std::sync::atomic::Ordering::SeqCst);
        std::thread::sleep(self.delay);
        self.released
            .store(true, std::sync::atomic::Ordering::SeqCst);
        Ok(QueueOffset(u64::MAX))
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
    fn compact(&mut self) -> crate::error::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn slow_durable_fsync_does_not_block_other_queue() {
    let entered = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let key = QueueKey::new("/", "slow");
    let memory = MemoryTracker::shared();
    let info = Arc::new(QueueInfo::new(
        key.clone(),
        &crate::queue::QueueDeclareOpts {
            durable: true,
            ..crate::queue::QueueDeclareOpts::default()
        },
    ));
    let mut boot = QueueActorBootstrap::new_empty(true).with_log(
        Box::new(SlowLog {
            delay: Duration::from_millis(300),
            entered: Arc::clone(&entered),
            released: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            appends: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }),
        DurabilityPolicy {
            policy: FsyncPolicy::Always,
            interval: Duration::from_millis(100),
            every_n_messages: 1,
        },
    );
    boot.durable = true;
    let (tx_slow, rx_slow) = mpsc::channel(8);
    let slow = tokio::spawn(run(key, rx_slow, Arc::clone(&memory), None, info, boot));

    let (tx_fast, fast_actor) = spawn_actor("fast").await;
    let mut msg = (*sample_msg(b"durable")).clone();
    msg.persistent = true;
    let slow_done = enqueue(&tx_slow, Arc::new(msg)).await;
    let started = std::time::Instant::now();
    while !entered.load(std::sync::atomic::Ordering::SeqCst) {
        if started.elapsed() > Duration::from_secs(2) {
            panic!("slow fsync did not start");
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let fast_done = enqueue(&tx_fast, sample_msg(b"transient")).await;
    let fast_at = std::time::Instant::now();
    fast_done.durable_done.await.unwrap().unwrap();
    assert!(
        fast_at.elapsed() < Duration::from_millis(150),
        "transient enqueue waited on the other queue's fsync"
    );
    slow_done.durable_done.await.unwrap().unwrap();
    shutdown(tx_slow, slow).await;
    shutdown(tx_fast, fast_actor).await;
}

/// Interval fsync still runs. The confirm returns only after that fsync covers the append.
#[tokio::test]
async fn every_n_ms_confirm_waits_for_the_interval_fsync() {
    let entered = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let key = QueueKey::new("/", "group");
    let memory = MemoryTracker::shared();
    let info = Arc::new(QueueInfo::new(
        key.clone(),
        &crate::queue::QueueDeclareOpts {
            durable: true,
            ..crate::queue::QueueDeclareOpts::default()
        },
    ));
    let mut boot = QueueActorBootstrap::new_empty(true).with_log(
        Box::new(SlowLog {
            delay: Duration::from_millis(80),
            entered: Arc::clone(&entered),
            released: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            appends: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }),
        DurabilityPolicy {
            policy: FsyncPolicy::EveryNMs,
            interval: Duration::from_millis(40),
            every_n_messages: 1,
        },
    );
    boot.durable = true;
    let (tx, rx) = mpsc::channel(8);
    let actor = tokio::spawn(run(key, rx, memory, None, info, boot));

    let mut msg = (*sample_msg(b"durable")).clone();
    msg.persistent = true;
    let mut done = enqueue(&tx, Arc::new(msg)).await;
    let started = std::time::Instant::now();
    let wait = std::time::Instant::now();
    while !entered.load(std::sync::atomic::Ordering::SeqCst) {
        if wait.elapsed() > Duration::from_secs(2) {
            panic!("group-commit timer did not fsync");
        }
        tokio::task::yield_now().await;
    }
    assert!(
        wait.elapsed() < Duration::from_millis(30),
        "lone confirm waited out the interval before fsync started"
    );
    assert!(
        done.durable_done.try_recv().is_err(),
        "publisher confirm returned before the interval fsync finished"
    );
    done.durable_done.await.unwrap().unwrap();
    assert!(
        started.elapsed() >= Duration::from_millis(60),
        "publisher confirm returned before the group-commit fsync"
    );
    shutdown(tx, actor).await;
}

/// A deep burst after one interval sync is covered by the next sync, not the next tick.
#[tokio::test]
async fn pipeline_burst_after_interval_sync_does_not_wait_another_tick() {
    let key = QueueKey::new("/", "pipeline-follow");
    let memory = MemoryTracker::shared();
    let info = Arc::new(QueueInfo::new(
        key.clone(),
        &crate::queue::QueueDeclareOpts {
            durable: true,
            ..crate::queue::QueueDeclareOpts::default()
        },
    ));
    let mut boot = QueueActorBootstrap::new_empty(true).with_log(
        Box::new(SlowLog {
            delay: Duration::from_millis(30),
            entered: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            released: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            appends: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }),
        DurabilityPolicy {
            policy: FsyncPolicy::EveryNMs,
            interval: Duration::from_millis(400),
            every_n_messages: 1,
        },
    );
    boot.durable = true;
    let (tx, rx) = mpsc::channel(128);
    let actor = tokio::spawn(run(key, rx, memory, None, info, boot));

    let mut first = (*sample_msg(b"first")).clone();
    first.persistent = true;
    let first_done = enqueue(&tx, Arc::new(first)).await;
    let started = std::time::Instant::now();
    first_done.durable_done.await.unwrap().unwrap();
    assert!(
        started.elapsed() >= Duration::from_millis(20),
        "the first confirm returned before its fsync"
    );
    assert!(
        started.elapsed() < Duration::from_millis(80),
        "the first confirm waited out the interval"
    );

    let burst = std::time::Instant::now();
    let mut waiting = Vec::new();
    for i in 0..96u8 {
        let mut msg = (*sample_msg(&[i])).clone();
        msg.persistent = true;
        waiting.push(enqueue(&tx, Arc::new(msg)).await);
    }
    for done in waiting {
        done.durable_done.await.unwrap().unwrap();
    }
    let elapsed = burst.elapsed();
    assert!(
        elapsed >= Duration::from_millis(20),
        "pipeline confirms returned before the covering fsync: {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_millis(250),
        "pipeline confirms waited for another interval: {elapsed:?}"
    );

    // A single publish after that burst flushes on its own, still after the fsync.
    let mut quiet = (*sample_msg(b"quiet")).clone();
    quiet.persistent = true;
    let quiet_started = std::time::Instant::now();
    let quiet_done = enqueue(&tx, Arc::new(quiet)).await;
    quiet_done.durable_done.await.unwrap().unwrap();
    assert!(
        quiet_started.elapsed() >= Duration::from_millis(20),
        "a lone confirm returned before its fsync"
    );
    assert!(
        quiet_started.elapsed() < Duration::from_millis(80),
        "a lone confirm waited out the interval"
    );
    shutdown(tx, actor).await;
}

struct CountLog {
    syncs: Arc<std::sync::atomic::AtomicU64>,
    appends: Arc<std::sync::atomic::AtomicU64>,
}

impl DurableQueueLog for CountLog {
    fn append_enqueue(&mut self, _offset: QueueOffset, _msg: &Message) -> crate::error::Result<()> {
        self.appends
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
    fn acknowledge(&mut self, _offset: QueueOffset) -> crate::error::Result<()> {
        Ok(())
    }
    fn fsync(&mut self) -> crate::error::Result<QueueOffset> {
        self.syncs.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(QueueOffset(u64::MAX))
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
    fn compact(&mut self) -> crate::error::Result<()> {
        Ok(())
    }
}

/// 128 durable publishes already in the mailbox share one fsync. The confirm
/// resolves only after that fsync, not at the 400 ms interval.
#[tokio::test(flavor = "current_thread")]
async fn one_hundred_twenty_eight_waiters_share_one_fsync() {
    let syncs = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let appends = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let key = QueueKey::new("/", "batch-128");
    let memory = MemoryTracker::shared();
    let info = Arc::new(QueueInfo::new(
        key.clone(),
        &crate::queue::QueueDeclareOpts {
            durable: true,
            ..crate::queue::QueueDeclareOpts::default()
        },
    ));
    let mut boot = QueueActorBootstrap::new_empty(true).with_log(
        Box::new(CountLog {
            syncs: Arc::clone(&syncs),
            appends: Arc::clone(&appends),
        }),
        DurabilityPolicy {
            policy: FsyncPolicy::EveryNMs,
            interval: Duration::from_millis(400),
            every_n_messages: 1,
        },
    );
    boot.durable = true;
    let (tx, rx) = mpsc::channel(256);
    let actor = tokio::spawn(run(key, rx, memory, None, info, boot));

    let started = std::time::Instant::now();
    let mut waiting = Vec::new();
    for i in 0..128u8 {
        let mut msg = (*sample_msg(&[i])).clone();
        msg.persistent = true;
        waiting.push(enqueue(&tx, Arc::new(msg)).await);
    }
    for done in waiting {
        done.durable_done.await.unwrap().unwrap();
    }
    assert_eq!(appends.load(std::sync::atomic::Ordering::SeqCst), 128);
    assert_eq!(syncs.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert!(
        started.elapsed() < Duration::from_millis(50),
        "128 confirms waited out the interval: {:?}",
        started.elapsed()
    );
    shutdown(tx, actor).await;
}

/// The second and later lone confirms must not sit out the 1 ms quiet window.
#[tokio::test]
async fn steady_lone_confirm_does_not_wait_a_millisecond() {
    let syncs = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let key = QueueKey::new("/", "steady-lone");
    let memory = MemoryTracker::shared();
    let info = Arc::new(QueueInfo::new(
        key.clone(),
        &crate::queue::QueueDeclareOpts {
            durable: true,
            ..crate::queue::QueueDeclareOpts::default()
        },
    ));
    let mut boot = QueueActorBootstrap::new_empty(true).with_log(
        Box::new(CountLog {
            syncs: Arc::clone(&syncs),
            appends: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }),
        DurabilityPolicy {
            policy: FsyncPolicy::EveryNMs,
            interval: Duration::from_millis(400),
            every_n_messages: 1,
        },
    );
    boot.durable = true;
    let (tx, rx) = mpsc::channel(8);
    let actor = tokio::spawn(run(key, rx, memory, None, info, boot));

    let mut first = (*sample_msg(b"first")).clone();
    first.persistent = true;
    let first_done = enqueue(&tx, Arc::new(first)).await;
    first_done.durable_done.await.unwrap().unwrap();

    let started = std::time::Instant::now();
    for i in 0..12u8 {
        let mut msg = (*sample_msg(&[i])).clone();
        msg.persistent = true;
        let done = enqueue(&tx, Arc::new(msg)).await;
        done.durable_done.await.unwrap().unwrap();
    }
    let elapsed = started.elapsed();
    assert_eq!(
        syncs.load(std::sync::atomic::Ordering::SeqCst),
        13,
        "steady lone confirms did not each wait for their own fsync"
    );
    assert!(
        elapsed < Duration::from_millis(10),
        "steady lone confirms still waited out a quiet window: {elapsed:?}"
    );
    shutdown(tx, actor).await;
}

/// A burst already sitting in the mailbox still shares one fsync after a lone confirm.
#[tokio::test(flavor = "current_thread")]
async fn learned_lone_flag_keeps_a_queued_burst_on_one_fsync() {
    let syncs = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let appends = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let key = QueueKey::new("/", "lone-then-burst");
    let memory = MemoryTracker::shared();
    let info = Arc::new(QueueInfo::new(
        key.clone(),
        &crate::queue::QueueDeclareOpts {
            durable: true,
            ..crate::queue::QueueDeclareOpts::default()
        },
    ));
    let mut boot = QueueActorBootstrap::new_empty(true).with_log(
        Box::new(CountLog {
            syncs: Arc::clone(&syncs),
            appends: Arc::clone(&appends),
        }),
        DurabilityPolicy {
            policy: FsyncPolicy::EveryNMs,
            interval: Duration::from_millis(400),
            every_n_messages: 1,
        },
    );
    boot.durable = true;
    let (tx, rx) = mpsc::channel(256);
    let actor = tokio::spawn(run(key, rx, memory, None, info, boot));

    let mut first = (*sample_msg(b"learn")).clone();
    first.persistent = true;
    let first_done = enqueue(&tx, Arc::new(first)).await;
    first_done.durable_done.await.unwrap().unwrap();
    assert_eq!(syncs.load(std::sync::atomic::Ordering::SeqCst), 1);

    let started = std::time::Instant::now();
    let mut waiting = Vec::new();
    for i in 0..128u8 {
        let mut msg = (*sample_msg(&[i])).clone();
        msg.persistent = true;
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.try_send(QueueCmd::Enqueue {
            msg: Arc::new(msg),
            reply: reply_tx,
        })
        .expect("mailbox accepted the burst");
        waiting.push(reply_rx);
    }
    let mut dones = Vec::new();
    for reply in waiting {
        dones.push(reply.await.unwrap().unwrap());
    }
    for done in dones {
        done.durable_done.await.unwrap().unwrap();
    }
    assert_eq!(appends.load(std::sync::atomic::Ordering::SeqCst), 129);
    assert_eq!(
        syncs.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "a queued burst after a lone confirm fsynced one message at a time"
    );
    assert!(
        started.elapsed() < Duration::from_millis(50),
        "queued burst waited out the interval: {:?}",
        started.elapsed()
    );
    shutdown(tx, actor).await;
}

/// A confirm issued while the interval fsync is blocked waits until its own append is synced.
#[tokio::test]
async fn interval_fsync_does_not_queue_the_next_confirm() {
    let entered = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let released = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let appends = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let key = QueueKey::new("/", "inflight");
    let memory = MemoryTracker::shared();
    let info = Arc::new(QueueInfo::new(
        key.clone(),
        &crate::queue::QueueDeclareOpts {
            durable: true,
            ..crate::queue::QueueDeclareOpts::default()
        },
    ));
    let mut boot = QueueActorBootstrap::new_empty(true).with_log(
        Box::new(SlowLog {
            delay: Duration::from_millis(400),
            entered: Arc::clone(&entered),
            released: Arc::clone(&released),
            appends: Arc::clone(&appends),
        }),
        DurabilityPolicy {
            policy: FsyncPolicy::EveryNMs,
            interval: Duration::from_millis(30),
            every_n_messages: 1,
        },
    );
    boot.durable = true;
    let (tx, rx) = mpsc::channel(8);
    let actor = tokio::spawn(run(key, rx, memory, None, info, boot));

    let mut first = (*sample_msg(b"first")).clone();
    first.persistent = true;
    let first_done = enqueue(&tx, Arc::new(first)).await;
    let wait = std::time::Instant::now();
    while !entered.load(std::sync::atomic::Ordering::SeqCst) {
        if wait.elapsed() > Duration::from_secs(2) {
            panic!("interval fsync did not start");
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(
        !released.load(std::sync::atomic::Ordering::SeqCst),
        "fsync returned before the second publish"
    );

    let mut second = (*sample_msg(b"second")).clone();
    second.persistent = true;
    let second_done = enqueue(&tx, Arc::new(second)).await;
    assert_eq!(
        appends.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "the second durable enqueue was written before the blocked fsync returned"
    );

    // Keep a command queued for the whole fsync. The join has to flush the
    // deferred append anyway, and the following command must observe that.
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let flood_tx = tx.clone();
    loop {
        let (reply_tx, _reply_rx) = oneshot::channel();
        if flood_tx
            .try_send(QueueCmd::Stats { reply: reply_tx })
            .is_err()
        {
            break;
        }
    }
    let flood_stop = Arc::clone(&stop);
    let flood = std::thread::spawn(move || {
        while !flood_stop.load(std::sync::atomic::Ordering::SeqCst) {
            let (reply_tx, _reply_rx) = oneshot::channel();
            if flood_tx
                .blocking_send(QueueCmd::Stats { reply: reply_tx })
                .is_err()
            {
                break;
            }
        }
    });
    let flushed = std::time::Instant::now();
    while appends.load(std::sync::atomic::Ordering::SeqCst) < 2 {
        if flushed.elapsed() > Duration::from_secs(2) {
            panic!("commands in the mailbox prevented the deferred append from flushing");
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(
        released.load(std::sync::atomic::Ordering::SeqCst),
        "deferred append flushed before the interval fsync returned"
    );
    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    tokio::task::spawn_blocking(move || flood.join())
        .await
        .unwrap()
        .unwrap();
    let (snap_tx, snap_rx) = oneshot::channel();
    tx.send(QueueCmd::TestDeferredAppends { reply: snap_tx })
        .await
        .unwrap();
    let still_deferred = snap_rx.await.unwrap();
    assert_eq!(
        still_deferred, 0,
        "following command ran while the deferred append was still unflushed"
    );
    first_done.durable_done.await.unwrap().unwrap();
    second_done.durable_done.await.unwrap().unwrap();
    shutdown(tx, actor).await;
}

/// Quorum flush must not complete while the body is only in `deferred_appends`.
#[tokio::test]
async fn flush_durable_waits_for_the_deferred_append_to_be_fsynced() {
    let release_first = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let release_second = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let appends = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let fsyncs = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let max_appended = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let synced = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let key = QueueKey::new("/", "quorum-flush");
    let memory = MemoryTracker::shared();
    let info = Arc::new(QueueInfo::new(
        key.clone(),
        &crate::queue::QueueDeclareOpts {
            durable: true,
            ..crate::queue::QueueDeclareOpts::default()
        },
    ));
    let mut boot = QueueActorBootstrap::new_empty(true).with_log(
        Box::new(GateLog {
            release_first: Arc::clone(&release_first),
            release_second: Arc::clone(&release_second),
            appends: Arc::clone(&appends),
            fsyncs: Arc::clone(&fsyncs),
            max_appended: Arc::clone(&max_appended),
            synced: Arc::clone(&synced),
        }),
        DurabilityPolicy {
            policy: FsyncPolicy::EveryNMs,
            interval: Duration::from_millis(20),
            every_n_messages: 1,
        },
    );
    boot.durable = true;
    let (tx, rx) = mpsc::channel(8);
    let actor = tokio::spawn(run(key, rx, memory, None, info, boot));

    let mut first = (*sample_msg(b"first")).clone();
    first.persistent = true;
    let first_done = enqueue(&tx, Arc::new(first)).await;
    let wait = std::time::Instant::now();
    while fsyncs.load(std::sync::atomic::Ordering::SeqCst) < 1 {
        if wait.elapsed() > Duration::from_secs(2) {
            panic!("interval fsync did not start");
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let mut second = (*sample_msg(b"kept")).clone();
    second.persistent = true;
    let second_done = enqueue(&tx, Arc::new(second)).await;
    let offset = second_done.offset;

    let (flush_tx, mut flush_rx) = oneshot::channel();
    tx.send(QueueCmd::FlushDurable {
        offset,
        reply: flush_tx,
    })
    .await
    .unwrap();
    let (stats_tx, stats_rx) = oneshot::channel();
    tx.send(QueueCmd::Stats { reply: stats_tx }).await.unwrap();
    stats_rx.await.unwrap();
    assert!(
        flush_rx.try_recv().is_err(),
        "quorum flush completed while the interval fsync still held the log"
    );
    assert_eq!(
        appends.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "the second body was appended before the parked log returned"
    );

    release_first.store(true, std::sync::atomic::Ordering::SeqCst);
    let parked = std::time::Instant::now();
    while fsyncs.load(std::sync::atomic::Ordering::SeqCst) < 2 {
        if parked.elapsed() > Duration::from_secs(2) {
            panic!("deferred append was not fsynced");
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(
        flush_rx.try_recv().is_err(),
        "quorum flush completed before the deferred append was fsynced"
    );
    assert!(
        appends.load(std::sync::atomic::Ordering::SeqCst) >= 2,
        "the body was not in the log when the second fsync started"
    );

    release_second.store(true, std::sync::atomic::Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(2), flush_rx)
        .await
        .expect("quorum flush timed out")
        .unwrap()
        .unwrap();
    assert!(
        synced.load(std::sync::atomic::Ordering::SeqCst) >= offset.0,
        "confirm returned before a fsync covered the enqueue offset"
    );
    first_done.durable_done.await.unwrap().unwrap();
    second_done.durable_done.await.unwrap().unwrap();
    shutdown(tx, actor).await;
}

struct GateLog {
    release_first: Arc<std::sync::atomic::AtomicBool>,
    release_second: Arc<std::sync::atomic::AtomicBool>,
    appends: Arc<std::sync::atomic::AtomicU64>,
    fsyncs: Arc<std::sync::atomic::AtomicU64>,
    max_appended: Arc<std::sync::atomic::AtomicU64>,
    synced: Arc<std::sync::atomic::AtomicU64>,
}

impl DurableQueueLog for GateLog {
    fn append_enqueue(&mut self, offset: QueueOffset, _msg: &Message) -> crate::error::Result<()> {
        self.appends
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.max_appended
            .fetch_max(offset.0, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
    fn acknowledge(&mut self, _offset: QueueOffset) -> crate::error::Result<()> {
        Ok(())
    }
    fn fsync(&mut self) -> crate::error::Result<QueueOffset> {
        let seen = self.max_appended.load(std::sync::atomic::Ordering::SeqCst);
        let n = self
            .fsyncs
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if n < 2 {
            let gate = if n == 0 {
                &self.release_first
            } else {
                &self.release_second
            };
            while !gate.load(std::sync::atomic::Ordering::SeqCst) {
                std::thread::sleep(Duration::from_millis(5));
            }
        }
        self.synced.store(seen, std::sync::atomic::Ordering::SeqCst);
        Ok(QueueOffset(seen))
    }
    fn durable_offset(&self) -> QueueOffset {
        QueueOffset(self.synced.load(std::sync::atomic::Ordering::SeqCst))
    }
    fn ack_watermark(&self) -> QueueOffset {
        QueueOffset(0)
    }
    fn meta_dirty(&self) -> bool {
        false
    }
    fn compact(&mut self) -> crate::error::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn recovered_past_deadline_is_due_immediately() {
    let (tx, actor) = spawn_actor("ttl").await;
    let mut msg = (*sample_msg(b"old")).clone();
    msg.expires_unix_ms = Some(1);
    let _ = enqueue(&tx, Arc::new(msg)).await;
    let stats = stats(&tx).await;
    assert_eq!(
        stats.messages_ready, 0,
        "a deadline already in the past must not sit in ready"
    );
    shutdown(tx, actor).await;
}

#[tokio::test]
async fn enqueue_deliver_ack_roundtrip() {
    let (tx, actor) = spawn_actor("t").await;

    let completion = enqueue(&tx, sample_msg(b"hi")).await;
    assert!(completion.durable_done.await.unwrap().is_ok());

    let (dtx, mut drx) = mpsc::channel(64);
    let (reg_tx, reg_rx) = oneshot::channel();
    tx.send(QueueCmd::RegisterConsumer {
        session: ConsumerSessionId(1),
        no_ack: false,
        exclusive: false,
        priority: 0,
        initial_credit: Some(1),
        deliver_tx: dtx,
        reply: reg_tx,
    })
    .await
    .unwrap();
    reg_rx.await.unwrap().unwrap();

    let delivery = tokio::time::timeout(std::time::Duration::from_secs(1), drx.recv())
        .await
        .expect("timeout")
        .expect("delivery");
    assert_eq!(delivery.message.message.body.as_ref(), b"hi");

    tx.send(QueueCmd::Ack {
        id: delivery.delivery_id,
        multiple_to: None,
    })
    .await
    .unwrap();

    let s = stats(&tx).await;
    assert_eq!(s.messages_ready, 0);
    assert_eq!(s.messages_unacked, 0);
    assert_eq!(s.consumer_count, 1);

    let (shut_tx, shut_rx) = oneshot::channel();
    tx.send(QueueCmd::Shutdown { reply: shut_tx })
        .await
        .unwrap();
    shut_rx.await.unwrap().expect("shutdown ok");
    actor.await.unwrap();
}

#[tokio::test]
async fn nack_requeue_sets_redelivered() {
    let (tx, actor) = spawn_actor("t2").await;
    let _ = enqueue(&tx, sample_msg(b"x")).await;

    let (dtx, mut drx) = mpsc::channel(64);
    let (reg_tx, reg_rx) = oneshot::channel();
    tx.send(QueueCmd::RegisterConsumer {
        session: ConsumerSessionId(7),
        no_ack: false,
        exclusive: false,
        priority: 0,
        initial_credit: Some(10),
        deliver_tx: dtx,
        reply: reg_tx,
    })
    .await
    .unwrap();
    reg_rx.await.unwrap().unwrap();

    let d1 = drx.recv().await.unwrap();
    assert!(!d1.message.message.redelivered);

    tx.send(QueueCmd::Nack {
        id: d1.delivery_id,
        requeue: true,
    })
    .await
    .unwrap();
    tx.send(QueueCmd::AddCredit {
        session: ConsumerSessionId(7),
        credit: 1,
    })
    .await
    .unwrap();

    let d2 = tokio::time::timeout(std::time::Duration::from_secs(1), drx.recv())
        .await
        .expect("timeout")
        .expect("redelivery");
    assert!(d2.message.message.redelivered);

    let (shut_tx, shut_rx) = oneshot::channel();
    tx.send(QueueCmd::Shutdown { reply: shut_tx })
        .await
        .unwrap();
    shut_rx.await.unwrap().expect("shutdown ok");
    actor.await.unwrap();
}

#[tokio::test]
async fn zero_credit_holds_ready_until_add_credit() {
    let (tx, actor) = spawn_actor("t3").await;
    let _ = enqueue(&tx, sample_msg(b"held")).await;

    let (dtx, mut drx) = mpsc::channel(64);
    let (reg_tx, reg_rx) = oneshot::channel();
    tx.send(QueueCmd::RegisterConsumer {
        session: ConsumerSessionId(3),
        no_ack: false,
        exclusive: false,
        priority: 0,
        initial_credit: Some(0),
        deliver_tx: dtx,
        reply: reg_tx,
    })
    .await
    .unwrap();
    reg_rx.await.unwrap().unwrap();

    let no_delivery = tokio::time::timeout(std::time::Duration::from_millis(50), drx.recv()).await;
    assert!(no_delivery.is_err(), "zero credit must not deliver");

    let s = stats(&tx).await;
    assert_eq!(s.messages_ready, 1);
    assert_eq!(s.messages_unacked, 0);

    tx.send(QueueCmd::AddCredit {
        session: ConsumerSessionId(3),
        credit: 1,
    })
    .await
    .unwrap();

    let d = tokio::time::timeout(std::time::Duration::from_secs(1), drx.recv())
        .await
        .expect("timeout")
        .expect("delivery after credit");
    assert_eq!(d.message.message.body.as_ref(), b"held");

    let (shut_tx, shut_rx) = oneshot::channel();
    tx.send(QueueCmd::Shutdown { reply: shut_tx })
        .await
        .unwrap();
    shut_rx.await.unwrap().expect("shutdown ok");
    actor.await.unwrap();
}

#[tokio::test]
async fn unlimited_credit_delivers() {
    let (tx, actor) = spawn_actor("t4").await;
    let _ = enqueue(&tx, sample_msg(b"go")).await;

    let (dtx, mut drx) = mpsc::channel(64);
    let (reg_tx, reg_rx) = oneshot::channel();
    tx.send(QueueCmd::RegisterConsumer {
        session: ConsumerSessionId(4),
        no_ack: false,
        exclusive: false,
        priority: 0,
        initial_credit: None,
        deliver_tx: dtx,
        reply: reg_tx,
    })
    .await
    .unwrap();
    reg_rx.await.unwrap().unwrap();

    let d = tokio::time::timeout(std::time::Duration::from_secs(1), drx.recv())
        .await
        .expect("timeout")
        .expect("delivery");
    assert_eq!(d.message.message.body.as_ref(), b"go");

    shutdown(tx, actor).await;
}

#[tokio::test(start_paused = true)]
async fn message_ttl_expires_ready_only() {
    let args = QueueArgs {
        message_ttl_ms: Some(100),
        ..QueueArgs::default()
    };
    let (tx, actor) = spawn_actor_args("ttl", args).await;
    let _ = enqueue(&tx, sample_msg(b"die")).await;
    assert_eq!(stats(&tx).await.messages_ready, 1);

    tokio::time::advance(Duration::from_millis(150)).await;
    // Yield so the actor can process the timer.
    for _ in 0..10 {
        tokio::task::yield_now().await;
        if stats(&tx).await.messages_ready == 0 {
            break;
        }
        tokio::time::advance(Duration::from_millis(10)).await;
    }
    assert_eq!(stats(&tx).await.messages_ready, 0);

    shutdown(tx, actor).await;
}

#[tokio::test(start_paused = true)]
async fn per_message_expiration_property() {
    let (tx, actor) = spawn_actor("exp").await;
    let _ = enqueue(&tx, sample_msg_exp(b"soon", "50")).await;
    assert_eq!(stats(&tx).await.messages_ready, 1);

    tokio::time::advance(Duration::from_millis(80)).await;
    for _ in 0..10 {
        tokio::task::yield_now().await;
        if stats(&tx).await.messages_ready == 0 {
            break;
        }
        tokio::time::advance(Duration::from_millis(10)).await;
    }
    assert_eq!(stats(&tx).await.messages_ready, 0);

    shutdown(tx, actor).await;
}

#[tokio::test(start_paused = true)]
async fn ttl_does_not_expire_unacked() {
    let args = QueueArgs {
        message_ttl_ms: Some(50),
        ..QueueArgs::default()
    };
    let (tx, actor) = spawn_actor_args("unack-ttl", args).await;
    let _ = enqueue(&tx, sample_msg(b"held")).await;

    let (dtx, mut drx) = mpsc::channel(64);
    let (reg_tx, reg_rx) = oneshot::channel();
    tx.send(QueueCmd::RegisterConsumer {
        session: ConsumerSessionId(1),
        no_ack: false,
        exclusive: false,
        priority: 0,
        initial_credit: Some(1),
        deliver_tx: dtx,
        reply: reg_tx,
    })
    .await
    .unwrap();
    reg_rx.await.unwrap().unwrap();
    let d = drx.recv().await.unwrap();
    assert_eq!(stats(&tx).await.messages_unacked, 1);

    // Past TTL while unacked — must stay unacked.
    tokio::time::advance(Duration::from_millis(200)).await;
    for _ in 0..5 {
        tokio::task::yield_now().await;
    }
    let s = stats(&tx).await;
    assert_eq!(s.messages_unacked, 1);
    assert_eq!(s.messages_ready, 0);

    // Ack cleans up.
    tx.send(QueueCmd::Ack {
        id: d.delivery_id,
        multiple_to: None,
    })
    .await
    .unwrap();
    assert_eq!(stats(&tx).await.messages_unacked, 0);

    shutdown(tx, actor).await;
}

#[tokio::test]
async fn max_length_drop_head() {
    let args = QueueArgs {
        max_length: Some(2),
        overflow: OverflowPolicy::DropHead,
        ..QueueArgs::default()
    };
    let (tx, actor) = spawn_actor_args("ml", args).await;
    let _ = enqueue(&tx, sample_msg(b"a")).await;
    let _ = enqueue(&tx, sample_msg(b"b")).await;
    let _ = enqueue(&tx, sample_msg(b"c")).await; // drops "a"
    assert_eq!(stats(&tx).await.messages_ready, 2);

    // Remaining should be b, c (FIFO after drop-head).
    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(QueueCmd::Get {
        no_ack: true,
        reply: reply_tx,
    })
    .await
    .unwrap();
    let (_, qm, _) = reply_rx.await.unwrap().unwrap();
    assert_eq!(qm.message.body.as_ref(), b"b");

    shutdown(tx, actor).await;
}

#[tokio::test]
async fn priority_p9_before_p0() {
    let args = QueueArgs {
        max_priority: Some(9),
        ..QueueArgs::default()
    };
    let (tx, actor) = spawn_actor_args("prio", args).await;
    // Enqueue low then high — deliver must prefer p=9.
    let _ = enqueue(&tx, sample_msg_prio(b"p0", 0)).await;
    let _ = enqueue(&tx, sample_msg_prio(b"p9", 9)).await;
    let _ = enqueue(&tx, sample_msg_prio(b"p5", 5)).await;

    let s = stats(&tx).await;
    assert_eq!(s.messages_ready, 3);
    assert_eq!(s.max_priority, Some(9));
    let bands = s.ready_by_priority.expect("priority bands");
    assert_eq!(bands.len(), 10);
    assert_eq!(bands[0], 1);
    assert_eq!(bands[5], 1);
    assert_eq!(bands[9], 1);

    async fn get_body(tx: &mpsc::Sender<QueueCmd>) -> Vec<u8> {
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(QueueCmd::Get {
            no_ack: true,
            reply: reply_tx,
        })
        .await
        .unwrap();
        reply_rx.await.unwrap().unwrap().1.message.body.to_vec()
    }

    assert_eq!(get_body(&tx).await, b"p9");
    assert_eq!(get_body(&tx).await, b"p5");
    assert_eq!(get_body(&tx).await, b"p0");

    shutdown(tx, actor).await;
}

#[tokio::test]
async fn priority_drop_head_from_lowest() {
    let args = QueueArgs {
        max_priority: Some(9),
        max_length: Some(2),
        overflow: OverflowPolicy::DropHead,
        ..QueueArgs::default()
    };
    let (tx, actor) = spawn_actor_args("prio-drop", args).await;
    let _ = enqueue(&tx, sample_msg_prio(b"low", 0)).await;
    let _ = enqueue(&tx, sample_msg_prio(b"high", 9)).await;
    // Exceeds max-length=2 → drop lowest-priority oldest ("low").
    let _ = enqueue(&tx, sample_msg_prio(b"mid", 5)).await;
    assert_eq!(stats(&tx).await.messages_ready, 2);

    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(QueueCmd::Get {
        no_ack: true,
        reply: reply_tx,
    })
    .await
    .unwrap();
    let (_, qm, _) = reply_rx.await.unwrap().unwrap();
    assert_eq!(
        qm.message.body.as_ref(),
        b"high",
        "deliver still prefers highest remaining priority"
    );

    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(QueueCmd::Get {
        no_ack: true,
        reply: reply_tx,
    })
    .await
    .unwrap();
    let (_, qm, _) = reply_rx.await.unwrap().unwrap();
    assert_eq!(qm.message.body.as_ref(), b"mid");

    shutdown(tx, actor).await;
}

#[tokio::test]
async fn priority_recovery_relanes_by_offset() {
    // Simulate WAL recovery: flat offset-ordered ready + max_priority args.
    let args = QueueArgs {
        max_priority: Some(9),
        ..QueueArgs::default()
    };
    let mut boot = QueueActorBootstrap::new_empty(false);
    boot.args = args;
    let mut ready = std::collections::VecDeque::new();
    ready.push_back(QueueMessage::new(
        QueueOffset(1),
        sample_msg_prio(b"first-low", 0),
    ));
    ready.push_back(QueueMessage::new(
        QueueOffset(2),
        sample_msg_prio(b"high", 9),
    ));
    ready.push_back(QueueMessage::new(
        QueueOffset(3),
        sample_msg_prio(b"second-low", 0),
    ));
    boot.ready = ready;
    boot.next_offset = 4;

    let key = QueueKey::new("/", "rec-prio");
    let memory = MemoryTracker::shared();
    let (tx, rx) = mpsc::channel(16);
    let info = Arc::new(QueueInfo::new(
        key.clone(),
        &crate::queue::QueueDeclareOpts::default(),
    ));
    let actor = tokio::spawn(run(key, rx, Arc::clone(&memory), None, info, boot));

    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(QueueCmd::Get {
        no_ack: true,
        reply: reply_tx,
    })
    .await
    .unwrap();
    assert_eq!(
        reply_rx.await.unwrap().unwrap().1.message.body.as_ref(),
        b"high"
    );

    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(QueueCmd::Get {
        no_ack: true,
        reply: reply_tx,
    })
    .await
    .unwrap();
    assert_eq!(
        reply_rx.await.unwrap().unwrap().1.message.body.as_ref(),
        b"first-low",
        "same priority preserves offset FIFO after re-lane"
    );

    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(QueueCmd::Get {
        no_ack: true,
        reply: reply_tx,
    })
    .await
    .unwrap();
    assert_eq!(
        reply_rx.await.unwrap().unwrap().1.message.body.as_ref(),
        b"second-low"
    );

    shutdown(tx, actor).await;
}

#[tokio::test]
async fn max_length_reject_publish() {
    let args = QueueArgs {
        max_length: Some(1),
        overflow: OverflowPolicy::RejectPublish,
        ..QueueArgs::default()
    };
    let (tx, actor) = spawn_actor_args("rj", args).await;
    let _ = enqueue(&tx, sample_msg(b"a")).await;

    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(QueueCmd::Enqueue {
        msg: sample_msg(b"b"),
        reply: reply_tx,
    })
    .await
    .unwrap();
    let err = reply_rx.await.unwrap().unwrap_err();
    assert!(matches!(err, Error::PreconditionFailed(_)));
    assert_eq!(stats(&tx).await.messages_ready, 1);

    shutdown(tx, actor).await;
}

#[tokio::test]
async fn max_length_bytes_drop_head() {
    let args = QueueArgs {
        max_length_bytes: Some(4),
        overflow: OverflowPolicy::DropHead,
        ..QueueArgs::default()
    };
    let (tx, actor) = spawn_actor_args("mlb", args).await;
    let _ = enqueue(&tx, sample_msg(b"12")).await; // 2 bytes
    let _ = enqueue(&tx, sample_msg(b"34")).await; // 2 bytes → total 4
    let _ = enqueue(&tx, sample_msg(b"56")).await; // need room → drop first
    assert_eq!(stats(&tx).await.messages_ready, 2);

    shutdown(tx, actor).await;
}

#[tokio::test]
async fn nack_no_requeue_without_dlx_drops() {
    let (tx, actor) = spawn_actor("nack-drop").await;
    let _ = enqueue(&tx, sample_msg(b"x")).await;

    let (dtx, mut drx) = mpsc::channel(64);
    let (reg_tx, reg_rx) = oneshot::channel();
    tx.send(QueueCmd::RegisterConsumer {
        session: ConsumerSessionId(1),
        no_ack: false,
        exclusive: false,
        priority: 0,
        initial_credit: Some(1),
        deliver_tx: dtx,
        reply: reg_tx,
    })
    .await
    .unwrap();
    reg_rx.await.unwrap().unwrap();
    let d = drx.recv().await.unwrap();

    tx.send(QueueCmd::Nack {
        id: d.delivery_id,
        requeue: false,
    })
    .await
    .unwrap();
    // Let actor process.
    tokio::task::yield_now().await;
    let s = stats(&tx).await;
    assert_eq!(s.messages_ready, 0);
    assert_eq!(s.messages_unacked, 0);

    shutdown(tx, actor).await;
}

/// Overflow + configured DLX with no routes must not hang the actor (Issue 2).
#[tokio::test]
async fn overflow_dlx_fail_does_not_spin() {
    use crate::domain::{Exchange, ExchangeType};
    use crate::queue::dlx::DlxRouter;
    use crate::queue::meta::NoopMetaStore;
    use crate::queue::registry::QueueRegistry;
    use crate::router::ExchangeRouter;
    use std::time::Duration as StdDuration;

    let reg = Arc::new(QueueRegistry::new(
        Arc::new(NoopMetaStore),
        MemoryTracker::shared(),
    ));
    let router = Arc::new(ExchangeRouter::new());
    // DLX exchange exists but has zero bindings → publish Err.
    router.put_exchange(Exchange::new("/", "dlx", ExchangeType::Fanout));
    let dlx = Arc::new(DlxRouter::new(router, Arc::downgrade(&reg)));

    let args = QueueArgs {
        max_length: Some(1),
        overflow: OverflowPolicy::DropHead,
        dead_letter_exchange: Some(CompactString::from("dlx")),
        ..QueueArgs::default()
    };
    let mut boot = QueueActorBootstrap::new_empty(false);
    boot.args = args;
    boot.dlx = Some(dlx);

    let key = QueueKey::new("/", "ov-dlx");
    let memory = MemoryTracker::shared();
    let (tx, rx) = mpsc::channel(16);
    let info = Arc::new(QueueInfo::new(
        key.clone(),
        &crate::queue::QueueDeclareOpts::default(),
    ));
    let actor = tokio::spawn(run(key, rx, Arc::clone(&memory), None, info, boot));

    // Fill queue.
    let _ = enqueue(&tx, sample_msg(b"a")).await;
    // Second publish forces drop-head + failing DLX; must complete promptly.
    let done = tokio::time::timeout(StdDuration::from_secs(2), async {
        enqueue(&tx, sample_msg(b"b")).await
    })
    .await
    .expect("enqueue must not hang on DLX failure");
    let _ = done;
    // Head dropped (or pending free); at most 1 ready.
    let s = stats(&tx).await;
    assert!(s.messages_ready <= 1);

    // Actor still services stats after overflow path.
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    assert_eq!(stats(&tx).await.messages_ready, 1);

    shutdown(tx, actor).await;
}

/// TTL expiry with failing DLX must not livelock (Issue 3).
#[tokio::test(start_paused = true)]
async fn expire_dlx_fail_makes_progress() {
    use crate::domain::{Exchange, ExchangeType};
    use crate::queue::dlx::DlxRouter;
    use crate::queue::meta::NoopMetaStore;
    use crate::queue::registry::QueueRegistry;
    use crate::router::ExchangeRouter;

    let reg = Arc::new(QueueRegistry::new(
        Arc::new(NoopMetaStore),
        MemoryTracker::shared(),
    ));
    let router = Arc::new(ExchangeRouter::new());
    router.put_exchange(Exchange::new("/", "dlx", ExchangeType::Fanout));
    let dlx = Arc::new(DlxRouter::new(router, Arc::downgrade(&reg)));

    let args = QueueArgs {
        message_ttl_ms: Some(50),
        dead_letter_exchange: Some(CompactString::from("dlx")),
        ..QueueArgs::default()
    };
    let mut boot = QueueActorBootstrap::new_empty(false);
    boot.args = args;
    boot.dlx = Some(dlx);

    let key = QueueKey::new("/", "ttl-dlx");
    let memory = MemoryTracker::shared();
    let (tx, rx) = mpsc::channel(16);
    let info = Arc::new(QueueInfo::new(
        key.clone(),
        &crate::queue::QueueDeclareOpts::default(),
    ));
    let actor = tokio::spawn(run(key, rx, Arc::clone(&memory), None, info, boot));

    let _ = enqueue(&tx, sample_msg(b"die")).await;
    assert_eq!(stats(&tx).await.messages_ready, 1);

    tokio::time::advance(Duration::from_millis(100)).await;
    for _ in 0..20 {
        tokio::task::yield_now().await;
        if stats(&tx).await.messages_ready == 0 {
            break;
        }
        tokio::time::advance(Duration::from_millis(10)).await;
    }
    assert_eq!(
        stats(&tx).await.messages_ready,
        0,
        "expired message must leave ready even when DLX fails"
    );

    shutdown(tx, actor).await;
}

/// A↔B mutual DLX under overflow must not deadlock actors (Issue 1).
#[tokio::test]
async fn mutual_dlx_overflow_no_deadlock() {
    use crate::domain::{Binding, Exchange, ExchangeType};
    use crate::queue::dlx::DlxRouter;
    use crate::queue::meta::NoopMetaStore;
    use crate::queue::registry::{QueueDeclareOpts, QueueRegistry};
    use crate::router::ExchangeRouter;
    use std::time::Duration as StdDuration;

    let reg = Arc::new(
        QueueRegistry::new(Arc::new(NoopMetaStore), MemoryTracker::shared())
            .with_mailbox_capacity(16),
    );
    let router = Arc::new(ExchangeRouter::new());
    router.put_exchange(Exchange::new("/", "to-a", ExchangeType::Fanout));
    router.put_exchange(Exchange::new("/", "to-b", ExchangeType::Fanout));
    reg.set_dlx(Arc::new(DlxRouter::new(
        Arc::clone(&router),
        Arc::downgrade(&reg),
    )));

    let a = reg
        .declare(
            "/",
            "qa",
            QueueDeclareOpts {
                args: QueueArgs {
                    max_length: Some(1),
                    overflow: OverflowPolicy::DropHead,
                    dead_letter_exchange: Some(CompactString::from("to-b")),
                    ..QueueArgs::default()
                },
                ..QueueDeclareOpts::default()
            },
        )
        .await
        .unwrap();
    let b = reg
        .declare(
            "/",
            "qb",
            QueueDeclareOpts {
                args: QueueArgs {
                    max_length: Some(1),
                    overflow: OverflowPolicy::DropHead,
                    dead_letter_exchange: Some(CompactString::from("to-a")),
                    ..QueueArgs::default()
                },
                ..QueueDeclareOpts::default()
            },
        )
        .await
        .unwrap();

    router.bind(Binding::new("/", "to-a", "qa", "")).unwrap();
    router.bind(Binding::new("/", "to-b", "qb", "")).unwrap();

    // Fill both queues.
    for (h, body) in [(&a.handle, b"a1"), (&b.handle, b"b1")] {
        let (rtx, rrx) = oneshot::channel();
        h.tx.send(QueueCmd::Enqueue {
            msg: sample_msg(body),
            reply: rtx,
        })
        .await
        .unwrap();
        let _ = rrx.await.unwrap().unwrap();
    }

    // Concurrent overflow enqueues that force A↔B DLX handoff.
    let ha = a.handle.clone();
    let hb = b.handle.clone();
    let t1 = tokio::spawn(async move {
        for i in 0..5u8 {
            let (rtx, rrx) = oneshot::channel();
            ha.tx
                .send(QueueCmd::Enqueue {
                    msg: sample_msg(&[b'A', i]),
                    reply: rtx,
                })
                .await
                .unwrap();
            let _ = rrx.await;
        }
    });
    let t2 = tokio::spawn(async move {
        for i in 0..5u8 {
            let (rtx, rrx) = oneshot::channel();
            hb.tx
                .send(QueueCmd::Enqueue {
                    msg: sample_msg(&[b'B', i]),
                    reply: rtx,
                })
                .await
                .unwrap();
            let _ = rrx.await;
        }
    });

    tokio::time::timeout(StdDuration::from_secs(5), async {
        let _ = t1.await;
        let _ = t2.await;
    })
    .await
    .expect("mutual DLX must not deadlock queue actors");

    // Still responsive.
    let (stx, srx) = oneshot::channel();
    a.handle
        .tx
        .send(QueueCmd::Stats { reply: stx })
        .await
        .unwrap();
    let _ = srx.await.unwrap();
}

struct ConfirmCounter {
    n: std::sync::atomic::AtomicU64,
}

impl metrics::CounterFn for ConfirmCounter {
    fn increment(&self, value: u64) {
        self.n
            .fetch_add(value, std::sync::atomic::Ordering::Relaxed);
    }
    fn absolute(&self, value: u64) {
        self.n.store(value, std::sync::atomic::Ordering::Relaxed);
    }
}

struct ConfirmRecorder {
    counters: std::sync::Mutex<std::collections::HashMap<String, std::sync::Arc<ConfirmCounter>>>,
}

impl ConfirmRecorder {
    fn get(&self, name: &str) -> u64 {
        self.counters
            .lock()
            .unwrap()
            .get(name)
            .map(|c| c.n.load(std::sync::atomic::Ordering::Relaxed))
            .unwrap_or(0)
    }
}

impl metrics::Recorder for ConfirmRecorder {
    fn describe_counter(
        &self,
        _key: metrics::KeyName,
        _unit: Option<metrics::Unit>,
        _description: metrics::SharedString,
    ) {
    }
    fn describe_gauge(
        &self,
        _key: metrics::KeyName,
        _unit: Option<metrics::Unit>,
        _description: metrics::SharedString,
    ) {
    }
    fn describe_histogram(
        &self,
        _key: metrics::KeyName,
        _unit: Option<metrics::Unit>,
        _description: metrics::SharedString,
    ) {
    }
    fn register_counter(
        &self,
        key: &metrics::Key,
        _metadata: &metrics::Metadata<'_>,
    ) -> metrics::Counter {
        let mut map = self.counters.lock().unwrap();
        let counter = map
            .entry(key.name().to_string())
            .or_insert_with(|| {
                std::sync::Arc::new(ConfirmCounter {
                    n: std::sync::atomic::AtomicU64::new(0),
                })
            })
            .clone();
        metrics::Counter::from_arc(counter)
    }
    fn register_gauge(
        &self,
        _key: &metrics::Key,
        _metadata: &metrics::Metadata<'_>,
    ) -> metrics::Gauge {
        metrics::Gauge::noop()
    }
    fn register_histogram(
        &self,
        _key: &metrics::Key,
        _metadata: &metrics::Metadata<'_>,
    ) -> metrics::Histogram {
        metrics::Histogram::noop()
    }
}

fn confirm_recorder() -> &'static ConfirmRecorder {
    static ONCE: std::sync::OnceLock<&'static ConfirmRecorder> = std::sync::OnceLock::new();
    ONCE.get_or_init(|| {
        let rec: &'static ConfirmRecorder = Box::leak(Box::new(ConfirmRecorder {
            counters: std::sync::Mutex::new(std::collections::HashMap::new()),
        }));
        metrics::set_global_recorder(rec).expect("install confirm recorder");
        rec
    })
}

fn confirm_before_total() -> u64 {
    confirm_recorder().get("queueforge_confirm_before_fsync_total")
}

/// The counter is process-global. Tests that read it hold this lock so a
/// parallel test cannot move it between their two reads.
static CONFIRM_COUNTER: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// `never` completes the confirm after the buffered append. That is a confirm
/// before fsync, so the counter moves. A non-persistent publish does not.
#[tokio::test(flavor = "current_thread")]
async fn never_policy_counts_a_confirm_that_skips_fsync() {
    let _counter = CONFIRM_COUNTER.lock().await;
    let before = confirm_before_total();
    let key = QueueKey::new("/", "never-count");
    let memory = MemoryTracker::shared();
    let info = Arc::new(QueueInfo::new(
        key.clone(),
        &crate::queue::QueueDeclareOpts {
            durable: true,
            ..crate::queue::QueueDeclareOpts::default()
        },
    ));
    let mut boot = QueueActorBootstrap::new_empty(true).with_log(
        Box::new(CountLog {
            syncs: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            appends: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }),
        DurabilityPolicy {
            policy: FsyncPolicy::Never,
            interval: Duration::from_millis(400),
            every_n_messages: 1,
        },
    );
    boot.durable = true;
    let (tx, rx) = mpsc::channel(8);
    let actor = tokio::spawn(run(key, rx, memory, None, info, boot));

    let mut msg = (*sample_msg(b"buffered")).clone();
    msg.persistent = true;
    let done = enqueue(&tx, Arc::new(msg)).await;
    done.durable_done.await.unwrap().unwrap();
    assert_eq!(confirm_before_total() - before, 1);

    let transient = enqueue(&tx, sample_msg(b"temp")).await;
    transient.durable_done.await.unwrap().unwrap();
    assert_eq!(confirm_before_total() - before, 1);
    shutdown(tx, actor).await;
}

/// A confirm that waited for the covering fsync does not move the counter.
#[tokio::test(flavor = "current_thread")]
async fn covered_every_n_ms_confirm_does_not_count_before_fsync() {
    let _counter = CONFIRM_COUNTER.lock().await;
    let before = confirm_before_total();
    let syncs = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let key = QueueKey::new("/", "covered-count");
    let memory = MemoryTracker::shared();
    let info = Arc::new(QueueInfo::new(
        key.clone(),
        &crate::queue::QueueDeclareOpts {
            durable: true,
            ..crate::queue::QueueDeclareOpts::default()
        },
    ));
    let mut boot = QueueActorBootstrap::new_empty(true).with_log(
        Box::new(CountLog {
            syncs: Arc::clone(&syncs),
            appends: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }),
        DurabilityPolicy {
            policy: FsyncPolicy::EveryNMs,
            interval: Duration::from_millis(400),
            every_n_messages: 1,
        },
    );
    boot.durable = true;
    let (tx, rx) = mpsc::channel(8);
    let actor = tokio::spawn(run(key, rx, memory, None, info, boot));

    let mut msg = (*sample_msg(b"synced")).clone();
    msg.persistent = true;
    let done = enqueue(&tx, Arc::new(msg)).await;
    done.durable_done.await.unwrap().unwrap();
    assert_eq!(syncs.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(confirm_before_total(), before);
    shutdown(tx, actor).await;
}
