//! AMQP 0-9-1 connection state machine.
//!
//! Handshake:
//! ```text
//! protocol header → connection.start → start-ok (PLAIN)
//!   → tune → tune-ok → open → open-ok
//! ```
//!
//! After open: channel open/open-ok, heartbeats, connection close.
//!
//! Layout: [`reply`] (AMQP codes), [`helpers`] (pure helpers), this module (state machine).

tokio::task_local! {
    /// Common name of the verified client certificate on this connection's
    /// TLS session, set by the listener around the connection task.
    pub static PEER_CN: Option<String>;
}

/// The verified client certificate's common name, outside a TLS session `None`.
pub(crate) fn peer_cn() -> Option<String> {
    PEER_CN.try_with(Clone::clone).ok().flatten()
}

mod bind;
mod close;
mod confirm;
mod consume;
mod content;
mod deliver;
mod exchange;
mod get;
mod handshake;
pub(crate) mod headers;
mod helpers;
mod open;
mod publish;
mod purge;
mod queue;
mod reply;
mod returns;
mod settle;
mod topology;
mod tune;

#[cfg(test)]
mod tests;

use helpers::*;
use reply::*;

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use queueforge_amqp::channel as chan_method;
use queueforge_amqp::confirm as confirm_method;
use queueforge_amqp::connection as conn_method;
use queueforge_amqp::exchange as exchange_method;
use queueforge_amqp::queue as queue_method;
use queueforge_amqp::{
    basic as basic_method, decode_protocol_header, BasicProperties, Frame, FrameType, Method,
    FRAME_MIN_LEN, PROTOCOL_HEADER_LEN,
};
use queueforge_auth::AuthService;
use queueforge_core::{
    ConsumerDeliveryId, ConsumerSessionId, ExchangeRouter, QueueCmd, QueueDelivery, QueueHandle,
    QueueKey, QueueRegistry,
};
use queueforge_mgmt::ConnectionTracker;
use queueforge_store::MetadataStore;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::Instant;
use tracing::{debug, info, trace, warn};

use crate::shutdown::ConnectionGuard;

/// Server default for `channel_max` (design Protocol Behavior Appendix).
pub const DEFAULT_CHANNEL_MAX: u16 = 2047;
/// Server default for `frame_max` (128 KiB).
pub const DEFAULT_FRAME_MAX: u32 = 131_072;
/// Server default heartbeat interval in seconds.
pub const DEFAULT_HEARTBEAT: u16 = 60;
/// Minimum allowed negotiated `frame_max`.
pub const FRAME_MAX_FLOOR: u32 = 4096;
/// Default max content body size (16 MiB).
pub const DEFAULT_MAX_MESSAGE_BYTES: u64 = 16 * 1024 * 1024;

/// Queue-global consumer session ids. A per-connection counter is not enough:
/// the queue actor keys consumers by this id alone.
static NEXT_CONSUMER_SESSION: AtomicU64 = AtomicU64::new(1);
/// High 32 bits stamped onto session ids so two nodes never reuse the same id.
static SESSION_NAMESPACE: AtomicU64 = AtomicU64::new(0);

/// Give this process a non-zero slot so its session ids cannot collide with a peer.
///
/// `slot` is stored in the high 32 bits of session ids minted after this call.
/// A slot of `0` leaves later ids as the raw counter.
/// Returns nothing. Call it once before the first consumer session; a later call does not rewrite ids already issued.
pub fn install_session_namespace(slot: u64) {
    SESSION_NAMESPACE.store(slot, Ordering::Relaxed);
}

fn next_consumer_session() -> u64 {
    let n = NEXT_CONSUMER_SESSION.fetch_add(1, Ordering::Relaxed);
    let slot = SESSION_NAMESPACE.load(Ordering::Relaxed);
    if slot == 0 {
        n
    } else {
        (slot << 32) | (n & 0xffff_ffff)
    }
}

/// Tunable connection parameters offered by the server.
#[derive(Debug, Clone, Copy)]
pub struct ConnectionParams {
    /// Max channels offered (`channel_max`).
    pub channel_max: u16,
    /// Max frame size offered (`frame_max`).
    pub frame_max: u32,
    /// Heartbeat interval offered in seconds.
    pub heartbeat: u16,
    /// Maximum content body size (content-header `body-size`).
    pub max_message_bytes: u64,
    /// Prefetch applied when the client sends `prefetch_count = 0`.
    pub default_prefetch: u16,
    /// Queue type when a durable declare omits `x-queue-type`.
    pub default_queue_type: queueforge_core::QueueType,
}

impl Default for ConnectionParams {
    fn default() -> Self {
        Self {
            channel_max: DEFAULT_CHANNEL_MAX,
            frame_max: DEFAULT_FRAME_MAX,
            heartbeat: DEFAULT_HEARTBEAT,
            max_message_bytes: DEFAULT_MAX_MESSAGE_BYTES,
            default_prefetch: 256,
            default_queue_type: queueforge_core::QueueType::Classic,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    /// Waiting for the 8-byte AMQP protocol header.
    ExpectHeader,
    /// Sent `connection.start`; waiting for `start-ok`.
    StartSent,
    /// Sent `connection.tune`; waiting for `tune-ok`.
    TuneSent,
    /// Waiting for `connection.open`.
    OpenWait,
    /// Connection fully open; channels + heartbeats active.
    Open,
    /// Closing / closed.
    Closed,
}

/// Run the connection state machine for one accepted stream.
///
/// `stream` is typically a plain [`tokio::net::TcpStream`] or a
/// `tokio_rustls::server::TlsStream` when TLS is enabled.
///
/// `guard` must be obtained via [`crate::shutdown::ConnectionTracker::track`]
/// **before** this task is spawned so graceful drain cannot race past untracked
/// handlers. `shutdown` is the process-wide drain watch (broadcast
/// `connection.close` on SIGTERM).
#[allow(clippy::too_many_arguments)] // stream + store/queues/router + drain guard/watch
pub async fn handle_connection<S>(
    mut stream: S,
    peer: std::net::SocketAddr,
    store: Arc<MetadataStore>,
    queues: Arc<QueueRegistry>,
    router: Arc<ExchangeRouter>,
    connections: Arc<ConnectionTracker>,
    params: ConnectionParams,
    _guard: ConnectionGuard,
    shutdown: watch::Receiver<bool>,
    cluster: Option<Arc<crate::cluster::Cluster>>,
) where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let delivery_bound = usize::from(params.default_prefetch.max(1));
    let (delivery_tx, delivery_rx) = mpsc::channel(delivery_bound);
    let (confirm_tx, confirm_rx) = mpsc::unbounded_channel();
    let (handoff_tx, handoff_rx) = mpsc::unbounded_channel();
    let durable_tx = spawn_durable_confirms(confirm_tx.clone());
    let alarm_rx = connections.alarm_rx();
    let (reply_tx, reply_rx) = mpsc::unbounded_channel();
    let mut conn = Connection {
        stream: &mut stream,
        peer,
        store,
        queues,
        router,
        connections,
        params,
        state: State::ExpectHeader,
        read_buf: Vec::with_capacity(4096),
        // Pre-tune: allow the server default frame size as the receive budget.
        frame_max: params.frame_max.max(FRAME_MAX_FLOOR),
        channel_max: params.channel_max,
        heartbeat: params.heartbeat,
        last_recv: Instant::now(),
        last_send: Instant::now(),
        channels: HashMap::new(),
        user: None,
        vhost: None,
        // True after connection.open-ok increments queueforge_connections.
        gauges_held: false,
        // Management connection tracker id (set after open-ok).
        conn_track_id: None,
        force_close_rx: None,
        alarm_rx,
        wants_blocked: false,
        has_published: false,
        reply_tx,
        reply_rx,
        reply_addrs: HashMap::new(),
        granted_credit: HashMap::new(),
        replay: VecDeque::new(),
        delivery_tx,
        delivery_rx,
        handoff_tx,
        handoff_rx,
        next_delivery_seq: 0,
        next_delivery_write: 0,
        finished_handoffs: BTreeMap::new(),
        confirm_tx,
        confirm_rx,
        durable_tx,
        publish_counters: HashMap::new(),
        exchange_write_ok: HashMap::new(),
        tx_applying: false,
        sessions: HashMap::new(),
        declared_queues: Vec::new(),
        cleaned_up: false,
        cluster,
        outbound: Vec::with_capacity(4096),
        coalesce_depth: 0,
    };

    let run_result = conn.run(shutdown).await;
    // Issue 2: every exit path must cancel consumers / requeue unacked /
    // delete exclusive queues — including IO errors via `?` that skip local cleanup.
    conn.cleanup_on_close().await;
    conn.mark_closed();
    if let Err(err) = run_result {
        debug!(%peer, error = %err, "connection ended");
    }
    // `_guard` drops here, decrementing the connection tracker.
}

/// Per-channel AMQP delivery ledger entry.
#[allow(dead_code)] // consumer_tag retained for multi-dest / cancel diagnostics
struct OutstandingDelivery {
    queue_key: QueueKey,
    consumer_delivery_id: ConsumerDeliveryId,
    consumer_tag: Option<String>,
    session: Option<ConsumerSessionId>,
}

/// In-flight content assembly after basic.publish.
enum PublishAssemble {
    ExpectHeader {
        publish: basic_method::Publish,
    },
    ExpectBody {
        publish: basic_method::Publish,
        properties: Box<BasicProperties>,
        body_size: u64,
        body: Vec<u8>,
    },
}

struct ChannelState {
    /// `basic.qos` with `global=false`. `0` means unlimited per consumer.
    prefetch_count: u16,
    /// `basic.qos` with `global=true`, shared by every consumer on this channel.
    /// `0` means no channel-wide cap.
    global_prefetch: u16,
    next_delivery_tag: u64,
    delivery_ledger: BTreeMap<u64, OutstandingDelivery>,
    consumers: HashMap<String, ConsumerSessionId>,
    publish: Option<PublishAssemble>,
    /// Publisher confirms enabled via `confirm.select`.
    confirm_mode: bool,
    /// Next publish sequence number for confirms (starts at 1).
    next_publish_seq: u64,
    /// `channel.flow`. `false` holds further content on this channel.
    flow_active: bool,
    /// Frames received while `flow_active` is false (replayed when flow resumes).
    held: Vec<Frame>,
    /// `tx.select` is in effect. Publishes and ack/reject are buffered.
    tx_mode: bool,
    /// Unpublished operations waiting for `tx.commit` or `tx.rollback`.
    tx_ops: Vec<TxOp>,
    /// Confirm results waiting until every lower tag on this channel is ready.
    held_confirms: BTreeMap<u64, bool>,
    /// Next publisher-confirm tag to write. Tags below this were already sent.
    next_confirm_emit: u64,
}

enum TxOp {
    Publish {
        publish: basic_method::Publish,
        properties: Box<BasicProperties>,
        body: Bytes,
    },
    Ack(basic_method::Ack),
    Reject(basic_method::Reject),
}

impl ChannelState {
    fn new() -> Self {
        Self {
            prefetch_count: 0,
            global_prefetch: 0,
            next_delivery_tag: 1,
            delivery_ledger: BTreeMap::new(),
            consumers: HashMap::new(),
            publish: None,
            confirm_mode: false,
            next_publish_seq: 1,
            flow_active: true,
            held: Vec::new(),
            tx_mode: false,
            tx_ops: Vec::new(),
            held_confirms: BTreeMap::new(),
            next_confirm_emit: 1,
        }
    }

    /// Allocate the next publisher-confirm sequence number when confirm mode is on.
    fn take_publish_seq(&mut self) -> Option<u64> {
        if !self.confirm_mode {
            return None;
        }
        let seq = self.next_publish_seq;
        self.next_publish_seq = self.next_publish_seq.saturating_add(1);
        Some(seq)
    }

    fn outstanding(&self) -> u32 {
        self.delivery_ledger.len() as u32
    }
}

#[derive(Clone)]
struct SessionInfo {
    channel: u16,
    consumer_tag: String,
    queue_key: QueueKey,
    handle: QueueHandle,
    no_ack: bool,
}

struct Connection<'a, S> {
    stream: &'a mut S,
    peer: std::net::SocketAddr,
    store: Arc<MetadataStore>,
    queues: Arc<QueueRegistry>,
    router: Arc<ExchangeRouter>,
    connections: Arc<ConnectionTracker>,
    params: ConnectionParams,
    state: State,
    read_buf: Vec<u8>,
    frame_max: u32,
    channel_max: u16,
    heartbeat: u16,
    last_recv: Instant,
    last_send: Instant,
    channels: HashMap<u16, ChannelState>,
    user: Option<String>,
    vhost: Option<String>,
    /// Whether `queueforge_connections` has been incremented for this connection.
    gauges_held: bool,
    /// Id in [`ConnectionTracker`] after successful open (for management list).
    conn_track_id: Option<String>,
    /// Force-close signal from management API (set after open-ok).
    force_close_rx: Option<watch::Receiver<bool>>,
    /// Broker-wide memory or disk alarm. `Some(reason)` holds publishes.
    alarm_rx: watch::Receiver<Option<String>>,
    /// The client advertised the `connection.blocked` capability.
    wants_blocked: bool,
    /// This connection has published. RabbitMQ stops reading only from
    /// publishers while an alarm lasts, so consumers can drain.
    has_published: bool,
    /// Replies addressed to this connection's direct reply-to consumers.
    reply_tx: mpsc::UnboundedSender<(u16, String, Arc<queueforge_core::Message>)>,
    reply_rx: mpsc::UnboundedReceiver<(u16, String, Arc<queueforge_core::Message>)>,
    /// Direct reply-to address per channel.
    reply_addrs: HashMap<u16, (String, String)>,
    /// Unused credit already handed to each consumer. A channel-global cap counts
    /// this together with unacked deliveries on that channel.
    granted_credit: HashMap<ConsumerSessionId, u32>,
    /// Frames released by `channel.flow active=true`, processed before the socket.
    replay: VecDeque<Frame>,
    delivery_tx: mpsc::Sender<QueueDelivery>,
    cluster: Option<Arc<crate::cluster::Cluster>>,
    delivery_rx: mpsc::Receiver<QueueDelivery>,
    /// Completed quorum claims, in the order `stage_delivery` reserved them.
    /// The read loop writes `basic.deliver` only for the next sequence, after
    /// that claim. Publisher confirms are not waiting on this channel.
    handoff_tx: mpsc::UnboundedSender<FinishedHandoff>,
    handoff_rx: mpsc::UnboundedReceiver<FinishedHandoff>,
    next_delivery_seq: u64,
    next_delivery_write: u64,
    finished_handoffs: BTreeMap<u64, FinishedHandoff>,
    /// Confirms whose covering fsync finished. The read loop writes the ack.
    confirm_tx: mpsc::UnboundedSender<DeferredConfirm>,
    confirm_rx: mpsc::UnboundedReceiver<DeferredConfirm>,
    /// One task waits for classic fsyncs. A task per publish was the pipeline tax.
    durable_tx: mpsc::UnboundedSender<DurableWait>,
    /// `queueforge_publish_total` handles, keyed by exchange. The first publish
    /// to an exchange builds the handle; the rest clone it.
    publish_counters: HashMap<String, metrics::Counter>,
    /// Exchange write permission already resolved for this connection.
    /// A publish to the same exchange does not read metadata again.
    exchange_write_ok: HashMap<String, bool>,
    /// True while applying a `tx.commit` batch so those ops are not re-buffered.
    tx_applying: bool,
    sessions: HashMap<ConsumerSessionId, SessionInfo>,
    /// Queues declared on this connection (for exclusive/auto-delete cleanup).
    declared_queues: Vec<QueueKey>,
    /// True after [`Self::cleanup_on_close`] has run (idempotent).
    cleaned_up: bool,
    /// Staged AMQP bytes. Written when [`Self::coalesce_depth`] returns to 0,
    /// or sooner once the buffer reaches [`Self::COALESCE_LIMIT`].
    outbound: Vec<u8>,
    /// Nesting count for a coalesced write. Zero sends each frame immediately
    /// so a heartbeat or connection.close reaches the socket before the next read.
    coalesce_depth: u32,
}

/// One quorum delivery whose follower drop has finished, or a delivery that
/// did not need a drop. `seq` is the order reserved on the connection.
struct FinishedHandoff {
    seq: u64,
    delivery: QueueDelivery,
    channel: u16,
    /// Tag reserved before the claim. `None` is a server cancel.
    delivery_tag: Option<u64>,
}

/// Publisher confirm whose fsync (or quorum append) finished off the read loop.
struct DeferredConfirm {
    channel: u16,
    delivery_tag: u64,
    ok: bool,
}

/// One classic publish waiting for its enqueue reply and the covering fsync.
struct DurableWait {
    channel: u16,
    seq: u64,
    waits:
        Vec<oneshot::Receiver<Result<queueforge_core::EnqueueCompletion, queueforge_core::Error>>>,
    send_failures: u32,
    counter: metrics::Counter,
}

/// Wait for classic confirms on one task per connection.
///
/// The read loop only enqueues the wait. Spawning a task per publish woke the
/// runtime once per message after every group commit.
fn spawn_durable_confirms(
    confirm_tx: mpsc::UnboundedSender<DeferredConfirm>,
) -> mpsc::UnboundedSender<DurableWait> {
    let (tx, mut rx) = mpsc::unbounded_channel::<DurableWait>();
    tokio::spawn(async move {
        while let Some(wait) = rx.recv().await {
            let mut ok = wait.send_failures == 0 && !wait.waits.is_empty();
            let mut completions = Vec::with_capacity(wait.waits.len());
            for reply_rx in wait.waits {
                match reply_rx.await {
                    Ok(Ok(completion)) => completions.push(completion),
                    Ok(Err(error)) => {
                        warn!(error = %error, "enqueue rejected");
                        ok = false;
                    }
                    Err(_) => {
                        warn!("enqueue reply canceled");
                        ok = false;
                    }
                }
            }
            if ok {
                for completion in completions {
                    match completion.durable_done.await {
                        Ok(Ok(())) => {}
                        Err(_) => {
                            warn!("durable_done oneshot canceled (queue actor gone?)");
                            ok = false;
                        }
                        Ok(Err(error)) => {
                            warn!(error = %error, "durable_done error on enqueue");
                            ok = false;
                        }
                    }
                }
            }
            if ok {
                wait.counter.increment(1);
            } else {
                metrics::counter!("queueforge_publish_partial_failure_total").increment(1);
            }
            let _ = confirm_tx.send(DeferredConfirm {
                channel: wait.channel,
                delivery_tag: wait.seq,
                ok,
            });
        }
    });
    tx
}

/// Internal control-flow for the connection loop.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Step {
    Continue,
    Done,
}

#[derive(Debug, thiserror::Error)]
enum ConnError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("protocol: {0}")]
    Protocol(String),
    #[error("amqp codec: {0}")]
    Amqp(#[from] queueforge_amqp::Error),
}

impl<'a, S> Connection<'a, S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// Run an authz predicate on the blocking pool. redb must not run on this task.
    async fn auth_bool<F>(&self, f: F) -> Result<bool, queueforge_auth::AuthError>
    where
        F: FnOnce(&AuthService<'_>) -> Result<bool, queueforge_auth::AuthError> + Send + 'static,
    {
        let store = Arc::clone(&self.store);
        match MetadataStore::blocking(store, move |s| Ok(f(&AuthService::new(s)))).await {
            Ok(inner) => inner,
            Err(e) => Err(e.into()),
        }
    }

    async fn run(&mut self, mut shutdown: watch::Receiver<bool>) -> Result<(), ConnError> {
        // If drain already started before we entered the loop, close immediately.
        if *shutdown.borrow() {
            self.server_shutdown_close().await;
            return Ok(());
        }

        loop {
            if self.state == State::Closed {
                return Ok(());
            }

            // Process any complete frames already buffered.
            match self.drain_buffer().await? {
                Step::Done => return Ok(()),
                Step::Continue => {}
            }

            // A biased select that always prefers a readable socket will never
            // write deliveries while a publisher floods this connection.
            // One coalesce covers deliveries and confirms from this read.
            self.begin_coalesce();
            let mut failed: Option<ConnError> = None;
            let mut confirm_failed = false;
            if let Err(e) = self.flush_deliveries().await {
                failed = Some(e);
            } else if let Err(e) = self.flush_staged_deliveries().await {
                failed = Some(e);
            } else if let Err(e) = self.flush_confirms().await {
                confirm_failed = true;
                failed = Some(e);
            }
            if let Err(e) = self.end_coalesce().await {
                if failed.is_none() {
                    failed = Some(e);
                }
            }
            if let Some(e) = failed {
                debug!(
                    peer = %self.peer,
                    error = %e,
                    "{}",
                    if confirm_failed {
                        "confirm forward failed"
                    } else {
                        "delivery forward failed"
                    }
                );
                self.cleanup_on_close().await;
                self.mark_closed();
                return Ok(());
            }

            // Heartbeat timers (only meaningful once negotiated; 0 disables).
            let now = Instant::now();
            let (recv_deadline, send_deadline) = self.heartbeat_deadlines(now);

            let mut tmp = [0u8; 16 * 1024];
            let open = self.state == State::Open;
            let held = self.has_published && self.alarm_rx.borrow().is_some();
            tokio::select! {
                biased;

                // A direct reply-to reply for one of this connection's channels.
                Some((channel, consumer_tag, msg)) = self.reply_rx.recv() => {
                    if self.channels.contains_key(&channel) {
                        let tag = match self.channels.get_mut(&channel) {
                            Some(ch) => {
                                let tag = ch.next_delivery_tag;
                                ch.next_delivery_tag = ch.next_delivery_tag.saturating_add(1);
                                tag
                            }
                            None => 0,
                        };
                        let props = headers::message_to_properties(&msg);
                        self.send_method(
                            channel,
                            &Method::BasicDeliver(basic_method::Deliver {
                                consumer_tag,
                                delivery_tag: tag,
                                redelivered: false,
                                exchange: String::new(),
                                routing_key: msg.routing_key.to_string(),
                            }),
                        )
                        .await?;
                        self.send_content(channel, &props, &msg.body).await?;
                    }
                    continue;
                }

                // Memory or disk alarm raised or cleared.
                changed = self.alarm_rx.changed() => {
                    if changed.is_ok() && open && self.wants_blocked {
                        let reason = self.alarm_rx.borrow_and_update().clone();
                        let method = match reason {
                            Some(reason) => Method::ConnectionBlocked(conn_method::Blocked { reason }),
                            None => Method::ConnectionUnblocked(conn_method::Unblocked),
                        };
                        self.send_method(0, &method).await?;
                    } else {
                        self.alarm_rx.borrow_and_update();
                    }
                    continue;
                }

                // Process-wide graceful drain (SIGTERM): connection.close then exit.
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        self.server_shutdown_close().await;
                        return Ok(());
                    }
                }

                // Management force-close (DELETE /api/connections/{id}).
                changed = async {
                    match self.force_close_rx.as_mut() {
                        Some(rx) => rx.changed().await,
                        None => std::future::pending().await,
                    }
                } => {
                    if changed.is_err()
                        || self
                            .force_close_rx
                            .as_ref()
                            .map(|rx| *rx.borrow())
                            .unwrap_or(false)
                    {
                        info!(
                            peer = %self.peer,
                            "force-close requested by management; sending connection.close"
                        );
                        if self.state != State::ExpectHeader {
                            let _ = self
                                .send_connection_close(
                                    REPLY_CONNECTION_FORCED,
                                    "CONNECTION_FORCED - closed by administrator",
                                    0,
                                    0,
                                )
                                .await;
                        }
                        self.cleanup_on_close().await;
                        self.mark_closed();
                        return Ok(());
                    }
                }

                result = self.stream.read(&mut tmp), if !held => {
                    let n = result?;
                    if n == 0 {
                        debug!(peer = %self.peer, "peer closed TCP");
                        // Issue 3: release gauges before leaving Open (EOF path).
                        self.cleanup_on_close().await;
                        self.mark_closed();
                        return Ok(());
                    }
                    // Issue 4: do not update last_recv on partial TCP reads —
                    // only complete frames (in drain_buffer) reset the timer.
                    self.read_buf.extend_from_slice(&tmp[..n]);
                }

                Some(delivery) = self.delivery_rx.recv(), if open => {
                    // The wake delivers one message. Drain the rest in the same write.
                    self.begin_coalesce();
                    let forwarded = self.forward_delivery(delivery).await;
                    let rest = if forwarded.is_ok() {
                        self.flush_deliveries().await
                    } else {
                        forwarded
                    };
                    let ended = self.end_coalesce().await;
                    if let Err(e) = rest.and(ended) {
                        debug!(peer = %self.peer, error = %e, "delivery forward failed");
                        self.cleanup_on_close().await;
                        self.mark_closed();
                        return Ok(());
                    }
                }

                Some(done) = self.handoff_rx.recv(), if open => {
                    self.finished_handoffs.insert(done.seq, done);
                    if let Err(e) = self.flush_staged_deliveries().await {
                        debug!(peer = %self.peer, error = %e, "delivery forward failed");
                        self.cleanup_on_close().await;
                        self.mark_closed();
                        return Ok(());
                    }
                }

                Some(confirm) = self.confirm_rx.recv(), if open => {
                    self.stage_confirm(confirm.channel, confirm.delivery_tag, confirm.ok);
                    if let Err(e) = self.flush_confirms().await {
                        debug!(peer = %self.peer, error = %e, "confirm forward failed");
                        self.cleanup_on_close().await;
                        self.mark_closed();
                        return Ok(());
                    }
                }

                _ = sleep_until_opt(send_deadline), if send_deadline.is_some() => {
                    // No frames sent for `heartbeat` seconds → send heartbeat.
                    if self.state == State::Open || self.state == State::OpenWait
                        || self.state == State::TuneSent || self.state == State::StartSent
                    {
                        self.send_frame(&Frame::heartbeat()).await?;
                        debug!(peer = %self.peer, "sent heartbeat");
                    }
                }

                _ = sleep_until_opt(recv_deadline), if recv_deadline.is_some() => {
                    // No frames received for 2×heartbeat → close.
                    warn!(peer = %self.peer, "heartbeat timeout");
                    metrics::counter!("queueforge_connection_heartbeat_timeouts_total")
                        .increment(1);
                    let _ = self
                        .send_connection_close(
                            REPLY_CONNECTION_FORCED,
                            "CONNECTION_FORCED - heartbeat timeout",
                            0,
                            0,
                        )
                        .await;
                    self.cleanup_on_close().await;
                    self.mark_closed();
                    return Ok(());
                }
            }
        }
    }

    /// Server-initiated close for process drain: `connection.close` then cleanup.
    async fn server_shutdown_close(&mut self) {
        if self.state == State::Closed {
            return;
        }
        info!(
            peer = %self.peer,
            state = ?self.state,
            "broker shutting down; sending connection.close"
        );
        if self.state != State::ExpectHeader {
            let _ = self
                .send_connection_close(
                    REPLY_CONNECTION_FORCED,
                    "CONNECTION_FORCED - broker shutting down",
                    0,
                    0,
                )
                .await;
            // Best-effort brief wait for connection.close-ok / peer TCP close.
            // Overall drain deadline is enforced by ConnectionTracker::wait_drained.
            let mut tmp = [0u8; 4096];
            let _ = tokio::time::timeout(Duration::from_millis(500), async {
                loop {
                    match self.stream.read(&mut tmp).await {
                        Ok(0) => break,
                        Ok(n) => {
                            self.read_buf.extend_from_slice(&tmp[..n]);
                            // Drain any complete frames (may include close-ok).
                            if self.drain_buffer().await.is_err() {
                                break;
                            }
                            if self.state == State::Closed {
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
            })
            .await;
        }
        self.cleanup_on_close().await;
        self.mark_closed();
    }

    fn heartbeat_deadlines(&self, now: Instant) -> (Option<Instant>, Option<Instant>) {
        if self.heartbeat == 0 || self.state == State::ExpectHeader {
            return (None, None);
        }
        // Start enforcing after start-ok so auth lag does not false-timeout.
        if matches!(self.state, State::StartSent | State::ExpectHeader) {
            return (None, None);
        }
        let hb = Duration::from_secs(u64::from(self.heartbeat));
        let recv = self.last_recv + hb * 2;
        let send = self.last_send + hb;
        (Some(recv.max(now)), Some(send.max(now)))
    }

    async fn flush_deliveries(&mut self) -> Result<(), ConnError> {
        if self.state != State::Open {
            return Ok(());
        }
        self.begin_coalesce();
        let mut result = Ok(());
        loop {
            match self.delivery_rx.try_recv() {
                Ok(delivery) => {
                    if let Err(e) = self.forward_delivery(delivery).await {
                        result = Err(e);
                        break;
                    }
                }
                Err(_) => break,
            }
        }
        self.end_coalesce().await?;
        result
    }

    /// Write publisher confirms whose fsync already finished.
    async fn flush_confirms(&mut self) -> Result<(), ConnError> {
        if self.state != State::Open {
            while self.confirm_rx.try_recv().is_ok() {}
            return Ok(());
        }
        self.begin_coalesce();
        loop {
            match self.confirm_rx.try_recv() {
                Ok(confirm) => {
                    self.stage_confirm(confirm.channel, confirm.delivery_tag, confirm.ok);
                }
                Err(_) => break,
            }
        }
        let result = self.emit_ready_confirms().await;
        self.end_coalesce().await?;
        result
    }

    async fn drain_buffer(&mut self) -> Result<Step, ConnError> {
        // One socket read holds many publishes. The per-frame flush below
        // pulls deliveries and confirms into the burst, and this outer
        // coalesce writes that burst once when the buffered frames are done.
        self.begin_coalesce();
        let step = self.drain_buffered_frames().await;
        let ended = self.end_coalesce().await;
        match step {
            Ok(step) => ended.map(|()| step),
            Err(e) => Err(e),
        }
    }

    async fn drain_buffered_frames(&mut self) -> Result<Step, ConnError> {
        loop {
            if self.state == State::Closed {
                return Ok(Step::Done);
            }

            if self.state == State::ExpectHeader {
                if self.read_buf.len() < PROTOCOL_HEADER_LEN {
                    return Ok(Step::Continue);
                }
                match decode_protocol_header(&self.read_buf) {
                    Ok(n) => {
                        self.read_buf.drain(..n);
                        self.last_recv = Instant::now();
                        self.send_connection_start().await?;
                        self.state = State::StartSent;
                        continue;
                    }
                    Err(queueforge_amqp::Error::Incomplete { .. }) => {
                        return Ok(Step::Continue);
                    }
                    Err(_) => {
                        // Wrong header: close TCP without a method frame.
                        warn!(peer = %self.peer, "invalid protocol header");
                        self.mark_closed();
                        return Ok(Step::Done);
                    }
                }
            }

            if let Some(frame) = self.replay.pop_front() {
                match self.handle_frame(frame).await? {
                    Step::Done => return Ok(Step::Done),
                    Step::Continue => continue,
                }
            }

            // Decode one frame.
            let max_payload = self.max_payload();
            match Frame::decode_with_limit(&self.read_buf, max_payload) {
                Ok((frame, n)) => {
                    self.read_buf.drain(..n);
                    self.last_recv = Instant::now();
                    match self.handle_frame(frame).await? {
                        Step::Done => return Ok(Step::Done),
                        Step::Continue => {
                            // Pull deliveries and confirms before the next
                            // publish frame so one connection is not stuck
                            // behind its own flood. The outer coalesce holds
                            // the TCP write until this buffer is drained.
                            // Staged quorum bodies are written only after their
                            // claim; a claim still in flight does not hold this.
                            self.flush_deliveries().await?;
                            self.flush_staged_deliveries().await?;
                            self.flush_confirms().await?;
                            continue;
                        }
                    }
                }
                Err(queueforge_amqp::Error::Incomplete { .. }) => {
                    return Ok(Step::Continue);
                }
                Err(queueforge_amqp::Error::FrameTooLarge { size, max }) => {
                    warn!(
                        peer = %self.peer,
                        size,
                        max,
                        "frame too large; closing"
                    );
                    let _ = self
                        .send_connection_close(REPLY_COMMAND_INVALID, "frame too large", 0, 0)
                        .await;
                    self.mark_closed();
                    return Ok(Step::Done);
                }
                Err(e) => {
                    warn!(peer = %self.peer, error = %e, "frame decode error");
                    let _ = self
                        .send_connection_close(REPLY_COMMAND_INVALID, "frame error", 0, 0)
                        .await;
                    self.mark_closed();
                    return Ok(Step::Done);
                }
            }
        }
    }

    fn max_payload(&self) -> usize {
        // AMQP `frame_max` is the maximum total frame size (header+payload+end).
        let total = self.frame_max.max(FRAME_MAX_FLOOR) as usize;
        total.saturating_sub(FRAME_MIN_LEN)
    }

    /// Hold publishes and other channel traffic while `channel.flow` is inactive.
    ///
    /// Heartbeats are channel 0. `channel.flow`, `channel.close`, and
    /// `connection.close` still run so a paused client can resume or disconnect.
    fn should_hold_frame(&self, frame: &Frame) -> bool {
        let Some(ch) = self.channels.get(&frame.channel) else {
            return false;
        };
        if ch.flow_active || frame.channel == 0 {
            return false;
        }
        if frame.kind != FrameType::Method || frame.payload.len() < 4 {
            return true;
        }
        let class = u16::from_be_bytes([frame.payload[0], frame.payload[1]]);
        let method = u16::from_be_bytes([frame.payload[2], frame.payload[3]]);
        let channel_control = class == chan_method::CLASS_ID
            && (method == chan_method::Flow::METHOD_ID
                || method == chan_method::Close::METHOD_ID
                || method == chan_method::CloseOk::METHOD_ID);
        let connection_close = class == conn_method::CLASS_ID
            && (method == conn_method::Close::METHOD_ID
                || method == conn_method::CloseOk::METHOD_ID);
        !(channel_control || connection_close)
    }

    /// Credit for a new or updated consumer. `None` means unlimited.
    ///
    /// `global=false` prefetch is per consumer. `global=true` prefetch is shared
    /// by every consumer on that channel. `prefetch_count=0` is unlimited.
    /// `replacing` is the session whose previous grant is being replaced.
    fn credit_for(
        &self,
        channel: u16,
        _no_ack: bool,
        replacing: Option<ConsumerSessionId>,
    ) -> Option<u32> {
        let Some(ch) = self.channels.get(&channel) else {
            return Some(0);
        };
        let per_consumer = if ch.prefetch_count == 0 {
            None
        } else {
            let outstanding = self.consumer_outstanding(channel, replacing);
            Some(u32::from(ch.prefetch_count).saturating_sub(outstanding))
        };
        match (per_consumer, self.channel_global_slots(channel, replacing)) {
            (None, None) => None,
            (Some(n), None) => Some(n),
            (None, Some(g)) => Some(g),
            (Some(n), Some(g)) => Some(n.min(g)),
        }
    }

    /// Unacked deliveries for one consumer. A new session has none.
    fn consumer_outstanding(&self, channel: u16, session: Option<ConsumerSessionId>) -> u32 {
        let Some(session) = session else {
            return 0;
        };
        let Some(ch) = self.channels.get(&channel) else {
            return 0;
        };
        ch.delivery_ledger
            .values()
            .filter(|e| e.session == Some(session))
            .count() as u32
    }

    /// Slots left under this channel's `global=true` prefetch.
    ///
    /// `None` when that prefetch is 0 (unlimited). Orphan grants whose session
    /// never registered still occupy a slot until they are removed.
    fn channel_global_slots(
        &self,
        channel: u16,
        replacing: Option<ConsumerSessionId>,
    ) -> Option<u32> {
        let ch = self.channels.get(&channel)?;
        if ch.global_prefetch == 0 {
            return None;
        }
        let cap = u32::from(ch.global_prefetch);
        let reserved: u32 = self
            .granted_credit
            .iter()
            .filter(|(id, _)| {
                if Some(**id) == replacing {
                    return false;
                }
                match self.sessions.get(*id) {
                    Some(info) => info.channel == channel,
                    None => true,
                }
            })
            .map(|(_, n)| *n)
            .sum();
        Some(cap.saturating_sub(ch.outstanding().saturating_add(reserved)))
    }

    fn remember_grant(&mut self, session: ConsumerSessionId, credit: Option<u32>) {
        match credit {
            Some(n) => {
                self.granted_credit.insert(session, n);
            }
            None => {
                self.granted_credit.remove(&session);
            }
        }
    }

    fn spend_grant(&mut self, session: ConsumerSessionId) {
        if let Some(n) = self.granted_credit.get_mut(&session) {
            *n = n.saturating_sub(1);
        }
    }

    async fn handle_frame(&mut self, frame: Frame) -> Result<Step, ConnError> {
        if self.should_hold_frame(&frame) {
            if let Some(ch) = self.channels.get_mut(&frame.channel) {
                ch.held.push(frame);
            }
            return Ok(Step::Continue);
        }
        if frame.kind == FrameType::Heartbeat {
            // Respond to peer heartbeats so clients that expect an echo stay alive.
            if self.heartbeat > 0 {
                self.send_frame(&Frame::heartbeat()).await?;
            }
            return Ok(Step::Continue);
        }

        if frame.kind != FrameType::Method {
            if self.state != State::Open {
                let _ = self
                    .send_connection_close(
                        REPLY_COMMAND_INVALID,
                        "unexpected content frame during handshake",
                        0,
                        0,
                    )
                    .await;
                self.mark_closed();
                return Ok(Step::Done);
            }
            return self.handle_content_frame(frame).await;
        }

        let method = match Method::from_frame(&frame) {
            Ok(m) => m,
            Err(e) => {
                warn!(peer = %self.peer, error = %e, "unknown/invalid method");
                let _ = self
                    .send_connection_close(REPLY_COMMAND_INVALID, "unknown method", 0, 0)
                    .await;
                self.mark_closed();
                return Ok(Step::Done);
            }
        };

        match self.state {
            State::StartSent => self.on_start_sent(frame.channel, method).await,
            State::TuneSent => self.on_tune_sent(frame.channel, method).await,
            State::OpenWait => self.on_open_wait(frame.channel, method).await,
            State::Open => self.on_open(frame.channel, method).await,
            State::ExpectHeader | State::Closed => Ok(Step::Continue),
        }
    }

    async fn on_open(&mut self, channel: u16, method: Method) -> Result<Step, ConnError> {
        // Connection-class methods must be on channel 0.
        if method.class_id() == conn_method::CLASS_ID {
            if channel != 0 {
                let _ = self
                    .send_connection_close(
                        REPLY_COMMAND_INVALID,
                        "connection methods must use channel 0",
                        method.class_id(),
                        method.method_id(),
                    )
                    .await;
                self.mark_closed();
                return Ok(Step::Done);
            }
            return self.on_open_connection_method(method).await;
        }

        // Channel-scoped methods require a non-zero channel.
        if channel == 0 {
            let _ = self
                .send_connection_close(
                    REPLY_COMMAND_INVALID,
                    "channel methods must use non-zero channel",
                    method.class_id(),
                    method.method_id(),
                )
                .await;
            self.mark_closed();
            return Ok(Step::Done);
        }

        if self.channel_max != 0 && channel > self.channel_max {
            let _ = self
                .send_connection_close(
                    REPLY_COMMAND_INVALID,
                    &format!("channel {channel} exceeds channel_max {}", self.channel_max),
                    method.class_id(),
                    method.method_id(),
                )
                .await;
            self.mark_closed();
            return Ok(Step::Done);
        }

        match method {
            Method::ChannelOpen(_open) => {
                if self.channels.contains_key(&channel) {
                    // Issue 2: server-initiated close must drop the channel from the set
                    // so the client can reopen the same id after close-ok.
                    self.server_channel_close(
                        channel,
                        REPLY_COMMAND_INVALID,
                        "channel already open",
                        chan_method::CLASS_ID,
                        chan_method::Open::METHOD_ID,
                    )
                    .await?;
                    return Ok(Step::Continue);
                }
                let user = self.user.clone().unwrap_or_default();
                if !self.connections.channel_allowed(&user) {
                    self.server_channel_close(
                        channel,
                        REPLY_ACCESS_REFUSED,
                        "ACCESS_REFUSED - channel limit reached",
                        chan_method::CLASS_ID,
                        chan_method::Open::METHOD_ID,
                    )
                    .await?;
                    return Ok(Step::Continue);
                }
                self.channels.insert(channel, ChannelState::new());
                metrics::gauge!("queueforge_channels").increment(1.0);
                queueforge_core::prom::channel_opened();
                self.sync_tracked_channels();
                self.send_method(channel, &Method::ChannelOpenOk(chan_method::OpenOk::new()))
                    .await?;
                debug!(peer = %self.peer, channel, "channel open");
                Ok(Step::Continue)
            }
            Method::ChannelClose(close) => {
                debug!(
                    peer = %self.peer,
                    channel,
                    code = close.reply_code,
                    text = %close.reply_text,
                    "channel close"
                );
                self.teardown_channel(channel, /*requeue=*/ true).await;
                self.send_method(channel, &Method::ChannelCloseOk(chan_method::CloseOk))
                    .await?;
                Ok(Step::Continue)
            }
            Method::ChannelCloseOk(_) => {
                // Unsolicited close-ok: ignore.
                Ok(Step::Continue)
            }
            Method::ChannelFlow(flow) => {
                if !self.channels.contains_key(&channel) {
                    self.server_channel_close(
                        channel,
                        REPLY_COMMAND_INVALID,
                        "channel not open",
                        chan_method::CLASS_ID,
                        chan_method::Flow::METHOD_ID,
                    )
                    .await?;
                    return Ok(Step::Continue);
                }
                if let Some(ch) = self.channels.get_mut(&channel) {
                    ch.flow_active = flow.active;
                }
                self.send_method(
                    channel,
                    &Method::ChannelFlowOk(chan_method::FlowOk {
                        active: flow.active,
                    }),
                )
                .await?;
                if flow.active {
                    let held = self
                        .channels
                        .get_mut(&channel)
                        .map(|c| std::mem::take(&mut c.held))
                        .unwrap_or_default();
                    for frame in held {
                        self.replay.push_back(frame);
                    }
                }
                Ok(Step::Continue)
            }
            Method::ExchangeDeclare(declare) => {
                if !self
                    .require_open_channel(
                        channel,
                        exchange_method::CLASS_ID,
                        exchange_method::Declare::METHOD_ID,
                    )
                    .await?
                {
                    return Ok(Step::Continue);
                }
                self.handle_exchange_declare(channel, declare).await
            }
            Method::ExchangeDelete(delete) => {
                if !self
                    .require_open_channel(
                        channel,
                        exchange_method::CLASS_ID,
                        exchange_method::Delete::METHOD_ID,
                    )
                    .await?
                {
                    return Ok(Step::Continue);
                }
                self.handle_exchange_delete(channel, delete).await
            }
            Method::ExchangeBind(bind) => {
                if !self
                    .require_open_channel(
                        channel,
                        exchange_method::CLASS_ID,
                        exchange_method::Bind::METHOD_ID,
                    )
                    .await?
                {
                    return Ok(Step::Continue);
                }
                self.handle_exchange_bind(channel, bind).await
            }
            Method::ExchangeUnbind(unbind) => {
                if !self
                    .require_open_channel(
                        channel,
                        exchange_method::CLASS_ID,
                        exchange_method::Unbind::METHOD_ID,
                    )
                    .await?
                {
                    return Ok(Step::Continue);
                }
                self.handle_exchange_unbind(channel, unbind).await
            }
            Method::QueueDeclare(declare) => {
                if !self
                    .require_open_channel(
                        channel,
                        queue_method::CLASS_ID,
                        queue_method::Declare::METHOD_ID,
                    )
                    .await?
                {
                    return Ok(Step::Continue);
                }
                self.handle_queue_declare(channel, declare).await
            }
            Method::QueueBind(bind) => {
                if !self
                    .require_open_channel(
                        channel,
                        queue_method::CLASS_ID,
                        queue_method::Bind::METHOD_ID,
                    )
                    .await?
                {
                    return Ok(Step::Continue);
                }
                if self.refuse_locked(channel, &bind.queue, queue_method::CLASS_ID, queue_method::Bind::METHOD_ID).await? {
                    return Ok(Step::Continue);
                }
                self.handle_queue_bind(channel, bind).await
            }
            Method::QueueUnbind(unbind) => {
                if !self
                    .require_open_channel(
                        channel,
                        queue_method::CLASS_ID,
                        queue_method::Unbind::METHOD_ID,
                    )
                    .await?
                {
                    return Ok(Step::Continue);
                }
                if self.refuse_locked(channel, &unbind.queue, queue_method::CLASS_ID, queue_method::Unbind::METHOD_ID).await? {
                    return Ok(Step::Continue);
                }
                self.handle_queue_unbind(channel, unbind).await
            }
            Method::QueuePurge(purge) => {
                if !self
                    .require_open_channel(
                        channel,
                        queue_method::CLASS_ID,
                        queue_method::Purge::METHOD_ID,
                    )
                    .await?
                {
                    return Ok(Step::Continue);
                }
                if self
                    .refuse_locked(channel, &purge.queue, queue_method::CLASS_ID, queue_method::Purge::METHOD_ID)
                    .await?
                {
                    return Ok(Step::Continue);
                }
                self.handle_queue_purge(channel, purge).await
            }
            Method::QueueDelete(delete) => {
                if !self
                    .require_open_channel(
                        channel,
                        queue_method::CLASS_ID,
                        queue_method::Delete::METHOD_ID,
                    )
                    .await?
                {
                    return Ok(Step::Continue);
                }
                if self.refuse_locked(channel, &delete.queue, queue_method::CLASS_ID, queue_method::Delete::METHOD_ID).await? {
                    return Ok(Step::Continue);
                }
                self.handle_queue_delete(channel, delete).await
            }
            Method::BasicQos(qos) => {
                if !self
                    .require_open_channel(
                        channel,
                        basic_method::CLASS_ID,
                        basic_method::Qos::METHOD_ID,
                    )
                    .await?
                {
                    return Ok(Step::Continue);
                }
                self.handle_basic_qos(channel, qos).await
            }
            Method::BasicConsume(consume) => {
                if !self
                    .require_open_channel(
                        channel,
                        basic_method::CLASS_ID,
                        basic_method::Consume::METHOD_ID,
                    )
                    .await?
                {
                    return Ok(Step::Continue);
                }
                if self.refuse_locked(channel, &consume.queue, basic_method::CLASS_ID, basic_method::Consume::METHOD_ID).await? {
                    return Ok(Step::Continue);
                }
                self.handle_basic_consume(channel, consume).await
            }
            Method::BasicCancel(cancel) => {
                if !self
                    .require_open_channel(
                        channel,
                        basic_method::CLASS_ID,
                        basic_method::Cancel::METHOD_ID,
                    )
                    .await?
                {
                    return Ok(Step::Continue);
                }
                self.handle_basic_cancel(channel, cancel).await
            }
            Method::BasicPublish(publish) => {
                if !self
                    .require_open_channel(
                        channel,
                        basic_method::CLASS_ID,
                        basic_method::Publish::METHOD_ID,
                    )
                    .await?
                {
                    return Ok(Step::Continue);
                }
                self.handle_basic_publish(channel, publish).await
            }
            Method::BasicGet(get) => {
                if !self
                    .require_open_channel(
                        channel,
                        basic_method::CLASS_ID,
                        basic_method::Get::METHOD_ID,
                    )
                    .await?
                {
                    return Ok(Step::Continue);
                }
                if self.refuse_locked(channel, &get.queue, basic_method::CLASS_ID, basic_method::Get::METHOD_ID).await? {
                    return Ok(Step::Continue);
                }
                self.handle_basic_get(channel, get).await
            }
            Method::BasicAck(ack) => {
                if !self
                    .require_open_channel(
                        channel,
                        basic_method::CLASS_ID,
                        basic_method::Ack::METHOD_ID,
                    )
                    .await?
                {
                    return Ok(Step::Continue);
                }
                if self.in_tx(channel) {
                    if let Some(ch) = self.channels.get_mut(&channel) {
                        ch.tx_ops.push(TxOp::Ack(ack));
                    }
                    return Ok(Step::Continue);
                }
                self.handle_basic_ack(channel, ack).await
            }
            Method::BasicNack(nack) => {
                if !self
                    .require_open_channel(
                        channel,
                        basic_method::CLASS_ID,
                        basic_method::Nack::METHOD_ID,
                    )
                    .await?
                {
                    return Ok(Step::Continue);
                }
                self.handle_basic_nack(channel, nack.delivery_tag, nack.multiple, nack.requeue)
                    .await
            }
            Method::BasicReject(reject) => {
                if !self
                    .require_open_channel(
                        channel,
                        basic_method::CLASS_ID,
                        basic_method::Reject::METHOD_ID,
                    )
                    .await?
                {
                    return Ok(Step::Continue);
                }
                if self.in_tx(channel) {
                    if let Some(ch) = self.channels.get_mut(&channel) {
                        ch.tx_ops.push(TxOp::Reject(reject));
                    }
                    return Ok(Step::Continue);
                }
                // reject is single-message nack without multiple.
                self.handle_basic_nack(channel, reject.delivery_tag, false, reject.requeue)
                    .await
            }
            Method::BasicRecover(recover) => {
                if !self
                    .require_open_channel(
                        channel,
                        basic_method::CLASS_ID,
                        basic_method::Recover::METHOD_ID,
                    )
                    .await?
                {
                    return Ok(Step::Continue);
                }
                if !recover.requeue {
                    self.server_channel_close(
                        channel,
                        REPLY_NOT_IMPLEMENTED,
                        "NOT_IMPLEMENTED - basic.recover requeue=false",
                        basic_method::CLASS_ID,
                        basic_method::Recover::METHOD_ID,
                    )
                    .await?;
                    return Ok(Step::Continue);
                }
                self.handle_basic_recover(channel).await
            }
            Method::TxSelect(_) => self.tx_select(channel).await,
            Method::TxCommit(_) => self.tx_commit(channel).await,
            Method::TxRollback(_) => self.tx_rollback(channel).await,
            Method::ConfirmSelect(select) => {
                if !self
                    .require_open_channel(
                        channel,
                        confirm_method::CLASS_ID,
                        confirm_method::Select::METHOD_ID,
                    )
                    .await?
                {
                    return Ok(Step::Continue);
                }
                self.handle_confirm_select(channel, select).await
            }
            other => {
                if !self.channels.contains_key(&channel) {
                    self.server_channel_close(
                        channel,
                        REPLY_COMMAND_INVALID,
                        "channel not open",
                        other.class_id(),
                        other.method_id(),
                    )
                    .await?;
                    return Ok(Step::Continue);
                }
                self.server_channel_close(
                    channel,
                    REPLY_NOT_IMPLEMENTED,
                    "NOT_IMPLEMENTED",
                    other.class_id(),
                    other.method_id(),
                )
                .await?;
                Ok(Step::Continue)
            }
        }
    }

    /// One TCP write once a burst reaches this size. 128 small deliveries fit.
    const COALESCE_LIMIT: usize = 64 * 1024;

    fn begin_coalesce(&mut self) {
        self.coalesce_depth = self.coalesce_depth.saturating_add(1);
    }

    async fn end_coalesce(&mut self) -> Result<(), ConnError> {
        debug_assert!(self.coalesce_depth > 0);
        self.coalesce_depth = self.coalesce_depth.saturating_sub(1);
        if self.coalesce_depth == 0 {
            self.flush_coalesced().await?;
        }
        Ok(())
    }

    async fn flush_coalesced(&mut self) -> Result<(), ConnError> {
        if self.outbound.is_empty() {
            return Ok(());
        }
        let bytes = std::mem::take(&mut self.outbound);
        self.stream.write_all(&bytes).await?;
        self.stream.flush().await?;
        self.last_send = Instant::now();
        Ok(())
    }

    async fn send_method(&mut self, channel: u16, method: &Method) -> Result<(), ConnError> {
        let frame = method.to_frame(channel)?;
        self.send_frame(&frame).await
    }

    async fn send_frame(&mut self, frame: &Frame) -> Result<(), ConnError> {
        let bytes = frame.encode()?;
        if self.coalesce_depth == 0 {
            if !self.outbound.is_empty() {
                self.flush_coalesced().await?;
            }
            self.stream.write_all(&bytes).await?;
            self.stream.flush().await?;
            self.last_send = Instant::now();
        } else {
            self.outbound.extend_from_slice(&bytes);
            if self.outbound.len() >= Self::COALESCE_LIMIT {
                self.flush_coalesced().await?;
            }
        }
        trace!(
            peer = %self.peer,
            kind = ?frame.kind,
            channel = frame.channel,
            len = bytes.len(),
            "sent frame"
        );
        Ok(())
    }
}

impl<S> Connection<'_, S> {
    /// Release connection/channel gauges at most once (idempotent).
    ///
    /// Does **not** requeue messages. Callers that may still hold ledger/sessions
    /// must run [`Self::cleanup_on_close`] (or [`Self::best_effort_requeue_sync`])
    /// first. Issue 2.
    fn release_gauges(&mut self) {
        if self.gauges_held {
            metrics::gauge!("queueforge_connections").decrement(1.0);
            queueforge_core::prom::connection_closed();
            self.gauges_held = false;
        }
        if let Some(id) = self.conn_track_id.take() {
            self.connections.unregister(&id);
        }
        // teardown_channel already decrements per open channel when cleaning up.
        // Only count leftovers (error path without async cleanup).
        let n = self.channels.len();
        if n > 0 {
            metrics::gauge!("queueforge_channels").decrement(n as f64);
            for _ in 0..n {
                queueforge_core::prom::channel_closed();
            }
        }
        self.channels.clear();
        self.sessions.clear();
    }

    /// Mark the connection closed and release any held gauges.
    ///
    /// Prefer `cleanup_on_close().await` first when the connection was Open.
    /// Does **not** set `cleaned_up` so a later async `cleanup_on_close` can
    /// still delete exclusive/auto-delete queues (Issue 2).
    fn mark_closed(&mut self) {
        if !self.cleaned_up {
            // Sync fallback requeue when async cleanup was skipped so far.
            self.best_effort_requeue_sync();
        }
        self.release_gauges();
        self.state = State::Closed;
    }

    /// Fire-and-forget requeue/unregister when async cleanup is unavailable.
    fn best_effort_requeue_sync(&mut self) {
        // Nack all ledger entries and unregister sessions via try_send.
        for ch in self.channels.values_mut() {
            for (_tag, entry) in std::mem::take(&mut ch.delivery_ledger) {
                if let Some(handle) = self.queues.get(&entry.queue_key) {
                    let _ = handle.tx.try_send(QueueCmd::Nack {
                        id: entry.consumer_delivery_id,
                        requeue: true,
                    });
                }
            }
        }
        for (session, info) in self.sessions.drain() {
            let (reply_tx, _reply_rx) = oneshot::channel();
            let _ = info.handle.tx.try_send(QueueCmd::UnregisterConsumer {
                session,
                requeue: true,
                reply: reply_tx,
            });
        }
        // Best-effort exclusive cleanup is async-only; handle_connection finally
        // covers the normal path via cleanup_on_close.
    }
}

impl<S> Drop for Connection<'_, S> {
    fn drop(&mut self) {
        // Issue 2: never leave unacked stuck if async cleanup was skipped.
        if !self.cleaned_up {
            self.best_effort_requeue_sync();
        }
        self.release_gauges();
    }
}
