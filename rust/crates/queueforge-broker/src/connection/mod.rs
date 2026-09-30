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

mod helpers;
mod reply;

#[cfg(test)]
mod tests;

use helpers::*;
use reply::*;

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use compact_str::CompactString;
use queueforge_amqp::channel as chan_method;
use queueforge_amqp::tx as tx_method;
use queueforge_amqp::confirm as confirm_method;
use queueforge_amqp::connection as conn_method;
use queueforge_amqp::exchange as exchange_method;
use queueforge_amqp::queue as queue_method;
use queueforge_amqp::{
    basic as basic_method, decode_protocol_header, BasicProperties, ContentHeader, FieldTable,
    FieldValue, Frame, FrameType, Method, FRAME_MIN_LEN, PROTOCOL_HEADER_LEN,
};
use queueforge_auth::{AuthService, PermissionKind, ResourceKind};
use queueforge_core::{
    generate_server_queue_name, Binding, ConsumerDeliveryId, ConsumerSessionId, Error as CoreError,
    Exchange, ExchangeRouter, ExchangeType, Message, QueueCmd, QueueDeclareOpts, QueueDelivery,
    QueueHandle, QueueKey, QueueRegistry, QueueType, DEFAULT_EXCHANGE_NAME,
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
        granted_credit: HashMap::new(),
        replay: VecDeque::new(),
        delivery_tx,
        delivery_rx,
        tx_applying: false,
        sessions: HashMap::new(),
        declared_queues: Vec::new(),
        cleaned_up: false,
        cluster,
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
    /// Unused credit already handed to each consumer. A channel-global cap counts
    /// this together with unacked deliveries on that channel.
    granted_credit: HashMap<ConsumerSessionId, u32>,
    /// Frames released by `channel.flow active=true`, processed before the socket.
    replay: VecDeque<Frame>,
    delivery_tx: mpsc::Sender<QueueDelivery>,
    cluster: Option<Arc<crate::cluster::Cluster>>,
    delivery_rx: mpsc::Receiver<QueueDelivery>,
    /// True while applying a `tx.commit` batch so those ops are not re-buffered.
    tx_applying: bool,
    sessions: HashMap<ConsumerSessionId, SessionInfo>,
    /// Queues declared on this connection (for exclusive/auto-delete cleanup).
    declared_queues: Vec<QueueKey>,
    /// True after [`Self::cleanup_on_close`] has run (idempotent).
    cleaned_up: bool,
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
            if let Err(e) = self.flush_deliveries().await {
                debug!(peer = %self.peer, error = %e, "delivery forward failed");
                self.cleanup_on_close().await;
                self.mark_closed();
                return Ok(());
            }

            // Heartbeat timers (only meaningful once negotiated; 0 disables).
            let now = Instant::now();
            let (recv_deadline, send_deadline) = self.heartbeat_deadlines(now);

            let mut tmp = [0u8; 16 * 1024];
            let open = self.state == State::Open;
            tokio::select! {
                biased;

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

                result = self.stream.read(&mut tmp) => {
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
                    if let Err(e) = self.forward_delivery(delivery).await {
                        debug!(peer = %self.peer, error = %e, "delivery forward failed");
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
        loop {
            match self.delivery_rx.try_recv() {
                Ok(delivery) => self.forward_delivery(delivery).await?,
                Err(_) => return Ok(()),
            }
        }
    }

    async fn drain_buffer(&mut self) -> Result<Step, ConnError> {
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
                            // Deliver before the next publish frame so a
                            // same-connection consumer is not starved.
                            self.flush_deliveries().await?;
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

    async fn on_start_sent(&mut self, channel: u16, method: Method) -> Result<Step, ConnError> {
        if channel != 0 {
            let _ = self
                .send_connection_close(
                    REPLY_COMMAND_INVALID,
                    "handshake methods must use channel 0",
                    0,
                    0,
                )
                .await;
            self.mark_closed();
            return Ok(Step::Done);
        }

        match method {
            Method::ConnectionStartOk(start_ok) => self.handle_start_ok(start_ok).await,
            Method::ConnectionClose(close) => {
                debug!(
                    peer = %self.peer,
                    code = close.reply_code,
                    text = %close.reply_text,
                    "client closed during handshake"
                );
                let _ = self
                    .send_method(0, &Method::ConnectionCloseOk(conn_method::CloseOk))
                    .await;
                self.mark_closed();
                Ok(Step::Done)
            }
            other => {
                let _ = self
                    .send_connection_close(
                        REPLY_COMMAND_INVALID,
                        &format!(
                            "expected connection.start-ok, got class={} method={}",
                            other.class_id(),
                            other.method_id()
                        ),
                        other.class_id(),
                        other.method_id(),
                    )
                    .await;
                self.mark_closed();
                Ok(Step::Done)
            }
        }
    }

    async fn handle_start_ok(&mut self, start_ok: conn_method::StartOk) -> Result<Step, ConnError> {
        if !start_ok.mechanism.eq_ignore_ascii_case("PLAIN") {
            let _ = self
                .send_connection_close(
                    REPLY_ACCESS_REFUSED,
                    &format!("unsupported SASL mechanism {}", start_ok.mechanism),
                    conn_method::CLASS_ID,
                    conn_method::StartOk::METHOD_ID,
                )
                .await;
            self.mark_closed();
            return Ok(Step::Done);
        }

        let (username, password) = match parse_plain_response(&start_ok.response) {
            Ok(pair) => pair,
            Err(msg) => {
                let _ = self
                    .send_connection_close(
                        REPLY_ACCESS_REFUSED,
                        msg,
                        conn_method::CLASS_ID,
                        conn_method::StartOk::METHOD_ID,
                    )
                    .await;
                self.mark_closed();
                return Ok(Step::Done);
            }
        };

        // Argon2 is intentionally slow; run off the async worker.
        let store = Arc::clone(&self.store);
        let user_owned = username.clone();
        let pass_owned = password.clone();
        let auth_result = MetadataStore::blocking(store, move |s| {
            Ok(AuthService::new(s).authenticate(&user_owned, &pass_owned))
        })
        .await
        .map_err(|e| ConnError::Protocol(format!("auth task: {e}")))?;

        match auth_result {
            Ok(Some(user)) => {
                info!(peer = %self.peer, user = %user.name, "authenticated");
                self.user = Some(user.name.to_string());
            }
            Ok(None) => {
                warn!(peer = %self.peer, user = %username, "authentication failed");
                let _ = self
                    .send_connection_close(
                        REPLY_ACCESS_REFUSED,
                        "ACCESS_REFUSED - Login was refused using authentication mechanism PLAIN",
                        conn_method::CLASS_ID,
                        conn_method::StartOk::METHOD_ID,
                    )
                    .await;
                self.mark_closed();
                return Ok(Step::Done);
            }
            Err(e) => {
                warn!(peer = %self.peer, error = %e, "auth error");
                let _ = self
                    .send_connection_close(
                        REPLY_ACCESS_REFUSED,
                        "ACCESS_REFUSED - authentication error",
                        conn_method::CLASS_ID,
                        conn_method::StartOk::METHOD_ID,
                    )
                    .await;
                self.mark_closed();
                return Ok(Step::Done);
            }
        }

        self.send_connection_tune().await?;
        self.state = State::TuneSent;
        Ok(Step::Continue)
    }

    async fn on_tune_sent(&mut self, channel: u16, method: Method) -> Result<Step, ConnError> {
        if channel != 0 {
            let _ = self
                .send_connection_close(
                    REPLY_COMMAND_INVALID,
                    "handshake methods must use channel 0",
                    0,
                    0,
                )
                .await;
            self.mark_closed();
            return Ok(Step::Done);
        }

        match method {
            Method::ConnectionTuneOk(tune_ok) => {
                if let Err(e) = self.apply_tune_ok(&tune_ok) {
                    warn!(peer = %self.peer, error = %e, "tune-ok rejected");
                    let _ = self
                        .send_connection_close(
                            REPLY_COMMAND_INVALID,
                            &e.to_string(),
                            conn_method::CLASS_ID,
                            conn_method::TuneOk::METHOD_ID,
                        )
                        .await;
                    self.mark_closed();
                    return Ok(Step::Done);
                }
                debug!(
                    peer = %self.peer,
                    channel_max = self.channel_max,
                    frame_max = self.frame_max,
                    heartbeat = self.heartbeat,
                    "tuned"
                );
                self.state = State::OpenWait;
                Ok(Step::Continue)
            }
            Method::ConnectionClose(close) => {
                debug!(
                    peer = %self.peer,
                    code = close.reply_code,
                    text = %close.reply_text,
                    "client closed during tune"
                );
                let _ = self
                    .send_method(0, &Method::ConnectionCloseOk(conn_method::CloseOk))
                    .await;
                self.mark_closed();
                Ok(Step::Done)
            }
            other => {
                let _ = self
                    .send_connection_close(
                        REPLY_COMMAND_INVALID,
                        &format!(
                            "expected connection.tune-ok, got class={} method={}",
                            other.class_id(),
                            other.method_id()
                        ),
                        other.class_id(),
                        other.method_id(),
                    )
                    .await;
                self.mark_closed();
                Ok(Step::Done)
            }
        }
    }

    fn apply_tune_ok(&mut self, tune_ok: &conn_method::TuneOk) -> Result<(), ConnError> {
        // Reject client proposals that exceed what we offered (except 0 = default)
        // *before* writing negotiated values (Issue 7).
        if tune_ok.channel_max != 0
            && self.params.channel_max != 0
            && tune_ok.channel_max > self.params.channel_max
        {
            return Err(ConnError::Protocol(format!(
                "client channel_max {} exceeds server offer {}",
                tune_ok.channel_max, self.params.channel_max
            )));
        }
        if tune_ok.frame_max != 0
            && tune_ok.frame_max > self.params.frame_max
            && self.params.frame_max != 0
        {
            return Err(ConnError::Protocol(format!(
                "client frame_max {} exceeds server offer {}",
                tune_ok.frame_max, self.params.frame_max
            )));
        }

        // channel_max: min(client, server); 0 → server default
        self.channel_max = negotiate_channel_max(self.params.channel_max, tune_ok.channel_max);
        // frame_max: min; floor 4096; 0 → server default
        self.frame_max = negotiate_frame_max(self.params.frame_max, tune_ok.frame_max);
        // heartbeat: min; either side 0 disables (Issue 1)
        self.heartbeat = negotiate_heartbeat(self.params.heartbeat, tune_ok.heartbeat);
        Ok(())
    }

    async fn on_open_wait(&mut self, channel: u16, method: Method) -> Result<Step, ConnError> {
        if channel != 0 {
            let _ = self
                .send_connection_close(
                    REPLY_COMMAND_INVALID,
                    "handshake methods must use channel 0",
                    0,
                    0,
                )
                .await;
            self.mark_closed();
            return Ok(Step::Done);
        }

        match method {
            Method::ConnectionOpen(open) => self.handle_connection_open(open).await,
            Method::ConnectionClose(close) => {
                debug!(
                    peer = %self.peer,
                    code = close.reply_code,
                    text = %close.reply_text,
                    "client closed before open"
                );
                let _ = self
                    .send_method(0, &Method::ConnectionCloseOk(conn_method::CloseOk))
                    .await;
                self.mark_closed();
                Ok(Step::Done)
            }
            other => {
                let _ = self
                    .send_connection_close(
                        REPLY_COMMAND_INVALID,
                        &format!(
                            "expected connection.open, got class={} method={}",
                            other.class_id(),
                            other.method_id()
                        ),
                        other.class_id(),
                        other.method_id(),
                    )
                    .await;
                self.mark_closed();
                Ok(Step::Done)
            }
        }
    }

    async fn handle_connection_open(&mut self, open: conn_method::Open) -> Result<Step, ConnError> {
        let vhost = open.virtual_host;
        let user = self
            .user
            .clone()
            .ok_or_else(|| ConnError::Protocol("open without auth".into()))?;

        // Vhost must exist. redb reads run off the worker.
        let store = Arc::clone(&self.store);
        let vhost_lookup = vhost.clone();
        match MetadataStore::blocking(store, move |s| s.get_vhost(&vhost_lookup)).await {
            Ok(Some(_)) => {}
            Ok(None) => {
                let _ = self
                    .send_connection_close(
                        REPLY_NOT_ALLOWED,
                        &format!("NOT_ALLOWED - vhost {vhost} not found"),
                        conn_method::CLASS_ID,
                        conn_method::Open::METHOD_ID,
                    )
                    .await;
                self.mark_closed();
                return Ok(Step::Done);
            }
            Err(e) => {
                warn!(error = %e, "get_vhost failed");
                let _ = self
                    .send_connection_close(
                        REPLY_ACCESS_REFUSED,
                        "ACCESS_REFUSED - internal error",
                        conn_method::CLASS_ID,
                        conn_method::Open::METHOD_ID,
                    )
                    .await;
                self.mark_closed();
                return Ok(Step::Done);
            }
        }

        // User must have a permission row on this vhost.
        let store = Arc::clone(&self.store);
        let user_lookup = user.clone();
        let vhost_perm = vhost.clone();
        match MetadataStore::blocking(store, move |s| s.get_permission(&user_lookup, &vhost_perm))
            .await
        {
            Ok(Some(_)) => {}
            Ok(None) => {
                let _ = self
                    .send_connection_close(
                        REPLY_ACCESS_REFUSED,
                        &format!("ACCESS_REFUSED - user '{user}' lacks access to vhost '{vhost}'"),
                        conn_method::CLASS_ID,
                        conn_method::Open::METHOD_ID,
                    )
                    .await;
                self.mark_closed();
                return Ok(Step::Done);
            }
            Err(e) => {
                warn!(error = %e, "get_permission failed");
                let _ = self
                    .send_connection_close(
                        REPLY_ACCESS_REFUSED,
                        "ACCESS_REFUSED - internal error",
                        conn_method::CLASS_ID,
                        conn_method::Open::METHOD_ID,
                    )
                    .await;
                self.mark_closed();
                return Ok(Step::Done);
            }
        }

        self.vhost = Some(vhost.clone());
        self.send_method(0, &Method::ConnectionOpenOk(conn_method::OpenOk::new()))
            .await?;
        info!(
            peer = %self.peer,
            user = %user,
            vhost = %vhost,
            "connection open"
        );
        metrics::gauge!("queueforge_connections").increment(1.0);
        queueforge_core::prom::connection_opened();
        self.gauges_held = true;
        let (track_id, force_rx) =
            self.connections
                .register(self.peer, user.as_str(), vhost.as_str());
        self.conn_track_id = Some(track_id);
        self.force_close_rx = Some(force_rx);
        self.state = State::Open;
        Ok(Step::Continue)
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
                self.handle_queue_unbind(channel, unbind).await
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

    /// Returns `false` when the channel was closed for not being open.
    async fn require_open_channel(
        &mut self,
        channel: u16,
        class_id: u16,
        method_id: u16,
    ) -> Result<bool, ConnError> {
        if self.channels.contains_key(&channel) {
            return Ok(true);
        }
        self.server_channel_close(
            channel,
            REPLY_COMMAND_INVALID,
            "channel not open",
            class_id,
            method_id,
        )
        .await?;
        Ok(false)
    }

    async fn handle_exchange_declare(
        &mut self,
        channel: u16,
        declare: exchange_method::Declare,
    ) -> Result<Step, ConnError> {
        let unknown: Vec<&str> = declare
            .arguments
            .entries
            .iter()
            .map(|(k, _)| k.as_str())
            .filter(|k| *k != "alternate-exchange")
            .collect();
        if !unknown.is_empty() {
            let keys = unknown;
            self.server_channel_close(
                channel,
                REPLY_PRECONDITION_FAILED,
                &format!(
                    "PRECONDITION_FAILED - unknown exchange argument(s): {}",
                    keys.join(", ")
                ),
                exchange_method::CLASS_ID,
                exchange_method::Declare::METHOD_ID,
            )
            .await?;
            return Ok(Step::Continue);
        }

        if declare.exchange.is_empty() {
            self.server_channel_close(
                channel,
                REPLY_ACCESS_REFUSED,
                "ACCESS_REFUSED - cannot declare the default exchange",
                exchange_method::CLASS_ID,
                exchange_method::Declare::METHOD_ID,
            )
            .await?;
            return Ok(Step::Continue);
        }

        let kind = match declare.kind.as_str() {
            "direct" => ExchangeType::Direct,
            "fanout" => ExchangeType::Fanout,
            "topic" => ExchangeType::Topic,
            "headers" => ExchangeType::Headers,
            other => {
                self.server_channel_close(
                    channel,
                    REPLY_PRECONDITION_FAILED,
                    &format!("PRECONDITION_FAILED - invalid exchange type '{other}'"),
                    exchange_method::CLASS_ID,
                    exchange_method::Declare::METHOD_ID,
                )
                .await?;
                return Ok(Step::Continue);
            }
        };

        // Exchange auto-delete (delete on last unbind) is not implemented yet —
        // refuse the flag so clients do not get a false durable-looking topology.
        if declare.auto_delete && !declare.passive {
            self.server_channel_close(
                channel,
                REPLY_PRECONDITION_FAILED,
                "PRECONDITION_FAILED - exchange auto_delete is not supported",
                exchange_method::CLASS_ID,
                exchange_method::Declare::METHOD_ID,
            )
            .await?;
            return Ok(Step::Continue);
        }

        let vhost = self.vhost.clone().unwrap_or_else(|| "/".into());
        let user = self.user.clone().unwrap_or_default();
        let exchange_name = declare.exchange.clone();
        let user_auth = user.clone();
        let vhost_auth = vhost.clone();
        match self
            .auth_bool(move |auth| {
                auth.check_permission(
                    &user_auth,
                    &vhost_auth,
                    &exchange_name,
                    ResourceKind::Exchange,
                    PermissionKind::Configure,
                )
            })
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                self.server_channel_close(
                    channel,
                    REPLY_ACCESS_REFUSED,
                    "ACCESS_REFUSED - configure access to exchange refused",
                    exchange_method::CLASS_ID,
                    exchange_method::Declare::METHOD_ID,
                )
                .await?;
                return Ok(Step::Continue);
            }
            Err(e) => {
                self.server_channel_close(
                    channel,
                    REPLY_INTERNAL_ERROR,
                    &format!("INTERNAL_ERROR - auth: {e}"),
                    exchange_method::CLASS_ID,
                    exchange_method::Declare::METHOD_ID,
                )
                .await?;
                return Ok(Step::Continue);
            }
        }

        if declare.passive {
            let stored = {
                let store = Arc::clone(&self.store);
                let vhost_lookup = vhost.clone();
                let name = declare.exchange.clone();
                MetadataStore::blocking(store, move |s| s.get_exchange(&vhost_lookup, &name)).await
            };
            match self
                .router
                .get_exchange(&vhost, declare.exchange.as_str())
                .or_else(|| stored.ok().flatten())
            {
                Some(_) => {
                    if !declare.no_wait {
                        self.send_method(
                            channel,
                            &Method::ExchangeDeclareOk(exchange_method::DeclareOk),
                        )
                        .await?;
                    }
                    return Ok(Step::Continue);
                }
                None => {
                    self.server_channel_close(
                        channel,
                        REPLY_NOT_FOUND,
                        &format!(
                            "NOT_FOUND - no exchange '{}' in vhost '{vhost}'",
                            declare.exchange
                        ),
                        exchange_method::CLASS_ID,
                        exchange_method::Declare::METHOD_ID,
                    )
                    .await?;
                    return Ok(Step::Continue);
                }
            }
        }

        let new_ex = Exchange {
            vhost: CompactString::from(vhost.as_str()),
            name: CompactString::from(declare.exchange.as_str()),
            kind,
            durable: declare.durable,
            auto_delete: declare.auto_delete,
            internal: declare.internal,
            alternate: alternate_exchange_arg(&declare.arguments),
        };

        let stored_existing = {
            let store = Arc::clone(&self.store);
            let vhost_lookup = vhost.clone();
            let name = declare.exchange.clone();
            MetadataStore::blocking(store, move |s| s.get_exchange(&vhost_lookup, &name)).await
        };
        if let Some(existing) = self
            .router
            .get_exchange(&vhost, declare.exchange.as_str())
            .or_else(|| stored_existing.ok().flatten())
        {
            // Redeclare must match properties (including builtins).
            if existing.kind != new_ex.kind
                || existing.durable != new_ex.durable
                || existing.auto_delete != new_ex.auto_delete
                || existing.internal != new_ex.internal
            {
                self.server_channel_close(
                    channel,
                    REPLY_PRECONDITION_FAILED,
                    &format!(
                        "PRECONDITION_FAILED - exchange '{}' exists with different properties",
                        declare.exchange
                    ),
                    exchange_method::CLASS_ID,
                    exchange_method::Declare::METHOD_ID,
                )
                .await?;
                return Ok(Step::Continue);
            }
            // Ensure live table has it (e.g. loaded from store).
            self.router.put_exchange(existing);
            if !declare.no_wait {
                self.send_method(
                    channel,
                    &Method::ExchangeDeclareOk(exchange_method::DeclareOk),
                )
                .await?;
            }
            return Ok(Step::Continue);
        }

        // New user exchange.
        if new_ex.is_builtin() {
            // Should not reach: empty name rejected; amq.* should exist from bootstrap.
            self.server_channel_close(
                channel,
                REPLY_ACCESS_REFUSED,
                "ACCESS_REFUSED - cannot create builtin exchange name",
                exchange_method::CLASS_ID,
                exchange_method::Declare::METHOD_ID,
            )
            .await?;
            return Ok(Step::Continue);
        }

        if new_ex.durable {
            let store = Arc::clone(&self.store);
            let to_store = new_ex.clone();
            if let Err(e) =
                MetadataStore::blocking(store, move |s| s.create_exchange(&to_store)).await
            {
                let msg = e.to_string();
                self.server_channel_close(
                    channel,
                    REPLY_INTERNAL_ERROR,
                    &format!("INTERNAL_ERROR - {msg}"),
                    exchange_method::CLASS_ID,
                    exchange_method::Declare::METHOD_ID,
                )
                .await?;
                return Ok(Step::Continue);
            }
        }
        if let Some(cluster) = &self.cluster {
            cluster
                .replicate_json(
                    "exchange",
                    serde_json::to_value(&new_ex).unwrap_or(serde_json::Value::Null),
                )
                .await;
        }
        self.router.put_exchange(new_ex);

        if !declare.no_wait {
            self.send_method(
                channel,
                &Method::ExchangeDeclareOk(exchange_method::DeclareOk),
            )
            .await?;
        }
        Ok(Step::Continue)
    }

    async fn handle_exchange_delete(
        &mut self,
        channel: u16,
        delete: exchange_method::Delete,
    ) -> Result<Step, ConnError> {
        if delete.exchange.is_empty()
            || queueforge_core::BUILTIN_EXCHANGE_NAMES.contains(&delete.exchange.as_str())
        {
            self.server_channel_close(
                channel,
                REPLY_ACCESS_REFUSED,
                "ACCESS_REFUSED - cannot delete builtin exchange",
                exchange_method::CLASS_ID,
                exchange_method::Delete::METHOD_ID,
            )
            .await?;
            return Ok(Step::Continue);
        }

        let vhost = self.vhost.clone().unwrap_or_else(|| "/".into());
        let user = self.user.clone().unwrap_or_default();
        let exchange_name = delete.exchange.clone();
        let user_auth = user.clone();
        let vhost_auth = vhost.clone();
        match self
            .auth_bool(move |auth| {
                auth.check_permission(
                    &user_auth,
                    &vhost_auth,
                    &exchange_name,
                    ResourceKind::Exchange,
                    PermissionKind::Configure,
                )
            })
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                self.server_channel_close(
                    channel,
                    REPLY_ACCESS_REFUSED,
                    "ACCESS_REFUSED - configure access to exchange refused",
                    exchange_method::CLASS_ID,
                    exchange_method::Delete::METHOD_ID,
                )
                .await?;
                return Ok(Step::Continue);
            }
            Err(e) => {
                self.server_channel_close(
                    channel,
                    REPLY_INTERNAL_ERROR,
                    &format!("INTERNAL_ERROR - auth: {e}"),
                    exchange_method::CLASS_ID,
                    exchange_method::Delete::METHOD_ID,
                )
                .await?;
                return Ok(Step::Continue);
            }
        }

        if self
            .router
            .get_exchange(&vhost, delete.exchange.as_str())
            .is_none()
            && self
                .store
                .get_exchange(&vhost, delete.exchange.as_str())
                .ok()
                .flatten()
                .is_none()
        {
            self.server_channel_close(
                channel,
                REPLY_NOT_FOUND,
                &format!(
                    "NOT_FOUND - no exchange '{}' in vhost '{vhost}'",
                    delete.exchange
                ),
                exchange_method::CLASS_ID,
                exchange_method::Delete::METHOD_ID,
            )
            .await?;
            return Ok(Step::Continue);
        }

        if delete.if_unused {
            let n = self
                .router
                .index()
                .count_for_exchange(&vhost, delete.exchange.as_str());
            if n > 0 {
                self.server_channel_close(
                    channel,
                    REPLY_PRECONDITION_FAILED,
                    &format!(
                        "PRECONDITION_FAILED - exchange '{}' in use (bindings={n})",
                        delete.exchange
                    ),
                    exchange_method::CLASS_ID,
                    exchange_method::Delete::METHOD_ID,
                )
                .await?;
                return Ok(Step::Continue);
            }
        }

        // Durable row + cascade bindings in store (off the worker).
        let store = Arc::clone(&self.store);
        let vhost_del = vhost.clone();
        let exchange_del = delete.exchange.to_string();
        let _ = MetadataStore::blocking(store, move |s| {
            let _ = s.delete_exchange(&vhost_del, &exchange_del);
            for b in s
                .list_bindings_for_exchange(&vhost_del, &exchange_del)
                .unwrap_or_default()
            {
                let args_key = queueforge_store::binding_args_key(&b.args);
                let _ = s.delete_binding(
                    b.vhost.as_str(),
                    b.exchange.as_str(),
                    b.queue.as_str(),
                    b.routing_key.as_str(),
                    &args_key,
                );
            }
            Ok(())
        })
        .await;
        let _ = self
            .router
            .delete_exchange(&vhost, delete.exchange.as_str());

        if !delete.no_wait {
            self.send_method(
                channel,
                &Method::ExchangeDeleteOk(exchange_method::DeleteOk),
            )
            .await?;
        }
        Ok(Step::Continue)
    }

    async fn handle_exchange_bind(
        &mut self,
        channel: u16,
        bind: exchange_method::Bind,
    ) -> Result<Step, ConnError> {
        let vhost = self.vhost.clone().unwrap_or_else(|| "/".into());
        if let Err(e) = self.router.bind_exchange(
            &vhost,
            bind.source.as_str(),
            bind.destination.as_str(),
            bind.routing_key.as_str(),
        ) {
            let (code, text) = match e {
                queueforge_core::Error::NotFound(_) => (
                    REPLY_NOT_FOUND,
                    format!(
                        "NOT_FOUND - no exchange '{}' or '{}' in vhost '{vhost}'",
                        bind.source, bind.destination
                    ),
                ),
                other => (
                    REPLY_PRECONDITION_FAILED,
                    format!("PRECONDITION_FAILED - {other}"),
                ),
            };
            self.server_channel_close(
                channel,
                code,
                &text,
                exchange_method::CLASS_ID,
                exchange_method::Bind::METHOD_ID,
            )
            .await?;
            return Ok(Step::Continue);
        }
        if !bind.no_wait {
            self.send_method(channel, &Method::ExchangeBindOk(exchange_method::BindOk))
                .await?;
        }
        Ok(Step::Continue)
    }

    async fn handle_exchange_unbind(
        &mut self,
        channel: u16,
        unbind: exchange_method::Unbind,
    ) -> Result<Step, ConnError> {
        let vhost = self.vhost.clone().unwrap_or_else(|| "/".into());
        let _ = self.router.unbind_exchange(
            &vhost,
            unbind.source.as_str(),
            unbind.destination.as_str(),
            unbind.routing_key.as_str(),
        );
        if !unbind.no_wait {
            self.send_method(
                channel,
                &Method::ExchangeUnbindOk(exchange_method::UnbindOk),
            )
            .await?;
        }
        Ok(Step::Continue)
    }

    async fn handle_queue_bind(
        &mut self,
        channel: u16,
        bind: queue_method::Bind,
    ) -> Result<Step, ConnError> {
        if bind.exchange.is_empty() {
            self.server_channel_close(
                channel,
                REPLY_ACCESS_REFUSED,
                "ACCESS_REFUSED - cannot bind to the default exchange",
                queue_method::CLASS_ID,
                queue_method::Bind::METHOD_ID,
            )
            .await?;
            return Ok(Step::Continue);
        }

        let vhost = self.vhost.clone().unwrap_or_else(|| "/".into());
        let user = self.user.clone().unwrap_or_default();
        let queue_name = bind.queue.clone();
        let exchange_name = bind.exchange.clone();
        let user_auth = user.clone();
        let vhost_auth = vhost.clone();
        match self
            .auth_bool(move |auth| {
                auth.check_queue_bind(&user_auth, &vhost_auth, &queue_name, &exchange_name)
            })
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                self.server_channel_close(
                    channel,
                    REPLY_ACCESS_REFUSED,
                    "ACCESS_REFUSED - bind access refused",
                    queue_method::CLASS_ID,
                    queue_method::Bind::METHOD_ID,
                )
                .await?;
                return Ok(Step::Continue);
            }
            Err(e) => {
                self.server_channel_close(
                    channel,
                    REPLY_INTERNAL_ERROR,
                    &format!("INTERNAL_ERROR - auth: {e}"),
                    queue_method::CLASS_ID,
                    queue_method::Bind::METHOD_ID,
                )
                .await?;
                return Ok(Step::Continue);
            }
        }

        // Exchange must exist in router or store.
        if self
            .router
            .get_exchange(&vhost, bind.exchange.as_str())
            .is_none()
        {
            if let Some(ex) = self
                .store
                .get_exchange(&vhost, bind.exchange.as_str())
                .ok()
                .flatten()
            {
                self.router.put_exchange(ex);
            } else {
                self.server_channel_close(
                    channel,
                    REPLY_NOT_FOUND,
                    &format!(
                        "NOT_FOUND - no exchange '{}' in vhost '{vhost}'",
                        bind.exchange
                    ),
                    queue_method::CLASS_ID,
                    queue_method::Bind::METHOD_ID,
                )
                .await?;
                return Ok(Step::Continue);
            }
        }

        let key = QueueKey::new(vhost.as_str(), bind.queue.as_str());
        let Some(handle) = self.resolve_queue(&key).await else {
            self.server_channel_close(
                channel,
                REPLY_NOT_FOUND,
                &format!("NOT_FOUND - no queue '{}' in vhost '{vhost}'", bind.queue),
                queue_method::CLASS_ID,
                queue_method::Bind::METHOD_ID,
            )
            .await?;
            return Ok(Step::Continue);
        };

        let mut binding = Binding::new(
            vhost.as_str(),
            bind.exchange.as_str(),
            bind.queue.as_str(),
            bind.routing_key.as_str(),
        );
        binding.args = field_table_to_header_args(&bind.arguments);

        match self.router.bind(binding.clone()) {
            Ok(created) => {
                // Persist when exchange and queue are durable (fail-closed).
                let ex_durable = self
                    .router
                    .get_exchange(&vhost, bind.exchange.as_str())
                    .map(|e| e.durable)
                    .unwrap_or(false);
                if created && ex_durable && handle.info.durable {
                    let store = Arc::clone(&self.store);
                    let to_store = binding.clone();
                    if let Err(e) =
                        MetadataStore::blocking(store, move |s| s.put_binding(&to_store)).await
                    {
                        // Roll back live binding so restart cannot lose topology
                        // the client thought was durable.
                        let _ = self.router.unbind(&binding);
                        self.server_channel_close(
                            channel,
                            REPLY_INTERNAL_ERROR,
                            &format!("INTERNAL_ERROR - failed to persist binding: {e}"),
                            queue_method::CLASS_ID,
                            queue_method::Bind::METHOD_ID,
                        )
                        .await?;
                        return Ok(Step::Continue);
                    }
                }
                if let Some(cluster) = &self.cluster {
                    cluster
                        .replicate_json(
                            "binding",
                            serde_json::to_value(&binding).unwrap_or(serde_json::Value::Null),
                        )
                        .await;
                }
                if !bind.no_wait {
                    self.send_method(channel, &Method::QueueBindOk(queue_method::BindOk))
                        .await?;
                }
                Ok(Step::Continue)
            }
            Err(err) => {
                let (code, text) = core_error_to_amqp(&err);
                self.server_channel_close(
                    channel,
                    code,
                    &text,
                    queue_method::CLASS_ID,
                    queue_method::Bind::METHOD_ID,
                )
                .await?;
                Ok(Step::Continue)
            }
        }
    }

    async fn handle_queue_unbind(
        &mut self,
        channel: u16,
        unbind: queue_method::Unbind,
    ) -> Result<Step, ConnError> {
        if unbind.exchange.is_empty() {
            self.server_channel_close(
                channel,
                REPLY_ACCESS_REFUSED,
                "ACCESS_REFUSED - cannot unbind from the default exchange",
                queue_method::CLASS_ID,
                queue_method::Unbind::METHOD_ID,
            )
            .await?;
            return Ok(Step::Continue);
        }

        let vhost = self.vhost.clone().unwrap_or_else(|| "/".into());
        let user = self.user.clone().unwrap_or_default();
        let queue_name = unbind.queue.clone();
        let exchange_name = unbind.exchange.clone();
        let user_auth = user.clone();
        let vhost_auth = vhost.clone();
        match self
            .auth_bool(move |auth| {
                auth.check_queue_unbind(&user_auth, &vhost_auth, &queue_name, &exchange_name)
            })
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                self.server_channel_close(
                    channel,
                    REPLY_ACCESS_REFUSED,
                    "ACCESS_REFUSED - unbind access refused",
                    queue_method::CLASS_ID,
                    queue_method::Unbind::METHOD_ID,
                )
                .await?;
                return Ok(Step::Continue);
            }
            Err(e) => {
                self.server_channel_close(
                    channel,
                    REPLY_INTERNAL_ERROR,
                    &format!("INTERNAL_ERROR - auth: {e}"),
                    queue_method::CLASS_ID,
                    queue_method::Unbind::METHOD_ID,
                )
                .await?;
                return Ok(Step::Continue);
            }
        }

        let mut binding = Binding::new(
            vhost.as_str(),
            unbind.exchange.as_str(),
            unbind.queue.as_str(),
            unbind.routing_key.as_str(),
        );
        binding.args = field_table_to_header_args(&unbind.arguments);
        let _ = self.router.unbind(&binding);
        let store = Arc::clone(&self.store);
        let vhost_u = binding.vhost.to_string();
        let exchange_u = binding.exchange.to_string();
        let queue_u = binding.queue.to_string();
        let rk_u = binding.routing_key.to_string();
        let args_key = queueforge_store::binding_args_key(&binding.args);
        let _ = MetadataStore::blocking(store, move |s| {
            s.delete_binding(&vhost_u, &exchange_u, &queue_u, &rk_u, &args_key)
        })
        .await;
        if let Some(cluster) = &self.cluster {
            cluster
                .replicate_json(
                    "unbind",
                    serde_json::to_value(&binding).unwrap_or(serde_json::Value::Null),
                )
                .await;
        }

        self.send_method(channel, &Method::QueueUnbindOk(queue_method::UnbindOk))
            .await?;
        Ok(Step::Continue)
    }

    async fn handle_queue_declare(
        &mut self,
        channel: u16,
        declare: queue_method::Declare,
    ) -> Result<Step, ConnError> {
        // Closed declare-arguments set (TTL / DLX / max-length). Unknown keys → 406.
        let queue_args = match parse_queue_declare_args(&declare.arguments) {
            Ok(a) => a,
            Err(msg) => {
                self.server_channel_close(
                    channel,
                    REPLY_PRECONDITION_FAILED,
                    &format!("PRECONDITION_FAILED - {msg}"),
                    queue_method::CLASS_ID,
                    queue_method::Declare::METHOD_ID,
                )
                .await?;
                return Ok(Step::Continue);
            }
        };

        let vhost = self.vhost.clone().unwrap_or_else(|| "/".into());
        let user = self.user.clone().unwrap_or_default();

        // Generate server name first so configure AuthZ uses the final name
        // (Issue 4). Passive empty name is rejected by the registry as NotFound.
        let queue_name = if declare.queue.is_empty() {
            if declare.passive {
                // Fall through to registry for a clear NotFound / protocol error.
                String::new()
            } else {
                generate_server_queue_name()
            }
        } else {
            declare.queue.clone()
        };

        // Configure permission on the final queue name (or empty for passive).
        if !queue_name.is_empty() {
            let user_auth = user.clone();
            let vhost_auth = vhost.clone();
            let name_auth = queue_name.clone();
            match self
                .auth_bool(move |auth| {
                    auth.check_permission(
                        &user_auth,
                        &vhost_auth,
                        &name_auth,
                        ResourceKind::Queue,
                        PermissionKind::Configure,
                    )
                })
                .await
            {
                Ok(true) => {}
                Ok(false) => {
                    self.server_channel_close(
                        channel,
                        REPLY_ACCESS_REFUSED,
                        "ACCESS_REFUSED - configure access to queue refused",
                        queue_method::CLASS_ID,
                        queue_method::Declare::METHOD_ID,
                    )
                    .await?;
                    return Ok(Step::Continue);
                }
                Err(e) => {
                    self.server_channel_close(
                        channel,
                        REPLY_INTERNAL_ERROR,
                        &format!("INTERNAL_ERROR - auth: {e}"),
                        queue_method::CLASS_ID,
                        queue_method::Declare::METHOD_ID,
                    )
                    .await?;
                    return Ok(Step::Continue);
                }
            }
        }

        let declared_args = queue_args.clone();
        let mut queue_args = if declare.passive || queue_name.is_empty() {
            queue_args
        } else {
            self.router
                .queue_args_with_policy(&vhost, &queue_name, &queue_args)
        };
        if !declare.passive {
            if let Err(msg) = finalize_queue_type(
                &mut queue_args,
                self.params.default_queue_type,
                declare.durable,
                declare.exclusive,
            ) {
                self.server_channel_close(
                    channel,
                    REPLY_PRECONDITION_FAILED,
                    &format!("PRECONDITION_FAILED - {msg}"),
                    queue_method::CLASS_ID,
                    queue_method::Declare::METHOD_ID,
                )
                .await?;
                return Ok(Step::Continue);
            }
        }
        if !declare.passive
            && !declare.durable
            && !declare.exclusive
            && queue_args.queue_type != Some(queueforge_core::QueueType::Quorum)
        {
            let _ = self
                .send_connection_close(
                    REPLY_INTERNAL_ERROR,
                    "INTERNAL_ERROR - Feature `transient_nonexcl_queues` is deprecated.\nBy default, this feature is not permitted anymore.",
                    queue_method::CLASS_ID,
                    queue_method::Declare::METHOD_ID,
                )
                .await;
            self.mark_closed();
            return Ok(Step::Done);
        }
        let already = self
            .queues
            .get(&QueueKey::new(vhost.as_str(), queue_name.as_str()))
            .is_some();
        let opts = QueueDeclareOpts {
            declared_args: Some(declared_args),
            durable: declare.durable,
            exclusive: declare.exclusive,
            auto_delete: declare.auto_delete,
            passive: declare.passive,
            // Peer SocketAddr as connection stand-in until connection-manager ids.
            exclusive_owner: if declare.exclusive {
                Some(self.peer.to_string().into())
            } else {
                None
            },
            args: queue_args,
            home: None,
        };

        let declared = if let Some(cluster) = &self.cluster {
            cluster.declare_queue(&vhost, queue_name.as_str(), opts).await
        } else {
            self.queues.declare(&vhost, queue_name.as_str(), opts).await
        };
        match declared {
            Ok(result) => {
                queueforge_core::prom::queue_declared(!declare.passive && !already);
                let key = result.handle.info.key.clone();
                if !self.declared_queues.iter().any(|k| k == &key) {
                    self.declared_queues.push(key);
                }
                if declare.no_wait {
                    return Ok(Step::Continue);
                }
                self.send_method(
                    channel,
                    &Method::QueueDeclareOk(queue_method::DeclareOk {
                        queue: result.handle.info.key.name.to_string(),
                        message_count: result.message_count,
                        consumer_count: result.consumer_count,
                    }),
                )
                .await?;
                Ok(Step::Continue)
            }
            Err(err) => {
                let (code, text) = core_error_to_amqp(&err);
                self.server_channel_close(
                    channel,
                    code,
                    &text,
                    queue_method::CLASS_ID,
                    queue_method::Declare::METHOD_ID,
                )
                .await?;
                Ok(Step::Continue)
            }
        }
    }

    async fn handle_queue_delete(
        &mut self,
        channel: u16,
        delete: queue_method::Delete,
    ) -> Result<Step, ConnError> {
        let vhost = self.vhost.clone().unwrap_or_else(|| "/".into());
        let user = self.user.clone().unwrap_or_default();

        let queue_name = delete.queue.clone();
        let user_auth = user.clone();
        let vhost_auth = vhost.clone();
        match self
            .auth_bool(move |auth| {
                auth.check_permission(
                    &user_auth,
                    &vhost_auth,
                    &queue_name,
                    ResourceKind::Queue,
                    PermissionKind::Configure,
                )
            })
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                self.server_channel_close(
                    channel,
                    REPLY_ACCESS_REFUSED,
                    "ACCESS_REFUSED - configure access to queue refused",
                    queue_method::CLASS_ID,
                    queue_method::Delete::METHOD_ID,
                )
                .await?;
                return Ok(Step::Continue);
            }
            Err(e) => {
                self.server_channel_close(
                    channel,
                    REPLY_INTERNAL_ERROR,
                    &format!("INTERNAL_ERROR - auth: {e}"),
                    queue_method::CLASS_ID,
                    queue_method::Delete::METHOD_ID,
                )
                .await?;
                return Ok(Step::Continue);
            }
        }

        let key = QueueKey::new(vhost.as_str(), delete.queue.as_str());
        match self
            .delete_queue_and_bindings(&key, delete.if_unused, delete.if_empty)
            .await
        {
            Ok(message_count) => {
                queueforge_core::prom::queue_deleted();
                if delete.no_wait {
                    return Ok(Step::Continue);
                }
                self.send_method(
                    channel,
                    &Method::QueueDeleteOk(queue_method::DeleteOk { message_count }),
                )
                .await?;
                Ok(Step::Continue)
            }
            Err(err) => {
                let (code, text) = core_error_to_amqp(&err);
                self.server_channel_close(
                    channel,
                    code,
                    &text,
                    queue_method::CLASS_ID,
                    queue_method::Delete::METHOD_ID,
                )
                .await?;
                Ok(Step::Continue)
            }
        }
    }

    async fn on_open_connection_method(&mut self, method: Method) -> Result<Step, ConnError> {
        match method {
            Method::ConnectionClose(close) => {
                info!(
                    peer = %self.peer,
                    code = close.reply_code,
                    text = %close.reply_text,
                    "client connection.close"
                );
                self.cleanup_on_close().await;
                let _ = self
                    .send_method(0, &Method::ConnectionCloseOk(conn_method::CloseOk))
                    .await;
                self.mark_closed();
                Ok(Step::Done)
            }
            Method::ConnectionCloseOk(_) => {
                self.mark_closed();
                Ok(Step::Done)
            }
            other => {
                let _ = self
                    .send_connection_close(
                        REPLY_COMMAND_INVALID,
                        &format!(
                            "unexpected connection method class={} method={}",
                            other.class_id(),
                            other.method_id()
                        ),
                        other.class_id(),
                        other.method_id(),
                    )
                    .await;
                self.mark_closed();
                Ok(Step::Done)
            }
        }
    }

    /// Push current open-channel count into the management connection tracker.
    fn sync_tracked_channels(&self) {
        if let Some(id) = self.conn_track_id.as_deref() {
            self.connections
                .set_channels(id, self.channels.len() as u32);
        }
    }

    async fn handle_content_frame(&mut self, frame: Frame) -> Result<Step, ConnError> {
        let ch = frame.channel;
        if ch == 0 || !self.channels.contains_key(&ch) {
            if ch != 0 {
                self.server_channel_close(ch, REPLY_COMMAND_INVALID, "channel not open", 0, 0)
                    .await?;
            }
            return Ok(Step::Continue);
        }

        match frame.kind {
            FrameType::Header => {
                let header = match ContentHeader::decode(&frame.payload) {
                    Ok(h) => h,
                    Err(e) => {
                        self.server_channel_close(
                            ch,
                            REPLY_COMMAND_INVALID,
                            &format!("invalid content header: {e}"),
                            60,
                            0,
                        )
                        .await?;
                        return Ok(Step::Continue);
                    }
                };
                let max_msg = self.params.max_message_bytes;
                let header_bytes = properties_header_bytes(&header.properties);
                if header.body_size.saturating_add(header_bytes) > max_msg {
                    self.server_channel_close(
                        ch,
                        REPLY_PRECONDITION_FAILED,
                        &format!(
                            "PRECONDITION_FAILED - message size {} exceeds max {}",
                            header.body_size.saturating_add(header_bytes),
                            max_msg
                        ),
                        60,
                        0,
                    )
                    .await?;
                    // Drop in-flight publish assembly if any.
                    if let Some(ch_state) = self.channels.get_mut(&ch) {
                        ch_state.publish = None;
                    }
                    return Ok(Step::Continue);
                }

                let Some(ch_state) = self.channels.get_mut(&ch) else {
                    return Ok(Step::Continue);
                };
                match ch_state.publish.take() {
                    Some(PublishAssemble::ExpectHeader { publish }) => {
                        if header.body_size == 0 {
                            // Complete publish with empty body.
                            return self
                                .finish_publish(ch, publish, header.properties, Bytes::new())
                                .await;
                        }
                        ch_state.publish = Some(PublishAssemble::ExpectBody {
                            publish,
                            properties: Box::new(header.properties),
                            body_size: header.body_size,
                            body: Vec::with_capacity(header.body_size.min(1024 * 1024) as usize),
                        });
                        Ok(Step::Continue)
                    }
                    other => {
                        ch_state.publish = other;
                        self.server_channel_close(
                            ch,
                            REPLY_COMMAND_INVALID,
                            "unexpected content header",
                            60,
                            0,
                        )
                        .await?;
                        Ok(Step::Continue)
                    }
                }
            }
            FrameType::Body => {
                let (done_publish, props, body) = {
                    let Some(ch_state) = self.channels.get_mut(&ch) else {
                        return Ok(Step::Continue);
                    };
                    match ch_state.publish.take() {
                        Some(PublishAssemble::ExpectBody {
                            publish,
                            properties,
                            body_size,
                            mut body,
                        }) => {
                            body.extend_from_slice(&frame.payload);
                            if (body.len() as u64) < body_size {
                                ch_state.publish = Some(PublishAssemble::ExpectBody {
                                    publish,
                                    properties,
                                    body_size,
                                    body,
                                });
                                return Ok(Step::Continue);
                            }
                            if (body.len() as u64) > body_size {
                                self.server_channel_close(
                                    ch,
                                    REPLY_COMMAND_INVALID,
                                    "body larger than content header size",
                                    60,
                                    0,
                                )
                                .await?;
                                return Ok(Step::Continue);
                            }
                            (publish, *properties, Bytes::from(body))
                        }
                        other => {
                            ch_state.publish = other;
                            self.server_channel_close(
                                ch,
                                REPLY_COMMAND_INVALID,
                                "unexpected content body",
                                60,
                                0,
                            )
                            .await?;
                            return Ok(Step::Continue);
                        }
                    }
                };
                self.finish_publish(ch, done_publish, props, body).await
            }
            _ => Ok(Step::Continue),
        }
    }

    fn in_tx(&self, channel: u16) -> bool {
        !self.tx_applying && self.channels.get(&channel).is_some_and(|ch| ch.tx_mode)
    }

    async fn tx_select(&mut self, channel: u16) -> Result<Step, ConnError> {
        if let Some(ch) = self.channels.get_mut(&channel) {
            ch.tx_mode = true;
        }
        self.send_method(channel, &Method::TxSelectOk(tx_method::SelectOk))
            .await?;
        Ok(Step::Continue)
    }

    async fn tx_commit(&mut self, channel: u16) -> Result<Step, ConnError> {
        let in_tx = self.channels.get(&channel).is_some_and(|c| c.tx_mode);
        if !in_tx {
            self.server_channel_close(
                channel,
                REPLY_PRECONDITION_FAILED,
                "PRECONDITION_FAILED - channel not in tx mode",
                tx_method::CLASS_ID,
                tx_method::Commit::METHOD_ID,
            )
            .await?;
            return Ok(Step::Continue);
        }
        let ops = self
            .channels
            .get_mut(&channel)
            .map(|c| std::mem::take(&mut c.tx_ops))
            .unwrap_or_default();
        self.tx_applying = true;
        for op in ops {
            let step = match op {
                TxOp::Publish {
                    publish,
                    properties,
                    body,
                } => self.finish_publish(channel, publish, *properties, body).await?,
                TxOp::Ack(ack) => self.handle_basic_ack(channel, ack).await?,
                TxOp::Reject(reject) => {
                    self.handle_basic_nack(channel, reject.delivery_tag, false, reject.requeue)
                        .await?
                }
            };
            if step == Step::Done {
                self.tx_applying = false;
                return Ok(Step::Done);
            }
        }
        self.tx_applying = false;
        self.send_method(channel, &Method::TxCommitOk(tx_method::CommitOk))
            .await?;
        Ok(Step::Continue)
    }

    async fn tx_rollback(&mut self, channel: u16) -> Result<Step, ConnError> {
        let in_tx = self.channels.get(&channel).is_some_and(|c| c.tx_mode);
        if !in_tx {
            self.server_channel_close(
                channel,
                REPLY_PRECONDITION_FAILED,
                "PRECONDITION_FAILED - channel not in tx mode",
                tx_method::CLASS_ID,
                tx_method::Rollback::METHOD_ID,
            )
            .await?;
            return Ok(Step::Continue);
        }
        if let Some(ch) = self.channels.get_mut(&channel) {
            ch.tx_ops.clear();
        }
        self.send_method(channel, &Method::TxRollbackOk(tx_method::RollbackOk))
            .await?;
        Ok(Step::Continue)
    }

    async fn finish_publish(
        &mut self,
        channel: u16,
        publish: basic_method::Publish,
        properties: BasicProperties,
        body: Bytes,
    ) -> Result<Step, ConnError> {
        if !self.tx_applying
            && self
                .channels
                .get(&channel)
                .is_some_and(|ch| ch.tx_mode)
        {
                if let Some(ch) = self.channels.get_mut(&channel) {
                    ch.tx_ops.push(TxOp::Publish {
                        publish,
                        properties: Box::new(properties),
                        body,
                    });
                }
                return Ok(Step::Continue);
        }
        // Publisher-confirm sequence is assigned once content is fully received,
        // for every publish on a confirm-mode channel (success or failure path).
        let confirm_seq = self
            .channels
            .get_mut(&channel)
            .and_then(ChannelState::take_publish_seq);

        if publish.immediate {
            self.server_channel_close(
                channel,
                REPLY_NOT_IMPLEMENTED,
                "NOT_IMPLEMENTED - immediate=true",
                basic_method::CLASS_ID,
                basic_method::Publish::METHOD_ID,
            )
            .await?;
            return Ok(Step::Continue);
        }

        let vhost = self.vhost.clone().unwrap_or_else(|| "/".into());
        let user = self.user.clone().unwrap_or_default();
        let exchange_name = publish.exchange.clone();

        // Write permission on the exchange (default `""` → `amq.default`).
        let user_auth = user.clone();
        let vhost_auth = vhost.clone();
        let exchange_auth = exchange_name.clone();
        match self
            .auth_bool(move |auth| {
                auth.check_permission(
                    &user_auth,
                    &vhost_auth,
                    &exchange_auth,
                    ResourceKind::Exchange,
                    PermissionKind::Write,
                )
            })
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                self.server_channel_close(
                    channel,
                    REPLY_ACCESS_REFUSED,
                    "ACCESS_REFUSED - write access to exchange refused",
                    basic_method::CLASS_ID,
                    basic_method::Publish::METHOD_ID,
                )
                .await?;
                return Ok(Step::Continue);
            }
            Err(e) => {
                self.server_channel_close(
                    channel,
                    REPLY_INTERNAL_ERROR,
                    &format!("INTERNAL_ERROR - auth: {e}"),
                    basic_method::CLASS_ID,
                    basic_method::Publish::METHOD_ID,
                )
                .await?;
                return Ok(Step::Continue);
            }
        }

        queueforge_core::prom::message_received(confirm_seq.is_some());

        // Ensure named exchanges known only from durable meta are in the live table.
        if !exchange_name.is_empty() && self.router.get_exchange(&vhost, &exchange_name).is_none() {
            let store = Arc::clone(&self.store);
            let vhost_lookup = vhost.clone();
            let name = exchange_name.clone();
            if let Some(ex) =
                MetadataStore::blocking(store, move |s| s.get_exchange(&vhost_lookup, &name))
                    .await
                    .ok()
                    .flatten()
            {
                self.router.put_exchange(ex);
            }
        }

        let header_args = field_table_to_header_args(properties.headers.as_ref().unwrap_or(&FieldTable::default()));
        let mut route = match self.router.route_publish(
            &vhost,
            &exchange_name,
            publish.routing_key.as_str(),
            &header_args,
        ) {
            Ok(r) => r,
            Err(CoreError::NotFound(msg)) => {
                self.server_channel_close(
                    channel,
                    REPLY_NOT_FOUND,
                    &format!("NOT_FOUND - {msg}"),
                    basic_method::CLASS_ID,
                    basic_method::Publish::METHOD_ID,
                )
                .await?;
                return Ok(Step::Continue);
            }
            Err(err) => {
                let (code, text) = core_error_to_amqp(&err);
                self.server_channel_close(
                    channel,
                    code,
                    &text,
                    basic_method::CLASS_ID,
                    basic_method::Publish::METHOD_ID,
                )
                .await?;
                return Ok(Step::Continue);
            }
        };
        if let Some(table) = properties.headers.as_ref() {
            for name in ["CC", "BCC"] {
                for key in routing_header_keys(table, name) {
                    if key == publish.routing_key {
                        continue;
                    }
                    let Ok(more) = self.router.route_publish(&vhost, &exchange_name, &key, &header_args) else {
                        continue;
                    };
                    for dest in more.destinations {
                        if !route.destinations.iter().any(|existing| existing == &dest) {
                            route.destinations.push(dest);
                        }
                    }
                }
            }
        }

        for downstream in queueforge_core::federation::federation_targets(&vhost, &exchange_name) {
            let Ok(extra) = self.router.route_publish(&downstream, &exchange_name, publish.routing_key.as_str(), &header_args) else {
                continue;
            };
            for dest in extra.destinations {
                if !route.destinations.iter().any(|existing| existing == &dest) {
                    route.destinations.push(dest);
                }
            }
        }

        // Default exchange: missing queue is 404 (not basic.return).
        let (destinations, mut dest_failures): (Vec<QueueHandle>, u32) =
            if route.kind == ExchangeType::Default {
                let key = &route.destinations[0];
                match self.resolve_queue(key).await {
                    Some(h) if h.is_available() => (vec![h], 0),
                    Some(_) => {
                        self.server_channel_close(
                            channel,
                            REPLY_INTERNAL_ERROR,
                            "INTERNAL_ERROR - queue actor unavailable",
                            basic_method::CLASS_ID,
                            basic_method::Publish::METHOD_ID,
                        )
                        .await?;
                        return Ok(Step::Continue);
                    }
                    None => {
                        self.server_channel_close(
                            channel,
                            REPLY_NOT_FOUND,
                            &format!("NOT_FOUND - no queue '{}' in vhost '{vhost}'", key.name),
                            basic_method::CLASS_ID,
                            basic_method::Publish::METHOD_ID,
                        )
                        .await?;
                        return Ok(Step::Continue);
                    }
                }
            } else {
                // Named exchange: every routed dest must be live. Missing or
                // unavailable targets count as failures (design multi-dest wait-all).
                let mut handles = Vec::new();
                let mut failures = 0u32;
                for key in &route.destinations {
                    match self.resolve_queue(key).await {
                        Some(h) if h.is_available() => handles.push(h),
                        Some(_) | None => {
                            failures = failures.saturating_add(1);
                        }
                    }
                }
                (handles, failures)
            };

        // Zero routes from the index → unroutable (mandatory return / drop).
        // With confirms: still basic.ack after return/drop (message was handled).
        if route.destinations.is_empty() {
            queueforge_core::prom::message_unroutable(publish.mandatory);
            if publish.mandatory {
                self.send_basic_return(
                    channel,
                    REPLY_NO_ROUTE,
                    "NO_ROUTE",
                    publish.exchange.as_str(),
                    publish.routing_key.as_str(),
                    &properties,
                    &body,
                )
                .await?;
            } else {
                metrics::counter!(
                    "queueforge_publish_unroutable_total",
                    "vhost" => vhost.clone(),
                    "exchange" => exchange_name.to_string()
                )
                .increment(1);
            }
            if let Some(seq) = confirm_seq {
                self.send_publisher_confirm(channel, seq, true).await?;
            }
            return Ok(Step::Continue);
        }

        queueforge_core::prom::message_routed(route.destinations.len() as u64);

        // Routed N>0 but none live → partial failure (not silent unroutable).
        if destinations.is_empty() {
            metrics::counter!("queueforge_publish_partial_failure_total").increment(1);
            return self.publish_partial_failure(channel, confirm_seq).await;
        }

        let msg = Arc::new(Message {
            exchange: CompactString::from(if exchange_name.is_empty() {
                DEFAULT_EXCHANGE_NAME
            } else {
                exchange_name.as_str()
            }),
            routing_key: CompactString::from(publish.routing_key.as_str()),
            body,
            persistent: properties.is_persistent(),
            redelivered: false,
            content_type: properties.content_type.as_deref().map(CompactString::from),
            content_encoding: properties
                .content_encoding
                .as_deref()
                .map(CompactString::from),
            correlation_id: properties
                .correlation_id
                .as_deref()
                .map(CompactString::from),
            message_id: properties.message_id.as_deref().map(CompactString::from),
            reply_to: properties.reply_to.as_deref().map(CompactString::from),
            expiration: properties.expiration.as_deref().map(CompactString::from),
            app_id: properties.app_id.as_deref().map(CompactString::from),
            user_id: properties.user_id.as_deref().map(CompactString::from),
            type_: properties.type_.as_deref().map(CompactString::from),
            priority: properties.priority,
            timestamp: properties.timestamp,
            expires_unix_ms: None,
            headers: queueforge_core::MessageHeaders {
                app: properties
                    .headers
                    .as_ref()
                    .map(field_table_to_app_headers)
                    .unwrap_or_default()
                    .into_iter()
                    .filter(|(key, _)| key != "BCC")
                    .collect(),
                ..queueforge_core::MessageHeaders::default()
            },
        });

        let all_quorum = destinations.iter().all(|handle| {
            handle.info.args.lock().unwrap_or_else(|err| err.into_inner()).queue_type == Some(QueueType::Quorum)
        });
        if all_quorum {
            if let Some(cluster) = &self.cluster {
                let mut failed = false;
                for handle in &destinations {
                    if cluster.quorum_enqueue(&handle.info.key, Arc::clone(&msg)).await.is_err() {
                        failed = true;
                    }
                }
                if failed {
                    return self.publish_partial_failure(channel, confirm_seq).await;
                }
                if let Some(seq) = confirm_seq {
                    self.send_publisher_confirm(channel, seq, true).await?;
                }
                return Ok(Step::Continue);
            }
        }

        // Multi-destination wait-all: enqueue to every live dest, then await all
        // EnqueueCompletion.durable_done (non-transactional).
        // Single-dest overflow `reject-publish` → 406 PRECONDITION_FAILED.
        // Resource (memory/disk): if *no* dest succeeded → 506 RESOURCE_ERROR.
        // True partial multi-dest: confirms on → basic.nack; confirms off → channel 541.
        let mut completions = Vec::with_capacity(destinations.len());
        let mut precondition_msgs: Vec<String> = Vec::new();
        let mut other_failures = 0u32;
        let mut resource_err: Option<CoreError> = None;
        let mut success_enqueues: u32 = 0;
        for handle in &destinations {
            let (reply_tx, reply_rx) = oneshot::channel();
            if handle
                .tx
                .send(QueueCmd::Enqueue {
                    msg: Arc::clone(&msg),
                    reply: reply_tx,
                })
                .await
                .is_err()
            {
                dest_failures = dest_failures.saturating_add(1);
                other_failures = other_failures.saturating_add(1);
                continue;
            }
            match reply_rx.await {
                Ok(Ok(completion)) => {
                    success_enqueues = success_enqueues.saturating_add(1);
                    completions.push(completion);
                }
                Ok(Err(CoreError::PreconditionFailed(msg))) => {
                    dest_failures = dest_failures.saturating_add(1);
                    precondition_msgs.push(msg);
                }
                Ok(Err(e)) => {
                    if matches!(e, CoreError::Resource(_)) {
                        if resource_err.is_none() {
                            resource_err = Some(e);
                        }
                    } else {
                        dest_failures = dest_failures.saturating_add(1);
                        other_failures = other_failures.saturating_add(1);
                    }
                }
                Err(_) => {
                    dest_failures = dest_failures.saturating_add(1);
                    other_failures = other_failures.saturating_add(1);
                }
            }
        }

        // Always complete durable_done wait-all for enqueues that were accepted.
        for completion in completions {
            match completion.durable_done.await {
                Ok(Ok(())) => {}
                // oneshot cancel (actor death) is a durable failure — not success.
                Err(_) => {
                    warn!("durable_done oneshot canceled (queue actor gone?)");
                    dest_failures = dest_failures.saturating_add(1);
                    other_failures = other_failures.saturating_add(1);
                }
                Ok(Err(e)) => {
                    warn!(error = %e, "durable_done error on enqueue");
                    dest_failures = dest_failures.saturating_add(1);
                    other_failures = other_failures.saturating_add(1);
                }
            }
        }

        // Pure resource refusal (no dest accepted the message) → 506.
        if let Some(err) = resource_err {
            if success_enqueues == 0 && dest_failures == 0 {
                let (code, text) = core_error_to_amqp(&err);
                self.server_channel_close(
                    channel,
                    code,
                    &text,
                    basic_method::CLASS_ID,
                    basic_method::Publish::METHOD_ID,
                )
                .await?;
                return Ok(Step::Continue);
            }
            // Mixed: treat resource like any other dest failure for 541 path.
            dest_failures = dest_failures.saturating_add(1);
        }

        if dest_failures > 0 {
            // Pure capacity reject (x-overflow=reject-publish) → 406, even for
            // multi-dest when every failure was PreconditionFailed and no other errors.
            let pure_precondition = other_failures == 0
                && !precondition_msgs.is_empty()
                && dest_failures == precondition_msgs.len() as u32;
            // Single live dest + precondition is the common reject-publish case.
            let single_reject = destinations.len() == 1
                && other_failures == 0
                && precondition_msgs.len() == 1
                && dest_failures == 1;

            if pure_precondition || single_reject {
                if let Some(seq) = confirm_seq {
                    self.send_publisher_confirm(channel, seq, false).await?;
                    return Ok(Step::Continue);
                }
                let text = precondition_msgs
                    .first()
                    .map(|m| format!("PRECONDITION_FAILED - {m}"))
                    .unwrap_or_else(|| {
                        "PRECONDITION_FAILED - message rejected as queue length limit is reached"
                            .into()
                    });
                self.server_channel_close(
                    channel,
                    REPLY_PRECONDITION_FAILED,
                    &text,
                    basic_method::CLASS_ID,
                    basic_method::Publish::METHOD_ID,
                )
                .await?;
                return Ok(Step::Continue);
            }

            metrics::counter!("queueforge_publish_partial_failure_total").increment(1);
            // Confirms on → basic.nack; confirms off → channel 541.
            return self.publish_partial_failure(channel, confirm_seq).await;
        }

        metrics::counter!(
            "queueforge_publish_total",
            "vhost" => vhost.clone(),
            "exchange" => exchange_name.to_string()
        )
        .increment(1);

        if let Some(seq) = confirm_seq {
            self.send_publisher_confirm(channel, seq, true).await?;
        }
        Ok(Step::Continue)
    }

    /// Partial multi-destination publish failure.
    ///
    /// Confirms on → `basic.nack` for the publish sequence (channel stays open).
    /// Confirms off → channel exception 541.
    async fn publish_partial_failure(
        &mut self,
        channel: u16,
        confirm_seq: Option<u64>,
    ) -> Result<Step, ConnError> {
        if let Some(seq) = confirm_seq {
            self.send_publisher_confirm(channel, seq, false).await?;
            return Ok(Step::Continue);
        }
        self.server_channel_close(
            channel,
            REPLY_INTERNAL_ERROR,
            "INTERNAL_ERROR - partial multi-destination publish failure",
            basic_method::CLASS_ID,
            basic_method::Publish::METHOD_ID,
        )
        .await?;
        Ok(Step::Continue)
    }

    /// Map `EnqueueCompletion` outcome onto publisher `basic.ack` / `basic.nack`.
    async fn send_publisher_confirm(
        &mut self,
        channel: u16,
        delivery_tag: u64,
        ok: bool,
    ) -> Result<(), ConnError> {
        if ok {
            queueforge_core::prom::message_confirmed();
            self.send_method(
                channel,
                &Method::BasicAck(basic_method::Ack {
                    delivery_tag,
                    multiple: false,
                }),
            )
            .await
        } else {
            self.send_method(
                channel,
                &Method::BasicNack(basic_method::Nack {
                    delivery_tag,
                    multiple: false,
                    requeue: false,
                }),
            )
            .await
        }
    }

    async fn handle_confirm_select(
        &mut self,
        channel: u16,
        select: confirm_method::Select,
    ) -> Result<Step, ConnError> {
        let Some(ch) = self.channels.get_mut(&channel) else {
            return Ok(Step::Continue);
        };
        // Idempotent: re-select keeps sequence numbering continuous.
        ch.confirm_mode = true;
        if !select.nowait {
            self.send_method(channel, &Method::ConfirmSelectOk(confirm_method::SelectOk))
                .await?;
        }
        debug!(peer = %self.peer, channel, "confirm.select enabled");
        Ok(Step::Continue)
    }

    async fn resolve_queue(&self, key: &QueueKey) -> Option<QueueHandle> {
        if let Some(handle) = self.queues.get(key) {
            return Some(handle);
        }
        if let Some(cluster) = &self.cluster {
            return cluster.ensure_proxy(key).await;
        }
        None
    }

    /// Delete a queue from the registry and cascade live binding-index rows.
    ///
    /// Used by `queue.delete`, exclusive cleanup, and auto-delete so cascade
    /// cannot drift across entry points.
    async fn delete_queue_and_bindings(
        &self,
        key: &QueueKey,
        if_unused: bool,
        if_empty: bool,
    ) -> std::result::Result<u32, CoreError> {
        let message_count = if let Some(cluster) = &self.cluster {
            cluster.delete_queue(key, if_unused, if_empty).await?
        } else {
            self.queues.delete(key, if_unused, if_empty).await?
        };
        self.router
            .remove_queue_bindings(key.vhost.as_str(), key.name.as_str());
        Ok(message_count)
    }

    /// Send `basic.return` + content for an unroutable mandatory publish.
    #[allow(clippy::too_many_arguments)]
    async fn send_basic_return(
        &mut self,
        channel: u16,
        reply_code: u16,
        reply_text: &str,
        exchange: &str,
        routing_key: &str,
        properties: &BasicProperties,
        body: &Bytes,
    ) -> Result<Step, ConnError> {
        self.send_method(
            channel,
            &Method::BasicReturn(basic_method::Return {
                reply_code,
                reply_text: reply_text.to_string(),
                exchange: exchange.to_string(),
                routing_key: routing_key.to_string(),
            }),
        )
        .await?;
        self.send_content(channel, properties, body).await?;
        metrics::counter!(
            "queueforge_publish_unroutable_total",
            "vhost" => self.vhost.clone().unwrap_or_else(|| "/".into()),
            "exchange" => exchange.to_string()
        )
        .increment(1);
        Ok(Step::Continue)
    }

    async fn handle_basic_publish(
        &mut self,
        channel: u16,
        publish: basic_method::Publish,
    ) -> Result<Step, ConnError> {
        let Some(ch_state) = self.channels.get_mut(&channel) else {
            return Ok(Step::Continue);
        };
        if ch_state.publish.is_some() {
            self.server_channel_close(
                channel,
                REPLY_COMMAND_INVALID,
                "publish already in progress on channel",
                basic_method::CLASS_ID,
                basic_method::Publish::METHOD_ID,
            )
            .await?;
            return Ok(Step::Continue);
        }
        ch_state.publish = Some(PublishAssemble::ExpectHeader { publish });
        Ok(Step::Continue)
    }

    async fn handle_basic_qos(
        &mut self,
        channel: u16,
        qos: basic_method::Qos,
    ) -> Result<Step, ConnError> {
        // RabbitMQ 4 denies the global QoS feature. `global=true` still returns
        // qos-ok and the count limits each consumer on the channel.
        if let Some(ch) = self.channels.get_mut(&channel) {
            ch.prefetch_count = qos.prefetch_count;
            ch.global_prefetch = 0;
        }
        let sessions: Vec<ConsumerSessionId> = self
            .channels
            .get(&channel)
            .map(|c| c.consumers.values().copied().collect())
            .unwrap_or_default();

        // Reconcile per-consumer credit to the limits that apply on this channel.
        for session in sessions {
            let Some(info) = self.sessions.get(&session).cloned() else {
                continue;
            };
            if info.no_ack {
                continue;
            }
            let credit = self.credit_for(info.channel, false, Some(session));
            self.remember_grant(session, credit);
            let _ = info
                .handle
                .tx
                .send(QueueCmd::SetCredit { session, credit })
                .await;
        }

        // prefetch_size ignored (common for RabbitMQ-compatible brokers).
        self.send_method(channel, &Method::BasicQosOk(basic_method::QosOk))
            .await?;
        Ok(Step::Continue)
    }

    async fn handle_basic_consume(
        &mut self,
        channel: u16,
        consume: basic_method::Consume,
    ) -> Result<Step, ConnError> {
        let unknown: Vec<&str> = consume
            .arguments
            .entries
            .iter()
            .map(|(k, _)| k.as_str())
            .filter(|key| *key != "x-priority")
            .collect();
        if !unknown.is_empty() {
            let keys = unknown;
            self.server_channel_close(
                channel,
                REPLY_PRECONDITION_FAILED,
                &format!(
                    "PRECONDITION_FAILED - unknown consume argument(s): {}",
                    keys.join(", ")
                ),
                basic_method::CLASS_ID,
                basic_method::Consume::METHOD_ID,
            )
            .await?;
            return Ok(Step::Continue);
        }

        let vhost = self.vhost.clone().unwrap_or_else(|| "/".into());
        let user = self.user.clone().unwrap_or_default();
        let queue_name = consume.queue.clone();

        let user_auth = user.clone();
        let vhost_auth = vhost.clone();
        let queue_auth = queue_name.clone();
        match self
            .auth_bool(move |auth| {
                auth.check_permission(
                    &user_auth,
                    &vhost_auth,
                    &queue_auth,
                    ResourceKind::Queue,
                    PermissionKind::Read,
                )
            })
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                self.server_channel_close(
                    channel,
                    REPLY_ACCESS_REFUSED,
                    "ACCESS_REFUSED - read access to queue refused",
                    basic_method::CLASS_ID,
                    basic_method::Consume::METHOD_ID,
                )
                .await?;
                return Ok(Step::Continue);
            }
            Err(e) => {
                self.server_channel_close(
                    channel,
                    REPLY_INTERNAL_ERROR,
                    &format!("INTERNAL_ERROR - auth: {e}"),
                    basic_method::CLASS_ID,
                    basic_method::Consume::METHOD_ID,
                )
                .await?;
                return Ok(Step::Continue);
            }
        }

        let key = QueueKey::new(vhost.as_str(), queue_name.as_str());
        let Some(handle) = self.resolve_queue(&key).await else {
            self.server_channel_close(
                channel,
                REPLY_NOT_FOUND,
                &format!("NOT_FOUND - no queue '{queue_name}' in vhost '{vhost}'"),
                basic_method::CLASS_ID,
                basic_method::Consume::METHOD_ID,
            )
            .await?;
            return Ok(Step::Continue);
        };

        // Session ids are queue-global. Per-connection counters collide when
        // two connections consume from the same queue (both would use session 1).
        let session = ConsumerSessionId(next_consumer_session());
        let consumer_tag = if consume.consumer_tag.is_empty() {
            format!("ctag-{channel}-{}", session.0)
        } else {
            consume.consumer_tag.clone()
        };

        if self
            .channels
            .get(&channel)
            .map(|c| c.consumers.contains_key(&consumer_tag))
            .unwrap_or(false)
        {
            self.server_channel_close(
                channel,
                REPLY_PRECONDITION_FAILED,
                "PRECONDITION_FAILED - consumer tag already in use",
                basic_method::CLASS_ID,
                basic_method::Consume::METHOD_ID,
            )
            .await?;
            return Ok(Step::Continue);
        }

        // Issue 1: `None` = unlimited; `Some(0)` = zero remaining slots (hold).
        // Never overload 0 as unlimited.
        let initial_credit = self.credit_for(channel, consume.no_ack, Some(session));
        self.remember_grant(session, initial_credit);

        let quorum = handle
            .info
            .args
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .queue_type
            == Some(QueueType::Quorum);
        let handle = if quorum {
            self.cluster
                .as_ref()
                .and_then(|cluster| cluster.leader_consume_handle(&key))
                .unwrap_or(handle)
        } else {
            handle
        };

        let (reply_tx, reply_rx) = oneshot::channel();
        if handle
            .tx
            .send(QueueCmd::RegisterConsumer {
                session,
                no_ack: consume.no_ack,
                exclusive: consume.exclusive,
                priority: consumer_priority(&consume.arguments),
                initial_credit,
                deliver_tx: self.delivery_tx.clone(),
                reply: reply_tx,
            })
            .await
            .is_err()
        {
            self.granted_credit.remove(&session);
            self.server_channel_close(
                channel,
                REPLY_INTERNAL_ERROR,
                "INTERNAL_ERROR - queue mailbox closed",
                basic_method::CLASS_ID,
                basic_method::Consume::METHOD_ID,
            )
            .await?;
            return Ok(Step::Continue);
        }

        match reply_rx.await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                self.granted_credit.remove(&session);
                let (code, text) = core_error_to_amqp(&err);
                self.server_channel_close(
                    channel,
                    code,
                    &text,
                    basic_method::CLASS_ID,
                    basic_method::Consume::METHOD_ID,
                )
                .await?;
                return Ok(Step::Continue);
            }
            Err(_) => {
                self.granted_credit.remove(&session);
                self.server_channel_close(
                    channel,
                    REPLY_INTERNAL_ERROR,
                    "INTERNAL_ERROR - register consumer reply dropped",
                    basic_method::CLASS_ID,
                    basic_method::Consume::METHOD_ID,
                )
                .await?;
                return Ok(Step::Continue);
            }
        }

        self.sessions.insert(
            session,
            SessionInfo {
                channel,
                consumer_tag: consumer_tag.clone(),
                queue_key: key,
                handle: handle.clone(),
                no_ack: consume.no_ack,
            },
        );
        if let Some(ch) = self.channels.get_mut(&channel) {
            ch.consumers.insert(consumer_tag.clone(), session);
        }

        if !consume.no_wait {
            self.send_method(
                channel,
                &Method::BasicConsumeOk(basic_method::ConsumeOk {
                    consumer_tag: consumer_tag.clone(),
                }),
            )
            .await?;
        }
        Ok(Step::Continue)
    }

    async fn handle_basic_cancel(
        &mut self,
        channel: u16,
        cancel: basic_method::Cancel,
    ) -> Result<Step, ConnError> {
        let session = self
            .channels
            .get(&channel)
            .and_then(|c| c.consumers.get(&cancel.consumer_tag).copied());

        if let Some(session) = session {
            self.cancel_session(session, /*requeue=*/ true).await;
            if let Some(ch) = self.channels.get_mut(&channel) {
                ch.consumers.remove(&cancel.consumer_tag);
            }
        }

        if !cancel.no_wait {
            self.send_method(
                channel,
                &Method::BasicCancelOk(basic_method::CancelOk {
                    consumer_tag: cancel.consumer_tag,
                }),
            )
            .await?;
        }
        Ok(Step::Continue)
    }

    async fn handle_basic_get(
        &mut self,
        channel: u16,
        get: basic_method::Get,
    ) -> Result<Step, ConnError> {
        let vhost = self.vhost.clone().unwrap_or_else(|| "/".into());
        let user = self.user.clone().unwrap_or_default();

        let queue_name = get.queue.clone();
        let user_auth = user.clone();
        let vhost_auth = vhost.clone();
        match self
            .auth_bool(move |auth| {
                auth.check_permission(
                    &user_auth,
                    &vhost_auth,
                    &queue_name,
                    ResourceKind::Queue,
                    PermissionKind::Read,
                )
            })
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                self.server_channel_close(
                    channel,
                    REPLY_ACCESS_REFUSED,
                    "ACCESS_REFUSED - read access to queue refused",
                    basic_method::CLASS_ID,
                    basic_method::Get::METHOD_ID,
                )
                .await?;
                return Ok(Step::Continue);
            }
            Err(e) => {
                self.server_channel_close(
                    channel,
                    REPLY_INTERNAL_ERROR,
                    &format!("INTERNAL_ERROR - auth: {e}"),
                    basic_method::CLASS_ID,
                    basic_method::Get::METHOD_ID,
                )
                .await?;
                return Ok(Step::Continue);
            }
        }

        let key = QueueKey::new(vhost.as_str(), get.queue.as_str());
        let Some(mut handle) = self.resolve_queue(&key).await else {
            self.server_channel_close(
                channel,
                REPLY_NOT_FOUND,
                &format!("NOT_FOUND - no queue '{}' in vhost '{vhost}'", get.queue),
                basic_method::CLASS_ID,
                basic_method::Get::METHOD_ID,
            )
            .await?;
            return Ok(Step::Continue);
        };
        if let Some(cluster) = &self.cluster {
            let quorum = handle
                .info
                .args
                .lock()
                .unwrap_or_else(|err| err.into_inner())
                .queue_type
                == Some(QueueType::Quorum);
            if quorum && !cluster.is_quorum_leader() {
                if let Some(proxied) = cluster.leader_consume_handle(&key) {
                    handle = proxied;
                }
            }
        }

        let (reply_tx, reply_rx) = oneshot::channel();
        if handle
            .tx
            .send(QueueCmd::Get {
                no_ack: get.no_ack,
                reply: reply_tx,
            })
            .await
            .is_err()
        {
            self.server_channel_close(
                channel,
                REPLY_INTERNAL_ERROR,
                "INTERNAL_ERROR - queue mailbox closed",
                basic_method::CLASS_ID,
                basic_method::Get::METHOD_ID,
            )
            .await?;
            return Ok(Step::Continue);
        }

        match reply_rx.await {
            Ok(None) => {
                queueforge_core::prom::message_get_empty();
                self.send_method(
                    channel,
                    &Method::BasicGetEmpty(basic_method::GetEmpty::new()),
                )
                .await?;
                Ok(Step::Continue)
            }
            Ok(Some((delivery_id, qm, message_count))) => {
                let quorum = handle
                    .info
                    .args
                    .lock()
                    .unwrap_or_else(|err| err.into_inner())
                    .queue_type
                    == Some(QueueType::Quorum);
                if quorum {
                    if let Some(message_id) = qm.message.message_id.clone() {
                        if let Some(cluster) = &self.cluster {
                            let drop_local = !cluster.is_quorum_leader();
                            cluster.claim_for_handoff(&key, message_id.as_str(), drop_local).await;
                        }
                    }
                }
                let delivery_tag = {
                    let ch = self.channels.get_mut(&channel).unwrap();
                    let tag = ch.next_delivery_tag;
                    ch.next_delivery_tag = ch.next_delivery_tag.saturating_add(1);
                    if !get.no_ack {
                        ch.delivery_ledger.insert(
                            tag,
                            OutstandingDelivery {
                                queue_key: key.clone(),
                                consumer_delivery_id: delivery_id,
                                consumer_tag: None,
                                session: None,
                            },
                        );
                    }
                    tag
                };

                let props = message_to_properties(&qm.message);
                let body = qm.message.body.clone();
                let redelivered = qm.message.redelivered;

                self.send_method(
                    channel,
                    &Method::BasicGetOk(basic_method::GetOk {
                        delivery_tag,
                        redelivered,
                        exchange: qm.message.exchange.to_string(),
                        routing_key: qm.message.routing_key.to_string(),
                        message_count,
                    }),
                )
                .await?;
                self.send_content(channel, &props, &body).await?;
                Ok(Step::Continue)
            }
            Err(_) => {
                self.server_channel_close(
                    channel,
                    REPLY_INTERNAL_ERROR,
                    "INTERNAL_ERROR - queue home is unavailable",
                    basic_method::CLASS_ID,
                    basic_method::Get::METHOD_ID,
                )
                .await?;
                Ok(Step::Continue)
            }
        }
    }

    async fn handle_basic_ack(
        &mut self,
        channel: u16,
        ack: basic_method::Ack,
    ) -> Result<Step, ConnError> {
        self.ack_or_nack_tags(
            channel,
            ack.delivery_tag,
            ack.multiple,
            /*nack=*/ false,
            true,
        )
        .await
    }

    async fn handle_basic_recover(&mut self, channel: u16) -> Result<Step, ConnError> {
        let max_tag = self
            .channels
            .get(&channel)
            .and_then(|ch| ch.delivery_ledger.keys().next_back().copied());
        if let Some(tag) = max_tag {
            self.ack_or_nack_tags(channel, tag, true, true, true).await?;
        }
        self.send_method(channel, &Method::BasicRecoverOk(basic_method::RecoverOk))
            .await?;
        Ok(Step::Continue)
    }

    async fn handle_basic_nack(
        &mut self,
        channel: u16,
        delivery_tag: u64,
        multiple: bool,
        requeue: bool,
    ) -> Result<Step, ConnError> {
        self.ack_or_nack_tags(
            channel,
            delivery_tag,
            multiple,
            /*nack=*/ true,
            requeue,
        )
        .await
    }

    async fn ack_or_nack_tags(
        &mut self,
        channel: u16,
        delivery_tag: u64,
        multiple: bool,
        nack: bool,
        requeue: bool,
    ) -> Result<Step, ConnError> {
        let Some(ch) = self.channels.get_mut(&channel) else {
            return Ok(Step::Continue);
        };

        // Issue 3: non-zero delivery_tag must refer to a delivered message (multiple or not).
        // delivery_tag==0 with multiple=true means "all outstanding".
        if delivery_tag != 0 && !ch.delivery_ledger.contains_key(&delivery_tag) {
            self.server_channel_close(
                channel,
                REPLY_PRECONDITION_FAILED,
                "PRECONDITION_FAILED - unknown delivery tag",
                basic_method::CLASS_ID,
                if nack {
                    basic_method::Nack::METHOD_ID
                } else {
                    basic_method::Ack::METHOD_ID
                },
            )
            .await?;
            return Ok(Step::Continue);
        }
        if !multiple && delivery_tag == 0 {
            self.server_channel_close(
                channel,
                REPLY_PRECONDITION_FAILED,
                "PRECONDITION_FAILED - unknown delivery tag",
                basic_method::CLASS_ID,
                if nack {
                    basic_method::Nack::METHOD_ID
                } else {
                    basic_method::Ack::METHOD_ID
                },
            )
            .await?;
            return Ok(Step::Continue);
        }

        let tags: Vec<u64> = if multiple {
            if delivery_tag == 0 {
                ch.delivery_ledger.keys().copied().collect()
            } else {
                ch.delivery_ledger
                    .range(..=delivery_tag)
                    .map(|(t, _)| *t)
                    .collect()
            }
        } else {
            vec![delivery_tag]
        };

        let mut credit_by_session: HashMap<ConsumerSessionId, u32> = HashMap::new();
        let mut ops: Vec<(QueueKey, ConsumerDeliveryId, Option<ConsumerSessionId>)> = Vec::new();

        {
            let ch = self.channels.get_mut(&channel).unwrap();
            for tag in tags {
                if let Some(entry) = ch.delivery_ledger.remove(&tag) {
                    if let Some(session) = entry.session {
                        *credit_by_session.entry(session).or_insert(0) += 1;
                    }
                    ops.push((entry.queue_key, entry.consumer_delivery_id, entry.session));
                }
            }
        }

        for (queue_key, id, session) in ops {
            let from_session = session.and_then(|session| self.sessions.get(&session).map(|info| info.handle.clone()));
            let session_holds_ack = from_session.is_some();
            let Some(mut handle) = (match from_session {
                Some(handle) => Some(handle),
                None => self.resolve_queue(&queue_key).await,
            }) else {
                continue;
            };
            if !session_holds_ack {
                if let Some(cluster) = &self.cluster {
                    let quorum = handle
                        .info
                        .args
                        .lock()
                        .unwrap_or_else(|err| err.into_inner())
                        .queue_type
                        == Some(QueueType::Quorum);
                    if quorum && !cluster.is_quorum_leader() {
                        if let Some(proxied) = cluster.leader_consume_handle(&queue_key) {
                            handle = proxied;
                        }
                    }
                }
            }
            let quorum = handle
                .info
                .args
                .lock()
                .unwrap_or_else(|err| err.into_inner())
                .queue_type
                == Some(QueueType::Quorum);
            if quorum {
                let (reply_tx, reply_rx) = oneshot::channel();
                let cmd = if nack {
                    QueueCmd::NackReport { id, requeue, reply: reply_tx }
                } else {
                    QueueCmd::AckReport { id, reply: reply_tx }
                };
                if handle.tx.send(cmd).await.is_ok() {
                    if let Ok(Some(message_id)) = reply_rx.await {
                        if let Some(cluster) = &self.cluster {
                            cluster.quorum_forget(&queue_key, message_id.as_str()).await;
                        }
                    }
                }
            } else {
                let cmd = if nack {
                    QueueCmd::Nack { id, requeue }
                } else {
                    QueueCmd::Ack {
                        id,
                        multiple_to: None,
                    }
                };
                let _ = handle.tx.send(cmd).await;
            }
        }

        // Restore prefetch credit to consumers.
        if !nack || requeue {
            // For ack (and nack+requeue that frees channel outstanding), restore credit.
        }
        for (session, credit) in credit_by_session {
            if let Some(info) = self.sessions.get(&session).cloned() {
                if !info.no_ack {
                    let give = match self.channel_global_slots(info.channel, Some(session)) {
                        Some(slots) => credit.min(slots),
                        None => credit,
                    };
                    if give == 0 {
                        continue;
                    }
                    *self.granted_credit.entry(session).or_insert(0) += give;
                    let _ = info
                        .handle
                        .tx
                        .send(QueueCmd::AddCredit {
                            session,
                            credit: give,
                        })
                        .await;
                }
            }
        }

        Ok(Step::Continue)
    }

    async fn forward_delivery(&mut self, delivery: QueueDelivery) -> Result<(), ConnError> {
        if delivery.server_cancel {
            if let Some(info) = self.sessions.remove(&delivery.session) {
                if let Some(ch) = self.channels.get_mut(&info.channel) {
                    ch.consumers.remove(&info.consumer_tag);
                }
                self.send_method(
                    info.channel,
                    &Method::BasicCancel(basic_method::Cancel {
                        consumer_tag: info.consumer_tag,
                        no_wait: true,
                    }),
                )
                .await?;
            }
            return Ok(());
        }
        let Some(info) = self.sessions.get(&delivery.session).cloned() else {
            // Orphan delivery — nack/requeue.
            // We don't know the queue handle easily; drop.
            debug!(session = delivery.session.0, "orphan delivery dropped");
            return Ok(());
        };

        if !self.channels.contains_key(&info.channel) {
            // Channel gone: requeue via nack.
            let _ = info
                .handle
                .tx
                .send(QueueCmd::Nack {
                    id: delivery.delivery_id,
                    requeue: true,
                })
                .await;
            return Ok(());
        }

        // Channel prefetch enforcement: if over limit, requeue (shouldn't happen with credit).
        let over_prefetch = {
            let ch = self.channels.get(&info.channel).unwrap();
            let per_full = ch.prefetch_count > 0
                && self.consumer_outstanding(info.channel, Some(delivery.session))
                    >= u32::from(ch.prefetch_count);
            let global_full =
                ch.global_prefetch > 0 && ch.outstanding() >= u32::from(ch.global_prefetch);
            (per_full || global_full) && !info.no_ack
        };
        if over_prefetch {
            // The actor already spent one credit to send this delivery. Do not
            // give it back: AddCredit here makes the actor redeliver immediately
            // and the connection nack again.
            self.spend_grant(delivery.session);
            let _ = info
                .handle
                .tx
                .send(QueueCmd::Nack {
                    id: delivery.delivery_id,
                    requeue: true,
                })
                .await;
            return Ok(());
        }
        self.spend_grant(delivery.session);

        let local_holder = self
            .queues
            .get(&info.queue_key)
            .map(|local| local.tx.same_channel(&info.handle.tx))
            .unwrap_or(false);
        let quorum = info
            .handle
            .info
            .args
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .queue_type
            == Some(QueueType::Quorum);
        if quorum {
            if let Some(message_id) = delivery.message.message.message_id.clone() {
                if let Some(cluster) = &self.cluster {
                    cluster
                        .claim_for_handoff(&info.queue_key, message_id.as_str(), !local_holder)
                        .await;
                }
            }
        }

        let delivery_tag = {
            let ch = self.channels.get_mut(&info.channel).unwrap();
            let tag = ch.next_delivery_tag;
            ch.next_delivery_tag = ch.next_delivery_tag.saturating_add(1);
            if !info.no_ack {
                ch.delivery_ledger.insert(
                    tag,
                    OutstandingDelivery {
                        queue_key: info.queue_key.clone(),
                        consumer_delivery_id: delivery.delivery_id,
                        consumer_tag: Some(info.consumer_tag.clone()),
                        session: Some(delivery.session),
                    },
                );
            }
            tag
        };

        let props = message_to_properties(&delivery.message.message);
        let body = delivery.message.message.body.clone();
        let redelivered = delivery.message.message.redelivered;

        self.send_method(
            info.channel,
            &Method::BasicDeliver(basic_method::Deliver {
                consumer_tag: info.consumer_tag.clone(),
                delivery_tag,
                redelivered,
                exchange: delivery.message.message.exchange.to_string(),
                routing_key: delivery.message.message.routing_key.to_string(),
            }),
        )
        .await?;
        self.send_content(info.channel, &props, &body).await?;
        if delivery.settles_on_write {
            let _ = info
                .handle
                .tx
                .send(QueueCmd::SettleDelivered {
                    id: delivery.delivery_id,
                })
                .await;
            let _ = info
                .handle
                .tx
                .send(QueueCmd::AddCredit {
                    session: delivery.session,
                    credit: 1,
                })
                .await;
        }
        Ok(())
    }

    async fn send_content(
        &mut self,
        channel: u16,
        props: &BasicProperties,
        body: &Bytes,
    ) -> Result<(), ConnError> {
        let header = ContentHeader::basic(body.len() as u64, props.clone());
        let header_payload = header.encode().map_err(ConnError::Amqp)?;
        self.send_frame(&Frame::header(channel, header_payload))
            .await?;

        if body.is_empty() {
            return Ok(());
        }

        // Split body to respect frame_max.
        let max_payload = self.max_payload();
        let mut offset = 0;
        while offset < body.len() {
            let end = (offset + max_payload).min(body.len());
            self.send_frame(&Frame::body(channel, body[offset..end].to_vec()))
                .await?;
            offset = end;
        }
        Ok(())
    }

    async fn cancel_session(&mut self, session: ConsumerSessionId, requeue: bool) {
        let Some(info) = self.sessions.get(&session).cloned() else {
            return;
        };
        let quorum = info
            .handle
            .info
            .args
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .queue_type
            == Some(QueueType::Quorum);
        if quorum {
            // Detach the leader consumer and leave the unacked entry for the client's nack.
            let (reply_tx, reply_rx) = oneshot::channel();
            let _ = info
                .handle
                .tx
                .send(QueueCmd::UnregisterConsumer {
                    session,
                    requeue: false,
                    reply: reply_tx,
                })
                .await;
            let _ = reply_rx.await;
            self.sessions.remove(&session);
            self.granted_credit.remove(&session);
            if let Some(ch) = self.channels.get_mut(&info.channel) {
                ch.consumers.remove(&info.consumer_tag);
            }
            return;
        }
        self.sessions.remove(&session);
        self.granted_credit.remove(&session);

        // Drop ledger entries for this session and nack on queue.
        if let Some(ch) = self.channels.get_mut(&info.channel) {
            let tags: Vec<u64> = ch
                .delivery_ledger
                .iter()
                .filter(|(_, e)| e.session == Some(session))
                .map(|(t, _)| *t)
                .collect();
            for tag in tags {
                if let Some(entry) = ch.delivery_ledger.remove(&tag) {
                    if requeue {
                        let _ = info
                            .handle
                            .tx
                            .send(QueueCmd::Nack {
                                id: entry.consumer_delivery_id,
                                requeue: true,
                            })
                            .await;
                    } else {
                        let _ = info
                            .handle
                            .tx
                            .send(QueueCmd::Ack {
                                id: entry.consumer_delivery_id,
                                multiple_to: None,
                            })
                            .await;
                    }
                }
            }
            ch.consumers.remove(&info.consumer_tag);
        }

        let (reply_tx, reply_rx) = oneshot::channel();
        let _ = info
            .handle
            .tx
            .send(QueueCmd::UnregisterConsumer {
                session,
                requeue,
                reply: reply_tx,
            })
            .await;
        let _ = reply_rx.await;

        // Auto-delete: if no consumers remain, delete the queue + bindings.
        if info.handle.info.auto_delete {
            let key = info.queue_key;
            let _ = self
                .delete_queue_and_bindings(&key, /*if_unused=*/ true, /*if_empty=*/ false)
                .await;
        }
    }

    async fn teardown_channel(&mut self, channel: u16, requeue: bool) {
        let Some(mut ch_state) = self.channels.remove(&channel) else {
            return;
        };
        metrics::gauge!("queueforge_channels").decrement(1.0);
        queueforge_core::prom::channel_closed();
        self.sync_tracked_channels();

        // Cancel all consumers on this channel.
        let sessions: Vec<ConsumerSessionId> = ch_state.consumers.values().copied().collect();
        for session in sessions {
            // cancel_session also tries to clean channel consumers; channel already removed.
            if let Some(info) = self.sessions.remove(&session) {
                self.granted_credit.remove(&session);
                // Requeue ledger entries still in ch_state.
                let tags: Vec<u64> = ch_state
                    .delivery_ledger
                    .iter()
                    .filter(|(_, e)| e.session == Some(session))
                    .map(|(t, _)| *t)
                    .collect();
                for tag in tags {
                    if let Some(entry) = ch_state.delivery_ledger.remove(&tag) {
                        let _ = info
                            .handle
                            .tx
                            .send(QueueCmd::Nack {
                                id: entry.consumer_delivery_id,
                                requeue,
                            })
                            .await;
                    }
                }
                let (reply_tx, reply_rx) = oneshot::channel();
                let _ = info
                    .handle
                    .tx
                    .send(QueueCmd::UnregisterConsumer {
                        session,
                        requeue,
                        reply: reply_tx,
                    })
                    .await;
                let _ = reply_rx.await;
                if info.handle.info.auto_delete {
                    let _ = self
                        .delete_queue_and_bindings(&info.queue_key, true, false)
                        .await;
                }
            }
        }

        // Remaining ledger entries (e.g. basic.get).
        for (_, entry) in std::mem::take(&mut ch_state.delivery_ledger) {
            if let Some(handle) = self.resolve_queue(&entry.queue_key).await {
                let _ = handle
                    .tx
                    .send(QueueCmd::Nack {
                        id: entry.consumer_delivery_id,
                        requeue,
                    })
                    .await;
            }
        }
    }

    async fn cleanup_on_close(&mut self) {
        if self.cleaned_up {
            return;
        }
        self.cleaned_up = true;

        let channels: Vec<u16> = self.channels.keys().copied().collect();
        for ch in channels {
            self.teardown_channel(ch, /*requeue=*/ true).await;
        }

        // Delete exclusive queues owned by this connection (cascade bindings).
        let owner = self.peer.to_string();
        let exclusive = self.queues.list_exclusive_owned_by(&owner);
        for handle in exclusive {
            let key = handle.info.key.clone();
            let _ = self.delete_queue_and_bindings(&key, false, false).await;
            self.declared_queues.retain(|k| k != &key);
        }

        // Auto-delete queues declared here with no consumers (exclusive already gone).
        let vhost = self.vhost.clone().unwrap_or_else(|| "/".into());
        let auto = self.queues.list_auto_delete_in_vhost(&vhost);
        for handle in auto {
            // Only delete if this connection declared them and they have no consumers.
            if self.declared_queues.iter().any(|k| k == &handle.info.key) {
                let _ = self
                    .delete_queue_and_bindings(&handle.info.key, /*if_unused=*/ true, false)
                    .await;
            }
        }
        self.declared_queues.clear();
        self.sessions.clear();
    }

    async fn send_connection_start(&mut self) -> Result<(), ConnError> {
        let mut props = FieldTable::new();
        props.insert("product", FieldValue::long_str("QueueForge"));
        props.insert("version", FieldValue::long_str(env!("CARGO_PKG_VERSION")));
        props.insert(
            "copyright",
            FieldValue::long_str("Copyright (c) QueueForge"),
        );
        props.insert(
            "capabilities",
            FieldValue::Table(FieldTable::from_pairs([
                // Only advertise capabilities we actually honor.
                ("publisher_confirms", FieldValue::Bool(true)),
                ("consumer_cancel_notify", FieldValue::Bool(true)),
                ("basic.nack", FieldValue::Bool(true)),
                ("connection.blocked", FieldValue::Bool(false)),
                ("authentication_failure_close", FieldValue::Bool(true)),
            ])),
        );
        props.insert(
            "information",
            FieldValue::long_str("https://github.com/queueforge/queueforge"),
        );

        let start = Method::ConnectionStart(conn_method::Start {
            version_major: 0,
            version_minor: 9,
            server_properties: props,
            mechanisms: b"PLAIN".to_vec(),
            locales: b"en_US".to_vec(),
        });
        self.send_method(0, &start).await
    }

    async fn send_connection_tune(&mut self) -> Result<(), ConnError> {
        let tune = Method::ConnectionTune(conn_method::Tune {
            channel_max: self.params.channel_max,
            frame_max: self.params.frame_max,
            heartbeat: self.params.heartbeat,
        });
        self.send_method(0, &tune).await
    }

    async fn send_connection_close(
        &mut self,
        reply_code: u16,
        reply_text: &str,
        class_id: u16,
        method_id: u16,
    ) -> Result<(), ConnError> {
        let close = Method::ConnectionClose(conn_method::Close {
            reply_code,
            reply_text: reply_text.to_string(),
            class_id,
            method_id,
        });
        // Best-effort; peer may already be gone.
        let _ = self.send_method(0, &close).await;
        Ok(())
    }

    /// Server-initiated channel.close: drop the channel from the open set
    /// (and the channels gauge) before sending, so clients may reopen the id.
    async fn server_channel_close(
        &mut self,
        channel: u16,
        reply_code: u16,
        reply_text: &str,
        class_id: u16,
        method_id: u16,
    ) -> Result<(), ConnError> {
        // Requeue unacked and drop consumers for this channel.
        self.teardown_channel(channel, /*requeue=*/ true).await;
        let close = Method::ChannelClose(chan_method::Close {
            reply_code,
            reply_text: reply_text.to_string(),
            class_id,
            method_id,
        });
        self.send_method(channel, &close).await
    }

    async fn send_method(&mut self, channel: u16, method: &Method) -> Result<(), ConnError> {
        let frame = method.to_frame(channel)?;
        self.send_frame(&frame).await
    }

    async fn send_frame(&mut self, frame: &Frame) -> Result<(), ConnError> {
        let bytes = frame.encode()?;
        self.stream.write_all(&bytes).await?;
        self.stream.flush().await?;
        self.last_send = Instant::now();
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
