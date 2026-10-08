//! AMQP TCP / TLS listener: accept connections and spawn handshake tasks.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use queueforge_core::{ExchangeRouter, QueueRegistry};
use queueforge_mgmt::ConnectionTracker as MgmtConnectionTracker;
use queueforge_store::MetadataStore;
use rustls::ServerConfig;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_rustls::TlsAcceptor;
use tracing::{error, info, warn};

use crate::connection::{handle_connection, ConnectionParams};
use crate::limits::{ConnectionLimiter, ConnectionPermit};
use crate::shutdown::{ConnectionTracker, CONNECTION_DRAIN_TIMEOUT};

/// Running AMQP TCP (or TLS) listener.
pub struct AmqpListener {
    /// Bound local address (useful when listening on port 0).
    pub local_addr: SocketAddr,
    /// Whether accepted connections are wrapped in TLS.
    pub tls: bool,
    /// Shared drain tracker (drain signal + active count).
    pub tracker: ConnectionTracker,
    accept_join: JoinHandle<()>,
    /// Signals the accept loop to stop (dropped after stop).
    stop_tx: Option<oneshot::Sender<()>>,
}

impl AmqpListener {
    /// Abort the accept loop without draining connections.
    pub fn abort(mut self) {
        if let Some(tx) = self.stop_tx.take() {
            let _ = tx.send(());
        }
        self.accept_join.abort();
    }

    /// Stop accepting, broadcast `connection.close`, wait for drain (or timeout).
    pub async fn graceful_stop(mut self, drain_timeout: Duration) -> bool {
        self.tracker.begin_drain();

        info!(local_addr = %self.local_addr, "stopping AMQP accept loop");
        if let Some(tx) = self.stop_tx.take() {
            let _ = tx.send(());
        }
        let accept_join = self.accept_join;
        let abort = accept_join.abort_handle();
        match tokio::time::timeout(Duration::from_secs(2), accept_join).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) if e.is_cancelled() => {}
            Ok(Err(e)) => {
                warn!(error = %e, "AMQP accept task join error");
            }
            Err(_) => {
                warn!("AMQP accept task did not exit in time; aborting");
                abort.abort();
            }
        }

        let ok = self.tracker.wait_drained(drain_timeout).await;
        if ok {
            info!("AMQP connections drained");
        }
        ok
    }

    /// [`graceful_stop`] with the design-default drain timeout (10s).
    pub async fn graceful_stop_default(self) -> bool {
        self.graceful_stop(CONNECTION_DRAIN_TIMEOUT).await
    }

    /// Clone of the local address.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}

/// Handle returned when the accept loop is detached (same as [`AmqpListener`]).
pub type AmqpListenerHandle = AmqpListener;

/// Bind `addr` and accept AMQP connections until stopped.
pub async fn start_amqp_listener(
    addr: SocketAddr,
    store: Arc<MetadataStore>,
    queues: Arc<QueueRegistry>,
    router: Arc<ExchangeRouter>,
    connections: Arc<MgmtConnectionTracker>,
    params: ConnectionParams,
) -> std::io::Result<AmqpListener> {
    start_amqp_listener_with_limits(
        addr,
        store,
        queues,
        router,
        connections,
        params,
        None,
        None,
        None,
    )
    .await
}

/// Like [`start_amqp_listener`] with optional connection limiter and TLS.
#[allow(clippy::too_many_arguments)]
pub async fn start_amqp_listener_with_limits(
    addr: SocketAddr,
    store: Arc<MetadataStore>,
    queues: Arc<QueueRegistry>,
    router: Arc<ExchangeRouter>,
    connections: Arc<MgmtConnectionTracker>,
    params: ConnectionParams,
    limiter: Option<Arc<ConnectionLimiter>>,
    tls: Option<Arc<ServerConfig>>,
    cluster: Option<Arc<crate::cluster::Cluster>>,
) -> std::io::Result<AmqpListener> {
    let listener = TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;
    let tls_enabled = tls.is_some();
    info!(%local_addr, tls = tls_enabled, "AMQP listening");
    if !tls_enabled {
        // Bridge sessions (MQTT, STOMP, AMQP 1.0) log in through a plain listener.
        queueforge_mgmt::bridge::set_loopback_port(local_addr.port());
    }

    let tracker = ConnectionTracker::new();
    let (stop_tx, stop_rx) = oneshot::channel();
    let tracker_accept = tracker.clone();

    let accept_join = tokio::spawn(async move {
        accept_loop(
            listener,
            store,
            queues,
            router,
            connections,
            params,
            limiter,
            tls,
            cluster,
            tracker_accept,
            stop_rx,
        )
        .await;
    });

    Ok(AmqpListener {
        local_addr,
        tls: tls_enabled,
        tracker,
        accept_join,
        stop_tx: Some(stop_tx),
    })
}

#[allow(clippy::too_many_arguments)]
async fn accept_loop(
    listener: TcpListener,
    store: Arc<MetadataStore>,
    queues: Arc<QueueRegistry>,
    router: Arc<ExchangeRouter>,
    connections: Arc<MgmtConnectionTracker>,
    params: ConnectionParams,
    limiter: Option<Arc<ConnectionLimiter>>,
    tls: Option<Arc<ServerConfig>>,
    cluster: Option<Arc<crate::cluster::Cluster>>,
    tracker: ConnectionTracker,
    mut stop_rx: oneshot::Receiver<()>,
) {
    let acceptor = tls.map(TlsAcceptor::from);

    loop {
        tokio::select! {
            _ = &mut stop_rx => {
                info!("AMQP accept loop stopped");
                break;
            }
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, peer)) => {
                        if tracker.is_draining() {
                            drop(stream);
                            continue;
                        }
                        if let Err(e) = stream.set_nodelay(true) {
                            warn!(%peer, error = %e, "failed to set TCP_NODELAY");
                        }

                        let permit = if let Some(lim) = limiter.as_ref() {
                            match ConnectionPermit::try_acquire(lim) {
                                Some(p) => Some(p),
                                None => {
                                    metrics::counter!(
                                        "queueforge_connection_refused_total",
                                        "reason" => "max_connections"
                                    )
                                    .increment(1);
                                    warn!(
                                        %peer,
                                        max = lim.max(),
                                        active = lim.active(),
                                        "refusing connection: max_connections reached"
                                    );
                                    drop(stream);
                                    continue;
                                }
                            }
                        } else {
                            None
                        };

                        let guard = tracker.track();
                        let shutdown = tracker.subscribe();
                        if *shutdown.borrow() {
                            drop(stream);
                            drop(guard);
                            drop(permit);
                            continue;
                        }

                        let store = Arc::clone(&store);
                        let queues = Arc::clone(&queues);
                        let router = Arc::clone(&router);
                        let connections = Arc::clone(&connections);
                        let cluster = cluster.clone();
                        let acceptor = acceptor.clone();
                        tokio::spawn(async move {
                            match acceptor {
                                Some(acceptor) => match acceptor.accept(stream).await {
                                    Ok(mut tls_stream) => {
                                        // A TLS stream cannot be peeked: read the header, then replay it.
                                        let mut head = [0u8; 8];
                                        let n = read_header(&mut tls_stream, &mut head).await;
                                        let local_port = tls_stream.get_ref().0.local_addr().map(|a| a.port()).unwrap_or(5671);
                                        // rustls has verified any certificate the client sent.
                                        let peer_cn = tls_stream
                                            .get_ref()
                                            .1
                                            .peer_certificates()
                                            .and_then(|c| c.first())
                                            .and_then(|c| crate::tls::common_name(c.as_ref()));
                                        let tls_stream = Prefixed { head, at: 0, len: n, inner: tls_stream };
                                        if n == 8 && (head == *b"AMQP\x00\x01\x00\x00" || head == *b"AMQP\x03\x01\x00\x00") {
                                            let port = queueforge_mgmt::bridge::loopback_port().unwrap_or(local_port);
                                            queueforge_mgmt::bridge::amqp10::serve(tls_stream, port).await;
                                            drop(guard);
                                            drop(permit);
                                            return;
                                        }
                                        crate::connection::PEER_CN.scope(peer_cn, handle_connection(
                                            tls_stream,
                                            peer,
                                            store,
                                            queues,
                                            router,
                                            connections,
                                            params,
                                            guard,
                                            shutdown,
                                            cluster,
                                        ))
                                        .await;
                                    }
                                    Err(e) => {
                                        warn!(%peer, error = %e, "TLS handshake failed");
                                    }
                                },
                                None => {
                                    let mut peeked = [0u8; 8];
                                    let amqp10 = stream.peek(&mut peeked).await.ok() == Some(8)
                                        && (peeked == *b"AMQP\x00\x01\x00\x00" || peeked == *b"AMQP\x03\x01\x00\x00");
                                    if amqp10 {
                                        // AMQP 1.0 is a client of this listener, as RabbitMQ's 1.0 sessions sit on its core.
                                        let port = stream.local_addr().map(|a| a.port()).unwrap_or(5672);
                                        queueforge_mgmt::bridge::amqp10::serve(stream, port).await;
                                        drop(guard);
                                        drop(permit);
                                        return;
                                    }
                                    handle_connection(
                                        stream,
                                        peer,
                                        store,
                                        queues,
                                        router,
                                        connections,
                                        params,
                                        guard,
                                        shutdown,
                                        cluster,
                                    )
                                    .await;
                                }
                            }
                            drop(permit);
                        });
                    }
                    Err(e) => {
                        error!(error = %e, "AMQP accept error");
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    }
                }
            }
        }
    }
}

/// Read up to `head.len()` bytes; fewer means the peer closed first.
async fn read_header<S: tokio::io::AsyncRead + Unpin>(io: &mut S, head: &mut [u8; 8]) -> usize {
    use tokio::io::AsyncReadExt;
    let mut n = 0;
    while n < head.len() {
        match io.read(&mut head[n..]).await {
            Ok(0) | Err(_) => break,
            Ok(got) => n += got,
        }
    }
    n
}

/// A stream whose first bytes were already read; they are handed out again first.
struct Prefixed<S> {
    head: [u8; 8],
    at: usize,
    len: usize,
    inner: S,
}

impl<S: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for Prefixed<S> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        if self.at < self.len {
            let n = (self.len - self.at).min(buf.remaining());
            let (at, end) = (self.at, self.at + n);
            buf.put_slice(&self.head[at..end]);
            self.at = end;
            return std::task::Poll::Ready(Ok(()));
        }
        std::pin::Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<S: tokio::io::AsyncWrite + Unpin> tokio::io::AsyncWrite for Prefixed<S> {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
    }

    fn poll_write_vectored(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.inner).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
}
