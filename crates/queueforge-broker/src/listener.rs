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
    start_amqp_listener_with_limits(addr, store, queues, router, connections, params, None, None)
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
) -> std::io::Result<AmqpListener> {
    let listener = TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;
    let tls_enabled = tls.is_some();
    info!(%local_addr, tls = tls_enabled, "AMQP listening");

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
                        let acceptor = acceptor.clone();
                        tokio::spawn(async move {
                            match acceptor {
                                Some(acceptor) => match acceptor.accept(stream).await {
                                    Ok(tls_stream) => {
                                        handle_connection(
                                            tls_stream,
                                            peer,
                                            store,
                                            queues,
                                            router,
                                            connections,
                                            params,
                                            guard,
                                            shutdown,
                                        )
                                        .await;
                                    }
                                    Err(e) => {
                                        warn!(%peer, error = %e, "TLS handshake failed");
                                    }
                                },
                                None => {
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
