//! MQTT, STOMP and RabbitMQ stream listeners, plain or TLS.
//!
//! Each session is an AMQP client of this broker over loopback, as
//! RabbitMQ's plugins are; the protocol code lives in
//! `queueforge_mgmt::bridge`. AMQP 1.0 arrives on the AMQP port instead.
//! A TLS listener (RabbitMQ's `mqtt.listeners.ssl`, `stomp.listeners.ssl`
//! and `stream.listeners.ssl`) does the handshake, then serves the same
//! protocol on the decrypted stream.

use std::future::Future;
use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tracing::{debug, warn};

/// Accept on `addr` until the task is dropped and hand each connection,
/// after the TLS handshake when `tls` is set, to `serve`.
fn spawn_listener<F, Fut>(
    addr: std::net::SocketAddr,
    name: &'static str,
    tls: Option<Arc<rustls::ServerConfig>>,
    serve: F,
) -> tokio::task::JoinHandle<()>
where
    F: Fn(Box<dyn Io>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let serve = Arc::new(serve);
    let acceptor = tls.map(TlsAcceptor::from);
    tokio::spawn(async move {
        let Ok(listener) = TcpListener::bind(addr).await else {
            warn!(%addr, "{name} bind failed");
            return;
        };
        loop {
            let Ok((socket, peer)) = listener.accept().await else {
                break;
            };
            let _ = socket.set_nodelay(true);
            let serve = Arc::clone(&serve);
            match acceptor.clone() {
                None => {
                    tokio::spawn(serve(Box::new(socket)));
                }
                Some(acceptor) => {
                    tokio::spawn(async move {
                        match acceptor.accept(socket).await {
                            Ok(stream) => serve(Box::new(stream)).await,
                            Err(err) => debug!(%peer, error = %err, "{name} TLS handshake failed"),
                        }
                    });
                }
            }
        }
    })
}

/// A client stream, plain or TLS.
pub trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

/// Bind `addr` and serve MQTT 3.1 to 5.0 until the task is dropped. Each
/// session is an AMQP client of this broker, as in RabbitMQ.
pub fn spawn_mqtt(addr: std::net::SocketAddr, amqp_port: u16, tls: Option<Arc<rustls::ServerConfig>>) -> tokio::task::JoinHandle<()> {
    spawn_listener(addr, "mqtt", tls, move |io| queueforge_mgmt::bridge::mqtt::serve(io, amqp_port))
}

/// Bind `addr` and serve STOMP 1.0 to 1.2 until the task is dropped. Each
/// session is an AMQP client of this broker, as in RabbitMQ.
pub fn spawn_stomp(addr: std::net::SocketAddr, amqp_port: u16, tls: Option<Arc<rustls::ServerConfig>>) -> tokio::task::JoinHandle<()> {
    spawn_listener(addr, "stomp", tls, move |io| queueforge_mgmt::bridge::stomp::serve(io, amqp_port))
}

/// Bind `addr` and serve the RabbitMQ stream protocol until the task is
/// dropped. Each session is an AMQP client of this broker's stream queues.
pub fn spawn_stream(
    addr: std::net::SocketAddr,
    ctx: Arc<queueforge_mgmt::bridge::stream::StreamContext>,
    tls: Option<Arc<rustls::ServerConfig>>,
) -> tokio::task::JoinHandle<()> {
    spawn_listener(addr, "stream", tls, move |io| queueforge_mgmt::bridge::stream::serve(io, Arc::clone(&ctx)))
}
