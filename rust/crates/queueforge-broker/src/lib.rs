//! QueueForge broker library.
//!
//! Provides the AMQP TCP listener and per-connection handshake / channel state
//! machine used by the `queueforge` binary and integration tests.

#![deny(missing_docs)]

/// Static cluster membership, topology replication, and queue forwarding.
pub mod cluster;
/// Per-connection AMQP 0-9-1 state machine.
pub mod connection;
/// Connection and resource limit helpers.
pub mod limits;
/// AMQP TCP accept loop.
pub mod listener;
/// MQTT, STOMP, AMQP 1.0, and stream listeners.
pub mod protocols;
/// Quorum confirm rule: a majority must hold the body in durable storage.
pub mod quorum_confirm;
/// Graceful drain: connection tracker and shutdown signal.
pub mod shutdown;
/// rustls server config for AMQPS / HTTPS.
pub mod tls;

pub use cluster::{queue_home, Cluster};
pub use connection::{
    handle_connection, ConnectionParams, DEFAULT_CHANNEL_MAX, DEFAULT_FRAME_MAX, DEFAULT_HEARTBEAT,
    DEFAULT_MAX_MESSAGE_BYTES, FRAME_MAX_FLOOR,
};
pub use limits::{ConnectionLimiter, ConnectionPermit};
pub use listener::{
    start_amqp_listener, start_amqp_listener_with_limits, AmqpListener, AmqpListenerHandle,
};
pub use shutdown::{ConnectionGuard, ConnectionTracker, CONNECTION_DRAIN_TIMEOUT};
pub use tls::{load_server_config, with_https_alpn, TlsError};
