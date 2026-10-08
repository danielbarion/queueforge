//! MQTT, STOMP and RabbitMQ stream listeners.
//!
//! Each session is an AMQP client of this broker over loopback, as
//! RabbitMQ's plugins are; the protocol code lives in
//! `queueforge_mgmt::bridge`. AMQP 1.0 arrives on the AMQP port instead.

use std::sync::Arc;

use tokio::net::TcpListener;
use tracing::warn;

/// Bind `addr` and serve MQTT 3.1 to 5.0 until the task is dropped. Each
/// session is an AMQP client of this broker, as in RabbitMQ.
pub fn spawn_mqtt(addr: std::net::SocketAddr, amqp_port: u16) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let Ok(listener) = TcpListener::bind(addr).await else {
            warn!(%addr, "mqtt bind failed");
            return;
        };
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                break;
            };
            let _ = socket.set_nodelay(true);
            tokio::spawn(queueforge_mgmt::bridge::mqtt::serve(socket, amqp_port));
        }
    })
}

/// Bind `addr` and serve STOMP 1.0 to 1.2 until the task is dropped. Each
/// session is an AMQP client of this broker, as in RabbitMQ.
pub fn spawn_stomp(addr: std::net::SocketAddr, amqp_port: u16) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let Ok(listener) = TcpListener::bind(addr).await else {
            warn!(%addr, "stomp bind failed");
            return;
        };
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                break;
            };
            let _ = socket.set_nodelay(true);
            tokio::spawn(queueforge_mgmt::bridge::stomp::serve(socket, amqp_port));
        }
    })
}

/// Bind `addr` and serve the RabbitMQ stream protocol until the task is
/// dropped. Each session is an AMQP client of this broker's stream queues.
pub fn spawn_stream(addr: std::net::SocketAddr, ctx: Arc<queueforge_mgmt::bridge::stream::StreamContext>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let Ok(listener) = TcpListener::bind(addr).await else {
            warn!(%addr, "stream bind failed");
            return;
        };
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                break;
            };
            let _ = socket.set_nodelay(true);
            tokio::spawn(queueforge_mgmt::bridge::stream::serve(socket, Arc::clone(&ctx)));
        }
    })
}
