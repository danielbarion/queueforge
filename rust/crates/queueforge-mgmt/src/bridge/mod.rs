//! MQTT, STOMP and AMQP 1.0, served over a loopback AMQP connection the way RabbitMQ's
//! plugins are. The protocol code is generic over any byte stream, so the
//! broker's TCP listeners and the management port's `/ws` share it.

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::{IntoResponse, Response};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::state::MgmtState;

pub mod amqp10;
pub mod mqtt;
pub mod stomp;
pub mod stream;

static LOOPBACK_PORT: std::sync::OnceLock<u16> = std::sync::OnceLock::new();

/// Record the plain AMQP port bridges log in through. The first call wins.
pub fn set_loopback_port(port: u16) {
    let _ = LOOPBACK_PORT.set(port);
}

/// The plain AMQP port bridges log in through, once a plain listener is up.
pub fn loopback_port() -> Option<u16> {
    LOOPBACK_PORT.get().copied()
}

/// The AMQP URI a bridge session logs in with: the local listener and the
/// client's own credentials, so permissions are checked as for AMQP.
pub(crate) fn amqp_uri(port: u16, user: &str, password: &str, vhost: &str) -> String {
    format!("amqp://{}:{}@127.0.0.1:{port}/{}", encode(user), encode(password), encode(vhost))
}

fn encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(char::from(b));
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// A random hex token for client ids, queue names and consumer tags.
pub(crate) fn token() -> String {
    use std::hash::{BuildHasher, Hasher};
    let mut out = String::with_capacity(32);
    for _ in 0..2 {
        let mut h = std::collections::hash_map::RandomState::new().build_hasher();
        h.write_u128(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0),
        );
        out.push_str(&format!("{:016x}", h.finish()));
    }
    out
}

/// GET /ws: MQTT or STOMP over WebSocket, picked by the subprotocol the client
/// offers ("mqtt", or "v12.stomp" and its older forms), as RabbitMQ's
/// web_mqtt and web_stomp plugins accept.
pub async fn ws_handler(State(state): State<MgmtState>, ws: WebSocketUpgrade) -> Response {
    let port = loopback_port().or_else(|| state.config.amqp_listeners.first().map(|(_, p)| *p)).unwrap_or(5672);
    let ws = ws.protocols(["mqtt", "mqttv3.1", "v12.stomp", "v11.stomp", "v10.stomp", "stomp"]);
    let protocol = ws
        .selected_protocol()
        .and_then(|p| p.to_str().ok())
        .map(str::to_string)
        .unwrap_or_default();
    let mqtt = protocol.starts_with("mqtt");
    if !mqtt && !protocol.contains("stomp") {
        return (axum::http::StatusCode::BAD_REQUEST, "offer the mqtt or v12.stomp subprotocol").into_response();
    }
    ws.on_upgrade(move |socket| pump(socket, mqtt, port))
}

/// Carry WebSocket messages to and from a protocol session through an
/// in-memory byte pipe.
async fn pump(mut socket: WebSocket, mqtt: bool, port: u16) {
    let (mut near, far) = tokio::io::duplex(256 * 1024);
    let session = tokio::spawn(async move {
        if mqtt {
            mqtt::serve(far, port).await;
        } else {
            stomp::serve(far, port).await;
        }
    });
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        tokio::select! {
            incoming = socket.recv() => match incoming {
                Some(Ok(Message::Binary(b))) => {
                    if near.write_all(&b).await.is_err() {
                        break;
                    }
                }
                Some(Ok(Message::Text(t))) => {
                    if near.write_all(t.as_bytes()).await.is_err() {
                        break;
                    }
                }
                Some(Ok(Message::Ping(_) | Message::Pong(_))) => {}
                _ => break,
            },
            n = near.read(&mut buf) => match n {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    // STOMP frames go out as text, as RabbitMQ sends them.
                    let msg = if mqtt {
                        Message::Binary(buf[..n].to_vec().into())
                    } else {
                        Message::Text(String::from_utf8_lossy(&buf[..n]).into_owned().into())
                    };
                    if socket.send(msg).await.is_err() {
                        break;
                    }
                }
            },
        }
    }
    drop(near);
    let _ = socket.send(Message::Close(None)).await;
    let _ = session.await;
}
