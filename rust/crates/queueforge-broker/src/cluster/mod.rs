//! Static-membership cluster: replicate topology and forward queue ops to the home node.
//!
//! A config with no members does not start this service. Each queue's home is a
//! stable hash of `(vhost, name)` over the sorted member ids, unless the queue
//! is exclusive (always local to the connection's node).
//! Startup, quorum, peer IO, snapshots, and proxy forwarding live in sibling modules.

use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use base64::Engine;
use bytes::Bytes;
use compact_str::CompactString;
use queueforge_core::{
    Binding, ClusterMember, ConsumerSessionId, Error, Exchange, ExchangeRouter, Message,
    Permission, Queue, QueueKey, QueueRegistry, User, Vhost,
};
use queueforge_store::MetadataStore;

use crate::quorum_confirm::{durable_majority, MemberCopy};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{mpsc, oneshot, Mutex};

const BASE64: base64::engine::general_purpose::GeneralPurpose =
    base64::engine::general_purpose::STANDARD;

/// Choose the home node for a non-exclusive queue. `members` must be non-empty.
pub fn queue_home<'a>(members: &'a [ClusterMember], vhost: &str, queue: &str) -> &'a str {
    let mut ordered: Vec<&ClusterMember> = members.iter().collect();
    ordered.sort_by(|a, b| a.id.cmp(&b.id));
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in vhost
        .bytes()
        .chain(std::iter::once(0xff))
        .chain(queue.bytes())
    {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    ordered[(hash as usize) % ordered.len()].id.as_str()
}

/// Running cluster endpoint. Absent on a single-node broker.
pub struct Cluster {
    inner: Arc<Inner>,
}

struct Inner {
    node_id: String,
    members: Vec<ClusterMember>,
    store: Arc<MetadataStore>,
    queues: Arc<QueueRegistry>,
    router: Arc<ExchangeRouter>,
    peers: Mutex<HashMap<String, Arc<Peer>>>,
    next_id: AtomicU64,
    /// Lowest reachable member id once a majority is visible. Empty until then.
    leader: std::sync::Mutex<String>,
    /// Quorum bodies on a follower. `basic.get` and `basic.consume` do not pop this.
    replicas: Mutex<HashMap<String, Arc<Message>>>,
    /// Quorum message ids a member has dropped. Sent on hello so a restarted peer does not deliver them again.
    consumed: Mutex<Vec<Value>>,
}

struct Peer {
    tx: mpsc::Sender<String>,
    pending: Arc<Mutex<HashMap<u64, oneshot::Sender<Msg>>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Msg {
    id: u64,
    op: String,
    #[serde(default)]
    ok: bool,
    #[serde(default)]
    error: String,
    #[serde(default)]
    payload: Value,
    /// Protocol version. `1` is the shared Rust/Bun quorum body.
    #[serde(default)]
    v: u32,
    #[serde(default, rename = "nodeId")]
    node_id: String,
    #[serde(default)]
    from: String,
    /// Top-level apply kind. Bun sends this beside `payload`; Rust nests it.
    #[serde(default)]
    kind: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct Snapshot {
    users: Vec<User>,
    vhosts: Vec<Vhost>,
    permissions: Vec<Permission>,
    exchanges: Vec<Exchange>,
    queues: Vec<Queue>,
    bindings: Vec<Binding>,
}

#[derive(Debug, Serialize, Deserialize)]
struct WireMessage {
    exchange: String,
    routing_key: String,
    body_b64: String,
    persistent: bool,
    redelivered: bool,
    content_type: Option<String>,
    content_encoding: Option<String>,
    correlation_id: Option<String>,
    message_id: Option<String>,
    reply_to: Option<String>,
    expiration: Option<String>,
    app_id: Option<String>,
    user_id: Option<String>,
    type_: Option<String>,
    priority: Option<u8>,
    timestamp: Option<u64>,
    expires_unix_ms: Option<u64>,
    headers: queueforge_core::MessageHeaders,
}

struct SubOpen {
    home: String,
    queue: Queue,
    session: ConsumerSessionId,
    no_ack: bool,
    exclusive: bool,
    priority: i32,
    initial_credit: Option<u32>,
    deliver_tx: mpsc::Sender<queueforge_core::QueueDelivery>,
}

mod dispatch;
mod forward;
mod lifecycle;
mod net;
mod proxy;
mod quorum;
mod state;
mod wire;

pub use wire::{decode_quorum_append, encode_quorum_append};

#[cfg(test)]
mod tests;
