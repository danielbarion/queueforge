//! Cluster membership: replicate topology and forward queue ops to the home node.
//!
//! A config with no members does not start this service. Each queue's home is a
//! stable hash of `(vhost, name)` over the sorted member ids, unless the queue
//! is exclusive (always local to the connection's node).
//! Startup, quorum, peer IO, snapshots, and proxy forwarding live in sibling modules.

use std::collections::{HashMap, HashSet};
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
use tokio::sync::{mpsc, oneshot, Mutex, Notify};

const BASE64: base64::engine::general_purpose::GeneralPurpose =
    base64::engine::general_purpose::STANDARD;

/// Choose the home node for a non-exclusive queue.
///
/// `members` is the current membership and must be non-empty; an empty slice panics on the modulo.
/// `vhost` and `queue` are the queue key hashed as bytes with a `0xff` separator.
/// Returns the home member id borrowed from `members` after sorting ids.
/// Exclusive queues stay on the connection's node and do not call this.
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
    members: std::sync::Mutex<Vec<ClusterMember>>,
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
    ///
    /// A set, not a scanned list. Every quorum ack and follower drop records an id,
    /// and a linear scan of that list sat on the confirm connection.
    consumed: Mutex<HashSet<ConsumedId>>,
    /// Quorum append RPCs still waiting on each peer.
    ///
    /// A peer the majority does not need is skipped once this climbs past
    /// [`quorum::EXTRA_APPEND_CAP`]. The confirm does not wait on that cap.
    append_inflight: std::sync::Mutex<HashMap<String, usize>>,
    /// Member ids whose hello consumed-set has been applied on this node.
    heard: std::sync::Mutex<HashSet<String>>,
    /// Refused dials since the last successful connect, per member id.
    dial_fails: std::sync::Mutex<HashMap<String, u8>>,
    /// Members that refused [`UNREACHABLE_STRIKES`] connects. They do not block catchup.
    down: std::sync::Mutex<HashSet<String>>,
    /// Wakes [`Cluster::wait_quorum_catchup`] after `heard` or `down` gains a member.
    catchup: Notify,
}

/// Refused connects before a member is treated as down.
///
/// One refusal is a peer that is still binding. Five refusals, with a success
/// clearing the count, is a member that is not listening. A peer that accepts
/// a TCP connection stays required until its hello consumed-set is applied.
pub(super) const UNREACHABLE_STRIKES: u8 = 5;

/// True when every member other than `node_id` has applied its consumed set or refused the dial.
pub(super) fn catchup_satisfied(
    node_id: &str,
    member_ids: &[String],
    heard: &HashSet<String>,
    down: &HashSet<String>,
) -> bool {
    member_ids
        .iter()
        .all(|id| id == node_id || heard.contains(id) || down.contains(id))
}

/// Count one refused dial. The peer joins `down` on the fifth refusal.
pub(super) fn record_unreachable(
    fails: &mut HashMap<String, u8>,
    down: &mut HashSet<String>,
    id: &str,
) -> bool {
    if id.is_empty() || down.contains(id) {
        return false;
    }
    let n = fails.entry(id.to_string()).or_insert(0);
    *n = n.saturating_add(1);
    if *n >= UNREACHABLE_STRIKES {
        down.insert(id.to_string());
        true
    } else {
        false
    }
}

/// A refused dial during catchup. A member already heard is left alone.
pub(super) fn record_dial_failure(inner: &Inner, id: &str) {
    if id.is_empty() || id == inner.node_id {
        return;
    }
    {
        let heard = inner.heard.lock().unwrap_or_else(|err| err.into_inner());
        if heard.contains(id) {
            return;
        }
    }
    let mut fails = inner
        .dial_fails
        .lock()
        .unwrap_or_else(|err| err.into_inner());
    let mut down = inner.down.lock().unwrap_or_else(|err| err.into_inner());
    if record_unreachable(&mut fails, &mut down, id) {
        inner.catchup.notify_one();
    }
}

/// A connect succeeded, so earlier refusals were the peer still starting.
pub(super) fn clear_dial_failure(inner: &Inner, id: &str) {
    if id.is_empty() {
        return;
    }
    let mut fails = inner
        .dial_fails
        .lock()
        .unwrap_or_else(|err| err.into_inner());
    fails.remove(id);
}

/// One quorum id this node has already handed to a consumer.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
struct ConsumedId {
    vhost: String,
    queue: String,
    id: String,
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
    #[serde(default)]
    exchange: String,
    #[serde(default)]
    routing_key: String,
    #[serde(default)]
    body_b64: String,
    #[serde(default)]
    persistent: bool,
    #[serde(default)]
    redelivered: bool,
    #[serde(default)]
    content_type: Option<String>,
    #[serde(default)]
    content_encoding: Option<String>,
    #[serde(default)]
    correlation_id: Option<String>,
    #[serde(default)]
    message_id: Option<String>,
    #[serde(default)]
    reply_to: Option<String>,
    #[serde(default)]
    expiration: Option<String>,
    #[serde(default)]
    app_id: Option<String>,
    #[serde(default)]
    user_id: Option<String>,
    #[serde(default)]
    type_: Option<String>,
    #[serde(default)]
    priority: Option<u8>,
    #[serde(default)]
    timestamp: Option<u64>,
    #[serde(default)]
    expires_unix_ms: Option<u64>,
    #[serde(default)]
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

impl Inner {
    /// Copy the current member list. Join and forget replace the list behind the mutex.
    pub(super) fn member_list(&self) -> Vec<ClusterMember> {
        self.members
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .clone()
    }
}

mod dispatch;
mod forward;
mod ingress;
mod lifecycle;
mod membership;
mod net;
mod proxy;
mod quorum;
mod state;
mod subscribe;
mod wire;

pub use wire::{decode_quorum_append, encode_quorum_append};

#[cfg(test)]
mod tests;
