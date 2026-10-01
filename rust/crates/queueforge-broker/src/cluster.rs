//! Static-membership cluster: replicate topology and forward queue ops to the home node.
//!
//! A config with no members does not start this service. Each queue's home is a
//! stable hash of `(vhost, name)` over the sorted member ids, unless the queue
//! is exclusive (always local to the connection's node).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use bytes::Bytes;
use compact_str::CompactString;
use queueforge_core::{
    Binding, ClusterMember, ConsumerDeliveryId, ConsumerSessionId, EnqueueCompletion, Error,
    Exchange, ExchangeRouter, Message, Permission, Policy, Queue, QueueCmd, QueueDeclareOpts, QueueHandle,
    QueueKey, QueueOffset, QueueRegistry, User, Vhost,
};
use queueforge_store::MetadataStore;

use crate::quorum_confirm::{durable_majority, MemberCopy};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, Mutex};
use tracing::{debug, info, warn};

const BASE64: base64::engine::general_purpose::GeneralPurpose = base64::engine::general_purpose::STANDARD;

/// Choose the home node for a non-exclusive queue. `members` must be non-empty.
pub fn queue_home<'a>(members: &'a [ClusterMember], vhost: &str, queue: &str) -> &'a str {
    let mut ordered: Vec<&ClusterMember> = members.iter().collect();
    ordered.sort_by(|a, b| a.id.cmp(&b.id));
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in vhost.bytes().chain(std::iter::once(0xff)).chain(queue.bytes()) {
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

impl Cluster {
    /// Bind the RPC listener and connect to peers in the background.
    pub async fn start(
        node_id: String,
        members: Vec<ClusterMember>,
        listen: std::net::SocketAddr,
        store: Arc<MetadataStore>,
        queues: Arc<QueueRegistry>,
        router: Arc<ExchangeRouter>,
    ) -> std::io::Result<Arc<Self>> {
        let mut ordered: Vec<&ClusterMember> = members.iter().collect();
        ordered.sort_by(|a, b| a.id.cmp(&b.id));
        let slot = ordered
            .iter()
            .position(|member| member.id == node_id)
            .map(|index| index as u64 + 1)
            .unwrap_or(1);
        crate::connection::install_session_namespace(slot);
        let listener = TcpListener::bind(listen).await?;
        info!(%listen, node_id, members = members.len(), "cluster listening");
        let inner = Arc::new(Inner {
            node_id,
            members,
            store,
            queues,
            router,
            peers: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
            leader: std::sync::Mutex::new(String::new()),
            replicas: Mutex::new(HashMap::new()),
            consumed: Mutex::new(Vec::new()),
        });
        let cluster = Arc::new(Self { inner: Arc::clone(&inner) });
        let accept_inner = Arc::clone(&inner);
        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, peer)) => {
                        let inner = Arc::clone(&accept_inner);
                        tokio::spawn(async move {
                            if let Err(err) = serve_conn(inner, stream).await {
                                debug!(%peer, error = %err, "cluster connection closed");
                            }
                        });
                    }
                    Err(err) => {
                        warn!(error = %err, "cluster accept failed");
                        break;
                    }
                }
            }
        });
        let dial = Arc::clone(&cluster);
        tokio::spawn(async move {
            loop {
                dial.connect_peers().await;
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        });
        Ok(cluster)
    }

    /// Home id for this queue.
    pub fn home_of(&self, vhost: &str, queue: &str, exclusive: bool) -> String {
        if exclusive {
            return self.inner.node_id.clone();
        }
        queue_home(&self.inner.members, vhost, queue).to_string()
    }

    /// Whether the queue actor should live in this process.
    pub fn is_local(&self, vhost: &str, queue: &str, exclusive: bool) -> bool {
        self.home_of(vhost, queue, exclusive) == self.inner.node_id
    }

    /// Declare on the home node and make sure this process has a usable handle.
    pub async fn declare_queue(
        &self,
        vhost: &str,
        name: &str,
        mut opts: QueueDeclareOpts,
    ) -> Result<queueforge_core::DeclareResult, Error> {
        let home = self.home_of(vhost, name, opts.exclusive);
        opts.home = Some(CompactString::from(home.as_str()));
        let quorum = opts.args.queue_type == Some(queueforge_core::QueueType::Quorum);
        if quorum {
            opts.home = Some(CompactString::from(self.inner.node_id.as_str()));
        }
        if quorum || home == self.inner.node_id {
            let result = self.inner.queues.declare(vhost, name, opts).await?;
            let queue = self.queue_row(&result.handle);
            self.broadcast_apply("queue", serde_json::to_value(&queue).unwrap_or(Value::Null))
                .await;
            return Ok(result);
        }
        let payload = serde_json::json!({
            "vhost": vhost,
            "queue": name,
            "durable": opts.durable,
            "exclusive": opts.exclusive,
            "auto_delete": opts.auto_delete,
            "passive": opts.passive,
            "args": opts.args,
            "home": home,
        });
        let reply = self.call(&home, "declare", payload).await?;
        if !reply.ok {
            return Err(Error::Unavailable(if reply.error.is_empty() {
                format!("queue {vhost}/{name} home {home} rejected declare")
            } else {
                reply.error
            }));
        }
        let queue: Queue = serde_json::from_value(reply.payload["queue"].clone())
            .map_err(|err| Error::Unavailable(format!("bad declare reply: {err}")))?;
        if queue.durable {
            let _ = MetadataStore::blocking(Arc::clone(&self.inner.store), {
                let queue = queue.clone();
                move |store| {
                    if store.get_queue(queue.vhost.as_str(), queue.name.as_str())?.is_none() {
                        store.create_queue(&queue)?;
                    }
                    Ok(())
                }
            })
            .await;
        }
        let handle = self.proxy_for(&queue).await;
        let stats = self.call(&home, "stats", serde_json::json!({"vhost": vhost, "queue": name})).await;
        let (message_count, consumer_count) = match stats {
            Ok(msg) if msg.ok => (
                msg.payload["messages_ready"].as_u64().unwrap_or(0) as u32,
                msg.payload["consumer_count"].as_u64().unwrap_or(0) as u32,
            ),
            _ => (0, 0),
        };
        Ok(queueforge_core::DeclareResult {
            handle,
            message_count,
            consumer_count,
        })
    }

    /// Local handle, installing a proxy when the queue is homed on a peer.
    pub async fn ensure_proxy(&self, key: &QueueKey) -> Option<QueueHandle> {
        if let Some(handle) = self.inner.queues.get(key) {
            return Some(handle);
        }
        let queue = MetadataStore::blocking(Arc::clone(&self.inner.store), {
            let vhost = key.vhost.to_string();
            let name = key.name.to_string();
            move |store| store.get_queue(&vhost, &name)
        })
        .await
        .ok()
        .flatten()?;
        let home = queue.home.as_deref().unwrap_or(self.inner.node_id.as_str());
        if home == self.inner.node_id {
            return self.inner.queues.get(key);
        }
        Some(self.proxy_for(&queue).await)
    }

    /// Delete on the home. A down home returns [`Error::Unavailable`] and does not drop the row.
    pub async fn delete_queue(
        &self,
        key: &QueueKey,
        if_unused: bool,
        if_empty: bool,
    ) -> Result<u32, Error> {
        let home = self
            .stored_home(key)
            .await
            .unwrap_or_else(|| self.home_of(key.vhost.as_str(), key.name.as_str(), false));
        if home == self.inner.node_id {
            let count = self.inner.queues.delete(key, if_unused, if_empty).await?;
            self.broadcast_apply(
                "delete_queue",
                serde_json::json!({"vhost": key.vhost.as_str(), "queue": key.name.as_str()}),
            )
            .await;
            return Ok(count);
        }
        let reply = self
            .call(
                &home,
                "delete",
                serde_json::json!({
                    "vhost": key.vhost.as_str(),
                    "queue": key.name.as_str(),
                    "if_unused": if_unused,
                    "if_empty": if_empty,
                }),
            )
            .await?;
        if !reply.ok {
            return Err(Error::Unavailable(if reply.error.is_empty() {
                format!("queue {key} home {home} is unavailable")
            } else {
                reply.error
            }));
        }
        let _ = MetadataStore::blocking(Arc::clone(&self.inner.store), {
            let vhost = key.vhost.to_string();
            let name = key.name.to_string();
            move |store| store.delete_queue(&vhost, &name)
        })
        .await;
        Ok(reply.payload["message_count"].as_u64().unwrap_or(0) as u32)
    }

    /// Push an exchange or binding so peers can route without declaring again.
    pub async fn replicate_json(&self, kind: &str, payload: Value) {
        self.broadcast_apply(kind, payload).await;
    }

    fn queue_row(&self, handle: &QueueHandle) -> Queue {
        let mut queue = handle.info.to_domain();
        queue.home = Some(CompactString::from(self.inner.node_id.as_str()));
        queue
    }

    async fn stored_home(&self, key: &QueueKey) -> Option<String> {
        MetadataStore::blocking(Arc::clone(&self.inner.store), {
            let vhost = key.vhost.to_string();
            let name = key.name.to_string();
            move |store| store.get_queue(&vhost, &name)
        })
        .await
        .ok()
        .flatten()
        .and_then(|queue| queue.home.map(|home| home.to_string()))
    }

    async fn proxy_for(&self, queue: &Queue) -> QueueHandle {
        let key = QueueKey::new(queue.vhost.as_str(), queue.name.as_str());
        if let Some(handle) = self.inner.queues.get(&key) {
            return handle;
        }
        let opts = QueueDeclareOpts {
            durable: queue.durable,
            exclusive: queue.exclusive,
            auto_delete: queue.auto_delete,
            passive: false,
            exclusive_owner: None,
            args: queue.args.clone(),
            declared_args: None,
            home: queue.home.clone(),
        };
        let Some((handle, rx)) = self.inner.queues.begin_proxy(key.clone(), opts, 256) else {
            return self.inner.queues.get(&key).expect("proxy handle");
        };
        let cluster = self.clone_inner();
        let queue = queue.clone();
        tokio::spawn(async move {
            proxy_loop(cluster, queue, rx).await;
        });
        handle
    }

    fn clone_inner(&self) -> Arc<Inner> {
        Arc::clone(&self.inner)
    }

    async fn connect_peers(&self) {
        let stale: Vec<String> = {
            let peers = self.inner.peers.lock().await;
            peers
                .iter()
                .filter(|(_, peer)| peer.tx.is_closed())
                .map(|(id, _)| id.clone())
                .collect()
        };
        if !stale.is_empty() {
            let mut peers = self.inner.peers.lock().await;
            for id in stale {
                peers.remove(&id);
            }
            drop(peers);
            refresh_leader(&self.inner).await;
        }
        for member in &self.inner.members {
            if member.id == self.inner.node_id || member.id.as_str() < self.inner.node_id.as_str() {
                continue;
            }
            let have = self.inner.peers.lock().await.contains_key(&member.id);
            if have {
                continue;
            }
            let inner = Arc::clone(&self.inner);
            let id = member.id.clone();
            let addr = member.addr;
            tokio::spawn(async move {
                match TcpStream::connect(addr).await {
                    Ok(stream) => {
                        if let Err(err) = attach_peer(inner, id.clone(), addr, stream, true).await {
                            debug!(peer = %id, error = %err, "cluster dial failed");
                        }
                    }
                    Err(err) => {
                        debug!(peer = %id, error = %err, "cluster peer unreachable");
                    }
                }
            });
        }
        let connected = self.inner.peers.lock().await.len();
        let expected = self
            .inner
            .members
            .iter()
            .filter(|member| member.id != self.inner.node_id)
            .count();
        queueforge_core::prom::unreachable_peers(expected.saturating_sub(connected) as u64);
    }

    async fn call(&self, node: &str, op: &str, payload: Value) -> Result<Msg, Error> {
        let peer = self
            .inner
            .peers
            .lock()
            .await
            .get(node)
            .cloned()
            .ok_or_else(|| Error::Unavailable(format!("cluster peer {node} is down")))?;
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        peer.pending.lock().await.insert(id, tx);
        let line = serde_json::to_string(&Msg {
            id,
            op: op.to_string(),
            ok: false,
            error: String::new(),
            payload,
            v: 1,
            node_id: self.inner.node_id.clone(),
            from: self.inner.node_id.clone(),
            kind: String::new(),
        })
        .map_err(|err| Error::Unavailable(err.to_string()))?;
        if peer.tx.send(line).await.is_err() {
            peer.pending.lock().await.remove(&id);
            self.inner.peers.lock().await.remove(node);
            return Err(Error::Unavailable(format!("cluster peer {node} is down")));
        }
        match tokio::time::timeout(Duration::from_secs(5), rx).await {
            Ok(Ok(msg)) => Ok(msg),
            _ => {
                peer.pending.lock().await.remove(&id);
                self.inner.peers.lock().await.remove(node);
                Err(Error::Unavailable(format!("cluster peer {node} timed out")))
            }
        }
    }

    /// Store one quorum message on this node and every reachable peer.
    ///
    /// The confirm is refused when fewer than a majority of configured members are reachable.
    pub async fn quorum_enqueue(&self, key: &QueueKey, message: Arc<Message>) -> Result<(), Error> {
        let peers = self.live_peers().await;
        let members = self.inner.members.len().max(1);
        let majority = members / 2 + 1;
        if peers.len() + 1 < majority {
            return Err(Error::Unavailable("quorum has no majority".into()));
        }
        let mut owned = (*message).clone();
        if owned.message_id.is_none() {
            let n = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
            owned.message_id = Some(CompactString::from(format!("q{n}-{}", self.inner.node_id)));
        }
        let message = Arc::new(owned);
        let message_id = message.message_id.clone().unwrap_or_default();
        // Peers store the body before this node can deliver it. A delivery claims
        // those copies, and that claim must not race ahead of the replicate.
        let mut stored_on = Vec::new();
        let mut copies = Vec::new();
        for peer in &peers {
            let reply = self.call(peer, "quorum_append", encode_quorum_append(key, &message)).await;
            if peer_append_durable(&reply) {
                stored_on.push(peer.clone());
                copies.push(MemberCopy::Durable);
            } else {
                copies.push(MemberCopy::MemoryOnly);
            }
        }
        let local_offset = match local_enqueue(&self.inner, key, Arc::clone(&message)).await {
            Ok(offset) => offset,
            Err(_) => {
                copies.push(MemberCopy::MemoryOnly);
                for peer in &stored_on {
                    let _ = self.call(peer, "forget", wire_forget(key, message_id.as_str())).await;
                }
                let _ = local_forget(&self.inner, key, message_id.as_str()).await;
                return Err(Error::Unavailable("quorum has no majority".into()));
            }
        };
        if flush_queue(&self.inner, key, local_offset).await.is_err() {
            copies.push(MemberCopy::MemoryOnly);
            for peer in &stored_on {
                let _ = self.call(peer, "forget", wire_forget(key, message_id.as_str())).await;
            }
            let _ = local_forget(&self.inner, key, message_id.as_str()).await;
            return Err(Error::Unavailable("quorum has no majority".into()));
        }
        copies.push(MemberCopy::Durable);
        if !durable_majority(members, &copies) {
            for peer in &stored_on {
                let _ = self.call(peer, "forget", wire_forget(key, message_id.as_str())).await;
            }
            let _ = local_forget(&self.inner, key, message_id.as_str()).await;
            return Err(Error::Unavailable("quorum has no majority".into()));
        }
        Ok(())
    }

    async fn forget_local(&self, key: &QueueKey, message_id: &str) {
        let Some(handle) = self.inner.queues.get(key) else {
            return;
        };
        remember_consumed(&self.inner, key, message_id).await;
        let _ = handle.tx.send(QueueCmd::Forget {
            message_id: CompactString::from(message_id),
        }).await;
        let (tx, rx) = oneshot::channel();
        if handle.tx.send(QueueCmd::Touch { reply: tx }).await.is_ok() {
            let _ = rx.await;
        }
    }

    /// Drop follower replica-log entries before the client sees the body.
    ///
    /// `drop_local` removes this node's replica log only. The leader's unacked
    /// entry stays until the client acks or nacks.
    /// Remember a quorum id that must not be delivered again after a peer restarts.
    pub async fn note_quorum_consumed(&self, key: &QueueKey, message_id: &str) {
        remember_consumed(&self.inner, key, message_id).await;
    }

    /// Drop follower replica-log entries before the client sees the body.
    ///
    /// `drop_local` removes this node's replica log only. The leader's unacked
    /// entry stays until the client acks or nacks.
    pub async fn claim_for_handoff(&self, key: &QueueKey, message_id: &str, drop_local: bool) {
        if drop_local {
            self.inner.replicas.lock().await.remove(&replica_key(key, message_id));
            // The follower append is in the ready queue. Drop it here so this
            // node does not deliver the body again after the leader is gone.
            self.forget_local(key, message_id).await;
        }
        self.quorum_forget(key, message_id).await;
    }

    /// Lowest reachable member once a majority is up. That node pushes quorum deliveries.
    pub fn quorum_leader(&self) -> String {
        let slot = self.inner.leader.lock().unwrap_or_else(|err| err.into_inner());
        if !slot.is_empty() {
            return slot.clone();
        }
        let mut ids: Vec<&str> = self.inner.members.iter().map(|member| member.id.as_str()).collect();
        if ids.is_empty() {
            return self.inner.node_id.clone();
        }
        ids.sort_unstable();
        ids[0].to_string()
    }

    /// Whether this process is the quorum leader.
    pub fn is_quorum_leader(&self) -> bool {
        self.quorum_leader() == self.inner.node_id
    }

    /// Mailbox that runs consume, ack, and nack on the quorum leader.
    ///
    /// A local replica still exists on this node. Registering the consumer there
    /// would deliver the same body from every ready queue.
    pub fn leader_consume_handle(&self, key: &QueueKey) -> Option<QueueHandle> {
        let local = self.inner.queues.get(key)?;
        if self.is_quorum_leader() {
            return Some(local);
        }
        let mut queue = local.info.to_domain();
        queue.home = Some(CompactString::from(self.quorum_leader()));
        let (tx, rx) = mpsc::channel(64);
        let handle = QueueHandle {
            tx,
            info: Arc::clone(&local.info),
        };
        let inner = Arc::clone(&self.inner);
        tokio::spawn(async move {
            proxy_loop(inner, queue, rx).await;
        });
        Some(handle)
    }

    /// Remove one quorum message from every other member.
    pub async fn quorum_forget(&self, key: &QueueKey, message_id: &str) {
        let leader = self.quorum_leader();
        let peers = self.live_peers().await;
        let wire = serde_json::json!({
            "vhost": key.vhost.as_str(),
            "queue": key.name.as_str(),
            "message_id": message_id,
        });
        for peer in peers {
            if peer == leader {
                continue;
            }
            let _ = self.call(&peer, "forget", wire.clone()).await;
        }
    }

    async fn live_peers(&self) -> Vec<String> {
        self.inner
            .peers
            .lock()
            .await
            .keys()
            .filter(|id| !id.starts_with("inbound-") && id.as_str() != self.inner.node_id)
            .cloned()
            .collect()
    }

    async fn broadcast_apply(&self, kind: &str, payload: Value) {
        let peers: Vec<String> = self
            .inner
            .peers
            .lock()
            .await
            .keys()
            .filter(|id| !id.starts_with("inbound-"))
            .cloned()
            .collect();
        let body = serde_json::json!({"kind": kind, "body": payload});
        for id in peers {
            let _ = self.call(&id, "apply", body.clone()).await;
        }
    }
}

async fn attach_peer(
    inner: Arc<Inner>,
    node_id: String,
    addr: std::net::SocketAddr,
    stream: TcpStream,
    send_hello: bool,
) -> std::io::Result<()> {
    let (read, mut write) = stream.into_split();
    let (tx, mut rx) = mpsc::channel::<String>(256);
    let pending = Arc::new(Mutex::new(HashMap::new()));
    let peer = Arc::new(Peer {
        tx: tx.clone(),
        pending: Arc::clone(&pending),
    });
    let _ = addr;
    inner.peers.lock().await.insert(node_id.clone(), Arc::clone(&peer));
    refresh_leader(&inner).await;
    tokio::spawn(async move {
        while let Some(line) = rx.recv().await {
            if write.write_all(line.as_bytes()).await.is_err() || write.write_all(b"\n").await.is_err() {
                break;
            }
        }
    });
    if send_hello {
        let _ = peer.tx.send(
            serde_json::to_string(&Msg {
                id: 0,
                op: "hello".into(),
                ok: false,
                error: String::new(),
                payload: serde_json::json!({"v": 1, "node": inner.node_id, "snapshot": snapshot(&inner).await, "consumed": inner.consumed.lock().await.clone()}),
                v: 1,
                node_id: inner.node_id.clone(),
                from: inner.node_id.clone(),
                kind: String::new(),
            })
            .unwrap_or_default(),
        )
        .await;
    }
    let mut lines = BufReader::new(read).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        let Ok(msg) = serde_json::from_str::<Msg>(&line) else {
            continue;
        };
        if msg.op == "reply" {
            if let Some(snap) = msg.payload.get("snapshot") {
                apply_snapshot(&inner, snap).await;
            }
            if let Some(consumed) = msg.payload.get("consumed") {
                apply_consumed(&inner, consumed).await;
            }
            if let Some(waiter) = pending.lock().await.remove(&msg.id) {
                let _ = waiter.send(msg);
            }
            continue;
        }
        if msg.op == "hello" {
            let node_name = msg
                .payload
                .get("node")
                .and_then(|v| v.as_str())
                .filter(|name| !name.is_empty())
                .map(str::to_string)
                .or_else(|| if msg.node_id.is_empty() { None } else { Some(msg.node_id.clone()) });
            if let Some(node) = node_name.as_deref() {
                inner.peers.lock().await.insert(node.to_string(), Arc::clone(&peer));
                refresh_leader(&inner).await;
                if let Some(snap) = msg.payload.get("snapshot") {
                    apply_snapshot(&inner, snap).await;
                }
                if let Some(consumed) = msg.payload.get("consumed") {
                    apply_consumed(&inner, consumed).await;
                }
                let _ = peer.tx.send(
                    serde_json::to_string(&Msg {
                        id: msg.id,
                        op: "reply".into(),
                        ok: true,
                        error: String::new(),
                        payload: serde_json::json!({"v": 1, "node": inner.node_id, "snapshot": snapshot(&inner).await, "consumed": inner.consumed.lock().await.clone()}),
                        v: 1,
                        node_id: inner.node_id.clone(),
                        from: inner.node_id.clone(),
                kind: String::new(),
                    })
                    .unwrap_or_default(),
                )
                .await;
                let _ = node;
            }
            continue;
        }
        let inner = Arc::clone(&inner);
        let peer_tx = peer.tx.clone();
        tokio::spawn(async move {
            let reply = dispatch(&inner, &msg, peer_tx.clone()).await;
            let _ = peer_tx.send(serde_json::to_string(&reply).unwrap_or_default()).await;
        });
    }
    let mut peers = inner.peers.lock().await;
    peers.retain(|_, existing| !Arc::ptr_eq(existing, &peer));
    drop(peers);
    refresh_leader(&inner).await;
    Ok(())
}

async fn refresh_leader(inner: &Arc<Inner>) {
    let mut ids = vec![inner.node_id.clone()];
    {
        let peers = inner.peers.lock().await;
        for id in peers.keys() {
            if inner.members.iter().any(|member| member.id == *id) {
                ids.push(id.clone());
            }
        }
    }
    ids.sort();
    ids.dedup();
    let majority = inner.members.len().max(1) / 2 + 1;
    if ids.len() < majority {
        return;
    }
    let elected = ids[0].clone();
    let became = {
        let mut slot = inner.leader.lock().unwrap_or_else(|err| err.into_inner());
        let became = elected == inner.node_id && *slot != inner.node_id;
        *slot = elected;
        became
    };
    if became {
        promote_replicas(inner).await;
    }
}

async fn promote_replicas(inner: &Arc<Inner>) {
    let replicas: Vec<(String, Arc<Message>)> = inner.replicas.lock().await.drain().collect();
    for (key, message) in replicas {
        let mut parts = key.split('\0');
        let Some(vhost) = parts.next() else { continue };
        let Some(name) = parts.next() else { continue };
        let queue = QueueKey::new(vhost, name);
        let _ = local_enqueue(inner, &queue, message).await;
    }
}

async fn local_enqueue(inner: &Inner, key: &QueueKey, message: Arc<Message>) -> Result<QueueOffset, Error> {
    let handle = inner
        .queues
        .get(key)
        .ok_or_else(|| Error::Unavailable(format!("queue {key} is not local")))?;
    let (reply_tx, reply_rx) = oneshot::channel();
    handle
        .tx
        .send(QueueCmd::Enqueue { msg: message, reply: reply_tx })
        .await
        .map_err(|_| Error::Unavailable(format!("queue {key} is down")))?;
    let completion = reply_rx.await.map_err(|_| Error::Unavailable(format!("queue {key} is down")))??;
    let _ = completion.durable_done.await;
    Ok(completion.offset)
}

/// A peer is a durable copy only when the append reply has `ok: true`.
///
/// [`Cluster::call`] returns `Ok` for any reply that arrived, including a
/// handler failure (`ok: false`). A timeout or a down peer is `Err`.
fn peer_append_durable(reply: &Result<Msg, Error>) -> bool {
    matches!(reply, Ok(msg) if msg.ok)
}

/// Fsync this node's queue log through `offset`. Quorum confirms call this. Classic confirms do not.
async fn flush_queue(inner: &Inner, key: &QueueKey, offset: QueueOffset) -> Result<(), Error> {
    let handle = inner
        .queues
        .get(key)
        .ok_or_else(|| Error::Unavailable(format!("queue {key} is not local")))?;
    let (reply_tx, reply_rx) = oneshot::channel();
    handle
        .tx
        .send(QueueCmd::FlushDurable { offset, reply: reply_tx })
        .await
        .map_err(|_| Error::Unavailable(format!("queue {key} is down")))?;
    reply_rx.await.map_err(|_| Error::Unavailable(format!("queue {key} is down")))?
}

async fn local_forget(inner: &Inner, key: &QueueKey, message_id: &str) -> Result<(), Error> {
    let Some(handle) = inner.queues.get(key) else {
        return Ok(());
    };
    handle
        .tx
        .send(QueueCmd::Forget { message_id: CompactString::from(message_id) })
        .await
        .map_err(|_| Error::Unavailable(format!("queue {key} is down")))?;
    let (tx, rx) = oneshot::channel();
    if handle.tx.send(QueueCmd::Touch { reply: tx }).await.is_ok() {
        let _ = rx.await;
    }
    Ok(())
}

async fn serve_conn(inner: Arc<Inner>, stream: TcpStream) -> std::io::Result<()> {
    let peer = stream.peer_addr().unwrap_or(std::net::SocketAddr::from(([0, 0, 0, 0], 0)));
    attach_peer(inner, format!("inbound-{peer}"), peer, stream, false).await
}

async fn snapshot(inner: &Inner) -> Value {
    let store = Arc::clone(&inner.store);
    let snap = MetadataStore::blocking(store, |store| {
        let mut users = Vec::new();
        let mut permissions = Vec::new();
        let vhosts = store.list_vhosts()?;
        for user in store.list_users()? {
            permissions.extend(store.list_permissions_for_user(user.name.as_str())?);
            users.push(user);
        }
        let mut exchanges = Vec::new();
        let mut queues = Vec::new();
        let mut bindings = Vec::new();
        for vhost in &vhosts {
            exchanges.extend(store.list_exchanges(vhost.name.as_str())?);
            queues.extend(store.list_queues(vhost.name.as_str())?);
            bindings.extend(store.list_bindings(vhost.name.as_str())?);
        }
        Ok(Snapshot {
            users,
            vhosts,
            permissions,
            exchanges,
            queues,
            bindings,
        })
    })
    .await;
    serde_json::to_value(snap.unwrap_or(Snapshot {
        users: Vec::new(),
        vhosts: Vec::new(),
        permissions: Vec::new(),
        exchanges: Vec::new(),
        queues: Vec::new(),
        bindings: Vec::new(),
    }))
    .unwrap_or(Value::Null)
}

async fn remember_consumed(inner: &Inner, key: &QueueKey, message_id: &str) {
    let entry = serde_json::json!({"vhost": key.vhost.as_str(), "queue": key.name.as_str(), "id": message_id});
    let mut consumed = inner.consumed.lock().await;
    if !consumed.iter().any(|item| item == &entry) {
        consumed.push(entry);
    }
}

async fn apply_consumed(inner: &Arc<Inner>, value: &Value) {
    let Some(items) = value.as_array() else { return };
    for item in items {
        let vhost = item.get("vhost").and_then(|v| v.as_str()).unwrap_or("");
        let queue = item.get("queue").and_then(|v| v.as_str()).unwrap_or("");
        let id = item.get("id").and_then(|v| v.as_str()).unwrap_or("");
        if vhost.is_empty() || queue.is_empty() || id.is_empty() {
            continue;
        }
        let key = QueueKey::new(vhost, queue);
        remember_consumed(inner, &key, id).await;
        let inner = Arc::clone(inner);
        let id = id.to_string();
        tokio::spawn(async move {
            for _ in 0..15 {
                let _ = local_forget(&inner, &key, &id).await;
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        });
    }
}

async fn apply_snapshot(inner: &Arc<Inner>, value: &Value) {
    let Ok(snap) = serde_json::from_value::<Snapshot>(value.clone()) else {
        return;
    };
    let _ = MetadataStore::blocking(Arc::clone(&inner.store), {
        let snap = snap_clone(&snap);
        move |store| apply_snapshot_store(store, &snap)
    })
    .await;
    for exchange in snap.exchanges {
        inner.router.put_exchange(exchange);
    }
    for binding in &snap.bindings {
        let _ = inner.router.bind(binding.clone());
    }
    for queue in snap.queues {
        if queue.home.as_deref().is_some_and(|home| home != inner.node_id) {
            let cluster = Cluster { inner: Arc::clone(inner) };
            let _ = cluster.proxy_for(&queue).await;
        }
    }
}

fn snap_clone(snap: &Snapshot) -> Snapshot {
    Snapshot {
        users: snap.users.clone(),
        vhosts: snap.vhosts.clone(),
        permissions: snap.permissions.clone(),
        exchanges: snap.exchanges.clone(),
        queues: snap.queues.clone(),
        bindings: snap.bindings.clone(),
    }
}

fn apply_snapshot_store(store: &MetadataStore, snap: &Snapshot) -> queueforge_store::Result<()> {
    for vhost in &snap.vhosts {
        if store.get_vhost(vhost.name.as_str())?.is_none() {
            let _ = store.create_vhost(vhost.name.as_str());
        }
    }
    for user in &snap.users {
        if store.get_user(user.name.as_str())?.is_none() {
            let _ = store.create_user(user);
        }
    }
    for perm in &snap.permissions {
        let _ = store.put_permission(perm);
    }
    for exchange in &snap.exchanges {
        if store.get_exchange(exchange.vhost.as_str(), exchange.name.as_str())?.is_none() {
            let _ = store.create_exchange(exchange);
        }
    }
    for queue in &snap.queues {
        if store.get_queue(queue.vhost.as_str(), queue.name.as_str())?.is_none() {
            let _ = store.create_queue(queue);
        }
    }
    for binding in &snap.bindings {
        let _ = store.put_binding(binding);
    }
    Ok(())
}

async fn dispatch(inner: &Arc<Inner>, msg: &Msg, peer_tx: mpsc::Sender<String>) -> Msg {
    let result = dispatch_op(inner, msg, peer_tx).await;
    match result {
        Ok(payload) => Msg {
            id: msg.id,
            op: "reply".into(),
            ok: true,
            error: String::new(),
            payload,
            v: 1,
            node_id: inner.node_id.clone(),
            from: inner.node_id.clone(),
                kind: String::new(),
        },
        Err(err) => Msg {
            id: msg.id,
            op: "reply".into(),
            ok: false,
            error: err.to_string(),
            v: 1,
            node_id: inner.node_id.clone(),
            from: inner.node_id.clone(),
                kind: String::new(),
            payload: Value::Null,
        },
    }
}

async fn dispatch_op(inner: &Arc<Inner>, msg: &Msg, peer_tx: mpsc::Sender<String>) -> Result<Value, Error> {
    match msg.op.as_str() {
        "apply" => {
            let nested = msg.payload.get("kind").and_then(|v| v.as_str()).unwrap_or("");
            let kind = if msg.kind.is_empty() { nested } else { msg.kind.as_str() };
            let body = msg.payload.get("body").cloned().filter(|value| !value.is_null()).unwrap_or_else(|| msg.payload.clone());
            apply_one(&Arc::clone(inner), kind, &body).await;
            Ok(Value::Null)
        }
        "declare" | "declare_queue" => {
            let vhost = json_str(&msg.payload, "vhost");
            let queued = json_str(&msg.payload, "queue");
            let name = if queued.is_empty() { json_str(&msg.payload, "name") } else { queued };
            let mut opts = QueueDeclareOpts {
                durable: msg.payload.get("durable").and_then(|v| v.as_bool()).unwrap_or(false),
                exclusive: msg.payload.get("exclusive").and_then(|v| v.as_bool()).unwrap_or(false),
                auto_delete: msg.payload.get("auto_delete").and_then(|v| v.as_bool()).unwrap_or(false),
                passive: msg.payload.get("passive").and_then(|v| v.as_bool()).unwrap_or(false),
                exclusive_owner: None,
                args: serde_json::from_value(msg.payload.get("args").cloned().unwrap_or(Value::Null))
                    .unwrap_or_default(),
                declared_args: None,
                home: Some(CompactString::from(inner.node_id.as_str())),
            };
            if let Some(home) = msg.payload.get("home").and_then(|v| v.as_str()) {
                opts.home = Some(CompactString::from(home));
            }
            if msg
                .payload
                .get("args")
                .and_then(|args| args.get("x-queue-type"))
                .and_then(|kind| kind.as_str())
                == Some("quorum")
            {
                opts.args.queue_type = Some(queueforge_core::QueueType::Quorum);
            }
            let result = inner.queues.declare(&vhost, &name, opts).await?;
            let mut queue = result.handle.info.to_domain();
            queue.home = Some(CompactString::from(inner.node_id.as_str()));
            if queue.durable {
                let stored = queue.clone();
                let _ = MetadataStore::blocking(Arc::clone(&inner.store), move |store| {
                    if store.get_queue(stored.vhost.as_str(), stored.name.as_str())?.is_some() {
                        store.put_queue(&stored)
                    } else {
                        store.create_queue(&stored)
                    }
                })
                .await;
            }
            Ok(serde_json::json!({"queue": queue}))
        }
        "forget" | "quorum_drop" => {
            let key = QueueKey::new(json_str(&msg.payload, "vhost"), json_str(&msg.payload, "queue"));
            let message_id = {
                let id = json_str(&msg.payload, "message_id");
                CompactString::from(if id.is_empty() { json_str(&msg.payload, "id") } else { id })
            };
            inner.replicas.lock().await.remove(&replica_key(&key, message_id.as_str()));
            remember_consumed(&inner, &key, message_id.as_str()).await;
            let handle = inner.queues.get(&key).ok_or_else(|| Error::Unavailable(format!("queue {key} is not local")))?;
            handle.tx.send(QueueCmd::Forget { message_id }).await.map_err(|_| Error::Unavailable(format!("queue {key} is down")))?;
            let (tx, rx) = oneshot::channel();
            handle.tx.send(QueueCmd::Touch { reply: tx }).await.map_err(|_| Error::Unavailable(format!("queue {key} is down")))?;
            let _ = rx.await;
            Ok(Value::Null)
        }
        "enqueue" | "quorum_append" => {
            let (key, message) = decode_quorum_append(&msg.payload).map_err(|err| Error::Unavailable(err))?;
            let handle = inner.queues.get(&key).ok_or_else(|| Error::Unavailable(format!("queue {key} is not local")))?;
            // Leader and follower both append before the peer is acked. every_n_ms
            // completes durable_done after that write; the fsync stays on the timer.
            let (reply_tx, reply_rx) = oneshot::channel();
            handle.tx.send(QueueCmd::Enqueue { msg: Arc::new(message), reply: reply_tx }).await.map_err(|_| Error::Unavailable(format!("queue {key} is down")))?;
            let completion = reply_rx.await.map_err(|_| Error::Unavailable(format!("queue {key} is down")))??;
            let _ = completion.durable_done.await;
            flush_queue(&inner, &key, completion.offset).await?;
            Ok(serde_json::json!({"offset": completion.offset.0}))
        }
        "ack" => {
            let key = key_from(&msg.payload);
            forward_nowait(&inner.queues, &key, QueueCmd::Ack {
                id: ConsumerDeliveryId(msg.payload["delivery_id"].as_u64().unwrap_or(0)),
                multiple_to: None,
            }).await
        }
        "nack" => {
            let key = key_from(&msg.payload);
            forward_nowait(&inner.queues, &key, QueueCmd::Nack {
                id: ConsumerDeliveryId(msg.payload["delivery_id"].as_u64().unwrap_or(0)),
                requeue: msg.payload["requeue"].as_bool().unwrap_or(true),
            }).await
        }
        "set-args" => {
            let key = key_from(&msg.payload);
            let args = serde_json::from_value(msg.payload.get("args").cloned().unwrap_or(Value::Null))
                .unwrap_or_default();
            forward_nowait(&inner.queues, &key, QueueCmd::SetArgs { args }).await
        }
        "settle" => {
            let key = key_from(&msg.payload);
            forward_nowait(&inner.queues, &key, QueueCmd::SettleDelivered {
                id: ConsumerDeliveryId(msg.payload["delivery_id"].as_u64().unwrap_or(0)),
            }).await
        }
        "stats" => {
            let key = key_from(&msg.payload);
            let handle = inner.queues.get(&key).ok_or_else(|| Error::Unavailable(format!("queue {key} is not local")))?;
            let (tx, rx) = oneshot::channel();
            handle.tx.send(QueueCmd::Stats { reply: tx }).await.map_err(|_| Error::Unavailable(format!("queue {key} is down")))?;
            let stats = rx.await.map_err(|_| Error::Unavailable(format!("queue {key} is down")))?;
            Ok(serde_json::json!({
                "messages_ready": stats.messages_ready,
                "messages_unacked": stats.messages_unacked,
                "consumer_count": stats.consumer_count,
            }))
        }
        "get" => {
            let key = key_from(&msg.payload);
            let handle = inner.queues.get(&key).ok_or_else(|| Error::Unavailable(format!("queue {key} is not local")))?;
            let (tx, rx) = oneshot::channel();
            handle.tx.send(QueueCmd::Get {
                no_ack: msg.payload["no_ack"].as_bool().unwrap_or(false),
                reply: tx,
            }).await.map_err(|_| Error::Unavailable(format!("queue {key} is down")))?;
            match rx.await.map_err(|_| Error::Unavailable(format!("queue {key} is down")))? {
                Some((id, qm, ready)) => Ok(serde_json::json!({
                    "delivery_id": id.0,
                    "message": message_to_wire(&qm.message),
                    "ready": ready,
                })),
                None => Ok(Value::Null),
            }
        }
        "sub" => {
            let key = key_from(&msg.payload);
            let handle = inner.queues.get(&key).ok_or_else(|| Error::Unavailable(format!("queue {key} is not local")))?;
            let session = ConsumerSessionId(msg.payload["session"].as_u64().unwrap_or(0));
            let credit = match msg.payload.get("credit") {
                Some(Value::Number(n)) => Some(n.as_u64().unwrap_or(0) as u32),
                _ => None,
            };
            let (deliver_tx, mut deliver_rx) = mpsc::channel(256);
            let (reply_tx, reply_rx) = oneshot::channel();
            handle.tx.send(QueueCmd::RegisterConsumer {
                session,
                no_ack: msg.payload["no_ack"].as_bool().unwrap_or(false),
                exclusive: msg.payload["exclusive"].as_bool().unwrap_or(false),
                priority: msg.payload["priority"].as_i64().unwrap_or(0) as i32,
                initial_credit: credit,
                deliver_tx,
                reply: reply_tx,
            }).await.map_err(|_| Error::Unavailable(format!("queue {key} is down")))?;
            reply_rx.await.map_err(|_| Error::Unavailable(format!("queue {key} is down")))??;
            let sub_id = msg.id;
            let cluster = Cluster { inner: Arc::clone(&inner) };
            let forget_key = key.clone();
            tokio::spawn(async move {
                while let Some(delivery) = deliver_rx.recv().await {
                    if let Some(message_id) = delivery.message.message.message_id.clone() {
                        cluster.quorum_forget(&forget_key, message_id.as_str()).await;
                    }
                    let line = serde_json::to_string(&Msg {
                        id: sub_id,
                        op: "deliver".into(),
                        ok: true,
                        error: String::new(),
                        v: 1,
                        node_id: cluster.inner.node_id.clone(),
                        from: cluster.inner.node_id.clone(),
                        kind: String::new(),
                        payload: serde_json::json!({
                            "session": delivery.session.0,
                            "delivery_id": delivery.delivery_id.0,
                            "offset": delivery.message.offset.0,
                            "settles_on_write": delivery.settles_on_write,
                            "message": message_to_wire(&delivery.message.message),
                        }),
                    }).unwrap_or_default();
                    if peer_tx.send(line).await.is_err() {
                        break;
                    }
                }
            });
            Ok(Value::Null)
        }
        "credit" => {
            let key = key_from(&msg.payload);
            forward_nowait(&inner.queues, &key, QueueCmd::AddCredit {
                session: ConsumerSessionId(msg.payload["session"].as_u64().unwrap_or(0)),
                credit: msg.payload["credit"].as_u64().unwrap_or(0) as u32,
            }).await
        }
        "set_credit" => {
            let key = key_from(&msg.payload);
            let credit = match msg.payload.get("credit") {
                Some(Value::Number(n)) => Some(n.as_u64().unwrap_or(0) as u32),
                _ => None,
            };
            forward_nowait(&inner.queues, &key, QueueCmd::SetCredit {
                session: ConsumerSessionId(msg.payload["session"].as_u64().unwrap_or(0)),
                credit,
            }).await
        }
        "unsub" => {
            let key = key_from(&msg.payload);
            let handle = inner.queues.get(&key).ok_or_else(|| Error::Unavailable(format!("queue {key} is not local")))?;
            let (tx, rx) = oneshot::channel();
            handle.tx.send(QueueCmd::UnregisterConsumer {
                session: ConsumerSessionId(msg.payload["session"].as_u64().unwrap_or(0)),
                requeue: msg.payload["requeue"].as_bool().unwrap_or(true),
                reply: tx,
            }).await.map_err(|_| Error::Unavailable(format!("queue {key} is down")))?;
            let _ = rx.await;
            Ok(Value::Null)
        }
        "requeue" => {
            let key = key_from(&msg.payload);
            let sessions = msg.payload.get("sessions").and_then(|v| v.as_array()).map(|items| {
                items.iter().filter_map(|v| v.as_u64()).map(ConsumerSessionId).collect()
            }).unwrap_or_default();
            let handle = inner.queues.get(&key).ok_or_else(|| Error::Unavailable(format!("queue {key} is not local")))?;
            let (tx, rx) = oneshot::channel();
            handle.tx.send(QueueCmd::RequeueUnacked { sessions, reply: tx }).await.map_err(|_| Error::Unavailable(format!("queue {key} is down")))?;
            let _ = rx.await;
            Ok(Value::Null)
        }
        "delete" => {
            let key = key_from(&msg.payload);
            let count = inner.queues.delete(
                &key,
                msg.payload["if_unused"].as_bool().unwrap_or(false),
                msg.payload["if_empty"].as_bool().unwrap_or(false),
            ).await?;
            Ok(serde_json::json!({"message_count": count}))
        }
        "purge" => {
            let key = key_from(&msg.payload);
            let handle = inner.queues.get(&key).ok_or_else(|| Error::Unavailable(format!("queue {key} is not local")))?;
            let (tx, rx) = oneshot::channel();
            handle.tx.send(QueueCmd::Purge { reply: tx }).await.map_err(|_| Error::Unavailable(format!("queue {key} is down")))?;
            let n = rx.await.map_err(|_| Error::Unavailable(format!("queue {key} is down")))?;
            Ok(serde_json::json!({"purged": n}))
        }
        other => Err(Error::Unavailable(format!("unknown cluster op {other}"))),
    }
}

async fn forward_nowait(queues: &QueueRegistry, key: &QueueKey, cmd: QueueCmd) -> Result<Value, Error> {
    let handle = queues.get(key).ok_or_else(|| Error::Unavailable(format!("queue {key} is not local")))?;
    handle.tx.send(cmd).await.map_err(|_| Error::Unavailable(format!("queue {key} is down")))?;
    Ok(Value::Null)
}

async fn apply_one(inner: &Arc<Inner>, kind: &str, body: &Value) {
    match kind {
        "queue" => {
            if let Ok(queue) = serde_json::from_value::<Queue>(body.clone()) {
                if queue.durable {
                    let _ = MetadataStore::blocking(Arc::clone(&inner.store), {
                        let queue = queue.clone();
                        move |store| {
                            if store.get_queue(queue.vhost.as_str(), queue.name.as_str())?.is_some() {
                                store.put_queue(&queue)
                            } else {
                                store.create_queue(&queue)
                            }
                        }
                    })
                    .await;
                }
                if queue.args.queue_type == Some(queueforge_core::QueueType::Quorum) {
                    if inner.queues.get(&QueueKey::new(queue.vhost.as_str(), queue.name.as_str())).is_none() {
                        let _ = inner.queues.declare(queue.vhost.as_str(), queue.name.as_str(), QueueDeclareOpts {
                            durable: queue.durable,
                            exclusive: queue.exclusive,
                            auto_delete: queue.auto_delete,
                            passive: false,
                            exclusive_owner: None,
                            args: queue.args.clone(),
                            declared_args: None,
                            home: Some(CompactString::from(inner.node_id.as_str())),
                        }).await;
                    }
                } else if queue.home.as_deref().is_some_and(|home| home != inner.node_id) {
                    let cluster = Cluster { inner: Arc::clone(inner) };
                    let _ = cluster.proxy_for(&queue).await;
                } else if inner.queues.get(&QueueKey::new(queue.vhost.as_str(), queue.name.as_str())).is_none() {
                    let _ = inner.queues.declare(queue.vhost.as_str(), queue.name.as_str(), QueueDeclareOpts {
                        durable: queue.durable,
                        exclusive: queue.exclusive,
                        auto_delete: queue.auto_delete,
                        passive: false,
                        exclusive_owner: None,
                        args: queue.args.clone(),
                        declared_args: None,
                        home: queue.home.clone(),
                    }).await;
                }
            }
        }
        "delete_queue" => {
            let vhost = json_str(body, "vhost");
            let name = json_str(body, "queue");
            let key = QueueKey::new(vhost.as_str(), name.as_str());
            if inner.queues.get(&key).is_some() {
                let _ = inner.queues.delete(&key, false, false).await;
            }
            let _ = MetadataStore::blocking(Arc::clone(&inner.store), move |store| store.delete_queue(&vhost, &name)).await;
        }
        "exchange" => {
            if let Ok(exchange) = serde_json::from_value::<Exchange>(body.clone()) {
                let _ = MetadataStore::blocking(Arc::clone(&inner.store), {
                    let exchange = exchange.clone();
                    move |store| {
                        if store.get_exchange(exchange.vhost.as_str(), exchange.name.as_str())?.is_some() {
                            Ok(())
                        } else {
                            store.create_exchange(&exchange)
                        }
                    }
                })
                .await;
                inner.router.put_exchange(exchange);
            }
        }
        "binding" => {
            if let Ok(binding) = serde_json::from_value::<Binding>(body.clone()) {
                let _ = MetadataStore::blocking(Arc::clone(&inner.store), {
                    let binding = binding.clone();
                    move |store| store.put_binding(&binding)
                })
                .await;
                let _ = inner.router.bind(binding);
            }
        }
        "policy" => {
            if let Ok(policy) = serde_json::from_value::<Policy>(body.clone()) {
                let _ = inner.router.upsert_policy(policy.clone());
                let _ = MetadataStore::blocking(Arc::clone(&inner.store), {
                    let policy = policy.clone();
                    move |store| store.put_policy(&policy)
                })
                .await;
            }
        }
        "delete_policy" => {
            let vhost = json_str(body, "vhost");
            let name = json_str(body, "name");
            let _ = inner.router.delete_policy(&vhost, &name);
            let _ = MetadataStore::blocking(Arc::clone(&inner.store), move |store| store.delete_policy(&vhost, &name)).await;
        }
        "user" => {
            if let Ok(user) = serde_json::from_value::<User>(body.clone()) {
                let _ = MetadataStore::blocking(Arc::clone(&inner.store), {
                    let user = user.clone();
                    move |store| {
                        if store.get_user(user.name.as_str())?.is_some() {
                            store.put_user(&user)
                        } else {
                            store.create_user(&user)
                        }
                    }
                })
                .await;
            }
        }
        "delete_user" => {
            let name = json_str(body, "name");
            let _ = MetadataStore::blocking(Arc::clone(&inner.store), move |store| store.delete_user(&name)).await;
        }
        "permission" => {
            if let Ok(permission) = serde_json::from_value::<Permission>(body.clone()) {
                let _ = MetadataStore::blocking(Arc::clone(&inner.store), {
                    let permission = permission.clone();
                    move |store| store.put_permission(&permission)
                })
                .await;
            }
        }
        "delete_permission" => {
            let user = json_str(body, "user");
            let vhost = json_str(body, "vhost");
            let _ = MetadataStore::blocking(Arc::clone(&inner.store), move |store| {
                store.delete_permission(&user, &vhost)
            })
            .await;
        }
        "vhost" => {
            let name = json_str(body, "name");
            let _ = MetadataStore::blocking(Arc::clone(&inner.store), {
                let name = name.clone();
                move |store| {
                    if store.get_vhost(&name)?.is_none() {
                        store.create_vhost(&name)?;
                    }
                    Ok(())
                }
            })
            .await;
            for exchange in queueforge_core::Exchange::builtins_for(&name) {
                inner.router.put_exchange(exchange);
            }
        }
        "delete_vhost" => {
            let name = json_str(body, "name");
            let _ = MetadataStore::blocking(Arc::clone(&inner.store), {
                let name = name.clone();
                move |store| {
                    let _ = store.delete_vhost(&name);
                    Ok(())
                }
            })
            .await;
        }
        "unbind" => {
            if let Ok(binding) = serde_json::from_value::<Binding>(body.clone()) {
                let _ = inner.router.unbind(&binding);
                let args = queueforge_core::binding_args_key(&binding.args);
                let _ = MetadataStore::blocking(Arc::clone(&inner.store), {
                    let binding = binding.clone();
                    let args = args.clone();
                    move |store| store.delete_binding(binding.vhost.as_str(), binding.exchange.as_str(), binding.queue.as_str(), binding.routing_key.as_str(), &args)
                })
                .await;
            }
        }
        _ => {}
    }
}

async fn proxy_loop(inner: Arc<Inner>, queue: Queue, mut rx: mpsc::Receiver<QueueCmd>) {
    let home = queue.home.clone().map(|h| h.to_string()).unwrap_or_else(|| inner.node_id.clone());
    let cluster = Cluster { inner: Arc::clone(&inner) };
    while let Some(cmd) = rx.recv().await {
        match cmd {
            QueueCmd::Enqueue { msg, reply } => {
                let result = cluster.call(&home, "enqueue", serde_json::json!({
                    "vhost": queue.vhost.as_str(),
                    "queue": queue.name.as_str(),
                    "message": message_to_wire(&msg),
                })).await;
                let _ = reply.send(match result {
                    Ok(msg) if msg.ok => {
                        let (done_tx, done_rx) = oneshot::channel();
                        let _ = done_tx.send(Ok(()));
                        Ok(EnqueueCompletion {
                            offset: queueforge_core::QueueOffset(msg.payload["offset"].as_u64().unwrap_or(0)),
                            durable_done: done_rx,
                        })
                    }
                    Ok(msg) => Err(Error::Unavailable(msg.error)),
                    Err(err) => Err(err),
                });
            }
            QueueCmd::Ack { id, .. } => {
                let _ = cluster.call(&home, "ack", serde_json::json!({
                    "vhost": queue.vhost.as_str(),
                    "queue": queue.name.as_str(),
                    "delivery_id": id.0,
                })).await;
            }
            QueueCmd::Nack { id, requeue } => {
                let _ = cluster.call(&home, "nack", serde_json::json!({
                    "vhost": queue.vhost.as_str(),
                    "queue": queue.name.as_str(),
                    "delivery_id": id.0,
                    "requeue": requeue,
                })).await;
            }
            QueueCmd::AckReport { id, reply } => {
                let _ = cluster.call(&home, "ack", serde_json::json!({
                    "vhost": queue.vhost.as_str(),
                    "queue": queue.name.as_str(),
                    "delivery_id": id.0,
                })).await;
                let _ = reply.send(None);
            }
            QueueCmd::NackReport { id, requeue, reply } => {
                let _ = cluster.call(&home, "nack", serde_json::json!({
                    "vhost": queue.vhost.as_str(),
                    "queue": queue.name.as_str(),
                    "delivery_id": id.0,
                    "requeue": requeue,
                })).await;
                let _ = reply.send(None);
            }
            QueueCmd::Forget { message_id } => {
                let _ = cluster.call(&home, "forget", serde_json::json!({
                    "vhost": queue.vhost.as_str(),
                    "queue": queue.name.as_str(),
                    "message_id": message_id.as_str(),
                })).await;
            }
            QueueCmd::SettleDelivered { id } => {
                let _ = cluster.call(&home, "settle", serde_json::json!({
                    "vhost": queue.vhost.as_str(),
                    "queue": queue.name.as_str(),
                    "delivery_id": id.0,
                })).await;
            }
            QueueCmd::Get { no_ack, reply } => {
                let result = cluster.call(&home, "get", serde_json::json!({
                    "vhost": queue.vhost.as_str(),
                    "queue": queue.name.as_str(),
                    "no_ack": no_ack,
                })).await;
                let mapped = match result {
                    Ok(msg) if msg.ok && (msg.payload.is_null() || msg.payload.get("empty").and_then(|v| v.as_bool()) == Some(true)) => Ok(None),
                    Ok(msg) if msg.ok => {
                        let id = ConsumerDeliveryId(msg.payload["delivery_id"].as_u64().unwrap_or(0));
                        let message = if let Some(nested) = msg.payload.get("message") {
                            wire_to_message(serde_json::from_value(nested.clone()).unwrap_or(WireMessage {
                                exchange: String::new(),
                                routing_key: String::new(),
                                body_b64: String::new(),
                                persistent: false,
                                redelivered: false,
                                content_type: None,
                                content_encoding: None,
                                correlation_id: None,
                                message_id: None,
                                reply_to: None,
                                expiration: None,
                                app_id: None,
                                user_id: None,
                                type_: None,
                                priority: None,
                                timestamp: None,
                                expires_unix_ms: None,
                                headers: Default::default(),
                            }))
                        } else if let Some(body_b64) = msg.payload.pointer("/msg/body").and_then(|v| v.as_str()) {
                            let mut message = Message::blank();
                            if let Ok(body) = BASE64.decode(body_b64.as_bytes()) {
                                message.body = Bytes::from(body);
                            }
                            if let Some(message_id) = msg.payload.pointer("/msg/id").and_then(|v| v.as_str()) {
                                if !message_id.is_empty() {
                                    message.message_id = Some(CompactString::from(message_id));
                                }
                            }
                            message.persistent = msg.payload.pointer("/msg/persistent").and_then(|v| v.as_bool()).unwrap_or(true);
                            message.routing_key = CompactString::from(msg.payload.pointer("/msg/routingKey").and_then(|v| v.as_str()).unwrap_or(""));
                            message.exchange = CompactString::from(msg.payload.pointer("/msg/exchange").and_then(|v| v.as_str()).unwrap_or(""));
                            message
                        } else {
                            let _ = reply.send(None);
                            continue;
                        };
                        let ready = msg.payload["ready"].as_u64().unwrap_or(0) as u32;
                        Ok(Some((id, queueforge_core::QueueMessage::new(queueforge_core::QueueOffset(0), Arc::new(message)), ready)))
                    }
                    Ok(msg) => Err(Error::Unavailable(msg.error)),
                    Err(err) => Err(err),
                };
                match mapped {
                    Ok(value) => {
                        let _ = reply.send(value);
                    }
                    Err(_) => drop(reply),
                }
            }
            QueueCmd::RegisterConsumer { session, no_ack, exclusive, priority, initial_credit, deliver_tx, reply } => {
                let opened = open_subscription(&cluster, SubOpen {
                    home: home.clone(),
                    queue: queue.clone(),
                    session,
                    no_ack,
                    exclusive,
                    priority,
                    initial_credit,
                    deliver_tx,
                }).await;
                let _ = reply.send(opened);
            }
            QueueCmd::AddCredit { session, credit } => {
                let _ = cluster.call(&home, "credit", serde_json::json!({
                    "vhost": queue.vhost.as_str(),
                    "queue": queue.name.as_str(),
                    "session": session.0,
                    "credit": credit,
                })).await;
            }
            QueueCmd::SetCredit { session, credit } => {
                let _ = cluster.call(&home, "set_credit", serde_json::json!({
                    "vhost": queue.vhost.as_str(),
                    "queue": queue.name.as_str(),
                    "session": session.0,
                    "credit": credit,
                })).await;
            }
            QueueCmd::UnregisterConsumer { session, requeue, reply } => {
                let _ = cluster.call(&home, "unsub", serde_json::json!({
                    "vhost": queue.vhost.as_str(),
                    "queue": queue.name.as_str(),
                    "session": session.0,
                    "requeue": requeue,
                })).await;
                let _ = reply.send(());
            }
            QueueCmd::RequeueUnacked { sessions, reply } => {
                let _ = cluster.call(&home, "requeue", serde_json::json!({
                    "vhost": queue.vhost.as_str(),
                    "queue": queue.name.as_str(),
                    "sessions": sessions.iter().map(|s| s.0).collect::<Vec<_>>(),
                })).await;
                let _ = reply.send(());
            }
            QueueCmd::Purge { reply } => {
                let result = cluster.call(&home, "purge", serde_json::json!({
                    "vhost": queue.vhost.as_str(),
                    "queue": queue.name.as_str(),
                })).await;
                let _ = reply.send(result.ok().and_then(|msg| msg.payload["purged"].as_u64()).unwrap_or(0) as u32);
            }
            QueueCmd::Stats { reply } => {
                let result = cluster.call(&home, "stats", serde_json::json!({
                    "vhost": queue.vhost.as_str(),
                    "queue": queue.name.as_str(),
                })).await;
                let stats = result.ok().map(|msg| queueforge_core::QueueStats {
                    messages_ready: msg.payload["messages_ready"].as_u64().unwrap_or(0) as u32,
                    messages_unacked: msg.payload["messages_unacked"].as_u64().unwrap_or(0) as u32,
                    consumer_count: msg.payload["consumer_count"].as_u64().unwrap_or(0) as u32,
                    max_priority: None,
                    ready_by_priority: None,
                }).unwrap_or_default();
                let _ = reply.send(stats);
            }
            QueueCmd::SetArgs { args } => {
                let _ = cluster.call(&home, "set-args", serde_json::json!({
                    "vhost": queue.vhost.as_str(),
                    "queue": queue.name.as_str(),
                    "args": args,
                })).await;
            }
            QueueCmd::FlushDurable { reply, .. } => { let _ = reply.send(Ok(())); }
            QueueCmd::Touch { reply } => { let _ = reply.send(()); }
            QueueCmd::Shutdown { reply } => { let _ = reply.send(Ok(())); }
            QueueCmd::Deliver { reply, .. } => {
                let _ = reply.send(None);
            }
            QueueCmd::DlxResolved { .. } => {}
        }
    }
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

async fn open_subscription(cluster: &Cluster, open: SubOpen) -> Result<(), Error> {
    let SubOpen {
        home,
        queue,
        session,
        no_ack,
        exclusive,
        priority,
        initial_credit,
        deliver_tx,
    } = open;
    let member = cluster.inner.members.iter().find(|m| m.id == home).ok_or_else(|| Error::Unavailable(format!("unknown home {home}")))?;
    let stream = TcpStream::connect(member.addr).await.map_err(|_| Error::Unavailable(format!("cluster peer {home} is down")))?;
    let (read, mut write) = stream.into_split();
    let id = cluster.inner.next_id.fetch_add(1, Ordering::Relaxed);
    let line = serde_json::to_string(&Msg {
        id,
        op: "sub".into(),
        ok: false,
        error: String::new(),
        v: 1,
        node_id: cluster.inner.node_id.clone(),
        from: cluster.inner.node_id.clone(),
                        kind: String::new(),
        payload: serde_json::json!({
            "vhost": queue.vhost.as_str(),
            "queue": queue.name.as_str(),
            "session": session.0,
            "no_ack": no_ack,
            "exclusive": exclusive,
            "priority": priority,
            "credit": initial_credit,
        }),
    }).map_err(|err| Error::Unavailable(err.to_string()))?;
    write.write_all(line.as_bytes()).await.map_err(|_| Error::Unavailable(format!("cluster peer {home} is down")))?;
    write.write_all(b"\n").await.map_err(|_| Error::Unavailable(format!("cluster peer {home} is down")))?;
    let mut lines = BufReader::new(read).lines();
    let first = lines
        .next_line()
        .await
        .map_err(|_| Error::Unavailable(format!("cluster peer {home} is down")))?
        .ok_or_else(|| Error::Unavailable(format!("cluster peer {home} closed the subscribe")))?;
    let reply: Msg = serde_json::from_str(&first).map_err(|err| Error::Unavailable(err.to_string()))?;
    if !reply.ok {
        let text = if reply.error.is_empty() {
            "subscribe rejected".to_string()
        } else {
            reply.error
        };
        return Err(if text.contains("already registered") || text.contains("precondition") {
            Error::PreconditionFailed(text)
        } else {
            Error::Unavailable(text)
        });
    }
    let (cmd_tx, mut cmd_rx) = mpsc::channel::<String>(64);
    tokio::spawn(async move {
        while let Some(line) = cmd_rx.recv().await {
            if write.write_all(line.as_bytes()).await.is_err() || write.write_all(b"\n").await.is_err() {
                break;
            }
        }
    });
    let home_owned = home.to_string();
    tokio::spawn(async move {
        while let Ok(Some(line)) = lines.next_line().await {
            let Ok(msg) = serde_json::from_str::<Msg>(&line) else { continue };
            if msg.op == "deliver" {
                let Ok(wire) = serde_json::from_value::<WireMessage>(msg.payload["message"].clone()) else { continue };
                let delivery = queueforge_core::QueueDelivery {
                    session: ConsumerSessionId(msg.payload["session"].as_u64().unwrap_or(session.0)),
                    delivery_id: ConsumerDeliveryId(msg.payload["delivery_id"].as_u64().unwrap_or(0)),
                    message: queueforge_core::QueueMessage::new(
                        queueforge_core::QueueOffset(msg.payload["offset"].as_u64().unwrap_or(0)),
                        Arc::new(wire_to_message(wire)),
                    ),
                    settles_on_write: msg.payload["settles_on_write"].as_bool().unwrap_or(false),
                    server_cancel: false,
                };
                if deliver_tx.send(delivery).await.is_err() {
                    break;
                }
            }
        }
        let _ = home_owned;
    });
    let _ = cmd_tx;
    Ok(())
}

fn node_is_quorum_leader(inner: &Inner) -> bool {
    if inner.members.is_empty() {
        return true;
    }
    let slot = inner.leader.lock().unwrap_or_else(|err| err.into_inner());
    !slot.is_empty() && *slot == inner.node_id
}

fn replica_key(key: &QueueKey, message_id: &str) -> String {
    format!("{}\0{}\0{message_id}", key.vhost, key.name)
}

fn wire_forget(key: &QueueKey, message_id: &str) -> Value {
    serde_json::json!({
        "vhost": key.vhost.as_str(),
        "queue": key.name.as_str(),
        "message_id": message_id,
    })
}

fn key_from(payload: &Value) -> QueueKey {
    QueueKey::new(json_str(payload, "vhost"), json_str(payload, "queue"))
}

fn json_str(payload: &Value, field: &str) -> String {
    payload.get(field).and_then(|v| v.as_str()).unwrap_or("").to_string()
}

/// Version 1 quorum body. Both apps encode this and accept it without reading the sender's files.
pub fn encode_quorum_append(key: &QueueKey, message: &Message) -> Value {
    serde_json::json!({
        "v": 1,
        "vhost": key.vhost.as_str(),
        "queue": key.name.as_str(),
        "message_id": message.message_id.as_ref().map(|id| id.as_str()).unwrap_or(""),
        "body_b64": BASE64.encode(&message.body),
        "persistent": message.persistent,
        "routing_key": message.routing_key.as_str(),
        "exchange": message.exchange.as_str(),
    })
}

/// Decode a version-1 quorum append, or the older nested `message` / raw-body shapes.
pub fn decode_quorum_append(payload: &Value) -> Result<(QueueKey, Message), String> {
    let vhost = json_str(payload, "vhost");
    let queue = json_str(payload, "queue");
    if payload.get("body_b64").is_some() || payload.get("v").and_then(|v| v.as_u64()) == Some(1) {
        let body = BASE64
            .decode(json_str(payload, "body_b64").as_bytes())
            .map_err(|err| err.to_string())?;
        let mut message = Message::blank();
        message.body = Bytes::from(body);
        message.persistent = payload.get("persistent").and_then(|v| v.as_bool()).unwrap_or(true);
        message.routing_key = CompactString::from(json_str(payload, "routing_key"));
        message.exchange = CompactString::from(json_str(payload, "exchange"));
        let id = json_str(payload, "message_id");
        if !id.is_empty() {
            message.message_id = Some(CompactString::from(id));
        }
        return Ok((QueueKey::new(vhost, queue), message));
    }
    if let Some(nested) = payload.get("message") {
        let wire: WireMessage = serde_json::from_value(nested.clone()).map_err(|err| err.to_string())?;
        return Ok((QueueKey::new(vhost, queue), wire_to_message(wire)));
    }
    if payload.get("body").is_some() {
        let body = BASE64.decode(json_str(payload, "body").as_bytes()).map_err(|err| err.to_string())?;
        let mut message = Message::blank();
        message.body = Bytes::from(body);
        message.persistent = payload.get("persistent").and_then(|v| v.as_bool()).unwrap_or(true);
        let key_name = {
            let routing = json_str(payload, "routing_key");
            if routing.is_empty() { json_str(payload, "routingKey") } else { routing }
        };
        message.routing_key = CompactString::from(key_name);
        message.exchange = CompactString::from(json_str(payload, "exchange"));
        let id = {
            let message_id = json_str(payload, "message_id");
            if !message_id.is_empty() {
                message_id
            } else {
                let qid = json_str(payload, "qid");
                if qid.is_empty() { json_str(payload, "id") } else { qid }
            }
        };
        if !id.is_empty() {
            message.message_id = Some(CompactString::from(id));
        }
        return Ok((QueueKey::new(vhost, queue), message));
    }
    Err("quorum append has no body".into())
}

fn message_to_wire(message: &Message) -> WireMessage {
    WireMessage {
        exchange: message.exchange.to_string(),
        routing_key: message.routing_key.to_string(),
        body_b64: BASE64.encode(&message.body),
        persistent: message.persistent,
        redelivered: message.redelivered,
        content_type: message.content_type.as_ref().map(|s| s.to_string()),
        content_encoding: message.content_encoding.as_ref().map(|s| s.to_string()),
        correlation_id: message.correlation_id.as_ref().map(|s| s.to_string()),
        message_id: message.message_id.as_ref().map(|s| s.to_string()),
        reply_to: message.reply_to.as_ref().map(|s| s.to_string()),
        expiration: message.expiration.as_ref().map(|s| s.to_string()),
        app_id: message.app_id.as_ref().map(|s| s.to_string()),
        user_id: message.user_id.as_ref().map(|s| s.to_string()),
        type_: message.type_.as_ref().map(|s| s.to_string()),
        priority: message.priority,
        timestamp: message.timestamp,
        expires_unix_ms: message.expires_unix_ms,
        headers: message.headers.clone(),
    }
}

fn wire_to_message(wire: WireMessage) -> Message {
    let body = BASE64.decode(wire.body_b64.as_bytes()).unwrap_or_default();
    Message {
        exchange: CompactString::from(wire.exchange),
        routing_key: CompactString::from(wire.routing_key),
        body: Bytes::from(body),
        persistent: wire.persistent,
        redelivered: wire.redelivered,
        content_type: wire.content_type.map(CompactString::from),
        content_encoding: wire.content_encoding.map(CompactString::from),
        correlation_id: wire.correlation_id.map(CompactString::from),
        message_id: wire.message_id.map(CompactString::from),
        reply_to: wire.reply_to.map(CompactString::from),
        expiration: wire.expiration.map(CompactString::from),
        app_id: wire.app_id.map(CompactString::from),
        user_id: wire.user_id.map(CompactString::from),
        type_: wire.type_.map(CompactString::from),
        priority: wire.priority,
        timestamp: wire.timestamp,
        expires_unix_ms: wire.expires_unix_ms,
        headers: wire.headers,
    }
}

fn opt_str(value: Option<&CompactString>) -> Option<String> {
    value.map(|s| s.to_string())
}

#[allow(dead_code)]
fn _use_opt(value: Option<&CompactString>) -> Option<String> {
    opt_str(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn home_is_stable_and_covers_both_nodes() {
        let members = vec![
            ClusterMember { id: "b".into(), addr: "127.0.0.1:2".parse().unwrap() },
            ClusterMember { id: "a".into(), addr: "127.0.0.1:1".parse().unwrap() },
        ];
        assert_eq!(queue_home(&members, "/", "orders"), queue_home(&members, "/", "orders"));
        let mut seen = std::collections::HashSet::new();
        for i in 0..40 {
            seen.insert(queue_home(&members, "/", &format!("q{i}")));
        }
        assert!(seen.len() > 1, "both nodes should home some queues");
    }

    #[test]
    fn quorum_append_v1_round_trip_keeps_the_body() {
        let mut message = Message::blank();
        message.body = Bytes::from_static(b"mixed-body");
        message.persistent = true;
        message.message_id = Some(CompactString::from("m1"));
        message.routing_key = CompactString::from("orders");
        let encoded = encode_quorum_append(&QueueKey::new("/", "orders"), &message);
        assert_eq!(encoded["v"], 1);
        let (key, decoded) = decode_quorum_append(&encoded).expect("decode v1");
        assert_eq!(key.name.as_str(), "orders");
        assert_eq!(decoded.body.as_ref(), b"mixed-body");
        assert_eq!(decoded.message_id.as_deref(), Some("m1"));
        let bun_shape = serde_json::json!({
            "vhost": "/",
            "queue": "orders",
            "qid": "m1",
            "body": BASE64.encode(b"mixed-body"),
            "persistent": true,
            "routingKey": "orders"
        });
        let (_, from_bun) = decode_quorum_append(&bun_shape).expect("decode bun shape");
        assert_eq!(from_bun.body.as_ref(), b"mixed-body");
    }

    fn sample_reply(ok: bool) -> Msg {
        Msg {
            id: 1,
            op: "quorum_append".into(),
            ok,
            error: if ok { String::new() } else { "append failed".into() },
            payload: Value::Null,
            v: 1,
            node_id: "peer".into(),
            from: "peer".into(),
            kind: String::new(),
        }
    }

    #[test]
    fn failed_peer_reply_is_not_a_durable_copy() {
        let failed = Ok(sample_reply(false));
        let accepted = Ok(sample_reply(true));
        let down = Err(Error::Unavailable("cluster peer is down".into()));
        assert!(!peer_append_durable(&failed));
        assert!(!peer_append_durable(&down));
        assert!(peer_append_durable(&accepted));
        let copies = [
            if peer_append_durable(&failed) { MemberCopy::Durable } else { MemberCopy::MemoryOnly },
            if peer_append_durable(&down) { MemberCopy::Durable } else { MemberCopy::MemoryOnly },
            MemberCopy::Durable,
        ];
        assert!(
            !durable_majority(3, &copies),
            "an ok:false reply must not satisfy a durable majority"
        );
    }
}
