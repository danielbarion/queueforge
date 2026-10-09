//! Cluster startup, queue home, declare and delete, and peer RPC.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use compact_str::CompactString;
use queueforge_core::{
    ClusterMember, Error, ExchangeRouter, Queue, QueueDeclareOpts, QueueHandle, QueueKey,
    QueueRegistry,
};
use queueforge_store::MetadataStore;
use serde_json::Value;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{oneshot, Mutex};
use tracing::{debug, info, warn};

use super::net::{attach_peer, refresh_leader, serve_conn};
use super::proxy::proxy_loop;
use super::queue_home;
use super::{Cluster, Inner, Msg};

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
        let members = super::membership::load_members(&store, &members);
        let inner = Arc::new(Inner {
            node_id,
            members: std::sync::Mutex::new(members),
            store,
            queues,
            router,
            peers: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
            leader: std::sync::Mutex::new(String::new()),
            replicas: Mutex::new(HashMap::new()),
            consumed: Mutex::new(std::collections::HashSet::new()),
            append_inflight: std::sync::Mutex::new(HashMap::new()),
            heard: std::sync::Mutex::new(std::collections::HashSet::new()),
            dial_fails: std::sync::Mutex::new(HashMap::new()),
            down: std::sync::Mutex::new(std::collections::HashSet::new()),
            catchup: tokio::sync::Notify::new(),
            raft: std::sync::OnceLock::new(),
            raft_peers: std::sync::Mutex::new(std::collections::HashSet::new()),
            raft_auto_peers: std::sync::Mutex::new(std::collections::HashSet::new()),
            quorum_live: Mutex::new(HashMap::new()),
            qgroups: std::sync::Mutex::new(HashMap::new()),
            qleaders: std::sync::Mutex::new(HashMap::new()),
            raft_qgroup_peers: std::sync::Mutex::new(std::collections::HashSet::new()),
            settings: std::sync::OnceLock::new(),
        });
        super::consensus::start(&inner);
        let cluster = Arc::new(Self {
            inner: Arc::clone(&inner),
        });
        let accept_inner = Arc::clone(&inner);
        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, peer)) => {
                        let _ = stream.set_nodelay(true);
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

    /// Wait until every other member's hello has applied its consumed set.
    ///
    /// Startup calls this before `/readyz` when quorum messages were recovered,
    /// so a restarted leader cannot `basic.get` a body a survivor already acked.
    /// A member that refuses the cluster dial five times is down. That member
    /// does not keep `/readyz` false. A member that accepts stays required.
    pub async fn wait_quorum_catchup(&self) {
        loop {
            if self.catchup_done() {
                return;
            }
            self.probe_lower_peers().await;
            if self.catchup_done() {
                return;
            }
            tokio::select! {
                _ = self.inner.catchup.notified() => {}
                _ = tokio::time::sleep(Duration::from_millis(200)) => {}
            }
        }
    }

    fn catchup_done(&self) -> bool {
        let ids: Vec<String> = self
            .inner
            .member_list()
            .into_iter()
            .map(|member| member.id)
            .collect();
        let heard = self
            .inner
            .heard
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .clone();
        let down = self
            .inner
            .down
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .clone();
        super::catchup_satisfied(&self.inner.node_id, &ids, &heard, &down)
    }

    /// Dial members with a lower id. The steady-state loop only dials higher ids,
    /// so a dead lower id would otherwise never produce a refused connect.
    async fn probe_lower_peers(&self) {
        let snapshot = {
            let heard = self
                .inner
                .heard
                .lock()
                .unwrap_or_else(|err| err.into_inner())
                .clone();
            let down = self
                .inner
                .down
                .lock()
                .unwrap_or_else(|err| err.into_inner())
                .clone();
            (heard, down, self.inner.member_list())
        };
        let (heard, down, members) = snapshot;
        for member in members {
            if member.id.as_str() >= self.inner.node_id.as_str() {
                continue;
            }
            if heard.contains(member.id.as_str()) || down.contains(member.id.as_str()) {
                continue;
            }
            let id = member.id.clone();
            let addr = member.addr;
            match tokio::time::timeout(Duration::from_millis(200), TcpStream::connect(addr)).await {
                Ok(Ok(stream)) => {
                    drop(stream);
                    super::clear_dial_failure(&self.inner, &id);
                }
                Ok(Err(_)) => super::record_dial_failure(&self.inner, &id),
                Err(_) => {}
            }
        }
    }

    /// Home id for this queue.
    pub fn home_of(&self, vhost: &str, queue: &str, exclusive: bool) -> String {
        if exclusive {
            return self.inner.node_id.clone();
        }
        queue_home(&self.inner.member_list(), vhost, queue).to_string()
    }

    /// Home for a new queue. `x-queue-leader-locator` decides as in RabbitMQ:
    /// `client-local` is this node, `balanced` the member that homes the fewest
    /// queues (ties go to the lowest id). With no locator the home hash decides,
    /// so every implementation agrees without asking.
    async fn place(&self, vhost: &str, name: &str, opts: &QueueDeclareOpts) -> String {
        if opts.exclusive {
            return self.inner.node_id.clone();
        }
        match opts.args.leader_locator.as_deref() {
            Some("client-local") => self.inner.node_id.clone(),
            Some("balanced") => {
                let mut members: Vec<String> = self.inner.member_list().into_iter().map(|m| m.id).collect();
                if members.is_empty() {
                    return self.inner.node_id.clone();
                }
                members.sort();
                let homes = MetadataStore::blocking(Arc::clone(&self.inner.store), |store| {
                    let mut homes = Vec::new();
                    for vh in store.list_vhosts()? {
                        for q in store.list_queues(vh.name.as_str())? {
                            if let Some(home) = q.home {
                                homes.push(home.to_string());
                            }
                        }
                    }
                    Ok(homes)
                })
                .await
                .unwrap_or_default();
                members
                    .into_iter()
                    .min_by_key(|id| homes.iter().filter(|h| *h == id).count())
                    .unwrap_or_else(|| self.inner.node_id.clone())
            }
            _ => self.home_of(vhost, name, false),
        }
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
        let key = QueueKey::new(vhost, name);
        // A queue that exists keeps its home: a locator may have put it off the hash.
        let stored = self.stored_home(&key).await;
        let home = match stored.clone() {
            Some(home) if !opts.exclusive => home,
            _ => self.place(vhost, name, &opts).await,
        };
        opts.home = Some(CompactString::from(home.as_str()));
        let quorum = opts.args.queue_type == Some(queueforge_core::QueueType::Quorum);
        let stream = opts.args.queue_type == Some(queueforge_core::QueueType::Stream);
        if quorum {
            opts.home = Some(CompactString::from(self.inner.node_id.as_str()));
        }
        // A new quorum queue or stream gets its own Raft group when the
        // cluster runs them. A replicated stream has a copy on every member,
        // so it has no single home. The leader locator picks the member that
        // campaigns first.
        let had_group = super::qgroups::group_of(&self.inner, &key);
        let fresh = had_group.is_none() && stored.is_none() && self.inner.queues.get(&key).is_none();
        let new_group = if fresh && !opts.passive && (quorum || stream) {
            super::qgroups::group_for_new_queue(&self.inner, vhost, name)
        } else {
            None
        };
        if let Some(group) = had_group.clone().or(new_group.clone()) {
            let leader = new_group.as_ref().map(|_| super::qgroups::choose_leader(&self.inner, opts.args.leader_locator.as_deref()));
            if leader.is_some() {
                super::qgroups::register(&self.inner, &key, &group, false);
            }
            if stream {
                opts.home = None;
            }
            let durable = opts.durable;
            let result = match self.inner.queues.declare(vhost, name, opts).await {
                Ok(result) => result,
                Err(err) => {
                    if new_group.is_some() {
                        super::qgroups::unregister(&self.inner, &key);
                    }
                    return Err(err);
                }
            };
            let queue = self.queue_row(&result.handle);
            if durable {
                let row = queue.clone();
                let _ = MetadataStore::blocking(Arc::clone(&self.inner.store), move |store| store.put_queue(&row)).await;
            }
            let mut payload = serde_json::to_value(&queue).unwrap_or(Value::Null);
            if let Some(leader) = &leader {
                payload["raftLeader"] = Value::from(leader.as_str());
            }
            self.broadcast_apply("queue", payload).await;
            // Campaign once the members have the queue, and so its group: a
            // vote asked before that is ignored and another member may win.
            if leader.as_deref() == Some(self.inner.node_id.as_str()) {
                super::qgroups::register(&self.inner, &key, &group, true);
            }
            super::qgroups::wait_group_leader(&self.inner, &group).await;
            return Ok(result);
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
                    if store
                        .get_queue(queue.vhost.as_str(), queue.name.as_str())?
                        .is_none()
                    {
                        store.create_queue(&queue)?;
                    }
                    Ok(())
                }
            })
            .await;
        }
        let handle = self.proxy_for(&queue).await;
        let stats = self
            .call(
                &home,
                "stats",
                serde_json::json!({"vhost": vhost, "queue": name}),
            )
            .await;
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
        let grouped = super::qgroups::group_of(&self.inner, key).is_some();
        let home = self
            .stored_home(key)
            .await
            .unwrap_or_else(|| self.home_of(key.vhost.as_str(), key.name.as_str(), false));
        // A queue with its own group has a copy here; deleting it stops the group.
        if grouped || home == self.inner.node_id {
            let count = self.inner.queues.delete(key, if_unused, if_empty).await?;
            super::qgroups::unregister(&self.inner, key);
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

    /// Change the member list from the management API's `[{id, addr}]`.
    /// `Err` names why the cluster refused it (no majority with Raft on).
    pub async fn change_members_json(&self, members: &Value) -> Result<(), String> {
        let members = super::membership::members_from(members);
        super::membership::change_members(&self.inner, members)
            .await
            .map(|_| ())
            .map_err(|err| err.to_string())
    }

    /// The registry whose limits and topic permissions the replicated
    /// settings kinds update (`user_limits`, `topic_permission`, ...).
    pub fn attach_settings(&self, connections: Arc<queueforge_mgmt::ConnectionTracker>) {
        let _ = self.inner.settings.set(connections);
    }

    /// Push an exchange or binding so peers can route without declaring again.
    pub async fn replicate_json(&self, kind: &str, payload: Value) {
        self.broadcast_apply(kind, payload).await;
    }

    /// Copy the registry row for `handle`. Returns the queue record the cluster replicates. The caller must already hold the queue this node owns or proxies.
    pub(super) fn queue_row(&self, handle: &QueueHandle) -> Queue {
        let mut queue = handle.info.to_domain();
        queue.home = Some(CompactString::from(self.inner.node_id.as_str()));
        let key = QueueKey::new(queue.vhost.as_str(), queue.name.as_str());
        queue.raft_group = super::qgroups::group_of(&self.inner, &key).map(CompactString::from);
        if queue.raft_group.is_some() && queue.args.queue_type == Some(queueforge_core::QueueType::Stream) {
            queue.home = None;
        }
        queue
    }

    /// Read the stored home node for `key`. Returns `None` when metadata has no row. A missing row is not treated as this node.
    pub(super) async fn stored_home(&self, key: &QueueKey) -> Option<String> {
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

    /// Open a local proxy mailbox for `queue` when its home is remote. Returns the handle sessions use. The proxy forwards commands; it does not become the durable home.
    pub(super) async fn proxy_for(&self, queue: &Queue) -> QueueHandle {
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

    /// Clone the shared cluster state handle. Returns another `Arc` to the same inner state.
    pub(super) fn clone_inner(&self) -> Arc<Inner> {
        Arc::clone(&self.inner)
    }

    /// Dial every configured member except this node. A failed dial is retried by the peer task; startup does not wait for every peer.
    pub(super) async fn connect_peers(&self) {
        super::membership::reload_from_disk(&self.inner);
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
        let members = self.inner.member_list();
        for member in &members {
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
                        let _ = stream.set_nodelay(true);
                        super::clear_dial_failure(&inner, &id);
                        if let Err(err) = attach_peer(inner, id.clone(), addr, stream, true).await {
                            debug!(peer = %id, error = %err, "cluster dial failed");
                        }
                    }
                    Err(err) => {
                        debug!(peer = %id, error = %err, "cluster peer unreachable");
                        super::record_dial_failure(&inner, &id);
                    }
                }
            });
        }
        let connected = self.inner.peers.lock().await.len();
        let expected = self
            .inner
            .member_list()
            .iter()
            .filter(|member| member.id != self.inner.node_id)
            .count();
        queueforge_core::prom::unreachable_peers(expected.saturating_sub(connected) as u64);
    }

    /// Send `op` with `payload` to `node` and wait for one reply. Returns the reply message, or unavailable when the peer is down or times out. `Ok` means a reply arrived, including one with `ok: false`.
    pub(super) async fn call(&self, node: &str, op: &str, payload: Value) -> Result<Msg, Error> {
        self.begin_call(node, op, payload).await?.wait().await
    }

    /// Send one RPC and return before the reply arrives. The caller chooses when to wait, so a proxy can keep several appends in flight. Sends from one task stay in call order.
    pub(super) async fn begin_call(
        &self,
        node: &str,
        op: &str,
        payload: Value,
    ) -> Result<PendingCall, Error> {
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
        Ok(PendingCall {
            rx,
            peer,
            id,
            node: node.to_string(),
        })
    }
}

/// An RPC whose request line has already been sent.
pub(super) struct PendingCall {
    rx: oneshot::Receiver<Msg>,
    peer: Arc<super::Peer>,
    id: u64,
    node: String,
}

impl PendingCall {
    /// Wait for the reply. A timeout forgets this call only.
    ///
    /// The socket stays in the peer map. Removing it here also removed a
    /// connection that had already replaced this one, and the next publish
    /// then failed before the dial loop could restore the member.
    pub(super) async fn wait(self) -> Result<Msg, Error> {
        match tokio::time::timeout(Duration::from_secs(5), self.rx).await {
            Ok(Ok(msg)) => Ok(msg),
            _ => {
                self.peer.pending.lock().await.remove(&self.id);
                Err(Error::Unavailable(format!(
                    "cluster peer {} timed out",
                    self.node
                )))
            }
        }
    }
}
