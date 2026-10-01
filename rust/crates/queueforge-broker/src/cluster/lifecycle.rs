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
        let cluster = Arc::new(Self {
            inner: Arc::clone(&inner),
        });
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

    /// Copy the registry row for `handle`. Returns the queue record the cluster replicates. The caller must already hold the queue this node owns or proxies.
    pub(super) fn queue_row(&self, handle: &QueueHandle) -> Queue {
        let mut queue = handle.info.to_domain();
        queue.home = Some(CompactString::from(self.inner.node_id.as_str()));
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

    /// Send `op` with `payload` to `node` and wait for one reply. Returns the reply message, or unavailable when the peer is down or times out. `Ok` means a reply arrived, including one with `ok: false`.
    pub(super) async fn call(&self, node: &str, op: &str, payload: Value) -> Result<Msg, Error> {
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
}
