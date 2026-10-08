//! Peer sockets, leader refresh, and the cluster RPC connection.

use std::collections::HashMap;
use std::sync::Arc;

use queueforge_core::{Message, QueueKey};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Mutex};

use super::ingress::dispatch;
use super::quorum::local_enqueue;
use super::state::{apply_consumed, apply_snapshot, snapshot};
use super::{Inner, Msg, Peer};

/// Read and write one peer socket. `inner` is cluster state, `node` is the peer id, and `stream` is the accepted or dialed connection. Returns when the socket closes.
pub(super) async fn attach_peer(
    inner: Arc<Inner>,
    node_id: String,
    addr: std::net::SocketAddr,
    stream: TcpStream,
    send_hello: bool,
) -> std::io::Result<()> {
    let (read, mut write) = stream.into_split();
    let (tx, mut rx) = mpsc::channel::<String>(1024);
    let pending = Arc::new(Mutex::new(HashMap::new()));
    let peer = Arc::new(Peer {
        tx: tx.clone(),
        pending: Arc::clone(&pending),
    });
    let _ = addr;
    inner
        .peers
        .lock()
        .await
        .insert(node_id.clone(), Arc::clone(&peer));
    refresh_leader(&inner).await;
    tokio::spawn(async move {
        while let Some(line) = rx.recv().await {
            // One write covers the lines already queued. A quiet socket still
            // sends this line immediately; the batch does not wait for more.
            let mut buf = String::with_capacity(line.len().saturating_add(1));
            buf.push_str(&line);
            buf.push('\n');
            while buf.len() < 64 * 1024 {
                match rx.try_recv() {
                    Ok(next) => {
                        buf.push_str(&next);
                        buf.push('\n');
                    }
                    Err(_) => break,
                }
            }
            if write.write_all(buf.as_bytes()).await.is_err() {
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
                payload: serde_json::json!({"v": 1, "node": inner.node_id, "snapshot": snapshot(&inner).await, "consumed": inner.consumed.lock().await.clone(), "features": super::consensus::features()}),
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
        if msg.op == "raft" {
            // One-way (docs/raft.md, section 1): never answered with a reply.
            if let Some(node) = super::consensus::node(&inner) {
                let from = if msg.from.is_empty() { msg.node_id.as_str() } else { msg.from.as_str() };
                node.step(from, msg.payload);
            }
            continue;
        }
        if msg.op == "reply" {
            if msg.id == 0 || msg.payload.get("features").is_some() {
                let from = if msg.from.is_empty() { msg.node_id.clone() } else { msg.from.clone() };
                super::consensus::note_features(&inner, &from, &msg.payload);
            }
            if let Some(snap) = msg.payload.get("snapshot") {
                apply_snapshot(&inner, snap).await;
            }
            if let Some(consumed) = msg.payload.get("consumed") {
                apply_consumed(&inner, consumed).await;
                let peer = if !msg.from.is_empty() {
                    msg.from.clone()
                } else if !msg.node_id.is_empty() {
                    msg.node_id.clone()
                } else {
                    node_id.clone()
                };
                note_peer_caught_up(&inner, &peer);
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
                .or_else(|| {
                    if msg.node_id.is_empty() {
                        None
                    } else {
                        Some(msg.node_id.clone())
                    }
                });
            if let Some(node) = node_name.as_deref() {
                inner
                    .peers
                    .lock()
                    .await
                    .insert(node.to_string(), Arc::clone(&peer));
                refresh_leader(&inner).await;
                if let Some(snap) = msg.payload.get("snapshot") {
                    apply_snapshot(&inner, snap).await;
                }
                if let Some(consumed) = msg.payload.get("consumed") {
                    apply_consumed(&inner, consumed).await;
                }
                note_peer_caught_up(&inner, node);
                super::consensus::note_features(&inner, node, &msg.payload);
                let _ = peer.tx.send(
                    serde_json::to_string(&Msg {
                        id: msg.id,
                        op: "reply".into(),
                        ok: true,
                        error: String::new(),
                        payload: serde_json::json!({"v": 1, "node": inner.node_id, "snapshot": snapshot(&inner).await, "consumed": inner.consumed.lock().await.clone(), "features": super::consensus::features()}),
                        v: 1,
                        node_id: inner.node_id.clone(),
                        from: inner.node_id.clone(),
                kind: String::new(),
                    })
                    .unwrap_or_default(),
                )
                .await;
            }
            continue;
        }
        let inner = Arc::clone(&inner);
        let peer_tx = peer.tx.clone();
        tokio::spawn(async move {
            let reply = dispatch(&inner, &msg, peer_tx.clone()).await;
            let _ = peer_tx
                .send(serde_json::to_string(&reply).unwrap_or_default())
                .await;
        });
    }
    let mut peers = inner.peers.lock().await;
    peers.retain(|_, existing| !Arc::ptr_eq(existing, &peer));
    drop(peers);
    refresh_leader(&inner).await;
    Ok(())
}

/// Record that `peer`'s hello consumed-set has been applied.
fn note_peer_caught_up(inner: &Inner, peer: &str) {
    if peer.is_empty() || peer == inner.node_id || peer.starts_with("inbound-") {
        return;
    }
    let mut heard = inner.heard.lock().unwrap_or_else(|err| err.into_inner());
    if heard.insert(peer.to_string()) {
        inner.catchup.notify_one();
    }
}

/// Recompute the quorum leader from `inner` membership and the live set. The lowest live member id, including this node, becomes leader.
pub(super) async fn refresh_leader(inner: &Arc<Inner>) {
    // With Raft on, the quorum group's election names the leader.
    if super::consensus::node(inner).is_some() {
        return;
    }
    let mut ids = vec![inner.node_id.clone()];
    {
        let peers = inner.peers.lock().await;
        for id in peers.keys() {
            if inner.member_list().iter().any(|member| member.id == *id) {
                ids.push(id.clone());
            }
        }
    }
    ids.sort();
    ids.dedup();
    let majority = inner.member_list().len().max(1) / 2 + 1;
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

/// Ask `inner` to serve quorum replicas when this node becomes leader. Queues that already have a local actor are left running.
pub(super) async fn promote_replicas(inner: &Arc<Inner>) {
    let replicas: Vec<(String, Arc<Message>)> = inner.replicas.lock().await.drain().collect();
    for (key, message) in replicas {
        let mut parts = key.split('\0');
        let Some(vhost) = parts.next() else { continue };
        let Some(name) = parts.next() else { continue };
        let queue = QueueKey::new(vhost, name);
        let _ = local_enqueue(inner, &queue, message).await;
    }
}

/// Serve one accepted cluster socket on `stream` for `inner`. Returns the IO error that closed the socket.
pub(super) async fn serve_conn(inner: Arc<Inner>, stream: TcpStream) -> std::io::Result<()> {
    let peer = stream
        .peer_addr()
        .unwrap_or(std::net::SocketAddr::from(([0, 0, 0, 0], 0)));
    attach_peer(inner, format!("inbound-{peer}"), peer, stream, false).await
}
