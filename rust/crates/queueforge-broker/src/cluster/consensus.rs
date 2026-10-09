//! Turning Raft on: the `raft` feature flag of docs/raft.md, section 8.
//!
//! Every hello advertises `features: ["raft"]`. The flag behaves as a
//! RabbitMQ feature flag. A cluster created by this build turns it on by
//! itself once every voter supports it: each also advertises `raft_auto`,
//! written when its data directory was new. A node upgraded from a build
//! without Raft waits for `PUT /api/feature-flags/raft/enable`, which every
//! member then applies (`op: "feature"`, and `raft_on` in later hellos).
//! Either way `raft/enabled` is written and the node never goes back to the
//! version 1 paths. Until then quorum queues keep their majority-ack
//! replication and metadata is pushed, so a member without Raft (PHP) keeps
//! the cluster on version 1.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Weak};

use queueforge_core::flags::{self, RaftFlag};
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tracing::{info, warn};

use super::raft_apply::run_applier;
use super::raft_node::RaftNode;
use super::Inner;

/// `QUEUEFORGE_RAFT=0` keeps a node on version 1, as a build without Raft.
pub(super) fn supported() -> bool {
    std::env::var("QUEUEFORGE_RAFT").map(|v| v != "0").unwrap_or(true)
}

/// The `features` list for a hello payload.
pub(super) fn features(inner: &Inner) -> Value {
    if !supported() {
        return json!([]);
    }
    let mut out = vec!["raft"];
    let on = inner.raft.get().is_some();
    if on || dir(inner).join("auto").exists() {
        out.push("raft_auto");
    }
    if on {
        out.push("raft_on");
    }
    if super::qgroups::supported() {
        out.push("raft_qgroups");
    }
    json!(out)
}

fn dir(inner: &Inner) -> PathBuf {
    inner.store.data_dir().join("raft")
}

fn write_marker(dir: &Path, name: &str) -> std::io::Result<()> {
    fs::create_dir_all(dir)?;
    let path = dir.join(name);
    fs::write(&path, b"raft\n")?;
    fs::File::open(&path)?.sync_all()
}

/// Record that `data_dir` was created by a build with Raft, so the cluster may
/// turn it on by itself. Call before anything else is written there.
pub fn mark_fresh(data_dir: &Path) {
    if supported() {
        if let Err(err) = write_marker(&data_dir.join("raft"), "auto") {
            warn!(error = %err, "could not record a new raft data directory");
        }
    }
}

/// The running driver, once Raft is enabled.
pub(super) fn node(inner: &Inner) -> Option<&Arc<RaftNode>> {
    inner.raft.get()
}

/// At startup: resume Raft if it was enabled before, else wait for hellos.
pub(super) fn start(inner: &Arc<Inner>) {
    if !supported() {
        flags::set_raft(RaftFlag::Unsupported);
        return;
    }
    let weak: Weak<Inner> = Arc::downgrade(inner);
    flags::set_raft_enabler(move || weak.upgrade().is_some_and(|inner| enable_by_operator(&inner)));
    let weak: Weak<Inner> = Arc::downgrade(inner);
    flags::set_queue_raft(move |vhost, name| {
        let inner = weak.upgrade()?;
        super::qgroups::queue_raft(&inner, &queueforge_core::QueueKey::new(vhost, name))
    });
    if dir(inner).join("enabled").exists() {
        enable(inner);
    } else {
        flags::set_raft(RaftFlag::Disabled);
    }
}

/// Every voter, this node included, can run Raft.
fn all_support(inner: &Inner) -> bool {
    if !supported() {
        return false;
    }
    let peers = inner.raft_peers.lock().unwrap_or_else(|e| e.into_inner());
    inner
        .member_list()
        .iter()
        .all(|m| m.id == inner.node_id || peers.contains(&m.id))
}

/// Turn Raft on because an operator or a member asked. Returns false when a
/// voter cannot run it.
pub(super) fn enable_now(inner: &Arc<Inner>) -> bool {
    if inner.raft.get().is_some() {
        return true;
    }
    if !all_support(inner) {
        return false;
    }
    if let Err(err) = write_marker(&dir(inner), "enabled") {
        warn!(error = %err, "could not record the raft feature flag");
        return false;
    }
    info!("the raft feature flag was enabled");
    enable(inner);
    inner.raft.get().is_some()
}

/// `PUT /api/feature-flags/raft/enable`: enable here, then tell every member.
fn enable_by_operator(inner: &Arc<Inner>) -> bool {
    if !enable_now(inner) {
        return false;
    }
    let line = json!({
        "v": 1, "op": "feature", "id": 0,
        "from": inner.node_id, "nodeId": inner.node_id,
        "payload": {"name": "raft"},
    })
    .to_string();
    let inner = Arc::clone(inner);
    tokio::spawn(async move {
        let peers: Vec<_> = inner.peers.lock().await.values().cloned().collect();
        for peer in peers {
            let _ = peer.tx.try_send(line.clone());
        }
    });
    true
}

/// Record the features a peer's hello (or hello reply) advertised.
pub(super) fn note_features(inner: &Arc<Inner>, peer: &str, payload: &Value) {
    if peer.is_empty() || peer.starts_with("inbound-") {
        return;
    }
    let has = |name: &str| {
        payload
            .get("features")
            .and_then(|v| v.as_array())
            .is_some_and(|list| list.iter().any(|f| f.as_str() == Some(name)))
    };
    if !has("raft") || !supported() {
        return;
    }
    {
        let mut qpeers = inner.raft_qgroup_peers.lock().unwrap_or_else(|e| e.into_inner());
        if has("raft_qgroups") {
            qpeers.insert(peer.to_string());
        } else {
            qpeers.remove(peer);
        }
    }
    inner
        .raft_peers
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(peer.to_string());
    if has("raft_auto") {
        inner
            .raft_auto_peers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(peer.to_string());
    }
    // A member already runs Raft: the cluster turned the flag on.
    if has("raft_on") && inner.raft.get().is_none() {
        if let Err(err) = write_marker(&dir(inner), "enabled") {
            warn!(error = %err, "could not record the raft feature flag");
            return;
        }
        info!(peer, "a member runs raft; enabling it");
        enable(inner);
        return;
    }
    maybe_enable(inner);
}

/// Enable Raft by itself in a cluster this build created: every voter
/// supports it and every data directory was new.
fn maybe_enable(inner: &Arc<Inner>) {
    if inner.raft.get().is_some() || !all_support(inner) || !dir(inner).join("auto").exists() {
        return;
    }
    let all = {
        let auto = inner.raft_auto_peers.lock().unwrap_or_else(|e| e.into_inner());
        inner
            .member_list()
            .iter()
            .all(|m| m.id == inner.node_id || auto.contains(&m.id))
    };
    if !all {
        return;
    }
    if let Err(err) = write_marker(&dir(inner), "enabled") {
        warn!(error = %err, "could not record the raft feature flag");
        return;
    }
    info!("every voter supports raft; enabling it");
    enable(inner);
}

fn enable(inner: &Arc<Inner>) {
    let mut voters: Vec<String> = inner.member_list().into_iter().map(|m| m.id).collect();
    if voters.is_empty() {
        voters.push(inner.node_id.clone());
    }
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<(String, Value)>();
    let (applied_tx, applied_rx) = mpsc::unbounded_channel();
    let node = match RaftNode::start(&inner.node_id, &dir(inner), voters, out_tx, applied_tx) {
        Ok(node) => node,
        Err(err) => {
            warn!(error = %err, "raft did not start");
            return;
        }
    };
    if inner.raft.set(Arc::clone(&node)).is_err() {
        return;
    }
    flags::set_raft(RaftFlag::Enabled);
    inner.queues.set_stream_replicator(Arc::new(super::qgroups::Streams { inner: Arc::downgrade(inner) }));
    let sender = Arc::clone(inner);
    tokio::spawn(async move {
        // Raft messages to a member keep to one socket while it is open. A
        // peer reachable on both a dialed and an accepted socket would
        // otherwise get a burst split across them, and proposals forwarded
        // to a leader would be appended out of order.
        let mut routes: std::collections::HashMap<String, Arc<super::Peer>> = std::collections::HashMap::new();
        while let Some((to, payload)) = out_rx.recv().await {
            let line = json!({
                "v": 1, "op": "raft", "id": 0,
                "from": sender.node_id, "nodeId": sender.node_id,
                "payload": payload,
            })
            .to_string();
            let peer = match routes.get(&to).filter(|p| !p.tx.is_closed()) {
                Some(peer) => Some(Arc::clone(peer)),
                None => {
                    let peer = sender.peers.lock().await.get(&to).cloned();
                    match &peer {
                        Some(p) => routes.insert(to.clone(), Arc::clone(p)),
                        None => routes.remove(&to),
                    };
                    peer
                }
            };
            // A member that is not connected misses this one; Raft resends.
            if let Some(peer) = peer {
                let _ = peer.tx.try_send(line);
            }
        }
    });
    let applier = Arc::clone(inner);
    tokio::spawn(async move {
        // Queue groups of stored queues are known before their entries apply.
        super::qgroups::load_stored(&applier).await;
        run_applier(applier, node, applied_rx).await;
    });
}

/// The member list changed: move the voters with it.
pub(super) fn members_changed(inner: &Inner) {
    if let Some(node) = inner.raft.get() {
        node.set_voters(inner.member_list().into_iter().map(|m| m.id).collect());
    }
}
