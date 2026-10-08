//! Turning Raft on: the `raft` feature flag of docs/raft.md, section 8.
//!
//! Every hello advertises `features: ["raft"]`. Once each voter has done so,
//! this node writes `raft/enabled` to its data directory and starts the
//! driver. From then on it never goes back to the version 1 paths. Until
//! then quorum queues keep their majority-ack replication and metadata is
//! pushed, so a member without Raft (PHP) keeps the cluster on version 1.

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

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
pub(super) fn features() -> Value {
    if supported() {
        json!(["raft"])
    } else {
        json!([])
    }
}

fn dir(inner: &Inner) -> PathBuf {
    inner.store.data_dir().join("raft")
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
    if dir(inner).join("enabled").exists() {
        enable(inner);
    } else {
        flags::set_raft(RaftFlag::Disabled);
    }
}

/// Record the features a peer's hello (or hello reply) advertised.
pub(super) fn note_features(inner: &Arc<Inner>, peer: &str, payload: &Value) {
    if peer.is_empty() || peer.starts_with("inbound-") {
        return;
    }
    let raft = payload
        .get("features")
        .and_then(|v| v.as_array())
        .is_some_and(|list| list.iter().any(|f| f.as_str() == Some("raft")));
    if raft {
        inner
            .raft_peers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(peer.to_string());
        maybe_enable(inner);
    }
}

/// Enable Raft once every other voter has advertised it.
fn maybe_enable(inner: &Arc<Inner>) {
    if !supported() || inner.raft.get().is_some() {
        return;
    }
    let all = {
        let peers = inner.raft_peers.lock().unwrap_or_else(|e| e.into_inner());
        inner
            .member_list()
            .iter()
            .all(|m| m.id == inner.node_id || peers.contains(&m.id))
    };
    if !all {
        return;
    }
    let dir = dir(inner);
    if let Err(err) = fs::create_dir_all(&dir).and_then(|_| {
        let path = dir.join("enabled");
        fs::write(&path, b"raft\n")?;
        fs::File::open(&path)?.sync_all()
    }) {
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
    let sender = Arc::clone(inner);
    tokio::spawn(async move {
        while let Some((to, payload)) = out_rx.recv().await {
            let line = json!({
                "v": 1, "op": "raft", "id": 0,
                "from": sender.node_id, "nodeId": sender.node_id,
                "payload": payload,
            })
            .to_string();
            let peer = sender.peers.lock().await.get(&to).cloned();
            // A member that is not connected misses this one; Raft resends.
            if let Some(peer) = peer {
                let _ = peer.tx.try_send(line);
            }
        }
    });
    tokio::spawn(run_applier(Arc::clone(inner), node, applied_rx));
}

/// The member list changed: move the voters with it.
pub(super) fn members_changed(inner: &Inner) {
    if let Some(node) = inner.raft.get() {
        node.set_voters(inner.member_list().into_iter().map(|m| m.id).collect());
    }
}
