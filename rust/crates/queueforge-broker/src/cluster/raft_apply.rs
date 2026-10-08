//! Applies committed Raft entries to this node: the `meta` group to the
//! metadata store and router, the `quorum` group to the local quorum queue
//! actors (docs/raft.md, sections 5 and 6).
//!
//! Entries arrive in log order. Every apply is idempotent, because a member
//! replays its log after a restart and the proposer has already applied
//! its own command.

use std::collections::HashMap;
use std::sync::Arc;

use compact_str::CompactString;
use queueforge_core::{QueueCmd, QueueKey};
use serde_json::{json, Value};
use tokio::sync::{mpsc, oneshot};
use tracing::{info, warn};

use super::forward::apply_one;
use super::net::promote_replicas;
use super::quorum::local_forget;
use super::raft::Entry;
use super::raft_node::{compact_after, Applied, RaftNode, Waiter, META, QUORUM};
use super::state::{apply_snapshot, remember_consumed, snapshot};
use super::wire::{json_str, replica_key};
use super::{decode_quorum_append, Inner};

/// Apply commits until the driver stops.
pub(super) async fn run_applier(inner: Arc<Inner>, node: Arc<RaftNode>, mut rx: mpsc::UnboundedReceiver<Applied>) {
    let mut since_snapshot: HashMap<&'static str, u64> = HashMap::new();
    while let Some(item) = rx.recv().await {
        match item {
            Applied::Entries { group, items } => {
                for (entry, waiter) in items {
                    match group {
                        META => apply_meta(&inner, &entry).await,
                        _ => apply_quorum(&inner, &entry).await,
                    }
                    match waiter {
                        Some(Waiter::Local(reply)) => {
                            let _ = reply.send(Ok(()));
                        }
                        Some(Waiter::Remote { from, rid }) => node.reply_remote(&from, group, &rid, true, None),
                        None => {}
                    }
                    let n = since_snapshot.entry(group).or_insert(0);
                    *n += 1;
                    if *n >= compact_after(group) {
                        *n = 0;
                        node.compact(group, entry.index, group_state(&inner, group).await);
                    }
                }
            }
            Applied::Install { group, snapshot } => {
                info!(group, index = snapshot.index, "raft snapshot installed");
                since_snapshot.insert(group, 0);
                match group {
                    META => apply_snapshot(&inner, &snapshot.state).await,
                    _ => install_quorum(&inner, &snapshot.state).await,
                }
            }
            Applied::Leader { group, leader } => {
                if group == QUORUM {
                    set_quorum_leader(&inner, leader).await;
                }
            }
        }
    }
}

async fn apply_meta(inner: &Arc<Inner>, entry: &Entry) {
    if entry.kind == "noop" || entry.kind == "config" {
        return;
    }
    apply_one(inner, &entry.kind, &entry.data).await;
}

async fn apply_quorum(inner: &Arc<Inner>, entry: &Entry) {
    tracing::debug!(index = entry.index, kind = %entry.kind, id = %json_str(&entry.data, "message_id"), ids = ?entry.data.get("ids"), leader = is_leader(inner), "raft quorum apply");
    match entry.kind.as_str() {
        "enq" => apply_enq(inner, &entry.data).await,
        "drop" => {
            let key = QueueKey::new(json_str(&entry.data, "vhost"), json_str(&entry.data, "queue"));
            let ids: Vec<String> = entry
                .data
                .get("ids")
                .and_then(|v| v.as_array())
                .map(|ids| ids.iter().filter_map(|v| v.as_str().map(str::to_string)).collect())
                .unwrap_or_default();
            let leader = is_leader(inner);
            for id in ids {
                let rk = replica_key(&key, &id);
                inner.quorum_live.lock().await.remove(&rk);
                inner.replicas.lock().await.remove(&rk);
                remember_consumed(inner, &key, &id).await;
                // The leader delivered this body; its ack settles it there.
                if !leader {
                    let _ = local_forget(inner, &key, &id).await;
                }
            }
        }
        "purge" => {
            let key = QueueKey::new(json_str(&entry.data, "vhost"), json_str(&entry.data, "queue"));
            let prefix = format!("{}\0{}\0", key.vhost, key.name);
            inner.quorum_live.lock().await.retain(|k, _| !k.starts_with(&prefix));
            inner.replicas.lock().await.retain(|k, _| !k.starts_with(&prefix));
            if !is_leader(inner) {
                if let Some(handle) = inner.queues.get(&key) {
                    let (tx, rx) = oneshot::channel();
                    if handle.tx.send(QueueCmd::Purge { reply: tx }).await.is_ok() {
                        let _ = rx.await;
                    }
                }
            }
        }
        _ => {}
    }
}

/// Store a committed quorum message on this member, once.
async fn apply_enq(inner: &Arc<Inner>, data: &Value) {
    let Ok((key, message)) = decode_quorum_append(data) else {
        warn!("raft enq with an unreadable body");
        return;
    };
    let id = message.message_id.clone().unwrap_or_default();
    let rk = replica_key(&key, id.as_str());
    let fresh = inner.quorum_live.lock().await.insert(rk.clone(), data.clone()).is_none();
    if !fresh {
        // The proposer, or an earlier apply, already holds it.
        return;
    }
    let Some(handle) = inner.queues.get(&key) else {
        inner.replicas.lock().await.insert(rk, Arc::new(message));
        return;
    };
    // A replay after a restart finds the body already recovered from the
    // queue log. Forgetting the id first keeps one copy.
    let _ = handle.tx.send(QueueCmd::Forget { message_id: CompactString::from(id.as_str()) }).await;
    let (reply_tx, reply_rx) = oneshot::channel();
    if handle
        .tx
        .send(QueueCmd::Enqueue { msg: Arc::new(message), reply: reply_tx })
        .await
        .is_ok()
    {
        // The Raft log is the durable copy; the queue log fsync is not awaited.
        let _ = reply_rx.await;
    }
}

/// Replace the quorum state with a leader's snapshot.
async fn install_quorum(inner: &Arc<Inner>, state: &Value) {
    inner.quorum_live.lock().await.clear();
    let Some(queues) = state.get("queues").and_then(|v| v.as_array()) else { return };
    for queue in queues {
        for message in queue.get("messages").and_then(|v| v.as_array()).into_iter().flatten() {
            apply_enq(inner, message).await;
        }
    }
}

/// The state a snapshot of `group` holds (sections 5 and 6).
async fn group_state(inner: &Arc<Inner>, group: &str) -> Value {
    if group == META {
        return snapshot(inner).await;
    }
    let live = inner.quorum_live.lock().await;
    let mut by_queue: HashMap<(String, String), Vec<Value>> = HashMap::new();
    for data in live.values() {
        by_queue
            .entry((json_str(data, "vhost"), json_str(data, "queue")))
            .or_default()
            .push(data.clone());
    }
    let queues: Vec<Value> = by_queue
        .into_iter()
        .map(|((vhost, queue), messages)| json!({"vhost": vhost, "queue": queue, "messages": messages}))
        .collect();
    json!({"queues": queues})
}

fn is_leader(inner: &Inner) -> bool {
    *inner.leader.lock().unwrap_or_else(|e| e.into_inner()) == inner.node_id
}

/// The `quorum` group's leader is every quorum queue's leader (section 6).
async fn set_quorum_leader(inner: &Arc<Inner>, leader: Option<String>) {
    let leader = leader.unwrap_or_default();
    let became = {
        let mut slot = inner.leader.lock().unwrap_or_else(|e| e.into_inner());
        let became = leader == inner.node_id && *slot != inner.node_id;
        *slot = leader.clone();
        became
    };
    info!(leader, "raft quorum leader");
    if became {
        promote_replicas(inner).await;
    }
}
