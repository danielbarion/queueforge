//! Applies committed Raft entries to this node: the `meta` group to the
//! metadata store and router, the `quorum` group and each queue group to
//! the local quorum queue actors, and a replicated stream's group to its
//! stream (docs/raft.md, sections 5 and 6).
//!
//! Entries arrive in log order. Every apply is idempotent, because a member
//! replays its log after a restart and the proposer has already applied
//! its own command.

use std::collections::HashMap;
use std::sync::Arc;

use queueforge_core::{Error, QueueCmd, QueueKey, QueueType};
use serde_json::{json, Value};
use tokio::sync::{mpsc, oneshot};
use tracing::{info, warn};

use super::forward::apply_one;
use super::net::promote_replicas;
use super::qgroups;
use super::quorum::local_forget;
use super::raft::Entry;
use super::raft_node::{compact_after, Applied, RaftNode, Waiter, META, QUORUM};
use super::state::{apply_snapshot, remember_consumed, snapshot};
use super::wire::{json_str, replica_key};
use super::{decode_quorum_append, Inner};

/// Apply commits until the driver stops.
pub(super) async fn run_applier(
    inner: Arc<Inner>,
    node: Arc<RaftNode>,
    mut rx: mpsc::UnboundedReceiver<Applied>,
) {
    let mut since_snapshot: HashMap<String, u64> = HashMap::new();
    while let Some(item) = rx.recv().await {
        match item {
            Applied::Entries { group, items } => {
                for (entry, mut waiter) in items {
                    // A failed state-machine write must block later entries and compaction.
                    // The durable Raft entry is retried in order; the publisher is told it failed.
                    let mut delay = std::time::Duration::from_millis(100);
                    loop {
                        match apply_entry(&inner, &group, &entry).await {
                            Ok(()) => break,
                            Err(err) => {
                                warn!(group, index = entry.index, error = %err, "raft apply failed; retrying before advancing");
                                if let Some(waiter) = waiter.take() {
                                    match waiter {
                                        Waiter::Local(reply) => {
                                            let _ = reply.send(Err(err.to_string()));
                                        }
                                        Waiter::Remote { from, rid } => node.reply_remote(
                                            &from,
                                            &group,
                                            &rid,
                                            false,
                                            Some(&err.to_string()),
                                        ),
                                    }
                                }
                                tokio::time::sleep(delay).await;
                                delay = (delay * 2).min(std::time::Duration::from_secs(1));
                            }
                        }
                    }
                    match waiter {
                        Some(Waiter::Local(reply)) => {
                            let _ = reply.send(Ok(()));
                        }
                        Some(Waiter::Remote { from, rid }) => {
                            node.reply_remote(&from, &group, &rid, true, None)
                        }
                        None => {}
                    }
                    let n = since_snapshot.entry(group.clone()).or_insert(0);
                    *n += 1;
                    if *n >= compact_after(&group) {
                        *n = 0;
                        node.compact(&group, entry.index, group_state(&inner, &group).await);
                    }
                }
            }
            Applied::Install { group, snapshot } => {
                info!(group, index = snapshot.index, "raft snapshot installed");
                since_snapshot.insert(group.clone(), 0);
                loop {
                    let result = if group == META {
                        apply_snapshot(&inner, &snapshot.state).await;
                        Ok(())
                    } else if snapshot.state.get("stream").is_some() {
                        if let Some(key) = qgroups::key_of_group(&inner, &group) {
                            qgroups::install_stream(&inner, &key, &snapshot.state).await
                        } else {
                            Err(Error::Unavailable(format!(
                                "snapshot group {group} has no queue"
                            )))
                        }
                    } else {
                        install_quorum(&inner, &group, &snapshot.state).await
                    };
                    match result {
                        Ok(()) => break,
                        Err(err) => {
                            warn!(group, error = %err, "raft snapshot apply failed; retrying before advancing");
                            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                        }
                    }
                }
            }
            Applied::Leader { group, leader } => {
                if group == QUORUM {
                    set_quorum_leader(&inner, leader).await;
                } else if group != META {
                    qgroups::set_group_leader(&inner, &group, leader).await;
                }
            }
        }
    }
}

async fn apply_entry(inner: &Arc<Inner>, group: &str, entry: &Entry) -> Result<(), Error> {
    if group == META {
        apply_meta(inner, entry).await;
        Ok(())
    } else if entry.kind == "sappend" {
        qgroups::apply_sappend(inner, entry).await
    } else {
        apply_quorum(inner, entry).await
    }
}

async fn apply_meta(inner: &Arc<Inner>, entry: &Entry) {
    if entry.kind == "noop" || entry.kind == "config" {
        return;
    }
    apply_one(inner, &entry.kind, &entry.data).await;
}

async fn apply_quorum(inner: &Arc<Inner>, entry: &Entry) -> Result<(), Error> {
    tracing::debug!(index = entry.index, kind = %entry.kind, id = %json_str(&entry.data, "message_id"), ids = ?entry.data.get("ids"), "raft quorum apply");
    match entry.kind.as_str() {
        "enq" => {
            let mut data = entry.data.clone();
            data["_qf_order"] = json!(entry.index);
            apply_enq(inner, &data).await?;
        }
        "drop" => {
            let key = QueueKey::new(
                json_str(&entry.data, "vhost"),
                json_str(&entry.data, "queue"),
            );
            let mut ids: Vec<String> = entry
                .data
                .get("ids")
                .and_then(|v| v.as_array())
                .map(|ids| {
                    ids.iter()
                        .filter_map(|v| v.as_str().map(str::to_string))
                        .collect()
                })
                .unwrap_or_default();
            if ids.is_empty() {
                let id = json_str(&entry.data, "id");
                if !id.is_empty() {
                    ids.push(id);
                }
            }
            let leader = is_leader(inner, &key);
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
            let key = QueueKey::new(
                json_str(&entry.data, "vhost"),
                json_str(&entry.data, "queue"),
            );
            let prefix = format!("{}\0{}\0", key.vhost, key.name);
            inner
                .quorum_live
                .lock()
                .await
                .retain(|k, _| !k.starts_with(&prefix));
            inner
                .replicas
                .lock()
                .await
                .retain(|k, _| !k.starts_with(&prefix));
            if !is_leader(inner, &key) {
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
    Ok(())
}

/// Store a committed quorum message on this member, once.
async fn apply_enq(inner: &Arc<Inner>, data: &Value) -> Result<(), Error> {
    let (key, message) = decode_quorum_append(data).map_err(Error::PreconditionFailed)?;
    let id = message.message_id.clone().unwrap_or_default();
    let rk = replica_key(&key, id.as_str());
    if inner.quorum_live.lock().await.contains_key(&rk) {
        return Ok(());
    }
    let Some(handle) = inner.queues.get(&key) else {
        inner
            .replicas
            .lock()
            .await
            .insert(rk.clone(), Arc::new(message));
        inner.quorum_live.lock().await.insert(rk, data.clone());
        return Ok(());
    };
    // A replay after recovery replaces the body's old local offset exactly once.
    local_forget(inner, &key, id.as_str()).await?;
    let (reply_tx, reply_rx) = oneshot::channel();
    handle
        .tx
        .send(QueueCmd::Enqueue {
            msg: Arc::new(message),
            reply: reply_tx,
        })
        .await
        .map_err(|_| Error::Unavailable(format!("queue {key} is down")))?;
    let _completion = reply_rx
        .await
        .map_err(|_| Error::Unavailable(format!("queue {key} enqueue reply was lost")))??;
    // The durable Raft log owns the body; local queue-log fsync is not awaited here.
    inner.quorum_live.lock().await.insert(rk, data.clone());
    Ok(())
}

/// Whether `group` holds the quorum message under replica key `rk`.
fn holds(inner: &Inner, group: &str, rk: &str) -> bool {
    let mut parts = rk.split('\0');
    let key = QueueKey::new(parts.next().unwrap_or(""), parts.next().unwrap_or(""));
    qgroups::quorum_group_of(inner, &key) == group
}

/// Replace the messages `group` holds with a leader's snapshot.
async fn install_quorum(inner: &Arc<Inner>, group: &str, state: &Value) -> Result<(), Error> {
    let queues = state
        .get("queues")
        .and_then(|v| v.as_array())
        .ok_or_else(|| Error::PreconditionFailed("missing quorum snapshot queues".into()))?;
    // Clear all actors belonging to the group, including queues omitted from an empty snapshot.
    // Purge alone leaves unacked deliveries behind, so use a complete replica reset.
    for key in inner.queues.list_keys() {
        if qgroups::quorum_group_of(inner, &key) != group {
            continue;
        }
        let Some(handle) = inner.queues.get(&key) else {
            continue;
        };
        if handle
            .info
            .args
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .queue_type
            != Some(QueueType::Quorum)
        {
            continue;
        }
        let (reply, done) = oneshot::channel();
        handle
            .tx
            .send(QueueCmd::ResetReplicas { reply })
            .await
            .map_err(|_| Error::Unavailable(format!("queue {key} is down")))?;
        done.await
            .map_err(|_| Error::Unavailable(format!("queue {key} reset reply was lost")))??;
    }
    inner
        .quorum_live
        .lock()
        .await
        .retain(|rk, _| !holds(inner, group, rk));
    inner
        .replicas
        .lock()
        .await
        .retain(|rk, _| !holds(inner, group, rk));
    // Array order is the portable snapshot's FIFO sequence. Older Bun snapshots
    // carry no explicit index, so retain their order when this node compacts again.
    let mut order = 0u64;
    for queue in queues {
        for message in queue
            .get("messages")
            .and_then(|v| v.as_array())
            .into_iter()
            .flatten()
        {
            let mut message = message.clone();
            let key = QueueKey::new(json_str(&message, "vhost"), json_str(&message, "queue"));
            if qgroups::quorum_group_of(inner, &key) != group {
                continue;
            }
            order += 1;
            message["_qf_order"] = json!(order);
            apply_enq(inner, &message).await?;
        }
    }
    Ok(())
}

/// The state a snapshot of `group` holds (sections 5 and 6).
async fn group_state(inner: &Arc<Inner>, group: &str) -> Value {
    if group == META {
        return snapshot(inner).await;
    }
    if let Some(key) = qgroups::key_of_group(inner, group) {
        if qgroups::is_stream(inner, &key) {
            return qgroups::stream_state(inner, &key).await;
        }
    }
    let live = inner.quorum_live.lock().await;
    let mut by_queue: HashMap<(String, String), Vec<Value>> = HashMap::new();
    for (rk, data) in live.iter() {
        if !holds(inner, group, rk) {
            continue;
        }
        by_queue
            .entry((json_str(data, "vhost"), json_str(data, "queue")))
            .or_default()
            .push(data.clone());
    }
    let queues: Vec<Value> = by_queue
        .into_iter()
        .map(|((vhost, queue), mut messages)| {
            messages.sort_by_key(|message| {
                message
                    .get("_qf_order")
                    .and_then(Value::as_u64)
                    .unwrap_or(0)
            });
            json!({"vhost": vhost, "queue": queue, "messages": messages})
        })
        .collect();
    json!({"queues": queues})
}

fn is_leader(inner: &Inner, key: &QueueKey) -> bool {
    qgroups::leader_of(inner, key) == inner.node_id
}

/// The `quorum` group's leader leads every quorum queue without a group of its own (section 6).
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

#[cfg(test)]
mod tests {
    use super::super::{encode_quorum_append, ExchangeRouter, Inner, MetadataStore};
    use super::*;
    use queueforge_core::NoopMetaStore;
    use queueforge_core::{MemoryTracker, Message, QueueDeclareOpts, QueueRegistry};
    use std::sync::atomic::AtomicU64;
    use tokio::sync::Mutex;

    fn fixture(dir: &std::path::Path) -> Arc<Inner> {
        Arc::new(Inner {
            node_id: "follower".into(),
            members: std::sync::Mutex::new(vec![]),
            store: Arc::new(MetadataStore::open(dir).unwrap()),
            queues: Arc::new(
                QueueRegistry::new(Arc::new(NoopMetaStore), MemoryTracker::shared())
                    .with_durability(
                        Arc::new(queueforge_store::WalFactory::new(dir.to_path_buf(), 4096)),
                        queueforge_core::DurabilityPolicy {
                            policy: queueforge_core::FsyncPolicy::Always,
                            ..Default::default()
                        },
                    ),
            ),
            router: Arc::new(ExchangeRouter::new()),
            peers: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
            leader: std::sync::Mutex::new("leader".into()),
            replicas: Mutex::new(HashMap::new()),
            consumed: Mutex::new(Default::default()),
            append_inflight: Default::default(),
            heard: Default::default(),
            dial_fails: Default::default(),
            down: Default::default(),
            catchup: Default::default(),
            raft: Default::default(),
            raft_peers: Default::default(),
            raft_auto_peers: Default::default(),
            quorum_live: Mutex::new(HashMap::new()),
            qgroups: Default::default(),
            qleaders: Default::default(),
            raft_qgroup_peers: Default::default(),
            settings: Default::default(),
        })
    }
    async fn queue(inner: &Inner, name: &str, group: &str) -> QueueKey {
        let key = QueueKey::new("/", name);
        let mut opts = QueueDeclareOpts::default();
        opts.args.queue_type = Some(QueueType::Quorum);
        // The pure core fixture does not require a persisted definition.
        opts.durable = true;
        inner.queues.declare("/", name, opts).await.unwrap();
        inner
            .qgroups
            .lock()
            .unwrap()
            .insert(key.clone(), group.into());
        key
    }
    fn enq(key: &QueueKey, index: u64, id: &str) -> Entry {
        let mut msg = Message::blank();
        msg.persistent = true;
        msg.message_id = Some(id.into());
        msg.body = bytes::Bytes::copy_from_slice(id.as_bytes());
        Entry {
            index,
            term: 1,
            kind: "enq".into(),
            data: encode_quorum_append(key, &msg),
        }
    }
    async fn get(
        inner: &Inner,
        key: &QueueKey,
        no_ack: bool,
    ) -> Option<queueforge_core::QueueMessage> {
        let (reply, rx) = oneshot::channel();
        inner
            .queues
            .get(key)
            .unwrap()
            .tx
            .send(QueueCmd::Get { no_ack, reply })
            .await
            .unwrap();
        rx.await.unwrap().map(|(_, qm, _)| qm)
    }
    #[tokio::test]
    async fn quorum_snapshot_preserves_fifo_and_replaces_stale_ready_unacked_and_replicas() {
        let dir = tempfile::tempdir().unwrap();
        let inner = fixture(dir.path());
        let key = queue(&inner, "q", "q:test").await;
        let absent = queue(&inner, "absent", "q:test").await;
        let other = queue(&inner, "other", "q:other").await;
        for i in 1..=24 {
            apply_quorum(&inner, &enq(&key, i, &format!("m{i:02}")))
                .await
                .unwrap();
        }
        let snapshot = group_state(&inner, "q:test").await;
        let ids: Vec<_> = snapshot["queues"][0]["messages"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| json_str(v, "message_id"))
            .collect();
        assert_eq!(
            ids,
            (1..=24).map(|i| format!("m{i:02}")).collect::<Vec<_>>()
        );
        // A retained unacked body must be rebuilt in snapshot order, exactly once.
        assert_eq!(
            get(&inner, &key, false)
                .await
                .unwrap()
                .message
                .message_id
                .as_deref(),
            Some("m01")
        );
        apply_quorum(&inner, &enq(&key, 25, "stale")).await.unwrap();
        apply_quorum(&inner, &enq(&absent, 26, "absent-stale"))
            .await
            .unwrap();
        apply_quorum(&inner, &enq(&other, 1, "other-kept"))
            .await
            .unwrap();
        let ghost = QueueKey::new("/", "ghost");
        inner
            .qgroups
            .lock()
            .unwrap()
            .insert(ghost.clone(), "q:test".into());
        apply_quorum(&inner, &enq(&ghost, 27, "replica-stale"))
            .await
            .unwrap();
        install_quorum(&inner, "q:test", &snapshot).await.unwrap();
        install_quorum(&inner, "q:test", &snapshot).await.unwrap();
        for i in 1..=24 {
            assert_eq!(
                get(&inner, &key, true)
                    .await
                    .unwrap()
                    .message
                    .message_id
                    .as_deref(),
                Some(format!("m{i:02}").as_str())
            );
        }
        assert!(get(&inner, &key, true).await.is_none());
        assert!(get(&inner, &absent, true).await.is_none());
        assert!(inner.replicas.lock().await.is_empty());
        assert_eq!(
            get(&inner, &other, true)
                .await
                .unwrap()
                .message
                .message_id
                .as_deref(),
            Some("other-kept")
        );
        install_quorum(&inner, "q:test", &json!({"queues": []}))
            .await
            .unwrap();
        assert!(group_state(&inner, "q:test").await["queues"]
            .as_array()
            .unwrap()
            .is_empty());
    }
    #[tokio::test]
    async fn snapshot_removed_bodies_do_not_return_after_queue_log_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let inner = fixture(dir.path());
        let key = queue(&inner, "q", "q:test").await;
        let kept = enq(&key, 2, "kept");
        apply_quorum(&inner, &enq(&key, 1, "removed-unacked"))
            .await
            .unwrap();
        assert!(get(&inner, &key, false).await.is_some());
        apply_quorum(&inner, &kept).await.unwrap();
        apply_quorum(&inner, &enq(&key, 3, "removed-ready"))
            .await
            .unwrap();
        install_quorum(
            &inner,
            "q:test",
            &json!({"queues":[{"messages":[kept.data]}]}),
        )
        .await
        .unwrap();
        inner.queues.shutdown_all().await;
        let mut wal = queueforge_store::QueueWal::open(dir.path(), "/", "q", 4096).unwrap();
        let recovered = wal.recover_messages().unwrap();
        assert_eq!(recovered.messages.len(), 1);
        assert_eq!(recovered.messages[0].1.message_id.as_deref(), Some("kept"));
    }
    #[tokio::test]
    async fn stream_apply_error_reaches_the_raft_applier() {
        let dir = tempfile::tempdir().unwrap();
        let inner = fixture(dir.path());
        let entry = Entry {
            index: 1,
            term: 1,
            kind: "sappend".into(),
            data: json!({"vhost":"/", "queue":"missing"}),
        };
        assert!(apply_entry(&inner, "q:missing", &entry).await.is_err());
    }
}
