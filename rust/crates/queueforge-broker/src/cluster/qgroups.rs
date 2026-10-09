//! Queue groups: one Raft group per quorum queue or replicated stream
//! (docs/raft.md, sections 2 and 6), as Bun runs them.
//!
//! A cluster runs them once Raft is on and every voter advertises
//! `raft_qgroups`; otherwise every quorum queue shares the `quorum` group
//! and a stream lives on one node. The group is chosen in the same meta
//! entry that creates the queue (`raftGroup` on the row), with
//! `raftLeader` naming the member the leader locator picked: that member
//! campaigns at once. A queue's leader is its group's leader.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Weak};

use base64::Engine;
use compact_str::CompactString;
use queueforge_amqp::{FieldTable, FieldValue};
use queueforge_core::{
    Error, Message, QueueCmd, QueueKey, StreamCopy, StreamEntryCopy, StreamReplicator,
};
use queueforge_store::MetadataStore;
use serde_json::{json, Value};
use tokio::sync::oneshot;
use tracing::info;

use super::raft::Entry;
use super::raft_node::{queue_group, QUORUM};
use super::wire::json_str;
use super::{Inner, BASE64};
use crate::connection::headers::{apply_props_raw, message_props_raw, message_to_properties};

/// `QUEUEFORGE_RAFT_QGROUPS=0` keeps this node on the shared `quorum` group.
pub(super) fn supported() -> bool {
    std::env::var("QUEUEFORGE_RAFT_QGROUPS")
        .map(|v| v != "0")
        .unwrap_or(true)
}

/// The queue's own group, when it has one.
pub(super) fn group_of(inner: &Inner, key: &QueueKey) -> Option<String> {
    inner
        .qgroups
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(key)
        .cloned()
}

/// The group that holds `key`'s messages: its own, or the shared `quorum`.
pub(super) fn quorum_group_of(inner: &Inner, key: &QueueKey) -> String {
    group_of(inner, key).unwrap_or_else(|| QUORUM.to_string())
}

/// The queue a group belongs to.
pub(super) fn key_of_group(inner: &Inner, group: &str) -> Option<QueueKey> {
    inner
        .qgroups
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .find(|(_, g)| g.as_str() == group)
        .map(|(k, _)| k.clone())
}

/// A group's leader as the applier last announced it; empty during an election.
pub(super) fn group_leader(inner: &Inner, group: &str) -> String {
    if group == QUORUM {
        return inner
            .leader
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
    }
    inner
        .qleaders
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(group)
        .cloned()
        .unwrap_or_default()
}

/// The leader of `key`'s group, with Raft on.
pub(super) fn leader_of(inner: &Inner, key: &QueueKey) -> String {
    group_leader(inner, &quorum_group_of(inner, key))
}

/// The group a new quorum queue or stream gets: its own when every voter
/// runs queue groups, else `None` (the shared group, or a one-node stream).
pub(super) fn group_for_new_queue(inner: &Inner, vhost: &str, name: &str) -> Option<String> {
    super::consensus::node(inner)?;
    if !supported() {
        return None;
    }
    let peers = inner
        .raft_qgroup_peers
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let all = inner
        .member_list()
        .iter()
        .all(|m| m.id == inner.node_id || peers.contains(&m.id));
    all.then(|| queue_group(vhost, name))
}

/// The member a new queue group should be led by: this one (`client-local`,
/// the default) or the one leading the fewest queue groups (`balanced`).
pub(super) fn choose_leader(inner: &Inner, locator: Option<&str>) -> String {
    if locator != Some("balanced") {
        return inner.node_id.clone();
    }
    let Some(node) = super::consensus::node(inner) else {
        return inner.node_id.clone();
    };
    let led = node.queue_leaders();
    let mut ids: Vec<String> = inner.member_list().into_iter().map(|m| m.id).collect();
    if ids.is_empty() {
        return inner.node_id.clone();
    }
    ids.sort();
    ids.into_iter()
        .min_by_key(|id| led.get(id).copied().unwrap_or(0))
        .unwrap_or_else(|| inner.node_id.clone())
}

/// Record `key`'s group and start it here. `lead` campaigns at once.
pub(super) fn register(inner: &Inner, key: &QueueKey, group: &str, lead: bool) {
    inner
        .qgroups
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(key.clone(), group.to_string());
    if let Some(node) = super::consensus::node(inner) {
        node.add_group(group, lead);
    }
}

/// The queue was deleted: stop its group and remove its files.
pub(super) fn unregister(inner: &Inner, key: &QueueKey) {
    let group = inner
        .qgroups
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .remove(key);
    if let Some(group) = group {
        inner
            .qleaders
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&group);
        if let Some(node) = super::consensus::node(inner) {
            node.drop_group(&group);
        }
    }
}

/// Wait, up to two elections, for `group` to have a leader: a declare
/// returns once its group has one, as RabbitMQ's does once its Ra cluster
/// started.
pub(super) async fn wait_group_leader(inner: &Inner, group: &str) {
    let deadline = tokio::time::Instant::now()
        + std::time::Duration::from_millis(super::raft::ELECTION_MAX_MS * 2);
    while group_leader(inner, group).is_empty() && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

/// After a restart: the groups of stored queues, before their entries apply.
pub(super) async fn load_stored(inner: &Arc<Inner>) {
    let rows = MetadataStore::blocking(Arc::clone(&inner.store), |store| {
        let mut out = Vec::new();
        for vh in store.list_vhosts()? {
            for q in store.list_queues(vh.name.as_str())? {
                if let Some(group) = q.raft_group.clone() {
                    out.push((
                        QueueKey::new(q.vhost.as_str(), q.name.as_str()),
                        group.to_string(),
                    ));
                }
            }
        }
        Ok(out)
    })
    .await
    .unwrap_or_default();
    for (key, group) in rows {
        register(inner, &key, &group, false);
    }
}

/// The applier saw `group`'s leader change.
pub(super) async fn set_group_leader(inner: &Arc<Inner>, group: &str, leader: Option<String>) {
    let leader = leader.unwrap_or_default();
    let became = {
        let mut leaders = inner.qleaders.lock().unwrap_or_else(|e| e.into_inner());
        let before = leaders
            .insert(group.to_string(), leader.clone())
            .unwrap_or_default();
        leader == inner.node_id && before != inner.node_id
    };
    info!(group, leader, "raft queue leader");
    if became {
        if let Some(key) = key_of_group(inner, group) {
            super::net::promote_replicas_of(inner, Some(&key)).await;
        }
    }
}

/// Commits appends to replicated streams through their groups.
pub(super) struct Streams {
    pub(super) inner: Weak<Inner>,
}

impl StreamReplicator for Streams {
    fn replicated(&self, key: &QueueKey) -> bool {
        self.inner.upgrade().is_some_and(|inner| {
            super::consensus::node(&inner).is_some() && group_of(&inner, key).is_some()
        })
    }

    fn append(
        &self,
        key: QueueKey,
        msg: Arc<Message>,
    ) -> Pin<Box<dyn Future<Output = queueforge_core::Result<()>> + Send>> {
        // Submitted before returning: the stream actor calls this in publish
        // order, and the group must append in that order.
        let submitted = (|| {
            let inner = self
                .inner
                .upgrade()
                .ok_or_else(|| Error::Unavailable("cluster stopped".into()))?;
            let node = super::consensus::node(&inner)
                .cloned()
                .ok_or_else(|| Error::Unavailable("raft is not running".into()))?;
            let group = group_of(&inner, &key)
                .ok_or_else(|| Error::Unavailable(format!("stream {key} has no group")))?;
            Ok::<_, Error>(node.submit(&group, "sappend", encode_sappend(&key, &msg)))
        })();
        Box::pin(async move {
            super::raft_node::RaftNode::settled(submitted?)
                .await
                .map_err(|err| Error::Unavailable(format!("stream append did not commit: {err}")))
        })
    }
}

/// The `sappend` body Bun writes and reads.
pub(super) fn encode_sappend(key: &QueueKey, msg: &Message) -> Value {
    let headers = message_to_properties(msg)
        .headers
        .map(|t| bun_fields(&t))
        .unwrap_or_else(|| json!([]));
    json!({
        "vhost": key.vhost.as_str(),
        "queue": key.name.as_str(),
        "ts": now_ms(),
        "body_b64": BASE64.encode(&msg.body),
        "exchange": msg.exchange.as_str(),
        "routing_key": msg.routing_key.as_str(),
        "headers": headers,
        "propRaw": BASE64.encode(message_props_raw(msg)),
    })
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// A stream entry from its JSON: an `sappend` body or a snapshot entry.
fn decode_entry(data: &Value) -> Message {
    let mut message = Message::blank();
    message.body = BASE64
        .decode(json_str(data, "body_b64").as_bytes())
        .unwrap_or_default()
        .into();
    message.exchange = CompactString::from(json_str(data, "exchange"));
    message.routing_key = CompactString::from(json_str(data, "routing_key"));
    message.persistent = true;
    if let Ok(raw) = BASE64.decode(json_str(data, "propRaw").as_bytes()) {
        apply_props_raw(&mut message, &raw);
    }
    message
}

/// Apply one committed `sappend`: every member appends it in log order.
pub(super) async fn apply_sappend(inner: &Arc<Inner>, entry: &Entry) -> Result<(), Error> {
    let key = QueueKey::new(
        json_str(&entry.data, "vhost"),
        json_str(&entry.data, "queue"),
    );
    let handle = inner
        .queues
        .get(&key)
        .ok_or_else(|| Error::Unavailable(format!("stream {key} is missing")))?;
    let at_ms = entry
        .data
        .get("ts")
        .and_then(|v| v.as_u64())
        .unwrap_or_else(now_ms);
    let (reply, done) = oneshot::channel();
    let msg = Arc::new(decode_entry(&entry.data));
    handle
        .tx
        .send(QueueCmd::StreamApply {
            index: entry.index,
            at_ms,
            msg,
            reply,
        })
        .await
        .map_err(|_| Error::Unavailable(format!("stream {key} is down")))?;
    done.await
        .map_err(|_| Error::Unavailable(format!("stream {key} apply reply was lost")))?
}

/// A replicated stream as its Raft snapshot, in Bun's shape.
pub(super) async fn stream_state(inner: &Arc<Inner>, key: &QueueKey) -> Value {
    let Some(handle) = inner.queues.get(key) else {
        return Value::Null;
    };
    let (reply, rx) = oneshot::channel();
    if handle
        .tx
        .send(QueueCmd::StreamDump { reply })
        .await
        .is_err()
    {
        return Value::Null;
    }
    let copy = rx.await.unwrap_or_default();
    let entries: Vec<Value> = copy
        .entries
        .iter()
        .map(|e| {
            let mut v = encode_sappend(key, &e.message);
            v["offset"] = json!(e.offset);
            v["ts"] = json!(e.at_ms);
            v
        })
        .collect();
    json!({"stream": {
        "vhost": key.vhost.as_str(),
        "queue": key.name.as_str(),
        "first": copy.first,
        "next": copy.next,
        "raftIndex": copy.raft_index,
        "entries": entries,
    }})
}

/// Replace this member's copy of a replicated stream with a snapshot.
pub(super) async fn install_stream(
    inner: &Arc<Inner>,
    key: &QueueKey,
    state: &Value,
) -> Result<(), Error> {
    let snap = state
        .get("stream")
        .ok_or_else(|| Error::PreconditionFailed("missing stream snapshot".into()))?;
    let handle = inner
        .queues
        .get(key)
        .ok_or_else(|| Error::Unavailable(format!("stream {key} is missing")))?;
    let entries: Vec<StreamEntryCopy> = snap
        .get("entries")
        .and_then(|v| v.as_array())
        .into_iter()
        .flatten()
        .map(|e| StreamEntryCopy {
            offset: e.get("offset").and_then(|v| v.as_u64()).unwrap_or(0),
            at_ms: e.get("ts").and_then(|v| v.as_u64()).unwrap_or_else(now_ms),
            message: Arc::new(decode_entry(e)),
        })
        .collect();
    let first = snap
        .get("first")
        .and_then(|v| v.as_u64())
        .or_else(|| entries.first().map(|e| e.offset))
        .unwrap_or(0);
    let copy = StreamCopy {
        first,
        next: snap.get("next").and_then(|v| v.as_u64()).unwrap_or(first),
        raft_index: snap.get("raftIndex").and_then(|v| v.as_u64()).unwrap_or(0),
        entries,
    };
    let (reply, done) = oneshot::channel();
    handle
        .tx
        .send(QueueCmd::StreamInstall { copy, reply })
        .await
        .map_err(|_| Error::Unavailable(format!("stream {key} is down")))?;
    done.await
        .map_err(|_| Error::Unavailable(format!("stream {key} snapshot reply was lost")))?
}

/// RabbitMQ's `leader` and `members` of a quorum queue or replicated stream.
pub(super) fn queue_raft(
    inner: &Arc<Inner>,
    key: &QueueKey,
) -> Option<queueforge_core::flags::QueueRaft> {
    let kind = inner
        .queues
        .get(key)?
        .info
        .args
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .queue_type;
    let own = group_of(inner, key);
    let quorum = kind == Some(queueforge_core::QueueType::Quorum);
    if !quorum && !(kind == Some(queueforge_core::QueueType::Stream) && own.is_some()) {
        return None;
    }
    let group = own.unwrap_or_else(|| QUORUM.to_string());
    let (leader, members) = match super::consensus::node(inner) {
        Some(node) => (group_leader(inner, &group), node.members(&group)),
        None => {
            let cluster = super::Cluster {
                inner: Arc::clone(inner),
            };
            (
                cluster.quorum_leader(),
                inner.member_list().into_iter().map(|m| m.id).collect(),
            )
        }
    };
    let members = if members.is_empty() {
        vec![inner.node_id.clone()]
    } else {
        members
    };
    Some(queueforge_core::flags::QueueRaft {
        leader,
        members,
        group,
    })
}

/// Whether `key` is a stream (its snapshot is a stream copy).
pub(super) fn is_stream(inner: &Inner, key: &QueueKey) -> bool {
    inner.queues.get(key).is_some_and(|h| {
        h.info
            .args
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .queue_type
            == Some(queueforge_core::QueueType::Stream)
    })
}

/// An AMQP table in Bun's JSON form, `[[name, {t, v}], ...]`.
fn bun_fields(table: &FieldTable) -> Value {
    Value::Array(
        table
            .entries
            .iter()
            .map(|(k, v)| json!([k, bun_field(v)]))
            .collect(),
    )
}

fn bun_field(v: &FieldValue) -> Value {
    match v {
        FieldValue::Bool(b) => json!({"t": "t", "v": b}),
        FieldValue::I8(n) => json!({"t": "I", "v": n}),
        FieldValue::U8(n) => json!({"t": "I", "v": n}),
        FieldValue::I16(n) => json!({"t": "I", "v": n}),
        FieldValue::U16(n) => json!({"t": "I", "v": n}),
        FieldValue::I32(n) => json!({"t": "I", "v": n}),
        FieldValue::U32(n) => json!({"t": "l", "v": n}),
        FieldValue::I64(n) => json!({"t": "l", "v": n}),
        FieldValue::U64(n) => json!({"t": "l", "v": n}),
        FieldValue::F32(n) => json!({"t": "S", "v": n.to_string()}),
        FieldValue::F64(n) => json!({"t": "S", "v": n.to_string()}),
        FieldValue::Decimal { scale, value } => {
            json!({"t": "S", "v": (f64::from(*value) / 10f64.powi(i32::from(*scale))).to_string()})
        }
        FieldValue::ShortString(s) => json!({"t": "S", "v": s}),
        FieldValue::LongString(b) | FieldValue::Bytes(b) => {
            json!({"t": "S", "v": String::from_utf8_lossy(b)})
        }
        FieldValue::Array(items) => {
            json!({"t": "A", "v": items.iter().map(bun_field).collect::<Vec<_>>()})
        }
        FieldValue::Timestamp(t) => json!({"t": "T", "v": t}),
        FieldValue::Table(t) => json!({"t": "F", "v": bun_fields(t)}),
        FieldValue::Void => json!({"t": "V"}),
    }
}
