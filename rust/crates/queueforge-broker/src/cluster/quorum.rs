//! Quorum enqueue, consume handoff, and the local durable append.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use compact_str::CompactString;
use queueforge_core::{Error, Message, QueueCmd, QueueHandle, QueueKey, QueueOffset};
use serde_json::Value;
use tokio::sync::{mpsc, oneshot};

use super::proxy::proxy_loop;
use super::state::remember_consumed;
use super::wire::{encode_quorum_append, replica_key, wire_forget};
use super::{Cluster, Inner, Msg};
use crate::quorum_confirm::{durable_majority, MemberCopy};

impl Cluster {
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
            let reply = self
                .call(peer, "quorum_append", encode_quorum_append(key, &message))
                .await;
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
                    let _ = self
                        .call(peer, "forget", wire_forget(key, message_id.as_str()))
                        .await;
                }
                let _ = local_forget(&self.inner, key, message_id.as_str()).await;
                return Err(Error::Unavailable("quorum has no majority".into()));
            }
        };
        if flush_queue(&self.inner, key, local_offset).await.is_err() {
            copies.push(MemberCopy::MemoryOnly);
            for peer in &stored_on {
                let _ = self
                    .call(peer, "forget", wire_forget(key, message_id.as_str()))
                    .await;
            }
            let _ = local_forget(&self.inner, key, message_id.as_str()).await;
            return Err(Error::Unavailable("quorum has no majority".into()));
        }
        copies.push(MemberCopy::Durable);
        if !durable_majority(members, &copies) {
            for peer in &stored_on {
                let _ = self
                    .call(peer, "forget", wire_forget(key, message_id.as_str()))
                    .await;
            }
            let _ = local_forget(&self.inner, key, message_id.as_str()).await;
            return Err(Error::Unavailable("quorum has no majority".into()));
        }
        Ok(())
    }

    /// Drop `message_id` from the local queue `key` when this node has that queue. A missing queue is ignored.
    pub(super) async fn forget_local(&self, key: &QueueKey, message_id: &str) {
        let Some(handle) = self.inner.queues.get(key) else {
            return;
        };
        remember_consumed(&self.inner, key, message_id).await;
        let _ = handle
            .tx
            .send(QueueCmd::Forget {
                message_id: CompactString::from(message_id),
            })
            .await;
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
            self.inner
                .replicas
                .lock()
                .await
                .remove(&replica_key(key, message_id));
            // The follower append is in the ready queue. Drop it here so this
            // node does not deliver the body again after the leader is gone.
            self.forget_local(key, message_id).await;
        }
        self.quorum_forget(key, message_id).await;
    }

    /// Lowest reachable member once a majority is up. That node pushes quorum deliveries.
    pub fn quorum_leader(&self) -> String {
        let slot = self
            .inner
            .leader
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        if !slot.is_empty() {
            return slot.clone();
        }
        let mut ids: Vec<&str> = self
            .inner
            .members
            .iter()
            .map(|member| member.id.as_str())
            .collect();
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

    /// Return the ids of peers with a live RPC socket. This node is not included. An empty list means only the local member can store a copy.
    pub(super) async fn live_peers(&self) -> Vec<String> {
        self.inner
            .peers
            .lock()
            .await
            .keys()
            .filter(|id| !id.starts_with("inbound-") && id.as_str() != self.inner.node_id)
            .cloned()
            .collect()
    }

    /// Replicate `kind` and `payload` to every live peer without waiting for a durable majority. A down peer is skipped.
    pub(super) async fn broadcast_apply(&self, kind: &str, payload: Value) {
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

/// Append `message` to the local queue `key` owned by `inner`. Returns the queue offset after the actor accepts it. The caller fsyncs that offset before counting the copy as durable.
pub(super) async fn local_enqueue(
    inner: &Inner,
    key: &QueueKey,
    message: Arc<Message>,
) -> Result<QueueOffset, Error> {
    let handle = inner
        .queues
        .get(key)
        .ok_or_else(|| Error::Unavailable(format!("queue {key} is not local")))?;
    let (reply_tx, reply_rx) = oneshot::channel();
    handle
        .tx
        .send(QueueCmd::Enqueue {
            msg: message,
            reply: reply_tx,
        })
        .await
        .map_err(|_| Error::Unavailable(format!("queue {key} is down")))?;
    let completion = reply_rx
        .await
        .map_err(|_| Error::Unavailable(format!("queue {key} is down")))??;
    let _ = completion.durable_done.await;
    Ok(completion.offset)
}

/// A peer is a durable copy only when the append reply has `ok: true`.
///
/// [`Cluster::call`] returns `Ok` for any reply that arrived, including a
/// handler failure (`ok: false`). A timeout or a down peer is `Err`.
pub(super) fn peer_append_durable(reply: &Result<Msg, Error>) -> bool {
    matches!(reply, Ok(msg) if msg.ok)
}

/// Fsync this node's queue log through `offset`. Quorum confirms call this. Classic confirms do not.
pub(super) async fn flush_queue(
    inner: &Inner,
    key: &QueueKey,
    offset: QueueOffset,
) -> Result<(), Error> {
    let handle = inner
        .queues
        .get(key)
        .ok_or_else(|| Error::Unavailable(format!("queue {key} is not local")))?;
    let (reply_tx, reply_rx) = oneshot::channel();
    handle
        .tx
        .send(QueueCmd::FlushDurable {
            offset,
            reply: reply_tx,
        })
        .await
        .map_err(|_| Error::Unavailable(format!("queue {key} is down")))?;
    reply_rx
        .await
        .map_err(|_| Error::Unavailable(format!("queue {key} is down")))?
}

/// Remove `message_id` from the local queue `key`. Returns the actor error when the queue is missing or the command fails.
pub(super) async fn local_forget(
    inner: &Inner,
    key: &QueueKey,
    message_id: &str,
) -> Result<(), Error> {
    let Some(handle) = inner.queues.get(key) else {
        return Ok(());
    };
    handle
        .tx
        .send(QueueCmd::Forget {
            message_id: CompactString::from(message_id),
        })
        .await
        .map_err(|_| Error::Unavailable(format!("queue {key} is down")))?;
    let (tx, rx) = oneshot::channel();
    if handle.tx.send(QueueCmd::Touch { reply: tx }).await.is_ok() {
        let _ = rx.await;
    }
    Ok(())
}
