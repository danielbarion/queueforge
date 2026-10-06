//! Quorum enqueue, consume handoff, and the local durable append.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
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
        let members = self.inner.member_list().len().max(1);
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
        // Peers store the body before this node can deliver it. A slow peer must
        // not hold the confirm once a durable majority is already possible.
        // An extra peer already holding EXTRA_APPEND_CAP appends is skipped.
        // Waiting for it, or letting those tasks pile up on this node, stalled
        // confirms that a faster peer had already made durable.
        let payload = encode_quorum_append(key, &message);
        let aborted = Arc::new(AtomicBool::new(false));
        let permits = self.reserve_append_permits(&peers, majority.saturating_sub(1));
        let contacted: Vec<String> = permits.iter().map(|permit| permit.peer.clone()).collect();
        let (tx, mut rx) = mpsc::channel::<(String, Result<Msg, Error>)>(contacted.len().max(1));
        for permit in permits {
            let tx = tx.clone();
            let cluster = Cluster {
                inner: Arc::clone(&self.inner),
            };
            let peer = permit.peer.clone();
            let payload = payload.clone();
            let aborted = Arc::clone(&aborted);
            let forget_key = key.clone();
            let forget_id = message_id.clone();
            tokio::spawn(async move {
                let reply = cluster.call(&peer, "quorum_append", payload).await;
                if aborted.load(Ordering::Relaxed) && peer_append_durable(&reply) {
                    let _ = cluster
                        .call(
                            &peer,
                            "quorum_drop",
                            wire_forget(&forget_key, forget_id.as_str()),
                        )
                        .await;
                }
                drop(permit);
                let _ = tx.send((peer, reply)).await;
            });
        }
        drop(tx);
        // The local fsync and the peer fsync cover the same append. Waiting for
        // the peer to finish before starting the local one added a second
        // group-commit interval to every quorum confirm.
        let mut local = Box::pin(local_enqueue(&self.inner, key, Arc::clone(&message)));
        let mut local_result: Option<Result<QueueOffset, Error>> = None;
        let mut peer_durable = 0usize;
        let mut answered = 0usize;
        let mut peers_closed = contacted.is_empty();
        loop {
            if let Some(result) = &local_result {
                let local_ok = result.is_ok();
                if local_ok && peer_durable + 1 >= majority {
                    return Ok(());
                }
                if !local_ok || peers_closed {
                    break;
                }
            }
            tokio::select! {
                biased;
                result = &mut local, if local_result.is_none() => {
                    local_result = Some(result);
                }
                reply = rx.recv(), if !peers_closed => {
                    match reply {
                        Some((_, result)) => {
                            answered += 1;
                            if peer_append_durable(&result) {
                                peer_durable += 1;
                            }
                            if answered >= contacted.len() {
                                peers_closed = true;
                            }
                        }
                        None => {
                            peers_closed = true;
                        }
                    }
                }
            }
        }
        let local_ok = matches!(local_result, Some(Ok(_)));
        let mut copies = Vec::with_capacity(peer_durable + 1);
        copies.extend(std::iter::repeat(MemberCopy::Durable).take(peer_durable));
        if local_ok {
            copies.push(MemberCopy::Durable);
        }
        if local_ok && durable_majority(members, &copies) {
            return Ok(());
        }
        aborted.store(true, Ordering::Relaxed);
        for peer in &contacted {
            let _ = self
                .call(peer, "quorum_drop", wire_forget(key, message_id.as_str()))
                .await;
        }
        let _ = local_forget(&self.inner, key, message_id.as_str()).await;
        Err(Error::Unavailable("quorum has no majority".into()))
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
        let members = self.inner.member_list();
        let mut ids: Vec<&str> = members.iter().map(|member| member.id.as_str()).collect();
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
    ///
    /// Peers are dropped together. One slow member must not add its own wait
    /// on top of the others, and the call is `quorum_drop` so it cannot be
    /// read as a membership change.
    pub async fn quorum_forget(&self, key: &QueueKey, message_id: &str) {
        let leader = self.quorum_leader();
        let peers = self.live_peers().await;
        let wire = serde_json::json!({
            "vhost": key.vhost.as_str(),
            "queue": key.name.as_str(),
            "message_id": message_id,
            "id": message_id,
        });
        let mut joins = Vec::new();
        for peer in peers {
            if peer == leader {
                continue;
            }
            let cluster = Cluster {
                inner: Arc::clone(&self.inner),
            };
            let wire = wire.clone();
            joins.push(tokio::spawn(async move {
                let _ = cluster.call(&peer, "quorum_drop", wire).await;
            }));
        }
        for join in joins {
            let _ = join.await;
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
    completion
        .durable_done
        .await
        .map_err(|_| Error::Unavailable(format!("queue {key} is down")))??;
    Ok(completion.offset)
}

/// In-flight appends allowed on a peer the majority does not need.
///
/// The peers required to reach a majority are always contacted. A further peer
/// at or above this cap is left out of that publish, so a slow replica cannot
/// fill this node's runtime with append tasks the confirm is not waiting on.
pub(super) const EXTRA_APPEND_CAP: usize = 32;

/// Permits one in-flight quorum append. Dropping it frees the peer's slot.
struct AppendPermit {
    inner: Arc<Inner>,
    peer: String,
}

impl Drop for AppendPermit {
    fn drop(&mut self) {
        let mut inflight = self
            .inner
            .append_inflight
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        if let Some(count) = inflight.get_mut(&self.peer) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                inflight.remove(&self.peer);
            }
        }
    }
}

impl Cluster {
    /// Reserve append slots for this publish.
    ///
    /// `needed` peers are always reserved, lowest in-flight first. Further peers
    /// are reserved only while they are under [`EXTRA_APPEND_CAP`].
    fn reserve_append_permits(&self, peers: &[String], needed: usize) -> Vec<AppendPermit> {
        let mut inflight = self
            .inner
            .append_inflight
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        let chosen = peers_for_quorum_append(peers, &inflight, needed, EXTRA_APPEND_CAP);
        let mut permits = Vec::with_capacity(chosen.len());
        for peer in chosen {
            *inflight.entry(peer.to_string()).or_insert(0) += 1;
            permits.push(AppendPermit {
                inner: Arc::clone(&self.inner),
                peer: peer.to_string(),
            });
        }
        permits
    }
}

/// Choose which peers receive this append.
///
/// `needed` is how many peer copies a local durable copy still requires.
/// Peers are ordered by current in-flight count, then by id. The first `needed`
/// are always returned. Later peers are returned only when their count is below `cap`.
pub(super) fn peers_for_quorum_append<'a>(
    peers: &'a [String],
    inflight: &HashMap<String, usize>,
    needed: usize,
    cap: usize,
) -> Vec<&'a str> {
    let mut ranked: Vec<(usize, &str)> = peers
        .iter()
        .map(|peer| (inflight.get(peer).copied().unwrap_or(0), peer.as_str()))
        .collect();
    ranked.sort_by(|left, right| left.0.cmp(&right.0).then(left.1.cmp(right.1)));
    let mut chosen = Vec::new();
    for (index, (load, peer)) in ranked.into_iter().enumerate() {
        if index < needed || load < cap {
            chosen.push(peer);
        }
    }
    chosen
}

/// A peer is a durable copy only when the append reply has `ok: true`.
///
/// [`Cluster::call`] returns `Ok` for any reply that arrived, including a
/// handler failure (`ok: false`). A timeout or a down peer is `Err`.
pub(super) fn peer_append_durable(reply: &Result<Msg, Error>) -> bool {
    matches!(reply, Ok(msg) if msg.ok)
}

/// Fsync this node's queue log through `offset`. Kept for callers that must
/// force a second sync. Confirm paths wait on `durable_done` instead.
#[allow(dead_code)]
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
