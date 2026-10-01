//! Inbound cluster RPC: route one request to a local queue or the metadata store.

use std::sync::Arc;

use compact_str::CompactString;
use queueforge_core::{
    ConsumerDeliveryId, ConsumerSessionId, Error, QueueCmd, QueueDeclareOpts, QueueKey,
};
use queueforge_store::MetadataStore;
use serde_json::Value;
use tokio::sync::{mpsc, oneshot};

use super::forward::{apply_one, forward_nowait};
use super::quorum::flush_queue;
use super::state::remember_consumed;
use super::wire::{decode_quorum_append, json_str, key_from, message_to_wire, replica_key};
use super::{Cluster, Inner, Msg};

/// Handle one inbound `msg` for `inner`, using `peer_tx` to answer on the socket. Returns the response message. Handler failures come back as `ok: false`, not as a dropped socket.
pub(super) async fn dispatch(inner: &Arc<Inner>, msg: &Msg, peer_tx: mpsc::Sender<String>) -> Msg {
    let result = dispatch_op(inner, msg, peer_tx).await;
    match result {
        Ok(payload) => Msg {
            id: msg.id,
            op: "reply".into(),
            ok: true,
            error: String::new(),
            payload,
            v: 1,
            node_id: inner.node_id.clone(),
            from: inner.node_id.clone(),
            kind: String::new(),
        },
        Err(err) => Msg {
            id: msg.id,
            op: "reply".into(),
            ok: false,
            error: err.to_string(),
            v: 1,
            node_id: inner.node_id.clone(),
            from: inner.node_id.clone(),
            kind: String::new(),
            payload: Value::Null,
        },
    }
}

/// Run the operation inside `msg`. `inner` is this node and `peer_tx` can push a follow-up frame. Returns the JSON result or the handler error.
pub(super) async fn dispatch_op(
    inner: &Arc<Inner>,
    msg: &Msg,
    peer_tx: mpsc::Sender<String>,
) -> Result<Value, Error> {
    match msg.op.as_str() {
        "apply" => {
            let nested = msg
                .payload
                .get("kind")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let kind = if msg.kind.is_empty() {
                nested
            } else {
                msg.kind.as_str()
            };
            let body = msg
                .payload
                .get("body")
                .cloned()
                .filter(|value| !value.is_null())
                .unwrap_or_else(|| msg.payload.clone());
            apply_one(&Arc::clone(inner), kind, &body).await;
            Ok(Value::Null)
        }
        "declare" | "declare_queue" => {
            let vhost = json_str(&msg.payload, "vhost");
            let queued = json_str(&msg.payload, "queue");
            let name = if queued.is_empty() {
                json_str(&msg.payload, "name")
            } else {
                queued
            };
            let mut opts = QueueDeclareOpts {
                durable: msg
                    .payload
                    .get("durable")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false),
                exclusive: msg
                    .payload
                    .get("exclusive")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false),
                auto_delete: msg
                    .payload
                    .get("auto_delete")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false),
                passive: msg
                    .payload
                    .get("passive")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false),
                exclusive_owner: None,
                args: serde_json::from_value(
                    msg.payload.get("args").cloned().unwrap_or(Value::Null),
                )
                .unwrap_or_default(),
                declared_args: None,
                home: Some(CompactString::from(inner.node_id.as_str())),
            };
            if let Some(home) = msg.payload.get("home").and_then(|v| v.as_str()) {
                opts.home = Some(CompactString::from(home));
            }
            if msg
                .payload
                .get("args")
                .and_then(|args| args.get("x-queue-type"))
                .and_then(|kind| kind.as_str())
                == Some("quorum")
            {
                opts.args.queue_type = Some(queueforge_core::QueueType::Quorum);
            }
            let result = inner.queues.declare(&vhost, &name, opts).await?;
            let mut queue = result.handle.info.to_domain();
            queue.home = Some(CompactString::from(inner.node_id.as_str()));
            if queue.durable {
                let stored = queue.clone();
                let _ = MetadataStore::blocking(Arc::clone(&inner.store), move |store| {
                    if store
                        .get_queue(stored.vhost.as_str(), stored.name.as_str())?
                        .is_some()
                    {
                        store.put_queue(&stored)
                    } else {
                        store.create_queue(&stored)
                    }
                })
                .await;
            }
            Ok(serde_json::json!({"queue": queue}))
        }
        "forget" | "quorum_drop" => {
            let key = QueueKey::new(
                json_str(&msg.payload, "vhost"),
                json_str(&msg.payload, "queue"),
            );
            let message_id = {
                let id = json_str(&msg.payload, "message_id");
                CompactString::from(if id.is_empty() {
                    json_str(&msg.payload, "id")
                } else {
                    id
                })
            };
            inner
                .replicas
                .lock()
                .await
                .remove(&replica_key(&key, message_id.as_str()));
            remember_consumed(&inner, &key, message_id.as_str()).await;
            let handle = inner
                .queues
                .get(&key)
                .ok_or_else(|| Error::Unavailable(format!("queue {key} is not local")))?;
            handle
                .tx
                .send(QueueCmd::Forget { message_id })
                .await
                .map_err(|_| Error::Unavailable(format!("queue {key} is down")))?;
            let (tx, rx) = oneshot::channel();
            handle
                .tx
                .send(QueueCmd::Touch { reply: tx })
                .await
                .map_err(|_| Error::Unavailable(format!("queue {key} is down")))?;
            let _ = rx.await;
            Ok(Value::Null)
        }
        "enqueue" | "quorum_append" => {
            let (key, message) =
                decode_quorum_append(&msg.payload).map_err(|err| Error::Unavailable(err))?;
            let handle = inner
                .queues
                .get(&key)
                .ok_or_else(|| Error::Unavailable(format!("queue {key} is not local")))?;
            // Leader and follower both append before the peer is acked. every_n_ms
            // completes durable_done after that write; the fsync stays on the timer.
            let (reply_tx, reply_rx) = oneshot::channel();
            handle
                .tx
                .send(QueueCmd::Enqueue {
                    msg: Arc::new(message),
                    reply: reply_tx,
                })
                .await
                .map_err(|_| Error::Unavailable(format!("queue {key} is down")))?;
            let completion = reply_rx
                .await
                .map_err(|_| Error::Unavailable(format!("queue {key} is down")))??;
            let _ = completion.durable_done.await;
            flush_queue(&inner, &key, completion.offset).await?;
            Ok(serde_json::json!({"offset": completion.offset.0}))
        }
        "ack" => {
            let key = key_from(&msg.payload);
            forward_nowait(
                &inner.queues,
                &key,
                QueueCmd::Ack {
                    id: ConsumerDeliveryId(msg.payload["delivery_id"].as_u64().unwrap_or(0)),
                    multiple_to: None,
                },
            )
            .await
        }
        "nack" => {
            let key = key_from(&msg.payload);
            forward_nowait(
                &inner.queues,
                &key,
                QueueCmd::Nack {
                    id: ConsumerDeliveryId(msg.payload["delivery_id"].as_u64().unwrap_or(0)),
                    requeue: msg.payload["requeue"].as_bool().unwrap_or(true),
                },
            )
            .await
        }
        "set-args" => {
            let key = key_from(&msg.payload);
            let args =
                serde_json::from_value(msg.payload.get("args").cloned().unwrap_or(Value::Null))
                    .unwrap_or_default();
            forward_nowait(&inner.queues, &key, QueueCmd::SetArgs { args }).await
        }
        "settle" => {
            let key = key_from(&msg.payload);
            forward_nowait(
                &inner.queues,
                &key,
                QueueCmd::SettleDelivered {
                    id: ConsumerDeliveryId(msg.payload["delivery_id"].as_u64().unwrap_or(0)),
                },
            )
            .await
        }
        "stats" => {
            let key = key_from(&msg.payload);
            let handle = inner
                .queues
                .get(&key)
                .ok_or_else(|| Error::Unavailable(format!("queue {key} is not local")))?;
            let (tx, rx) = oneshot::channel();
            handle
                .tx
                .send(QueueCmd::Stats { reply: tx })
                .await
                .map_err(|_| Error::Unavailable(format!("queue {key} is down")))?;
            let stats = rx
                .await
                .map_err(|_| Error::Unavailable(format!("queue {key} is down")))?;
            Ok(serde_json::json!({
                "messages_ready": stats.messages_ready,
                "messages_unacked": stats.messages_unacked,
                "consumer_count": stats.consumer_count,
            }))
        }
        "get" => {
            let key = key_from(&msg.payload);
            let handle = inner
                .queues
                .get(&key)
                .ok_or_else(|| Error::Unavailable(format!("queue {key} is not local")))?;
            let (tx, rx) = oneshot::channel();
            handle
                .tx
                .send(QueueCmd::Get {
                    no_ack: msg.payload["no_ack"].as_bool().unwrap_or(false),
                    reply: tx,
                })
                .await
                .map_err(|_| Error::Unavailable(format!("queue {key} is down")))?;
            match rx
                .await
                .map_err(|_| Error::Unavailable(format!("queue {key} is down")))?
            {
                Some((id, qm, ready)) => Ok(serde_json::json!({
                    "delivery_id": id.0,
                    "message": message_to_wire(&qm.message),
                    "ready": ready,
                })),
                None => Ok(Value::Null),
            }
        }
        "sub" => {
            let key = key_from(&msg.payload);
            let handle = inner
                .queues
                .get(&key)
                .ok_or_else(|| Error::Unavailable(format!("queue {key} is not local")))?;
            let session = ConsumerSessionId(msg.payload["session"].as_u64().unwrap_or(0));
            let credit = match msg.payload.get("credit") {
                Some(Value::Number(n)) => Some(n.as_u64().unwrap_or(0) as u32),
                _ => None,
            };
            let (deliver_tx, mut deliver_rx) = mpsc::channel(256);
            let (reply_tx, reply_rx) = oneshot::channel();
            handle
                .tx
                .send(QueueCmd::RegisterConsumer {
                    session,
                    no_ack: msg.payload["no_ack"].as_bool().unwrap_or(false),
                    exclusive: msg.payload["exclusive"].as_bool().unwrap_or(false),
                    priority: msg.payload["priority"].as_i64().unwrap_or(0) as i32,
                    initial_credit: credit,
                    deliver_tx,
                    reply: reply_tx,
                })
                .await
                .map_err(|_| Error::Unavailable(format!("queue {key} is down")))?;
            reply_rx
                .await
                .map_err(|_| Error::Unavailable(format!("queue {key} is down")))??;
            let sub_id = msg.id;
            let cluster = Cluster {
                inner: Arc::clone(&inner),
            };
            let forget_key = key.clone();
            tokio::spawn(async move {
                while let Some(delivery) = deliver_rx.recv().await {
                    if let Some(message_id) = delivery.message.message.message_id.clone() {
                        cluster
                            .quorum_forget(&forget_key, message_id.as_str())
                            .await;
                    }
                    let line = serde_json::to_string(&Msg {
                        id: sub_id,
                        op: "deliver".into(),
                        ok: true,
                        error: String::new(),
                        v: 1,
                        node_id: cluster.inner.node_id.clone(),
                        from: cluster.inner.node_id.clone(),
                        kind: String::new(),
                        payload: serde_json::json!({
                            "session": delivery.session.0,
                            "delivery_id": delivery.delivery_id.0,
                            "offset": delivery.message.offset.0,
                            "settles_on_write": delivery.settles_on_write,
                            "message": message_to_wire(&delivery.message.message),
                        }),
                    })
                    .unwrap_or_default();
                    if peer_tx.send(line).await.is_err() {
                        break;
                    }
                }
            });
            Ok(Value::Null)
        }
        "credit" => {
            let key = key_from(&msg.payload);
            forward_nowait(
                &inner.queues,
                &key,
                QueueCmd::AddCredit {
                    session: ConsumerSessionId(msg.payload["session"].as_u64().unwrap_or(0)),
                    credit: msg.payload["credit"].as_u64().unwrap_or(0) as u32,
                },
            )
            .await
        }
        "set_credit" => {
            let key = key_from(&msg.payload);
            let credit = match msg.payload.get("credit") {
                Some(Value::Number(n)) => Some(n.as_u64().unwrap_or(0) as u32),
                _ => None,
            };
            forward_nowait(
                &inner.queues,
                &key,
                QueueCmd::SetCredit {
                    session: ConsumerSessionId(msg.payload["session"].as_u64().unwrap_or(0)),
                    credit,
                },
            )
            .await
        }
        "unsub" => {
            let key = key_from(&msg.payload);
            let handle = inner
                .queues
                .get(&key)
                .ok_or_else(|| Error::Unavailable(format!("queue {key} is not local")))?;
            let (tx, rx) = oneshot::channel();
            handle
                .tx
                .send(QueueCmd::UnregisterConsumer {
                    session: ConsumerSessionId(msg.payload["session"].as_u64().unwrap_or(0)),
                    requeue: msg.payload["requeue"].as_bool().unwrap_or(true),
                    reply: tx,
                })
                .await
                .map_err(|_| Error::Unavailable(format!("queue {key} is down")))?;
            let _ = rx.await;
            Ok(Value::Null)
        }
        "requeue" => {
            let key = key_from(&msg.payload);
            let sessions = msg
                .payload
                .get("sessions")
                .and_then(|v| v.as_array())
                .map(|items| {
                    items
                        .iter()
                        .filter_map(|v| v.as_u64())
                        .map(ConsumerSessionId)
                        .collect()
                })
                .unwrap_or_default();
            let handle = inner
                .queues
                .get(&key)
                .ok_or_else(|| Error::Unavailable(format!("queue {key} is not local")))?;
            let (tx, rx) = oneshot::channel();
            handle
                .tx
                .send(QueueCmd::RequeueUnacked {
                    sessions,
                    reply: tx,
                })
                .await
                .map_err(|_| Error::Unavailable(format!("queue {key} is down")))?;
            let _ = rx.await;
            Ok(Value::Null)
        }
        "delete" => {
            let key = key_from(&msg.payload);
            let count = inner
                .queues
                .delete(
                    &key,
                    msg.payload["if_unused"].as_bool().unwrap_or(false),
                    msg.payload["if_empty"].as_bool().unwrap_or(false),
                )
                .await?;
            Ok(serde_json::json!({"message_count": count}))
        }
        "purge" => {
            let key = key_from(&msg.payload);
            let handle = inner
                .queues
                .get(&key)
                .ok_or_else(|| Error::Unavailable(format!("queue {key} is not local")))?;
            let (tx, rx) = oneshot::channel();
            handle
                .tx
                .send(QueueCmd::Purge { reply: tx })
                .await
                .map_err(|_| Error::Unavailable(format!("queue {key} is down")))?;
            let n = rx
                .await
                .map_err(|_| Error::Unavailable(format!("queue {key} is down")))?;
            Ok(serde_json::json!({"purged": n}))
        }
        other => Err(Error::Unavailable(format!("unknown cluster op {other}"))),
    }
}
