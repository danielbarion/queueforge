//! Proxy loop for a queue whose home is another node, and consumer subscriptions opened there.

use std::sync::Arc;

use base64::Engine;
use bytes::Bytes;
use compact_str::CompactString;
use queueforge_core::{ConsumerDeliveryId, EnqueueCompletion, Error, Message, Queue, QueueCmd};
use tokio::sync::{mpsc, oneshot};

use super::subscribe::open_subscription;
use super::wire::{message_to_wire, wire_to_message};
use super::{Cluster, Inner, SubOpen, WireMessage, BASE64};

/// Forward mailbox commands for `queue` to its home through `inner`. `rx` is the local proxy mailbox. The loop ends when the mailbox closes.
pub(super) async fn proxy_loop(inner: Arc<Inner>, queue: Queue, mut rx: mpsc::Receiver<QueueCmd>) {
    let home = queue
        .home
        .clone()
        .map(|h| h.to_string())
        .unwrap_or_else(|| inner.node_id.clone());
    let cluster = Cluster {
        inner: Arc::clone(&inner),
    };
    while let Some(cmd) = rx.recv().await {
        match cmd {
            QueueCmd::Enqueue { msg, reply } => {
                let result = cluster
                    .call(
                        &home,
                        "enqueue",
                        serde_json::json!({
                            "vhost": queue.vhost.as_str(),
                            "queue": queue.name.as_str(),
                            "message": message_to_wire(&msg),
                        }),
                    )
                    .await;
                let _ = reply.send(match result {
                    Ok(msg) if msg.ok => {
                        let (done_tx, done_rx) = oneshot::channel();
                        let _ = done_tx.send(Ok(()));
                        Ok(EnqueueCompletion {
                            offset: queueforge_core::QueueOffset(
                                msg.payload["offset"].as_u64().unwrap_or(0),
                            ),
                            durable_done: done_rx,
                        })
                    }
                    Ok(msg) => Err(Error::Unavailable(msg.error)),
                    Err(err) => Err(err),
                });
            }
            QueueCmd::Ack { id, .. } => {
                let _ = cluster
                    .call(
                        &home,
                        "ack",
                        serde_json::json!({
                            "vhost": queue.vhost.as_str(),
                            "queue": queue.name.as_str(),
                            "delivery_id": id.0,
                        }),
                    )
                    .await;
            }
            QueueCmd::Nack { id, requeue } => {
                let _ = cluster
                    .call(
                        &home,
                        "nack",
                        serde_json::json!({
                            "vhost": queue.vhost.as_str(),
                            "queue": queue.name.as_str(),
                            "delivery_id": id.0,
                            "requeue": requeue,
                        }),
                    )
                    .await;
            }
            QueueCmd::AckReport { id, reply } => {
                let _ = cluster
                    .call(
                        &home,
                        "ack",
                        serde_json::json!({
                            "vhost": queue.vhost.as_str(),
                            "queue": queue.name.as_str(),
                            "delivery_id": id.0,
                        }),
                    )
                    .await;
                let _ = reply.send(None);
            }
            QueueCmd::NackReport { id, requeue, reply } => {
                let _ = cluster
                    .call(
                        &home,
                        "nack",
                        serde_json::json!({
                            "vhost": queue.vhost.as_str(),
                            "queue": queue.name.as_str(),
                            "delivery_id": id.0,
                            "requeue": requeue,
                        }),
                    )
                    .await;
                let _ = reply.send(None);
            }
            QueueCmd::Forget { message_id } => {
                let _ = cluster
                    .call(
                        &home,
                        "forget",
                        serde_json::json!({
                            "vhost": queue.vhost.as_str(),
                            "queue": queue.name.as_str(),
                            "message_id": message_id.as_str(),
                        }),
                    )
                    .await;
            }
            QueueCmd::SettleDelivered { id } => {
                let _ = cluster
                    .call(
                        &home,
                        "settle",
                        serde_json::json!({
                            "vhost": queue.vhost.as_str(),
                            "queue": queue.name.as_str(),
                            "delivery_id": id.0,
                        }),
                    )
                    .await;
            }
            QueueCmd::Get { no_ack, reply } => {
                let result = cluster
                    .call(
                        &home,
                        "get",
                        serde_json::json!({
                            "vhost": queue.vhost.as_str(),
                            "queue": queue.name.as_str(),
                            "no_ack": no_ack,
                        }),
                    )
                    .await;
                let mapped = match result {
                    Ok(msg)
                        if msg.ok
                            && (msg.payload.is_null()
                                || msg.payload.get("empty").and_then(|v| v.as_bool())
                                    == Some(true)) =>
                    {
                        Ok(None)
                    }
                    Ok(msg) if msg.ok => {
                        let id =
                            ConsumerDeliveryId(msg.payload["delivery_id"].as_u64().unwrap_or(0));
                        let message = if let Some(nested) = msg.payload.get("message") {
                            wire_to_message(serde_json::from_value(nested.clone()).unwrap_or(
                                WireMessage {
                                    exchange: String::new(),
                                    routing_key: String::new(),
                                    body_b64: String::new(),
                                    persistent: false,
                                    redelivered: false,
                                    content_type: None,
                                    content_encoding: None,
                                    correlation_id: None,
                                    message_id: None,
                                    reply_to: None,
                                    expiration: None,
                                    app_id: None,
                                    user_id: None,
                                    type_: None,
                                    priority: None,
                                    timestamp: None,
                                    expires_unix_ms: None,
                                    headers: Default::default(),
                                },
                            ))
                        } else if let Some(body_b64) =
                            msg.payload.pointer("/msg/body").and_then(|v| v.as_str())
                        {
                            let mut message = Message::blank();
                            if let Ok(body) = BASE64.decode(body_b64.as_bytes()) {
                                message.body = Bytes::from(body);
                            }
                            if let Some(message_id) =
                                msg.payload.pointer("/msg/id").and_then(|v| v.as_str())
                            {
                                if !message_id.is_empty() {
                                    message.message_id = Some(CompactString::from(message_id));
                                }
                            }
                            message.persistent = msg
                                .payload
                                .pointer("/msg/persistent")
                                .and_then(|v| v.as_bool())
                                .unwrap_or(true);
                            message.routing_key = CompactString::from(
                                msg.payload
                                    .pointer("/msg/routingKey")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or(""),
                            );
                            message.exchange = CompactString::from(
                                msg.payload
                                    .pointer("/msg/exchange")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or(""),
                            );
                            message
                        } else {
                            let _ = reply.send(None);
                            continue;
                        };
                        let ready = msg.payload["ready"].as_u64().unwrap_or(0) as u32;
                        Ok(Some((
                            id,
                            queueforge_core::QueueMessage::new(
                                queueforge_core::QueueOffset(0),
                                Arc::new(message),
                            ),
                            ready,
                        )))
                    }
                    Ok(msg) => Err(Error::Unavailable(msg.error)),
                    Err(err) => Err(err),
                };
                match mapped {
                    Ok(value) => {
                        let _ = reply.send(value);
                    }
                    Err(_) => drop(reply),
                }
            }
            QueueCmd::RegisterConsumer {
                session,
                no_ack,
                exclusive,
                priority,
                initial_credit,
                deliver_tx,
                reply,
            } => {
                let opened = open_subscription(
                    &cluster,
                    SubOpen {
                        home: home.clone(),
                        queue: queue.clone(),
                        session,
                        no_ack,
                        exclusive,
                        priority,
                        initial_credit,
                        deliver_tx,
                    },
                )
                .await;
                let _ = reply.send(opened);
            }
            QueueCmd::AddCredit { session, credit } => {
                let _ = cluster
                    .call(
                        &home,
                        "credit",
                        serde_json::json!({
                            "vhost": queue.vhost.as_str(),
                            "queue": queue.name.as_str(),
                            "session": session.0,
                            "credit": credit,
                        }),
                    )
                    .await;
            }
            QueueCmd::SetCredit { session, credit } => {
                let _ = cluster
                    .call(
                        &home,
                        "set_credit",
                        serde_json::json!({
                            "vhost": queue.vhost.as_str(),
                            "queue": queue.name.as_str(),
                            "session": session.0,
                            "credit": credit,
                        }),
                    )
                    .await;
            }
            QueueCmd::UnregisterConsumer {
                session,
                requeue,
                reply,
            } => {
                let _ = cluster
                    .call(
                        &home,
                        "unsub",
                        serde_json::json!({
                            "vhost": queue.vhost.as_str(),
                            "queue": queue.name.as_str(),
                            "session": session.0,
                            "requeue": requeue,
                        }),
                    )
                    .await;
                let _ = reply.send(());
            }
            QueueCmd::RequeueUnacked { sessions, reply } => {
                let _ = cluster
                    .call(
                        &home,
                        "requeue",
                        serde_json::json!({
                            "vhost": queue.vhost.as_str(),
                            "queue": queue.name.as_str(),
                            "sessions": sessions.iter().map(|s| s.0).collect::<Vec<_>>(),
                        }),
                    )
                    .await;
                let _ = reply.send(());
            }
            QueueCmd::Purge { reply } => {
                let result = cluster
                    .call(
                        &home,
                        "purge",
                        serde_json::json!({
                            "vhost": queue.vhost.as_str(),
                            "queue": queue.name.as_str(),
                        }),
                    )
                    .await;
                let _ = reply.send(
                    result
                        .ok()
                        .and_then(|msg| msg.payload["purged"].as_u64())
                        .unwrap_or(0) as u32,
                );
            }
            QueueCmd::Stats { reply } => {
                let result = cluster
                    .call(
                        &home,
                        "stats",
                        serde_json::json!({
                            "vhost": queue.vhost.as_str(),
                            "queue": queue.name.as_str(),
                        }),
                    )
                    .await;
                let stats = result
                    .ok()
                    .map(|msg| queueforge_core::QueueStats {
                        messages_ready: msg.payload["messages_ready"].as_u64().unwrap_or(0) as u32,
                        messages_unacked: msg.payload["messages_unacked"].as_u64().unwrap_or(0)
                            as u32,
                        consumer_count: msg.payload["consumer_count"].as_u64().unwrap_or(0) as u32,
                        max_priority: None,
                        ready_by_priority: None,
                    })
                    .unwrap_or_default();
                let _ = reply.send(stats);
            }
            QueueCmd::SetArgs { args } => {
                let _ = cluster
                    .call(
                        &home,
                        "set-args",
                        serde_json::json!({
                            "vhost": queue.vhost.as_str(),
                            "queue": queue.name.as_str(),
                            "args": args,
                        }),
                    )
                    .await;
            }
            QueueCmd::FlushDurable { reply, .. } => {
                let _ = reply.send(Ok(()));
            }
            QueueCmd::Touch { reply } => {
                let _ = reply.send(());
            }
            QueueCmd::Shutdown { reply } => {
                let _ = reply.send(Ok(()));
            }
            QueueCmd::Deliver { reply, .. } => {
                let _ = reply.send(None);
            }
            QueueCmd::DlxResolved { .. } => {}
        }
    }
}
