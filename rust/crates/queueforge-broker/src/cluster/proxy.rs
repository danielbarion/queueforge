//! Proxy loop for a queue whose home is another node, and consumer subscriptions opened there.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use base64::Engine;
use bytes::Bytes;
use compact_str::CompactString;
use queueforge_core::{
    ConsumerDeliveryId, ConsumerSessionId, EnqueueCompletion, Error, Message, Queue, QueueCmd,
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};

use super::wire::{message_to_wire, wire_to_message};
use super::{Cluster, Inner, Msg, SubOpen, WireMessage, BASE64};

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

/// Open a consumer on the home node described by `open`, using `cluster` for the RPC. Returns the RPC error. Credit and acks stay on this proxy connection.
pub(super) async fn open_subscription(cluster: &Cluster, open: SubOpen) -> Result<(), Error> {
    let SubOpen {
        home,
        queue,
        session,
        no_ack,
        exclusive,
        priority,
        initial_credit,
        deliver_tx,
    } = open;
    let member = cluster
        .inner
        .members
        .iter()
        .find(|m| m.id == home)
        .ok_or_else(|| Error::Unavailable(format!("unknown home {home}")))?;
    let stream = TcpStream::connect(member.addr)
        .await
        .map_err(|_| Error::Unavailable(format!("cluster peer {home} is down")))?;
    let (read, mut write) = stream.into_split();
    let id = cluster.inner.next_id.fetch_add(1, Ordering::Relaxed);
    let line = serde_json::to_string(&Msg {
        id,
        op: "sub".into(),
        ok: false,
        error: String::new(),
        v: 1,
        node_id: cluster.inner.node_id.clone(),
        from: cluster.inner.node_id.clone(),
        kind: String::new(),
        payload: serde_json::json!({
            "vhost": queue.vhost.as_str(),
            "queue": queue.name.as_str(),
            "session": session.0,
            "no_ack": no_ack,
            "exclusive": exclusive,
            "priority": priority,
            "credit": initial_credit,
        }),
    })
    .map_err(|err| Error::Unavailable(err.to_string()))?;
    write
        .write_all(line.as_bytes())
        .await
        .map_err(|_| Error::Unavailable(format!("cluster peer {home} is down")))?;
    write
        .write_all(b"\n")
        .await
        .map_err(|_| Error::Unavailable(format!("cluster peer {home} is down")))?;
    let mut lines = BufReader::new(read).lines();
    let first = lines
        .next_line()
        .await
        .map_err(|_| Error::Unavailable(format!("cluster peer {home} is down")))?
        .ok_or_else(|| Error::Unavailable(format!("cluster peer {home} closed the subscribe")))?;
    let reply: Msg =
        serde_json::from_str(&first).map_err(|err| Error::Unavailable(err.to_string()))?;
    if !reply.ok {
        let text = if reply.error.is_empty() {
            "subscribe rejected".to_string()
        } else {
            reply.error
        };
        return Err(
            if text.contains("already registered") || text.contains("precondition") {
                Error::PreconditionFailed(text)
            } else {
                Error::Unavailable(text)
            },
        );
    }
    let (cmd_tx, mut cmd_rx) = mpsc::channel::<String>(64);
    tokio::spawn(async move {
        while let Some(line) = cmd_rx.recv().await {
            if write.write_all(line.as_bytes()).await.is_err()
                || write.write_all(b"\n").await.is_err()
            {
                break;
            }
        }
    });
    let home_owned = home.to_string();
    tokio::spawn(async move {
        while let Ok(Some(line)) = lines.next_line().await {
            let Ok(msg) = serde_json::from_str::<Msg>(&line) else {
                continue;
            };
            if msg.op == "deliver" {
                let Ok(wire) =
                    serde_json::from_value::<WireMessage>(msg.payload["message"].clone())
                else {
                    continue;
                };
                let delivery = queueforge_core::QueueDelivery {
                    session: ConsumerSessionId(
                        msg.payload["session"].as_u64().unwrap_or(session.0),
                    ),
                    delivery_id: ConsumerDeliveryId(
                        msg.payload["delivery_id"].as_u64().unwrap_or(0),
                    ),
                    message: queueforge_core::QueueMessage::new(
                        queueforge_core::QueueOffset(msg.payload["offset"].as_u64().unwrap_or(0)),
                        Arc::new(wire_to_message(wire)),
                    ),
                    settles_on_write: msg.payload["settles_on_write"].as_bool().unwrap_or(false),
                    server_cancel: false,
                };
                if deliver_tx.send(delivery).await.is_err() {
                    break;
                }
            }
        }
        let _ = home_owned;
    });
    let _ = cmd_tx;
    Ok(())
}
