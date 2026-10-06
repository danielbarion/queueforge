//! Open a consumer subscription on the queue's home node.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use queueforge_core::{ConsumerDeliveryId, ConsumerSessionId, Error};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::mpsc;

use super::wire::wire_to_message;
use super::{Cluster, Msg, SubOpen, WireMessage};

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
    let members = cluster.inner.member_list();
    let member = members
        .iter()
        .find(|m| m.id == home)
        .ok_or_else(|| Error::Unavailable(format!("unknown home {home}")))?;
    let stream = TcpStream::connect(member.addr)
        .await
        .map_err(|_| Error::Unavailable(format!("cluster peer {home} is down")))?;
    let _ = stream.set_nodelay(true);
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
        // Keep the write half open for the life of the read. Dropping `cmd_tx`
        // half-closes the socket, and Bun then destroys it, so a consumer that
        // attached before the publish never sees a delivery.
        let _writer = cmd_tx;
        while let Ok(Some(line)) = lines.next_line().await {
            let Ok(msg) = serde_json::from_str::<Msg>(&line) else {
                continue;
            };
            if msg.op == "deliver" {
                let mut message_value = msg
                    .payload
                    .get("message")
                    .cloned()
                    .filter(|value| !value.is_null())
                    .unwrap_or_else(|| msg.payload.clone());
                if message_value.get("body_b64").is_none() {
                    if let Some(body) = message_value.get("body").cloned() {
                        message_value["body_b64"] = body;
                    }
                }
                if message_value.get("routing_key").is_none() {
                    if let Some(key) = message_value.get("routingKey").cloned() {
                        message_value["routing_key"] = key;
                    }
                }
                if message_value.get("message_id").is_none() {
                    if let Some(id) = message_value.get("id").cloned() {
                        message_value["message_id"] = id;
                    }
                }
                let Ok(wire) = serde_json::from_value::<WireMessage>(message_value) else {
                    continue;
                };
                if wire.body_b64.is_empty() {
                    continue;
                }
                let delivery = queueforge_core::QueueDelivery {
                    session: ConsumerSessionId(
                        msg.payload["session"].as_u64().unwrap_or(session.0),
                    ),
                    delivery_id: ConsumerDeliveryId(
                        msg.payload["delivery_id"]
                            .as_u64()
                            .or_else(|| msg.payload["id"].as_u64())
                            .or_else(|| {
                                msg.payload["id"]
                                    .as_str()
                                    .and_then(|text| text.parse().ok())
                            })
                            .unwrap_or(0),
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
    Ok(())
}
