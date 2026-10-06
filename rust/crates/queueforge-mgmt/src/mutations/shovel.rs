//! Shovel and federation-upstream parameter puts.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use compact_str::CompactString;
use queueforge_core::{Message, QueueCmd, QueueDeclareOpts, QueueKey};
use tokio::sync::oneshot;

use crate::authz::{require_administrator, require_session};
use crate::error::MgmtError;
use crate::routes::decode_vhost;
use crate::state::MgmtState;

#[derive(serde::Deserialize)]
pub(crate) struct ShovelPut {
    value: serde_json::Map<String, serde_json::Value>,
}

fn json_str(map: &serde_json::Map<String, serde_json::Value>, key: &str) -> Option<String> {
    map.get(key).and_then(|v| v.as_str()).map(|s| s.to_string())
}

fn vhost_from_amqp_uri(uri: &str) -> String {
    let Some(scheme) = uri.find("://") else {
        return "/".into();
    };
    let rest = &uri[scheme + 3..];
    let Some(slash) = rest.find('/') else {
        return "/".into();
    };
    let path = &rest[slash + 1..];
    if path.is_empty() || path.eq_ignore_ascii_case("%2F") {
        "/".into()
    } else {
        path.to_string()
    }
}

/// PUT /api/parameters/shovel/{vhost}/{name}
pub async fn put_shovel(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Json(body): Json<ShovelPut>,
) -> Result<StatusCode, MgmtError> {
    let session = require_session(&state, &headers).await?;
    require_administrator(&session)?;
    let src = json_str(&body.value, "src-queue")
        .ok_or_else(|| MgmtError::BadRequest("src-queue is required".into()))?;
    let dest = json_str(&body.value, "dest-queue")
        .ok_or_else(|| MgmtError::BadRequest("dest-queue is required".into()))?;
    if let (Some(src_uri), Some(dest_uri)) = (
        json_str(&body.value, "src-uri"),
        json_str(&body.value, "dest-uri"),
    ) {
        tokio::spawn(async move {
            if let Err(err) = shovel_link(src_uri, dest_uri, src, dest).await {
                tracing::warn!(error = %err, "shovel stopped");
            }
        });
        return Ok(StatusCode::CREATED);
    }
    let queues = Arc::clone(&state.queues);
    tokio::spawn(async move {
        loop {
            let key = QueueKey::new("/", &src);
            let Some(handle) = queues.get(&key) else {
                tokio::time::sleep(std::time::Duration::from_millis(30)).await;
                continue;
            };
            let (tx, rx) = oneshot::channel();
            if handle
                .tx
                .send(QueueCmd::Get {
                    no_ack: true,
                    reply: tx,
                })
                .await
                .is_err()
            {
                break;
            }
            let Ok(Some((_, message, _))) = rx.await else {
                tokio::time::sleep(std::time::Duration::from_millis(30)).await;
                continue;
            };
            let body = message.message.body.clone();
            let dest_key = QueueKey::new("/", &dest);
            if queues.get(&dest_key).is_none() {
                let _ = queues
                    .declare("/", &dest, QueueDeclareOpts::default())
                    .await;
            }
            let Some(dest_handle) = queues.get(&dest_key) else {
                continue;
            };
            let mut msg = Message::blank();
            msg.routing_key = CompactString::from(dest.as_str());
            msg.body = body;
            let (reply_tx, reply_rx) = oneshot::channel();
            if dest_handle
                .tx
                .send(QueueCmd::Enqueue {
                    msg: Arc::new(msg),
                    reply: reply_tx,
                })
                .await
                .is_err()
            {
                break;
            }
            let _ = reply_rx.await;
        }
    });
    Ok(StatusCode::CREATED)
}

/// PUT /api/parameters/federation-upstream/{vhost}/{name}
pub async fn put_federation_upstream(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Path((raw_vhost, _name)): Path<(String, String)>,
    Json(body): Json<ShovelPut>,
) -> Result<StatusCode, MgmtError> {
    let session = require_session(&state, &headers).await?;
    require_administrator(&session)?;
    let downstream = decode_vhost(&raw_vhost)?;
    let uri = json_str(&body.value, "uri").unwrap_or_default();
    queueforge_core::federation::add_federation_upstream(
        downstream.clone(),
        vhost_from_amqp_uri(&uri),
    );
    if uri.contains("://") {
        queueforge_core::federation::add_federation_uri(downstream, uri);
    }
    Ok(StatusCode::CREATED)
}

async fn shovel_link(
    src_uri: String,
    dest_uri: String,
    src_queue: String,
    dest_queue: String,
) -> Result<(), String> {
    use futures_lite::StreamExt;
    use lapin::options::{
        BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, QueueDeclareOptions,
    };
    use lapin::types::FieldTable;
    use lapin::{BasicProperties, Connection, ConnectionProperties};
    let src = Connection::connect(&src_uri, ConnectionProperties::default())
        .await
        .map_err(|err| err.to_string())?;
    let dest = Connection::connect(&dest_uri, ConnectionProperties::default())
        .await
        .map_err(|err| err.to_string())?;
    let src_ch = src.create_channel().await.map_err(|err| err.to_string())?;
    let dest_ch = dest.create_channel().await.map_err(|err| err.to_string())?;
    let declare = QueueDeclareOptions {
        durable: true,
        ..QueueDeclareOptions::default()
    };
    src_ch
        .queue_declare(&src_queue, declare, FieldTable::default())
        .await
        .map_err(|err| err.to_string())?;
    dest_ch
        .queue_declare(&dest_queue, declare, FieldTable::default())
        .await
        .map_err(|err| err.to_string())?;
    let mut consumer = src_ch
        .basic_consume(
            &src_queue,
            "queueforge-shovel",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .map_err(|err| err.to_string())?;
    while let Some(delivery) = consumer.next().await {
        let delivery = delivery.map_err(|err| err.to_string())?;
        dest_ch
            .basic_publish(
                "",
                &dest_queue,
                BasicPublishOptions::default(),
                &delivery.data,
                BasicProperties::default().with_delivery_mode(2),
            )
            .await
            .map_err(|err| err.to_string())?
            .await
            .map_err(|err| err.to_string())?;
        delivery
            .ack(BasicAckOptions::default())
            .await
            .map_err(|err| err.to_string())?;
    }
    Ok(())
}

/// Dial `uri`, consume `pattern` as an exchange name, and enqueue each body on `downstream`.
pub(super) async fn federation_link(
    uri: String,
    downstream: String,
    pattern: String,
    queues: Arc<queueforge_core::QueueRegistry>,
    router: Arc<queueforge_core::ExchangeRouter>,
) -> Result<(), String> {
    let Some(exchange) = exchange_from_pattern(&pattern) else {
        return Ok(());
    };
    use futures_lite::StreamExt;
    use lapin::options::{
        BasicAckOptions, BasicConsumeOptions, ExchangeDeclareOptions, QueueBindOptions,
        QueueDeclareOptions,
    };
    use lapin::types::FieldTable;
    use lapin::{Connection, ConnectionProperties};
    use queueforge_core::{Message, QueueCmd};
    let conn = Connection::connect(&uri, ConnectionProperties::default())
        .await
        .map_err(|err| err.to_string())?;
    let ch = conn.create_channel().await.map_err(|err| err.to_string())?;
    let queue = format!("qf-fed-{exchange}");
    ch.exchange_declare(
        &exchange,
        lapin::ExchangeKind::Topic,
        ExchangeDeclareOptions {
            durable: true,
            ..ExchangeDeclareOptions::default()
        },
        FieldTable::default(),
    )
    .await
    .map_err(|err| err.to_string())?;
    ch.queue_declare(
        &queue,
        QueueDeclareOptions {
            durable: true,
            ..QueueDeclareOptions::default()
        },
        FieldTable::default(),
    )
    .await
    .map_err(|err| err.to_string())?;
    ch.queue_bind(
        &queue,
        &exchange,
        "#",
        QueueBindOptions::default(),
        FieldTable::default(),
    )
    .await
    .map_err(|err| err.to_string())?;
    let mut consumer = ch
        .basic_consume(
            &queue,
            "queueforge-federation",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .map_err(|err| err.to_string())?;
    while let Some(delivery) = consumer.next().await {
        let delivery = delivery.map_err(|err| err.to_string())?;
        let key = delivery.routing_key.as_str();
        let Ok(route) = router.route_publish(&downstream, &exchange, key, &[]) else {
            delivery
                .ack(BasicAckOptions::default())
                .await
                .map_err(|err| err.to_string())?;
            continue;
        };
        for dest in route.destinations {
            let Some(handle) = queues.get(&dest) else {
                continue;
            };
            let mut msg = Message::blank();
            msg.routing_key = compact_str::CompactString::from(key);
            msg.body = bytes::Bytes::from(delivery.data.clone());
            msg.persistent = true;
            let (tx, rx) = tokio::sync::oneshot::channel();
            if handle
                .tx
                .send(QueueCmd::Enqueue {
                    msg: std::sync::Arc::new(msg),
                    reply: tx,
                })
                .await
                .is_err()
            {
                continue;
            }
            let _ = rx.await;
        }
        delivery
            .ack(BasicAckOptions::default())
            .await
            .map_err(|err| err.to_string())?;
    }
    Ok(())
}

fn exchange_from_pattern(pattern: &str) -> Option<String> {
    let body = pattern.strip_prefix('^')?.strip_suffix('$')?;
    let name = body.replace("\\.", ".");
    if name.is_empty() || name.contains(['*', '+', '?', '(', '[', '|']) {
        return None;
    }
    Some(name)
}
