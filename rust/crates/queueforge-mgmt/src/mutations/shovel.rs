//! Shovel and federation-upstream parameter puts.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use compact_str::CompactString;
use queueforge_core::{QueueCmd, QueueDeclareOpts, QueueKey};
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

/// Running shovels by `vhost` and name, so DELETE can stop one.
static SHOVELS: std::sync::LazyLock<std::sync::Mutex<std::collections::HashMap<(String, String), tokio::task::AbortHandle>>> =
    std::sync::LazyLock::new(Default::default);

/// The vhost a local URI names. `amqp://` with no host means this broker, as
/// in RabbitMQ; a missing URI does too. Returns `None` for a remote URI.
fn local_vhost(uri: Option<&str>, fallback: &str) -> Option<String> {
    let Some(uri) = uri else {
        return Some(fallback.to_string());
    };
    let rest = uri.strip_prefix("amqp://").or_else(|| uri.strip_prefix("amqps://"))?;
    let (host, path) = rest.split_once('/').unwrap_or((rest, ""));
    if !host.is_empty() {
        return None;
    }
    if path.is_empty() {
        return Some(fallback.to_string());
    }
    Some(decode_vhost(path).unwrap_or_else(|_| path.to_string()))
}

/// PUT /api/parameters/shovel/{vhost}/{name}
pub async fn put_shovel(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Path((raw_vhost, name)): Path<(String, String)>,
    Json(body): Json<ShovelPut>,
) -> Result<StatusCode, MgmtError> {
    let session = require_session(&state, &headers).await?;
    require_administrator(&session)?;
    let vhost = decode_vhost(&raw_vhost)?;
    let src = json_str(&body.value, "src-queue")
        .ok_or_else(|| MgmtError::BadRequest("src-queue is required".into()))?;
    let dest = json_str(&body.value, "dest-queue")
        .ok_or_else(|| MgmtError::BadRequest("dest-queue is required".into()))?;
    let src_uri = json_str(&body.value, "src-uri");
    let dest_uri = json_str(&body.value, "dest-uri");
    let task = match (local_vhost(src_uri.as_deref(), &vhost), local_vhost(dest_uri.as_deref(), &vhost)) {
        (Some(src_vhost), Some(dest_vhost)) => {
            let queues = Arc::clone(&state.queues);
            tokio::spawn(local_shovel(queues, src_vhost, src, dest_vhost, dest))
        }
        _ => {
            let (src_uri, dest_uri) = (
                src_uri.unwrap_or_else(|| "amqp://".into()),
                dest_uri.unwrap_or_else(|| "amqp://".into()),
            );
            tokio::spawn(async move {
                if let Err(err) = shovel_link(src_uri, dest_uri, src, dest).await {
                    tracing::warn!(error = %err, "shovel stopped");
                }
            })
        }
    };
    if let Some(old) = SHOVELS
        .lock()
        .expect("shovels poisoned")
        .insert((vhost, name), task.abort_handle())
    {
        old.abort();
    }
    Ok(StatusCode::CREATED)
}

/// DELETE /api/parameters/shovel/{vhost}/{name}
pub async fn delete_shovel(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Path((raw_vhost, name)): Path<(String, String)>,
) -> Result<StatusCode, MgmtError> {
    let session = require_session(&state, &headers).await?;
    require_administrator(&session)?;
    let vhost = decode_vhost(&raw_vhost)?;
    match SHOVELS.lock().expect("shovels poisoned").remove(&(vhost, name.clone())) {
        Some(task) => {
            task.abort();
            Ok(StatusCode::NO_CONTENT)
        }
        None => Err(MgmtError::NotFound(format!("shovel '{name}'"))),
    }
}

/// Move messages between two queues on this broker, keeping their properties.
async fn local_shovel(
    queues: Arc<queueforge_core::QueueRegistry>,
    src_vhost: String,
    src: String,
    dest_vhost: String,
    dest: String,
) {
    let idle = std::time::Duration::from_millis(25);
    loop {
        let Some(handle) = queues.get(&QueueKey::new(&src_vhost, &src)) else {
            tokio::time::sleep(idle).await;
            continue;
        };
        let dest_key = QueueKey::new(&dest_vhost, &dest);
        if queues.get(&dest_key).is_none() {
            let _ = queues.declare(&dest_vhost, &dest, QueueDeclareOpts::default()).await;
        }
        let Some(dest_handle) = queues.get(&dest_key) else {
            tokio::time::sleep(idle).await;
            continue;
        };
        let (tx, rx) = oneshot::channel();
        if handle.tx.send(QueueCmd::Get { no_ack: true, reply: tx }).await.is_err() {
            tokio::time::sleep(idle).await;
            continue;
        }
        let Ok(Some((_, message, _))) = rx.await else {
            tokio::time::sleep(idle).await;
            continue;
        };
        let (reply_tx, reply_rx) = oneshot::channel();
        let msg = Arc::clone(&message.message);
        if dest_handle.tx.send(QueueCmd::Enqueue { msg, reply: reply_tx }).await.is_err() {
            continue;
        }
        if let Ok(Ok(done)) = reply_rx.await {
            let _ = done.durable_done.await;
        }
    }
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
