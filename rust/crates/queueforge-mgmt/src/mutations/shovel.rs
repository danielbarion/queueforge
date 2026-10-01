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
    queueforge_core::federation::add_federation_upstream(downstream, vhost_from_amqp_uri(&uri));
    Ok(StatusCode::CREATED)
}
