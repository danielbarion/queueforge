//! Get messages from a queue through the management API.

use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::Json;
use queueforge_core::{QueueCmd, QueueKey};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use crate::authz::{check_queue_read, require_management_tag, require_session};
use crate::error::MgmtError;
use crate::routes::decode_vhost;
use crate::state::MgmtState;

#[derive(Debug, Deserialize)]
pub struct GetMessagesBody {
    /// Number of messages to get (v1: only 1 supported effectively; max 100).
    #[serde(default = "default_count")]
    count: u32,
    /// `ack_requeue_true` | `ack_requeue_false` | `reject_requeue_true` | `reject_requeue_false`.
    #[serde(default = "default_ackmode")]
    ackmode: String,
    /// `auto` | `base64`.
    #[serde(default = "default_encoding")]
    encoding: String,
    /// Truncate payload preview (bytes); 0 = no truncate.
    #[serde(default)]
    truncate: Option<u32>,
}

fn default_count() -> u32 {
    1
}
fn default_ackmode() -> String {
    "ack_requeue_true".into()
}
fn default_encoding() -> String {
    "auto".into()
}

#[derive(Debug, Serialize)]
pub(crate) struct GottenMessage {
    payload_bytes: usize,
    redelivered: bool,
    exchange: String,
    routing_key: String,
    message_count: u32,
    properties: serde_json::Value,
    payload: String,
    payload_encoding: String,
}

/// POST /api/queues/{vhost}/{name}/get
pub async fn get_messages(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Path((raw_vhost, name)): Path<(String, String)>,
    Json(body): Json<GetMessagesBody>,
) -> Result<Json<Vec<GottenMessage>>, MgmtError> {
    let session = require_session(&state, &headers).await?;
    require_management_tag(&session)?;
    let vhost = decode_vhost(&raw_vhost)?;
    check_queue_read(&state, &session.username, &vhost, &name).await?;

    let handle = state
        .queues
        .get(&QueueKey::new(&vhost, &name))
        .ok_or_else(|| MgmtError::NotFound(format!("queue '{name}'")))?;

    let count = body.count.clamp(1, 100);
    // requeue semantics: ack_requeue_true peeks (get + nack requeue);
    // ack_requeue_false acks (consumes).
    let requeue = match body.ackmode.as_str() {
        "ack_requeue_true" | "reject_requeue_true" => true,
        "ack_requeue_false" | "reject_requeue_false" => false,
        other => {
            return Err(MgmtError::BadRequest(format!("unknown ackmode '{other}'")));
        }
    };

    let mut out = Vec::new();
    for _ in 0..count {
        let (tx, rx) = oneshot::channel();
        // no_ack=false so we can requeue or ack via Nack/Ack.
        handle
            .tx
            .send(QueueCmd::Get {
                no_ack: false,
                reply: tx,
            })
            .await
            .map_err(|_| MgmtError::Internal("queue mailbox closed".into()))?;
        let Some((delivery_id, qm, message_count)) = rx
            .await
            .map_err(|_| MgmtError::Internal("get reply cancelled".into()))?
        else {
            break;
        };

        let payload_bytes = qm.message.body.len();
        let mut payload_slice = qm.message.body.as_ref();
        if let Some(t) = body.truncate {
            if t > 0 {
                payload_slice = &payload_slice[..payload_slice.len().min(t as usize)];
            }
        }
        let (payload, payload_encoding) = match body.encoding.as_str() {
            "base64" => {
                use base64::Engine;
                (
                    base64::engine::general_purpose::STANDARD.encode(payload_slice),
                    "base64".to_string(),
                )
            }
            "auto" | "string" => match std::str::from_utf8(payload_slice) {
                Ok(s) => (s.to_string(), "string".to_string()),
                Err(_) => {
                    use base64::Engine;
                    (
                        base64::engine::general_purpose::STANDARD.encode(payload_slice),
                        "base64".to_string(),
                    )
                }
            },
            other => {
                return Err(MgmtError::BadRequest(format!("unknown encoding '{other}'")));
            }
        };

        let properties = serde_json::json!({
            "content_type": qm.message.content_type.as_ref().map(|s| s.as_str()),
            "content_encoding": qm.message.content_encoding.as_ref().map(|s| s.as_str()),
            "correlation_id": qm.message.correlation_id.as_ref().map(|s| s.as_str()),
            "message_id": qm.message.message_id.as_ref().map(|s| s.as_str()),
            "reply_to": qm.message.reply_to.as_ref().map(|s| s.as_str()),
            "expiration": qm.message.expiration.as_ref().map(|s| s.as_str()),
            "app_id": qm.message.app_id.as_ref().map(|s| s.as_str()),
            "user_id": qm.message.user_id.as_ref().map(|s| s.as_str()),
            "type": qm.message.type_.as_ref().map(|s| s.as_str()),
            "priority": qm.message.priority,
            "timestamp": qm.message.timestamp,
            "delivery_mode": if qm.message.persistent { 2 } else { 1 },
        });

        out.push(GottenMessage {
            payload_bytes,
            redelivered: qm.message.redelivered,
            exchange: qm.message.exchange.to_string(),
            routing_key: qm.message.routing_key.to_string(),
            message_count,
            properties,
            payload,
            payload_encoding,
        });

        if requeue {
            let _ = handle
                .tx
                .send(QueueCmd::Nack {
                    id: delivery_id,
                    requeue: true,
                })
                .await;
        } else {
            let _ = handle
                .tx
                .send(QueueCmd::Ack {
                    id: delivery_id,
                    multiple_to: None,
                })
                .await;
        }
    }

    Ok(Json(out))
}
