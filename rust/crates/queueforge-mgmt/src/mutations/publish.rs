//! Publish one message through the management API.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::Json;
use bytes::Bytes;
use compact_str::CompactString;
use queueforge_core::{Message, QueueCmd, DEFAULT_EXCHANGE_NAME};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use crate::authz::{check_publish, require_management_tag, require_session};
use crate::error::MgmtError;
use crate::routes::decode_vhost;
use crate::state::MgmtState;

use super::{db, ensure_vhost};

#[derive(Debug, Deserialize)]
pub struct PublishBody {
    /// Routing key.
    #[serde(default)]
    routing_key: String,
    /// Message payload.
    payload: String,
    /// `string` (default) or `base64`.
    #[serde(default = "default_payload_encoding")]
    payload_encoding: String,
    /// Optional properties (delivery_mode, content_type, ...).
    #[serde(default)]
    properties: PublishProperties,
}

fn default_payload_encoding() -> String {
    "string".into()
}

#[derive(Debug, Default, Deserialize)]
pub(crate) struct PublishProperties {
    #[serde(default)]
    delivery_mode: Option<u8>,
    #[serde(default)]
    content_type: Option<String>,
    #[serde(default)]
    content_encoding: Option<String>,
    #[serde(default)]
    correlation_id: Option<String>,
    #[serde(default)]
    message_id: Option<String>,
    #[serde(default)]
    reply_to: Option<String>,
    #[serde(default)]
    expiration: Option<String>,
    #[serde(default)]
    app_id: Option<String>,
    #[serde(default)]
    type_: Option<String>,
    #[serde(default)]
    priority: Option<u8>,
    #[serde(default)]
    timestamp: Option<u64>,
}

#[derive(Debug, Serialize)]
pub(crate) struct PublishResponse {
    routed: bool,
}

fn decode_payload(payload: &str, encoding: &str) -> Result<Bytes, MgmtError> {
    match encoding {
        "string" => Ok(Bytes::from(payload.as_bytes().to_vec())),
        "base64" => {
            use base64::Engine;
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(payload.as_bytes())
                .map_err(|e| MgmtError::BadRequest(format!("invalid base64 payload: {e}")))?;
            Ok(Bytes::from(bytes))
        }
        other => Err(MgmtError::BadRequest(format!(
            "unsupported payload_encoding '{other}'"
        ))),
    }
}

/// POST /api/exchanges/{vhost}/{name}/publish
pub async fn publish(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Path((raw_vhost, exchange_name)): Path<(String, String)>,
    Json(body): Json<PublishBody>,
) -> Result<Json<PublishResponse>, MgmtError> {
    let session = require_session(&state, &headers).await?;
    require_management_tag(&session)?;
    let vhost = decode_vhost(&raw_vhost)?;
    ensure_vhost(&state, &vhost).await?;
    // Path may use `amq.default` for the default exchange; accept empty too.
    let exchange = if exchange_name == "amq.default" {
        DEFAULT_EXCHANGE_NAME.to_string()
    } else {
        exchange_name
    };
    check_publish(&state, &session.username, &vhost, &exchange).await?;
    if !state.connections.topic_write_allowed(
        &session.username,
        vhost.as_str(),
        &exchange,
        &body.routing_key,
    ) {
        return Err(MgmtError::Forbidden);
    }

    if !exchange.is_empty() && state.router.get_exchange(&vhost, &exchange).is_none() {
        let vhost_ex = vhost.clone();
        let exchange_name = exchange.clone();
        if let Some(ex) = db(&state, move |s| s.get_exchange(&vhost_ex, &exchange_name))
            .await?
            .as_ref()
        {
            state.router.put_exchange(ex.clone());
        } else {
            return Err(MgmtError::NotFound(format!("exchange '{exchange}'")));
        }
    }

    let route = state
        .router
        .route(&vhost, &exchange, &body.routing_key)
        .map_err(MgmtError::from)?;

    let body_bytes = decode_payload(&body.payload, &body.payload_encoding)?;
    let persistent = body.properties.delivery_mode == Some(2);
    let msg = Arc::new(Message {
        exchange: CompactString::from(if exchange.is_empty() {
            DEFAULT_EXCHANGE_NAME
        } else {
            exchange.as_str()
        }),
        routing_key: CompactString::from(body.routing_key.as_str()),
        body: body_bytes,
        persistent,
        redelivered: false,
        content_type: body
            .properties
            .content_type
            .as_deref()
            .map(CompactString::from),
        content_encoding: body
            .properties
            .content_encoding
            .as_deref()
            .map(CompactString::from),
        correlation_id: body
            .properties
            .correlation_id
            .as_deref()
            .map(CompactString::from),
        message_id: body
            .properties
            .message_id
            .as_deref()
            .map(CompactString::from),
        reply_to: body.properties.reply_to.as_deref().map(CompactString::from),
        expiration: body
            .properties
            .expiration
            .as_deref()
            .map(CompactString::from),
        app_id: body.properties.app_id.as_deref().map(CompactString::from),
        user_id: Some(CompactString::from(session.username.as_str())),
        type_: body.properties.type_.as_deref().map(CompactString::from),
        priority: body.properties.priority,
        timestamp: body.properties.timestamp,
        expires_unix_ms: None,
        headers: Default::default(),
    });

    // Default exchange: missing queue → not found (match AMQP).
    use queueforge_core::ExchangeType as Et;
    let destinations: Vec<_> = if route.kind == Et::Default {
        let key = &route.destinations[0];
        match state.queues.get(key) {
            Some(h) if h.is_available() => vec![h],
            Some(_) => {
                return Err(MgmtError::Internal("queue actor unavailable".into()));
            }
            None => {
                return Err(MgmtError::NotFound(format!("queue '{}'", key.name)));
            }
        }
    } else {
        if route.destinations.is_empty() {
            return Ok(Json(PublishResponse { routed: false }));
        }
        let mut handles = Vec::new();
        for key in &route.destinations {
            if let Some(h) = state.queues.get(key) {
                if h.is_available() {
                    handles.push(h);
                }
            }
        }
        if handles.is_empty() {
            return Ok(Json(PublishResponse { routed: false }));
        }
        handles
    };

    for handle in &destinations {
        let (reply_tx, reply_rx) = oneshot::channel();
        handle
            .tx
            .send(QueueCmd::Enqueue {
                msg: Arc::clone(&msg),
                reply: reply_tx,
            })
            .await
            .map_err(|_| MgmtError::Internal("queue mailbox closed".into()))?;
        let completion = reply_rx
            .await
            .map_err(|_| MgmtError::Internal("enqueue reply cancelled".into()))?
            .map_err(MgmtError::from)?;
        completion
            .durable_done
            .await
            .map_err(|_| MgmtError::Internal("durable_done cancelled".into()))?
            .map_err(MgmtError::from)?;
    }

    Ok(Json(PublishResponse { routed: true }))
}
