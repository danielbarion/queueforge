//! Queue declare, delete, purge, and argument parsing.

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use queueforge_core::{ArgValue, QueueArgs, QueueCmd, QueueDeclareOpts, QueueKey};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use crate::authz::{check_queue_configure, require_management_tag, require_session};
use crate::error::MgmtError;
use crate::routes::decode_vhost;
use crate::state::MgmtState;

use super::IfUnusedQuery;
use super::{db, ensure_vhost};

// ── Queues ──────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct PutQueueBody {
    #[serde(default)]
    durable: bool,
    #[serde(default)]
    exclusive: bool,
    #[serde(default)]
    auto_delete: bool,
    /// Optional queue declare arguments (`x-message-ttl`, `x-max-length`, DLX, …).
    #[serde(default)]
    arguments: Option<serde_json::Map<String, serde_json::Value>>,
}

/// Parse management JSON queue arguments into [`QueueArgs`].
///
/// Known x- arguments are type-checked; other keys are ignored, as RabbitMQ does.
pub(crate) fn parse_mgmt_queue_args(
    arguments: &Option<serde_json::Map<String, serde_json::Value>>,
) -> Result<QueueArgs, MgmtError> {
    let Some(map) = arguments else {
        return Ok(QueueArgs::default());
    };
    if map.is_empty() {
        return Ok(QueueArgs::default());
    }

    // Hold owned strings so ArgValue::Str can borrow them for the duration of parse.
    let mut owned_strings: Vec<(String, String)> = Vec::new();
    let mut long_pairs: Vec<(String, i64)> = Vec::new();

    for (key, value) in map {
        match value {
            serde_json::Value::Number(n) => {
                let as_i64 = n.as_i64().or_else(|| n.as_u64().map(|u| u as i64));
                match as_i64 {
                    Some(v) => long_pairs.push((key.clone(), v)),
                    None => {
                        return Err(MgmtError::BadRequest(format!(
                            "argument '{key}' must be an integer"
                        )));
                    }
                }
            }
            serde_json::Value::String(s) => owned_strings.push((key.clone(), s.clone())),
            // A JSON boolean, such as x-single-active-consumer: true.
            serde_json::Value::Bool(b) => long_pairs.push((key.clone(), i64::from(*b))),
            // RabbitMQ stores arguments it does not interpret; only known x- keys are typed.
            _ if !is_known_arg(key) => {}
            other => {
                return Err(MgmtError::BadRequest(format!(
                    "argument '{key}' has unsupported JSON type ({})",
                    match other {
                        serde_json::Value::Bool(_) => "bool",
                        serde_json::Value::Array(_) => "array",
                        serde_json::Value::Object(_) => "object",
                        serde_json::Value::Null => "null",
                        _ => "unknown",
                    }
                )));
            }
        }
    }

    let mut pairs: Vec<(&str, ArgValue<'_>)> = Vec::new();
    for (k, v) in &long_pairs {
        pairs.push((k.as_str(), ArgValue::Long(*v)));
    }
    for (k, v) in &owned_strings {
        pairs.push((k.as_str(), ArgValue::Str(v.as_str())));
    }

    QueueArgs::parse(pairs).map_err(|e| match e {
        queueforge_core::Error::PreconditionFailed(msg) => MgmtError::PreconditionFailed(msg),
        other => MgmtError::BadRequest(other.to_string()),
    })
}

/// Whether `QueueArgs::parse` interprets this argument.
fn is_known_arg(key: &str) -> bool {
    matches!(
        key,
        "x-message-ttl"
            | "x-expires"
            | "x-max-length"
            | "x-max-length-bytes"
            | "x-overflow"
            | "x-dead-letter-exchange"
            | "x-dead-letter-routing-key"
            | "x-max-death-hops"
            | "x-max-priority"
            | "x-single-active-consumer"
            | "x-delivery-limit"
            | "x-queue-type"
            | "x-dead-letter-strategy"
            | "x-max-age"
            | "x-queue-leader-locator"
    )
}

#[derive(Debug, Serialize)]
pub(crate) struct QueueCreated {
    name: String,
    vhost: String,
    durable: bool,
    exclusive: bool,
    auto_delete: bool,
}

/// PUT /api/queues/{vhost}/{name}
pub async fn put_queue(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Path((raw_vhost, name)): Path<(String, String)>,
    Json(body): Json<PutQueueBody>,
) -> Result<Response, MgmtError> {
    let session = require_session(&state, &headers).await?;
    require_management_tag(&session)?;
    let vhost = decode_vhost(&raw_vhost)?;
    ensure_vhost(&state, &vhost).await?;
    if name.is_empty() {
        return Err(MgmtError::BadRequest("queue name required".into()));
    }
    if body.exclusive {
        return Err(MgmtError::BadRequest(
            "exclusive queues cannot be declared via management".into(),
        ));
    }
    if !body.durable && !state.connections.transient_nonexcl_permitted() {
        return Err(MgmtError::BadRequest(
            "Feature `transient_nonexcl_queues` is deprecated. By default, this feature is not permitted anymore.".into(),
        ));
    }
    let already = state
        .queues
        .get(&queueforge_core::QueueKey::new(
            vhost.as_str(),
            name.as_str(),
        ))
        .is_some();
    let queues_here = state
        .queues
        .list_keys()
        .into_iter()
        .filter(|key| key.vhost.as_str() == vhost.as_str())
        .count();
    let current = if already {
        queues_here.saturating_sub(1)
    } else {
        queues_here
    };
    if !state.connections.queue_allowed(vhost.as_str(), current) {
        return Err(MgmtError::Forbidden);
    }
    check_queue_configure(&state, &session.username, &vhost, &name).await?;

    let declared = parse_mgmt_queue_args(&body.arguments)?;
    let args = state
        .router
        .queue_args_with_policy(&vhost, &name, &declared);
    let opts = QueueDeclareOpts {
        durable: body.durable,
        exclusive: false,
        auto_delete: body.auto_delete,
        passive: false,
        exclusive_owner: None,
        args,
        declared_args: Some(declared),
        home: None,
    };
    let vhost_q = vhost.clone();
    let name_q = name.clone();
    let existed = state.queues.get(&QueueKey::new(&vhost, &name)).is_some()
        || db(&state, move |s| s.get_queue(&vhost_q, &name_q))
            .await?
            .is_some();
    let result = state.queues.declare(&vhost, &name, opts).await?;
    let status = if existed {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::CREATED
    };
    if status == StatusCode::CREATED {
        Ok((
            status,
            Json(QueueCreated {
                name: result.handle.info.key.name.to_string(),
                vhost: result.handle.info.key.vhost.to_string(),
                durable: result.handle.info.durable,
                exclusive: result.handle.info.exclusive,
                auto_delete: result.handle.info.auto_delete,
            }),
        )
            .into_response())
    } else {
        Ok(status.into_response())
    }
}

/// DELETE /api/queues/{vhost}/{name}
pub async fn delete_queue(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Path((raw_vhost, name)): Path<(String, String)>,
    Query(q): Query<IfUnusedQuery>,
) -> Result<Json<serde_json::Value>, MgmtError> {
    let session = require_session(&state, &headers).await?;
    require_management_tag(&session)?;
    let vhost = decode_vhost(&raw_vhost)?;
    check_queue_configure(&state, &session.username, &vhost, &name).await?;
    let key = QueueKey::new(&vhost, &name);
    let message_count = state
        .queues
        .delete(
            &key,
            q.if_unused.unwrap_or(false),
            q.if_empty.unwrap_or(false),
        )
        .await?;
    state.router.remove_queue_bindings(&vhost, &name);
    // Durable binding rows cascade is store's responsibility on queue delete;
    // also clean any remaining.
    let vhost_b = vhost.clone();
    let bindings = db(&state, move |s| s.list_bindings(&vhost_b))
        .await
        .unwrap_or_default();
    for b in bindings {
        if b.queue.as_str() == name {
            let vhost_d = b.vhost.to_string();
            let exchange_d = b.exchange.to_string();
            let queue_d = b.queue.to_string();
            let rk_d = b.routing_key.to_string();
            let args_d = queueforge_store::binding_args_key(&b.args);
            let _ = db(&state, move |s| {
                s.delete_binding(&vhost_d, &exchange_d, &queue_d, &rk_d, &args_d)
            })
            .await;
            let _ = state.router.unbind(&b);
        }
    }
    Ok(Json(serde_json::json!({ "message_count": message_count })))
}

/// POST /api/queues/{vhost}/{name}/purge
pub async fn purge_queue(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Path((raw_vhost, name)): Path<(String, String)>,
) -> Result<Json<serde_json::Value>, MgmtError> {
    let session = require_session(&state, &headers).await?;
    require_management_tag(&session)?;
    let vhost = decode_vhost(&raw_vhost)?;
    // Purge requires read (or configure); design lists purge under resource AuthZ.
    // AMQP queue.purge uses configure on RabbitMQ; our design says configure/write/read
    // checked on purge — use configure for parity with declare/delete.
    check_queue_configure(&state, &session.username, &vhost, &name).await?;
    let handle = state
        .queues
        .get(&QueueKey::new(&vhost, &name))
        .ok_or_else(|| MgmtError::NotFound(format!("queue '{name}'")))?;
    let (tx, rx) = oneshot::channel();
    handle
        .tx
        .send(QueueCmd::Purge { reply: tx })
        .await
        .map_err(|_| MgmtError::Internal("queue mailbox closed".into()))?;
    let purged = rx
        .await
        .map_err(|_| MgmtError::Internal("purge reply cancelled".into()))?;
    Ok(Json(serde_json::json!({ "message_count": purged })))
}

// ── Bindings ────────────────────────────────────────────────────────────
