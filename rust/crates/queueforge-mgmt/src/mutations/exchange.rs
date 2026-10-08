//! Exchange declare and delete.

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use compact_str::CompactString;
use queueforge_core::{Exchange, ExchangeType, BUILTIN_EXCHANGE_NAMES};
use serde::{Deserialize, Serialize};

use crate::authz::{check_exchange_configure, require_management_tag, require_session};
use crate::error::MgmtError;
use crate::routes::decode_vhost;
use crate::state::MgmtState;

use super::IfUnusedQuery;
use super::{db, ensure_vhost};

#[derive(Debug, Deserialize)]
pub struct PutExchangeBody {
    /// Exchange type: direct | fanout | topic.
    #[serde(default = "default_exchange_type")]
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    durable: bool,
    #[serde(default)]
    auto_delete: bool,
    #[serde(default)]
    internal: bool,
}

fn default_exchange_type() -> String {
    "direct".into()
}

#[derive(Debug, Serialize)]
pub(crate) struct ExchangeItem {
    name: String,
    vhost: String,
    #[serde(rename = "type")]
    kind: String,
    durable: bool,
    auto_delete: bool,
    internal: bool,
}

fn parse_exchange_type(s: &str) -> Result<ExchangeType, MgmtError> {
    ExchangeType::parse(&s.to_ascii_lowercase())
        .ok_or_else(|| MgmtError::BadRequest(format!("invalid exchange type '{s}'")))
}

/// PUT /api/exchanges/{vhost}/{name}
pub async fn put_exchange(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Path((raw_vhost, name)): Path<(String, String)>,
    Json(body): Json<PutExchangeBody>,
) -> Result<Response, MgmtError> {
    let session = require_session(&state, &headers).await?;
    require_management_tag(&session)?;
    let vhost = decode_vhost(&raw_vhost)?;
    ensure_vhost(&state, &vhost).await?;
    if name.is_empty() || BUILTIN_EXCHANGE_NAMES.contains(&name.as_str()) {
        return Err(MgmtError::BadRequest(
            "cannot declare builtin/default exchange via management".into(),
        ));
    }
    if body.auto_delete {
        return Err(MgmtError::BadRequest(
            "exchange auto_delete is not supported".into(),
        ));
    }
    check_exchange_configure(&state, &session.username, &vhost, &name).await?;

    let kind = parse_exchange_type(&body.kind)?;
    let new_ex = Exchange {
        vhost: CompactString::from(vhost.as_str()),
        name: CompactString::from(name.as_str()),
        kind,
        durable: body.durable,
        auto_delete: body.auto_delete,
        internal: body.internal,
        alternate: None,
        delayed_type: None,
    };

    let vhost_lookup = vhost.clone();
    let name_lookup = name.clone();
    let stored_ex = db(&state, move |s| s.get_exchange(&vhost_lookup, &name_lookup)).await;
    if let Some(existing) = state
        .router
        .get_exchange(&vhost, &name)
        .or_else(|| stored_ex.ok().flatten())
    {
        if existing.kind != new_ex.kind
            || existing.durable != new_ex.durable
            || existing.auto_delete != new_ex.auto_delete
            || existing.internal != new_ex.internal
        {
            return Err(MgmtError::PreconditionFailed(format!(
                "exchange '{name}' exists with different properties"
            )));
        }
        state.router.put_exchange(existing);
        return Ok(StatusCode::NO_CONTENT.into_response());
    }

    if new_ex.durable {
        let stored = new_ex.clone();
        db(&state, move |s| s.create_exchange(&stored)).await?;
    }
    state.router.put_exchange(new_ex.clone());
    Ok((
        StatusCode::CREATED,
        Json(ExchangeItem {
            name: new_ex.name.to_string(),
            vhost: new_ex.vhost.to_string(),
            kind: new_ex.kind.as_str().to_string(),
            durable: new_ex.durable,
            auto_delete: new_ex.auto_delete,
            internal: new_ex.internal,
        }),
    )
        .into_response())
}

/// DELETE /api/exchanges/{vhost}/{name}
pub async fn delete_exchange(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Path((raw_vhost, name)): Path<(String, String)>,
    Query(q): Query<IfUnusedQuery>,
) -> Result<StatusCode, MgmtError> {
    let session = require_session(&state, &headers).await?;
    require_management_tag(&session)?;
    let vhost = decode_vhost(&raw_vhost)?;
    if name.is_empty() || BUILTIN_EXCHANGE_NAMES.contains(&name.as_str()) {
        return Err(MgmtError::BadRequest(
            "cannot delete builtin exchange".into(),
        ));
    }
    check_exchange_configure(&state, &session.username, &vhost, &name).await?;

    let vhost_lookup = vhost.clone();
    let name_lookup = name.clone();
    let stored = db(&state, move |s| s.get_exchange(&vhost_lookup, &name_lookup)).await?;
    if state.router.get_exchange(&vhost, &name).is_none() && stored.is_none() {
        return Err(MgmtError::NotFound(format!("exchange '{name}'")));
    }

    if q.if_unused.unwrap_or(false) {
        let n = state.router.index().count_for_exchange(&vhost, &name);
        if n > 0 {
            return Err(MgmtError::PreconditionFailed(format!(
                "exchange '{name}' in use (bindings={n})"
            )));
        }
    }

    let vhost_del = vhost.clone();
    let name_del = name.clone();
    let doomed = db(&state, move |s| {
        let _ = s.delete_exchange(&vhost_del, &name_del);
        s.list_bindings_for_exchange(&vhost_del, &name_del)
    })
    .await
    .unwrap_or_default();
    for b in doomed {
        let vhost_b = b.vhost.to_string();
        let exchange_b = b.exchange.to_string();
        let queue_b = b.queue.to_string();
        let rk_b = b.routing_key.to_string();
        let args_b = queueforge_store::binding_args_key(&b.args);
        let _ = db(&state, move |s| {
            s.delete_binding(&vhost_b, &exchange_b, &queue_b, &rk_b, &args_b)
        })
        .await;
    }
    let _ = state.router.delete_exchange(&vhost, &name);
    Ok(StatusCode::NO_CONTENT)
}
