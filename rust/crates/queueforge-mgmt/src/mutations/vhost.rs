//! Vhost create and delete.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use queueforge_core::{Exchange, DEFAULT_VHOST};
use serde::Serialize;

use crate::authz::{require_administrator, require_session};
use crate::error::MgmtError;
use crate::routes::decode_vhost;
use crate::state::MgmtState;

use super::{db, replicate};

#[derive(Debug, Serialize)]
pub(crate) struct VhostItem {
    name: String,
}

/// PUT /api/vhosts/{vhost}
pub async fn put_vhost(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Path(raw_vhost): Path<String>,
    body: Option<Json<serde_json::Value>>,
) -> Result<Response, MgmtError> {
    let session = require_session(&state, &headers).await?;
    require_administrator(&session)?;
    let vhost = decode_vhost(&raw_vhost)?;
    if vhost.is_empty() {
        return Err(MgmtError::BadRequest("vhost name required".into()));
    }
    // RabbitMQ turns firehose tracing on and off with the vhost's `tracing` field.
    if let Some(on) = body.as_ref().and_then(|b| b.get("tracing")).and_then(|t| t.as_bool()) {
        state.connections.set_tracing(&vhost, on);
    }
    let created = {
        let name = vhost.clone();
        queueforge_store::MetadataStore::blocking(Arc::clone(&state.store), move |s| {
            s.create_vhost(&name)
        })
        .await
    };
    match created {
        Ok(_) => {
            // Load builtins into live router for this vhost.
            for ex in Exchange::builtins_for(vhost.as_str()) {
                state.router.put_exchange(ex);
            }
            replicate(&state, "vhost", serde_json::json!({ "name": vhost })).await;
            Ok((StatusCode::CREATED, Json(VhostItem { name: vhost })).into_response())
        }
        Err(queueforge_store::StoreError::VhostExists(_)) => {
            Ok((StatusCode::NO_CONTENT).into_response())
        }
        Err(e) => Err(MgmtError::from(e)),
    }
}

/// DELETE /api/vhosts/{vhost}
pub async fn delete_vhost(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Path(raw_vhost): Path<String>,
) -> Result<StatusCode, MgmtError> {
    let session = require_session(&state, &headers).await?;
    require_administrator(&session)?;
    let vhost = decode_vhost(&raw_vhost)?;
    if vhost == DEFAULT_VHOST {
        return Err(MgmtError::BadRequest(
            "cannot delete the default vhost".into(),
        ));
    }
    // Drop live queues in this vhost first.
    for key in state.queues.list_keys() {
        if key.vhost.as_str() == vhost {
            let _ = state.queues.delete(&key, false, false).await;
            state
                .router
                .remove_queue_bindings(&vhost, key.name.as_str());
        }
    }
    for ex in state.router.list_exchanges(&vhost) {
        let _ = state.router.delete_exchange(&vhost, ex.name.as_str());
    }
    let name = vhost.clone();
    let deleted = db(&state, move |s| s.delete_vhost(&name)).await?;
    if deleted {
        replicate(&state, "delete_vhost", serde_json::json!({ "name": vhost })).await;
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(MgmtError::NotFound(format!("vhost '{vhost}'")))
    }
}

// ── Users ───────────────────────────────────────────────────────────────
