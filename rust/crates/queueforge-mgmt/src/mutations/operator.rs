//! Operator policy list, create, and delete.

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::Json;

use crate::authz::{require_administrator, require_management_tag, require_session};
use crate::error::MgmtError;
use crate::routes::decode_vhost;
use crate::state::MgmtState;

use super::ensure_vhost;
use super::policy::{apply_policies_live, policy_from_body, policy_item, PutPolicyBody};

/// GET /api/operator-policies
pub async fn list_operator_policies(
    State(state): State<MgmtState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, MgmtError> {
    let session = require_session(&state, &headers).await?;
    require_management_tag(&session)?;
    let items: Vec<_> = state
        .router
        .list_operator_policies(None)
        .iter()
        .map(policy_item)
        .collect();
    Ok(Json(serde_json::json!({ "items": items })))
}

/// GET /api/operator-policies/{vhost}
pub async fn list_operator_policies_vhost(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Path(raw_vhost): Path<String>,
) -> Result<Json<serde_json::Value>, MgmtError> {
    let session = require_session(&state, &headers).await?;
    require_management_tag(&session)?;
    let vhost = decode_vhost(&raw_vhost)?;
    let items: Vec<_> = state
        .router
        .list_operator_policies(Some(&vhost))
        .iter()
        .map(policy_item)
        .collect();
    Ok(Json(serde_json::json!({ "items": items })))
}

/// PUT /api/operator-policies/{vhost}/{name}
pub async fn put_operator_policy(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Path((raw_vhost, name)): Path<(String, String)>,
    Json(body): Json<PutPolicyBody>,
) -> Result<StatusCode, MgmtError> {
    let session = require_session(&state, &headers).await?;
    require_administrator(&session)?;
    let vhost = decode_vhost(&raw_vhost)?;
    ensure_vhost(&state, &vhost).await?;
    if name.is_empty() {
        return Err(MgmtError::BadRequest("policy name required".into()));
    }
    let existed = state
        .router
        .list_operator_policies(Some(&vhost))
        .iter()
        .any(|p| p.name.as_str() == name);
    let policy = policy_from_body(&vhost, &name, body)?;
    state
        .router
        .upsert_operator_policy(policy)
        .map_err(|e| MgmtError::BadRequest(e.to_string()))?;
    apply_policies_live(&state).await;
    Ok(if existed {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::CREATED
    })
}

/// DELETE /api/operator-policies/{vhost}/{name}
pub async fn delete_operator_policy(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Path((raw_vhost, name)): Path<(String, String)>,
) -> Result<StatusCode, MgmtError> {
    let session = require_session(&state, &headers).await?;
    require_administrator(&session)?;
    let vhost = decode_vhost(&raw_vhost)?;
    if !state.router.delete_operator_policy(&vhost, &name) {
        return Err(MgmtError::NotFound(format!("operator policy '{name}'")));
    }
    apply_policies_live(&state).await;
    Ok(StatusCode::NO_CONTENT)
}
