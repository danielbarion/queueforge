//! Permission list, create, and delete.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use queueforge_auth::AuthService;
use serde::{Deserialize, Serialize};

use crate::authz::{require_administrator, require_session};
use crate::error::MgmtError;
use crate::routes::decode_vhost;
use crate::state::MgmtState;

use super::{db, replicate};

#[derive(Debug, Serialize)]
pub(crate) struct PermissionItem {
    user: String,
    vhost: String,
    configure: String,
    write: String,
    read: String,
}

#[derive(Debug, Deserialize)]
pub struct PutPermissionBody {
    configure: String,
    write: String,
    read: String,
}

/// GET /api/permissions
pub async fn list_permissions(
    State(state): State<MgmtState>,
    headers: HeaderMap,
) -> Result<Json<Vec<PermissionItem>>, MgmtError> {
    let session = require_session(&state, &headers).await?;
    require_administrator(&session)?;
    let mut perms = db(&state, |s| s.list_permissions()).await?;
    perms.sort_by(|a, b| a.user.cmp(&b.user).then_with(|| a.vhost.cmp(&b.vhost)));
    Ok(Json(
        perms
            .into_iter()
            .map(|p| PermissionItem {
                user: p.user.to_string(),
                vhost: p.vhost.to_string(),
                configure: p.configure,
                write: p.write,
                read: p.read,
            })
            .collect(),
    ))
}

/// PUT /api/permissions/{user}/{vhost}
pub async fn put_permission(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Path((user, raw_vhost)): Path<(String, String)>,
    Json(body): Json<PutPermissionBody>,
) -> Result<StatusCode, MgmtError> {
    let session = require_session(&state, &headers).await?;
    require_administrator(&session)?;
    let (user, vhost) = crate::compat::perm_target(&state, &user, &raw_vhost).await?;
    let user_lookup = user.clone();
    if db(&state, move |s| s.get_user(&user_lookup))
        .await?
        .is_none()
    {
        return Err(MgmtError::NotFound(format!("user '{user}'")));
    }
    let vhost_lookup = vhost.clone();
    if db(&state, move |s| s.get_vhost(&vhost_lookup))
        .await?
        .is_none()
    {
        return Err(MgmtError::NotFound(format!("vhost '{vhost}'")));
    }
    let store = Arc::clone(&state.store);
    let user_p = user.clone();
    let vhost_p = vhost.clone();
    let configure = body.configure.clone();
    let write = body.write.clone();
    let read = body.read.clone();
    tokio::task::spawn_blocking(move || {
        AuthService::new(store.as_ref())
            .set_permission(&user_p, &vhost_p, &configure, &write, &read)
    })
    .await
    .map_err(|e| MgmtError::Internal(format!("permission task panicked: {e}")))??;
    replicate(
        &state,
        "permission",
        serde_json::json!({
            "user": user,
            "vhost": vhost,
            "configure": body.configure,
            "write": body.write,
            "read": body.read,
        }),
    )
    .await;
    Ok(StatusCode::NO_CONTENT)
}

/// DELETE /api/permissions/{user}/{vhost}
pub async fn delete_permission(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Path((user, raw_vhost)): Path<(String, String)>,
) -> Result<StatusCode, MgmtError> {
    let session = require_session(&state, &headers).await?;
    require_administrator(&session)?;
    let (user, vhost) = crate::compat::perm_target(&state, &user, &raw_vhost).await?;
    let user_d = user.clone();
    let vhost_d = vhost.clone();
    if !db(&state, move |s| s.delete_permission(&user_d, &vhost_d)).await? {
        return Err(MgmtError::NotFound(format!(
            "permission '{user}'@'{vhost}'"
        )));
    }
    replicate(
        &state,
        "delete_permission",
        serde_json::json!({ "user": user, "vhost": vhost }),
    )
    .await;
    Ok(StatusCode::NO_CONTENT)
}

// ── Exchanges ───────────────────────────────────────────────────────────
