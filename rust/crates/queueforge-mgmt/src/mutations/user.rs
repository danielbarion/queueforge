//! User list, create, and delete.

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use queueforge_auth::hash_password;
use queueforge_core::{User, UserTag};
use serde::{Deserialize, Serialize};

use crate::authz::{require_administrator, require_session};
use crate::error::MgmtError;
use crate::state::MgmtState;

use super::{db, replicate};

#[derive(Debug, Deserialize)]
pub struct PutUserBody {
    /// Plaintext password (required on create; optional on update).
    #[serde(default)]
    password: Option<String>,
    /// Tags: administrator / management / monitoring. RabbitMQ sends one
    /// comma-separated string; the admin UI sends an array.
    #[serde(default, deserialize_with = "tags_any")]
    tags: Vec<String>,
}

/// Read `tags` as an array of strings or as RabbitMQ's comma-separated string.
fn tags_any<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Vec<String>, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Tags {
        List(Vec<String>),
        Text(String),
    }
    Ok(match Option::<Tags>::deserialize(d)? {
        None => Vec::new(),
        Some(Tags::List(list)) => list,
        Some(Tags::Text(text)) => text
            .split(',')
            .map(|t| t.trim().to_string())
            .filter(|t| !t.is_empty())
            .collect(),
    })
}

#[derive(Debug, Serialize)]
pub(crate) struct UserItem {
    name: String,
    tags: Vec<&'static str>,
}

fn parse_tags(tags: &[String]) -> Result<Vec<UserTag>, MgmtError> {
    let mut out = Vec::new();
    for t in tags {
        match t.to_ascii_lowercase().as_str() {
            "administrator" => out.push(UserTag::Administrator),
            "management" => out.push(UserTag::Management),
            "monitoring" => out.push(UserTag::Monitoring),
            other => {
                return Err(MgmtError::BadRequest(format!("unknown tag '{other}'")));
            }
        }
    }
    Ok(out)
}

/// GET /api/users
pub async fn list_users(
    State(state): State<MgmtState>,
    headers: HeaderMap,
) -> Result<Json<Vec<UserItem>>, MgmtError> {
    let session = require_session(&state, &headers).await?;
    require_administrator(&session)?;
    let mut users = db(&state, |s| s.list_users()).await?;
    users.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(Json(
        users
            .into_iter()
            .map(|u| UserItem {
                name: u.name.to_string(),
                tags: u.tags.iter().map(|t| t.as_str()).collect(),
            })
            .collect(),
    ))
}

/// PUT /api/users/{name}
pub async fn put_user(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Path(name): Path<String>,
    Json(body): Json<PutUserBody>,
) -> Result<Response, MgmtError> {
    let session = require_session(&state, &headers).await?;
    require_administrator(&session)?;
    if name.is_empty() {
        return Err(MgmtError::BadRequest("username required".into()));
    }
    let tags = parse_tags(&body.tags)?;

    let lookup = name.clone();
    match db(&state, move |s| s.get_user(&lookup)).await? {
        None => {
            let password = body.password.ok_or_else(|| {
                MgmtError::BadRequest("password required when creating a user".into())
            })?;
            let user_name = name.clone();
            let user = tokio::task::spawn_blocking(move || {
                let password_hash = hash_password(&password)?;
                Ok::<_, queueforge_auth::AuthError>(User::new(user_name, password_hash, tags))
            })
            .await
            .map_err(|e| MgmtError::Internal(format!("hash join: {e}")))??;
            let stored = user.clone();
            db(&state, move |s| s.create_user(&stored)).await?;
            replicate(
                &state,
                "user",
                serde_json::to_value(&user).unwrap_or(serde_json::Value::Null),
            )
            .await;
            Ok((
                StatusCode::CREATED,
                Json(UserItem {
                    name: user.name.to_string(),
                    tags: user.tags.iter().map(|t| t.as_str()).collect(),
                }),
            )
                .into_response())
        }
        Some(mut existing) => {
            if let Some(password) = body.password {
                let hash = tokio::task::spawn_blocking(move || hash_password(&password))
                    .await
                    .map_err(|e| MgmtError::Internal(format!("hash join: {e}")))??;
                existing.password_hash = hash;
            }
            existing.tags = tags;
            let stored = existing.clone();
            db(&state, move |s| s.put_user(&stored)).await?;
            replicate(
                &state,
                "user",
                serde_json::to_value(&existing).unwrap_or(serde_json::Value::Null),
            )
            .await;
            Ok(StatusCode::NO_CONTENT.into_response())
        }
    }
}

/// DELETE /api/users/{name}
pub async fn delete_user(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Result<StatusCode, MgmtError> {
    let session = require_session(&state, &headers).await?;
    require_administrator(&session)?;
    if name == session.username {
        return Err(MgmtError::BadRequest(
            "cannot delete the currently authenticated user".into(),
        ));
    }
    let target = name.clone();
    if !db(&state, move |s| s.delete_user(&target)).await? {
        return Err(MgmtError::NotFound(format!("user '{name}'")));
    }
    replicate(&state, "delete_user", serde_json::json!({ "name": name })).await;
    Ok(StatusCode::NO_CONTENT)
}

// ── Permissions ─────────────────────────────────────────────────────────
