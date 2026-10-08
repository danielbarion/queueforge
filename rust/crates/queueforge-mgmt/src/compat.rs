//! RabbitMQ management API compatibility.
//!
//! The admin UI reads lists as `{ items }` and sends permission paths as
//! `user/vhost`. RabbitMQ's API answers lists as bare arrays and orders
//! permission paths `vhost/user`. Tools that speak RabbitMQ's API use HTTP
//! Basic auth, so a Basic caller gets RabbitMQ's shapes and the UI is unchanged.

use axum::body::{to_bytes, Body};
use axum::extract::{Path, Request, State};
use axum::http::{header, HeaderMap};
use axum::middleware::Next;
use axum::response::Response;
use axum::Json;
use serde_json::{json, Value};

use crate::authz::require_session;
use crate::error::MgmtError;
use crate::routes::decode_vhost;
use crate::state::MgmtState;

/// The largest list body the unwrapper rewrites. Larger bodies pass through.
const UNWRAP_LIMIT: usize = 64 * 1024 * 1024;

fn is_basic(headers: &HeaderMap) -> bool {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.len() > 6 && v[..6].eq_ignore_ascii_case("basic "))
}

/// Middleware: for a Basic-auth caller, answer `{ "items": [...] }` as the bare array.
pub async fn unwrap_items(req: Request, next: Next) -> Response {
    let basic = is_basic(req.headers());
    let res = next.run(req).await;
    let json = res
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.starts_with("application/json"));
    if !basic || !json || !res.status().is_success() {
        return res;
    }
    let (mut parts, body) = res.into_parts();
    let Ok(bytes) = to_bytes(body, UNWRAP_LIMIT).await else {
        return Response::from_parts(parts, Body::empty());
    };
    let rewritten = match serde_json::from_slice::<Value>(&bytes) {
        Ok(Value::Object(mut map)) if map.get("items").is_some_and(Value::is_array) => {
            map.remove("items").and_then(|items| serde_json::to_vec(&items).ok())
        }
        _ => None,
    };
    let out = rewritten.unwrap_or_else(|| bytes.to_vec());
    parts.headers.remove(header::CONTENT_LENGTH);
    Response::from_parts(parts, Body::from(out))
}

/// Resolve a permission path to `(user, vhost)`. Whichever segment names an
/// existing vhost is the vhost; otherwise the UI's `user/vhost` order holds.
pub async fn perm_target(state: &MgmtState, first: &str, second: &str) -> Result<(String, String), MgmtError> {
    let a = decode_vhost(first)?;
    let b = decode_vhost(second)?;
    let store = std::sync::Arc::clone(&state.store);
    let (qa, qb) = (a.clone(), b.clone());
    let (a_is_vhost, b_is_vhost) = queueforge_store::MetadataStore::blocking(store, move |s| {
        Ok((s.get_vhost(&qa)?.is_some(), s.get_vhost(&qb)?.is_some()))
    })
    .await
    .map_err(MgmtError::from)?;
    if a_is_vhost && !b_is_vhost {
        Ok((b, a))
    } else {
        Ok((a, b))
    }
}

/// GET /api/vhost-limits/{vhost}: RabbitMQ's `[{ vhost, value: {...} }]`.
pub async fn get_vhost_limits(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Path(raw): Path<String>,
) -> Result<Json<Value>, MgmtError> {
    require_session(&state, &headers).await?;
    let vhost = decode_vhost(&raw)?;
    let rows: Vec<Value> = state
        .connections
        .list_vhost_limits()
        .into_iter()
        .filter(|row| row.0 == vhost)
        .map(|(vhost, connections, queues)| {
            let mut value = serde_json::Map::new();
            if let Some(n) = connections {
                value.insert("max-connections".into(), json!(n));
            }
            if let Some(n) = queues {
                value.insert("max-queues".into(), json!(n));
            }
            json!({ "vhost": vhost, "value": value })
        })
        .collect();
    Ok(Json(Value::Array(rows)))
}

/// GET /api/user-limits/{user}: RabbitMQ's `[{ user, value: {...} }]`.
pub async fn get_user_limits(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Path(user): Path<String>,
) -> Result<Json<Value>, MgmtError> {
    require_session(&state, &headers).await?;
    let rows: Vec<Value> = state
        .connections
        .list_user_limits()
        .into_iter()
        .filter(|row| row.0 == user)
        .map(|(user, connections, channels)| {
            let mut value = serde_json::Map::new();
            if let Some(n) = connections {
                value.insert("max-connections".into(), json!(n));
            }
            if let Some(n) = channels {
                value.insert("max-channels".into(), json!(n));
            }
            json!({ "user": user, "value": value })
        })
        .collect();
    Ok(Json(Value::Array(rows)))
}
