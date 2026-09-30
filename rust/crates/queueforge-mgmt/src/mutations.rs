//! Management mutation handlers: CRUD for topology, users, permissions; publish/get.

use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use bytes::Bytes;
use compact_str::CompactString;
use queueforge_auth::{hash_password, AuthService};
use queueforge_core::{
    binding_args_key, binding_properties_key, ArgValue, Binding, Exchange, ExchangeType, HeaderArg,
    Message, Policy, PolicyTarget, QueueArgs, QueueCmd, QueueDeclareOpts, QueueKey, User, UserTag,
    BUILTIN_EXCHANGE_NAMES, DEFAULT_EXCHANGE_NAME, DEFAULT_VHOST,
};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use crate::authz::{
    check_bind, check_exchange_configure, check_publish, check_queue_configure, check_queue_read,
    require_administrator, require_management_tag, require_session,
};
use crate::error::MgmtError;
use crate::pagination::Page;
use crate::routes::decode_vhost;
use crate::state::{MgmtState, ReplicateReq};

/// Run one metadata operation off the HTTP worker.
async fn db<T, F>(state: &MgmtState, f: F) -> Result<T, MgmtError>
where
    T: Send + 'static,
    F: FnOnce(&queueforge_store::MetadataStore) -> queueforge_store::Result<T> + Send + 'static,
{
    queueforge_store::MetadataStore::blocking(Arc::clone(&state.store), f)
        .await
        .map_err(MgmtError::from)
}

async fn replicate(state: &MgmtState, kind: &str, payload: serde_json::Value) {
    let Some(tx) = &state.replicate_tx else {
        return;
    };
    let (done, rx) = tokio::sync::oneshot::channel();
    if tx
        .send(ReplicateReq {
            kind: kind.to_string(),
            payload,
            done,
        })
        .is_err()
    {
        return;
    }
    let _ = rx.await;
}

// ── Vhosts ──────────────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
pub(crate) struct VhostItem {
    name: String,
}

/// PUT /api/vhosts/{vhost}
pub async fn put_vhost(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Path(raw_vhost): Path<String>,
) -> Result<Response, MgmtError> {
    let session = require_session(&state, &headers).await?;
    require_administrator(&session)?;
    let vhost = decode_vhost(&raw_vhost)?;
    if vhost.is_empty() {
        return Err(MgmtError::BadRequest("vhost name required".into()));
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

#[derive(Debug, Deserialize)]
pub struct PutUserBody {
    /// Plaintext password (required on create; optional on update).
    #[serde(default)]
    password: Option<String>,
    /// Tags: administrator / management / monitoring.
    #[serde(default)]
    tags: Vec<String>,
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
            replicate(&state, "user", serde_json::to_value(&user).unwrap_or(serde_json::Value::Null)).await;
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
    let vhost = decode_vhost(&raw_vhost)?;
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
    let vhost = decode_vhost(&raw_vhost)?;
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
    match s.to_ascii_lowercase().as_str() {
        "direct" => Ok(ExchangeType::Direct),
        "fanout" => Ok(ExchangeType::Fanout),
        "topic" => Ok(ExchangeType::Topic),
        "headers" => Ok(ExchangeType::Headers),
        other => Err(MgmtError::BadRequest(format!(
            "invalid exchange type '{other}'"
        ))),
    }
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

#[derive(Debug, Default, Deserialize)]
pub struct IfUnusedQuery {
    if_unused: Option<bool>,
    if_empty: Option<bool>,
}

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
/// Accepts the closed v1 x-arg set; unknown keys return [`MgmtError::PreconditionFailed`].
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
    if !body.durable {
        return Err(MgmtError::BadRequest(
            "Feature `transient_nonexcl_queues` is deprecated. By default, this feature is not permitted anymore.".into(),
        ));
    }
    check_queue_configure(&state, &session.username, &vhost, &name).await?;

    let declared = parse_mgmt_queue_args(&body.arguments)?;
    let args = state.router.queue_args_with_policy(&vhost, &name, &declared);
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

#[derive(Debug, Serialize)]
pub(crate) struct BindingItem {
    source: String,
    destination: String,
    destination_type: &'static str,
    routing_key: String,
    vhost: String,
    properties_key: String,
}

#[derive(Debug, Deserialize)]
pub struct CreateBindingBody {
    /// Source exchange.
    source: String,
    /// Destination queue.
    destination: String,
    /// Routing key / topic pattern.
    #[serde(default)]
    routing_key: String,
    /// Must be "queue" (exchange-to-exchange not supported).
    #[serde(default = "default_dest_type")]
    destination_type: String,
    /// Header arguments. String and integer values are stored; other JSON types are ignored.
    #[serde(default)]
    arguments: serde_json::Map<String, serde_json::Value>,
}

pub(crate) fn args_from_json(
    arguments: &serde_json::Map<String, serde_json::Value>,
) -> Vec<(CompactString, HeaderArg)> {
    let mut args = Vec::new();
    for (name, value) in arguments {
        let arg = match value {
            serde_json::Value::String(text) => Some(HeaderArg::Str(text.clone())),
            serde_json::Value::Number(n) => n.as_i64().map(HeaderArg::Int),
            serde_json::Value::Bool(flag) => Some(HeaderArg::Str(flag.to_string())),
            _ => None,
        };
        if let Some(arg) = arg {
            args.push((CompactString::from(name.as_str()), arg));
        }
    }
    args
}

fn same_listed_binding(item: &BindingItem, binding: &Binding) -> bool {
    item.source == binding.exchange.as_str()
        && item.destination == binding.queue.as_str()
        && item.properties_key
            == binding_properties_key(binding.routing_key.as_str(), &binding.args)
}

fn default_dest_type() -> String {
    "queue".into()
}

fn binding_item(b: &Binding) -> BindingItem {
    BindingItem {
        source: b.exchange.to_string(),
        destination: b.queue.to_string(),
        destination_type: "queue",
        routing_key: b.routing_key.to_string(),
        vhost: b.vhost.to_string(),
        properties_key: binding_properties_key(b.routing_key.as_str(), &b.args),
    }
}

/// GET /api/bindings/{vhost}
pub async fn list_bindings(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Path(raw_vhost): Path<String>,
) -> Result<Json<Page<BindingItem>>, MgmtError> {
    let _session = require_session(&state, &headers).await?;
    let vhost = decode_vhost(&raw_vhost)?;
    ensure_vhost(&state, &vhost).await?;
    let mut items: Vec<BindingItem> = state
        .router
        .index()
        .list()
        .into_iter()
        .filter(|k| k.vhost.as_str() == vhost)
        .map(|k| binding_item(&k.to_binding()))
        .collect();
    // Also include durable-only store bindings not yet in live index.
    let vhost_list = vhost.clone();
    for b in db(&state, move |s| s.list_bindings(&vhost_list)).await? {
        if !items.iter().any(|i| same_listed_binding(i, &b)) {
            items.push(binding_item(&b));
        }
    }
    items.sort_by(|a, b| {
        a.source
            .cmp(&b.source)
            .then_with(|| a.destination.cmp(&b.destination))
            .then_with(|| a.routing_key.cmp(&b.routing_key))
    });
    let total = items.len() as u64;
    Ok(Json(Page {
        items,
        next_cursor: None,
        total_count: Some(total),
    }))
}

/// GET /api/exchanges/{vhost}/{name}/bindings
pub async fn list_exchange_bindings(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Path((raw_vhost, name)): Path<(String, String)>,
) -> Result<Json<Vec<BindingItem>>, MgmtError> {
    let _session = require_session(&state, &headers).await?;
    let vhost = decode_vhost(&raw_vhost)?;
    let mut items: Vec<BindingItem> = state
        .router
        .index()
        .list_for_exchange(&vhost, &name)
        .into_iter()
        .map(|k| binding_item(&k.to_binding()))
        .collect();
    let vhost_list = vhost.clone();
    let name_list = name.clone();
    for b in db(&state, move |s| {
        s.list_bindings_for_exchange(&vhost_list, &name_list)
    })
    .await?
    {
        if !items.iter().any(|i| same_listed_binding(i, &b)) {
            items.push(binding_item(&b));
        }
    }
    items.sort_by(|a, b| {
        a.destination
            .cmp(&b.destination)
            .then_with(|| a.routing_key.cmp(&b.routing_key))
    });
    Ok(Json(items))
}

/// GET /api/queues/{vhost}/{name}/bindings
pub async fn list_queue_bindings(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Path((raw_vhost, name)): Path<(String, String)>,
) -> Result<Json<Vec<BindingItem>>, MgmtError> {
    let _session = require_session(&state, &headers).await?;
    let vhost = decode_vhost(&raw_vhost)?;
    let mut items: Vec<BindingItem> = state
        .router
        .index()
        .list()
        .into_iter()
        .filter(|k| k.vhost.as_str() == vhost && k.queue.as_str() == name)
        .map(|k| binding_item(&k.to_binding()))
        .collect();
    let vhost_list = vhost.clone();
    for b in db(&state, move |s| s.list_bindings(&vhost_list)).await? {
        if b.queue.as_str() == name && !items.iter().any(|i| same_listed_binding(i, &b)) {
            items.push(binding_item(&b));
        }
    }
    items.sort_by(|a, b| {
        a.source
            .cmp(&b.source)
            .then_with(|| a.routing_key.cmp(&b.routing_key))
    });
    Ok(Json(items))
}

/// POST /api/bindings/{vhost}
pub async fn create_binding(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Path(raw_vhost): Path<String>,
    Json(body): Json<CreateBindingBody>,
) -> Result<Response, MgmtError> {
    let session = require_session(&state, &headers).await?;
    require_management_tag(&session)?;
    let vhost = decode_vhost(&raw_vhost)?;
    ensure_vhost(&state, &vhost).await?;
    if body.destination_type != "queue" {
        return Err(MgmtError::BadRequest(
            "only destination_type=queue is supported".into(),
        ));
    }
    if body.source.is_empty() {
        return Err(MgmtError::BadRequest(
            "cannot bind to the default exchange".into(),
        ));
    }
    check_bind(
        &state,
        &session.username,
        &vhost,
        &body.destination,
        &body.source,
    )
    .await?;

    // Ensure exchange exists.
    if state.router.get_exchange(&vhost, &body.source).is_none() {
        let vhost_ex = vhost.clone();
        let source = body.source.clone();
        if let Some(ex) = db(&state, move |s| s.get_exchange(&vhost_ex, &source))
            .await?
            .as_ref()
        {
            state.router.put_exchange(ex.clone());
        } else {
            return Err(MgmtError::NotFound(format!("exchange '{}'", body.source)));
        }
    }
    let key = QueueKey::new(&vhost, &body.destination);
    let handle = state
        .queues
        .get(&key)
        .ok_or_else(|| MgmtError::NotFound(format!("queue '{}'", body.destination)))?;

    let mut binding = Binding::new(
        vhost.as_str(),
        body.source.as_str(),
        body.destination.as_str(),
        body.routing_key.as_str(),
    );
    binding.args = args_from_json(&body.arguments);

    match state.router.bind(binding.clone()) {
        Ok(true) => {
            // Persist when both durable.
            let ex_durable = state
                .router
                .get_exchange(&vhost, &body.source)
                .map(|e| e.durable)
                .unwrap_or(false);
            if ex_durable && handle.info.durable {
                let stored = binding.clone();
                if let Err(e) = db(&state, move |s| s.create_binding(&stored)).await {
                    let _ = state.router.unbind(&binding);
                    return Err(e);
                }
            }
            Ok((StatusCode::CREATED, Json(binding_item(&binding))).into_response())
        }
        Ok(false) => Ok(StatusCode::NO_CONTENT.into_response()),
        Err(e) => Err(MgmtError::from(e)),
    }
}

/// DELETE /api/bindings/{vhost}/{exchange}/{queue}/{properties_key}
pub async fn delete_binding(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Path((raw_vhost, exchange, queue, properties_key)): Path<(String, String, String, String)>,
) -> Result<StatusCode, MgmtError> {
    let session = require_session(&state, &headers).await?;
    require_management_tag(&session)?;
    let vhost = decode_vhost(&raw_vhost)?;
    if exchange.is_empty() {
        return Err(MgmtError::BadRequest(
            "cannot unbind from the default exchange".into(),
        ));
    }
    check_bind(&state, &session.username, &vhost, &queue, &exchange).await?;

    let mut targets: Vec<Binding> = state
        .router
        .index()
        .list_for_exchange(&vhost, &exchange)
        .into_iter()
        .map(|key| key.to_binding())
        .filter(|binding| {
            binding.queue.as_str() == queue
                && binding_properties_key(binding.routing_key.as_str(), &binding.args)
                    == properties_key
        })
        .collect();
    if targets.is_empty() {
        let vhost_q = vhost.clone();
        let exchange_q = exchange.clone();
        let properties_q = properties_key.clone();
        let queue_q = queue.clone();
        targets = db(&state, move |store| {
            store.list_bindings_for_exchange(&vhost_q, &exchange_q)
        })
        .await?
        .into_iter()
        .filter(|binding| {
            binding.queue.as_str() == queue_q
                && binding_properties_key(binding.routing_key.as_str(), &binding.args)
                    == properties_q
        })
        .collect();
    }
    if targets.is_empty() {
        return Err(MgmtError::NotFound(format!(
            "binding {exchange}→{queue} properties_key={properties_key}"
        )));
    }
    for binding in targets {
        let _ = state.router.unbind(&binding);
        let args_key = binding_args_key(&binding.args);
        let vhost_d = binding.vhost.to_string();
        let exchange_d = binding.exchange.to_string();
        let queue_d = binding.queue.to_string();
        let routing_key = binding.routing_key.to_string();
        let _ = db(&state, move |store| {
            store.delete_binding(&vhost_d, &exchange_d, &queue_d, &routing_key, &args_key)
        })
        .await?;
    }
    Ok(StatusCode::NO_CONTENT)
}

// ── Publish / Get ───────────────────────────────────────────────────────

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

#[derive(Debug, Deserialize)]
pub(crate) struct PutPolicyBody {
    pub(crate) pattern: String,
    #[serde(rename = "apply-to", default = "default_apply_to")]
    pub(crate) apply_to: String,
    #[serde(default)]
    pub(crate) priority: i32,
    #[serde(default)]
    pub(crate) definition: serde_json::Map<String, serde_json::Value>,
}

fn default_apply_to() -> String {
    "all".into()
}

fn policy_u64(map: &serde_json::Map<String, serde_json::Value>, key: &str) -> Result<Option<u64>, MgmtError> {
    match map.get(key) {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::Number(n)) => {
            let v = n.as_u64().ok_or_else(|| MgmtError::BadRequest(format!("{key} must be a positive integer")))?;
            if v == 0 {
                return Err(MgmtError::BadRequest(format!("{key} must be a positive integer")));
            }
            Ok(Some(v))
        }
        Some(serde_json::Value::String(s)) => {
            let v = s.parse::<u64>().ok().filter(|n| *n > 0).ok_or_else(|| {
                MgmtError::BadRequest(format!("{key} must be a positive integer"))
            })?;
            Ok(Some(v))
        }
        _ => Err(MgmtError::BadRequest(format!("{key} must be a positive integer"))),
    }
}

fn policy_str(map: &serde_json::Map<String, serde_json::Value>, key: &str) -> Result<Option<CompactString>, MgmtError> {
    match map.get(key) {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::String(s)) if !s.is_empty() => Ok(Some(CompactString::from(s.as_str()))),
        Some(serde_json::Value::String(_)) => Ok(None),
        _ => Err(MgmtError::BadRequest(format!("{key} must be a string"))),
    }
}

fn policy_item(p: &Policy) -> serde_json::Value {
    let mut definition = serde_json::Map::new();
    if let Some(v) = p.message_ttl_ms {
        definition.insert("message-ttl".into(), serde_json::json!(v));
    }
    if let Some(v) = &p.dead_letter_exchange {
        definition.insert("dead-letter-exchange".into(), serde_json::json!(v.as_str()));
    }
    if let Some(v) = &p.dead_letter_routing_key {
        definition.insert("dead-letter-routing-key".into(), serde_json::json!(v.as_str()));
    }
    if let Some(v) = p.max_length {
        definition.insert("max-length".into(), serde_json::json!(v));
    }
    if let Some(v) = p.max_length_bytes {
        definition.insert("max-length-bytes".into(), serde_json::json!(v));
    }
    if let Some(v) = p.expires_ms {
        definition.insert("expires".into(), serde_json::json!(v));
    }
    if let Some(v) = p.overflow {
        definition.insert("overflow".into(), serde_json::json!(match v {
            queueforge_core::OverflowPolicy::DropHead => "drop-head",
            queueforge_core::OverflowPolicy::RejectPublish => "reject-publish",
            queueforge_core::OverflowPolicy::RejectPublishDlx => "reject-publish-dlx",
        }));
    }
    if let Some(v) = p.delivery_limit {
        definition.insert("delivery-limit".into(), serde_json::json!(v));
    }
    if let Some(v) = &p.alternate_exchange {
        definition.insert("alternate-exchange".into(), serde_json::json!(v.as_str()));
    }
    if let Some(v) = p.dead_letter_strategy {
        definition.insert(
            "dead-letter-strategy".into(),
            serde_json::json!(match v {
                queueforge_core::DeadLetterStrategy::AtMostOnce => "at-most-once",
                queueforge_core::DeadLetterStrategy::AtLeastOnce => "at-least-once",
            }),
        );
    }
    serde_json::json!({
        "vhost": p.vhost.as_str(),
        "name": p.name.as_str(),
        "pattern": p.pattern,
        "apply-to": p.apply_to.as_str(),
        "priority": p.priority,
        "definition": definition,
    })
}

pub(crate) fn policy_from_body(vhost: &str, name: &str, body: PutPolicyBody) -> Result<Policy, MgmtError> {
    let Some(apply_to) = PolicyTarget::parse(&body.apply_to) else {
        return Err(MgmtError::BadRequest(
            "apply-to must be queues, exchanges, or all".into(),
        ));
    };
    if body.pattern.is_empty() {
        return Err(MgmtError::BadRequest("pattern is required".into()));
    }
    const KNOWN: &[&str] = &[
        "message-ttl",
        "dead-letter-exchange",
        "dead-letter-routing-key",
        "max-length",
        "max-length-bytes",
        "expires",
        "overflow",
        "delivery-limit",
        "alternate-exchange",
        "dead-letter-strategy",
        "federation-upstream-set",
    ];
    let unknown: Vec<&str> = body.definition.keys().filter(|k| !KNOWN.contains(&k.as_str())).map(|k| k.as_str()).collect();
    if !unknown.is_empty() {
        return Err(MgmtError::BadRequest(format!(
            "{unknown:?} are not recognised policy settings"
        )));
    }
    Ok(Policy {
        vhost: CompactString::from(vhost),
        name: CompactString::from(name),
        pattern: body.pattern,
        apply_to,
        priority: body.priority,
        message_ttl_ms: policy_u64(&body.definition, "message-ttl")?,
        dead_letter_exchange: policy_str(&body.definition, "dead-letter-exchange")?,
        dead_letter_routing_key: policy_str(&body.definition, "dead-letter-routing-key")?,
        max_length: policy_u64(&body.definition, "max-length")?,
        max_length_bytes: policy_u64(&body.definition, "max-length-bytes")?,
        expires_ms: policy_u64(&body.definition, "expires")?,
        overflow: match policy_str(&body.definition, "overflow")? {
            None => None,
            Some(v) if v.as_str() == "drop-head" => Some(queueforge_core::OverflowPolicy::DropHead),
            Some(v) if v.as_str() == "reject-publish" => Some(queueforge_core::OverflowPolicy::RejectPublish),
            Some(v) if v.as_str() == "reject-publish-dlx" => Some(queueforge_core::OverflowPolicy::RejectPublishDlx),
            Some(v) => {
                return Err(MgmtError::BadRequest(format!(
                    "overflow must be drop-head, reject-publish, or reject-publish-dlx (got {v})"
                )))
            }
        },
        delivery_limit: policy_u64(&body.definition, "delivery-limit")?.map(|n| n as u32),
        alternate_exchange: policy_str(&body.definition, "alternate-exchange")?,
        dead_letter_strategy: match policy_str(&body.definition, "dead-letter-strategy")? {
            None => None,
            Some(v) if v.as_str() == "at-most-once" => Some(queueforge_core::DeadLetterStrategy::AtMostOnce),
            Some(v) if v.as_str() == "at-least-once" => Some(queueforge_core::DeadLetterStrategy::AtLeastOnce),
            Some(v) => {
                return Err(MgmtError::BadRequest(format!(
                    "dead-letter-strategy must be at-most-once or at-least-once (got {v})"
                )))
            }
        },
    })
}

/// GET /api/policies
pub async fn list_policies(
    State(state): State<MgmtState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, MgmtError> {
    let session = require_session(&state, &headers).await?;
    require_management_tag(&session)?;
    let items: Vec<_> = state.router.list_policies(None).iter().map(policy_item).collect();
    Ok(Json(serde_json::json!({ "items": items })))
}

/// GET /api/policies/{vhost}
pub async fn list_policies_vhost(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Path(raw_vhost): Path<String>,
) -> Result<Json<serde_json::Value>, MgmtError> {
    let session = require_session(&state, &headers).await?;
    require_management_tag(&session)?;
    let vhost = decode_vhost(&raw_vhost)?;
    let items: Vec<_> = state
        .router
        .list_policies(Some(&vhost))
        .iter()
        .map(policy_item)
        .collect();
    Ok(Json(serde_json::json!({ "items": items })))
}

/// PUT /api/policies/{vhost}/{name}
pub async fn put_policy(
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
        .list_policies(Some(&vhost))
        .iter()
        .any(|p| p.name.as_str() == name);
    if body.definition.get("federation-upstream-set").is_some() {
        let guard = queueforge_core::federation::upstreams_of(&vhost);
        for upstream in guard {
            queueforge_core::federation::add_federation_link(upstream, vhost.clone(), body.pattern.clone());
        }
    }
    let policy = policy_from_body(&vhost, &name, body)?;
    state.router.upsert_policy(policy.clone()).map_err(|e| MgmtError::BadRequest(e.to_string()))?;
    let stored = policy.clone();
    db(&state, move |s| s.put_policy(&stored)).await?;
    apply_policies_live(&state).await;
    replicate(&state, "policy", serde_json::to_value(&policy).unwrap_or(serde_json::Value::Null)).await;
    Ok(if existed { StatusCode::NO_CONTENT } else { StatusCode::CREATED })
}

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
    let src = json_str(&body.value, "src-queue").ok_or_else(|| MgmtError::BadRequest("src-queue is required".into()))?;
    let dest = json_str(&body.value, "dest-queue").ok_or_else(|| MgmtError::BadRequest("dest-queue is required".into()))?;
    let queues = Arc::clone(&state.queues);
    tokio::spawn(async move {
        loop {
            let key = QueueKey::new("/", &src);
            let Some(handle) = queues.get(&key) else {
                tokio::time::sleep(std::time::Duration::from_millis(30)).await;
                continue;
            };
            let (tx, rx) = oneshot::channel();
            if handle.tx.send(QueueCmd::Get { no_ack: true, reply: tx }).await.is_err() {
                break;
            }
            let Ok(Some((_, message, _))) = rx.await else {
                tokio::time::sleep(std::time::Duration::from_millis(30)).await;
                continue;
            };
            let body = message.message.body.clone();
            let dest_key = QueueKey::new("/", &dest);
            if queues.get(&dest_key).is_none() {
                let _ = queues.declare("/", &dest, QueueDeclareOpts::default()).await;
            }
            let Some(dest_handle) = queues.get(&dest_key) else { continue };
            let mut msg = Message::blank();
            msg.routing_key = CompactString::from(dest.as_str());
            msg.body = body;
            let (reply_tx, reply_rx) = oneshot::channel();
            if dest_handle.tx.send(QueueCmd::Enqueue { msg: Arc::new(msg), reply: reply_tx }).await.is_err() {
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

/// DELETE /api/policies/{vhost}/{name}
pub async fn delete_policy(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Path((raw_vhost, name)): Path<(String, String)>,
) -> Result<StatusCode, MgmtError> {
    let session = require_session(&state, &headers).await?;
    require_administrator(&session)?;
    let vhost = decode_vhost(&raw_vhost)?;
    if !state.router.delete_policy(&vhost, &name) {
        return Err(MgmtError::NotFound(format!("policy '{name}'")));
    }
    let vhost_owned = vhost.clone();
    let name_owned = name.clone();
    let _ = db(&state, move |s| s.delete_policy(&vhost_owned, &name_owned)).await;
    apply_policies_live(&state).await;
    replicate(&state, "delete_policy", serde_json::json!({ "vhost": vhost, "name": name })).await;
    Ok(StatusCode::NO_CONTENT)
}

async fn apply_policies_live(state: &MgmtState) {
    for key in state.queues.list_keys() {
        let Some(handle) = state.queues.get(&key) else {
            continue;
        };
        let declared = handle.info.declared_args.clone();
        let mut merged = state.router.queue_args_with_policy(key.vhost.as_str(), key.name.as_str(), &declared);
        if merged.queue_type == Some(queueforge_core::QueueType::Quorum) && merged.delivery_limit.is_none() {
            merged.delivery_limit = Some(20);
        }
        if let Ok(mut slot) = handle.info.args.lock() {
            *slot = merged.clone();
        }
        let _ = handle.tx.send(queueforge_core::QueueCmd::SetArgs { args: merged.clone() }).await;
        if handle.info.durable {
            let mut row = handle.info.to_domain();
            row.args = merged;
            let _ = db(state, move |s| s.put_queue(&row)).await;
        }
    }
}

async fn ensure_vhost(state: &MgmtState, vhost: &str) -> Result<(), MgmtError> {
    let name = vhost.to_string();
    if db(state, move |s| s.get_vhost(&name)).await?.is_none() {
        return Err(MgmtError::NotFound(format!("vhost '{vhost}'")));
    }
    Ok(())
}
