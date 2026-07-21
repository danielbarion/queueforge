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
    ArgValue, Binding, Exchange, ExchangeType, Message, QueueArgs, QueueCmd, QueueDeclareOpts,
    QueueKey, User, UserTag, BUILTIN_EXCHANGE_NAMES, DEFAULT_EXCHANGE_NAME, DEFAULT_VHOST,
};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use crate::authz::{
    check_bind, check_exchange_configure, check_publish, check_queue_configure, check_queue_read,
    require_administrator, require_management_tag, require_session,
};
use crate::error::MgmtError;
use crate::routes::decode_vhost;
use crate::state::MgmtState;

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
    let session = require_session(&state, &headers)?;
    require_administrator(&session)?;
    let vhost = decode_vhost(&raw_vhost)?;
    if vhost.is_empty() {
        return Err(MgmtError::BadRequest("vhost name required".into()));
    }
    match state.store.create_vhost(&vhost) {
        Ok(_) => {
            // Load builtins into live router for this vhost.
            for ex in Exchange::builtins_for(vhost.as_str()) {
                state.router.put_exchange(ex);
            }
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
    let session = require_session(&state, &headers)?;
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
    let deleted = state.store.delete_vhost(&vhost)?;
    if deleted {
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
    let session = require_session(&state, &headers)?;
    require_administrator(&session)?;
    let mut users = state.store.list_users()?;
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
    let session = require_session(&state, &headers)?;
    require_administrator(&session)?;
    if name.is_empty() {
        return Err(MgmtError::BadRequest("username required".into()));
    }
    let tags = parse_tags(&body.tags)?;
    let auth = AuthService::new(state.store.as_ref());

    match state.store.get_user(&name)? {
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
            state.store.create_user(&user)?;
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
            state.store.put_user(&existing)?;
            let _ = auth;
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
    let session = require_session(&state, &headers)?;
    require_administrator(&session)?;
    if name == session.username {
        return Err(MgmtError::BadRequest(
            "cannot delete the currently authenticated user".into(),
        ));
    }
    if !state.store.delete_user(&name)? {
        return Err(MgmtError::NotFound(format!("user '{name}'")));
    }
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
    let session = require_session(&state, &headers)?;
    require_administrator(&session)?;
    let mut perms = state.store.list_permissions()?;
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
    let session = require_session(&state, &headers)?;
    require_administrator(&session)?;
    let vhost = decode_vhost(&raw_vhost)?;
    if state.store.get_user(&user)?.is_none() {
        return Err(MgmtError::NotFound(format!("user '{user}'")));
    }
    if state.store.get_vhost(&vhost)?.is_none() {
        return Err(MgmtError::NotFound(format!("vhost '{vhost}'")));
    }
    let auth = AuthService::new(state.store.as_ref());
    auth.set_permission(&user, &vhost, &body.configure, &body.write, &body.read)?;
    Ok(StatusCode::NO_CONTENT)
}

/// DELETE /api/permissions/{user}/{vhost}
pub async fn delete_permission(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Path((user, raw_vhost)): Path<(String, String)>,
) -> Result<StatusCode, MgmtError> {
    let session = require_session(&state, &headers)?;
    require_administrator(&session)?;
    let vhost = decode_vhost(&raw_vhost)?;
    if !state.store.delete_permission(&user, &vhost)? {
        return Err(MgmtError::NotFound(format!(
            "permission '{user}'@'{vhost}'"
        )));
    }
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
    let session = require_session(&state, &headers)?;
    require_management_tag(&session)?;
    let vhost = decode_vhost(&raw_vhost)?;
    ensure_vhost(&state, &vhost)?;
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
    check_exchange_configure(&state, &session.username, &vhost, &name)?;

    let kind = parse_exchange_type(&body.kind)?;
    let new_ex = Exchange {
        vhost: CompactString::from(vhost.as_str()),
        name: CompactString::from(name.as_str()),
        kind,
        durable: body.durable,
        auto_delete: body.auto_delete,
        internal: body.internal,
    };

    if let Some(existing) = state
        .router
        .get_exchange(&vhost, &name)
        .or_else(|| state.store.get_exchange(&vhost, &name).ok().flatten())
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
        state.store.create_exchange(&new_ex)?;
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
    let session = require_session(&state, &headers)?;
    require_management_tag(&session)?;
    let vhost = decode_vhost(&raw_vhost)?;
    if name.is_empty() || BUILTIN_EXCHANGE_NAMES.contains(&name.as_str()) {
        return Err(MgmtError::BadRequest(
            "cannot delete builtin exchange".into(),
        ));
    }
    check_exchange_configure(&state, &session.username, &vhost, &name)?;

    if state.router.get_exchange(&vhost, &name).is_none()
        && state.store.get_exchange(&vhost, &name)?.is_none()
    {
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

    let _ = state.store.delete_exchange(&vhost, &name);
    for b in state
        .store
        .list_bindings_for_exchange(&vhost, &name)
        .unwrap_or_default()
    {
        let _ = state.store.delete_binding(
            b.vhost.as_str(),
            b.exchange.as_str(),
            b.queue.as_str(),
            b.routing_key.as_str(),
        );
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
fn parse_mgmt_queue_args(
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
    let session = require_session(&state, &headers)?;
    require_management_tag(&session)?;
    let vhost = decode_vhost(&raw_vhost)?;
    ensure_vhost(&state, &vhost)?;
    if name.is_empty() {
        return Err(MgmtError::BadRequest("queue name required".into()));
    }
    if body.exclusive {
        return Err(MgmtError::BadRequest(
            "exclusive queues cannot be declared via management".into(),
        ));
    }
    check_queue_configure(&state, &session.username, &vhost, &name)?;

    let args = parse_mgmt_queue_args(&body.arguments)?;
    let opts = QueueDeclareOpts {
        durable: body.durable,
        exclusive: false,
        auto_delete: body.auto_delete,
        passive: false,
        exclusive_owner: None,
        args,
    };
    let existed = state.queues.get(&QueueKey::new(&vhost, &name)).is_some()
        || state.store.get_queue(&vhost, &name)?.is_some();
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
    let session = require_session(&state, &headers)?;
    require_management_tag(&session)?;
    let vhost = decode_vhost(&raw_vhost)?;
    check_queue_configure(&state, &session.username, &vhost, &name)?;
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
    for b in state.store.list_bindings(&vhost).unwrap_or_default() {
        if b.queue.as_str() == name {
            let _ = state.store.delete_binding(
                b.vhost.as_str(),
                b.exchange.as_str(),
                b.queue.as_str(),
                b.routing_key.as_str(),
            );
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
    let session = require_session(&state, &headers)?;
    require_management_tag(&session)?;
    let vhost = decode_vhost(&raw_vhost)?;
    // Purge requires read (or configure); design lists purge under resource AuthZ.
    // AMQP queue.purge uses configure on RabbitMQ; our design says configure/write/read
    // checked on purge — use configure for parity with declare/delete.
    check_queue_configure(&state, &session.username, &vhost, &name)?;
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
        properties_key: b.routing_key.to_string(),
    }
}

/// GET /api/bindings/{vhost}
pub async fn list_bindings(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Path(raw_vhost): Path<String>,
) -> Result<Json<Vec<BindingItem>>, MgmtError> {
    let _session = require_session(&state, &headers)?;
    let vhost = decode_vhost(&raw_vhost)?;
    ensure_vhost(&state, &vhost)?;
    let mut items: Vec<BindingItem> = state
        .router
        .index()
        .list()
        .into_iter()
        .filter(|k| k.vhost.as_str() == vhost)
        .map(|k| binding_item(&k.to_binding()))
        .collect();
    // Also include durable-only store bindings not yet in live index.
    for b in state.store.list_bindings(&vhost)? {
        if !items.iter().any(|i| {
            i.source == b.exchange.as_str()
                && i.destination == b.queue.as_str()
                && i.routing_key == b.routing_key.as_str()
        }) {
            items.push(binding_item(&b));
        }
    }
    items.sort_by(|a, b| {
        a.source
            .cmp(&b.source)
            .then_with(|| a.destination.cmp(&b.destination))
            .then_with(|| a.routing_key.cmp(&b.routing_key))
    });
    Ok(Json(items))
}

/// GET /api/exchanges/{vhost}/{name}/bindings
pub async fn list_exchange_bindings(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Path((raw_vhost, name)): Path<(String, String)>,
) -> Result<Json<Vec<BindingItem>>, MgmtError> {
    let _session = require_session(&state, &headers)?;
    let vhost = decode_vhost(&raw_vhost)?;
    let mut items: Vec<BindingItem> = state
        .router
        .index()
        .list_for_exchange(&vhost, &name)
        .into_iter()
        .map(|k| binding_item(&k.to_binding()))
        .collect();
    for b in state.store.list_bindings_for_exchange(&vhost, &name)? {
        if !items
            .iter()
            .any(|i| i.destination == b.queue.as_str() && i.routing_key == b.routing_key.as_str())
        {
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
    let _session = require_session(&state, &headers)?;
    let vhost = decode_vhost(&raw_vhost)?;
    let mut items: Vec<BindingItem> = state
        .router
        .index()
        .list()
        .into_iter()
        .filter(|k| k.vhost.as_str() == vhost && k.queue.as_str() == name)
        .map(|k| binding_item(&k.to_binding()))
        .collect();
    for b in state.store.list_bindings(&vhost)? {
        if b.queue.as_str() == name
            && !items
                .iter()
                .any(|i| i.source == b.exchange.as_str() && i.routing_key == b.routing_key.as_str())
        {
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
    let session = require_session(&state, &headers)?;
    require_management_tag(&session)?;
    let vhost = decode_vhost(&raw_vhost)?;
    ensure_vhost(&state, &vhost)?;
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
    )?;

    // Ensure exchange exists.
    if state.router.get_exchange(&vhost, &body.source).is_none() {
        if let Some(ex) = state.store.get_exchange(&vhost, &body.source)?.as_ref() {
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

    let binding = Binding::new(
        vhost.as_str(),
        body.source.as_str(),
        body.destination.as_str(),
        body.routing_key.as_str(),
    );

    match state.router.bind(binding.clone()) {
        Ok(true) => {
            // Persist when both durable.
            let ex_durable = state
                .router
                .get_exchange(&vhost, &body.source)
                .map(|e| e.durable)
                .unwrap_or(false);
            if ex_durable && handle.info.durable {
                if let Err(e) = state.store.create_binding(&binding) {
                    let _ = state.router.unbind(&binding);
                    return Err(MgmtError::from(e));
                }
            }
            Ok((StatusCode::CREATED, Json(binding_item(&binding))).into_response())
        }
        Ok(false) => Ok(StatusCode::NO_CONTENT.into_response()),
        Err(e) => Err(MgmtError::from(e)),
    }
}

/// DELETE /api/bindings/{vhost}/{exchange}/{queue}/{routing_key}
pub async fn delete_binding(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Path((raw_vhost, exchange, queue, routing_key)): Path<(String, String, String, String)>,
) -> Result<StatusCode, MgmtError> {
    let session = require_session(&state, &headers)?;
    require_management_tag(&session)?;
    let vhost = decode_vhost(&raw_vhost)?;
    if exchange.is_empty() {
        return Err(MgmtError::BadRequest(
            "cannot unbind from the default exchange".into(),
        ));
    }
    check_bind(&state, &session.username, &vhost, &queue, &exchange)?;
    let binding = Binding::new(
        vhost.as_str(),
        exchange.as_str(),
        queue.as_str(),
        routing_key.as_str(),
    );
    let live = state.router.unbind(&binding).unwrap_or(false);
    let store = state
        .store
        .delete_binding(&vhost, &exchange, &queue, &routing_key)?;
    if live || store {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(MgmtError::NotFound(format!(
            "binding {exchange}→{queue} rk={routing_key}"
        )))
    }
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
    let session = require_session(&state, &headers)?;
    require_management_tag(&session)?;
    let vhost = decode_vhost(&raw_vhost)?;
    ensure_vhost(&state, &vhost)?;
    // Path may use `amq.default` for the default exchange; accept empty too.
    let exchange = if exchange_name == "amq.default" {
        DEFAULT_EXCHANGE_NAME.to_string()
    } else {
        exchange_name
    };
    check_publish(&state, &session.username, &vhost, &exchange)?;

    if !exchange.is_empty() && state.router.get_exchange(&vhost, &exchange).is_none() {
        if let Some(ex) = state.store.get_exchange(&vhost, &exchange)?.as_ref() {
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
    let session = require_session(&state, &headers)?;
    require_management_tag(&session)?;
    let vhost = decode_vhost(&raw_vhost)?;
    check_queue_read(&state, &session.username, &vhost, &name)?;

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

fn ensure_vhost(state: &MgmtState, vhost: &str) -> Result<(), MgmtError> {
    if state.store.get_vhost(vhost)?.is_none() {
        return Err(MgmtError::NotFound(format!("vhost '{vhost}'")));
    }
    Ok(())
}
