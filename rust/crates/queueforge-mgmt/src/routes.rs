//! Axum route handlers for the management HTTP API.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;

use axum::extract::{ConnectInfo, Path, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post, put};
use axum::{Json, Router};
use queueforge_auth::AuthService;
use queueforge_core::{QueueCmd, QueueKey, QueueStats, User, UserTag, DEFAULT_VHOST};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use tracing::debug;

use crate::authz::{require_management_tag, require_session};
use crate::definitions::{export_definitions, import_definitions};
use crate::error::MgmtError;
use crate::mutations::{
    create_binding, delete_binding, delete_exchange, delete_permission, delete_queue, delete_user,
    delete_vhost, get_messages, list_bindings, list_exchange_bindings, list_permissions,
    delete_policy, list_policies, list_policies_vhost, list_queue_bindings, list_users, publish,
    purge_queue, put_exchange, put_permission, put_policy, put_queue, put_user, put_vhost,
};
use crate::pagination::{paginate_by_name, ListQuery, Page};
use crate::session::{
    build_session_cookie, clear_session_cookie, extract_token_from_cookie_header,
};
use crate::state::MgmtState;

async fn db<T, F>(state: &MgmtState, f: F) -> Result<T, MgmtError>
where
    T: Send + 'static,
    F: FnOnce(&queueforge_store::MetadataStore) -> queueforge_store::Result<T> + Send + 'static,
{
    queueforge_store::MetadataStore::blocking(std::sync::Arc::clone(&state.store), f)
        .await
        .map_err(MgmtError::from)
}

/// Build the full management router.
pub fn router(state: MgmtState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/api/login", post(login))
        .route("/api/logout", post(logout))
        .route("/api/whoami", get(whoami))
        .route("/api/overview", get(overview))
        .route("/api/vhosts", get(list_vhosts))
        .route("/api/vhosts/{vhost}", put(put_vhost).delete(delete_vhost))
        .route("/api/users", get(list_users))
        .route("/api/users/{name}", put(put_user).delete(delete_user))
        .route("/api/permissions", get(list_permissions))
        .route(
            "/api/permissions/{user}/{vhost}",
            put(put_permission).delete(delete_permission),
        )
        .route("/api/queues/{vhost}", get(list_queues))
        .route(
            "/api/queues/{vhost}/{name}",
            put(put_queue).delete(delete_queue),
        )
        .route("/api/queues/{vhost}/{name}/purge", post(purge_queue))
        .route("/api/queues/{vhost}/{name}/get", post(get_messages))
        .route(
            "/api/queues/{vhost}/{name}/bindings",
            get(list_queue_bindings),
        )
        .route("/api/exchanges/{vhost}", get(list_exchanges))
        .route(
            "/api/exchanges/{vhost}/{name}",
            put(put_exchange).delete(delete_exchange),
        )
        .route(
            "/api/exchanges/{vhost}/{name}/bindings",
            get(list_exchange_bindings),
        )
        .route("/api/exchanges/{vhost}/{name}/publish", post(publish))
        .route(
            "/api/bindings/{vhost}",
            get(list_bindings).post(create_binding),
        )
        .route(
            "/api/bindings/{vhost}/{exchange}/{queue}/{routing_key}",
            delete(delete_binding),
        )
        .route("/api/connections", get(list_connections))
        .route("/api/connections/{name}", delete(delete_connection))
        .route(
            "/api/definitions",
            get(export_definitions).post(import_definitions),
        )
        .route("/api/policies", get(list_policies))
        .route("/api/policies/{vhost}", get(list_policies_vhost))
        .route(
            "/api/policies/{vhost}/{name}",
            put(put_policy).delete(delete_policy),
        )
        .route(
            "/api/parameters/shovel/{vhost}/{name}",
            put(crate::mutations::put_shovel),
        )
        .route(
            "/api/parameters/federation-upstream/{vhost}/{name}",
            put(crate::mutations::put_federation_upstream),
        )
        .fallback(crate::spa::static_handler)
        .with_state(state)
}

// ── Health ──────────────────────────────────────────────────────────────

async fn healthz() -> impl IntoResponse {
    (StatusCode::OK, "ok\n")
}

async fn readyz(State(state): State<MgmtState>) -> Response {
    if state.ready.is_ready() {
        (StatusCode::OK, "ready\n").into_response()
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "not ready\n").into_response()
    }
}

// ── Auth ────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct LoginRequest {
    username: String,
    password: String,
}

#[derive(Debug, Serialize)]
struct WhoamiResponse {
    name: String,
    tags: Vec<&'static str>,
}

/// Login rate-limit key. Trust `X-Forwarded-For` only from a configured proxy.
pub(crate) fn login_client_key(peer: SocketAddr, headers: &HeaderMap, cidrs: &[String]) -> String {
    if peer_in_cidrs(peer.ip(), cidrs) {
        if let Some(xff) = headers.get("x-forwarded-for").and_then(|v| v.to_str().ok()) {
            if let Some(first) = xff.split(',').next() {
                let first = first.trim();
                if !first.is_empty() {
                    return first.to_string();
                }
            }
        }
    }
    peer.ip().to_string()
}

fn peer_in_cidrs(ip: IpAddr, cidrs: &[String]) -> bool {
    cidrs.iter().any(|c| ip_in_cidr(ip, c))
}

fn ip_in_cidr(ip: IpAddr, cidr: &str) -> bool {
    let cidr = cidr.trim();
    if let Some((addr, prefix)) = cidr.split_once('/') {
        let Ok(bits) = prefix.parse::<u8>() else {
            return false;
        };
        match (ip, addr.parse::<IpAddr>()) {
            (IpAddr::V4(ip), Ok(IpAddr::V4(base))) => ipv4_in_prefix(ip, base, bits),
            (IpAddr::V6(ip), Ok(IpAddr::V6(base))) => ipv6_in_prefix(ip, base, bits),
            _ => false,
        }
    } else {
        cidr.parse::<IpAddr>().ok() == Some(ip)
    }
}

fn ipv4_in_prefix(ip: Ipv4Addr, base: Ipv4Addr, bits: u8) -> bool {
    if bits > 32 {
        return false;
    }
    let mask = if bits == 0 {
        0u32
    } else {
        u32::MAX << (32 - bits)
    };
    (u32::from(ip) & mask) == (u32::from(base) & mask)
}

fn ipv6_in_prefix(ip: Ipv6Addr, base: Ipv6Addr, bits: u8) -> bool {
    if bits > 128 {
        return false;
    }
    let mask = if bits == 0 {
        0u128
    } else {
        u128::MAX << (128 - bits)
    };
    (u128::from(ip) & mask) == (u128::from(base) & mask)
}

async fn login(
    State(state): State<MgmtState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(body): Json<LoginRequest>,
) -> Result<Response, MgmtError> {
    let ip = login_client_key(peer, &headers, &state.config.trusted_proxy_cidrs);

    if state.sessions.is_login_rate_limited(&ip) {
        return Err(MgmtError::TooManyRequests);
    }

    if body.username.is_empty() || body.password.is_empty() {
        return Err(MgmtError::BadRequest(
            "username and password are required".into(),
        ));
    }

    // Argon2 is intentionally slow — run off the async runtime.
    let store = Arc::clone(&state.store);
    let username = body.username.clone();
    let password = body.password.clone();
    let user = tokio::task::spawn_blocking(move || {
        let auth = AuthService::new(store.as_ref());
        auth.authenticate(&username, &password)
    })
    .await
    .map_err(|e| MgmtError::Internal(format!("login task join: {e}")))?
    .map_err(MgmtError::from)?;

    let Some(user) = user else {
        state.sessions.record_login_failure(&ip);
        return Err(MgmtError::Unauthorized);
    };

    if !can_access_management(&user) {
        state.sessions.record_login_failure(&ip);
        return Err(MgmtError::Forbidden);
    }

    state.sessions.clear_login_failures(&ip);
    let token = state.sessions.create(user.name.as_str(), user.tags.clone());

    let cookie = build_session_cookie(&token, state.config.cookie_secure);
    let body = WhoamiResponse {
        name: user.name.to_string(),
        tags: tags_as_str(&user.tags),
    };

    let mut res = (StatusCode::OK, Json(body)).into_response();
    res.headers_mut().insert(
        header::SET_COOKIE,
        cookie
            .parse()
            .map_err(|e| MgmtError::Internal(format!("cookie header: {e}")))?,
    );
    Ok(res)
}

async fn logout(State(state): State<MgmtState>, headers: HeaderMap) -> Result<Response, MgmtError> {
    if let Some(token) = session_token_from_headers(&headers) {
        state.sessions.remove(&token);
    }
    let cookie = clear_session_cookie(state.config.cookie_secure);
    let mut res = StatusCode::NO_CONTENT.into_response();
    res.headers_mut().insert(
        header::SET_COOKIE,
        cookie
            .parse()
            .map_err(|e| MgmtError::Internal(format!("cookie header: {e}")))?,
    );
    Ok(res)
}

async fn whoami(
    State(state): State<MgmtState>,
    headers: HeaderMap,
) -> Result<Json<WhoamiResponse>, MgmtError> {
    let session = require_session(&state, &headers).await?;
    Ok(Json(WhoamiResponse {
        name: session.username,
        tags: tags_as_str(&session.tags),
    }))
}

// ── Overview ────────────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
struct OverviewResponse {
    product_name: &'static str,
    product_version: String,
    management_version: String,
    rabbitmq_version_compat: &'static str,
    object_totals: ObjectTotals,
    queue_totals: QueueTotals,
}

#[derive(Debug, Serialize)]
struct ObjectTotals {
    connections: u64,
    channels: u64,
    queues: u64,
    exchanges: u64,
    consumers: u64,
    vhosts: u64,
}

#[derive(Debug, Serialize)]
struct QueueTotals {
    messages: u64,
    messages_ready: u64,
    messages_unacknowledged: u64,
}

async fn overview(
    State(state): State<MgmtState>,
    headers: HeaderMap,
) -> Result<Json<OverviewResponse>, MgmtError> {
    let _session = require_session(&state, &headers).await?;

    let (vhosts, exchange_count) = db(&state, |s| {
        let vhosts = s.list_vhosts()?;
        let mut exchange_count = 0u64;
        for vh in &vhosts {
            exchange_count += s.list_exchanges(vh.name.as_str())?.len() as u64;
        }
        Ok((vhosts, exchange_count))
    })
    .await?;

    let connections = state.connections.list();
    let channel_count: u64 = connections.iter().map(|c| c.channels as u64).sum();

    let mut messages_ready = 0u64;
    let mut messages_unacked = 0u64;
    let mut consumers = 0u64;
    for key in state.queues.list_keys() {
        if let Some(handle) = state.queues.get(&key) {
            if let Some(stats) = query_stats(&handle.tx).await {
                messages_ready += stats.messages_ready as u64;
                messages_unacked += stats.messages_unacked as u64;
                consumers += stats.consumer_count as u64;
            }
        }
    }

    Ok(Json(OverviewResponse {
        product_name: "QueueForge",
        product_version: state.config.product_version.clone(),
        management_version: state.config.product_version.clone(),
        rabbitmq_version_compat: "0.9.1",
        object_totals: ObjectTotals {
            connections: connections.len() as u64,
            channels: channel_count,
            queues: state.queues.len() as u64,
            exchanges: exchange_count,
            consumers,
            vhosts: vhosts.len() as u64,
        },
        queue_totals: QueueTotals {
            messages: messages_ready + messages_unacked,
            messages_ready,
            messages_unacknowledged: messages_unacked,
        },
    }))
}

// ── Vhosts ──────────────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
struct VhostItem {
    name: String,
}

async fn list_vhosts(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Query(query): Query<ListQuery>,
) -> Result<Json<Page<VhostItem>>, MgmtError> {
    let _session = require_session(&state, &headers).await?;
    let mut items: Vec<VhostItem> = db(&state, |s| s.list_vhosts())
        .await?
        .into_iter()
        .map(|v| VhostItem {
            name: v.name.to_string(),
        })
        .collect();
    items.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(Json(paginate_by_name(items, &query, |v| v.name.as_str())))
}

// ── Queues ──────────────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
struct QueueItem {
    name: String,
    vhost: String,
    durable: bool,
    exclusive: bool,
    auto_delete: bool,
    state: String,
    messages: u64,
    messages_ready: u64,
    messages_unacknowledged: u64,
    consumers: u32,
    #[serde(rename = "type")]
    queue_type: String,
    arguments: serde_json::Value,
}

async fn list_queues(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Path(vhost): Path<String>,
    Query(query): Query<ListQuery>,
) -> Result<Json<Page<QueueItem>>, MgmtError> {
    let _session = require_session(&state, &headers).await?;
    let vhost = decode_vhost(&vhost)?;

    // Ensure vhost exists.
    let vhost_lookup = vhost.clone();
    if db(&state, move |s| s.get_vhost(&vhost_lookup))
        .await?
        .is_none()
    {
        return Err(MgmtError::NotFound(format!("vhost {vhost}")));
    }

    let keys = state.queues.list_keys();
    let mut items = Vec::new();
    for key in keys {
        if key.vhost.as_str() != vhost.as_str() {
            continue;
        }
        let Some(handle) = state.queues.get(&key) else {
            continue;
        };
        let stats = query_stats(&handle.tx).await.unwrap_or_default();
        let state_str = match handle.info.state() {
            queueforge_core::QueueActorState::Running => "running",
            queueforge_core::QueueActorState::Unavailable => "unavailable",
        };
        items.push(QueueItem {
            name: key.name.to_string(),
            vhost: key.vhost.to_string(),
            durable: handle.info.durable,
            exclusive: handle.info.exclusive,
            auto_delete: handle.info.auto_delete,
            state: state_str.into(),
            messages: (stats.messages_ready as u64) + (stats.messages_unacked as u64),
            messages_ready: stats.messages_ready as u64,
            messages_unacknowledged: stats.messages_unacked as u64,
            consumers: stats.consumer_count,
            queue_type: handle
                .info
                .args
                .lock()
                .map(|a| a.queue_type.unwrap_or(queueforge_core::QueueType::Classic).as_str().to_string())
                .unwrap_or_else(|_| "classic".into()),
            arguments: handle
                .info
                .args
                .lock()
                .map(|a| serde_json::to_value(&*a).unwrap_or(serde_json::json!({})))
                .unwrap_or(serde_json::json!({})),
        });
    }

    // Include durable queues present only in metadata (not yet live).
    let vhost_queues = vhost.clone();
    for q in db(&state, move |s| s.list_queues(&vhost_queues)).await? {
        let key = QueueKey::new(q.vhost.as_str(), q.name.as_str());
        if state.queues.get(&key).is_some() {
            continue;
        }
        items.push(QueueItem {
            name: q.name.to_string(),
            vhost: q.vhost.to_string(),
            durable: q.durable,
            exclusive: q.exclusive,
            auto_delete: q.auto_delete,
            state: "idle".into(),
            messages: 0,
            messages_ready: 0,
            messages_unacknowledged: 0,
            consumers: 0,
            queue_type: q
                .args
                .queue_type
                .unwrap_or(queueforge_core::QueueType::Classic)
                .as_str()
                .to_string(),
            arguments: serde_json::to_value(&q.args).unwrap_or(serde_json::json!({})),
        });
    }

    items.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(Json(paginate_by_name(items, &query, |q| q.name.as_str())))
}

// ── Exchanges ───────────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
struct ExchangeItem {
    name: String,
    vhost: String,
    #[serde(rename = "type")]
    kind: String,
    durable: bool,
    auto_delete: bool,
    internal: bool,
}

async fn list_exchanges(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Path(vhost): Path<String>,
    Query(query): Query<ListQuery>,
) -> Result<Json<Page<ExchangeItem>>, MgmtError> {
    let _session = require_session(&state, &headers).await?;
    let vhost = decode_vhost(&vhost)?;

    let vhost_lookup = vhost.clone();
    if db(&state, move |s| s.get_vhost(&vhost_lookup))
        .await?
        .is_none()
    {
        return Err(MgmtError::NotFound(format!("vhost {vhost}")));
    }

    let vhost_ex = vhost.clone();
    let mut items: Vec<ExchangeItem> = db(&state, move |s| s.list_exchanges(&vhost_ex))
        .await?
        .into_iter()
        .map(|ex| ExchangeItem {
            name: ex.name.to_string(),
            vhost: ex.vhost.to_string(),
            kind: ex.kind.as_str().to_string(),
            durable: ex.durable,
            auto_delete: ex.auto_delete,
            internal: ex.internal,
        })
        .collect();
    items.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(Json(paginate_by_name(items, &query, |e| e.name.as_str())))
}

// ── Connections ─────────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
struct ConnectionItem {
    name: String,
    user: String,
    vhost: String,
    peer_host: String,
    peer_port: u16,
    channels: u32,
    connected_at: u64,
}

async fn list_connections(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Query(query): Query<ListQuery>,
) -> Result<Json<Page<ConnectionItem>>, MgmtError> {
    let _session = require_session(&state, &headers).await?;
    let mut items: Vec<ConnectionItem> = state
        .connections
        .list()
        .into_iter()
        .map(|c| ConnectionItem {
            name: c.id.clone(),
            user: c.user,
            vhost: c.vhost,
            peer_host: c.peer.ip().to_string(),
            peer_port: c.peer.port(),
            channels: c.channels,
            connected_at: c.connected_at,
        })
        .collect();
    items.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(Json(paginate_by_name(items, &query, |c| c.name.as_str())))
}

/// DELETE /api/connections/{name} — force-close a live AMQP connection.
async fn delete_connection(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Result<StatusCode, MgmtError> {
    let session = require_session(&state, &headers).await?;
    // Operators with management (or stronger) may force-close.
    require_management_tag(&session)?;
    if state.connections.force_close(&name) {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(MgmtError::NotFound(format!("connection '{name}'")))
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────

fn can_access_management(user: &User) -> bool {
    user.has_management() || user.has_monitoring()
}

fn tags_as_str(tags: &[UserTag]) -> Vec<&'static str> {
    tags.iter().map(|t| t.as_str()).collect()
}

fn session_token_from_headers(headers: &HeaderMap) -> Option<String> {
    let raw = headers.get(header::COOKIE)?.to_str().ok()?;
    extract_token_from_cookie_header(raw)
}

/// Decode a path-captured vhost. Axum percent-decodes path params, so `%2F`
/// arrives as `/`. Also accept the literal `%2F` if a proxy left it encoded.
pub fn decode_vhost(raw: &str) -> Result<String, MgmtError> {
    if raw.is_empty() {
        // `/api/queues/` with empty segment → treat as default vhost.
        return Ok(DEFAULT_VHOST.to_string());
    }
    // If still percent-encoded (e.g. double-encoding avoided by client tools).
    if raw.contains('%') {
        let decoded = percent_decode(raw).map_err(MgmtError::BadRequest)?;
        return Ok(decoded);
    }
    Ok(raw.to_string())
}

fn percent_decode(input: &str) -> Result<String, String> {
    let mut out = Vec::with_capacity(input.len());
    let bytes = input.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                let h = std::str::from_utf8(&bytes[i + 1..i + 3]).map_err(|e| e.to_string())?;
                let v = u8::from_str_radix(h, 16).map_err(|e| e.to_string())?;
                out.push(v);
                i += 3;
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8(out).map_err(|e| e.to_string())
}

async fn query_stats(tx: &tokio::sync::mpsc::Sender<QueueCmd>) -> Option<QueueStats> {
    let (reply_tx, reply_rx) = oneshot::channel();
    if tx.send(QueueCmd::Stats { reply: reply_tx }).await.is_err() {
        return None;
    }
    match tokio::time::timeout(std::time::Duration::from_secs(2), reply_rx).await {
        Ok(Ok(stats)) => Some(stats),
        _ => {
            debug!("queue stats query timed out or cancelled");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connections::ConnectionTracker;
    use crate::session::{LOGIN_FAIL_MAX, SESSION_COOKIE_NAME};
    use crate::state::MgmtConfig;
    use axum::body::Body;
    use axum::extract::connect_info::MockConnectInfo;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use queueforge_auth::{AuthService, BootstrapMode};
    use queueforge_core::{MemoryTracker, QueueDeclareOpts, QueueMetaStore, QueueRegistry};
    use queueforge_metrics::ReadyFlag;
    use queueforge_store::MetadataStore;
    use tempfile::TempDir;
    use tower::ServiceExt;

    async fn test_state() -> (TempDir, MgmtState) {
        let dir = TempDir::new().unwrap();
        let store = MetadataStore::open(dir.path()).unwrap();
        let auth = AuthService::new(&store);
        assert!(auth
            .bootstrap_admin_if_empty(BootstrapMode::DevFallback)
            .unwrap());
        let store = Arc::new(store);
        let queues = QueueRegistry::shared(
            Arc::clone(&store) as Arc<dyn QueueMetaStore>,
            MemoryTracker::shared(),
        );
        let ready = ReadyFlag::new();
        ready.set_ready(true);
        let router = std::sync::Arc::new(store.bootstrap_router().expect("bootstrap router"));
        let state = MgmtState::new(
            store,
            queues,
            router,
            ConnectionTracker::shared(),
            ready,
            MgmtConfig::default(),
        );
        (dir, state)
    }

    /// Router with mocked peer IP for ConnectInfo extraction in oneshot tests.
    fn test_app(state: MgmtState, peer: SocketAddr) -> Router {
        router(state).layer(MockConnectInfo(peer))
    }

    fn peer(ip: [u8; 4], port: u16) -> SocketAddr {
        SocketAddr::from((ip, port))
    }

    async fn body_json(res: Response) -> serde_json::Value {
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| serde_json::Value::String(String::from_utf8_lossy(&bytes).into()))
    }

    fn cookie_from(res: &Response) -> Option<String> {
        res.headers()
            .get(header::SET_COOKIE)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.split(';').next().map(|p| p.trim().to_string()))
    }

    fn login_body(user: &str, pass: &str) -> Body {
        Body::from(format!(r#"{{"username":"{user}","password":"{pass}"}}"#))
    }

    async fn post_login(app: Router, body: Body) -> Response {
        app.oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/login")
                .header(header::CONTENT_TYPE, "application/json")
                .body(body)
                .unwrap(),
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn login_sets_session_cookie_and_whoami() {
        let (_dir, state) = test_state().await;
        let app = test_app(state, peer([127, 0, 0, 1], 10001));

        let res = post_login(app.clone(), login_body("admin", "devpassword12")).await;
        assert_eq!(res.status(), StatusCode::OK);
        let cookie = cookie_from(&res).expect("Set-Cookie");
        assert!(cookie.starts_with(&format!("{SESSION_COOKIE_NAME}=")));
        // Dev defaults: HttpOnly + SameSite=Lax, no Secure flag.
        let set_cookie = res
            .headers()
            .get(header::SET_COOKIE)
            .unwrap()
            .to_str()
            .unwrap();
        assert!(set_cookie.contains("HttpOnly"));
        assert!(
            !set_cookie
                .split(';')
                .any(|p| p.trim().eq_ignore_ascii_case("Secure")),
            "dev cookie must not be Secure: {set_cookie}"
        );

        let json = body_json(res).await;
        assert_eq!(json["name"], "admin");

        let res = app
            .oneshot(
                Request::builder()
                    .uri("/api/whoami")
                    .header(header::COOKIE, &cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let json = body_json(res).await;
        assert_eq!(json["name"], "admin");
    }

    /// When TLS is enabled, `MgmtConfig.cookie_secure=true` must mark Set-Cookie Secure.
    #[tokio::test]
    async fn login_sets_secure_cookie_when_cookie_secure_true() {
        let dir = TempDir::new().unwrap();
        let store = MetadataStore::open(dir.path()).unwrap();
        let auth = AuthService::new(&store);
        assert!(auth
            .bootstrap_admin_if_empty(BootstrapMode::DevFallback)
            .unwrap());
        let store = Arc::new(store);
        let queues = QueueRegistry::shared(
            Arc::clone(&store) as Arc<dyn QueueMetaStore>,
            MemoryTracker::shared(),
        );
        let ready = ReadyFlag::new();
        ready.set_ready(true);
        let router = std::sync::Arc::new(store.bootstrap_router().expect("bootstrap router"));
        let state = MgmtState::new(
            store,
            queues,
            router,
            ConnectionTracker::shared(),
            ready,
            MgmtConfig {
                cookie_secure: true,
                product_version: "0.1.0-test".into(),
                trusted_proxy_cidrs: Vec::new(),
            },
        );
        let app = test_app(state, peer([127, 0, 0, 1], 10011));

        let res = post_login(app, login_body("admin", "devpassword12")).await;
        assert_eq!(res.status(), StatusCode::OK);
        let set_cookie = res
            .headers()
            .get(header::SET_COOKIE)
            .expect("Set-Cookie")
            .to_str()
            .unwrap();
        assert!(
            set_cookie
                .split(';')
                .any(|p| p.trim().eq_ignore_ascii_case("Secure")),
            "TLS cookie must include Secure: {set_cookie}"
        );
        assert!(set_cookie.contains("HttpOnly"));
        assert!(set_cookie.contains("SameSite=Lax") || set_cookie.contains("SameSite=lax"));
    }

    #[tokio::test]
    async fn login_bad_password_unauthorized() {
        let (_dir, state) = test_state().await;
        let app = test_app(state, peer([127, 0, 0, 1], 10002));
        let res = post_login(app, login_body("admin", "wrong-password")).await;
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn login_rate_limit_uses_peer_ip_not_xff() {
        let (_dir, state) = test_state().await;
        let peer_a = peer([10, 0, 0, 1], 40000);
        let app = test_app(state.clone(), peer_a);

        // Exhaust the rate limit for peer A (LOGIN_FAIL_MAX failures).
        for i in 0..LOGIN_FAIL_MAX {
            let res = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/api/login")
                        .header(header::CONTENT_TYPE, "application/json")
                        // Spoofed XFF must NOT bypass peer-IP rate limiting.
                        .header("x-forwarded-for", format!("1.2.3.{i}"))
                        .header("x-real-ip", format!("9.9.9.{i}"))
                        .body(login_body("admin", "wrong-password"))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(
                res.status(),
                StatusCode::UNAUTHORIZED,
                "failure {i} should be 401"
            );
        }

        // N+1 without spoofable-header bypass → 429, still with unique XFF.
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/login")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header("x-forwarded-for", "203.0.113.99")
                    .body(login_body("admin", "wrong-password"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            res.status(),
            StatusCode::TOO_MANY_REQUESTS,
            "peer IP must be rate-limited regardless of XFF"
        );

        // Distinct peer B still allowed (not globally locked by "unknown").
        let app_b = test_app(state, peer([10, 0, 0, 2], 40001));
        let res = post_login(app_b, login_body("admin", "wrong-password")).await;
        assert_eq!(
            res.status(),
            StatusCode::UNAUTHORIZED,
            "other peer must not share rate-limit bucket"
        );
    }

    #[tokio::test]
    async fn list_queues_requires_auth_and_returns_declared() {
        let (_dir, state) = test_state().await;

        // Declare a queue via registry.
        state
            .queues
            .declare(
                DEFAULT_VHOST,
                "orders",
                QueueDeclareOpts {
                    durable: false,
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let app = test_app(state.clone(), peer([127, 0, 0, 1], 10003));

        // Unauthenticated → 401.
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/queues/%2F")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);

        // Login.
        let res = post_login(app.clone(), login_body("admin", "devpassword12")).await;
        assert_eq!(res.status(), StatusCode::OK);
        let cookie = cookie_from(&res).unwrap();

        // List queues for default vhost (%2F).
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/queues/%2F")
                    .header(header::COOKIE, &cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK, "list queues status");
        let json = body_json(res).await;
        let items = json["items"].as_array().expect("items array");
        assert!(
            items
                .iter()
                .any(|q| q["name"] == "orders" && q["vhost"] == "/"),
            "expected orders queue in {json}"
        );
        assert!(json["total_count"].as_u64().unwrap() >= 1);

        // Overview works.
        let res = app
            .oneshot(
                Request::builder()
                    .uri("/api/overview")
                    .header(header::COOKIE, &cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let json = body_json(res).await;
        assert_eq!(json["product_name"], "QueueForge");
        assert!(json["object_totals"]["queues"].as_u64().unwrap() >= 1);
        // Builtin exchanges on default vhost.
        assert!(json["object_totals"]["exchanges"].as_u64().unwrap() >= 4);
    }

    #[tokio::test]
    async fn list_exchanges_default_vhost() {
        let (_dir, state) = test_state().await;
        let app = test_app(state, peer([127, 0, 0, 1], 10004));

        let res = post_login(app.clone(), login_body("admin", "devpassword12")).await;
        let cookie = cookie_from(&res).unwrap();

        let res = app
            .oneshot(
                Request::builder()
                    .uri("/api/exchanges/%2F")
                    .header(header::COOKIE, &cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let json = body_json(res).await;
        let items = json["items"].as_array().unwrap();
        // default + amq.direct + amq.fanout + amq.topic
        assert!(items.len() >= 4, "exchanges: {json}");
        assert!(items.iter().any(|e| e["name"] == ""
            || e["name"] == serde_json::Value::String(String::new())
            || e["name"].as_str() == Some("")));
        assert!(items.iter().any(|e| e["name"] == "amq.topic"));
    }

    #[tokio::test]
    async fn logout_clears_session() {
        let (_dir, state) = test_state().await;
        let app = test_app(state, peer([127, 0, 0, 1], 10005));

        let res = post_login(app.clone(), login_body("admin", "devpassword12")).await;
        let cookie = cookie_from(&res).unwrap();

        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/logout")
                    .header(header::COOKIE, &cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NO_CONTENT);

        let res = app
            .oneshot(
                Request::builder()
                    .uri("/api/whoami")
                    .header(header::COOKIE, &cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    }

    #[test]
    fn decode_vhost_slash() {
        assert_eq!(decode_vhost("/").unwrap(), "/");
        assert_eq!(decode_vhost("%2F").unwrap(), "/");
        assert_eq!(decode_vhost("%2f").unwrap(), "/");
    }

    async fn auth_cookie(app: Router) -> String {
        let res = post_login(app, login_body("admin", "devpassword12")).await;
        assert_eq!(res.status(), StatusCode::OK);
        cookie_from(&res).expect("session cookie")
    }

    #[tokio::test]
    async fn crud_queue_exchange_binding_publish_get() {
        let (_dir, state) = test_state().await;
        let app = test_app(state, peer([127, 0, 0, 1], 20001));
        let cookie = auth_cookie(app.clone()).await;

        // Create exchange
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/api/exchanges/%2F/mgmt.ex")
                    .header(header::COOKIE, &cookie)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"type":"direct","durable":false}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::CREATED, "create exchange");

        // Create queue
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/api/queues/%2F/mgmt.q")
                    .header(header::COOKIE, &cookie)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"durable":false}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::CREATED, "create queue");

        // Bind
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/bindings/%2F")
                    .header(header::COOKIE, &cookie)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"source":"mgmt.ex","destination":"mgmt.q","routing_key":"rk1"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::CREATED, "bind");

        // Publish
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/exchanges/%2F/mgmt.ex/publish")
                    .header(header::COOKIE, &cookie)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"routing_key":"rk1","payload":"hello-mgmt","payload_encoding":"string"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK, "publish");
        let json = body_json(res).await;
        assert_eq!(json["routed"], true);

        // Get (consume)
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/queues/%2F/mgmt.q/get")
                    .header(header::COOKIE, &cookie)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"count":1,"ackmode":"ack_requeue_false","encoding":"auto"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK, "get");
        let json = body_json(res).await;
        let items = json.as_array().expect("array");
        assert_eq!(items.len(), 1);
        assert_eq!(items[0]["payload"], "hello-mgmt");

        // Definitions export includes exchange/queue
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/definitions")
                    .header(header::COOKIE, &cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let defs = body_json(res).await;
        assert!(defs["queues"]
            .as_array()
            .unwrap()
            .iter()
            .any(|q| q["name"] == "mgmt.q"));
        assert!(defs["exchanges"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["name"] == "mgmt.ex"));

        // Unbind + delete
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/bindings/%2F/mgmt.ex/mgmt.q/rk1")
                    .header(header::COOKIE, &cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NO_CONTENT);

        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/queues/%2F/mgmt.q")
                    .header(header::COOKIE, &cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let res = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/exchanges/%2F/mgmt.ex")
                    .header(header::COOKIE, &cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn users_and_permissions_admin_only() {
        let (_dir, state) = test_state().await;
        let app = test_app(state, peer([127, 0, 0, 1], 20002));
        let cookie = auth_cookie(app.clone()).await;

        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/api/users/alice")
                    .header(header::COOKIE, &cookie)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"password":"alicepassword1","tags":["management"]}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::CREATED);

        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/api/permissions/alice/%2F")
                    .header(header::COOKIE, &cookie)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"configure":".*","write":".*","read":".*"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NO_CONTENT);

        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/users")
                    .header(header::COOKIE, &cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let json = body_json(res).await;
        assert!(json
            .as_array()
            .unwrap()
            .iter()
            .any(|u| u["name"] == "alice"));

        let res = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/users/alice")
                    .header(header::COOKIE, &cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn mutation_requires_auth() {
        let (_dir, state) = test_state().await;
        let app = test_app(state, peer([127, 0, 0, 1], 20003));
        let res = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/api/queues/%2F/noauth")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn unknown_api_route_is_json_404_not_spa() {
        let (_dir, state) = test_state().await;
        let app = test_app(state, peer([127, 0, 0, 1], 10006));

        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/no-such-route")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
        let ct = res
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        assert!(
            ct.contains("application/json"),
            "expected JSON 404, got content-type {ct}"
        );
        let json = body_json(res).await;
        assert_eq!(json["error"], "not found");

        // POST also must not fall through to SPA shell.
        let res = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/nope")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
        let ct = res
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        assert!(ct.contains("application/json"), "POST ct={ct}");
    }

    #[tokio::test]
    async fn missing_static_asset_is_404_not_spa() {
        let (_dir, state) = test_state().await;
        let app = test_app(state, peer([127, 0, 0, 1], 10007));

        let res = app
            .oneshot(
                Request::builder()
                    .uri("/assets/missing.js")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
        let ct = res
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        assert!(
            !ct.contains("text/html"),
            "missing asset must not be SPA shell, ct={ct}"
        );
    }

    #[tokio::test]
    async fn spa_index_still_served_and_overview_still_json() {
        let (_dir, state) = test_state().await;
        let app = test_app(state, peer([127, 0, 0, 1], 10008));

        // SPA root.
        let res = app
            .clone()
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let ct = res
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        assert!(ct.contains("text/html"));
        assert_eq!(
            res.headers()
                .get(header::X_CONTENT_TYPE_OPTIONS)
                .and_then(|v| v.to_str().ok()),
            Some("nosniff")
        );

        // Registered API still JSON (unauthenticated → 401).
        let res = app
            .oneshot(
                Request::builder()
                    .uri("/api/overview")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
        let json = body_json(res).await;
        assert_eq!(json["error"], "unauthorized");
    }

    #[tokio::test]
    async fn force_close_connection_and_404() {
        let (_dir, state) = test_state().await;
        let (id, mut rx) = state
            .connections
            .register("127.0.0.1:9".parse().unwrap(), "admin", "/");
        let app = test_app(state.clone(), peer([127, 0, 0, 1], 20010));
        let cookie = auth_cookie(app.clone()).await;

        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(format!("/api/connections/{id}"))
                    .header(header::COOKIE, &cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NO_CONTENT);
        assert!(*rx.borrow_and_update());

        let res = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/connections/conn-does-not-exist")
                    .header(header::COOKIE, &cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
        state.connections.unregister(&id);
    }

    #[tokio::test]
    async fn put_queue_accepts_arguments_and_rejects_unknown() {
        let (_dir, state) = test_state().await;
        let app = test_app(state, peer([127, 0, 0, 1], 20011));
        let cookie = auth_cookie(app.clone()).await;

        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/api/queues/%2F/args.q")
                    .header(header::COOKIE, &cookie)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"durable":false,"arguments":{"x-message-ttl":5000,"x-max-length":10,"x-max-priority":5}}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::CREATED, "declare with args");

        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/api/queues/%2F/bad.args")
                    .header(header::COOKIE, &cookie)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"arguments":{"x-unknown":1}}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::CREATED, "unknown x- argument is ignored");

        let res = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/api/queues/%2F/bad.plain")
                    .header(header::COOKIE, &cookie)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"arguments":{"not-an-x-arg":1}}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::PRECONDITION_FAILED);
    }

    #[tokio::test]
    async fn delete_binding_uses_properties_key_for_header_args() {
        let (_dir, state) = test_state().await;
        let app = test_app(state, peer([127, 0, 0, 1], 20012));
        let cookie = auth_cookie(app.clone()).await;

        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/api/exchanges/%2F/hdr")
                    .header(header::COOKIE, &cookie)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"type":"headers","durable":false}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::CREATED);
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/api/queues/%2F/hq")
                    .header(header::COOKIE, &cookie)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"durable":false}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::CREATED);

        for (name, value) in [("color", "blue"), ("size", "l")] {
            let body = format!(
                r#"{{"source":"hdr","destination":"hq","routing_key":"","arguments":{{"{name}":"{value}"}}}}"#
            );
            let res = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/api/bindings/%2F")
                        .header(header::COOKIE, &cookie)
                        .header(header::CONTENT_TYPE, "application/json")
                        .body(Body::from(body))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(res.status(), StatusCode::CREATED, "{name}");
        }

        let color = queueforge_core::binding_properties_key(
            "",
            &[(
                compact_str::CompactString::from("color"),
                queueforge_core::HeaderArg::Str("blue".into()),
            )],
        );
        let mut encoded = String::new();
        for b in color.bytes() {
            if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
                encoded.push(b as char);
            } else {
                encoded.push_str(&format!("%{b:02X}"));
            }
        }
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(format!("/api/bindings/%2F/hdr/hq/{encoded}"))
                    .header(header::COOKIE, &cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NO_CONTENT);

        let res = app
            .oneshot(
                Request::builder()
                    .uri("/api/bindings/%2F")
                    .header(header::COOKIE, &cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let json = body_json(res).await;
        let left: Vec<_> = json
            .as_array()
            .unwrap()
            .iter()
            .filter(|item| item["source"] == "hdr")
            .collect();
        assert_eq!(left.len(), 1);
        let size = queueforge_core::binding_properties_key(
            "",
            &[(
                compact_str::CompactString::from("size"),
                queueforge_core::HeaderArg::Str("l".into()),
            )],
        );
        assert_eq!(left[0]["properties_key"], size);
    }
}
