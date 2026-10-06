//! Channels, consumers, limits, topic permissions, flags, and cluster node.

use std::sync::OnceLock;
use std::time::Instant;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use queueforge_core::prom::traffic_totals;
use queueforge_core::QueueKey;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::authz::{require_administrator, require_session};
use crate::connections::TopicPermission;
use crate::error::{MgmtError, Result};
use crate::routes::{decode_vhost, query_stats};
use crate::state::MgmtState;

fn started() -> Instant {
    static START: OnceLock<Instant> = OnceLock::new();
    *START.get_or_init(Instant::now)
}

/// GET /api/channels
pub async fn list_channels(
    State(state): State<MgmtState>,
    headers: HeaderMap,
) -> Result<Json<Value>> {
    let _session = require_session(&state, &headers).await?;
    let items: Vec<_> = state
        .connections
        .list_channels()
        .into_iter()
        .map(|c| {
            json!({
                "name": c.name,
                "connection_details": { "name": c.connection },
                "user": c.user,
                "vhost": c.vhost,
                "number": c.number,
                "peer_host": c.peer_host,
                "peer_port": c.peer_port,
            })
        })
        .collect();
    Ok(Json(json!({ "items": items, "total_count": items.len() })))
}

/// GET /api/channels/{name}
pub async fn get_channel(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Result<Json<Value>> {
    let _session = require_session(&state, &headers).await?;
    let channel = state
        .connections
        .get_channel(&name)
        .ok_or_else(|| MgmtError::NotFound(format!("channel '{name}'")))?;
    Ok(Json(json!({
        "name": channel.name,
        "connection_details": { "name": channel.connection },
        "user": channel.user,
        "vhost": channel.vhost,
        "number": channel.number,
        "peer_host": channel.peer_host,
        "peer_port": channel.peer_port,
    })))
}

/// GET /api/consumers/{vhost}
pub async fn list_consumers(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Path(vhost): Path<String>,
) -> Result<Json<Value>> {
    let _session = require_session(&state, &headers).await?;
    let vhost = decode_vhost(&vhost)?;
    Ok(Json(consumer_page(&state, Some(&vhost), None)))
}

/// GET /api/queues/{vhost}/{name}
pub async fn get_queue(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Path((vhost, name)): Path<(String, String)>,
) -> Result<Json<Value>> {
    let _session = require_session(&state, &headers).await?;
    let vhost = decode_vhost(&vhost)?;
    let key = QueueKey::new(vhost.as_str(), name.as_str());
    let Some(handle) = state.queues.get(&key) else {
        return Err(MgmtError::NotFound(format!("queue '{name}'")));
    };
    let stats = query_stats(&handle.tx).await.unwrap_or_default();
    let queue_type = handle
        .info
        .args
        .lock()
        .unwrap_or_else(|err| err.into_inner())
        .queue_type
        .map(|kind| kind.as_str())
        .unwrap_or("classic");
    let (policy, operator_policy) = state.router.matching_policy_names(&vhost, &name);
    let traffic = traffic_totals();
    let consumers = state.connections.list_consumers(Some(&vhost), Some(&name));
    Ok(Json(json!({
        "name": name,
        "vhost": vhost,
        "durable": handle.info.durable,
        "state": "running",
        "messages": (stats.messages_ready as u64) + (stats.messages_unacked as u64),
        "messages_ready": stats.messages_ready,
        "messages_unacknowledged": stats.messages_unacked,
        "consumers": stats.consumer_count,
        "consumer_details": consumers.iter().map(|c| json!({
            "consumer_tag": c.consumer_tag,
            "channel_details": { "name": format!("{}:{}", c.connection, c.channel), "connection_name": c.connection, "number": c.channel },
            "queue": { "name": c.queue, "vhost": c.vhost },
            "ack_required": true,
        })).collect::<Vec<_>>(),
        "type": queue_type,
        "policy": policy,
        "operator_policy": operator_policy,
        "message_stats": { "publish": traffic.publish, "deliver": traffic.deliver, "ack": traffic.ack },
    })))
}

/// GET /api/connections/{name}
pub async fn get_connection(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Result<Json<Value>> {
    let _session = require_session(&state, &headers).await?;
    let conn = state
        .connections
        .list()
        .into_iter()
        .find(|c| c.id == name)
        .ok_or_else(|| MgmtError::NotFound(format!("connection '{name}'")))?;
    Ok(Json(json!({
        "name": conn.id,
        "user": conn.user,
        "vhost": conn.vhost,
        "peer_host": conn.peer.ip().to_string(),
        "peer_port": conn.peer.port(),
        "channels": conn.channels,
        "connected_at": conn.connected_at,
    })))
}

fn consumer_page(state: &MgmtState, vhost: Option<&str>, queue: Option<&str>) -> Value {
    let items: Vec<_> = state.connections.list_consumers(vhost, queue).into_iter().map(|c| {
        json!({
            "consumer_tag": c.consumer_tag,
            "channel_details": { "name": format!("{}:{}", c.connection, c.channel), "connection_name": c.connection, "number": c.channel },
            "queue": { "name": c.queue, "vhost": c.vhost },
        })
    }).collect();
    json!({ "items": items, "total_count": items.len() })
}

#[derive(Deserialize)]
pub struct TopicBody {
    exchange: String,
    write: String,
    read: String,
}

/// GET /api/topic-permissions
pub async fn list_topic_permissions(
    State(state): State<MgmtState>,
    headers: HeaderMap,
) -> Result<Json<Value>> {
    let _session = require_session(&state, &headers).await?;
    let items: Vec<_> = state.connections.list_topic_permissions(None).into_iter().map(|p| {
        json!({ "user": p.user, "vhost": p.vhost, "exchange": p.exchange, "write": p.write, "read": p.read })
    }).collect();
    Ok(Json(json!({ "items": items })))
}

/// PUT /api/topic-permissions/{user}/{vhost}
pub async fn put_topic_permission(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Path((user, vhost)): Path<(String, String)>,
    Json(body): Json<TopicBody>,
) -> Result<StatusCode> {
    let session = require_session(&state, &headers).await?;
    require_administrator(&session)?;
    let vhost = decode_vhost(&vhost)?;
    if body.exchange.is_empty()
        || regex::Regex::new(&body.write).is_err()
        || regex::Regex::new(&body.read).is_err()
    {
        return Err(MgmtError::BadRequest(
            "exchange and valid write/read patterns are required".into(),
        ));
    }
    state.connections.put_topic_permission(TopicPermission {
        user,
        vhost,
        exchange: body.exchange,
        write: body.write,
        read: body.read,
    });
    Ok(StatusCode::CREATED)
}

/// DELETE /api/topic-permissions/{user}/{vhost}/{exchange}
pub async fn delete_topic_permission(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Path((user, vhost, exchange)): Path<(String, String, String)>,
) -> Result<StatusCode> {
    let session = require_session(&state, &headers).await?;
    require_administrator(&session)?;
    let vhost = decode_vhost(&vhost)?;
    if !state
        .connections
        .delete_topic_permission(&user, &vhost, &exchange)
    {
        return Err(MgmtError::NotFound("topic permission".into()));
    }
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
pub struct LimitBody {
    value: u32,
}

/// GET /api/user-limits and vhost limits together for the Limits page.
pub async fn list_limits(
    State(state): State<MgmtState>,
    headers: HeaderMap,
) -> Result<Json<Value>> {
    let _session = require_session(&state, &headers).await?;
    let users: Vec<_> = state.connections.list_user_limits().into_iter().map(|(user, connections, channels)| {
        json!({ "user": user, "max-connections": connections, "max-channels": channels })
    }).collect();
    let vhosts: Vec<_> = state.connections.list_vhost_limits().into_iter().map(|(vhost, connections, queues)| {
        json!({ "vhost": vhost, "max-connections": connections, "max-queues": queues })
    }).collect();
    Ok(Json(
        json!({ "user_limits": users, "vhost_limits": vhosts }),
    ))
}

/// PUT /api/user-limits/{user}/{kind}
pub async fn put_user_limit(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Path((user, kind)): Path<(String, String)>,
    Json(body): Json<LimitBody>,
) -> Result<StatusCode> {
    let session = require_session(&state, &headers).await?;
    require_administrator(&session)?;
    let current = state
        .connections
        .list_user_limits()
        .into_iter()
        .find(|row| row.0 == user);
    let (connections, channels) = current.map(|row| (row.1, row.2)).unwrap_or((None, None));
    match kind.as_str() {
        "max-connections" => state
            .connections
            .set_user_limit(&user, Some(body.value), channels),
        "max-channels" => state
            .connections
            .set_user_limit(&user, connections, Some(body.value)),
        _ => {
            return Err(MgmtError::BadRequest(
                "limit must be max-connections or max-channels".into(),
            ))
        }
    }
    Ok(StatusCode::NO_CONTENT)
}

/// DELETE /api/user-limits/{user}/{kind}
pub async fn delete_user_limit(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Path((user, kind)): Path<(String, String)>,
) -> Result<StatusCode> {
    let session = require_session(&state, &headers).await?;
    require_administrator(&session)?;
    let current = state
        .connections
        .list_user_limits()
        .into_iter()
        .find(|row| row.0 == user);
    let (connections, channels) = current.map(|row| (row.1, row.2)).unwrap_or((None, None));
    match kind.as_str() {
        "max-connections" => state.connections.set_user_limit(&user, None, channels),
        "max-channels" => state.connections.set_user_limit(&user, connections, None),
        _ => {
            return Err(MgmtError::BadRequest(
                "limit must be max-connections or max-channels".into(),
            ))
        }
    }
    Ok(StatusCode::NO_CONTENT)
}

/// PUT /api/vhost-limits/{vhost}/{kind}
pub async fn put_vhost_limit(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Path((vhost, kind)): Path<(String, String)>,
    Json(body): Json<LimitBody>,
) -> Result<StatusCode> {
    let session = require_session(&state, &headers).await?;
    require_administrator(&session)?;
    let vhost = decode_vhost(&vhost)?;
    let current = state
        .connections
        .list_vhost_limits()
        .into_iter()
        .find(|row| row.0 == vhost);
    let (connections, queues) = current.map(|row| (row.1, row.2)).unwrap_or((None, None));
    match kind.as_str() {
        "max-connections" => state
            .connections
            .set_vhost_limit(&vhost, Some(body.value), queues),
        "max-queues" => state
            .connections
            .set_vhost_limit(&vhost, connections, Some(body.value)),
        _ => {
            return Err(MgmtError::BadRequest(
                "limit must be max-connections or max-queues".into(),
            ))
        }
    }
    Ok(StatusCode::NO_CONTENT)
}

/// DELETE /api/vhost-limits/{vhost}/{kind}
pub async fn delete_vhost_limit(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Path((vhost, kind)): Path<(String, String)>,
) -> Result<StatusCode> {
    let session = require_session(&state, &headers).await?;
    require_administrator(&session)?;
    let vhost = decode_vhost(&vhost)?;
    let current = state
        .connections
        .list_vhost_limits()
        .into_iter()
        .find(|row| row.0 == vhost);
    let (connections, queues) = current.map(|row| (row.1, row.2)).unwrap_or((None, None));
    match kind.as_str() {
        "max-connections" => state.connections.set_vhost_limit(&vhost, None, queues),
        "max-queues" => state.connections.set_vhost_limit(&vhost, connections, None),
        _ => {
            return Err(MgmtError::BadRequest(
                "limit must be max-connections or max-queues".into(),
            ))
        }
    }
    Ok(StatusCode::NO_CONTENT)
}

/// GET /api/feature-flags
pub async fn list_feature_flags(
    State(state): State<MgmtState>,
    headers: HeaderMap,
) -> Result<Json<Value>> {
    let _session = require_session(&state, &headers).await?;
    let transient = state.connections.transient_nonexcl_permitted();
    Ok(Json(json!({
        "items": [
            { "name": "quorum_queues", "state": "enabled", "stability": "stable" },
            { "name": "transient_nonexcl_queues", "state": if transient { "enabled" } else { "disabled" }, "stability": "experimental" }
        ]
    })))
}

/// POST /api/feature-flags/{name}/enable
pub async fn enable_feature_flag(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Result<StatusCode> {
    let session = require_session(&state, &headers).await?;
    require_administrator(&session)?;
    match name.as_str() {
        "quorum_queues" => Ok(StatusCode::NO_CONTENT),
        "transient_nonexcl_queues" => {
            state.connections.set_transient_nonexcl(true);
            Ok(StatusCode::NO_CONTENT)
        }
        _ => Err(MgmtError::NotFound(format!("feature flag '{name}'"))),
    }
}

/// POST /api/feature-flags/{name}/disable
pub async fn disable_feature_flag(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Result<StatusCode> {
    let session = require_session(&state, &headers).await?;
    require_administrator(&session)?;
    match name.as_str() {
        "quorum_queues" => Err(MgmtError::BadRequest("quorum queues stay available".into())),
        "transient_nonexcl_queues" => {
            state.connections.set_transient_nonexcl(false);
            Ok(StatusCode::NO_CONTENT)
        }
        _ => Err(MgmtError::NotFound(format!("feature flag '{name}'"))),
    }
}

/// GET /api/deprecated-features
pub async fn list_deprecated(
    State(state): State<MgmtState>,
    headers: HeaderMap,
) -> Result<Json<Value>> {
    let _session = require_session(&state, &headers).await?;
    let acknowledged = state.connections.transient_nonexcl_permitted();
    Ok(Json(json!({
        "items": [{
            "name": "transient_nonexcl_queues",
            "deprecation_phase": "denied_by_default",
            "acknowledged": acknowledged,
            "description": "Non-exclusive transient queues are refused unless this deprecated behavior is acknowledged."
        }]
    })))
}

/// DELETE /api/deprecated-features/{name} acknowledges the deprecated behavior.
pub async fn acknowledge_deprecated(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Result<StatusCode> {
    let session = require_session(&state, &headers).await?;
    require_administrator(&session)?;
    if name != "transient_nonexcl_queues" {
        return Err(MgmtError::NotFound(format!("deprecated feature '{name}'")));
    }
    state.connections.set_transient_nonexcl(true);
    Ok(StatusCode::NO_CONTENT)
}

/// GET /api/nodes
pub async fn list_nodes(State(state): State<MgmtState>, headers: HeaderMap) -> Result<Json<Value>> {
    let _session = require_session(&state, &headers).await?;
    let mem = memory_bytes();
    let disk = disk_free_bytes(&state.config.data_dir);
    let listeners: Vec<_> = state
        .config
        .amqp_listeners
        .iter()
        .map(|(host, port)| json!({ "protocol": "amqp", "ip_address": host, "port": port }))
        .collect();
    let peers: Vec<_> = read_member_ids(&state.config.data_dir)
        .unwrap_or_else(|| state.config.peers.clone())
        .into_iter()
        .map(|id| json!({ "name": id }))
        .collect();
    Ok(Json(json!({
        "items": [{
            "name": state.config.node_name,
            "running": true,
            "uptime": started().elapsed().as_millis() as u64,
            "mem_used": mem,
            "disk_free": disk,
            "mem_alarm": false,
            "disk_free_alarm": disk > 0 && disk < 50 * 1024 * 1024,
            "listeners": listeners,
            "peers": peers,
        }]
    })))
}

/// POST /api/nodes — add `{id, addr}` to the running member list.
pub async fn put_node(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Json<Value>> {
    let session = require_session(&state, &headers).await?;
    require_administrator(&session)?;
    let id = body
        .get("id")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    let addr = body
        .get("addr")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    if id.is_empty() || addr.parse::<std::net::SocketAddr>().is_err() {
        return Err(MgmtError::BadRequest("id and addr are required".into()));
    }
    let mut members = read_member_rows(&state.config.data_dir)
        .ok_or_else(|| MgmtError::BadRequest("cluster membership is not configured".into()))?;
    if let Some(row) = members
        .iter_mut()
        .find(|row| row.get("id").and_then(|v| v.as_str()) == Some(id.as_str()))
    {
        row["addr"] = json!(addr);
    } else {
        members.push(json!({"id": id, "addr": addr}));
    }
    write_members(&state, members).await
}

/// DELETE /api/nodes/{name} — drop a member that is not the home of a stored queue.
pub async fn delete_node(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Result<StatusCode> {
    let session = require_session(&state, &headers).await?;
    require_administrator(&session)?;
    if name == state.config.node_name {
        return Err(MgmtError::BadRequest("a node cannot forget itself".into()));
    }
    for vhost in state
        .store
        .list_vhosts()
        .map_err(|err| MgmtError::Internal(err.to_string()))?
    {
        for queue in state
            .store
            .list_queues(vhost.name.as_str())
            .map_err(|err| MgmtError::Internal(err.to_string()))?
        {
            if queue.home.as_deref() == Some(name.as_str()) {
                return Err(MgmtError::BadRequest(format!(
                    "member {name} still homes a classic queue"
                )));
            }
        }
    }
    let Some(mut members) = read_member_rows(&state.config.data_dir) else {
        return Err(MgmtError::BadRequest(
            "cluster membership is not configured".into(),
        ));
    };
    members.retain(|row| row.get("id").and_then(|v| v.as_str()) != Some(name.as_str()));
    if members.is_empty() {
        return Err(MgmtError::BadRequest(
            "the member list cannot become empty".into(),
        ));
    }
    write_members(&state, members).await?;
    Ok(StatusCode::NO_CONTENT)
}

fn read_member_ids(dir: &str) -> Option<Vec<String>> {
    Some(
        read_member_rows(dir)?
            .iter()
            .filter_map(|row| {
                row.get("id")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            })
            .collect(),
    )
}

fn read_member_rows(dir: &str) -> Option<Vec<Value>> {
    let text = std::fs::read_to_string(std::path::Path::new(dir).join("members.json")).ok()?;
    serde_json::from_str(&text).ok()
}

async fn write_members(state: &MgmtState, members: Vec<Value>) -> Result<Json<Value>> {
    let path = std::path::Path::new(&state.config.data_dir).join("members.json");
    std::fs::write(
        &path,
        serde_json::to_string(&members).unwrap_or_else(|_| "[]".into()),
    )
    .map_err(|err| MgmtError::Internal(err.to_string()))?;
    if let Some(tx) = &state.replicate_tx {
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        let _ = tx.send(crate::state::ReplicateReq {
            kind: "members".into(),
            payload: Value::Array(members.clone()),
            done: done_tx,
        });
        let _ = done_rx.await;
    }
    Ok(Json(json!({ "members": members })))
}

/// GET /api/cluster-name
pub async fn cluster_name(
    State(state): State<MgmtState>,
    headers: HeaderMap,
) -> Result<Json<Value>> {
    let _session = require_session(&state, &headers).await?;
    Ok(Json(json!({ "name": state.config.node_name })))
}

fn memory_bytes() -> u64 {
    let text = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kb: u64 = rest
                .split_whitespace()
                .next()
                .and_then(|n| n.parse().ok())
                .unwrap_or(0);
            return kb.saturating_mul(1024);
        }
    }
    0
}

fn disk_free_bytes(path: &str) -> u64 {
    if path.is_empty() {
        return 0;
    }
    let mut buf = [0u8; 256];
    let c_path =
        std::ffi::CString::new(path).unwrap_or_else(|_| std::ffi::CString::new(".").unwrap());
    extern "C" {
        fn statvfs(path: *const i8, buf: *mut u8) -> i32;
    }
    let rc = unsafe { statvfs(c_path.as_ptr() as *const i8, buf.as_mut_ptr()) };
    if rc != 0 {
        return 0;
    }
    let frsize = u64::from_ne_bytes(buf[8..16].try_into().unwrap_or([0; 8]));
    let bavail = u64::from_ne_bytes(buf[32..40].try_into().unwrap_or([0; 8]));
    frsize.saturating_mul(bavail)
}
