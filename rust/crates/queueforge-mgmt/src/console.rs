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
    let raft = queueforge_core::flags::queue_raft(&vhost, &name);
    let mut body = json!({
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
    });
    if let Some(raft) = raft {
        body["leader"] = json!(raft.leader);
        body["members"] = json!(raft.members);
        body["raft_group"] = json!(raft.group);
    }
    Ok(Json(body))
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
    let (user, vhost) = crate::compat::perm_target(&state, &user, &vhost).await?;
    if body.exchange.is_empty()
        || regex::Regex::new(&body.write).is_err()
        || regex::Regex::new(&body.read).is_err()
    {
        return Err(MgmtError::BadRequest(
            "exchange and valid write/read patterns are required".into(),
        ));
    }
    let perm = TopicPermission {
        user,
        vhost,
        exchange: body.exchange,
        write: body.write,
        read: body.read,
    };
    let row = crate::settings::topic_row(&perm);
    let stored = perm.clone();
    with_store(&state, move |store| {
        crate::settings::store_topic(store, &stored.user, &stored.vhost, &stored.exchange, Some(&stored))
    })
    .await;
    state.connections.put_topic_permission(perm);
    crate::mutations::replicate(&state, "topic_permission", row).await;
    Ok(StatusCode::CREATED)
}

/// Run `f` on the metadata store off the async runtime.
async fn with_store<T: Send + 'static>(
    state: &MgmtState,
    f: impl FnOnce(&queueforge_store::MetadataStore) -> T + Send + 'static,
) -> Option<T> {
    queueforge_store::MetadataStore::blocking(std::sync::Arc::clone(&state.store), move |store| Ok(f(store)))
        .await
        .ok()
}

/// Store and replicate `user`'s limits after a change.
async fn user_limits_changed(state: &MgmtState, user: &str) {
    let conns = std::sync::Arc::clone(&state.connections);
    let name = user.to_string();
    with_store(state, move |store| crate::settings::store_user_limits(&conns, store, &name)).await;
    let row = crate::settings::user_limits_row(&state.connections, user);
    crate::mutations::replicate(state, "user_limits", row).await;
}

/// Store and replicate `vhost`'s limits after a change.
async fn vhost_limits_changed(state: &MgmtState, vhost: &str) {
    let conns = std::sync::Arc::clone(&state.connections);
    let name = vhost.to_string();
    with_store(state, move |store| crate::settings::store_vhost_limits(&conns, store, &name)).await;
    let row = crate::settings::vhost_limits_row(&state.connections, vhost);
    crate::mutations::replicate(state, "vhost_limits", row).await;
}

/// DELETE /api/topic-permissions/{user}/{vhost}/{exchange}
pub async fn delete_topic_permission(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Path((user, vhost, exchange)): Path<(String, String, String)>,
) -> Result<StatusCode> {
    let session = require_session(&state, &headers).await?;
    require_administrator(&session)?;
    let (user, vhost) = crate::compat::perm_target(&state, &user, &vhost).await?;
    if !state
        .connections
        .delete_topic_permission(&user, &vhost, &exchange)
    {
        return Err(MgmtError::NotFound("topic permission".into()));
    }
    let (u, v, e) = (user.clone(), vhost.clone(), exchange.clone());
    with_store(&state, move |store| crate::settings::store_topic(store, &u, &v, &e, None)).await;
    crate::mutations::replicate(
        &state,
        "delete_topic_permission",
        json!({"user": user, "vhost": vhost, "exchange": exchange}),
    )
    .await;
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
    user_limits_changed(&state, &user).await;
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
    user_limits_changed(&state, &user).await;
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
    vhost_limits_changed(&state, &vhost).await;
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
    vhost_limits_changed(&state, &vhost).await;
    Ok(StatusCode::NO_CONTENT)
}

/// RabbitMQ 4.3 flags whose behaviour this broker has, with RabbitMQ's text.
const REQUIRED_FLAGS: &[(&str, &str)] = &[
    ("quorum_queue", "Support queues of type `quorum`"),
    ("stream_queue", "Support queues of type `stream`"),
    ("implicit_default_bindings", "Default bindings are now implicit, instead of being stored in the database"),
    ("user_limits", "Configure connection and channel limits for a user"),
];

fn flag_row(name: &str, state: &str, stability: &str, desc: &str, provider: &str) -> Value {
    json!({
        "name": name,
        "desc": desc,
        "doc_url": "",
        "state": state,
        "stability": stability,
        "require_level": if stability == "required" { "hard" } else { "none" },
        "experiment_level": "supported",
        "callbacks": [],
        "provided_by": provider,
    })
}

/// GET /api/feature-flags
pub async fn list_feature_flags(
    State(state): State<MgmtState>,
    headers: HeaderMap,
) -> Result<Json<Value>> {
    let _session = require_session(&state, &headers).await?;
    let transient = state.connections.transient_nonexcl_permitted();
    let mut items: Vec<Value> = REQUIRED_FLAGS
        .iter()
        .map(|(name, desc)| flag_row(name, "enabled", "required", desc, "rabbit"))
        .collect();
    // Raft is a feature flag as RabbitMQ's khepri_db: on by itself in a new
    // cluster, enabled by an operator after an upgrade (docs/raft.md, section 8).
    match queueforge_core::flags::raft() {
        queueforge_core::flags::RaftFlag::Unsupported => {}
        flag => items.push(flag_row(
            "raft",
            if flag == queueforge_core::flags::RaftFlag::Enabled { "enabled" } else { "disabled" },
            "stable",
            "Raft consensus for metadata and quorum queues",
            "queueforge",
        )),
    }
    items.push(flag_row(
        "transient_nonexcl_queues",
        if transient { "enabled" } else { "disabled" },
        "experimental",
        "Allow transient non-exclusive queues (a deprecated feature)",
        "queueforge",
    ));
    items.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
    Ok(Json(json!({ "items": items })))
}

/// PUT or POST /api/feature-flags/{name}/enable
pub async fn enable_feature_flag(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Result<StatusCode> {
    let session = require_session(&state, &headers).await?;
    require_administrator(&session)?;
    match name.as_str() {
        // quorum_queues is the name QueueForge listed before it used RabbitMQ's.
        "quorum_queues" => Ok(StatusCode::NO_CONTENT),
        name if REQUIRED_FLAGS.iter().any(|(flag, _)| *flag == name) => Ok(StatusCode::NO_CONTENT),
        "transient_nonexcl_queues" => {
            state.connections.set_transient_nonexcl(true);
            Ok(StatusCode::NO_CONTENT)
        }
        "raft" if queueforge_core::flags::enable_raft() => Ok(StatusCode::NO_CONTENT),
        _ => Err(MgmtError::BadRequest("unsupported".into())),
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
        // The deprecated-feature toggle, kept here as QueueForge always had it.
        "transient_nonexcl_queues" => {
            state.connections.set_transient_nonexcl(false);
            Ok(StatusCode::NO_CONTENT)
        }
        // A RabbitMQ feature flag cannot be turned off once it is enabled.
        _ => Err(MgmtError::BadRequest("a feature flag cannot be disabled".into())),
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
            "mem_alarm": state.connections.alarm().as_deref() == Some("low on memory"),
            "disk_free_alarm": state.connections.alarm().as_deref() == Some("low on disk"),
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
    // In a cluster the change commits through the meta log first, so it is
    // refused (503) without a majority, as RabbitMQ refuses one.
    if let Some(tx) = &state.replicate_tx {
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        let _ = tx.send(crate::state::ReplicateReq {
            kind: "members".into(),
            payload: Value::Array(members.clone()),
            done: done_tx,
        });
        if let Ok(Err(why)) = done_rx.await {
            return Err(MgmtError::Unavailable(why));
        }
        return Ok(Json(json!({ "members": members })));
    }
    let path = std::path::Path::new(&state.config.data_dir).join("members.json");
    std::fs::write(
        &path,
        serde_json::to_string(&members).unwrap_or_else(|_| "[]".into()),
    )
    .map_err(|err| MgmtError::Internal(err.to_string()))?;
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

/// GET /api/parameters, /api/parameters/{component}, /api/parameters/{component}/{vhost}
pub async fn list_parameters(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    path: Option<Path<Vec<String>>>,
) -> Result<Json<Value>> {
    let _session = require_session(&state, &headers).await?;
    let parts = path.map(|Path(p)| p).unwrap_or_default();
    let component = parts.first().cloned();
    let vhost = match parts.get(1) {
        Some(v) => Some(decode_vhost(v)?),
        None => None,
    };
    let conns = std::sync::Arc::clone(&state.connections);
    let rows = with_store(&state, move |store| {
        crate::settings::list_parameters(&conns, store, component.as_deref(), vhost.as_deref())
    })
    .await
    .unwrap_or_default();
    Ok(Json(Value::Array(rows)))
}

/// GET /api/parameters/{component}/{vhost}/{name}
pub async fn get_parameter(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Path((component, vhost, name)): Path<(String, String, String)>,
) -> Result<Json<Value>> {
    let _session = require_session(&state, &headers).await?;
    let vhost = decode_vhost(&vhost)?;
    let conns = std::sync::Arc::clone(&state.connections);
    let (c, v) = (component.clone(), vhost.clone());
    let rows = with_store(&state, move |store| crate::settings::list_parameters(&conns, store, Some(&c), Some(&v)))
        .await
        .unwrap_or_default();
    rows.into_iter()
        .find(|r| r["name"] == name.as_str())
        .map(Json)
        .ok_or_else(|| MgmtError::NotFound(format!("parameter {component}/{vhost}/{name}")))
}

#[derive(Deserialize)]
pub struct ParameterBody {
    #[serde(default)]
    value: Value,
}

/// PUT /api/parameters/{component}/{vhost}/{name}
pub async fn put_parameter(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Path((component, vhost, name)): Path<(String, String, String)>,
    Json(body): Json<ParameterBody>,
) -> Result<StatusCode> {
    let session = require_session(&state, &headers).await?;
    require_administrator(&session)?;
    let vhost = decode_vhost(&vhost)?;
    let conns = std::sync::Arc::clone(&state.connections);
    let (c, v, n, value) = (component.clone(), vhost.clone(), name.clone(), body.value.clone());
    with_store(&state, move |store| crate::settings::put_parameter(&conns, store, &c, &v, &n, &value)).await;
    crate::mutations::replicate(
        &state,
        "parameter",
        json!({"component": component, "vhost": vhost, "name": name, "value": body.value}),
    )
    .await;
    Ok(StatusCode::CREATED)
}

/// DELETE /api/parameters/{component}/{vhost}/{name}
pub async fn delete_parameter(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Path((component, vhost, name)): Path<(String, String, String)>,
) -> Result<StatusCode> {
    let session = require_session(&state, &headers).await?;
    require_administrator(&session)?;
    let vhost = decode_vhost(&vhost)?;
    let conns = std::sync::Arc::clone(&state.connections);
    let (c, v, n) = (component.clone(), vhost.clone(), name.clone());
    let had = with_store(&state, move |store| crate::settings::delete_parameter(&conns, store, &c, &v, &n))
        .await
        .unwrap_or(false);
    if !had {
        return Err(MgmtError::NotFound(format!("parameter {component}/{vhost}/{name}")));
    }
    crate::mutations::replicate(&state, "delete_parameter", json!({"component": component, "vhost": vhost, "name": name}))
        .await;
    Ok(StatusCode::NO_CONTENT)
}

/// GET /api/global-parameters
pub async fn list_global_parameters(State(state): State<MgmtState>, headers: HeaderMap) -> Result<Json<Value>> {
    let _session = require_session(&state, &headers).await?;
    let node = state.config.node_name.clone();
    let rows = with_store(&state, move |store| crate::settings::list_globals(store, &node)).await.unwrap_or_default();
    Ok(Json(Value::Array(rows)))
}

/// GET /api/global-parameters/{name}
pub async fn get_global_parameter(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Result<Json<Value>> {
    let _session = require_session(&state, &headers).await?;
    let node = state.config.node_name.clone();
    let rows = with_store(&state, move |store| crate::settings::list_globals(store, &node)).await.unwrap_or_default();
    rows.into_iter()
        .find(|r| r["name"] == name.as_str())
        .map(Json)
        .ok_or_else(|| MgmtError::NotFound(format!("global parameter {name}")))
}

/// PUT /api/global-parameters/{name}
pub async fn put_global_parameter(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Path(name): Path<String>,
    Json(body): Json<ParameterBody>,
) -> Result<StatusCode> {
    let session = require_session(&state, &headers).await?;
    require_administrator(&session)?;
    let (n, value) = (name.clone(), body.value.clone());
    with_store(&state, move |store| crate::settings::put_global(store, &n, &value)).await;
    crate::mutations::replicate(&state, "global_parameter", json!({"name": name, "value": body.value})).await;
    Ok(StatusCode::CREATED)
}

/// DELETE /api/global-parameters/{name}
pub async fn delete_global_parameter(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Result<StatusCode> {
    let session = require_session(&state, &headers).await?;
    require_administrator(&session)?;
    let n = name.clone();
    if !with_store(&state, move |store| crate::settings::delete_global(store, &n)).await.unwrap_or(false) {
        return Err(MgmtError::NotFound(format!("global parameter {name}")));
    }
    crate::mutations::replicate(&state, "delete_global_parameter", json!({"name": name})).await;
    Ok(StatusCode::NO_CONTENT)
}
