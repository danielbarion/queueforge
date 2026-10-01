//! Read-only vhost, queue, exchange, and connection listings.

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use queueforge_core::{QueueCmd, QueueKey, QueueStats};
use tokio::sync::oneshot;
use tracing::debug;

use super::codec::decode_vhost;
use super::db;
use super::{ConnectionItem, ExchangeItem, QueueItem, VhostItem};
use crate::authz::{require_management_tag, require_session};
use crate::error::MgmtError;
use crate::pagination::{paginate_by_name, ListQuery, Page};
use crate::state::MgmtState;

/// List vhosts visible to the session in `headers`. Returns a page of vhost rows. A user without the management tag gets 403.
pub(super) async fn list_vhosts(
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

/// List queues on the vhost in `path`. `query` is the page. Returns queue rows including live stats. `decode_vhost` must accept the path segment.
pub(super) async fn list_queues(
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
                .map(|a| {
                    a.queue_type
                        .unwrap_or(queueforge_core::QueueType::Classic)
                        .as_str()
                        .to_string()
                })
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

/// Map stored exchange `ex` to the JSON row the exchanges page renders. Returns that row. Builtin exchanges stay in the list.
pub(super) fn exchange_list_item(ex: queueforge_core::Exchange) -> ExchangeItem {
    ExchangeItem {
        name: ex.name.to_string(),
        vhost: ex.vhost.to_string(),
        kind: ex.kind.as_str().to_string(),
        durable: ex.durable,
        auto_delete: ex.auto_delete,
        internal: ex.internal,
    }
}

/// Read one exchange. `vhost` and `name` come from the path. Returns the row, or 404 when the store has no such exchange.
pub(super) async fn get_exchange(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Path((vhost, name)): Path<(String, String)>,
) -> Result<Json<serde_json::Value>, MgmtError> {
    let _session = require_session(&state, &headers).await?;
    let vhost = decode_vhost(&vhost)?;
    let found = state
        .router
        .list_exchanges(&vhost)
        .into_iter()
        .find(|ex| ex.name.as_str() == name);
    let Some(ex) = found else {
        return Err(MgmtError::NotFound(format!("exchange '{name}'")));
    };
    let (policy, operator_policy) = state.router.matching_exchange_policy_names(&vhost, &name);
    Ok(Json(serde_json::json!({
        "name": ex.name.as_str(),
        "vhost": vhost,
        "type": ex.kind.as_str(),
        "durable": ex.durable,
        "auto_delete": ex.auto_delete,
        "internal": ex.internal,
        "policy": policy,
        "operator_policy": operator_policy,
    })))
}

/// List exchanges on the path vhost. Returns a page. The default exchange is included.
pub(super) async fn list_exchanges(
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

    let mut items: Vec<ExchangeItem> = state
        .router
        .list_exchanges(&vhost)
        .into_iter()
        .map(exchange_list_item)
        .collect();
    let vhost_ex = vhost.clone();
    for ex in db(&state, move |s| s.list_exchanges(&vhost_ex)).await? {
        if !items.iter().any(|item| item.name == ex.name.as_str()) {
            items.push(exchange_list_item(ex));
        }
    }
    items.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(Json(paginate_by_name(items, &query, |e| e.name.as_str())))
}

/// List AMQP connections tracked by `state`. Returns the rows the connections page shows. Closed connections are already gone from the tracker.
pub(super) async fn list_connections(
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
pub(super) async fn delete_connection(
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

/// Ask the queue actor on `tx` for stats. Returns `None` when the actor is gone. Callers show an empty queue instead of failing the list.
pub(crate) async fn query_stats(tx: &tokio::sync::mpsc::Sender<QueueCmd>) -> Option<QueueStats> {
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
