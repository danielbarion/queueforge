//! Binding list, create, and delete.

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use compact_str::CompactString;
use queueforge_core::{binding_args_key, binding_properties_key, Binding, HeaderArg, QueueKey};
use serde::{Deserialize, Serialize};

use crate::authz::{check_bind, require_management_tag, require_session};
use crate::error::MgmtError;
use crate::pagination::Page;
use crate::routes::decode_vhost;
use crate::state::MgmtState;

use super::{db, ensure_vhost};

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
