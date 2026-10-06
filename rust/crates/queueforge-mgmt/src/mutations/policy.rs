//! User policies. Operator policies live in the operator module.

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use compact_str::CompactString;
use queueforge_core::{Policy, PolicyTarget};
use serde::Deserialize;

use crate::authz::{require_administrator, require_management_tag, require_session};
use crate::error::MgmtError;
use crate::routes::decode_vhost;
use crate::state::MgmtState;

use super::{db, ensure_vhost, replicate};

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

fn policy_u64(
    map: &serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Result<Option<u64>, MgmtError> {
    match map.get(key) {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::Number(n)) => {
            let v = n.as_u64().ok_or_else(|| {
                MgmtError::BadRequest(format!("{key} must be a positive integer"))
            })?;
            if v == 0 {
                return Err(MgmtError::BadRequest(format!(
                    "{key} must be a positive integer"
                )));
            }
            Ok(Some(v))
        }
        Some(serde_json::Value::String(s)) => {
            let v = s.parse::<u64>().ok().filter(|n| *n > 0).ok_or_else(|| {
                MgmtError::BadRequest(format!("{key} must be a positive integer"))
            })?;
            Ok(Some(v))
        }
        _ => Err(MgmtError::BadRequest(format!(
            "{key} must be a positive integer"
        ))),
    }
}

fn policy_str(
    map: &serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Result<Option<CompactString>, MgmtError> {
    match map.get(key) {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::String(s)) if !s.is_empty() => {
            Ok(Some(CompactString::from(s.as_str())))
        }
        Some(serde_json::Value::String(_)) => Ok(None),
        _ => Err(MgmtError::BadRequest(format!("{key} must be a string"))),
    }
}

/// Map stored policy `p` to the management JSON row.
///
/// Returns the list-endpoint object. Definition keys stay as stored.
pub(super) fn policy_item(p: &Policy) -> serde_json::Value {
    let mut definition = serde_json::Map::new();
    if let Some(v) = p.message_ttl_ms {
        definition.insert("message-ttl".into(), serde_json::json!(v));
    }
    if let Some(v) = &p.dead_letter_exchange {
        definition.insert("dead-letter-exchange".into(), serde_json::json!(v.as_str()));
    }
    if let Some(v) = &p.dead_letter_routing_key {
        definition.insert(
            "dead-letter-routing-key".into(),
            serde_json::json!(v.as_str()),
        );
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
        definition.insert(
            "overflow".into(),
            serde_json::json!(match v {
                queueforge_core::OverflowPolicy::DropHead => "drop-head",
                queueforge_core::OverflowPolicy::RejectPublish => "reject-publish",
                queueforge_core::OverflowPolicy::RejectPublishDlx => "reject-publish-dlx",
            }),
        );
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

pub(crate) fn policy_from_body(
    vhost: &str,
    name: &str,
    body: PutPolicyBody,
) -> Result<Policy, MgmtError> {
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
    let unknown: Vec<&str> = body
        .definition
        .keys()
        .filter(|k| !KNOWN.contains(&k.as_str()))
        .map(|k| k.as_str())
        .collect();
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
            Some(v) if v.as_str() == "reject-publish" => {
                Some(queueforge_core::OverflowPolicy::RejectPublish)
            }
            Some(v) if v.as_str() == "reject-publish-dlx" => {
                Some(queueforge_core::OverflowPolicy::RejectPublishDlx)
            }
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
            Some(v) if v.as_str() == "at-most-once" => {
                Some(queueforge_core::DeadLetterStrategy::AtMostOnce)
            }
            Some(v) if v.as_str() == "at-least-once" => {
                Some(queueforge_core::DeadLetterStrategy::AtLeastOnce)
            }
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
    let items: Vec<_> = state
        .router
        .list_policies(None)
        .iter()
        .map(policy_item)
        .collect();
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
            queueforge_core::federation::add_federation_link(
                upstream,
                vhost.clone(),
                body.pattern.clone(),
            );
        }
        for uri in queueforge_core::federation::federation_uris(&vhost) {
            let downstream = vhost.clone();
            let pattern = body.pattern.clone();
            let queues = std::sync::Arc::clone(&state.queues);
            let router = std::sync::Arc::clone(&state.router);
            tokio::spawn(async move {
                if let Err(err) =
                    super::shovel::federation_link(uri, downstream, pattern, queues, router).await
                {
                    tracing::warn!(error = %err, "federation link stopped");
                }
            });
        }
    }
    let policy = policy_from_body(&vhost, &name, body)?;
    state
        .router
        .upsert_policy(policy.clone())
        .map_err(|e| MgmtError::BadRequest(e.to_string()))?;
    let stored = policy.clone();
    db(&state, move |s| s.put_policy(&stored)).await?;
    apply_policies_live(&state).await;
    replicate(
        &state,
        "policy",
        serde_json::to_value(&policy).unwrap_or(serde_json::Value::Null),
    )
    .await;
    Ok(if existed {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::CREATED
    })
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
    replicate(
        &state,
        "delete_policy",
        serde_json::json!({ "vhost": vhost, "name": name }),
    )
    .await;
    Ok(StatusCode::NO_CONTENT)
}

/// Push merged queue arguments to live actors after a policy write.
///
/// `state` is the management process. A queue whose actor is gone is skipped.
/// Durable queues are written back to the store.
pub(super) async fn apply_policies_live(state: &MgmtState) {
    for key in state.queues.list_keys() {
        let Some(handle) = state.queues.get(&key) else {
            continue;
        };
        let declared = handle.info.declared_args.clone();
        let mut merged =
            state
                .router
                .queue_args_with_policy(key.vhost.as_str(), key.name.as_str(), &declared);
        if merged.queue_type == Some(queueforge_core::QueueType::Quorum)
            && merged.delivery_limit.is_none()
        {
            merged.delivery_limit = Some(20);
        }
        if let Ok(mut slot) = handle.info.args.lock() {
            *slot = merged.clone();
        }
        let _ = handle
            .tx
            .send(queueforge_core::QueueCmd::SetArgs {
                args: merged.clone(),
            })
            .await;
        if handle.info.durable {
            let mut row = handle.info.to_domain();
            row.args = merged;
            let _ = db(state, move |s| s.put_queue(&row)).await;
        }
    }
}
