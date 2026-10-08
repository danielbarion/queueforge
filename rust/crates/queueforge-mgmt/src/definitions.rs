//! Definitions export / import (RabbitMQ-compatible subset).

use std::sync::Arc;

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use compact_str::CompactString;
use queueforge_auth::hash_password;
use queueforge_core::{
    binding_args_key, Binding, Exchange, ExchangeType, HeaderArg, Permission, QueueDeclareOpts,
    User, UserTag, BUILTIN_EXCHANGE_NAMES, DEFAULT_EXCHANGE_NAME,
};
use serde::{Deserialize, Serialize};

use crate::authz::{require_administrator, require_session};
use crate::error::MgmtError;
use crate::mutations::{policy_from_body, PutPolicyBody};
use crate::state::MgmtState;

#[derive(Debug, Serialize, Deserialize)]
pub struct Definitions {
    #[serde(default)]
    rabbit_version: String,
    #[serde(default)]
    queueforge_version: String,
    #[serde(default)]
    users: Vec<DefUser>,
    #[serde(default)]
    vhosts: Vec<DefVhost>,
    #[serde(default)]
    permissions: Vec<DefPermission>,
    #[serde(default)]
    exchanges: Vec<DefExchange>,
    #[serde(default)]
    queues: Vec<DefQueue>,
    #[serde(default)]
    bindings: Vec<DefBinding>,
    #[serde(default)]
    policies: Vec<DefPolicy>,
}

#[derive(Debug, Serialize, Deserialize)]
struct DefPolicy {
    vhost: String,
    name: String,
    pattern: String,
    #[serde(rename = "apply-to", default = "default_apply_all")]
    apply_to: String,
    #[serde(default)]
    priority: i32,
    #[serde(default)]
    definition: serde_json::Map<String, serde_json::Value>,
}

fn default_apply_all() -> String {
    "all".into()
}

#[derive(Debug, Serialize, Deserialize)]
struct DefUser {
    name: String,
    /// RabbitMQ password-hash when exporting; optional on import if `password` set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    password_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    password: Option<String>,
    #[serde(default, deserialize_with = "tags_as_csv")]
    tags: String,
}

fn tags_as_csv<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    match value {
        serde_json::Value::String(text) => Ok(text),
        serde_json::Value::Array(items) => Ok(items
            .iter()
            .filter_map(|item| item.as_str())
            .collect::<Vec<_>>()
            .join(",")),
        _ => Ok(String::new()),
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct DefVhost {
    name: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct DefPermission {
    user: String,
    vhost: String,
    configure: String,
    write: String,
    read: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct DefExchange {
    name: String,
    vhost: String,
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    durable: bool,
    #[serde(default)]
    auto_delete: bool,
    #[serde(default)]
    internal: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct DefQueue {
    name: String,
    vhost: String,
    #[serde(default)]
    durable: bool,
    #[serde(default)]
    exclusive: bool,
    #[serde(default)]
    auto_delete: bool,
    #[serde(default)]
    arguments: serde_json::Map<String, serde_json::Value>,
    #[serde(rename = "type", default = "default_queue_type")]
    queue_type: String,
}

fn default_queue_type() -> String {
    "classic".into()
}

fn queue_args_map(args: &queueforge_core::QueueArgs) -> serde_json::Map<String, serde_json::Value> {
    let mut map = serde_json::Map::new();
    if let Some(v) = args.message_ttl_ms {
        map.insert("x-message-ttl".into(), serde_json::json!(v));
    }
    if let Some(v) = args.expires_ms {
        map.insert("x-expires".into(), serde_json::json!(v));
    }
    if let Some(v) = args.max_length {
        map.insert("x-max-length".into(), serde_json::json!(v));
    }
    if let Some(v) = args.max_length_bytes {
        map.insert("x-max-length-bytes".into(), serde_json::json!(v));
    }
    if let Some(v) = args.dead_letter_exchange.as_ref() {
        map.insert(
            "x-dead-letter-exchange".into(),
            serde_json::json!(v.as_str()),
        );
    }
    if let Some(v) = args.dead_letter_routing_key.as_ref() {
        map.insert(
            "x-dead-letter-routing-key".into(),
            serde_json::json!(v.as_str()),
        );
    }
    if let Some(v) = args.max_priority {
        map.insert("x-max-priority".into(), serde_json::json!(v));
    }
    if let Some(v) = args.delivery_limit {
        map.insert("x-delivery-limit".into(), serde_json::json!(v));
    }
    if args.single_active {
        map.insert("x-single-active-consumer".into(), serde_json::json!("true"));
    }
    if let Some(kind) = args.queue_type {
        map.insert("x-queue-type".into(), serde_json::json!(kind.as_str()));
    }
    map
}

#[derive(Debug, Serialize, Deserialize)]
struct DefBinding {
    source: String,
    vhost: String,
    destination: String,
    destination_type: String,
    routing_key: String,
    #[serde(default)]
    arguments: serde_json::Map<String, serde_json::Value>,
}

fn args_to_json(args: &[(CompactString, HeaderArg)]) -> serde_json::Map<String, serde_json::Value> {
    let mut map = serde_json::Map::new();
    for (name, value) in args {
        let stored = match value {
            HeaderArg::Str(text) => serde_json::Value::String(text.clone()),
            HeaderArg::Int(n) => serde_json::Value::Number((*n).into()),
        };
        map.insert(name.to_string(), stored);
    }
    map
}

/// GET /api/definitions
pub async fn export_definitions(
    State(state): State<MgmtState>,
    headers: HeaderMap,
) -> Result<Json<Definitions>, MgmtError> {
    let session = require_session(&state, &headers).await?;
    // Export allowed for any management session (including monitoring).
    let _ = session;

    let store = Arc::clone(&state.store);
    let snap = queueforge_store::MetadataStore::blocking(store, |s| {
        let vhosts = s.list_vhosts()?;
        let mut exchanges = Vec::new();
        let mut queues = Vec::new();
        let mut bindings = Vec::new();
        for vh in &vhosts {
            exchanges.extend(s.list_exchanges(vh.name.as_str())?);
            queues.extend(s.list_queues(vh.name.as_str())?);
            bindings.extend(s.list_bindings(vh.name.as_str())?);
        }
        Ok((
            s.list_users()?,
            vhosts,
            s.list_permissions()?,
            exchanges,
            queues,
            bindings,
        ))
    })
    .await?;
    let (snap_users, snap_vhosts, snap_perms, snap_exchanges, snap_queues, snap_bindings) = snap;

    let users = snap_users
        .into_iter()
        .map(|u| DefUser {
            name: u.name.to_string(),
            password_hash: Some(u.password_hash),
            password: None,
            tags: u
                .tags
                .iter()
                .map(|t| t.as_str())
                .collect::<Vec<_>>()
                .join(","),
        })
        .collect();

    let vhosts = snap_vhosts
        .iter()
        .map(|v| DefVhost {
            name: v.name.to_string(),
        })
        .collect();

    let permissions = snap_perms
        .into_iter()
        .map(|p| DefPermission {
            user: p.user.to_string(),
            vhost: p.vhost.to_string(),
            configure: p.configure,
            write: p.write,
            read: p.read,
        })
        .collect();

    let mut exchanges = Vec::new();
    for vh in &snap_vhosts {
        for ex in snap_exchanges.iter().filter(|ex| ex.vhost == vh.name) {
            // Skip default exchange name in export (implicit).
            if ex.name.as_str() == DEFAULT_EXCHANGE_NAME {
                continue;
            }
            exchanges.push(DefExchange {
                name: ex.name.to_string(),
                vhost: ex.vhost.to_string(),
                kind: match ex.kind {
                    ExchangeType::Default => "direct".into(),
                    other => other.as_str().to_string(),
                },
                durable: ex.durable,
                auto_delete: ex.auto_delete,
                internal: ex.internal,
            });
        }
        // Live-only (non-durable) exchanges from the router.
        for ex in state.router.list_exchanges(vh.name.as_str()) {
            if ex.name.as_str() == DEFAULT_EXCHANGE_NAME {
                continue;
            }
            if exchanges
                .iter()
                .any(|e| e.vhost == ex.vhost.as_str() && e.name == ex.name.as_str())
            {
                continue;
            }
            exchanges.push(DefExchange {
                name: ex.name.to_string(),
                vhost: ex.vhost.to_string(),
                kind: match ex.kind {
                    ExchangeType::Default => "direct".into(),
                    other => other.as_str().to_string(),
                },
                durable: ex.durable,
                auto_delete: ex.auto_delete,
                internal: ex.internal,
            });
        }
    }

    let mut queues = Vec::new();
    for q in &snap_queues {
        queues.push(DefQueue {
            name: q.name.to_string(),
            vhost: q.vhost.to_string(),
            durable: q.durable,
            exclusive: q.exclusive,
            auto_delete: q.auto_delete,
            arguments: queue_args_map(&q.args),
            queue_type: q
                .args
                .queue_type
                .unwrap_or(queueforge_core::QueueType::Classic)
                .as_str()
                .to_string(),
        });
    }
    // Include live-only (transient) queues from registry.
    for key in state.queues.list_keys() {
        if let Some(h) = state.queues.get(&key) {
            if !queues
                .iter()
                .any(|q| q.vhost == key.vhost.as_str() && q.name == key.name.as_str())
            {
                let args = h
                    .info
                    .args
                    .lock()
                    .unwrap_or_else(|err| err.into_inner())
                    .clone();
                queues.push(DefQueue {
                    name: h.info.key.name.to_string(),
                    vhost: h.info.key.vhost.to_string(),
                    durable: h.info.durable,
                    exclusive: h.info.exclusive,
                    auto_delete: h.info.auto_delete,
                    arguments: queue_args_map(&args),
                    queue_type: args
                        .queue_type
                        .unwrap_or(queueforge_core::QueueType::Classic)
                        .as_str()
                        .to_string(),
                });
            }
        }
    }

    let mut bindings = Vec::new();
    for b in &snap_bindings {
        bindings.push(DefBinding {
            source: b.exchange.to_string(),
            vhost: b.vhost.to_string(),
            destination: b.queue.to_string(),
            destination_type: "queue".into(),
            routing_key: b.routing_key.to_string(),
            arguments: args_to_json(&b.args),
        });
    }
    for k in state.router.index().list() {
        if !bindings.iter().any(|b| {
            b.vhost == k.vhost.as_str()
                && b.source == k.exchange.as_str()
                && b.destination == k.queue.as_str()
                && b.routing_key == k.routing_key.as_str()
                && binding_args_key(&crate::mutations::args_from_json(&b.arguments))
                    == binding_args_key(&k.args)
        }) {
            bindings.push(DefBinding {
                source: k.exchange.to_string(),
                vhost: k.vhost.to_string(),
                destination: k.queue.to_string(),
                destination_type: "queue".into(),
                routing_key: k.routing_key.to_string(),
                arguments: args_to_json(&k.args),
            });
        }
    }

    for (vhost, source, destination, routing_key) in state.router.list_exchange_links() {
        bindings.push(DefBinding {
            source,
            vhost,
            destination,
            destination_type: "exchange".into(),
            routing_key,
            arguments: args_to_json(&[]),
        });
    }

    let policies = state
        .router
        .list_policies(None)
        .into_iter()
        .map(|p| {
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
            DefPolicy {
                vhost: p.vhost.to_string(),
                name: p.name.to_string(),
                pattern: p.pattern,
                apply_to: p.apply_to.as_str().to_string(),
                priority: p.priority,
                definition,
            }
        })
        .collect();

    Ok(Json(Definitions {
        rabbit_version: "3.13.0".into(),
        queueforge_version: state.config.product_version.clone(),
        users,
        vhosts,
        permissions,
        exchanges,
        queues,
        bindings,
        policies,
    }))
}

/// POST /api/definitions — merge import (upsert topology).
pub async fn import_definitions(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Json(body): Json<Definitions>,
) -> Result<StatusCode, MgmtError> {
    let session = require_session(&state, &headers).await?;
    require_administrator(&session)?;

    // 1. Vhosts
    for vh in &body.vhosts {
        let vhost_name = vh.name.clone();
        if queueforge_store::MetadataStore::blocking(Arc::clone(&state.store), {
            let vhost_name = vhost_name.clone();
            move |s| s.get_vhost(&vhost_name)
        })
        .await?
        .is_none()
        {
            queueforge_store::MetadataStore::blocking(Arc::clone(&state.store), {
                let vhost_name = vhost_name.clone();
                move |s| s.create_vhost(&vhost_name)
            })
            .await?;
            for ex in Exchange::builtins_for(vh.name.as_str()) {
                state.router.put_exchange(ex);
            }
        }
    }

    // 2. Users
    for u in &body.users {
        let tags = parse_tag_csv(&u.tags)?;
        let password_hash = if let Some(ref p) = u.password {
            let p = p.clone();
            tokio::task::spawn_blocking(move || hash_password(&p))
                .await
                .map_err(|e| MgmtError::Internal(format!("hash join: {e}")))??
        } else if let Some(ref h) = u.password_hash {
            h.clone()
        } else {
            return Err(MgmtError::BadRequest(format!(
                "user '{}' needs password or password_hash",
                u.name
            )));
        };
        let user = User::new(u.name.as_str(), password_hash, tags);
        let stored = user.clone();
        queueforge_store::MetadataStore::blocking(Arc::clone(&state.store), move |s| {
            s.put_user(&stored)
        })
        .await?;
    }

    // 3. Permissions
    for p in &body.permissions {
        let vhost_name = p.vhost.clone();
        if queueforge_store::MetadataStore::blocking(Arc::clone(&state.store), move |s| {
            s.get_vhost(&vhost_name)
        })
        .await?
        .is_none()
        {
            return Err(MgmtError::NotFound(format!("vhost '{}'", p.vhost)));
        }
        let perm = Permission::new(
            p.user.as_str(),
            p.vhost.as_str(),
            p.configure.as_str(),
            p.write.as_str(),
            p.read.as_str(),
        );
        let stored = perm.clone();
        queueforge_store::MetadataStore::blocking(Arc::clone(&state.store), move |s| {
            s.put_permission(&stored)
        })
        .await?;
    }

    // 4. Exchanges
    for ex in &body.exchanges {
        if ex.name.is_empty() || BUILTIN_EXCHANGE_NAMES.contains(&ex.name.as_str()) {
            // Skip default/builtin — already present after vhost create.
            continue;
        }
        let kind = match ExchangeType::parse(&ex.kind.to_ascii_lowercase()) {
            Some(kind) => kind,
            None => {
                let other = ex.kind.as_str();
                return Err(MgmtError::BadRequest(format!(
                    "invalid exchange type '{other}'"
                )));
            }
        };
        let exchange = Exchange {
            vhost: CompactString::from(ex.vhost.as_str()),
            name: CompactString::from(ex.name.as_str()),
            kind,
            durable: ex.durable,
            auto_delete: ex.auto_delete,
            internal: ex.internal,
            alternate: None,
            delayed_type: None,
        };
        if exchange.durable {
            let stored = exchange.clone();
            let _ = queueforge_store::MetadataStore::blocking(Arc::clone(&state.store), move |s| {
                s.put_exchange(&stored)
            })
            .await;
        }
        state.router.put_exchange(exchange);
    }

    // 5. Policies, before queues so a matching policy fills declare arguments.
    for p in &body.policies {
        let policy = policy_from_body(
            &p.vhost,
            &p.name,
            PutPolicyBody {
                pattern: p.pattern.clone(),
                apply_to: p.apply_to.clone(),
                priority: p.priority,
                definition: p.definition.clone(),
            },
        )?;
        state
            .router
            .upsert_policy(policy)
            .map_err(|e| MgmtError::BadRequest(e.to_string()))?;
    }

    // 6. Queues
    for q in &body.queues {
        if q.exclusive {
            continue; // skip exclusive on import
        }
        let mut argument_map = q.arguments.clone();
        if !q.queue_type.is_empty() {
            argument_map.insert("x-queue-type".into(), serde_json::json!(q.queue_type));
        }
        let declared = crate::mutations::parse_mgmt_queue_args(&Some(argument_map))?;
        let mut args = state
            .router
            .queue_args_with_policy(&q.vhost, &q.name, &declared);
        if args.queue_type == Some(queueforge_core::QueueType::Quorum)
            && args.delivery_limit.is_none()
        {
            args.delivery_limit = Some(20);
        }
        let opts = QueueDeclareOpts {
            declared_args: Some(declared.clone()),
            home: None,
            durable: q.durable,
            exclusive: false,
            auto_delete: q.auto_delete,
            passive: false,
            exclusive_owner: None,
            args,
        };
        let _ = state.queues.declare(&q.vhost, &q.name, opts).await?;
    }

    // 7. Bindings
    for b in &body.bindings {
        if b.destination_type == "exchange" && !b.source.is_empty() {
            for name in [&b.source, &b.destination] {
                if state.router.get_exchange(&b.vhost, name).is_some() {
                    continue;
                }
                let (vhost_name, ex_name) = (b.vhost.clone(), name.clone());
                if let Some(ex) = queueforge_store::MetadataStore::blocking(Arc::clone(&state.store), move |s| {
                    s.get_exchange(&vhost_name, &ex_name)
                })
                .await?
                {
                    state.router.put_exchange(ex);
                }
            }
            state
                .router
                .bind_exchange(&b.vhost, &b.source, &b.destination, &b.routing_key)
                .map_err(|e| MgmtError::BadRequest(format!("exchange binding: {e}")))?;
            let durable = |n: &str| state.router.get_exchange(&b.vhost, n).is_some_and(|ex| ex.durable);
            if durable(&b.source) && durable(&b.destination) {
                let (v, src, dst, rk) = (b.vhost.clone(), b.source.clone(), b.destination.clone(), b.routing_key.clone());
                queueforge_store::MetadataStore::blocking(Arc::clone(&state.store), move |s| {
                    s.put_exchange_binding(&v, &src, &dst, &rk)
                })
                .await?;
            }
            continue;
        }
        if b.destination_type != "queue" {
            continue;
        }
        if b.source.is_empty() {
            continue; // implicit default
        }
        // Ensure exchange in router.
        if state.router.get_exchange(&b.vhost, &b.source).is_none() {
            let vhost_name = b.vhost.clone();
            let source = b.source.clone();
            if let Some(ex) =
                queueforge_store::MetadataStore::blocking(Arc::clone(&state.store), move |s| {
                    s.get_exchange(&vhost_name, &source)
                })
                .await?
                .as_ref()
            {
                state.router.put_exchange(ex.clone());
            } else {
                return Err(MgmtError::NotFound(format!(
                    "exchange '{}' on import bind",
                    b.source
                )));
            }
        }
        if state
            .queues
            .get(&queueforge_core::QueueKey::new(&b.vhost, &b.destination))
            .is_none()
        {
            return Err(MgmtError::NotFound(format!(
                "queue '{}' on import bind",
                b.destination
            )));
        }
        let mut binding = Binding::new(
            b.vhost.as_str(),
            b.source.as_str(),
            b.destination.as_str(),
            b.routing_key.as_str(),
        );
        binding.args = crate::mutations::args_from_json(&b.arguments);
        match state.router.bind(binding.clone()) {
            Ok(true) => {
                let ex_durable = state
                    .router
                    .get_exchange(&b.vhost, &b.source)
                    .map(|e| e.durable)
                    .unwrap_or(false);
                let q_durable = state
                    .queues
                    .get(&queueforge_core::QueueKey::new(&b.vhost, &b.destination))
                    .map(|h| h.info.durable)
                    .unwrap_or(false);
                if ex_durable && q_durable {
                    let stored = binding.clone();
                    let _ = queueforge_store::MetadataStore::blocking(
                        Arc::clone(&state.store),
                        move |s| s.put_binding(&stored),
                    )
                    .await;
                }
            }
            Ok(false) => {}
            Err(e) => return Err(MgmtError::from(e)),
        }
    }

    Ok(StatusCode::NO_CONTENT)
}

fn parse_tag_csv(s: &str) -> Result<Vec<UserTag>, MgmtError> {
    let mut out = Vec::new();
    for part in s.split(|c: char| c == ',' || c.is_whitespace()) {
        let t = part.trim();
        if t.is_empty() {
            continue;
        }
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
