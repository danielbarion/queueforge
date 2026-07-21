//! Definitions export / import (RabbitMQ-compatible subset).

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use compact_str::CompactString;
use queueforge_auth::hash_password;
use queueforge_core::{
    Binding, Exchange, ExchangeType, Permission, QueueDeclareOpts, User, UserTag,
    BUILTIN_EXCHANGE_NAMES, DEFAULT_EXCHANGE_NAME,
};
use serde::{Deserialize, Serialize};

use crate::authz::{require_administrator, require_session};
use crate::error::MgmtError;
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
}

#[derive(Debug, Serialize, Deserialize)]
struct DefUser {
    name: String,
    /// PHC Argon2 hash when exporting; optional on import if `password` set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    password_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    password: Option<String>,
    #[serde(default)]
    tags: String,
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
}

#[derive(Debug, Serialize, Deserialize)]
struct DefBinding {
    source: String,
    vhost: String,
    destination: String,
    destination_type: String,
    routing_key: String,
}

/// GET /api/definitions
pub async fn export_definitions(
    State(state): State<MgmtState>,
    headers: HeaderMap,
) -> Result<Json<Definitions>, MgmtError> {
    let session = require_session(&state, &headers)?;
    // Export allowed for any management session (including monitoring).
    let _ = session;

    let users = state
        .store
        .list_users()?
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

    let vhosts = state
        .store
        .list_vhosts()?
        .into_iter()
        .map(|v| DefVhost {
            name: v.name.to_string(),
        })
        .collect();

    let permissions = state
        .store
        .list_permissions()?
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
    for vh in state.store.list_vhosts()? {
        for ex in state.store.list_exchanges(vh.name.as_str())? {
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
    for vh in state.store.list_vhosts()? {
        for q in state.store.list_queues(vh.name.as_str())? {
            queues.push(DefQueue {
                name: q.name.to_string(),
                vhost: q.vhost.to_string(),
                durable: q.durable,
                exclusive: q.exclusive,
                auto_delete: q.auto_delete,
            });
        }
    }
    // Include live-only (transient) queues from registry.
    for key in state.queues.list_keys() {
        if let Some(h) = state.queues.get(&key) {
            if !queues
                .iter()
                .any(|q| q.vhost == key.vhost.as_str() && q.name == key.name.as_str())
            {
                queues.push(DefQueue {
                    name: h.info.key.name.to_string(),
                    vhost: h.info.key.vhost.to_string(),
                    durable: h.info.durable,
                    exclusive: h.info.exclusive,
                    auto_delete: h.info.auto_delete,
                });
            }
        }
    }

    let mut bindings = Vec::new();
    for vh in state.store.list_vhosts()? {
        for b in state.store.list_bindings(vh.name.as_str())? {
            bindings.push(DefBinding {
                source: b.exchange.to_string(),
                vhost: b.vhost.to_string(),
                destination: b.queue.to_string(),
                destination_type: "queue".into(),
                routing_key: b.routing_key.to_string(),
            });
        }
    }
    for k in state.router.index().list() {
        if !bindings.iter().any(|b| {
            b.vhost == k.vhost.as_str()
                && b.source == k.exchange.as_str()
                && b.destination == k.queue.as_str()
                && b.routing_key == k.routing_key.as_str()
        }) {
            bindings.push(DefBinding {
                source: k.exchange.to_string(),
                vhost: k.vhost.to_string(),
                destination: k.queue.to_string(),
                destination_type: "queue".into(),
                routing_key: k.routing_key.to_string(),
            });
        }
    }

    Ok(Json(Definitions {
        rabbit_version: "3.13.0".into(),
        queueforge_version: state.config.product_version.clone(),
        users,
        vhosts,
        permissions,
        exchanges,
        queues,
        bindings,
    }))
}

/// POST /api/definitions — merge import (upsert topology).
pub async fn import_definitions(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Json(body): Json<Definitions>,
) -> Result<StatusCode, MgmtError> {
    let session = require_session(&state, &headers)?;
    require_administrator(&session)?;

    // 1. Vhosts
    for vh in &body.vhosts {
        if state.store.get_vhost(&vh.name)?.is_none() {
            state.store.create_vhost(&vh.name)?;
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
        state.store.put_user(&user)?;
    }

    // 3. Permissions
    for p in &body.permissions {
        if state.store.get_vhost(&p.vhost)?.is_none() {
            return Err(MgmtError::NotFound(format!("vhost '{}'", p.vhost)));
        }
        let perm = Permission::new(
            p.user.as_str(),
            p.vhost.as_str(),
            p.configure.as_str(),
            p.write.as_str(),
            p.read.as_str(),
        );
        state.store.put_permission(&perm)?;
    }

    // 4. Exchanges
    for ex in &body.exchanges {
        if ex.name.is_empty() || BUILTIN_EXCHANGE_NAMES.contains(&ex.name.as_str()) {
            // Skip default/builtin — already present after vhost create.
            continue;
        }
        let kind = match ex.kind.to_ascii_lowercase().as_str() {
            "direct" => ExchangeType::Direct,
            "fanout" => ExchangeType::Fanout,
            "topic" => ExchangeType::Topic,
            other => {
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
        };
        if exchange.durable {
            let _ = state.store.put_exchange(&exchange);
        }
        state.router.put_exchange(exchange);
    }

    // 5. Queues
    for q in &body.queues {
        if q.exclusive {
            continue; // skip exclusive on import
        }
        let opts = QueueDeclareOpts {
            durable: q.durable,
            exclusive: false,
            auto_delete: q.auto_delete,
            passive: false,
            exclusive_owner: None,
            args: Default::default(),
        };
        let _ = state.queues.declare(&q.vhost, &q.name, opts).await?;
    }

    // 6. Bindings
    for b in &body.bindings {
        if b.destination_type != "queue" {
            continue;
        }
        if b.source.is_empty() {
            continue; // implicit default
        }
        // Ensure exchange in router.
        if state.router.get_exchange(&b.vhost, &b.source).is_none() {
            if let Some(ex) = state.store.get_exchange(&b.vhost, &b.source)?.as_ref() {
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
        let binding = Binding::new(
            b.vhost.as_str(),
            b.source.as_str(),
            b.destination.as_str(),
            b.routing_key.as_str(),
        );
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
                    let _ = state.store.put_binding(&binding);
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
