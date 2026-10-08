//! Management-plane authorization helpers (parity with AMQP predicates).

use axum::http::HeaderMap;
use queueforge_auth::{AuthService, PermissionKind, ResourceKind, DEFAULT_EXCHANGE_PERM_NAME};
use queueforge_core::UserTag;

use crate::error::MgmtError;
use crate::session::{cookie_name_from_host, extract_named_token, Session};
use crate::state::MgmtState;

/// Extract session cookie token from request headers.
pub fn session_token_from_headers(headers: &HeaderMap) -> Option<String> {
    let raw = headers.get(axum::http::header::COOKIE)?.to_str().ok()?;
    let host = headers
        .get(axum::http::header::HOST)
        .and_then(|v| v.to_str().ok());
    extract_named_token(raw, &cookie_name_from_host(host))
}

/// Require a valid management session.
///
/// Tags are reloaded from the user record on every request. A deleted user
/// loses the session immediately. Demotion takes effect without a new login.
///
/// Without a session cookie, `Authorization: Basic` credentials are accepted,
/// as RabbitMQ's management API and its tools (rabbitmqadmin, Terraform,
/// monitoring agents) use them. The user must hold a management tag.
pub async fn require_session(state: &MgmtState, headers: &HeaderMap) -> Result<Session, MgmtError> {
    let cookie = session_token_from_headers(headers)
        .and_then(|token| state.sessions.get(&token).map(|session| (token, session)));
    let Some((token, mut session)) = cookie else {
        return basic_session(state, headers).await;
    };
    let store = std::sync::Arc::clone(&state.store);
    let name = session.username.clone();
    let user = queueforge_store::MetadataStore::blocking(store, move |s| s.get_user(&name))
        .await
        .map_err(MgmtError::from)?;
    let Some(user) = user else {
        state.sessions.remove(&token);
        return Err(MgmtError::Unauthorized);
    };
    session.tags = user.tags;
    Ok(session)
}

/// Decode `Authorization: Basic` into a user name and password.
fn basic_credentials(headers: &HeaderMap) -> Option<(String, String)> {
    use base64::Engine as _;
    let raw = headers.get(axum::http::header::AUTHORIZATION)?.to_str().ok()?;
    let encoded = raw.strip_prefix("Basic ").or_else(|| raw.strip_prefix("basic "))?;
    let decoded = base64::engine::general_purpose::STANDARD.decode(encoded.trim()).ok()?;
    let text = String::from_utf8(decoded).ok()?;
    let (user, password) = text.split_once(':')?;
    if user.is_empty() {
        return None;
    }
    Some((user.to_string(), password.to_string()))
}

/// Build a one-request session from Basic credentials. No cookie is set.
async fn basic_session(state: &MgmtState, headers: &HeaderMap) -> Result<Session, MgmtError> {
    let (name, password) = basic_credentials(headers).ok_or(MgmtError::Unauthorized)?;
    let store = std::sync::Arc::clone(&state.store);
    let lookup = name.clone();
    let user = queueforge_store::MetadataStore::blocking(store, move |s| s.get_user(&lookup))
        .await
        .map_err(MgmtError::from)?;
    let ok = match &user {
        Some(u) => queueforge_auth::verify_password(&password, &u.password_hash).map_err(MgmtError::from)?,
        None => {
            // Same work as a real check, so a missing user is not faster.
            let _ = queueforge_auth::verify_password(&password, queueforge_auth::dummy_password_hash());
            false
        }
    };
    let user = match (user, ok) {
        (Some(u), true) => u,
        // The next backends: OAuth 2.0, then LDAP. Tags come from the login.
        (stored, _) => {
            let Some(login) = queueforge_auth::external::login(&name, &password).await else {
                return Err(MgmtError::Unauthorized);
            };
            let Some(p) = queueforge_auth::external::principal(&login) else {
                return Err(MgmtError::Unauthorized);
            };
            if stored.is_some() {
                return Err(MgmtError::Unauthorized);
            }
            let tags: Vec<UserTag> = p
                .tags
                .iter()
                .filter_map(|t| [UserTag::Administrator, UserTag::Management, UserTag::Monitoring].into_iter().find(|k| k.as_str() == t))
                .collect();
            let mut u = queueforge_core::User::new(login.as_str(), String::new(), Vec::new());
            u.tags = tags;
            u
        }
    };
    if !(user.has_management() || user.has_monitoring()) {
        return Err(MgmtError::Forbidden);
    }
    let now = std::time::Instant::now();
    Ok(Session {
        username: name,
        tags: user.tags,
        created_at: now,
        last_seen: now,
    })
}

/// Resource mutations require `management` or `administrator` (not monitoring-only).
pub fn require_management_tag(session: &Session) -> Result<(), MgmtError> {
    if session
        .tags
        .iter()
        .any(|t| matches!(t, UserTag::Management | UserTag::Administrator))
    {
        Ok(())
    } else {
        Err(MgmtError::Forbidden)
    }
}

/// User / permission / vhost admin and definitions import require `administrator`.
pub fn require_administrator(session: &Session) -> Result<(), MgmtError> {
    if session.tags.contains(&UserTag::Administrator) {
        Ok(())
    } else {
        Err(MgmtError::Forbidden)
    }
}

/// AMQP-identical resource permission check; maps deny → 403.
///
/// The permission row is loaded on the blocking pool so redb does not stall
/// the HTTP worker.
pub async fn check_resource(
    state: &MgmtState,
    user: &str,
    vhost: &str,
    resource: &str,
    resource_kind: ResourceKind,
    kind: PermissionKind,
) -> Result<(), MgmtError> {
    let store = std::sync::Arc::clone(&state.store);
    let user = user.to_string();
    let vhost = vhost.to_string();
    let resource = resource.to_string();
    let joined = tokio::task::spawn_blocking(move || {
        let auth = AuthService::new(store.as_ref());
        auth.check_permission(&user, &vhost, &resource, resource_kind, kind)
    })
    .await
    .map_err(|e| MgmtError::Internal(format!("permission task panicked: {e}")))?;
    match joined {
        Ok(true) => Ok(()),
        Ok(false) => Err(MgmtError::Forbidden),
        Err(e) => Err(MgmtError::from(e)),
    }
}

/// Bind/unbind AuthZ (write on queue, read on exchange).
pub async fn check_bind(
    state: &MgmtState,
    user: &str,
    vhost: &str,
    queue: &str,
    exchange: &str,
) -> Result<(), MgmtError> {
    let store = std::sync::Arc::clone(&state.store);
    let user = user.to_string();
    let vhost = vhost.to_string();
    let queue = queue.to_string();
    let exchange = exchange.to_string();
    let joined = tokio::task::spawn_blocking(move || {
        let auth = AuthService::new(store.as_ref());
        auth.check_queue_bind(&user, &vhost, &queue, &exchange)
    })
    .await
    .map_err(|e| MgmtError::Internal(format!("permission task panicked: {e}")))?;
    match joined {
        Ok(true) => Ok(()),
        Ok(false) => Err(MgmtError::Forbidden),
        Err(e) => Err(MgmtError::from(e)),
    }
}

/// Write permission on exchange (including default exchange → `amq.default`).
pub async fn check_publish(
    state: &MgmtState,
    user: &str,
    vhost: &str,
    exchange: &str,
) -> Result<(), MgmtError> {
    check_resource(
        state,
        user,
        vhost,
        exchange,
        ResourceKind::Exchange,
        PermissionKind::Write,
    )
    .await
}

/// Read permission on queue for get/purge list of messages.
pub async fn check_queue_read(
    state: &MgmtState,
    user: &str,
    vhost: &str,
    queue: &str,
) -> Result<(), MgmtError> {
    check_resource(
        state,
        user,
        vhost,
        queue,
        ResourceKind::Queue,
        PermissionKind::Read,
    )
    .await
}

/// Configure permission on queue.
pub async fn check_queue_configure(
    state: &MgmtState,
    user: &str,
    vhost: &str,
    queue: &str,
) -> Result<(), MgmtError> {
    check_resource(
        state,
        user,
        vhost,
        queue,
        ResourceKind::Queue,
        PermissionKind::Configure,
    )
    .await
}

/// Configure permission on exchange.
pub async fn check_exchange_configure(
    state: &MgmtState,
    user: &str,
    vhost: &str,
    exchange: &str,
) -> Result<(), MgmtError> {
    check_resource(
        state,
        user,
        vhost,
        exchange,
        ResourceKind::Exchange,
        PermissionKind::Configure,
    )
    .await
}

/// Map empty exchange name to the permission name used by AuthZ.
#[allow(dead_code)]
pub fn exchange_perm_name(exchange: &str) -> &str {
    if exchange.is_empty() {
        DEFAULT_EXCHANGE_PERM_NAME
    } else {
        exchange
    }
}
