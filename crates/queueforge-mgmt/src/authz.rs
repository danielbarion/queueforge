//! Management-plane authorization helpers (parity with AMQP predicates).

use axum::http::HeaderMap;
use queueforge_auth::{AuthService, PermissionKind, ResourceKind, DEFAULT_EXCHANGE_PERM_NAME};
use queueforge_core::UserTag;

use crate::error::MgmtError;
use crate::session::{extract_token_from_cookie_header, Session};
use crate::state::MgmtState;

/// Extract session cookie token from request headers.
pub fn session_token_from_headers(headers: &HeaderMap) -> Option<String> {
    let raw = headers.get(axum::http::header::COOKIE)?.to_str().ok()?;
    extract_token_from_cookie_header(raw)
}

/// Require a valid management session.
pub fn require_session(state: &MgmtState, headers: &HeaderMap) -> Result<Session, MgmtError> {
    let token = session_token_from_headers(headers).ok_or(MgmtError::Unauthorized)?;
    state.sessions.get(&token).ok_or(MgmtError::Unauthorized)
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
pub fn check_resource(
    state: &MgmtState,
    user: &str,
    vhost: &str,
    resource: &str,
    resource_kind: ResourceKind,
    kind: PermissionKind,
) -> Result<(), MgmtError> {
    let auth = AuthService::new(state.store.as_ref());
    match auth.check_permission(user, vhost, resource, resource_kind, kind) {
        Ok(true) => Ok(()),
        Ok(false) => Err(MgmtError::Forbidden),
        Err(e) => Err(MgmtError::from(e)),
    }
}

/// Bind/unbind AuthZ (write on queue, read on exchange).
pub fn check_bind(
    state: &MgmtState,
    user: &str,
    vhost: &str,
    queue: &str,
    exchange: &str,
) -> Result<(), MgmtError> {
    let auth = AuthService::new(state.store.as_ref());
    match auth.check_queue_bind(user, vhost, queue, exchange) {
        Ok(true) => Ok(()),
        Ok(false) => Err(MgmtError::Forbidden),
        Err(e) => Err(MgmtError::from(e)),
    }
}

/// Write permission on exchange (including default exchange → `amq.default`).
pub fn check_publish(
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
}

/// Read permission on queue for get/purge list of messages.
pub fn check_queue_read(
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
}

/// Configure permission on queue.
pub fn check_queue_configure(
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
}

/// Configure permission on exchange.
pub fn check_exchange_configure(
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
