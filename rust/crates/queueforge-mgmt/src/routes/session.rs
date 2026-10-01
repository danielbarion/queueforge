//! Login, logout, whoami, and the management session cookie.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::{ConnectInfo, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use queueforge_auth::AuthService;
use queueforge_core::{User, UserTag};

use super::health::peer_in_cidrs;
use super::{LoginRequest, WhoamiResponse};
use crate::authz::require_session;
use crate::error::MgmtError;
use crate::session::{
    build_session_cookie_named, clear_session_cookie_named, cookie_name_from_host,
    extract_named_token,
};
use crate::state::MgmtState;

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

/// Check the password in the JSON body against `state`. Returns a session cookie on success. A bad password is 401 and does not set a cookie.
pub(super) async fn login(
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

    let cookie_name =
        cookie_name_from_host(headers.get(header::HOST).and_then(|v| v.to_str().ok()));
    let cookie = build_session_cookie_named(&cookie_name, &token, state.config.cookie_secure);
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

/// Clear the session cookie named for this host. `headers` supply the current cookie. Returns 200 even when no session was present.
pub(super) async fn logout(
    State(state): State<MgmtState>,
    headers: HeaderMap,
) -> Result<Response, MgmtError> {
    if let Some(token) = session_token_from_headers(&headers) {
        state.sessions.remove(&token);
    }
    let cookie_name =
        cookie_name_from_host(headers.get(header::HOST).and_then(|v| v.to_str().ok()));
    let cookie = clear_session_cookie_named(&cookie_name, state.config.cookie_secure);
    let mut res = StatusCode::NO_CONTENT.into_response();
    res.headers_mut().insert(
        header::SET_COOKIE,
        cookie
            .parse()
            .map_err(|e| MgmtError::Internal(format!("cookie header: {e}")))?,
    );
    Ok(res)
}

/// Return the user for the session cookie in `headers`. Returns 401 when the cookie is missing or expired.
pub(super) async fn whoami(
    State(state): State<MgmtState>,
    headers: HeaderMap,
) -> Result<Json<WhoamiResponse>, MgmtError> {
    let session = require_session(&state, &headers).await?;
    Ok(Json(WhoamiResponse {
        name: session.username,
        tags: tags_as_str(&session.tags),
    }))
}

/// Report whether `user` may call the management API. Returns true when a management tag is present. Untagged users are refused by the handlers that call this.
pub(super) fn can_access_management(user: &User) -> bool {
    user.has_management() || user.has_monitoring()
}

/// Render `tags` as the strings the management UI shows. Returns one string per tag.
pub(super) fn tags_as_str(tags: &[UserTag]) -> Vec<&'static str> {
    tags.iter().map(|t| t.as_str()).collect()
}

/// Read the session token from `headers`. Returns `None` when the cookie for this host is absent.
pub(super) fn session_token_from_headers(headers: &HeaderMap) -> Option<String> {
    let raw = headers.get(header::COOKIE)?.to_str().ok()?;
    let host = headers.get(header::HOST).and_then(|v| v.to_str().ok());
    extract_named_token(raw, &cookie_name_from_host(host))
}
