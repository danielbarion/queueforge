//! Management API errors mapped to HTTP status codes.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;
use thiserror::Error;
use tracing::error;

/// Result alias for management handlers.
pub type Result<T> = std::result::Result<T, MgmtError>;

/// Errors returned by the management API.
#[derive(Debug, Error)]
pub enum MgmtError {
    /// Missing or invalid session cookie.
    #[error("unauthorized")]
    Unauthorized,
    /// Authenticated but lacking tags or resource permission.
    #[error("forbidden")]
    Forbidden,
    /// Invalid request body or query parameters.
    #[error("bad request: {0}")]
    BadRequest(String),
    /// Resource not found.
    #[error("not found: {0}")]
    NotFound(String),
    /// Precondition failed (e.g. in-use, property mismatch).
    #[error("precondition failed: {0}")]
    PreconditionFailed(String),
    /// Login rate limited.
    #[error("too many login failures")]
    TooManyRequests,
    /// Conflict (already exists).
    #[error("conflict: {0}")]
    Conflict(String),
    /// The cluster cannot commit the change now (no majority).
    #[error("service unavailable: {0}")]
    Unavailable(String),
    /// Internal / store / auth failures.
    ///
    /// The payload is logged server-side only; clients receive a stable
    /// `"internal error"` code without the detail string.
    #[error("internal error: {0}")]
    Internal(String),
}

#[derive(Serialize)]
struct ErrorBody {
    /// Stable public error code (never includes internal diagnostics).
    error: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
}

impl IntoResponse for MgmtError {
    fn into_response(self) -> Response {
        let (status, error_code, reason) = match &self {
            MgmtError::Unauthorized => (StatusCode::UNAUTHORIZED, "unauthorized", None),
            MgmtError::Forbidden => (StatusCode::FORBIDDEN, "forbidden", None),
            MgmtError::BadRequest(r) => (StatusCode::BAD_REQUEST, "bad request", Some(r.clone())),
            MgmtError::NotFound(r) => (StatusCode::NOT_FOUND, "not found", Some(r.clone())),
            MgmtError::PreconditionFailed(r) => (
                StatusCode::PRECONDITION_FAILED,
                "precondition failed",
                Some(r.clone()),
            ),
            MgmtError::Conflict(r) => (StatusCode::CONFLICT, "conflict", Some(r.clone())),
            MgmtError::Unavailable(r) => (StatusCode::SERVICE_UNAVAILABLE, "service unavailable", Some(r.clone())),
            MgmtError::TooManyRequests => (
                StatusCode::TOO_MANY_REQUESTS,
                "too many login failures",
                None,
            ),
            MgmtError::Internal(detail) => {
                error!(error = %detail, "management internal error");
                (StatusCode::INTERNAL_SERVER_ERROR, "internal error", None)
            }
        };
        let body = ErrorBody {
            error: error_code.to_string(),
            reason,
        };
        (status, Json(body)).into_response()
    }
}

impl From<queueforge_store::StoreError> for MgmtError {
    fn from(e: queueforge_store::StoreError) -> Self {
        use queueforge_store::StoreError;
        match e {
            StoreError::VhostNotFound(n)
            | StoreError::UserNotFound(n)
            | StoreError::QueueNotFound { name: n, .. }
            | StoreError::ExchangeNotFound { name: n, .. } => MgmtError::NotFound(n),
            StoreError::BindingNotFound {
                exchange,
                queue,
                routing_key,
                ..
            } => MgmtError::NotFound(format!("{exchange}→{queue} rk={routing_key}")),
            StoreError::PermissionNotFound { user, vhost } => {
                MgmtError::NotFound(format!("{user}@{vhost}"))
            }
            StoreError::VhostExists(n)
            | StoreError::UserExists(n)
            | StoreError::QueueExists { name: n, .. }
            | StoreError::ExchangeExists { name: n, .. } => MgmtError::Conflict(n),
            StoreError::BindingExists {
                exchange,
                queue,
                routing_key,
                ..
            } => MgmtError::Conflict(format!("{exchange}→{queue} rk={routing_key}")),
            StoreError::BuiltinExchange { name, .. } => {
                MgmtError::BadRequest(format!("cannot modify builtin exchange '{name}'"))
            }
            other => MgmtError::Internal(other.to_string()),
        }
    }
}

impl From<queueforge_auth::AuthError> for MgmtError {
    fn from(e: queueforge_auth::AuthError) -> Self {
        use queueforge_auth::AuthError;
        match e {
            AuthError::PasswordPolicy(m) | AuthError::InvalidUsername(m) => {
                MgmtError::BadRequest(m)
            }
            AuthError::InvalidRegex { kind, source } => {
                MgmtError::BadRequest(format!("invalid {kind} regex: {source}"))
            }
            AuthError::Store(s) => MgmtError::from(s),
            other => MgmtError::Internal(other.to_string()),
        }
    }
}

impl From<queueforge_core::Error> for MgmtError {
    fn from(e: queueforge_core::Error) -> Self {
        use queueforge_core::Error;
        match e {
            Error::NotFound(m) => MgmtError::NotFound(m),
            Error::AlreadyExists(m) => MgmtError::Conflict(m),
            Error::PreconditionFailed(m) => MgmtError::PreconditionFailed(m),
            Error::ResourceLocked(m) => MgmtError::PreconditionFailed(m),
            Error::NotImplemented(m) => MgmtError::BadRequest(m),
            other => MgmtError::Internal(other.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;

    #[tokio::test]
    async fn internal_error_does_not_leak_detail() {
        let res = MgmtError::Internal("secret disk full".into()).into_response();
        assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let body = String::from_utf8_lossy(&bytes);
        assert!(body.contains("internal error"));
        assert!(!body.contains("secret"));
        assert!(!body.contains("disk full"));
    }

    #[tokio::test]
    async fn bad_request_includes_safe_reason() {
        let res = MgmtError::BadRequest("missing field".into()).into_response();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let body = String::from_utf8_lossy(&bytes);
        assert!(body.contains("bad request"));
        assert!(body.contains("missing field"));
    }
}
