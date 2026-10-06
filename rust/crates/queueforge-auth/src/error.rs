//! Auth error types.

use thiserror::Error;

/// Errors from authentication / authorization.
#[derive(Debug, Error)]
pub enum AuthError {
    /// Password does not meet policy (empty or shorter than minimum).
    #[error("password policy violation: {0}")]
    PasswordPolicy(String),

    /// Password hash could not be checked.
    #[error("password hash error: {0}")]
    PasswordHash(String),

    /// Bootstrap required but credentials were not provided.
    #[error(
        "bootstrap required: set QUEUEFORGE_ADMIN_USER and QUEUEFORGE_ADMIN_PASSWORD \
         (or pass --dev-bootstrap for local development)"
    )]
    BootstrapCredentialsMissing,

    /// Username is empty or invalid.
    #[error("invalid username: {0}")]
    InvalidUsername(String),

    /// Invalid permission regex pattern.
    #[error("invalid permission regex ({kind}): {source}")]
    InvalidRegex {
        /// Which permission field failed (`configure`, `write`, or `read`).
        kind: &'static str,
        /// Underlying regex error.
        #[source]
        source: regex::Error,
    },

    /// Underlying metadata store failure.
    #[error(transparent)]
    Store(#[from] queueforge_store::StoreError),
}

/// Result alias for auth operations.
pub type Result<T> = std::result::Result<T, AuthError>;
