//! Authentication and authorization for QueueForge.
//!
//! - **AuthN:** Argon2id password hashing (design params: m=19456, t=2, p=1)
//! - **AuthZ:** per-vhost configure/write/read regexes (RabbitMQ-compatible),
//!   including `queue.bind` / `queue.unbind` (write on queue, read on exchange)
//! - **Bootstrap:** admin from `QUEUEFORGE_ADMIN_USER` /
//!   `QUEUEFORGE_ADMIN_PASSWORD`, or `--dev-bootstrap` local defaults
//!
//! Users and permissions are persisted via [`queueforge_store::MetadataStore`].

#![deny(missing_docs)]

/// Auth error types.
pub mod error;
/// Argon2id password hashing.
pub mod password;
/// Permission kinds and regex matching.
pub mod permission;
/// Auth service and bootstrap.
pub mod service;

pub use error::{AuthError, Result};
pub use password::{
    dummy_password_hash, hash_password, validate_password_policy, verify_password,
    ARGON2_MEMORY_KIB, ARGON2_PARALLELISM, ARGON2_TIME_COST, MAX_PASSWORD_BYTES, MIN_PASSWORD_LEN,
};
pub use permission::{
    check_permission, check_queue_bind, check_queue_unbind, check_user_permission,
    normalize_resource_name, PermissionKind, ResourceKind, DEFAULT_EXCHANGE_PERM_NAME,
};
pub use service::{
    resolve_bootstrap_credentials, AuthService, BootstrapMode, DEV_BOOTSTRAP_PASSWORD,
    DEV_BOOTSTRAP_USER, ENV_ADMIN_PASSWORD, ENV_ADMIN_USER,
};

// Re-export domain types used by callers of this crate.
pub use queueforge_core::{Permission, User, UserTag};
