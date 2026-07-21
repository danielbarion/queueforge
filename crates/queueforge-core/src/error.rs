//! Shared error type for QueueForge core.

use thiserror::Error;

/// Core / broker domain errors (registry, actors, config).
#[derive(Debug, Error)]
pub enum Error {
    /// Configuration load or validation failure.
    #[error("config error: {0}")]
    Config(String),

    /// Named resource was not found.
    #[error("not found: {0}")]
    NotFound(String),

    /// Named resource already exists with incompatible properties.
    #[error("already exists: {0}")]
    AlreadyExists(String),

    /// Operation violated a precondition (AMQP 406-class conditions).
    #[error("precondition failed: {0}")]
    PreconditionFailed(String),

    /// Resource is locked (e.g. exclusive queue owned by another connection).
    #[error("resource locked: {0}")]
    ResourceLocked(String),

    /// Queue actor is unavailable (panic isolation; no silent restart).
    #[error("unavailable: {0}")]
    Unavailable(String),

    /// Resource limit exceeded (memory watermark, disk free budget, etc.).
    ///
    /// Mapped to AMQP channel reply **506 RESOURCE_ERROR**.
    #[error("resource error: {0}")]
    Resource(String),

    /// Metadata / store failure.
    #[error("store error: {0}")]
    Store(String),

    /// Feature not yet implemented (stub path).
    #[error("not implemented: {0}")]
    NotImplemented(String),

    /// Internal invariant failure.
    #[error("internal error: {0}")]
    Internal(String),
}

/// Result alias for core operations.
pub type Result<T> = std::result::Result<T, Error>;
