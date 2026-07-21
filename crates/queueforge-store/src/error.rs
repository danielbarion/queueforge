//! Store error types.

use thiserror::Error;

/// Errors from the metadata / message store.
#[derive(Debug, Error)]
pub enum StoreError {
    /// Underlying redb database failure.
    #[error("database error: {0}")]
    Database(String),

    /// Filesystem I/O failure.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// JSON encode/decode failure for stored values.
    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    /// Unsupported or corrupt schema version.
    #[error("unsupported schema version: {0}")]
    UnsupportedSchema(u32),

    /// Vhost already exists.
    #[error("vhost already exists: {0}")]
    VhostExists(String),

    /// Vhost not found.
    #[error("vhost not found: {0}")]
    VhostNotFound(String),

    /// Exchange already exists.
    #[error("exchange already exists: {vhost}/{name}")]
    ExchangeExists {
        /// Virtual host.
        vhost: String,
        /// Exchange name.
        name: String,
    },

    /// Exchange not found.
    #[error("exchange not found: {vhost}/{name}")]
    ExchangeNotFound {
        /// Virtual host.
        vhost: String,
        /// Exchange name.
        name: String,
    },

    /// Queue already exists.
    #[error("queue already exists: {vhost}/{name}")]
    QueueExists {
        /// Virtual host.
        vhost: String,
        /// Queue name.
        name: String,
    },

    /// Queue not found.
    #[error("queue not found: {vhost}/{name}")]
    QueueNotFound {
        /// Virtual host.
        vhost: String,
        /// Queue name.
        name: String,
    },

    /// Attempt to delete a builtin exchange.
    #[error("cannot delete builtin exchange: {vhost}/{name}")]
    BuiltinExchange {
        /// Virtual host.
        vhost: String,
        /// Exchange name.
        name: String,
    },

    /// Binding already exists.
    #[error("binding already exists: {vhost}/{exchange} → {queue} ({routing_key})")]
    BindingExists {
        /// Virtual host.
        vhost: String,
        /// Exchange name.
        exchange: String,
        /// Queue name.
        queue: String,
        /// Routing key.
        routing_key: String,
    },

    /// Binding not found.
    #[error("binding not found: {vhost}/{exchange} → {queue} ({routing_key})")]
    BindingNotFound {
        /// Virtual host.
        vhost: String,
        /// Exchange name.
        exchange: String,
        /// Queue name.
        queue: String,
        /// Routing key.
        routing_key: String,
    },

    /// User already exists.
    #[error("user already exists: {0}")]
    UserExists(String),

    /// User not found.
    #[error("user not found: {0}")]
    UserNotFound(String),

    /// Permission entry not found for (user, vhost).
    #[error("permission not found: {user} @ {vhost}")]
    PermissionNotFound {
        /// Username.
        user: String,
        /// Virtual host.
        vhost: String,
    },

    /// WAL segment is corrupt (CRC / magic / version); queue recovery must halt.
    #[error("WAL corrupt at {path}: {reason}")]
    WalCorrupt {
        /// Segment path (may be empty when decoding a buffer).
        path: String,
        /// Human-readable reason.
        reason: String,
    },

    /// Incomplete record at end of segment (safe to truncate).
    #[error("WAL torn tail at byte {at}")]
    WalTornTail {
        /// Byte offset of the incomplete record.
        at: u64,
    },
}

impl From<redb::DatabaseError> for StoreError {
    fn from(err: redb::DatabaseError) -> Self {
        Self::Database(err.to_string())
    }
}

impl From<redb::TransactionError> for StoreError {
    fn from(err: redb::TransactionError) -> Self {
        Self::Database(err.to_string())
    }
}

impl From<redb::TableError> for StoreError {
    fn from(err: redb::TableError) -> Self {
        Self::Database(err.to_string())
    }
}

impl From<redb::StorageError> for StoreError {
    fn from(err: redb::StorageError) -> Self {
        Self::Database(err.to_string())
    }
}

impl From<redb::CommitError> for StoreError {
    fn from(err: redb::CommitError) -> Self {
        Self::Database(err.to_string())
    }
}

impl From<redb::Error> for StoreError {
    fn from(err: redb::Error) -> Self {
        Self::Database(err.to_string())
    }
}

/// Result alias for store operations.
pub type Result<T> = std::result::Result<T, StoreError>;
