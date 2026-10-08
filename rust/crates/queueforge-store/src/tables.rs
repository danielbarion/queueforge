//! redb table definitions for metadata schema v1.

use redb::TableDefinition;

/// Current metadata schema version written on first boot.
pub const SCHEMA_VERSION_V1: u32 = 1;

/// Key used in the `schema_version` table.
///
/// Design models this table as unit key `()` → `u32`. redb is more ergonomic
/// with a named string key for a single-row version table, so we intentionally
/// store `"version" → u32` instead of inventing a second convention later.
pub const SCHEMA_VERSION_KEY: &str = "version";

/// `schema_version`: key [`SCHEMA_VERSION_KEY`] → `u32`.
pub const SCHEMA_VERSION: TableDefinition<&str, u32> = TableDefinition::new("schema_version");

/// `vhosts`: name → JSON [`queueforge_core::Vhost`] bytes.
pub const VHOSTS: TableDefinition<&str, &[u8]> = TableDefinition::new("vhosts");

/// `users`: name → JSON [`queueforge_core::User`] bytes.
pub const USERS: TableDefinition<&str, &[u8]> = TableDefinition::new("users");

/// `permissions`: (user, vhost) → JSON [`queueforge_core::Permission`] bytes.
pub const PERMISSIONS: TableDefinition<(&str, &str), &[u8]> = TableDefinition::new("permissions");

/// `exchanges`: (vhost, name) → JSON [`queueforge_core::Exchange`] bytes.
pub const EXCHANGES: TableDefinition<(&str, &str), &[u8]> = TableDefinition::new("exchanges");

/// `queues`: (vhost, name) → JSON [`queueforge_core::Queue`] bytes.
pub const QUEUES: TableDefinition<(&str, &str), &[u8]> = TableDefinition::new("queues");

/// `policies`: (vhost, name) → JSON [`queueforge_core::Policy`] bytes.
pub const POLICIES: TableDefinition<(&str, &str), &[u8]> = TableDefinition::new("policies");

/// Previous bindings table: (vhost, exchange, queue, routing_key) → JSON bytes.
///
/// Opened only to copy rows into [`BINDINGS`]. Not used after that copy.
pub const LEGACY_BINDINGS: TableDefinition<(&str, &str, &str, &str), &[u8]> =
    TableDefinition::new("bindings");

/// `bindings_v2`: (vhost, exchange, queue, routing_key, args_fingerprint)
/// → JSON [`queueforge_core::Binding`] bytes.
///
/// The args fingerprint distinguishes headers bindings that share a routing key.
pub const BINDINGS: TableDefinition<(&str, &str, &str, &str, &str), &[u8]> =
    TableDefinition::new("bindings_v2");

/// `exchange_bindings`: (vhost, source, destination, routing_key) → empty.
///
/// Exchange-to-exchange bindings. Stored when both exchanges are durable.
pub const EXCHANGE_BINDINGS: TableDefinition<(&str, &str, &str, &str), &[u8]> =
    TableDefinition::new("exchange_bindings");

/// `parameters`: (component, vhost, name) → value bytes.
///
/// Runtime parameters without a table of their own, such as stream
/// consumer offsets and publisher sequences.
pub const PARAMETERS: TableDefinition<(&str, &str, &str), &[u8]> = TableDefinition::new("parameters");
