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

/// `bindings`: (vhost, exchange, queue, routing_key) → JSON [`queueforge_core::Binding`] bytes.
pub const BINDINGS: TableDefinition<(&str, &str, &str, &str), &[u8]> =
    TableDefinition::new("bindings");
