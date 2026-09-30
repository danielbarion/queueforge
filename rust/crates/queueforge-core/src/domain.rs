//! Protocol-agnostic domain types: vhosts, exchanges, queues, bindings, users,
//! and permissions.
//!
//! Queue declare arguments are persisted on the queue row and restored with the
//! actor. Binding rows do not store argument tables.
//!

use compact_str::CompactString;
use serde::{Deserialize, Serialize};

/// Default virtual host name (`/`).
pub const DEFAULT_VHOST: &str = "/";

/// Name of the unnamed default exchange (empty string).
pub const DEFAULT_EXCHANGE_NAME: &str = "";

/// Builtin exchange names created with every vhost.
pub const BUILTIN_EXCHANGE_NAMES: &[&str] = &["", "amq.direct", "amq.fanout", "amq.topic"];

/// A virtual host isolating exchanges, queues, and bindings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Vhost {
    /// Vhost name (e.g. `/`).
    pub name: CompactString,
}

impl Vhost {
    /// Create a vhost with the given name.
    pub fn new(name: impl Into<CompactString>) -> Self {
        Self { name: name.into() }
    }
}

/// Exchange routing type.
///
/// `Default` is the unnamed (`""`) direct exchange used for publish-by-queue-name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ExchangeType {
    /// Route by exact routing-key match.
    Direct,
    /// Ignore routing key; deliver to all bound queues.
    Fanout,
    /// Route by topic pattern (`*` / `#` wildcards).
    Topic,
    /// Route by message header table (`x-match` all/any). Routing key is ignored.
    Headers,
    /// The unnamed default exchange (`""`); routes when routing key equals queue name.
    Default,
}

impl ExchangeType {
    /// AMQP short-string name for this type (`direct`, `fanout`, `topic`, or empty for default).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::Fanout => "fanout",
            Self::Topic => "topic",
            Self::Headers => "headers",
            Self::Default => "direct",
        }
    }
}

/// An exchange definition within a vhost.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Exchange {
    /// Owning virtual host.
    pub vhost: CompactString,
    /// Exchange name (`""` for the default exchange).
    pub name: CompactString,
    /// Routing type.
    pub kind: ExchangeType,
    /// Survives broker restart when true.
    pub durable: bool,
    /// Deleted when last binding is removed (AMQP auto-delete).
    pub auto_delete: bool,
    /// Not directly publishable by clients when true (e.g. default exchange).
    pub internal: bool,
    /// Exchange named by the `alternate-exchange` argument, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alternate: Option<CompactString>,
}

impl Exchange {
    /// Construct a durable, non-auto-delete, non-internal exchange.
    pub fn new(
        vhost: impl Into<CompactString>,
        name: impl Into<CompactString>,
        kind: ExchangeType,
    ) -> Self {
        Self {
            vhost: vhost.into(),
            name: name.into(),
            kind,
            durable: true,
            auto_delete: false,
            internal: false,
            alternate: None,
        }
    }

    /// Builtin exchanges inserted at vhost creation.
    ///
    /// Returns `""` (default/internal direct), `amq.direct`, `amq.fanout`, `amq.topic`.
    pub fn builtins_for(vhost: impl Into<CompactString>) -> [Exchange; 4] {
        let vhost = vhost.into();
        [
            Exchange {
                vhost: vhost.clone(),
                name: CompactString::from(DEFAULT_EXCHANGE_NAME),
                kind: ExchangeType::Default,
                durable: true,
                auto_delete: false,
                internal: true,
                alternate: None,
            },
            Exchange {
                vhost: vhost.clone(),
                name: CompactString::from("amq.direct"),
                kind: ExchangeType::Direct,
                durable: true,
                auto_delete: false,
                internal: false,
                alternate: None,
            },
            Exchange {
                vhost: vhost.clone(),
                name: CompactString::from("amq.fanout"),
                kind: ExchangeType::Fanout,
                durable: true,
                auto_delete: false,
                internal: false,
                alternate: None,
            },
            Exchange {
                vhost,
                name: CompactString::from("amq.topic"),
                kind: ExchangeType::Topic,
                durable: true,
                auto_delete: false,
                internal: false,
                alternate: None,
            },
        ]
    }

    /// Whether this exchange is a server builtin (`""` or `amq.*` set above).
    pub fn is_builtin(&self) -> bool {
        BUILTIN_EXCHANGE_NAMES.contains(&self.name.as_str())
    }
}

/// Queue definition (metadata stub; message storage is separate).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Queue {
    /// Owning virtual host.
    pub vhost: CompactString,
    /// Queue name.
    pub name: CompactString,
    /// Survives broker restart when true.
    pub durable: bool,
    /// Restricted to a single connection when true.
    pub exclusive: bool,
    /// Deleted when last consumer cancels (AMQP auto-delete).
    pub auto_delete: bool,
    /// Closed declare-arguments set (TTL / DLX / max-length).
    #[serde(default, skip_serializing_if = "is_default_args")]
    pub args: crate::queue::QueueArgs,
    /// Node that owns the queue actor and write-ahead log.
    ///
    /// `None` on a single-node broker. Peers forward operations to this node.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub home: Option<CompactString>,
}

fn is_default_args(args: &crate::queue::QueueArgs) -> bool {
    args.is_default()
}

impl Queue {
    /// Construct a non-durable, non-exclusive, non-auto-delete queue.
    pub fn new(vhost: impl Into<CompactString>, name: impl Into<CompactString>) -> Self {
        Self {
            vhost: vhost.into(),
            name: name.into(),
            durable: false,
            exclusive: false,
            auto_delete: false,
            args: crate::queue::QueueArgs::default(),
            home: None,
        }
    }
}

/// Exchange→queue binding (user topology; not the implicit default-exchange rows).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Binding {
    /// Owning virtual host.
    pub vhost: CompactString,
    /// Source exchange name.
    pub exchange: CompactString,
    /// Destination queue name.
    pub queue: CompactString,
    /// Binding routing key / topic pattern.
    pub routing_key: CompactString,
    /// Header match arguments. Includes `x-match` when this is a headers binding.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<(CompactString, HeaderArg)>,
}

/// A header value stored on a binding argument.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(untagged)]
pub enum HeaderArg {
    /// Integer header.
    Int(i64),
    /// String header, including `x-match`.
    Str(String),
}

/// Stable store and management key for header arguments. Empty when there are none.
pub fn binding_args_key(args: &[(CompactString, HeaderArg)]) -> String {
    let mut parts: Vec<String> = args
        .iter()
        .map(|(name, value)| match value {
            HeaderArg::Str(text) => format!("s\u{1}{name}\u{1}{text}"),
            HeaderArg::Int(n) => format!("i\u{1}{name}\u{1}{n}"),
        })
        .collect();
    parts.sort();
    parts.join("\u{2}")
}

/// Management path key. Empty arguments keep the routing key so existing clients still match.
pub fn binding_properties_key(routing_key: &str, args: &[(CompactString, HeaderArg)]) -> String {
    let fingerprint = binding_args_key(args);
    if fingerprint.is_empty() {
        routing_key.to_string()
    } else {
        format!("{routing_key}\u{1e}{fingerprint}")
    }
}

impl Binding {
    /// Construct a binding.
    pub fn new(
        vhost: impl Into<CompactString>,
        exchange: impl Into<CompactString>,
        queue: impl Into<CompactString>,
        routing_key: impl Into<CompactString>,
    ) -> Self {
        Self {
            vhost: vhost.into(),
            exchange: exchange.into(),
            queue: queue.into(),
            routing_key: routing_key.into(),
            args: Vec::new(),
        }
    }
}

/// Management / capability tags attached to a user.
///
/// Tags control management-plane access; they do **not** bypass AMQP resource
/// permission regexes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum UserTag {
    /// Full management API and user administration.
    Administrator,
    /// Management UI / API access (mutations still need resource perms).
    Management,
    /// Read-only monitoring access in the management plane.
    Monitoring,
}

impl UserTag {
    /// Stable string form used in APIs and storage.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Administrator => "administrator",
            Self::Management => "management",
            Self::Monitoring => "monitoring",
        }
    }
}

/// Stored user credentials and tags.
///
/// `password_hash` is a PHC Argon2id string, or a RabbitMQ hash
/// (base64 of a 4-byte salt plus SHA-256 or SHA-512 of `salt || password`).
///
/// [`Debug`] redacts `password_hash` so logs/spans do not leak credential material.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct User {
    /// Unique username.
    pub name: CompactString,
    /// PHC Argon2id hash, or a RabbitMQ SHA-256/SHA-512 password hash.
    pub password_hash: String,
    /// Capability tags (see [`UserTag`]).
    pub tags: Vec<UserTag>,
}

impl std::fmt::Debug for User {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("User")
            .field("name", &self.name)
            .field("password_hash", &"[redacted]")
            .field("tags", &self.tags)
            .finish()
    }
}

impl User {
    /// Construct a user with the given name, password hash, and tags.
    pub fn new(
        name: impl Into<CompactString>,
        password_hash: impl Into<String>,
        tags: Vec<UserTag>,
    ) -> Self {
        Self {
            name: name.into(),
            password_hash: password_hash.into(),
            tags,
        }
    }

    /// Whether this user has the `administrator` tag.
    pub fn is_administrator(&self) -> bool {
        self.tags.contains(&UserTag::Administrator)
    }

    /// Whether this user has the `management` tag (or administrator).
    pub fn has_management(&self) -> bool {
        self.tags
            .iter()
            .any(|t| matches!(t, UserTag::Management | UserTag::Administrator))
    }

    /// Whether this user has the `monitoring` tag (or administrator).
    pub fn has_monitoring(&self) -> bool {
        self.tags
            .iter()
            .any(|t| matches!(t, UserTag::Monitoring | UserTag::Administrator))
    }
}

/// Per-vhost resource permissions as three regular expressions.
///
/// Matches RabbitMQ-style configure / write / read regexes. An empty string is
/// treated as matching nothing (`^$`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Permission {
    /// Username these permissions apply to.
    pub user: CompactString,
    /// Virtual host these permissions apply to.
    pub vhost: CompactString,
    /// Regex matched against resource names for configure operations.
    pub configure: String,
    /// Regex matched against resource names for write operations.
    pub write: String,
    /// Regex matched against resource names for read operations.
    pub read: String,
}

impl Permission {
    /// Construct permissions for `(user, vhost)`.
    pub fn new(
        user: impl Into<CompactString>,
        vhost: impl Into<CompactString>,
        configure: impl Into<String>,
        write: impl Into<String>,
        read: impl Into<String>,
    ) -> Self {
        Self {
            user: user.into(),
            vhost: vhost.into(),
            configure: configure.into(),
            write: write.into(),
            read: read.into(),
        }
    }

    /// Full access on a vhost (`.*` for configure, write, and read).
    pub fn full_access(user: impl Into<CompactString>, vhost: impl Into<CompactString>) -> Self {
        Self::new(user, vhost, ".*", ".*", ".*")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builtins_are_durable_and_named_correctly() {
        let builtins = Exchange::builtins_for("/");
        assert_eq!(builtins.len(), 4);
        assert_eq!(builtins[0].name, "");
        assert_eq!(builtins[0].kind, ExchangeType::Default);
        assert!(builtins[0].internal);
        assert!(builtins[0].durable);
        assert!(!builtins[0].auto_delete);

        assert_eq!(builtins[1].name, "amq.direct");
        assert_eq!(builtins[1].kind, ExchangeType::Direct);
        assert!(!builtins[1].internal);

        assert_eq!(builtins[2].name, "amq.fanout");
        assert_eq!(builtins[2].kind, ExchangeType::Fanout);

        assert_eq!(builtins[3].name, "amq.topic");
        assert_eq!(builtins[3].kind, ExchangeType::Topic);

        for ex in &builtins {
            assert!(ex.durable);
            assert!(!ex.auto_delete);
            assert!(ex.is_builtin());
        }
    }

    #[test]
    fn queue_defaults() {
        let q = Queue::new("/", "orders");
        assert!(!q.durable);
        assert!(!q.exclusive);
        assert!(!q.auto_delete);
    }

    #[test]
    fn user_tags_helpers() {
        let admin = User::new("a", "hash", vec![UserTag::Administrator]);
        assert!(admin.is_administrator());
        assert!(admin.has_management());
        assert!(admin.has_monitoring());

        let mon = User::new("m", "hash", vec![UserTag::Monitoring]);
        assert!(!mon.is_administrator());
        assert!(!mon.has_management());
        assert!(mon.has_monitoring());
    }

    #[test]
    fn user_debug_redacts_password_hash() {
        let u = User::new("alice", "$argon2id$secret-material", vec![]);
        let dbg = format!("{u:?}");
        assert!(dbg.contains("alice"));
        assert!(dbg.contains("[redacted]"));
        assert!(!dbg.contains("secret-material"));
        assert!(!dbg.contains("$argon2id$"));
    }

    #[test]
    fn permission_full_access() {
        let p = Permission::full_access("admin", "/");
        assert_eq!(p.configure, ".*");
        assert_eq!(p.write, ".*");
        assert_eq!(p.read, ".*");
    }
}
