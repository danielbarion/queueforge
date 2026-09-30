//! Broker configuration loaded from TOML (and optional env overrides).

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::Error;

/// Default WAL segment rotation size (128 MiB).
pub const DEFAULT_WAL_SEGMENT_MAX_BYTES: u64 = 134_217_728;

/// Default group-commit interval for `every_n_ms` fsync policy.
pub const DEFAULT_FSYNC_INTERVAL_MS: u64 = 100;

/// Default minimum free disk space before durable publishes are refused (2 GiB).
pub const DEFAULT_DISK_FREE_LIMIT_BYTES: u64 = 2_147_483_648;

/// Default relative hard memory watermark (fraction of system RAM).
pub const DEFAULT_HIGH_WATERMARK_RELATIVE: f64 = 0.6;

/// Default relative soft memory watermark (fraction of system RAM).
pub const DEFAULT_SOFT_WATERMARK_RELATIVE: f64 = 0.5;

/// Default AMQP `frame_max` offer (128 KiB).
pub const DEFAULT_FRAME_MAX: u32 = 131_072;

/// Default AMQP `channel_max` offer.
pub const DEFAULT_CHANNEL_MAX: u16 = 2047;

/// Default heartbeat interval offered to clients (seconds).
pub const DEFAULT_HEARTBEAT: u16 = 60;

/// Default maximum content body size (16 MiB).
pub const DEFAULT_MAX_MESSAGE_BYTES: u64 = 16 * 1024 * 1024;

/// Default maximum concurrent AMQP connections.
pub const DEFAULT_MAX_CONNECTIONS: u32 = 10_000;

/// Default per-queue actor mailbox capacity.
pub const DEFAULT_QUEUE_ENQUEUE_BOUND: usize = 1024;

/// Prefetch used when a client sends `basic.qos` prefetch 0 (or never sends qos).
///
/// Zero is not unlimited: an unbounded consumer mailbox would let a slow
/// consumer pull a whole queue into the connection task.
pub const DEFAULT_PREFETCH: u16 = 256;

/// Top-level QueueForge configuration.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    /// Network listeners.
    pub listeners: ListenersConfig,
    /// On-disk data layout.
    pub data: DataConfig,
    /// Memory watermark relative limits.
    pub memory: MemoryConfig,
    /// Protocol / resource limits.
    pub limits: LimitsConfig,
    /// Logging.
    pub logging: LoggingConfig,
    /// Management HTTP behavior.
    pub management: ManagementConfig,
    /// Optional TLS (AMQPS + HTTPS management).
    pub tls: TlsConfig,
    /// Optional static cluster membership. Empty means a single node.
    pub cluster: ClusterConfig,
}

/// One process in a static cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterMember {
    /// Stable node id. Queue homes are chosen from these ids.
    pub id: String,
    /// Cluster RPC address.
    pub addr: SocketAddr,
}

/// Peer list for a multi-node broker.
///
/// `members` empty (the default) keeps a single process: no cluster listener
/// and every queue actor stays local. When `members` is non-empty it includes
/// this node, `node_id` matches one entry, and `listen` is that entry's address.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ClusterConfig {
    /// This process's id. Empty when not clustered.
    pub node_id: String,
    /// RPC listen address. Required when [`Self::members`] is non-empty.
    pub listen: Option<SocketAddr>,
    /// Full membership, including this node.
    pub members: Vec<ClusterMember>,
}

impl ClusterConfig {
    /// Whether this process should peer with other nodes.
    pub fn is_enabled(&self) -> bool {
        !self.members.is_empty()
    }

    /// Node id passed to durable recovery. `None` restores every local queue.
    pub fn local_node_id(&self) -> Option<String> {
        if self.is_enabled() {
            Some(self.node_id.clone())
        } else {
            None
        }
    }

    /// Reject a membership list that does not include this node exactly once.
    pub fn validate(&self) -> Result<(), Error> {
        if self.members.is_empty() {
            return Ok(());
        }
        if self.node_id.is_empty() {
            return Err(Error::Config(
                "cluster.node_id is required when cluster.members is set".into(),
            ));
        }
        let mine: Vec<_> = self
            .members
            .iter()
            .filter(|m| m.id == self.node_id)
            .collect();
        if mine.len() != 1 {
            return Err(Error::Config(format!(
                "cluster.members must contain node_id {} exactly once",
                self.node_id
            )));
        }
        let Some(listen) = self.listen else {
            return Err(Error::Config(
                "cluster.listen is required when cluster.members is set".into(),
            ));
        };
        if mine[0].addr != listen {
            return Err(Error::Config(
                "cluster.listen must equal this node's member address".into(),
            ));
        }
        let mut ids: Vec<_> = self.members.iter().map(|m| m.id.as_str()).collect();
        ids.sort_unstable();
        if ids.windows(2).any(|w| w[0] == w[1]) {
            return Err(Error::Config(
                "cluster.members ids must be unique".into(),
            ));
        }
        Ok(())
    }
}

/// AMQP / management / metrics bind addresses.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ListenersConfig {
    /// AMQP 0-9-1 listener (default `0.0.0.0:5672`).
    pub amqp: SocketAddr,
    /// Management HTTP API (default `0.0.0.0:15672`).
    pub management: SocketAddr,
    /// Prometheus metrics (default `127.0.0.1:15692`).
    pub metrics: SocketAddr,
    /// MQTT 3.1.1 listener. Unset leaves MQTT unbound.
    #[serde(default)]
    pub mqtt: Option<SocketAddr>,
    /// STOMP 1.2 listener. Unset leaves STOMP unbound.
    #[serde(default)]
    pub stomp: Option<SocketAddr>,
    /// RabbitMQ stream listener. Unset leaves streams unbound.
    #[serde(default)]
    pub stream: Option<SocketAddr>,
}

/// WAL fsync / group-commit policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum FsyncPolicy {
    /// Never fsync (dev only; durable_done completes after buffered write).
    Never,
    /// Group-commit fsync every `fsync_interval_ms` (production default).
    /// Publisher confirms complete after the buffered write, before this fsync.
    #[default]
    EveryNMs,
    /// Fsync after every `fsync_every_n_messages` durable appends.
    EveryNMessages,
    /// Fsync after every durable append (max durability, highest latency).
    Always,
}

/// Persistence / data directory settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DataConfig {
    /// Root data directory (WAL, metadata, etc.).
    pub dir: PathBuf,
    /// When durable+persistent messages are fsynced.
    pub fsync_policy: FsyncPolicy,
    /// Interval for [`FsyncPolicy::EveryNMs`] (default 100).
    pub fsync_interval_ms: u64,
    /// Batch size for [`FsyncPolicy::EveryNMessages`] (default 1).
    pub fsync_every_n_messages: u64,
    /// Max segment file size before rotation (default 128 MiB).
    pub wal_segment_max_bytes: u64,
    /// Refuse durable publishes when free space on `dir` is below this many bytes.
    pub disk_free_limit_bytes: u64,
}

/// Memory watermark configuration (fractions of detected system RAM).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MemoryConfig {
    /// Hard watermark: block new publishes when tracked memory ≥ this × RAM.
    pub high_watermark_relative: f64,
    /// Soft watermark: raise alarm metric / log when tracked memory ≥ this × RAM.
    pub soft_watermark_relative: f64,
}

/// Protocol and process resource limits.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LimitsConfig {
    /// Server `frame_max` offer (negotiated min with client; floor 4096).
    pub frame_max: u32,
    /// Server `channel_max` offer.
    pub channel_max: u16,
    /// Heartbeat interval offered in seconds (`0` disables).
    pub heartbeat_default: u16,
    /// Maximum content body size in bytes (content-header `body-size`).
    pub max_message_bytes: u64,
    /// Maximum concurrent AMQP TCP connections (`0` = unlimited).
    pub max_connections: u32,
    /// Per-queue actor mailbox capacity (enqueue bound).
    pub queue_enqueue_bound: usize,
    /// Server prefetch when the client sets `prefetch_count` to 0.
    pub default_prefetch: u16,
    /// Queue type used when a durable non-exclusive declare omits `x-queue-type`.
    /// `classic` or `quorum`.
    pub default_queue_type: String,
}

/// Management API settings.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ManagementConfig {
    /// Reverse proxies allowed to supply `X-Forwarded-For`.
    ///
    /// Each entry is an IP or a CIDR (`10.0.0.0/8`, `2001:db8::/32`). When the
    /// TCP peer is inside this list, login rate limiting uses the left-most
    /// forwarding hop. Otherwise the peer address is used and the header is ignored.
    pub trusted_proxy_cidrs: Vec<String>,
}

/// Logging configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LoggingConfig {
    /// `RUST_LOG`-style filter (e.g. `info,queueforge=debug`).
    pub level: String,
}

/// TLS settings for AMQP and management listeners.
///
/// When [`TlsConfig::enabled`] is `true`, both the AMQP listener (AMQPS) and the
/// management HTTP API (HTTPS) terminate TLS with rustls using the configured
/// certificate and private key. Local development defaults to `enabled = false`.
///
/// See `docs/PRODUCTION_TLS.md` for the production checklist.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TlsConfig {
    /// Enable TLS for AMQP and management (default `false`).
    pub enabled: bool,
    /// Path to PEM-encoded server certificate (or full chain).
    ///
    /// Required when `enabled` is `true`.
    pub cert_path: Option<PathBuf>,
    /// Path to PEM-encoded private key (PKCS#8 or RSA).
    ///
    /// Required when `enabled` is `true`.
    pub key_path: Option<PathBuf>,
}

impl Default for ListenersConfig {
    fn default() -> Self {
        Self {
            amqp: "0.0.0.0:5672".parse().expect("valid default addr"),
            management: "0.0.0.0:15672".parse().expect("valid default addr"),
            metrics: "127.0.0.1:15692".parse().expect("valid default addr"),
            mqtt: None,
            stomp: None,
            stream: None,
        }
    }
}

impl Default for DataConfig {
    fn default() -> Self {
        Self {
            dir: PathBuf::from("./data"),
            fsync_policy: FsyncPolicy::EveryNMs,
            fsync_interval_ms: DEFAULT_FSYNC_INTERVAL_MS,
            fsync_every_n_messages: 1,
            wal_segment_max_bytes: DEFAULT_WAL_SEGMENT_MAX_BYTES,
            disk_free_limit_bytes: DEFAULT_DISK_FREE_LIMIT_BYTES,
        }
    }
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            high_watermark_relative: DEFAULT_HIGH_WATERMARK_RELATIVE,
            soft_watermark_relative: DEFAULT_SOFT_WATERMARK_RELATIVE,
        }
    }
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            frame_max: DEFAULT_FRAME_MAX,
            channel_max: DEFAULT_CHANNEL_MAX,
            heartbeat_default: DEFAULT_HEARTBEAT,
            max_message_bytes: DEFAULT_MAX_MESSAGE_BYTES,
            max_connections: DEFAULT_MAX_CONNECTIONS,
            queue_enqueue_bound: DEFAULT_QUEUE_ENQUEUE_BOUND,
            default_prefetch: DEFAULT_PREFETCH,
            default_queue_type: "classic".into(),
        }
    }
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
        }
    }
}

impl TlsConfig {
    /// Validate TLS settings.
    ///
    /// When `enabled` is false, always succeeds. When true, requires non-empty
    /// `cert_path` and `key_path` (paths themselves are checked at load time by
    /// the TLS acceptor).
    pub fn validate(&self) -> Result<(), Error> {
        if !self.enabled {
            return Ok(());
        }
        match (&self.cert_path, &self.key_path) {
            (Some(cert), Some(key))
                if !cert.as_os_str().is_empty() && !key.as_os_str().is_empty() =>
            {
                Ok(())
            }
            _ => Err(Error::Config(
                "tls.enabled=true requires non-empty cert_path and key_path".to_string(),
            )),
        }
    }

    /// Certificate path when TLS is enabled and validated.
    pub fn cert_path_required(&self) -> Result<&Path, Error> {
        self.validate()?;
        self.cert_path
            .as_deref()
            .ok_or_else(|| Error::Config("tls.cert_path is required".to_string()))
    }

    /// Private key path when TLS is enabled and validated.
    pub fn key_path_required(&self) -> Result<&Path, Error> {
        self.validate()?;
        self.key_path
            .as_deref()
            .ok_or_else(|| Error::Config("tls.key_path is required".to_string()))
    }
}

impl Config {
    /// Load configuration from a TOML file.
    pub fn load_from_file(path: &Path) -> Result<Self, Error> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| Error::Config(format!("failed to read {}: {e}", path.display())))?;
        let cfg = Self::parse_toml(&text)?;
        cfg.tls.validate()?;
        Ok(cfg)
    }

    /// Parse configuration from a TOML string.
    ///
    /// Does **not** call [`TlsConfig::validate`]; callers that load production
    /// config should validate (or use [`Self::load_from_file`]).
    pub fn parse_toml(text: &str) -> Result<Self, Error> {
        let cfg: Self =
            toml::from_str(text).map_err(|e| Error::Config(format!("invalid TOML: {e}")))?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Validate relative watermarks and limit ranges.
    pub fn validate(&self) -> Result<(), Error> {
        if !(0.0..=1.0).contains(&self.memory.high_watermark_relative) {
            return Err(Error::Config(format!(
                "memory.high_watermark_relative must be in [0, 1], got {}",
                self.memory.high_watermark_relative
            )));
        }
        if !(0.0..=1.0).contains(&self.memory.soft_watermark_relative) {
            return Err(Error::Config(format!(
                "memory.soft_watermark_relative must be in [0, 1], got {}",
                self.memory.soft_watermark_relative
            )));
        }
        if self.memory.soft_watermark_relative > self.memory.high_watermark_relative {
            return Err(Error::Config(format!(
                "memory.soft_watermark_relative ({}) must be ≤ high_watermark_relative ({})",
                self.memory.soft_watermark_relative, self.memory.high_watermark_relative
            )));
        }
        if self.limits.frame_max != 0 && self.limits.frame_max < 4096 {
            return Err(Error::Config(format!(
                "limits.frame_max must be 0 or ≥ 4096, got {}",
                self.limits.frame_max
            )));
        }
        if self.limits.max_message_bytes == 0 {
            return Err(Error::Config("limits.max_message_bytes must be > 0".into()));
        }
        if self.limits.queue_enqueue_bound == 0 {
            return Err(Error::Config(
                "limits.queue_enqueue_bound must be > 0".into(),
            ));
        }
        self.cluster.validate()?;
        Ok(())
    }

    /// Apply environment variable overrides (`QUEUEFORGE_*`).
    ///
    /// Returns an error if an override is present but invalid (e.g. bad socket address).
    pub fn apply_env_overrides(&mut self) -> Result<(), Error> {
        if let Ok(v) = std::env::var("QUEUEFORGE_DATA_DIR") {
            self.data.dir = PathBuf::from(v);
        }
        if let Ok(v) = std::env::var("QUEUEFORGE_AMQP_ADDR") {
            self.listeners.amqp = parse_socket_override("QUEUEFORGE_AMQP_ADDR", &v)?;
        }
        if let Ok(v) = std::env::var("QUEUEFORGE_MGMT_ADDR") {
            self.listeners.management = parse_socket_override("QUEUEFORGE_MGMT_ADDR", &v)?;
        }
        if let Ok(v) = std::env::var("QUEUEFORGE_LOG") {
            self.logging.level = v;
        }
        if let Ok(v) = std::env::var("QUEUEFORGE_TLS_ENABLED") {
            self.tls.enabled = parse_bool_override("QUEUEFORGE_TLS_ENABLED", &v)?;
        }
        if let Ok(v) = std::env::var("QUEUEFORGE_TLS_CERT") {
            self.tls.cert_path = Some(PathBuf::from(v));
        }
        if let Ok(v) = std::env::var("QUEUEFORGE_TLS_KEY") {
            self.tls.key_path = Some(PathBuf::from(v));
        }
        Ok(())
    }
}

fn parse_bool_override(name: &str, value: &str) -> Result<bool, Error> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(Error::Config(format!(
            "invalid {name} value {value:?}: expected true/false"
        ))),
    }
}

fn parse_socket_override(name: &str, value: &str) -> Result<SocketAddr, Error> {
    value.parse().map_err(|e| {
        Error::Config(format!(
            "invalid {name} value {value:?}: {e} (expected host:port)"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_parses_empty_toml() {
        let cfg = Config::parse_toml("").unwrap();
        assert_eq!(cfg.listeners.amqp.port(), 5672);
        assert_eq!(cfg.listeners.management.port(), 15672);
        // Metrics must default to loopback (unauthenticated scrape).
        let metrics_default: SocketAddr = "127.0.0.1:15692".parse().unwrap();
        assert_eq!(cfg.listeners.metrics, metrics_default);
        assert!(cfg.listeners.metrics.ip().is_loopback());
        assert_eq!(cfg.logging.level, "info");
    }

    #[test]
    fn example_toml_shape() {
        let text = r#"
[listeners]
amqp = "0.0.0.0:5672"
management = "0.0.0.0:15672"
metrics = "127.0.0.1:15692"

[data]
dir = "/var/lib/queueforge"
fsync_policy = "every_n_ms"
fsync_interval_ms = 100
wal_segment_max_bytes = 134217728
disk_free_limit_bytes = 2147483648

[memory]
high_watermark_relative = 0.6
soft_watermark_relative = 0.5

[limits]
frame_max = 131072
channel_max = 2047
heartbeat_default = 60
max_message_bytes = 16777216
max_connections = 10000
queue_enqueue_bound = 1024

[logging]
level = "info,queueforge=debug"
"#;
        let cfg = Config::parse_toml(text).unwrap();
        assert_eq!(cfg.data.dir, PathBuf::from("/var/lib/queueforge"));
        assert_eq!(cfg.data.fsync_policy, FsyncPolicy::EveryNMs);
        assert_eq!(cfg.data.fsync_interval_ms, 100);
        assert_eq!(cfg.data.wal_segment_max_bytes, 134_217_728);
        assert_eq!(
            cfg.data.disk_free_limit_bytes,
            DEFAULT_DISK_FREE_LIMIT_BYTES
        );
        assert!((cfg.memory.high_watermark_relative - 0.6).abs() < f64::EPSILON);
        assert!((cfg.memory.soft_watermark_relative - 0.5).abs() < f64::EPSILON);
        assert_eq!(cfg.limits.max_message_bytes, DEFAULT_MAX_MESSAGE_BYTES);
        assert_eq!(cfg.limits.max_connections, DEFAULT_MAX_CONNECTIONS);
        assert_eq!(cfg.logging.level, "info,queueforge=debug");
        let metrics_expected: SocketAddr = "127.0.0.1:15692".parse().unwrap();
        assert_eq!(cfg.listeners.metrics, metrics_expected);
        assert!(cfg.listeners.metrics.ip().is_loopback());
    }

    #[test]
    fn soft_above_hard_is_rejected() {
        let text = r#"
[memory]
high_watermark_relative = 0.4
soft_watermark_relative = 0.5
"#;
        let err = Config::parse_toml(text).unwrap_err();
        assert!(
            err.to_string().contains("soft_watermark_relative"),
            "unexpected: {err}"
        );
    }

    #[test]
    fn default_fsync_is_group_commit() {
        let cfg = Config::default();
        assert_eq!(cfg.data.fsync_policy, FsyncPolicy::EveryNMs);
        assert_eq!(cfg.data.fsync_interval_ms, DEFAULT_FSYNC_INTERVAL_MS);
        assert_eq!(
            cfg.data.wal_segment_max_bytes,
            DEFAULT_WAL_SEGMENT_MAX_BYTES
        );
    }

    #[test]
    fn invalid_toml_syntax_errors() {
        let err = Config::parse_toml("listeners = [").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("invalid TOML") || msg.contains("config error"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn invalid_socket_address_errors() {
        let text = r#"
[listeners]
amqp = "not-a-socket"
"#;
        let err = Config::parse_toml(text).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("invalid") || msg.contains("socket"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn unknown_field_is_rejected() {
        let text = r#"
[listeners]
amq = "0.0.0.0:5672"
"#;
        let err = Config::parse_toml(text).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("unknown field") || msg.contains("invalid TOML"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn load_from_missing_file_errors() {
        let path = PathBuf::from("/tmp/queueforge-definitely-missing-config-9f3a.toml");
        let err = Config::load_from_file(&path).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("failed to read") || msg.contains("No such file"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn parse_socket_override_accepts_valid() {
        let addr = parse_socket_override("QUEUEFORGE_AMQP_ADDR", "127.0.0.1:15672").unwrap();
        assert_eq!(addr.port(), 15672);
    }

    #[test]
    fn parse_socket_override_rejects_invalid() {
        let err = parse_socket_override("QUEUEFORGE_AMQP_ADDR", "not-valid").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("QUEUEFORGE_AMQP_ADDR"),
            "unexpected error: {msg}"
        );
        assert!(msg.contains("not-valid"), "unexpected error: {msg}");
    }

    #[test]
    fn default_tls_is_disabled() {
        let cfg = Config::default();
        assert!(!cfg.tls.enabled);
        assert!(cfg.tls.cert_path.is_none());
        assert!(cfg.tls.key_path.is_none());
        cfg.tls.validate().expect("disabled TLS is always valid");
    }

    #[test]
    fn tls_config_parses_from_toml() {
        let text = r#"
[tls]
enabled = true
cert_path = "/etc/queueforge/tls/server.crt"
key_path = "/etc/queueforge/tls/server.key"
"#;
        let cfg = Config::parse_toml(text).unwrap();
        assert!(cfg.tls.enabled);
        assert_eq!(
            cfg.tls.cert_path.as_deref(),
            Some(Path::new("/etc/queueforge/tls/server.crt"))
        );
        assert_eq!(
            cfg.tls.key_path.as_deref(),
            Some(Path::new("/etc/queueforge/tls/server.key"))
        );
        cfg.tls.validate().expect("paths present");
    }

    #[test]
    fn tls_enabled_without_paths_fails_validate() {
        let text = r#"
[tls]
enabled = true
"#;
        let cfg = Config::parse_toml(text).unwrap();
        let err = cfg.tls.validate().unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("cert_path") && msg.contains("key_path"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn tls_enabled_empty_paths_fails_validate() {
        let cfg = TlsConfig {
            enabled: true,
            cert_path: Some(PathBuf::from("")),
            key_path: Some(PathBuf::from("/tmp/key.pem")),
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn load_from_file_rejects_enabled_tls_without_paths() {
        let dir = std::env::temp_dir();
        let path = dir.join("queueforge-tls-config-test-missing-paths.toml");
        std::fs::write(
            &path,
            r#"
[tls]
enabled = true
"#,
        )
        .unwrap();
        let err = Config::load_from_file(&path).unwrap_err();
        let _ = std::fs::remove_file(&path);
        let msg = err.to_string();
        assert!(
            msg.contains("cert_path") || msg.contains("key_path") || msg.contains("tls"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn load_from_file_accepts_valid_tls_shape() {
        let dir = std::env::temp_dir();
        let path = dir.join("queueforge-tls-config-test-valid.toml");
        std::fs::write(
            &path,
            r#"
[listeners]
amqp = "0.0.0.0:5671"
management = "0.0.0.0:15671"

[tls]
enabled = true
cert_path = "/etc/queueforge/tls/server.crt"
key_path = "/etc/queueforge/tls/server.key"
"#,
        )
        .unwrap();
        let cfg = Config::load_from_file(&path).expect("valid TLS config should load");
        let _ = std::fs::remove_file(&path);
        assert!(cfg.tls.enabled);
        assert_eq!(cfg.listeners.amqp.port(), 5671);
        assert_eq!(cfg.listeners.management.port(), 15671);
    }

    #[test]
    fn parse_bool_override_accepts_common_forms() {
        assert!(parse_bool_override("QUEUEFORGE_TLS_ENABLED", "true").unwrap());
        assert!(parse_bool_override("QUEUEFORGE_TLS_ENABLED", "1").unwrap());
        assert!(!parse_bool_override("QUEUEFORGE_TLS_ENABLED", "false").unwrap());
        assert!(!parse_bool_override("QUEUEFORGE_TLS_ENABLED", "off").unwrap());
        assert!(parse_bool_override("QUEUEFORGE_TLS_ENABLED", "maybe").is_err());
    }
}
