//! Shared management application state.

use std::sync::Arc;

use queueforge_core::{ExchangeRouter, QueueRegistry};
use queueforge_metrics::ReadyFlag;
use queueforge_store::MetadataStore;
use tokio::sync::{mpsc, oneshot};

/// One metadata change that must be applied on every peer before the caller returns.
pub struct ReplicateReq {
    /// Record kind (`user`, `permission`, `vhost`, and their deletes).
    pub kind: String,
    /// JSON body the peer applies.
    pub payload: serde_json::Value,
    /// Completed after peers have applied the record.
    pub done: oneshot::Sender<()>,
}

use crate::connections::ConnectionTracker;
use crate::session::SessionStore;

/// Runtime configuration for the management HTTP surface.
#[derive(Debug, Clone)]
pub struct MgmtConfig {
    /// When true, session cookies are marked `Secure` (TLS).
    ///
    /// Dev default is `false` so browsers accept cookies over plain HTTP.
    pub cookie_secure: bool,
    /// Product version string for `/api/overview`.
    pub product_version: String,
    /// CIDRs of reverse proxies whose `X-Forwarded-For` may be trusted.
    ///
    /// Empty means the TCP peer is the client. A spoofed forwarding header from
    /// any other peer is ignored.
    pub trusted_proxy_cidrs: Vec<String>,
}

impl Default for MgmtConfig {
    fn default() -> Self {
        Self {
            cookie_secure: false,
            product_version: env!("CARGO_PKG_VERSION").to_string(),
            trusted_proxy_cidrs: Vec::new(),
        }
    }
}

/// Shared state for management handlers.
#[derive(Clone)]
pub struct MgmtState {
    /// Metadata store (vhosts, exchanges, users, durable queues).
    pub store: Arc<MetadataStore>,
    /// Live queue registry.
    pub queues: Arc<QueueRegistry>,
    /// Live exchange + binding router (shared with AMQP).
    pub router: Arc<ExchangeRouter>,
    /// Server-side sessions.
    pub sessions: Arc<SessionStore>,
    /// Live AMQP connections.
    pub connections: Arc<ConnectionTracker>,
    /// Process readiness (for `/readyz`).
    pub ready: ReadyFlag,
    /// Cookie / product settings.
    pub config: MgmtConfig,
    /// When set, user/vhost/permission writes are pushed to cluster peers.
    pub replicate_tx: Option<mpsc::UnboundedSender<ReplicateReq>>,
    /// Prometheus text for `GET /metrics` on the management listener.
    pub metrics_text: Option<Arc<dyn Fn() -> String + Send + Sync>>,
}

impl MgmtState {
    /// Build state from shared broker components.
    pub fn new(
        store: Arc<MetadataStore>,
        queues: Arc<QueueRegistry>,
        router: Arc<ExchangeRouter>,
        connections: Arc<ConnectionTracker>,
        ready: ReadyFlag,
        config: MgmtConfig,
    ) -> Self {
        Self {
            store,
            queues,
            router,
            sessions: SessionStore::shared(),
            connections,
            ready,
            config,
            replicate_tx: None,
            metrics_text: None,
        }
    }

    /// Serve this scrape body at `GET /metrics` on the management listener.
    pub fn with_metrics_text(mut self, render: Arc<dyn Fn() -> String + Send + Sync>) -> Self {
        self.metrics_text = Some(render);
        self
    }

    /// Attach the cluster fan-out used after user, permission, and vhost writes.
    pub fn with_replicator(mut self, tx: mpsc::UnboundedSender<ReplicateReq>) -> Self {
        self.replicate_tx = Some(tx);
        self
    }
}
