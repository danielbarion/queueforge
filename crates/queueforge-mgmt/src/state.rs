//! Shared management application state.

use std::sync::Arc;

use queueforge_core::{ExchangeRouter, QueueRegistry};
use queueforge_metrics::ReadyFlag;
use queueforge_store::MetadataStore;

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
}

impl Default for MgmtConfig {
    fn default() -> Self {
        Self {
            cookie_secure: false,
            product_version: env!("CARGO_PKG_VERSION").to_string(),
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
        }
    }
}
