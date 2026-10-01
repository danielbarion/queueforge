//! Management HTTP API for QueueForge.
//!
//! Serves on the management bind address (default `0.0.0.0:15672`):
//! - Session cookie auth (`POST /api/login`, `POST /api/logout`, `GET /api/whoami`)
//! - Resource CRUD: vhosts, users, permissions, queues, exchanges, bindings
//! - Definitions export/import (`GET|POST /api/definitions`)
//! - Test publish/get (`POST .../publish`, `POST .../get`)
//! - Health: `GET /healthz`, `GET /readyz`
//! - Embedded React SPA at `/` (assets from `ui/dist` via `rust-embed`)
//!
//! When a rustls [`ServerConfig`] is supplied, the listener terminates TLS
//! (HTTPS). Session cookies then set the `Secure` flag via [`MgmtConfig`].
//!
//! Vhost path segments are URL-encoded (`/` → `%2F`). List endpoints use
//! cursor pagination (`page_size`, `cursor`, `name_prefix`).
//!
//! Login rate limiting keys off the real TCP peer (`ConnectInfo`), never
//! client-controlled `X-Forwarded-For` headers.
//!
//! Mutations use the same configure/write/read predicates as AMQP; user
//! administration and definitions import require the `administrator` tag.

#![deny(missing_docs)]

mod authz;
mod connections;
mod console;
mod definitions;
mod error;
mod mutations;
mod pagination;
mod routes;
mod session;
mod spa;
mod state;

pub use connections::{ChannelInfo, ConnectionInfo, ConnectionTracker, ConsumerInfo, TopicPermission};
pub use error::{MgmtError, Result};
pub use session::{SessionStore, SESSION_COOKIE_NAME};
pub use state::{MgmtConfig, MgmtState};

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use axum_server::tls_rustls::RustlsConfig;
use axum_server::Handle as ServerHandle;
use rustls::ServerConfig;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tracing::info;

/// Handle to a running management HTTP(S) server.
pub struct MgmtServer {
    /// Local address the server is bound to.
    pub local_addr: SocketAddr,
    /// Whether the listener terminates TLS.
    pub tls: bool,
    serve_join: JoinHandle<()>,
    /// Optional axum-server handle used to shut down the TLS acceptor cleanly.
    tls_handle: Option<ServerHandle>,
}

impl MgmtServer {
    /// Abort / shut down the HTTP(S) accept loop.
    pub fn abort(self) {
        if let Some(handle) = self.tls_handle {
            handle.shutdown();
        }
        self.serve_join.abort();
    }
}

/// Build the management Axum router (useful for unit tests).
///
/// For oneshot tests that hit `/api/login`, layer
/// [`axum::extract::connect_info::MockConnectInfo`] so `ConnectInfo` is available,
/// or insert `ConnectInfo` into request extensions.
pub fn router(state: MgmtState) -> Router {
    routes::router(state)
}

/// Bind `addr` and serve the management API in the background.
///
/// When `tls` is `None`, serves plain HTTP via [`axum::serve`].
/// When `Some`, terminates TLS with rustls (HTTPS) via `axum-server`.
///
/// Uses [`Router::into_make_service_with_connect_info`] so login rate limiting
/// sees the real peer `SocketAddr`.
pub async fn start_server(
    addr: SocketAddr,
    state: MgmtState,
    tls: Option<Arc<ServerConfig>>,
) -> std::io::Result<MgmtServer> {
    let app = router(state);
    let tls_enabled = tls.is_some();

    match tls {
        None => {
            let listener = TcpListener::bind(addr).await?;
            let local_addr = listener.local_addr()?;
            info!(%local_addr, tls = false, "management HTTP listener started");

            let serve_join = tokio::spawn(async move {
                if let Err(err) = axum::serve(
                    listener,
                    app.into_make_service_with_connect_info::<SocketAddr>(),
                )
                .await
                {
                    tracing::error!(error = %err, "management server exited with error");
                }
            });

            Ok(MgmtServer {
                local_addr,
                tls: false,
                serve_join,
                tls_handle: None,
            })
        }
        Some(server_config) => {
            // Bind first so we can report the actual port (including port 0).
            let std_listener = std::net::TcpListener::bind(addr)?;
            std_listener.set_nonblocking(true)?;
            let local_addr = std_listener.local_addr()?;

            // `RustlsConfig::from_config` does not inject ALPN. Shared PEM load
            // leaves ALPN empty for AMQPS; ensure HTTPS advertises h2 + http/1.1.
            let https_config = ensure_https_alpn(server_config);
            let rustls_config = RustlsConfig::from_config(https_config);
            let handle = ServerHandle::new();
            let handle_for_task = handle.clone();

            info!(%local_addr, tls = true, "management HTTPS listener started");

            let serve_join = tokio::spawn(async move {
                if let Err(err) = axum_server::from_tcp_rustls(std_listener, rustls_config)
                    .handle(handle_for_task)
                    .serve(app.into_make_service_with_connect_info::<SocketAddr>())
                    .await
                {
                    tracing::error!(error = %err, "management HTTPS server exited with error");
                }
            });

            Ok(MgmtServer {
                local_addr,
                tls: tls_enabled,
                serve_join,
                tls_handle: Some(handle),
            })
        }
    }
}

/// Ensure a rustls server config advertises HTTP ALPN for management HTTPS.
///
/// Clones when ALPN is empty so an AMQPS-oriented shared config is not mutated.
fn ensure_https_alpn(config: Arc<ServerConfig>) -> Arc<ServerConfig> {
    if !config.alpn_protocols.is_empty() {
        return config;
    }
    let mut cfg = (*config).clone();
    cfg.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Arc::new(cfg)
}
