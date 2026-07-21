//! Prometheus metrics registry and process health HTTP endpoints for QueueForge.
//!
//! Serves on the metrics bind address (default `127.0.0.1:15692`):
//! - `GET /metrics` — Prometheus text exposition
//! - `GET /healthz` — always `200` while the process is up
//! - `GET /readyz` — `200` only after [`ReadyFlag`] is set; otherwise `503`
//!
//! Application metric names use the `queueforge_` prefix. Process metrics use
//! the same prefix (e.g. `queueforge_process_start_time_seconds`).
//!
//! # Probe placement
//!
//! For now `/healthz` and `/readyz` are collocated with Prometheus scrape on
//! the metrics bind (localhost by default). Remote kubelet-style probes that
//! target the pod IP will not reach these endpoints unless operators rebind
//! `listeners.metrics` (which also exposes unauthenticated `/metrics` on that
//! interface — protect with network policy). When the management HTTP surface
//! lands, health routes will also be available there.

#![deny(missing_docs)]

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use metrics_process::Collector as ProcessCollector;
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tracing::info;

/// Errors from metrics installation or the health/metrics listener.
#[derive(Debug, Error)]
pub enum Error {
    /// Failed to install the global Prometheus metrics recorder.
    #[error("failed to install metrics recorder: {0}")]
    Install(String),
    /// Failed to bind the metrics/health HTTP listener.
    #[error("failed to bind metrics listener on {addr}: {source}")]
    Bind {
        /// Address that failed to bind.
        addr: SocketAddr,
        /// Underlying I/O error.
        #[source]
        source: std::io::Error,
    },
}

/// Shared readiness flag used by `/readyz` and the broker lifecycle.
///
/// Starts as **not ready** (`false`). The broker sets it to `true` after
/// recovery completes (and back to `false` during graceful shutdown).
#[derive(Debug, Clone, Default)]
pub struct ReadyFlag {
    ready: Arc<AtomicBool>,
}

impl ReadyFlag {
    /// Create a new flag in the not-ready state.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set whether the process is ready to serve traffic.
    pub fn set_ready(&self, ready: bool) {
        self.ready.store(ready, Ordering::Release);
    }

    /// Returns `true` when the process is ready.
    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }
}

/// Handle to a running metrics/health HTTP server.
pub struct MetricsServer {
    /// Shared readiness flag for this process.
    pub ready: ReadyFlag,
    /// Local address the server is bound to.
    pub local_addr: SocketAddr,
    serve_join: JoinHandle<()>,
    upkeep_join: JoinHandle<()>,
}

impl MetricsServer {
    /// Abort the HTTP accept loop and the recorder upkeep task.
    pub fn abort(self) {
        self.serve_join.abort();
        self.upkeep_join.abort();
    }
}

#[derive(Clone)]
struct AppState {
    handle: PrometheusHandle,
    ready: ReadyFlag,
    process: Arc<ProcessCollector>,
}

/// Install the global Prometheus metrics recorder.
///
/// Must be called at most once per process. Describes process metrics under the
/// `queueforge_` prefix.
pub fn install_recorder() -> Result<PrometheusHandle, Error> {
    let handle = PrometheusBuilder::new()
        .install_recorder()
        .map_err(|e| Error::Install(e.to_string()))?;

    // Describe process metrics once; values are refreshed on each /metrics scrape.
    ProcessCollector::new("queueforge_").describe();

    Ok(handle)
}

/// Bind `addr` and serve `/metrics`, `/healthz`, and `/readyz` in the background.
///
/// Also spawns a periodic upkeep task for the Prometheus recorder (owned by the
/// returned [`MetricsServer`] and aborted with it).
pub async fn start_server(
    addr: SocketAddr,
    handle: PrometheusHandle,
    ready: ReadyFlag,
) -> Result<MetricsServer, Error> {
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|source| Error::Bind { addr, source })?;
    let local_addr = listener
        .local_addr()
        .map_err(|source| Error::Bind { addr, source })?;

    // Recorder upkeep (idle metric eviction, etc.).
    let upkeep_handle = handle.clone();
    let upkeep_join = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            upkeep_handle.run_upkeep();
        }
    });

    let state = AppState {
        handle,
        ready: ready.clone(),
        process: Arc::new(ProcessCollector::new("queueforge_")),
    };

    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/healthz", get(healthz_handler))
        .route("/readyz", get(readyz_handler))
        .with_state(state);

    info!(%local_addr, "metrics and health listener started");

    let serve_join = tokio::spawn(async move {
        if let Err(err) = axum::serve(listener, app).await {
            tracing::error!(error = %err, "metrics/health server exited with error");
        }
    });

    Ok(MetricsServer {
        ready,
        local_addr,
        serve_join,
        upkeep_join,
    })
}

async fn healthz_handler() -> impl IntoResponse {
    (StatusCode::OK, "ok\n")
}

async fn readyz_handler(State(state): State<AppState>) -> Response {
    if state.ready.is_ready() {
        (StatusCode::OK, "ready\n").into_response()
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "not ready\n").into_response()
    }
}

async fn metrics_handler(State(state): State<AppState>) -> impl IntoResponse {
    // Refresh process gauges/counters immediately before rendering.
    state.process.collect();
    let body = state.handle.render();
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        body,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use std::sync::OnceLock;
    use tower::ServiceExt;

    fn test_handle() -> PrometheusHandle {
        static HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();
        HANDLE
            .get_or_init(|| install_recorder().expect("install metrics recorder for tests"))
            .clone()
    }

    fn test_app(ready: ReadyFlag) -> Router {
        let state = AppState {
            handle: test_handle(),
            ready,
            process: Arc::new(ProcessCollector::new("queueforge_")),
        };
        Router::new()
            .route("/metrics", get(metrics_handler))
            .route("/healthz", get(healthz_handler))
            .route("/readyz", get(readyz_handler))
            .with_state(state)
    }

    #[test]
    fn ready_flag_defaults_to_not_ready() {
        let flag = ReadyFlag::new();
        assert!(!flag.is_ready());
        flag.set_ready(true);
        assert!(flag.is_ready());
        flag.set_ready(false);
        assert!(!flag.is_ready());
    }

    async fn response_body(response: axum::response::Response) -> String {
        let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    #[tokio::test]
    async fn healthz_always_ok() {
        let app = test_app(ReadyFlag::new());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response_body(response).await, "ok\n");
    }

    #[tokio::test]
    async fn readyz_503_until_ready() {
        let ready = ReadyFlag::new();
        let app = test_app(ready.clone());

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/readyz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response_body(response).await, "not ready\n");

        ready.set_ready(true);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/readyz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response_body(response).await, "ready\n");
    }

    #[tokio::test]
    async fn metrics_exposes_process_series() {
        let app = test_app(ReadyFlag::new());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let content_type = response
            .headers()
            .get(axum::http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        assert!(
            content_type.contains("text/plain"),
            "unexpected content-type: {content_type}"
        );

        let text = response_body(response).await;
        assert!(
            text.contains("queueforge_process_start_time_seconds")
                || text.contains("queueforge_process_resident_memory_bytes"),
            "expected process metrics in scrape body, got:\n{text}"
        );
    }

    #[tokio::test]
    async fn bind_server_and_scrape() {
        let ready = ReadyFlag::new();
        let handle = test_handle();
        let server = start_server("127.0.0.1:0".parse().unwrap(), handle, ready.clone())
            .await
            .expect("start server");

        let addr = server.local_addr;
        let url = format!("http://{addr}/healthz");
        let client = reqwest::Client::new();
        let resp = client.get(&url).send().await.expect("GET /healthz");
        assert_eq!(resp.status(), reqwest::StatusCode::OK);

        let resp = client
            .get(format!("http://{addr}/readyz"))
            .send()
            .await
            .expect("GET /readyz");
        assert_eq!(resp.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);

        ready.set_ready(true);
        let resp = client
            .get(format!("http://{addr}/readyz"))
            .send()
            .await
            .expect("GET /readyz after ready");
        assert_eq!(resp.status(), reqwest::StatusCode::OK);

        let resp = client
            .get(format!("http://{addr}/metrics"))
            .send()
            .await
            .expect("GET /metrics");
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
        let text = resp.text().await.unwrap();
        assert!(text.contains("queueforge_process_"), "metrics body: {text}");

        server.abort();
    }
}
