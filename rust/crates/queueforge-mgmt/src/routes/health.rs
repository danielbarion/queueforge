//! Health, readiness, metrics scrape, and the overview payload.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use axum::extract::State;
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;

use super::catalog::query_stats;
use super::db;
use super::{MessageStats, ObjectTotals, OverviewResponse, QueueTotals};
use crate::authz::require_session;
use crate::error::MgmtError;
use crate::state::MgmtState;

/// Liveness response. Takes no arguments. Returns HTTP 200 and the text `ok`. It does not check the store.
pub(super) async fn healthz() -> impl IntoResponse {
    (StatusCode::OK, "ok\n")
}

/// Which implementation is serving this management port. Public, so a console can tell Rust, Bun, and PHP apart before login.
pub(super) async fn identity() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "product_name": "QueueForge",
        "kind": "rust",
    }))
}

/// `state` is the management process. Returns 200 when this node may serve traffic, including the optional client-CIDR check. A disallowed peer address returns 503.
pub(super) async fn readyz(State(state): State<MgmtState>) -> Response {
    if state.ready.is_ready() {
        (StatusCode::OK, "ready\n").into_response()
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "not ready\n").into_response()
    }
}

/// Report whether `ip` is inside any entry of `cidrs`. Returns true when the list is empty. A malformed CIDR does not match.
pub(super) fn peer_in_cidrs(ip: IpAddr, cidrs: &[String]) -> bool {
    cidrs.iter().any(|c| ip_in_cidr(ip, c))
}

/// Report whether `ip` is inside `cidr`. Returns false when `cidr` is not an IP prefix this process understands.
pub(super) fn ip_in_cidr(ip: IpAddr, cidr: &str) -> bool {
    let cidr = cidr.trim();
    if let Some((addr, prefix)) = cidr.split_once('/') {
        let Ok(bits) = prefix.parse::<u8>() else {
            return false;
        };
        match (ip, addr.parse::<IpAddr>()) {
            (IpAddr::V4(ip), Ok(IpAddr::V4(base))) => ipv4_in_prefix(ip, base, bits),
            (IpAddr::V6(ip), Ok(IpAddr::V6(base))) => ipv6_in_prefix(ip, base, bits),
            _ => false,
        }
    } else {
        cidr.parse::<IpAddr>().ok() == Some(ip)
    }
}

/// Report whether IPv4 `ip` is inside `base`/`bits`. Returns false when `bits` is greater than 32.
pub(super) fn ipv4_in_prefix(ip: Ipv4Addr, base: Ipv4Addr, bits: u8) -> bool {
    if bits > 32 {
        return false;
    }
    let mask = if bits == 0 {
        0u32
    } else {
        u32::MAX << (32 - bits)
    };
    (u32::from(ip) & mask) == (u32::from(base) & mask)
}

/// Report whether IPv6 `ip` is inside `base`/`bits`. Returns false when `bits` is greater than 128.
pub(super) fn ipv6_in_prefix(ip: Ipv6Addr, base: Ipv6Addr, bits: u8) -> bool {
    if bits > 128 {
        return false;
    }
    let mask = if bits == 0 {
        0u128
    } else {
        u128::MAX << (128 - bits)
    };
    (u128::from(ip) & mask) == (u128::from(base) & mask)
}

/// Return counts for the management overview page from `state`. Queue message totals come from live actors. A queue whose actor is down counts as empty rather than failing the page.
pub(super) async fn overview(
    State(state): State<MgmtState>,
    headers: HeaderMap,
) -> Result<Json<OverviewResponse>, MgmtError> {
    let _session = require_session(&state, &headers).await?;

    let (vhosts, exchange_count) = db(&state, |s| {
        let vhosts = s.list_vhosts()?;
        let mut exchange_count = 0u64;
        for vh in &vhosts {
            exchange_count += s.list_exchanges(vh.name.as_str())?.len() as u64;
        }
        Ok((vhosts, exchange_count))
    })
    .await?;

    let connections = state.connections.list();
    let channel_count: u64 = connections.iter().map(|c| c.channels as u64).sum();

    let mut messages_ready = 0u64;
    let mut messages_unacked = 0u64;
    let mut consumers = 0u64;
    for key in state.queues.list_keys() {
        if let Some(handle) = state.queues.get(&key) {
            if let Some(stats) = query_stats(&handle.tx).await {
                messages_ready += stats.messages_ready as u64;
                messages_unacked += stats.messages_unacked as u64;
                consumers += stats.consumer_count as u64;
            }
        }
    }

    let traffic = queueforge_core::prom::traffic_totals();
    Ok(Json(OverviewResponse {
        product_name: "QueueForge",
        product_version: state.config.product_version.clone(),
        management_version: state.config.product_version.clone(),
        rabbitmq_version_compat: "0.9.1",
        object_totals: ObjectTotals {
            connections: connections.len() as u64,
            channels: channel_count,
            queues: state.queues.len() as u64,
            exchanges: exchange_count,
            consumers,
            vhosts: vhosts.len() as u64,
        },
        queue_totals: QueueTotals {
            messages: messages_ready + messages_unacked,
            messages_ready,
            messages_unacknowledged: messages_unacked,
        },
        message_stats: MessageStats {
            publish: traffic.publish,
            deliver: traffic.deliver,
            ack: traffic.ack,
        },
    }))
}

/// Render Prometheus text from `state`. Returns the scrape body. This route is not behind the management session.
pub(super) async fn metrics_scrape(State(state): State<MgmtState>) -> Response {
    let Some(render) = state.metrics_text else {
        return (StatusCode::NOT_FOUND, "metrics are not installed\n").into_response();
    };
    (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        render(),
    )
        .into_response()
}
