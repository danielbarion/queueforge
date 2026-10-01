//! Axum route handlers for the management HTTP API.
//! Health, session, catalog, and vhost decoding live in sibling modules.

use std::net::SocketAddr;

use axum::http::header;
use axum::response::Response;
use axum::routing::{delete, get, post, put};
use axum::Router;
use serde::{Deserialize, Serialize};

use crate::definitions::{export_definitions, import_definitions};
use crate::error::MgmtError;
use crate::mutations::{
    create_binding, delete_binding, delete_exchange, delete_permission, delete_policy,
    delete_queue, delete_user, delete_vhost, get_messages, list_bindings, list_exchange_bindings,
    list_permissions, list_policies, list_policies_vhost, list_queue_bindings, list_users, publish,
    purge_queue, put_exchange, put_permission, put_policy, put_queue, put_user, put_vhost,
};
use crate::state::MgmtState;

async fn db<T, F>(state: &MgmtState, f: F) -> Result<T, MgmtError>
where
    T: Send + 'static,
    F: FnOnce(&queueforge_store::MetadataStore) -> queueforge_store::Result<T> + Send + 'static,
{
    queueforge_store::MetadataStore::blocking(std::sync::Arc::clone(&state.store), f)
        .await
        .map_err(MgmtError::from)
}

/// Build the full management router.
pub fn router(state: MgmtState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics_scrape))
        .route("/api/login", post(login))
        .route("/api/logout", post(logout))
        .route("/api/whoami", get(whoami))
        .route("/api/overview", get(overview))
        .route("/api/vhosts", get(list_vhosts))
        .route("/api/vhosts/{vhost}", put(put_vhost).delete(delete_vhost))
        .route("/api/users", get(list_users))
        .route("/api/users/{name}", put(put_user).delete(delete_user))
        .route("/api/permissions", get(list_permissions))
        .route(
            "/api/permissions/{user}/{vhost}",
            put(put_permission).delete(delete_permission),
        )
        .route("/api/queues/{vhost}", get(list_queues))
        .route("/api/queues/{vhost}/{name}/purge", post(purge_queue))
        .route("/api/queues/{vhost}/{name}/get", post(get_messages))
        .route(
            "/api/queues/{vhost}/{name}/bindings",
            get(list_queue_bindings),
        )
        .route("/api/exchanges/{vhost}", get(list_exchanges))
        .route(
            "/api/exchanges/{vhost}/{name}",
            get(get_exchange).put(put_exchange).delete(delete_exchange),
        )
        .route(
            "/api/exchanges/{vhost}/{name}/bindings",
            get(list_exchange_bindings),
        )
        .route("/api/exchanges/{vhost}/{name}/publish", post(publish))
        .route(
            "/api/bindings/{vhost}",
            get(list_bindings).post(create_binding),
        )
        .route(
            "/api/bindings/{vhost}/{exchange}/{queue}/{routing_key}",
            delete(delete_binding),
        )
        .route("/api/connections", get(list_connections))
        .route(
            "/api/connections/{name}",
            get(crate::console::get_connection).delete(delete_connection),
        )
        .route("/api/channels", get(crate::console::list_channels))
        .route("/api/channels/{name}", get(crate::console::get_channel))
        .route(
            "/api/consumers/{vhost}",
            get(crate::console::list_consumers),
        )
        .route(
            "/api/queues/{vhost}/{name}",
            get(crate::console::get_queue)
                .put(put_queue)
                .delete(delete_queue),
        )
        .route(
            "/api/topic-permissions",
            get(crate::console::list_topic_permissions),
        )
        .route(
            "/api/topic-permissions/{user}/{vhost}",
            put(crate::console::put_topic_permission),
        )
        .route(
            "/api/topic-permissions/{user}/{vhost}/{exchange}",
            delete(crate::console::delete_topic_permission),
        )
        .route("/api/limits", get(crate::console::list_limits))
        .route(
            "/api/user-limits/{user}/{kind}",
            put(crate::console::put_user_limit).delete(crate::console::delete_user_limit),
        )
        .route(
            "/api/vhost-limits/{vhost}/{kind}",
            put(crate::console::put_vhost_limit).delete(crate::console::delete_vhost_limit),
        )
        .route(
            "/api/feature-flags",
            get(crate::console::list_feature_flags),
        )
        .route(
            "/api/feature-flags/{name}/enable",
            post(crate::console::enable_feature_flag),
        )
        .route(
            "/api/feature-flags/{name}/disable",
            post(crate::console::disable_feature_flag),
        )
        .route(
            "/api/deprecated-features",
            get(crate::console::list_deprecated),
        )
        .route(
            "/api/deprecated-features/{name}",
            delete(crate::console::acknowledge_deprecated),
        )
        .route("/api/nodes", get(crate::console::list_nodes))
        .route("/api/cluster-name", get(crate::console::cluster_name))
        .route(
            "/api/operator-policies",
            get(crate::mutations::list_operator_policies),
        )
        .route(
            "/api/operator-policies/{vhost}",
            get(crate::mutations::list_operator_policies_vhost),
        )
        .route(
            "/api/operator-policies/{vhost}/{name}",
            put(crate::mutations::put_operator_policy)
                .delete(crate::mutations::delete_operator_policy),
        )
        .route(
            "/api/definitions",
            get(export_definitions).post(import_definitions),
        )
        .route("/api/policies", get(list_policies))
        .route("/api/policies/{vhost}", get(list_policies_vhost))
        .route(
            "/api/policies/{vhost}/{name}",
            put(put_policy).delete(delete_policy),
        )
        .route(
            "/api/parameters/shovel/{vhost}/{name}",
            put(crate::mutations::put_shovel),
        )
        .route(
            "/api/parameters/federation-upstream/{vhost}/{name}",
            put(crate::mutations::put_federation_upstream),
        )
        .fallback(crate::spa::static_handler)
        .with_state(state)
}

// ── Health ──────────────────────────────────────────────────────────────

// ── Auth ────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct LoginRequest {
    username: String,
    password: String,
}

#[derive(Debug, Serialize)]
struct WhoamiResponse {
    name: String,
    tags: Vec<&'static str>,
}

// ── Overview ────────────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
struct OverviewResponse {
    product_name: &'static str,
    product_version: String,
    management_version: String,
    rabbitmq_version_compat: &'static str,
    object_totals: ObjectTotals,
    queue_totals: QueueTotals,
    message_stats: MessageStats,
}

#[derive(Debug, Serialize)]
struct MessageStats {
    publish: u64,
    deliver: u64,
    ack: u64,
}

#[derive(Debug, Serialize)]
struct ObjectTotals {
    connections: u64,
    channels: u64,
    queues: u64,
    exchanges: u64,
    consumers: u64,
    vhosts: u64,
}

#[derive(Debug, Serialize)]
struct QueueTotals {
    messages: u64,
    messages_ready: u64,
    messages_unacknowledged: u64,
}

// ── Vhosts ──────────────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
struct VhostItem {
    name: String,
}

// ── Queues ──────────────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
struct QueueItem {
    name: String,
    vhost: String,
    durable: bool,
    exclusive: bool,
    auto_delete: bool,
    state: String,
    messages: u64,
    messages_ready: u64,
    messages_unacknowledged: u64,
    consumers: u32,
    #[serde(rename = "type")]
    queue_type: String,
    arguments: serde_json::Value,
}

// ── Exchanges ───────────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
struct ExchangeItem {
    name: String,
    vhost: String,
    #[serde(rename = "type")]
    kind: String,
    durable: bool,
    auto_delete: bool,
    internal: bool,
}

// ── Connections ─────────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
struct ConnectionItem {
    name: String,
    user: String,
    vhost: String,
    peer_host: String,
    peer_port: u16,
    channels: u32,
    connected_at: u64,
}

// ── Helpers ─────────────────────────────────────────────────────────────

mod catalog;
mod codec;
mod health;
mod session;

use catalog::{
    delete_connection, get_exchange, list_connections, list_exchanges, list_queues, list_vhosts,
};
use health::{healthz, metrics_scrape, overview, readyz};
use session::{login, logout, whoami};

pub(crate) use catalog::query_stats;
pub use codec::decode_vhost;

#[cfg(test)]
mod tests;
