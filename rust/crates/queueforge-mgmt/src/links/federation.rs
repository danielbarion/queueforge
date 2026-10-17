//! Federation (components `federation-upstream` and
//! `federation-upstream-set`, policy keys `federation-upstream` and
//! `federation-upstream-set`).
//!
//! The links follow the stored upstreams and policies: a reconcile pass
//! starts a link for each federated exchange or queue and upstream, and
//! stops links nothing asks for any more. It runs after each upstream or
//! policy change and once a second, so exchanges and queues declared later
//! are picked up without a hook on the declare path.
//!
//! - An exchange link consumes the upstream exchange through a queue on the
//!   upstream, bound with the downstream exchange's binding keys (kept in
//!   step), and republishes into the downstream exchange. `x-received-from`
//!   records each hop; a message that has made `max-hops` hops stops.
//! - A queue link moves messages from the upstream queue only while the
//!   downstream queue has consumers and nothing ready, acking upstream after
//!   the downstream confirm.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use futures_lite::StreamExt;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicGetOptions, BasicPublishOptions, BasicQosOptions,
    ConfirmSelectOptions, ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions, QueueDeleteOptions,
};
use lapin::types::{AMQPValue, FieldArray, FieldTable, LongString, ShortString};
use lapin::{Channel, Connection, ConnectionProperties};
use queueforge_core::{HeaderArg, PolicyTarget, QueueKey};
use serde_json::{json, Value};
use tokio::sync::Notify;

use super::{first_uri, new_status, resolve, set_status, SharedStatus};
use crate::authz::{require_administrator, require_session};
use crate::error::MgmtError;
use crate::routes::decode_vhost;
use crate::state::MgmtState;

const UPSTREAM: &str = "federation-upstream";
const UPSTREAM_SET: &str = "federation-upstream-set";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum Kind {
    Exchange,
    Queue,
}

/// One link: a federated exchange or queue and one of its upstreams.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct LinkKey {
    vhost: String,
    kind: Kind,
    /// Downstream exchange or queue.
    name: String,
    /// Upstream parameter name.
    upstream: String,
}

/// A parsed `federation-upstream` value.
#[derive(Clone, Debug, PartialEq)]
struct Upstream {
    uri: String,
    exchange: Option<String>,
    queue: Option<String>,
    max_hops: u64,
    prefetch: u16,
    reconnect_delay: Duration,
    ack_mode: String,
    expires: Option<u64>,
    message_ttl: Option<u64>,
}

impl Upstream {
    fn parse(v: &Value) -> Result<Self, String> {
        let uri = first_uri(v.get("uri")).ok_or("uri is required")?;
        super::parse_uri(&uri)?;
        let ack_mode = v.get("ack-mode").and_then(Value::as_str).unwrap_or("on-confirm").to_string();
        if !matches!(ack_mode.as_str(), "on-confirm" | "on-publish" | "no-ack") {
            return Err(format!("ack-mode must be on-confirm, on-publish or no-ack (got {ack_mode})"));
        }
        let text = |k: &str| v.get(k).and_then(Value::as_str).filter(|s| !s.is_empty()).map(str::to_string);
        Ok(Upstream {
            uri,
            exchange: text("exchange"),
            queue: text("queue"),
            max_hops: v.get("max-hops").and_then(Value::as_u64).unwrap_or(1).max(1),
            prefetch: v.get("prefetch-count").and_then(Value::as_u64).unwrap_or(1000).clamp(1, u64::from(u16::MAX)) as u16,
            reconnect_delay: Duration::from_secs(v.get("reconnect-delay").and_then(Value::as_u64).unwrap_or(5)),
            ack_mode,
            expires: v.get("expires").and_then(Value::as_u64),
            message_ttl: v.get("message-ttl").and_then(Value::as_u64),
        })
    }
}

struct Running {
    task: tokio::task::AbortHandle,
    status: SharedStatus,
    upstream: Upstream,
}

static LINKS: LazyLock<Mutex<HashMap<LinkKey, Running>>> = LazyLock::new(Default::default);
static POKE: LazyLock<Notify> = LazyLock::new(Notify::new);

/// Reconcile soon (an upstream or policy changed).
pub(crate) fn poke() {
    POKE.notify_one();
}

/// Reconcile forever: after every poke and once a second.
pub(crate) async fn run() {
    loop {
        reconcile().await;
        tokio::select! {
            _ = POKE.notified() => {}
            _ = tokio::time::sleep(Duration::from_secs(1)) => {}
        }
    }
}

fn stored(state: &MgmtState, component: &str) -> Vec<(String, String, Value)> {
    state
        .store
        .list_parameters(&crate::settings::component_key(component))
        .unwrap_or_default()
        .into_iter()
        .filter_map(|(vhost, name, bytes)| Some((vhost, name, serde_json::from_slice(&bytes).ok()?)))
        .collect()
}

/// The links the stored upstreams and policies ask for.
fn desired(state: &MgmtState) -> HashMap<LinkKey, Upstream> {
    let mut upstreams: HashMap<(String, String), Upstream> = HashMap::new();
    for (vhost, name, value) in stored(state, UPSTREAM) {
        match Upstream::parse(&value) {
            Ok(u) => {
                upstreams.insert((vhost, name), u);
            }
            Err(err) => tracing::warn!(%vhost, %name, error = %err, "invalid federation upstream"),
        }
    }
    let mut sets: HashMap<(String, String), Vec<String>> = HashMap::new();
    for (vhost, name, value) in stored(state, UPSTREAM_SET) {
        let names = value
            .as_array()
            .map(|items| items.iter().filter_map(|i| i.get("upstream").and_then(Value::as_str).map(str::to_string)).collect())
            .unwrap_or_default();
        sets.insert((vhost, name), names);
    }
    let names_for = |vhost: &str, policy: &queueforge_core::Policy| -> Vec<String> {
        if let Some(one) = &policy.federation_upstream {
            return vec![one.to_string()];
        }
        match policy.federation_upstream_set.as_deref() {
            Some("all") => upstreams.keys().filter(|(v, _)| v == vhost).map(|(_, n)| n.clone()).collect(),
            Some(set) => sets.get(&(vhost.to_string(), set.to_string())).cloned().unwrap_or_default(),
            None => Vec::new(),
        }
    };
    let mut out = HashMap::new();
    let policies = state.router.list_policies(None);
    if !policies.iter().any(|p| p.federation_upstream.is_some() || p.federation_upstream_set.is_some()) {
        return out;
    }
    let vhosts: BTreeSet<String> = policies.iter().map(|p| p.vhost.to_string()).collect();
    let queue_keys = state.queues.list_keys();
    for vhost in vhosts {
        let mut entities: Vec<(Kind, String)> = state
            .router
            .list_exchanges(&vhost)
            .into_iter()
            .filter(|x| !x.name.is_empty() && !x.name.starts_with("amq."))
            .map(|x| (Kind::Exchange, x.name.to_string()))
            .collect();
        entities.extend(
            queue_keys
                .iter()
                .filter(|k| k.vhost.as_str() == vhost && !k.name.starts_with("federation: "))
                .map(|k| (Kind::Queue, k.name.to_string())),
        );
        for (kind, name) in entities {
            let target = if kind == Kind::Exchange { PolicyTarget::Exchanges } else { PolicyTarget::Queues };
            let Some(policy) = queueforge_core::select_policy(&policies, &vhost, &name, target) else { continue };
            for upstream in names_for(&vhost, policy) {
                if let Some(u) = upstreams.get(&(vhost.clone(), upstream.clone())) {
                    out.insert(LinkKey { vhost: vhost.clone(), kind, name: name.clone(), upstream }, u.clone());
                }
            }
        }
    }
    out
}

async fn reconcile() {
    let Some(state) = super::state() else { return };
    let st = state.clone();
    let Ok(want) = tokio::task::spawn_blocking(move || desired(&st)).await else { return };
    let mut links = LINKS.lock().expect("federation links");
    links.retain(|key, running| {
        let keep = want.get(key).is_some_and(|u| *u == running.upstream);
        if !keep {
            running.task.abort();
            if key.kind == Kind::Exchange {
                // The upstream queue goes with the link, as RabbitMQ's does.
                let (key, upstream) = (key.clone(), running.upstream.clone());
                tokio::spawn(async move { drop_upstream_queue(&key, &upstream).await });
            }
        }
        keep
    });
    for (key, upstream) in want {
        if links.contains_key(&key) {
            continue;
        }
        let status = new_status();
        let task = tokio::spawn(supervise(key.clone(), upstream.clone(), Arc::clone(&status)));
        links.insert(key, Running { task: task.abort_handle(), status, upstream });
    }
}

async fn supervise(key: LinkKey, upstream: Upstream, status: SharedStatus) {
    let mut attempts = 0u32;
    loop {
        set_status(&status, "starting", None);
        let result = match key.kind {
            Kind::Exchange => exchange_link(&key, &upstream, &status).await,
            Kind::Queue => queue_link(&key, &upstream, &status).await,
        };
        let err = result.err().unwrap_or_else(|| "link ended".into());
        tracing::debug!(link = ?key, error = %err, "federation link stopped; reconnecting");
        set_status(&status, "error", Some(err));
        attempts += 1;
        tokio::time::sleep(super::retry_delay(upstream.reconnect_delay, attempts)).await;
    }
}

async fn connect(uri: &str) -> Result<(Connection, Channel, super::Endpoint), String> {
    let ep = resolve(uri).await?;
    let conn = Connection::connect(&ep.amqp_uri(), ConnectionProperties::default()).await.map_err(|e| format!("{}: {e}", ep.shown))?;
    let ch = conn.create_channel().await.map_err(|e| e.to_string())?;
    Ok((conn, ch, ep))
}

/// This broker's downstream vhost, over loopback.
async fn downstream(vhost: &str) -> Result<(Connection, Channel), String> {
    let (conn, ch, _) = connect(&format!("amqp:///{}", super::pct(vhost))).await?;
    ch.confirm_select(ConfirmSelectOptions::default()).await.map_err(|e| e.to_string())?;
    Ok((conn, ch))
}

fn node_name() -> String {
    super::state().map(|s| s.config.node_name.clone()).unwrap_or_else(|| "queueforge".into())
}

fn upstream_queue_name(key: &LinkKey) -> String {
    format!("federation: {} -> {}:{}:{}", key.name, node_name(), key.vhost, key.upstream)
}

async fn drop_upstream_queue(key: &LinkKey, upstream: &Upstream) {
    if let Ok((conn, ch, _)) = connect(&upstream.uri).await {
        let _ = ch.queue_delete(&upstream_queue_name(key), QueueDeleteOptions::default()).await;
        let _ = conn.close(200, "federation link removed").await;
    }
}

/// The downstream exchange's bindings as (routing key, header arguments).
fn binding_keys(vhost: &str, exchange: &str) -> BTreeSet<(String, Vec<(String, String)>)> {
    let Some(state) = super::state() else { return BTreeSet::new() };
    let mut keys: BTreeSet<(String, Vec<(String, String)>)> = state
        .router
        .index()
        .list_for_exchange(vhost, exchange)
        .into_iter()
        .map(|b| {
            let args = b
                .args
                .iter()
                .map(|(k, v)| {
                    let v = match v {
                        HeaderArg::Str(s) => s.clone(),
                        HeaderArg::Int(n) => format!("\u{0}{n}"),
                    };
                    (k.to_string(), v)
                })
                .collect();
            (b.routing_key.to_string(), args)
        })
        .collect();
    for (v, source, _destination, key) in state.router.list_exchange_links() {
        if v == vhost && source == exchange {
            keys.insert((key, Vec::new()));
        }
    }
    keys
}

fn args_table(args: &[(String, String)]) -> FieldTable {
    let mut t = FieldTable::default();
    for (k, v) in args {
        let value = match v.strip_prefix('\u{0}').and_then(|n| n.parse::<i64>().ok()) {
            Some(n) => AMQPValue::LongLongInt(n),
            None => AMQPValue::LongString(LongString::from(v.as_bytes().to_vec())),
        };
        t.insert(ShortString::from(k.clone()), value);
    }
    t
}

async fn sync_bindings(
    ch: &Channel,
    queue: &str,
    up_exchange: &str,
    key: &LinkKey,
    bound: &mut BTreeSet<(String, Vec<(String, String)>)>,
) -> Result<(), String> {
    let want = binding_keys(&key.vhost, &key.name);
    for (rk, args) in want.difference(bound) {
        ch.queue_bind(queue, up_exchange, rk, QueueBindOptions::default(), args_table(args))
            .await
            .map_err(|e| format!("bind upstream: {e}"))?;
    }
    for (rk, args) in bound.difference(&want) {
        ch.queue_unbind(queue, up_exchange, rk, args_table(args)).await.map_err(|e| format!("unbind upstream: {e}"))?;
    }
    *bound = want;
    Ok(())
}

/// Hops a message has made, from its `x-received-from` header.
fn hops(headers: &FieldTable) -> usize {
    match headers.inner().get("x-received-from") {
        Some(AMQPValue::FieldArray(a)) => a.as_slice().len(),
        _ => 0,
    }
}

async fn exchange_link(key: &LinkKey, upstream: &Upstream, status: &SharedStatus) -> Result<(), String> {
    let up_exchange = upstream.exchange.clone().unwrap_or_else(|| key.name.clone());
    let (_up_conn, up, ep) = connect(&upstream.uri).await?;
    // The upstream exchange must exist; the link retries until it does.
    let passive = ExchangeDeclareOptions { passive: true, ..Default::default() };
    up.exchange_declare(&up_exchange, lapin::ExchangeKind::Direct, passive, FieldTable::default())
        .await
        .map_err(|e| format!("upstream exchange {up_exchange}: {e}"))?;
    let up = _up_conn.create_channel().await.map_err(|e| e.to_string())?;
    let queue = upstream_queue_name(key);
    let mut args = FieldTable::default();
    if let Some(ms) = upstream.expires {
        args.insert("x-expires".into(), AMQPValue::LongLongInt(ms as i64));
    }
    if let Some(ms) = upstream.message_ttl {
        args.insert("x-message-ttl".into(), AMQPValue::LongLongInt(ms as i64));
    }
    let durable = QueueDeclareOptions { durable: true, ..Default::default() };
    up.queue_declare(&queue, durable, args).await.map_err(|e| format!("upstream queue: {e}"))?;
    let mut bound = BTreeSet::new();
    sync_bindings(&up, &queue, &up_exchange, key, &mut bound).await?;
    let no_ack = upstream.ack_mode == "no-ack";
    up.basic_qos(upstream.prefetch, BasicQosOptions::default()).await.map_err(|e| e.to_string())?;
    let mut consumer = up
        .basic_consume(&queue, "federation-link", BasicConsumeOptions { no_ack, ..Default::default() }, FieldTable::default())
        .await
        .map_err(|e| format!("consume upstream: {e}"))?;
    let (_down_conn, down) = downstream(&key.vhost).await?;
    set_status(status, "running", None);
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    loop {
        tokio::select! {
            delivery = consumer.next() => {
                let d = delivery.ok_or("upstream consumer ended")?.map_err(|e| e.to_string())?;
                let mut headers = d.properties.headers().clone().unwrap_or_default();
                if hops(&headers) >= upstream.max_hops as usize {
                    if !no_ack {
                        d.acker.ack(BasicAckOptions::default()).await.map_err(|e| e.to_string())?;
                    }
                    continue;
                }
                let mut trail: Vec<AMQPValue> = match headers.inner().get("x-received-from") {
                    Some(AMQPValue::FieldArray(a)) => a.as_slice().to_vec(),
                    _ => Vec::new(),
                };
                let mut hop = FieldTable::default();
                hop.insert("uri".into(), AMQPValue::LongString(LongString::from(ep.shown.as_bytes().to_vec())));
                hop.insert("exchange".into(), AMQPValue::LongString(LongString::from(up_exchange.as_bytes().to_vec())));
                hop.insert("redelivered".into(), AMQPValue::Boolean(d.redelivered));
                hop.insert("cluster-name".into(), AMQPValue::LongString(LongString::from(node_name().into_bytes())));
                trail.push(AMQPValue::FieldTable(hop));
                headers.insert("x-received-from".into(), AMQPValue::FieldArray(FieldArray::from(trail)));
                let props = d.properties.clone().with_headers(headers);
                let confirm = down
                    .basic_publish(&key.name, d.routing_key.as_str(), BasicPublishOptions::default(), &d.data, props)
                    .await
                    .map_err(|e| e.to_string())?;
                if upstream.ack_mode == "on-confirm" {
                    confirm.await.map_err(|e| e.to_string())?;
                }
                if !no_ack {
                    d.acker.ack(BasicAckOptions::default()).await.map_err(|e| e.to_string())?;
                }
            }
            _ = tick.tick() => sync_bindings(&up, &queue, &up_exchange, key, &mut bound).await?,
        }
    }
}

async fn queue_link(key: &LinkKey, upstream: &Upstream, status: &SharedStatus) -> Result<(), String> {
    let up_queue = upstream.queue.clone().unwrap_or_else(|| key.name.clone());
    let (up_conn, up, _) = connect(&upstream.uri).await?;
    let passive = QueueDeclareOptions { passive: true, ..Default::default() };
    up.queue_declare(&up_queue, passive, FieldTable::default())
        .await
        .map_err(|e| format!("upstream queue {up_queue}: {e}"))?;
    let (_down_conn, down) = downstream(&key.vhost).await?;
    set_status(status, "running", None);
    let state = super::state().ok_or("links not started")?;
    let local = QueueKey::new(key.vhost.as_str(), key.name.as_str());
    let _keep = up_conn;
    loop {
        // Pull only for consumers that are waiting: the downstream queue has
        // consumers and nothing ready.
        let hungry = match state.queues.get(&local) {
            Some(handle) => crate::routes::query_stats(&handle.tx)
                .await
                .is_some_and(|s| s.consumer_count > 0 && s.messages_ready == 0),
            None => false,
        };
        if !hungry {
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }
        let no_ack = upstream.ack_mode == "no-ack";
        let Some(got) = up.basic_get(&up_queue, BasicGetOptions { no_ack }).await.map_err(|e| e.to_string())? else {
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        };
        let d = got.delivery;
        let confirm = down
            .basic_publish("", &key.name, BasicPublishOptions::default(), &d.data, d.properties.clone())
            .await
            .map_err(|e| e.to_string())?;
        if upstream.ack_mode == "on-confirm" {
            confirm.await.map_err(|e| e.to_string())?;
        }
        if !no_ack {
            d.acker.ack(BasicAckOptions::default()).await.map_err(|e| e.to_string())?;
        }
    }
}

/// The links on this node as `GET /api/federation-links` lists them.
fn list(vhost: Option<&str>) -> Vec<Value> {
    let node = node_name();
    let links = LINKS.lock().expect("federation links");
    let mut keys: Vec<&LinkKey> = links.keys().filter(|k| vhost.is_none_or(|v| v == k.vhost)).collect();
    keys.sort();
    keys.into_iter()
        .map(|key| {
            let running = &links[key];
            let status = running.status.lock().map(|s| s.clone()).ok();
            let mut row = json!({
                "node": node,
                "vhost": key.vhost,
                "upstream": key.upstream,
                "id": format!("{:x}", fnv(&format!("{key:?}"))),
                "uri": super::shown_uri(&running.upstream.uri),
                "status": status.as_ref().map_or("starting", |s| s.state),
                "timestamp": status.as_ref().map(|s| s.timestamp.clone()),
                "local_connection": format!("<federation link {}>", key.name),
            });
            match key.kind {
                Kind::Exchange => {
                    row["type"] = json!("exchange");
                    row["exchange"] = json!(key.name);
                    row["upstream_exchange"] = json!(running.upstream.exchange.clone().unwrap_or_else(|| key.name.clone()));
                }
                Kind::Queue => {
                    row["type"] = json!("queue");
                    row["queue"] = json!(key.name);
                    row["upstream_queue"] = json!(running.upstream.queue.clone().unwrap_or_else(|| key.name.clone()));
                }
            }
            if let Some(err) = status.and_then(|s| s.error) {
                row["error"] = json!(err);
            }
            row
        })
        .collect()
}

fn fnv(s: &str) -> u64 {
    s.bytes().fold(0xcbf29ce484222325u64, |h, b| (h ^ u64::from(b)).wrapping_mul(0x100000001b3))
}

/// GET /api/federation-links and /api/federation-links/{vhost}
pub async fn list_links(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    path: Option<Path<String>>,
) -> Result<Json<Value>, MgmtError> {
    let _session = require_session(&state, &headers).await?;
    let vhost = match path {
        Some(Path(v)) => Some(decode_vhost(&v)?),
        None => None,
    };
    Ok(Json(Value::Array(list(vhost.as_deref()))))
}

#[derive(serde::Deserialize)]
pub(crate) struct ParamPut {
    value: Value,
}

/// PUT /api/parameters/federation-upstream/{vhost}/{name}
pub async fn put_upstream(
    state: State<MgmtState>,
    headers: HeaderMap,
    Path((vhost, name)): Path<(String, String)>,
    body: Json<ParamPut>,
) -> Result<StatusCode, MgmtError> {
    put_param(state, headers, UPSTREAM.into(), vhost, name, body).await
}

/// PUT /api/parameters/federation-upstream-set/{vhost}/{name}
pub async fn put_upstream_set(
    state: State<MgmtState>,
    headers: HeaderMap,
    Path((vhost, name)): Path<(String, String)>,
    body: Json<ParamPut>,
) -> Result<StatusCode, MgmtError> {
    put_param(state, headers, UPSTREAM_SET.into(), vhost, name, body).await
}

/// DELETE /api/parameters/federation-upstream/{vhost}/{name}
pub async fn delete_upstream(
    state: State<MgmtState>,
    headers: HeaderMap,
    Path((vhost, name)): Path<(String, String)>,
) -> Result<StatusCode, MgmtError> {
    delete_param(state, headers, UPSTREAM.into(), vhost, name).await
}

/// DELETE /api/parameters/federation-upstream-set/{vhost}/{name}
pub async fn delete_upstream_set(
    state: State<MgmtState>,
    headers: HeaderMap,
    Path((vhost, name)): Path<(String, String)>,
) -> Result<StatusCode, MgmtError> {
    delete_param(state, headers, UPSTREAM_SET.into(), vhost, name).await
}

async fn put_param(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    component: String,
    raw_vhost: String,
    name: String,
    Json(body): Json<ParamPut>,
) -> Result<StatusCode, MgmtError> {
    let session = require_session(&state, &headers).await?;
    require_administrator(&session)?;
    let vhost = decode_vhost(&raw_vhost)?;
    if component == UPSTREAM {
        Upstream::parse(&body.value).map_err(MgmtError::BadRequest)?;
    } else if !body.value.is_array() {
        return Err(MgmtError::BadRequest("a federation-upstream-set is a list of {upstream}".into()));
    }
    let (store, conns) = (Arc::clone(&state.store), Arc::clone(&state.connections));
    let (c, v, n, value) = (component.clone(), vhost.clone(), name.clone(), body.value.clone());
    let existed = tokio::task::spawn_blocking(move || {
        let had = store.get_parameter(&crate::settings::component_key(&c), &v, &n).ok().flatten().is_some();
        crate::settings::put_parameter(&conns, &store, &c, &v, &n, &value);
        had
    })
    .await
    .unwrap_or(false);
    crate::mutations::replicate(&state, "parameter", json!({"component": component, "vhost": vhost, "name": name, "value": body.value})).await;
    poke();
    Ok(if existed { StatusCode::NO_CONTENT } else { StatusCode::CREATED })
}

async fn delete_param(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    component: String,
    raw_vhost: String,
    name: String,
) -> Result<StatusCode, MgmtError> {
    let session = require_session(&state, &headers).await?;
    require_administrator(&session)?;
    let vhost = decode_vhost(&raw_vhost)?;
    let (store, conns) = (Arc::clone(&state.store), Arc::clone(&state.connections));
    let (c, v, n) = (component.clone(), vhost.clone(), name.clone());
    let had = tokio::task::spawn_blocking(move || crate::settings::delete_parameter(&conns, &store, &c, &v, &n))
        .await
        .unwrap_or(false);
    poke();
    if !had {
        return Err(MgmtError::NotFound(format!("parameter {component}/{vhost}/{name}")));
    }
    crate::mutations::replicate(&state, "delete_parameter", json!({"component": component, "vhost": vhost, "name": name})).await;
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upstream_values_parse_with_rabbitmq_defaults() {
        let u = Upstream::parse(&json!({"uri": ["amqp://", "amqp://other"], "exchange": "up"})).unwrap();
        assert_eq!(u.uri, "amqp://");
        assert_eq!(u.exchange.as_deref(), Some("up"));
        assert_eq!((u.max_hops, u.prefetch, u.ack_mode.as_str()), (1, 1000, "on-confirm"));
        assert_eq!(u.reconnect_delay, Duration::from_secs(5));
        assert!(Upstream::parse(&json!({})).is_err());
        assert!(Upstream::parse(&json!({"uri": "amqp://", "ack-mode": "never"})).is_err());
    }

    #[test]
    fn hop_count_reads_x_received_from() {
        let mut h = FieldTable::default();
        assert_eq!(hops(&h), 0);
        h.insert("x-received-from".into(), AMQPValue::FieldArray(FieldArray::from(vec![AMQPValue::Boolean(true)])));
        assert_eq!(hops(&h), 1);
    }
}
