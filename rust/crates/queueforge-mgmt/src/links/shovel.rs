//! Dynamic shovels (component `shovel`): move messages from a source queue,
//! exchange or stream to a destination, over AMQP 0-9-1 or 1.0.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use futures_lite::StreamExt;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, BasicQosOptions, ConfirmSelectOptions,
    QueueBindOptions, QueueDeclareOptions,
};
use lapin::types::FieldTable;
use lapin::{BasicProperties, Channel, Connection, ConnectionProperties};
use serde_json::{json, Value};

use super::amqp10_client::{self, Client};
use super::{first_uri, json_table, new_status, resolve, set_status, SharedStatus};
use crate::authz::{require_administrator, require_session};
use crate::error::MgmtError;
use crate::routes::decode_vhost;
use crate::state::MgmtState;

const COMPONENT: &str = "shovel";

struct Running {
    task: tokio::task::AbortHandle,
    status: SharedStatus,
    spec: Spec,
}

/// Running shovels by vhost and name.
static RUNNING: LazyLock<Mutex<HashMap<(String, String), Running>>> = LazyLock::new(Default::default);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Proto {
    Amqp091,
    Amqp10,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AckMode {
    OnConfirm,
    OnPublish,
    NoAck,
}

#[derive(Clone, Debug)]
struct SrcSpec {
    protocol: Proto,
    uri: String,
    queue: Option<String>,
    exchange: Option<String>,
    exchange_key: String,
    address: Option<String>,
    consumer_args: Value,
}

#[derive(Clone, Debug)]
struct DestSpec {
    protocol: Proto,
    uri: String,
    queue: Option<String>,
    exchange: Option<String>,
    exchange_key: Option<String>,
    address: Option<String>,
}

/// A validated shovel definition.
#[derive(Clone, Debug)]
pub(crate) struct Spec {
    src: SrcSpec,
    dest: DestSpec,
    ack_mode: AckMode,
    prefetch: u16,
    /// `src-delete-after` as a message count; `None` runs forever.
    delete_after: Option<u64>,
    reconnect_delay: Duration,
}

fn text(v: &Value, key: &str) -> Option<String> {
    v.get(key).and_then(Value::as_str).filter(|s| !s.is_empty()).map(str::to_string)
}

fn proto(v: &Value, key: &str) -> Result<Proto, String> {
    match v.get(key).and_then(Value::as_str).unwrap_or("amqp091") {
        "amqp091" => Ok(Proto::Amqp091),
        "amqp10" => Ok(Proto::Amqp10),
        other => Err(format!("{key} must be amqp091 or amqp10 (got {other})")),
    }
}

impl Spec {
    /// Parse a RabbitMQ shovel parameter value.
    pub(crate) fn parse(v: &Value) -> Result<Self, String> {
        let src_protocol = proto(v, "src-protocol")?;
        let dest_protocol = proto(v, "dest-protocol")?;
        let src = SrcSpec {
            protocol: src_protocol,
            uri: first_uri(v.get("src-uri")).unwrap_or_else(|| "amqp://".into()),
            queue: text(v, "src-queue"),
            exchange: text(v, "src-exchange"),
            exchange_key: text(v, "src-exchange-key").unwrap_or_else(|| "#".into()),
            address: text(v, "src-address"),
            consumer_args: v.get("src-consumer-args").cloned().unwrap_or(Value::Null),
        };
        let dest = DestSpec {
            protocol: dest_protocol,
            uri: first_uri(v.get("dest-uri")).unwrap_or_else(|| "amqp://".into()),
            queue: text(v, "dest-queue"),
            exchange: text(v, "dest-exchange"),
            exchange_key: text(v, "dest-exchange-key"),
            address: text(v, "dest-address"),
        };
        match src.protocol {
            Proto::Amqp091 if src.queue.is_none() && src.exchange.is_none() => {
                return Err("src-queue or src-exchange is required".into())
            }
            Proto::Amqp10 if src.address.is_none() && src.queue.is_none() => return Err("src-address is required".into()),
            _ => {}
        }
        match dest.protocol {
            Proto::Amqp091 if dest.queue.is_none() && dest.exchange.is_none() => {
                return Err("dest-queue or dest-exchange is required".into())
            }
            Proto::Amqp10 if dest.address.is_none() && dest.queue.is_none() => return Err("dest-address is required".into()),
            _ => {}
        }
        let ack_mode = match v.get("ack-mode").and_then(Value::as_str).unwrap_or("on-confirm") {
            "on-confirm" => AckMode::OnConfirm,
            "on-publish" => AckMode::OnPublish,
            "no-ack" => AckMode::NoAck,
            other => return Err(format!("ack-mode must be on-confirm, on-publish or no-ack (got {other})")),
        };
        let prefetch = v.get("src-prefetch-count").and_then(Value::as_u64).unwrap_or(1000).clamp(1, u64::from(u16::MAX)) as u16;
        let delete_after = match v.get("src-delete-after") {
            Some(Value::Number(n)) => n.as_u64(),
            _ => None,
        };
        let reconnect_delay = Duration::from_secs(v.get("reconnect-delay").and_then(Value::as_u64).unwrap_or(5));
        Ok(Spec { src, dest, ack_mode, prefetch, delete_after, reconnect_delay })
    }
}

/// Start (or restart) `vhost`/`name` with `spec`.
fn launch(vhost: &str, name: &str, spec: Spec) {
    let status = new_status();
    let task = tokio::spawn(supervise(spec.clone(), Arc::clone(&status), vhost.to_string(), name.to_string()));
    let old = RUNNING
        .lock()
        .expect("shovels")
        .insert((vhost.to_string(), name.to_string()), Running { task: task.abort_handle(), status, spec });
    if let Some(old) = old {
        old.task.abort();
    }
}

/// Start every shovel stored before a restart.
pub(crate) async fn restore() {
    let Some(state) = super::state() else { return };
    let store = Arc::clone(&state.store);
    let rows = tokio::task::spawn_blocking(move || store.list_parameters(&crate::settings::component_key(COMPONENT)))
        .await
        .ok()
        .and_then(Result::ok)
        .unwrap_or_default();
    for (vhost, name, bytes) in rows {
        let Ok(value) = serde_json::from_slice::<Value>(&bytes) else { continue };
        match Spec::parse(&value) {
            Ok(spec) => launch(&vhost, &name, spec),
            Err(err) => tracing::warn!(%vhost, %name, error = %err, "stored shovel is invalid"),
        }
    }
}

#[derive(serde::Deserialize)]
pub(crate) struct ShovelPut {
    value: Value,
}

/// PUT /api/parameters/shovel/{vhost}/{name}
pub async fn put_shovel(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Path((raw_vhost, name)): Path<(String, String)>,
    Json(body): Json<ShovelPut>,
) -> Result<StatusCode, MgmtError> {
    let session = require_session(&state, &headers).await?;
    require_administrator(&session)?;
    let vhost = decode_vhost(&raw_vhost)?;
    let spec = Spec::parse(&body.value).map_err(MgmtError::BadRequest)?;
    let existed = RUNNING.lock().expect("shovels").contains_key(&(vhost.clone(), name.clone()));
    let (store, conns) = (Arc::clone(&state.store), Arc::clone(&state.connections));
    let (v, n, value) = (vhost.clone(), name.clone(), body.value.clone());
    let _ = tokio::task::spawn_blocking(move || crate::settings::put_parameter(&conns, &store, COMPONENT, &v, &n, &value)).await;
    launch(&vhost, &name, spec);
    Ok(if existed { StatusCode::NO_CONTENT } else { StatusCode::CREATED })
}

/// DELETE /api/parameters/shovel/{vhost}/{name}
pub async fn delete_shovel(
    State(state): State<MgmtState>,
    headers: HeaderMap,
    Path((raw_vhost, name)): Path<(String, String)>,
) -> Result<StatusCode, MgmtError> {
    let session = require_session(&state, &headers).await?;
    require_administrator(&session)?;
    let vhost = decode_vhost(&raw_vhost)?;
    let running = RUNNING.lock().expect("shovels").remove(&(vhost.clone(), name.clone()));
    if let Some(r) = &running {
        r.task.abort();
    }
    let (store, conns) = (Arc::clone(&state.store), Arc::clone(&state.connections));
    let (v, n) = (vhost.clone(), name.clone());
    let stored = tokio::task::spawn_blocking(move || crate::settings::delete_parameter(&conns, &store, COMPONENT, &v, &n))
        .await
        .unwrap_or(false);
    if running.is_none() && !stored {
        return Err(MgmtError::NotFound(format!("shovel '{name}'")));
    }
    Ok(StatusCode::NO_CONTENT)
}

/// The shovels on this node as `GET /api/shovels` lists them.
pub(crate) fn list(vhost: Option<&str>) -> Vec<Value> {
    let node = super::state().map(|s| s.config.node_name.clone()).unwrap_or_default();
    let running = RUNNING.lock().expect("shovels");
    let mut out: Vec<Value> = running
        .iter()
        .filter(|((v, _), _)| vhost.is_none_or(|want| want == v))
        .map(|((v, n), r)| {
            let status = r.status.lock().map(|s| s.clone()).ok();
            let mut row = json!({
                "node": node,
                "vhost": v,
                "name": n,
                "type": "dynamic",
                "state": status.as_ref().map_or("starting", |s| s.state),
                "timestamp": status.as_ref().map(|s| s.timestamp.clone()),
                "src_protocol": if r.spec.src.protocol == Proto::Amqp10 { "amqp10" } else { "amqp091" },
                "dest_protocol": if r.spec.dest.protocol == Proto::Amqp10 { "amqp10" } else { "amqp091" },
                "src_uri": super::shown_uri(&r.spec.src.uri),
                "dest_uri": super::shown_uri(&r.spec.dest.uri),
            });
            let fields = [
                ("src_queue", r.spec.src.queue.clone()),
                ("src_exchange", r.spec.src.exchange.clone()),
                ("src_address", r.spec.src.address.clone()),
                ("dest_queue", r.spec.dest.queue.clone()),
                ("dest_exchange", r.spec.dest.exchange.clone()),
                ("dest_address", r.spec.dest.address.clone()),
                ("reason", status.and_then(|s| s.error)),
            ];
            for (k, v) in fields {
                if let Some(v) = v {
                    row[k] = json!(v);
                }
            }
            row
        })
        .collect();
    out.sort_by(|a, b| (a["vhost"].to_string(), a["name"].to_string()).cmp(&(b["vhost"].to_string(), b["name"].to_string())));
    out
}

/// GET /api/shovels and /api/shovels/{vhost}
pub async fn list_shovels(
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

/// Run a shovel, reconnecting after `reconnect-delay` on failure, until it
/// is deleted or its `src-delete-after` count is moved.
async fn supervise(spec: Spec, status: SharedStatus, vhost: String, name: String) {
    let mut moved = 0u64;
    let mut attempts = 0u32;
    loop {
        set_status(&status, "starting", None);
        match run_once(&spec, &status, &mut moved).await {
            Ok(()) => {
                // src-delete-after reached: the shovel removes itself, as in RabbitMQ.
                set_status(&status, "terminated", None);
                RUNNING.lock().expect("shovels").remove(&(vhost.clone(), name.clone()));
                if let Some(state) = super::state() {
                    let (store, conns) = (Arc::clone(&state.store), Arc::clone(&state.connections));
                    let _ = tokio::task::spawn_blocking(move || crate::settings::delete_parameter(&conns, &store, COMPONENT, &vhost, &name)).await;
                }
                return;
            }
            Err(err) => {
                tracing::debug!(error = %err, "shovel stopped; reconnecting");
                set_status(&status, "error", Some(err));
                attempts += 1;
                tokio::time::sleep(super::retry_delay(spec.reconnect_delay, attempts)).await;
            }
        }
    }
}

/// One message on its way through a shovel.
struct Msg {
    body: Vec<u8>,
    props: BasicProperties,
    exchange: String,
    routing_key: String,
    settle: Settle,
}

enum Settle {
    Amqp091(lapin::acker::Acker),
    Amqp10(u32),
    None,
}

enum Source {
    Amqp091 { _conn: Connection, consumer: lapin::Consumer, no_ack: bool },
    Amqp10(Client),
}

enum Dest {
    Amqp091 { _conn: Connection, ch: Channel, exchange: String, key: Option<String> },
    Amqp10(Client),
}

async fn amqp091(uri: &str) -> Result<(Connection, Channel, String), String> {
    let ep = resolve(uri).await?;
    let conn = Connection::connect(&ep.amqp_uri(), ConnectionProperties::default()).await.map_err(|e| format!("{}: {e}", ep.shown))?;
    let ch = conn.create_channel().await.map_err(|e| e.to_string())?;
    Ok((conn, ch, ep.vhost))
}

async fn amqp10(uri: &str) -> Result<Client, String> {
    let ep = resolve(uri).await?;
    Client::connect(&ep.host, ep.port, &ep.user, &ep.pass, &ep.vhost).await
}

/// Declare `queue` unless it exists (a passive declare first, so a stream or
/// a queue with arguments is used as it is).
async fn ensure_queue(conn: &Connection, ch: Channel, queue: &str) -> Result<Channel, String> {
    let passive = QueueDeclareOptions { passive: true, ..Default::default() };
    if ch.queue_declare(queue, passive, FieldTable::default()).await.is_ok() {
        return Ok(ch);
    }
    let ch = conn.create_channel().await.map_err(|e| e.to_string())?;
    let durable = QueueDeclareOptions { durable: true, ..Default::default() };
    ch.queue_declare(queue, durable, FieldTable::default()).await.map_err(|e| format!("declare {queue}: {e}"))?;
    Ok(ch)
}

impl Source {
    async fn open(spec: &Spec) -> Result<Self, String> {
        let s = &spec.src;
        match s.protocol {
            Proto::Amqp10 => {
                let mut c = amqp10(&s.uri).await?;
                let address = s.address.clone().or_else(|| s.queue.clone()).unwrap_or_default();
                c.receiver(&amqp10_client::address(&address), u32::from(spec.prefetch)).await?;
                Ok(Source::Amqp10(c))
            }
            Proto::Amqp091 => {
                let (conn, ch, _) = amqp091(&s.uri).await?;
                let queue = match (&s.queue, &s.exchange) {
                    (Some(q), _) => {
                        let ch2 = ensure_queue(&conn, ch, q).await?;
                        return Self::consume(conn, ch2, q, spec).await;
                    }
                    (None, Some(x)) => {
                        let opts = QueueDeclareOptions { exclusive: true, auto_delete: true, ..Default::default() };
                        let q = ch.queue_declare("", opts, FieldTable::default()).await.map_err(|e| e.to_string())?;
                        let name = q.name().to_string();
                        ch.queue_bind(&name, x, &s.exchange_key, QueueBindOptions::default(), FieldTable::default())
                            .await
                            .map_err(|e| format!("bind to {x}: {e}"))?;
                        name
                    }
                    (None, None) => return Err("no source".into()),
                };
                Self::consume(conn, ch, &queue, spec).await
            }
        }
    }

    async fn consume(conn: Connection, ch: Channel, queue: &str, spec: &Spec) -> Result<Self, String> {
        let no_ack = spec.ack_mode == AckMode::NoAck;
        ch.basic_qos(spec.prefetch, BasicQosOptions::default()).await.map_err(|e| e.to_string())?;
        let consumer = ch
            .basic_consume(
                queue,
                "queueforge-shovel",
                BasicConsumeOptions { no_ack, ..Default::default() },
                json_table(Some(&spec.src.consumer_args)),
            )
            .await
            .map_err(|e| format!("consume {queue}: {e}"))?;
        Ok(Source::Amqp091 { _conn: conn, consumer, no_ack })
    }

    async fn next(&mut self) -> Result<Msg, String> {
        match self {
            Source::Amqp091 { consumer, no_ack, .. } => {
                let d = consumer.next().await.ok_or("source consumer ended")?.map_err(|e| e.to_string())?;
                Ok(Msg {
                    body: d.data,
                    props: d.properties,
                    exchange: d.exchange.to_string(),
                    routing_key: d.routing_key.to_string(),
                    settle: if *no_ack { Settle::None } else { Settle::Amqp091(d.acker) },
                })
            }
            Source::Amqp10(c) => {
                let r = c.recv().await?;
                Ok(Msg {
                    body: r.body,
                    props: r.props,
                    exchange: String::new(),
                    routing_key: r.subject.unwrap_or_default(),
                    settle: Settle::Amqp10(r.delivery_id),
                })
            }
        }
    }

    async fn settle(&mut self, settle: Settle) -> Result<(), String> {
        match (self, settle) {
            (_, Settle::Amqp091(acker)) => acker.ack(BasicAckOptions::default()).await.map_err(|e| e.to_string()),
            (Source::Amqp10(c), Settle::Amqp10(id)) => c.accept(id).await,
            _ => Ok(()),
        }
    }
}

impl Dest {
    async fn open(spec: &Spec) -> Result<Self, String> {
        let d = &spec.dest;
        match d.protocol {
            Proto::Amqp10 => {
                let mut c = amqp10(&d.uri).await?;
                let address = d.address.clone().or_else(|| d.queue.clone()).unwrap_or_default();
                c.sender(&amqp10_client::address(&address)).await?;
                Ok(Dest::Amqp10(c))
            }
            Proto::Amqp091 => {
                let (conn, ch, _) = amqp091(&d.uri).await?;
                let (ch, exchange, key) = match (&d.queue, &d.exchange) {
                    (Some(q), _) => (ensure_queue(&conn, ch, q).await?, String::new(), Some(q.clone())),
                    (None, Some(x)) => (ch, x.clone(), d.exchange_key.clone()),
                    (None, None) => return Err("no destination".into()),
                };
                if spec.ack_mode == AckMode::OnConfirm {
                    ch.confirm_select(ConfirmSelectOptions::default()).await.map_err(|e| e.to_string())?;
                }
                Ok(Dest::Amqp091 { _conn: conn, ch, exchange, key })
            }
        }
    }

    async fn send(&mut self, msg: &Msg, confirm: bool) -> Result<(), String> {
        match self {
            Dest::Amqp091 { ch, exchange, key, .. } => {
                let rk = key.clone().unwrap_or_else(|| msg.routing_key.clone());
                let pending = ch
                    .basic_publish(exchange, &rk, BasicPublishOptions::default(), &msg.body, msg.props.clone())
                    .await
                    .map_err(|e| e.to_string())?;
                if confirm {
                    let c = pending.await.map_err(|e| e.to_string())?;
                    if c.is_nack() {
                        return Err("destination refused the message".into());
                    }
                }
                Ok(())
            }
            Dest::Amqp10(c) => c.send(&msg.exchange, &msg.routing_key, &msg.props, &msg.body).await,
        }
    }
}

async fn run_once(spec: &Spec, status: &SharedStatus, moved: &mut u64) -> Result<(), String> {
    let mut src = Source::open(spec).await?;
    let mut dest = Dest::open(spec).await?;
    set_status(status, "running", None);
    loop {
        if spec.delete_after.is_some_and(|n| *moved >= n) {
            return Ok(());
        }
        let mut msg = src.next().await?;
        let settle = std::mem::replace(&mut msg.settle, Settle::None);
        dest.send(&msg, spec.ack_mode == AckMode::OnConfirm).await?;
        src.settle(settle).await?;
        *moved += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_rabbitmq_shovel_values() {
        let spec = Spec::parse(&json!({
            "src-protocol": "amqp10", "src-uri": ["amqp://"], "src-address": "/queues/in",
            "dest-protocol": "amqp091", "dest-uri": "amqp://", "dest-exchange": "x", "dest-exchange-key": "k",
            "ack-mode": "on-publish", "src-delete-after": 5
        }))
        .unwrap();
        assert_eq!(spec.src.protocol, Proto::Amqp10);
        assert_eq!(spec.src.address.as_deref(), Some("/queues/in"));
        assert_eq!(spec.dest.exchange_key.as_deref(), Some("k"));
        assert_eq!(spec.ack_mode, AckMode::OnPublish);
        assert_eq!(spec.delete_after, Some(5));
        assert!(Spec::parse(&json!({"src-queue": "a"})).is_err());
        assert!(Spec::parse(&json!({"src-queue": "a", "dest-queue": "b", "src-protocol": "mqtt"})).is_err());
    }
}
