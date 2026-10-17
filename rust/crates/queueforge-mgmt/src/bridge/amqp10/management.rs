//! RabbitMQ 4's AMQP 1.0 management node ("HTTP over AMQP", v2 addresses).
//!
//! A client attaches a sender and a receiver, both named alike, to
//! `/management`. Each request has the HTTP method as subject, the path as
//! `to` and an amqp-value body; the reply goes back on the paired receiver
//! with the status code as subject and the request's message-id as
//! correlation-id. Operations run as 0-9-1 methods on the bridge's loopback
//! connection, so permissions, policies and limits apply as for AMQP 0-9-1.
//! An operation the user may not do ends the session with
//! `amqp:unauthorized-access`, as RabbitMQ does.

use lapin::options::{
    ExchangeBindOptions, ExchangeDeclareOptions, ExchangeDeleteOptions, ExchangeUnbindOptions, QueueBindOptions,
    QueueDeclareOptions, QueueDeleteOptions, QueuePurgeOptions,
};
use lapin::types::{AMQPValue, FieldTable};
use lapin::{Channel, Connection, ExchangeKind};

use super::map::to_field;
use super::segment;
use super::types::{Decoder, Encoder, V};

/// The address both links of the management pair attach to.
pub(super) const ADDRESS: &str = "/management";

pub(super) struct Request {
    pub id: V,
    pub to: String,
    pub method: String,
    pub body: V,
}

/// What a request comes to.
pub(super) enum Outcome {
    /// Status code and body for the reply.
    Reply(&'static str, V),
    /// The user may not do this: end the session with this description.
    Refused(String),
    /// The loopback connection is unusable: close with this condition.
    Fail(&'static str, String),
}

/// Pick the management fields out of a request message.
pub(super) fn parse(payload: &[u8]) -> Result<Request, String> {
    let mut d = Decoder::new(payload);
    let mut req = Request { id: V::Null, to: String::new(), method: String::new(), body: V::Null };
    while d.more() {
        let section = d.value()?;
        match section.code() {
            Some(0x73) => {
                req.id = section.field(0).clone();
                req.to = section.field(2).as_str().unwrap_or_default().to_string();
                req.method = section.field(3).as_str().unwrap_or_default().to_ascii_uppercase();
            }
            Some(0x75) | Some(0x77) => req.body = section.inner().clone(),
            _ => {}
        }
    }
    if req.to.is_empty() || req.method.is_empty() {
        return Err("a management request needs 'to' and 'subject'".into());
    }
    Ok(req)
}

/// The reply message: status as subject, correlated with the request.
pub(super) fn reply(correlation: &V, status: &str, body: &V) -> Vec<u8> {
    let mut e = Encoder::new();
    e.value(&V::described(
        0x73,
        V::List(vec![V::Null, V::Null, V::Null, V::Str(status.into()), V::Null, correlation.clone()]),
    ));
    e.value(&V::described(0x77, body.clone()));
    e.buf
}

fn key(s: &str) -> V {
    V::Str(s.into())
}

fn json(v: &serde_json::Value) -> V {
    match v {
        serde_json::Value::Null => V::Null,
        serde_json::Value::Bool(b) => V::Bool(*b),
        serde_json::Value::Number(n) => match (n.as_i64(), n.as_u64()) {
            (Some(i), _) => V::Long(i),
            (None, Some(u)) => V::Ulong(u),
            _ => V::Double(n.as_f64().unwrap_or(0.0)),
        },
        serde_json::Value::String(s) => V::Str(s.clone()),
        serde_json::Value::Array(items) => V::List(items.iter().map(json).collect()),
        serde_json::Value::Object(m) => V::Map(m.iter().map(|(k, v)| (key(k), json(v))).collect()),
    }
}

/// A field of the request body map.
fn get<'a>(body: &'a V, name: &str) -> &'a V {
    match body {
        V::Map(pairs) => pairs.iter().find(|(k, _)| k.as_str() == Some(name)).map(|(_, v)| v).unwrap_or(&V::Null),
        _ => &V::Null,
    }
}

fn flag(body: &V, name: &str, default: bool) -> bool {
    match get(body, name) {
        V::Bool(b) => *b,
        V::Str(s) => s == "true",
        V::Null => default,
        other => other.as_u64().map(|n| n != 0).unwrap_or(default),
    }
}

fn text(body: &V, name: &str) -> String {
    get(body, name).as_str().unwrap_or_default().to_string()
}

fn arguments(body: &V) -> FieldTable {
    match to_field(get(body, "arguments")) {
        AMQPValue::FieldTable(t) => t,
        _ => FieldTable::default(),
    }
}

/// A refused 0-9-1 method as a management outcome. The channel is gone after it.
fn refused(e: &lapin::Error) -> Outcome {
    match e {
        lapin::Error::ProtocolError(err) => {
            let message = err.get_message().as_str().to_string();
            match err.get_id() {
                403 => Outcome::Refused(message),
                404 => Outcome::Reply("404", V::Str(message)),
                405 | 406 => Outcome::Reply("409", V::Str(message)),
                530 | 320 => Outcome::Fail("amqp:unauthorized-access", message),
                _ => Outcome::Reply("400", V::Str(message)),
            }
        }
        other => Outcome::Fail("amqp:internal-error", other.to_string()),
    }
}

/// The node's name, for a queue's leader and replicas.
fn node() -> String {
    super::super::node_name()
}

/// Run one request on the loopback connection. `ch` is reused until a refusal closes it.
pub(super) async fn handle(amqp: &Connection, ch: &mut Option<Channel>, vhost: &str, req: &Request) -> Outcome {
    let (path, query) = req.to.split_once('?').unwrap_or((req.to.as_str(), ""));
    let parts: Vec<&str> = path.split('/').skip(1).collect();
    if req.method == "PUT" && parts == ["auth", "tokens"] {
        let token = match &req.body {
            V::Binary(b) => String::from_utf8_lossy(b).into_owned(),
            V::Str(s) => s.clone(),
            _ => return Outcome::Reply("400", V::Str("the body must be the new token as binary".into())),
        };
        return match amqp.update_secret(&token, "AMQP 1.0 token refresh").await {
            Ok(()) => Outcome::Reply("204", V::Null),
            Err(e) => Outcome::Fail("amqp:unauthorized-access", format!("token refresh failed: {e}")),
        };
    }
    if ch.as_ref().is_none_or(|c| !c.status().connected()) {
        match amqp.create_channel().await {
            Ok(c) => *ch = Some(c),
            Err(e) => return Outcome::Fail("amqp:internal-error", e.to_string()),
        }
    }
    let Some(c) = ch.as_ref() else { return Outcome::Fail("amqp:internal-error", "no channel".into()) };
    let out = route(c, vhost, &req.method, &parts, query, &req.body).await;
    if matches!(out, Outcome::Refused(_)) || !c.status().connected() {
        *ch = None;
    }
    out
}

async fn route(c: &Channel, vhost: &str, method: &str, parts: &[&str], query: &str, body: &V) -> Outcome {
    match (method, parts) {
        ("PUT", ["queues", q]) => declare_queue(c, vhost, &segment(q), body).await,
        ("GET", ["queues", q]) => get_queue(c, vhost, &segment(q)).await,
        ("DELETE", ["queues", q]) => match c.queue_delete(&segment(q), QueueDeleteOptions::default()).await {
            Ok(n) => Outcome::Reply("200", V::Map(vec![(key("message_count"), V::Ulong(u64::from(n)))])),
            Err(e) => refused(&e),
        },
        ("DELETE", ["queues", q, "messages"]) => match c.queue_purge(&segment(q), QueuePurgeOptions::default()).await {
            Ok(n) => Outcome::Reply("200", V::Map(vec![(key("message_count"), V::Ulong(u64::from(n)))])),
            Err(e) => refused(&e),
        },
        ("PUT", ["exchanges", x]) => {
            let kind = match text(body, "type").as_str() {
                "" | "direct" => ExchangeKind::Direct,
                "fanout" => ExchangeKind::Fanout,
                "topic" => ExchangeKind::Topic,
                "headers" => ExchangeKind::Headers,
                other => ExchangeKind::Custom(other.to_string()),
            };
            let opts = ExchangeDeclareOptions {
                durable: flag(body, "durable", true),
                auto_delete: flag(body, "auto_delete", false),
                internal: flag(body, "internal", false),
                ..Default::default()
            };
            match c.exchange_declare(&segment(x), kind, opts, arguments(body)).await {
                Ok(()) => Outcome::Reply("204", V::Null),
                Err(e) => refused(&e),
            }
        }
        ("DELETE", ["exchanges", x]) => match c.exchange_delete(&segment(x), ExchangeDeleteOptions::default()).await {
            Ok(()) => Outcome::Reply("204", V::Null),
            Err(e) => refused(&e),
        },
        ("POST", ["bindings"]) => {
            let source = text(body, "source");
            let key = text(body, "binding_key");
            let args = arguments(body);
            let done = match (get(body, "destination_queue").as_str(), get(body, "destination_exchange").as_str()) {
                (Some(q), _) => c.queue_bind(q, &source, &key, QueueBindOptions::default(), args).await,
                (None, Some(x)) => c.exchange_bind(x, &source, &key, ExchangeBindOptions::default(), args).await,
                _ => return Outcome::Reply("400", V::Str("a binding needs destination_queue or destination_exchange".into())),
            };
            match done {
                Ok(()) => Outcome::Reply("204", V::Null),
                Err(e) => refused(&e),
            }
        }
        ("DELETE", ["bindings", spec]) => {
            let b = BindingSpec::parse(spec);
            let done = match (&b.queue, &b.exchange) {
                (Some(q), _) => c.queue_unbind(q, &b.source, &b.key, FieldTable::default()).await,
                (None, Some(x)) => c.exchange_unbind(x, &b.source, &b.key, ExchangeUnbindOptions::default(), FieldTable::default()).await,
                _ => return Outcome::Reply("400", V::Str("a binding needs dstq or dste".into())),
            };
            match done {
                Ok(()) => Outcome::Reply("204", V::Null),
                Err(e) => refused(&e),
            }
        }
        ("GET", ["bindings"]) => list_bindings(vhost, query),
        (_, ["queues", ..] | ["exchanges", ..] | ["bindings", ..]) => Outcome::Reply("405", V::Str(format!("{method} is not allowed here"))),
        _ => Outcome::Reply("400", V::Str(format!("unknown path /{}", parts.join("/")))),
    }
}

/// A binding named by `src=..;dstq=..;key=..;args=` (or the query-string form).
#[derive(Debug, Default, PartialEq)]
struct BindingSpec {
    source: String,
    queue: Option<String>,
    exchange: Option<String>,
    key: String,
}

impl BindingSpec {
    fn parse(spec: &str) -> Self {
        let mut b = BindingSpec::default();
        for pair in spec.split([';', '&']) {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            let v = segment(v);
            match k {
                "src" => b.source = v,
                "dstq" => b.queue = Some(v),
                "dste" => b.exchange = Some(v),
                "key" => b.key = v,
                _ => {}
            }
        }
        b
    }
}

fn list_bindings(vhost: &str, query: &str) -> Outcome {
    let want = BindingSpec::parse(query);
    let Some(store) = super::super::mqtt::store() else { return Outcome::Reply("200", V::List(Vec::new())) };
    let all = store.list_bindings(vhost).unwrap_or_default();
    let found = all
        .into_iter()
        .filter(|b| want.source.is_empty() || b.exchange.as_str() == want.source)
        .filter(|b| want.queue.as_deref().is_none_or(|q| b.queue.as_str() == q))
        .filter(|_| want.exchange.is_none())
        .filter(|b| !query.contains("key=") || b.routing_key.as_str() == want.key)
        .map(|b| {
            V::Map(vec![
                (key("source"), V::Str(b.exchange.to_string())),
                (key("vhost"), V::Str(b.vhost.to_string())),
                (key("destination_queue"), V::Str(b.queue.to_string())),
                (key("binding_key"), V::Str(b.routing_key.to_string())),
                (key("arguments"), V::Map(Vec::new())),
            ])
        })
        .collect();
    Outcome::Reply("200", V::List(found))
}

/// The queue map RabbitMQ replies with.
fn queue_map(vhost: &str, name: &str, messages: u32, consumers: u32, fallback: Option<(bool, bool, bool, &V)>) -> V {
    let stored = super::super::mqtt::store().and_then(|s| s.get_queue(vhost, name).ok().flatten());
    let (durable, auto_delete, exclusive, kind, args) = match (&stored, fallback) {
        (Some(q), _) => (
            q.durable,
            q.auto_delete,
            q.exclusive,
            q.args.queue_type.map(|t| t.as_str()).unwrap_or("classic").to_string(),
            json(&serde_json::Value::Object(crate::definitions::queue_args_map(&q.args))),
        ),
        (None, Some((d, a, x, body))) => {
            let args = get(body, "arguments").clone();
            let kind = get(&args, "x-queue-type").as_str().unwrap_or("classic").to_string();
            (d, a, x, kind, if args.is_null() { V::Map(Vec::new()) } else { args })
        }
        (None, None) => (true, false, false, "classic".to_string(), V::Map(Vec::new())),
    };
    let node = node();
    V::Map(vec![
        (key("name"), V::Str(name.into())),
        (key("vhost"), V::Str(vhost.into())),
        (key("durable"), V::Bool(durable)),
        (key("auto_delete"), V::Bool(auto_delete)),
        (key("exclusive"), V::Bool(exclusive)),
        (key("type"), V::Str(kind)),
        (key("arguments"), args),
        (key("message_count"), V::Ulong(u64::from(messages))),
        (key("consumer_count"), V::Uint(consumers)),
        (key("leader"), V::Str(node.clone())),
        (key("replicas"), V::List(vec![V::Str(node)])),
    ])
}

async fn declare_queue(c: &Channel, vhost: &str, name: &str, body: &V) -> Outcome {
    let existed = match super::super::mqtt::store() {
        Some(store) => matches!(store.get_queue(vhost, name), Ok(Some(_))),
        None => false,
    };
    let durable = flag(body, "durable", true);
    let auto_delete = flag(body, "auto_delete", false);
    let exclusive = flag(body, "exclusive", false);
    let opts = QueueDeclareOptions { durable, auto_delete, exclusive, ..Default::default() };
    match c.queue_declare(name, opts, arguments(body)).await {
        Ok(q) => {
            let map = queue_map(vhost, name, q.message_count(), q.consumer_count(), Some((durable, auto_delete, exclusive, body)));
            Outcome::Reply(if existed { "200" } else { "201" }, map)
        }
        Err(e) => refused(&e),
    }
}

async fn get_queue(c: &Channel, vhost: &str, name: &str) -> Outcome {
    if let Some(store) = super::super::mqtt::store() {
        if !matches!(store.get_queue(vhost, name), Ok(Some(_))) {
            return Outcome::Reply("404", V::Str(format!("queue '{name}' in vhost '{vhost}' does not exist")));
        }
    }
    let passive = QueueDeclareOptions { passive: true, ..Default::default() };
    match c.queue_declare(name, passive, FieldTable::default()).await {
        Ok(q) => Outcome::Reply("200", queue_map(vhost, name, q.message_count(), q.consumer_count(), None)),
        Err(e) => refused(&e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_request_and_encodes_its_reply() {
        let mut e = Encoder::new();
        e.value(&V::described(
            0x73,
            V::List(vec![V::Str("req-1".into()), V::Null, V::Str("/queues/a%2Fb".into()), V::Str("put".into()), V::Str("$me".into())]),
        ));
        e.value(&V::described(0x77, V::Map(vec![(key("durable"), V::Bool(true))])));
        let req = parse(&e.buf).unwrap();
        assert_eq!(req.method, "PUT");
        assert_eq!(req.to, "/queues/a%2Fb");
        assert!(flag(&req.body, "durable", false));
        let out = reply(&req.id, "201", &V::Null);
        let mut d = Decoder::new(&out);
        let props = d.value().unwrap();
        assert_eq!(props.field(3).as_str(), Some("201"));
        assert_eq!(props.field(5), &V::Str("req-1".into()));
    }

    #[test]
    fn parses_binding_specs() {
        let b = BindingSpec::parse("src=x%201;dstq=q;key=k1;args=");
        assert_eq!(b, BindingSpec { source: "x 1".into(), queue: Some("q".into()), exchange: None, key: "k1".into() });
        let b = BindingSpec::parse("src=x;dste=y;key=");
        assert_eq!(b.exchange.as_deref(), Some("y"));
    }
}
