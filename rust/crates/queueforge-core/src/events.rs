//! Event exchange (`amq.rabbitmq.event`) and firehose tracing
//! (`amq.rabbitmq.trace`), as RabbitMQ's plugin and `rabbitmqctl trace_on` do.
//!
//! Both are internal topic exchanges. The broker publishes into them here;
//! clients may bind to them but not publish. A message nobody is bound to
//! costs one routing lookup.

use std::sync::Arc;

use bytes::Bytes;
use compact_str::CompactString;
use tokio::sync::oneshot;

use crate::queue::{AppHeaderValue, Message, QueueCmd, QueueRegistry};
use crate::router::ExchangeRouter;

/// RabbitMQ's event exchange. Events from every vhost go to the one in `/`.
pub const EVENT_EXCHANGE: &str = "amq.rabbitmq.event";
/// RabbitMQ's firehose exchange, one per vhost.
pub const TRACE_EXCHANGE: &str = "amq.rabbitmq.trace";
const EVENT_VHOST: &str = "/";

/// Publish one broker-made message to an internal exchange on this node.
/// Errors are dropped: an event or a trace copy never fails its cause.
pub fn publish_internal(
    router: &ExchangeRouter,
    queues: &Arc<QueueRegistry>,
    vhost: &str,
    exchange: &str,
    routing_key: &str,
    headers: Vec<(CompactString, AppHeaderValue)>,
    body: Bytes,
) {
    let Ok(route) = router.route_with_headers(vhost, exchange, routing_key, &[]) else {
        return;
    };
    if route.destinations.is_empty() {
        return;
    }
    let mut msg = Message::blank();
    msg.exchange = CompactString::from(exchange);
    msg.routing_key = CompactString::from(routing_key);
    msg.body = body;
    msg.timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .map(|d| d.as_secs());
    msg.headers.app = headers;
    let msg = Arc::new(msg);
    for key in route.destinations {
        let Some(handle) = queues.get(&key) else { continue };
        let msg = Arc::clone(&msg);
        tokio::spawn(async move {
            let (reply, done) = oneshot::channel();
            if handle.tx.send(QueueCmd::Enqueue { msg, reply }).await.is_ok() {
                let _ = done.await;
            }
        });
    }
}

/// Emit an `amq.rabbitmq.event` message, such as `queue.created`.
pub fn emit_event(
    router: &ExchangeRouter,
    queues: &Arc<QueueRegistry>,
    key: &str,
    vhost: &str,
    fields: Vec<(&str, AppHeaderValue)>,
) {
    let mut headers: Vec<(CompactString, AppHeaderValue)> =
        vec![(CompactString::from("vhost"), AppHeaderValue::Str(vhost.to_string()))];
    headers.extend(fields.into_iter().map(|(k, v)| (CompactString::from(k), v)));
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    headers.push((CompactString::from("timestamp_in_ms"), AppHeaderValue::I64(now_ms)));
    publish_internal(router, queues, EVENT_VHOST, EVENT_EXCHANGE, key, headers, Bytes::new());
}

/// Copy one publish to `amq.rabbitmq.trace` as `publish.<exchange>`, with
/// the headers RabbitMQ's firehose adds.
#[allow(clippy::too_many_arguments)]
pub fn trace_publish(
    router: &ExchangeRouter,
    queues: &Arc<QueueRegistry>,
    vhost: &str,
    exchange: &str,
    routing_key: &str,
    user: &str,
    node: &str,
    app_headers: &[(CompactString, AppHeaderValue)],
    body: Bytes,
) {
    let props = app_headers
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect::<Vec<_>>();
    let headers = vec![
        (CompactString::from("exchange_name"), AppHeaderValue::Str(exchange.to_string())),
        (
            CompactString::from("routing_keys"),
            AppHeaderValue::Array(vec![AppHeaderValue::Str(routing_key.to_string())]),
        ),
        (
            CompactString::from("properties"),
            AppHeaderValue::Table(vec![("headers".to_string(), AppHeaderValue::Table(props))]),
        ),
        (CompactString::from("node"), AppHeaderValue::Str(node.to_string())),
        (CompactString::from("vhost"), AppHeaderValue::Str(vhost.to_string())),
        (CompactString::from("user"), AppHeaderValue::Str(user.to_string())),
    ];
    publish_internal(router, queues, vhost, TRACE_EXCHANGE, &format!("publish.{exchange}"), headers, body);
}

/// Copy one delivery from `queue` to `amq.rabbitmq.trace` as
/// `deliver.<queue>`, as RabbitMQ's firehose does. A message published to
/// the trace exchange itself is not traced again.
#[allow(clippy::too_many_arguments)]
pub fn trace_deliver(
    router: &ExchangeRouter,
    queues: &Arc<QueueRegistry>,
    vhost: &str,
    queue: &str,
    node: &str,
    message: &crate::Message,
) {
    if message.exchange.as_str() == TRACE_EXCHANGE {
        return;
    }
    let props = message
        .headers
        .app
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect::<Vec<_>>();
    let headers = vec![
        (CompactString::from("exchange_name"), AppHeaderValue::Str(message.exchange.to_string())),
        (
            CompactString::from("routing_keys"),
            AppHeaderValue::Array(vec![AppHeaderValue::Str(message.routing_key.to_string())]),
        ),
        (
            CompactString::from("properties"),
            AppHeaderValue::Table(vec![("headers".to_string(), AppHeaderValue::Table(props))]),
        ),
        (CompactString::from("node"), AppHeaderValue::Str(node.to_string())),
        (CompactString::from("redelivered"), AppHeaderValue::Bool(message.redelivered)),
        (CompactString::from("vhost"), AppHeaderValue::Str(vhost.to_string())),
    ];
    publish_internal(router, queues, vhost, TRACE_EXCHANGE, &format!("deliver.{queue}"), headers, message.body.clone());
}
