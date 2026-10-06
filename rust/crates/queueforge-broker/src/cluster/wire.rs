//! JSON bodies for quorum append, forget, and queue identity on the cluster socket.

use base64::Engine;
use bytes::Bytes;
use compact_str::CompactString;
use queueforge_core::{Message, QueueKey};
use serde_json::Value;

use super::{Inner, WireMessage, BASE64};

/// Report whether `inner` is the current quorum leader. Callers must not start a leader-only consume on a follower.
pub(super) fn node_is_quorum_leader(inner: &Inner) -> bool {
    if inner.member_list().is_empty() {
        return true;
    }
    let slot = inner.leader.lock().unwrap_or_else(|err| err.into_inner());
    !slot.is_empty() && *slot == inner.node_id
}

/// Build the consumed-set key for `key` and `message_id`. Returns the string stored in the replica map. Two different message ids must not share a key.
pub(super) fn replica_key(key: &QueueKey, message_id: &str) -> String {
    format!("{}\0{}\0{message_id}", key.vhost, key.name)
}

/// JSON body for a quorum forget of `message_id` on `key`. Returns the payload `call` sends. The peer drops that id only.
pub(super) fn wire_forget(key: &QueueKey, message_id: &str) -> Value {
    serde_json::json!({
        "vhost": key.vhost.as_str(),
        "queue": key.name.as_str(),
        "message_id": message_id,
        "id": message_id,
    })
}

/// Read the queue key from `payload`. Returns vhost and name. A missing field becomes an empty string, which will not match a real queue.
pub(super) fn key_from(payload: &Value) -> QueueKey {
    QueueKey::new(json_str(payload, "vhost"), json_str(payload, "queue"))
}

/// Read string `field` from `payload`. Returns an empty string when the field is absent or not a string.
pub(super) fn json_str(payload: &Value, field: &str) -> String {
    payload
        .get(field)
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string()
}

/// Version 1 quorum body. Both apps encode this and accept it without reading the sender's files.
pub fn encode_quorum_append(key: &QueueKey, message: &Message) -> Value {
    serde_json::json!({
        "v": 1,
        "vhost": key.vhost.as_str(),
        "queue": key.name.as_str(),
        "message_id": message.message_id.as_ref().map(|id| id.as_str()).unwrap_or(""),
        "body_b64": BASE64.encode(&message.body),
        "persistent": message.persistent,
        "routing_key": message.routing_key.as_str(),
        "exchange": message.exchange.as_str(),
    })
}

/// Decode a version-1 quorum append, or the older nested `message` / raw-body shapes.
pub fn decode_quorum_append(payload: &Value) -> Result<(QueueKey, Message), String> {
    let vhost = json_str(payload, "vhost");
    let queue = json_str(payload, "queue");
    if payload.get("body_b64").is_some() || payload.get("v").and_then(|v| v.as_u64()) == Some(1) {
        let body = BASE64
            .decode(json_str(payload, "body_b64").as_bytes())
            .map_err(|err| err.to_string())?;
        let mut message = Message::blank();
        message.body = Bytes::from(body);
        message.persistent = payload
            .get("persistent")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        message.routing_key = CompactString::from(json_str(payload, "routing_key"));
        message.exchange = CompactString::from(json_str(payload, "exchange"));
        let id = json_str(payload, "message_id");
        if !id.is_empty() {
            message.message_id = Some(CompactString::from(id));
        }
        return Ok((QueueKey::new(vhost, queue), message));
    }
    if let Some(nested) = payload.get("message") {
        let wire: WireMessage =
            serde_json::from_value(nested.clone()).map_err(|err| err.to_string())?;
        return Ok((QueueKey::new(vhost, queue), wire_to_message(wire)));
    }
    if payload.get("body").is_some() {
        let body = BASE64
            .decode(json_str(payload, "body").as_bytes())
            .map_err(|err| err.to_string())?;
        let mut message = Message::blank();
        message.body = Bytes::from(body);
        message.persistent = payload
            .get("persistent")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        let key_name = {
            let routing = json_str(payload, "routing_key");
            if routing.is_empty() {
                json_str(payload, "routingKey")
            } else {
                routing
            }
        };
        message.routing_key = CompactString::from(key_name);
        message.exchange = CompactString::from(json_str(payload, "exchange"));
        let id = {
            let message_id = json_str(payload, "message_id");
            if !message_id.is_empty() {
                message_id
            } else {
                let qid = json_str(payload, "qid");
                if qid.is_empty() {
                    json_str(payload, "id")
                } else {
                    qid
                }
            }
        };
        if !id.is_empty() {
            message.message_id = Some(CompactString::from(id));
        }
        return Ok((QueueKey::new(vhost, queue), message));
    }
    Err("quorum append has no body".into())
}

/// Convert `message` into the cluster wire struct. Returns the struct `encode_quorum_append` serializes. Body bytes are still raw here.
pub(super) fn message_to_wire(message: &Message) -> WireMessage {
    WireMessage {
        exchange: message.exchange.to_string(),
        routing_key: message.routing_key.to_string(),
        body_b64: BASE64.encode(&message.body),
        persistent: message.persistent,
        redelivered: message.redelivered,
        content_type: message.content_type.as_ref().map(|s| s.to_string()),
        content_encoding: message.content_encoding.as_ref().map(|s| s.to_string()),
        correlation_id: message.correlation_id.as_ref().map(|s| s.to_string()),
        message_id: message.message_id.as_ref().map(|s| s.to_string()),
        reply_to: message.reply_to.as_ref().map(|s| s.to_string()),
        expiration: message.expiration.as_ref().map(|s| s.to_string()),
        app_id: message.app_id.as_ref().map(|s| s.to_string()),
        user_id: message.user_id.as_ref().map(|s| s.to_string()),
        type_: message.type_.as_ref().map(|s| s.to_string()),
        priority: message.priority,
        timestamp: message.timestamp,
        expires_unix_ms: message.expires_unix_ms,
        headers: message.headers.clone(),
    }
}

/// Convert `wire` back into a queue message. Returns the message a peer append stores. Header fields the sender omitted stay empty.
pub(super) fn wire_to_message(wire: WireMessage) -> Message {
    let body = BASE64.decode(wire.body_b64.as_bytes()).unwrap_or_default();
    Message {
        exchange: CompactString::from(wire.exchange),
        routing_key: CompactString::from(wire.routing_key),
        body: Bytes::from(body),
        persistent: wire.persistent,
        redelivered: wire.redelivered,
        content_type: wire.content_type.map(CompactString::from),
        content_encoding: wire.content_encoding.map(CompactString::from),
        correlation_id: wire.correlation_id.map(CompactString::from),
        message_id: wire.message_id.map(CompactString::from),
        reply_to: wire.reply_to.map(CompactString::from),
        expiration: wire.expiration.map(CompactString::from),
        app_id: wire.app_id.map(CompactString::from),
        user_id: wire.user_id.map(CompactString::from),
        type_: wire.type_.map(CompactString::from),
        priority: wire.priority,
        timestamp: wire.timestamp,
        expires_unix_ms: wire.expires_unix_ms,
        headers: wire.headers,
    }
}

/// Copy `value` into an owned `String`. Returns `None` when the compact string is absent.
pub(super) fn opt_str(value: Option<&CompactString>) -> Option<String> {
    value.map(|s| s.to_string())
}

#[allow(dead_code)]
/// Forward `value` to `opt_str`. Returns the same option. Kept so the helper stays linked while no caller uses it.
fn _use_opt(value: Option<&CompactString>) -> Option<String> {
    opt_str(value)
}
