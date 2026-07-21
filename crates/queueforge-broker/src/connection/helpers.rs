//! Pure helpers shared by the connection state machine.

use queueforge_amqp::{BasicProperties, FieldTable, FieldValue};
use queueforge_core::{ArgValue, Error as CoreError, QueueArgs};
use tokio::time::{sleep_until, Instant};

use super::reply::*;
use super::{DEFAULT_CHANNEL_MAX, DEFAULT_FRAME_MAX, FRAME_MAX_FLOOR};

pub(super) fn core_error_to_amqp(err: &CoreError) -> (u16, String) {
    match err {
        CoreError::NotFound(msg) => (REPLY_NOT_FOUND, format!("NOT_FOUND - {msg}")),
        CoreError::ResourceLocked(msg) => {
            (REPLY_RESOURCE_LOCKED, format!("RESOURCE_LOCKED - {msg}"))
        }
        CoreError::PreconditionFailed(msg) => (
            REPLY_PRECONDITION_FAILED,
            format!("PRECONDITION_FAILED - {msg}"),
        ),
        CoreError::AlreadyExists(msg) => (
            REPLY_PRECONDITION_FAILED,
            format!("PRECONDITION_FAILED - {msg}"),
        ),
        CoreError::Resource(msg) => (REPLY_RESOURCE_ERROR, format!("RESOURCE_ERROR - {msg}")),
        CoreError::Unavailable(msg) => (REPLY_INTERNAL_ERROR, format!("INTERNAL_ERROR - {msg}")),
        CoreError::NotImplemented(msg) => {
            (REPLY_NOT_IMPLEMENTED, format!("NOT_IMPLEMENTED - {msg}"))
        }
        other => (REPLY_INTERNAL_ERROR, format!("INTERNAL_ERROR - {other}")),
    }
}

/// Parse SASL PLAIN response: `\0username\0password` (optional authzid prefix).
///
/// Accepts both `\0user\0pass` and `authzid\0user\0pass`.
pub(super) fn parse_plain_response(response: &[u8]) -> Result<(String, String), &'static str> {
    // Split on NUL into at most 3 parts.
    let parts: Vec<&[u8]> = response.split(|&b| b == 0).collect();
    // Common forms:
    //   ["", user, pass]            → \0user\0pass
    //   [authzid, user, pass]       → authzid\0user\0pass
    //   [user, pass] is invalid for PLAIN (must have leading empty or authzid)
    let (user, pass) = match parts.as_slice() {
        [_, user, pass] => (*user, *pass),
        // Some clients omit the authzid field entirely as user\0pass — accept it.
        [user, pass] => (*user, *pass),
        _ => return Err("ACCESS_REFUSED - malformed PLAIN response"),
    };
    if user.is_empty() {
        return Err("ACCESS_REFUSED - empty username");
    }
    let username = std::str::from_utf8(user)
        .map_err(|_| "ACCESS_REFUSED - username not UTF-8")?
        .to_string();
    let password = std::str::from_utf8(pass)
        .map_err(|_| "ACCESS_REFUSED - password not UTF-8")?
        .to_string();
    Ok((username, password))
}

pub(super) fn negotiate_channel_max(server: u16, client: u16) -> u16 {
    match (server, client) {
        (0, 0) => DEFAULT_CHANNEL_MAX,
        (0, c) => c,
        (s, 0) => s,
        (s, c) => s.min(c),
    }
}

pub(super) fn negotiate_frame_max(server: u32, client: u32) -> u32 {
    let raw = match (server, client) {
        (0, 0) => DEFAULT_FRAME_MAX,
        (0, c) => c,
        (s, 0) => s,
        (s, c) => s.min(c),
    };
    raw.max(FRAME_MAX_FLOOR)
}

/// Heartbeat negotiation: effective value is `min(server, client)`.
/// Either side proposing `0` disables heartbeats (design + RabbitMQ).
pub(super) fn negotiate_heartbeat(server: u16, client: u16) -> u16 {
    server.min(client)
}

pub(super) async fn sleep_until_opt(deadline: Option<Instant>) {
    if let Some(d) = deadline {
        sleep_until(d).await;
    } else {
        // Never resolves; used only when the branch is disabled via `if`.
        std::future::pending::<()>().await;
    }
}

/// Parse the closed queue declare-arguments set into [`QueueArgs`].
pub(super) fn parse_queue_declare_args(table: &FieldTable) -> Result<QueueArgs, String> {
    if table.is_empty() {
        return Ok(QueueArgs::default());
    }
    // Collect owned (name, value) so string args outlive the iterator.
    let mut owned: Vec<(String, OwnedArg)> = Vec::with_capacity(table.entries.len());
    for (k, v) in &table.entries {
        let val = match v {
            FieldValue::I8(n) => OwnedArg::Long(i64::from(*n)),
            FieldValue::U8(n) => OwnedArg::Long(i64::from(*n)),
            FieldValue::I16(n) => OwnedArg::Long(i64::from(*n)),
            FieldValue::U16(n) => OwnedArg::Long(i64::from(*n)),
            FieldValue::I32(n) => OwnedArg::Long(i64::from(*n)),
            FieldValue::U32(n) => OwnedArg::Long(i64::from(*n)),
            FieldValue::I64(n) => OwnedArg::Long(*n),
            FieldValue::U64(n) if *n <= i64::MAX as u64 => OwnedArg::Long(*n as i64),
            FieldValue::ShortString(s) => OwnedArg::Str(s.clone()),
            FieldValue::LongString(bytes) => match std::str::from_utf8(bytes) {
                Ok(s) => OwnedArg::Str(s.to_string()),
                Err(_) => return Err(format!("argument '{k}' is not valid UTF-8")),
            },
            _ => return Err(format!("unsupported type for queue argument '{k}'")),
        };
        owned.push((k.clone(), val));
    }
    let pairs: Vec<(&str, ArgValue<'_>)> = owned
        .iter()
        .map(|(k, v)| {
            (
                k.as_str(),
                match v {
                    OwnedArg::Long(n) => ArgValue::Long(*n),
                    OwnedArg::Str(s) => ArgValue::Str(s.as_str()),
                },
            )
        })
        .collect();
    QueueArgs::parse(pairs).map_err(|e| e.to_string().replace("precondition failed: ", ""))
}

enum OwnedArg {
    Long(i64),
    Str(String),
}

pub(super) fn message_to_properties(msg: &queueforge_core::Message) -> BasicProperties {
    BasicProperties {
        content_type: msg.content_type.as_ref().map(|s| s.to_string()),
        content_encoding: msg.content_encoding.as_ref().map(|s| s.to_string()),
        headers: death_headers_to_field_table(&msg.headers),
        delivery_mode: if msg.persistent { Some(2) } else { None },
        priority: msg.priority,
        correlation_id: msg.correlation_id.as_ref().map(|s| s.to_string()),
        reply_to: msg.reply_to.as_ref().map(|s| s.to_string()),
        expiration: msg.expiration.as_ref().map(|s| s.to_string()),
        message_id: msg.message_id.as_ref().map(|s| s.to_string()),
        timestamp: msg.timestamp,
        type_: msg.type_.as_ref().map(|s| s.to_string()),
        user_id: msg.user_id.as_ref().map(|s| s.to_string()),
        app_id: msg.app_id.as_ref().map(|s| s.to_string()),
        cluster_id: None,
    }
}

/// Convert core death headers into an AMQP field table (`x-death`, first-death).
pub(super) fn death_headers_to_field_table(
    headers: &queueforge_core::MessageHeaders,
) -> Option<FieldTable> {
    if headers.is_empty() {
        return None;
    }
    let mut table = FieldTable::new();
    if !headers.deaths.is_empty() {
        let mut arr = Vec::with_capacity(headers.deaths.len());
        for d in &headers.deaths {
            let mut entry = FieldTable::new();
            entry.insert("queue", FieldValue::long_str(d.queue.as_str()));
            entry.insert("reason", FieldValue::long_str(d.reason.as_str()));
            entry.insert("time", FieldValue::Timestamp(d.time));
            entry.insert("exchange", FieldValue::long_str(d.exchange.as_str()));
            let rks: Vec<FieldValue> = d
                .routing_keys
                .iter()
                .map(|rk| FieldValue::long_str(rk.as_str()))
                .collect();
            entry.insert("routing-keys", FieldValue::Array(rks));
            entry.insert("count", FieldValue::I64(d.count as i64));
            arr.push(FieldValue::Table(entry));
        }
        table.insert("x-death", FieldValue::Array(arr));
    }
    if let Some(r) = headers.first_death_reason {
        table.insert("x-first-death-reason", FieldValue::long_str(r.as_str()));
    }
    if let Some(ref q) = headers.first_death_queue {
        table.insert("x-first-death-queue", FieldValue::long_str(q.as_str()));
    }
    if let Some(ref e) = headers.first_death_exchange {
        table.insert("x-first-death-exchange", FieldValue::long_str(e.as_str()));
    }
    Some(table)
}
