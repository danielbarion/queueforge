//! Pure helpers shared by the connection state machine.

use queueforge_amqp::{FieldTable, FieldValue};
use queueforge_core::{ArgValue, Error as CoreError, QueueArgs};
use tokio::time::{sleep_until, Instant};

use super::reply::*;
use super::{DEFAULT_CHANNEL_MAX, DEFAULT_FRAME_MAX, FRAME_MAX_FLOOR};

pub(super) fn alternate_exchange_arg(table: &FieldTable) -> Option<compact_str::CompactString> {
    table.entries.iter().find_map(|(k, v)| {
        if k != "alternate-exchange" {
            return None;
        }
        match v {
            FieldValue::ShortString(s) => Some(compact_str::CompactString::from(s.as_str())),
            FieldValue::LongString(b) => std::str::from_utf8(b)
                .ok()
                .map(compact_str::CompactString::from),
            _ => None,
        }
    })
}

pub(super) fn consumer_priority(table: &FieldTable) -> i32 {
    table
        .entries
        .iter()
        .find(|(key, _)| key == "x-priority")
        .and_then(|(_, value)| match value {
            FieldValue::I8(n) => Some(i32::from(*n)),
            FieldValue::U8(n) => Some(i32::from(*n)),
            FieldValue::I16(n) => Some(i32::from(*n)),
            FieldValue::U16(n) => Some(i32::from(*n)),
            FieldValue::I32(n) => Some(*n),
            FieldValue::U32(n) => i32::try_from(*n).ok(),
            FieldValue::I64(n) => i32::try_from(*n).ok(),
            _ => None,
        })
        .unwrap_or(0)
}

pub(super) fn routing_header_keys(table: &FieldTable, name: &str) -> Vec<String> {
    let Some(value) = table
        .entries
        .iter()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value)
    else {
        return Vec::new();
    };
    match value {
        FieldValue::Array(items) => items
            .iter()
            .filter_map(|item| match item {
                FieldValue::ShortString(text) => Some(text.clone()),
                FieldValue::LongString(bytes) => {
                    std::str::from_utf8(bytes).ok().map(str::to_string)
                }
                _ => None,
            })
            .collect(),
        FieldValue::ShortString(text) => vec![text.clone()],
        FieldValue::LongString(bytes) => std::str::from_utf8(bytes)
            .ok()
            .map(|text| vec![text.to_string()])
            .unwrap_or_default(),
        _ => Vec::new(),
    }
}

pub(super) fn field_table_to_header_args(
    table: &FieldTable,
) -> Vec<(compact_str::CompactString, queueforge_core::HeaderArg)> {
    table
        .entries
        .iter()
        .filter_map(|(k, v)| {
            header_arg(v).map(|arg| (compact_str::CompactString::from(k.as_str()), arg))
        })
        .collect()
}

fn header_arg(v: &FieldValue) -> Option<queueforge_core::HeaderArg> {
    match v {
        FieldValue::ShortString(s) => Some(queueforge_core::HeaderArg::Str(s.clone())),
        FieldValue::LongString(b) => std::str::from_utf8(b)
            .ok()
            .map(|s| queueforge_core::HeaderArg::Str(s.to_string())),
        FieldValue::I8(n) => Some(queueforge_core::HeaderArg::Int(i64::from(*n))),
        FieldValue::U8(n) => Some(queueforge_core::HeaderArg::Int(i64::from(*n))),
        FieldValue::I16(n) => Some(queueforge_core::HeaderArg::Int(i64::from(*n))),
        FieldValue::U16(n) => Some(queueforge_core::HeaderArg::Int(i64::from(*n))),
        FieldValue::I32(n) => Some(queueforge_core::HeaderArg::Int(i64::from(*n))),
        FieldValue::U32(n) => Some(queueforge_core::HeaderArg::Int(i64::from(*n))),
        FieldValue::I64(n) => Some(queueforge_core::HeaderArg::Int(*n)),
        FieldValue::Bool(b) => Some(queueforge_core::HeaderArg::Str(b.to_string())),
        _ => None,
    }
}

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
            FieldValue::Bool(b) => OwnedArg::Long(if *b { 1 } else { 0 }),
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

/// Resolve omitted `x-queue-type` and reject an illegal quorum declare.
pub(super) fn finalize_queue_type(
    args: &mut queueforge_core::QueueArgs,
    default_type: queueforge_core::QueueType,
    durable: bool,
    exclusive: bool,
) -> Result<(), String> {
    let resolved = match args.queue_type {
        Some(kind) => kind,
        None if default_type == queueforge_core::QueueType::Quorum && durable && !exclusive => {
            queueforge_core::QueueType::Quorum
        }
        None => queueforge_core::QueueType::Classic,
    };
    if resolved == queueforge_core::QueueType::Quorum && (!durable || exclusive) {
        return Err("quorum queue must be durable and non-exclusive".into());
    }
    args.queue_type = Some(resolved);
    if resolved == queueforge_core::QueueType::Quorum && args.delivery_limit.is_none() {
        args.delivery_limit = Some(20);
    }
    Ok(())
}

enum OwnedArg {
    Long(i64),
    Str(String),
}

pub(super) use super::headers::*;
