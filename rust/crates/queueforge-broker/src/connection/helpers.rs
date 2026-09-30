//! Pure helpers shared by the connection state machine.

use queueforge_amqp::{BasicProperties, FieldTable, FieldValue};
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
    let Some(value) = table.entries.iter().find(|(key, _)| key == name).map(|(_, value)| value) else {
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
        .filter_map(|(k, v)| header_arg(v).map(|arg| (compact_str::CompactString::from(k.as_str()), arg)))
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

/// Body plus basic-property bytes counted against `max_message_bytes`.
pub(super) fn properties_header_bytes(props: &BasicProperties) -> u64 {
    let mut n = 0u64;
    for s in [
        props.content_type.as_deref(),
        props.content_encoding.as_deref(),
        props.correlation_id.as_deref(),
        props.reply_to.as_deref(),
        props.expiration.as_deref(),
        props.message_id.as_deref(),
        props.type_.as_deref(),
        props.user_id.as_deref(),
        props.app_id.as_deref(),
    ]
    .into_iter()
    .flatten()
    {
        n = n.saturating_add(s.len() as u64);
    }
    if let Some(table) = &props.headers {
        n = n.saturating_add(field_table_bytes(table));
    }
    n
}

fn field_table_bytes(table: &FieldTable) -> u64 {
    table.entries.iter().fold(0u64, |n, (k, v)| {
        n.saturating_add(k.len() as u64)
            .saturating_add(field_value_bytes(v))
    })
}

fn field_value_bytes(v: &FieldValue) -> u64 {
    match v {
        FieldValue::Void | FieldValue::Bool(_) => 1,
        FieldValue::I8(_) | FieldValue::U8(_) => 1,
        FieldValue::I16(_) | FieldValue::U16(_) => 2,
        FieldValue::I32(_) | FieldValue::U32(_) | FieldValue::Decimal { .. } => 4,
        FieldValue::I64(_) | FieldValue::U64(_) | FieldValue::F64(_) | FieldValue::Timestamp(_) => {
            8
        }
        FieldValue::F32(_) => 4,
        FieldValue::ShortString(s) => s.len() as u64,
        FieldValue::LongString(b) | FieldValue::Bytes(b) => b.len() as u64,
        FieldValue::Array(items) => items
            .iter()
            .fold(0u64, |n, i| n.saturating_add(field_value_bytes(i))),
        FieldValue::Table(t) => field_table_bytes(t),
    }
}

pub(super) fn message_to_properties(msg: &queueforge_core::Message) -> BasicProperties {
    BasicProperties {
        content_type: msg.content_type.as_ref().map(|s| s.to_string()),
        content_encoding: msg.content_encoding.as_ref().map(|s| s.to_string()),
        headers: headers_to_field_table(&msg.headers),
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

/// Client application headers, then broker death keys (death wins on conflict).
pub(super) fn headers_to_field_table(
    headers: &queueforge_core::MessageHeaders,
) -> Option<FieldTable> {
    let mut table = app_headers_to_field_table(&headers.app);
    if let Some(death) = death_headers_to_field_table(headers) {
        for (k, v) in death.entries {
            table.insert(k, v);
        }
    }
    if table.is_empty() {
        None
    } else {
        Some(table)
    }
}

fn app_headers_to_field_table(
    app: &[(compact_str::CompactString, queueforge_core::AppHeaderValue)],
) -> FieldTable {
    let mut table = FieldTable::new();
    for (k, v) in app {
        if is_death_header_key(k) {
            continue;
        }
        table.insert(k.as_str(), app_value_to_field(v));
    }
    table
}

fn is_death_header_key(key: &str) -> bool {
    matches!(
        key,
        "x-death" | "x-first-death-reason" | "x-first-death-queue" | "x-first-death-exchange"
    )
}

pub(super) fn field_table_to_app_headers(
    table: &FieldTable,
) -> Vec<(compact_str::CompactString, queueforge_core::AppHeaderValue)> {
    let mut out = Vec::new();
    for (k, v) in &table.entries {
        if is_death_header_key(k) {
            continue;
        }
        out.push((
            compact_str::CompactString::from(k.as_str()),
            field_to_app_value(v),
        ));
    }
    out
}

fn field_to_app_value(v: &FieldValue) -> queueforge_core::AppHeaderValue {
    use queueforge_core::AppHeaderValue;
    match v {
        FieldValue::Void => AppHeaderValue::Null,
        FieldValue::Bool(b) => AppHeaderValue::Bool(*b),
        FieldValue::I8(n) => AppHeaderValue::I64(i64::from(*n)),
        FieldValue::U8(n) => AppHeaderValue::I64(i64::from(*n)),
        FieldValue::I16(n) => AppHeaderValue::I64(i64::from(*n)),
        FieldValue::U16(n) => AppHeaderValue::I64(i64::from(*n)),
        FieldValue::I32(n) => AppHeaderValue::I64(i64::from(*n)),
        FieldValue::U32(n) => AppHeaderValue::I64(i64::from(*n)),
        FieldValue::I64(n) => AppHeaderValue::I64(*n),
        FieldValue::U64(n) => AppHeaderValue::I64(*n as i64),
        FieldValue::F32(n) => AppHeaderValue::F64Bits(f64::from(*n).to_bits()),
        FieldValue::F64(n) => AppHeaderValue::F64Bits(n.to_bits()),
        FieldValue::Decimal { scale, value } => {
            AppHeaderValue::Str(format!("decimal:{scale}:{value}"))
        }
        FieldValue::ShortString(s) => AppHeaderValue::Str(s.clone()),
        FieldValue::LongString(b) => match std::str::from_utf8(b) {
            Ok(s) => AppHeaderValue::Str(s.to_string()),
            Err(_) => AppHeaderValue::Bytes(b.clone()),
        },
        FieldValue::Bytes(b) => AppHeaderValue::Bytes(b.clone()),
        FieldValue::Timestamp(t) => AppHeaderValue::I64(*t as i64),
        FieldValue::Array(items) => {
            AppHeaderValue::Array(items.iter().map(field_to_app_value).collect())
        }
        FieldValue::Table(t) => AppHeaderValue::Table(
            t.entries
                .iter()
                .map(|(k, v)| (k.clone(), field_to_app_value(v)))
                .collect(),
        ),
    }
}

fn app_value_to_field(v: &queueforge_core::AppHeaderValue) -> FieldValue {
    use queueforge_core::AppHeaderValue;
    match v {
        AppHeaderValue::Null => FieldValue::Void,
        AppHeaderValue::Bool(b) => FieldValue::Bool(*b),
        AppHeaderValue::I64(n) => FieldValue::I64(*n),
        AppHeaderValue::F64Bits(bits) => FieldValue::F64(f64::from_bits(*bits)),
        AppHeaderValue::Str(s) => FieldValue::long_str(s.clone()),
        AppHeaderValue::Bytes(b) => FieldValue::Bytes(b.clone()),
        AppHeaderValue::Array(items) => {
            FieldValue::Array(items.iter().map(app_value_to_field).collect())
        }
        AppHeaderValue::Table(entries) => {
            let mut table = FieldTable::new();
            for (k, v) in entries {
                table.insert(k.as_str(), app_value_to_field(v));
            }
            FieldValue::Table(table)
        }
    }
}

/// Convert core death headers into an AMQP field table (`x-death`, first-death).
pub(super) fn death_headers_to_field_table(
    headers: &queueforge_core::MessageHeaders,
) -> Option<FieldTable> {
    let death_empty = headers.deaths.is_empty()
        && headers.first_death_reason.is_none()
        && headers.first_death_queue.is_none()
        && headers.first_death_exchange.is_none();
    if death_empty {
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
