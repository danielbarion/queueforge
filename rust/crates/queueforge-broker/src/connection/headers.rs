//! Basic-property byte size and application header conversion.

use queueforge_amqp::{BasicProperties, FieldTable, FieldValue};

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
