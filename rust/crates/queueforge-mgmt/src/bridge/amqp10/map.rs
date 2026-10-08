//! AMQP 1.0 messages to and from 0-9-1 properties, as RabbitMQ 4 maps them.
//!
//! A single data section is the 0-9-1 body. Properties map field for field.
//! Application properties become headers and come back as application
//! properties, except `x-` headers, which travel as message annotations.
//! Any other body (amqp-value, amqp-sequence, several data sections) is kept
//! as its encoded sections under [`SECTIONS_TYPE`], so a 1.0 consumer gets
//! it back unchanged.

use std::collections::BTreeMap;

use lapin::types::{AMQPValue, FieldTable, LongString, ShortString};
use lapin::BasicProperties;

use super::types::{Decoder, Encoder, V};

/// content-type of a 0-9-1 body that holds encoded AMQP 1.0 body sections.
pub const SECTIONS_TYPE: &str = "message/vnd.rabbitmq.amqp";

pub struct Inbound {
    pub body: Vec<u8>,
    pub props: BasicProperties,
    /// properties.to, for links with a null target.
    pub to: Option<String>,
    /// properties.subject, the routing key for an /exchanges/:x address.
    pub subject: Option<String>,
}

fn id_text(v: &V) -> Option<String> {
    match v {
        V::Str(s) | V::Sym(s) => Some(s.clone()),
        V::Binary(b) => Some(String::from_utf8_lossy(b).into_owned()),
        V::Uuid(u) => {
            let h: String = u.iter().map(|b| format!("{b:02x}")).collect();
            Some(format!("{}-{}-{}-{}-{}", &h[0..8], &h[8..12], &h[12..16], &h[16..20], &h[20..]))
        }
        V::Null => None,
        other => other.as_u64().map(|n| n.to_string()),
    }
}

fn text(v: &V) -> Option<String> {
    match v {
        V::Str(s) | V::Sym(s) => Some(s.clone()),
        V::Binary(b) => Some(String::from_utf8_lossy(b).into_owned()),
        _ => None,
    }
}

/// One application property or annotation as a 0-9-1 field.
fn to_field(v: &V) -> AMQPValue {
    match v {
        V::Str(s) | V::Sym(s) => AMQPValue::LongString(LongString::from(s.as_bytes().to_vec())),
        V::Bool(b) => AMQPValue::Boolean(*b),
        V::Binary(b) => AMQPValue::ByteArray(b.clone().into()),
        V::Timestamp(ms) => AMQPValue::Timestamp((*ms / 1000).max(0) as u64),
        V::Double(d) => AMQPValue::Double(*d),
        V::Float(f) => AMQPValue::Float(*f),
        V::Null => AMQPValue::Void,
        V::Long(n) => AMQPValue::LongLongInt(*n),
        V::Int(n) => AMQPValue::LongInt(*n),
        other => match other.as_u64() {
            Some(n) => AMQPValue::LongLongInt(n as i64),
            None => AMQPValue::LongString(LongString::from(format!("{other:?}").into_bytes())),
        },
    }
}

/// A 0-9-1 field as an AMQP 1.0 simple value. Tables and arrays have no
/// application-property form and are dropped.
fn from_field(f: &AMQPValue) -> Option<V> {
    Some(match f {
        AMQPValue::LongString(s) => V::Str(String::from_utf8_lossy(s.as_bytes()).into_owned()),
        AMQPValue::ShortString(s) => V::Str(s.as_str().to_string()),
        AMQPValue::Boolean(b) => V::Bool(*b),
        AMQPValue::ShortShortInt(n) => V::Long(i64::from(*n)),
        AMQPValue::ShortShortUInt(n) => V::Long(i64::from(*n)),
        AMQPValue::ShortInt(n) => V::Long(i64::from(*n)),
        AMQPValue::ShortUInt(n) => V::Long(i64::from(*n)),
        AMQPValue::LongInt(n) => V::Long(i64::from(*n)),
        AMQPValue::LongUInt(n) => V::Long(i64::from(*n)),
        AMQPValue::LongLongInt(n) => V::Long(*n),
        AMQPValue::Float(n) => V::Double(f64::from(*n)),
        AMQPValue::Double(n) => V::Double(*n),
        AMQPValue::Timestamp(t) => V::Timestamp(*t as i64 * 1000),
        AMQPValue::ByteArray(b) => V::Binary(b.as_slice().to_vec()),
        AMQPValue::Void => V::Null,
        _ => return None,
    })
}

/// Parse every section of one message, frames already joined.
pub fn inbound(payload: &[u8]) -> Result<Inbound, String> {
    let mut d = Decoder::new(payload);
    let mut headers: BTreeMap<ShortString, AMQPValue> = BTreeMap::new();
    let mut props = BasicProperties::default();
    let mut persistent = false;
    let mut to = None;
    let mut subject = None;
    let mut data: Vec<Vec<u8>> = Vec::new();
    let mut other_body = false;
    let mut body_start = None;
    let mut body_end = payload.len();
    let mut expiration: Option<String> = None;
    while d.more() {
        let start = d.at;
        let section = d.value()?;
        let Some(code) = section.code() else { continue };
        let v = section.inner();
        match code {
            0x75..=0x77 => {
                body_start.get_or_insert(start);
                body_end = d.at;
                match (code, v) {
                    (0x75, V::Binary(b)) => data.push(b.clone()),
                    _ => other_body = true,
                }
            }
            0x70 => {
                persistent = section.field(0).as_bool();
                if let Some(p) = section.field(1).as_u64() {
                    props = props.with_priority(p.min(255) as u8);
                }
                if let Some(ttl) = section.field(2).as_u64() {
                    expiration = Some(ttl.to_string());
                }
            }
            0x72 => {
                if let V::Map(pairs) = v {
                    for (k, val) in pairs {
                        if let Some(key) = k.as_str() {
                            if key.starts_with("x-") && key != "x-exchange" && key != "x-routing-key" {
                                headers.insert(ShortString::from(key), to_field(val));
                            }
                        }
                    }
                }
            }
            0x73 => {
                if let Some(id) = id_text(section.field(0)) {
                    props = props.with_message_id(ShortString::from(id));
                }
                if let V::Binary(u) = section.field(1) {
                    props = props.with_user_id(ShortString::from(String::from_utf8_lossy(u).into_owned()));
                }
                to = text(section.field(2));
                subject = text(section.field(3));
                if let Some(r) = text(section.field(4)) {
                    props = props.with_reply_to(ShortString::from(r));
                }
                if let Some(c) = id_text(section.field(5)) {
                    props = props.with_correlation_id(ShortString::from(c));
                }
                if let Some(ct) = text(section.field(6)) {
                    props = props.with_content_type(ShortString::from(ct));
                }
                if let Some(ce) = text(section.field(7)) {
                    props = props.with_content_encoding(ShortString::from(ce));
                }
                if let V::Timestamp(created) = section.field(9) {
                    props = props.with_timestamp((*created / 1000).max(0) as u64);
                }
                if let (V::Timestamp(expiry), None) = (section.field(8), &expiration) {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_millis() as i64)
                        .unwrap_or(0);
                    expiration = Some((expiry - now).max(0).to_string());
                }
            }
            0x74 => {
                if let V::Map(pairs) = v {
                    for (k, val) in pairs {
                        if let Some(key) = k.as_str() {
                            headers.insert(ShortString::from(key), to_field(val));
                        }
                    }
                }
            }
            _ => {}
        }
    }
    let body = if !other_body && data.len() == 1 {
        data.pop().unwrap_or_default()
    } else if !other_body && data.is_empty() {
        Vec::new()
    } else {
        props = props.with_content_type(ShortString::from(SECTIONS_TYPE));
        payload[body_start.unwrap_or(0)..body_end].to_vec()
    };
    if let Some(e) = expiration {
        props = props.with_expiration(ShortString::from(e));
    }
    if !headers.is_empty() {
        props = props.with_headers(FieldTable::from(headers));
    }
    props = props.with_delivery_mode(if persistent { 2 } else { 1 });
    Ok(Inbound { body, props, to, subject })
}

/// Encode a delivery as a 1.0 transfer payload. Headers named in `skip` are left out.
pub fn outbound(exchange: &str, routing_key: &str, redelivered: bool, props: &BasicProperties, body: &[u8], skip: &[&str]) -> Vec<u8> {
    let mut e = Encoder::new();
    let durable = props.delivery_mode().unwrap_or(1) == 2;
    let ttl = props.expiration().as_ref().and_then(|x| x.as_str().parse::<u32>().ok());
    e.value(&V::described(
        0x70,
        V::List(vec![
            V::Bool(durable),
            V::Ubyte(props.priority().unwrap_or(4)),
            ttl.map(V::Uint).unwrap_or(V::Null),
            V::Bool(!redelivered),
            // delivery-count counts failed attempts; a plain requeue does not add to it.
            V::Uint(0),
        ]),
    ));
    let mut annotations = vec![(V::sym("x-exchange"), V::Str(exchange.into())), (V::sym("x-routing-key"), V::Str(routing_key.into()))];
    let mut app = Vec::new();
    if let Some(h) = props.headers() {
        for (k, f) in h.inner() {
            let Some(v) = from_field(f) else { continue };
            if skip.contains(&k.as_str()) {
                continue;
            }
            if k.as_str().starts_with("x-") {
                annotations.push((V::sym(k.as_str()), v));
            } else {
                app.push((V::Str(k.as_str().into()), v));
            }
        }
    }
    e.value(&V::described(0x72, V::Map(annotations)));
    let s = |x: &Option<ShortString>| x.as_ref().map(|v| V::Str(v.as_str().into())).unwrap_or(V::Null);
    let content_type = props.content_type().as_ref().map(|c| c.as_str()).filter(|c| *c != SECTIONS_TYPE);
    let mut fields = vec![
        s(props.message_id()),
        props.user_id().as_ref().map(|u| V::Binary(u.as_str().as_bytes().to_vec())).unwrap_or(V::Null),
        V::Null,
        V::Null,
        s(props.reply_to()),
        s(props.correlation_id()),
        content_type.map(V::sym).unwrap_or(V::Null),
        props.content_encoding().as_ref().map(|c| V::sym(c.as_str())).unwrap_or(V::Null),
        V::Null,
        props.timestamp().map(|t| V::Timestamp(t as i64 * 1000)).unwrap_or(V::Null),
    ];
    while fields.last() == Some(&V::Null) {
        fields.pop();
    }
    if !fields.is_empty() {
        e.value(&V::described(0x73, V::List(fields)));
    }
    if !app.is_empty() {
        e.value(&V::described(0x74, V::Map(app)));
    }
    if props.content_type().as_ref().map(|c| c.as_str()) == Some(SECTIONS_TYPE) {
        e.buf.extend_from_slice(body);
    } else {
        e.value(&V::described(0x75, V::Binary(body.to_vec())));
    }
    e.buf
}
