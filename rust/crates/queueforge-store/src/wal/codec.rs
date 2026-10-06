//! Binary encode/decode for WAL records.

use std::io::Read;

use bytes::Bytes;
use compact_str::CompactString;
use crc32fast::Hasher;
use queueforge_core::{AppHeaderValue, DeathEntry, DeathReason, Message, MessageHeaders};
use serde::{Deserialize, Serialize};

use crate::error::{Result, StoreError};

/// Magic `VLRA` little-endian (`0x56_4C_52_41` as LE bytes of ASCII).
pub const WAL_MAGIC: u32 = u32::from_le_bytes(*b"VLRA");

/// Current record version.
pub const WAL_VERSION: u8 = 1;

/// Record type byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RecordType {
    /// Enqueue message payload.
    Enqueue = 1,
    /// Optional sparse ack watermark advance (v1 may omit).
    AckWatermark = 2,
}

impl RecordType {
    fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::Enqueue),
            2 => Some(Self::AckWatermark),
            _ => None,
        }
    }
}

/// Serialized message properties (body stored separately).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalProps {
    pub exchange: String,
    pub routing_key: String,
    pub persistent: bool,
    pub redelivered: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_encoding: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply_to: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expiration: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub app_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub type_: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<u64>,
    /// Absolute expiry. Missing on WAL records written before this field existed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at_unix_ms: Option<u64>,
    /// Dead-letter chain (`x-death` / first-death).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub headers: Option<WalHeaders>,
}

/// Serializable death-header subset for WAL props.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WalHeaders {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub deaths: Vec<DeathEntry>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_death_reason: Option<DeathReason>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_death_queue: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_death_exchange: Option<String>,
    /// Client application headers. Absent on records written before this field existed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub app: Vec<(String, AppHeaderValue)>,
}

impl From<&MessageHeaders> for WalHeaders {
    fn from(h: &MessageHeaders) -> Self {
        if h.is_empty() {
            return Self::default();
        }
        Self {
            deaths: h.deaths.clone(),
            first_death_reason: h.first_death_reason,
            first_death_queue: h.first_death_queue.as_ref().map(|s| s.to_string()),
            first_death_exchange: h.first_death_exchange.as_ref().map(|s| s.to_string()),
            app: h
                .app
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect(),
        }
    }
}

impl WalHeaders {
    fn into_message_headers(self) -> MessageHeaders {
        MessageHeaders {
            deaths: self.deaths,
            first_death_reason: self.first_death_reason,
            first_death_queue: self.first_death_queue.map(CompactString::from),
            first_death_exchange: self.first_death_exchange.map(CompactString::from),
            app: self
                .app
                .into_iter()
                .map(|(k, v)| (CompactString::from(k), v))
                .collect(),
        }
    }

    fn is_empty(&self) -> bool {
        self.deaths.is_empty()
            && self.first_death_reason.is_none()
            && self.first_death_queue.is_none()
            && self.first_death_exchange.is_none()
            && self.app.is_empty()
    }
}

impl From<&Message> for WalProps {
    fn from(m: &Message) -> Self {
        let headers = WalHeaders::from(&m.headers);
        Self {
            exchange: m.exchange.to_string(),
            routing_key: m.routing_key.to_string(),
            persistent: m.persistent,
            redelivered: m.redelivered,
            content_type: m.content_type.as_ref().map(|s| s.to_string()),
            content_encoding: m.content_encoding.as_ref().map(|s| s.to_string()),
            correlation_id: m.correlation_id.as_ref().map(|s| s.to_string()),
            message_id: m.message_id.as_ref().map(|s| s.to_string()),
            reply_to: m.reply_to.as_ref().map(|s| s.to_string()),
            expiration: m.expiration.as_ref().map(|s| s.to_string()),
            app_id: m.app_id.as_ref().map(|s| s.to_string()),
            user_id: m.user_id.as_ref().map(|s| s.to_string()),
            type_: m.type_.as_ref().map(|s| s.to_string()),
            priority: m.priority,
            timestamp: m.timestamp,
            expires_at_unix_ms: m.expires_unix_ms,
            headers: if headers.is_empty() {
                None
            } else {
                Some(headers)
            },
        }
    }
}

impl WalProps {
    fn into_message(self, body: Bytes) -> Message {
        Message {
            exchange: CompactString::from(self.exchange),
            routing_key: CompactString::from(self.routing_key),
            body,
            persistent: self.persistent,
            redelivered: self.redelivered,
            content_type: self.content_type.map(CompactString::from),
            content_encoding: self.content_encoding.map(CompactString::from),
            correlation_id: self.correlation_id.map(CompactString::from),
            message_id: self.message_id.map(CompactString::from),
            reply_to: self.reply_to.map(CompactString::from),
            expiration: self.expiration.map(CompactString::from),
            app_id: self.app_id.map(CompactString::from),
            user_id: self.user_id.map(CompactString::from),
            type_: self.type_.map(CompactString::from),
            priority: self.priority,
            timestamp: self.timestamp,
            expires_unix_ms: self.expires_at_unix_ms,
            headers: self
                .headers
                .map(WalHeaders::into_message_headers)
                .unwrap_or_default(),
        }
    }
}

/// A decoded record.
#[derive(Debug)]
pub struct DecodedRecord {
    pub rtype: RecordType,
    pub offset: u64,
    pub flags: u8,
    pub message: Option<Message>,
    pub encoded_len: usize,
}

/// Encode an ack-watermark record. The offset is the contiguous watermark.
pub fn encode_ack_record(watermark: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(4 + 1 + 1 + 8 + 1 + 4 + 4 + 4);
    buf.extend_from_slice(&WAL_MAGIC.to_le_bytes());
    buf.push(WAL_VERSION);
    buf.push(RecordType::AckWatermark as u8);
    buf.extend_from_slice(&watermark.to_le_bytes());
    buf.push(0u8);
    buf.extend_from_slice(&0u32.to_le_bytes());
    buf.extend_from_slice(&0u32.to_le_bytes());
    let mut hasher = Hasher::new();
    hasher.update(&buf);
    let crc = hasher.finalize();
    buf.extend_from_slice(&crc.to_le_bytes());
    buf
}

/// A bench publish has no headers and no optional properties. Skipping serde
/// avoids a property struct and a JSON buffer on every append.
fn message_props_are_plain(msg: &Message) -> bool {
    msg.content_type.is_none()
        && msg.content_encoding.is_none()
        && msg.correlation_id.is_none()
        && msg.message_id.is_none()
        && msg.reply_to.is_none()
        && msg.expiration.is_none()
        && msg.app_id.is_none()
        && msg.user_id.is_none()
        && msg.type_.is_none()
        && msg.priority.is_none()
        && msg.timestamp.is_none()
        && msg.expires_unix_ms.is_none()
        && msg.headers.is_empty()
}

fn push_json_str(out: &mut Vec<u8>, value: &str) {
    out.push(b'"');
    for byte in value.bytes() {
        match byte {
            b'"' | b'\\' => {
                out.push(b'\\');
                out.push(byte);
            }
            b'\n' => out.extend_from_slice(br"\n"),
            b'\r' => out.extend_from_slice(br"\r"),
            b'\t' => out.extend_from_slice(br"\t"),
            0x00..=0x1f => {
                out.extend_from_slice(br"\u00");
                const HEX: &[u8; 16] = b"0123456789abcdef";
                out.push(HEX[(byte >> 4) as usize]);
                out.push(HEX[(byte & 0x0f) as usize]);
            }
            _ => out.push(byte),
        }
    }
    out.push(b'"');
}

fn plain_props_json(msg: &Message) -> Vec<u8> {
    let mut props = Vec::with_capacity(48 + msg.exchange.len() + msg.routing_key.len());
    props.extend_from_slice(br#"{"exchange":"#);
    push_json_str(&mut props, &msg.exchange);
    props.extend_from_slice(br#","routing_key":"#);
    push_json_str(&mut props, &msg.routing_key);
    props.extend_from_slice(if msg.persistent {
        br#","persistent":true"#
    } else {
        br#","persistent":false"#
    });
    props.extend_from_slice(if msg.redelivered {
        br#","redelivered":true}"#
    } else {
        br#","redelivered":false}"#
    });
    props
}

/// Encode an enqueue record (including CRC).
pub fn encode_enqueue_record(offset: u64, msg: &Message) -> Result<Vec<u8>> {
    let props_bytes = if message_props_are_plain(msg) {
        plain_props_json(msg)
    } else {
        let props = WalProps::from(msg);
        serde_json::to_vec(&props)?
    };
    let body = msg.body.as_ref();

    // Header without CRC: magic(4)+ver(1)+rtype(1)+offset(8)+flags(1)+props_len(4)+props+body_len(4)+body
    let mut buf =
        Vec::with_capacity(4 + 1 + 1 + 8 + 1 + 4 + props_bytes.len() + 4 + body.len() + 4);
    buf.extend_from_slice(&WAL_MAGIC.to_le_bytes());
    buf.push(WAL_VERSION);
    buf.push(RecordType::Enqueue as u8);
    buf.extend_from_slice(&offset.to_le_bytes());
    buf.push(0u8); // flags
    buf.extend_from_slice(&(props_bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(&props_bytes);
    buf.extend_from_slice(&(body.len() as u32).to_le_bytes());
    buf.extend_from_slice(body);

    let mut hasher = Hasher::new();
    hasher.update(&buf);
    let crc = hasher.finalize();
    buf.extend_from_slice(&crc.to_le_bytes());
    Ok(buf)
}

/// Read one record from `reader`. Returns `Ok(None)` on clean EOF at a record boundary.
pub fn read_record<R: Read>(reader: &mut R) -> Result<Option<DecodedRecord>> {
    // Two-phase header read for torn-tail reporting.
    let mut magic_buf = [0u8; 4];
    match read_exact_or_eof(reader, &mut magic_buf)? {
        ReadStatus::Eof => return Ok(None),
        ReadStatus::Torn => {
            return Err(StoreError::WalTornTail { at: 0 });
        }
        ReadStatus::Ok => {}
    }
    let magic = u32::from_le_bytes(magic_buf);
    if magic != WAL_MAGIC {
        return Err(StoreError::WalCorrupt {
            path: String::new(),
            reason: format!("bad magic {magic:#x}"),
        });
    }

    let mut rest_hdr = [0u8; 1 + 1 + 8 + 1 + 4];
    match read_exact_or_eof(reader, &mut rest_hdr)? {
        ReadStatus::Ok => {}
        ReadStatus::Eof | ReadStatus::Torn => {
            return Err(StoreError::WalTornTail { at: 4 });
        }
    }

    let version = rest_hdr[0];
    if version != WAL_VERSION {
        return Err(StoreError::WalCorrupt {
            path: String::new(),
            reason: format!("unsupported WAL version {version}"),
        });
    }
    let rtype = RecordType::from_u8(rest_hdr[1]).ok_or_else(|| StoreError::WalCorrupt {
        path: String::new(),
        reason: format!("unknown record type {}", rest_hdr[1]),
    })?;
    let offset = u64::from_le_bytes(rest_hdr[2..10].try_into().unwrap());
    let flags = rest_hdr[10];
    let props_len = u32::from_le_bytes(rest_hdr[11..15].try_into().unwrap()) as usize;

    let mut props_bytes = vec![0u8; props_len];
    if props_len > 0 {
        match read_exact_or_eof(reader, &mut props_bytes)? {
            ReadStatus::Ok => {}
            ReadStatus::Eof | ReadStatus::Torn => {
                return Err(StoreError::WalTornTail { at: 19 });
            }
        }
    }

    let mut body_len_buf = [0u8; 4];
    match read_exact_or_eof(reader, &mut body_len_buf)? {
        ReadStatus::Ok => {}
        ReadStatus::Eof | ReadStatus::Torn => {
            return Err(StoreError::WalTornTail {
                at: 19 + props_len as u64,
            });
        }
    }
    let body_len = u32::from_le_bytes(body_len_buf) as usize;
    let mut body = vec![0u8; body_len];
    if body_len > 0 {
        match read_exact_or_eof(reader, &mut body)? {
            ReadStatus::Ok => {}
            ReadStatus::Eof | ReadStatus::Torn => {
                return Err(StoreError::WalTornTail {
                    at: 23 + props_len as u64,
                });
            }
        }
    }

    let mut crc_buf = [0u8; 4];
    match read_exact_or_eof(reader, &mut crc_buf)? {
        ReadStatus::Ok => {}
        ReadStatus::Eof | ReadStatus::Torn => {
            return Err(StoreError::WalTornTail {
                at: 23 + props_len as u64 + body_len as u64,
            });
        }
    }
    let crc_read = u32::from_le_bytes(crc_buf);

    // Rebuild bytes for CRC (everything except CRC).
    let mut crc_input = Vec::with_capacity(4 + rest_hdr.len() + props_len + 4 + body_len);
    crc_input.extend_from_slice(&magic_buf);
    crc_input.extend_from_slice(&rest_hdr);
    crc_input.extend_from_slice(&props_bytes);
    crc_input.extend_from_slice(&body_len_buf);
    crc_input.extend_from_slice(&body);
    let mut hasher = Hasher::new();
    hasher.update(&crc_input);
    let crc_calc = hasher.finalize();
    if crc_calc != crc_read {
        return Err(StoreError::WalCorrupt {
            path: String::new(),
            reason: format!(
                "CRC mismatch at offset record {offset}: expected {crc_calc:#x} got {crc_read:#x}"
            ),
        });
    }

    let encoded_len = crc_input.len() + 4;
    let message = if rtype == RecordType::Enqueue {
        let props: WalProps = serde_json::from_slice(&props_bytes)?;
        Some(props.into_message(Bytes::from(body)))
    } else {
        None
    };

    Ok(Some(DecodedRecord {
        rtype,
        offset,
        flags,
        message,
        encoded_len,
    }))
}

/// Decode all complete records from a byte slice (tests / helpers).
pub fn decode_records(data: &[u8]) -> Result<Vec<DecodedRecord>> {
    let mut cursor = std::io::Cursor::new(data);
    let mut out = Vec::new();
    loop {
        match read_record(&mut cursor) {
            Ok(Some(r)) => out.push(r),
            Ok(None) => break,
            Err(StoreError::WalTornTail { .. }) => break,
            Err(e) => return Err(e),
        }
    }
    Ok(out)
}

enum ReadStatus {
    Ok,
    Eof,
    Torn,
}

fn read_exact_or_eof(reader: &mut impl Read, buf: &mut [u8]) -> Result<ReadStatus> {
    let mut filled = 0;
    while filled < buf.len() {
        match reader.read(&mut buf[filled..]) {
            Ok(0) => {
                return if filled == 0 {
                    Ok(ReadStatus::Eof)
                } else {
                    Ok(ReadStatus::Torn)
                };
            }
            Ok(n) => filled += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(StoreError::Io(e)),
        }
    }
    Ok(ReadStatus::Ok)
}

#[cfg(test)]
mod tests {
    use super::*;
    use compact_str::CompactString;

    #[test]
    fn encode_decode_roundtrip() {
        let msg = Message {
            exchange: CompactString::from("amq.direct"),
            routing_key: CompactString::from("rk"),
            body: Bytes::from_static(b"payload"),
            persistent: true,
            redelivered: false,
            content_type: Some(CompactString::from("text/plain")),
            content_encoding: None,
            correlation_id: Some(CompactString::from("c1")),
            message_id: None,
            reply_to: None,
            expiration: None,
            app_id: None,
            user_id: None,
            type_: None,
            priority: Some(9),
            timestamp: Some(123),
            expires_unix_ms: None,
            headers: Default::default(),
        };
        let bytes = encode_enqueue_record(42, &msg).unwrap();
        let recs = decode_records(&bytes).unwrap();
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].offset, 42);
        let m = recs[0].message.as_ref().unwrap();
        assert_eq!(m.body.as_ref(), b"payload");
        assert_eq!(m.priority, Some(9));
        assert_eq!(m.correlation_id.as_deref(), Some("c1"));
    }

    #[test]
    fn plain_publish_roundtrips_without_optional_props() {
        let msg = Message {
            exchange: CompactString::from(""),
            routing_key: CompactString::from("bench-q"),
            body: Bytes::from_static(b"payload"),
            persistent: true,
            redelivered: false,
            content_type: None,
            content_encoding: None,
            correlation_id: None,
            message_id: None,
            reply_to: None,
            expiration: None,
            app_id: None,
            user_id: None,
            type_: None,
            priority: None,
            timestamp: None,
            expires_unix_ms: None,
            headers: Default::default(),
        };
        let bytes = encode_enqueue_record(7, &msg).unwrap();
        let recs = decode_records(&bytes).unwrap();
        let decoded = recs[0].message.as_ref().unwrap();
        assert_eq!(decoded.exchange.as_str(), "");
        assert_eq!(decoded.routing_key.as_str(), "bench-q");
        assert!(decoded.persistent);
        assert!(!decoded.redelivered);
        assert_eq!(decoded.body.as_ref(), b"payload");
        assert!(decoded.priority.is_none());
    }
}
