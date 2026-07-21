//! Binary encode/decode for WAL records.

use std::io::Read;

use bytes::Bytes;
use compact_str::CompactString;
use crc32fast::Hasher;
use queueforge_core::{DeathEntry, DeathReason, Message, MessageHeaders};
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
        }
    }

    fn is_empty(&self) -> bool {
        self.deaths.is_empty()
            && self.first_death_reason.is_none()
            && self.first_death_queue.is_none()
            && self.first_death_exchange.is_none()
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

/// Encode an enqueue record (including CRC).
pub fn encode_enqueue_record(offset: u64, msg: &Message) -> Result<Vec<u8>> {
    let props = WalProps::from(msg);
    let props_bytes = serde_json::to_vec(&props)?;
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
}
