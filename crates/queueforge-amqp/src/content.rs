//! Content header frames and basic properties (AMQP class 60).
//!
//! Content-header payload layout:
//! ```text
//! class-id (short) | weight (short) | body-size (longlong)
//! | property-flags (short) | property fields…
//! ```
//!
//! Property flags use bit 15 = content-type through bit 2 = cluster-id
//! (AMQP 0-9-1 basic properties). Bit 0 is the continuation flag (unsupported
//! — we reject non-zero continuation).

use crate::error::{Error, Result};
use crate::types::{Decoder, Encoder, FieldTable};

/// Content-header frame payload (decoded).
#[derive(Debug, Clone, PartialEq)]
pub struct ContentHeader {
    /// Class id of the content (60 for basic).
    pub class_id: u16,
    /// Weight (must be 0 in AMQP 0-9-1).
    pub weight: u16,
    /// Total body size in octets across subsequent body frames.
    pub body_size: u64,
    /// Basic class properties.
    pub properties: BasicProperties,
}

impl ContentHeader {
    /// Encode into a content-header frame payload.
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut enc = Encoder::with_capacity(32);
        enc.write_short(self.class_id);
        enc.write_short(self.weight);
        enc.write_longlong(self.body_size);
        self.properties.encode_into(&mut enc)?;
        Ok(enc.finish())
    }

    /// Decode a content-header frame payload.
    pub fn decode(payload: &[u8]) -> Result<Self> {
        let mut dec = Decoder::new(payload);
        let class_id = dec.read_short()?;
        let weight = dec.read_short()?;
        let body_size = dec.read_longlong()?;
        let properties = BasicProperties::decode_from(&mut dec)?;
        // Tolerate trailing zeros if any; reject non-empty trailing data.
        if dec.remaining() > 0 {
            // Some clients pad; allow only all-zero remainder.
            let rest = &payload[payload.len() - dec.remaining()..];
            if rest.iter().any(|&b| b != 0) {
                return Err(Error::TrailingBytes(dec.remaining()));
            }
        }
        Ok(Self {
            class_id,
            weight,
            body_size,
            properties,
        })
    }

    /// Convenience: basic class header with given body size and properties.
    pub fn basic(body_size: u64, properties: BasicProperties) -> Self {
        Self {
            class_id: 60,
            weight: 0,
            body_size,
            properties,
        }
    }
}

/// AMQP basic properties carried in content headers and deliveries.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct BasicProperties {
    /// MIME content type.
    pub content_type: Option<String>,
    /// MIME content encoding.
    pub content_encoding: Option<String>,
    /// Application headers.
    pub headers: Option<FieldTable>,
    /// Delivery mode (1 non-persistent, 2 persistent).
    pub delivery_mode: Option<u8>,
    /// Priority 0–9.
    pub priority: Option<u8>,
    /// Correlation id.
    pub correlation_id: Option<String>,
    /// Reply-to address.
    pub reply_to: Option<String>,
    /// Expiration (ms as string, or absolute depending on client).
    pub expiration: Option<String>,
    /// Application message id.
    pub message_id: Option<String>,
    /// Timestamp (posix seconds).
    pub timestamp: Option<u64>,
    /// Type name.
    pub type_: Option<String>,
    /// Creating user id.
    pub user_id: Option<String>,
    /// Creating application id.
    pub app_id: Option<String>,
    /// Deprecated cluster id.
    pub cluster_id: Option<String>,
}

impl BasicProperties {
    /// Encode property flags + present fields into `enc`.
    pub fn encode_into(&self, enc: &mut Encoder) -> Result<()> {
        let mut flags: u16 = 0;
        if self.content_type.is_some() {
            flags |= 1 << 15;
        }
        if self.content_encoding.is_some() {
            flags |= 1 << 14;
        }
        if self.headers.is_some() {
            flags |= 1 << 13;
        }
        if self.delivery_mode.is_some() {
            flags |= 1 << 12;
        }
        if self.priority.is_some() {
            flags |= 1 << 11;
        }
        if self.correlation_id.is_some() {
            flags |= 1 << 10;
        }
        if self.reply_to.is_some() {
            flags |= 1 << 9;
        }
        if self.expiration.is_some() {
            flags |= 1 << 8;
        }
        if self.message_id.is_some() {
            flags |= 1 << 7;
        }
        if self.timestamp.is_some() {
            flags |= 1 << 6;
        }
        if self.type_.is_some() {
            flags |= 1 << 5;
        }
        if self.user_id.is_some() {
            flags |= 1 << 4;
        }
        if self.app_id.is_some() {
            flags |= 1 << 3;
        }
        if self.cluster_id.is_some() {
            flags |= 1 << 2;
        }
        // bit 0 = continuation: always 0 (single flags word)
        enc.write_short(flags);

        if let Some(ref v) = self.content_type {
            enc.write_shortstr(v)?;
        }
        if let Some(ref v) = self.content_encoding {
            enc.write_shortstr(v)?;
        }
        if let Some(ref v) = self.headers {
            enc.write_table(v)?;
        }
        if let Some(v) = self.delivery_mode {
            enc.write_octet(v);
        }
        if let Some(v) = self.priority {
            enc.write_octet(v);
        }
        if let Some(ref v) = self.correlation_id {
            enc.write_shortstr(v)?;
        }
        if let Some(ref v) = self.reply_to {
            enc.write_shortstr(v)?;
        }
        if let Some(ref v) = self.expiration {
            enc.write_shortstr(v)?;
        }
        if let Some(ref v) = self.message_id {
            enc.write_shortstr(v)?;
        }
        if let Some(v) = self.timestamp {
            enc.write_longlong(v);
        }
        if let Some(ref v) = self.type_ {
            enc.write_shortstr(v)?;
        }
        if let Some(ref v) = self.user_id {
            enc.write_shortstr(v)?;
        }
        if let Some(ref v) = self.app_id {
            enc.write_shortstr(v)?;
        }
        if let Some(ref v) = self.cluster_id {
            enc.write_shortstr(v)?;
        }
        Ok(())
    }

    /// Decode property flags + fields from `dec`.
    pub fn decode_from(dec: &mut Decoder<'_>) -> Result<Self> {
        let flags = dec.read_short()?;
        if flags & 1 != 0 {
            // Continuation bit set — multi-word flags not supported.
            return Err(Error::ValueOutOfRange(
                "basic property flags continuation bit",
            ));
        }
        let mut props = Self::default();
        if flags & (1 << 15) != 0 {
            props.content_type = Some(dec.read_shortstr()?);
        }
        if flags & (1 << 14) != 0 {
            props.content_encoding = Some(dec.read_shortstr()?);
        }
        if flags & (1 << 13) != 0 {
            props.headers = Some(dec.read_table()?);
        }
        if flags & (1 << 12) != 0 {
            props.delivery_mode = Some(dec.read_octet()?);
        }
        if flags & (1 << 11) != 0 {
            props.priority = Some(dec.read_octet()?);
        }
        if flags & (1 << 10) != 0 {
            props.correlation_id = Some(dec.read_shortstr()?);
        }
        if flags & (1 << 9) != 0 {
            props.reply_to = Some(dec.read_shortstr()?);
        }
        if flags & (1 << 8) != 0 {
            props.expiration = Some(dec.read_shortstr()?);
        }
        if flags & (1 << 7) != 0 {
            props.message_id = Some(dec.read_shortstr()?);
        }
        if flags & (1 << 6) != 0 {
            props.timestamp = Some(dec.read_longlong()?);
        }
        if flags & (1 << 5) != 0 {
            props.type_ = Some(dec.read_shortstr()?);
        }
        if flags & (1 << 4) != 0 {
            props.user_id = Some(dec.read_shortstr()?);
        }
        if flags & (1 << 3) != 0 {
            props.app_id = Some(dec.read_shortstr()?);
        }
        if flags & (1 << 2) != 0 {
            props.cluster_id = Some(dec.read_shortstr()?);
        }
        Ok(props)
    }

    /// True when `delivery_mode == 2`.
    pub fn is_persistent(&self) -> bool {
        self.delivery_mode == Some(2)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_empty_props() {
        let h = ContentHeader::basic(0, BasicProperties::default());
        let enc = h.encode().unwrap();
        let dec = ContentHeader::decode(&enc).unwrap();
        assert_eq!(dec, h);
    }

    #[test]
    fn roundtrip_with_body_and_mode() {
        let props = BasicProperties {
            content_type: Some("text/plain".into()),
            delivery_mode: Some(2),
            priority: Some(5),
            message_id: Some("m-1".into()),
            ..Default::default()
        };
        let h = ContentHeader::basic(42, props);
        let enc = h.encode().unwrap();
        let dec = ContentHeader::decode(&enc).unwrap();
        assert_eq!(dec.body_size, 42);
        assert_eq!(dec.properties.content_type.as_deref(), Some("text/plain"));
        assert_eq!(dec.properties.delivery_mode, Some(2));
        assert_eq!(dec.properties.priority, Some(5));
        assert_eq!(dec.properties.message_id.as_deref(), Some("m-1"));
        assert!(dec.properties.is_persistent());
    }
}
