//! AMQP 0-9-1 frame encode/decode.
//!
//! Wire layout (network byte order):
//! ```text
//! 0      1         3         7                  size+7   size+8
//! +------+---------+---------+------------------+---------+
//! | type | channel |  size   | payload          | frame-end|
//! +------+---------+---------+------------------+---------+
//!  octet   short     long      size octets        octet
//! ```
//!
//! Frame-end is always `0xCE`. Payload is opaque at this layer (method catalog
//! lives in a later PR).

use crate::error::{Error, Result};

/// Frame-end marker required after every AMQP frame payload.
pub const FRAME_END: u8 = 0xCE;

/// Octets before the payload: type (1) + channel (2) + size (4) = 7.
///
/// Does **not** include the frame-end octet. Full empty-frame size is
/// [`FRAME_MIN_LEN`].
pub const FRAME_HEADER_LEN: usize = 7;
/// Length of the frame-end octet.
pub const FRAME_END_LEN: usize = 1;
/// Minimum on-wire size of a frame (empty payload): header + frame-end.
pub const FRAME_MIN_LEN: usize = FRAME_HEADER_LEN + FRAME_END_LEN;

/// AMQP frame type discriminant (octet).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum FrameType {
    /// Method frame (class/method + arguments).
    Method = 1,
    /// Content header frame (class, body size, property flags/fields).
    Header = 2,
    /// Content body frame (opaque message body chunk).
    Body = 3,
    /// Heartbeat frame (channel 0, empty payload).
    Heartbeat = 8,
}

impl FrameType {
    /// Convert a wire octet into a [`FrameType`].
    pub fn from_u8(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::Method),
            2 => Ok(Self::Header),
            3 => Ok(Self::Body),
            8 => Ok(Self::Heartbeat),
            other => Err(Error::UnknownFrameType(other)),
        }
    }

    /// Wire value for this frame type.
    pub const fn as_u8(self) -> u8 {
        self as u8
    }
}

/// A decoded AMQP frame.
///
/// The payload is left as raw bytes; higher layers interpret method/header
/// content. Heartbeats always use channel `0` and an empty payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    /// Frame type (method / header / body / heartbeat).
    pub kind: FrameType,
    /// Channel number (0 for connection-global frames and heartbeats).
    pub channel: u16,
    /// Opaque payload octets (excludes frame header and frame-end).
    pub payload: Vec<u8>,
}

impl Frame {
    /// Construct a method frame.
    pub fn method(channel: u16, payload: impl Into<Vec<u8>>) -> Self {
        Self {
            kind: FrameType::Method,
            channel,
            payload: payload.into(),
        }
    }

    /// Construct a content-header frame.
    pub fn header(channel: u16, payload: impl Into<Vec<u8>>) -> Self {
        Self {
            kind: FrameType::Header,
            channel,
            payload: payload.into(),
        }
    }

    /// Construct a content-body frame.
    pub fn body(channel: u16, payload: impl Into<Vec<u8>>) -> Self {
        Self {
            kind: FrameType::Body,
            channel,
            payload: payload.into(),
        }
    }

    /// Construct a heartbeat frame (channel 0, empty payload).
    pub fn heartbeat() -> Self {
        Self {
            kind: FrameType::Heartbeat,
            channel: 0,
            payload: Vec::new(),
        }
    }

    /// Total on-wire length of this frame once encoded.
    pub fn encoded_len(&self) -> usize {
        FRAME_MIN_LEN + self.payload.len()
    }

    /// Encode this frame into a new buffer.
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(self.encoded_len());
        self.encode_into(&mut buf)?;
        Ok(buf)
    }

    /// Encode this frame, appending to `buf`.
    pub fn encode_into(&self, buf: &mut Vec<u8>) -> Result<()> {
        if self.kind == FrameType::Heartbeat {
            if self.channel != 0 {
                return Err(Error::HeartbeatNonZeroChannel(self.channel));
            }
            if !self.payload.is_empty() {
                return Err(Error::HeartbeatNonEmptyPayload(self.payload.len()));
            }
        }

        let size = u32::try_from(self.payload.len())
            .map_err(|_| Error::PayloadTooLarge(self.payload.len()))?;

        buf.reserve(self.encoded_len());
        buf.push(self.kind.as_u8());
        buf.extend_from_slice(&self.channel.to_be_bytes());
        buf.extend_from_slice(&size.to_be_bytes());
        buf.extend_from_slice(&self.payload);
        buf.push(FRAME_END);
        Ok(())
    }

    /// Decode one frame from `buf` with no payload-size limit.
    ///
    /// On success returns `(frame, bytes_consumed)`. Remaining bytes in `buf`
    /// after the returned length belong to subsequent frames.
    ///
    /// **Safety:** only use unlimited decode when the read buffer is externally
    /// capped (or prefer [`Self::decode_with_limit`] with negotiated
    /// `frame_max`). A malicious peer can declare a huge size field and force
    /// large buffering if the caller always reads `need` more bytes.
    pub fn decode(buf: &[u8]) -> Result<(Self, usize)> {
        Self::decode_with_limit(buf, usize::MAX)
    }

    /// Decode one frame from `buf`, rejecting payloads larger than `max_payload`.
    ///
    /// When the frame size field exceeds `max_payload`, returns
    /// [`Error::FrameTooLarge`] **before** asking the caller to buffer more
    /// data. Connection code should pass the negotiated `frame_max` (payload
    /// budget; typically the AMQP `frame-max` minus the fixed frame overhead,
    /// or the full frame-max depending on how the peer defines it).
    ///
    /// On success returns `(frame, bytes_consumed)`.
    pub fn decode_with_limit(buf: &[u8], max_payload: usize) -> Result<(Self, usize)> {
        // Wait for the full 7-octet header so size is known and `need` is exact.
        if buf.len() < FRAME_HEADER_LEN {
            return Err(Error::Incomplete {
                need: FRAME_HEADER_LEN - buf.len(),
                have: buf.len(),
            });
        }

        let kind = FrameType::from_u8(buf[0])?;
        let channel = u16::from_be_bytes([buf[1], buf[2]]);
        let size = u32::from_be_bytes([buf[3], buf[4], buf[5], buf[6]]) as usize;

        if size > max_payload {
            return Err(Error::FrameTooLarge {
                size,
                max: max_payload,
            });
        }

        let total = FRAME_HEADER_LEN
            .checked_add(size)
            .and_then(|n| n.checked_add(FRAME_END_LEN))
            .ok_or(Error::FrameSizeOverflow(size))?;

        if buf.len() < total {
            return Err(Error::Incomplete {
                need: total - buf.len(),
                have: buf.len(),
            });
        }

        let end = buf[FRAME_HEADER_LEN + size];
        if end != FRAME_END {
            return Err(Error::InvalidFrameEnd(end));
        }

        let payload = buf[FRAME_HEADER_LEN..FRAME_HEADER_LEN + size].to_vec();

        if kind == FrameType::Heartbeat {
            if channel != 0 {
                return Err(Error::HeartbeatNonZeroChannel(channel));
            }
            if !payload.is_empty() {
                return Err(Error::HeartbeatNonEmptyPayload(payload.len()));
            }
        }

        Ok((
            Self {
                kind,
                channel,
                payload,
            },
            total,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_type_values() {
        assert_eq!(FrameType::Method.as_u8(), 1);
        assert_eq!(FrameType::Header.as_u8(), 2);
        assert_eq!(FrameType::Body.as_u8(), 3);
        assert_eq!(FrameType::Heartbeat.as_u8(), 8);
        assert_eq!(FrameType::from_u8(1).unwrap(), FrameType::Method);
        assert_eq!(FrameType::from_u8(8).unwrap(), FrameType::Heartbeat);
        assert!(matches!(
            FrameType::from_u8(99),
            Err(Error::UnknownFrameType(99))
        ));
    }

    #[test]
    fn method_frame_roundtrip() {
        let payload = vec![0x00, 0x0A, 0x00, 0x0A]; // connection.start-ish
        let frame = Frame::method(0, payload.clone());
        let encoded = frame.encode().unwrap();

        // type(1) + channel(2) + size(4) + payload(4) + end(1) = 12
        assert_eq!(encoded.len(), 12);
        assert_eq!(encoded[0], 1);
        assert_eq!(&encoded[1..3], &0u16.to_be_bytes());
        assert_eq!(&encoded[3..7], &4u32.to_be_bytes());
        assert_eq!(&encoded[7..11], payload.as_slice());
        assert_eq!(encoded[11], FRAME_END);

        let (decoded, n) = Frame::decode(&encoded).unwrap();
        assert_eq!(n, encoded.len());
        assert_eq!(decoded, frame);
    }

    #[test]
    fn header_and_body_roundtrip() {
        let header = Frame::header(3, vec![0xAB, 0xCD]);
        let body = Frame::body(3, b"hello world".to_vec());

        for frame in [&header, &body] {
            let encoded = frame.encode().unwrap();
            let (decoded, n) = Frame::decode(&encoded).unwrap();
            assert_eq!(n, encoded.len());
            assert_eq!(&decoded, frame);
        }
    }

    #[test]
    fn heartbeat_roundtrip() {
        let frame = Frame::heartbeat();
        let encoded = frame.encode().unwrap();
        // empty payload → 8 bytes
        assert_eq!(encoded, [8, 0, 0, 0, 0, 0, 0, FRAME_END]);

        let (decoded, n) = Frame::decode(&encoded).unwrap();
        assert_eq!(n, 8);
        assert_eq!(decoded.kind, FrameType::Heartbeat);
        assert_eq!(decoded.channel, 0);
        assert!(decoded.payload.is_empty());
    }

    #[test]
    fn heartbeat_rejects_nonzero_channel_on_encode() {
        let bad = Frame {
            kind: FrameType::Heartbeat,
            channel: 1,
            payload: Vec::new(),
        };
        assert!(matches!(
            bad.encode(),
            Err(Error::HeartbeatNonZeroChannel(1))
        ));
    }

    #[test]
    fn heartbeat_rejects_nonempty_payload_on_encode() {
        let bad = Frame {
            kind: FrameType::Heartbeat,
            channel: 0,
            payload: vec![0xFF],
        };
        assert!(matches!(
            bad.encode(),
            Err(Error::HeartbeatNonEmptyPayload(1))
        ));
    }

    #[test]
    fn heartbeat_rejects_nonzero_channel_on_decode() {
        // type=8, channel=1, size=0, end=0xCE
        let raw = [8u8, 0, 1, 0, 0, 0, 0, FRAME_END];
        assert!(matches!(
            Frame::decode(&raw),
            Err(Error::HeartbeatNonZeroChannel(1))
        ));
    }

    #[test]
    fn heartbeat_rejects_nonempty_payload_on_decode() {
        let raw = [8u8, 0, 0, 0, 0, 0, 1, 0xFF, FRAME_END];
        assert!(matches!(
            Frame::decode(&raw),
            Err(Error::HeartbeatNonEmptyPayload(1))
        ));
    }

    #[test]
    fn unknown_frame_type_on_decode() {
        let raw = [4u8, 0, 0, 0, 0, 0, 0, FRAME_END];
        assert!(matches!(
            Frame::decode(&raw),
            Err(Error::UnknownFrameType(4))
        ));
    }

    #[test]
    fn invalid_frame_end() {
        let mut encoded = Frame::method(1, vec![1, 2, 3]).encode().unwrap();
        let last = encoded.len() - 1;
        encoded[last] = 0x00;
        assert!(matches!(
            Frame::decode(&encoded),
            Err(Error::InvalidFrameEnd(0x00))
        ));
    }

    #[test]
    fn incomplete_header() {
        let err = Frame::decode(&[1, 0, 0]).unwrap_err();
        assert_eq!(
            err,
            Error::Incomplete {
                need: FRAME_HEADER_LEN - 3,
                have: 3
            }
        );
    }

    #[test]
    fn incomplete_payload() {
        // claims size=10 but only provides the 7-octet header
        // total = 7 + 10 + 1 = 18 → need = 11
        let raw = [1u8, 0, 0, 0, 0, 0, 10];
        let err = Frame::decode(&raw).unwrap_err();
        assert_eq!(err, Error::Incomplete { need: 11, have: 7 });
    }

    #[test]
    fn incomplete_seven_bytes_uses_size_field() {
        // have == 7, size = 0 → still need the frame-end octet
        let raw = [1u8, 0, 0, 0, 0, 0, 0];
        let err = Frame::decode(&raw).unwrap_err();
        assert_eq!(err, Error::Incomplete { need: 1, have: 7 });
    }

    #[test]
    fn decode_with_limit_rejects_oversized_size_field() {
        // size = 100, max_payload = 50 → hard error, no Incomplete
        let raw = [1u8, 0, 0, 0, 0, 0, 100];
        let err = Frame::decode_with_limit(&raw, 50).unwrap_err();
        assert_eq!(err, Error::FrameTooLarge { size: 100, max: 50 });
    }

    #[test]
    fn decode_with_limit_allows_exact_max() {
        let frame = Frame::method(1, vec![1, 2, 3, 4]);
        let encoded = frame.encode().unwrap();
        let (decoded, n) = Frame::decode_with_limit(&encoded, 4).unwrap();
        assert_eq!(n, encoded.len());
        assert_eq!(decoded, frame);

        let err = Frame::decode_with_limit(&encoded, 3).unwrap_err();
        assert_eq!(err, Error::FrameTooLarge { size: 4, max: 3 });
    }

    #[test]
    fn decode_with_trailing_bytes() {
        let mut buf = Frame::method(2, vec![9]).encode().unwrap();
        let first_len = buf.len();
        buf.extend_from_slice(&[0xFF, 0xFE]); // garbage trailing
        let (decoded, n) = Frame::decode(&buf).unwrap();
        assert_eq!(n, first_len);
        assert_eq!(decoded.channel, 2);
        assert_eq!(decoded.payload, vec![9]);
    }

    #[test]
    fn empty_method_payload() {
        let frame = Frame::method(0, Vec::new());
        let encoded = frame.encode().unwrap();
        assert_eq!(encoded.len(), FRAME_MIN_LEN);
        let (decoded, _) = Frame::decode(&encoded).unwrap();
        assert_eq!(decoded, frame);
    }

    #[test]
    fn channel_max_u16() {
        let frame = Frame::body(u16::MAX, vec![0x42]);
        let encoded = frame.encode().unwrap();
        let (decoded, _) = Frame::decode(&encoded).unwrap();
        assert_eq!(decoded.channel, u16::MAX);
    }
}
