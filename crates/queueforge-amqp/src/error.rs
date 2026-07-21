//! Error types for AMQP framing and method codecs.

use thiserror::Error;

/// Errors produced while encoding or decoding AMQP frames, headers, and methods.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum Error {
    /// Not enough bytes available to complete a decode.
    #[error("incomplete input: need at least {need} more byte(s), have {have}")]
    Incomplete {
        /// Additional bytes required.
        ///
        /// Accurate once the 7-octet frame header is present (derived from the
        /// declared size). Before that, a lower bound of
        /// `FRAME_HEADER_LEN - have`.
        need: usize,
        /// Number of bytes currently available.
        have: usize,
    },

    /// Protocol header magic or version bytes did not match AMQP 0-9-1.
    #[error("invalid protocol header")]
    InvalidProtocolHeader,

    /// Frame type octet is not one of METHOD/HEADER/BODY/HEARTBEAT.
    #[error("unknown frame type: {0}")]
    UnknownFrameType(u8),

    /// Frame-end octet was not `0xCE`.
    #[error("invalid frame end: expected 0xCE, got 0x{0:02X}")]
    InvalidFrameEnd(u8),

    /// Heartbeat frames must use channel 0.
    #[error("heartbeat frame must use channel 0, got {0}")]
    HeartbeatNonZeroChannel(u16),

    /// Heartbeat frames must have an empty payload.
    #[error("heartbeat frame must have empty payload, got {0} byte(s)")]
    HeartbeatNonEmptyPayload(usize),

    /// Encoded payload length exceeds what fits in a 32-bit size field.
    #[error("payload too large to encode: {0} bytes (exceeds u32::MAX)")]
    PayloadTooLarge(usize),

    /// Frame total length (header + payload + frame-end) overflowed `usize`.
    #[error("frame size overflow for payload length {0}")]
    FrameSizeOverflow(usize),

    /// Declared payload size exceeds the configured maximum (`frame_max`).
    #[error("frame payload size {size} exceeds limit {max}")]
    FrameTooLarge {
        /// Size field from the frame header.
        size: usize,
        /// Maximum allowed payload octets.
        max: usize,
    },

    /// Method payload is shorter than required for the declared fields.
    #[error("truncated method arguments: need {need} more byte(s), have {have}")]
    TruncatedMethod {
        /// Additional bytes required.
        need: usize,
        /// Bytes remaining in the buffer.
        have: usize,
    },

    /// Trailing garbage after a fully-decoded method payload.
    #[error("trailing bytes after method arguments: {0}")]
    TrailingBytes(usize),

    /// Class/method id pair is not in the supported catalog.
    #[error("unknown method class={class_id} method={method_id}")]
    UnknownMethod {
        /// AMQP class id.
        class_id: u16,
        /// AMQP method id within the class.
        method_id: u16,
    },

    /// shortstr length exceeds 255 or claimed length exceeds remaining input.
    #[error("invalid shortstr length {0}")]
    InvalidShortstr(usize),

    /// longstr claimed length exceeds remaining input.
    #[error("invalid longstr length {0}")]
    InvalidLongstr(usize),

    /// Field-table or field-array claimed byte length exceeds remaining input.
    #[error("invalid table/array length {0}")]
    InvalidTableLength(usize),

    /// Unknown field-value type tag in a table or array.
    #[error("unknown field value type tag: {0:?} (0x{0:02X})")]
    UnknownFieldValueType(u8),

    /// shortstr / table key is not valid UTF-8.
    #[error("invalid UTF-8 in AMQP string")]
    InvalidUtf8,

    /// Value does not fit in the target AMQP wire type.
    #[error("value out of range for AMQP type: {0}")]
    ValueOutOfRange(&'static str),

    /// [`crate::Method::from_frame`] was given a non-method frame.
    #[error("expected method frame, got frame type {0}")]
    ExpectedMethodFrame(u8),
}

/// Result alias for AMQP framing and method operations.
pub type Result<T> = std::result::Result<T, Error>;
