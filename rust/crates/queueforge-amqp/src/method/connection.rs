//! Connection class methods (class-id 10).

use crate::error::Result;
use crate::types::{Decoder, Encoder, FieldTable};

/// AMQP connection class id.
pub const CLASS_ID: u16 = 10;

/// connection.start (10)
#[derive(Debug, Clone, PartialEq)]
pub struct Start {
    /// Protocol major version (0 for 0-9-1).
    pub version_major: u8,
    /// Protocol minor version (9 for 0-9-1).
    pub version_minor: u8,
    /// Server properties table.
    pub server_properties: FieldTable,
    /// Space-separated SASL mechanisms (e.g. `PLAIN AMQPLAIN`).
    pub mechanisms: Vec<u8>,
    /// Space-separated locales (e.g. `en_US`).
    pub locales: Vec<u8>,
}

impl Start {
    /// Method id within the connection class.
    pub const METHOD_ID: u16 = 10;

    /// Encode arguments only (no class/method id).
    pub fn encode_args(&self, enc: &mut Encoder) -> Result<()> {
        enc.write_octet(self.version_major);
        enc.write_octet(self.version_minor);
        enc.write_table(&self.server_properties)?;
        enc.write_longstr(&self.mechanisms)?;
        enc.write_longstr(&self.locales)?;
        Ok(())
    }

    /// Decode arguments.
    pub fn decode_args(dec: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            version_major: dec.read_octet()?,
            version_minor: dec.read_octet()?,
            server_properties: dec.read_table()?,
            mechanisms: dec.read_longstr()?,
            locales: dec.read_longstr()?,
        })
    }
}

/// connection.start-ok (11)
#[derive(Debug, Clone, PartialEq)]
pub struct StartOk {
    /// Client properties table.
    pub client_properties: FieldTable,
    /// Selected SASL mechanism.
    pub mechanism: String,
    /// SASL response (binary; for PLAIN: `\0user\0pass`).
    pub response: Vec<u8>,
    /// Selected locale.
    pub locale: String,
}

impl StartOk {
    /// Method id.
    pub const METHOD_ID: u16 = 11;

    /// Encode arguments.
    pub fn encode_args(&self, enc: &mut Encoder) -> Result<()> {
        enc.write_table(&self.client_properties)?;
        enc.write_shortstr(&self.mechanism)?;
        enc.write_longstr(&self.response)?;
        enc.write_shortstr(&self.locale)?;
        Ok(())
    }

    /// Decode arguments.
    pub fn decode_args(dec: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            client_properties: dec.read_table()?,
            mechanism: dec.read_shortstr()?,
            response: dec.read_longstr()?,
            locale: dec.read_shortstr()?,
        })
    }
}

/// connection.tune (30)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tune {
    /// Proposed max channels (0 = no limit).
    pub channel_max: u16,
    /// Proposed max frame size (0 = no limit).
    pub frame_max: u32,
    /// Proposed heartbeat interval in seconds (0 = disable).
    pub heartbeat: u16,
}

impl Tune {
    /// Method id.
    pub const METHOD_ID: u16 = 30;

    /// Encode arguments.
    pub fn encode_args(&self, enc: &mut Encoder) -> Result<()> {
        enc.write_short(self.channel_max);
        enc.write_long(self.frame_max);
        enc.write_short(self.heartbeat);
        Ok(())
    }

    /// Decode arguments.
    pub fn decode_args(dec: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            channel_max: dec.read_short()?,
            frame_max: dec.read_long()?,
            heartbeat: dec.read_short()?,
        })
    }
}

/// connection.tune-ok (31)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TuneOk {
    /// Negotiated max channels.
    pub channel_max: u16,
    /// Negotiated max frame size.
    pub frame_max: u32,
    /// Negotiated heartbeat interval in seconds.
    pub heartbeat: u16,
}

impl TuneOk {
    /// Method id.
    pub const METHOD_ID: u16 = 31;

    /// Encode arguments.
    pub fn encode_args(&self, enc: &mut Encoder) -> Result<()> {
        enc.write_short(self.channel_max);
        enc.write_long(self.frame_max);
        enc.write_short(self.heartbeat);
        Ok(())
    }

    /// Decode arguments.
    pub fn decode_args(dec: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            channel_max: dec.read_short()?,
            frame_max: dec.read_long()?,
            heartbeat: dec.read_short()?,
        })
    }
}

/// connection.open (40)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Open {
    /// Virtual host path (e.g. `/`).
    pub virtual_host: String,
    /// Deprecated reserved field; encode as empty.
    pub reserved_1: String,
    /// Deprecated insist bit; encode as false.
    pub reserved_2: bool,
}

impl Open {
    /// Method id.
    pub const METHOD_ID: u16 = 40;

    /// Construct with the usual defaults for reserved fields.
    pub fn new(virtual_host: impl Into<String>) -> Self {
        Self {
            virtual_host: virtual_host.into(),
            reserved_1: String::new(),
            reserved_2: false,
        }
    }

    /// Encode arguments.
    pub fn encode_args(&self, enc: &mut Encoder) -> Result<()> {
        enc.write_shortstr(&self.virtual_host)?;
        enc.write_shortstr(&self.reserved_1)?;
        enc.write_bit(self.reserved_2);
        Ok(())
    }

    /// Decode arguments.
    pub fn decode_args(dec: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            virtual_host: dec.read_shortstr()?,
            reserved_1: dec.read_shortstr()?,
            reserved_2: dec.read_bit()?,
        })
    }
}

/// connection.open-ok (41)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenOk {
    /// Deprecated reserved field; empty.
    pub reserved_1: String,
}

impl OpenOk {
    /// Method id.
    pub const METHOD_ID: u16 = 41;

    /// Empty reserved field.
    pub fn new() -> Self {
        Self {
            reserved_1: String::new(),
        }
    }

    /// Encode arguments.
    pub fn encode_args(&self, enc: &mut Encoder) -> Result<()> {
        enc.write_shortstr(&self.reserved_1)?;
        Ok(())
    }

    /// Decode arguments.
    pub fn decode_args(dec: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            reserved_1: dec.read_shortstr()?,
        })
    }
}

impl Default for OpenOk {
    fn default() -> Self {
        Self::new()
    }
}

/// connection.close (50)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Close {
    /// Reply code (200 = success).
    pub reply_code: u16,
    /// Human-readable reply text.
    pub reply_text: String,
    /// Failing class id (0 if none).
    pub class_id: u16,
    /// Failing method id (0 if none).
    pub method_id: u16,
}

impl Close {
    /// Method id.
    pub const METHOD_ID: u16 = 50;

    /// Encode arguments.
    pub fn encode_args(&self, enc: &mut Encoder) -> Result<()> {
        enc.write_short(self.reply_code);
        enc.write_shortstr(&self.reply_text)?;
        enc.write_short(self.class_id);
        enc.write_short(self.method_id);
        Ok(())
    }

    /// Decode arguments.
    pub fn decode_args(dec: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            reply_code: dec.read_short()?,
            reply_text: dec.read_shortstr()?,
            class_id: dec.read_short()?,
            method_id: dec.read_short()?,
        })
    }
}

/// connection.close-ok (51)
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CloseOk;

impl CloseOk {
    /// Method id.
    pub const METHOD_ID: u16 = 51;

    /// Encode arguments (empty).
    pub fn encode_args(&self, _enc: &mut Encoder) -> Result<()> {
        Ok(())
    }

    /// Decode arguments (empty).
    pub fn decode_args(_dec: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self)
    }
}
