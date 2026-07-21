//! Channel class methods (class-id 20).

use crate::error::Result;
use crate::types::{Decoder, Encoder};

/// AMQP channel class id.
pub const CLASS_ID: u16 = 20;

/// channel.open (10)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Open {
    /// Deprecated reserved field; empty shortstr.
    pub reserved_1: String,
}

impl Open {
    /// Method id.
    pub const METHOD_ID: u16 = 10;

    /// Default open (empty reserved).
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

impl Default for Open {
    fn default() -> Self {
        Self::new()
    }
}

/// channel.open-ok (11)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenOk {
    /// Deprecated reserved field; empty longstr.
    pub reserved_1: Vec<u8>,
}

impl OpenOk {
    /// Method id.
    pub const METHOD_ID: u16 = 11;

    /// Default open-ok.
    pub fn new() -> Self {
        Self {
            reserved_1: Vec::new(),
        }
    }

    /// Encode arguments.
    pub fn encode_args(&self, enc: &mut Encoder) -> Result<()> {
        enc.write_longstr(&self.reserved_1)?;
        Ok(())
    }

    /// Decode arguments.
    pub fn decode_args(dec: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            reserved_1: dec.read_longstr()?,
        })
    }
}

impl Default for OpenOk {
    fn default() -> Self {
        Self::new()
    }
}

/// channel.flow (20)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Flow {
    /// Whether the peer should start/continue sending content.
    pub active: bool,
}

impl Flow {
    /// Method id.
    pub const METHOD_ID: u16 = 20;

    /// Encode arguments.
    pub fn encode_args(&self, enc: &mut Encoder) -> Result<()> {
        enc.write_bit(self.active);
        Ok(())
    }

    /// Decode arguments.
    pub fn decode_args(dec: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            active: dec.read_bit()?,
        })
    }
}

/// channel.flow-ok (21)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowOk {
    /// Confirmed flow state.
    pub active: bool,
}

impl FlowOk {
    /// Method id.
    pub const METHOD_ID: u16 = 21;

    /// Encode arguments.
    pub fn encode_args(&self, enc: &mut Encoder) -> Result<()> {
        enc.write_bit(self.active);
        Ok(())
    }

    /// Decode arguments.
    pub fn decode_args(dec: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            active: dec.read_bit()?,
        })
    }
}

/// channel.close (40)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Close {
    /// Reply code.
    pub reply_code: u16,
    /// Reply text.
    pub reply_text: String,
    /// Failing class id (0 if none).
    pub class_id: u16,
    /// Failing method id (0 if none).
    pub method_id: u16,
}

impl Close {
    /// Method id.
    pub const METHOD_ID: u16 = 40;

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

/// channel.close-ok (41)
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CloseOk;

impl CloseOk {
    /// Method id.
    pub const METHOD_ID: u16 = 41;

    /// Encode arguments (empty).
    pub fn encode_args(&self, _enc: &mut Encoder) -> Result<()> {
        Ok(())
    }

    /// Decode arguments (empty).
    pub fn decode_args(_dec: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self)
    }
}
