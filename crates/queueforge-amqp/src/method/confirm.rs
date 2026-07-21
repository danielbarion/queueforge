//! Confirm class methods (class-id 85) — RabbitMQ publisher confirms extension.

use crate::error::Result;
use crate::types::{Decoder, Encoder};

/// AMQP confirm class id (RabbitMQ extension).
pub const CLASS_ID: u16 = 85;

/// confirm.select (10)
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Select {
    /// When true, do not expect `confirm.select-ok`.
    pub nowait: bool,
}

impl Select {
    /// Method id.
    pub const METHOD_ID: u16 = 10;

    /// Construct with the given `nowait` flag.
    pub fn new(nowait: bool) -> Self {
        Self { nowait }
    }

    /// Encode arguments.
    pub fn encode_args(&self, enc: &mut Encoder) -> Result<()> {
        enc.write_bit(self.nowait);
        Ok(())
    }

    /// Decode arguments.
    pub fn decode_args(dec: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            nowait: dec.read_bit()?,
        })
    }
}

/// confirm.select-ok (11)
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SelectOk;

impl SelectOk {
    /// Method id.
    pub const METHOD_ID: u16 = 11;

    /// Encode arguments (empty).
    pub fn encode_args(&self, _enc: &mut Encoder) -> Result<()> {
        Ok(())
    }

    /// Decode arguments (empty).
    pub fn decode_args(_dec: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self)
    }
}
