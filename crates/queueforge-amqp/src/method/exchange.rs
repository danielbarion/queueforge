//! Exchange class methods (class-id 40).

use crate::error::Result;
use crate::types::{Decoder, Encoder, FieldTable};

/// AMQP exchange class id.
pub const CLASS_ID: u16 = 40;

/// exchange.declare (10)
#[derive(Debug, Clone, PartialEq)]
pub struct Declare {
    /// Reserved ticket (must be 0).
    pub reserved_1: u16,
    /// Exchange name.
    pub exchange: String,
    /// Exchange type (`direct`, `fanout`, `topic`, `headers`).
    pub kind: String,
    /// Do not create; assert it exists.
    pub passive: bool,
    /// Survive broker restart.
    pub durable: bool,
    /// Delete when no longer used.
    pub auto_delete: bool,
    /// Internal (not used by publishers).
    pub internal: bool,
    /// Do not send declare-ok.
    pub no_wait: bool,
    /// Optional arguments.
    pub arguments: FieldTable,
}

impl Declare {
    /// Method id.
    pub const METHOD_ID: u16 = 10;

    /// Encode arguments.
    pub fn encode_args(&self, enc: &mut Encoder) -> Result<()> {
        enc.write_short(self.reserved_1);
        enc.write_shortstr(&self.exchange)?;
        enc.write_shortstr(&self.kind)?;
        enc.write_bit(self.passive);
        enc.write_bit(self.durable);
        enc.write_bit(self.auto_delete);
        enc.write_bit(self.internal);
        enc.write_bit(self.no_wait);
        enc.write_table(&self.arguments)?;
        Ok(())
    }

    /// Decode arguments.
    pub fn decode_args(dec: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            reserved_1: dec.read_short()?,
            exchange: dec.read_shortstr()?,
            kind: dec.read_shortstr()?,
            passive: dec.read_bit()?,
            durable: dec.read_bit()?,
            auto_delete: dec.read_bit()?,
            internal: dec.read_bit()?,
            no_wait: dec.read_bit()?,
            arguments: dec.read_table()?,
        })
    }
}

/// exchange.declare-ok (11)
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DeclareOk;

impl DeclareOk {
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

/// exchange.delete (20)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delete {
    /// Reserved ticket (0).
    pub reserved_1: u16,
    /// Exchange name.
    pub exchange: String,
    /// Only delete if unused.
    pub if_unused: bool,
    /// Do not send delete-ok.
    pub no_wait: bool,
}

impl Delete {
    /// Method id.
    pub const METHOD_ID: u16 = 20;

    /// Encode arguments.
    pub fn encode_args(&self, enc: &mut Encoder) -> Result<()> {
        enc.write_short(self.reserved_1);
        enc.write_shortstr(&self.exchange)?;
        enc.write_bit(self.if_unused);
        enc.write_bit(self.no_wait);
        Ok(())
    }

    /// Decode arguments.
    pub fn decode_args(dec: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            reserved_1: dec.read_short()?,
            exchange: dec.read_shortstr()?,
            if_unused: dec.read_bit()?,
            no_wait: dec.read_bit()?,
        })
    }
}

/// exchange.delete-ok (21)
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DeleteOk;

impl DeleteOk {
    /// Method id.
    pub const METHOD_ID: u16 = 21;

    /// Encode arguments (empty).
    pub fn encode_args(&self, _enc: &mut Encoder) -> Result<()> {
        Ok(())
    }

    /// Decode arguments (empty).
    pub fn decode_args(_dec: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self)
    }
}

/// exchange.bind (30) — decoded for completeness; server returns 540 until E2E.
#[derive(Debug, Clone, PartialEq)]
pub struct Bind {
    /// Reserved ticket (0).
    pub reserved_1: u16,
    /// Destination exchange.
    pub destination: String,
    /// Source exchange.
    pub source: String,
    /// Routing key.
    pub routing_key: String,
    /// Do not send bind-ok.
    pub no_wait: bool,
    /// Optional arguments.
    pub arguments: FieldTable,
}

impl Bind {
    /// Method id.
    pub const METHOD_ID: u16 = 30;

    /// Encode arguments.
    pub fn encode_args(&self, enc: &mut Encoder) -> Result<()> {
        enc.write_short(self.reserved_1);
        enc.write_shortstr(&self.destination)?;
        enc.write_shortstr(&self.source)?;
        enc.write_shortstr(&self.routing_key)?;
        enc.write_bit(self.no_wait);
        enc.write_table(&self.arguments)?;
        Ok(())
    }

    /// Decode arguments.
    pub fn decode_args(dec: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            reserved_1: dec.read_short()?,
            destination: dec.read_shortstr()?,
            source: dec.read_shortstr()?,
            routing_key: dec.read_shortstr()?,
            no_wait: dec.read_bit()?,
            arguments: dec.read_table()?,
        })
    }
}

/// exchange.bind-ok (31)
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BindOk;

impl BindOk {
    /// Method id.
    pub const METHOD_ID: u16 = 31;

    /// Encode arguments (empty).
    pub fn encode_args(&self, _enc: &mut Encoder) -> Result<()> {
        Ok(())
    }

    /// Decode arguments (empty).
    pub fn decode_args(_dec: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self)
    }
}

/// exchange.unbind (40)
#[derive(Debug, Clone, PartialEq)]
pub struct Unbind {
    /// Reserved ticket (0).
    pub reserved_1: u16,
    /// Destination exchange.
    pub destination: String,
    /// Source exchange.
    pub source: String,
    /// Routing key.
    pub routing_key: String,
    /// Do not send unbind-ok.
    pub no_wait: bool,
    /// Optional arguments.
    pub arguments: FieldTable,
}

impl Unbind {
    /// Method id.
    pub const METHOD_ID: u16 = 40;

    /// Encode arguments.
    pub fn encode_args(&self, enc: &mut Encoder) -> Result<()> {
        enc.write_short(self.reserved_1);
        enc.write_shortstr(&self.destination)?;
        enc.write_shortstr(&self.source)?;
        enc.write_shortstr(&self.routing_key)?;
        enc.write_bit(self.no_wait);
        enc.write_table(&self.arguments)?;
        Ok(())
    }

    /// Decode arguments.
    pub fn decode_args(dec: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            reserved_1: dec.read_short()?,
            destination: dec.read_shortstr()?,
            source: dec.read_shortstr()?,
            routing_key: dec.read_shortstr()?,
            no_wait: dec.read_bit()?,
            arguments: dec.read_table()?,
        })
    }
}

/// exchange.unbind-ok (51)
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct UnbindOk;

impl UnbindOk {
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
