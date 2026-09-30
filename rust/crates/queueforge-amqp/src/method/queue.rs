//! Queue class methods (class-id 50).

use crate::error::Result;
use crate::types::{Decoder, Encoder, FieldTable};

/// AMQP queue class id.
pub const CLASS_ID: u16 = 50;

/// queue.declare (10)
#[derive(Debug, Clone, PartialEq)]
pub struct Declare {
    /// Reserved ticket (0).
    pub reserved_1: u16,
    /// Queue name (empty = server-generated).
    pub queue: String,
    /// Do not create; assert it exists.
    pub passive: bool,
    /// Survive broker restart.
    pub durable: bool,
    /// Exclusive to this connection.
    pub exclusive: bool,
    /// Delete when last consumer cancels / connection closes.
    pub auto_delete: bool,
    /// Do not send declare-ok.
    pub no_wait: bool,
    /// Optional arguments (`x-max-priority`, etc.).
    pub arguments: FieldTable,
}

impl Declare {
    /// Method id.
    pub const METHOD_ID: u16 = 10;

    /// Encode arguments.
    pub fn encode_args(&self, enc: &mut Encoder) -> Result<()> {
        enc.write_short(self.reserved_1);
        enc.write_shortstr(&self.queue)?;
        enc.write_bit(self.passive);
        enc.write_bit(self.durable);
        enc.write_bit(self.exclusive);
        enc.write_bit(self.auto_delete);
        enc.write_bit(self.no_wait);
        enc.write_table(&self.arguments)?;
        Ok(())
    }

    /// Decode arguments.
    pub fn decode_args(dec: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            reserved_1: dec.read_short()?,
            queue: dec.read_shortstr()?,
            passive: dec.read_bit()?,
            durable: dec.read_bit()?,
            exclusive: dec.read_bit()?,
            auto_delete: dec.read_bit()?,
            no_wait: dec.read_bit()?,
            arguments: dec.read_table()?,
        })
    }
}

/// queue.declare-ok (11)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeclareOk {
    /// Queue name (server-generated if client sent empty).
    pub queue: String,
    /// Messages ready in the queue.
    pub message_count: u32,
    /// Active consumers.
    pub consumer_count: u32,
}

impl DeclareOk {
    /// Method id.
    pub const METHOD_ID: u16 = 11;

    /// Encode arguments.
    pub fn encode_args(&self, enc: &mut Encoder) -> Result<()> {
        enc.write_shortstr(&self.queue)?;
        enc.write_long(self.message_count);
        enc.write_long(self.consumer_count);
        Ok(())
    }

    /// Decode arguments.
    pub fn decode_args(dec: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            queue: dec.read_shortstr()?,
            message_count: dec.read_long()?,
            consumer_count: dec.read_long()?,
        })
    }
}

/// queue.bind (20)
#[derive(Debug, Clone, PartialEq)]
pub struct Bind {
    /// Reserved ticket (0).
    pub reserved_1: u16,
    /// Queue name.
    pub queue: String,
    /// Exchange name.
    pub exchange: String,
    /// Routing key.
    pub routing_key: String,
    /// Do not send bind-ok.
    pub no_wait: bool,
    /// Optional arguments.
    pub arguments: FieldTable,
}

impl Bind {
    /// Method id.
    pub const METHOD_ID: u16 = 20;

    /// Encode arguments.
    pub fn encode_args(&self, enc: &mut Encoder) -> Result<()> {
        enc.write_short(self.reserved_1);
        enc.write_shortstr(&self.queue)?;
        enc.write_shortstr(&self.exchange)?;
        enc.write_shortstr(&self.routing_key)?;
        enc.write_bit(self.no_wait);
        enc.write_table(&self.arguments)?;
        Ok(())
    }

    /// Decode arguments.
    pub fn decode_args(dec: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            reserved_1: dec.read_short()?,
            queue: dec.read_shortstr()?,
            exchange: dec.read_shortstr()?,
            routing_key: dec.read_shortstr()?,
            no_wait: dec.read_bit()?,
            arguments: dec.read_table()?,
        })
    }
}

/// queue.bind-ok (21)
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BindOk;

impl BindOk {
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

/// queue.unbind (50)
#[derive(Debug, Clone, PartialEq)]
pub struct Unbind {
    /// Reserved ticket (0).
    pub reserved_1: u16,
    /// Queue name.
    pub queue: String,
    /// Exchange name.
    pub exchange: String,
    /// Routing key.
    pub routing_key: String,
    /// Optional arguments.
    pub arguments: FieldTable,
}

impl Unbind {
    /// Method id.
    pub const METHOD_ID: u16 = 50;

    /// Encode arguments.
    pub fn encode_args(&self, enc: &mut Encoder) -> Result<()> {
        enc.write_short(self.reserved_1);
        enc.write_shortstr(&self.queue)?;
        enc.write_shortstr(&self.exchange)?;
        enc.write_shortstr(&self.routing_key)?;
        enc.write_table(&self.arguments)?;
        Ok(())
    }

    /// Decode arguments.
    pub fn decode_args(dec: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            reserved_1: dec.read_short()?,
            queue: dec.read_shortstr()?,
            exchange: dec.read_shortstr()?,
            routing_key: dec.read_shortstr()?,
            arguments: dec.read_table()?,
        })
    }
}

/// queue.unbind-ok (51)
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

/// queue.purge (30)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Purge {
    /// Reserved ticket (0).
    pub reserved_1: u16,
    /// Queue name.
    pub queue: String,
    /// Do not send purge-ok.
    pub no_wait: bool,
}

impl Purge {
    /// Method id.
    pub const METHOD_ID: u16 = 30;

    /// Encode arguments.
    pub fn encode_args(&self, enc: &mut Encoder) -> Result<()> {
        enc.write_short(self.reserved_1);
        enc.write_shortstr(&self.queue)?;
        enc.write_bit(self.no_wait);
        Ok(())
    }

    /// Decode arguments.
    pub fn decode_args(dec: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            reserved_1: dec.read_short()?,
            queue: dec.read_shortstr()?,
            no_wait: dec.read_bit()?,
        })
    }
}

/// queue.purge-ok (31)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PurgeOk {
    /// Messages purged.
    pub message_count: u32,
}

impl PurgeOk {
    /// Method id.
    pub const METHOD_ID: u16 = 31;

    /// Encode arguments.
    pub fn encode_args(&self, enc: &mut Encoder) -> Result<()> {
        enc.write_long(self.message_count);
        Ok(())
    }

    /// Decode arguments.
    pub fn decode_args(dec: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            message_count: dec.read_long()?,
        })
    }
}

/// queue.delete (40)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delete {
    /// Reserved ticket (0).
    pub reserved_1: u16,
    /// Queue name.
    pub queue: String,
    /// Only delete if unused.
    pub if_unused: bool,
    /// Only delete if empty.
    pub if_empty: bool,
    /// Do not send delete-ok.
    pub no_wait: bool,
}

impl Delete {
    /// Method id.
    pub const METHOD_ID: u16 = 40;

    /// Encode arguments.
    pub fn encode_args(&self, enc: &mut Encoder) -> Result<()> {
        enc.write_short(self.reserved_1);
        enc.write_shortstr(&self.queue)?;
        enc.write_bit(self.if_unused);
        enc.write_bit(self.if_empty);
        enc.write_bit(self.no_wait);
        Ok(())
    }

    /// Decode arguments.
    pub fn decode_args(dec: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            reserved_1: dec.read_short()?,
            queue: dec.read_shortstr()?,
            if_unused: dec.read_bit()?,
            if_empty: dec.read_bit()?,
            no_wait: dec.read_bit()?,
        })
    }
}

/// queue.delete-ok (41)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteOk {
    /// Messages deleted with the queue.
    pub message_count: u32,
}

impl DeleteOk {
    /// Method id.
    pub const METHOD_ID: u16 = 41;

    /// Encode arguments.
    pub fn encode_args(&self, enc: &mut Encoder) -> Result<()> {
        enc.write_long(self.message_count);
        Ok(())
    }

    /// Decode arguments.
    pub fn decode_args(dec: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            message_count: dec.read_long()?,
        })
    }
}
