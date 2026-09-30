//! Basic class methods (class-id 60).

use crate::error::Result;
use crate::types::{Decoder, Encoder, FieldTable};

/// AMQP basic class id.
pub const CLASS_ID: u16 = 60;

/// basic.qos (10)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Qos {
    /// Prefetch window in octets (0 = no limit); ignored by most brokers.
    pub prefetch_size: u32,
    /// Prefetch window in whole messages (0 = unlimited).
    pub prefetch_count: u16,
    /// Apply to whole connection when true (RabbitMQ: global vs per-channel).
    pub global: bool,
}

impl Qos {
    /// Method id.
    pub const METHOD_ID: u16 = 10;

    /// Encode arguments.
    pub fn encode_args(&self, enc: &mut Encoder) -> Result<()> {
        enc.write_long(self.prefetch_size);
        enc.write_short(self.prefetch_count);
        enc.write_bit(self.global);
        Ok(())
    }

    /// Decode arguments.
    pub fn decode_args(dec: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            prefetch_size: dec.read_long()?,
            prefetch_count: dec.read_short()?,
            global: dec.read_bit()?,
        })
    }
}

/// basic.qos-ok (11)
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct QosOk;

impl QosOk {
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

/// basic.consume (20)
#[derive(Debug, Clone, PartialEq)]
pub struct Consume {
    /// Reserved ticket (0).
    pub reserved_1: u16,
    /// Queue name.
    pub queue: String,
    /// Consumer tag (empty = server-generated).
    pub consumer_tag: String,
    /// Do not receive publishes from this connection (rarely used).
    pub no_local: bool,
    /// Auto-ack deliveries.
    pub no_ack: bool,
    /// Exclusive consumer.
    pub exclusive: bool,
    /// Do not send consume-ok.
    pub no_wait: bool,
    /// Optional arguments.
    pub arguments: FieldTable,
}

impl Consume {
    /// Method id.
    pub const METHOD_ID: u16 = 20;

    /// Encode arguments.
    pub fn encode_args(&self, enc: &mut Encoder) -> Result<()> {
        enc.write_short(self.reserved_1);
        enc.write_shortstr(&self.queue)?;
        enc.write_shortstr(&self.consumer_tag)?;
        enc.write_bit(self.no_local);
        enc.write_bit(self.no_ack);
        enc.write_bit(self.exclusive);
        enc.write_bit(self.no_wait);
        enc.write_table(&self.arguments)?;
        Ok(())
    }

    /// Decode arguments.
    pub fn decode_args(dec: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            reserved_1: dec.read_short()?,
            queue: dec.read_shortstr()?,
            consumer_tag: dec.read_shortstr()?,
            no_local: dec.read_bit()?,
            no_ack: dec.read_bit()?,
            exclusive: dec.read_bit()?,
            no_wait: dec.read_bit()?,
            arguments: dec.read_table()?,
        })
    }
}

/// basic.consume-ok (21)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumeOk {
    /// Consumer tag (server-generated if client sent empty).
    pub consumer_tag: String,
}

impl ConsumeOk {
    /// Method id.
    pub const METHOD_ID: u16 = 21;

    /// Encode arguments.
    pub fn encode_args(&self, enc: &mut Encoder) -> Result<()> {
        enc.write_shortstr(&self.consumer_tag)?;
        Ok(())
    }

    /// Decode arguments.
    pub fn decode_args(dec: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            consumer_tag: dec.read_shortstr()?,
        })
    }
}

/// basic.cancel (30)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cancel {
    /// Consumer tag to cancel.
    pub consumer_tag: String,
    /// Do not send cancel-ok.
    pub no_wait: bool,
}

impl Cancel {
    /// Method id.
    pub const METHOD_ID: u16 = 30;

    /// Encode arguments.
    pub fn encode_args(&self, enc: &mut Encoder) -> Result<()> {
        enc.write_shortstr(&self.consumer_tag)?;
        enc.write_bit(self.no_wait);
        Ok(())
    }

    /// Decode arguments.
    pub fn decode_args(dec: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            consumer_tag: dec.read_shortstr()?,
            no_wait: dec.read_bit()?,
        })
    }
}

/// basic.cancel-ok (31)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CancelOk {
    /// Cancelled consumer tag.
    pub consumer_tag: String,
}

impl CancelOk {
    /// Method id.
    pub const METHOD_ID: u16 = 31;

    /// Encode arguments.
    pub fn encode_args(&self, enc: &mut Encoder) -> Result<()> {
        enc.write_shortstr(&self.consumer_tag)?;
        Ok(())
    }

    /// Decode arguments.
    pub fn decode_args(dec: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            consumer_tag: dec.read_shortstr()?,
        })
    }
}

/// basic.publish (40)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Publish {
    /// Reserved ticket (0).
    pub reserved_1: u16,
    /// Exchange name (empty = default exchange).
    pub exchange: String,
    /// Routing key.
    pub routing_key: String,
    /// Return unroutable messages to the publisher.
    pub mandatory: bool,
    /// Immediate delivery (QueueForge: not implemented → 540).
    pub immediate: bool,
}

impl Publish {
    /// Method id.
    pub const METHOD_ID: u16 = 40;

    /// Encode arguments.
    pub fn encode_args(&self, enc: &mut Encoder) -> Result<()> {
        enc.write_short(self.reserved_1);
        enc.write_shortstr(&self.exchange)?;
        enc.write_shortstr(&self.routing_key)?;
        enc.write_bit(self.mandatory);
        enc.write_bit(self.immediate);
        Ok(())
    }

    /// Decode arguments.
    pub fn decode_args(dec: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            reserved_1: dec.read_short()?,
            exchange: dec.read_shortstr()?,
            routing_key: dec.read_shortstr()?,
            mandatory: dec.read_bit()?,
            immediate: dec.read_bit()?,
        })
    }
}

/// basic.return (50)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Return {
    /// Reply code (e.g. 312 NO_ROUTE).
    pub reply_code: u16,
    /// Reply text.
    pub reply_text: String,
    /// Exchange the message was published to.
    pub exchange: String,
    /// Routing key used.
    pub routing_key: String,
}

impl Return {
    /// Method id.
    pub const METHOD_ID: u16 = 50;

    /// Encode arguments.
    pub fn encode_args(&self, enc: &mut Encoder) -> Result<()> {
        enc.write_short(self.reply_code);
        enc.write_shortstr(&self.reply_text)?;
        enc.write_shortstr(&self.exchange)?;
        enc.write_shortstr(&self.routing_key)?;
        Ok(())
    }

    /// Decode arguments.
    pub fn decode_args(dec: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            reply_code: dec.read_short()?,
            reply_text: dec.read_shortstr()?,
            exchange: dec.read_shortstr()?,
            routing_key: dec.read_shortstr()?,
        })
    }
}

/// basic.deliver (60)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Deliver {
    /// Consumer tag.
    pub consumer_tag: String,
    /// Per-channel delivery tag.
    pub delivery_tag: u64,
    /// Message was previously delivered.
    pub redelivered: bool,
    /// Exchange the message was published to.
    pub exchange: String,
    /// Routing key.
    pub routing_key: String,
}

impl Deliver {
    /// Method id.
    pub const METHOD_ID: u16 = 60;

    /// Encode arguments.
    pub fn encode_args(&self, enc: &mut Encoder) -> Result<()> {
        enc.write_shortstr(&self.consumer_tag)?;
        enc.write_longlong(self.delivery_tag);
        enc.write_bit(self.redelivered);
        enc.write_shortstr(&self.exchange)?;
        enc.write_shortstr(&self.routing_key)?;
        Ok(())
    }

    /// Decode arguments.
    pub fn decode_args(dec: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            consumer_tag: dec.read_shortstr()?,
            delivery_tag: dec.read_longlong()?,
            redelivered: dec.read_bit()?,
            exchange: dec.read_shortstr()?,
            routing_key: dec.read_shortstr()?,
        })
    }
}

/// basic.get (70)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Get {
    /// Reserved ticket (0).
    pub reserved_1: u16,
    /// Queue name.
    pub queue: String,
    /// Auto-ack the message.
    pub no_ack: bool,
}

impl Get {
    /// Method id.
    pub const METHOD_ID: u16 = 70;

    /// Encode arguments.
    pub fn encode_args(&self, enc: &mut Encoder) -> Result<()> {
        enc.write_short(self.reserved_1);
        enc.write_shortstr(&self.queue)?;
        enc.write_bit(self.no_ack);
        Ok(())
    }

    /// Decode arguments.
    pub fn decode_args(dec: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            reserved_1: dec.read_short()?,
            queue: dec.read_shortstr()?,
            no_ack: dec.read_bit()?,
        })
    }
}

/// basic.get-ok (71)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetOk {
    /// Per-channel delivery tag.
    pub delivery_tag: u64,
    /// Message was previously delivered.
    pub redelivered: bool,
    /// Exchange the message was published to.
    pub exchange: String,
    /// Routing key.
    pub routing_key: String,
    /// Messages remaining in the queue.
    pub message_count: u32,
}

impl GetOk {
    /// Method id.
    pub const METHOD_ID: u16 = 71;

    /// Encode arguments.
    pub fn encode_args(&self, enc: &mut Encoder) -> Result<()> {
        enc.write_longlong(self.delivery_tag);
        enc.write_bit(self.redelivered);
        enc.write_shortstr(&self.exchange)?;
        enc.write_shortstr(&self.routing_key)?;
        enc.write_long(self.message_count);
        Ok(())
    }

    /// Decode arguments.
    pub fn decode_args(dec: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            delivery_tag: dec.read_longlong()?,
            redelivered: dec.read_bit()?,
            exchange: dec.read_shortstr()?,
            routing_key: dec.read_shortstr()?,
            message_count: dec.read_long()?,
        })
    }
}

/// basic.get-empty (72)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetEmpty {
    /// Deprecated reserved field; empty shortstr.
    pub reserved_1: String,
}

impl GetEmpty {
    /// Method id.
    pub const METHOD_ID: u16 = 72;

    /// Default get-empty.
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

impl Default for GetEmpty {
    fn default() -> Self {
        Self::new()
    }
}

/// basic.ack (80)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ack {
    /// Delivery tag to acknowledge (0 with multiple = all).
    pub delivery_tag: u64,
    /// Acknowledge all outstanding up to and including `delivery_tag`.
    pub multiple: bool,
}

impl Ack {
    /// Method id.
    pub const METHOD_ID: u16 = 80;

    /// Encode arguments.
    pub fn encode_args(&self, enc: &mut Encoder) -> Result<()> {
        enc.write_longlong(self.delivery_tag);
        enc.write_bit(self.multiple);
        Ok(())
    }

    /// Decode arguments.
    pub fn decode_args(dec: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            delivery_tag: dec.read_longlong()?,
            multiple: dec.read_bit()?,
        })
    }
}

/// basic.reject (90)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reject {
    /// Delivery tag to reject.
    pub delivery_tag: u64,
    /// Requeue the message when true.
    pub requeue: bool,
}

impl Reject {
    /// Method id.
    pub const METHOD_ID: u16 = 90;

    /// Encode arguments.
    pub fn encode_args(&self, enc: &mut Encoder) -> Result<()> {
        enc.write_longlong(self.delivery_tag);
        enc.write_bit(self.requeue);
        Ok(())
    }

    /// Decode arguments.
    pub fn decode_args(dec: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            delivery_tag: dec.read_longlong()?,
            requeue: dec.read_bit()?,
        })
    }
}

/// basic.recover (110)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Recover {
    /// Requeue unacknowledged messages when true.
    pub requeue: bool,
}

impl Recover {
    /// Method id.
    pub const METHOD_ID: u16 = 110;

    /// Encode arguments.
    pub fn encode_args(&self, enc: &mut Encoder) -> Result<()> {
        enc.write_bit(self.requeue);
        Ok(())
    }

    /// Decode arguments.
    pub fn decode_args(dec: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            requeue: dec.read_bit()?,
        })
    }
}

/// basic.recover-ok (111)
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RecoverOk;

impl RecoverOk {
    /// Method id.
    pub const METHOD_ID: u16 = 111;

    /// Encode arguments (empty).
    pub fn encode_args(&self, _enc: &mut Encoder) -> Result<()> {
        Ok(())
    }

    /// Decode arguments (empty).
    pub fn decode_args(_dec: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self)
    }
}

/// basic.nack (120) — RabbitMQ extension, widely supported.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Nack {
    /// Delivery tag to negatively acknowledge.
    pub delivery_tag: u64,
    /// Nack all outstanding up to and including `delivery_tag`.
    pub multiple: bool,
    /// Requeue when true.
    pub requeue: bool,
}

impl Nack {
    /// Method id.
    pub const METHOD_ID: u16 = 120;

    /// Encode arguments.
    pub fn encode_args(&self, enc: &mut Encoder) -> Result<()> {
        enc.write_longlong(self.delivery_tag);
        enc.write_bit(self.multiple);
        enc.write_bit(self.requeue);
        Ok(())
    }

    /// Decode arguments.
    pub fn decode_args(dec: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            delivery_tag: dec.read_longlong()?,
            multiple: dec.read_bit()?,
            requeue: dec.read_bit()?,
        })
    }
}
