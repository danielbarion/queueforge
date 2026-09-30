//! AMQP 0-9-1 method catalog (connection / channel / exchange / queue / basic).
//!
//! Method frame payload layout:
//! ```text
//! class-id (short) | method-id (short) | arguments...
//! ```
//!
//! Class and method ids follow the AMQP 0-9-1 + RabbitMQ extended XML.

pub mod basic;
pub mod channel;
pub mod confirm;
pub mod connection;
pub mod exchange;
pub mod queue;
pub mod tx;

use crate::error::{Error, Result};
use crate::frame::Frame;
use crate::types::{Decoder, Encoder};

/// Top-level AMQP method (class + method + arguments).
#[derive(Debug, Clone, PartialEq)]
pub enum Method {
    // --- connection (10) ---
    /// connection.start
    ConnectionStart(connection::Start),
    /// connection.start-ok
    ConnectionStartOk(connection::StartOk),
    /// connection.tune
    ConnectionTune(connection::Tune),
    /// connection.tune-ok
    ConnectionTuneOk(connection::TuneOk),
    /// connection.open
    ConnectionOpen(connection::Open),
    /// connection.open-ok
    ConnectionOpenOk(connection::OpenOk),
    /// connection.close
    ConnectionClose(connection::Close),
    /// connection.close-ok
    ConnectionCloseOk(connection::CloseOk),

    // --- channel (20) ---
    /// channel.open
    ChannelOpen(channel::Open),
    /// channel.open-ok
    ChannelOpenOk(channel::OpenOk),
    /// channel.flow
    ChannelFlow(channel::Flow),
    /// channel.flow-ok
    ChannelFlowOk(channel::FlowOk),
    /// channel.close
    ChannelClose(channel::Close),
    /// channel.close-ok
    ChannelCloseOk(channel::CloseOk),

    // --- exchange (40) ---
    /// exchange.declare
    ExchangeDeclare(exchange::Declare),
    /// exchange.declare-ok
    ExchangeDeclareOk(exchange::DeclareOk),
    /// exchange.delete
    ExchangeDelete(exchange::Delete),
    /// exchange.delete-ok
    ExchangeDeleteOk(exchange::DeleteOk),
    /// exchange.bind (decoded; server may reply 540 until E2E)
    ExchangeBind(exchange::Bind),
    /// exchange.bind-ok
    ExchangeBindOk(exchange::BindOk),
    /// exchange.unbind
    ExchangeUnbind(exchange::Unbind),
    /// exchange.unbind-ok
    ExchangeUnbindOk(exchange::UnbindOk),

    // --- queue (50) ---
    /// queue.declare
    QueueDeclare(queue::Declare),
    /// queue.declare-ok
    QueueDeclareOk(queue::DeclareOk),
    /// queue.bind
    QueueBind(queue::Bind),
    /// queue.bind-ok
    QueueBindOk(queue::BindOk),
    /// queue.unbind
    QueueUnbind(queue::Unbind),
    /// queue.unbind-ok
    QueueUnbindOk(queue::UnbindOk),
    /// queue.purge
    QueuePurge(queue::Purge),
    /// queue.purge-ok
    QueuePurgeOk(queue::PurgeOk),
    /// queue.delete
    QueueDelete(queue::Delete),
    /// queue.delete-ok
    QueueDeleteOk(queue::DeleteOk),

    // --- basic (60) ---
    /// basic.qos
    BasicQos(basic::Qos),
    /// basic.qos-ok
    BasicQosOk(basic::QosOk),
    /// basic.consume
    BasicConsume(basic::Consume),
    /// basic.consume-ok
    BasicConsumeOk(basic::ConsumeOk),
    /// basic.cancel
    BasicCancel(basic::Cancel),
    /// basic.cancel-ok
    BasicCancelOk(basic::CancelOk),
    /// basic.publish
    BasicPublish(basic::Publish),
    /// basic.return
    BasicReturn(basic::Return),
    /// basic.deliver
    BasicDeliver(basic::Deliver),
    /// basic.get
    BasicGet(basic::Get),
    /// basic.get-ok
    BasicGetOk(basic::GetOk),
    /// basic.get-empty
    BasicGetEmpty(basic::GetEmpty),
    /// basic.ack
    BasicAck(basic::Ack),
    /// basic.reject
    BasicReject(basic::Reject),
    /// basic.recover
    BasicRecover(basic::Recover),
    /// basic.recover-ok
    BasicRecoverOk(basic::RecoverOk),
    /// basic.nack
    BasicNack(basic::Nack),

    // --- confirm (85) — RabbitMQ publisher confirms ---
    /// confirm.select
    ConfirmSelect(confirm::Select),
    /// confirm.select-ok
    ConfirmSelectOk(confirm::SelectOk),

    /// tx.select
    TxSelect(tx::Select),
    /// tx.select-ok
    TxSelectOk(tx::SelectOk),
    /// tx.commit
    TxCommit(tx::Commit),
    /// tx.commit-ok
    TxCommitOk(tx::CommitOk),
    /// tx.rollback
    TxRollback(tx::Rollback),
    /// tx.rollback-ok
    TxRollbackOk(tx::RollbackOk),
}

impl Method {
    /// AMQP class id for this method.
    pub fn class_id(&self) -> u16 {
        match self {
            Self::ConnectionStart(_)
            | Self::ConnectionStartOk(_)
            | Self::ConnectionTune(_)
            | Self::ConnectionTuneOk(_)
            | Self::ConnectionOpen(_)
            | Self::ConnectionOpenOk(_)
            | Self::ConnectionClose(_)
            | Self::ConnectionCloseOk(_) => connection::CLASS_ID,

            Self::ChannelOpen(_)
            | Self::ChannelOpenOk(_)
            | Self::ChannelFlow(_)
            | Self::ChannelFlowOk(_)
            | Self::ChannelClose(_)
            | Self::ChannelCloseOk(_) => channel::CLASS_ID,

            Self::ExchangeDeclare(_)
            | Self::ExchangeDeclareOk(_)
            | Self::ExchangeDelete(_)
            | Self::ExchangeDeleteOk(_)
            | Self::ExchangeBind(_)
            | Self::ExchangeBindOk(_)
            | Self::ExchangeUnbind(_)
            | Self::ExchangeUnbindOk(_) => exchange::CLASS_ID,

            Self::QueueDeclare(_)
            | Self::QueueDeclareOk(_)
            | Self::QueueBind(_)
            | Self::QueueBindOk(_)
            | Self::QueueUnbind(_)
            | Self::QueueUnbindOk(_)
            | Self::QueuePurge(_)
            | Self::QueuePurgeOk(_)
            | Self::QueueDelete(_)
            | Self::QueueDeleteOk(_) => queue::CLASS_ID,

            Self::BasicQos(_)
            | Self::BasicQosOk(_)
            | Self::BasicConsume(_)
            | Self::BasicConsumeOk(_)
            | Self::BasicCancel(_)
            | Self::BasicCancelOk(_)
            | Self::BasicPublish(_)
            | Self::BasicReturn(_)
            | Self::BasicDeliver(_)
            | Self::BasicGet(_)
            | Self::BasicGetOk(_)
            | Self::BasicGetEmpty(_)
            | Self::BasicAck(_)
            | Self::BasicReject(_)
            | Self::BasicRecover(_)
            | Self::BasicRecoverOk(_)
            | Self::BasicNack(_) => basic::CLASS_ID,

            Self::ConfirmSelect(_) | Self::ConfirmSelectOk(_) => confirm::CLASS_ID,

            Self::TxSelect(_)
            | Self::TxSelectOk(_)
            | Self::TxCommit(_)
            | Self::TxCommitOk(_)
            | Self::TxRollback(_)
            | Self::TxRollbackOk(_) => tx::CLASS_ID,
        }
    }

    /// AMQP method id within the class.
    pub fn method_id(&self) -> u16 {
        match self {
            Self::ConnectionStart(_) => connection::Start::METHOD_ID,
            Self::ConnectionStartOk(_) => connection::StartOk::METHOD_ID,
            Self::ConnectionTune(_) => connection::Tune::METHOD_ID,
            Self::ConnectionTuneOk(_) => connection::TuneOk::METHOD_ID,
            Self::ConnectionOpen(_) => connection::Open::METHOD_ID,
            Self::ConnectionOpenOk(_) => connection::OpenOk::METHOD_ID,
            Self::ConnectionClose(_) => connection::Close::METHOD_ID,
            Self::ConnectionCloseOk(_) => connection::CloseOk::METHOD_ID,

            Self::ChannelOpen(_) => channel::Open::METHOD_ID,
            Self::ChannelOpenOk(_) => channel::OpenOk::METHOD_ID,
            Self::ChannelFlow(_) => channel::Flow::METHOD_ID,
            Self::ChannelFlowOk(_) => channel::FlowOk::METHOD_ID,
            Self::ChannelClose(_) => channel::Close::METHOD_ID,
            Self::ChannelCloseOk(_) => channel::CloseOk::METHOD_ID,

            Self::ExchangeDeclare(_) => exchange::Declare::METHOD_ID,
            Self::ExchangeDeclareOk(_) => exchange::DeclareOk::METHOD_ID,
            Self::ExchangeDelete(_) => exchange::Delete::METHOD_ID,
            Self::ExchangeDeleteOk(_) => exchange::DeleteOk::METHOD_ID,
            Self::ExchangeBind(_) => exchange::Bind::METHOD_ID,
            Self::ExchangeBindOk(_) => exchange::BindOk::METHOD_ID,
            Self::ExchangeUnbind(_) => exchange::Unbind::METHOD_ID,
            Self::ExchangeUnbindOk(_) => exchange::UnbindOk::METHOD_ID,

            Self::QueueDeclare(_) => queue::Declare::METHOD_ID,
            Self::QueueDeclareOk(_) => queue::DeclareOk::METHOD_ID,
            Self::QueueBind(_) => queue::Bind::METHOD_ID,
            Self::QueueBindOk(_) => queue::BindOk::METHOD_ID,
            Self::QueueUnbind(_) => queue::Unbind::METHOD_ID,
            Self::QueueUnbindOk(_) => queue::UnbindOk::METHOD_ID,
            Self::QueuePurge(_) => queue::Purge::METHOD_ID,
            Self::QueuePurgeOk(_) => queue::PurgeOk::METHOD_ID,
            Self::QueueDelete(_) => queue::Delete::METHOD_ID,
            Self::QueueDeleteOk(_) => queue::DeleteOk::METHOD_ID,

            Self::BasicQos(_) => basic::Qos::METHOD_ID,
            Self::BasicQosOk(_) => basic::QosOk::METHOD_ID,
            Self::BasicConsume(_) => basic::Consume::METHOD_ID,
            Self::BasicConsumeOk(_) => basic::ConsumeOk::METHOD_ID,
            Self::BasicCancel(_) => basic::Cancel::METHOD_ID,
            Self::BasicCancelOk(_) => basic::CancelOk::METHOD_ID,
            Self::BasicPublish(_) => basic::Publish::METHOD_ID,
            Self::BasicReturn(_) => basic::Return::METHOD_ID,
            Self::BasicDeliver(_) => basic::Deliver::METHOD_ID,
            Self::BasicGet(_) => basic::Get::METHOD_ID,
            Self::BasicGetOk(_) => basic::GetOk::METHOD_ID,
            Self::BasicGetEmpty(_) => basic::GetEmpty::METHOD_ID,
            Self::BasicAck(_) => basic::Ack::METHOD_ID,
            Self::BasicReject(_) => basic::Reject::METHOD_ID,
            Self::BasicRecover(_) => basic::Recover::METHOD_ID,
            Self::BasicRecoverOk(_) => basic::RecoverOk::METHOD_ID,
            Self::BasicNack(_) => basic::Nack::METHOD_ID,

            Self::ConfirmSelect(_) => confirm::Select::METHOD_ID,
            Self::ConfirmSelectOk(_) => confirm::SelectOk::METHOD_ID,

            Self::TxSelect(_) => tx::Select::METHOD_ID,
            Self::TxSelectOk(_) => tx::SelectOk::METHOD_ID,
            Self::TxCommit(_) => tx::Commit::METHOD_ID,
            Self::TxCommitOk(_) => tx::CommitOk::METHOD_ID,
            Self::TxRollback(_) => tx::Rollback::METHOD_ID,
            Self::TxRollbackOk(_) => tx::RollbackOk::METHOD_ID,
        }
    }

    /// Encode this method into a method-frame payload (class + method + args).
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut enc = Encoder::with_capacity(64);
        enc.write_short(self.class_id());
        enc.write_short(self.method_id());
        self.encode_args(&mut enc)?;
        Ok(enc.finish())
    }

    /// Encode arguments only (after class/method ids already written).
    fn encode_args(&self, enc: &mut Encoder) -> Result<()> {
        match self {
            Self::ConnectionStart(m) => m.encode_args(enc),
            Self::ConnectionStartOk(m) => m.encode_args(enc),
            Self::ConnectionTune(m) => m.encode_args(enc),
            Self::ConnectionTuneOk(m) => m.encode_args(enc),
            Self::ConnectionOpen(m) => m.encode_args(enc),
            Self::ConnectionOpenOk(m) => m.encode_args(enc),
            Self::ConnectionClose(m) => m.encode_args(enc),
            Self::ConnectionCloseOk(m) => m.encode_args(enc),

            Self::ChannelOpen(m) => m.encode_args(enc),
            Self::ChannelOpenOk(m) => m.encode_args(enc),
            Self::ChannelFlow(m) => m.encode_args(enc),
            Self::ChannelFlowOk(m) => m.encode_args(enc),
            Self::ChannelClose(m) => m.encode_args(enc),
            Self::ChannelCloseOk(m) => m.encode_args(enc),

            Self::ExchangeDeclare(m) => m.encode_args(enc),
            Self::ExchangeDeclareOk(m) => m.encode_args(enc),
            Self::ExchangeDelete(m) => m.encode_args(enc),
            Self::ExchangeDeleteOk(m) => m.encode_args(enc),
            Self::ExchangeBind(m) => m.encode_args(enc),
            Self::ExchangeBindOk(m) => m.encode_args(enc),
            Self::ExchangeUnbind(m) => m.encode_args(enc),
            Self::ExchangeUnbindOk(m) => m.encode_args(enc),

            Self::QueueDeclare(m) => m.encode_args(enc),
            Self::QueueDeclareOk(m) => m.encode_args(enc),
            Self::QueueBind(m) => m.encode_args(enc),
            Self::QueueBindOk(m) => m.encode_args(enc),
            Self::QueueUnbind(m) => m.encode_args(enc),
            Self::QueueUnbindOk(m) => m.encode_args(enc),
            Self::QueuePurge(m) => m.encode_args(enc),
            Self::QueuePurgeOk(m) => m.encode_args(enc),
            Self::QueueDelete(m) => m.encode_args(enc),
            Self::QueueDeleteOk(m) => m.encode_args(enc),

            Self::BasicQos(m) => m.encode_args(enc),
            Self::BasicQosOk(m) => m.encode_args(enc),
            Self::BasicConsume(m) => m.encode_args(enc),
            Self::BasicConsumeOk(m) => m.encode_args(enc),
            Self::BasicCancel(m) => m.encode_args(enc),
            Self::BasicCancelOk(m) => m.encode_args(enc),
            Self::BasicPublish(m) => m.encode_args(enc),
            Self::BasicReturn(m) => m.encode_args(enc),
            Self::BasicDeliver(m) => m.encode_args(enc),
            Self::BasicGet(m) => m.encode_args(enc),
            Self::BasicGetOk(m) => m.encode_args(enc),
            Self::BasicGetEmpty(m) => m.encode_args(enc),
            Self::BasicAck(m) => m.encode_args(enc),
            Self::BasicReject(m) => m.encode_args(enc),
            Self::BasicRecover(m) => m.encode_args(enc),
            Self::BasicRecoverOk(m) => m.encode_args(enc),
            Self::BasicNack(m) => m.encode_args(enc),

            Self::ConfirmSelect(m) => m.encode_args(enc),
            Self::ConfirmSelectOk(m) => m.encode_args(enc),
            Self::TxSelect(m) => m.encode_args(enc),
            Self::TxSelectOk(m) => m.encode_args(enc),
            Self::TxCommit(m) => m.encode_args(enc),
            Self::TxCommitOk(m) => m.encode_args(enc),
            Self::TxRollback(m) => m.encode_args(enc),
            Self::TxRollbackOk(m) => m.encode_args(enc),
        }
    }

    /// Decode a method from a method-frame payload.
    pub fn decode(payload: &[u8]) -> Result<Self> {
        let mut dec = Decoder::new(payload);
        let class_id = dec.read_short()?;
        let method_id = dec.read_short()?;
        let method = Self::decode_body(class_id, method_id, &mut dec)?;
        dec.finish()?;
        Ok(method)
    }

    fn decode_body(class_id: u16, method_id: u16, dec: &mut Decoder<'_>) -> Result<Self> {
        match (class_id, method_id) {
            (connection::CLASS_ID, connection::Start::METHOD_ID) => {
                Ok(Self::ConnectionStart(connection::Start::decode_args(dec)?))
            }
            (connection::CLASS_ID, connection::StartOk::METHOD_ID) => Ok(Self::ConnectionStartOk(
                connection::StartOk::decode_args(dec)?,
            )),
            (connection::CLASS_ID, connection::Tune::METHOD_ID) => {
                Ok(Self::ConnectionTune(connection::Tune::decode_args(dec)?))
            }
            (connection::CLASS_ID, connection::TuneOk::METHOD_ID) => Ok(Self::ConnectionTuneOk(
                connection::TuneOk::decode_args(dec)?,
            )),
            (connection::CLASS_ID, connection::Open::METHOD_ID) => {
                Ok(Self::ConnectionOpen(connection::Open::decode_args(dec)?))
            }
            (connection::CLASS_ID, connection::OpenOk::METHOD_ID) => Ok(Self::ConnectionOpenOk(
                connection::OpenOk::decode_args(dec)?,
            )),
            (connection::CLASS_ID, connection::Close::METHOD_ID) => {
                Ok(Self::ConnectionClose(connection::Close::decode_args(dec)?))
            }
            (connection::CLASS_ID, connection::CloseOk::METHOD_ID) => Ok(Self::ConnectionCloseOk(
                connection::CloseOk::decode_args(dec)?,
            )),

            (channel::CLASS_ID, channel::Open::METHOD_ID) => {
                Ok(Self::ChannelOpen(channel::Open::decode_args(dec)?))
            }
            (channel::CLASS_ID, channel::OpenOk::METHOD_ID) => {
                Ok(Self::ChannelOpenOk(channel::OpenOk::decode_args(dec)?))
            }
            (channel::CLASS_ID, channel::Flow::METHOD_ID) => {
                Ok(Self::ChannelFlow(channel::Flow::decode_args(dec)?))
            }
            (channel::CLASS_ID, channel::FlowOk::METHOD_ID) => {
                Ok(Self::ChannelFlowOk(channel::FlowOk::decode_args(dec)?))
            }
            (channel::CLASS_ID, channel::Close::METHOD_ID) => {
                Ok(Self::ChannelClose(channel::Close::decode_args(dec)?))
            }
            (channel::CLASS_ID, channel::CloseOk::METHOD_ID) => {
                Ok(Self::ChannelCloseOk(channel::CloseOk::decode_args(dec)?))
            }

            (exchange::CLASS_ID, exchange::Declare::METHOD_ID) => {
                Ok(Self::ExchangeDeclare(exchange::Declare::decode_args(dec)?))
            }
            (exchange::CLASS_ID, exchange::DeclareOk::METHOD_ID) => Ok(Self::ExchangeDeclareOk(
                exchange::DeclareOk::decode_args(dec)?,
            )),
            (exchange::CLASS_ID, exchange::Delete::METHOD_ID) => {
                Ok(Self::ExchangeDelete(exchange::Delete::decode_args(dec)?))
            }
            (exchange::CLASS_ID, exchange::DeleteOk::METHOD_ID) => Ok(Self::ExchangeDeleteOk(
                exchange::DeleteOk::decode_args(dec)?,
            )),
            (exchange::CLASS_ID, exchange::Bind::METHOD_ID) => {
                Ok(Self::ExchangeBind(exchange::Bind::decode_args(dec)?))
            }
            (exchange::CLASS_ID, exchange::BindOk::METHOD_ID) => {
                Ok(Self::ExchangeBindOk(exchange::BindOk::decode_args(dec)?))
            }
            (exchange::CLASS_ID, exchange::Unbind::METHOD_ID) => {
                Ok(Self::ExchangeUnbind(exchange::Unbind::decode_args(dec)?))
            }
            (exchange::CLASS_ID, exchange::UnbindOk::METHOD_ID) => Ok(Self::ExchangeUnbindOk(
                exchange::UnbindOk::decode_args(dec)?,
            )),

            (queue::CLASS_ID, queue::Declare::METHOD_ID) => {
                Ok(Self::QueueDeclare(queue::Declare::decode_args(dec)?))
            }
            (queue::CLASS_ID, queue::DeclareOk::METHOD_ID) => {
                Ok(Self::QueueDeclareOk(queue::DeclareOk::decode_args(dec)?))
            }
            (queue::CLASS_ID, queue::Bind::METHOD_ID) => {
                Ok(Self::QueueBind(queue::Bind::decode_args(dec)?))
            }
            (queue::CLASS_ID, queue::BindOk::METHOD_ID) => {
                Ok(Self::QueueBindOk(queue::BindOk::decode_args(dec)?))
            }
            (queue::CLASS_ID, queue::Unbind::METHOD_ID) => {
                Ok(Self::QueueUnbind(queue::Unbind::decode_args(dec)?))
            }
            (queue::CLASS_ID, queue::UnbindOk::METHOD_ID) => {
                Ok(Self::QueueUnbindOk(queue::UnbindOk::decode_args(dec)?))
            }
            (queue::CLASS_ID, queue::Purge::METHOD_ID) => {
                Ok(Self::QueuePurge(queue::Purge::decode_args(dec)?))
            }
            (queue::CLASS_ID, queue::PurgeOk::METHOD_ID) => {
                Ok(Self::QueuePurgeOk(queue::PurgeOk::decode_args(dec)?))
            }
            (queue::CLASS_ID, queue::Delete::METHOD_ID) => {
                Ok(Self::QueueDelete(queue::Delete::decode_args(dec)?))
            }
            (queue::CLASS_ID, queue::DeleteOk::METHOD_ID) => {
                Ok(Self::QueueDeleteOk(queue::DeleteOk::decode_args(dec)?))
            }

            (basic::CLASS_ID, basic::Qos::METHOD_ID) => {
                Ok(Self::BasicQos(basic::Qos::decode_args(dec)?))
            }
            (basic::CLASS_ID, basic::QosOk::METHOD_ID) => {
                Ok(Self::BasicQosOk(basic::QosOk::decode_args(dec)?))
            }
            (basic::CLASS_ID, basic::Consume::METHOD_ID) => {
                Ok(Self::BasicConsume(basic::Consume::decode_args(dec)?))
            }
            (basic::CLASS_ID, basic::ConsumeOk::METHOD_ID) => {
                Ok(Self::BasicConsumeOk(basic::ConsumeOk::decode_args(dec)?))
            }
            (basic::CLASS_ID, basic::Cancel::METHOD_ID) => {
                Ok(Self::BasicCancel(basic::Cancel::decode_args(dec)?))
            }
            (basic::CLASS_ID, basic::CancelOk::METHOD_ID) => {
                Ok(Self::BasicCancelOk(basic::CancelOk::decode_args(dec)?))
            }
            (basic::CLASS_ID, basic::Publish::METHOD_ID) => {
                Ok(Self::BasicPublish(basic::Publish::decode_args(dec)?))
            }
            (basic::CLASS_ID, basic::Return::METHOD_ID) => {
                Ok(Self::BasicReturn(basic::Return::decode_args(dec)?))
            }
            (basic::CLASS_ID, basic::Deliver::METHOD_ID) => {
                Ok(Self::BasicDeliver(basic::Deliver::decode_args(dec)?))
            }
            (basic::CLASS_ID, basic::Get::METHOD_ID) => {
                Ok(Self::BasicGet(basic::Get::decode_args(dec)?))
            }
            (basic::CLASS_ID, basic::GetOk::METHOD_ID) => {
                Ok(Self::BasicGetOk(basic::GetOk::decode_args(dec)?))
            }
            (basic::CLASS_ID, basic::GetEmpty::METHOD_ID) => {
                Ok(Self::BasicGetEmpty(basic::GetEmpty::decode_args(dec)?))
            }
            (basic::CLASS_ID, basic::Ack::METHOD_ID) => {
                Ok(Self::BasicAck(basic::Ack::decode_args(dec)?))
            }
            (basic::CLASS_ID, basic::Reject::METHOD_ID) => {
                Ok(Self::BasicReject(basic::Reject::decode_args(dec)?))
            }
            (basic::CLASS_ID, basic::Recover::METHOD_ID) => {
                Ok(Self::BasicRecover(basic::Recover::decode_args(dec)?))
            }
            (basic::CLASS_ID, basic::RecoverOk::METHOD_ID) => {
                Ok(Self::BasicRecoverOk(basic::RecoverOk::decode_args(dec)?))
            }
            (basic::CLASS_ID, basic::Nack::METHOD_ID) => {
                Ok(Self::BasicNack(basic::Nack::decode_args(dec)?))
            }

            (confirm::CLASS_ID, confirm::Select::METHOD_ID) => {
                Ok(Self::ConfirmSelect(confirm::Select::decode_args(dec)?))
            }
            (confirm::CLASS_ID, confirm::SelectOk::METHOD_ID) => {
                Ok(Self::ConfirmSelectOk(confirm::SelectOk::decode_args(dec)?))
            }

            (tx::CLASS_ID, tx::Select::METHOD_ID) => Ok(Self::TxSelect(tx::Select::decode_args(dec)?)),
            (tx::CLASS_ID, tx::SelectOk::METHOD_ID) => {
                Ok(Self::TxSelectOk(tx::SelectOk::decode_args(dec)?))
            }
            (tx::CLASS_ID, tx::Commit::METHOD_ID) => Ok(Self::TxCommit(tx::Commit::decode_args(dec)?)),
            (tx::CLASS_ID, tx::CommitOk::METHOD_ID) => {
                Ok(Self::TxCommitOk(tx::CommitOk::decode_args(dec)?))
            }
            (tx::CLASS_ID, tx::Rollback::METHOD_ID) => {
                Ok(Self::TxRollback(tx::Rollback::decode_args(dec)?))
            }
            (tx::CLASS_ID, tx::RollbackOk::METHOD_ID) => {
                Ok(Self::TxRollbackOk(tx::RollbackOk::decode_args(dec)?))
            }

            _ => Err(Error::UnknownMethod {
                class_id,
                method_id,
            }),
        }
    }

    /// Encode into a method [`Frame`] on the given channel.
    pub fn to_frame(&self, channel: u16) -> Result<Frame> {
        Ok(Frame::method(channel, self.encode()?))
    }

    /// Decode a method from a method frame.
    ///
    /// Returns [`Error::ExpectedMethodFrame`] when `frame.kind` is not
    /// [`crate::FrameType::Method`].
    pub fn from_frame(frame: &Frame) -> Result<Self> {
        if frame.kind != crate::FrameType::Method {
            return Err(Error::ExpectedMethodFrame(frame.kind.as_u8()));
        }
        Self::decode(&frame.payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{FieldTable, FieldValue};

    fn roundtrip(m: Method) {
        let encoded = m.encode().expect("encode");
        let decoded = Method::decode(&encoded).expect("decode");
        assert_eq!(
            decoded,
            m,
            "roundtrip mismatch for class={} method={}",
            m.class_id(),
            m.method_id()
        );
        // frame wrapper
        let frame = m.to_frame(1).expect("to_frame");
        assert_eq!(frame.channel, 1);
        let again = Method::from_frame(&frame).expect("from_frame");
        assert_eq!(again, m);
    }

    #[test]
    fn class_method_ids() {
        assert_eq!(connection::CLASS_ID, 10);
        assert_eq!(channel::CLASS_ID, 20);
        assert_eq!(exchange::CLASS_ID, 40);
        assert_eq!(queue::CLASS_ID, 50);
        assert_eq!(basic::CLASS_ID, 60);
        assert_eq!(confirm::CLASS_ID, 85);

        assert_eq!(connection::Start::METHOD_ID, 10);
        assert_eq!(connection::StartOk::METHOD_ID, 11);
        assert_eq!(connection::Tune::METHOD_ID, 30);
        assert_eq!(connection::Open::METHOD_ID, 40);
        assert_eq!(connection::Close::METHOD_ID, 50);
        assert_eq!(basic::Publish::METHOD_ID, 40);
        assert_eq!(basic::Deliver::METHOD_ID, 60);
        assert_eq!(basic::Ack::METHOD_ID, 80);
        assert_eq!(basic::Nack::METHOD_ID, 120);
        assert_eq!(confirm::Select::METHOD_ID, 10);
        assert_eq!(confirm::SelectOk::METHOD_ID, 11);
    }

    #[test]
    fn confirm_select_roundtrip() {
        roundtrip(Method::ConfirmSelect(confirm::Select { nowait: false }));
        roundtrip(Method::ConfirmSelect(confirm::Select { nowait: true }));
        roundtrip(Method::ConfirmSelectOk(confirm::SelectOk));
    }

    #[test]
    fn connection_start_roundtrip() {
        let mut props = FieldTable::new();
        props.insert("product", FieldValue::long_str("QueueForge"));
        props.insert("version", FieldValue::long_str("0.1.0"));
        props.insert(
            "capabilities",
            FieldValue::Table(FieldTable::from_pairs([
                ("publisher_confirms", FieldValue::Bool(true)),
                ("consumer_cancel_notify", FieldValue::Bool(true)),
                ("basic.nack", FieldValue::Bool(true)),
            ])),
        );

        roundtrip(Method::ConnectionStart(connection::Start {
            version_major: 0,
            version_minor: 9,
            server_properties: props,
            mechanisms: b"PLAIN AMQPLAIN".to_vec(),
            locales: b"en_US".to_vec(),
        }));
    }

    #[test]
    fn connection_start_ok_plain_auth() {
        let mut props = FieldTable::new();
        props.insert("product", FieldValue::long_str("test-client"));
        // PLAIN: \0username\0password
        let response = b"\0admin\0s3cret".to_vec();
        roundtrip(Method::ConnectionStartOk(connection::StartOk {
            client_properties: props,
            mechanism: "PLAIN".into(),
            response,
            locale: "en_US".into(),
        }));
    }

    #[test]
    fn connection_tune_open_close_roundtrip() {
        roundtrip(Method::ConnectionTune(connection::Tune {
            channel_max: 2047,
            frame_max: 131_072,
            heartbeat: 60,
        }));
        roundtrip(Method::ConnectionTuneOk(connection::TuneOk {
            channel_max: 2047,
            frame_max: 131_072,
            heartbeat: 60,
        }));
        roundtrip(Method::ConnectionOpen(connection::Open::new("/")));
        roundtrip(Method::ConnectionOpenOk(connection::OpenOk::new()));
        roundtrip(Method::ConnectionClose(connection::Close {
            reply_code: 200,
            reply_text: "OK".into(),
            class_id: 0,
            method_id: 0,
        }));
        roundtrip(Method::ConnectionCloseOk(connection::CloseOk));
    }

    #[test]
    fn channel_methods_roundtrip() {
        roundtrip(Method::ChannelOpen(channel::Open::new()));
        roundtrip(Method::ChannelOpenOk(channel::OpenOk::new()));
        roundtrip(Method::ChannelFlow(channel::Flow { active: true }));
        roundtrip(Method::ChannelFlowOk(channel::FlowOk { active: false }));
        roundtrip(Method::ChannelClose(channel::Close {
            reply_code: 404,
            reply_text: "NOT_FOUND".into(),
            class_id: 50,
            method_id: 10,
        }));
        roundtrip(Method::ChannelCloseOk(channel::CloseOk));
    }

    #[test]
    fn queue_declare_with_flags_and_args() {
        let mut args = FieldTable::new();
        args.insert("x-max-priority", FieldValue::I32(10));
        roundtrip(Method::QueueDeclare(queue::Declare {
            reserved_1: 0,
            queue: "orders".into(),
            passive: false,
            durable: true,
            exclusive: false,
            auto_delete: false,
            no_wait: false,
            arguments: args,
        }));
        roundtrip(Method::QueueDeclareOk(queue::DeclareOk {
            queue: "orders".into(),
            message_count: 3,
            consumer_count: 1,
        }));
    }

    #[test]
    fn queue_bind_unbind_purge_delete() {
        roundtrip(Method::QueueBind(queue::Bind {
            reserved_1: 0,
            queue: "q".into(),
            exchange: "ex".into(),
            routing_key: "rk".into(),
            no_wait: false,
            arguments: FieldTable::new(),
        }));
        roundtrip(Method::QueueBindOk(queue::BindOk));
        roundtrip(Method::QueueUnbind(queue::Unbind {
            reserved_1: 0,
            queue: "q".into(),
            exchange: "ex".into(),
            routing_key: "rk".into(),
            arguments: FieldTable::new(),
        }));
        roundtrip(Method::QueueUnbindOk(queue::UnbindOk));
        roundtrip(Method::QueuePurge(queue::Purge {
            reserved_1: 0,
            queue: "q".into(),
            no_wait: false,
        }));
        roundtrip(Method::QueuePurgeOk(queue::PurgeOk { message_count: 7 }));
        roundtrip(Method::QueueDelete(queue::Delete {
            reserved_1: 0,
            queue: "q".into(),
            if_unused: true,
            if_empty: true,
            no_wait: false,
        }));
        roundtrip(Method::QueueDeleteOk(queue::DeleteOk { message_count: 0 }));
    }

    #[test]
    fn exchange_declare_and_bind() {
        roundtrip(Method::ExchangeDeclare(exchange::Declare {
            reserved_1: 0,
            exchange: "logs".into(),
            kind: "topic".into(),
            passive: false,
            durable: true,
            auto_delete: false,
            internal: false,
            no_wait: false,
            arguments: FieldTable::new(),
        }));
        roundtrip(Method::ExchangeDeclareOk(exchange::DeclareOk));
        roundtrip(Method::ExchangeBind(exchange::Bind {
            reserved_1: 0,
            destination: "dst".into(),
            source: "src".into(),
            routing_key: "#".into(),
            no_wait: false,
            arguments: FieldTable::new(),
        }));
        roundtrip(Method::ExchangeDelete(exchange::Delete {
            reserved_1: 0,
            exchange: "logs".into(),
            if_unused: true,
            no_wait: false,
        }));
        roundtrip(Method::ExchangeDeleteOk(exchange::DeleteOk));
    }

    #[test]
    fn basic_publish_consume_deliver_ack() {
        roundtrip(Method::BasicQos(basic::Qos {
            prefetch_size: 0,
            prefetch_count: 50,
            global: false,
        }));
        roundtrip(Method::BasicQosOk(basic::QosOk));

        roundtrip(Method::BasicConsume(basic::Consume {
            reserved_1: 0,
            queue: "orders".into(),
            consumer_tag: String::new(),
            no_local: false,
            no_ack: false,
            exclusive: false,
            no_wait: false,
            arguments: FieldTable::new(),
        }));
        roundtrip(Method::BasicConsumeOk(basic::ConsumeOk {
            consumer_tag: "ctag-1".into(),
        }));

        roundtrip(Method::BasicPublish(basic::Publish {
            reserved_1: 0,
            exchange: String::new(),
            routing_key: "orders".into(),
            mandatory: true,
            immediate: false,
        }));

        roundtrip(Method::BasicDeliver(basic::Deliver {
            consumer_tag: "ctag-1".into(),
            delivery_tag: 1,
            redelivered: false,
            exchange: String::new(),
            routing_key: "orders".into(),
        }));

        roundtrip(Method::BasicAck(basic::Ack {
            delivery_tag: 1,
            multiple: false,
        }));
        roundtrip(Method::BasicNack(basic::Nack {
            delivery_tag: 2,
            multiple: true,
            requeue: true,
        }));
        roundtrip(Method::BasicReject(basic::Reject {
            delivery_tag: 3,
            requeue: false,
        }));
    }

    #[test]
    fn basic_get_return_cancel_recover() {
        roundtrip(Method::BasicGet(basic::Get {
            reserved_1: 0,
            queue: "q".into(),
            no_ack: false,
        }));
        roundtrip(Method::BasicGetOk(basic::GetOk {
            delivery_tag: 9,
            redelivered: true,
            exchange: "ex".into(),
            routing_key: "rk".into(),
            message_count: 4,
        }));
        roundtrip(Method::BasicGetEmpty(basic::GetEmpty::new()));
        roundtrip(Method::BasicReturn(basic::Return {
            reply_code: 312,
            reply_text: "NO_ROUTE".into(),
            exchange: String::new(),
            routing_key: "missing".into(),
        }));
        roundtrip(Method::BasicCancel(basic::Cancel {
            consumer_tag: "ctag-1".into(),
            no_wait: false,
        }));
        roundtrip(Method::BasicCancelOk(basic::CancelOk {
            consumer_tag: "ctag-1".into(),
        }));
        roundtrip(Method::BasicRecover(basic::Recover { requeue: true }));
        roundtrip(Method::BasicRecoverOk(basic::RecoverOk));
    }

    #[test]
    fn unknown_method_errors() {
        // class 10 method 99
        let payload = {
            let mut enc = Encoder::new();
            enc.write_short(10);
            enc.write_short(99);
            enc.finish()
        };
        assert!(matches!(
            Method::decode(&payload),
            Err(Error::UnknownMethod {
                class_id: 10,
                method_id: 99
            })
        ));
    }

    #[test]
    fn truncated_method_errors() {
        // class+method only, missing tune fields
        let payload = {
            let mut enc = Encoder::new();
            enc.write_short(10);
            enc.write_short(30);
            enc.finish()
        };
        assert!(matches!(
            Method::decode(&payload),
            Err(Error::TruncatedMethod { .. })
        ));
    }

    #[test]
    fn connection_start_wire_ids_prefix() {
        let m = Method::ConnectionStart(connection::Start {
            version_major: 0,
            version_minor: 9,
            server_properties: FieldTable::new(),
            mechanisms: b"PLAIN".to_vec(),
            locales: b"en_US".to_vec(),
        });
        let bytes = m.encode().unwrap();
        // class 10, method 10
        assert_eq!(&bytes[0..4], &[0x00, 0x0A, 0x00, 0x0A]);
        assert_eq!(bytes[4], 0); // version-major
        assert_eq!(bytes[5], 9); // version-minor
    }

    #[test]
    fn queue_declare_flags_bit_packing() {
        // durable=true, exclusive=true → bits 1 and 2 set → 0b0000_0110
        let m = Method::QueueDeclare(queue::Declare {
            reserved_1: 0,
            queue: "q".into(),
            passive: false,
            durable: true,
            exclusive: true,
            auto_delete: false,
            no_wait: false,
            arguments: FieldTable::new(),
        });
        let bytes = m.encode().unwrap();
        // class(2)+method(2)+reserved(2)+shortstr "q"(2) = 8, then flags octet
        // 00 32 00 0A | 00 00 | 01 71 | flags | table len
        assert_eq!(&bytes[0..4], &[0x00, 0x32, 0x00, 0x0A]); // class 50, method 10
        assert_eq!(&bytes[4..6], &[0x00, 0x00]); // reserved
        assert_eq!(&bytes[6..8], &[0x01, b'q']);
        assert_eq!(bytes[8], 0b0000_0110);
    }

    #[test]
    fn handshake_sequence_frame_encode() {
        // Smoke: encode a typical server connection.start as a channel-0 frame.
        let start = Method::ConnectionStart(connection::Start {
            version_major: 0,
            version_minor: 9,
            server_properties: FieldTable::new(),
            mechanisms: b"PLAIN".to_vec(),
            locales: b"en_US".to_vec(),
        });
        let frame = start.to_frame(0).unwrap();
        let wire = frame.encode().unwrap();
        let (decoded_frame, n) = Frame::decode(&wire).unwrap();
        assert_eq!(n, wire.len());
        let decoded = Method::from_frame(&decoded_frame).unwrap();
        assert_eq!(decoded, start);
    }

    #[test]
    fn from_frame_rejects_non_method() {
        let hb = Frame::heartbeat();
        assert!(matches!(
            Method::from_frame(&hb),
            Err(Error::ExpectedMethodFrame(8))
        ));
        let body = Frame::body(1, vec![1, 2, 3]);
        assert!(matches!(
            Method::from_frame(&body),
            Err(Error::ExpectedMethodFrame(3))
        ));
    }

    /// Golden method payloads (exact byte strings) for third-party interop confidence.
    #[test]
    fn golden_connection_start_minimal() {
        let m = Method::ConnectionStart(connection::Start {
            version_major: 0,
            version_minor: 9,
            server_properties: FieldTable::new(),
            mechanisms: b"PLAIN".to_vec(),
            locales: b"en_US".to_vec(),
        });
        // class=10 method=10 | ver 0,9 | empty table | longstr PLAIN | longstr en_US
        let expected: &[u8] = &[
            0x00, 0x0A, 0x00, 0x0A, //
            0x00, 0x09, //
            0x00, 0x00, 0x00, 0x00, // empty server-properties
            0x00, 0x00, 0x00, 0x05, b'P', b'L', b'A', b'I', b'N', //
            0x00, 0x00, 0x00, 0x05, b'e', b'n', b'_', b'U', b'S',
        ];
        assert_eq!(m.encode().unwrap(), expected);
        assert_eq!(Method::decode(expected).unwrap(), m);
    }

    #[test]
    fn golden_queue_declare_durable() {
        let m = Method::QueueDeclare(queue::Declare {
            reserved_1: 0,
            queue: "q".into(),
            passive: false,
            durable: true,
            exclusive: false,
            auto_delete: false,
            no_wait: false,
            arguments: FieldTable::new(),
        });
        // class=50 method=10 | ticket 0 | "q" | flags durable only (0x02) | empty table
        let expected: &[u8] = &[
            0x00, 0x32, 0x00, 0x0A, //
            0x00, 0x00, //
            0x01, b'q', //
            0x02, // bit1 = durable
            0x00, 0x00, 0x00, 0x00,
        ];
        assert_eq!(m.encode().unwrap(), expected);
        assert_eq!(Method::decode(expected).unwrap(), m);
    }

    #[test]
    fn golden_connection_start_ok_plain() {
        let m = Method::ConnectionStartOk(connection::StartOk {
            client_properties: FieldTable::new(),
            mechanism: "PLAIN".into(),
            response: b"\0admin\0s3cret".to_vec(),
            locale: "en_US".into(),
        });
        // class=10 method=11 | empty table | shortstr PLAIN | longstr \0admin\0s3cret | shortstr en_US
        // SASL response is \0 admin \0 s3cret = 1+5+1+6 = 13 bytes
        let expected: &[u8] = &[
            0x00, 0x0A, 0x00, 0x0B, //
            0x00, 0x00, 0x00, 0x00, // empty client-properties
            0x05, b'P', b'L', b'A', b'I', b'N', //
            0x00, 0x00, 0x00, 0x0D, // 13-byte SASL response
            0x00, b'a', b'd', b'm', b'i', b'n', 0x00, b's', b'3', b'c', b'r', b'e', b't', //
            0x05, b'e', b'n', b'_', b'U', b'S',
        ];
        assert_eq!(m.encode().unwrap(), expected);
        assert_eq!(Method::decode(expected).unwrap(), m);
    }
}
