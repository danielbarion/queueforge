//! Decode a method payload or a method frame.

use super::{basic, channel, confirm, connection, exchange, queue, tx, Method};
use crate::error::{Error, Result};
use crate::frame::Frame;
use crate::types::Decoder;

impl Method {
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

            (tx::CLASS_ID, tx::Select::METHOD_ID) => {
                Ok(Self::TxSelect(tx::Select::decode_args(dec)?))
            }
            (tx::CLASS_ID, tx::SelectOk::METHOD_ID) => {
                Ok(Self::TxSelectOk(tx::SelectOk::decode_args(dec)?))
            }
            (tx::CLASS_ID, tx::Commit::METHOD_ID) => {
                Ok(Self::TxCommit(tx::Commit::decode_args(dec)?))
            }
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
