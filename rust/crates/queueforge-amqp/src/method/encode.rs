//! Encode a method into argument bytes and a method frame.

use super::Method;
use crate::error::Result;
use crate::frame::Frame;
use crate::types::Encoder;

impl Method {
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
            Self::ConnectionBlocked(m) => m.encode_args(enc),
            Self::ConnectionUnblocked(m) => m.encode_args(enc),

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

    /// Encode into a method [`Frame`] on the given channel.
    pub fn to_frame(&self, channel: u16) -> Result<Frame> {
        Ok(Frame::method(channel, self.encode()?))
    }
}
