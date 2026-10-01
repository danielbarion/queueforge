//! Class and method ids for each `Method` variant.

use super::{basic, channel, confirm, connection, exchange, queue, tx, Method};

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
}
