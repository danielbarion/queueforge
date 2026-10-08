//! AMQP 0-9-1 method catalog (connection / channel / exchange / queue / basic).
//! Class ids, encoding, and decoding live in sibling modules.
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
    /// connection.blocked (RabbitMQ extension)
    ConnectionBlocked(connection::Blocked),
    /// connection.unblocked (RabbitMQ extension)
    ConnectionUnblocked(connection::Unblocked),

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

mod decode;
mod encode;
mod id;

#[cfg(test)]
mod tests;
