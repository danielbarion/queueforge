//! confirm.select and the tx class.
//!
//! These methods belong to [`Connection`]. The frame loop in the parent module calls them.

use super::*;

use queueforge_amqp::confirm as confirm_method;
use queueforge_amqp::tx as tx_method;
use queueforge_amqp::Method;
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::{debug, warn};

impl<'a, S> Connection<'a, S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// `tx_select` on the open connection.
    pub(in crate::connection) async fn tx_select(
        &mut self,
        channel: u16,
    ) -> Result<Step, ConnError> {
        if let Some(ch) = self.channels.get_mut(&channel) {
            ch.tx_mode = true;
        }
        self.send_method(channel, &Method::TxSelectOk(tx_method::SelectOk))
            .await?;
        Ok(Step::Continue)
    }

    /// `tx_commit` on the open connection.
    pub(in crate::connection) async fn tx_commit(
        &mut self,
        channel: u16,
    ) -> Result<Step, ConnError> {
        let in_tx = self.channels.get(&channel).is_some_and(|c| c.tx_mode);
        if !in_tx {
            self.server_channel_close(
                channel,
                REPLY_PRECONDITION_FAILED,
                "PRECONDITION_FAILED - channel not in tx mode",
                tx_method::CLASS_ID,
                tx_method::Commit::METHOD_ID,
            )
            .await?;
            return Ok(Step::Continue);
        }
        let ops = self
            .channels
            .get_mut(&channel)
            .map(|c| std::mem::take(&mut c.tx_ops))
            .unwrap_or_default();
        self.tx_applying = true;
        for op in ops {
            let step = match op {
                TxOp::Publish {
                    publish,
                    properties,
                    body,
                } => {
                    self.finish_publish(channel, publish, *properties, body)
                        .await?
                }
                TxOp::Ack(ack) => self.handle_basic_ack(channel, ack).await?,
                TxOp::Reject(reject) => {
                    self.handle_basic_nack(channel, reject.delivery_tag, false, reject.requeue)
                        .await?
                }
            };
            if step == Step::Done {
                self.tx_applying = false;
                self.tx_durable.clear();
                self.tx_flush.clear();
                return Ok(Step::Done);
            }
        }
        self.tx_applying = false;
        // The publishes were applied in order. Their fsyncs are awaited
        // together: commit-ok still follows the covering sync of every one.
        // One fsync per queue the commit wrote to, now. The group-commit
        // interval would add up to 10 ms to every commit.
        for (_, tx, offset) in std::mem::take(&mut self.tx_flush) {
            let (reply, _done) = oneshot::channel();
            let _ = tx.send(QueueCmd::FlushDurable { offset, reply }).await;
        }
        let mut failed = false;
        for wait in std::mem::take(&mut self.tx_durable) {
            match wait.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    warn!(error = %e, "durable_done error on tx.commit publish");
                    failed = true;
                }
                Err(_) => {
                    warn!("durable_done oneshot canceled during tx.commit (queue actor gone?)");
                    failed = true;
                }
            }
        }
        if failed {
            metrics::counter!("queueforge_publish_partial_failure_total").increment(1);
            return self.publish_partial_failure(channel, None).await;
        }
        self.send_method(channel, &Method::TxCommitOk(tx_method::CommitOk))
            .await?;
        Ok(Step::Continue)
    }

    /// `tx_rollback` on the open connection.
    pub(in crate::connection) async fn tx_rollback(
        &mut self,
        channel: u16,
    ) -> Result<Step, ConnError> {
        let in_tx = self.channels.get(&channel).is_some_and(|c| c.tx_mode);
        if !in_tx {
            self.server_channel_close(
                channel,
                REPLY_PRECONDITION_FAILED,
                "PRECONDITION_FAILED - channel not in tx mode",
                tx_method::CLASS_ID,
                tx_method::Rollback::METHOD_ID,
            )
            .await?;
            return Ok(Step::Continue);
        }
        if let Some(ch) = self.channels.get_mut(&channel) {
            ch.tx_ops.clear();
        }
        self.send_method(channel, &Method::TxRollbackOk(tx_method::RollbackOk))
            .await?;
        Ok(Step::Continue)
    }

    /// `handle_confirm_select` on the open connection.
    pub(in crate::connection) async fn handle_confirm_select(
        &mut self,
        channel: u16,
        select: confirm_method::Select,
    ) -> Result<Step, ConnError> {
        let Some(ch) = self.channels.get_mut(&channel) else {
            return Ok(Step::Continue);
        };
        // Idempotent: re-select keeps sequence numbering continuous.
        ch.confirm_mode = true;
        if !select.nowait {
            self.send_method(channel, &Method::ConfirmSelectOk(confirm_method::SelectOk))
                .await?;
        }
        debug!(peer = %self.peer, channel, "confirm.select enabled");
        Ok(Step::Continue)
    }
}
