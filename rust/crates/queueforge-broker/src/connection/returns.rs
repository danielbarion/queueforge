//! Publisher confirms, partial publish failure, and basic.return.

use super::*;

use bytes::Bytes;
use queueforge_amqp::{basic as basic_method, BasicProperties, Method};
use tokio::io::{AsyncRead, AsyncWrite};

impl<'a, S> Connection<'a, S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// Partial multi-destination publish failure.
    ///
    /// Confirms on → `basic.nack` for the publish sequence (channel stays open).
    /// Confirms off → channel exception 541.
    pub(in crate::connection) async fn publish_partial_failure(
        &mut self,
        channel: u16,
        confirm_seq: Option<u64>,
    ) -> Result<Step, ConnError> {
        if let Some(seq) = confirm_seq {
            self.send_publisher_confirm(channel, seq, false).await?;
            return Ok(Step::Continue);
        }
        self.server_channel_close(
            channel,
            REPLY_INTERNAL_ERROR,
            "INTERNAL_ERROR - partial multi-destination publish failure",
            basic_method::CLASS_ID,
            basic_method::Publish::METHOD_ID,
        )
        .await?;
        Ok(Step::Continue)
    }
    /// Map `EnqueueCompletion` outcome onto publisher `basic.ack` / `basic.nack`.
    ///
    /// A contiguous run of acks is one `basic.ack` with `multiple=true`. A tag
    /// is held until every lower tag on the channel has been sent, so a later
    /// flush cannot cover a publish that is still waiting on its fsync.
    pub(in crate::connection) async fn send_publisher_confirm(
        &mut self,
        channel: u16,
        delivery_tag: u64,
        ok: bool,
    ) -> Result<(), ConnError> {
        self.stage_confirm(channel, delivery_tag, ok);
        self.emit_ready_confirms().await
    }

    pub(in crate::connection) fn stage_confirm(
        &mut self,
        channel: u16,
        delivery_tag: u64,
        ok: bool,
    ) {
        let Some(ch) = self.channels.get_mut(&channel) else {
            return;
        };
        ch.held_confirms.insert(delivery_tag, ok);
    }

    pub(in crate::connection) async fn emit_ready_confirms(&mut self) -> Result<(), ConnError> {
        let mut pending = Vec::new();
        for (&channel, ch) in self.channels.iter_mut() {
            loop {
                let Some(&ok) = ch.held_confirms.get(&ch.next_confirm_emit) else {
                    break;
                };
                if !ok {
                    ch.held_confirms.remove(&ch.next_confirm_emit);
                    let tag = ch.next_confirm_emit;
                    ch.next_confirm_emit = ch.next_confirm_emit.saturating_add(1);
                    pending.push((channel, tag, false, 1u64));
                    continue;
                }
                let start = ch.next_confirm_emit;
                let mut last = start;
                loop {
                    let next = last.saturating_add(1);
                    if ch.held_confirms.get(&next) == Some(&true) {
                        last = next;
                    } else {
                        break;
                    }
                }
                for tag in start..=last {
                    ch.held_confirms.remove(&tag);
                }
                ch.next_confirm_emit = last.saturating_add(1);
                pending.push((channel, last, true, last - start + 1));
            }
        }
        for (channel, tag, ok, count) in pending {
            if ok {
                for _ in 0..count {
                    queueforge_core::prom::message_confirmed();
                }
                self.send_method(
                    channel,
                    &Method::BasicAck(basic_method::Ack {
                        delivery_tag: tag,
                        multiple: count > 1,
                    }),
                )
                .await?;
            } else {
                self.send_method(
                    channel,
                    &Method::BasicNack(basic_method::Nack {
                        delivery_tag: tag,
                        multiple: false,
                        requeue: false,
                    }),
                )
                .await?;
            }
        }
        Ok(())
    }
    /// `handle_basic_publish` on the open connection.
    pub(in crate::connection) async fn handle_basic_publish(
        &mut self,
        channel: u16,
        publish: basic_method::Publish,
    ) -> Result<Step, ConnError> {
        let Some(ch_state) = self.channels.get_mut(&channel) else {
            return Ok(Step::Continue);
        };
        if ch_state.publish.is_some() {
            self.server_channel_close(
                channel,
                REPLY_COMMAND_INVALID,
                "publish already in progress on channel",
                basic_method::CLASS_ID,
                basic_method::Publish::METHOD_ID,
            )
            .await?;
            return Ok(Step::Continue);
        }
        let user = self.user.clone().unwrap_or_default();
        let vhost = self.vhost.clone().unwrap_or_default();
        if !self.connections.topic_write_allowed(
            &user,
            &vhost,
            &publish.exchange,
            &publish.routing_key,
        ) {
            self.server_channel_close(
                channel,
                REPLY_ACCESS_REFUSED,
                "ACCESS_REFUSED - topic permission write pattern",
                basic_method::CLASS_ID,
                basic_method::Publish::METHOD_ID,
            )
            .await?;
            return Ok(Step::Continue);
        }
        ch_state.publish = Some(PublishAssemble::ExpectHeader { publish });
        Ok(Step::Continue)
    }
    /// `send_basic_return` on the open connection.
    pub(in crate::connection) async fn send_basic_return(
        &mut self,
        channel: u16,
        reply_code: u16,
        reply_text: &str,
        exchange: &str,
        routing_key: &str,
        properties: &BasicProperties,
        body: &Bytes,
    ) -> Result<Step, ConnError> {
        self.send_method(
            channel,
            &Method::BasicReturn(basic_method::Return {
                reply_code,
                reply_text: reply_text.to_string(),
                exchange: exchange.to_string(),
                routing_key: routing_key.to_string(),
            }),
        )
        .await?;
        self.send_content(channel, properties, body).await?;
        metrics::counter!(
            "queueforge_publish_unroutable_total",
            "vhost" => self.vhost.clone().unwrap_or_else(|| "/".into()),
            "exchange" => exchange.to_string()
        )
        .increment(1);
        Ok(Step::Continue)
    }
}
