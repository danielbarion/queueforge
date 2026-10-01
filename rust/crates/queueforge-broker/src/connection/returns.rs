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
    pub(in crate::connection) async fn send_publisher_confirm(
        &mut self,
        channel: u16,
        delivery_tag: u64,
        ok: bool,
    ) -> Result<(), ConnError> {
        if ok {
            queueforge_core::prom::message_confirmed();
            self.send_method(
                channel,
                &Method::BasicAck(basic_method::Ack {
                    delivery_tag,
                    multiple: false,
                }),
            )
            .await
        } else {
            self.send_method(
                channel,
                &Method::BasicNack(basic_method::Nack {
                    delivery_tag,
                    multiple: false,
                    requeue: false,
                }),
            )
            .await
        }
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
