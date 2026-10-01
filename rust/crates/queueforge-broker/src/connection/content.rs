//! Content header and body frames, and the per-channel transaction flag.

use super::*;

use bytes::Bytes;
use queueforge_amqp::{ContentHeader, Frame, FrameType};
use tokio::io::{AsyncRead, AsyncWrite};

impl<'a, S> Connection<'a, S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// `handle_content_frame` on the open connection.
    pub(in crate::connection) async fn handle_content_frame(
        &mut self,
        frame: Frame,
    ) -> Result<Step, ConnError> {
        let ch = frame.channel;
        if ch == 0 || !self.channels.contains_key(&ch) {
            if ch != 0 {
                self.server_channel_close(ch, REPLY_COMMAND_INVALID, "channel not open", 0, 0)
                    .await?;
            }
            return Ok(Step::Continue);
        }

        match frame.kind {
            FrameType::Header => {
                let header = match ContentHeader::decode(&frame.payload) {
                    Ok(h) => h,
                    Err(e) => {
                        self.server_channel_close(
                            ch,
                            REPLY_COMMAND_INVALID,
                            &format!("invalid content header: {e}"),
                            60,
                            0,
                        )
                        .await?;
                        return Ok(Step::Continue);
                    }
                };
                let max_msg = self.params.max_message_bytes;
                let header_bytes = properties_header_bytes(&header.properties);
                if header.body_size.saturating_add(header_bytes) > max_msg {
                    self.server_channel_close(
                        ch,
                        REPLY_PRECONDITION_FAILED,
                        &format!(
                            "PRECONDITION_FAILED - message size {} exceeds max {}",
                            header.body_size.saturating_add(header_bytes),
                            max_msg
                        ),
                        60,
                        0,
                    )
                    .await?;
                    // Drop in-flight publish assembly if any.
                    if let Some(ch_state) = self.channels.get_mut(&ch) {
                        ch_state.publish = None;
                    }
                    return Ok(Step::Continue);
                }

                let Some(ch_state) = self.channels.get_mut(&ch) else {
                    return Ok(Step::Continue);
                };
                match ch_state.publish.take() {
                    Some(PublishAssemble::ExpectHeader { publish }) => {
                        if header.body_size == 0 {
                            // Complete publish with empty body.
                            return self
                                .finish_publish(ch, publish, header.properties, Bytes::new())
                                .await;
                        }
                        ch_state.publish = Some(PublishAssemble::ExpectBody {
                            publish,
                            properties: Box::new(header.properties),
                            body_size: header.body_size,
                            body: Vec::with_capacity(header.body_size.min(1024 * 1024) as usize),
                        });
                        Ok(Step::Continue)
                    }
                    other => {
                        ch_state.publish = other;
                        self.server_channel_close(
                            ch,
                            REPLY_COMMAND_INVALID,
                            "unexpected content header",
                            60,
                            0,
                        )
                        .await?;
                        Ok(Step::Continue)
                    }
                }
            }
            FrameType::Body => {
                let (done_publish, props, body) = {
                    let Some(ch_state) = self.channels.get_mut(&ch) else {
                        return Ok(Step::Continue);
                    };
                    match ch_state.publish.take() {
                        Some(PublishAssemble::ExpectBody {
                            publish,
                            properties,
                            body_size,
                            mut body,
                        }) => {
                            body.extend_from_slice(&frame.payload);
                            if (body.len() as u64) < body_size {
                                ch_state.publish = Some(PublishAssemble::ExpectBody {
                                    publish,
                                    properties,
                                    body_size,
                                    body,
                                });
                                return Ok(Step::Continue);
                            }
                            if (body.len() as u64) > body_size {
                                self.server_channel_close(
                                    ch,
                                    REPLY_COMMAND_INVALID,
                                    "body larger than content header size",
                                    60,
                                    0,
                                )
                                .await?;
                                return Ok(Step::Continue);
                            }
                            (publish, *properties, Bytes::from(body))
                        }
                        other => {
                            ch_state.publish = other;
                            self.server_channel_close(
                                ch,
                                REPLY_COMMAND_INVALID,
                                "unexpected content body",
                                60,
                                0,
                            )
                            .await?;
                            return Ok(Step::Continue);
                        }
                    }
                };
                self.finish_publish(ch, done_publish, props, body).await
            }
            _ => Ok(Step::Continue),
        }
    }
    /// `in_tx` on the open connection.
    pub(in crate::connection) fn in_tx(&self, channel: u16) -> bool {
        !self.tx_applying && self.channels.get(&channel).is_some_and(|ch| ch.tx_mode)
    }
}
