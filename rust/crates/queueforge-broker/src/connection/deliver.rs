//! Delivery forwarding, content writes, and consumer teardown.

use super::*;

use bytes::Bytes;
use queueforge_amqp::{basic as basic_method, BasicProperties, ContentHeader, Frame, Method};
use queueforge_core::{ConsumerSessionId, QueueCmd, QueueDelivery, QueueType};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::oneshot;
use tracing::debug;

impl<'a, S> Connection<'a, S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// `forward_delivery` on the open connection.
    pub(in crate::connection) async fn forward_delivery(
        &mut self,
        delivery: QueueDelivery,
    ) -> Result<(), ConnError> {
        if delivery.server_cancel {
            if let Some(info) = self.sessions.remove(&delivery.session) {
                if let Some(ch) = self.channels.get_mut(&info.channel) {
                    ch.consumers.remove(&info.consumer_tag);
                }
                self.send_method(
                    info.channel,
                    &Method::BasicCancel(basic_method::Cancel {
                        consumer_tag: info.consumer_tag,
                        no_wait: true,
                    }),
                )
                .await?;
            }
            return Ok(());
        }
        let Some(info) = self.sessions.get(&delivery.session).cloned() else {
            // Orphan delivery — nack/requeue.
            // We don't know the queue handle easily; drop.
            debug!(session = delivery.session.0, "orphan delivery dropped");
            return Ok(());
        };

        if !self.channels.contains_key(&info.channel) {
            // Channel gone: requeue via nack.
            let _ = info
                .handle
                .tx
                .send(QueueCmd::Nack {
                    id: delivery.delivery_id,
                    requeue: true,
                })
                .await;
            return Ok(());
        }

        // Channel prefetch enforcement: if over limit, requeue (shouldn't happen with credit).
        let over_prefetch = {
            let ch = self.channels.get(&info.channel).unwrap();
            let per_full = ch.prefetch_count > 0
                && self.consumer_outstanding(info.channel, Some(delivery.session))
                    >= u32::from(ch.prefetch_count);
            let global_full =
                ch.global_prefetch > 0 && ch.outstanding() >= u32::from(ch.global_prefetch);
            (per_full || global_full) && !info.no_ack
        };
        if over_prefetch {
            // The actor already spent one credit to send this delivery. Do not
            // give it back: AddCredit here makes the actor redeliver immediately
            // and the connection nack again.
            self.spend_grant(delivery.session);
            let _ = info
                .handle
                .tx
                .send(QueueCmd::Nack {
                    id: delivery.delivery_id,
                    requeue: true,
                })
                .await;
            return Ok(());
        }
        self.spend_grant(delivery.session);

        let local_holder = self
            .queues
            .get(&info.queue_key)
            .map(|local| local.tx.same_channel(&info.handle.tx))
            .unwrap_or(false);
        let quorum = info
            .handle
            .info
            .args
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .queue_type
            == Some(QueueType::Quorum);
        if quorum {
            if let Some(message_id) = delivery.message.message.message_id.clone() {
                if let Some(cluster) = &self.cluster {
                    cluster
                        .claim_for_handoff(&info.queue_key, message_id.as_str(), !local_holder)
                        .await;
                }
            }
        }

        let delivery_tag = {
            let ch = self.channels.get_mut(&info.channel).unwrap();
            let tag = ch.next_delivery_tag;
            ch.next_delivery_tag = ch.next_delivery_tag.saturating_add(1);
            if !info.no_ack {
                ch.delivery_ledger.insert(
                    tag,
                    OutstandingDelivery {
                        queue_key: info.queue_key.clone(),
                        consumer_delivery_id: delivery.delivery_id,
                        consumer_tag: Some(info.consumer_tag.clone()),
                        session: Some(delivery.session),
                    },
                );
            }
            tag
        };

        let props = message_to_properties(&delivery.message.message);
        let body = delivery.message.message.body.clone();
        let redelivered = delivery.message.message.redelivered;

        self.send_method(
            info.channel,
            &Method::BasicDeliver(basic_method::Deliver {
                consumer_tag: info.consumer_tag.clone(),
                delivery_tag,
                redelivered,
                exchange: delivery.message.message.exchange.to_string(),
                routing_key: delivery.message.message.routing_key.to_string(),
            }),
        )
        .await?;
        self.send_content(info.channel, &props, &body).await?;
        if delivery.settles_on_write {
            let _ = info
                .handle
                .tx
                .send(QueueCmd::SettleDelivered {
                    id: delivery.delivery_id,
                })
                .await;
            let _ = info
                .handle
                .tx
                .send(QueueCmd::AddCredit {
                    session: delivery.session,
                    credit: 1,
                })
                .await;
        }
        Ok(())
    }
    /// `send_content` on the open connection.
    pub(in crate::connection) async fn send_content(
        &mut self,
        channel: u16,
        props: &BasicProperties,
        body: &Bytes,
    ) -> Result<(), ConnError> {
        let header = ContentHeader::basic(body.len() as u64, props.clone());
        let header_payload = header.encode().map_err(ConnError::Amqp)?;
        self.send_frame(&Frame::header(channel, header_payload))
            .await?;

        if body.is_empty() {
            return Ok(());
        }

        // Split body to respect frame_max.
        let max_payload = self.max_payload();
        let mut offset = 0;
        while offset < body.len() {
            let end = (offset + max_payload).min(body.len());
            self.send_frame(&Frame::body(channel, body[offset..end].to_vec()))
                .await?;
            offset = end;
        }
        Ok(())
    }
    /// `cancel_session` on the open connection.
    pub(in crate::connection) async fn cancel_session(
        &mut self,
        session: ConsumerSessionId,
        requeue: bool,
    ) {
        let Some(info) = self.sessions.get(&session).cloned() else {
            return;
        };
        let quorum = info
            .handle
            .info
            .args
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .queue_type
            == Some(QueueType::Quorum);
        if quorum {
            // Detach the leader consumer and leave the unacked entry for the client's nack.
            let (reply_tx, reply_rx) = oneshot::channel();
            let _ = info
                .handle
                .tx
                .send(QueueCmd::UnregisterConsumer {
                    session,
                    requeue: false,
                    reply: reply_tx,
                })
                .await;
            let _ = reply_rx.await;
            self.sessions.remove(&session);
            self.granted_credit.remove(&session);
            if let Some(ch) = self.channels.get_mut(&info.channel) {
                ch.consumers.remove(&info.consumer_tag);
            }
            return;
        }
        self.sessions.remove(&session);
        self.granted_credit.remove(&session);

        // Drop ledger entries for this session and nack on queue.
        if let Some(ch) = self.channels.get_mut(&info.channel) {
            let tags: Vec<u64> = ch
                .delivery_ledger
                .iter()
                .filter(|(_, e)| e.session == Some(session))
                .map(|(t, _)| *t)
                .collect();
            for tag in tags {
                if let Some(entry) = ch.delivery_ledger.remove(&tag) {
                    if requeue {
                        let _ = info
                            .handle
                            .tx
                            .send(QueueCmd::Nack {
                                id: entry.consumer_delivery_id,
                                requeue: true,
                            })
                            .await;
                    } else {
                        let _ = info
                            .handle
                            .tx
                            .send(QueueCmd::Ack {
                                id: entry.consumer_delivery_id,
                                multiple_to: None,
                            })
                            .await;
                    }
                }
            }
            ch.consumers.remove(&info.consumer_tag);
        }

        let (reply_tx, reply_rx) = oneshot::channel();
        let _ = info
            .handle
            .tx
            .send(QueueCmd::UnregisterConsumer {
                session,
                requeue,
                reply: reply_tx,
            })
            .await;
        let _ = reply_rx.await;

        // Auto-delete: if no consumers remain, delete the queue + bindings.
        if info.handle.info.auto_delete {
            let key = info.queue_key;
            let _ = self
                .delete_queue_and_bindings(&key, /*if_unused=*/ true, /*if_empty=*/ false)
                .await;
        }
    }
    /// `teardown_channel` on the open connection.
    pub(in crate::connection) async fn teardown_channel(&mut self, channel: u16, requeue: bool) {
        let Some(mut ch_state) = self.channels.remove(&channel) else {
            return;
        };
        metrics::gauge!("queueforge_channels").decrement(1.0);
        queueforge_core::prom::channel_closed();

        // Cancel all consumers on this channel.
        let sessions: Vec<ConsumerSessionId> = ch_state.consumers.values().copied().collect();
        for session in sessions {
            // cancel_session also tries to clean channel consumers; channel already removed.
            if let Some(info) = self.sessions.remove(&session) {
                self.granted_credit.remove(&session);
                // Requeue ledger entries still in ch_state.
                let tags: Vec<u64> = ch_state
                    .delivery_ledger
                    .iter()
                    .filter(|(_, e)| e.session == Some(session))
                    .map(|(t, _)| *t)
                    .collect();
                for tag in tags {
                    if let Some(entry) = ch_state.delivery_ledger.remove(&tag) {
                        let _ = info
                            .handle
                            .tx
                            .send(QueueCmd::Nack {
                                id: entry.consumer_delivery_id,
                                requeue,
                            })
                            .await;
                    }
                }
                let (reply_tx, reply_rx) = oneshot::channel();
                let _ = info
                    .handle
                    .tx
                    .send(QueueCmd::UnregisterConsumer {
                        session,
                        requeue,
                        reply: reply_tx,
                    })
                    .await;
                let _ = reply_rx.await;
                if info.handle.info.auto_delete {
                    let _ = self
                        .delete_queue_and_bindings(&info.queue_key, true, false)
                        .await;
                }
            }
        }

        self.sync_tracked_channels();

        // Remaining ledger entries (e.g. basic.get).
        for (_, entry) in std::mem::take(&mut ch_state.delivery_ledger) {
            if let Some(handle) = self.resolve_queue(&entry.queue_key).await {
                let _ = handle
                    .tx
                    .send(QueueCmd::Nack {
                        id: entry.consumer_delivery_id,
                        requeue,
                    })
                    .await;
            }
        }
    }
}
