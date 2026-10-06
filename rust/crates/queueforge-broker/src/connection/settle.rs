//! basic.ack, basic.nack, basic.recover, and tag settlement.

use super::*;

use std::collections::HashMap;
use std::sync::Arc;

use queueforge_amqp::{basic as basic_method, Method};
use queueforge_core::{ConsumerDeliveryId, ConsumerSessionId, QueueCmd, QueueKey, QueueType};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::oneshot;

impl<'a, S> Connection<'a, S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// `handle_basic_ack` on the open connection.
    pub(in crate::connection) async fn handle_basic_ack(
        &mut self,
        channel: u16,
        ack: basic_method::Ack,
    ) -> Result<Step, ConnError> {
        self.ack_or_nack_tags(
            channel,
            ack.delivery_tag,
            ack.multiple,
            /*nack=*/ false,
            true,
        )
        .await
    }
    /// `handle_basic_recover` on the open connection.
    pub(in crate::connection) async fn handle_basic_recover(
        &mut self,
        channel: u16,
    ) -> Result<Step, ConnError> {
        let max_tag = self
            .channels
            .get(&channel)
            .and_then(|ch| ch.delivery_ledger.keys().next_back().copied());
        if let Some(tag) = max_tag {
            self.ack_or_nack_tags(channel, tag, true, true, true)
                .await?;
        }
        self.send_method(channel, &Method::BasicRecoverOk(basic_method::RecoverOk))
            .await?;
        Ok(Step::Continue)
    }
    /// `handle_basic_nack` on the open connection.
    pub(in crate::connection) async fn handle_basic_nack(
        &mut self,
        channel: u16,
        delivery_tag: u64,
        multiple: bool,
        requeue: bool,
    ) -> Result<Step, ConnError> {
        self.ack_or_nack_tags(
            channel,
            delivery_tag,
            multiple,
            /*nack=*/ true,
            requeue,
        )
        .await
    }
    /// `ack_or_nack_tags` on the open connection.
    pub(in crate::connection) async fn ack_or_nack_tags(
        &mut self,
        channel: u16,
        delivery_tag: u64,
        multiple: bool,
        nack: bool,
        requeue: bool,
    ) -> Result<Step, ConnError> {
        let Some(ch) = self.channels.get_mut(&channel) else {
            return Ok(Step::Continue);
        };

        // Issue 3: non-zero delivery_tag must refer to a delivered message (multiple or not).
        // delivery_tag==0 with multiple=true means "all outstanding".
        if delivery_tag != 0 && !ch.delivery_ledger.contains_key(&delivery_tag) {
            self.server_channel_close(
                channel,
                REPLY_PRECONDITION_FAILED,
                "PRECONDITION_FAILED - unknown delivery tag",
                basic_method::CLASS_ID,
                if nack {
                    basic_method::Nack::METHOD_ID
                } else {
                    basic_method::Ack::METHOD_ID
                },
            )
            .await?;
            return Ok(Step::Continue);
        }
        if !multiple && delivery_tag == 0 {
            self.server_channel_close(
                channel,
                REPLY_PRECONDITION_FAILED,
                "PRECONDITION_FAILED - unknown delivery tag",
                basic_method::CLASS_ID,
                if nack {
                    basic_method::Nack::METHOD_ID
                } else {
                    basic_method::Ack::METHOD_ID
                },
            )
            .await?;
            return Ok(Step::Continue);
        }

        let tags: Vec<u64> = if multiple {
            if delivery_tag == 0 {
                ch.delivery_ledger.keys().copied().collect()
            } else {
                ch.delivery_ledger
                    .range(..=delivery_tag)
                    .map(|(t, _)| *t)
                    .collect()
            }
        } else {
            vec![delivery_tag]
        };

        let mut credit_by_session: HashMap<ConsumerSessionId, u32> = HashMap::new();
        let mut ops: Vec<(QueueKey, ConsumerDeliveryId, Option<ConsumerSessionId>)> = Vec::new();

        {
            let ch = self.channels.get_mut(&channel).unwrap();
            for tag in tags {
                if let Some(entry) = ch.delivery_ledger.remove(&tag) {
                    if let Some(session) = entry.session {
                        *credit_by_session.entry(session).or_insert(0) += 1;
                    }
                    ops.push((entry.queue_key, entry.consumer_delivery_id, entry.session));
                }
            }
        }

        for (queue_key, id, session) in ops {
            let from_session = session
                .and_then(|session| self.sessions.get(&session).map(|info| info.handle.clone()));
            let session_holds_ack = from_session.is_some();
            let Some(mut handle) = (match from_session {
                Some(handle) => Some(handle),
                None => self.resolve_queue(&queue_key).await,
            }) else {
                continue;
            };
            if !session_holds_ack {
                if let Some(cluster) = &self.cluster {
                    let quorum = handle
                        .info
                        .args
                        .lock()
                        .unwrap_or_else(|err| err.into_inner())
                        .queue_type
                        == Some(QueueType::Quorum);
                    if quorum && !cluster.is_quorum_leader() {
                        if let Some(proxied) = cluster.leader_consume_handle(&queue_key) {
                            handle = proxied;
                        }
                    }
                }
            }
            let quorum = handle
                .info
                .args
                .lock()
                .unwrap_or_else(|err| err.into_inner())
                .queue_type
                == Some(QueueType::Quorum);
            if quorum {
                let (reply_tx, reply_rx) = oneshot::channel();
                let cmd = if nack {
                    QueueCmd::NackReport {
                        id,
                        requeue,
                        reply: reply_tx,
                    }
                } else {
                    QueueCmd::AckReport {
                        id,
                        reply: reply_tx,
                    }
                };
                if handle.tx.send(cmd).await.is_ok() {
                    if let Ok(Some(message_id)) = reply_rx.await {
                        if let Some(cluster) = &self.cluster {
                            cluster
                                .note_quorum_consumed(&queue_key, message_id.as_str())
                                .await;
                            // Deliver already waited for this drop before the
                            // client saw the body. Waiting again here puts one
                            // round trip per ack ahead of publisher confirms.
                            let cluster = Arc::clone(cluster);
                            let key = queue_key.clone();
                            let id = message_id;
                            tokio::spawn(async move {
                                cluster.quorum_forget(&key, id.as_str()).await;
                            });
                        }
                    }
                }
            } else {
                let cmd = if nack {
                    QueueCmd::Nack { id, requeue }
                } else {
                    QueueCmd::Ack {
                        id,
                        multiple_to: None,
                    }
                };
                let _ = handle.tx.send(cmd).await;
            }
        }

        // Restore prefetch credit to consumers.
        if !nack || requeue {
            // For ack (and nack+requeue that frees channel outstanding), restore credit.
        }
        for (session, credit) in credit_by_session {
            if let Some(info) = self.sessions.get(&session).cloned() {
                if !info.no_ack {
                    let give = match self.channel_global_slots(info.channel, Some(session)) {
                        Some(slots) => credit.min(slots),
                        None => credit,
                    };
                    if give == 0 {
                        continue;
                    }
                    *self.granted_credit.entry(session).or_insert(0) += give;
                    let _ = info
                        .handle
                        .tx
                        .send(QueueCmd::AddCredit {
                            session,
                            credit: give,
                        })
                        .await;
                }
            }
        }

        Ok(Step::Continue)
    }
}
