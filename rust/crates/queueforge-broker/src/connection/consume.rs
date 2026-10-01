//! basic.consume, get, ack, nack, recover, and delivery.
//!
//! These methods belong to [`Connection`]. The frame loop in the parent module calls them.

use super::*;

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use compact_str::CompactString;
use queueforge_amqp::channel as chan_method;
use queueforge_amqp::tx as tx_method;
use queueforge_amqp::confirm as confirm_method;
use queueforge_amqp::connection as conn_method;
use queueforge_amqp::exchange as exchange_method;
use queueforge_amqp::queue as queue_method;
use queueforge_amqp::{
    basic as basic_method, BasicProperties, ContentHeader, FieldTable, FieldValue, Frame, FrameType,
    Method,
};
use queueforge_auth::{PermissionKind, ResourceKind};
use queueforge_core::{
    generate_server_queue_name, Binding, ConsumerDeliveryId, ConsumerSessionId, Error as CoreError,
    Exchange, ExchangeType, Message, QueueCmd, QueueDeclareOpts, QueueDelivery, QueueHandle,
    QueueKey, QueueType, DEFAULT_EXCHANGE_NAME,
};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot, watch};
use tracing::{debug, info, trace, warn};

impl<'a, S> Connection<'a, S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// `handle_basic_qos` on the open connection.
    pub(in crate::connection) async fn handle_basic_qos(
        &mut self,
        channel: u16,
        qos: basic_method::Qos,
    ) -> Result<Step, ConnError> {
        // RabbitMQ 4 denies the global QoS feature. `global=true` still returns
        // qos-ok and the count limits each consumer on the channel.
        if let Some(ch) = self.channels.get_mut(&channel) {
            ch.prefetch_count = qos.prefetch_count;
            ch.global_prefetch = 0;
        }
        let sessions: Vec<ConsumerSessionId> = self
            .channels
            .get(&channel)
            .map(|c| c.consumers.values().copied().collect())
            .unwrap_or_default();

        // Reconcile per-consumer credit to the limits that apply on this channel.
        for session in sessions {
            let Some(info) = self.sessions.get(&session).cloned() else {
                continue;
            };
            if info.no_ack {
                continue;
            }
            let credit = self.credit_for(info.channel, false, Some(session));
            self.remember_grant(session, credit);
            let _ = info
                .handle
                .tx
                .send(QueueCmd::SetCredit { session, credit })
                .await;
        }

        // prefetch_size ignored (common for RabbitMQ-compatible brokers).
        self.send_method(channel, &Method::BasicQosOk(basic_method::QosOk))
            .await?;
        Ok(Step::Continue)
    }

    /// `handle_basic_consume` on the open connection.
    pub(in crate::connection) async fn handle_basic_consume(
        &mut self,
        channel: u16,
        consume: basic_method::Consume,
    ) -> Result<Step, ConnError> {
        let unknown: Vec<&str> = consume
            .arguments
            .entries
            .iter()
            .map(|(k, _)| k.as_str())
            .filter(|key| *key != "x-priority")
            .collect();
        if !unknown.is_empty() {
            let keys = unknown;
            self.server_channel_close(
                channel,
                REPLY_PRECONDITION_FAILED,
                &format!(
                    "PRECONDITION_FAILED - unknown consume argument(s): {}",
                    keys.join(", ")
                ),
                basic_method::CLASS_ID,
                basic_method::Consume::METHOD_ID,
            )
            .await?;
            return Ok(Step::Continue);
        }

        let vhost = self.vhost.clone().unwrap_or_else(|| "/".into());
        let user = self.user.clone().unwrap_or_default();
        let queue_name = consume.queue.clone();

        let user_auth = user.clone();
        let vhost_auth = vhost.clone();
        let queue_auth = queue_name.clone();
        match self
            .auth_bool(move |auth| {
                auth.check_permission(
                    &user_auth,
                    &vhost_auth,
                    &queue_auth,
                    ResourceKind::Queue,
                    PermissionKind::Read,
                )
            })
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                self.server_channel_close(
                    channel,
                    REPLY_ACCESS_REFUSED,
                    "ACCESS_REFUSED - read access to queue refused",
                    basic_method::CLASS_ID,
                    basic_method::Consume::METHOD_ID,
                )
                .await?;
                return Ok(Step::Continue);
            }
            Err(e) => {
                self.server_channel_close(
                    channel,
                    REPLY_INTERNAL_ERROR,
                    &format!("INTERNAL_ERROR - auth: {e}"),
                    basic_method::CLASS_ID,
                    basic_method::Consume::METHOD_ID,
                )
                .await?;
                return Ok(Step::Continue);
            }
        }

        let key = QueueKey::new(vhost.as_str(), queue_name.as_str());
        let Some(handle) = self.resolve_queue(&key).await else {
            self.server_channel_close(
                channel,
                REPLY_NOT_FOUND,
                &format!("NOT_FOUND - no queue '{queue_name}' in vhost '{vhost}'"),
                basic_method::CLASS_ID,
                basic_method::Consume::METHOD_ID,
            )
            .await?;
            return Ok(Step::Continue);
        };

        // Session ids are queue-global. Per-connection counters collide when
        // two connections consume from the same queue (both would use session 1).
        let session = ConsumerSessionId(next_consumer_session());
        let consumer_tag = if consume.consumer_tag.is_empty() {
            format!("ctag-{channel}-{}", session.0)
        } else {
            consume.consumer_tag.clone()
        };

        if self
            .channels
            .get(&channel)
            .map(|c| c.consumers.contains_key(&consumer_tag))
            .unwrap_or(false)
        {
            self.server_channel_close(
                channel,
                REPLY_PRECONDITION_FAILED,
                "PRECONDITION_FAILED - consumer tag already in use",
                basic_method::CLASS_ID,
                basic_method::Consume::METHOD_ID,
            )
            .await?;
            return Ok(Step::Continue);
        }

        // Issue 1: `None` = unlimited; `Some(0)` = zero remaining slots (hold).
        // Never overload 0 as unlimited.
        let initial_credit = self.credit_for(channel, consume.no_ack, Some(session));
        self.remember_grant(session, initial_credit);

        let quorum = handle
            .info
            .args
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .queue_type
            == Some(QueueType::Quorum);
        let handle = if quorum {
            self.cluster
                .as_ref()
                .and_then(|cluster| cluster.leader_consume_handle(&key))
                .unwrap_or(handle)
        } else {
            handle
        };

        let (reply_tx, reply_rx) = oneshot::channel();
        if handle
            .tx
            .send(QueueCmd::RegisterConsumer {
                session,
                no_ack: consume.no_ack,
                exclusive: consume.exclusive,
                priority: consumer_priority(&consume.arguments),
                initial_credit,
                deliver_tx: self.delivery_tx.clone(),
                reply: reply_tx,
            })
            .await
            .is_err()
        {
            self.granted_credit.remove(&session);
            self.server_channel_close(
                channel,
                REPLY_INTERNAL_ERROR,
                "INTERNAL_ERROR - queue mailbox closed",
                basic_method::CLASS_ID,
                basic_method::Consume::METHOD_ID,
            )
            .await?;
            return Ok(Step::Continue);
        }

        match reply_rx.await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                self.granted_credit.remove(&session);
                let (code, text) = core_error_to_amqp(&err);
                self.server_channel_close(
                    channel,
                    code,
                    &text,
                    basic_method::CLASS_ID,
                    basic_method::Consume::METHOD_ID,
                )
                .await?;
                return Ok(Step::Continue);
            }
            Err(_) => {
                self.granted_credit.remove(&session);
                self.server_channel_close(
                    channel,
                    REPLY_INTERNAL_ERROR,
                    "INTERNAL_ERROR - register consumer reply dropped",
                    basic_method::CLASS_ID,
                    basic_method::Consume::METHOD_ID,
                )
                .await?;
                return Ok(Step::Continue);
            }
        }

        self.sessions.insert(
            session,
            SessionInfo {
                channel,
                consumer_tag: consumer_tag.clone(),
                queue_key: key,
                handle: handle.clone(),
                no_ack: consume.no_ack,
            },
        );
        if let Some(ch) = self.channels.get_mut(&channel) {
            ch.consumers.insert(consumer_tag.clone(), session);
        }

        if !consume.no_wait {
            self.send_method(
                channel,
                &Method::BasicConsumeOk(basic_method::ConsumeOk {
                    consumer_tag: consumer_tag.clone(),
                }),
            )
            .await?;
        }
        self.sync_tracked_channels();
        Ok(Step::Continue)
    }

    /// `handle_basic_cancel` on the open connection.
    pub(in crate::connection) async fn handle_basic_cancel(
        &mut self,
        channel: u16,
        cancel: basic_method::Cancel,
    ) -> Result<Step, ConnError> {
        let session = self
            .channels
            .get(&channel)
            .and_then(|c| c.consumers.get(&cancel.consumer_tag).copied());

        if let Some(session) = session {
            self.cancel_session(session, /*requeue=*/ true).await;
            if let Some(ch) = self.channels.get_mut(&channel) {
                ch.consumers.remove(&cancel.consumer_tag);
            }
        }

        if !cancel.no_wait {
            self.send_method(
                channel,
                &Method::BasicCancelOk(basic_method::CancelOk {
                    consumer_tag: cancel.consumer_tag,
                }),
            )
            .await?;
        }
        self.sync_tracked_channels();
        Ok(Step::Continue)
    }

    /// `handle_basic_get` on the open connection.
    pub(in crate::connection) async fn handle_basic_get(
        &mut self,
        channel: u16,
        get: basic_method::Get,
    ) -> Result<Step, ConnError> {
        let vhost = self.vhost.clone().unwrap_or_else(|| "/".into());
        let user = self.user.clone().unwrap_or_default();

        let queue_name = get.queue.clone();
        let user_auth = user.clone();
        let vhost_auth = vhost.clone();
        match self
            .auth_bool(move |auth| {
                auth.check_permission(
                    &user_auth,
                    &vhost_auth,
                    &queue_name,
                    ResourceKind::Queue,
                    PermissionKind::Read,
                )
            })
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                self.server_channel_close(
                    channel,
                    REPLY_ACCESS_REFUSED,
                    "ACCESS_REFUSED - read access to queue refused",
                    basic_method::CLASS_ID,
                    basic_method::Get::METHOD_ID,
                )
                .await?;
                return Ok(Step::Continue);
            }
            Err(e) => {
                self.server_channel_close(
                    channel,
                    REPLY_INTERNAL_ERROR,
                    &format!("INTERNAL_ERROR - auth: {e}"),
                    basic_method::CLASS_ID,
                    basic_method::Get::METHOD_ID,
                )
                .await?;
                return Ok(Step::Continue);
            }
        }

        let key = QueueKey::new(vhost.as_str(), get.queue.as_str());
        let Some(mut handle) = self.resolve_queue(&key).await else {
            self.server_channel_close(
                channel,
                REPLY_NOT_FOUND,
                &format!("NOT_FOUND - no queue '{}' in vhost '{vhost}'", get.queue),
                basic_method::CLASS_ID,
                basic_method::Get::METHOD_ID,
            )
            .await?;
            return Ok(Step::Continue);
        };
        if let Some(cluster) = &self.cluster {
            let quorum = handle
                .info
                .args
                .lock()
                .unwrap_or_else(|err| err.into_inner())
                .queue_type
                == Some(QueueType::Quorum);
            if quorum && !cluster.is_quorum_leader() {
                if let Some(proxied) = cluster.leader_consume_handle(&key) {
                    handle = proxied;
                }
            }
        }

        let (reply_tx, reply_rx) = oneshot::channel();
        if handle
            .tx
            .send(QueueCmd::Get {
                no_ack: get.no_ack,
                reply: reply_tx,
            })
            .await
            .is_err()
        {
            self.server_channel_close(
                channel,
                REPLY_INTERNAL_ERROR,
                "INTERNAL_ERROR - queue mailbox closed",
                basic_method::CLASS_ID,
                basic_method::Get::METHOD_ID,
            )
            .await?;
            return Ok(Step::Continue);
        }

        match reply_rx.await {
            Ok(None) => {
                queueforge_core::prom::message_get_empty();
                self.send_method(
                    channel,
                    &Method::BasicGetEmpty(basic_method::GetEmpty::new()),
                )
                .await?;
                Ok(Step::Continue)
            }
            Ok(Some((delivery_id, qm, message_count))) => {
                let quorum = handle
                    .info
                    .args
                    .lock()
                    .unwrap_or_else(|err| err.into_inner())
                    .queue_type
                    == Some(QueueType::Quorum);
                if quorum {
                    if let Some(message_id) = qm.message.message_id.clone() {
                        if let Some(cluster) = &self.cluster {
                            let drop_local = !cluster.is_quorum_leader();
                            cluster.claim_for_handoff(&key, message_id.as_str(), drop_local).await;
                        }
                    }
                }
                let delivery_tag = {
                    let ch = self.channels.get_mut(&channel).unwrap();
                    let tag = ch.next_delivery_tag;
                    ch.next_delivery_tag = ch.next_delivery_tag.saturating_add(1);
                    if !get.no_ack {
                        ch.delivery_ledger.insert(
                            tag,
                            OutstandingDelivery {
                                queue_key: key.clone(),
                                consumer_delivery_id: delivery_id,
                                consumer_tag: None,
                                session: None,
                            },
                        );
                    }
                    tag
                };

                let props = message_to_properties(&qm.message);
                let body = qm.message.body.clone();
                let redelivered = qm.message.redelivered;

                self.send_method(
                    channel,
                    &Method::BasicGetOk(basic_method::GetOk {
                        delivery_tag,
                        redelivered,
                        exchange: qm.message.exchange.to_string(),
                        routing_key: qm.message.routing_key.to_string(),
                        message_count,
                    }),
                )
                .await?;
                self.send_content(channel, &props, &body).await?;
                Ok(Step::Continue)
            }
            Err(_) => {
                self.server_channel_close(
                    channel,
                    REPLY_INTERNAL_ERROR,
                    "INTERNAL_ERROR - queue home is unavailable",
                    basic_method::CLASS_ID,
                    basic_method::Get::METHOD_ID,
                )
                .await?;
                Ok(Step::Continue)
            }
        }
    }

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
    pub(in crate::connection) async fn handle_basic_recover(&mut self, channel: u16) -> Result<Step, ConnError> {
        let max_tag = self
            .channels
            .get(&channel)
            .and_then(|ch| ch.delivery_ledger.keys().next_back().copied());
        if let Some(tag) = max_tag {
            self.ack_or_nack_tags(channel, tag, true, true, true).await?;
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
            let from_session = session.and_then(|session| self.sessions.get(&session).map(|info| info.handle.clone()));
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
                    QueueCmd::NackReport { id, requeue, reply: reply_tx }
                } else {
                    QueueCmd::AckReport { id, reply: reply_tx }
                };
                if handle.tx.send(cmd).await.is_ok() {
                    if let Ok(Some(message_id)) = reply_rx.await {
                        if let Some(cluster) = &self.cluster {
                            cluster.note_quorum_consumed(&queue_key, message_id.as_str()).await;
                            cluster.quorum_forget(&queue_key, message_id.as_str()).await;
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

    /// `forward_delivery` on the open connection.
    pub(in crate::connection) async fn forward_delivery(&mut self, delivery: QueueDelivery) -> Result<(), ConnError> {
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
    pub(in crate::connection) async fn cancel_session(&mut self, session: ConsumerSessionId, requeue: bool) {
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

    /// `cleanup_on_close` on the open connection.
    pub(in crate::connection) async fn cleanup_on_close(&mut self) {
        if self.cleaned_up {
            return;
        }
        self.cleaned_up = true;

        let channels: Vec<u16> = self.channels.keys().copied().collect();
        for ch in channels {
            self.teardown_channel(ch, /*requeue=*/ true).await;
        }

        // Delete exclusive queues owned by this connection (cascade bindings).
        let owner = self.peer.to_string();
        let exclusive = self.queues.list_exclusive_owned_by(&owner);
        for handle in exclusive {
            let key = handle.info.key.clone();
            let _ = self.delete_queue_and_bindings(&key, false, false).await;
            self.declared_queues.retain(|k| k != &key);
        }

        // Auto-delete queues declared here with no consumers (exclusive already gone).
        let vhost = self.vhost.clone().unwrap_or_else(|| "/".into());
        let auto = self.queues.list_auto_delete_in_vhost(&vhost);
        for handle in auto {
            // Only delete if this connection declared them and they have no consumers.
            if self.declared_queues.iter().any(|k| k == &handle.info.key) {
                let _ = self
                    .delete_queue_and_bindings(&handle.info.key, /*if_unused=*/ true, false)
                    .await;
            }
        }
        self.declared_queues.clear();
        self.sessions.clear();
    }

    /// Server-initiated channel.close: drop the channel from the open set
    /// (and the channels gauge) before sending, so clients may reopen the id.
    pub(in crate::connection) async fn server_channel_close(
        &mut self,
        channel: u16,
        reply_code: u16,
        reply_text: &str,
        class_id: u16,
        method_id: u16,
    ) -> Result<(), ConnError> {
        // Requeue unacked and drop consumers for this channel.
        self.teardown_channel(channel, /*requeue=*/ true).await;
        let close = Method::ChannelClose(chan_method::Close {
            reply_code,
            reply_text: reply_text.to_string(),
            class_id,
            method_id,
        });
        self.send_method(channel, &close).await
    }
}
