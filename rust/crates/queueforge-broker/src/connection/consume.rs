//! basic.qos, basic.consume, and basic.cancel.

use super::*;

use queueforge_amqp::{basic as basic_method, Method};
use queueforge_auth::{PermissionKind, ResourceKind};
use queueforge_core::{ConsumerSessionId, QueueCmd, QueueKey, QueueType};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::oneshot;

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
}
