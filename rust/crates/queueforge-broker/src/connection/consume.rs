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
        if consume.queue == REPLY_TO {
            return self.consume_replies(channel, consume).await;
        }
        let unknown: Vec<&str> = consume
            .arguments
            .entries
            .iter()
            .map(|(k, _)| k.as_str())
            .filter(|key| *key != "x-priority" && *key != "x-stream-offset")
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
        if quorum {
            if let Some(cluster) = &self.cluster {
                cluster.wait_quorum_leader().await;
            }
        }
        let handle = if quorum {
            self.cluster
                .as_ref()
                .and_then(|cluster| cluster.leader_consume_handle(&key))
                .unwrap_or(handle)
        } else {
            handle
        };

        // A stream consumer needs a prefetch and manual acks, as in RabbitMQ,
        // and starts where x-stream-offset says.
        let stream = handle
            .info
            .args
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .queue_type
            == Some(QueueType::Stream);
        if stream {
            let refuse = if consume.no_ack {
                Some("PRECONDITION_FAILED - stream queues need manual acknowledgement")
            } else if initial_credit.is_none() {
                Some("PRECONDITION_FAILED - consumer prefetch count is not set for stream queue")
            } else {
                None
            };
            if let Some(text) = refuse {
                self.granted_credit.remove(&session);
                self.server_channel_close(channel, REPLY_PRECONDITION_FAILED, text, basic_method::CLASS_ID, basic_method::Consume::METHOD_ID)
                    .await?;
                return Ok(Step::Continue);
            }
            let start = match stream_start(&consume.arguments) {
                Ok(start) => start,
                Err(text) => {
                    self.granted_credit.remove(&session);
                    self.server_channel_close(channel, REPLY_PRECONDITION_FAILED, &text, basic_method::CLASS_ID, basic_method::Consume::METHOD_ID)
                        .await?;
                    return Ok(Step::Continue);
                }
            };
            let _ = handle.tx.send(QueueCmd::StreamStart { session, start }).await;
        }

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
        let event_vhost = self.vhost.clone().unwrap_or_else(|| "/".into());
        self.emit_event(
            "consumer.created",
            &event_vhost,
            vec![
                ("queue", queueforge_core::AppHeaderValue::Str(consume.queue.to_string())),
                ("consumer_tag", queueforge_core::AppHeaderValue::Str(consumer_tag.to_string())),
                ("exclusive", queueforge_core::AppHeaderValue::Bool(consume.exclusive)),
                ("ack_required", queueforge_core::AppHeaderValue::Bool(!consume.no_ack)),
            ],
        );

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

        if self
            .reply_addrs
            .get(&channel)
            .is_some_and(|(_, tag)| *tag == cancel.consumer_tag)
        {
            if let Some((addr, _)) = self.reply_addrs.remove(&channel) {
                self.connections.remove_reply(&addr);
            }
        }
        if let Some(session) = session {
            self.cancel_session(session, /*requeue=*/ true).await;
            let event_vhost = self.vhost.clone().unwrap_or_else(|| "/".into());
            self.emit_event("consumer.deleted", &event_vhost, vec![("consumer_tag", queueforge_core::AppHeaderValue::Str(cancel.consumer_tag.to_string()))]);
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

/// The direct reply-to pseudo-queue.
pub(super) const REPLY_TO: &str = "amq.rabbitmq.reply-to";

impl<'a, S> Connection<'a, S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// basic.consume on `amq.rabbitmq.reply-to`: give this channel a reply
    /// address. RabbitMQ requires no-ack and allows one per channel.
    async fn consume_replies(&mut self, channel: u16, consume: basic_method::Consume) -> Result<Step, ConnError> {
        let refuse = if !consume.no_ack {
            Some("PRECONDITION_FAILED - reply consumer cannot acknowledge")
        } else if self.reply_addrs.contains_key(&channel) {
            Some("PRECONDITION_FAILED - reply consumer already set")
        } else {
            None
        };
        if let Some(text) = refuse {
            self.server_channel_close(channel, REPLY_PRECONDITION_FAILED, text, basic_method::CLASS_ID, basic_method::Consume::METHOD_ID)
                .await?;
            return Ok(Step::Continue);
        }
        let tag = if consume.consumer_tag.is_empty() {
            format!("amq.ctag-{}", random_token())
        } else {
            consume.consumer_tag.clone()
        };
        let addr = format!("{REPLY_TO}.{}", random_token());
        self.connections.add_reply(
            addr.clone(),
            queueforge_mgmt::ReplySink {
                channel,
                consumer_tag: tag.clone(),
                tx: self.reply_tx.clone(),
            },
        );
        self.reply_addrs.insert(channel, (addr, tag.clone()));
        if !consume.no_wait {
            self.send_method(channel, &Method::BasicConsumeOk(basic_method::ConsumeOk { consumer_tag: tag }))
                .await?;
        }
        Ok(Step::Continue)
    }
}

/// A random URL-safe token for reply addresses and consumer tags.
fn random_token() -> String {
    use std::fmt::Write as _;
    let mut bytes = [0u8; 16];
    getrandom_fill(&mut bytes);
    let mut out = String::with_capacity(32);
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}

fn getrandom_fill(buf: &mut [u8]) {
    use std::hash::{BuildHasher, Hasher};
    for chunk in buf.chunks_mut(8) {
        let mut h = std::collections::hash_map::RandomState::new().build_hasher();
        h.write_u128(std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0));
        let v = h.finish().to_le_bytes();
        chunk.copy_from_slice(&v[..chunk.len()]);
    }
}

/// `x-stream-offset` from basic.consume arguments: first, last, next, an
/// offset number, a timestamp, or an age such as `1h`.
fn stream_start(args: &queueforge_amqp::FieldTable) -> Result<queueforge_core::StreamStart, String> {
    use queueforge_amqp::FieldValue as F;
    use queueforge_core::StreamStart as S;
    let Some(value) = args.get("x-stream-offset") else {
        return Ok(S::Next);
    };
    let n = |v: i64| u64::try_from(v).map_err(|_| "PRECONDITION_FAILED - x-stream-offset must not be negative".to_string());
    Ok(match value.clone() {
        F::LongString(b) => match String::from_utf8_lossy(&b).as_ref() {
            "first" => S::First,
            "last" => S::Last,
            "next" => S::Next,
            other => S::AgeMs(queueforge_core::queue_parse_age(other).ok_or_else(|| format!("PRECONDITION_FAILED - invalid x-stream-offset '{other}'"))?),
        },
        F::ShortString(s) => match s.as_str() {
            "first" => S::First,
            "last" => S::Last,
            "next" => S::Next,
            other => S::AgeMs(queueforge_core::queue_parse_age(other).ok_or_else(|| format!("PRECONDITION_FAILED - invalid x-stream-offset '{other}'"))?),
        },
        F::Timestamp(secs) => S::TimestampMs(secs.saturating_mul(1000)),
        F::I8(v) => S::Offset(n(i64::from(v))?),
        F::U8(v) => S::Offset(u64::from(v)),
        F::I16(v) => S::Offset(n(i64::from(v))?),
        F::U16(v) => S::Offset(u64::from(v)),
        F::I32(v) => S::Offset(n(i64::from(v))?),
        F::U32(v) => S::Offset(u64::from(v)),
        F::I64(v) => S::Offset(n(v)?),
        F::U64(v) => S::Offset(v),
        _ => return Err("PRECONDITION_FAILED - invalid x-stream-offset".into()),
    })
}
