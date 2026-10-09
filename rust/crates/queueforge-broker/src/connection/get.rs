//! basic.get.

use super::*;

use queueforge_amqp::{basic as basic_method, Method};
use queueforge_auth::{PermissionKind, ResourceKind};
use queueforge_core::{QueueCmd, QueueKey, QueueType};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::oneshot;

impl<'a, S> Connection<'a, S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
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
            if quorum {
                cluster.wait_quorum_leader(&key).await;
            }
            if quorum && !cluster.is_quorum_leader(&key) {
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
                            let drop_local = !cluster.is_quorum_leader(&key);
                            cluster
                                .claim_for_handoff(&key, message_id.as_str(), drop_local, get.no_ack)
                                .await;
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
                self.trace_delivery(key.name.as_str(), &qm.message);

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
}
