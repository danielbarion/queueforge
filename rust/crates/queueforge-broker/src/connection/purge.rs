//! queue.purge, and the exclusive-queue lock other methods check first.

use super::*;

use queueforge_amqp::queue as queue_method;
use queueforge_amqp::Method;
use queueforge_auth::{PermissionKind, ResourceKind};
use queueforge_core::{QueueCmd, QueueKey};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::oneshot;

/// REPLY_RESOURCE_LOCKED: another connection owns the exclusive queue.
const REPLY_RESOURCE_LOCKED: u16 = 405;

impl<'a, S> Connection<'a, S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// Close `channel` with 405 when another connection owns exclusive queue
    /// `queue`, as RabbitMQ does for consume, get, purge, bind and delete.
    /// Returns true when the channel was closed. A missing queue is left to the caller.
    pub(in crate::connection) async fn refuse_locked(
        &mut self,
        channel: u16,
        queue: &str,
        class_id: u16,
        method_id: u16,
    ) -> Result<bool, ConnError> {
        let vhost = self.vhost.clone().unwrap_or_else(|| "/".into());
        let me = self.peer.to_string();
        let locked = self
            .queues
            .get(&QueueKey::new(vhost.as_str(), queue))
            .is_some_and(|handle| {
                handle.info.exclusive
                    && handle
                        .info
                        .exclusive_owner
                        .as_ref()
                        .is_some_and(|owner| owner.as_str() != me)
            });
        if !locked {
            return Ok(false);
        }
        self.server_channel_close(
            channel,
            REPLY_RESOURCE_LOCKED,
            &format!(
                "RESOURCE_LOCKED - cannot obtain exclusive access to locked queue '{queue}' in vhost '{vhost}'"
            ),
            class_id,
            method_id,
        )
        .await?;
        Ok(true)
    }

    /// `queue.purge`: drop the ready messages and report how many went.
    /// Unacked deliveries stay with their consumers. Needs read permission.
    pub(in crate::connection) async fn handle_queue_purge(
        &mut self,
        channel: u16,
        purge: queue_method::Purge,
    ) -> Result<Step, ConnError> {
        let vhost = self.vhost.clone().unwrap_or_else(|| "/".into());
        let user = self.user.clone().unwrap_or_default();
        let queue_name = purge.queue.clone();
        let (user_auth, vhost_auth) = (user.clone(), vhost.clone());
        let allowed = self
            .auth_bool(move |auth| {
                auth.check_permission(
                    &user_auth,
                    &vhost_auth,
                    &queue_name,
                    ResourceKind::Queue,
                    PermissionKind::Read,
                )
            })
            .await;
        match allowed {
            Ok(true) => {}
            Ok(false) => {
                self.server_channel_close(
                    channel,
                    REPLY_ACCESS_REFUSED,
                    "ACCESS_REFUSED - read access to queue refused",
                    queue_method::CLASS_ID,
                    queue_method::Purge::METHOD_ID,
                )
                .await?;
                return Ok(Step::Continue);
            }
            Err(e) => {
                self.server_channel_close(
                    channel,
                    REPLY_INTERNAL_ERROR,
                    &format!("INTERNAL_ERROR - auth: {e}"),
                    queue_method::CLASS_ID,
                    queue_method::Purge::METHOD_ID,
                )
                .await?;
                return Ok(Step::Continue);
            }
        }
        let key = QueueKey::new(vhost.as_str(), purge.queue.as_str());
        let Some(handle) = self.resolve_queue(&key).await else {
            self.server_channel_close(
                channel,
                REPLY_NOT_FOUND,
                &format!("NOT_FOUND - no queue '{}' in vhost '{vhost}'", purge.queue),
                queue_method::CLASS_ID,
                queue_method::Purge::METHOD_ID,
            )
            .await?;
            return Ok(Step::Continue);
        };
        let (tx, rx) = oneshot::channel();
        let purged = match handle.tx.send(QueueCmd::Purge { reply: tx }).await {
            Ok(()) => rx.await.ok(),
            Err(_) => None,
        };
        let Some(message_count) = purged else {
            self.server_channel_close(
                channel,
                REPLY_INTERNAL_ERROR,
                "INTERNAL_ERROR - queue is unavailable",
                queue_method::CLASS_ID,
                queue_method::Purge::METHOD_ID,
            )
            .await?;
            return Ok(Step::Continue);
        };
        if !purge.no_wait {
            self.send_method(channel, &Method::QueuePurgeOk(queue_method::PurgeOk { message_count }))
                .await?;
        }
        Ok(Step::Continue)
    }
}
