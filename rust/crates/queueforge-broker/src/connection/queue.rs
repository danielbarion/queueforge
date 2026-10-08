//! queue.declare, queue.delete, and connection-class methods on an open connection.

use super::*;

use queueforge_amqp::connection as conn_method;
use queueforge_amqp::queue as queue_method;
use queueforge_amqp::Method;
use queueforge_auth::{PermissionKind, ResourceKind};
use queueforge_core::{
    generate_server_queue_name, Error as CoreError, QueueDeclareOpts, QueueHandle, QueueKey,
};
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::info;

impl<'a, S> Connection<'a, S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// `handle_queue_declare` on the open connection.
    pub(in crate::connection) async fn handle_queue_declare(
        &mut self,
        channel: u16,
        declare: queue_method::Declare,
    ) -> Result<Step, ConnError> {
        // Closed declare-arguments set (TTL / DLX / max-length). Unknown keys → 406.
        let queue_args = match parse_queue_declare_args(&declare.arguments) {
            Ok(a) => a,
            Err(msg) => {
                self.server_channel_close(
                    channel,
                    REPLY_PRECONDITION_FAILED,
                    &format!("PRECONDITION_FAILED - {msg}"),
                    queue_method::CLASS_ID,
                    queue_method::Declare::METHOD_ID,
                )
                .await?;
                return Ok(Step::Continue);
            }
        };

        let vhost = self.vhost.clone().unwrap_or_else(|| "/".into());
        let user = self.user.clone().unwrap_or_default();

        // Generate server name first so configure AuthZ uses the final name
        // (Issue 4). Passive empty name is rejected by the registry as NotFound.
        let queue_name = if declare.queue.is_empty() {
            if declare.passive {
                // Fall through to registry for a clear NotFound / protocol error.
                String::new()
            } else {
                generate_server_queue_name()
            }
        } else {
            declare.queue.clone()
        };

        // Configure permission on the final queue name (or empty for passive).
        if !queue_name.is_empty() {
            let user_auth = user.clone();
            let vhost_auth = vhost.clone();
            let name_auth = queue_name.clone();
            match self
                .auth_bool(move |auth| {
                    auth.check_permission(
                        &user_auth,
                        &vhost_auth,
                        &name_auth,
                        ResourceKind::Queue,
                        PermissionKind::Configure,
                    )
                })
                .await
            {
                Ok(true) => {}
                Ok(false) => {
                    self.server_channel_close(
                        channel,
                        REPLY_ACCESS_REFUSED,
                        "ACCESS_REFUSED - configure access to queue refused",
                        queue_method::CLASS_ID,
                        queue_method::Declare::METHOD_ID,
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
                        queue_method::Declare::METHOD_ID,
                    )
                    .await?;
                    return Ok(Step::Continue);
                }
            }
        }

        let declared_args = queue_args.clone();
        let mut queue_args = if declare.passive || queue_name.is_empty() {
            queue_args
        } else {
            self.router
                .queue_args_with_policy(&vhost, &queue_name, &queue_args)
        };
        if !declare.passive {
            if let Err(msg) = finalize_queue_type(
                &mut queue_args,
                self.params.default_queue_type,
                declare.durable,
                declare.exclusive,
            ) {
                self.server_channel_close(
                    channel,
                    REPLY_PRECONDITION_FAILED,
                    &format!("PRECONDITION_FAILED - {msg}"),
                    queue_method::CLASS_ID,
                    queue_method::Declare::METHOD_ID,
                )
                .await?;
                return Ok(Step::Continue);
            }
        }
        if !declare.passive
            && !declare.durable
            && !declare.exclusive
            && queue_args.queue_type != Some(queueforge_core::QueueType::Quorum)
            && !self.connections.transient_nonexcl_permitted()
        {
            let _ = self
                .send_connection_close(
                    REPLY_INTERNAL_ERROR,
                    "INTERNAL_ERROR - Feature `transient_nonexcl_queues` is deprecated.\nBy default, this feature is not permitted anymore.",
                    queue_method::CLASS_ID,
                    queue_method::Declare::METHOD_ID,
                )
                .await;
            self.mark_closed();
            return Ok(Step::Done);
        }
        let already = self
            .queues
            .get(&QueueKey::new(vhost.as_str(), queue_name.as_str()))
            .is_some();
        // A vhost max-queues limit refuses a new queue, as RabbitMQ does.
        if !declare.passive && !already {
            let here = self
                .queues
                .list_keys()
                .iter()
                .filter(|k| k.vhost.as_str() == vhost.as_str())
                .count();
            if !self.connections.queue_allowed(vhost.as_str(), here) {
                self.server_channel_close(
                    channel,
                    REPLY_PRECONDITION_FAILED,
                    &format!("PRECONDITION_FAILED - cannot declare queue '{queue_name}': queue limit in vhost '{vhost}' is reached"),
                    queue_method::CLASS_ID,
                    queue_method::Declare::METHOD_ID,
                )
                .await?;
                return Ok(Step::Continue);
            }
        }
        let opts = QueueDeclareOpts {
            declared_args: Some(declared_args),
            durable: declare.durable,
            exclusive: declare.exclusive,
            auto_delete: declare.auto_delete,
            passive: declare.passive,
            // Peer SocketAddr as connection stand-in until connection-manager ids.
            exclusive_owner: if declare.exclusive {
                Some(self.peer.to_string().into())
            } else {
                None
            },
            args: queue_args,
            home: None,
        };

        let declared = if let Some(cluster) = &self.cluster {
            cluster
                .declare_queue(&vhost, queue_name.as_str(), opts)
                .await
        } else {
            self.queues.declare(&vhost, queue_name.as_str(), opts).await
        };
        match declared {
            Ok(result) => {
                queueforge_core::prom::queue_declared(!declare.passive && !already);
                if !declare.passive && !already {
                    self.emit_event(
                        "queue.created",
                        &vhost,
                        vec![
                            ("name", queueforge_core::AppHeaderValue::Str(result.handle.info.key.name.to_string())),
                            ("durable", queueforge_core::AppHeaderValue::Bool(declare.durable)),
                            ("auto_delete", queueforge_core::AppHeaderValue::Bool(declare.auto_delete)),
                            ("exclusive", queueforge_core::AppHeaderValue::Bool(declare.exclusive)),
                        ],
                    );
                }
                let key = result.handle.info.key.clone();
                if !self.declared_queues.iter().any(|k| k == &key) {
                    self.declared_queues.push(key);
                }
                if declare.no_wait {
                    return Ok(Step::Continue);
                }
                self.send_method(
                    channel,
                    &Method::QueueDeclareOk(queue_method::DeclareOk {
                        queue: result.handle.info.key.name.to_string(),
                        message_count: result.message_count,
                        consumer_count: result.consumer_count,
                    }),
                )
                .await?;
                Ok(Step::Continue)
            }
            Err(err) => {
                let (code, text) = core_error_to_amqp(&err);
                self.server_channel_close(
                    channel,
                    code,
                    &text,
                    queue_method::CLASS_ID,
                    queue_method::Declare::METHOD_ID,
                )
                .await?;
                Ok(Step::Continue)
            }
        }
    }
    /// `handle_queue_delete` on the open connection.
    pub(in crate::connection) async fn handle_queue_delete(
        &mut self,
        channel: u16,
        delete: queue_method::Delete,
    ) -> Result<Step, ConnError> {
        let vhost = self.vhost.clone().unwrap_or_else(|| "/".into());
        let user = self.user.clone().unwrap_or_default();

        let queue_name = delete.queue.clone();
        let user_auth = user.clone();
        let vhost_auth = vhost.clone();
        match self
            .auth_bool(move |auth| {
                auth.check_permission(
                    &user_auth,
                    &vhost_auth,
                    &queue_name,
                    ResourceKind::Queue,
                    PermissionKind::Configure,
                )
            })
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                self.server_channel_close(
                    channel,
                    REPLY_ACCESS_REFUSED,
                    "ACCESS_REFUSED - configure access to queue refused",
                    queue_method::CLASS_ID,
                    queue_method::Delete::METHOD_ID,
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
                    queue_method::Delete::METHOD_ID,
                )
                .await?;
                return Ok(Step::Continue);
            }
        }

        let key = QueueKey::new(vhost.as_str(), delete.queue.as_str());
        match self
            .delete_queue_and_bindings(&key, delete.if_unused, delete.if_empty)
            .await
        {
            Ok(message_count) => {
                queueforge_core::prom::queue_deleted();
                self.emit_event("queue.deleted", &vhost, vec![("name", queueforge_core::AppHeaderValue::Str(delete.queue.to_string()))]);
                if delete.no_wait {
                    return Ok(Step::Continue);
                }
                self.send_method(
                    channel,
                    &Method::QueueDeleteOk(queue_method::DeleteOk { message_count }),
                )
                .await?;
                Ok(Step::Continue)
            }
            // RabbitMQ answers delete-ok with 0 messages for a missing queue.
            Err(CoreError::NotFound(_)) => {
                if !delete.no_wait {
                    self.send_method(channel, &Method::QueueDeleteOk(queue_method::DeleteOk { message_count: 0 }))
                        .await?;
                }
                Ok(Step::Continue)
            }
            Err(err) => {
                let (code, text) = core_error_to_amqp(&err);
                self.server_channel_close(
                    channel,
                    code,
                    &text,
                    queue_method::CLASS_ID,
                    queue_method::Delete::METHOD_ID,
                )
                .await?;
                Ok(Step::Continue)
            }
        }
    }
    /// `resolve_queue` on the open connection.
    pub(in crate::connection) async fn resolve_queue(&self, key: &QueueKey) -> Option<QueueHandle> {
        if let Some(handle) = self.queues.get(key) {
            return Some(handle);
        }
        if let Some(cluster) = &self.cluster {
            return cluster.ensure_proxy(key).await;
        }
        None
    }
    /// Delete a queue from the registry and cascade live binding-index rows.
    ///
    /// Used by `queue.delete`, exclusive cleanup, and auto-delete so cascade
    /// cannot drift across entry points.
    pub(in crate::connection) async fn delete_queue_and_bindings(
        &self,
        key: &QueueKey,
        if_unused: bool,
        if_empty: bool,
    ) -> std::result::Result<u32, CoreError> {
        let message_count = if let Some(cluster) = &self.cluster {
            cluster.delete_queue(key, if_unused, if_empty).await?
        } else {
            self.queues.delete(key, if_unused, if_empty).await?
        };
        self.router
            .remove_queue_bindings(key.vhost.as_str(), key.name.as_str());
        Ok(message_count)
    }
    /// Send `basic.return` + content for an unroutable mandatory publish.
    #[allow(clippy::too_many_arguments)]

    /// `on_open_connection_method` on the open connection.
    pub(in crate::connection) async fn on_open_connection_method(
        &mut self,
        method: Method,
    ) -> Result<Step, ConnError> {
        match method {
            Method::ConnectionClose(close) => {
                info!(
                    peer = %self.peer,
                    code = close.reply_code,
                    text = %close.reply_text,
                    "client connection.close"
                );
                self.cleanup_on_close().await;
                let _ = self
                    .send_method(0, &Method::ConnectionCloseOk(conn_method::CloseOk))
                    .await;
                self.mark_closed();
                Ok(Step::Done)
            }
            Method::ConnectionCloseOk(_) => {
                self.mark_closed();
                Ok(Step::Done)
            }
            other => {
                let _ = self
                    .send_connection_close(
                        REPLY_COMMAND_INVALID,
                        &format!(
                            "unexpected connection method class={} method={}",
                            other.class_id(),
                            other.method_id()
                        ),
                        other.class_id(),
                        other.method_id(),
                    )
                    .await;
                self.mark_closed();
                Ok(Step::Done)
            }
        }
    }
    /// Push current open-channel count into the management connection tracker.
    pub(in crate::connection) fn sync_tracked_channels(&self) {
        let Some(id) = self.conn_track_id.as_deref() else {
            return;
        };
        let user = self.user.clone().unwrap_or_default();
        let vhost = self.vhost.clone().unwrap_or_default();
        let numbers: Vec<u16> = self.channels.keys().copied().collect();
        self.connections
            .sync_channels(id, &user, &vhost, self.peer, &numbers);
        let consumers = self
            .sessions
            .values()
            .map(|session| queueforge_mgmt::ConsumerInfo {
                consumer_tag: session.consumer_tag.clone(),
                connection: id.to_string(),
                channel: session.channel,
                vhost: session.queue_key.vhost.to_string(),
                queue: session.queue_key.name.to_string(),
            })
            .collect();
        self.connections.sync_consumers(id, consumers);
    }
}
