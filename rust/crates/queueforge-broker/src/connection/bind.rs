//! queue.bind and queue.unbind.

use super::*;

use std::sync::Arc;

use queueforge_amqp::queue as queue_method;
use queueforge_amqp::Method;
use queueforge_core::{Binding, QueueKey};
use tokio::io::{AsyncRead, AsyncWrite};

impl<'a, S> Connection<'a, S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// `handle_queue_bind` on the open connection.
    pub(in crate::connection) async fn handle_queue_bind(
        &mut self,
        channel: u16,
        bind: queue_method::Bind,
    ) -> Result<Step, ConnError> {
        if bind.exchange.is_empty() {
            self.server_channel_close(
                channel,
                REPLY_ACCESS_REFUSED,
                "ACCESS_REFUSED - cannot bind to the default exchange",
                queue_method::CLASS_ID,
                queue_method::Bind::METHOD_ID,
            )
            .await?;
            return Ok(Step::Continue);
        }

        let vhost = self.vhost.clone().unwrap_or_else(|| "/".into());
        let user = self.user.clone().unwrap_or_default();
        let queue_name = bind.queue.clone();
        let exchange_name = bind.exchange.clone();
        let user_auth = user.clone();
        let vhost_auth = vhost.clone();
        match self
            .auth_bool(move |auth| {
                auth.check_queue_bind(&user_auth, &vhost_auth, &queue_name, &exchange_name)
            })
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                self.server_channel_close(
                    channel,
                    REPLY_ACCESS_REFUSED,
                    "ACCESS_REFUSED - bind access refused",
                    queue_method::CLASS_ID,
                    queue_method::Bind::METHOD_ID,
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
                    queue_method::Bind::METHOD_ID,
                )
                .await?;
                return Ok(Step::Continue);
            }
        }

        // Exchange must exist in router or store.
        if self
            .router
            .get_exchange(&vhost, bind.exchange.as_str())
            .is_none()
        {
            if let Some(ex) = self
                .store
                .get_exchange(&vhost, bind.exchange.as_str())
                .ok()
                .flatten()
            {
                self.router.put_exchange(ex);
            } else {
                self.server_channel_close(
                    channel,
                    REPLY_NOT_FOUND,
                    &format!(
                        "NOT_FOUND - no exchange '{}' in vhost '{vhost}'",
                        bind.exchange
                    ),
                    queue_method::CLASS_ID,
                    queue_method::Bind::METHOD_ID,
                )
                .await?;
                return Ok(Step::Continue);
            }
        }

        let key = QueueKey::new(vhost.as_str(), bind.queue.as_str());
        let Some(handle) = self.resolve_queue(&key).await else {
            self.server_channel_close(
                channel,
                REPLY_NOT_FOUND,
                &format!("NOT_FOUND - no queue '{}' in vhost '{vhost}'", bind.queue),
                queue_method::CLASS_ID,
                queue_method::Bind::METHOD_ID,
            )
            .await?;
            return Ok(Step::Continue);
        };

        let mut binding = Binding::new(
            vhost.as_str(),
            bind.exchange.as_str(),
            bind.queue.as_str(),
            bind.routing_key.as_str(),
        );
        binding.args = field_table_to_header_args(&bind.arguments);

        match self.router.bind(binding.clone()) {
            Ok(created) => {
                // Persist when exchange and queue are durable (fail-closed).
                let ex_durable = self
                    .router
                    .get_exchange(&vhost, bind.exchange.as_str())
                    .map(|e| e.durable)
                    .unwrap_or(false);
                if created && ex_durable && handle.info.durable {
                    let store = Arc::clone(&self.store);
                    let to_store = binding.clone();
                    if let Err(e) =
                        MetadataStore::blocking(store, move |s| s.put_binding(&to_store)).await
                    {
                        // Roll back live binding so restart cannot lose topology
                        // the client thought was durable.
                        let _ = self.router.unbind(&binding);
                        self.server_channel_close(
                            channel,
                            REPLY_INTERNAL_ERROR,
                            &format!("INTERNAL_ERROR - failed to persist binding: {e}"),
                            queue_method::CLASS_ID,
                            queue_method::Bind::METHOD_ID,
                        )
                        .await?;
                        return Ok(Step::Continue);
                    }
                }
                if let Some(cluster) = &self.cluster {
                    cluster
                        .replicate_json(
                            "binding",
                            serde_json::to_value(&binding).unwrap_or(serde_json::Value::Null),
                        )
                        .await;
                }
                if !bind.no_wait {
                    self.send_method(channel, &Method::QueueBindOk(queue_method::BindOk))
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
                    queue_method::Bind::METHOD_ID,
                )
                .await?;
                Ok(Step::Continue)
            }
        }
    }
    /// `handle_queue_unbind` on the open connection.
    pub(in crate::connection) async fn handle_queue_unbind(
        &mut self,
        channel: u16,
        unbind: queue_method::Unbind,
    ) -> Result<Step, ConnError> {
        if unbind.exchange.is_empty() {
            self.server_channel_close(
                channel,
                REPLY_ACCESS_REFUSED,
                "ACCESS_REFUSED - cannot unbind from the default exchange",
                queue_method::CLASS_ID,
                queue_method::Unbind::METHOD_ID,
            )
            .await?;
            return Ok(Step::Continue);
        }

        let vhost = self.vhost.clone().unwrap_or_else(|| "/".into());
        let user = self.user.clone().unwrap_or_default();
        let queue_name = unbind.queue.clone();
        let exchange_name = unbind.exchange.clone();
        let user_auth = user.clone();
        let vhost_auth = vhost.clone();
        match self
            .auth_bool(move |auth| {
                auth.check_queue_unbind(&user_auth, &vhost_auth, &queue_name, &exchange_name)
            })
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                self.server_channel_close(
                    channel,
                    REPLY_ACCESS_REFUSED,
                    "ACCESS_REFUSED - unbind access refused",
                    queue_method::CLASS_ID,
                    queue_method::Unbind::METHOD_ID,
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
                    queue_method::Unbind::METHOD_ID,
                )
                .await?;
                return Ok(Step::Continue);
            }
        }

        let mut binding = Binding::new(
            vhost.as_str(),
            unbind.exchange.as_str(),
            unbind.queue.as_str(),
            unbind.routing_key.as_str(),
        );
        binding.args = field_table_to_header_args(&unbind.arguments);
        let _ = self.router.unbind(&binding);
        let store = Arc::clone(&self.store);
        let vhost_u = binding.vhost.to_string();
        let exchange_u = binding.exchange.to_string();
        let queue_u = binding.queue.to_string();
        let rk_u = binding.routing_key.to_string();
        let args_key = queueforge_store::binding_args_key(&binding.args);
        let _ = MetadataStore::blocking(store, move |s| {
            s.delete_binding(&vhost_u, &exchange_u, &queue_u, &rk_u, &args_key)
        })
        .await;
        if let Some(cluster) = &self.cluster {
            cluster
                .replicate_json(
                    "unbind",
                    serde_json::to_value(&binding).unwrap_or(serde_json::Value::Null),
                )
                .await;
        }

        self.send_method(channel, &Method::QueueUnbindOk(queue_method::UnbindOk))
            .await?;
        Ok(Step::Continue)
    }
}
