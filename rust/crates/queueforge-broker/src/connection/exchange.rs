//! exchange.delete, exchange.bind, and exchange.unbind.

use super::*;

use std::sync::Arc;

use queueforge_amqp::exchange as exchange_method;
use queueforge_amqp::Method;
use queueforge_auth::{PermissionKind, ResourceKind};
use tokio::io::{AsyncRead, AsyncWrite};

impl<'a, S> Connection<'a, S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// `handle_exchange_delete` on the open connection.
    pub(in crate::connection) async fn handle_exchange_delete(
        &mut self,
        channel: u16,
        delete: exchange_method::Delete,
    ) -> Result<Step, ConnError> {
        if delete.exchange.is_empty()
            || queueforge_core::BUILTIN_EXCHANGE_NAMES.contains(&delete.exchange.as_str())
        {
            self.server_channel_close(
                channel,
                REPLY_ACCESS_REFUSED,
                "ACCESS_REFUSED - cannot delete builtin exchange",
                exchange_method::CLASS_ID,
                exchange_method::Delete::METHOD_ID,
            )
            .await?;
            return Ok(Step::Continue);
        }

        let vhost = self.vhost.clone().unwrap_or_else(|| "/".into());
        let user = self.user.clone().unwrap_or_default();
        let exchange_name = delete.exchange.clone();
        let user_auth = user.clone();
        let vhost_auth = vhost.clone();
        match self
            .auth_bool(move |auth| {
                auth.check_permission(
                    &user_auth,
                    &vhost_auth,
                    &exchange_name,
                    ResourceKind::Exchange,
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
                    "ACCESS_REFUSED - configure access to exchange refused",
                    exchange_method::CLASS_ID,
                    exchange_method::Delete::METHOD_ID,
                )
                .await?;
                return Ok(Step::Continue);
            }
            Err(e) => {
                self.server_channel_close(
                    channel,
                    REPLY_INTERNAL_ERROR,
                    &format!("INTERNAL_ERROR - auth: {e}"),
                    exchange_method::CLASS_ID,
                    exchange_method::Delete::METHOD_ID,
                )
                .await?;
                return Ok(Step::Continue);
            }
        }

        if self
            .router
            .get_exchange(&vhost, delete.exchange.as_str())
            .is_none()
            && self
                .store
                .get_exchange(&vhost, delete.exchange.as_str())
                .ok()
                .flatten()
                .is_none()
        {
            self.server_channel_close(
                channel,
                REPLY_NOT_FOUND,
                &format!(
                    "NOT_FOUND - no exchange '{}' in vhost '{vhost}'",
                    delete.exchange
                ),
                exchange_method::CLASS_ID,
                exchange_method::Delete::METHOD_ID,
            )
            .await?;
            return Ok(Step::Continue);
        }

        if delete.if_unused {
            let n = self
                .router
                .index()
                .count_for_exchange(&vhost, delete.exchange.as_str());
            if n > 0 {
                self.server_channel_close(
                    channel,
                    REPLY_PRECONDITION_FAILED,
                    &format!(
                        "PRECONDITION_FAILED - exchange '{}' in use (bindings={n})",
                        delete.exchange
                    ),
                    exchange_method::CLASS_ID,
                    exchange_method::Delete::METHOD_ID,
                )
                .await?;
                return Ok(Step::Continue);
            }
        }

        // Durable row + cascade bindings in store (off the worker).
        let store = Arc::clone(&self.store);
        let vhost_del = vhost.clone();
        let exchange_del = delete.exchange.to_string();
        let _ = MetadataStore::blocking(store, move |s| {
            let _ = s.delete_exchange(&vhost_del, &exchange_del);
            for b in s
                .list_bindings_for_exchange(&vhost_del, &exchange_del)
                .unwrap_or_default()
            {
                let args_key = queueforge_store::binding_args_key(&b.args);
                let _ = s.delete_binding(
                    b.vhost.as_str(),
                    b.exchange.as_str(),
                    b.queue.as_str(),
                    b.routing_key.as_str(),
                    &args_key,
                );
            }
            Ok(())
        })
        .await;
        let _ = self
            .router
            .delete_exchange(&vhost, delete.exchange.as_str());

        if !delete.no_wait {
            self.send_method(
                channel,
                &Method::ExchangeDeleteOk(exchange_method::DeleteOk),
            )
            .await?;
        }
        Ok(Step::Continue)
    }
    /// `handle_exchange_bind` on the open connection.
    pub(in crate::connection) async fn handle_exchange_bind(
        &mut self,
        channel: u16,
        bind: exchange_method::Bind,
    ) -> Result<Step, ConnError> {
        let vhost = self.vhost.clone().unwrap_or_else(|| "/".into());
        if let Err(e) = self.router.bind_exchange(
            &vhost,
            bind.source.as_str(),
            bind.destination.as_str(),
            bind.routing_key.as_str(),
        ) {
            let (code, text) = match e {
                queueforge_core::Error::NotFound(_) => (
                    REPLY_NOT_FOUND,
                    format!(
                        "NOT_FOUND - no exchange '{}' or '{}' in vhost '{vhost}'",
                        bind.source, bind.destination
                    ),
                ),
                other => (
                    REPLY_PRECONDITION_FAILED,
                    format!("PRECONDITION_FAILED - {other}"),
                ),
            };
            self.server_channel_close(
                channel,
                code,
                &text,
                exchange_method::CLASS_ID,
                exchange_method::Bind::METHOD_ID,
            )
            .await?;
            return Ok(Step::Continue);
        }
        if !bind.no_wait {
            self.send_method(channel, &Method::ExchangeBindOk(exchange_method::BindOk))
                .await?;
        }
        Ok(Step::Continue)
    }
    /// `handle_exchange_unbind` on the open connection.
    pub(in crate::connection) async fn handle_exchange_unbind(
        &mut self,
        channel: u16,
        unbind: exchange_method::Unbind,
    ) -> Result<Step, ConnError> {
        let vhost = self.vhost.clone().unwrap_or_else(|| "/".into());
        let _ = self.router.unbind_exchange(
            &vhost,
            unbind.source.as_str(),
            unbind.destination.as_str(),
            unbind.routing_key.as_str(),
        );
        if !unbind.no_wait {
            self.send_method(
                channel,
                &Method::ExchangeUnbindOk(exchange_method::UnbindOk),
            )
            .await?;
        }
        Ok(Step::Continue)
    }
}
