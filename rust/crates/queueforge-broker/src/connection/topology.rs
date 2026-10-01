//! Exchange and queue declare, bind, unbind, and delete.
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
    /// Returns `false` when the channel was closed for not being open.
    pub(in crate::connection) async fn require_open_channel(
        &mut self,
        channel: u16,
        class_id: u16,
        method_id: u16,
    ) -> Result<bool, ConnError> {
        if self.channels.contains_key(&channel) {
            return Ok(true);
        }
        self.server_channel_close(
            channel,
            REPLY_COMMAND_INVALID,
            "channel not open",
            class_id,
            method_id,
        )
        .await?;
        Ok(false)
    }

    /// `handle_exchange_declare` on the open connection.
    pub(in crate::connection) async fn handle_exchange_declare(
        &mut self,
        channel: u16,
        declare: exchange_method::Declare,
    ) -> Result<Step, ConnError> {
        let unknown: Vec<&str> = declare
            .arguments
            .entries
            .iter()
            .map(|(k, _)| k.as_str())
            .filter(|k| *k != "alternate-exchange")
            .collect();
        if !unknown.is_empty() {
            let keys = unknown;
            self.server_channel_close(
                channel,
                REPLY_PRECONDITION_FAILED,
                &format!(
                    "PRECONDITION_FAILED - unknown exchange argument(s): {}",
                    keys.join(", ")
                ),
                exchange_method::CLASS_ID,
                exchange_method::Declare::METHOD_ID,
            )
            .await?;
            return Ok(Step::Continue);
        }

        if declare.exchange.is_empty() {
            self.server_channel_close(
                channel,
                REPLY_ACCESS_REFUSED,
                "ACCESS_REFUSED - cannot declare the default exchange",
                exchange_method::CLASS_ID,
                exchange_method::Declare::METHOD_ID,
            )
            .await?;
            return Ok(Step::Continue);
        }

        let kind = match declare.kind.as_str() {
            "direct" => ExchangeType::Direct,
            "fanout" => ExchangeType::Fanout,
            "topic" => ExchangeType::Topic,
            "headers" => ExchangeType::Headers,
            other => {
                self.server_channel_close(
                    channel,
                    REPLY_PRECONDITION_FAILED,
                    &format!("PRECONDITION_FAILED - invalid exchange type '{other}'"),
                    exchange_method::CLASS_ID,
                    exchange_method::Declare::METHOD_ID,
                )
                .await?;
                return Ok(Step::Continue);
            }
        };

        // Exchange auto-delete (delete on last unbind) is not implemented yet —
        // refuse the flag so clients do not get a false durable-looking topology.
        if declare.auto_delete && !declare.passive {
            self.server_channel_close(
                channel,
                REPLY_PRECONDITION_FAILED,
                "PRECONDITION_FAILED - exchange auto_delete is not supported",
                exchange_method::CLASS_ID,
                exchange_method::Declare::METHOD_ID,
            )
            .await?;
            return Ok(Step::Continue);
        }

        let vhost = self.vhost.clone().unwrap_or_else(|| "/".into());
        let user = self.user.clone().unwrap_or_default();
        let exchange_name = declare.exchange.clone();
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
                    exchange_method::Declare::METHOD_ID,
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
                    exchange_method::Declare::METHOD_ID,
                )
                .await?;
                return Ok(Step::Continue);
            }
        }

        if declare.passive {
            let stored = {
                let store = Arc::clone(&self.store);
                let vhost_lookup = vhost.clone();
                let name = declare.exchange.clone();
                MetadataStore::blocking(store, move |s| s.get_exchange(&vhost_lookup, &name)).await
            };
            match self
                .router
                .get_exchange(&vhost, declare.exchange.as_str())
                .or_else(|| stored.ok().flatten())
            {
                Some(_) => {
                    if !declare.no_wait {
                        self.send_method(
                            channel,
                            &Method::ExchangeDeclareOk(exchange_method::DeclareOk),
                        )
                        .await?;
                    }
                    return Ok(Step::Continue);
                }
                None => {
                    self.server_channel_close(
                        channel,
                        REPLY_NOT_FOUND,
                        &format!(
                            "NOT_FOUND - no exchange '{}' in vhost '{vhost}'",
                            declare.exchange
                        ),
                        exchange_method::CLASS_ID,
                        exchange_method::Declare::METHOD_ID,
                    )
                    .await?;
                    return Ok(Step::Continue);
                }
            }
        }

        let new_ex = Exchange {
            vhost: CompactString::from(vhost.as_str()),
            name: CompactString::from(declare.exchange.as_str()),
            kind,
            durable: declare.durable,
            auto_delete: declare.auto_delete,
            internal: declare.internal,
            alternate: alternate_exchange_arg(&declare.arguments),
        };

        let stored_existing = {
            let store = Arc::clone(&self.store);
            let vhost_lookup = vhost.clone();
            let name = declare.exchange.clone();
            MetadataStore::blocking(store, move |s| s.get_exchange(&vhost_lookup, &name)).await
        };
        if let Some(existing) = self
            .router
            .get_exchange(&vhost, declare.exchange.as_str())
            .or_else(|| stored_existing.ok().flatten())
        {
            // Redeclare must match properties (including builtins).
            if existing.kind != new_ex.kind
                || existing.durable != new_ex.durable
                || existing.auto_delete != new_ex.auto_delete
                || existing.internal != new_ex.internal
            {
                self.server_channel_close(
                    channel,
                    REPLY_PRECONDITION_FAILED,
                    &format!(
                        "PRECONDITION_FAILED - exchange '{}' exists with different properties",
                        declare.exchange
                    ),
                    exchange_method::CLASS_ID,
                    exchange_method::Declare::METHOD_ID,
                )
                .await?;
                return Ok(Step::Continue);
            }
            // Ensure live table has it (e.g. loaded from store).
            self.router.put_exchange(existing);
            if !declare.no_wait {
                self.send_method(
                    channel,
                    &Method::ExchangeDeclareOk(exchange_method::DeclareOk),
                )
                .await?;
            }
            return Ok(Step::Continue);
        }

        // New user exchange.
        if new_ex.is_builtin() {
            // Should not reach: empty name rejected; amq.* should exist from bootstrap.
            self.server_channel_close(
                channel,
                REPLY_ACCESS_REFUSED,
                "ACCESS_REFUSED - cannot create builtin exchange name",
                exchange_method::CLASS_ID,
                exchange_method::Declare::METHOD_ID,
            )
            .await?;
            return Ok(Step::Continue);
        }

        if new_ex.durable {
            let store = Arc::clone(&self.store);
            let to_store = new_ex.clone();
            if let Err(e) =
                MetadataStore::blocking(store, move |s| s.create_exchange(&to_store)).await
            {
                let msg = e.to_string();
                self.server_channel_close(
                    channel,
                    REPLY_INTERNAL_ERROR,
                    &format!("INTERNAL_ERROR - {msg}"),
                    exchange_method::CLASS_ID,
                    exchange_method::Declare::METHOD_ID,
                )
                .await?;
                return Ok(Step::Continue);
            }
        }
        if let Some(cluster) = &self.cluster {
            cluster
                .replicate_json(
                    "exchange",
                    serde_json::to_value(&new_ex).unwrap_or(serde_json::Value::Null),
                )
                .await;
        }
        self.router.put_exchange(new_ex);

        if !declare.no_wait {
            self.send_method(
                channel,
                &Method::ExchangeDeclareOk(exchange_method::DeclareOk),
            )
            .await?;
        }
        Ok(Step::Continue)
    }

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
            cluster.declare_queue(&vhost, queue_name.as_str(), opts).await
        } else {
            self.queues.declare(&vhost, queue_name.as_str(), opts).await
        };
        match declared {
            Ok(result) => {
                queueforge_core::prom::queue_declared(!declare.passive && !already);
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
    pub(in crate::connection) async fn on_open_connection_method(&mut self, method: Method) -> Result<Step, ConnError> {
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
        self.connections.sync_channels(id, &user, &vhost, self.peer, &numbers);
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
