//! Channel checks and exchange.declare.

use super::*;

use std::sync::Arc;

use compact_str::CompactString;
use queueforge_amqp::exchange as exchange_method;
use queueforge_amqp::Method;
use queueforge_auth::{PermissionKind, ResourceKind};
use queueforge_core::{Exchange, ExchangeType};
use tokio::io::{AsyncRead, AsyncWrite};

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
        // A passive declare only checks existence. Type, arguments and
        // permissions are not looked at, as in RabbitMQ.
        if declare.passive {
            let vhost = self.vhost.clone().unwrap_or_else(|| "/".into());
            if declare.exchange.is_empty() {
                if !declare.no_wait {
                    self.send_method(channel, &Method::ExchangeDeclareOk(exchange_method::DeclareOk)).await?;
                }
                return Ok(Step::Continue);
            }
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

        let unknown: Vec<&str> = declare
            .arguments
            .entries
            .iter()
            .map(|(k, _)| k.as_str())
            .filter(|k| *k != "alternate-exchange" && *k != "x-delayed-type")
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

        let kind = match ExchangeType::parse(declare.kind.as_str()) {
            Some(kind) => kind,
            None => {
                let other = declare.kind.as_str();
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

        let new_ex = Exchange {
            vhost: CompactString::from(vhost.as_str()),
            name: CompactString::from(declare.exchange.as_str()),
            kind,
            durable: declare.durable,
            auto_delete: declare.auto_delete,
            internal: declare.internal,
            alternate: alternate_exchange_arg(&declare.arguments),
            delayed_type: None,
        };
        // x-delayed-message routes as its x-delayed-type once a delay ends.
        let mut new_ex = new_ex;
        if kind == ExchangeType::Delayed {
            let delayed = match declare.arguments.get("x-delayed-type") {
                Some(queueforge_amqp::FieldValue::LongString(s)) => ExchangeType::parse(&String::from_utf8_lossy(s)),
                _ => None,
            };
            match delayed {
                Some(t) if !matches!(t, ExchangeType::Delayed | ExchangeType::Default) => new_ex.delayed_type = Some(t),
                _ => {
                    self.server_channel_close(
                        channel,
                        REPLY_PRECONDITION_FAILED,
                        "PRECONDITION_FAILED - Invalid argument, 'x-delayed-type' must be an existing exchange type",
                        exchange_method::CLASS_ID,
                        exchange_method::Declare::METHOD_ID,
                    )
                    .await?;
                    return Ok(Step::Continue);
                }
            }
        }

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
        self.emit_event(
            "exchange.created",
            &vhost,
            vec![
                ("name", queueforge_core::AppHeaderValue::Str(new_ex.name.to_string())),
                ("type", queueforge_core::AppHeaderValue::Str(new_ex.kind.as_str().to_string())),
                ("durable", queueforge_core::AppHeaderValue::Bool(new_ex.durable)),
                ("internal", queueforge_core::AppHeaderValue::Bool(new_ex.internal)),
            ],
        );
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
}
