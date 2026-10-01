//! basic.publish, content frames, returns, and publisher confirms.
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
    /// `handle_content_frame` on the open connection.
    pub(in crate::connection) async fn handle_content_frame(&mut self, frame: Frame) -> Result<Step, ConnError> {
        let ch = frame.channel;
        if ch == 0 || !self.channels.contains_key(&ch) {
            if ch != 0 {
                self.server_channel_close(ch, REPLY_COMMAND_INVALID, "channel not open", 0, 0)
                    .await?;
            }
            return Ok(Step::Continue);
        }

        match frame.kind {
            FrameType::Header => {
                let header = match ContentHeader::decode(&frame.payload) {
                    Ok(h) => h,
                    Err(e) => {
                        self.server_channel_close(
                            ch,
                            REPLY_COMMAND_INVALID,
                            &format!("invalid content header: {e}"),
                            60,
                            0,
                        )
                        .await?;
                        return Ok(Step::Continue);
                    }
                };
                let max_msg = self.params.max_message_bytes;
                let header_bytes = properties_header_bytes(&header.properties);
                if header.body_size.saturating_add(header_bytes) > max_msg {
                    self.server_channel_close(
                        ch,
                        REPLY_PRECONDITION_FAILED,
                        &format!(
                            "PRECONDITION_FAILED - message size {} exceeds max {}",
                            header.body_size.saturating_add(header_bytes),
                            max_msg
                        ),
                        60,
                        0,
                    )
                    .await?;
                    // Drop in-flight publish assembly if any.
                    if let Some(ch_state) = self.channels.get_mut(&ch) {
                        ch_state.publish = None;
                    }
                    return Ok(Step::Continue);
                }

                let Some(ch_state) = self.channels.get_mut(&ch) else {
                    return Ok(Step::Continue);
                };
                match ch_state.publish.take() {
                    Some(PublishAssemble::ExpectHeader { publish }) => {
                        if header.body_size == 0 {
                            // Complete publish with empty body.
                            return self
                                .finish_publish(ch, publish, header.properties, Bytes::new())
                                .await;
                        }
                        ch_state.publish = Some(PublishAssemble::ExpectBody {
                            publish,
                            properties: Box::new(header.properties),
                            body_size: header.body_size,
                            body: Vec::with_capacity(header.body_size.min(1024 * 1024) as usize),
                        });
                        Ok(Step::Continue)
                    }
                    other => {
                        ch_state.publish = other;
                        self.server_channel_close(
                            ch,
                            REPLY_COMMAND_INVALID,
                            "unexpected content header",
                            60,
                            0,
                        )
                        .await?;
                        Ok(Step::Continue)
                    }
                }
            }
            FrameType::Body => {
                let (done_publish, props, body) = {
                    let Some(ch_state) = self.channels.get_mut(&ch) else {
                        return Ok(Step::Continue);
                    };
                    match ch_state.publish.take() {
                        Some(PublishAssemble::ExpectBody {
                            publish,
                            properties,
                            body_size,
                            mut body,
                        }) => {
                            body.extend_from_slice(&frame.payload);
                            if (body.len() as u64) < body_size {
                                ch_state.publish = Some(PublishAssemble::ExpectBody {
                                    publish,
                                    properties,
                                    body_size,
                                    body,
                                });
                                return Ok(Step::Continue);
                            }
                            if (body.len() as u64) > body_size {
                                self.server_channel_close(
                                    ch,
                                    REPLY_COMMAND_INVALID,
                                    "body larger than content header size",
                                    60,
                                    0,
                                )
                                .await?;
                                return Ok(Step::Continue);
                            }
                            (publish, *properties, Bytes::from(body))
                        }
                        other => {
                            ch_state.publish = other;
                            self.server_channel_close(
                                ch,
                                REPLY_COMMAND_INVALID,
                                "unexpected content body",
                                60,
                                0,
                            )
                            .await?;
                            return Ok(Step::Continue);
                        }
                    }
                };
                self.finish_publish(ch, done_publish, props, body).await
            }
            _ => Ok(Step::Continue),
        }
    }

    /// `in_tx` on the open connection.
    pub(in crate::connection) fn in_tx(&self, channel: u16) -> bool {
        !self.tx_applying && self.channels.get(&channel).is_some_and(|ch| ch.tx_mode)
    }

    /// `finish_publish` on the open connection.
    pub(in crate::connection) async fn finish_publish(
        &mut self,
        channel: u16,
        publish: basic_method::Publish,
        properties: BasicProperties,
        body: Bytes,
    ) -> Result<Step, ConnError> {
        if !self.tx_applying
            && self
                .channels
                .get(&channel)
                .is_some_and(|ch| ch.tx_mode)
        {
                if let Some(ch) = self.channels.get_mut(&channel) {
                    ch.tx_ops.push(TxOp::Publish {
                        publish,
                        properties: Box::new(properties),
                        body,
                    });
                }
                return Ok(Step::Continue);
        }
        // Publisher-confirm sequence is assigned once content is fully received,
        // for every publish on a confirm-mode channel (success or failure path).
        let confirm_seq = self
            .channels
            .get_mut(&channel)
            .and_then(ChannelState::take_publish_seq);

        if publish.immediate {
            self.server_channel_close(
                channel,
                REPLY_NOT_IMPLEMENTED,
                "NOT_IMPLEMENTED - immediate=true",
                basic_method::CLASS_ID,
                basic_method::Publish::METHOD_ID,
            )
            .await?;
            return Ok(Step::Continue);
        }

        let vhost = self.vhost.clone().unwrap_or_else(|| "/".into());
        let user = self.user.clone().unwrap_or_default();
        let exchange_name = publish.exchange.clone();

        // Write permission on the exchange (default `""` → `amq.default`).
        let user_auth = user.clone();
        let vhost_auth = vhost.clone();
        let exchange_auth = exchange_name.clone();
        match self
            .auth_bool(move |auth| {
                auth.check_permission(
                    &user_auth,
                    &vhost_auth,
                    &exchange_auth,
                    ResourceKind::Exchange,
                    PermissionKind::Write,
                )
            })
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                self.server_channel_close(
                    channel,
                    REPLY_ACCESS_REFUSED,
                    "ACCESS_REFUSED - write access to exchange refused",
                    basic_method::CLASS_ID,
                    basic_method::Publish::METHOD_ID,
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
                    basic_method::Publish::METHOD_ID,
                )
                .await?;
                return Ok(Step::Continue);
            }
        }

        queueforge_core::prom::message_received(confirm_seq.is_some());

        // Ensure named exchanges known only from durable meta are in the live table.
        if !exchange_name.is_empty() && self.router.get_exchange(&vhost, &exchange_name).is_none() {
            let store = Arc::clone(&self.store);
            let vhost_lookup = vhost.clone();
            let name = exchange_name.clone();
            if let Some(ex) =
                MetadataStore::blocking(store, move |s| s.get_exchange(&vhost_lookup, &name))
                    .await
                    .ok()
                    .flatten()
            {
                self.router.put_exchange(ex);
            }
        }

        let header_args = field_table_to_header_args(properties.headers.as_ref().unwrap_or(&FieldTable::default()));
        let mut route = match self.router.route_publish(
            &vhost,
            &exchange_name,
            publish.routing_key.as_str(),
            &header_args,
        ) {
            Ok(r) => r,
            Err(CoreError::NotFound(msg)) => {
                self.server_channel_close(
                    channel,
                    REPLY_NOT_FOUND,
                    &format!("NOT_FOUND - {msg}"),
                    basic_method::CLASS_ID,
                    basic_method::Publish::METHOD_ID,
                )
                .await?;
                return Ok(Step::Continue);
            }
            Err(err) => {
                let (code, text) = core_error_to_amqp(&err);
                self.server_channel_close(
                    channel,
                    code,
                    &text,
                    basic_method::CLASS_ID,
                    basic_method::Publish::METHOD_ID,
                )
                .await?;
                return Ok(Step::Continue);
            }
        };
        if let Some(table) = properties.headers.as_ref() {
            for name in ["CC", "BCC"] {
                for key in routing_header_keys(table, name) {
                    if key == publish.routing_key {
                        continue;
                    }
                    let Ok(more) = self.router.route_publish(&vhost, &exchange_name, &key, &header_args) else {
                        continue;
                    };
                    for dest in more.destinations {
                        if !route.destinations.iter().any(|existing| existing == &dest) {
                            route.destinations.push(dest);
                        }
                    }
                }
            }
        }

        for downstream in queueforge_core::federation::federation_targets(&vhost, &exchange_name) {
            let Ok(extra) = self.router.route_publish(&downstream, &exchange_name, publish.routing_key.as_str(), &header_args) else {
                continue;
            };
            for dest in extra.destinations {
                if !route.destinations.iter().any(|existing| existing == &dest) {
                    route.destinations.push(dest);
                }
            }
        }

        // Default exchange: missing queue is 404 (not basic.return).
        let (destinations, mut dest_failures): (Vec<QueueHandle>, u32) =
            if route.kind == ExchangeType::Default {
                let key = &route.destinations[0];
                match self.resolve_queue(key).await {
                    Some(h) if h.is_available() => (vec![h], 0),
                    Some(_) => {
                        self.server_channel_close(
                            channel,
                            REPLY_INTERNAL_ERROR,
                            "INTERNAL_ERROR - queue actor unavailable",
                            basic_method::CLASS_ID,
                            basic_method::Publish::METHOD_ID,
                        )
                        .await?;
                        return Ok(Step::Continue);
                    }
                    None => {
                        self.server_channel_close(
                            channel,
                            REPLY_NOT_FOUND,
                            &format!("NOT_FOUND - no queue '{}' in vhost '{vhost}'", key.name),
                            basic_method::CLASS_ID,
                            basic_method::Publish::METHOD_ID,
                        )
                        .await?;
                        return Ok(Step::Continue);
                    }
                }
            } else {
                // Named exchange: every routed dest must be live. Missing or
                // unavailable targets count as failures (design multi-dest wait-all).
                let mut handles = Vec::new();
                let mut failures = 0u32;
                for key in &route.destinations {
                    match self.resolve_queue(key).await {
                        Some(h) if h.is_available() => handles.push(h),
                        Some(_) | None => {
                            failures = failures.saturating_add(1);
                        }
                    }
                }
                (handles, failures)
            };

        // Zero routes from the index → unroutable (mandatory return / drop).
        // With confirms: still basic.ack after return/drop (message was handled).
        if route.destinations.is_empty() {
            queueforge_core::prom::message_unroutable(publish.mandatory);
            if publish.mandatory {
                self.send_basic_return(
                    channel,
                    REPLY_NO_ROUTE,
                    "NO_ROUTE",
                    publish.exchange.as_str(),
                    publish.routing_key.as_str(),
                    &properties,
                    &body,
                )
                .await?;
            } else {
                metrics::counter!(
                    "queueforge_publish_unroutable_total",
                    "vhost" => vhost.clone(),
                    "exchange" => exchange_name.to_string()
                )
                .increment(1);
            }
            if let Some(seq) = confirm_seq {
                self.send_publisher_confirm(channel, seq, true).await?;
            }
            return Ok(Step::Continue);
        }

        queueforge_core::prom::message_routed(route.destinations.len() as u64);

        // Routed N>0 but none live → partial failure (not silent unroutable).
        if destinations.is_empty() {
            metrics::counter!("queueforge_publish_partial_failure_total").increment(1);
            return self.publish_partial_failure(channel, confirm_seq).await;
        }

        let msg = Arc::new(Message {
            exchange: CompactString::from(if exchange_name.is_empty() {
                DEFAULT_EXCHANGE_NAME
            } else {
                exchange_name.as_str()
            }),
            routing_key: CompactString::from(publish.routing_key.as_str()),
            body,
            persistent: properties.is_persistent(),
            redelivered: false,
            content_type: properties.content_type.as_deref().map(CompactString::from),
            content_encoding: properties
                .content_encoding
                .as_deref()
                .map(CompactString::from),
            correlation_id: properties
                .correlation_id
                .as_deref()
                .map(CompactString::from),
            message_id: properties.message_id.as_deref().map(CompactString::from),
            reply_to: properties.reply_to.as_deref().map(CompactString::from),
            expiration: properties.expiration.as_deref().map(CompactString::from),
            app_id: properties.app_id.as_deref().map(CompactString::from),
            user_id: properties.user_id.as_deref().map(CompactString::from),
            type_: properties.type_.as_deref().map(CompactString::from),
            priority: properties.priority,
            timestamp: properties.timestamp,
            expires_unix_ms: None,
            headers: queueforge_core::MessageHeaders {
                app: properties
                    .headers
                    .as_ref()
                    .map(field_table_to_app_headers)
                    .unwrap_or_default()
                    .into_iter()
                    .filter(|(key, _)| key != "BCC")
                    .collect(),
                ..queueforge_core::MessageHeaders::default()
            },
        });

        let all_quorum = destinations.iter().all(|handle| {
            handle.info.args.lock().unwrap_or_else(|err| err.into_inner()).queue_type == Some(QueueType::Quorum)
        });
        if all_quorum {
            if let Some(cluster) = &self.cluster {
                let mut failed = false;
                for handle in &destinations {
                    if cluster.quorum_enqueue(&handle.info.key, Arc::clone(&msg)).await.is_err() {
                        failed = true;
                    }
                }
                if failed {
                    return self.publish_partial_failure(channel, confirm_seq).await;
                }
                if let Some(seq) = confirm_seq {
                    self.send_publisher_confirm(channel, seq, true).await?;
                }
                return Ok(Step::Continue);
            }
        }

        // Multi-destination wait-all: enqueue to every live dest, then await all
        // EnqueueCompletion.durable_done (non-transactional).
        // Single-dest overflow `reject-publish` → 406 PRECONDITION_FAILED.
        // Resource (memory/disk): if *no* dest succeeded → 506 RESOURCE_ERROR.
        // True partial multi-dest: confirms on → basic.nack; confirms off → channel 541.
        let mut completions = Vec::with_capacity(destinations.len());
        let mut precondition_msgs: Vec<String> = Vec::new();
        let mut other_failures = 0u32;
        let mut resource_err: Option<CoreError> = None;
        let mut success_enqueues: u32 = 0;
        for handle in &destinations {
            let (reply_tx, reply_rx) = oneshot::channel();
            if handle
                .tx
                .send(QueueCmd::Enqueue {
                    msg: Arc::clone(&msg),
                    reply: reply_tx,
                })
                .await
                .is_err()
            {
                dest_failures = dest_failures.saturating_add(1);
                other_failures = other_failures.saturating_add(1);
                continue;
            }
            match reply_rx.await {
                Ok(Ok(completion)) => {
                    success_enqueues = success_enqueues.saturating_add(1);
                    completions.push(completion);
                }
                Ok(Err(CoreError::PreconditionFailed(msg))) => {
                    dest_failures = dest_failures.saturating_add(1);
                    precondition_msgs.push(msg);
                }
                Ok(Err(e)) => {
                    if matches!(e, CoreError::Resource(_)) {
                        if resource_err.is_none() {
                            resource_err = Some(e);
                        }
                    } else {
                        dest_failures = dest_failures.saturating_add(1);
                        other_failures = other_failures.saturating_add(1);
                    }
                }
                Err(_) => {
                    dest_failures = dest_failures.saturating_add(1);
                    other_failures = other_failures.saturating_add(1);
                }
            }
        }

        // Always complete durable_done wait-all for enqueues that were accepted.
        for completion in completions {
            match completion.durable_done.await {
                Ok(Ok(())) => {}
                // oneshot cancel (actor death) is a durable failure — not success.
                Err(_) => {
                    warn!("durable_done oneshot canceled (queue actor gone?)");
                    dest_failures = dest_failures.saturating_add(1);
                    other_failures = other_failures.saturating_add(1);
                }
                Ok(Err(e)) => {
                    warn!(error = %e, "durable_done error on enqueue");
                    dest_failures = dest_failures.saturating_add(1);
                    other_failures = other_failures.saturating_add(1);
                }
            }
        }

        // Pure resource refusal (no dest accepted the message) → 506.
        if let Some(err) = resource_err {
            if success_enqueues == 0 && dest_failures == 0 {
                let (code, text) = core_error_to_amqp(&err);
                self.server_channel_close(
                    channel,
                    code,
                    &text,
                    basic_method::CLASS_ID,
                    basic_method::Publish::METHOD_ID,
                )
                .await?;
                return Ok(Step::Continue);
            }
            // Mixed: treat resource like any other dest failure for 541 path.
            dest_failures = dest_failures.saturating_add(1);
        }

        if dest_failures > 0 {
            // Pure capacity reject (x-overflow=reject-publish) → 406, even for
            // multi-dest when every failure was PreconditionFailed and no other errors.
            let pure_precondition = other_failures == 0
                && !precondition_msgs.is_empty()
                && dest_failures == precondition_msgs.len() as u32;
            // Single live dest + precondition is the common reject-publish case.
            let single_reject = destinations.len() == 1
                && other_failures == 0
                && precondition_msgs.len() == 1
                && dest_failures == 1;

            if pure_precondition || single_reject {
                if let Some(seq) = confirm_seq {
                    self.send_publisher_confirm(channel, seq, false).await?;
                    return Ok(Step::Continue);
                }
                let text = precondition_msgs
                    .first()
                    .map(|m| format!("PRECONDITION_FAILED - {m}"))
                    .unwrap_or_else(|| {
                        "PRECONDITION_FAILED - message rejected as queue length limit is reached"
                            .into()
                    });
                self.server_channel_close(
                    channel,
                    REPLY_PRECONDITION_FAILED,
                    &text,
                    basic_method::CLASS_ID,
                    basic_method::Publish::METHOD_ID,
                )
                .await?;
                return Ok(Step::Continue);
            }

            metrics::counter!("queueforge_publish_partial_failure_total").increment(1);
            // Confirms on → basic.nack; confirms off → channel 541.
            return self.publish_partial_failure(channel, confirm_seq).await;
        }

        metrics::counter!(
            "queueforge_publish_total",
            "vhost" => vhost.clone(),
            "exchange" => exchange_name.to_string()
        )
        .increment(1);

        if let Some(seq) = confirm_seq {
            self.send_publisher_confirm(channel, seq, true).await?;
        }
        Ok(Step::Continue)
    }

    /// Partial multi-destination publish failure.
    ///
    /// Confirms on → `basic.nack` for the publish sequence (channel stays open).
    /// Confirms off → channel exception 541.
    pub(in crate::connection) async fn publish_partial_failure(
        &mut self,
        channel: u16,
        confirm_seq: Option<u64>,
    ) -> Result<Step, ConnError> {
        if let Some(seq) = confirm_seq {
            self.send_publisher_confirm(channel, seq, false).await?;
            return Ok(Step::Continue);
        }
        self.server_channel_close(
            channel,
            REPLY_INTERNAL_ERROR,
            "INTERNAL_ERROR - partial multi-destination publish failure",
            basic_method::CLASS_ID,
            basic_method::Publish::METHOD_ID,
        )
        .await?;
        Ok(Step::Continue)
    }

    /// Map `EnqueueCompletion` outcome onto publisher `basic.ack` / `basic.nack`.
    pub(in crate::connection) async fn send_publisher_confirm(
        &mut self,
        channel: u16,
        delivery_tag: u64,
        ok: bool,
    ) -> Result<(), ConnError> {
        if ok {
            queueforge_core::prom::message_confirmed();
            self.send_method(
                channel,
                &Method::BasicAck(basic_method::Ack {
                    delivery_tag,
                    multiple: false,
                }),
            )
            .await
        } else {
            self.send_method(
                channel,
                &Method::BasicNack(basic_method::Nack {
                    delivery_tag,
                    multiple: false,
                    requeue: false,
                }),
            )
            .await
        }
    }

    /// `handle_basic_publish` on the open connection.
    pub(in crate::connection) async fn handle_basic_publish(
        &mut self,
        channel: u16,
        publish: basic_method::Publish,
    ) -> Result<Step, ConnError> {
        let Some(ch_state) = self.channels.get_mut(&channel) else {
            return Ok(Step::Continue);
        };
        if ch_state.publish.is_some() {
            self.server_channel_close(
                channel,
                REPLY_COMMAND_INVALID,
                "publish already in progress on channel",
                basic_method::CLASS_ID,
                basic_method::Publish::METHOD_ID,
            )
            .await?;
            return Ok(Step::Continue);
        }
        let user = self.user.clone().unwrap_or_default();
        let vhost = self.vhost.clone().unwrap_or_default();
        if !self.connections.topic_write_allowed(&user, &vhost, &publish.exchange, &publish.routing_key) {
            self.server_channel_close(
                channel,
                REPLY_ACCESS_REFUSED,
                "ACCESS_REFUSED - topic permission write pattern",
                basic_method::CLASS_ID,
                basic_method::Publish::METHOD_ID,
            )
            .await?;
            return Ok(Step::Continue);
        }
        ch_state.publish = Some(PublishAssemble::ExpectHeader { publish });
        Ok(Step::Continue)
    }

    /// `send_basic_return` on the open connection.
    pub(in crate::connection) async fn send_basic_return(
        &mut self,
        channel: u16,
        reply_code: u16,
        reply_text: &str,
        exchange: &str,
        routing_key: &str,
        properties: &BasicProperties,
        body: &Bytes,
    ) -> Result<Step, ConnError> {
        self.send_method(
            channel,
            &Method::BasicReturn(basic_method::Return {
                reply_code,
                reply_text: reply_text.to_string(),
                exchange: exchange.to_string(),
                routing_key: routing_key.to_string(),
            }),
        )
        .await?;
        self.send_content(channel, properties, body).await?;
        metrics::counter!(
            "queueforge_publish_unroutable_total",
            "vhost" => self.vhost.clone().unwrap_or_else(|| "/".into()),
            "exchange" => exchange.to_string()
        )
        .increment(1);
        Ok(Step::Continue)
    }
}
