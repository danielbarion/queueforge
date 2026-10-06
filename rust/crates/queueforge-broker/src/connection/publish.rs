//! Finish a basic.publish after the body has been read.

use super::*;

use std::sync::Arc;

use bytes::Bytes;
use compact_str::CompactString;
use queueforge_amqp::{basic as basic_method, BasicProperties, FieldTable};
use queueforge_auth::{PermissionKind, ResourceKind};
use queueforge_core::{
    Error as CoreError, ExchangeType, Message, QueueCmd, QueueHandle, QueueType,
    DEFAULT_EXCHANGE_NAME,
};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::oneshot;
use tracing::warn;

impl<'a, S> Connection<'a, S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// Counter for publishes to `exchange` on this connection. The handle is
    /// built once so a confirm burst does not allocate label strings.
    fn cached_publish_counter(&mut self, exchange: &str) -> metrics::Counter {
        if let Some(counter) = self.publish_counters.get(exchange) {
            return counter.clone();
        }
        let vhost = self.vhost.clone().unwrap_or_else(|| "/".to_string());
        let counter = metrics::counter!(
            "queueforge_publish_total",
            "vhost" => vhost,
            "exchange" => exchange.to_string()
        );
        self.publish_counters
            .insert(exchange.to_string(), counter.clone());
        counter
    }

    /// `finish_publish` on the open connection.
    pub(in crate::connection) async fn finish_publish(
        &mut self,
        channel: u16,
        publish: basic_method::Publish,
        properties: BasicProperties,
        body: Bytes,
    ) -> Result<Step, ConnError> {
        if !self.tx_applying && self.channels.get(&channel).is_some_and(|ch| ch.tx_mode) {
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
        let exchange_key = exchange_name.to_string();
        let allowed = if let Some(allowed) = self.exchange_write_ok.get(&exchange_key).copied() {
            allowed
        } else {
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
                Ok(allowed) => {
                    self.exchange_write_ok.insert(exchange_key, allowed);
                    allowed
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
        };
        if !allowed {
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

        let header_args = field_table_to_header_args(
            properties
                .headers
                .as_ref()
                .unwrap_or(&FieldTable::default()),
        );
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
                    let Ok(more) =
                        self.router
                            .route_publish(&vhost, &exchange_name, &key, &header_args)
                    else {
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
            let Ok(extra) = self.router.route_publish(
                &downstream,
                &exchange_name,
                publish.routing_key.as_str(),
                &header_args,
            ) else {
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
            handle
                .info
                .args
                .lock()
                .unwrap_or_else(|err| err.into_inner())
                .queue_type
                == Some(QueueType::Quorum)
        });
        if all_quorum {
            if let Some(cluster) = &self.cluster {
                // The read loop must accept the next publish while this majority
                // fsync is still running. tx.commit still waits inline.
                if let Some(seq) = confirm_seq {
                    if !self.tx_applying {
                        let cluster = Arc::clone(cluster);
                        let keys: Vec<_> = destinations
                            .iter()
                            .map(|handle| handle.info.key.clone())
                            .collect();
                        let msg = Arc::clone(&msg);
                        let tx = self.confirm_tx.clone();
                        tokio::spawn(async move {
                            let mut failed = false;
                            for key in &keys {
                                if cluster.quorum_enqueue(key, Arc::clone(&msg)).await.is_err() {
                                    failed = true;
                                }
                            }
                            let _ = tx.send(DeferredConfirm {
                                channel,
                                delivery_tag: seq,
                                ok: !failed,
                            });
                        });
                        return Ok(Step::Continue);
                    }
                }
                let mut failed = false;
                for handle in &destinations {
                    if cluster
                        .quorum_enqueue(&handle.info.key, Arc::clone(&msg))
                        .await
                        .is_err()
                    {
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

        // Confirms: hand the publish to the queue and keep reading. The actor
        // appends while the next frame is parsed. The ack waits for that
        // append's covering fsync, off this loop.
        if confirm_seq.is_some() && !self.tx_applying {
            let seq = confirm_seq.expect("confirm seq");
            let mut waits = Vec::with_capacity(destinations.len());
            let mut send_failures = 0u32;
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
                    send_failures = send_failures.saturating_add(1);
                    continue;
                }
                waits.push(reply_rx);
            }
            let counter = self.cached_publish_counter(&exchange_name);
            if self
                .durable_tx
                .send(DurableWait {
                    channel,
                    seq,
                    waits,
                    send_failures,
                    counter,
                })
                .is_err()
            {
                let _ = self.confirm_tx.send(DeferredConfirm {
                    channel,
                    delivery_tag: seq,
                    ok: false,
                });
            }
            return Ok(Step::Continue);
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

        // Transactions and publishes without confirms wait here. Confirm-mode
        // publishes returned above so the next frame can join this flush.
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
}
