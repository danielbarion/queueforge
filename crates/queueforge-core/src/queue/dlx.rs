//! Dead-letter routing: `x-death` headers, hop/cycle guard, multi-dest enqueue.

use std::sync::{Arc, Weak};
use std::time::{SystemTime, UNIX_EPOCH};

use compact_str::CompactString;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use tracing::{debug, warn};

use super::cmd::{Message, MessageHeaders, QueueCmd};
use super::registry::{QueueKey, QueueRegistry};
use crate::error::{Error, Result};
use crate::router::ExchangeRouter;

/// Why a message was dead-lettered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DeathReason {
    /// Ready-state message TTL expired.
    Expired,
    /// `basic.nack` / `basic.reject` with `requeue=false`.
    Rejected,
    /// Dropped by max-length overflow (`drop-head`).
    Maxlen,
}

impl DeathReason {
    /// Wire / header string form.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Expired => "expired",
            Self::Rejected => "rejected",
            Self::Maxlen => "maxlen",
        }
    }

    /// Parse from header string.
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "expired" => Some(Self::Expired),
            "rejected" => Some(Self::Rejected),
            "maxlen" => Some(Self::Maxlen),
            _ => None,
        }
    }
}

/// One entry in the `x-death` header array.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeathEntry {
    /// Queue that dead-lettered the message.
    pub queue: CompactString,
    /// Death reason.
    pub reason: DeathReason,
    /// Unix timestamp (seconds) when dead-lettered.
    pub time: u64,
    /// Exchange the message was published to when it entered this queue.
    pub exchange: CompactString,
    /// Routing keys (typically the message RK at death).
    pub routing_keys: Vec<CompactString>,
    /// How many times this queue+reason pair has dead-lettered the message.
    pub count: u64,
}

/// Outcome of preparing a dead-lettered message.
#[derive(Debug)]
pub enum PrepareDeath {
    /// Message ready to publish to the DLX (headers updated).
    Ready(Arc<Message>),
    /// Dropped due to hop limit or cycle detection.
    CycleDrop,
}

/// Total death hops = sum of `count` across `x-death` entries.
pub fn death_hop_count(headers: &MessageHeaders) -> u64 {
    headers.deaths.iter().map(|d| d.count).sum()
}

/// Whether another death hop would exceed `max_hops`.
pub fn should_drop_for_cycle(headers: &MessageHeaders, _queue: &str, max_hops: u32) -> bool {
    // Hop guard: drop before recording another death when already at the limit.
    death_hop_count(headers) >= u64::from(max_hops)
}

/// Whether publishing into `dest_queue` would form a DLX cycle (queue already in `x-death`).
pub fn is_cycle_destination(headers: &MessageHeaders, dest_queue: &str) -> bool {
    headers
        .deaths
        .iter()
        .any(|d| d.queue.as_str() == dest_queue)
}

/// Append / update `x-death` on a clone of `msg` for dead-lettering from `queue`.
///
/// Returns [`PrepareDeath::CycleDrop`] when the hop guard fires.
pub fn prepare_dead_letter(
    msg: &Message,
    queue: &str,
    reason: DeathReason,
    max_hops: u32,
) -> PrepareDeath {
    if should_drop_for_cycle(&msg.headers, queue, max_hops) {
        metrics::counter!("queueforge_dlx_cycle_drop_total").increment(1);
        return PrepareDeath::CycleDrop;
    }

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    let mut headers = msg.headers.clone();
    // Merge into existing same queue+reason entry, else push.
    if let Some(entry) = headers
        .deaths
        .iter_mut()
        .find(|d| d.queue.as_str() == queue && d.reason == reason)
    {
        entry.count = entry.count.saturating_add(1);
        entry.time = now;
        entry.exchange = msg.exchange.clone();
        entry.routing_keys = vec![msg.routing_key.clone()];
    } else {
        headers.deaths.push(DeathEntry {
            queue: CompactString::from(queue),
            reason,
            time: now,
            exchange: msg.exchange.clone(),
            routing_keys: vec![msg.routing_key.clone()],
            count: 1,
        });
    }

    if headers.first_death_reason.is_none() {
        headers.first_death_reason = Some(reason);
        headers.first_death_queue = Some(CompactString::from(queue));
        headers.first_death_exchange = Some(msg.exchange.clone());
    }

    let mut new_msg = msg.clone();
    new_msg.headers = headers;
    // Clear absolute expiry; destination recomputes from its own TTL + property.
    // Keep AMQP expiration property string for destination min(queue, per-msg).
    new_msg.redelivered = false;
    PrepareDeath::Ready(Arc::new(new_msg))
}

/// Routes dead-lettered messages through the exchange router into destination queues.
pub struct DlxRouter {
    exchange_router: Arc<ExchangeRouter>,
    registry: Weak<QueueRegistry>,
}

impl DlxRouter {
    /// Create a DLX router bound to `registry` (weak to avoid cycles).
    pub fn new(exchange_router: Arc<ExchangeRouter>, registry: Weak<QueueRegistry>) -> Self {
        Self {
            exchange_router,
            registry,
        }
    }

    /// Publish `msg` to `exchange` / `routing_key` in `vhost`.
    ///
    /// - **All destinations cycle-filtered** → [`DlxPublishResult::AllCycle`]
    /// - **≥1 dest accepted** (even if others fail) → `Full` / `Partial` (source must free;
    ///   requeue would duplicate)
    /// - **Zero dest accepted** → `Err` (caller may requeue source)
    ///
    /// Safe to call from a background task (not from inside a queue actor that
    /// may itself be a DLX destination — see actor `schedule_dead_letter`).
    pub async fn publish(
        &self,
        vhost: &str,
        exchange: &str,
        routing_key: &str,
        msg: Arc<Message>,
    ) -> Result<DlxPublishResult> {
        let registry = self
            .registry
            .upgrade()
            .ok_or_else(|| Error::Unavailable("queue registry gone; cannot dead-letter".into()))?;

        let route = self.exchange_router.route(vhost, exchange, routing_key)?;
        if route.destinations.is_empty() {
            return Err(Error::NotFound(format!(
                "DLX {vhost}/{exchange} has no routes for rk={routing_key}"
            )));
        }

        // Filter cycle destinations (queue already in x-death).
        let mut live = Vec::with_capacity(route.destinations.len());
        let mut cycles = 0u32;
        for dest in &route.destinations {
            if is_cycle_destination(&msg.headers, dest.name.as_str()) {
                cycles = cycles.saturating_add(1);
                continue;
            }
            live.push(dest.clone());
        }
        if live.is_empty() {
            metrics::counter!("queueforge_dlx_cycle_drop_total").increment(1);
            return Ok(DlxPublishResult::AllCycle);
        }
        if cycles > 0 {
            metrics::counter!("queueforge_dlx_cycle_drop_total").increment(u64::from(cycles));
        }

        let mut succeeded = 0u32;
        let mut last_err: Option<Error> = None;
        let mut completions = Vec::new();

        for dest in &live {
            let handle = match registry.get(dest) {
                Some(h) if h.is_available() => h,
                Some(_) => {
                    last_err = Some(Error::Unavailable(format!(
                        "DLX destination queue {dest} unavailable"
                    )));
                    continue;
                }
                None => {
                    last_err = Some(Error::NotFound(format!(
                        "DLX destination queue {dest} not found"
                    )));
                    continue;
                }
            };
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
                last_err = Some(Error::Unavailable(format!(
                    "DLX destination queue {dest} mailbox closed"
                )));
                continue;
            }
            match reply_rx.await {
                Ok(Ok(c)) => completions.push(c),
                Ok(Err(e)) => {
                    last_err = Some(e);
                }
                Err(_) => {
                    last_err = Some(Error::Unavailable(format!(
                        "DLX destination queue {dest} dropped reply"
                    )));
                }
            }
        }

        for c in completions {
            match c.durable_done.await {
                Ok(Ok(())) => {
                    succeeded = succeeded.saturating_add(1);
                }
                Ok(Err(e)) => {
                    last_err = Some(e);
                }
                Err(_) => {
                    last_err = Some(Error::Unavailable(
                        "DLX destination durable_done canceled".into(),
                    ));
                }
            }
        }

        if succeeded == 0 {
            return Err(last_err.unwrap_or_else(|| {
                Error::Unavailable("DLX publish: no destination accepted".into())
            }));
        }

        let total_live = live.len() as u32;
        let result = if succeeded >= total_live {
            DlxPublishResult::Full
        } else {
            DlxPublishResult::Partial {
                succeeded,
                failed: total_live.saturating_sub(succeeded),
            }
        };
        debug!(
            vhost,
            exchange, routing_key, succeeded, "dead-letter published"
        );
        Ok(result)
    }

    /// Dead-letter helper (run off the source queue actor task).
    pub async fn dead_letter(
        &self,
        source: &QueueKey,
        dlx_exchange: &str,
        dlx_routing_key: &str,
        reason: DeathReason,
        msg: Arc<Message>,
        max_hops: u32,
    ) -> Result<DlxOutcome> {
        match prepare_dead_letter(msg.as_ref(), source.name.as_str(), reason, max_hops) {
            PrepareDeath::CycleDrop => {
                warn!(
                    vhost = %source.vhost,
                    queue = %source.name,
                    reason = reason.as_str(),
                    "DLX cycle/hop guard dropped message"
                );
                Ok(DlxOutcome::DroppedCycle)
            }
            PrepareDeath::Ready(dead) => {
                // Routing key: configured override, else original message RK.
                let rk = if dlx_routing_key.is_empty() {
                    dead.routing_key.as_str()
                } else {
                    dlx_routing_key
                };
                // Rewrite exchange on the envelope to the DLX name for the next hop.
                let mut routed = (*dead).clone();
                routed.exchange = CompactString::from(dlx_exchange);
                routed.routing_key = CompactString::from(rk);
                let routed = Arc::new(routed);

                match self
                    .publish(source.vhost.as_str(), dlx_exchange, rk, routed)
                    .await
                {
                    Ok(DlxPublishResult::AllCycle) => Ok(DlxOutcome::DroppedCycle),
                    Ok(DlxPublishResult::Full) => Ok(DlxOutcome::Published),
                    Ok(DlxPublishResult::Partial { .. }) => Ok(DlxOutcome::PartialPublished),
                    Err(e) => {
                        warn!(
                            vhost = %source.vhost,
                            queue = %source.name,
                            error = %e,
                            "DLX publish failed (no destination accepted)"
                        );
                        Err(e)
                    }
                }
            }
        }
    }
}

/// Result of routing a dead-letter publish to one or more destinations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DlxPublishResult {
    /// Every non-cycle destination accepted the message.
    Full,
    /// Some destinations accepted; others failed (source must **not** requeue).
    Partial {
        /// Number of destinations that fully completed durable_done.
        succeeded: u32,
        /// Number of live destinations that failed.
        failed: u32,
    },
    /// Every routed destination was filtered as an x-death cycle.
    AllCycle,
}

/// Result of a dead-letter attempt that did not hard-fail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DlxOutcome {
    /// Enqueued to all non-cycle DLX destinations.
    Published,
    /// At least one dest accepted; others failed — source must free (no requeue).
    PartialPublished,
    /// Dropped by hop guard or all-destination cycle filter.
    DroppedCycle,
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn sample() -> Message {
        Message {
            exchange: CompactString::from("ex"),
            routing_key: CompactString::from("rk"),
            body: Bytes::from_static(b"x"),
            persistent: false,
            redelivered: false,
            content_type: None,
            content_encoding: None,
            correlation_id: None,
            message_id: None,
            reply_to: None,
            expiration: None,
            app_id: None,
            user_id: None,
            type_: None,
            priority: None,
            timestamp: None,
            headers: MessageHeaders::default(),
        }
    }

    #[test]
    fn death_headers_accumulate() {
        let msg = sample();
        let PrepareDeath::Ready(m1) = prepare_dead_letter(&msg, "q1", DeathReason::Expired, 16)
        else {
            panic!("expected ready");
        };
        assert_eq!(m1.headers.deaths.len(), 1);
        assert_eq!(m1.headers.deaths[0].count, 1);
        assert_eq!(m1.headers.first_death_reason, Some(DeathReason::Expired));
        assert_eq!(m1.headers.first_death_queue.as_deref(), Some("q1"));

        let PrepareDeath::Ready(m2) =
            prepare_dead_letter(m1.as_ref(), "q1", DeathReason::Expired, 16)
        else {
            panic!("expected ready");
        };
        assert_eq!(m2.headers.deaths.len(), 1);
        assert_eq!(m2.headers.deaths[0].count, 2);
    }

    #[test]
    fn cycle_destination_detection() {
        let msg = sample();
        let PrepareDeath::Ready(m1) = prepare_dead_letter(&msg, "q1", DeathReason::Rejected, 16)
        else {
            panic!();
        };
        assert!(is_cycle_destination(&m1.headers, "q1"));
        assert!(!is_cycle_destination(&m1.headers, "q2"));
        // Preparing another death from q1 still allowed (count++), hop guard separate.
        assert!(matches!(
            prepare_dead_letter(m1.as_ref(), "q1", DeathReason::Rejected, 16),
            PrepareDeath::Ready(_)
        ));
    }

    #[test]
    fn hop_limit() {
        let mut msg = sample();
        // Simulate 16 hops via counts.
        msg.headers.deaths.push(DeathEntry {
            queue: CompactString::from("a"),
            reason: DeathReason::Expired,
            time: 0,
            exchange: CompactString::from("e"),
            routing_keys: vec![CompactString::from("r")],
            count: 16,
        });
        assert!(matches!(
            prepare_dead_letter(&msg, "b", DeathReason::Expired, 16),
            PrepareDeath::CycleDrop
        ));
    }
}
