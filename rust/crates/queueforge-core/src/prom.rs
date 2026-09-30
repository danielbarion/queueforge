//! RabbitMQ Prometheus families, spelled with a `rabbitmq_` prefix.
//!
//! Names match the RabbitMQ 4 aggregated metrics a client dashboard reads
//! (`rabbitmq_global_messages_received_total` and the rest), with the prefix
//! swapped. Erlang, Raft, and stream internals are not emitted.

use std::sync::atomic::{AtomicU64, Ordering};

static QUEUES: AtomicU64 = AtomicU64::new(0);
static CONSUMERS: AtomicU64 = AtomicU64::new(0);
static PUBLISHED: AtomicU64 = AtomicU64::new(0);
static DELIVERED: AtomicU64 = AtomicU64::new(0);
static ACKED: AtomicU64 = AtomicU64::new(0);

/// Cumulative publish, deliver, and ack counts for the management overview.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TrafficTotals {
    /// `rabbitmq_global_messages_received_total`.
    pub publish: u64,
    /// `rabbitmq_global_messages_delivered_total`.
    pub deliver: u64,
    /// `rabbitmq_global_messages_acknowledged_total`.
    pub ack: u64,
}

/// Latest cumulative traffic counters.
pub fn traffic_totals() -> TrafficTotals {
    TrafficTotals {
        publish: PUBLISHED.load(Ordering::Relaxed),
        deliver: DELIVERED.load(Ordering::Relaxed),
        ack: ACKED.load(Ordering::Relaxed),
    }
}

fn counter(name: &'static str) {
    metrics::counter!(name).increment(1);
}

fn gauge(name: &'static str, value: f64) {
    metrics::gauge!(name).set(value);
}

/// Register the renamed families at zero so an idle scrape still lists them.
pub fn prime() {
    gauge("rabbitmq_connections", 0.0);
    counter_abs("rabbitmq_connections_opened_total");
    counter_abs("rabbitmq_connections_closed_total");
    gauge("rabbitmq_channels", 0.0);
    counter_abs("rabbitmq_channels_opened_total");
    counter_abs("rabbitmq_channels_closed_total");
    gauge("rabbitmq_queues", 0.0);
    counter_abs("rabbitmq_queues_declared_total");
    counter_abs("rabbitmq_queues_created_total");
    counter_abs("rabbitmq_queues_deleted_total");
    gauge("rabbitmq_consumers", 0.0);
    gauge("rabbitmq_global_consumers", 0.0);
    gauge("rabbitmq_global_publishers", 0.0);
    for name in GLOBAL_COUNTERS {
        counter_abs(name);
    }
    gauge("rabbitmq_alarms_memory_used_watermark", 0.0);
    gauge("rabbitmq_alarms_free_disk_space_watermark", 0.0);
    gauge("rabbitmq_disk_space_available_bytes", 0.0);
    gauge("rabbitmq_unreachable_cluster_peers_count", 0.0);
    metrics::counter!("queueforge_confirm_before_fsync_total").absolute(0);
    metrics::gauge!("rabbitmq_identity_info", "rabbitmq_node" => "queueforge", "rabbitmq_cluster" => "queueforge").set(1.0);
    metrics::gauge!("rabbitmq_build_info", "rabbitmq_version" => env!("CARGO_PKG_VERSION")).set(1.0);
}

fn counter_abs(name: &'static str) {
    metrics::counter!(name).absolute(0);
}

const GLOBAL_COUNTERS: &[&str] = &[
    "rabbitmq_global_messages_received_total",
    "rabbitmq_global_messages_received_confirm_total",
    "rabbitmq_global_messages_confirmed_total",
    "rabbitmq_global_messages_routed_total",
    "rabbitmq_global_messages_unroutable_dropped_total",
    "rabbitmq_global_messages_unroutable_returned_total",
    "rabbitmq_global_messages_delivered_total",
    "rabbitmq_global_messages_delivered_consume_manual_ack_total",
    "rabbitmq_global_messages_delivered_consume_auto_ack_total",
    "rabbitmq_global_messages_delivered_get_manual_ack_total",
    "rabbitmq_global_messages_delivered_get_auto_ack_total",
    "rabbitmq_global_messages_get_empty_total",
    "rabbitmq_global_messages_acknowledged_total",
    "rabbitmq_global_messages_redelivered_total",
    "rabbitmq_global_messages_dead_lettered_expired_total",
    "rabbitmq_global_messages_dead_lettered_rejected_total",
    "rabbitmq_global_messages_dead_lettered_maxlen_total",
    "rabbitmq_global_messages_dead_lettered_delivery_limit_total",
    "rabbitmq_global_messages_dead_lettered_confirmed_total",
];

/// Identify this process. Labels use `rabbitmq_` rather than `rabbitmq_`.
pub fn identity(node: &str, cluster: &str) {
    metrics::gauge!("rabbitmq_identity_info", "rabbitmq_node" => node.to_string(), "rabbitmq_cluster" => cluster.to_string()).set(1.0);
}

/// A connection finished `connection.open`.
pub fn connection_opened() {
    counter("rabbitmq_connections_opened_total");
}

/// A connection that had opened is gone.
pub fn connection_closed() {
    counter("rabbitmq_connections_closed_total");
}

/// A channel finished `channel.open`.
pub fn channel_opened() {
    counter("rabbitmq_channels_opened_total");
}

/// An open channel was torn down.
pub fn channel_closed() {
    counter("rabbitmq_channels_closed_total");
}

/// `queue.declare` succeeded. `created` is true only for a new queue.
pub fn queue_declared(created: bool) {
    counter("rabbitmq_queues_declared_total");
    if created {
        counter("rabbitmq_queues_created_total");
        gauge("rabbitmq_queues", QUEUES.fetch_add(1, Ordering::Relaxed) as f64 + 1.0);
    }
}

/// A queue was deleted.
pub fn queue_deleted() {
    counter("rabbitmq_queues_deleted_total");
    let left = QUEUES.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| Some(n.saturating_sub(1)));
    gauge("rabbitmq_queues", left.unwrap_or(0).saturating_sub(1) as f64);
}

/// A consumer was registered.
pub fn consumer_opened() {
    let n = CONSUMERS.fetch_add(1, Ordering::Relaxed) + 1;
    gauge("rabbitmq_consumers", n as f64);
    gauge("rabbitmq_global_consumers", n as f64);
}

/// A registered consumer was removed.
pub fn consumer_closed() {
    let left = CONSUMERS
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| Some(n.saturating_sub(1)))
        .unwrap_or(0)
        .saturating_sub(1);
    gauge("rabbitmq_consumers", left as f64);
    gauge("rabbitmq_global_consumers", left as f64);
}

/// A publish body was accepted. `confirm` is publisher-confirm mode.
pub fn message_received(confirm: bool) {
    PUBLISHED.fetch_add(1, Ordering::Relaxed);
    counter("rabbitmq_global_messages_received_total");
    if confirm {
        counter("rabbitmq_global_messages_received_confirm_total");
    }
}

/// A publisher confirm was acked.
pub fn message_confirmed() {
    counter("rabbitmq_global_messages_confirmed_total");
}

/// A publish matched `n` queue destinations.
pub fn message_routed(n: u64) {
    if n > 0 {
        metrics::counter!("rabbitmq_global_messages_routed_total").increment(n);
    }
}

/// A publish matched no queue. `returned` is `mandatory`.
pub fn message_unroutable(returned: bool) {
    if returned {
        counter("rabbitmq_global_messages_unroutable_returned_total");
    } else {
        counter("rabbitmq_global_messages_unroutable_dropped_total");
    }
}

/// A message was pushed to a consumer.
pub fn message_delivered_consume(auto_ack: bool) {
    DELIVERED.fetch_add(1, Ordering::Relaxed);
    counter("rabbitmq_global_messages_delivered_total");
    if auto_ack {
        counter("rabbitmq_global_messages_delivered_consume_auto_ack_total");
    } else {
        counter("rabbitmq_global_messages_delivered_consume_manual_ack_total");
    }
}

/// `basic.get` returned a message.
pub fn message_delivered_get(auto_ack: bool) {
    DELIVERED.fetch_add(1, Ordering::Relaxed);
    counter("rabbitmq_global_messages_delivered_total");
    if auto_ack {
        counter("rabbitmq_global_messages_delivered_get_auto_ack_total");
    } else {
        counter("rabbitmq_global_messages_delivered_get_manual_ack_total");
    }
}

/// `basic.get` found the queue empty.
pub fn message_get_empty() {
    counter("rabbitmq_global_messages_get_empty_total");
}

/// A delivered message was acknowledged.
pub fn message_acked() {
    ACKED.fetch_add(1, Ordering::Relaxed);
    counter("rabbitmq_global_messages_acknowledged_total");
}

/// A delivered message had the redelivered flag set.
pub fn message_redelivered() {
    counter("rabbitmq_global_messages_redelivered_total");
}

/// A message left its queue for `reason` (`expired`, `maxlen`, `delivery_limit`, or rejected).
pub fn dead_lettered(reason: &str) {
    match reason {
        "expired" => counter("rabbitmq_global_messages_dead_lettered_expired_total"),
        "maxlen" => counter("rabbitmq_global_messages_dead_lettered_maxlen_total"),
        "delivery_limit" => counter("rabbitmq_global_messages_dead_lettered_delivery_limit_total"),
        _ => counter("rabbitmq_global_messages_dead_lettered_rejected_total"),
    }
}

/// Per-queue ready, unacked, total, and consumer gauges.
pub fn queue_depth(vhost: &str, queue: &str, ready: u64, unacked: u64, consumers: u64) {
    metrics::gauge!("rabbitmq_queue_messages_ready", "vhost" => vhost.to_string(), "queue" => queue.to_string()).set(ready as f64);
    metrics::gauge!("rabbitmq_queue_messages_unacked", "vhost" => vhost.to_string(), "queue" => queue.to_string()).set(unacked as f64);
    metrics::gauge!("rabbitmq_queue_messages", "vhost" => vhost.to_string(), "queue" => queue.to_string()).set((ready + unacked) as f64);
    metrics::gauge!("rabbitmq_queue_consumers", "vhost" => vhost.to_string(), "queue" => queue.to_string()).set(consumers as f64);
}

/// Free bytes on the data volume.
pub fn disk_space_available(bytes: u64) {
    gauge("rabbitmq_disk_space_available_bytes", bytes as f64);
}

/// Memory high-watermark alarm. `1` while publishes are blocked.
pub fn memory_alarm(on: bool) {
    gauge("rabbitmq_alarms_memory_used_watermark", if on { 1.0 } else { 0.0 });
}

/// Free-disk watermark alarm.
pub fn disk_alarm(on: bool) {
    gauge("rabbitmq_alarms_free_disk_space_watermark", if on { 1.0 } else { 0.0 });
}

/// Configured cluster peers that are not connected.
pub fn unreachable_peers(n: u64) {
    gauge("rabbitmq_unreachable_cluster_peers_count", n as f64);
}
