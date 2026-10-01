/**
 * Prometheus text for the Bun broker.
 *
 * Names follow the rabbitmq_* series the dashboard already graphs, plus the
 * QueueForge fsync counters. Label values are escaped by {@link promLabel}.
 */
import type { Broker } from "../broker/index.ts";

/**
 * Escape a Prometheus label value.
 *
 * @param value Raw label text, such as a vhost or queue name.
 * @returns The value with backslash, newline, and quote escaped.
 */
function promLabel(value: string): string {
  return value.replace(/\\/g, "\\\\").replace(/\n/g, "\\n").replace(/"/g, '\\"');
}

/**
 * Render the broker's Prometheus text.
 *
 * @param broker Live broker. Queue gauges are one series per queue currently in memory.
 * @returns Prometheus text ending in a newline. Counters that this broker does not
 * track are emitted as 0 so the dashboard's metric names still resolve.
 */
export function metricsText(broker: Broker): string {
  const p = broker.prom;
  const node = promLabel(broker.cfg.nodeId || "queueforge");
  const lines = [
    "# TYPE rabbitmq_up gauge",
    "rabbitmq_up 1",
    "# TYPE rabbitmq_ready gauge",
    `rabbitmq_ready ${broker.ready ? 1 : 0}`,
    "# TYPE rabbitmq_connections gauge",
    `rabbitmq_connections ${p.connections}`,
    "# TYPE rabbitmq_connections_opened_total counter",
    `rabbitmq_connections_opened_total ${p.connectionsOpened}`,
    "# TYPE rabbitmq_connections_closed_total counter",
    `rabbitmq_connections_closed_total ${p.connectionsClosed}`,
    "# TYPE rabbitmq_channels gauge",
    `rabbitmq_channels ${p.channels}`,
    "# TYPE rabbitmq_channels_opened_total counter",
    `rabbitmq_channels_opened_total ${p.channelsOpened}`,
    "# TYPE rabbitmq_channels_closed_total counter",
    `rabbitmq_channels_closed_total ${p.channelsClosed}`,
    "# TYPE rabbitmq_queues gauge",
    `rabbitmq_queues ${broker.queues.size}`,
    "# TYPE rabbitmq_queues_declared_total counter",
    `rabbitmq_queues_declared_total ${p.queuesDeclared}`,
    "# TYPE rabbitmq_queues_created_total counter",
    `rabbitmq_queues_created_total ${p.queuesCreated}`,
    "# TYPE rabbitmq_queues_deleted_total counter",
    `rabbitmq_queues_deleted_total ${p.queuesDeleted}`,
    "# TYPE rabbitmq_consumers gauge",
    `rabbitmq_consumers ${p.consumers}`,
    "# TYPE rabbitmq_global_consumers gauge",
    `rabbitmq_global_consumers ${p.consumers}`,
    "# TYPE rabbitmq_global_publishers gauge",
    "rabbitmq_global_publishers 0",
    "# TYPE rabbitmq_global_messages_received_total counter",
    `rabbitmq_global_messages_received_total ${p.received}`,
    "# TYPE rabbitmq_global_messages_received_confirm_total counter",
    `rabbitmq_global_messages_received_confirm_total ${p.receivedConfirm}`,
    "# TYPE rabbitmq_global_messages_confirmed_total counter",
    `rabbitmq_global_messages_confirmed_total ${p.confirmed}`,
    "# TYPE rabbitmq_global_messages_routed_total counter",
    `rabbitmq_global_messages_routed_total ${p.routed}`,
    "# TYPE rabbitmq_global_messages_unroutable_dropped_total counter",
    `rabbitmq_global_messages_unroutable_dropped_total ${p.unroutableDropped}`,
    "# TYPE rabbitmq_global_messages_unroutable_returned_total counter",
    `rabbitmq_global_messages_unroutable_returned_total ${p.unroutableReturned}`,
    "# TYPE rabbitmq_global_messages_delivered_total counter",
    `rabbitmq_global_messages_delivered_total ${p.delivered}`,
    "# TYPE rabbitmq_global_messages_delivered_consume_manual_ack_total counter",
    `rabbitmq_global_messages_delivered_consume_manual_ack_total ${p.deliveredConsumeManual}`,
    "# TYPE rabbitmq_global_messages_delivered_consume_auto_ack_total counter",
    `rabbitmq_global_messages_delivered_consume_auto_ack_total ${p.deliveredConsumeAuto}`,
    "# TYPE rabbitmq_global_messages_delivered_get_manual_ack_total counter",
    `rabbitmq_global_messages_delivered_get_manual_ack_total ${p.deliveredGetManual}`,
    "# TYPE rabbitmq_global_messages_delivered_get_auto_ack_total counter",
    `rabbitmq_global_messages_delivered_get_auto_ack_total ${p.deliveredGetAuto}`,
    "# TYPE rabbitmq_global_messages_get_empty_total counter",
    `rabbitmq_global_messages_get_empty_total ${p.getEmpty}`,
    "# TYPE rabbitmq_global_messages_acknowledged_total counter",
    `rabbitmq_global_messages_acknowledged_total ${p.acknowledged}`,
    "# TYPE rabbitmq_global_messages_redelivered_total counter",
    `rabbitmq_global_messages_redelivered_total ${p.redelivered}`,
    "# TYPE rabbitmq_global_messages_dead_lettered_expired_total counter",
    `rabbitmq_global_messages_dead_lettered_expired_total ${p.dlxExpired}`,
    "# TYPE rabbitmq_global_messages_dead_lettered_rejected_total counter",
    `rabbitmq_global_messages_dead_lettered_rejected_total ${p.dlxRejected}`,
    "# TYPE rabbitmq_global_messages_dead_lettered_maxlen_total counter",
    `rabbitmq_global_messages_dead_lettered_maxlen_total ${p.dlxMaxlen}`,
    "# TYPE rabbitmq_global_messages_dead_lettered_delivery_limit_total counter",
    `rabbitmq_global_messages_dead_lettered_delivery_limit_total ${p.dlxDeliveryLimit}`,
    "# TYPE rabbitmq_global_messages_dead_lettered_confirmed_total counter",
    "rabbitmq_global_messages_dead_lettered_confirmed_total 0",
    "# TYPE rabbitmq_alarms_memory_used_watermark gauge",
    "rabbitmq_alarms_memory_used_watermark 0",
    "# TYPE rabbitmq_alarms_free_disk_space_watermark gauge",
    "rabbitmq_alarms_free_disk_space_watermark 0",
    "# TYPE rabbitmq_disk_space_available_bytes gauge",
    "rabbitmq_disk_space_available_bytes 0",
    "# TYPE rabbitmq_unreachable_cluster_peers_count gauge",
    "rabbitmq_unreachable_cluster_peers_count 0",
    ...[...broker.queues.values()].flatMap((q) => {
      const labels = `vhost="${promLabel(q.vhost)}",queue="${promLabel(q.name)}"`;
      return [
        "# TYPE rabbitmq_queue_messages_ready gauge",
        `rabbitmq_queue_messages_ready{${labels}} ${q.ready.length}`,
        "# TYPE rabbitmq_queue_messages_unacked gauge",
        `rabbitmq_queue_messages_unacked{${labels}} ${q.unacked.size}`,
        "# TYPE rabbitmq_queue_messages gauge",
        `rabbitmq_queue_messages{${labels}} ${q.ready.length + q.unacked.size}`,
        "# TYPE rabbitmq_queue_consumers gauge",
        `rabbitmq_queue_consumers{${labels}} ${q.consumers.length}`,
      ];
    }),
    "# TYPE queueforge_wal_fsync_seconds histogram",
    `queueforge_wal_fsync_seconds_count ${broker.store.fullFlushCount}`,
    `queueforge_wal_fsync_seconds_sum 0`,
    "# TYPE queueforge_confirm_before_fsync_total counter",
    `queueforge_confirm_before_fsync_total ${broker.store.confirmsBeforeFsync}`,
    "# TYPE queueforge_full_flush_total counter",
    `queueforge_full_flush_total ${broker.store.fullFlushCount}`,
    "# TYPE rabbitmq_identity_info gauge",
    `rabbitmq_identity_info{rabbitmq_node="${node}",rabbitmq_cluster="queueforge"} 1`,
    "# TYPE rabbitmq_build_info gauge",
    'rabbitmq_build_info{rabbitmq_version="0.1.0"} 1',
    "",
  ];
  return lines.join("\n");
}
