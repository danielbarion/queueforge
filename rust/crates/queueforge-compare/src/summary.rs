//! Rates, confirm latency, and the load step where a broker stops keeping up.

/// One offered-rate step. Rates are messages per second over that step.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LoadStep {
    /// Rate the publisher attempted.
    pub offered_per_sec: f64,
    /// Publisher confirms completed during the step.
    pub confirmed_per_sec: f64,
    /// Deliveries acked during the step.
    pub consumed_per_sec: f64,
}

/// Counts the summary is computed from. The client fills this from the run.
#[derive(Debug, Clone)]
pub struct SummaryInput {
    /// Confirmed publishes across every step.
    pub confirmed: u64,
    /// Acked deliveries across every step.
    pub consumed: u64,
    /// Wall time of the measured steps, in seconds.
    pub elapsed_secs: f64,
    /// Publisher-confirm round-trip samples, in milliseconds.
    pub confirm_latency_ms: Vec<f64>,
    /// Offered-rate steps, in the order they ran.
    pub steps: Vec<LoadStep>,
}

/// Printed result of one broker run.
#[derive(Debug, Clone, PartialEq)]
pub struct Summary {
    /// Acked deliveries per second over the whole run.
    pub messages_per_sec: f64,
    /// Median publisher-confirm latency, in milliseconds.
    pub confirm_latency_ms: f64,
    /// First offered rate that fell behind, or the last rate when every step kept up.
    pub saturation_load: f64,
    /// True when every step confirmed and consumed at least 95% of the offered rate.
    pub kept_up: bool,
}

/// Seconds after the step start when publish `index` (0-based) should be sent
/// so the publisher offers `rate_per_sec`. Message k is scheduled at `k / rate`,
/// not at `(confirmed + sent) / rate`.
pub fn publish_slot_secs(index: u64, rate_per_sec: f64) -> f64 {
    index as f64 / rate_per_sec
}

/// A step keeps up when confirms and acks both reach 95% of the offered rate.
pub fn keeps_up(step: LoadStep) -> bool {
    let floor = step.offered_per_sec * 0.95;
    step.confirmed_per_sec >= floor && step.consumed_per_sec >= floor
}

/// Messages per second, median confirm latency, and the saturation load.
pub fn summarize(input: &SummaryInput) -> Summary {
    let _confirmed_total = input.confirmed;
    let messages_per_sec = if input.elapsed_secs > 0.0 {
        input.consumed as f64 / input.elapsed_secs
    } else {
        0.0
    };
    let confirm_latency_ms = median_ms(&input.confirm_latency_ms);
    let mut saturation_load = 0.0;
    let mut kept_up = !input.steps.is_empty();
    for step in &input.steps {
        saturation_load = step.offered_per_sec;
        if !keeps_up(*step) {
            kept_up = false;
            break;
        }
    }
    Summary {
        messages_per_sec,
        confirm_latency_ms,
        saturation_load,
        kept_up,
    }
}

fn median_ms(samples: &[f64]) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    let mut sorted = samples.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    sorted[sorted.len() / 2]
}

/// Text the binary prints. Confirm latency and the disk-flush setting share one line.
pub fn format_report(summary: &Summary, disk_flush: &str) -> String {
    format!(
        "messages_per_sec={:.2}\nconfirm_latency_ms={:.2} disk_flush={disk_flush}\nsaturation_load={:.0} kept_up={}\n",
        summary.messages_per_sec,
        summary.confirm_latency_ms,
        summary.saturation_load,
        summary.kept_up
    )
}

/// Disk flush the bench process actually runs for this AMQP URL.
///
/// RabbitMQ 4 (`:35672`) confirms before fsync and flushes the classic queue v2
/// write buffer at least every 200ms. Rust (`:35673`) and Bun (`:35674`) fsync
/// on `fsync_interval_ms` and also complete the publisher confirm before that fsync.
pub fn disk_flush_for_url(amqp_url: &str) -> &'static str {
    if amqp_url.contains(":35672") {
        "classic_queue.default_version=2; write-buffer flush at least every 200ms; publisher confirms before fsync"
    } else {
        "fsync_policy=every_n_ms fsync_interval_ms=10; group-commit timer, publisher confirm before fsync"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn summary_reports_rate_latency_and_the_step_that_falls_behind() {
        let summary = summarize(&SummaryInput {
            confirmed: 300,
            consumed: 280,
            elapsed_secs: 2.0,
            confirm_latency_ms: vec![2.0, 9.0, 4.0],
            steps: vec![
                LoadStep {
                    offered_per_sec: 100.0,
                    confirmed_per_sec: 100.0,
                    consumed_per_sec: 100.0,
                },
                LoadStep {
                    offered_per_sec: 400.0,
                    confirmed_per_sec: 250.0,
                    consumed_per_sec: 240.0,
                },
                LoadStep {
                    offered_per_sec: 800.0,
                    confirmed_per_sec: 800.0,
                    consumed_per_sec: 800.0,
                },
            ],
        });
        assert!((summary.messages_per_sec - 140.0).abs() < 1e-9);
        assert!((summary.confirm_latency_ms - 4.0).abs() < 1e-9);
        assert!((summary.saturation_load - 400.0).abs() < 1e-9);
        assert!(!summary.kept_up);
        let text = format_report(&summary, disk_flush_for_url("amqp://admin:devpassword12@127.0.0.1:35673/%2f"));
        assert!(text.contains("messages_per_sec=140.00"));
        assert!(text.contains("confirm_latency_ms=4.00 disk_flush=fsync_policy=every_n_ms fsync_interval_ms=10"));
        assert!(text.contains("saturation_load=400 kept_up=false"));
    }

    #[test]
    fn summary_uses_the_last_step_when_every_step_keeps_up() {
        let summary = summarize(&SummaryInput {
            confirmed: 100,
            consumed: 100,
            elapsed_secs: 2.0,
            confirm_latency_ms: vec![1.5],
            steps: vec![
                LoadStep {
                    offered_per_sec: 100.0,
                    confirmed_per_sec: 100.0,
                    consumed_per_sec: 98.0,
                },
                LoadStep {
                    offered_per_sec: 200.0,
                    confirmed_per_sec: 196.0,
                    consumed_per_sec: 190.0,
                },
            ],
        });
        assert!((summary.messages_per_sec - 50.0).abs() < 1e-9);
        assert!((summary.confirm_latency_ms - 1.5).abs() < 1e-9);
        assert!((summary.saturation_load - 200.0).abs() < 1e-9);
        assert!(summary.kept_up);
        let text = format_report(
            &summary,
            disk_flush_for_url("amqp://admin:devpassword12@127.0.0.1:35672/%2f"),
        );
        assert!(text.contains("saturation_load=200 kept_up=true"));
        assert!(text.contains("classic_queue.default_version=2"));
        let bun = disk_flush_for_url("amqp://admin:devpassword12@127.0.0.1:35674/%2f");
        assert!(bun.contains("fsync_interval_ms=10"));
        assert!(bun.contains("publisher confirm before fsync"));
        assert!(!bun.contains("PRAGMA synchronous=FULL"));
    }

    #[test]
    fn publish_slots_follow_the_labeled_rate() {
        assert!((publish_slot_secs(0, 200.0) - 0.0).abs() < 1e-12);
        assert!((publish_slot_secs(1, 200.0) - 0.005).abs() < 1e-12);
        assert!((publish_slot_secs(400, 200.0) - 2.0).abs() < 1e-12);
        // Two publishers splitting 200/s each send at 100/s. Their k-th sends
        // land on the full-rate grid, not at half of it.
        assert!((publish_slot_secs(1, 100.0) - 0.01).abs() < 1e-12);
    }
}
