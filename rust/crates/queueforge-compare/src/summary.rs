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
    /// Confirmed publishes across every paced step.
    pub confirmed: u64,
    /// Acked deliveries across every paced step.
    pub consumed: u64,
    /// Counted time of the paced steps, in seconds.
    pub elapsed_secs: f64,
    /// Publisher-confirm round-trip samples, in milliseconds.
    pub confirm_latency_ms: Vec<f64>,
    /// Offered-rate steps, in the order they ran.
    pub steps: Vec<LoadStep>,
    /// Acked deliveries and wall seconds of one uncapped step.
    ///
    /// When this is set, `messages_per_sec` is that step's consumer rate.
    /// The paced ladder still decides `pace_messages_per_sec`, saturation, and
    /// `kept_up`. The bench leaves this empty.
    pub uncapped: Option<(u64, f64)>,
    /// Left false by the bench. Message k is published at k/rate, and a kept
    /// step counts the offered window. The historical offers are the keep-up bars.
    pub pipeline: bool,
    /// Kept false. `messages_per_sec` is acks over the counted windows even
    /// when a caller sets this. The highest kept step is not the throughput.
    pub score_sustained: bool,
}

/// Printed result of one broker run.
#[derive(Debug, Clone, PartialEq)]
pub struct Summary {
    /// Acked deliveries per second used as the throughput figure.
    ///
    /// This is acks over the counted offer windows, the same quantity as
    /// `pace_messages_per_sec`. When `uncapped` is set, this is that step
    /// instead. The bench leaves `uncapped` empty. The highest kept step,
    /// the first missed step, and the fastest step stay out of this figure.
    pub messages_per_sec: f64,
    /// Acked deliveries per second over the paced steps.
    ///
    /// Total paced acks divided by counted step time. When every step is kept,
    /// this sits on the average of the offers.
    pub pace_messages_per_sec: f64,
    /// True when `messages_per_sec` came from the uncapped step.
    pub uncapped: bool,
    /// True when the steps ran without a slot wait. The bench leaves this false.
    pub pipeline: bool,
    /// Always false. The report labels the window mean `throughput=paced`.
    pub score_sustained: bool,
    /// Fastest per-step consumer rate, in messages per second.
    ///
    /// Each step records acks divided by the longer of that step's wall and the
    /// offered window. A later step can be faster than the saturation step.
    /// `messages_per_sec` does not use this peak.
    pub saturation_messages_per_sec: f64,
    /// Median publisher-confirm latency, in milliseconds. Same value as `confirm_p50_ms`.
    pub confirm_latency_ms: f64,
    /// Median publisher-confirm latency, in milliseconds.
    pub confirm_p50_ms: f64,
    /// 99th percentile publisher-confirm latency, in milliseconds.
    pub confirm_p99_ms: f64,
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

/// Stop offering when the quota is sent, the next slot is outside the step,
/// or the clock is more than 15 ms past the step. A confirm wait that crosses
/// the deadline still sends a message whose slot was inside the step.
pub fn offer_closed(
    sent: u64,
    per: u64,
    next_slot_secs: f64,
    now_secs: f64,
    step_secs: f64,
) -> bool {
    sent >= per || next_slot_secs > step_secs || now_secs >= step_secs + 0.015
}

/// A pipeline step sends the offered quota (`per` = rate × step) as soon as
/// the inflight window has room. It stops when that many publishes are out,
/// or when the step clock is up. It does not sleep until the next slot.
pub fn pipeline_closed(sent: u64, per: u64, now_secs: f64, step_secs: f64) -> bool {
    sent >= per || now_secs >= step_secs + 0.015
}

/// Consumer rate for one step, in messages per second.
///
/// The denominator is the longer of the step wall and the offered window.
/// A publisher that finishes its quota before the window ends still scores
/// acks over that window, so quota / short wall cannot raise the rate.
pub fn step_consumer_per_sec(count: u64, actual_secs: f64, step_secs: f64) -> f64 {
    let scored = actual_secs.max(step_secs).max(0.001);
    count as f64 / scored
}

/// Consumer rate on the highest kept step.
///
/// The bench does not copy this into `messages_per_sec`. A step after the
/// first miss does not raise this figure. Each step rate is acks divided by
/// the longer of the step wall and the offered window.
#[cfg(test)]
pub fn sustained_consumer_per_sec(steps: &[LoadStep]) -> f64 {
    let mut best: Option<f64> = None;
    for step in steps {
        if keeps_up(*step) {
            best = Some(step.consumed_per_sec);
        } else {
            return best.unwrap_or(step.consumed_per_sec);
        }
    }
    best.unwrap_or(0.0)
}

/// Consumer rate on the saturation step.
///
/// Saturation is the first offered step whose confirms or acks missed 95% of
/// the offer. When every step kept up, saturation is the last step. The rate
/// is what the consumer acked on that step, already divided by the longer of
/// the step wall and the offered window. `summarize` does not copy this into
/// `messages_per_sec`.
#[cfg(test)]
pub fn consumer_rate_at_saturation(steps: &[LoadStep]) -> f64 {
    let Some(first) = steps.first() else {
        return 0.0;
    };
    for step in steps {
        if !keeps_up(*step) {
            return step.consumed_per_sec;
        }
    }
    steps.last().unwrap_or(first).consumed_per_sec
}

/// A step keeps up when confirms and acks both reach 95% of the offered rate.
pub fn keeps_up(step: LoadStep) -> bool {
    let floor = step.offered_per_sec * 0.95;
    step.confirmed_per_sec >= floor && step.consumed_per_sec >= floor
}

/// Elapsed seconds counted toward `messages_per_sec`.
///
/// A step that kept the offered rate counts as the offered window. The group
/// commit after the last publish is a few milliseconds and is not a slower
/// consumer. A step that missed the rate keeps the real clock, including the
/// time spent unable to confirm or deliver.
pub fn counted_step_elapsed(
    actual_secs: f64,
    step_secs: f64,
    confirmed: u64,
    consumed: u64,
    offered_per_sec: f64,
) -> f64 {
    let target = offered_per_sec * step_secs;
    let floor = target * 0.95;
    let kept = confirmed as f64 >= floor && consumed as f64 >= floor;
    if kept {
        step_secs
    } else {
        actual_secs.max(0.001)
    }
}

/// Messages per second, median confirm latency, and the saturation load.
pub fn summarize(input: &SummaryInput) -> Summary {
    let _confirmed_total = input.confirmed;
    let pace_messages_per_sec = if input.elapsed_secs > 0.0 {
        input.consumed as f64 / input.elapsed_secs
    } else {
        0.0
    };
    // Peak consumer rate across every step, including offers past the first miss.
    let saturation_messages_per_sec = input
        .steps
        .iter()
        .map(|step| step.consumed_per_sec)
        .fold(0.0_f64, f64::max);
    let _ = input.score_sustained;
    let (messages_per_sec, uncapped) = match input.uncapped {
        Some((consumed, elapsed_secs)) if elapsed_secs > 0.0 => {
            (consumed as f64 / elapsed_secs, true)
        }
        _ => (pace_messages_per_sec, false),
    };
    let confirm_latency_ms = median_ms(&input.confirm_latency_ms);
    let confirm_p99_ms = percentile_ms(&input.confirm_latency_ms, 0.99);
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
        pace_messages_per_sec,
        uncapped,
        pipeline: input.pipeline,
        score_sustained: false,
        saturation_messages_per_sec,
        confirm_latency_ms,
        confirm_p50_ms: confirm_latency_ms,
        confirm_p99_ms,
        saturation_load,
        kept_up,
    }
}

fn sorted_ms(samples: &[f64]) -> Vec<f64> {
    let mut sorted = samples.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    sorted
}

fn median_ms(samples: &[f64]) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    let sorted = sorted_ms(samples);
    sorted[sorted.len() / 2]
}

/// Smallest sample at or above `pct` of the ordered samples. `pct` is 0.99 for p99.
fn percentile_ms(samples: &[f64], pct: f64) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    let sorted = sorted_ms(samples);
    let rank = (sorted.len() as f64 * pct).ceil() as usize;
    let idx = rank.saturating_sub(1).min(sorted.len() - 1);
    sorted[idx]
}

/// Text the binary prints. Confirm latency and the disk-flush setting share one line.
pub fn format_report(summary: &Summary, disk_flush: &str) -> String {
    format!(
        "messages_per_sec={:.2}\npace_messages_per_sec={:.2}\nthroughput={}\nsaturation_messages_per_sec={:.2}\nconfirm_latency_ms={:.2} disk_flush={disk_flush}\nconfirm_p50_ms={:.2}\nconfirm_p99_ms={:.2}\nsaturation_load={:.0} kept_up={}\n",
        summary.messages_per_sec,
        summary.pace_messages_per_sec,
        if summary.uncapped {
            "uncapped"
        } else if summary.pipeline {
            "pipeline"
        } else {
            "paced"
        },
        summary.saturation_messages_per_sec,
        summary.confirm_latency_ms,
        summary.confirm_p50_ms,
        summary.confirm_p99_ms,
        summary.saturation_load,
        summary.kept_up
    )
}

/// Disk flush the bench process actually runs for this AMQP URL.
///
/// RabbitMQ 4 (`:35672`) confirms before fsync and flushes the classic queue v2
/// write buffer at least every 200ms. Rust (`:35673`) and Bun (`:35674`) fsync
/// on `fsync_interval_ms` and complete the publisher confirm after that fsync.
pub fn disk_flush_for_url(amqp_url: &str) -> &'static str {
    if amqp_url.contains(":35672") {
        "classic_queue.default_version=2; write-buffer flush at least every 200ms; publisher confirms before fsync"
    } else {
        "fsync_policy=every_n_ms fsync_interval_ms=10; group-commit timer, publisher confirm after that fsync"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_slot_inside_the_step_is_still_offered_just_after_the_deadline() {
        assert!(!offer_closed(1999, 2000, 1.999, 2.005, 2.0));
        assert!(offer_closed(1999, 2000, 1.999, 2.020, 2.0));
        assert!(offer_closed(2000, 2000, 2.0, 1.9, 2.0));
        assert!(offer_closed(10, 2000, 2.001, 1.5, 2.0));
    }

    #[test]
    fn pipeline_step_stops_at_the_offered_quota() {
        // 200/s for 2s is 400 publishes. Stop at that count, and also at the deadline.
        assert!(!pipeline_closed(399, 400, 0.05, 2.0));
        assert!(pipeline_closed(400, 400, 0.05, 2.0));
        assert!(!pipeline_closed(10, 32_000, 2.010, 2.0));
        assert!(pipeline_closed(10, 32_000, 2.020, 2.0));
    }

    #[test]
    fn kept_step_counts_the_offered_window_and_a_miss_keeps_the_clock() {
        let kept = counted_step_elapsed(2.006, 2.0, 1999, 1999, 1000.0);
        assert!((kept - 2.0).abs() < 1e-9);
        let early = counted_step_elapsed(1.997, 2.0, 400, 400, 200.0);
        assert!((early - 2.0).abs() < 1e-9);
        let missed = counted_step_elapsed(2.231, 2.0, 80_000, 41_000, 48_000.0);
        assert!((missed - 2.231).abs() < 1e-9);
    }

    #[test]
    fn summary_reports_rate_latency_and_the_step_that_falls_behind() {
        let summary = summarize(&SummaryInput {
            confirmed: 300,
            consumed: 280,
            elapsed_secs: 2.0,
            confirm_latency_ms: vec![2.0, 9.0, 4.0],
            uncapped: None,
            pipeline: false,
            score_sustained: false,
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
        assert!((summary.saturation_messages_per_sec - 800.0).abs() < 1e-9);
        assert!((summary.confirm_latency_ms - 4.0).abs() < 1e-9);
        assert!((summary.confirm_p50_ms - 4.0).abs() < 1e-9);
        assert!((summary.confirm_p99_ms - 9.0).abs() < 1e-9);
        assert!((summary.saturation_load - 400.0).abs() < 1e-9);
        assert!(!summary.kept_up);
        let text = format_report(
            &summary,
            disk_flush_for_url("amqp://admin:devpassword12@127.0.0.1:35673/%2f"),
        );
        assert!(text.contains("messages_per_sec=140.00"));
        assert!(text.contains("saturation_messages_per_sec=800.00"));
        assert!(text.contains(
            "confirm_latency_ms=4.00 disk_flush=fsync_policy=every_n_ms fsync_interval_ms=10"
        ));
        assert!(text.contains("saturation_load=400 kept_up=false"));
    }

    #[test]
    fn summary_uses_the_last_step_when_every_step_keeps_up() {
        let summary = summarize(&SummaryInput {
            confirmed: 100,
            consumed: 100,
            elapsed_secs: 2.0,
            confirm_latency_ms: vec![1.5],
            uncapped: None,
            pipeline: false,
            score_sustained: false,
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
        assert!((summary.saturation_messages_per_sec - 190.0).abs() < 1e-9);
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
        assert!(bun.contains("publisher confirm after that fsync"));
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

    #[test]
    fn pipelined_128_inflight_summary_reports_rate_p50_p99_saturation_and_confirm_after_fsync() {
        let mut confirm_latency_ms = vec![8.0; 128];
        confirm_latency_ms[126] = 21.0;
        confirm_latency_ms[127] = 40.0;
        assert_eq!(confirm_latency_ms.len(), 128);
        let steps = vec![
            LoadStep {
                offered_per_sec: 200.0,
                confirmed_per_sec: 200.0,
                consumed_per_sec: 200.0,
            },
            LoadStep {
                offered_per_sec: 1000.0,
                confirmed_per_sec: 1000.0,
                consumed_per_sec: 1000.0,
            },
            LoadStep {
                offered_per_sec: 2000.0,
                confirmed_per_sec: 2000.0,
                consumed_per_sec: 2000.0,
            },
            LoadStep {
                offered_per_sec: 4000.0,
                confirmed_per_sec: 4000.0,
                consumed_per_sec: 4000.0,
            },
            LoadStep {
                offered_per_sec: 8000.0,
                confirmed_per_sec: 1000.0,
                consumed_per_sec: 1000.0,
            },
        ];
        let step_secs = 2.0;
        let elapsed_secs = step_secs * steps.len() as f64;
        let consumed = steps
            .iter()
            .map(|step| (step.consumed_per_sec * step_secs).round() as u64)
            .sum();
        let confirmed = steps
            .iter()
            .map(|step| (step.confirmed_per_sec * step_secs).round() as u64)
            .sum();
        let summary = summarize(&SummaryInput {
            confirmed,
            consumed,
            elapsed_secs,
            confirm_latency_ms,
            uncapped: None,
            pipeline: false,
            score_sustained: false,
            steps,
        });
        assert!((summary.messages_per_sec - 1640.0).abs() < 1e-9);
        assert!((summary.saturation_messages_per_sec - 4000.0).abs() < 1e-9);
        assert!((summary.confirm_latency_ms - 8.0).abs() < 1e-9);
        assert!((summary.confirm_p50_ms - 8.0).abs() < 1e-9);
        assert!((summary.confirm_p99_ms - 21.0).abs() < 1e-9);
        assert!((summary.saturation_load - 8000.0).abs() < 1e-9);
        assert!(!summary.kept_up);
        let text = format_report(
            &summary,
            disk_flush_for_url("amqp://admin:devpassword12@127.0.0.1:35673/%2f"),
        );
        assert!(text.contains("messages_per_sec=1640.00"));
        assert!(text.contains("saturation_messages_per_sec=4000.00"));
        assert!(text.contains("confirm_p50_ms=8.00"));
        assert!(text.contains("confirm_p99_ms=21.00"));
        assert!(text.contains("saturation_load=8000 kept_up=false"));
        assert!(text.contains("publisher confirm after that fsync"));
        assert!(text.contains("pace_messages_per_sec=1640.00"));
        assert!(text.contains("throughput=paced"));
    }

    #[test]
    fn uncapped_step_is_the_throughput_and_the_paced_ladder_stays() {
        // Two kept steps: 400 acks in 2s and 2000 acks in 2s. The pace mean is 600.
        // The uncapped step delivered 46000 acks in 2s, which is the throughput.
        let summary = summarize(&SummaryInput {
            confirmed: 2400,
            consumed: 2400,
            elapsed_secs: 4.0,
            confirm_latency_ms: vec![4.0, 8.0],
            uncapped: Some((46_000, 2.0)),
            pipeline: false,
            score_sustained: false,
            steps: vec![
                LoadStep {
                    offered_per_sec: 200.0,
                    confirmed_per_sec: 200.0,
                    consumed_per_sec: 200.0,
                },
                LoadStep {
                    offered_per_sec: 1000.0,
                    confirmed_per_sec: 1000.0,
                    consumed_per_sec: 1000.0,
                },
            ],
        });
        assert!((summary.messages_per_sec - 23_000.0).abs() < 1e-9);
        assert!((summary.pace_messages_per_sec - 600.0).abs() < 1e-9);
        assert!(summary.uncapped);
        assert!(summary.kept_up);
        assert!((summary.saturation_load - 1000.0).abs() < 1e-9);
        let text = format_report(
            &summary,
            disk_flush_for_url("amqp://admin:devpassword12@127.0.0.1:35673/%2f"),
        );
        assert!(text.contains("messages_per_sec=23000.00"));
        assert!(text.contains("pace_messages_per_sec=600.00"));
        assert!(text.contains("throughput=uncapped"));
        assert!(text.contains("saturation_load=1000 kept_up=true"));
        assert!(text.contains("publisher confirm after that fsync"));
    }

    #[test]
    fn kept_durable_and_fan_ladders_score_the_offered_window() {
        // Six kept durable steps of 2s. Quotas are 400, 2000, 4000, 8000,
        // 16000, and 32000. Counted time is 12s, so acks over the window are 5200.
        let durable = [200.0, 1000.0, 2000.0, 4000.0, 8000.0, 16_000.0];
        let durable_steps: Vec<LoadStep> = durable
            .iter()
            .map(|rate| LoadStep {
                offered_per_sec: *rate,
                confirmed_per_sec: *rate,
                consumed_per_sec: *rate,
            })
            .collect();
        let durable_acks: u64 = durable.iter().map(|rate| (*rate * 2.0) as u64).sum();
        let durable_summary = summarize(&SummaryInput {
            confirmed: durable_acks,
            consumed: durable_acks,
            elapsed_secs: 12.0,
            confirm_latency_ms: vec![10.0, 12.0],
            uncapped: None,
            pipeline: false,
            score_sustained: false,
            steps: durable_steps,
        });
        assert!((durable_summary.messages_per_sec - 5200.0).abs() < 1e-9);
        assert!((durable_summary.pace_messages_per_sec - 5200.0).abs() < 1e-9);
        assert!(!durable_summary.pipeline);
        assert!(!durable_summary.uncapped);
        assert!(durable_summary.kept_up);
        assert!((durable_summary.saturation_load - 16_000.0).abs() < 1e-9);
        let durable_text = format_report(
            &durable_summary,
            disk_flush_for_url("amqp://admin:devpassword12@127.0.0.1:35673/%2f"),
        );
        assert!(durable_text.contains("messages_per_sec=5200.00"));
        assert!(durable_text.contains("pace_messages_per_sec=5200.00"));
        assert!(durable_text.contains("throughput=paced"));
        assert!(durable_text.contains("saturation_load=16000 kept_up=true"));
        assert!(durable_text.contains("publisher confirm after that fsync"));

        // Fan offers 200, 800, 1600, 3200, 6400, 12800. The same 12s window
        // is 4166.67 messages/s.
        let fan = [200.0, 800.0, 1600.0, 3200.0, 6400.0, 12_800.0];
        let fan_steps: Vec<LoadStep> = fan
            .iter()
            .map(|rate| LoadStep {
                offered_per_sec: *rate,
                confirmed_per_sec: *rate,
                consumed_per_sec: *rate,
            })
            .collect();
        let fan_acks: u64 = fan.iter().map(|rate| (*rate * 2.0) as u64).sum();
        let fan_summary = summarize(&SummaryInput {
            confirmed: fan_acks,
            consumed: fan_acks,
            elapsed_secs: 12.0,
            confirm_latency_ms: vec![8.0],
            uncapped: None,
            pipeline: false,
            score_sustained: false,
            steps: fan_steps,
        });
        assert!((fan_summary.messages_per_sec - (25_000.0 / 6.0)).abs() < 1e-9);
        assert!((fan_summary.pace_messages_per_sec - (25_000.0 / 6.0)).abs() < 1e-9);
        assert!(fan_summary.kept_up);
        assert!((fan_summary.saturation_load - 12_800.0).abs() < 1e-9);
        let fan_text = format_report(
            &fan_summary,
            disk_flush_for_url("amqp://admin:devpassword12@127.0.0.1:35674/%2f"),
        );
        assert!(fan_text.contains("messages_per_sec=4166.67"));
        assert!(fan_text.contains("throughput=paced"));
        assert!(fan_text.contains("publisher confirm after that fsync"));
    }

    #[test]
    fn offered_window_score_ignores_a_faster_step_consumer_rate() {
        // Six kept historical offers. A later step's consumer rate is recorded
        // beside the score and does not replace messages_per_sec.
        let summary = summarize(&SummaryInput {
            confirmed: 62_400,
            consumed: 62_400,
            elapsed_secs: 12.0,
            confirm_latency_ms: vec![6.0, 12.0],
            uncapped: None,
            pipeline: false,
            score_sustained: false,
            steps: vec![
                LoadStep {
                    offered_per_sec: 4_000.0,
                    confirmed_per_sec: 4_000.0,
                    consumed_per_sec: 4_000.0,
                },
                LoadStep {
                    offered_per_sec: 16_000.0,
                    confirmed_per_sec: 16_000.0,
                    consumed_per_sec: 16_000.0,
                },
                LoadStep {
                    offered_per_sec: 32_000.0,
                    confirmed_per_sec: 36_000.0,
                    consumed_per_sec: 36_000.0,
                },
            ],
        });
        assert!((summary.messages_per_sec - (62_400.0 / 12.0)).abs() < 1e-9);
        assert!((summary.messages_per_sec - summary.pace_messages_per_sec).abs() < 1e-9);
        assert!((summary.saturation_messages_per_sec - 36_000.0).abs() < 1e-9);
        assert!(summary.messages_per_sec < summary.saturation_messages_per_sec);
        let text = format_report(
            &summary,
            disk_flush_for_url("amqp://admin:devpassword12@127.0.0.1:35673/%2f"),
        );
        assert!(text.contains("messages_per_sec=5200.00"));
        assert!(text.contains("throughput=paced"));
        assert!(!text.contains("throughput=consumer"));
        assert!(!text.contains("throughput=pipeline"));
        assert!(!text.contains("throughput=achieved"));
        assert!(text.contains("publisher confirm after that fsync"));
    }

    #[test]
    fn a_missed_step_still_scores_the_counted_offer_window() {
        // The fastest step is 37828 acks/s. messages_per_sec stays acks over
        // the counted windows (23209.80), and the report does not call that
        // peak the throughput.
        let rabbit = summarize(&SummaryInput {
            confirmed: 136_734,
            consumed: 136_734,
            elapsed_secs: 8.0,
            confirm_latency_ms: vec![2.0],
            uncapped: None,
            pipeline: false,
            score_sustained: false,
            steps: vec![
                LoadStep {
                    offered_per_sec: 4_000.0,
                    confirmed_per_sec: 3_997.0,
                    consumed_per_sec: 3_997.0,
                },
                LoadStep {
                    offered_per_sec: 16_000.0,
                    confirmed_per_sec: 15_900.0,
                    consumed_per_sec: 15_900.0,
                },
                LoadStep {
                    offered_per_sec: 32_000.0,
                    confirmed_per_sec: 23_943.0,
                    consumed_per_sec: 23_943.0,
                },
                LoadStep {
                    offered_per_sec: 96_000.0,
                    confirmed_per_sec: 24_527.0,
                    consumed_per_sec: 24_527.0,
                },
            ],
        });
        let rust = summarize(&SummaryInput {
            confirmed: 232_098,
            consumed: 232_098,
            elapsed_secs: 10.0,
            confirm_latency_ms: vec![6.0, 12.0],
            uncapped: None,
            pipeline: false,
            score_sustained: false,
            steps: vec![
                LoadStep {
                    offered_per_sec: 4_000.0,
                    confirmed_per_sec: 3_990.0,
                    consumed_per_sec: 3_990.0,
                },
                LoadStep {
                    offered_per_sec: 8_000.0,
                    confirmed_per_sec: 7_964.0,
                    consumed_per_sec: 7_964.0,
                },
                LoadStep {
                    offered_per_sec: 32_000.0,
                    confirmed_per_sec: 31_800.0,
                    consumed_per_sec: 31_800.0,
                },
                LoadStep {
                    offered_per_sec: 48_000.0,
                    confirmed_per_sec: 34_467.0,
                    consumed_per_sec: 34_467.0,
                },
                LoadStep {
                    offered_per_sec: 96_000.0,
                    confirmed_per_sec: 37_828.0,
                    consumed_per_sec: 37_828.0,
                },
            ],
        });
        assert!((rabbit.messages_per_sec - (136_734.0 / 8.0)).abs() < 1e-9);
        assert!((rabbit.messages_per_sec - rabbit.pace_messages_per_sec).abs() < 1e-9);
        assert!((rabbit.saturation_messages_per_sec - 24_527.0).abs() < 1e-9);
        assert!(rabbit.messages_per_sec < rabbit.saturation_messages_per_sec);
        assert!((rust.messages_per_sec - (232_098.0 / 10.0)).abs() < 1e-9);
        assert!((rust.messages_per_sec - rust.pace_messages_per_sec).abs() < 1e-9);
        assert!((rust.saturation_messages_per_sec - 37_828.0).abs() < 1e-9);
        assert!(rust.messages_per_sec < rust.saturation_messages_per_sec);
        assert!(37_828.0 / 24_527.0 > 1.3);
        assert!(!rabbit.pipeline && !rust.pipeline);
        assert!(!rabbit.kept_up);
        assert!(!rust.kept_up);
        assert!((rabbit.saturation_load - 32_000.0).abs() < 1e-9);
        assert!((rust.saturation_load - 48_000.0).abs() < 1e-9);
        let text = format_report(
            &rust,
            disk_flush_for_url("amqp://admin:devpassword12@127.0.0.1:35673/%2f"),
        );
        assert!(text.contains("messages_per_sec=23209.80"));
        assert!(text.contains("pace_messages_per_sec=23209.80"));
        assert!(text.contains("saturation_messages_per_sec=37828.00"));
        assert!(text.contains("throughput=paced"));
        assert!(text.contains("publisher confirm after that fsync"));
        assert!(!text.contains("throughput=consumer"));
        assert!(!text.contains("throughput=pipeline"));
        assert!(!text.contains("throughput=achieved"));
    }

    #[test]
    fn a_short_wall_cannot_raise_the_step_consumer_rate() {
        // 16000/s for 2s is 32000 acks. Finishing that quota in 0.872s is the
        // drain clock. The scored rate stays on the offered window.
        let drained = step_consumer_per_sec(32_000, 0.872, 2.0);
        assert!((drained - 16_000.0).abs() < 1e-6);
        assert!(drained < 32_000.0 / 0.872);
        // A step that runs past the window keeps the real clock.
        let held = step_consumer_per_sec(48_570, 2.20, 2.0);
        assert!((held - (48_570.0 / 2.20)).abs() < 1e-6);
    }

    #[test]
    fn a_fully_kept_ladder_stays_on_the_offered_window_mean() {
        let steps = vec![
            LoadStep {
                offered_per_sec: 4_000.0,
                confirmed_per_sec: 3_990.0,
                consumed_per_sec: 3_990.0,
            },
            LoadStep {
                offered_per_sec: 16_000.0,
                confirmed_per_sec: 15_965.0,
                consumed_per_sec: 15_965.0,
            },
        ];
        let summary = summarize(&SummaryInput {
            confirmed: 62_400,
            consumed: 62_400,
            elapsed_secs: 12.0,
            confirm_latency_ms: vec![6.0],
            uncapped: None,
            pipeline: false,
            score_sustained: false,
            steps,
        });
        assert!((summary.messages_per_sec - (62_400.0 / 12.0)).abs() < 1e-9);
        assert!((summary.messages_per_sec - summary.pace_messages_per_sec).abs() < 1e-9);
        assert!((summary.saturation_messages_per_sec - 15_965.0).abs() < 1e-9);
        assert!(summary.messages_per_sec < summary.saturation_messages_per_sec);
        assert!(summary.kept_up);
        assert!(!summary.pipeline);
        let text = format_report(
            &summary,
            disk_flush_for_url("amqp://admin:devpassword12@127.0.0.1:35674/%2f"),
        );
        assert!(text.starts_with("messages_per_sec=5200.00\n"));
        assert!(text.contains("pace_messages_per_sec=5200.00"));
        assert!(text.contains("throughput=paced"));
        assert!(!text.contains("throughput=consumer"));
        assert!(text.contains("saturation_messages_per_sec=15965.00"));
    }

    #[test]
    fn the_first_miss_and_the_drain_clock_are_not_messages_per_sec() {
        // Rabbit's first miss acks 23566/s and a later step acks 24527/s.
        // Rust's first miss acks 31408/s. That pair is above 1.3×, and it is
        // not the score. messages_per_sec stays acks over the counted windows.
        // A 0.872s drain of a 2s 16000/s quota scores 16000.
        let rabbit = summarize(&SummaryInput {
            confirmed: 100_000,
            consumed: 100_000,
            elapsed_secs: 6.0,
            confirm_latency_ms: vec![2.0],
            uncapped: None,
            pipeline: false,
            score_sustained: false,
            steps: vec![
                LoadStep {
                    offered_per_sec: 16_000.0,
                    confirmed_per_sec: 15_900.0,
                    consumed_per_sec: 15_900.0,
                },
                LoadStep {
                    offered_per_sec: 32_000.0,
                    confirmed_per_sec: 23_566.0,
                    consumed_per_sec: 23_566.0,
                },
                LoadStep {
                    offered_per_sec: 96_000.0,
                    confirmed_per_sec: 24_527.0,
                    consumed_per_sec: 24_527.0,
                },
            ],
        });
        let rust = summarize(&SummaryInput {
            confirmed: 200_000,
            consumed: 200_000,
            elapsed_secs: 8.0,
            confirm_latency_ms: vec![6.0, 12.0],
            uncapped: None,
            pipeline: false,
            score_sustained: false,
            steps: vec![
                LoadStep {
                    offered_per_sec: 32_000.0,
                    confirmed_per_sec: 31_586.0,
                    consumed_per_sec: 31_586.0,
                },
                LoadStep {
                    offered_per_sec: 48_000.0,
                    confirmed_per_sec: 31_408.0,
                    consumed_per_sec: 31_408.0,
                },
                LoadStep {
                    offered_per_sec: 96_000.0,
                    confirmed_per_sec: 35_322.0,
                    consumed_per_sec: 35_322.0,
                },
            ],
        });
        assert!((rabbit.messages_per_sec - (100_000.0 / 6.0)).abs() < 1e-9);
        assert!((rabbit.messages_per_sec - rabbit.pace_messages_per_sec).abs() < 1e-9);
        assert!((rabbit.messages_per_sec - 23_566.0).abs() > 1.0);
        assert!((rabbit.saturation_messages_per_sec - 24_527.0).abs() < 1e-9);
        assert!(rabbit.messages_per_sec < rabbit.saturation_messages_per_sec);
        assert!((rabbit.saturation_load - 32_000.0).abs() < 1e-9);
        assert!(!rabbit.kept_up);
        assert!(!rabbit.pipeline);
        assert!((rust.messages_per_sec - (200_000.0 / 8.0)).abs() < 1e-9);
        assert!((rust.messages_per_sec - 31_408.0).abs() > 1.0);
        assert!((rust.saturation_messages_per_sec - 35_322.0).abs() < 1e-9);
        assert!((rust.saturation_load - 48_000.0).abs() < 1e-9);
        let drained = step_consumer_per_sec(32_000, 0.872, 2.0);
        assert!((drained - 16_000.0).abs() < 1e-6);
        assert!(drained < 32_000.0 / 0.872);
        let text = format_report(
            &rust,
            disk_flush_for_url("amqp://admin:devpassword12@127.0.0.1:35673/%2f"),
        );
        assert!(text.starts_with("messages_per_sec=25000.00\n"));
        assert!(text.contains("pace_messages_per_sec=25000.00"));
        assert!(text.contains("throughput=paced"));
        assert!(text.contains("publisher confirm after that fsync"));
        assert!(!text.contains("throughput=saturation"));
        assert!(!text.contains("throughput=pipeline"));
        assert!(!text.contains("throughput=consumer"));
        assert!(!text.contains("throughput=achieved"));
    }

    #[test]
    fn messages_per_sec_is_acks_over_the_counted_windows() {
        // Rabbit keeps 16000 at 15900 acks/s and misses 32000 at 23566.
        // Rust keeps 32000 at 31586. The printed rate is acks over the
        // counted windows. The highest kept step, the first miss, and the
        // later peak stay out of messages_per_sec. A flag on the input does
        // not switch the score. A 0.872s drain of a 2s quota scores the window.
        let rabbit_steps = vec![
            LoadStep {
                offered_per_sec: 16_000.0,
                confirmed_per_sec: 15_900.0,
                consumed_per_sec: 15_900.0,
            },
            LoadStep {
                offered_per_sec: 32_000.0,
                confirmed_per_sec: 23_566.0,
                consumed_per_sec: 23_566.0,
            },
            LoadStep {
                offered_per_sec: 96_000.0,
                confirmed_per_sec: 24_527.0,
                consumed_per_sec: 24_527.0,
            },
        ];
        let rabbit = summarize(&SummaryInput {
            confirmed: 129_207,
            consumed: 129_207,
            elapsed_secs: 10.0,
            confirm_latency_ms: vec![2.0],
            uncapped: None,
            pipeline: false,
            score_sustained: true,
            steps: rabbit_steps.clone(),
        });
        let rust_steps = vec![
            LoadStep {
                offered_per_sec: 16_000.0,
                confirmed_per_sec: 15_980.0,
                consumed_per_sec: 15_980.0,
            },
            LoadStep {
                offered_per_sec: 32_000.0,
                confirmed_per_sec: 31_586.0,
                consumed_per_sec: 31_586.0,
            },
            LoadStep {
                offered_per_sec: 48_000.0,
                confirmed_per_sec: 31_408.0,
                consumed_per_sec: 31_408.0,
            },
            LoadStep {
                offered_per_sec: 96_000.0,
                confirmed_per_sec: 35_322.0,
                consumed_per_sec: 35_322.0,
            },
        ];
        let rust = summarize(&SummaryInput {
            confirmed: 171_366,
            consumed: 171_366,
            elapsed_secs: 10.0,
            confirm_latency_ms: vec![6.0, 12.0],
            uncapped: None,
            pipeline: false,
            score_sustained: true,
            steps: rust_steps.clone(),
        });
        assert!((sustained_consumer_per_sec(&rabbit_steps) - 15_900.0).abs() < 1e-9);
        assert!((sustained_consumer_per_sec(&rust_steps) - 31_586.0).abs() < 1e-9);
        assert!((consumer_rate_at_saturation(&rabbit_steps) - 23_566.0).abs() < 1e-9);
        assert!((consumer_rate_at_saturation(&rust_steps) - 31_408.0).abs() < 1e-9);
        assert!((rabbit.messages_per_sec - 12_920.7).abs() < 1e-9);
        assert!((rabbit.pace_messages_per_sec - rabbit.messages_per_sec).abs() < 1e-9);
        assert!((rabbit.messages_per_sec - 15_900.0).abs() > 1.0);
        assert!((rabbit.messages_per_sec - 23_566.0).abs() > 1.0);
        assert!((rabbit.saturation_messages_per_sec - 24_527.0).abs() < 1e-9);
        assert!((rabbit.saturation_load - 32_000.0).abs() < 1e-9);
        assert!(!rabbit.kept_up);
        assert!(!rabbit.score_sustained);
        assert!(!rabbit.pipeline);
        assert!((rust.messages_per_sec - 17_136.6).abs() < 1e-9);
        assert!((rust.pace_messages_per_sec - rust.messages_per_sec).abs() < 1e-9);
        assert!((rust.messages_per_sec - 31_586.0).abs() > 1.0);
        assert!((rust.messages_per_sec - 31_408.0).abs() > 1.0);
        assert!((rust.messages_per_sec - 35_322.0).abs() > 1.0);
        assert!((rust.saturation_messages_per_sec - 35_322.0).abs() < 1e-9);
        assert!((rust.saturation_load - 48_000.0).abs() < 1e-9);
        assert!(!rust.score_sustained);
        assert!(!rust.pipeline);
        assert!(rust.messages_per_sec / rabbit.messages_per_sec > 1.3);
        let drained = step_consumer_per_sec(32_000, 0.872, 2.0);
        assert!((drained - 16_000.0).abs() < 1e-6);
        assert!(drained < 32_000.0 / 0.872);
        let first_miss = sustained_consumer_per_sec(&[LoadStep {
            offered_per_sec: 200.0,
            confirmed_per_sec: 50.0,
            consumed_per_sec: 40.0,
        }]);
        assert!((first_miss - 40.0).abs() < 1e-9);
        let text = format_report(
            &rust,
            disk_flush_for_url("amqp://admin:devpassword12@127.0.0.1:35673/%2f"),
        );
        assert!(text.starts_with("messages_per_sec=17136.60\n"));
        assert!(text.contains("pace_messages_per_sec=17136.60"));
        assert!(text.contains("throughput=paced"));
        assert!(text.contains("publisher confirm after that fsync"));
        assert!(!text.contains("throughput=sustained"));
        assert!(!text.contains("throughput=saturation"));
        assert!(!text.contains("throughput=pipeline"));
        assert!(!text.contains("throughput=consumer"));
        assert!(!text.contains("throughput=achieved"));
    }

    #[test]
    fn a_fully_kept_ladder_scores_the_window_mean_when_the_flag_is_set() {
        let summary = summarize(&SummaryInput {
            confirmed: 62_400,
            consumed: 62_400,
            elapsed_secs: 12.0,
            confirm_latency_ms: vec![6.0],
            uncapped: None,
            pipeline: false,
            score_sustained: true,
            steps: vec![
                LoadStep {
                    offered_per_sec: 4_000.0,
                    confirmed_per_sec: 3_990.0,
                    consumed_per_sec: 3_990.0,
                },
                LoadStep {
                    offered_per_sec: 16_000.0,
                    confirmed_per_sec: 15_965.0,
                    consumed_per_sec: 15_965.0,
                },
            ],
        });
        assert!((summary.messages_per_sec - 5_200.0).abs() < 1e-9);
        assert!((summary.pace_messages_per_sec - 5_200.0).abs() < 1e-9);
        assert!((summary.messages_per_sec - 15_965.0).abs() > 1.0);
        assert!(summary.kept_up);
        assert!(!summary.score_sustained);
        assert!(!summary.pipeline);
        assert!((summary.saturation_load - 16_000.0).abs() < 1e-9);
        let text = format_report(
            &summary,
            disk_flush_for_url("amqp://admin:devpassword12@127.0.0.1:35674/%2f"),
        );
        assert!(text.starts_with("messages_per_sec=5200.00\n"));
        assert!(text.contains("pace_messages_per_sec=5200.00"));
        assert!(text.contains("throughput=paced"));
        assert!(!text.contains("throughput=sustained"));
        assert!(!text.contains("throughput=saturation"));
        assert!(!text.contains("throughput=pipeline"));
    }
}
