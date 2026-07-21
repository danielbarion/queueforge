//! Latency / throughput aggregation for bench runs.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Duration;

use hdrhistogram::Histogram;

/// Shared counters + latency histogram for one shape run.
pub struct Stats {
    /// Messages successfully published (ingress).
    pub published: AtomicU64,
    /// Messages successfully consumed + acked.
    pub consumed: AtomicU64,
    /// Publish/confirm failures.
    pub publish_errors: AtomicU64,
    /// Consume/ack failures.
    pub consume_errors: AtomicU64,
    /// Management list requests completed.
    pub mgmt_requests: AtomicU64,
    /// Management list failures.
    pub mgmt_errors: AtomicU64,
    /// Publish→confirm latency (or publish completion when confirms off), nanoseconds.
    publish_lat_ns: Mutex<Histogram<u64>>,
    /// Publish→consume E2E latency (timestamp-in-body), nanoseconds.
    e2e_lat_ns: Mutex<Histogram<u64>>,
    /// Management list latency, nanoseconds.
    mgmt_lat_ns: Mutex<Histogram<u64>>,
}

impl Stats {
    pub fn new() -> Self {
        Self {
            published: AtomicU64::new(0),
            consumed: AtomicU64::new(0),
            publish_errors: AtomicU64::new(0),
            consume_errors: AtomicU64::new(0),
            mgmt_requests: AtomicU64::new(0),
            mgmt_errors: AtomicU64::new(0),
            // 1 ns .. ~1 hour, 3 sig figs
            publish_lat_ns: Mutex::new(
                Histogram::new_with_bounds(1, 3_600_000_000_000, 3).unwrap(),
            ),
            e2e_lat_ns: Mutex::new(Histogram::new_with_bounds(1, 3_600_000_000_000, 3).unwrap()),
            mgmt_lat_ns: Mutex::new(Histogram::new_with_bounds(1, 3_600_000_000_000, 3).unwrap()),
        }
    }

    pub fn record_publish_latency(&self, d: Duration) {
        let ns = d.as_nanos().min(u128::from(u64::MAX)) as u64;
        if ns > 0 {
            let _ = self.publish_lat_ns.lock().unwrap().record(ns);
        }
    }

    pub fn record_e2e_latency(&self, d: Duration) {
        let ns = d.as_nanos().min(u128::from(u64::MAX)) as u64;
        if ns > 0 {
            let _ = self.e2e_lat_ns.lock().unwrap().record(ns);
        }
    }

    pub fn record_mgmt_latency(&self, d: Duration) {
        let ns = d.as_nanos().min(u128::from(u64::MAX)) as u64;
        if ns > 0 {
            let _ = self.mgmt_lat_ns.lock().unwrap().record(ns);
        }
    }

    pub fn snapshot(&self, elapsed: Duration) -> Report {
        let published = self.published.load(Ordering::Relaxed);
        let consumed = self.consumed.load(Ordering::Relaxed);
        let secs = elapsed.as_secs_f64().max(1e-9);
        Report {
            elapsed,
            published,
            consumed,
            publish_errors: self.publish_errors.load(Ordering::Relaxed),
            consume_errors: self.consume_errors.load(Ordering::Relaxed),
            mgmt_requests: self.mgmt_requests.load(Ordering::Relaxed),
            mgmt_errors: self.mgmt_errors.load(Ordering::Relaxed),
            publish_rate: published as f64 / secs,
            consume_rate: consumed as f64 / secs,
            publish_lat: lat_summary(&self.publish_lat_ns.lock().unwrap()),
            e2e_lat: lat_summary(&self.e2e_lat_ns.lock().unwrap()),
            mgmt_lat: lat_summary(&self.mgmt_lat_ns.lock().unwrap()),
        }
    }
}

impl Default for Stats {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct LatencySummary {
    pub count: u64,
    pub p50: Duration,
    pub p95: Duration,
    pub p99: Duration,
    pub max: Duration,
}

impl LatencySummary {
    fn empty() -> Self {
        Self {
            count: 0,
            p50: Duration::ZERO,
            p95: Duration::ZERO,
            p99: Duration::ZERO,
            max: Duration::ZERO,
        }
    }
}

fn lat_summary(h: &Histogram<u64>) -> LatencySummary {
    if h.is_empty() {
        return LatencySummary::empty();
    }
    LatencySummary {
        count: h.len(),
        p50: Duration::from_nanos(h.value_at_quantile(0.50)),
        p95: Duration::from_nanos(h.value_at_quantile(0.95)),
        p99: Duration::from_nanos(h.value_at_quantile(0.99)),
        max: Duration::from_nanos(h.max()),
    }
}

/// Final report for one shape.
#[derive(Debug, Clone)]
pub struct Report {
    pub elapsed: Duration,
    pub published: u64,
    pub consumed: u64,
    pub publish_errors: u64,
    pub consume_errors: u64,
    pub mgmt_requests: u64,
    pub mgmt_errors: u64,
    pub publish_rate: f64,
    pub consume_rate: f64,
    pub publish_lat: LatencySummary,
    pub e2e_lat: LatencySummary,
    pub mgmt_lat: LatencySummary,
}

impl Report {
    pub fn print(&self, shape_name: &str) {
        println!();
        println!("=== Shape {shape_name} results ===");
        println!("  duration:         {:.2}s", self.elapsed.as_secs_f64());
        println!(
            "  published:        {}  ({:.0} msg/s ingress)",
            self.published, self.publish_rate
        );
        println!(
            "  consumed:         {}  ({:.0} msg/s)",
            self.consumed, self.consume_rate
        );
        if self.publish_errors > 0 || self.consume_errors > 0 {
            println!(
                "  errors:           publish={} consume={}",
                self.publish_errors, self.consume_errors
            );
        }
        print_lat("publish/confirm latency", &self.publish_lat);
        print_lat("E2E publish→consume", &self.e2e_lat);
        if self.mgmt_requests > 0 || self.mgmt_errors > 0 {
            println!(
                "  mgmt list calls:  {} (errors={})",
                self.mgmt_requests, self.mgmt_errors
            );
            print_lat("mgmt list latency", &self.mgmt_lat);
        }
    }
}

fn print_lat(label: &str, s: &LatencySummary) {
    if s.count == 0 {
        println!("  {label}:  (no samples)");
        return;
    }
    println!(
        "  {label}:  n={}  p50={}  p95={}  p99={}  max={}",
        s.count,
        fmt_dur(s.p50),
        fmt_dur(s.p95),
        fmt_dur(s.p99),
        fmt_dur(s.max)
    );
}

fn fmt_dur(d: Duration) -> String {
    let ns = d.as_nanos();
    if ns < 1_000 {
        format!("{ns}ns")
    } else if ns < 1_000_000 {
        format!("{:.1}µs", ns as f64 / 1_000.0)
    } else if ns < 1_000_000_000 {
        format!("{:.2}ms", ns as f64 / 1_000_000.0)
    } else {
        format!("{:.2}s", ns as f64 / 1_000_000_000.0)
    }
}

/// Hypothesis SLO check result.
#[derive(Debug)]
pub struct SloCheck {
    pub name: &'static str,
    pub ok: bool,
    pub detail: String,
}

/// Evaluate hypothesis SLOs for a shape (see docs/PERFORMANCE.md).
pub fn check_hypothesis_slos(
    shape: &str,
    report: &Report,
    fsync_interval_ms: u64,
) -> Vec<SloCheck> {
    let mut out = Vec::new();
    match shape {
        "A" => {
            out.push(rate_slo(
                "A transient throughput (stretch ≥200k msg/s)",
                report.publish_rate,
                200_000.0,
            ));
            out.push(lat_slo(
                "A E2E publish latency p99 ≤ 1 ms",
                report.e2e_lat.p99,
                Duration::from_millis(1),
                report.e2e_lat.count > 0,
            ));
        }
        "B" => {
            out.push(rate_slo(
                "B multi-conn throughput ≥ 100k msg/s",
                report.publish_rate,
                100_000.0,
            ));
        }
        "C" => {
            out.push(rate_slo(
                "C fanout ingress ≥ 50k msg/s",
                report.publish_rate,
                50_000.0,
            ));
        }
        "D" => {
            out.push(rate_slo(
                "D persistent+confirms ≥ 50k msg/s",
                report.publish_rate,
                50_000.0,
            ));
            let budget = Duration::from_millis(fsync_interval_ms.saturating_add(2));
            out.push(lat_slo(
                "D durable confirm p99 ≤ fsync_interval + 2 ms",
                report.publish_lat.p99,
                budget,
                report.publish_lat.count > 0,
            ));
        }
        "E" => {
            out.push(rate_slo(
                "E multi-conn throughput ≥ 100k msg/s (with mgmt load)",
                report.publish_rate,
                100_000.0,
            ));
            out.push(lat_slo(
                "E management list p99 ≤ 50 ms",
                report.mgmt_lat.p99,
                Duration::from_millis(50),
                report.mgmt_lat.count > 0,
            ));
        }
        "F" => {
            // No hard rate SLO yet — observational shape for priority overhead.
            out.push(SloCheck {
                name: "F priority mix (observational; no contractual rate SLO)",
                ok: report.published > 0 && report.consumed > 0,
                detail: format!(
                    "published={} consumed={} rate={:.0} msg/s e2e_p99={}",
                    report.published,
                    report.consumed,
                    report.publish_rate,
                    fmt_dur(report.e2e_lat.p99)
                ),
            });
        }
        _ => {}
    }
    out
}

fn rate_slo(name: &'static str, actual: f64, target: f64) -> SloCheck {
    let ok = actual >= target;
    SloCheck {
        name,
        ok,
        detail: format!("{actual:.0} msg/s vs target {target:.0} msg/s"),
    }
}

fn lat_slo(name: &'static str, actual: Duration, budget: Duration, has_samples: bool) -> SloCheck {
    if !has_samples {
        return SloCheck {
            name,
            ok: false,
            detail: "no samples".into(),
        };
    }
    let ok = actual <= budget;
    SloCheck {
        name,
        ok,
        detail: format!("{} vs budget {}", fmt_dur(actual), fmt_dur(budget)),
    }
}
