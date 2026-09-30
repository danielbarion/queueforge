//! `queueforge-bench` — client-side load harness for QueueForge hypothesis SLOs.
//!
//! Load shapes (see `docs/PERFORMANCE.md`):
//! - **A** — 1 producer, 1 consumer, 1 queue (baseline)
//! - **B** — 32 producers, 32 consumers, 1 queue
//! - **C** — 1 producer, fanout 1→10 queues, 1 consumer each
//! - **D** — durable + publisher confirms (shape A topology)
//! - **E** — management list/poll concurrent with shape B
//! - **F** — priority mix 20% p=9 / 80% p=0 on `x-max-priority=9` (optional; needs PR 11b)
//!
//! Requires a running broker (and management HTTP for shape E).

mod shapes;
mod stats;

use std::time::Duration;

use anyhow::{bail, Context, Result};
use clap::{Parser, ValueEnum};
use tracing::info;

use shapes::RunConfig;
use stats::{check_hypothesis_slos, Report};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
#[value(rename_all = "UPPER")]
enum Shape {
    A,
    B,
    C,
    D,
    E,
    F,
    /// Run A–E (skips F unless `--include-f`).
    #[value(name = "all")]
    All,
}

#[derive(Debug, Parser)]
#[command(
    name = "queueforge-bench",
    about = "QueueForge load-shape benchmark harness (shapes A–F)",
    version
)]
struct Cli {
    /// AMQP URI (vhost `/` URL-encoded as %2f).
    #[arg(
        long,
        env = "QUEUEFORGE_BENCH_URI",
        default_value = "amqp://admin:devpassword12@127.0.0.1:5672/%2f"
    )]
    uri: String,

    /// Consumer AMQP URI. When set, publish stays on `--uri` and consume uses this URI.
    #[arg(long)]
    consume_uri: Option<String>,

    /// Load shape to run.
    #[arg(long, short = 's', value_enum, default_value_t = Shape::A)]
    shape: Shape,

    /// Include optional shape F when `--shape all`.
    #[arg(long, default_value_t = false)]
    include_f: bool,

    /// Measurement window (seconds).
    #[arg(long, short = 'd', default_value_t = 30)]
    duration_secs: u64,

    /// Warmup before measurement (seconds).
    #[arg(long, default_value_t = 3)]
    warmup_secs: u64,

    /// Message body size in bytes (8-byte timestamp prefix + padding).
    #[arg(long, default_value_t = 1024)]
    body_size: usize,

    /// Consumer prefetch (basic.qos). 0 is unlimited.
    ///
    /// The default is high enough that a same-connection publisher does not
    /// stall the consumer behind a 100-unacked round trip.
    #[arg(long, default_value_t = 1000)]
    prefetch: u16,

    /// Queue / exchange name prefix.
    #[arg(long, default_value = "qf-bench")]
    queue_prefix: String,

    /// Management base URL for shape E (login + list queues).
    #[arg(
        long,
        env = "QUEUEFORGE_BENCH_MGMT_URL",
        default_value = "http://127.0.0.1:15672"
    )]
    mgmt_url: String,

    /// Management username.
    #[arg(long, default_value = "admin")]
    mgmt_user: String,

    /// Management password.
    #[arg(long, default_value = "devpassword12")]
    mgmt_password: String,

    /// Interval between management list polls (milliseconds).
    #[arg(long, default_value_t = 50)]
    mgmt_poll_ms: u64,

    /// Assumed group-commit fsync interval (ms) for shape D confirm SLO.
    #[arg(long, default_value_t = 100)]
    fsync_interval_ms: u64,

    /// Exit non-zero if any hypothesis SLO is missed.
    #[arg(long, default_value_t = false)]
    check_slo: bool,

    /// Log filter (RUST_LOG style).
    #[arg(long, default_value = "info")]
    log: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&cli.log)),
        )
        .with_target(false)
        .init();

    if cli.body_size < 8 {
        bail!("--body-size must be >= 8 (timestamp prefix)");
    }
    if cli.duration_secs == 0 {
        bail!("--duration-secs must be > 0");
    }

    let cfg = RunConfig {
        uri: cli.uri.clone(),
        consume_uri: cli.consume_uri.clone(),
        duration: Duration::from_secs(cli.duration_secs),
        warmup: Duration::from_secs(cli.warmup_secs),
        body_size: cli.body_size,
        prefetch: cli.prefetch,
        queue_prefix: cli.queue_prefix.clone(),
        mgmt_url: Some(cli.mgmt_url.clone()),
        mgmt_user: cli.mgmt_user.clone(),
        mgmt_password: cli.mgmt_password.clone(),
        mgmt_poll_interval: Duration::from_millis(cli.mgmt_poll_ms),
        fsync_interval_ms: cli.fsync_interval_ms,
    };

    println!("queueforge-bench");
    println!("  uri:          {}", cfg.uri);
    if let Some(consume) = &cfg.consume_uri {
        println!("  consume_uri:  {consume}");
    }
    println!(
        "  duration:     {}s (warmup {}s)",
        cli.duration_secs, cli.warmup_secs
    );
    println!("  body_size:    {} B", cfg.body_size);
    println!("  prefetch:     {}", cfg.prefetch);

    let shapes = selected_shapes(cli.shape, cli.include_f);
    let mut all_reports: Vec<(String, Report)> = Vec::new();
    let mut any_slo_miss = false;

    for shape in shapes {
        info!(shape, "starting shape");
        let (stats, elapsed) = run_shape(shape, &cfg)
            .await
            .with_context(|| format!("shape {shape} failed"))?;
        let report = stats.snapshot(elapsed);
        report.print(shape);
        all_reports.push((shape.to_string(), report.clone()));

        let checks = check_hypothesis_slos(shape, &report, cfg.fsync_interval_ms);
        if !checks.is_empty() {
            println!("  hypothesis SLOs:");
            for c in &checks {
                let mark = if c.ok { "PASS" } else { "MISS" };
                println!("    [{mark}] {} — {}", c.name, c.detail);
                if !c.ok {
                    any_slo_miss = true;
                }
            }
        }
    }

    println!();
    println!("=== Summary ===");
    for (name, r) in &all_reports {
        println!(
            "  {name}: ingress={:.0} msg/s  consume={:.0} msg/s  e2e_p99={:?}  pub_p99={:?}",
            r.publish_rate, r.consume_rate, r.e2e_lat.p99, r.publish_lat.p99
        );
    }

    if cli.check_slo && any_slo_miss {
        bail!("one or more hypothesis SLOs missed (see above)");
    }
    Ok(())
}

fn selected_shapes(shape: Shape, include_f: bool) -> Vec<&'static str> {
    match shape {
        Shape::A => vec!["A"],
        Shape::B => vec!["B"],
        Shape::C => vec!["C"],
        Shape::D => vec!["D"],
        Shape::E => vec!["E"],
        Shape::F => vec!["F"],
        Shape::All => {
            let mut v = vec!["A", "B", "C", "D", "E"];
            if include_f {
                v.push("F");
            }
            v
        }
    }
}

async fn run_shape(
    shape: &str,
    cfg: &RunConfig,
) -> Result<(std::sync::Arc<stats::Stats>, Duration)> {
    // drive_timed is inside each shape; shapes return Stats after the full window.
    // We reconstruct elapsed from cfg.duration (measure window only).
    let stats = match shape {
        "A" => shapes::run_a(cfg).await?,
        "B" => shapes::run_b(cfg).await?,
        "C" => shapes::run_c(cfg).await?,
        "D" => shapes::run_d(cfg).await?,
        "E" => shapes::run_e(cfg).await?,
        "F" => shapes::run_f(cfg).await?,
        other => bail!("unknown shape {other}"),
    };
    Ok((stats, cfg.duration))
}
