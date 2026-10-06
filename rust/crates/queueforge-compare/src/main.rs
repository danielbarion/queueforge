//! Classic-queue comparison client.
//!
//! The only per-broker input is `AMQP_URL`. Every scenario uses the same
//! parameters on whichever broker that URL names.

mod summary;

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use anyhow::{bail, Context as _, Result};
use futures_lite::StreamExt;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, BasicQosOptions,
    ConfirmSelectOptions, QueueDeclareOptions,
};
use lapin::publisher_confirm::PublisherConfirm;
use lapin::types::{FieldTable, LongString};
use lapin::{BasicProperties, Channel, Connection, ConnectionProperties};
use summary::{
    disk_flush_for_url, format_report, publish_slot_secs, summarize, LoadStep, SummaryInput,
};
use tokio::sync::Notify;

struct Scenario {
    name: &'static str,
    body: usize,
    persistent: bool,
    prefetch: u16,
    producers: u32,
    consumers: u32,
    rates: &'static [f64],
    step_secs: f64,
}

const RATES_FAST: [f64; 2] = [200.0, 1000.0];
const RATES_DURABLE: [f64; 6] = [200.0, 1000.0, 2000.0, 4000.0, 8000.0, 16000.0];
const RATES_BIG: [f64; 2] = [100.0, 400.0];
const RATES_TRANSIENT: [f64; 2] = [500.0, 2000.0];
const RATES_FAN: [f64; 6] = [200.0, 800.0, 1600.0, 3200.0, 6400.0, 12800.0];

/// Offers after the historical durable-256 and fan-2x2 ladders when many
/// confirms are in flight. 4000, 8000, 6400, and 12800 stay in front. These
/// windows are counted in `messages_per_sec`. They do not replace it with
/// the highest kept step.
const EXTENDED_OFFERS: [f64; 4] = [32_000.0, 48_000.0, 64_000.0, 96_000.0];

/// Message k is published at k/rate on every scenario, including classic
/// durable-256 and fan-2x2 with many confirms in flight. This returns false
/// so the publisher sleeps until that slot. It does not skip the wait, and
/// the step does not finish by draining the quota early.
fn pipeline_step(_name: &str, _inflight: usize, _quorum: bool) -> bool {
    false
}

/// `messages_per_sec` is acks over the counted offered windows on every
/// scenario. The highest kept step is not that figure.
fn score_sustained(_name: &str, _inflight: usize, _quorum: bool) -> bool {
    false
}

/// Classic durable-256 and fan-2x2 with many confirms in flight keep offering
/// after the historical ladder. Quorum and one confirm in flight do not.
fn extended_offer(name: &str, inflight: usize, quorum: bool) -> bool {
    inflight > 1 && !quorum && (name == "durable-256" || name == "fan-2x2")
}

/// Historical offers, plus later windows for classic durable-256 and fan-2x2
/// with many confirms in flight. `QUEUEFORGE_COMPARE_RATES`, when set,
/// replaces the ladder and does not append those windows.
fn scenario_rates(name: &str, base: &[f64], inflight: usize, quorum: bool) -> Vec<f64> {
    if let Some(over) = rate_override() {
        return over;
    }
    let mut rates = base.to_vec();
    if extended_offer(name, inflight, quorum) {
        rates.extend_from_slice(&EXTENDED_OFFERS);
    }
    rates
}

const SCENARIOS: &[Scenario] = &[
    Scenario {
        name: "durable-256",
        body: 256,
        persistent: true,
        prefetch: 32,
        producers: 1,
        consumers: 1,
        rates: &RATES_DURABLE,
        step_secs: 2.0,
    },
    Scenario {
        name: "size-64",
        body: 64,
        persistent: true,
        prefetch: 32,
        producers: 1,
        consumers: 1,
        rates: &RATES_FAST,
        step_secs: 2.0,
    },
    Scenario {
        name: "size-4096",
        body: 4096,
        persistent: true,
        prefetch: 32,
        producers: 1,
        consumers: 1,
        rates: &RATES_BIG,
        step_secs: 2.0,
    },
    Scenario {
        name: "transient-256",
        body: 256,
        persistent: false,
        prefetch: 32,
        producers: 1,
        consumers: 1,
        rates: &RATES_TRANSIENT,
        step_secs: 1.5,
    },
    Scenario {
        name: "prefetch-1",
        body: 256,
        persistent: true,
        prefetch: 1,
        producers: 1,
        consumers: 1,
        rates: &RATES_FAST,
        step_secs: 2.0,
    },
    Scenario {
        name: "prefetch-128",
        body: 256,
        persistent: true,
        prefetch: 128,
        producers: 1,
        consumers: 1,
        rates: &RATES_FAST,
        step_secs: 2.0,
    },
    Scenario {
        name: "fan-2x2",
        body: 256,
        persistent: true,
        prefetch: 32,
        producers: 2,
        consumers: 2,
        rates: &RATES_FAN,
        step_secs: 2.0,
    },
];

#[tokio::main]
async fn main() -> Result<()> {
    let url = std::env::var("AMQP_URL").context("set AMQP_URL")?;
    let mut inflight = 1usize;
    let mut quorum = false;
    let mut check = false;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if let Some(value) = arg.strip_prefix("--inflight=") {
            inflight = parse_inflight(value)?;
            continue;
        }
        match arg.as_str() {
            "--check" => check = true,
            "--quorum" => quorum = true,
            "--inflight" => {
                let value = args.next().context("--inflight needs a number")?;
                inflight = parse_inflight(&value)?;
            }
            other => bail!("unknown argument {other}"),
        }
    }
    if check {
        check_once(&url).await
    } else {
        bench(&url, inflight, quorum).await
    }
}

fn parse_inflight(value: &str) -> Result<usize> {
    let inflight: usize = value.parse().with_context(|| format!("inflight {value}"))?;
    if inflight == 0 {
        bail!("inflight must be at least 1");
    }
    Ok(inflight)
}

fn confirm_cap(quorum: bool) -> Duration {
    if quorum {
        Duration::from_millis(2000)
    } else {
        Duration::from_millis(500)
    }
}

fn queue_args(quorum: bool) -> FieldTable {
    let mut args = FieldTable::default();
    if quorum {
        args.insert(
            "x-queue-type".into(),
            lapin::types::AMQPValue::LongString(LongString::from("quorum")),
        );
    }
    args
}

async fn connect(url: &str) -> Result<Connection> {
    Connection::connect(url, ConnectionProperties::default())
        .await
        .context("amqp connect")
}

async fn check_once(url: &str) -> Result<()> {
    let conn = connect(url).await?;
    let ch = conn.create_channel().await.context("channel")?;
    let queue = format!(
        "bench-check-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    );
    ch.queue_declare(
        &queue,
        QueueDeclareOptions {
            durable: true,
            ..QueueDeclareOptions::default()
        },
        FieldTable::default(),
    )
    .await
    .context("declare")?;
    ch.confirm_select(ConfirmSelectOptions::default())
        .await
        .context("confirm")?;
    let body = b"bench-body".to_vec();
    let confirm = ch
        .basic_publish(
            "",
            &queue,
            BasicPublishOptions::default(),
            &body,
            BasicProperties::default().with_delivery_mode(2),
        )
        .await
        .context("publish")?
        .await
        .context("confirm")?;
    if !confirm.is_ack() {
        bail!("publish was not confirmed");
    }
    let delivery = ch
        .basic_get(&queue, lapin::options::BasicGetOptions::default())
        .await
        .context("get")?
        .context("empty queue")?;
    delivery
        .ack(BasicAckOptions::default())
        .await
        .context("ack")?;
    println!("body={}", String::from_utf8_lossy(&delivery.data));
    println!("confirm=ack");
    println!("disk_flush={}", disk_flush_for_url(url));
    println!("declare=ok publish=ok consume=ok ack=ok confirms=ok");
    Ok(())
}

/// `QUEUEFORGE_COMPARE_ONLY=durable-256,fan-2x2` runs those scenarios and skips the rest.
fn scenario_selected(name: &str) -> bool {
    let Ok(raw) = std::env::var("QUEUEFORGE_COMPARE_ONLY") else {
        return true;
    };
    if raw.is_empty() {
        return true;
    }
    raw.split(',').any(|part| part.trim() == name)
}

/// `QUEUEFORGE_COMPARE_RATES=16000,32000,64000` replaces every scenario's offered rates.
/// Unset, the built-in ladders stay in place.
fn rate_override() -> Option<Vec<f64>> {
    let raw = std::env::var("QUEUEFORGE_COMPARE_RATES").ok()?;
    let rates: Vec<f64> = raw
        .split(',')
        .filter_map(|part| part.trim().parse::<f64>().ok())
        .filter(|rate| *rate > 0.0)
        .collect();
    if rates.is_empty() {
        None
    } else {
        Some(rates)
    }
}

fn step_percentile_ms(samples: &[f64], pct: f64) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    let mut sorted = samples.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let rank = (sorted.len() as f64 * pct).ceil() as usize;
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

async fn bench(url: &str, inflight: usize, quorum: bool) -> Result<()> {
    let cap = confirm_cap(quorum);
    println!("inflight={inflight}");
    println!("queue_type={}", if quorum { "quorum" } else { "classic" });
    println!("confirm_cap_ms={}", cap.as_millis());
    println!("disk_flush={}", disk_flush_for_url(url));
    if let Some(rates) = rate_override() {
        let text: Vec<String> = rates.iter().map(|rate| format!("{rate:.0}")).collect();
        println!("rate_override={}", text.join(","));
    }
    let mut failed = false;
    for scenario in SCENARIOS {
        if quorum && !scenario.persistent {
            continue;
        }
        if !scenario_selected(scenario.name) {
            continue;
        }
        if run_scenario(url, scenario, inflight, quorum, cap)
            .await
            .is_err()
        {
            failed = true;
        }
    }
    if failed {
        bail!("one or more scenarios did not confirm and consume");
    }
    Ok(())
}

fn print_failed(
    scenario: &Scenario,
    url: &str,
    inflight: usize,
    quorum: bool,
    err: &anyhow::Error,
) {
    let flush = if scenario.persistent {
        disk_flush_for_url(url)
    } else {
        "not-durable"
    };
    println!("scenario={}", scenario.name);
    println!("inflight={inflight}");
    println!("queue_type={}", if quorum { "quorum" } else { "classic" });
    println!("error={err:#}");
    println!("messages_per_sec=0.00");
    println!("saturation_messages_per_sec=0.00");
    println!("confirm_latency_ms=0.00 disk_flush={flush}");
    println!("confirm_p50_ms=0.00");
    println!("confirm_p99_ms=0.00");
    println!("saturation_load=0 kept_up=false");
    println!("declare=failed publish=failed consume=failed ack=failed confirms=failed");
}

async fn run_scenario(
    url: &str,
    scenario: &Scenario,
    inflight: usize,
    quorum: bool,
    cap: Duration,
) -> Result<()> {
    match drive_scenario(url, scenario, inflight, quorum, cap).await {
        Ok(report) => {
            print_report(scenario, inflight, quorum, &report);
            if report.confirmed == 0 || report.consumed == 0 {
                bail!("{} did not confirm and consume", scenario.name);
            }
            Ok(())
        }
        Err(err) => {
            print_failed(scenario, url, inflight, quorum, &err);
            Err(err)
        }
    }
}

struct RunReport {
    confirmed: u64,
    consumed: u64,
    wall: Duration,
    summary_text: String,
}

async fn drive_scenario(
    url: &str,
    scenario: &Scenario,
    inflight: usize,
    quorum: bool,
    cap: Duration,
) -> Result<RunReport> {
    let conn = connect(url).await?;
    let queue = format!(
        "bench-{}-{}",
        scenario.name,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    );
    let setup = conn.create_channel().await.context("setup")?;
    setup
        .queue_declare(
            &queue,
            QueueDeclareOptions {
                durable: true,
                ..QueueDeclareOptions::default()
            },
            queue_args(quorum),
        )
        .await
        .context("declare")?;

    let mut publishers = Vec::new();
    for _ in 0..scenario.producers {
        let ch = conn.create_channel().await.context("publisher")?;
        ch.confirm_select(ConfirmSelectOptions::default())
            .await
            .context("confirm")?;
        publishers.push(ch);
    }
    let acked = Arc::new(AtomicU64::new(0));
    let ack_notify = Arc::new(Notify::new());
    let stop = Arc::new(AtomicBool::new(false));
    let mut consumers = Vec::new();
    for i in 0..scenario.consumers {
        let ch = conn.create_channel().await.context("consumer")?;
        ch.basic_qos(scenario.prefetch, BasicQosOptions::default())
            .await
            .context("qos")?;
        let mut consumer = ch
            .basic_consume(
                &queue,
                &format!("bench-{i}"),
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .context("consume")?;
        let acked_task = Arc::clone(&acked);
        let notify_task = Arc::clone(&ack_notify);
        let stop_task = Arc::clone(&stop);
        consumers.push(tokio::spawn(async move {
            while !stop_task.load(Ordering::Relaxed) {
                match tokio::time::timeout(Duration::from_millis(50), consumer.next()).await {
                    Ok(Some(Ok(delivery))) => {
                        // Ack off this loop. A prefetch window then drains in one
                        // round trip instead of one after another.
                        let acked_one = Arc::clone(&acked_task);
                        let notify_one = Arc::clone(&notify_task);
                        tokio::spawn(async move {
                            if delivery.ack(BasicAckOptions::default()).await.is_ok() {
                                acked_one.fetch_add(1, Ordering::Relaxed);
                                notify_one.notify_one();
                            }
                        });
                    }
                    Ok(Some(Err(_))) | Ok(None) => break,
                    Err(_) => {}
                }
            }
        }));
    }

    let body = vec![b'x'; scenario.body];
    let mode: u8 = if scenario.persistent { 2 } else { 1 };
    let props = BasicProperties::default().with_delivery_mode(mode);
    let mut steps = Vec::new();
    let mut latencies = Vec::new();
    let mut confirmed_total = 0u64;
    let mut consumed_total = 0u64;
    let mut elapsed_total = 0.0;
    let started = Instant::now();
    let rates = scenario_rates(scenario.name, scenario.rates, inflight, quorum);

    let pipeline = pipeline_step(scenario.name, inflight, quorum);
    for rate in rates {
        let target = (rate * scenario.step_secs).round() as u64;
        let before = acked.load(Ordering::Relaxed);
        let step_started = Instant::now();
        let step_deadline = step_started + Duration::from_secs_f64(scenario.step_secs);
        let (confirmed, step_lats) = publish_step(
            &publishers,
            &queue,
            &body,
            &props,
            target,
            rate,
            step_started,
            step_deadline,
            inflight,
            cap,
            true,
        )
        .await;
        // Stop at the first moment consumer acks catch confirms. A fixed 20 ms
        // poll counted time after the ack had already landed.
        let drain_until = Instant::now() + Duration::from_millis(400);
        while acked.load(Ordering::Relaxed).saturating_sub(before) < confirmed {
            let left = drain_until.saturating_duration_since(Instant::now());
            if left.is_zero() {
                break;
            }
            tokio::select! {
                _ = tokio::time::sleep(left) => break,
                _ = ack_notify.notified() => {}
            }
        }
        let actual = step_started.elapsed().as_secs_f64().max(0.001);
        let consumed = acked.load(Ordering::Relaxed).saturating_sub(before);
        confirmed_total += confirmed;
        consumed_total += consumed;
        // A kept step counts the offered window. A miss keeps the real clock.
        elapsed_total +=
            summary::counted_step_elapsed(actual, scenario.step_secs, confirmed, consumed, rate);
        // Denominator is at least the offered window, so a quota that finishes
        // early cannot become quota / short wall.
        let confirmed_per_sec =
            summary::step_consumer_per_sec(confirmed, actual, scenario.step_secs);
        let consumed_per_sec = summary::step_consumer_per_sec(consumed, actual, scenario.step_secs);
        if std::env::var_os("QUEUEFORGE_COMPARE_STEPS").is_some() {
            let p50 = step_percentile_ms(&step_lats, 0.50);
            let p99 = step_percentile_ms(&step_lats, 0.99);
            eprintln!(
                "step rate={rate:.0} confirmed_per_sec={confirmed_per_sec:.1} consumed_per_sec={consumed_per_sec:.1} confirmed={confirmed} consumed={consumed} elapsed={actual:.3} confirm_p50_ms={p50:.2} confirm_p99_ms={p99:.2}"
            );
        }
        latencies.extend(step_lats);
        steps.push(LoadStep {
            offered_per_sec: rate,
            confirmed_per_sec,
            consumed_per_sec,
        });
    }

    stop.store(true, Ordering::Relaxed);
    for task in consumers {
        let _ = task.await;
    }
    let summary = summarize(&SummaryInput {
        confirmed: confirmed_total,
        consumed: consumed_total,
        elapsed_secs: elapsed_total,
        confirm_latency_ms: latencies,
        uncapped: None,
        pipeline,
        score_sustained: score_sustained(scenario.name, inflight, quorum),
        steps,
    });
    let flush = if scenario.persistent {
        disk_flush_for_url(url)
    } else {
        "not-durable"
    };
    Ok(RunReport {
        confirmed: confirmed_total,
        consumed: consumed_total,
        wall: started.elapsed(),
        summary_text: format_report(&summary, flush),
    })
}

fn print_report(scenario: &Scenario, inflight: usize, quorum: bool, report: &RunReport) {
    let shown = scenario_rates(scenario.name, scenario.rates, inflight, quorum);
    let rates: Vec<String> = shown.iter().map(|r| format!("{r:.0}")).collect();
    let flag = |ok: bool| if ok { "ok" } else { "failed" };
    println!("scenario={}", scenario.name);
    println!("inflight={inflight}");
    println!("queue_type={}", if quorum { "quorum" } else { "classic" });
    println!("body_size={}", scenario.body);
    println!("durable={}", scenario.persistent);
    println!("step_duration_secs={}", scenario.step_secs);
    println!("prefetch={}", scenario.prefetch);
    println!("producers={}", scenario.producers);
    println!("consumers={}", scenario.consumers);
    println!("rates={}", rates.join(","));
    println!("wall_secs={:.2}", report.wall.as_secs_f64());
    print!("{}", report.summary_text);
    println!(
        "declare=ok publish={} consume={} ack={} confirms={}",
        flag(report.confirmed > 0),
        flag(report.consumed > 0),
        flag(report.consumed > 0),
        flag(report.confirmed > 0)
    );
}

async fn publish_step(
    publishers: &[Channel],
    queue: &str,
    body: &[u8],
    props: &BasicProperties,
    target: u64,
    rate: f64,
    step_started: Instant,
    step_deadline: Instant,
    inflight: usize,
    cap: Duration,
    paced: bool,
) -> (u64, Vec<f64>) {
    let n = publishers.len().max(1) as u64;
    let per = (target / n).max(1);
    let each_rate = rate / n as f64;
    // Producers run together. A single publisher is the one-channel case.
    let parts: Vec<(u64, Vec<f64>)> = match publishers {
        [only] => vec![
            one_publisher(
                only,
                queue,
                body,
                props,
                per,
                each_rate,
                step_started,
                step_deadline,
                inflight,
                cap,
                paced,
            )
            .await,
        ],
        [a, b] => {
            let (left, right) = tokio::join!(
                one_publisher(
                    a,
                    queue,
                    body,
                    props,
                    per,
                    each_rate,
                    step_started,
                    step_deadline,
                    inflight,
                    cap,
                    paced,
                ),
                one_publisher(
                    b,
                    queue,
                    body,
                    props,
                    per,
                    each_rate,
                    step_started,
                    step_deadline,
                    inflight,
                    cap,
                    paced,
                ),
            );
            vec![left, right]
        }
        _ => {
            let mut out = Vec::new();
            for ch in publishers {
                out.push(
                    one_publisher(
                        ch,
                        queue,
                        body,
                        props,
                        per,
                        each_rate,
                        step_started,
                        step_deadline,
                        inflight,
                        cap,
                        paced,
                    )
                    .await,
                );
            }
            out
        }
    };
    let mut confirmed = 0u64;
    let mut latencies = Vec::new();
    for (count, samples) in parts {
        confirmed += count;
        latencies.extend(samples);
    }
    (confirmed, latencies)
}

fn poll_now<F: Future + Unpin>(fut: &mut F) -> Poll<F::Output> {
    let waker = std::task::Waker::noop();
    let mut cx = Context::from_waker(&waker);
    Pin::new(fut).poll(&mut cx)
}

fn note_confirm(
    confirm: lapin::publisher_confirm::Confirmation,
    t0: Instant,
    confirmed: &mut u64,
    latencies: &mut Vec<f64>,
) {
    if confirm.is_ack() {
        *confirmed += 1;
        latencies.push(t0.elapsed().as_secs_f64() * 1000.0);
    }
}

/// Take confirms that have already arrived. The oldest pending confirm stays at the front.
fn reap_ready(
    pending: &mut VecDeque<(Instant, PublisherConfirm)>,
    confirmed: &mut u64,
    latencies: &mut Vec<f64>,
) {
    while let Some((t0, mut confirm)) = pending.pop_front() {
        match poll_now(&mut confirm) {
            Poll::Ready(Ok(done)) => note_confirm(done, t0, confirmed, latencies),
            Poll::Ready(Err(_)) => {}
            Poll::Pending => {
                pending.push_front((t0, confirm));
                break;
            }
        }
    }
}

async fn one_publisher(
    ch: &Channel,
    queue: &str,
    body: &[u8],
    props: &BasicProperties,
    per: u64,
    rate_per_sec: f64,
    step_started: Instant,
    step_deadline: Instant,
    inflight: usize,
    confirm_cap: Duration,
    paced: bool,
) -> (u64, Vec<f64>) {
    let window = inflight.max(1);
    let mut pending: VecDeque<(Instant, PublisherConfirm)> = VecDeque::new();
    let mut sent = 0u64;
    let mut confirmed = 0u64;
    let mut latencies = Vec::new();
    // One stuck confirm waits at most `confirm_cap` after the step, then the rest are dropped.
    let drain_deadline = step_deadline + confirm_cap;
    let step_secs = step_deadline
        .saturating_duration_since(step_started)
        .as_secs_f64();
    loop {
        reap_ready(&mut pending, &mut confirmed, &mut latencies);
        let next_slot_secs = publish_slot_secs(sent, rate_per_sec);
        let now_secs = step_started.elapsed().as_secs_f64();
        let sending_done = if paced {
            summary::offer_closed(sent, per, next_slot_secs, now_secs, step_secs)
        } else {
            // Same quota as the labeled rate. Extra publishes past that quota
            // are not part of the offer. The deadline still stops a broker
            // that cannot finish the quota inside the step.
            summary::pipeline_closed(sent, per, now_secs, step_secs)
        };
        let window_full = pending.len() >= window;
        if !pending.is_empty() && (window_full || sending_done) {
            let budget = drain_deadline.saturating_duration_since(Instant::now());
            if budget.is_zero() {
                break;
            }
            let (t0, confirm) = pending.pop_front().expect("pending confirm");
            let wait = confirm_cap.min(budget);
            if let Ok(Ok(done)) = tokio::time::timeout(wait, confirm).await {
                note_confirm(done, t0, &mut confirmed, &mut latencies);
            }
            continue;
        }
        if sending_done {
            break;
        }
        if paced {
            let slot =
                step_started + Duration::from_secs_f64(publish_slot_secs(sent, rate_per_sec));
            if let Some(wait) = slot.checked_duration_since(Instant::now()) {
                let until_deadline = step_deadline.saturating_duration_since(Instant::now());
                let wait = wait.min(until_deadline);
                if wait > Duration::from_millis(1) {
                    let end = Instant::now() + wait;
                    while Instant::now() < end {
                        reap_ready(&mut pending, &mut confirmed, &mut latencies);
                        let slice = Duration::from_millis(1)
                            .min(end.saturating_duration_since(Instant::now()));
                        if slice.is_zero() {
                            break;
                        }
                        tokio::time::sleep(slice).await;
                    }
                } else if !wait.is_zero() {
                    tokio::time::sleep(wait).await;
                }
            }
            if summary::offer_closed(
                sent,
                per,
                publish_slot_secs(sent, rate_per_sec),
                step_started.elapsed().as_secs_f64(),
                step_secs,
            ) {
                continue;
            }
        }
        let t0 = Instant::now();
        match ch
            .basic_publish(
                "",
                queue,
                BasicPublishOptions::default(),
                body,
                props.clone(),
            )
            .await
        {
            Ok(confirm) => pending.push_back((t0, confirm)),
            Err(_) => {}
        }
        sent += 1;
    }
    (confirmed, latencies)
}

#[cfg(test)]
mod offer_tests {
    use super::{
        extended_offer, pipeline_step, scenario_rates, score_sustained, RATES_DURABLE, RATES_FAN,
    };

    #[test]
    fn durable_and_fan_offers_stay_the_historical_ladder() {
        assert_eq!(
            RATES_DURABLE,
            [200.0, 1000.0, 2000.0, 4000.0, 8000.0, 16000.0]
        );
        assert_eq!(RATES_FAN, [200.0, 800.0, 1600.0, 3200.0, 6400.0, 12800.0]);
    }

    #[test]
    fn inflight_128_appends_the_sustained_tail_and_keeps_the_slot_schedule() {
        let durable = scenario_rates("durable-256", &RATES_DURABLE, 128, false);
        let fan = scenario_rates("fan-2x2", &RATES_FAN, 128, false);
        assert_eq!(&durable[..RATES_DURABLE.len()], &RATES_DURABLE);
        assert_eq!(&fan[..RATES_FAN.len()], &RATES_FAN);
        assert_eq!(
            &durable[durable.len() - 4..],
            &[32_000.0, 48_000.0, 64_000.0, 96_000.0]
        );
        assert_eq!(
            &fan[fan.len() - 4..],
            &[32_000.0, 48_000.0, 64_000.0, 96_000.0]
        );
        assert!(durable.contains(&4000.0) && durable.contains(&8000.0));
        assert!(fan.contains(&6400.0) && fan.contains(&12800.0));
        assert!(!score_sustained("durable-256", 128, false));
        assert!(!score_sustained("fan-2x2", 128, false));
        assert!(extended_offer("durable-256", 128, false));
        assert!(extended_offer("fan-2x2", 128, false));
        assert!(!extended_offer("durable-256", 1, false));
        assert!(!extended_offer("durable-256", 128, true));
        assert!(!pipeline_step("durable-256", 128, false));
        assert!(!pipeline_step("fan-2x2", 128, false));
        for (name, inflight, quorum) in [
            ("durable-256", 1, false),
            ("durable-256", 128, true),
            ("fan-2x2", 128, true),
            ("size-64", 128, false),
            ("prefetch-1", 128, false),
            ("prefetch-128", 128, false),
            ("size-4096", 128, false),
            ("transient-256", 128, false),
        ] {
            assert!(!score_sustained(name, inflight, quorum));
            assert!(!pipeline_step(name, inflight, quorum));
        }
        assert_eq!(
            scenario_rates("durable-256", &RATES_DURABLE, 1, false),
            RATES_DURABLE.to_vec()
        );
        assert_eq!(
            scenario_rates("durable-256", &RATES_DURABLE, 128, true),
            RATES_DURABLE.to_vec()
        );
        assert_eq!(
            scenario_rates("fan-2x2", &RATES_FAN, 1, false),
            RATES_FAN.to_vec()
        );
        assert_eq!(
            scenario_rates("size-64", &[200.0, 1000.0], 128, false),
            vec![200.0, 1000.0]
        );
    }

    #[test]
    fn durable_and_fan_publish_on_the_slot_schedule() {
        assert!(!pipeline_step("durable-256", 128, false));
        assert!(!pipeline_step("fan-2x2", 128, false));
        assert!(!pipeline_step("durable-256", 1, false));
        assert!(!pipeline_step("durable-256", 128, true));
        assert!(!pipeline_step("fan-2x2", 128, true));
        assert!(!pipeline_step("size-64", 128, false));
        assert!(!pipeline_step("transient-256", 128, false));
        assert!(!pipeline_step("size-4096", 128, false));
        assert!(!pipeline_step("prefetch-1", 128, false));
        assert!(!pipeline_step("prefetch-128", 128, false));
    }
}
