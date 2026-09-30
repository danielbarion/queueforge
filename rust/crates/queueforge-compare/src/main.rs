//! Classic-queue comparison client.
//!
//! The only per-broker input is `AMQP_URL`. Every scenario uses the same
//! parameters on whichever broker that URL names.

mod summary;

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use futures_lite::StreamExt;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, BasicQosOptions, ConfirmSelectOptions,
    QueueDeclareOptions,
};
use lapin::types::FieldTable;
use lapin::{BasicProperties, Channel, Connection, ConnectionProperties};
use summary::{disk_flush_for_url, format_report, publish_slot_secs, summarize, LoadStep, SummaryInput};

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
const RATES_BIG: [f64; 2] = [100.0, 400.0];
const RATES_TRANSIENT: [f64; 2] = [500.0, 2000.0];
const RATES_FAN: [f64; 2] = [200.0, 800.0];

const SCENARIOS: &[Scenario] = &[
    Scenario { name: "durable-256", body: 256, persistent: true, prefetch: 32, producers: 1, consumers: 1, rates: &RATES_FAST, step_secs: 2.0 },
    Scenario { name: "size-64", body: 64, persistent: true, prefetch: 32, producers: 1, consumers: 1, rates: &RATES_FAST, step_secs: 2.0 },
    Scenario { name: "size-4096", body: 4096, persistent: true, prefetch: 32, producers: 1, consumers: 1, rates: &RATES_BIG, step_secs: 2.0 },
    Scenario { name: "transient-256", body: 256, persistent: false, prefetch: 32, producers: 1, consumers: 1, rates: &RATES_TRANSIENT, step_secs: 1.5 },
    Scenario { name: "prefetch-1", body: 256, persistent: true, prefetch: 1, producers: 1, consumers: 1, rates: &RATES_FAST, step_secs: 2.0 },
    Scenario { name: "prefetch-128", body: 256, persistent: true, prefetch: 128, producers: 1, consumers: 1, rates: &RATES_FAST, step_secs: 2.0 },
    Scenario { name: "fan-2x2", body: 256, persistent: true, prefetch: 32, producers: 2, consumers: 2, rates: &RATES_FAN, step_secs: 2.0 },
];

#[tokio::main]
async fn main() -> Result<()> {
    let url = std::env::var("AMQP_URL").context("set AMQP_URL")?;
    if std::env::args().any(|arg| arg == "--check") {
        check_once(&url).await
    } else {
        bench(&url).await
    }
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
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_nanos()
    );
    ch.queue_declare(&queue, QueueDeclareOptions { durable: true, ..QueueDeclareOptions::default() }, FieldTable::default())
        .await
        .context("declare")?;
    ch.confirm_select(ConfirmSelectOptions::default()).await.context("confirm")?;
    let body = b"bench-body".to_vec();
    let confirm = ch
        .basic_publish("", &queue, BasicPublishOptions::default(), &body, BasicProperties::default().with_delivery_mode(2))
        .await
        .context("publish")?
        .await
        .context("confirm")?;
    if !confirm.is_ack() {
        bail!("publish was not confirmed");
    }
    let delivery = ch.basic_get(&queue, lapin::options::BasicGetOptions::default()).await.context("get")?.context("empty queue")?;
    delivery.ack(BasicAckOptions::default()).await.context("ack")?;
    println!("body={}", String::from_utf8_lossy(&delivery.data));
    println!("declare=ok publish=ok consume=ok ack=ok confirms=ok");
    Ok(())
}

async fn bench(url: &str) -> Result<()> {
    let mut failed = false;
    for scenario in SCENARIOS {
        if run_scenario(url, scenario).await.is_err() {
            failed = true;
        }
    }
    if failed {
        bail!("one or more scenarios did not confirm and consume");
    }
    Ok(())
}

async fn run_scenario(url: &str, scenario: &Scenario) -> Result<()> {
    let conn = connect(url).await?;
    let queue = format!(
        "bench-{}-{}",
        scenario.name,
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_nanos()
    );
    let setup = conn.create_channel().await.context("setup")?;
    setup
        .queue_declare(&queue, QueueDeclareOptions { durable: true, ..QueueDeclareOptions::default() }, FieldTable::default())
        .await
        .context("declare")?;

    let mut publishers = Vec::new();
    for _ in 0..scenario.producers {
        let ch = conn.create_channel().await.context("publisher")?;
        ch.confirm_select(ConfirmSelectOptions::default()).await.context("confirm")?;
        publishers.push(ch);
    }
    let acked = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let mut consumers = Vec::new();
    for i in 0..scenario.consumers {
        let ch = conn.create_channel().await.context("consumer")?;
        ch.basic_qos(scenario.prefetch, BasicQosOptions::default()).await.context("qos")?;
        let mut consumer = ch
            .basic_consume(&queue, &format!("bench-{i}"), BasicConsumeOptions::default(), FieldTable::default())
            .await
            .context("consume")?;
        let acked_task = Arc::clone(&acked);
        let stop_task = Arc::clone(&stop);
        consumers.push(tokio::spawn(async move {
            while !stop_task.load(Ordering::Relaxed) {
                match tokio::time::timeout(Duration::from_millis(50), consumer.next()).await {
                    Ok(Some(Ok(delivery))) => {
                        if delivery.ack(BasicAckOptions::default()).await.is_ok() {
                            acked_task.fetch_add(1, Ordering::Relaxed);
                        }
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

    for rate in scenario.rates {
        let rate = *rate;
        let target = (rate * scenario.step_secs).round() as u64;
        let before = acked.load(Ordering::Relaxed);
        let step_started = Instant::now();
        let step_deadline = step_started + Duration::from_secs_f64(scenario.step_secs);
        let (confirmed, step_lats) = publish_step(&publishers, &queue, &body, &props, target, rate, step_started, step_deadline).await;
        latencies.extend(step_lats);
        let drain_until = Instant::now() + Duration::from_millis(400);
        while acked.load(Ordering::Relaxed).saturating_sub(before) < confirmed && Instant::now() < drain_until {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let elapsed = step_started.elapsed().as_secs_f64().max(0.001);
        let consumed = acked.load(Ordering::Relaxed).saturating_sub(before);
        confirmed_total += confirmed;
        consumed_total += consumed;
        elapsed_total += elapsed;
        steps.push(LoadStep {
            offered_per_sec: rate,
            confirmed_per_sec: confirmed as f64 / elapsed,
            consumed_per_sec: consumed as f64 / elapsed,
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
        steps,
    });
    let flush = if scenario.persistent { disk_flush_for_url(url) } else { "not-durable" };
    let rates: Vec<String> = scenario.rates.iter().map(|r| format!("{r:.0}")).collect();
    println!("scenario={}", scenario.name);
    println!("body_size={}", scenario.body);
    println!("durable={}", scenario.persistent);
    println!("step_duration_secs={}", scenario.step_secs);
    println!("prefetch={}", scenario.prefetch);
    println!("producers={}", scenario.producers);
    println!("consumers={}", scenario.consumers);
    println!("rates={}", rates.join(","));
    println!("wall_secs={:.2}", started.elapsed().as_secs_f64());
    print!("{}", format_report(&summary, flush));
    let flag = |ok: bool| if ok { "ok" } else { "failed" };
    println!(
        "declare=ok publish={} consume={} ack={} confirms={}",
        flag(confirmed_total > 0),
        flag(consumed_total > 0),
        flag(consumed_total > 0),
        flag(confirmed_total > 0)
    );
    if confirmed_total == 0 || consumed_total == 0 {
        bail!("{} did not confirm and consume", scenario.name);
    }
    Ok(())
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
) -> (u64, Vec<f64>) {
    let n = publishers.len().max(1) as u64;
    let per = (target / n).max(1);
    let each_rate = rate / n as f64;
    // Producers run together. A single publisher is the one-channel case.
    let parts: Vec<(u64, Vec<f64>)> = match publishers {
        [only] => vec![one_publisher(only, queue, body, props, per, each_rate, step_started, step_deadline).await],
        [a, b] => {
            let (left, right) = tokio::join!(
                one_publisher(a, queue, body, props, per, each_rate, step_started, step_deadline),
                one_publisher(b, queue, body, props, per, each_rate, step_started, step_deadline),
            );
            vec![left, right]
        }
        _ => {
            let mut out = Vec::new();
            for ch in publishers {
                out.push(one_publisher(ch, queue, body, props, per, each_rate, step_started, step_deadline).await);
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

async fn one_publisher(
    ch: &Channel,
    queue: &str,
    body: &[u8],
    props: &BasicProperties,
    per: u64,
    rate_per_sec: f64,
    step_started: Instant,
    step_deadline: Instant,
) -> (u64, Vec<f64>) {
    let mut sent = 0u64;
    let mut confirmed = 0u64;
    let mut latencies = Vec::new();
    while sent < per {
        if Instant::now() >= step_deadline {
            break;
        }
        let slot = step_started + Duration::from_secs_f64(publish_slot_secs(sent, rate_per_sec));
        if let Some(wait) = slot.checked_duration_since(Instant::now()) {
            tokio::time::sleep(wait).await;
        }
        if Instant::now() >= step_deadline {
            break;
        }
        let t0 = Instant::now();
        let published = ch.basic_publish("", queue, BasicPublishOptions::default(), body, props.clone()).await;
        sent += 1;
        let Ok(confirm) = published else { continue };
        if let Ok(Ok(confirm)) = tokio::time::timeout(Duration::from_millis(500), confirm).await {
            if confirm.is_ack() {
                confirmed += 1;
                latencies.push(t0.elapsed().as_secs_f64() * 1000.0);
            }
        }
    }
    (confirmed, latencies)
}
