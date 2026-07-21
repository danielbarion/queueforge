//! Load shapes A–F against a live QueueForge (or AMQP-compatible) broker.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use futures_lite::stream::StreamExt;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, BasicQosOptions,
    ConfirmSelectOptions, ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions,
    QueueDeleteOptions,
};
use lapin::types::{AMQPValue, FieldTable, ShortString};
use lapin::{BasicProperties, Connection, ConnectionProperties, ExchangeKind};
use tokio_executor_trait::Tokio as TokioExecutor;
use tokio_reactor_trait::Tokio as TokioReactor;
use tracing::{debug, warn};

use crate::stats::Stats;

/// Shared knobs for a shape run.
#[derive(Debug, Clone)]
pub struct RunConfig {
    pub uri: String,
    pub duration: Duration,
    pub warmup: Duration,
    pub body_size: usize,
    pub prefetch: u16,
    pub queue_prefix: String,
    /// Management base URL (shape E), e.g. `http://127.0.0.1:15672`.
    pub mgmt_url: Option<String>,
    pub mgmt_user: String,
    pub mgmt_password: String,
    /// How often shape E polls the management list.
    pub mgmt_poll_interval: Duration,
    /// Assumed fsync interval for SLO reporting (shape D).
    pub fsync_interval_ms: u64,
}

impl Default for RunConfig {
    fn default() -> Self {
        Self {
            uri: "amqp://admin:devpassword12@127.0.0.1:5672/%2f".into(),
            duration: Duration::from_secs(30),
            warmup: Duration::from_secs(3),
            body_size: 1024,
            prefetch: 100,
            queue_prefix: "qf-bench".into(),
            mgmt_url: Some("http://127.0.0.1:15672".into()),
            mgmt_user: "admin".into(),
            mgmt_password: "devpassword12".into(),
            mgmt_poll_interval: Duration::from_millis(50),
            fsync_interval_ms: 100,
        }
    }
}

fn conn_props() -> ConnectionProperties {
    ConnectionProperties::default()
        .with_executor(TokioExecutor::current())
        .with_reactor(TokioReactor)
}

async fn connect(uri: &str) -> Result<Connection> {
    Connection::connect(uri, conn_props())
        .await
        .with_context(|| format!("connect to {uri}"))
}

fn payload(body_size: usize, stamp_ns: u64) -> Vec<u8> {
    let mut buf = vec![0u8; body_size.max(8)];
    buf[..8].copy_from_slice(&stamp_ns.to_le_bytes());
    buf
}

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

fn e2e_from_body(data: &[u8]) -> Option<Duration> {
    if data.len() < 8 {
        return None;
    }
    let mut ts = [0u8; 8];
    ts.copy_from_slice(&data[..8]);
    let sent = u64::from_le_bytes(ts);
    let now = now_ns();
    if now >= sent {
        Some(Duration::from_nanos(now - sent))
    } else {
        None
    }
}

/// Shape A — 1 producer, 1 consumer, 1 transient queue (baseline).
pub async fn run_a(cfg: &RunConfig) -> Result<Arc<Stats>> {
    run_single_queue(cfg, ShapeKind::A).await
}

/// Shape B — 32 producers, 32 consumers, 1 transient queue.
pub async fn run_b(cfg: &RunConfig) -> Result<Arc<Stats>> {
    run_multi(cfg, 32, 32, false, None).await
}

/// Shape C — 1 producer, fanout 1→10 queues, 1 consumer each.
pub async fn run_c(cfg: &RunConfig) -> Result<Arc<Stats>> {
    run_fanout(cfg, 10).await
}

/// Shape D — durable queue + publisher confirms, shape A topology.
pub async fn run_d(cfg: &RunConfig) -> Result<Arc<Stats>> {
    run_single_queue(cfg, ShapeKind::D).await
}

/// Shape E — management list/poll concurrent with shape B load.
pub async fn run_e(cfg: &RunConfig) -> Result<Arc<Stats>> {
    run_multi(cfg, 32, 32, false, Some(MgmtPoll::from_cfg(cfg)?)).await
}

/// Shape F — priority mix 20% p=9 / 80% p=0 on `x-max-priority=9` queue.
pub async fn run_f(cfg: &RunConfig) -> Result<Arc<Stats>> {
    run_single_queue(cfg, ShapeKind::F).await
}

#[derive(Clone, Copy, Debug)]
enum ShapeKind {
    A,
    D,
    F,
}

struct MgmtPoll {
    base: String,
    user: String,
    password: String,
    interval: Duration,
}

impl MgmtPoll {
    fn from_cfg(cfg: &RunConfig) -> Result<Self> {
        let base = cfg
            .mgmt_url
            .clone()
            .context("shape E requires --mgmt-url")?;
        Ok(Self {
            base: base.trim_end_matches('/').to_string(),
            user: cfg.mgmt_user.clone(),
            password: cfg.mgmt_password.clone(),
            interval: cfg.mgmt_poll_interval,
        })
    }
}

async fn run_single_queue(cfg: &RunConfig, kind: ShapeKind) -> Result<Arc<Stats>> {
    let stats = Arc::new(Stats::new());
    let stop = Arc::new(AtomicBool::new(false));
    let measuring = Arc::new(AtomicBool::new(false));
    let qname = format!("{}-{:?}", cfg.queue_prefix, kind).to_lowercase();

    let conn = connect(&cfg.uri).await?;
    let ch = conn.create_channel().await.context("create channel")?;

    let durable = matches!(kind, ShapeKind::D);
    let mut args = FieldTable::default();
    if matches!(kind, ShapeKind::F) {
        // RabbitMQ-style: long-int table value for x-max-priority.
        args.insert(ShortString::from("x-max-priority"), AMQPValue::LongInt(9));
    }

    // Clean slate.
    let _ = ch
        .queue_delete(
            &qname,
            QueueDeleteOptions {
                if_unused: false,
                if_empty: false,
                ..QueueDeleteOptions::default()
            },
        )
        .await;
    ch.queue_declare(
        &qname,
        QueueDeclareOptions {
            durable,
            exclusive: false,
            auto_delete: false,
            ..QueueDeclareOptions::default()
        },
        args,
    )
    .await
    .context("queue.declare")?;

    if matches!(kind, ShapeKind::D) {
        ch.confirm_select(ConfirmSelectOptions::default())
            .await
            .context("confirm.select")?;
    }

    ch.basic_qos(cfg.prefetch, BasicQosOptions::default())
        .await
        .context("basic.qos")?;

    let consumer = ch
        .basic_consume(
            &qname,
            "bench-c0",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .context("basic.consume")?;

    // Consumer task.
    {
        let stats = Arc::clone(&stats);
        let measuring = Arc::clone(&measuring);
        let stop = Arc::clone(&stop);
        tokio::spawn(async move {
            let mut consumer = consumer;
            while !stop.load(Ordering::Relaxed) {
                match tokio::time::timeout(Duration::from_millis(200), consumer.next()).await {
                    Ok(Some(Ok(delivery))) => {
                        if measuring.load(Ordering::Relaxed) {
                            if let Some(d) = e2e_from_body(&delivery.data) {
                                stats.record_e2e_latency(d);
                            }
                            stats.consumed.fetch_add(1, Ordering::Relaxed);
                        }
                        if let Err(e) = delivery.ack(BasicAckOptions::default()).await {
                            stats.consume_errors.fetch_add(1, Ordering::Relaxed);
                            debug!(error = %e, "ack failed");
                        }
                    }
                    Ok(Some(Err(e))) => {
                        stats.consume_errors.fetch_add(1, Ordering::Relaxed);
                        debug!(error = %e, "delivery error");
                        break;
                    }
                    Ok(None) => break,
                    Err(_) => {} // timeout — recheck stop
                }
            }
        });
    }

    // Producer task.
    let body_size = cfg.body_size;
    let priority_counter = Arc::new(AtomicU64::new(0));
    {
        let stats = Arc::clone(&stats);
        let measuring = Arc::clone(&measuring);
        let stop = Arc::clone(&stop);
        let qname = qname.clone();
        let ch_pub = ch.clone();
        let prio_ctr = Arc::clone(&priority_counter);
        tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                let stamp = now_ns();
                let body = payload(body_size, stamp);
                let mut props = BasicProperties::default();
                if matches!(kind, ShapeKind::D) {
                    props = props.with_delivery_mode(2);
                }
                if matches!(kind, ShapeKind::F) {
                    // 20% high priority.
                    let n = prio_ctr.fetch_add(1, Ordering::Relaxed);
                    let p = if n % 5 == 0 { 9 } else { 0 };
                    props = props.with_priority(p);
                }
                let t0 = Instant::now();
                match ch_pub
                    .basic_publish("", &qname, BasicPublishOptions::default(), &body, props)
                    .await
                {
                    Ok(confirm) => match confirm.await {
                        Ok(_) => {
                            if measuring.load(Ordering::Relaxed) {
                                stats.record_publish_latency(t0.elapsed());
                                stats.published.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        Err(e) => {
                            if measuring.load(Ordering::Relaxed) {
                                stats.publish_errors.fetch_add(1, Ordering::Relaxed);
                            }
                            debug!(error = %e, "confirm failed");
                        }
                    },
                    Err(e) => {
                        if measuring.load(Ordering::Relaxed) {
                            stats.publish_errors.fetch_add(1, Ordering::Relaxed);
                        }
                        debug!(error = %e, "publish failed");
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                }
            }
        });
    }

    drive_timed(cfg, &stats, &measuring, &stop).await;

    // Cleanup best-effort.
    let _ = ch.queue_delete(&qname, QueueDeleteOptions::default()).await;
    let _ = ch.close(200, "bench done").await;
    let _ = conn.close(200, "bench done").await;
    Ok(stats)
}

async fn run_multi(
    cfg: &RunConfig,
    n_producers: usize,
    n_consumers: usize,
    durable: bool,
    mgmt: Option<MgmtPoll>,
) -> Result<Arc<Stats>> {
    let stats = Arc::new(Stats::new());
    let stop = Arc::new(AtomicBool::new(false));
    let measuring = Arc::new(AtomicBool::new(false));
    let qname = format!("{}-multi", cfg.queue_prefix);

    // Setup connection for topology.
    let setup = connect(&cfg.uri).await?;
    let ch = setup.create_channel().await?;
    let _ = ch.queue_delete(&qname, QueueDeleteOptions::default()).await;
    ch.queue_declare(
        &qname,
        QueueDeclareOptions {
            durable,
            ..QueueDeclareOptions::default()
        },
        FieldTable::default(),
    )
    .await
    .context("queue.declare multi")?;
    let _ = ch.close(200, "setup").await;
    let _ = setup.close(200, "setup").await;

    // Consumers — one connection each for multi-conn stress.
    for i in 0..n_consumers {
        let uri = cfg.uri.clone();
        let qname = qname.clone();
        let stats = Arc::clone(&stats);
        let measuring = Arc::clone(&measuring);
        let stop = Arc::clone(&stop);
        let prefetch = cfg.prefetch;
        tokio::spawn(async move {
            let Ok(conn) = connect(&uri).await else {
                stats.consume_errors.fetch_add(1, Ordering::Relaxed);
                return;
            };
            let Ok(ch) = conn.create_channel().await else {
                stats.consume_errors.fetch_add(1, Ordering::Relaxed);
                return;
            };
            if ch
                .basic_qos(prefetch, BasicQosOptions::default())
                .await
                .is_err()
            {
                stats.consume_errors.fetch_add(1, Ordering::Relaxed);
                return;
            }
            let Ok(mut consumer) = ch
                .basic_consume(
                    &qname,
                    &format!("bench-c{i}"),
                    BasicConsumeOptions::default(),
                    FieldTable::default(),
                )
                .await
            else {
                stats.consume_errors.fetch_add(1, Ordering::Relaxed);
                return;
            };
            while !stop.load(Ordering::Relaxed) {
                match tokio::time::timeout(Duration::from_millis(200), consumer.next()).await {
                    Ok(Some(Ok(delivery))) => {
                        if measuring.load(Ordering::Relaxed) {
                            if let Some(d) = e2e_from_body(&delivery.data) {
                                stats.record_e2e_latency(d);
                            }
                            stats.consumed.fetch_add(1, Ordering::Relaxed);
                        }
                        let _ = delivery.ack(BasicAckOptions::default()).await;
                    }
                    Ok(Some(Err(_))) => {
                        stats.consume_errors.fetch_add(1, Ordering::Relaxed);
                        break;
                    }
                    Ok(None) => break,
                    Err(_) => {}
                }
            }
            let _ = ch.close(200, "done").await;
            let _ = conn.close(200, "done").await;
        });
    }

    // Producers.
    for _ in 0..n_producers {
        let uri = cfg.uri.clone();
        let qname = qname.clone();
        let stats = Arc::clone(&stats);
        let measuring = Arc::clone(&measuring);
        let stop = Arc::clone(&stop);
        let body_size = cfg.body_size;
        tokio::spawn(async move {
            let Ok(conn) = connect(&uri).await else {
                stats.publish_errors.fetch_add(1, Ordering::Relaxed);
                return;
            };
            let Ok(ch) = conn.create_channel().await else {
                stats.publish_errors.fetch_add(1, Ordering::Relaxed);
                return;
            };
            while !stop.load(Ordering::Relaxed) {
                let body = payload(body_size, now_ns());
                let t0 = Instant::now();
                match ch
                    .basic_publish(
                        "",
                        &qname,
                        BasicPublishOptions::default(),
                        &body,
                        BasicProperties::default(),
                    )
                    .await
                {
                    Ok(confirm) => match confirm.await {
                        Ok(_) => {
                            if measuring.load(Ordering::Relaxed) {
                                stats.record_publish_latency(t0.elapsed());
                                stats.published.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        Err(_) => {
                            if measuring.load(Ordering::Relaxed) {
                                stats.publish_errors.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    },
                    Err(_) => {
                        if measuring.load(Ordering::Relaxed) {
                            stats.publish_errors.fetch_add(1, Ordering::Relaxed);
                        }
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                }
            }
            let _ = ch.close(200, "done").await;
            let _ = conn.close(200, "done").await;
        });
    }

    // Optional management poller (shape E).
    if let Some(mgmt) = mgmt {
        let stats = Arc::clone(&stats);
        let measuring = Arc::clone(&measuring);
        let stop = Arc::clone(&stop);
        tokio::spawn(async move {
            if let Err(e) = mgmt_poll_loop(mgmt, stats, measuring, stop).await {
                warn!(error = %e, "management poller exited");
            }
        });
    }

    drive_timed(cfg, &stats, &measuring, &stop).await;

    // Cleanup.
    if let Ok(conn) = connect(&cfg.uri).await {
        if let Ok(ch) = conn.create_channel().await {
            let _ = ch.queue_delete(&qname, QueueDeleteOptions::default()).await;
            let _ = ch.close(200, "cleanup").await;
        }
        let _ = conn.close(200, "cleanup").await;
    }
    Ok(stats)
}

async fn run_fanout(cfg: &RunConfig, n_queues: usize) -> Result<Arc<Stats>> {
    let stats = Arc::new(Stats::new());
    let stop = Arc::new(AtomicBool::new(false));
    let measuring = Arc::new(AtomicBool::new(false));
    let ex_name = format!("{}-fanout", cfg.queue_prefix);
    let qnames: Vec<String> = (0..n_queues)
        .map(|i| format!("{}-fanout-q{i}", cfg.queue_prefix))
        .collect();

    let setup = connect(&cfg.uri).await?;
    let ch = setup.create_channel().await?;

    let _ = ch
        .exchange_delete(
            &ex_name,
            lapin::options::ExchangeDeleteOptions {
                if_unused: false,
                ..Default::default()
            },
        )
        .await;
    ch.exchange_declare(
        &ex_name,
        ExchangeKind::Fanout,
        ExchangeDeclareOptions {
            durable: false,
            auto_delete: false,
            ..ExchangeDeclareOptions::default()
        },
        FieldTable::default(),
    )
    .await
    .context("exchange.declare fanout")?;

    for q in &qnames {
        let _ = ch.queue_delete(q, QueueDeleteOptions::default()).await;
        ch.queue_declare(
            q,
            QueueDeclareOptions {
                durable: false,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .with_context(|| format!("queue.declare {q}"))?;
        ch.queue_bind(
            q,
            &ex_name,
            "",
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await
        .with_context(|| format!("queue.bind {q}"))?;
    }
    let _ = ch.close(200, "setup").await;
    let _ = setup.close(200, "setup").await;

    // One consumer per queue.
    for (i, qname) in qnames.iter().cloned().enumerate() {
        let uri = cfg.uri.clone();
        let stats = Arc::clone(&stats);
        let measuring = Arc::clone(&measuring);
        let stop = Arc::clone(&stop);
        let prefetch = cfg.prefetch;
        tokio::spawn(async move {
            let Ok(conn) = connect(&uri).await else {
                return;
            };
            let Ok(ch) = conn.create_channel().await else {
                return;
            };
            let _ = ch.basic_qos(prefetch, BasicQosOptions::default()).await;
            let Ok(mut consumer) = ch
                .basic_consume(
                    &qname,
                    &format!("bench-fanout-{i}"),
                    BasicConsumeOptions::default(),
                    FieldTable::default(),
                )
                .await
            else {
                return;
            };
            while !stop.load(Ordering::Relaxed) {
                match tokio::time::timeout(Duration::from_millis(200), consumer.next()).await {
                    Ok(Some(Ok(delivery))) => {
                        if measuring.load(Ordering::Relaxed) {
                            if let Some(d) = e2e_from_body(&delivery.data) {
                                stats.record_e2e_latency(d);
                            }
                            stats.consumed.fetch_add(1, Ordering::Relaxed);
                        }
                        let _ = delivery.ack(BasicAckOptions::default()).await;
                    }
                    Ok(Some(Err(_))) | Ok(None) => break,
                    Err(_) => {}
                }
            }
            let _ = ch.close(200, "done").await;
            let _ = conn.close(200, "done").await;
        });
    }

    // Single producer to fanout exchange.
    {
        let uri = cfg.uri.clone();
        let ex_name = ex_name.clone();
        let stats = Arc::clone(&stats);
        let measuring = Arc::clone(&measuring);
        let stop = Arc::clone(&stop);
        let body_size = cfg.body_size;
        tokio::spawn(async move {
            let Ok(conn) = connect(&uri).await else {
                return;
            };
            let Ok(ch) = conn.create_channel().await else {
                return;
            };
            while !stop.load(Ordering::Relaxed) {
                let body = payload(body_size, now_ns());
                let t0 = Instant::now();
                match ch
                    .basic_publish(
                        &ex_name,
                        "",
                        BasicPublishOptions::default(),
                        &body,
                        BasicProperties::default(),
                    )
                    .await
                {
                    Ok(confirm) => match confirm.await {
                        Ok(_) => {
                            if measuring.load(Ordering::Relaxed) {
                                stats.record_publish_latency(t0.elapsed());
                                stats.published.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        Err(_) => {
                            if measuring.load(Ordering::Relaxed) {
                                stats.publish_errors.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    },
                    Err(_) => {
                        if measuring.load(Ordering::Relaxed) {
                            stats.publish_errors.fetch_add(1, Ordering::Relaxed);
                        }
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                }
            }
            let _ = ch.close(200, "done").await;
            let _ = conn.close(200, "done").await;
        });
    }

    drive_timed(cfg, &stats, &measuring, &stop).await;

    if let Ok(conn) = connect(&cfg.uri).await {
        if let Ok(ch) = conn.create_channel().await {
            for q in &qnames {
                let _ = ch.queue_delete(q, QueueDeleteOptions::default()).await;
            }
            let _ = ch
                .exchange_delete(&ex_name, lapin::options::ExchangeDeleteOptions::default())
                .await;
            let _ = ch.close(200, "cleanup").await;
        }
        let _ = conn.close(200, "cleanup").await;
    }
    Ok(stats)
}

async fn mgmt_poll_loop(
    mgmt: MgmtPoll,
    stats: Arc<Stats>,
    measuring: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
) -> Result<()> {
    let client = reqwest::Client::builder()
        .cookie_store(true)
        .timeout(Duration::from_secs(5))
        .build()
        .context("build reqwest client")?;

    // Login for session cookie.
    let login_url = format!("{}/api/login", mgmt.base);
    let resp = client
        .post(&login_url)
        .json(&serde_json::json!({
            "username": mgmt.user,
            "password": mgmt.password,
        }))
        .send()
        .await
        .context("POST /api/login")?;
    if !resp.status().is_success() {
        anyhow::bail!("management login failed: HTTP {}", resp.status());
    }

    let list_url = format!("{}/api/queues/%2F?page_size=100", mgmt.base);
    while !stop.load(Ordering::Relaxed) {
        let t0 = Instant::now();
        match client.get(&list_url).send().await {
            Ok(r) if r.status().is_success() => {
                // Drain body so connection can reuse.
                let _ = r.bytes().await;
                if measuring.load(Ordering::Relaxed) {
                    stats.record_mgmt_latency(t0.elapsed());
                    stats.mgmt_requests.fetch_add(1, Ordering::Relaxed);
                }
            }
            Ok(r) => {
                if measuring.load(Ordering::Relaxed) {
                    stats.mgmt_errors.fetch_add(1, Ordering::Relaxed);
                }
                debug!(status = %r.status(), "mgmt list non-success");
            }
            Err(e) => {
                if measuring.load(Ordering::Relaxed) {
                    stats.mgmt_errors.fetch_add(1, Ordering::Relaxed);
                }
                debug!(error = %e, "mgmt list error");
            }
        }
        tokio::time::sleep(mgmt.interval).await;
    }
    Ok(())
}

/// Warmup → measure window → stop workers.
async fn drive_timed(
    cfg: &RunConfig,
    _stats: &Arc<Stats>,
    measuring: &Arc<AtomicBool>,
    stop: &Arc<AtomicBool>,
) {
    if !cfg.warmup.is_zero() {
        debug!(?cfg.warmup, "warmup");
        tokio::time::sleep(cfg.warmup).await;
    }
    // Counters only accumulate while measuring is true.
    measuring.store(true, Ordering::SeqCst);
    debug!(?cfg.duration, "measuring");
    tokio::time::sleep(cfg.duration).await;
    measuring.store(false, Ordering::SeqCst);
    stop.store(true, Ordering::SeqCst);
    // Let in-flight acks settle briefly.
    tokio::time::sleep(Duration::from_millis(250)).await;
}
