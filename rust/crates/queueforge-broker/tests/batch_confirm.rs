//! Many persistent confirms must share one group-commit fsync.
//!
//! The frame reader returns before `durable_done`. One interval flush then
//! ack's the whole batch. A per-message wait would take `n * interval`.

use std::path::PathBuf;
use std::process::{Child, Command};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use lapin::options::{BasicPublishOptions, ConfirmSelectOptions, QueueDeclareOptions};
use lapin::{BasicProperties, Connection, ConnectionProperties};
use reqwest::Client;

struct Kid(Child);

impl Drop for Kid {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn write_cfg(
    path: &std::path::Path,
    amqp: u16,
    mgmt: u16,
    metrics: u16,
    data: &std::path::Path,
    interval_ms: u64,
) {
    std::fs::create_dir_all(data).unwrap();
    std::fs::write(
        path,
        format!(
            r#"[listeners]
amqp = "127.0.0.1:{amqp}"
management = "127.0.0.1:{mgmt}"
metrics = "127.0.0.1:{metrics}"
[data]
dir = "{data}"
fsync_policy = "every_n_ms"
fsync_interval_ms = {interval_ms}
fsync_every_n_messages = 1
[tls]
enabled = false
[logging]
level = "warn"
"#,
            data = data.display()
        ),
    )
    .unwrap();
}

fn spawn(
    kind: &str,
    amqp: u16,
    mgmt: u16,
    metrics: u16,
    data: &PathBuf,
    interval_ms: u64,
) -> Child {
    let cfg = data.join("qf.toml");
    write_cfg(&cfg, amqp, mgmt, metrics, data, interval_ms);
    if kind == "rust" {
        Command::new(env!("CARGO_BIN_EXE_queueforge"))
            .arg("--config")
            .arg(&cfg)
            .arg("--dev-bootstrap")
            .spawn()
            .expect("spawn rust")
    } else {
        let bun_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../bun");
        Command::new("bun")
            .arg("src/main.ts")
            .arg("--config")
            .arg(&cfg)
            .arg("--dev-bootstrap")
            .current_dir(&bun_dir)
            .spawn()
            .expect("spawn bun")
    }
}

async fn wait_ready(mgmt: u16) {
    let url = format!("http://127.0.0.1:{mgmt}/readyz");
    let client = Client::new();
    for _ in 0..80 {
        if let Ok(res) = client.get(&url).send().await {
            if res.status().is_success() && res.text().await.unwrap_or_default().contains("ready") {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("readyz did not succeed on {mgmt}");
}

async fn metric(metrics: u16, name: &str) -> f64 {
    let url = format!("http://127.0.0.1:{metrics}/metrics");
    let text = Client::new()
        .get(url)
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap_or_default();
    for line in text.lines() {
        if line.starts_with('#') || !line.starts_with(name) {
            continue;
        }
        let rest = line[name.len()..].trim_start();
        if rest.starts_with('{') {
            continue;
        }
        return rest
            .split_whitespace()
            .next()
            .unwrap_or("0")
            .parse()
            .unwrap_or(0.0);
    }
    0.0
}

async fn batch(
    kind: &str,
    amqp: u16,
    mgmt: u16,
    metrics: u16,
    count: u16,
    interval_ms: u64,
    min_elapsed: Duration,
    max_elapsed: Duration,
    max_flushes: f64,
) {
    let dir = std::env::temp_dir().join(format!("qf-batch-{kind}-{amqp}"));
    let _ = std::fs::remove_dir_all(&dir);
    let child = spawn(kind, amqp, mgmt, metrics, &dir, interval_ms);
    let mut kid = Kid(child);
    wait_ready(mgmt).await;
    let conn = Connection::connect(
        &format!("amqp://admin:devpassword12@127.0.0.1:{amqp}/%2f"),
        ConnectionProperties::default(),
    )
    .await
    .unwrap();
    let ch = conn.create_channel().await.unwrap();
    ch.queue_declare(
        "batch",
        QueueDeclareOptions {
            durable: true,
            ..QueueDeclareOptions::default()
        },
        Default::default(),
    )
    .await
    .unwrap();
    ch.confirm_select(ConfirmSelectOptions::default())
        .await
        .unwrap();
    let before = metric(metrics, "queueforge_wal_fsync_seconds_count").await;
    let early = metric(metrics, "queueforge_confirm_before_fsync_total").await;
    let started = Instant::now();
    let mut pending = Vec::new();
    for i in 0..count {
        let conf = ch
            .basic_publish(
                "",
                "batch",
                BasicPublishOptions::default(),
                &[(i & 0xff) as u8],
                BasicProperties::default().with_delivery_mode(2),
            )
            .await
            .unwrap();
        pending.push(conf);
    }
    for conf in pending {
        assert!(
            conf.await.expect("confirm").is_ack(),
            "{kind} confirm was not ack"
        );
    }
    let elapsed = started.elapsed();
    let after = metric(metrics, "queueforge_wal_fsync_seconds_count").await;
    let counter = metric(metrics, "queueforge_confirm_before_fsync_total").await;
    let flushes = after - before;
    let elapsed_ms = elapsed.as_millis();
    println!(
        "{kind} batch elapsed_ms={elapsed_ms} fsync {before} -> {after} (delta {flushes}) confirm_before_fsync {early} -> {counter}"
    );
    assert!(
        elapsed >= min_elapsed,
        "{kind} confirmed before the covering fsync: {elapsed:?}"
    );
    assert!(
        elapsed < max_elapsed,
        "{kind} confirms did not share one flush: {elapsed:?}"
    );
    assert!(flushes >= 1.0, "{kind} covering fsync did not run");
    assert!(
        flushes < max_flushes,
        "{kind} fsync ran per message: {flushes}"
    );
    assert_eq!(
        counter, early,
        "{kind} confirm moved queueforge_confirm_before_fsync_total"
    );
    drop(conn);
    let _ = kid.0.kill();
    let _ = kid.0.wait();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn pipelined_classic_confirms_share_one_fsync() {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    // Ports sit below the macOS ephemeral range (49152+), so outbound dials cannot take them.
    let base = 30000 + (nanos % 400) as u16;
    batch(
        "rust",
        base,
        base + 100,
        base + 200,
        24,
        80,
        Duration::from_millis(40),
        Duration::from_millis(800),
        12.0,
    )
    .await;
    batch(
        "bun",
        base + 1,
        base + 101,
        base + 201,
        24,
        80,
        Duration::from_millis(40),
        Duration::from_millis(800),
        12.0,
    )
    .await;
}

/// 128 confirms already in flight share one fsync. The 400 ms interval must not be the wait.
#[tokio::test]
async fn one_hundred_twenty_eight_classic_confirms_share_one_fsync() {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    let base = 31000 + (nanos % 400) as u16;
    batch(
        "rust",
        base,
        base + 100,
        base + 200,
        128,
        400,
        Duration::from_millis(0),
        Duration::from_millis(80),
        2.0,
    )
    .await;
    batch(
        "bun",
        base + 1,
        base + 101,
        base + 201,
        128,
        400,
        Duration::from_millis(0),
        Duration::from_millis(80),
        2.0,
    )
    .await;
}
