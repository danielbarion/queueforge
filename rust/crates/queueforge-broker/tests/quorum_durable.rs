//! Live AMQP path: a confirmed quorum publish survives kill -9 of the leader.
//!
//! `queueforge_broker::quorum_confirm::durable_majority` is what `Cluster::quorum_enqueue` calls
//! after each member flushes. This file drives that path with lapin. A second consume after
//! the killed member restarts must not see the body again.

use std::fs;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use lapin::options::{BasicAckOptions, BasicGetOptions, BasicPublishOptions, ConfirmSelectOptions, QueueDeclareOptions};
use lapin::types::{FieldTable, LongString};
use lapin::{BasicProperties, Connection, ConnectionProperties};
use reqwest::Client;

struct Kids(Vec<Child>);

impl Drop for Kids {
    fn drop(&mut self) {
        for child in &mut self.0 {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn write_cfg(path: &std::path::Path, amqp: u16, mgmt: u16, metrics: u16, data: &std::path::Path, cluster: &str, policy: &str) {
    fs::create_dir_all(data).unwrap();
    fs::write(
        path,
        format!(
            r#"[listeners]
amqp = "127.0.0.1:{amqp}"
management = "127.0.0.1:{mgmt}"
metrics = "127.0.0.1:{metrics}"
[data]
dir = "{data}"
fsync_policy = "{policy}"
fsync_interval_ms = 400
fsync_every_n_messages = 1
[tls]
enabled = false
[logging]
level = "warn"
{cluster}
"#,
            data = data.display()
        ),
    )
    .unwrap();
}

fn spawn(kind: &str, amqp: u16, mgmt: u16, metrics: u16, data: &PathBuf, cluster: &str, policy: &str) -> Child {
    let cfg = data.join("qf.toml");
    write_cfg(&cfg, amqp, mgmt, metrics, data, cluster, policy);
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
    let text = Client::new().get(url).send().await.unwrap().text().await.unwrap_or_default();
    for line in text.lines() {
        if line.starts_with('#') || !line.starts_with(name) {
            continue;
        }
        let rest = line[name.len()..].trim_start();
        if rest.starts_with('{') {
            continue;
        }
        return rest.split_whitespace().next().unwrap_or("0").parse().unwrap_or(0.0);
    }
    0.0
}

async fn declare_quorum(port: u16, queue: &str) {
    let conn = Connection::connect(
        &format!("amqp://admin:devpassword12@127.0.0.1:{port}/%2f"),
        ConnectionProperties::default(),
    )
    .await
    .unwrap();
    let mut args = FieldTable::default();
    args.insert("x-queue-type".into(), lapin::types::AMQPValue::LongString(LongString::from("quorum")));
    conn.create_channel()
        .await
        .unwrap()
        .queue_declare(queue, QueueDeclareOptions { durable: true, ..QueueDeclareOptions::default() }, args)
        .await
        .unwrap();
}

async fn publish(port: u16, queue: &str, body: &[u8]) {
    let conn = Connection::connect(
        &format!("amqp://admin:devpassword12@127.0.0.1:{port}/%2f"),
        ConnectionProperties::default(),
    )
    .await
    .unwrap();
    let ch = conn.create_channel().await.unwrap();
    ch.confirm_select(ConfirmSelectOptions::default()).await.unwrap();
    let conf = ch
        .basic_publish("", queue, BasicPublishOptions::default(), body, BasicProperties::default().with_delivery_mode(2))
        .await
        .unwrap();
    assert!(conf.await.expect("confirm").is_ack());
}

async fn take(port: u16, queue: &str) -> Option<(Vec<u8>, String)> {
    let conn = Connection::connect(
        &format!("amqp://admin:devpassword12@127.0.0.1:{port}/%2f"),
        ConnectionProperties::default(),
    )
    .await
    .ok()?;
    let ch = conn.create_channel().await.ok()?;
    let got = ch.basic_get(queue, BasicGetOptions { no_ack: false }).await.ok()??;
    let body = got.data.to_vec();
    let key = got.routing_key.to_string();
    ch.basic_ack(got.delivery_tag, BasicAckOptions::default()).await.ok()?;
    Some((body, key))
}

async fn failover(label: &str, kinds: [&str; 3], base: u16) {
    let ports: [(u16, u16, u16, u16); 3] = [
        (base, base + 100, base + 200, base + 300),
        (base + 2, base + 102, base + 202, base + 302),
        (base + 4, base + 104, base + 204, base + 304),
    ];
    let cluster = format!(
        r#"[cluster]
node_id = "NODE"
listen = "127.0.0.1:PORT"
members = [
  {{ id = "a", addr = "127.0.0.1:{}" }},
  {{ id = "b", addr = "127.0.0.1:{}" }},
  {{ id = "c", addr = "127.0.0.1:{}" }},
]
"#,
        ports[0].3, ports[1].3, ports[2].3
    );
    let ids = ["a", "b", "c"];
    let mut kids = Kids(Vec::new());
    let mut dirs = Vec::new();
    for (i, (amqp, mgmt, metrics, cluster_port)) in ports.iter().copied().enumerate() {
        let dir = std::env::temp_dir().join(format!("qf-qd-{label}-{i}-{}", ports[0].0));
        let _ = fs::remove_dir_all(&dir);
        let cfg = cluster.replace("NODE", ids[i]).replace("PORT", &cluster_port.to_string());
        kids.0.push(spawn(kinds[i], amqp, mgmt, metrics, &dir, &cfg, "every_n_ms"));
        dirs.push(dir);
    }
    for (_, mgmt, _, _) in ports {
        wait_ready(mgmt).await;
    }
    tokio::time::sleep(Duration::from_millis(400)).await;
    for (amqp, _, _, _) in ports {
        declare_quorum(amqp, "qq-durable").await;
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    publish(ports[0].0, "qq-durable", b"kept-body").await;
    println!("{label} confirm ack");
    kids.0[0].kill().expect("kill leader");
    let _ = kids.0[0].wait();
    println!("{label} leader killed");

    let mut seen = None;
    for _ in 0..40 {
        if let Some(msg) = take(ports[1].0, "qq-durable").await {
            seen = Some(msg);
            break;
        }
        if let Some(msg) = take(ports[2].0, "qq-durable").await {
            seen = Some(msg);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let (body, key) = seen.expect("survivor delivered nothing");
    assert_eq!(body, b"kept-body");
    assert_eq!(key, "qq-durable");
    println!("{label} survivor delivered once");

    let again = take(ports[1].0, "qq-durable").await.or(take(ports[2].0, "qq-durable").await);
    assert!(again.is_none(), "{label} delivered the body twice before restart");

    kids.0[0] = spawn(kinds[0], ports[0].0, ports[0].1, ports[0].2, &dirs[0], &cluster.replace("NODE", "a").replace("PORT", &ports[0].3.to_string()), "every_n_ms");
    wait_ready(ports[0].1).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let after_restart = take(ports[0].0, "qq-durable").await.or(take(ports[1].0, "qq-durable").await);
    assert!(after_restart.is_none(), "{label} restart duplicated or resurrected the body");
    println!("{label} restart did not duplicate");

    let publish_port = ports[1].0;
    let task = tokio::spawn(async move {
        let _ = publish_nofail(publish_port, "qq-durable", b"maybe-body").await;
    });
    tokio::time::sleep(Duration::from_millis(15)).await;
    let _ = kids.0[1].kill();
    let _ = kids.0[1].wait();
    let _ = task.await;
    println!("{label} unconfirmed publish may be absent");
}

async fn publish_nofail(port: u16, queue: &str, body: &[u8]) -> bool {
    let Ok(conn) = Connection::connect(
        &format!("amqp://admin:devpassword12@127.0.0.1:{port}/%2f"),
        ConnectionProperties::default(),
    )
    .await
    else {
        return false;
    };
    let Ok(ch) = conn.create_channel().await else { return false };
    if ch.confirm_select(ConfirmSelectOptions::default()).await.is_err() {
        return false;
    }
    let Ok(conf) = ch
        .basic_publish("", queue, BasicPublishOptions::default(), body, BasicProperties::default().with_delivery_mode(2))
        .await
    else {
        return false;
    };
    matches!(conf.await, Ok(ack) if ack.is_ack())
}

async fn classic(policy: &str, wait_for_fsync: bool) {
    let base = 46000 + (SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u16 % 400);
    let dir = std::env::temp_dir().join(format!("qf-classic-{policy}-{base}"));
    let _ = fs::remove_dir_all(&dir);
    let child = spawn("rust", base, base + 100, base + 200, &dir, "", policy);
    let mut kids = Kids(vec![child]);
    wait_ready(base + 100).await;
    let before = metric(base + 200, "queueforge_wal_fsync_seconds_count").await;
    let conn = Connection::connect(
        &format!("amqp://admin:devpassword12@127.0.0.1:{base}/%2f"),
        ConnectionProperties::default(),
    )
    .await
    .unwrap();
    let ch = conn.create_channel().await.unwrap();
    ch.queue_declare("classic-q", QueueDeclareOptions { durable: true, ..QueueDeclareOptions::default() }, FieldTable::default())
        .await
        .unwrap();
    ch.confirm_select(ConfirmSelectOptions::default()).await.unwrap();
    let started = Instant::now();
    let conf = ch
        .basic_publish("", "classic-q", BasicPublishOptions::default(), b"classic", BasicProperties::default().with_delivery_mode(2))
        .await
        .unwrap();
    assert!(conf.await.expect("classic confirm").is_ack());
    let elapsed = started.elapsed();
    let after = metric(base + 200, "queueforge_wal_fsync_seconds_count").await;
    println!("classic {policy} elapsed_ms={} fsync {before} -> {after}", elapsed.as_millis());
    if wait_for_fsync {
        assert!(after > before, "{policy} confirm returned before fsync");
    } else {
        assert!(elapsed < Duration::from_millis(350), "{policy} waited out the 400ms interval");
        assert_eq!(after, before, "{policy} fsynced before the confirm returned");
    }
    let _ = kids.0[0].kill();
}

#[tokio::test]
async fn rust_rust_bun_confirmed_publish_survives_leader_kill() {
    let base = 47000 + (SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u16 % 200);
    failover("rust-rust-bun", ["rust", "rust", "bun"], base).await;
}

#[tokio::test]
async fn bun_bun_rust_confirmed_publish_survives_leader_kill() {
    let base = 48000 + (SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u16 % 200);
    failover("bun-bun-rust", ["bun", "bun", "rust"], base).await;
}

#[tokio::test]
async fn classic_every_n_ms_confirms_before_the_interval() {
    classic("every_n_ms", false).await;
}

#[tokio::test]
async fn classic_always_waits_for_fsync() {
    classic("always", true).await;
}

#[tokio::test]
async fn classic_every_n_messages_waits_for_fsync() {
    classic("every_n_messages", true).await;
}
