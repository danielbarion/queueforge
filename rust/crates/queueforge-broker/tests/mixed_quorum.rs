//! One quorum queue across Rust and Bun. Confirm after an in-memory majority, then fail over.

use std::fs;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use lapin::options::{BasicGetOptions, BasicPublishOptions, ConfirmSelectOptions, QueueDeclareOptions};
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

fn write_cfg(path: &std::path::Path, amqp: u16, mgmt: u16, metrics: u16, data: &std::path::Path, cluster: &str) {
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
fsync_policy = "every_n_ms"
fsync_interval_ms = 10
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

fn spawn(kind: &str, amqp: u16, mgmt: u16, metrics: u16, data: &PathBuf, cluster: &str) -> Child {
    let cfg = data.join("qf.toml");
    write_cfg(&cfg, amqp, mgmt, metrics, data, cluster);
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
    for _ in 0..100 {
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

async fn publish(port: u16, queue: &str, body: &[u8]) {
    let conn = Connection::connect(
        &format!("amqp://admin:devpassword12@127.0.0.1:{port}/%2f"),
        ConnectionProperties::default(),
    )
    .await
    .expect("connect");
    let ch = conn.create_channel().await.unwrap();
    ch.confirm_select(ConfirmSelectOptions::default()).await.unwrap();
    let conf = ch
        .basic_publish("", queue, BasicPublishOptions::default(), body, BasicProperties::default().with_delivery_mode(2))
        .await
        .unwrap();
    assert!(conf.await.expect("confirm").is_ack(), "publisher confirm was nacked");
}

async fn get_body(port: u16, queue: &str) -> Option<Vec<u8>> {
    let conn = Connection::connect(
        &format!("amqp://admin:devpassword12@127.0.0.1:{port}/%2f"),
        ConnectionProperties::default(),
    )
    .await
    .ok()?;
    let ch = conn.create_channel().await.ok()?;
    ch.basic_get(queue, BasicGetOptions { no_ack: true }).await.ok()?.map(|msg| msg.data.to_vec())
}

fn dir_contains(dir: &std::path::Path, needle: &[u8]) -> bool {
    let mut stack = vec![dir.to_path_buf()];
    while let Some(path) = stack.pop() {
        let Ok(entries) = fs::read_dir(&path) else { continue };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            if fs::read(&path).ok().is_some_and(|bytes| bytes.windows(needle.len()).any(|w| w == needle)) {
                return true;
            }
        }
    }
    false
}

async fn mixed(label: &str, kinds: [&str; 3], base: u16) {
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
    let mut dirs = Vec::new();
    let mut kids = Kids(Vec::new());
    for (i, (amqp, mgmt, metrics, cluster_port)) in ports.iter().copied().enumerate() {
        let dir = std::env::temp_dir().join(format!("qf-mix-{label}-{base}-{i}"));
        let _ = fs::remove_dir_all(&dir);
        let cfg = cluster.replace("NODE", ids[i]).replace("PORT", &cluster_port.to_string());
        kids.0.push(spawn(kinds[i], amqp, mgmt, metrics, &dir, &cfg));
        dirs.push(dir);
    }
    for (_, mgmt, _, _) in ports {
        wait_ready(mgmt).await;
    }
    tokio::time::sleep(Duration::from_millis(800)).await;

    let mut args = FieldTable::default();
    args.insert("x-queue-type".into(), lapin::types::AMQPValue::LongString(LongString::from("quorum")));
    let decl = QueueDeclareOptions { durable: true, ..QueueDeclareOptions::default() };
    for (amqp, _, _, _) in ports {
        let conn = Connection::connect(
            &format!("amqp://admin:devpassword12@127.0.0.1:{amqp}/%2f"),
            ConnectionProperties::default(),
        )
        .await
        .unwrap();
        let declared = conn.create_channel().await.unwrap().queue_declare("qq-mix", decl, args.clone()).await;
        if let Err(err) = &declared {
            let text = err.to_string();
            assert!(text.contains("exists"), "{label} declare on {amqp} failed: {text}");
        }
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    let early = metric(ports[0].2, "queueforge_confirm_before_fsync_total").await;
    publish(ports[0].0, "qq-mix", b"body-one").await;
    let after = metric(ports[0].2, "queueforge_confirm_before_fsync_total").await;
    assert!(after > early, "{label} confirm waited for the interval fsync ({early} -> {after})");
    println!("{label} confirm before fsync {early} -> {after}");
    for (i, kind) in kinds.iter().enumerate() {
        if *kind != "rust" || i == 0 {
            continue;
        }
        let persisted = dir_contains(&dirs[i], b"body-one");
        assert!(persisted, "{label} rust follower {i} did not append body-one to its log");
        println!("{label} rust follower {i} appended body-one before fsync");
    }

    kids.0[0].kill().unwrap();
    let _ = kids.0[0].wait();
    let mut seen = None;
    for _ in 0..50 {
        if let Some(body) = get_body(ports[1].0, "qq-mix").await {
            seen = Some(body);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(seen.as_deref(), Some(b"body-one".as_slice()), "{label} other implementation did not deliver body-one");
    println!("{label} consumed body-one from the other implementation");

    let flush_before = metric(ports[1].2, "queueforge_wal_fsync_seconds_count").await
        + metric(ports[1].2, "queueforge_full_flush_total").await;
    publish(ports[1].0, "qq-mix", b"body-two").await;
    let mut flushed = false;
    for _ in 0..50 {
        let now = metric(ports[1].2, "queueforge_wal_fsync_seconds_count").await
            + metric(ports[1].2, "queueforge_full_flush_total").await;
        if now > flush_before {
            flushed = true;
            println!("{label} fsync observed {flush_before} -> {now}");
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(flushed, "{label} survivor fsync did not run");

    kids.0[1].kill().unwrap();
    let _ = kids.0[1].wait();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let cfg = cluster.replace("NODE", "b").replace("PORT", &ports[1].3.to_string());
    kids.0[1] = spawn(kinds[1], ports[1].0, ports[1].1, ports[1].2, &dirs[1], &cfg);
    wait_ready(ports[1].1).await;
    tokio::time::sleep(Duration::from_millis(800)).await;

    let mut restarted = None;
    for _ in 0..50 {
        if let Some(body) = get_body(ports[1].0, "qq-mix").await {
            restarted = Some(body);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(restarted.as_deref(), Some(b"body-two".as_slice()), "{label} restarted node did not deliver body-two");
    println!("{label} consumed body-two from restarted node");
}

#[tokio::test]
async fn mixed_two_rust_one_bun() {
    let base = 51000 + (SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u16 % 300);
    mixed("rust-majority", ["rust", "bun", "rust"], base).await;
}

#[tokio::test]
async fn mixed_two_bun_one_rust() {
    let base = 53000 + (SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u16 % 300);
    mixed("bun-majority", ["bun", "rust", "bun"], base).await;
}
