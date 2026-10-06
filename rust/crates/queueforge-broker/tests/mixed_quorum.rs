//! One quorum queue across Rust and Bun. A confirm counts only a durable fsynced majority, then the leader is killed.

use std::fs;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_lite::StreamExt;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicGetOptions, BasicPublishOptions,
    ConfirmSelectOptions, ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions,
};
use lapin::types::{FieldTable, LongString};
use lapin::ExchangeKind;
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

fn write_cfg(
    path: &std::path::Path,
    amqp: u16,
    mgmt: u16,
    metrics: u16,
    data: &std::path::Path,
    cluster: &str,
) {
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

async fn publish(port: u16, queue: &str, body: &[u8]) {
    let conn = Connection::connect(
        &format!("amqp://admin:devpassword12@127.0.0.1:{port}/%2f"),
        ConnectionProperties::default(),
    )
    .await
    .expect("connect");
    let ch = conn.create_channel().await.unwrap();
    ch.confirm_select(ConfirmSelectOptions::default())
        .await
        .unwrap();
    let conf = ch
        .basic_publish(
            "",
            queue,
            BasicPublishOptions::default(),
            body,
            BasicProperties::default().with_delivery_mode(2),
        )
        .await
        .unwrap();
    assert!(
        conf.await.expect("confirm").is_ack(),
        "publisher confirm was nacked"
    );
}

async fn get_body(port: u16, queue: &str) -> Option<Vec<u8>> {
    let conn = Connection::connect(
        &format!("amqp://admin:devpassword12@127.0.0.1:{port}/%2f"),
        ConnectionProperties::default(),
    )
    .await
    .ok()?;
    let ch = conn.create_channel().await.ok()?;
    ch.basic_get(queue, BasicGetOptions { no_ack: true })
        .await
        .ok()?
        .map(|msg| msg.data.to_vec())
}

async fn get_body_strict(port: u16, queue: &str) -> Result<Option<Vec<u8>>, String> {
    let conn = Connection::connect(
        &format!("amqp://admin:devpassword12@127.0.0.1:{port}/%2f"),
        ConnectionProperties::default(),
    )
    .await
    .map_err(|err| err.to_string())?;
    let ch = conn.create_channel().await.map_err(|err| err.to_string())?;
    let got = ch
        .basic_get(queue, BasicGetOptions { no_ack: true })
        .await
        .map_err(|err| err.to_string())?;
    Ok(got.map(|msg| msg.data.to_vec()))
}

fn dir_contains(dir: &std::path::Path, needle: &[u8]) -> bool {
    let mut stack = vec![dir.to_path_buf()];
    while let Some(path) = stack.pop() {
        let Ok(entries) = fs::read_dir(&path) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            if fs::read(&path)
                .ok()
                .is_some_and(|bytes| bytes.windows(needle.len()).any(|w| w == needle))
            {
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
        let cfg = cluster
            .replace("NODE", ids[i])
            .replace("PORT", &cluster_port.to_string());
        kids.0
            .push(spawn(kinds[i], amqp, mgmt, metrics, &dir, &cfg));
        dirs.push(dir);
    }
    for (_, mgmt, _, _) in ports {
        wait_ready(mgmt).await;
    }
    tokio::time::sleep(Duration::from_millis(800)).await;

    let mut args = FieldTable::default();
    args.insert(
        "x-queue-type".into(),
        lapin::types::AMQPValue::LongString(LongString::from("quorum")),
    );
    let decl = QueueDeclareOptions {
        durable: true,
        ..QueueDeclareOptions::default()
    };
    for (amqp, _, _, _) in ports {
        let conn = Connection::connect(
            &format!("amqp://admin:devpassword12@127.0.0.1:{amqp}/%2f"),
            ConnectionProperties::default(),
        )
        .await
        .unwrap();
        let declared = conn
            .create_channel()
            .await
            .unwrap()
            .queue_declare("qq-mix", decl, args.clone())
            .await;
        if let Err(err) = &declared {
            let text = err.to_string();
            assert!(
                text.contains("exists"),
                "{label} declare on {amqp} failed: {text}"
            );
        }
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    let early = metric(ports[0].2, "queueforge_confirm_before_fsync_total").await;
    publish(ports[0].0, "qq-mix", b"body-one").await;
    let after = metric(ports[0].2, "queueforge_confirm_before_fsync_total").await;
    assert_eq!(
        after, early,
        "{label} confirm counted a pre-fsync ack ({early} -> {after})"
    );
    println!("{label} confirm before fsync stayed {after}");
    for (i, kind) in kinds.iter().enumerate() {
        if *kind != "rust" || i == 0 {
            continue;
        }
        let persisted = dir_contains(&dirs[i], b"body-one");
        assert!(
            persisted,
            "{label} rust follower {i} did not append body-one to its log"
        );
        println!("{label} rust follower {i} has body-one in its log");
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
    assert_eq!(
        seen.as_deref(),
        Some(b"body-one".as_slice()),
        "{label} other implementation did not deliver body-one"
    );
    println!("{label} consumed body-one from the other implementation");

    let flush_before = metric(ports[1].2, "queueforge_wal_fsync_seconds_count").await
        + metric(ports[1].2, "queueforge_full_flush_total").await;
    let second_started = Instant::now();
    tokio::time::timeout(
        Duration::from_secs(2),
        publish(ports[1].0, "qq-mix", b"body-two"),
    )
    .await
    .unwrap_or_else(|_| panic!("{label} second quorum publish did not return within 2s"));
    println!(
        "{label} second quorum confirm returned in {} ms",
        second_started.elapsed().as_millis()
    );
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
    let cfg = cluster
        .replace("NODE", "b")
        .replace("PORT", &ports[1].3.to_string());
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
    assert_eq!(
        restarted.as_deref(),
        Some(b"body-two".as_slice()),
        "{label} restarted node did not deliver body-two"
    );
    println!("{label} consumed body-two from restarted node");
}

#[tokio::test]
async fn mixed_two_rust_one_bun() {
    let base = 51000
        + (SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u16
            % 300);
    mixed("rust-majority", ["rust", "bun", "rust"], base).await;
}

#[tokio::test]
async fn mixed_two_bun_one_rust() {
    let base = 53000
        + (SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u16
            % 300);
    mixed("bun-majority", ["bun", "rust", "bun"], base).await;
}

/// basic.get on the next leader consumes the current leader's copy. After that
/// leader is killed, the same node must not deliver the body a second time.
async fn claim_once(label: &str, kinds: [&str; 3], base: u16) {
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
        let dir = std::env::temp_dir().join(format!("qf-claim-{label}-{base}-{i}"));
        let _ = fs::remove_dir_all(&dir);
        let cfg = cluster
            .replace("NODE", ids[i])
            .replace("PORT", &cluster_port.to_string());
        kids.0
            .push(spawn(kinds[i], amqp, mgmt, metrics, &dir, &cfg));
        dirs.push(dir);
    }
    for (_, mgmt, _, _) in ports {
        wait_ready(mgmt).await;
    }
    tokio::time::sleep(Duration::from_millis(800)).await;
    let mut args = FieldTable::default();
    args.insert(
        "x-queue-type".into(),
        lapin::types::AMQPValue::LongString(LongString::from("quorum")),
    );
    let decl = QueueDeclareOptions {
        durable: true,
        ..QueueDeclareOptions::default()
    };
    for (amqp, _, _, _) in ports {
        let conn = Connection::connect(
            &format!("amqp://admin:devpassword12@127.0.0.1:{amqp}/%2f"),
            ConnectionProperties::default(),
        )
        .await
        .unwrap();
        let declared = conn
            .create_channel()
            .await
            .unwrap()
            .queue_declare("qq-claim", decl, args.clone())
            .await;
        if let Err(err) = &declared {
            let text = err.to_string();
            assert!(
                text.contains("exists"),
                "{label} declare on {amqp} failed: {text}"
            );
        }
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    publish(ports[0].0, "qq-claim", b"body-one").await;
    let first = get_body_strict(ports[1].0, "qq-claim").await;
    assert_eq!(
        first.as_ref().map(|body| body.as_deref()),
        Ok(Some(b"body-one".as_slice())),
        "{label} non-leader get: {first:?}"
    );
    println!("{label} claimed body-one from the other implementation");
    kids.0[0].kill().unwrap();
    let _ = kids.0[0].wait();
    publish(ports[1].0, "qq-claim", b"body-two").await;
    let mut seen = None;
    for _ in 0..50 {
        if let Some(body) = get_body(ports[1].0, "qq-claim").await {
            seen = Some(body);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        seen.as_deref(),
        Some(b"body-two".as_slice()),
        "{label} delivered {seen:?} after the leader was killed"
    );
    println!("{label} next get after leader kill was body-two");
    let _ = dirs;
}

#[tokio::test]
async fn claim_once_rust_majority() {
    let base = 55000
        + (SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u16
            % 300);
    claim_once("rust-majority", ["rust", "bun", "rust"], base).await;
}

#[tokio::test]
async fn claim_once_bun_majority() {
    let base = 57000
        + (SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u16
            % 300);
    claim_once("bun-majority", ["bun", "rust", "bun"], base).await;
}

/// 32-bit FNV-1a over UTF-16, matching `bun/src/broker/routing.ts` `queueHome`.
fn bun_home(vhost: &str, name: &str, ids: &[&str]) -> String {
    let mut hash: u32 = 0x811c9dc5;
    for unit in format!("{vhost}\0{name}").encode_utf16() {
        hash ^= u32::from(unit);
        hash = hash.wrapping_mul(0x01000193);
    }
    let mut sorted = ids.to_vec();
    sorted.sort_unstable();
    sorted[(hash as usize) % sorted.len()].to_string()
}

struct Attached {
    _conn: Connection,
    _ch: lapin::Channel,
    consumer: lapin::Consumer,
}

async fn attach(port: u16, queue: &str) -> Attached {
    let conn = Connection::connect(
        &format!("amqp://admin:devpassword12@127.0.0.1:{port}/%2f"),
        ConnectionProperties::default(),
    )
    .await
    .expect("connect");
    let ch = conn.create_channel().await.expect("channel");
    let consumer = ch
        .basic_consume(
            queue,
            "c",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("consume");
    Attached {
        _conn: conn,
        _ch: ch,
        consumer,
    }
}

async fn next_body(attached: &mut Attached) -> Result<Vec<u8>, String> {
    let delivery = tokio::time::timeout(Duration::from_secs(3), attached.consumer.next())
        .await
        .map_err(|_| format!("consume timed out"))?
        .ok_or_else(|| "consumer closed".to_string())?
        .map_err(|err| err.to_string())?;
    let body = delivery.data.to_vec();
    delivery
        .ack(BasicAckOptions::default())
        .await
        .map_err(|err| err.to_string())?;
    Ok(body)
}

async fn consume_one(port: u16, queue: &str) -> Result<Vec<u8>, String> {
    let conn = Connection::connect(
        &format!("amqp://admin:devpassword12@127.0.0.1:{port}/%2f"),
        ConnectionProperties::default(),
    )
    .await
    .map_err(|err| err.to_string())?;
    let ch = conn.create_channel().await.map_err(|err| err.to_string())?;
    let mut consumer = ch
        .basic_consume(
            queue,
            "c",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .map_err(|err| err.to_string())?;
    let delivery = tokio::time::timeout(Duration::from_secs(3), consumer.next())
        .await
        .map_err(|_| format!("consume {queue} timed out"))?
        .ok_or_else(|| "consumer closed".to_string())?
        .map_err(|err| err.to_string())?;
    let body = delivery.data.to_vec();
    delivery
        .ack(BasicAckOptions::default())
        .await
        .map_err(|err| err.to_string())?;
    Ok(body)
}

#[tokio::test]
async fn classic_consume_follows_the_stored_home_across_implementations() {
    use queueforge_broker::queue_home;
    use queueforge_core::ClusterMember;

    let base = 59000
        + (SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u16
            % 300);
    let ports: [(u16, u16, u16, u16); 2] = [
        (base, base + 100, base + 200, base + 300),
        (base + 2, base + 102, base + 202, base + 302),
    ];
    let cluster = format!(
        r#"[cluster]
node_id = "NODE"
listen = "127.0.0.1:PORT"
members = [
  {{ id = "a", addr = "127.0.0.1:{}" }},
  {{ id = "b", addr = "127.0.0.1:{}" }},
]
"#,
        ports[0].3, ports[1].3
    );
    let members = vec![
        ClusterMember {
            id: "a".into(),
            addr: format!("127.0.0.1:{}", ports[0].3).parse().unwrap(),
        },
        ClusterMember {
            id: "b".into(),
            addr: format!("127.0.0.1:{}", ports[1].3).parse().unwrap(),
        },
    ];
    let mut rust_on_bun = String::new();
    let mut bun_on_rust = String::new();
    for i in 0..80 {
        let rust_name = format!("rq{i}");
        let bun_name = format!("bq{i}");
        if rust_on_bun.is_empty() && queue_home(&members, "/", &rust_name) == "b" {
            rust_on_bun = rust_name;
        }
        if bun_on_rust.is_empty() && bun_home("/", &bun_name, &["a", "b"]) == "a" {
            bun_on_rust = bun_name;
        }
    }
    assert!(!rust_on_bun.is_empty() && !bun_on_rust.is_empty());
    let mut kids = Kids(Vec::new());
    for (i, (kind, id)) in [("rust", "a"), ("bun", "b")].into_iter().enumerate() {
        let dir = std::env::temp_dir().join(format!("qf-xhome-{base}-{i}"));
        let _ = fs::remove_dir_all(&dir);
        let cfg = cluster
            .replace("NODE", id)
            .replace("PORT", &ports[i].3.to_string());
        kids.0
            .push(spawn(kind, ports[i].0, ports[i].1, ports[i].2, &dir, &cfg));
    }
    wait_ready(ports[0].1).await;
    wait_ready(ports[1].1).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    async fn declare(port: u16, queue: &str) {
        let conn = Connection::connect(
            &format!("amqp://admin:devpassword12@127.0.0.1:{port}/%2f"),
            ConnectionProperties::default(),
        )
        .await
        .unwrap();
        conn.create_channel()
            .await
            .unwrap()
            .queue_declare(
                queue,
                QueueDeclareOptions {
                    durable: true,
                    ..QueueDeclareOptions::default()
                },
                FieldTable::default(),
            )
            .await
            .unwrap();
    }
    declare(ports[0].0, &rust_on_bun).await;
    declare(ports[1].0, &bun_on_rust).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    // Attach the consumers first. A subscribe socket that closes after the
    // reply drops publishes that arrive later.
    let mut rust_side = attach(ports[0].0, &rust_on_bun).await;
    let mut bun_side = attach(ports[1].0, &bun_on_rust).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    publish(ports[0].0, &rust_on_bun, b"from-rust").await;
    publish(ports[1].0, &bun_on_rust, b"from-bun").await;
    let from_rust = next_body(&mut rust_side).await;
    let from_bun = next_body(&mut bun_side).await;
    println!(
        "classic stored home rust-client/bun-home={from_rust:?} bun-client/rust-home={from_bun:?}"
    );
    assert_eq!(from_rust.as_deref(), Ok(b"from-rust".as_slice()));
    assert_eq!(from_bun.as_deref(), Ok(b"from-bun".as_slice()));
}

async fn authed(mgmt: u16) -> Client {
    let client = Client::builder().cookie_store(true).build().unwrap();
    let res = client
        .post(format!("http://127.0.0.1:{mgmt}/api/login"))
        .json(&serde_json::json!({"username": "admin", "password": "devpassword12"}))
        .send()
        .await
        .unwrap();
    assert!(res.status().is_success(), "login {mgmt} {}", res.status());
    client
}

async fn delete_member(client: &Client, mgmt: u16, id: &str) -> (u16, String) {
    let res = client
        .delete(format!("http://127.0.0.1:{mgmt}/api/nodes/{id}"))
        .send()
        .await
        .unwrap();
    let status = res.status().as_u16();
    let body = res.text().await.unwrap_or_default();
    (status, body)
}

async fn membership_refuses(kind: &str, base: u16) {
    use queueforge_broker::queue_home;
    use queueforge_core::ClusterMember;

    let ports: [(u16, u16, u16, u16); 2] = [
        (base, base + 100, base + 200, base + 300),
        (base + 2, base + 102, base + 202, base + 302),
    ];
    let cluster = format!(
        r#"[cluster]
node_id = "NODE"
listen = "127.0.0.1:PORT"
members = [
  {{ id = "a", addr = "127.0.0.1:{}" }},
  {{ id = "b", addr = "127.0.0.1:{}" }},
]
"#,
        ports[0].3, ports[1].3
    );
    let members = vec![
        ClusterMember {
            id: "a".into(),
            addr: format!("127.0.0.1:{}", ports[0].3).parse().unwrap(),
        },
        ClusterMember {
            id: "b".into(),
            addr: format!("127.0.0.1:{}", ports[1].3).parse().unwrap(),
        },
    ];
    let mut queue = String::new();
    for i in 0..80 {
        let name = format!("home{i}");
        let home = if kind == "rust" {
            queue_home(&members, "/", &name).to_string()
        } else {
            bun_home("/", &name, &["a", "b"])
        };
        if home == "b" {
            queue = name;
            break;
        }
    }
    assert!(!queue.is_empty(), "{kind} found no queue homed on b");
    let mut kids = Kids(Vec::new());
    let mut dirs = Vec::new();
    for (i, id) in ["a", "b"].into_iter().enumerate() {
        let dir = std::env::temp_dir().join(format!("qf-mem-{kind}-{base}-{i}"));
        let _ = fs::remove_dir_all(&dir);
        let cfg = cluster
            .replace("NODE", id)
            .replace("PORT", &ports[i].3.to_string());
        kids.0
            .push(spawn(kind, ports[i].0, ports[i].1, ports[i].2, &dir, &cfg));
        dirs.push(dir);
    }
    wait_ready(ports[0].1).await;
    wait_ready(ports[1].1).await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    let http = authed(ports[0].1).await;
    let (self_status, self_body) = delete_member(&http, ports[0].1, "a").await;
    println!("{kind} delete self status={self_status} body={self_body}");
    assert_eq!(self_status, 400, "{kind} delete self");
    assert!(
        self_body.contains("cannot forget itself"),
        "{kind} self body {self_body}"
    );
    let conn = Connection::connect(
        &format!("amqp://admin:devpassword12@127.0.0.1:{}/%2f", ports[0].0),
        ConnectionProperties::default(),
    )
    .await
    .unwrap();
    conn.create_channel()
        .await
        .unwrap()
        .queue_declare(
            &queue,
            QueueDeclareOptions {
                durable: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap();
    let (home_status, home_body) = delete_member(&http, ports[0].1, "b").await;
    println!("{kind} delete homed member status={home_status} body={home_body}");
    assert_eq!(home_status, 400, "{kind} delete home");
    assert!(
        home_body.contains("still homes a classic queue"),
        "{kind} home body {home_body}"
    );
    let ghost = serde_json::json!([{"id": "ghost", "addr": "127.0.0.1:9"}]);
    if kind == "bun" {
        kids.0[0].kill().expect("kill bun");
        let _ = kids.0[0].wait();
        fs::write(dirs[0].join("members.json"), ghost.to_string()).unwrap();
        let cfg = cluster
            .replace("NODE", "a")
            .replace("PORT", &ports[0].3.to_string());
        kids.0[0] = spawn(kind, ports[0].0, ports[0].1, ports[0].2, &dirs[0], &cfg);
        wait_ready(ports[0].1).await;
    } else {
        fs::write(dirs[0].join("members.json"), ghost.to_string()).unwrap();
    }
    let http = authed(ports[0].1).await;
    let (empty_status, empty_body) = delete_member(&http, ports[0].1, "ghost").await;
    println!("{kind} delete last member status={empty_status} body={empty_body}");
    assert_eq!(empty_status, 400, "{kind} empty list");
    assert!(
        empty_body.contains("cannot become empty"),
        "{kind} empty body {empty_body}"
    );
    let _ = conn;
}

#[tokio::test]
async fn membership_delete_refuses_self_homed_member_and_empty_list() {
    let base = 60100
        + (SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u16
            % 200);
    membership_refuses("rust", base).await;
    membership_refuses("bun", base + 20).await;
}

async fn put_json(client: &Client, mgmt: u16, path: &str, body: serde_json::Value) -> u16 {
    let res = client
        .put(format!("http://127.0.0.1:{mgmt}{path}"))
        .json(&body)
        .send()
        .await
        .unwrap();
    let status = res.status().as_u16();
    if !(200..300).contains(&status) {
        let text = res.text().await.unwrap_or_default();
        panic!("PUT {path} -> {status} {text}");
    }
    status
}

async fn wait_body(port: u16, queue: &str, expect: &[u8]) -> Vec<u8> {
    let deadline = Instant::now() + Duration::from_secs(8);
    while Instant::now() < deadline {
        if let Some(body) = get_body(port, queue).await {
            if body.as_slice() == expect {
                return body;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!(
        "timed out waiting for {} on {port}/{queue}",
        String::from_utf8_lossy(expect)
    );
}

/// Shovel and federation each dial the other process. The body cannot move in memory.
async fn uri_across(owner: &str, peer: &str, base: u16) {
    let ports: [(u16, u16, u16, u16); 2] = [
        (base, base + 100, base + 200, base + 300),
        (base + 2, base + 102, base + 202, base + 302),
    ];
    let mut kids = Kids(Vec::new());
    for (i, kind) in [owner, peer].into_iter().enumerate() {
        let dir = std::env::temp_dir().join(format!("qf-uri-{owner}-{base}-{i}"));
        let _ = fs::remove_dir_all(&dir);
        kids.0
            .push(spawn(kind, ports[i].0, ports[i].1, ports[i].2, &dir, ""));
    }
    wait_ready(ports[0].1).await;
    wait_ready(ports[1].1).await;
    let owner_http = authed(ports[0].1).await;
    let owner_uri = format!("amqp://admin:devpassword12@127.0.0.1:{}", ports[0].0);
    let peer_uri = format!("amqp://admin:devpassword12@127.0.0.1:{}", ports[1].0);
    let owner_ch =
        Connection::connect(&format!("{owner_uri}/%2f"), ConnectionProperties::default())
            .await
            .unwrap()
            .create_channel()
            .await
            .unwrap();
    let peer_ch = Connection::connect(&format!("{peer_uri}/%2f"), ConnectionProperties::default())
        .await
        .unwrap()
        .create_channel()
        .await
        .unwrap();
    let durable = QueueDeclareOptions {
        durable: true,
        ..QueueDeclareOptions::default()
    };
    owner_ch
        .queue_declare("shovel-src", durable, FieldTable::default())
        .await
        .unwrap();
    peer_ch
        .queue_declare("shovel-dest", durable, FieldTable::default())
        .await
        .unwrap();
    owner_ch
        .basic_publish(
            "",
            "shovel-src",
            BasicPublishOptions::default(),
            b"shovel-body",
            BasicProperties::default(),
        )
        .await
        .unwrap()
        .await
        .ok();
    let shovel = put_json(
        &owner_http,
        ports[0].1,
        "/api/parameters/shovel/%2F/move",
        serde_json::json!({"value": {
            "src-protocol": "amqp091",
            "src-uri": owner_uri,
            "src-queue": "shovel-src",
            "dest-protocol": "amqp091",
            "dest-uri": peer_uri,
            "dest-queue": "shovel-dest"
        }}),
    )
    .await;
    let moved = wait_body(ports[1].0, "shovel-dest", b"shovel-body").await;
    println!(
        "{owner} uri shovel status={shovel} delivered {}",
        String::from_utf8_lossy(&moved)
    );

    let exchange = ExchangeDeclareOptions {
        durable: true,
        ..ExchangeDeclareOptions::default()
    };
    for ch in [&owner_ch, &peer_ch] {
        ch.exchange_declare(
            "fed.ex",
            ExchangeKind::Topic,
            exchange,
            FieldTable::default(),
        )
        .await
        .unwrap();
        ch.queue_declare("fed-q", durable, FieldTable::default())
            .await
            .unwrap();
        ch.queue_bind(
            "fed-q",
            "fed.ex",
            "k",
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    }
    let upstream = put_json(
        &owner_http,
        ports[0].1,
        "/api/parameters/federation-upstream/%2F/origin",
        serde_json::json!({"value": {"uri": format!("{peer_uri}/")}}),
    )
    .await;
    let policy = put_json(
        &owner_http,
        ports[0].1,
        "/api/policies/%2F/fed-pol",
        serde_json::json!({
            "pattern": "^fed\\.ex$",
            "apply-to": "exchanges",
            "definition": {"federation-upstream-set": "all"}
        }),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    peer_ch
        .basic_publish(
            "fed.ex",
            "k",
            BasicPublishOptions::default(),
            b"fed-body",
            BasicProperties::default(),
        )
        .await
        .unwrap()
        .await
        .ok();
    let copied = wait_body(ports[0].0, "fed-q", b"fed-body").await;
    println!(
        "{owner} uri federation upstream={upstream} policy={policy} delivered {}",
        String::from_utf8_lossy(&copied)
    );
}

#[tokio::test]
async fn uri_shovel_and_federation_move_one_body_across_processes() {
    let base = 62100
        + (SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u16
            % 200);
    uri_across("rust", "bun", base).await;
    uri_across("bun", "rust", base + 20).await;
}
