//! Live AMQP path: a confirmed quorum publish survives kill -9 of the leader.
//!
//! `queueforge_broker::quorum_confirm::durable_majority` is what `Cluster::quorum_enqueue` calls
//! after each member flushes. This file drives that path with lapin. A second consume after
//! the killed member restarts must not see the body again.

use std::collections::HashSet;
use std::fs;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_lite::StreamExt;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicGetOptions, BasicPublishOptions, BasicQosOptions,
    ConfirmSelectOptions, QueueDeclareOptions,
};
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

fn write_cfg(
    path: &std::path::Path,
    amqp: u16,
    mgmt: u16,
    metrics: u16,
    data: &std::path::Path,
    cluster: &str,
    policy: &str,
    interval_ms: u64,
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
fsync_policy = "{policy}"
fsync_interval_ms = {interval_ms}
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

fn spawn(
    kind: &str,
    amqp: u16,
    mgmt: u16,
    metrics: u16,
    data: &PathBuf,
    cluster: &str,
    policy: &str,
    interval_ms: u64,
) -> Child {
    let cfg = data.join("qf.toml");
    write_cfg(
        &cfg,
        amqp,
        mgmt,
        metrics,
        data,
        cluster,
        policy,
        interval_ms,
    );
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

async fn declare_quorum(port: u16, queue: &str) {
    let conn = Connection::connect(
        &format!("amqp://admin:devpassword12@127.0.0.1:{port}/%2f"),
        ConnectionProperties::default(),
    )
    .await
    .unwrap();
    let mut args = FieldTable::default();
    args.insert(
        "x-queue-type".into(),
        lapin::types::AMQPValue::LongString(LongString::from("quorum")),
    );
    conn.create_channel()
        .await
        .unwrap()
        .queue_declare(
            queue,
            QueueDeclareOptions {
                durable: true,
                ..QueueDeclareOptions::default()
            },
            args,
        )
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
    let got = ch
        .basic_get(queue, BasicGetOptions { no_ack: false })
        .await
        .ok()??;
    let body = got.data.to_vec();
    let key = got.routing_key.to_string();
    ch.basic_ack(got.delivery_tag, BasicAckOptions::default())
        .await
        .ok()?;
    Some((body, key))
}

/// One `basic.get` after `/readyz` has already returned ready. Connect failure panics.
/// `None` is an empty queue. A body is a delivery.
async fn take_ready(port: u16, queue: &str) -> Option<Vec<u8>> {
    let conn = Connection::connect(
        &format!("amqp://admin:devpassword12@127.0.0.1:{port}/%2f"),
        ConnectionProperties::default(),
    )
    .await
    .expect("amqp accepts connections once readyz is ready");
    let ch = conn.create_channel().await.expect("channel after readyz");
    let got = ch
        .basic_get(queue, BasicGetOptions { no_ack: false })
        .await
        .expect("basic.get after readyz");
    got.map(|msg| msg.data.to_vec())
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
        let cfg = cluster
            .replace("NODE", ids[i])
            .replace("PORT", &cluster_port.to_string());
        kids.0.push(spawn(
            kinds[i],
            amqp,
            mgmt,
            metrics,
            &dir,
            &cfg,
            "every_n_ms",
            400,
        ));
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

    let again = take(ports[1].0, "qq-durable")
        .await
        .or(take(ports[2].0, "qq-durable").await);
    assert!(
        again.is_none(),
        "{label} delivered the body twice before restart"
    );

    kids.0[0] = spawn(
        kinds[0],
        ports[0].0,
        ports[0].1,
        ports[0].2,
        &dirs[0],
        &cluster
            .replace("NODE", "a")
            .replace("PORT", &ports[0].3.to_string()),
        "every_n_ms",
        400,
    );
    wait_ready(ports[0].1).await;
    // The first basic.get after readyz is the check. A sleep here would hide a
    // body that was already on the ready queue when /readyz flipped.
    let on_leader = take_ready(ports[0].0, "qq-durable").await;
    let on_survivor = take_ready(ports[1].0, "qq-durable").await;
    assert!(
        on_leader.is_none() && on_survivor.is_none(),
        "{label} restart duplicated or resurrected the body: leader={} survivor={}",
        on_leader
            .as_ref()
            .map(|body| String::from_utf8_lossy(body).into_owned())
            .unwrap_or_default(),
        on_survivor
            .as_ref()
            .map(|body| String::from_utf8_lossy(body).into_owned())
            .unwrap_or_default()
    );
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
    let Ok(ch) = conn.create_channel().await else {
        return false;
    };
    if ch
        .confirm_select(ConfirmSelectOptions::default())
        .await
        .is_err()
    {
        return false;
    }
    let Ok(conf) = ch
        .basic_publish(
            "",
            queue,
            BasicPublishOptions::default(),
            body,
            BasicProperties::default().with_delivery_mode(2),
        )
        .await
    else {
        return false;
    };
    matches!(conf.await, Ok(ack) if ack.is_ack())
}

async fn classic(policy: &str, wait_for_fsync: bool) {
    // Ports sit below the macOS ephemeral range (49152+), so outbound dials cannot take them.
    // The three policies run in parallel; each gets its own 250-port block.
    let slot = match policy {
        "every_n_ms" => 0,
        "always" => 1,
        _ => 2,
    };
    let base = 40000
        + slot * 250
        + (SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u16
            % 40);
    let dir = std::env::temp_dir().join(format!("qf-classic-{policy}-{base}"));
    let _ = fs::remove_dir_all(&dir);
    let child = spawn("rust", base, base + 100, base + 200, &dir, "", policy, 400);
    let mut kids = Kids(vec![child]);
    wait_ready(base + 100).await;
    let before = metric(base + 200, "queueforge_wal_fsync_seconds_count").await;
    let early = metric(base + 200, "queueforge_confirm_before_fsync_total").await;
    let conn = Connection::connect(
        &format!("amqp://admin:devpassword12@127.0.0.1:{base}/%2f"),
        ConnectionProperties::default(),
    )
    .await
    .unwrap();
    let ch = conn.create_channel().await.unwrap();
    ch.queue_declare(
        "classic-q",
        QueueDeclareOptions {
            durable: true,
            ..QueueDeclareOptions::default()
        },
        FieldTable::default(),
    )
    .await
    .unwrap();
    ch.confirm_select(ConfirmSelectOptions::default())
        .await
        .unwrap();
    // The first confirm creates the segment. Time the next lone confirm.
    let warm = ch
        .basic_publish(
            "",
            "classic-q",
            BasicPublishOptions::default(),
            b"warm",
            BasicProperties::default().with_delivery_mode(2),
        )
        .await
        .unwrap();
    assert!(warm.await.expect("warm confirm").is_ack());
    let started = Instant::now();
    let conf = ch
        .basic_publish(
            "",
            "classic-q",
            BasicPublishOptions::default(),
            b"classic",
            BasicProperties::default().with_delivery_mode(2),
        )
        .await
        .unwrap();
    assert!(conf.await.expect("classic confirm").is_ack());
    let elapsed = started.elapsed();
    let after = metric(base + 200, "queueforge_wal_fsync_seconds_count").await;
    let counter = metric(base + 200, "queueforge_confirm_before_fsync_total").await;
    println!(
        "classic {policy} elapsed_ms={:.3} fsync {before} -> {after} confirm_before_fsync {early} -> {counter}",
        elapsed.as_secs_f64() * 1000.0
    );
    assert_eq!(
        counter, early,
        "{policy} confirm moved queueforge_confirm_before_fsync_total"
    );
    if policy == "every_n_ms" {
        assert!(
            // The interval is 400 ms; 100 ms leaves room for a loaded machine.
            elapsed < Duration::from_millis(100),
            "{policy} lone confirm waited out the group-commit interval ({elapsed:?})"
        );
        assert!(after > before, "{policy} confirm returned before fsync");
    } else if wait_for_fsync {
        assert!(after > before, "{policy} confirm returned before fsync");
    }
    kids.0[0].kill().expect("kill -9");
    let _ = kids.0[0].wait();
    kids.0[0] = spawn("rust", base, base + 100, base + 200, &dir, "", policy, 400);
    wait_ready(base + 100).await;
    let mut saw_classic = 0u32;
    let mut saw_warm = 0u32;
    let mut empty = 0u32;
    for _ in 0..40 {
        match take(base, "classic-q").await {
            Some((body, _)) if body == b"classic" => saw_classic += 1,
            Some((body, _)) if body == b"warm" => saw_warm += 1,
            Some((body, _)) => panic!("{policy} delivered an unexpected body: {body:?}"),
            None => {
                empty += 1;
                if saw_classic > 0 && empty >= 2 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
    assert_eq!(
        saw_classic, 1,
        "{policy} restart delivered the confirmed body {saw_classic} times"
    );
    assert_eq!(saw_warm, 1, "{policy} warmup body was not delivered once");
    println!("classic {policy} survived kill -9");
}

/// Tests that assert a confirm latency run one at a time. Beside them, the
/// other cases here each run a three-node cluster on the same disk, and
/// their fsyncs are what such a test would measure.
static LATENCY: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[tokio::test]
async fn forwarded_classic_confirm_waits_for_the_home_fsync() {
    let _quiet = LATENCY.lock().await;
    use queueforge_broker::queue_home;
    use queueforge_core::ClusterMember;

    let base = 41000
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
    let mut queue = String::new();
    for i in 0..80 {
        let candidate = format!("fwd{i}");
        if queue_home(&members, "/", &candidate) == "b" {
            queue = candidate;
            break;
        }
    }
    assert!(!queue.is_empty(), "no queue name homed on b");
    let mut kids = Kids(Vec::new());
    let mut dirs = Vec::new();
    for (i, id) in ["a", "b"].into_iter().enumerate() {
        let dir = std::env::temp_dir().join(format!("qf-fwd-{base}-{i}"));
        let _ = fs::remove_dir_all(&dir);
        let cfg = cluster
            .replace("NODE", id)
            .replace("PORT", &ports[i].3.to_string());
        kids.0.push(spawn(
            "rust",
            ports[i].0,
            ports[i].1,
            ports[i].2,
            &dir,
            &cfg,
            "every_n_ms",
            10,
        ));
        dirs.push(dir);
    }
    wait_ready(ports[0].1).await;
    wait_ready(ports[1].1).await;
    tokio::time::sleep(Duration::from_millis(400)).await;
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
            .queue_declare(
                &queue,
                QueueDeclareOptions {
                    durable: true,
                    ..QueueDeclareOptions::default()
                },
                FieldTable::default(),
            )
            .await;
        if let Err(err) = declared {
            assert!(
                err.to_string().contains("exists"),
                "declare {queue} on {amqp}: {err}"
            );
        }
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    let before = metric(ports[1].2, "queueforge_wal_fsync_seconds_count").await;
    let early = metric(ports[0].2, "queueforge_confirm_before_fsync_total").await;
    let conn = Connection::connect(
        &format!("amqp://admin:devpassword12@127.0.0.1:{}/%2f", ports[0].0),
        ConnectionProperties::default(),
    )
    .await
    .unwrap();
    let ch = conn.create_channel().await.unwrap();
    ch.confirm_select(ConfirmSelectOptions::default())
        .await
        .unwrap();
    let started = Instant::now();
    let conf = tokio::time::timeout(Duration::from_secs(2), async {
        let conf = ch
            .basic_publish(
                "",
                &queue,
                BasicPublishOptions::default(),
                b"forwarded-body",
                BasicProperties::default().with_delivery_mode(2),
            )
            .await
            .expect("publish frame");
        conf.await.expect("confirm").is_ack()
    })
    .await
    .expect("forwarded confirm did not return within 2s");
    assert!(conf, "forwarded publish was nacked");
    let elapsed = started.elapsed();
    let after = metric(ports[1].2, "queueforge_wal_fsync_seconds_count").await;
    let counter = metric(ports[0].2, "queueforge_confirm_before_fsync_total").await;
    println!("forwarded classic elapsed_ms={} home fsync {before} -> {after} confirm_before_fsync {early} -> {counter}", elapsed.as_millis());
    assert!(
        after > before,
        "home fsync did not cover the forwarded append"
    );
    assert_eq!(
        counter, early,
        "forwarded confirm moved queueforge_confirm_before_fsync_total"
    );
    // About 13 ms alone. The other cases here run beside it, three brokers
    // each on the same disk, so the bound leaves room for their fsyncs.
    assert!(
        elapsed < Duration::from_millis(100),
        "remote confirm stalled at {} ms",
        elapsed.as_millis()
    );
    kids.0[1].kill().expect("kill home");
    let _ = kids.0[1].wait();
    let cfg = cluster
        .replace("NODE", "b")
        .replace("PORT", &ports[1].3.to_string());
    kids.0[1] = spawn(
        "rust",
        ports[1].0,
        ports[1].1,
        ports[1].2,
        &dirs[1],
        &cfg,
        "every_n_ms",
        10,
    );
    wait_ready(ports[1].1).await;
    let mut got = None;
    for _ in 0..40 {
        if let Some(msg) = take(ports[1].0, &queue).await {
            got = Some(msg);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        got.as_ref().map(|(body, _)| body.as_slice()),
        Some(b"forwarded-body".as_slice())
    );
    assert!(
        take(ports[1].0, &queue).await.is_none(),
        "forwarded body was delivered twice"
    );
    println!("forwarded classic survived kill -9 of the home");
}

#[tokio::test]
async fn pipelined_forwarded_confirms_share_one_home_fsync() {
    use queueforge_broker::queue_home;
    use queueforge_core::ClusterMember;

    let base = 42000
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
    let mut queue = String::new();
    for i in 0..80 {
        let candidate = format!("pipe{i}");
        if queue_home(&members, "/", &candidate) == "b" {
            queue = candidate;
            break;
        }
    }
    assert!(!queue.is_empty(), "no queue name homed on b");
    let mut kids = Kids(Vec::new());
    for (i, id) in ["a", "b"].into_iter().enumerate() {
        let dir = std::env::temp_dir().join(format!("qf-pipe-{base}-{i}"));
        let _ = fs::remove_dir_all(&dir);
        let cfg = cluster
            .replace("NODE", id)
            .replace("PORT", &ports[i].3.to_string());
        kids.0.push(spawn(
            "rust",
            ports[i].0,
            ports[i].1,
            ports[i].2,
            &dir,
            &cfg,
            "every_n_ms",
            400,
        ));
    }
    wait_ready(ports[0].1).await;
    wait_ready(ports[1].1).await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    let conn = Connection::connect(
        &format!("amqp://admin:devpassword12@127.0.0.1:{}/%2f", ports[0].0),
        ConnectionProperties::default(),
    )
    .await
    .unwrap();
    let ch = conn.create_channel().await.unwrap();
    ch.queue_declare(
        &queue,
        QueueDeclareOptions {
            durable: true,
            ..QueueDeclareOptions::default()
        },
        FieldTable::default(),
    )
    .await
    .unwrap();
    ch.confirm_select(ConfirmSelectOptions::default())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let before = metric(ports[1].2, "queueforge_wal_fsync_seconds_count").await;
    let early = metric(ports[0].2, "queueforge_confirm_before_fsync_total").await;
    let started = Instant::now();
    let n = 32u8;
    let mut waiting = Vec::with_capacity(n as usize);
    for i in 0..n {
        let conf = ch
            .basic_publish(
                "",
                &queue,
                BasicPublishOptions::default(),
                &[i],
                BasicProperties::default().with_delivery_mode(2),
            )
            .await
            .expect("publish frame");
        waiting.push(conf);
    }
    let joined = tokio::time::timeout(Duration::from_secs(3), async {
        for conf in waiting {
            assert!(
                conf.await.expect("confirm").is_ack(),
                "forwarded publish nacked"
            );
        }
    })
    .await;
    let elapsed = started.elapsed();
    assert!(
        joined.is_ok(),
        "32 pipelined remote confirms did not finish in 3s ({elapsed:?})"
    );
    let after = metric(ports[1].2, "queueforge_wal_fsync_seconds_count").await;
    let counter = metric(ports[0].2, "queueforge_confirm_before_fsync_total").await;
    println!(
        "pipelined forwarded elapsed_ms={} home fsync {before} -> {after} confirm_before_fsync {early} -> {counter}",
        elapsed.as_millis()
    );
    assert!(after > before, "home fsync did not cover the batch");
    assert!(
        after - before <= 3.0,
        "home fsynced once per message ({before} -> {after})"
    );
    assert_eq!(
        counter, early,
        "pipelined confirm moved confirm_before_fsync"
    );
    assert!(
        elapsed < Duration::from_millis(1500),
        "remote batch took {} ms; each publish waited for its own fsync",
        elapsed.as_millis()
    );
    let mut got = 0u8;
    for _ in 0..n {
        if take(ports[1].0, &queue).await.is_some() {
            got += 1;
        }
    }
    assert_eq!(got, n, "home did not retain every pipelined body");
}

#[tokio::test]
async fn rust_rust_bun_confirmed_publish_survives_leader_kill() {
    let base = 43000
        + (SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u16
            % 200);
    failover("rust-rust-bun", ["rust", "rust", "bun"], base).await;
}

#[tokio::test]
async fn bun_bun_rust_confirmed_publish_survives_leader_kill() {
    let base = 44000
        + (SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u16
            % 200);
    failover("bun-bun-rust", ["bun", "bun", "rust"], base).await;
}

fn bun_fnv32(text: &str) -> u32 {
    let mut hash: u32 = 0x811c9dc5;
    for unit in text.encode_utf16() {
        hash ^= u32::from(unit);
        hash = hash.wrapping_mul(0x01000193);
    }
    hash
}

/// Queue name whose Bun home is `index` among `width` sorted member ids.
fn bun_homed_name(index: usize, width: usize) -> String {
    for n in 0..4000 {
        let name = format!("qh-{n}");
        let key = format!("/\0{name}");
        if (bun_fnv32(&key) as usize) % width == index {
            return name;
        }
    }
    panic!("no bun home name for index {index}");
}

async fn ready_messages(metrics: u16, queue: &str) -> f64 {
    let text = match Client::new()
        .get(format!("http://127.0.0.1:{metrics}/metrics"))
        .send()
        .await
    {
        Ok(res) => res.text().await.unwrap_or_default(),
        Err(_) => return 0.0,
    };
    let needle = format!("queue=\"{queue}\"");
    for line in text.lines() {
        if line.starts_with("rabbitmq_queue_messages_ready{") && line.contains(&needle) {
            return line
                .split_whitespace()
                .last()
                .unwrap_or("0")
                .parse()
                .unwrap_or(0.0);
        }
    }
    0.0
}

/// Leader ack must delete the follower's ready copy. A membership "forget"
/// leaves that copy, so the follower delivers it again after the leader is gone.
#[tokio::test]
async fn quorum_ack_drops_the_follower_copy() {
    let ports = [free_port(), free_port(), free_port(), free_port()];
    let peer = [free_port(), free_port(), free_port(), free_port()];
    let dirs = [
        std::env::temp_dir().join(format!("qf-drop-a-{}", ports[0])),
        std::env::temp_dir().join(format!("qf-drop-b-{}", peer[0])),
    ];
    for dir in &dirs {
        let _ = fs::remove_dir_all(dir);
    }
    let cluster = format!(
        r#"[cluster]
node_id = "NODE"
listen = "127.0.0.1:PORT"
members = [
  {{ id = "a", addr = "127.0.0.1:{}" }},
  {{ id = "b", addr = "127.0.0.1:{}" }},
]
"#,
        ports[3], peer[3]
    );
    let mut kids = Kids(Vec::new());
    kids.0.push(spawn(
        "rust",
        ports[0],
        ports[1],
        ports[2],
        &dirs[0],
        &cluster
            .replace("NODE", "a")
            .replace("PORT", &ports[3].to_string()),
        "every_n_ms",
        20,
    ));
    kids.0.push(spawn(
        "rust",
        peer[0],
        peer[1],
        peer[2],
        &dirs[1],
        &cluster
            .replace("NODE", "b")
            .replace("PORT", &peer[3].to_string()),
        "every_n_ms",
        20,
    ));
    wait_ready(ports[1]).await;
    wait_ready(peer[1]).await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    declare_quorum(ports[0], "qq-drop").await;
    publish(ports[0], "qq-drop", b"once").await;
    let mut stored = 0.0;
    for _ in 0..20 {
        stored = ready_messages(peer[2], "qq-drop").await;
        if stored >= 1.0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        stored >= 1.0,
        "follower did not store the quorum body (ready={stored})"
    );
    let got = take(ports[0], "qq-drop").await.expect("leader get");
    assert_eq!(got.0, b"once");
    let mut left = ready_messages(peer[2], "qq-drop").await;
    for _ in 0..10 {
        if left == 0.0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
        left = ready_messages(peer[2], "qq-drop").await;
    }
    assert_eq!(
        left, 0.0,
        "follower still holds the body after the leader ack (ready={left})"
    );
    drop(kids);
    for dir in &dirs {
        let _ = fs::remove_dir_all(dir);
    }
}

/// A quorum declare on Bun is local. The classic hash may name a peer that
/// accepts TCP and never answers. That peer must not fail the declare.
/// Replicate still waits the peer out, so the budget is the replicate timeout
/// plus a margin, not the old "declare b: queue home is unavailable" failure.
#[tokio::test]
async fn bun_quorum_declare_does_not_wait_on_the_classic_home() {
    let amqp = free_port();
    let mgmt = free_port();
    let metrics = free_port();
    let cluster_port = free_port();
    let silent_port = free_port();
    let silent = std::net::TcpListener::bind(("127.0.0.1", silent_port)).expect("silent peer");
    std::thread::spawn(move || {
        for _ in 0..4 {
            let Ok((mut sock, _)) = silent.accept() else {
                break;
            };
            std::thread::spawn(move || {
                let mut buf = [0u8; 1024];
                loop {
                    match std::io::Read::read(&mut sock, &mut buf) {
                        Ok(0) | Err(_) => break,
                        Ok(_) => {}
                    }
                }
            });
        }
    });
    let dir = std::env::temp_dir().join(format!("qf-qdecl-{amqp}"));
    let _ = fs::remove_dir_all(&dir);
    let cluster = format!(
        r#"[cluster]
node_id = "a"
listen = "127.0.0.1:{cluster_port}"
members = [
  {{ id = "a", addr = "127.0.0.1:{cluster_port}" }},
  {{ id = "b", addr = "127.0.0.1:{silent_port}" }},
]
"#
    );
    let mut kids = Kids(Vec::new());
    kids.0.push(spawn(
        "bun",
        amqp,
        mgmt,
        metrics,
        &dir,
        &cluster,
        "every_n_ms",
        20,
    ));
    wait_ready(mgmt).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(bun_fnv32("/\0qh-1"), 3_250_089_577);
    assert_eq!(bun_homed_name(1, 2), "qh-1");
    let name = bun_homed_name(1, 2);
    let started = Instant::now();
    let declared = tokio::time::timeout(Duration::from_secs(5), declare_quorum(amqp, &name)).await;
    assert!(
        declared.is_ok(),
        "quorum declare whose classic home is the silent peer failed ({:?})",
        started.elapsed()
    );
    let again =
        tokio::time::timeout(Duration::from_secs(5), declare_quorum(amqp, "qh-second")).await;
    assert!(
        again.is_ok(),
        "second quorum declare failed after the silent peer ({:?})",
        started.elapsed()
    );
    let _ = fs::remove_dir_all(&dir);
}

/// Bun's declare record must arrive on Rust as quorum, and the body must be stored there.
#[tokio::test]
async fn bun_replicated_quorum_queue_is_quorum_on_rust() {
    let bun_ports = [free_port(), free_port(), free_port(), free_port()];
    let rust_ports = [free_port(), free_port(), free_port(), free_port()];
    let dirs = [
        std::env::temp_dir().join(format!("qf-brep-a-{}", bun_ports[0])),
        std::env::temp_dir().join(format!("qf-brep-b-{}", rust_ports[0])),
    ];
    for dir in &dirs {
        let _ = fs::remove_dir_all(dir);
    }
    let cluster = format!(
        r#"[cluster]
node_id = "NODE"
listen = "127.0.0.1:PORT"
members = [
  {{ id = "a", addr = "127.0.0.1:{}" }},
  {{ id = "b", addr = "127.0.0.1:{}" }},
]
"#,
        bun_ports[3], rust_ports[3]
    );
    let mut kids = Kids(Vec::new());
    kids.0.push(spawn(
        "bun",
        bun_ports[0],
        bun_ports[1],
        bun_ports[2],
        &dirs[0],
        &cluster
            .replace("NODE", "a")
            .replace("PORT", &bun_ports[3].to_string()),
        "every_n_ms",
        20,
    ));
    kids.0.push(spawn(
        "rust",
        rust_ports[0],
        rust_ports[1],
        rust_ports[2],
        &dirs[1],
        &cluster
            .replace("NODE", "b")
            .replace("PORT", &rust_ports[3].to_string()),
        "every_n_ms",
        20,
    ));
    wait_ready(bun_ports[1]).await;
    wait_ready(rust_ports[1]).await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    let name = bun_homed_name(0, 2);
    declare_quorum(bun_ports[0], &name).await;
    publish(bun_ports[0], &name, b"from-bun").await;
    let mut stored = 0.0;
    for _ in 0..30 {
        stored = ready_messages(rust_ports[2], &name).await;
        if stored >= 1.0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        stored >= 1.0,
        "rust did not store the bun quorum body (ready={stored})"
    );
    let got = take(bun_ports[0], &name).await.expect("bun leader get");
    assert_eq!(got.0, b"from-bun");
    let mut left = ready_messages(rust_ports[2], &name).await;
    for _ in 0..20 {
        if left == 0.0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        left = ready_messages(rust_ports[2], &name).await;
    }
    assert_eq!(
        left, 0.0,
        "rust still holds the body after the bun leader get (ready={left})"
    );
    let http = Client::builder().cookie_store(true).build().unwrap();
    let login = http
        .post(format!("http://127.0.0.1:{}/api/login", rust_ports[1]))
        .json(&serde_json::json!({"username": "admin", "password": "devpassword12"}))
        .send()
        .await
        .expect("login");
    assert!(login.status().is_success(), "login {}", login.status());
    let queue = http
        .get(format!(
            "http://127.0.0.1:{}/api/queues/%2F/{name}",
            rust_ports[1]
        ))
        .send()
        .await
        .expect("queue")
        .json::<serde_json::Value>()
        .await
        .expect("queue json");
    assert_eq!(queue["type"].as_str(), Some("quorum"), "queue json {queue}");
    drop(kids);
    for dir in &dirs {
        let _ = fs::remove_dir_all(dir);
    }
}

/// A peer that accepts the socket and never answers must stay in the membership.
/// The next call waits for that peer again. Dropping it made the next declare fail
/// at once with "queue home is unavailable".
#[tokio::test]
async fn slow_peer_remains_after_a_call_times_out() {
    for kind in ["rust", "bun"] {
        slow_peer_remains(kind).await;
    }
}

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("free port");
    listener.local_addr().expect("local addr").port()
}

async fn slow_peer_remains(kind: &str) {
    let amqp = free_port();
    let mgmt = free_port();
    let metrics = free_port();
    let cluster_port = free_port();
    let silent_port = free_port();
    let silent = std::net::TcpListener::bind(("127.0.0.1", silent_port)).expect("silent peer");
    std::thread::spawn(move || {
        for _ in 0..8 {
            let Ok((mut sock, _)) = silent.accept() else {
                break;
            };
            std::thread::spawn(move || {
                let mut buf = [0u8; 1024];
                loop {
                    match std::io::Read::read(&mut sock, &mut buf) {
                        Ok(0) | Err(_) => break,
                        Ok(_) => {}
                    }
                }
            });
        }
    });
    let dir = std::env::temp_dir().join(format!("qf-slow-{kind}-{amqp}"));
    let _ = fs::remove_dir_all(&dir);
    let cluster = format!(
        r#"[cluster]
node_id = "a"
listen = "127.0.0.1:{cluster_port}"
members = [
  {{ id = "a", addr = "127.0.0.1:{cluster_port}" }},
  {{ id = "b", addr = "127.0.0.1:{silent_port}" }},
]
"#
    );
    let mut kids = Kids(Vec::new());
    kids.0.push(spawn(
        kind,
        amqp,
        mgmt,
        metrics,
        &dir,
        &cluster,
        "every_n_ms",
        10,
    ));
    wait_ready(mgmt).await;
    // Give the dial loop a moment to connect to the silent peer.
    tokio::time::sleep(Duration::from_millis(400)).await;

    // A durable declare replicates to every peer. The silent peer must be waited
    // out on each declare. Dropping it after the first timeout makes the next one instant.
    let mut waited = Vec::new();
    for i in 0..2 {
        let name = format!("slow-{kind}-{i}");
        let started = Instant::now();
        let conn = Connection::connect(
            &format!("amqp://admin:devpassword12@127.0.0.1:{amqp}/%2f"),
            ConnectionProperties::default(),
        )
        .await
        .expect("connect");
        let _ = conn
            .create_channel()
            .await
            .expect("channel")
            .queue_declare(
                &name,
                QueueDeclareOptions {
                    durable: true,
                    ..QueueDeclareOptions::default()
                },
                FieldTable::default(),
            )
            .await;
        waited.push(started.elapsed());
    }
    assert!(
        waited[0] > Duration::from_secs(2),
        "{kind} first declare did not wait for the silent peer ({} ms)",
        waited[0].as_millis()
    );
    assert!(
        waited[1] > Duration::from_secs(2),
        "{kind} dropped the silent peer; the next declare returned in {} ms",
        waited[1].as_millis()
    );
    let _ = fs::remove_dir_all(&dir);
}

/// Prefetch 128 used to run each follower drop on the AMQP read loop, so confirm
/// time grew by about one round trip per slot. It has to stay next to prefetch 1:
/// the drop still finishes before the body is written, off that loop.
#[tokio::test]
async fn quorum_prefetch_confirm_is_one_flush() {
    let _quiet = LATENCY.lock().await;
    let ports: [(u16, u16, u16, u16); 3] = [
        (free_port(), free_port(), free_port(), free_port()),
        (free_port(), free_port(), free_port(), free_port()),
        (free_port(), free_port(), free_port(), free_port()),
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
    let mut kids = Kids(Vec::new());
    let mut dirs = Vec::new();
    for (i, id) in ["a", "b", "c"].into_iter().enumerate() {
        let dir = std::env::temp_dir().join(format!("qf-pref-{}", ports[i].0));
        let _ = fs::remove_dir_all(&dir);
        let cfg = cluster
            .replace("NODE", id)
            .replace("PORT", &ports[i].3.to_string());
        kids.0.push(spawn(
            "rust",
            ports[i].0,
            ports[i].1,
            ports[i].2,
            &dir,
            &cfg,
            "every_n_ms",
            10,
        ));
        dirs.push(dir);
    }
    for (_, mgmt, _, _) in ports {
        wait_ready(mgmt).await;
    }
    tokio::time::sleep(Duration::from_millis(400)).await;

    let one = pipelined_quorum(ports[0].0, "pref-1", 1, 40).await;
    let wide = pipelined_quorum(ports[0].0, "pref-128", 128, 80).await;
    let one_p50 = percentile(&one.0, 0.50);
    let wide_p50 = percentile(&wide.0, 0.50);
    let wide_p99 = percentile(&wide.0, 0.99);
    println!(
        "quorum prefetch confirm p50 prefetch-1={} ms prefetch-128={} ms p99={} ms",
        one_p50.as_millis(),
        wide_p50.as_millis(),
        wide_p99.as_millis()
    );
    assert_eq!(wide.1.len(), 80, "prefetch 128 consumed {}", wide.1.len());
    let distinct: HashSet<_> = wide.1.iter().cloned().collect();
    assert_eq!(distinct.len(), 80, "prefetch 128 delivered a body twice");
    assert!(
        wide_p50 <= one_p50 + Duration::from_millis(40),
        "prefetch 128 confirm p50 {} ms, prefetch 1 {} ms",
        wide_p50.as_millis(),
        one_p50.as_millis()
    );
    // With Raft on, a confirm is a commit: the leader's and a follower's
    // log fsync come before it, on top of the queue log. On one laptop disk
    // that is about two flushes; the relative check above is the real one.
    assert!(
        wide_p50 < Duration::from_millis(100),
        "prefetch 128 confirm p50 {} ms",
        wide_p50.as_millis()
    );
    assert!(
        wide_p99 < Duration::from_millis(150),
        "prefetch 128 confirm p99 {} ms",
        wide_p99.as_millis()
    );

    drop(kids);
    for dir in &dirs {
        let _ = fs::remove_dir_all(dir);
    }
}

async fn pipelined_quorum(
    port: u16,
    queue: &str,
    prefetch: u16,
    n: usize,
) -> (Vec<Duration>, Vec<Vec<u8>>) {
    declare_quorum(port, queue).await;
    let conn = Connection::connect(
        &format!("amqp://admin:devpassword12@127.0.0.1:{port}/%2f"),
        ConnectionProperties::default(),
    )
    .await
    .expect("connect");
    let publisher = conn.create_channel().await.expect("publisher channel");
    publisher
        .confirm_select(ConfirmSelectOptions::default())
        .await
        .expect("confirm");
    let consumer_ch = conn.create_channel().await.expect("consumer channel");
    consumer_ch
        .basic_qos(prefetch, BasicQosOptions { global: false })
        .await
        .expect("qos");
    let mut consumer = consumer_ch
        .basic_consume(
            queue,
            &format!("c-{queue}"),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("consume");
    let seen = Arc::new(Mutex::new(Vec::new()));
    let got = Arc::clone(&seen);
    tokio::spawn(async move {
        while let Some(delivery) = consumer.next().await {
            let delivery = delivery.expect("delivery");
            got.lock().expect("seen").push(delivery.data.to_vec());
            delivery.ack(BasicAckOptions::default()).await.expect("ack");
        }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let (tx, mut rx) = tokio::sync::mpsc::channel(n);
    for i in 0..n {
        let body = format!("body-{i:04}").into_bytes();
        let confirm = publisher
            .basic_publish(
                "",
                queue,
                BasicPublishOptions::default(),
                &body,
                BasicProperties::default().with_delivery_mode(2),
            )
            .await
            .expect("publish");
        let started = Instant::now();
        let tx = tx.clone();
        tokio::spawn(async move {
            let ack = confirm.await.expect("confirm").is_ack();
            let _ = tx.send((ack, started.elapsed())).await;
        });
    }
    drop(tx);
    let mut samples = Vec::with_capacity(n);
    for _ in 0..n {
        let (ack, elapsed) = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("confirm timed out")
            .expect("confirm channel");
        assert!(ack, "quorum publish was nacked");
        samples.push(elapsed);
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    while seen.lock().expect("seen").len() < n && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let bodies = seen.lock().expect("seen").clone();
    (samples, bodies)
}

fn percentile(samples: &[Duration], pct: f64) -> Duration {
    let mut owned = samples.to_vec();
    owned.sort();
    let idx = ((owned.len() - 1) as f64 * pct).round() as usize;
    owned[idx]
}

#[tokio::test]
async fn classic_every_n_ms_lone_confirm_waits_for_fsync() {
    let _quiet = LATENCY.lock().await;
    classic("every_n_ms", false).await;
}

#[tokio::test]
async fn classic_always_waits_for_fsync() {
    let _quiet = LATENCY.lock().await;
    classic("always", true).await;
}

#[tokio::test]
async fn classic_every_n_messages_waits_for_fsync() {
    let _quiet = LATENCY.lock().await;
    classic("every_n_messages", true).await;
}
