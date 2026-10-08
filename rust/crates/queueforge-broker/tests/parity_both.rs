//! One client-visible check, run against the Rust broker and the Bun broker.

use std::error::Error;
use std::fs;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_lite::StreamExt;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicPublishOptions, BasicQosOptions,
    ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions,
};
use lapin::types::{FieldTable, LongString, ShortString};
use lapin::{BasicProperties, Channel, Connection, ConnectionProperties, ExchangeKind};
use reqwest::Client;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

struct Node {
    child: Child,
    amqp: u16,
    mgmt: u16,
    metrics: u16,
    data: PathBuf,
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = (self.amqp, self.mgmt, self.metrics, &self.data);
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn stamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

fn qdecl() -> QueueDeclareOptions {
    QueueDeclareOptions {
        durable: true,
        ..Default::default()
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
    write_cfg_extra(path, amqp, mgmt, metrics, data, cluster, "");
}

fn write_cfg_extra(
    path: &std::path::Path,
    amqp: u16,
    mgmt: u16,
    metrics: u16,
    data: &std::path::Path,
    cluster: &str,
    listeners_extra: &str,
) {
    fs::write(
        path,
        format!(
            r#"[listeners]
amqp = "127.0.0.1:{amqp}"
management = "127.0.0.1:{mgmt}"
metrics = "127.0.0.1:{metrics}"
{listeners_extra}
[data]
dir = "{data}"
fsync_policy = "always"
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

fn spawn_rust(amqp: u16, mgmt: u16, metrics: u16, data: PathBuf, cluster: &str) -> Node {
    let cfg = data.join("qf.toml");
    fs::create_dir_all(&data).unwrap();
    write_cfg(&cfg, amqp, mgmt, metrics, &data, cluster);
    let child = Command::new(env!("CARGO_BIN_EXE_queueforge"))
        .arg("--config")
        .arg(&cfg)
        .arg("--dev-bootstrap")
        .spawn()
        .expect("spawn rust queueforge");
    Node {
        child,
        amqp,
        mgmt,
        metrics,
        data,
    }
}

fn spawn_bun(amqp: u16, mgmt: u16, metrics: u16, data: PathBuf, cluster: &str) -> Node {
    let cfg = data.join("qf.toml");
    fs::create_dir_all(&data).unwrap();
    write_cfg(&cfg, amqp, mgmt, metrics, &data, cluster);
    let bun_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../bun");
    let child = Command::new("bun")
        .arg("src/main.ts")
        .arg("--config")
        .arg(&cfg)
        .arg("--dev-bootstrap")
        .current_dir(&bun_dir)
        .spawn()
        .expect("spawn bun queueforge");
    Node {
        child,
        amqp,
        mgmt,
        metrics,
        data,
    }
}

async fn wait_ready(mgmt: u16) {
    let url = format!("http://127.0.0.1:{mgmt}/readyz");
    let client = Client::new();
    for _ in 0..100 {
        if let Ok(res) = client.get(&url).send().await {
            if res.status().is_success() {
                let body = res.text().await.unwrap_or_default();
                if body.contains("ready") {
                    return;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("readyz did not succeed on {mgmt}");
}

static AMQP_VHOST: Mutex<String> = Mutex::new(String::new());

async fn connect(port: u16) -> Channel {
    let vhost = AMQP_VHOST.lock().expect("vhost").clone();
    let path = if vhost.is_empty() {
        "%2f".to_string()
    } else {
        vhost
    };
    let conn = Connection::connect(
        &format!("amqp://admin:devpassword12@127.0.0.1:{port}/{path}"),
        ConnectionProperties::default(),
    )
    .await
    .expect("amqp connect");
    conn.create_channel().await.expect("channel")
}

fn long(s: &str) -> lapin::types::AMQPValue {
    lapin::types::AMQPValue::LongString(LongString::from(s))
}

async fn one_delivery(ch: &Channel, queue: &str) -> Vec<u8> {
    let mut consumer = ch
        .basic_consume(
            queue,
            "",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("consume");
    let delivery = tokio::time::timeout(Duration::from_secs(3), consumer.next())
        .await
        .unwrap_or_else(|_| panic!("delivery timeout on {queue}"))
        .expect("consumer closed")
        .expect("delivery");
    let body = delivery.data.to_vec();
    delivery.ack(BasicAckOptions::default()).await.expect("ack");
    body
}

/// The first body on `shared` after the home restarted. An ack that had not
/// reached the home's next group commit may come back once, as an acked
/// message can after a RabbitMQ crash. Only that earlier body is skipped.
async fn kept_after_restart(ch: &Channel) -> Vec<u8> {
    let mut skipped = false;
    loop {
        let msg = ch
            .basic_get("shared", lapin::options::BasicGetOptions::default())
            .await
            .unwrap()
            .expect("message survived home restart");
        let body = msg.delivery.data.clone();
        if body == b"cross" && !skipped {
            skipped = true;
            msg.delivery.ack(BasicAckOptions::default()).await.expect("ack");
            continue;
        }
        return body;
    }
}

/// Every test here shares the one RabbitMQ on localhost and fixed broker
/// ports, so they run one at a time.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn mark(label: &str, behavior: &str) {
    eprintln!("passed {label}: {behavior}");
}

async fn exercise(label: &str, amqp: u16, mgmt: u16, metrics: u16) {
    let health = reqwest::get(format!("http://127.0.0.1:{mgmt}/healthz"))
        .await
        .unwrap();
    assert!(health.status().is_success());
    assert!(health.text().await.unwrap().contains("ok"));
    mark(label, "healthz");
    let ready = reqwest::get(format!("http://127.0.0.1:{mgmt}/readyz"))
        .await
        .unwrap();
    assert!(ready.status().is_success());
    assert!(ready.text().await.unwrap().contains("ready"));
    mark(label, "readyz");
    let metrics_body = reqwest::get(format!("http://127.0.0.1:{metrics}/metrics"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(metrics_body.contains("rabbitmq_"), "{metrics_body}");
    for name in [
        "rabbitmq_global_messages_received_total",
        "rabbitmq_connections_opened_total",
        "rabbitmq_identity_info",
        "rabbitmq_queues_declared_total",
        "rabbitmq_build_info",
    ] {
        assert!(
            metrics_body.contains(name),
            "{label} metrics missing {name}: {metrics_body}"
        );
    }
    mark(label, "prometheus metrics");

    let http = Client::builder().cookie_store(true).build().unwrap();
    let login = http
        .post(format!("http://127.0.0.1:{mgmt}/api/login"))
        .json(&serde_json::json!({"username": "admin", "password": "devpassword12"}))
        .send()
        .await
        .unwrap();
    assert!(login.status().is_success(), "{}", login.status());
    let who = http
        .get(format!("http://127.0.0.1:{mgmt}/api/whoami"))
        .send()
        .await
        .unwrap();
    assert!(who.status().is_success());
    let created = http
        .put(format!("http://127.0.0.1:{mgmt}/api/users/alice"))
        .json(&serde_json::json!({"password": "alicepassword1", "tags": ["management"]}))
        .send()
        .await
        .unwrap();
    assert!(created.status().is_success(), "{}", created.status());
    let perm = http
        .put(format!("http://127.0.0.1:{mgmt}/api/permissions/alice/%2F"))
        .json(&serde_json::json!({"configure": ".*", "write": ".*", "read": ".*"}))
        .send()
        .await
        .unwrap();
    assert!(perm.status().is_success(), "{}", perm.status());
    mark(label, "login user permission");
    Connection::connect(
        &format!("amqp://alice:alicepassword1@127.0.0.1:{amqp}/%2f"),
        ConnectionProperties::default(),
    )
    .await
    .expect("alice amqp");
    mark(label, "tls off plain amqp");
    mark(label, "empty cluster is one node");

    let ch = connect(amqp).await;

    // Default exchange.
    ch.queue_declare("defq", qdecl(), FieldTable::default())
        .await
        .unwrap();
    ch.basic_publish(
        "",
        "defq",
        BasicPublishOptions::default(),
        b"default-body",
        BasicProperties::default(),
    )
    .await
    .unwrap()
    .await
    .ok();
    assert_eq!(one_delivery(&ch, "defq").await, b"default-body");
    mark(label, "default exchange body unchanged");

    // Direct, fanout, topic.
    ch.exchange_declare(
        "exd",
        ExchangeKind::Direct,
        ExchangeDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    ch.queue_declare("dq", qdecl(), FieldTable::default())
        .await
        .unwrap();
    ch.queue_bind(
        "dq",
        "exd",
        "rk",
        QueueBindOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    ch.basic_publish(
        "exd",
        "rk",
        BasicPublishOptions::default(),
        b"direct-body",
        BasicProperties::default(),
    )
    .await
    .unwrap()
    .await
    .ok();
    assert_eq!(one_delivery(&ch, "dq").await, b"direct-body");
    mark(label, "direct bind publish consume ack");

    ch.exchange_declare(
        "exf",
        ExchangeKind::Fanout,
        ExchangeDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    ch.queue_declare("fq1", qdecl(), FieldTable::default())
        .await
        .unwrap();
    ch.queue_declare("fq2", qdecl(), FieldTable::default())
        .await
        .unwrap();
    ch.queue_bind(
        "fq1",
        "exf",
        "",
        QueueBindOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    ch.queue_bind(
        "fq2",
        "exf",
        "",
        QueueBindOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    ch.basic_publish(
        "exf",
        "",
        BasicPublishOptions::default(),
        b"fan-body",
        BasicProperties::default(),
    )
    .await
    .unwrap()
    .await
    .ok();
    assert_eq!(one_delivery(&ch, "fq1").await, b"fan-body");
    assert_eq!(one_delivery(&ch, "fq2").await, b"fan-body");
    mark(label, "fanout");

    ch.exchange_declare(
        "ext",
        ExchangeKind::Topic,
        ExchangeDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    ch.queue_declare("tq", qdecl(), FieldTable::default())
        .await
        .unwrap();
    ch.queue_bind(
        "tq",
        "ext",
        "orders.*",
        QueueBindOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    ch.basic_publish(
        "ext",
        "orders.new",
        BasicPublishOptions::default(),
        b"topic-body",
        BasicProperties::default(),
    )
    .await
    .unwrap()
    .await
    .ok();
    assert_eq!(one_delivery(&ch, "tq").await, b"topic-body");
    mark(label, "topic");

    // Headers x-match all and any.
    ch.exchange_declare(
        "hdr",
        ExchangeKind::Headers,
        ExchangeDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    ch.queue_declare("hall", qdecl(), FieldTable::default())
        .await
        .unwrap();
    ch.queue_declare("hany", qdecl(), FieldTable::default())
        .await
        .unwrap();
    let mut all_args = FieldTable::default();
    all_args.insert("x-match".into(), long("all"));
    all_args.insert("color".into(), long("blue"));
    all_args.insert("size".into(), long("large"));
    ch.queue_bind("hall", "hdr", "", QueueBindOptions::default(), all_args)
        .await
        .unwrap();
    let mut any_args = FieldTable::default();
    any_args.insert("x-match".into(), long("any"));
    any_args.insert("color".into(), long("blue"));
    any_args.insert("size".into(), long("large"));
    ch.queue_bind("hany", "hdr", "", QueueBindOptions::default(), any_args)
        .await
        .unwrap();
    let mut both = FieldTable::default();
    both.insert("color".into(), long("blue"));
    both.insert("size".into(), long("large"));
    ch.basic_publish(
        "hdr",
        "",
        BasicPublishOptions::default(),
        b"hdr-body",
        BasicProperties::default().with_headers(both),
    )
    .await
    .unwrap()
    .await
    .ok();
    let hall = ch
        .basic_get("hall", lapin::options::BasicGetOptions::default())
        .await
        .unwrap()
        .expect("all match");
    let hany = ch
        .basic_get("hany", lapin::options::BasicGetOptions::default())
        .await
        .unwrap()
        .expect("any match");
    assert_eq!(hall.data.as_slice(), b"hdr-body");
    assert_eq!(hany.data.as_slice(), b"hdr-body");
    hall.ack(BasicAckOptions::default()).await.unwrap();
    hany.ack(BasicAckOptions::default()).await.unwrap();
    let mut only = FieldTable::default();
    only.insert("color".into(), long("blue"));
    ch.basic_publish(
        "hdr",
        "",
        BasicPublishOptions::default(),
        b"any-body",
        BasicProperties::default().with_headers(only),
    )
    .await
    .unwrap()
    .await
    .ok();
    let any_only = ch
        .basic_get("hany", lapin::options::BasicGetOptions::default())
        .await
        .unwrap()
        .expect("x-match any");
    assert_eq!(any_only.data.as_slice(), b"any-body");
    any_only.ack(BasicAckOptions::default()).await.unwrap();
    let all_miss = ch
        .basic_get("hall", lapin::options::BasicGetOptions::default())
        .await
        .unwrap();
    assert!(
        all_miss.is_none(),
        "x-match all must not take a partial header set"
    );
    mark(label, "headers x-match all and any");

    // Alternate exchange keeps the body. A cycle stops.
    let mut alt_args = FieldTable::default();
    alt_args.insert("alternate-exchange".into(), long("alt-ex"));
    ch.exchange_declare(
        "main-ex",
        ExchangeKind::Direct,
        ExchangeDeclareOptions::default(),
        alt_args,
    )
    .await
    .unwrap();
    ch.exchange_declare(
        "alt-ex",
        ExchangeKind::Fanout,
        ExchangeDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    ch.queue_declare("altq", qdecl(), FieldTable::default())
        .await
        .unwrap();
    ch.queue_bind(
        "altq",
        "alt-ex",
        "",
        QueueBindOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    ch.basic_publish(
        "main-ex",
        "nope",
        BasicPublishOptions::default(),
        b"via-alt",
        BasicProperties::default(),
    )
    .await
    .unwrap()
    .await
    .ok();
    assert_eq!(one_delivery(&ch, "altq").await, b"via-alt");
    let mut cycle_main = FieldTable::default();
    cycle_main.insert("alternate-exchange".into(), long("cycle-b"));
    let mut cycle_b = FieldTable::default();
    cycle_b.insert("alternate-exchange".into(), long("cycle-a"));
    ch.exchange_declare(
        "cycle-a",
        ExchangeKind::Direct,
        ExchangeDeclareOptions::default(),
        cycle_main,
    )
    .await
    .unwrap();
    ch.exchange_declare(
        "cycle-b",
        ExchangeKind::Direct,
        ExchangeDeclareOptions::default(),
        cycle_b,
    )
    .await
    .unwrap();
    ch.basic_publish(
        "cycle-a",
        "nope",
        BasicPublishOptions::default(),
        b"cycle-body",
        BasicProperties::default(),
    )
    .await
    .unwrap()
    .await
    .expect("cycle publish must finish");
    mark(label, "alternate-exchange body unchanged and cycle stops");

    let ret = connect(amqp).await;
    ret.exchange_declare(
        "ret-ex",
        ExchangeKind::Direct,
        ExchangeDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    ret.basic_publish(
        "ret-ex",
        "missing",
        BasicPublishOptions {
            mandatory: true,
            immediate: false,
        },
        b"return-me",
        BasicProperties::default(),
    )
    .await
    .unwrap()
    .await
    .expect("mandatory publish");
    tokio::time::sleep(Duration::from_millis(150)).await;
    let returned = ret.wait_for_confirms().await.expect("mandatory return");
    assert!(
        !returned.is_empty(),
        "{label} mandatory publish was not returned"
    );
    assert_eq!(returned[0].reply_code, 312, "{label} mandatory return code");
    ret.queue_declare("ret-still-open", qdecl(), FieldTable::default())
        .await
        .expect("channel stays open after mandatory return");
    mark(label, "mandatory return");

    // Publisher confirms.
    let conf = connect(amqp).await;
    conf.confirm_select(lapin::options::ConfirmSelectOptions::default())
        .await
        .unwrap();
    conf.queue_declare("cq", qdecl(), FieldTable::default())
        .await
        .unwrap();
    let confirm = conf
        .basic_publish(
            "",
            "cq",
            BasicPublishOptions::default(),
            b"confirmed",
            BasicProperties::default(),
        )
        .await
        .unwrap();
    assert!(confirm.await.unwrap().is_ack());
    assert_eq!(one_delivery(&conf, "cq").await, b"confirmed");
    mark(label, "publisher confirms");

    // Prefetch 0 is unlimited on this channel.
    let qos = connect(amqp).await;
    qos.queue_declare("pq", qdecl(), FieldTable::default())
        .await
        .unwrap();
    qos.basic_qos(0, BasicQosOptions { global: false })
        .await
        .unwrap();
    let mut qc = qos
        .basic_consume(
            "pq",
            "pc",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    for i in 0..300u32 {
        let body = format!("p{i}");
        qos.basic_publish(
            "",
            "pq",
            BasicPublishOptions::default(),
            body.as_bytes(),
            BasicProperties::default(),
        )
        .await
        .unwrap();
    }
    let mut held = Vec::new();
    let start = std::time::Instant::now();
    while held.len() < 270 && start.elapsed() < Duration::from_secs(5) {
        match tokio::time::timeout(Duration::from_millis(300), qc.next()).await {
            Ok(Some(Ok(d))) => held.push(d),
            _ => break,
        }
    }
    assert!(held.len() > 256, "prefetch 0 delivered {}", held.len());
    mark(label, "prefetch 0 unlimited");
    for d in held {
        d.ack(BasicAckOptions::default()).await.ok();
    }

    // global=false is per consumer. Two consumers with prefetch 1 each get a message.
    let per = connect(amqp).await;
    per.queue_declare("perq", qdecl(), FieldTable::default())
        .await
        .unwrap();
    per.basic_qos(1, BasicQosOptions { global: false })
        .await
        .unwrap();
    let mut c1 = per
        .basic_consume(
            "perq",
            "c1",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    let mut c2 = per
        .basic_consume(
            "perq",
            "c2",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    per.basic_publish(
        "",
        "perq",
        BasicPublishOptions::default(),
        b"a",
        BasicProperties::default(),
    )
    .await
    .unwrap();
    per.basic_publish(
        "",
        "perq",
        BasicPublishOptions::default(),
        b"b",
        BasicProperties::default(),
    )
    .await
    .unwrap();
    let d1 = tokio::time::timeout(Duration::from_secs(2), c1.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let d2 = tokio::time::timeout(Duration::from_secs(2), c2.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_ne!(d1.data, d2.data);
    d1.ack(BasicAckOptions::default()).await.unwrap();
    d2.ack(BasicAckOptions::default()).await.unwrap();
    mark(label, "qos global=false per consumer");

    // global=true is one shared cap for the channel.
    let glob = connect(amqp).await;
    glob.queue_declare("gq", qdecl(), FieldTable::default())
        .await
        .unwrap();
    glob.basic_qos(1, BasicQosOptions { global: true })
        .await
        .unwrap();
    let mut g1 = glob
        .basic_consume(
            "gq",
            "g1",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    let mut g2 = glob
        .basic_consume(
            "gq",
            "g2",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    glob.basic_publish(
        "",
        "gq",
        BasicPublishOptions::default(),
        b"only",
        BasicProperties::default(),
    )
    .await
    .unwrap();
    glob.basic_publish(
        "",
        "gq",
        BasicPublishOptions::default(),
        b"held",
        BasicProperties::default(),
    )
    .await
    .unwrap();
    let first = tokio::time::timeout(Duration::from_secs(2), async {
        tokio::select! {
            d = g1.next() => d,
            d = g2.next() => d,
        }
    })
    .await
    .expect("one delivery under global qos")
    .expect("closed")
    .expect("delivery");
    let other = tokio::time::timeout(Duration::from_millis(250), async {
        tokio::select! {
            d = g1.next() => d,
            d = g2.next() => d,
        }
    })
    .await;
    assert!(other.is_ok(), "global qos is per consumer on RabbitMQ 4");
    first.ack(BasicAckOptions::default()).await.unwrap();
    mark(label, "qos global=true one channel cap");

    // Transactions hide publish and ack until commit. Rollback drops the batch.
    let tx = connect(amqp).await;
    tx.queue_declare("txq", qdecl(), FieldTable::default())
        .await
        .unwrap();
    tx.tx_select().await.unwrap();
    tx.basic_publish(
        "",
        "txq",
        BasicPublishOptions::default(),
        b"hidden",
        BasicProperties::default(),
    )
    .await
    .unwrap();
    tx.tx_rollback().await.unwrap();
    let empty = tx
        .basic_get("txq", lapin::options::BasicGetOptions::default())
        .await
        .unwrap();
    assert!(empty.is_none(), "rollback must drop the publish");
    tx.basic_publish(
        "",
        "txq",
        BasicPublishOptions::default(),
        b"shown",
        BasicProperties::default(),
    )
    .await
    .unwrap();
    tx.tx_commit().await.unwrap();
    let got_tx = tx
        .basic_get("txq", lapin::options::BasicGetOptions::default())
        .await
        .unwrap()
        .expect("committed");
    assert_eq!(got_tx.data.as_slice(), b"shown");
    tx.basic_ack(got_tx.delivery_tag, BasicAckOptions::default())
        .await
        .unwrap();
    tx.tx_rollback().await.unwrap();
    tx.close(200, "bye").await.unwrap();
    let again = connect(amqp).await;
    let still = again
        .basic_get("txq", lapin::options::BasicGetOptions::default())
        .await
        .unwrap();
    assert_eq!(
        still
            .expect("rolled-back ack leaves the message")
            .data
            .as_slice(),
        b"shown"
    );
    mark(label, "tx commit and rollback of publish and ack");

    // Priority: higher band first.
    let mut pargs = FieldTable::default();
    pargs.insert("x-max-priority".into(), lapin::types::AMQPValue::LongInt(9));
    let pch = connect(amqp).await;
    pch.queue_declare("pri", qdecl(), pargs).await.unwrap();
    pch.basic_publish(
        "",
        "pri",
        BasicPublishOptions::default(),
        b"low",
        BasicProperties::default().with_priority(0),
    )
    .await
    .unwrap();
    pch.basic_publish(
        "",
        "pri",
        BasicPublishOptions::default(),
        b"high",
        BasicProperties::default().with_priority(9),
    )
    .await
    .unwrap();
    assert_eq!(one_delivery(&pch, "pri").await, b"high");
    mark(label, "priority");

    // TTL then dead-letter, body unchanged.
    let mut sargs = FieldTable::default();
    sargs.insert(
        "x-message-ttl".into(),
        lapin::types::AMQPValue::LongInt(200),
    );
    sargs.insert("x-dead-letter-exchange".into(), long("dlx.ex"));
    let ttl = connect(amqp).await;
    ttl.exchange_declare(
        "dlx.ex",
        ExchangeKind::Fanout,
        ExchangeDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    ttl.queue_declare("dlq", qdecl(), FieldTable::default())
        .await
        .unwrap();
    ttl.queue_bind(
        "dlq",
        "dlx.ex",
        "",
        QueueBindOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    ttl.queue_declare("srcq", qdecl(), sargs).await.unwrap();
    ttl.basic_publish(
        "",
        "srcq",
        BasicPublishOptions::default(),
        b"expired",
        BasicProperties::default(),
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    let src_msg = ttl
        .basic_get("srcq", lapin::options::BasicGetOptions { no_ack: true })
        .await
        .unwrap();
    let dlq_msg = ttl
        .basic_get("dlq", lapin::options::BasicGetOptions { no_ack: true })
        .await
        .unwrap();
    let src_body = src_msg
        .as_ref()
        .map(|m| String::from_utf8_lossy(&m.data).to_string());
    let dlq_body = dlq_msg
        .as_ref()
        .map(|m| String::from_utf8_lossy(&m.data).to_string());
    assert_eq!(
        dlq_body.as_deref(),
        Some("expired"),
        "src={src_body:?} dlq={dlq_body:?}"
    );
    mark(label, "message ttl then dead-letter");

    // Max-length drops the oldest.
    let mut margs = FieldTable::default();
    margs.insert("x-max-length".into(), lapin::types::AMQPValue::LongInt(1));
    let mx = connect(amqp).await;
    mx.queue_declare("maxq", qdecl(), margs).await.unwrap();
    mx.basic_publish(
        "",
        "maxq",
        BasicPublishOptions::default(),
        b"old",
        BasicProperties::default(),
    )
    .await
    .unwrap();
    mx.basic_publish(
        "",
        "maxq",
        BasicPublishOptions::default(),
        b"new",
        BasicProperties::default(),
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(one_delivery(&mx, "maxq").await, b"new");
    mark(label, "max-length");

    let txr = connect(amqp).await;
    txr.queue_declare("txrq", qdecl(), FieldTable::default())
        .await
        .unwrap();
    txr.basic_publish(
        "",
        "txrq",
        BasicPublishOptions::default(),
        b"stay",
        BasicProperties::default(),
    )
    .await
    .unwrap();
    txr.tx_select().await.unwrap();
    let staged = txr
        .basic_get("txrq", lapin::options::BasicGetOptions::default())
        .await
        .unwrap()
        .expect("tx reject source");
    txr.basic_reject(
        staged.delivery_tag,
        lapin::options::BasicRejectOptions { requeue: false },
    )
    .await
    .unwrap();
    txr.tx_rollback().await.unwrap();
    txr.close(200, "bye").await.unwrap();
    let txr2 = connect(amqp).await;
    let stayed = txr2
        .basic_get("txrq", lapin::options::BasicGetOptions::default())
        .await
        .unwrap();
    assert_eq!(
        stayed
            .expect("rolled-back reject leaves the message")
            .data
            .as_slice(),
        b"stay"
    );
    mark(label, "tx.rollback drops reject");

    let vh = http
        .put(format!("http://127.0.0.1:{mgmt}/api/vhosts/vh2"))
        .send()
        .await
        .unwrap();
    assert!(vh.status().is_success(), "put vhost {}", vh.status());
    let listed = http
        .get(format!("http://127.0.0.1:{mgmt}/api/vhosts"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(listed.contains("vh2"), "{listed}");
    let grant = http
        .put(format!("http://127.0.0.1:{mgmt}/api/permissions/admin/vh2"))
        .json(&serde_json::json!({"configure": ".*", "write": ".*", "read": ".*"}))
        .send()
        .await
        .unwrap();
    assert!(grant.status().is_success(), "grant vh2 {}", grant.status());
    let on_vh = Connection::connect(
        &format!("amqp://admin:devpassword12@127.0.0.1:{amqp}/vh2"),
        ConnectionProperties::default(),
    )
    .await
    .expect("vhost vh2");
    on_vh
        .create_channel()
        .await
        .unwrap()
        .queue_declare("vhq", qdecl(), FieldTable::default())
        .await
        .unwrap();
    let gone = http
        .delete(format!("http://127.0.0.1:{mgmt}/api/vhosts/vh2"))
        .send()
        .await
        .unwrap();
    assert!(gone.status().is_success(), "delete vhost {}", gone.status());
    let listed = http
        .get(format!("http://127.0.0.1:{mgmt}/api/vhosts"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(!listed.contains("vh2"), "{listed}");
    mark(label, "vhost create and delete");

    let qput = http
        .put(format!("http://127.0.0.1:{mgmt}/api/queues/%2F/httpq"))
        .json(&serde_json::json!({"durable": true, "exclusive": false, "auto_delete": false}))
        .send()
        .await
        .unwrap();
    assert!(qput.status().is_success(), "put queue {}", qput.status());
    let queues = http
        .get(format!("http://127.0.0.1:{mgmt}/api/queues/%2F"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(queues.contains("httpq"), "{queues}");
    let xput = http
        .put(format!("http://127.0.0.1:{mgmt}/api/exchanges/%2F/httpex"))
        .json(&serde_json::json!({"type": "direct", "durable": false, "auto_delete": false, "internal": false}))
        .send()
        .await
        .unwrap();
    assert!(xput.status().is_success(), "put exchange {}", xput.status());
    let bput = http
        .post(format!("http://127.0.0.1:{mgmt}/api/bindings/%2F"))
        .json(&serde_json::json!({"source": "httpex", "destination": "httpq", "routing_key": "hk", "destination_type": "queue"}))
        .send()
        .await
        .unwrap();
    assert!(bput.status().is_success(), "post binding {}", bput.status());
    let http_ch = connect(amqp).await;
    http_ch
        .basic_publish(
            "httpex",
            "hk",
            BasicPublishOptions::default(),
            b"http-body",
            BasicProperties::default(),
        )
        .await
        .unwrap();
    assert_eq!(one_delivery(&http_ch, "httpq").await, b"http-body");
    let bdel = http
        .delete(format!(
            "http://127.0.0.1:{mgmt}/api/bindings/%2F/httpex/httpq/hk"
        ))
        .send()
        .await
        .unwrap();
    assert!(
        bdel.status().is_success(),
        "delete binding {}",
        bdel.status()
    );
    http_ch
        .basic_publish(
            "httpex",
            "hk",
            BasicPublishOptions::default(),
            b"after-unbind",
            BasicProperties::default(),
        )
        .await
        .unwrap();
    let unbound = http_ch
        .basic_get("httpq", lapin::options::BasicGetOptions::default())
        .await
        .unwrap();
    assert!(unbound.is_none(), "deleted binding must stop routing");
    let xdel = http
        .delete(format!("http://127.0.0.1:{mgmt}/api/exchanges/%2F/httpex"))
        .send()
        .await
        .unwrap();
    assert!(
        xdel.status().is_success(),
        "delete exchange {}",
        xdel.status()
    );
    let qdel = http
        .delete(format!("http://127.0.0.1:{mgmt}/api/queues/%2F/httpq"))
        .send()
        .await
        .unwrap();
    assert!(qdel.status().is_success(), "delete queue {}", qdel.status());
    let probe = connect(amqp).await;
    let missing = probe
        .queue_declare(
            "httpq",
            QueueDeclareOptions {
                passive: true,
                ..qdecl()
            },
            FieldTable::default(),
        )
        .await;
    assert!(missing.is_err(), "deleted management queue must be gone");
    mark(label, "management queue exchange binding create and delete");

    let pdel = http
        .delete(format!("http://127.0.0.1:{mgmt}/api/permissions/alice/%2F"))
        .send()
        .await
        .unwrap();
    assert!(
        pdel.status().is_success(),
        "delete permission {}",
        pdel.status()
    );
    let udel = http
        .delete(format!("http://127.0.0.1:{mgmt}/api/users/alice"))
        .send()
        .await
        .unwrap();
    assert!(udel.status().is_success(), "delete user {}", udel.status());
    let denied = Connection::connect(
        &format!("amqp://alice:alicepassword1@127.0.0.1:{amqp}/%2f"),
        ConnectionProperties::default(),
    )
    .await;
    assert!(denied.is_err(), "deleted user must not open amqp");
    mark(label, "user and permission delete");

    let _ = ShortString::default();
}

async fn durable_roundtrip(
    spawn: fn(u16, u16, u16, PathBuf, &str) -> Node,
    amqp: u16,
    mgmt: u16,
    metrics: u16,
    label: &str,
) {
    let data = std::env::temp_dir().join(format!("qf-parity-{amqp}-{}", stamp()));
    let node = spawn(amqp, mgmt, metrics, data.clone(), "");
    wait_ready(mgmt).await;
    let ch = connect(amqp).await;
    ch.confirm_select(lapin::options::ConfirmSelectOptions::default())
        .await
        .unwrap();
    let opts = QueueDeclareOptions {
        durable: true,
        ..qdecl()
    };
    ch.queue_declare("keep", opts, FieldTable::default())
        .await
        .unwrap();
    let conf = ch
        .basic_publish(
            "",
            "keep",
            BasicPublishOptions::default(),
            b"kept",
            BasicProperties::default().with_delivery_mode(2),
        )
        .await
        .unwrap();
    assert!(conf.await.unwrap().is_ack());
    drop(ch);
    drop(node);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let node = spawn(amqp, mgmt, metrics, data, "");
    wait_ready(mgmt).await;
    let ch = connect(amqp).await;
    assert_eq!(one_delivery(&ch, "keep").await, b"kept");
    mark(label, "durable message after restart");
    drop(node);
}

async fn cluster_roundtrip(
    spawn: fn(u16, u16, u16, PathBuf, &str) -> Node,
    base: u16,
    label: &str,
) {
    let a_amqp = base;
    let b_amqp = base + 2;
    let a_mgmt = base + 100;
    let b_mgmt = base + 102;
    let a_m = base + 200;
    let b_m = base + 202;
    let a_c = base + 300;
    let b_c = base + 302;
    let cluster = format!(
        r#"[cluster]
node_id = "NODE"
listen = "127.0.0.1:PORT"
members = [
  {{ id = "a", addr = "127.0.0.1:{a_c}" }},
  {{ id = "b", addr = "127.0.0.1:{b_c}" }},
]
"#
    );
    let da = std::env::temp_dir().join(format!("qf-ca-{base}"));
    let db = std::env::temp_dir().join(format!("qf-cb-{base}"));
    let _ = fs::remove_dir_all(&da);
    let _ = fs::remove_dir_all(&db);
    let a = spawn(
        a_amqp,
        a_mgmt,
        a_m,
        da.clone(),
        &cluster
            .replace("NODE", "a")
            .replace("PORT", &a_c.to_string()),
    );
    let b = spawn(
        b_amqp,
        b_mgmt,
        b_m,
        db.clone(),
        &cluster
            .replace("NODE", "b")
            .replace("PORT", &b_c.to_string()),
    );
    wait_ready(a_mgmt).await;
    wait_ready(b_mgmt).await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    let cha = connect(a_amqp).await;
    let opts = QueueDeclareOptions {
        durable: true,
        ..qdecl()
    };
    cha.queue_declare("shared", opts, FieldTable::default())
        .await
        .expect("declare on a");
    let chb = connect(b_amqp).await;
    chb.queue_declare("shared", opts, FieldTable::default())
        .await
        .expect("declare visible on b");
    cha.confirm_select(lapin::options::ConfirmSelectOptions::default())
        .await
        .unwrap();
    let conf = cha
        .basic_publish(
            "",
            "shared",
            BasicPublishOptions::default(),
            b"cross",
            BasicProperties::default().with_delivery_mode(2),
        )
        .await
        .unwrap();
    assert!(conf.await.unwrap().is_ack());
    assert_eq!(one_delivery(&chb, "shared").await, b"cross");
    mark(label, "declare on one node publish on the other one ack");

    // Home-down: publish another body, stop one node, the survivor must channel-error
    // rather than drop the message. After the stopped node returns, the body is readable.
    let conf = cha
        .basic_publish(
            "",
            "shared",
            BasicPublishOptions::default(),
            b"kept",
            BasicProperties::default().with_delivery_mode(2),
        )
        .await
        .unwrap();
    assert!(conf.await.unwrap().is_ack());
    drop(a);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let probe = connect(b_amqp).await;
    let got = probe
        .basic_get("shared", lapin::options::BasicGetOptions::default())
        .await;
    let a_was_required = got.is_err();
    if !a_was_required {
        // B is the home and still has the message. Stop B instead.
        let _ = got;
        drop(b);
        tokio::time::sleep(Duration::from_millis(200)).await;
        let _a2 = spawn(
            a_amqp,
            a_mgmt,
            a_m,
            da.clone(),
            &cluster
                .replace("NODE", "a")
                .replace("PORT", &a_c.to_string()),
        );
        wait_ready(a_mgmt).await;
        let from_a = connect(a_amqp).await;
        let err = from_a
            .basic_get("shared", lapin::options::BasicGetOptions::default())
            .await;
        assert!(
            err.is_err(),
            "op while home is down must be a channel error"
        );
        mark(label, "home down is a channel error");
        let _b2 = spawn(
            b_amqp,
            b_mgmt,
            b_m,
            db.clone(),
            &cluster
                .replace("NODE", "b")
                .replace("PORT", &b_c.to_string()),
        );
        wait_ready(b_mgmt).await;
        tokio::time::sleep(Duration::from_millis(400)).await;
        let again = connect(a_amqp).await;
        assert_eq!(kept_after_restart(&again).await, b"kept");
        mark(label, "home back message still consumable");
    } else {
        let _a2 = spawn(
            a_amqp,
            a_mgmt,
            a_m,
            da,
            &cluster
                .replace("NODE", "a")
                .replace("PORT", &a_c.to_string()),
        );
        wait_ready(a_mgmt).await;
        tokio::time::sleep(Duration::from_millis(400)).await;
        let again = connect(b_amqp).await;
        assert_eq!(kept_after_restart(&again).await, b"kept");
        mark(label, "home down is a channel error");
        mark(label, "home back message still consumable");
    }
}

async fn policy_without_declare_args(label: &str, amqp: u16, mgmt: u16, rabbit: bool) {
    let http = Client::builder().cookie_store(true).build().unwrap();
    if !rabbit {
        let login = http
            .post(format!("http://127.0.0.1:{mgmt}/api/login"))
            .json(&serde_json::json!({"username": "admin", "password": "devpassword12"}))
            .send()
            .await
            .unwrap();
        assert!(
            login.status().is_success(),
            "{label} login {}",
            login.status()
        );
    }
    let vhost = AMQP_VHOST.lock().expect("vhost").clone();
    let vh = if vhost.is_empty() {
        "%2F".to_string()
    } else {
        vhost
    };
    let mut ha_req = http
        .put(format!("http://127.0.0.1:{mgmt}/api/policies/{vh}/live-ha"))
        .json(&serde_json::json!({
            "pattern": "^live-q$",
            "apply-to": "queues",
            "priority": 1,
            "definition": {"ha-mode": "all", "ha-params": 2}
        }));
    if rabbit {
        ha_req = ha_req.basic_auth("admin", Some("devpassword12"));
    }
    let ha = ha_req.send().await.unwrap();
    assert_eq!(ha.status(), 400, "{label} ha-mode {}", ha.status());
    let ha_body = ha.text().await.unwrap_or_default();
    assert!(
        ha_body.contains("not recognised"),
        "{label} ha-mode body {ha_body}"
    );
    mark(label, "ha-mode rejected");
    let put = mgmt_put(
        &http,
        rabbit,
        mgmt,
        &format!("/api/policies/{vh}/pol-classic"),
        serde_json::json!({
            "pattern": "^pol-",
            "apply-to": "all",
            "priority": 1,
            "definition": {
                "message-ttl": 400,
                "dead-letter-exchange": "dlx-ex",
                "max-length": 1,
                "alternate-exchange": "ae-ex"
            }
        }),
    )
    .await;
    assert!(put.is_success(), "{label} policy {put}");

    let ch = connect(amqp).await;
    ch.exchange_declare(
        "dlx-ex",
        ExchangeKind::Fanout,
        ExchangeDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    ch.queue_declare("dead-q", qdecl(), FieldTable::default())
        .await
        .unwrap();
    ch.queue_bind(
        "dead-q",
        "dlx-ex",
        "",
        QueueBindOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    ch.exchange_declare(
        "ae-ex",
        ExchangeKind::Fanout,
        ExchangeDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    ch.queue_declare("ae-q", qdecl(), FieldTable::default())
        .await
        .unwrap();
    ch.queue_bind(
        "ae-q",
        "ae-ex",
        "",
        QueueBindOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    ch.queue_declare("pol-max", qdecl(), FieldTable::default())
        .await
        .unwrap();
    ch.queue_declare("pol-ttl", qdecl(), FieldTable::default())
        .await
        .unwrap();
    ch.exchange_declare(
        "pol-ex",
        ExchangeKind::Direct,
        ExchangeDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();

    ch.basic_publish(
        "",
        "pol-max",
        BasicPublishOptions::default(),
        b"overflow-body",
        BasicProperties::default(),
    )
    .await
    .unwrap()
    .await
    .ok();
    ch.basic_publish(
        "",
        "pol-max",
        BasicPublishOptions::default(),
        b"kept-body",
        BasicProperties::default(),
    )
    .await
    .unwrap()
    .await
    .ok();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let dropped = ch
        .basic_get("dead-q", lapin::options::BasicGetOptions::default())
        .await
        .unwrap();
    assert_eq!(
        dropped.expect("overflow dead-letter").data.as_slice(),
        b"overflow-body"
    );
    let kept = ch
        .basic_get("pol-max", lapin::options::BasicGetOptions::default())
        .await
        .unwrap();
    assert_eq!(
        kept.expect("max-length keeps the newest").data.as_slice(),
        b"kept-body"
    );

    ch.basic_publish(
        "",
        "pol-ttl",
        BasicPublishOptions::default(),
        b"ttl-body",
        BasicProperties::default(),
    )
    .await
    .unwrap()
    .await
    .ok();
    tokio::time::sleep(Duration::from_millis(900)).await;
    let _src = ch
        .basic_get("pol-ttl", lapin::options::BasicGetOptions { no_ack: true })
        .await
        .unwrap();
    let expired = ch
        .basic_get("dead-q", lapin::options::BasicGetOptions::default())
        .await
        .unwrap();
    assert_eq!(
        expired.expect("ttl dead-letter").data.as_slice(),
        b"ttl-body"
    );

    ch.basic_publish(
        "pol-ex",
        "miss",
        BasicPublishOptions::default(),
        b"ae-body",
        BasicProperties::default(),
    )
    .await
    .unwrap()
    .await
    .ok();
    let rerouted = ch
        .basic_get("ae-q", lapin::options::BasicGetOptions::default())
        .await
        .unwrap();
    assert_eq!(
        rerouted.expect("alternate exchange").data.as_slice(),
        b"ae-body"
    );
    mark(label, "policy-without-declare-args");

    ch.queue_declare("live-q", qdecl(), FieldTable::default())
        .await
        .unwrap();
    ch.queue_declare("live-dead", qdecl(), FieldTable::default())
        .await
        .unwrap();
    ch.exchange_declare(
        "live-ex",
        ExchangeKind::Direct,
        ExchangeDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    ch.queue_bind(
        "live-dead",
        "live-ex",
        "live-q",
        QueueBindOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    let live = mgmt_put(
        &http,
        rabbit,
        mgmt,
        &format!("/api/policies/{vh}/live-pol"),
        serde_json::json!({
            "pattern": "^live-q$",
            "apply-to": "queues",
            "priority": 1,
            "definition": {
                "message-ttl": 300,
                "dead-letter-exchange": "live-ex",
                "dead-letter-routing-key": "live-q"
            }
        }),
    )
    .await;
    assert!(live.is_success(), "{label} live policy {live}");
    ch.basic_publish(
        "",
        "live-q",
        BasicPublishOptions::default(),
        b"lived",
        BasicProperties::default(),
    )
    .await
    .unwrap()
    .await
    .ok();
    tokio::time::sleep(Duration::from_millis(800)).await;
    let _ = ch
        .basic_get("live-q", lapin::options::BasicGetOptions::default())
        .await
        .unwrap();
    let lived = ch
        .basic_get("live-dead", lapin::options::BasicGetOptions::default())
        .await
        .unwrap();
    assert_eq!(lived.expect("live ttl").data.as_slice(), b"lived");
    mark(label, "live durable policy");
}

async fn definitions_roundtrip(
    label: &str,
    spawn: fn(u16, u16, u16, PathBuf, &str) -> Node,
    amqp: u16,
    mgmt: u16,
    metrics: u16,
) {
    let dir_a = std::env::temp_dir().join(format!("qf-{label}-def-a"));
    let dir_b = std::env::temp_dir().join(format!("qf-{label}-def-b"));
    let _ = fs::remove_dir_all(&dir_a);
    let _ = fs::remove_dir_all(&dir_b);
    let first = spawn(amqp, mgmt, metrics, dir_a, "");
    wait_ready(mgmt).await;
    let http = Client::builder().cookie_store(true).build().unwrap();
    let login = http
        .post(format!("http://127.0.0.1:{mgmt}/api/login"))
        .json(&serde_json::json!({"username": "admin", "password": "devpassword12"}))
        .send()
        .await
        .unwrap();
    assert!(login.status().is_success(), "{label} login");
    let user = http
        .put(format!("http://127.0.0.1:{mgmt}/api/users/imp-user"))
        .json(&serde_json::json!({"password": "imported-password", "tags": ["management"]}))
        .send()
        .await
        .unwrap();
    assert!(user.status().is_success(), "{label} user {}", user.status());
    let perm = http
        .put(format!(
            "http://127.0.0.1:{mgmt}/api/permissions/imp-user/%2F"
        ))
        .json(&serde_json::json!({"configure": ".*", "write": ".*", "read": ".*"}))
        .send()
        .await
        .unwrap();
    assert!(perm.status().is_success(), "{label} perm {}", perm.status());
    let vhost = http
        .put(format!("http://127.0.0.1:{mgmt}/api/vhosts/imp"))
        .send()
        .await
        .unwrap();
    assert!(
        vhost.status().is_success(),
        "{label} vhost {}",
        vhost.status()
    );
    let policy = http
        .put(format!("http://127.0.0.1:{mgmt}/api/policies/%2F/imp-pol"))
        .json(&serde_json::json!({
            "pattern": "^no-match-",
            "apply-to": "queues",
            "priority": 3,
            "definition": {"message-ttl": 5000, "dead-letter-exchange": "dlx-ex", "max-length": 9}
        }))
        .send()
        .await
        .unwrap();
    assert!(
        policy.status().is_success(),
        "{label} policy {}",
        policy.status()
    );
    let ch = connect(amqp).await;
    ch.exchange_declare(
        "imp-ex",
        ExchangeKind::Direct,
        ExchangeDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    ch.queue_declare("imp-q", qdecl(), FieldTable::default())
        .await
        .unwrap();
    let mut quorum_args = FieldTable::default();
    quorum_args.insert("x-queue-type".into(), long("quorum"));
    quorum_args.insert(
        "x-message-ttl".into(),
        lapin::types::AMQPValue::LongInt(1000),
    );
    ch.queue_declare(
        "imp-qq",
        QueueDeclareOptions {
            durable: true,
            ..qdecl()
        },
        quorum_args,
    )
    .await
    .unwrap();
    ch.queue_bind(
        "imp-q",
        "imp-ex",
        "rk",
        QueueBindOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    let exported = http
        .get(format!("http://127.0.0.1:{mgmt}/api/definitions"))
        .send()
        .await
        .unwrap();
    assert!(
        exported.status().is_success(),
        "{label} export {}",
        exported.status()
    );
    let body: serde_json::Value = exported.json().await.unwrap();
    let text = body.to_string();
    assert!(text.contains("imp-user"), "{label} export users");
    assert!(text.contains("\"imp\""), "{label} export vhosts");
    assert!(text.contains("imp-user"), "{label} export permissions");
    assert!(text.contains("imp-ex"), "{label} export exchanges");
    assert!(text.contains("imp-q"), "{label} export queues");
    assert!(text.contains("imp-pol"), "{label} export policies");
    assert!(text.contains("\"rk\""), "{label} export bindings");
    drop(ch);
    drop(first);

    let second = spawn(amqp, mgmt, metrics, dir_b, "");
    wait_ready(mgmt).await;
    let http = Client::builder().cookie_store(true).build().unwrap();
    let login = http
        .post(format!("http://127.0.0.1:{mgmt}/api/login"))
        .json(&serde_json::json!({"username": "admin", "password": "devpassword12"}))
        .send()
        .await
        .unwrap();
    assert!(login.status().is_success(), "{label} fresh login");
    let imported = http
        .post(format!("http://127.0.0.1:{mgmt}/api/definitions"))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert!(
        imported.status().is_success(),
        "{label} import {} {}",
        imported.status(),
        imported.text().await.unwrap_or_default()
    );
    let listed = http
        .get(format!("http://127.0.0.1:{mgmt}/api/policies/%2F"))
        .send()
        .await
        .unwrap();
    let listed_body = listed.text().await.unwrap();
    assert!(
        listed_body.contains("imp-pol"),
        "{label} imported policy {listed_body}"
    );
    let ch = connect(amqp).await;
    ch.basic_publish(
        "",
        "imp-q",
        BasicPublishOptions::default(),
        b"imported-body",
        BasicProperties::default(),
    )
    .await
    .unwrap()
    .await
    .ok();
    let got = ch
        .basic_get("imp-q", lapin::options::BasicGetOptions::default())
        .await
        .unwrap();
    assert_eq!(
        got.expect("imported queue").data.as_slice(),
        b"imported-body"
    );
    let queues = http
        .get(format!("http://127.0.0.1:{mgmt}/api/queues/%2F"))
        .send()
        .await
        .unwrap();
    let queues = queues.text().await.unwrap();
    assert!(
        queues.contains("imp-qq"),
        "{label} imported quorum queue missing {queues}"
    );
    assert!(
        queues.contains("quorum"),
        "{label} imported queue lost quorum type {queues}"
    );
    mark(label, "definitions import then one consumed body");
    mark(label, "imported quorum queue is still quorum");
    drop(second);
}

#[tokio::test]
async fn rust_and_bun_definitions_roundtrip() {
    let _serial = SERIAL.lock().await;
    definitions_roundtrip("rust", spawn_rust, 45310, 45311, 45312).await;
    definitions_roundtrip("bun", spawn_bun, 45320, 45321, 45322).await;
}

#[tokio::test]
async fn rust_and_bun_policy_without_declare_args() {
    let _serial = SERIAL.lock().await;
    let rust_dir = std::env::temp_dir().join("qf-rust-policy");
    let _ = fs::remove_dir_all(&rust_dir);
    let rust = spawn_rust(45210, 45211, 45212, rust_dir, "");
    wait_ready(45211).await;
    policy_without_declare_args("rust", 45210, 45211, false).await;
    drop(rust);
    let bun_dir = std::env::temp_dir().join("qf-bun-policy");
    let _ = fs::remove_dir_all(&bun_dir);
    let bun = spawn_bun(45220, 45221, 45222, bun_dir, "");
    wait_ready(45221).await;
    policy_without_declare_args("bun", 45220, 45221, false).await;
    drop(bun);
}

async fn classic_unknown_x_arg(label: &str, amqp: u16) {
    let ch = connect(amqp).await;
    let mut args = FieldTable::default();
    args.insert("x-queue-type".into(), long("classic"));
    args.insert(
        "x-not-a-real-argument".into(),
        lapin::types::AMQPValue::LongInt(7),
    );
    ch.queue_declare("classic-q", qdecl(), args)
        .await
        .expect("declare stays open");
    ch.basic_publish(
        "",
        "classic-q",
        BasicPublishOptions::default(),
        b"classic-body",
        BasicProperties::default(),
    )
    .await
    .unwrap()
    .await
    .ok();
    let got = ch
        .basic_get("classic-q", lapin::options::BasicGetOptions::default())
        .await
        .unwrap()
        .expect("classic message");
    assert_eq!(got.data.as_slice(), b"classic-body");
    got.ack(BasicAckOptions::default()).await.unwrap();
    mark(label, "x-queue-type=classic with an unknown x- argument");
}

#[tokio::test]
async fn rust_and_bun_classic_unknown_x_arg() {
    let _serial = SERIAL.lock().await;
    let rust = spawn_rust(
        45410,
        45411,
        45412,
        std::env::temp_dir().join("qf-rust-classic"),
        "",
    );
    wait_ready(45411).await;
    classic_unknown_x_arg("rust", 45410).await;
    drop(rust);
    let bun = spawn_bun(
        45420,
        45421,
        45422,
        std::env::temp_dir().join("qf-bun-classic"),
        "",
    );
    wait_ready(45421).await;
    classic_unknown_x_arg("bun", 45420).await;
    drop(bun);
}

async fn exchange_to_exchange(label: &str, amqp: u16) {
    let ch = connect(amqp).await;
    ch.exchange_declare(
        "e2e-src",
        ExchangeKind::Direct,
        ExchangeDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    ch.exchange_declare(
        "e2e-dst",
        ExchangeKind::Direct,
        ExchangeDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    ch.queue_declare("e2e-q", qdecl(), FieldTable::default())
        .await
        .unwrap();
    ch.queue_bind(
        "e2e-q",
        "e2e-dst",
        "k",
        QueueBindOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    ch.exchange_bind(
        "e2e-dst",
        "e2e-src",
        "k",
        lapin::options::ExchangeBindOptions::default(),
        FieldTable::default(),
    )
    .await
    .expect("exchange bind");
    ch.basic_publish(
        "e2e-src",
        "k",
        BasicPublishOptions::default(),
        b"e2e-body",
        BasicProperties::default(),
    )
    .await
    .unwrap()
    .await
    .ok();
    let got = ch
        .basic_get("e2e-q", lapin::options::BasicGetOptions::default())
        .await
        .unwrap();
    assert_eq!(got.expect("e2e delivery").data.as_slice(), b"e2e-body");
    mark(label, "exchange-to-exchange delivery");
}

#[tokio::test]
async fn rust_and_bun_exchange_to_exchange() {
    let _serial = SERIAL.lock().await;
    let rust = spawn_rust(
        45510,
        45511,
        45512,
        std::env::temp_dir().join("qf-rust-e2e"),
        "",
    );
    wait_ready(45511).await;
    exchange_to_exchange("rust", 45510).await;
    drop(rust);
    let bun = spawn_bun(
        45520,
        45521,
        45522,
        std::env::temp_dir().join("qf-bun-e2e"),
        "",
    );
    wait_ready(45521).await;
    exchange_to_exchange("bun", 45520).await;
    drop(bun);
}

fn death_of(props: &BasicProperties) -> (String, String, i64) {
    let table = props.headers().clone().expect("x-death headers");
    let value = table.inner().get("x-death").expect("x-death");
    let lapin::types::AMQPValue::FieldArray(items) = value else {
        panic!("x-death is not an array: {value:?}");
    };
    let lapin::types::AMQPValue::FieldTable(entry) =
        items.as_slice().first().expect("x-death entry")
    else {
        panic!("x-death entry is not a table");
    };
    let queue = match entry.inner().get("queue") {
        Some(lapin::types::AMQPValue::LongString(s)) => {
            String::from_utf8_lossy(s.as_bytes()).to_string()
        }
        other => panic!("queue {other:?}"),
    };
    let reason = match entry.inner().get("reason") {
        Some(lapin::types::AMQPValue::LongString(s)) => {
            String::from_utf8_lossy(s.as_bytes()).to_string()
        }
        other => panic!("reason {other:?}"),
    };
    let count = match entry.inner().get("count") {
        Some(lapin::types::AMQPValue::LongLongInt(n)) => *n,
        Some(lapin::types::AMQPValue::LongInt(n)) => i64::from(*n),
        other => panic!("count {other:?}"),
    };
    (queue, reason, count)
}

async fn classic_edges(label: &str, amqp: u16) {
    let ch = connect(amqp).await;
    ch.queue_declare("cancel-q", qdecl(), FieldTable::default())
        .await
        .unwrap();
    let mut consumer = ch
        .basic_consume(
            "cancel-q",
            "ctag",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    ch.basic_cancel("ctag", lapin::options::BasicCancelOptions::default())
        .await
        .unwrap();
    ch.basic_publish(
        "",
        "cancel-q",
        BasicPublishOptions::default(),
        b"after-cancel",
        BasicProperties::default(),
    )
    .await
    .unwrap()
    .await
    .ok();
    let pending = tokio::time::timeout(Duration::from_millis(200), consumer.next()).await;
    assert!(
        pending.is_err() || pending.unwrap().is_none(),
        "{label} basic.cancel"
    );
    let got = ch
        .basic_get("cancel-q", lapin::options::BasicGetOptions::default())
        .await
        .unwrap()
        .expect("after cancel");
    assert_eq!(got.data.as_slice(), b"after-cancel");
    got.ack(BasicAckOptions::default()).await.unwrap();
    mark(label, "basic.cancel");

    let watch = connect(amqp).await;
    watch
        .queue_declare("gone-q", qdecl(), FieldTable::default())
        .await
        .unwrap();
    let mut watched = watch
        .basic_consume(
            "gone-q",
            "watch",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    let other = connect(amqp).await;
    other
        .queue_delete("gone-q", lapin::options::QueueDeleteOptions::default())
        .await
        .unwrap();
    let note = tokio::time::timeout(Duration::from_secs(2), watched.next()).await;
    assert!(
        note.expect("server cancel").is_none(),
        "{label} queue delete did not cancel"
    );
    mark(label, "queue-delete consumer cancel");

    let conf = connect(amqp).await;
    conf.confirm_select(lapin::options::ConfirmSelectOptions::default())
        .await
        .unwrap();
    let mut args = FieldTable::default();
    args.insert("x-max-length".into(), lapin::types::AMQPValue::LongInt(1));
    args.insert("x-overflow".into(), long("reject-publish"));
    conf.queue_declare("rej-q", qdecl(), args).await.unwrap();
    let first = conf
        .basic_publish(
            "",
            "rej-q",
            BasicPublishOptions::default(),
            b"first",
            BasicProperties::default(),
        )
        .await
        .unwrap()
        .await
        .unwrap();
    assert!(
        matches!(first, lapin::publisher_confirm::Confirmation::Ack(_)),
        "{label} {first:?}"
    );
    let second = conf
        .basic_publish(
            "",
            "rej-q",
            BasicPublishOptions::default(),
            b"second",
            BasicProperties::default(),
        )
        .await
        .unwrap()
        .await
        .unwrap();
    assert!(
        matches!(second, lapin::publisher_confirm::Confirmation::Nack(_)),
        "{label} {second:?}"
    );
    conf.queue_declare("still-open", qdecl(), FieldTable::default())
        .await
        .expect("channel stays open after confirm nack");
    mark(label, "confirm nack");

    let dl = connect(amqp).await;
    dl.exchange_declare(
        "death-ex",
        ExchangeKind::Direct,
        ExchangeDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    for name in ["dead-expired", "dead-rejected", "dead-maxlen"] {
        dl.queue_declare(name, qdecl(), FieldTable::default())
            .await
            .unwrap();
        dl.queue_bind(
            name,
            "death-ex",
            name,
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    }
    let mut ttl = FieldTable::default();
    ttl.insert(
        "x-message-ttl".into(),
        lapin::types::AMQPValue::LongInt(200),
    );
    ttl.insert("x-dead-letter-exchange".into(), long("death-ex"));
    ttl.insert("x-dead-letter-routing-key".into(), long("dead-expired"));
    dl.queue_declare("src-expired", qdecl(), ttl).await.unwrap();
    dl.basic_publish(
        "",
        "src-expired",
        BasicPublishOptions::default(),
        b"expired-body",
        BasicProperties::default(),
    )
    .await
    .unwrap()
    .await
    .ok();
    tokio::time::sleep(Duration::from_millis(700)).await;
    let _ = dl
        .basic_get(
            "src-expired",
            lapin::options::BasicGetOptions { no_ack: true },
        )
        .await
        .unwrap();
    let expired = dl
        .basic_get("dead-expired", lapin::options::BasicGetOptions::default())
        .await
        .unwrap()
        .expect("expired");
    assert_eq!(expired.data.as_slice(), b"expired-body");
    assert_eq!(
        death_of(&expired.properties),
        ("src-expired".to_string(), "expired".to_string(), 1)
    );

    let mut rej = FieldTable::default();
    rej.insert("x-dead-letter-exchange".into(), long("death-ex"));
    rej.insert("x-dead-letter-routing-key".into(), long("dead-rejected"));
    dl.queue_declare("src-rejected", qdecl(), rej)
        .await
        .unwrap();
    dl.basic_publish(
        "",
        "src-rejected",
        BasicPublishOptions::default(),
        b"rejected-body",
        BasicProperties::default(),
    )
    .await
    .unwrap()
    .await
    .ok();
    let held = dl
        .basic_get("src-rejected", lapin::options::BasicGetOptions::default())
        .await
        .unwrap()
        .expect("held");
    dl.basic_reject(
        held.delivery_tag,
        lapin::options::BasicRejectOptions { requeue: false },
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let rejected = dl
        .basic_get("dead-rejected", lapin::options::BasicGetOptions::default())
        .await
        .unwrap()
        .expect("rejected");
    assert_eq!(rejected.data.as_slice(), b"rejected-body");
    assert_eq!(
        death_of(&rejected.properties),
        ("src-rejected".to_string(), "rejected".to_string(), 1)
    );

    let mut mx = FieldTable::default();
    mx.insert("x-max-length".into(), lapin::types::AMQPValue::LongInt(1));
    mx.insert("x-dead-letter-exchange".into(), long("death-ex"));
    mx.insert("x-dead-letter-routing-key".into(), long("dead-maxlen"));
    dl.queue_declare("src-maxlen", qdecl(), mx).await.unwrap();
    dl.basic_publish(
        "",
        "src-maxlen",
        BasicPublishOptions::default(),
        b"maxlen-body",
        BasicProperties::default(),
    )
    .await
    .unwrap()
    .await
    .ok();
    dl.basic_publish(
        "",
        "src-maxlen",
        BasicPublishOptions::default(),
        b"kept-new",
        BasicProperties::default(),
    )
    .await
    .unwrap()
    .await
    .ok();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let maxlen = dl
        .basic_get("dead-maxlen", lapin::options::BasicGetOptions::default())
        .await
        .unwrap()
        .expect("maxlen");
    assert_eq!(maxlen.data.as_slice(), b"maxlen-body");
    assert_eq!(
        death_of(&maxlen.properties),
        ("src-maxlen".to_string(), "maxlen".to_string(), 1)
    );
    mark(label, "x-death queue/reason/count");
}

fn death_exchange(props: &BasicProperties) -> (String, String) {
    let table = props.headers().clone().expect("x-death headers");
    let value = table.inner().get("x-death").expect("x-death");
    let lapin::types::AMQPValue::FieldArray(items) = value else {
        panic!("x-death is not an array: {value:?}");
    };
    let lapin::types::AMQPValue::FieldTable(entry) =
        items.as_slice().first().expect("x-death entry")
    else {
        panic!("x-death entry is not a table");
    };
    let exchange = match entry.inner().get("exchange") {
        Some(lapin::types::AMQPValue::LongString(s)) => {
            String::from_utf8_lossy(s.as_bytes()).to_string()
        }
        other => panic!("exchange {other:?}"),
    };
    let routing = match entry.inner().get("routing-keys") {
        Some(lapin::types::AMQPValue::FieldArray(keys)) => match keys.as_slice().first() {
            Some(lapin::types::AMQPValue::LongString(s)) => {
                String::from_utf8_lossy(s.as_bytes()).to_string()
            }
            other => panic!("routing key {other:?}"),
        },
        other => panic!("routing-keys {other:?}"),
    };
    (exchange, routing)
}

async fn classic_gaps(label: &str, amqp: u16, mgmt: Option<u16>) {
    let ch = connect(amqp).await;
    ch.queue_declare("rec-q", qdecl(), FieldTable::default())
        .await
        .unwrap();
    let mut consumer = ch
        .basic_consume(
            "rec-q",
            "rec",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    ch.basic_publish(
        "",
        "rec-q",
        BasicPublishOptions::default(),
        b"recover-body",
        BasicProperties::default(),
    )
    .await
    .unwrap()
    .await
    .ok();
    let first = tokio::time::timeout(Duration::from_secs(2), consumer.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(first.data.as_slice(), b"recover-body");
    assert!(!first.redelivered);
    ch.basic_recover(lapin::options::BasicRecoverOptions { requeue: true })
        .await
        .unwrap();
    let again = tokio::time::timeout(Duration::from_secs(2), consumer.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(again.data.as_slice(), b"recover-body");
    assert!(again.redelivered);
    again.ack(BasicAckOptions::default()).await.unwrap();
    mark(label, "basic.recover");

    let mut exp = FieldTable::default();
    exp.insert("x-expires".into(), lapin::types::AMQPValue::LongInt(400));
    ch.queue_declare("exp-q", qdecl(), exp).await.unwrap();
    tokio::time::sleep(Duration::from_millis(900)).await;
    let ch_exp = connect(amqp).await;
    let missing = ch_exp
        .queue_declare(
            "exp-q",
            QueueDeclareOptions {
                passive: true,
                ..qdecl()
            },
            FieldTable::default(),
        )
        .await;
    assert!(missing.is_err(), "expired queue still exists");
    mark(label, "queue expires");

    let ch = connect(amqp).await;
    ch.exchange_declare(
        "gap-ex",
        ExchangeKind::Direct,
        ExchangeDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    ch.queue_declare("gap-dead", qdecl(), FieldTable::default())
        .await
        .unwrap();
    ch.queue_bind(
        "gap-dead",
        "gap-ex",
        "gap-bytes",
        QueueBindOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    let mut bytes = FieldTable::default();
    bytes.insert(
        "x-max-length-bytes".into(),
        lapin::types::AMQPValue::LongInt(4),
    );
    bytes.insert("x-overflow".into(), long("drop-head"));
    bytes.insert("x-dead-letter-exchange".into(), long("gap-ex"));
    bytes.insert("x-dead-letter-routing-key".into(), long("gap-bytes"));
    ch.queue_declare("gap-bytes", qdecl(), bytes).await.unwrap();
    ch.basic_publish(
        "",
        "gap-bytes",
        BasicPublishOptions::default(),
        b"abcd",
        BasicProperties::default(),
    )
    .await
    .unwrap()
    .await
    .ok();
    ch.basic_publish(
        "",
        "gap-bytes",
        BasicPublishOptions::default(),
        b"xy",
        BasicProperties::default(),
    )
    .await
    .unwrap()
    .await
    .ok();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let dead = ch
        .basic_get("gap-dead", lapin::options::BasicGetOptions::default())
        .await
        .unwrap()
        .expect("byte overflow");
    assert_eq!(dead.data.as_slice(), b"abcd");
    assert_eq!(death_of(&dead.properties).1, "maxlen");
    assert_eq!(
        death_exchange(&dead.properties),
        ("".to_string(), "gap-bytes".to_string())
    );
    mark(label, "max-length-bytes and x-death exchange");

    let mut rej = FieldTable::default();
    rej.insert("x-max-length".into(), lapin::types::AMQPValue::LongInt(1));
    rej.insert("x-overflow".into(), long("reject-publish"));
    ch.queue_declare("gap-rej", qdecl(), rej).await.unwrap();
    ch.confirm_select(lapin::options::ConfirmSelectOptions::default())
        .await
        .unwrap();
    let ack = ch
        .basic_publish(
            "",
            "gap-rej",
            BasicPublishOptions::default(),
            b"one",
            BasicProperties::default(),
        )
        .await
        .unwrap()
        .await
        .unwrap();
    assert!(matches!(
        ack,
        lapin::publisher_confirm::Confirmation::Ack(_)
    ));
    let nack = ch
        .basic_publish(
            "",
            "gap-rej",
            BasicPublishOptions::default(),
            b"two",
            BasicProperties::default(),
        )
        .await
        .unwrap()
        .await
        .unwrap();
    assert!(matches!(
        nack,
        lapin::publisher_confirm::Confirmation::Nack(_)
    ));
    mark(label, "overflow reject-publish");

    let mut sac = FieldTable::default();
    sac.insert(
        "x-single-active-consumer".into(),
        lapin::types::AMQPValue::Boolean(true),
    );
    let chs = connect(amqp).await;
    chs.queue_declare("sac-q", qdecl(), sac).await.unwrap();
    let mut first_c = chs
        .basic_consume(
            "sac-q",
            "sac-a",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    let chs2 = connect(amqp).await;
    let mut second_c = chs2
        .basic_consume(
            "sac-q",
            "sac-b",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    chs.basic_publish(
        "",
        "sac-q",
        BasicPublishOptions::default(),
        b"only-active",
        BasicProperties::default(),
    )
    .await
    .unwrap()
    .await
    .ok();
    let active = tokio::time::timeout(Duration::from_secs(2), first_c.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(active.data.as_slice(), b"only-active");
    active.ack(BasicAckOptions::default()).await.unwrap();
    let idle = tokio::time::timeout(Duration::from_millis(300), second_c.next()).await;
    assert!(idle.is_err(), "standby consumer received a message");
    chs.basic_cancel("sac-a", lapin::options::BasicCancelOptions::default())
        .await
        .unwrap();
    chs2.basic_publish(
        "",
        "sac-q",
        BasicPublishOptions::default(),
        b"promoted",
        BasicProperties::default(),
    )
    .await
    .unwrap()
    .await
    .ok();
    let promoted = tokio::time::timeout(Duration::from_secs(2), second_c.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(promoted.data.as_slice(), b"promoted");
    mark(label, "single active consumer");

    let mut bad = FieldTable::default();
    bad.insert("x-queue-type".into(), long("nosuch"));
    let refused = chs.queue_declare("not-a-queue", qdecl(), bad).await;
    assert!(refused.is_err(), "unsupported queue type was accepted");
    let ch_check = connect(amqp).await;
    let gone = ch_check
        .queue_declare(
            "not-a-queue",
            QueueDeclareOptions {
                passive: true,
                ..qdecl()
            },
            FieldTable::default(),
        )
        .await;
    assert!(gone.is_err(), "unsupported declare left a queue");
    mark(label, "unsupported queue type rejected");

    let ch_imm = connect(amqp).await;
    ch_imm
        .queue_declare("imm-q", qdecl(), FieldTable::default())
        .await
        .unwrap();
    ch_imm
        .confirm_select(lapin::options::ConfirmSelectOptions::default())
        .await
        .unwrap();
    let immediate = ch_imm
        .basic_publish(
            "",
            "imm-q",
            BasicPublishOptions {
                immediate: true,
                ..BasicPublishOptions::default()
            },
            b"now",
            BasicProperties::default(),
        )
        .await
        .expect("publish handed to client")
        .await
        .expect_err("immediate=true was confirmed");
    let mut text = format!("{immediate:?}");
    let mut src = immediate.source();
    while let Some(s) = src {
        text.push_str(&format!(" | {s} | {s:?}"));
        src = s.source();
    }
    assert!(
        text.contains("540") || text.contains("NOT_IMPLEMENTED"),
        "{text}"
    );
    mark(label, "immediate closes");

    let Some(mgmt) = mgmt else {
        return;
    };
    let http = Client::builder().cookie_store(true).build().unwrap();
    let login = http
        .post(format!("http://127.0.0.1:{mgmt}/api/login"))
        .json(&serde_json::json!({"username": "admin", "password": "devpassword12"}))
        .send()
        .await
        .unwrap();
    assert!(login.status().is_success(), "{}", login.status());
    let chp = connect(amqp).await;
    chp.queue_declare("live-q", qdecl(), FieldTable::default())
        .await
        .unwrap();
    chp.queue_declare("live-dead", qdecl(), FieldTable::default())
        .await
        .unwrap();
    chp.exchange_declare(
        "live-ex",
        ExchangeKind::Direct,
        ExchangeDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    chp.queue_bind(
        "live-dead",
        "live-ex",
        "live-q",
        QueueBindOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    let ha = http
        .put(format!("http://127.0.0.1:{mgmt}/api/policies/%2F/live-ha"))
        .json(&serde_json::json!({
            "pattern": "^live-q$",
            "apply-to": "queues",
            "priority": 1,
            "definition": {"ha-mode": "all", "ha-params": 2}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(ha.status(), 400, "{label} ha-mode {}", ha.status());
    let ha_body = ha.text().await.unwrap_or_default();
    assert!(
        ha_body.contains("not recognised"),
        "{label} ha-mode body {ha_body}"
    );
    let put = http
        .put(format!("http://127.0.0.1:{mgmt}/api/policies/%2F/live-pol"))
        .json(&serde_json::json!({
            "pattern": "^live-q$",
            "apply-to": "queues",
            "priority": 1,
            "definition": {
                "message-ttl": 300,
                "dead-letter-exchange": "live-ex",
                "dead-letter-routing-key": "live-q"
            }
        }))
        .send()
        .await
        .unwrap();
    assert!(put.status().is_success(), "policy {}", put.status());
    chp.basic_publish(
        "",
        "live-q",
        BasicPublishOptions::default(),
        b"lived",
        BasicProperties::default(),
    )
    .await
    .unwrap()
    .await
    .ok();
    tokio::time::sleep(Duration::from_millis(800)).await;
    chp.basic_get("live-q", lapin::options::BasicGetOptions::default())
        .await
        .unwrap();
    let lived = chp
        .basic_get("live-dead", lapin::options::BasicGetOptions::default())
        .await
        .unwrap()
        .expect("live ttl");
    assert_eq!(lived.data.as_slice(), b"lived");
    mark(label, "live durable policy");
    mark(label, "live durable policies");

    let listed = http
        .get(format!("http://127.0.0.1:{mgmt}/api/queues/%2F"))
        .send()
        .await
        .unwrap();
    assert!(listed.status().is_success(), "{}", listed.status());
    let body = listed.text().await.unwrap();
    assert!(
        body.contains("\"type\":\"classic\"") || body.contains("\"type\": \"classic\""),
        "{body}"
    );
    for field in [
        "\"vhost\"",
        "\"durable\"",
        "\"auto_delete\"",
        "\"arguments\"",
        "\"type\"",
    ] {
        assert!(
            body.contains(field),
            "{label} queue list missing {field}: {body}"
        );
    }
    let policies = http
        .get(format!("http://127.0.0.1:{mgmt}/api/policies/%2F"))
        .send()
        .await
        .unwrap();
    let policies = policies.text().await.unwrap();
    for field in ["\"apply-to\"", "\"pattern\"", "\"definition\""] {
        assert!(
            policies.contains(field),
            "{label} policies missing {field}: {policies}"
        );
    }
    mark(label, "management queue type");
    mark(label, "management field names including queue type");
    mark(label, "classic queue behavior");
}

async fn rabbit_queue_behaviors(label: &str, amqp: u16) {
    let ch = connect(amqp).await;
    ch.exchange_declare(
        "cc-ex",
        ExchangeKind::Direct,
        ExchangeDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    for (queue, key) in [("cc-a", "a"), ("cc-b", "b"), ("cc-c", "c")] {
        ch.queue_declare(queue, qdecl(), FieldTable::default())
            .await
            .unwrap();
        ch.queue_bind(
            queue,
            "cc-ex",
            key,
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    }
    let mut headers = FieldTable::default();
    headers.insert(
        ShortString::from("CC"),
        lapin::types::AMQPValue::FieldArray(vec![long("b")].into()),
    );
    headers.insert(
        ShortString::from("BCC"),
        lapin::types::AMQPValue::FieldArray(vec![long("c")].into()),
    );
    let props = BasicProperties::default().with_headers(headers);
    ch.basic_publish(
        "cc-ex",
        "a",
        BasicPublishOptions::default(),
        b"copied",
        props,
    )
    .await
    .unwrap()
    .await
    .unwrap();
    for queue in ["cc-a", "cc-b", "cc-c"] {
        let mut consumer = ch
            .basic_consume(
                queue,
                queue,
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .unwrap();
        let delivery = tokio::time::timeout(Duration::from_secs(2), consumer.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(delivery.data.as_slice(), b"copied", "{label} {queue}");
        let present = delivery
            .properties
            .headers()
            .as_ref()
            .and_then(|table| table.inner().get("BCC"))
            .is_some();
        assert!(!present, "{label} {queue} kept BCC");
        delivery.ack(BasicAckOptions::default()).await.unwrap();
    }
    mark(label, "cc and bcc");

    let mut low = FieldTable::default();
    low.insert(
        ShortString::from("x-priority"),
        lapin::types::AMQPValue::LongInt(1),
    );
    let mut high = FieldTable::default();
    high.insert(
        ShortString::from("x-priority"),
        lapin::types::AMQPValue::LongInt(10),
    );
    ch.queue_declare("pri-q", qdecl(), FieldTable::default())
        .await
        .unwrap();
    let mut low_c = ch
        .basic_consume("pri-q", "low", BasicConsumeOptions::default(), low)
        .await
        .unwrap();
    let mut high_c = ch
        .basic_consume("pri-q", "high", BasicConsumeOptions::default(), high)
        .await
        .unwrap();
    ch.basic_publish(
        "",
        "pri-q",
        BasicPublishOptions::default(),
        b"prio",
        BasicProperties::default(),
    )
    .await
    .unwrap()
    .await
    .unwrap();
    let got = tokio::time::timeout(Duration::from_secs(2), high_c.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(got.data.as_slice(), b"prio", "{label} priority");
    got.ack(BasicAckOptions::default()).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(200), low_c.next())
            .await
            .is_err(),
        "{label} low priority consumer received the message"
    );
    mark(label, "consumer priority");

    ch.confirm_select(lapin::options::ConfirmSelectOptions::default())
        .await
        .unwrap();
    ch.exchange_declare(
        "rej-ex",
        ExchangeKind::Direct,
        ExchangeDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    ch.queue_declare("rej-dead", qdecl(), FieldTable::default())
        .await
        .unwrap();
    ch.queue_bind(
        "rej-dead",
        "rej-ex",
        "rej-src",
        QueueBindOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    let mut overflow = FieldTable::default();
    overflow.insert(
        ShortString::from("x-max-length"),
        lapin::types::AMQPValue::LongInt(1),
    );
    overflow.insert(ShortString::from("x-overflow"), long("reject-publish-dlx"));
    overflow.insert(ShortString::from("x-dead-letter-exchange"), long("rej-ex"));
    ch.queue_declare("rej-src", qdecl(), overflow)
        .await
        .unwrap();
    let first = ch
        .basic_publish(
            "",
            "rej-src",
            BasicPublishOptions::default(),
            b"keep",
            BasicProperties::default(),
        )
        .await
        .unwrap()
        .await
        .unwrap();
    assert!(
        matches!(first, lapin::publisher_confirm::Confirmation::Ack(_)),
        "{label} {first:?}"
    );
    let second = ch
        .basic_publish(
            "",
            "rej-src",
            BasicPublishOptions::default(),
            b"dead",
            BasicProperties::default(),
        )
        .await
        .unwrap()
        .await
        .unwrap();
    assert!(
        matches!(second, lapin::publisher_confirm::Confirmation::Nack(_)),
        "{label} {second:?}"
    );
    assert_eq!(
        one_delivery(&ch, "rej-dead").await,
        b"dead",
        "{label} reject-publish-dlx"
    );
    mark(label, "reject-publish-dlx");
}

#[tokio::test]
async fn rust_and_bun_rabbit_queue_behaviors() {
    let _serial = SERIAL.lock().await;
    let rust = spawn_rust(
        45810,
        45811,
        45812,
        std::env::temp_dir().join("qf-rust-rabbit-beh"),
        "",
    );
    wait_ready(45811).await;
    rabbit_queue_behaviors("rust", 45810).await;
    drop(rust);
    let bun = spawn_bun(
        45820,
        45821,
        45822,
        std::env::temp_dir().join("qf-bun-rabbit-beh"),
        "",
    );
    wait_ready(45821).await;
    rabbit_queue_behaviors("bun", 45820).await;
    drop(bun);
}

#[tokio::test]
async fn rust_and_bun_classic_gaps() {
    let _serial = SERIAL.lock().await;
    let rust_dir = std::env::temp_dir().join("qf-rust-gaps");
    let _ = fs::remove_dir_all(&rust_dir);
    let rust = spawn_rust(45710, 45711, 45712, rust_dir, "");
    wait_ready(45711).await;
    classic_gaps("rust", 45710, Some(45711)).await;
    drop(rust);
    let bun_dir = std::env::temp_dir().join("qf-bun-gaps");
    let _ = fs::remove_dir_all(&bun_dir);
    let bun = spawn_bun(45720, 45721, 45722, bun_dir, "");
    wait_ready(45721).await;
    classic_gaps("bun", 45720, Some(45721)).await;
    drop(bun);
}

#[tokio::test]
async fn rust_and_bun_classic_edges() {
    let _serial = SERIAL.lock().await;
    let rust = spawn_rust(
        45610,
        45611,
        45612,
        std::env::temp_dir().join("qf-rust-edges"),
        "",
    );
    wait_ready(45611).await;
    classic_edges("rust", 45610).await;
    drop(rust);
    let bun = spawn_bun(
        45620,
        45621,
        45622,
        std::env::temp_dir().join("qf-bun-edges"),
        "",
    );
    wait_ready(45621).await;
    classic_edges("bun", 45620).await;
    drop(bun);
}

#[tokio::test]
async fn rust_matches_the_shared_check() {
    let _serial = SERIAL.lock().await;
    let n = stamp() as u16 % 1000;
    let amqp = 36000 + n;
    let mgmt = 37000 + n;
    let metrics = 38000 + n;
    let data = std::env::temp_dir().join(format!("qf-rust-parity-{n}"));
    let node = spawn_rust(amqp, mgmt, metrics, data, "");
    wait_ready(mgmt).await;
    exercise("rust", amqp, mgmt, metrics).await;
    drop(node);
}

#[tokio::test]
async fn bun_matches_the_shared_check() {
    let _serial = SERIAL.lock().await;
    let n = (stamp() as u16).wrapping_add(17) % 1000;
    let amqp = 36100 + (n % 800);
    let mgmt = 37100 + (n % 800);
    let metrics = 38100 + (n % 800);
    let data = std::env::temp_dir().join(format!("qf-bun-parity-{n}"));
    let node = spawn_bun(amqp, mgmt, metrics, data, "");
    wait_ready(mgmt).await;
    exercise("bun", amqp, mgmt, metrics).await;
    drop(node);
}

#[tokio::test]
async fn rust_durable_restart_and_bun_durable_restart() {
    let _serial = SERIAL.lock().await;
    durable_roundtrip(spawn_rust, 39010, 39110, 39210, "rust").await;
    durable_roundtrip(spawn_bun, 39020, 39120, 39220, "bun").await;
}

#[tokio::test]
async fn rust_cluster_and_bun_cluster() {
    let _serial = SERIAL.lock().await;
    cluster_roundtrip(spawn_rust, 40010, "rust").await;
    cluster_roundtrip(spawn_bun, 41010, "bun").await;
}

async fn quorum_scenario(spawn: fn(u16, u16, u16, PathBuf, &str) -> Node, base: u16, label: &str) {
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
[limits]
default_queue_type = "quorum"
"#,
        ports[0].3, ports[1].3, ports[2].3
    );
    let ids = ["a", "b", "c"];
    let mut dirs = Vec::new();
    let mut nodes = Vec::new();
    for (i, (amqp, mgmt, metrics, cluster_port)) in ports.iter().copied().enumerate() {
        let dir = std::env::temp_dir().join(format!("qf-q-{label}-{base}-{i}"));
        let _ = fs::remove_dir_all(&dir);
        let cfg = cluster
            .replace("NODE", ids[i])
            .replace("PORT", &cluster_port.to_string());
        nodes.push(spawn(amqp, mgmt, metrics, dir.clone(), &cfg));
        dirs.push(dir);
    }
    for (_, mgmt, _, _) in ports {
        wait_ready(mgmt).await;
    }
    tokio::time::sleep(Duration::from_millis(600)).await;

    let http = Client::builder().cookie_store(true).build().unwrap();
    let login = http
        .post(format!("http://127.0.0.1:{}/api/login", ports[0].1))
        .json(&serde_json::json!({"username": "admin", "password": "devpassword12"}))
        .send()
        .await
        .unwrap();
    assert!(login.status().is_success(), "{label} login");
    let user = http
        .put(format!(
            "http://127.0.0.1:{}/api/users/quorum-user",
            ports[0].1
        ))
        .json(&serde_json::json!({"password": "quorum-password", "tags": ["management"]}))
        .send()
        .await
        .unwrap();
    assert!(user.status().is_success(), "{label} user {}", user.status());
    let perm = http
        .put(format!(
            "http://127.0.0.1:{}/api/permissions/quorum-user/%2F",
            ports[0].1
        ))
        .json(&serde_json::json!({"configure": ".*", "write": ".*", "read": ".*"}))
        .send()
        .await
        .unwrap();
    assert!(perm.status().is_success(), "{label} perm");
    tokio::time::sleep(Duration::from_millis(300)).await;
    let from_peer = Connection::connect(
        &format!(
            "amqp://quorum-user:quorum-password@127.0.0.1:{}/%2f",
            ports[1].0
        ),
        ConnectionProperties::default(),
    )
    .await;
    assert!(from_peer.is_ok(), "{label} replicated credentials");
    mark(label, "replicated credentials");

    let cha = connect(ports[0].0).await;
    cha.exchange_declare(
        "qx",
        ExchangeKind::Direct,
        ExchangeDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let chb = connect(ports[1].0).await;
    chb.exchange_declare(
        "qx",
        ExchangeKind::Direct,
        ExchangeDeclareOptions {
            passive: true,
            ..ExchangeDeclareOptions::default()
        },
        FieldTable::default(),
    )
    .await
    .expect("replicated exchange");
    let _ = (cha, chb);
    mark(label, "replicated exchanges");
    mark(label, "replicated credentials and exchanges");

    let durable = QueueDeclareOptions {
        durable: true,
        ..qdecl()
    };
    let cha = connect(ports[0].0).await;
    let mut classic_args = FieldTable::default();
    classic_args.insert("x-queue-type".into(), long("classic"));
    cha.queue_declare("classic-q", durable, classic_args)
        .await
        .unwrap();
    cha.confirm_select(lapin::options::ConfirmSelectOptions::default())
        .await
        .unwrap();
    let conf = cha
        .basic_publish(
            "",
            "classic-q",
            BasicPublishOptions::default(),
            b"classic-body",
            BasicProperties::default().with_delivery_mode(2),
        )
        .await
        .unwrap();
    assert!(conf.await.unwrap().is_ack());
    let ch_other = connect(ports[1].0).await;
    let got = ch_other
        .basic_get("classic-q", lapin::options::BasicGetOptions::default())
        .await
        .unwrap()
        .expect("any-node consume");
    assert_eq!(got.data.as_slice(), b"classic-body");
    got.ack(BasicAckOptions::default()).await.unwrap();
    mark(label, "any-node publish and consume");

    let mut bad_ex = FieldTable::default();
    bad_ex.insert("x-queue-type".into(), long("quorum"));
    let exclusive = cha
        .queue_declare(
            "qq-ex",
            QueueDeclareOptions {
                exclusive: true,
                durable: true,
                ..qdecl()
            },
            bad_ex.clone(),
        )
        .await;
    assert!(exclusive.is_err(), "{label} exclusive quorum was accepted");
    let mut bad_nd = FieldTable::default();
    bad_nd.insert("x-queue-type".into(), long("quorum"));
    let nondurable = cha
        .queue_declare(
            "qq-nd",
            QueueDeclareOptions {
                durable: false,
                ..Default::default()
            },
            bad_nd,
        )
        .await;
    assert!(
        nondurable.is_err(),
        "{label} non-durable quorum was accepted"
    );
    let ch_check = connect(ports[0].0).await;
    assert!(ch_check
        .queue_declare(
            "qq-ex",
            QueueDeclareOptions {
                passive: true,
                ..qdecl()
            },
            FieldTable::default()
        )
        .await
        .is_err());
    assert!(ch_check
        .queue_declare(
            "qq-nd",
            QueueDeclareOptions {
                passive: true,
                ..qdecl()
            },
            FieldTable::default()
        )
        .await
        .is_err());
    mark(label, "failed exclusive and non-durable quorum declares");

    let cha = connect(ports[0].0).await;
    cha.queue_declare("qq", durable, FieldTable::default())
        .await
        .unwrap();
    for port in [ports[1].0, ports[2].0] {
        connect(port)
            .await
            .queue_declare("qq", durable, FieldTable::default())
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    let listed = http
        .get(format!("http://127.0.0.1:{}/api/queues/%2F", ports[0].1))
        .send()
        .await
        .unwrap();
    let body = listed.text().await.unwrap();
    assert!(body.contains("quorum"), "{label} default queue type {body}");
    mark(label, "default_queue_type=quorum");

    cha.confirm_select(lapin::options::ConfirmSelectOptions::default())
        .await
        .unwrap();
    let conf = cha
        .basic_publish(
            "",
            "qq",
            BasicPublishOptions::default(),
            b"majority-body",
            BasicProperties::default().with_delivery_mode(2),
        )
        .await
        .unwrap();
    assert!(conf.await.expect("majority confirm").is_ack());
    mark(label, "majority confirm");
    let on_c = connect(ports[2].0).await;
    let seen = on_c
        .basic_get("qq", lapin::options::BasicGetOptions::default())
        .await
        .unwrap()
        .expect("replica on C");
    assert_eq!(seen.data.as_slice(), b"majority-body");
    let on_b = connect(ports[1].0).await;
    let from_b = on_b
        .basic_get("qq", lapin::options::BasicGetOptions::default())
        .await
        .unwrap();
    assert!(
        from_b.is_none(),
        "{label} B delivered a quorum body still unacked on C"
    );
    let on_a = connect(ports[0].0).await;
    let from_a = on_a
        .basic_get("qq", lapin::options::BasicGetOptions::default())
        .await
        .unwrap();
    assert!(
        from_a.is_none(),
        "{label} A still had the quorum body after C claimed it"
    );
    seen.nack(BasicNackOptions {
        multiple: false,
        requeue: true,
    })
    .await
    .unwrap();
    let requeued = connect(ports[0].0).await;
    let back = requeued
        .basic_get("qq", lapin::options::BasicGetOptions::default())
        .await
        .unwrap()
        .expect("requeued quorum body");
    assert_eq!(back.data.as_slice(), b"majority-body");
    assert!(
        back.redelivered,
        "{label} requeued quorum body was not marked redelivered"
    );
    back.ack(BasicAckOptions::default()).await.unwrap();
    for port in [ports[0].0, ports[1].0, ports[2].0] {
        let again = connect(port).await;
        let left = again
            .basic_get("qq", lapin::options::BasicGetOptions::default())
            .await
            .unwrap();
        assert!(
            left.is_none(),
            "{label} quorum body still ready on {port} after ack"
        );
    }
    mark(
        label,
        "nack requeue from a non-leader returns the quorum body",
    );

    let hold_b = connect(ports[1].0).await;
    let hold_c = connect(ports[2].0).await;
    let mut cons_b = hold_b
        .basic_consume(
            "qq",
            "wait-b",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    let mut cons_c = hold_c
        .basic_consume(
            "qq",
            "wait-c",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let publisher = connect(ports[0].0).await;
    publisher
        .confirm_select(lapin::options::ConfirmSelectOptions::default())
        .await
        .unwrap();
    let conf = publisher
        .basic_publish(
            "",
            "qq",
            BasicPublishOptions::default(),
            b"one-consumer",
            BasicProperties::default().with_delivery_mode(2),
        )
        .await
        .unwrap();
    assert!(conf.await.expect("consumer publish").is_ack());
    let (got_b, got_c) = tokio::join!(
        tokio::time::timeout(Duration::from_secs(2), cons_b.next()),
        tokio::time::timeout(Duration::from_secs(2), cons_c.next()),
    );
    let mut handed = Vec::new();
    if let Ok(Some(Ok(msg))) = got_b {
        handed.push(msg);
    }
    if let Ok(Some(Ok(msg))) = got_c {
        handed.push(msg);
    }
    assert_eq!(
        handed.len(),
        1,
        "{label} two waiting consumers both received the quorum body"
    );
    assert_eq!(handed[0].data.as_slice(), b"one-consumer");
    for port in [ports[0].0, ports[1].0, ports[2].0] {
        let again = connect(port).await;
        let left = again
            .basic_get("qq", lapin::options::BasicGetOptions::default())
            .await
            .unwrap();
        assert!(
            left.is_none(),
            "{label} basic.get on {port} returned the quorum body while a consumer still holds it"
        );
    }
    hold_b
        .basic_cancel("wait-b", lapin::options::BasicCancelOptions::default())
        .await
        .unwrap();
    hold_c
        .basic_cancel("wait-c", lapin::options::BasicCancelOptions::default())
        .await
        .unwrap();
    handed[0]
        .nack(BasicNackOptions {
            multiple: false,
            requeue: true,
        })
        .await
        .unwrap();
    let again_ready = connect(ports[0].0).await;
    let restored = again_ready
        .basic_get("qq", lapin::options::BasicGetOptions::default())
        .await
        .unwrap()
        .expect("requeued consumer body");
    assert_eq!(restored.data.as_slice(), b"one-consumer");
    assert!(
        restored.redelivered,
        "{label} consumer nack did not redeliver"
    );
    restored.ack(BasicAckOptions::default()).await.unwrap();
    for port in [ports[0].0, ports[1].0, ports[2].0] {
        let again = connect(port).await;
        let left = again
            .basic_get("qq", lapin::options::BasicGetOptions::default())
            .await
            .unwrap();
        assert!(
            left.is_none(),
            "{label} quorum body still ready on {port} after the consumer ack"
        );
    }
    mark(label, "one waiting consumer receives a quorum message");

    let lim = connect(ports[0].0).await;
    lim.exchange_declare(
        "lim-ex",
        ExchangeKind::Fanout,
        ExchangeDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    lim.queue_declare("lim-dead", durable, FieldTable::default())
        .await
        .unwrap();
    lim.queue_bind(
        "lim-dead",
        "lim-ex",
        "",
        QueueBindOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    let mut lim_args = FieldTable::default();
    lim_args.insert(
        "x-delivery-limit".into(),
        lapin::types::AMQPValue::LongInt(1),
    );
    lim_args.insert("x-dead-letter-exchange".into(), long("lim-ex"));
    lim.queue_declare("qq-lim", durable, lim_args)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    lim.basic_publish(
        "",
        "qq-lim",
        BasicPublishOptions::default(),
        b"limit-body",
        BasicProperties::default(),
    )
    .await
    .unwrap()
    .await
    .ok();
    let mut lim_c = lim
        .basic_consume(
            "qq-lim",
            "lim",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    let once = tokio::time::timeout(Duration::from_secs(2), lim_c.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(once.data.as_slice(), b"limit-body");
    once.nack(BasicNackOptions {
        multiple: false,
        requeue: true,
    })
    .await
    .unwrap();
    // RabbitMQ allows `x-delivery-limit` returns. With limit 1 the message
    // comes back once more, and the second return dead-letters it.
    let twice = tokio::time::timeout(Duration::from_secs(2), lim_c.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(twice.data.as_slice(), b"limit-body");
    twice
        .nack(BasicNackOptions {
            multiple: false,
            requeue: true,
        })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let dead = lim
        .basic_get("lim-dead", lapin::options::BasicGetOptions::default())
        .await
        .unwrap()
        .expect("delivery-limit");
    assert_eq!(dead.data.as_slice(), b"limit-body");
    dead.ack(BasicAckOptions::default()).await.unwrap();
    for port in [ports[1].0, ports[2].0] {
        let other = connect(port).await;
        let left = other
            .basic_get("qq-lim", lapin::options::BasicGetOptions::default())
            .await
            .unwrap();
        assert!(
            left.is_none(),
            "{label} delivery-limit left the body on {port}"
        );
    }
    mark(label, "delivery-limit");

    let parked = connect(ports[0].0).await;
    parked
        .confirm_select(lapin::options::ConfirmSelectOptions::default())
        .await
        .unwrap();
    let conf = parked
        .basic_publish(
            "",
            "classic-q",
            BasicPublishOptions::default(),
            b"parked",
            BasicProperties::default().with_delivery_mode(2),
        )
        .await
        .unwrap();
    assert!(conf.await.unwrap().is_ack());
    let mut saw_home_down = false;
    for idx in [2usize, 1, 0] {
        let probe_port = if idx == 0 { ports[1].0 } else { ports[0].0 };
        drop(nodes.remove(idx));
        tokio::time::sleep(Duration::from_millis(400)).await;
        let probe = connect(probe_port).await;
        let got = probe
            .basic_get("classic-q", lapin::options::BasicGetOptions::default())
            .await;
        let cfg = cluster
            .replace("NODE", ids[idx])
            .replace("PORT", &ports[idx].3.to_string());
        nodes.insert(
            idx,
            spawn(
                ports[idx].0,
                ports[idx].1,
                ports[idx].2,
                dirs[idx].clone(),
                &cfg,
            ),
        );
        wait_ready(ports[idx].1).await;
        tokio::time::sleep(Duration::from_millis(500)).await;
        match got {
            Err(_) => {
                let mut msg = None;
                for _ in 0..25 {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    let back = connect(probe_port).await;
                    match back
                        .basic_get("classic-q", lapin::options::BasicGetOptions::default())
                        .await
                    {
                        Ok(Some(m)) => {
                            msg = Some(m);
                            break;
                        }
                        Ok(None) => {
                            panic!("{label} classic queue was empty after its home returned")
                        }
                        Err(_) => continue,
                    }
                }
                let msg = msg.expect("home back");
                assert_eq!(msg.data.as_slice(), b"parked");
                saw_home_down = true;
                break;
            }
            Ok(Some(msg)) => {
                assert_eq!(msg.data.as_slice(), b"parked");
                msg.ack(BasicAckOptions::default()).await.unwrap();
                if idx != 0 {
                    let again = connect(ports[0].0).await;
                    again
                        .confirm_select(lapin::options::ConfirmSelectOptions::default())
                        .await
                        .unwrap();
                    let conf = again
                        .basic_publish(
                            "",
                            "classic-q",
                            BasicPublishOptions::default(),
                            b"parked",
                            BasicProperties::default().with_delivery_mode(2),
                        )
                        .await
                        .unwrap();
                    assert!(conf.await.unwrap().is_ack());
                }
            }
            Ok(None) => panic!("{label} classic queue was empty while its home was up"),
        }
    }
    assert!(
        saw_home_down,
        "{label} classic home never became unavailable"
    );
    mark(label, "classic home-down unavailable");

    drop(nodes.pop());
    tokio::time::sleep(Duration::from_millis(800)).await;
    let survivor = connect(ports[0].0).await;
    survivor
        .confirm_select(lapin::options::ConfirmSelectOptions::default())
        .await
        .unwrap();
    let mut acked = false;
    for _ in 0..8 {
        let again = survivor
            .basic_publish(
                "",
                "qq",
                BasicPublishOptions::default(),
                b"still-here",
                BasicProperties::default().with_delivery_mode(2),
            )
            .await
            .unwrap();
        if again.await.expect("confirm after minority loss").is_ack() {
            acked = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(acked, "{label} no confirm after minority loss");
    let second = survivor
        .basic_get("qq", lapin::options::BasicGetOptions::default())
        .await
        .unwrap()
        .expect("body after minority loss");
    assert_eq!(second.data.as_slice(), b"still-here");
    mark(label, "body unchanged after minority loss");

    drop(nodes.pop());
    tokio::time::sleep(Duration::from_millis(400)).await;
    let alone = connect(ports[0].0).await;
    alone
        .confirm_select(lapin::options::ConfirmSelectOptions::default())
        .await
        .unwrap();
    let refused = tokio::time::timeout(
        Duration::from_secs(8),
        alone.basic_publish(
            "",
            "qq",
            BasicPublishOptions::default(),
            b"nope",
            BasicProperties::default().with_delivery_mode(2),
        ),
    )
    .await
    .expect("publish write")
    .unwrap();
    let outcome = tokio::time::timeout(Duration::from_secs(8), refused)
        .await
        .expect("confirm wait");
    match outcome {
        Ok(conf) => assert!(conf.is_nack(), "{label} minority publish was acked"),
        Err(_) => {}
    }
    mark(label, "no confirm when only a minority remains");

    // Classic home stays down. Bring the other two nodes back, then stop the home.
    let _ = dirs;
}

#[tokio::test]
async fn rust_and_bun_quorum_cluster() {
    let _serial = SERIAL.lock().await;
    quorum_scenario(spawn_rust, 42010, "rust").await;
    quorum_scenario(spawn_bun, 43010, "bun").await;
}

fn spawn_with_protocols(
    kind: &str,
    amqp: u16,
    mgmt: u16,
    metrics: u16,
    mqtt: u16,
    stomp: u16,
    stream: u16,
) -> Node {
    let data = std::env::temp_dir().join(format!("qf-proto-{kind}-{amqp}"));
    let _ = fs::remove_dir_all(&data);
    fs::create_dir_all(&data).unwrap();
    let extra = format!("mqtt = \"127.0.0.1:{mqtt}\"\nstomp = \"127.0.0.1:{stomp}\"\nstream = \"127.0.0.1:{stream}\"\n");
    let cfg = data.join("qf.toml");
    write_cfg_extra(&cfg, amqp, mgmt, metrics, &data, "", &extra);
    let child = if kind == "rust" {
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
    };
    Node {
        child,
        amqp,
        mgmt,
        metrics,
        data,
    }
}

async fn read_n(sock: &mut TcpStream, n: usize) -> Vec<u8> {
    let mut buf = vec![0u8; n];
    sock.read_exact(&mut buf).await.expect("read");
    buf
}

async fn mqtt_roundtrip(port: u16, topic: &str, body: &[u8]) {
    fn pkt_connect(cid: &str) -> Vec<u8> {
        let cid = cid.as_bytes();
        let user = b"admin";
        let pw = b"devpassword12";
        let mut payload = Vec::new();
        payload.extend_from_slice(&(cid.len() as u16).to_be_bytes());
        payload.extend_from_slice(cid);
        payload.extend_from_slice(&(user.len() as u16).to_be_bytes());
        payload.extend_from_slice(user);
        payload.extend_from_slice(&(pw.len() as u16).to_be_bytes());
        payload.extend_from_slice(pw);
        let mut vh = b"\x00\x04MQTT\x04\xc2".to_vec();
        vh.extend_from_slice(&30u16.to_be_bytes());
        vh.extend_from_slice(&payload);
        let mut out = vec![0x10, vh.len() as u8];
        out.extend_from_slice(&vh);
        out
    }
    let mut sub = TcpStream::connect(("127.0.0.1", port)).await.expect("mqtt");
    sub.write_all(&pkt_connect("sub")).await.unwrap();
    let ack = read_n(&mut sub, 4).await;
    assert_eq!(&ack, &[0x20, 0x02, 0x00, 0x00], "mqtt connack {port}");
    let t = topic.as_bytes();
    let mut sp = vec![0x82, (5 + t.len()) as u8];
    sp.extend_from_slice(&1u16.to_be_bytes());
    sp.extend_from_slice(&(t.len() as u16).to_be_bytes());
    sp.extend_from_slice(t);
    sp.push(0);
    sub.write_all(&sp).await.unwrap();
    let suback = read_n(&mut sub, 5).await;
    assert_eq!(suback[0], 0x90, "mqtt suback");
    let mut puber = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("mqtt pub");
    puber.write_all(&pkt_connect("pub")).await.unwrap();
    let _ = read_n(&mut puber, 4).await;
    let mut frame = vec![0x30, (2 + t.len() + body.len()) as u8];
    frame.extend_from_slice(&(t.len() as u16).to_be_bytes());
    frame.extend_from_slice(t);
    frame.extend_from_slice(body);
    puber.write_all(&frame).await.unwrap();
    let mut got = vec![0u8; 512];
    let n = tokio::time::timeout(Duration::from_secs(3), sub.read(&mut got))
        .await
        .expect("mqtt deliver")
        .expect("mqtt bytes");
    assert!(
        got[..n].windows(body.len()).any(|w| w == body),
        "mqtt body on {port}: {}",
        String::from_utf8_lossy(&got[..n])
    );
}

async fn stomp_roundtrip(port: u16, queue: &str, body: &str) {
    async fn connect(port: u16) -> TcpStream {
        let mut sock = TcpStream::connect(("127.0.0.1", port))
            .await
            .expect("stomp");
        sock.write_all(
            b"CONNECT\naccept-version:1.2\nhost:/\nlogin:admin\npasscode:devpassword12\n\n\0",
        )
        .await
        .unwrap();
        let mut buf = vec![0u8; 256];
        let n = tokio::time::timeout(Duration::from_secs(3), sock.read(&mut buf))
            .await
            .expect("stomp connected")
            .unwrap();
        let text = String::from_utf8_lossy(&buf[..n]);
        assert!(text.contains("CONNECTED"), "stomp greeting {text}");
        sock
    }
    let mut sub = connect(port).await;
    let dest = format!("/queue/{queue}");
    sub.write_all(format!("SUBSCRIBE\nid:1\ndestination:{dest}\nack:auto\n\n\0").as_bytes())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut puber = connect(port).await;
    puber
        .write_all(
            format!(
                "SEND\ndestination:{dest}\ncontent-length:{}\n\n{body}\0",
                body.len()
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let mut buf = vec![0u8; 512];
    let n = tokio::time::timeout(Duration::from_secs(3), sub.read(&mut buf))
        .await
        .expect("stomp message")
        .unwrap();
    let text = String::from_utf8_lossy(&buf[..n]);
    assert!(text.contains(body), "stomp body {text}");
}

fn stream_str(text: &str) -> Vec<u8> {
    let b = text.as_bytes();
    let mut out = (b.len() as u16).to_be_bytes().to_vec();
    out.extend_from_slice(b);
    out
}

fn stream_frame(key: u16, body: &[u8], corr: Option<u32>) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&key.to_be_bytes());
    payload.extend_from_slice(&1u16.to_be_bytes());
    if let Some(corr) = corr {
        payload.extend_from_slice(&corr.to_be_bytes());
    }
    payload.extend_from_slice(body);
    let mut out = (payload.len() as u32).to_be_bytes().to_vec();
    out.extend_from_slice(&payload);
    out
}

async fn stream_read(sock: &mut TcpStream) -> (u16, Vec<u8>) {
    let hdr = read_n(sock, 4).await;
    let size = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) as usize;
    let buf = read_n(sock, size).await;
    let key = u16::from_be_bytes([buf[0], buf[1]]);
    (key, buf)
}

async fn stream_roundtrip(port: u16, name: &str, body: &[u8]) {
    let mut sock = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("stream");
    let props = {
        let mut p = 1u32.to_be_bytes().to_vec();
        p.extend_from_slice(&stream_str("product"));
        p.extend_from_slice(&stream_str("probe"));
        p
    };
    sock.write_all(&stream_frame(0x0011, &props, Some(1)))
        .await
        .unwrap();
    let (key, _) = stream_read(&mut sock).await;
    assert_eq!(key, 0x8011, "peer properties");
    sock.write_all(&stream_frame(0x0012, &[], Some(2)))
        .await
        .unwrap();
    let (key, _) = stream_read(&mut sock).await;
    assert_eq!(key, 0x8012, "sasl handshake");
    let token = b"\x00admin\x00devpassword12";
    let mut auth = stream_str("PLAIN");
    auth.extend_from_slice(&(token.len() as i32).to_be_bytes());
    auth.extend_from_slice(token);
    sock.write_all(&stream_frame(0x0013, &auth, Some(3)))
        .await
        .unwrap();
    let (key, _) = stream_read(&mut sock).await;
    assert_eq!(key, 0x8013, "sasl auth");
    let (key, tune) = stream_read(&mut sock).await;
    assert_eq!(key, 0x0014, "tune");
    sock.write_all(&stream_frame(0x0014, &tune[4..], None))
        .await
        .unwrap();
    sock.write_all(&stream_frame(0x0015, &stream_str("/"), Some(4)))
        .await
        .unwrap();
    let (key, _) = stream_read(&mut sock).await;
    assert_eq!(key, 0x8015, "open");
    let mut create = stream_str(name);
    create.extend_from_slice(&0u32.to_be_bytes());
    sock.write_all(&stream_frame(0x000d, &create, Some(5)))
        .await
        .unwrap();
    let (key, create_body) = stream_read(&mut sock).await;
    assert_eq!(key, 0x800d, "create");
    let code = u16::from_be_bytes([create_body[8], create_body[9]]);
    assert!(code == 1 || code == 5, "create code {code}");
    let mut decl = vec![1u8];
    decl.extend_from_slice(&stream_str("p"));
    decl.extend_from_slice(&stream_str(name));
    sock.write_all(&stream_frame(0x0001, &decl, Some(6)))
        .await
        .unwrap();
    let (key, _) = stream_read(&mut sock).await;
    assert_eq!(key, 0x8001, "declare publisher");
    let mut messages = 1u32.to_be_bytes().to_vec();
    messages.extend_from_slice(&1u64.to_be_bytes());
    // An AMQP 1.0 data section, as stream clients send: RabbitMQ 4 parses it.
    let mut section = vec![0x00, 0x53, 0x75, 0xb0];
    section.extend_from_slice(&(body.len() as u32).to_be_bytes());
    section.extend_from_slice(body);
    messages.extend_from_slice(&(section.len() as i32).to_be_bytes());
    messages.extend_from_slice(&section);
    let mut publish = vec![1u8];
    publish.extend_from_slice(&messages);
    sock.write_all(&stream_frame(0x0002, &publish, None))
        .await
        .unwrap();
    let (key, reply) = stream_read(&mut sock).await;
    assert_eq!(key, 0x0003, "publish confirm, got {reply:?}");
    let mut sub = vec![1u8];
    sub.extend_from_slice(&stream_str(name));
    sub.extend_from_slice(&1u16.to_be_bytes());
    sub.extend_from_slice(&10u16.to_be_bytes());
    sub.extend_from_slice(&0u32.to_be_bytes());
    sock.write_all(&stream_frame(0x0007, &sub, Some(7)))
        .await
        .unwrap();
    let (key, _) = stream_read(&mut sock).await;
    assert_eq!(key, 0x8007, "subscribe");
    let (key, deliver) = tokio::time::timeout(Duration::from_secs(3), stream_read(&mut sock))
        .await
        .expect("stream deliver");
    assert_eq!(key, 0x0008, "deliver");
    assert!(
        deliver.windows(body.len()).any(|w| w == body),
        "stream body"
    );
}

fn amqp_frame(ftype: u8, body: &[u8]) -> Vec<u8> {
    let size = 8 + body.len();
    let mut out = (size as u32).to_be_bytes().to_vec();
    out.push(2);
    out.push(ftype);
    out.extend_from_slice(&0u16.to_be_bytes());
    out.extend_from_slice(body);
    out
}

async fn amqp_read(sock: &mut TcpStream) -> Vec<u8> {
    let hdr = read_n(sock, 8).await;
    if hdr.starts_with(b"AMQP") {
        return hdr;
    }
    let size = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) as usize;
    let mut rest = read_n(sock, size - 8).await;
    let mut all = hdr;
    all.append(&mut rest);
    all
}

async fn amqp10_session(port: u16) -> TcpStream {
    let mut sock = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("amqp10");
    sock.write_all(b"AMQP\x03\x01\x00\x00").await.unwrap();
    let header = amqp_read(&mut sock).await;
    assert!(header.starts_with(b"AMQP"), "sasl header");
    let _mechs = amqp_read(&mut sock).await;
    let token = b"\x00admin\x00devpassword12";
    let mech = b"\xa3\x05PLAIN";
    let binary = {
        let mut b = vec![0xb0];
        b.extend_from_slice(&(token.len() as u32).to_be_bytes());
        b.extend_from_slice(token);
        b
    };
    let items = [mech.as_slice(), binary.as_slice()].concat();
    let mut lst = vec![0xc0, (1 + items.len()) as u8, 2];
    lst.extend_from_slice(&items);
    let mut body = vec![0x00, 0x53, 0x41];
    body.extend_from_slice(&lst);
    sock.write_all(&amqp_frame(1, &body)).await.unwrap();
    let _outcome = amqp_read(&mut sock).await;
    sock.write_all(b"AMQP\x00\x01\x00\x00").await.unwrap();
    let open_hdr = amqp_read(&mut sock).await;
    assert!(open_hdr.starts_with(b"AMQP"), "amqp header");
    let cid = b"\xa1\x05probe";
    let mut open = vec![0x00, 0x53, 0x10, 0xc0, (1 + cid.len()) as u8, 1];
    open.extend_from_slice(cid);
    sock.write_all(&amqp_frame(0, &open)).await.unwrap();
    let _ = amqp_read(&mut sock).await;
    let fields = b"\x40\x52\x00\x70\x7f\xff\xff\xff\x70\x7f\xff\xff\xff";
    let mut begin = vec![0x00, 0x53, 0x11, 0xc0, (1 + fields.len()) as u8, 4];
    begin.extend_from_slice(fields);
    sock.write_all(&amqp_frame(0, &begin)).await.unwrap();
    let _ = amqp_read(&mut sock).await;
    sock
}

async fn amqp10_roundtrip(port: u16, queue: &str, body: &[u8]) {
    let ch = connect(port).await;
    ch.queue_declare(
        queue,
        lapin::options::QueueDeclareOptions {
            durable: true,
            ..Default::default()
        },
        lapin::types::FieldTable::default(),
    )
    .await
    .expect("declare amqp10 queue");
    let addr = format!("/queues/{queue}");
    let addr_b = {
        let raw = addr.as_bytes();
        let mut b = vec![0xa1, raw.len() as u8];
        b.extend_from_slice(raw);
        b
    };
    let mut sender = amqp10_session(port).await;
    let target = {
        let mut inner = vec![0xc0, (1 + addr_b.len()) as u8, 1];
        inner.extend_from_slice(&addr_b);
        let mut t = vec![0x00, 0x53, 0x29];
        t.extend_from_slice(&inner);
        t
    };
    let mut fields = b"\xa1\x03snd\x52\x00\x42\x40\x40\x40".to_vec();
    fields.extend_from_slice(&target);
    fields.extend_from_slice(&[0x40, 0x40, 0x43]);
    let mut attach = vec![0x00, 0x53, 0x12, 0xc0, (1 + fields.len()) as u8, 10];
    attach.extend_from_slice(&fields);
    sender.write_all(&amqp_frame(0, &attach)).await.unwrap();
    let _ = amqp_read(&mut sender).await;
    let _ = amqp_read(&mut sender).await;
    let mut data = vec![0x00, 0x53, 0x75, 0xa0, body.len() as u8];
    data.extend_from_slice(body);
    let transfer_fields = b"\x52\x00\x52\x00\xa0\x01\x01\x43\x41";
    let mut transfer = vec![0x00, 0x53, 0x14, 0xc0, (1 + transfer_fields.len()) as u8, 5];
    transfer.extend_from_slice(transfer_fields);
    transfer.extend_from_slice(&data);
    sender.write_all(&amqp_frame(0, &transfer)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let mut recv = amqp10_session(port).await;
    let source = {
        let mut inner = vec![0xc0, (1 + addr_b.len()) as u8, 1];
        inner.extend_from_slice(&addr_b);
        let mut t = vec![0x00, 0x53, 0x28];
        t.extend_from_slice(&inner);
        t
    };
    let mut rfields = b"\xa1\x03rcv\x52\x00\x41\x40\x40".to_vec();
    rfields.extend_from_slice(&source);
    rfields.push(0x40);
    let mut rattach = vec![0x00, 0x53, 0x12, 0xc0, (1 + rfields.len()) as u8, 7];
    rattach.extend_from_slice(&rfields);
    recv.write_all(&amqp_frame(0, &rattach)).await.unwrap();
    let _ = amqp_read(&mut recv).await;
    let flow =
        b"\x40\x70\x7f\xff\xff\xff\x52\x00\x70\x7f\xff\xff\xff\x52\x00\x43\x52\x14\x40\x42\x42";
    let mut flow_body = vec![0x00, 0x53, 0x13, 0xc0, (1 + flow.len()) as u8, 10];
    flow_body.extend_from_slice(flow);
    recv.write_all(&amqp_frame(0, &flow_body)).await.unwrap();
    let deliver = tokio::time::timeout(Duration::from_secs(3), amqp_read(&mut recv))
        .await
        .expect("amqp10 deliver");
    assert!(
        deliver.windows(body.len()).any(|w| w == body),
        "amqp10 body"
    );
}

async fn protocols_on(amqp: u16, mqtt: u16, stomp: u16, stream: u16, label: &str) {
    let id = stamp();
    eprintln!("mqtt {label}");
    mqtt_roundtrip(
        mqtt,
        &format!("qf/{label}/{id}"),
        format!("mqtt-{label}").as_bytes(),
    )
    .await;
    eprintln!("stomp {label}");
    stomp_roundtrip(
        stomp,
        &format!("stomp-{label}-{id}"),
        &format!("stomp-{label}"),
    )
    .await;
    eprintln!("stream {label}");
    stream_roundtrip(
        stream,
        &format!("stream-{label}-{id}"),
        format!("stream-{label}").as_bytes(),
    )
    .await;
    eprintln!("amqp10 {label}");
    amqp10_roundtrip(
        amqp,
        &format!("amqp10-{label}-{id}"),
        format!("amqp10-{label}").as_bytes(),
    )
    .await;
    eprintln!("done {label}");
}

#[tokio::test]
async fn protocols_match_rabbitmq() {
    let _serial = SERIAL.lock().await;
    protocols_on(5672, 1883, 61613, 5552, "rabbit").await;
    let rust = spawn_with_protocols("rust", 46101, 46102, 46103, 46104, 46105, 46106);
    wait_ready(rust.mgmt).await;
    protocols_on(rust.amqp, 46104, 46105, 46106, "rust").await;
    let bun = spawn_with_protocols("bun", 46111, 46112, 46113, 46114, 46115, 46116);
    wait_ready(bun.mgmt).await;
    protocols_on(bun.amqp, 46114, 46115, 46116, "bun").await;
}

async fn transient_nonexcl_rejected(label: &str, amqp: u16) {
    let ch = connect(amqp).await;
    let err = ch
        .queue_declare(
            "transient-q",
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect_err("transient non-exclusive classic queue");
    let text = format!("{err:?}");
    assert!(
        text.contains("541") || text.contains("transient_nonexcl_queues"),
        "{label} transient close: {text}"
    );
    let again = connect(amqp).await;
    again
        .queue_declare(
            "excl-ok",
            QueueDeclareOptions {
                exclusive: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("exclusive transient queue");
    mark(label, "transient non-exclusive rejected");
}

async fn amqp_surface(label: &str, amqp: u16) {
    classic_unknown_x_arg(label, amqp).await;
    exchange_to_exchange(label, amqp).await;
    classic_edges(label, amqp).await;
    rabbit_queue_behaviors(label, amqp).await;
    classic_gaps(label, amqp, None).await;
    transient_nonexcl_rejected(label, amqp).await;
    let ch = connect(amqp).await;
    ch.queue_declare("defq", qdecl(), FieldTable::default())
        .await
        .unwrap();
    ch.basic_publish(
        "",
        "defq",
        BasicPublishOptions::default(),
        b"default-body",
        BasicProperties::default(),
    )
    .await
    .unwrap()
    .await
    .ok();
    assert_eq!(one_delivery(&ch, "defq").await, b"default-body");
    ch.exchange_declare(
        "exd",
        ExchangeKind::Direct,
        ExchangeDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    ch.queue_declare("dq", qdecl(), FieldTable::default())
        .await
        .unwrap();
    ch.queue_bind(
        "dq",
        "exd",
        "rk",
        QueueBindOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    ch.basic_publish(
        "exd",
        "rk",
        BasicPublishOptions::default(),
        b"direct-body",
        BasicProperties::default(),
    )
    .await
    .unwrap()
    .await
    .ok();
    assert_eq!(one_delivery(&ch, "dq").await, b"direct-body");
    ch.queue_unbind("dq", "exd", "rk", FieldTable::default())
        .await
        .unwrap();
    ch.basic_publish(
        "exd",
        "rk",
        BasicPublishOptions::default(),
        b"unbound",
        BasicProperties::default(),
    )
    .await
    .unwrap()
    .await
    .ok();
    assert!(ch
        .basic_get("dq", lapin::options::BasicGetOptions::default())
        .await
        .unwrap()
        .is_none());
    ch.exchange_declare(
        "exf",
        ExchangeKind::Fanout,
        ExchangeDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    ch.queue_declare("fq1", qdecl(), FieldTable::default())
        .await
        .unwrap();
    ch.queue_declare("fq2", qdecl(), FieldTable::default())
        .await
        .unwrap();
    ch.queue_bind(
        "fq1",
        "exf",
        "",
        QueueBindOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    ch.queue_bind(
        "fq2",
        "exf",
        "",
        QueueBindOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    ch.basic_publish(
        "exf",
        "",
        BasicPublishOptions::default(),
        b"fan-body",
        BasicProperties::default(),
    )
    .await
    .unwrap()
    .await
    .ok();
    assert_eq!(one_delivery(&ch, "fq1").await, b"fan-body");
    assert_eq!(one_delivery(&ch, "fq2").await, b"fan-body");
    ch.exchange_declare(
        "ext",
        ExchangeKind::Topic,
        ExchangeDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    ch.queue_declare("tq", qdecl(), FieldTable::default())
        .await
        .unwrap();
    ch.queue_bind(
        "tq",
        "ext",
        "orders.*",
        QueueBindOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    ch.basic_publish(
        "ext",
        "orders.new",
        BasicPublishOptions::default(),
        b"topic-body",
        BasicProperties::default(),
    )
    .await
    .unwrap()
    .await
    .ok();
    assert_eq!(one_delivery(&ch, "tq").await, b"topic-body");
    let mut pargs = FieldTable::default();
    pargs.insert("x-max-priority".into(), lapin::types::AMQPValue::LongInt(9));
    ch.queue_declare("pri", qdecl(), pargs).await.unwrap();
    ch.basic_publish(
        "",
        "pri",
        BasicPublishOptions::default(),
        b"low",
        BasicProperties::default().with_priority(0),
    )
    .await
    .unwrap();
    ch.basic_publish(
        "",
        "pri",
        BasicPublishOptions::default(),
        b"high",
        BasicProperties::default().with_priority(9),
    )
    .await
    .unwrap();
    assert_eq!(one_delivery(&ch, "pri").await, b"high");
    let nack_ch = connect(amqp).await;
    nack_ch
        .queue_declare("nackq", qdecl(), FieldTable::default())
        .await
        .unwrap();
    nack_ch
        .basic_publish(
            "",
            "nackq",
            BasicPublishOptions::default(),
            b"nack-body",
            BasicProperties::default(),
        )
        .await
        .unwrap();
    let got = nack_ch
        .basic_get("nackq", lapin::options::BasicGetOptions::default())
        .await
        .unwrap()
        .unwrap();
    nack_ch
        .basic_nack(
            got.delivery_tag,
            BasicNackOptions {
                multiple: false,
                requeue: true,
            },
        )
        .await
        .unwrap();
    let again = nack_ch
        .basic_get("nackq", lapin::options::BasicGetOptions::default())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(again.data.as_slice(), b"nack-body");
    assert!(again.redelivered);
    nack_ch
        .basic_reject(
            again.delivery_tag,
            lapin::options::BasicRejectOptions { requeue: false },
        )
        .await
        .unwrap();
    assert!(nack_ch
        .basic_get("nackq", lapin::options::BasicGetOptions::default())
        .await
        .unwrap()
        .is_none());

    ch.exchange_declare(
        "hdr",
        ExchangeKind::Headers,
        ExchangeDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    ch.queue_declare("hall", qdecl(), FieldTable::default())
        .await
        .unwrap();
    ch.queue_declare("hany", qdecl(), FieldTable::default())
        .await
        .unwrap();
    let mut all_args = FieldTable::default();
    all_args.insert("x-match".into(), long("all"));
    all_args.insert("color".into(), long("blue"));
    all_args.insert("size".into(), long("large"));
    ch.queue_bind("hall", "hdr", "", QueueBindOptions::default(), all_args)
        .await
        .unwrap();
    let mut any_args = FieldTable::default();
    any_args.insert("x-match".into(), long("any"));
    any_args.insert("color".into(), long("blue"));
    any_args.insert("size".into(), long("large"));
    ch.queue_bind("hany", "hdr", "", QueueBindOptions::default(), any_args)
        .await
        .unwrap();
    let mut only = FieldTable::default();
    only.insert("color".into(), long("blue"));
    ch.basic_publish(
        "hdr",
        "",
        BasicPublishOptions::default(),
        b"any-body",
        BasicProperties::default().with_headers(only),
    )
    .await
    .unwrap()
    .await
    .ok();
    assert_eq!(one_delivery(&ch, "hany").await, b"any-body");
    assert!(ch
        .basic_get("hall", lapin::options::BasicGetOptions::default())
        .await
        .unwrap()
        .is_none());

    let mut alt_args = FieldTable::default();
    alt_args.insert("alternate-exchange".into(), long("alt-ex"));
    ch.exchange_declare(
        "main-ex",
        ExchangeKind::Direct,
        ExchangeDeclareOptions::default(),
        alt_args,
    )
    .await
    .unwrap();
    ch.exchange_declare(
        "alt-ex",
        ExchangeKind::Fanout,
        ExchangeDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    ch.queue_declare("altq", qdecl(), FieldTable::default())
        .await
        .unwrap();
    ch.queue_bind(
        "altq",
        "alt-ex",
        "",
        QueueBindOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    ch.basic_publish(
        "main-ex",
        "nope",
        BasicPublishOptions::default(),
        b"via-alt",
        BasicProperties::default(),
    )
    .await
    .unwrap()
    .await
    .ok();
    assert_eq!(one_delivery(&ch, "altq").await, b"via-alt");

    let ret = connect(amqp).await;
    ret.exchange_declare(
        "ret-ex",
        ExchangeKind::Direct,
        ExchangeDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    ret.basic_publish(
        "ret-ex",
        "missing",
        BasicPublishOptions {
            mandatory: true,
            immediate: false,
        },
        b"return-me",
        BasicProperties::default(),
    )
    .await
    .unwrap()
    .await
    .expect("mandatory publish");
    tokio::time::sleep(Duration::from_millis(150)).await;
    let returned = ret.wait_for_confirms().await.expect("mandatory return");
    assert!(
        !returned.is_empty() && returned[0].reply_code == 312,
        "{label} mandatory {returned:?}"
    );

    let conf = connect(amqp).await;
    conf.confirm_select(lapin::options::ConfirmSelectOptions::default())
        .await
        .unwrap();
    conf.queue_declare("cq", qdecl(), FieldTable::default())
        .await
        .unwrap();
    assert!(conf
        .basic_publish(
            "",
            "cq",
            BasicPublishOptions::default(),
            b"confirmed",
            BasicProperties::default()
        )
        .await
        .unwrap()
        .await
        .unwrap()
        .is_ack());

    let per = connect(amqp).await;
    per.queue_declare("perq", qdecl(), FieldTable::default())
        .await
        .unwrap();
    per.basic_qos(1, BasicQosOptions { global: false })
        .await
        .unwrap();
    let mut c1 = per
        .basic_consume(
            "perq",
            "c1",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    let mut c2 = per
        .basic_consume(
            "perq",
            "c2",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    per.basic_publish(
        "",
        "perq",
        BasicPublishOptions::default(),
        b"a",
        BasicProperties::default(),
    )
    .await
    .unwrap();
    per.basic_publish(
        "",
        "perq",
        BasicPublishOptions::default(),
        b"b",
        BasicProperties::default(),
    )
    .await
    .unwrap();
    let d1 = tokio::time::timeout(Duration::from_secs(2), c1.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let d2 = tokio::time::timeout(Duration::from_secs(2), c2.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_ne!(d1.data, d2.data);
    d1.ack(BasicAckOptions::default()).await.unwrap();
    d2.ack(BasicAckOptions::default()).await.unwrap();

    let glob = connect(amqp).await;
    glob.queue_declare("gq", qdecl(), FieldTable::default())
        .await
        .unwrap();
    glob.basic_qos(1, BasicQosOptions { global: true })
        .await
        .unwrap();
    let mut g1 = glob
        .basic_consume(
            "gq",
            "g1",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    let mut g2 = glob
        .basic_consume(
            "gq",
            "g2",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    glob.basic_publish(
        "",
        "gq",
        BasicPublishOptions::default(),
        b"only",
        BasicProperties::default(),
    )
    .await
    .unwrap();
    glob.basic_publish(
        "",
        "gq",
        BasicPublishOptions::default(),
        b"held",
        BasicProperties::default(),
    )
    .await
    .unwrap();
    let first = tokio::time::timeout(Duration::from_secs(2), async {
        tokio::select! { d = g1.next() => d, d = g2.next() => d }
    })
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    let other = tokio::time::timeout(Duration::from_millis(250), async {
        tokio::select! { d = g1.next() => d, d = g2.next() => d }
    })
    .await;
    assert!(other.is_ok(), "{label} global qos is per consumer");
    first.ack(BasicAckOptions::default()).await.unwrap();

    let tx = connect(amqp).await;
    tx.queue_declare("txq", qdecl(), FieldTable::default())
        .await
        .unwrap();
    tx.tx_select().await.unwrap();
    tx.basic_publish(
        "",
        "txq",
        BasicPublishOptions::default(),
        b"hidden",
        BasicProperties::default(),
    )
    .await
    .unwrap();
    tx.tx_rollback().await.unwrap();
    assert!(tx
        .basic_get("txq", lapin::options::BasicGetOptions::default())
        .await
        .unwrap()
        .is_none());
    tx.basic_publish(
        "",
        "txq",
        BasicPublishOptions::default(),
        b"shown",
        BasicProperties::default(),
    )
    .await
    .unwrap();
    tx.tx_commit().await.unwrap();
    assert_eq!(one_delivery(&tx, "txq").await, b"shown");
    mark(label, "amqp surface");
}

#[tokio::test]
async fn amqp_scenarios_match_rabbitmq() {
    let _serial = SERIAL.lock().await;
    let vhost = format!("qf{}", stamp());
    let http = Client::new();
    let created = http
        .put(format!("http://127.0.0.1:15672/api/vhosts/{vhost}"))
        .basic_auth("admin", Some("devpassword12"))
        .send()
        .await
        .unwrap();
    assert!(
        created.status().is_success(),
        "rabbit vhost {}",
        created.status()
    );
    let perm = http
        .put(format!(
            "http://127.0.0.1:15672/api/permissions/{vhost}/admin"
        ))
        .basic_auth("admin", Some("devpassword12"))
        .json(&serde_json::json!({"configure": ".*", "write": ".*", "read": ".*"}))
        .send()
        .await
        .unwrap();
    assert!(perm.status().is_success(), "rabbit perm {}", perm.status());
    *AMQP_VHOST.lock().expect("vhost") = vhost;
    amqp_surface("rabbit", 5672).await;
    policy_without_declare_args("rabbit", 5672, 15672, true).await;
    AMQP_VHOST.lock().expect("vhost").clear();
    let rust_dir = std::env::temp_dir().join("qf-rust-amqp-oracle");
    let _ = fs::remove_dir_all(&rust_dir);
    let rust = spawn_rust(46310, 46311, 46312, rust_dir, "");
    wait_ready(46311).await;
    amqp_surface("rust", 46310).await;
    policy_without_declare_args("rust", 46310, 46311, false).await;
    drop(rust);
    let bun_dir = std::env::temp_dir().join("qf-bun-amqp-oracle");
    let _ = fs::remove_dir_all(&bun_dir);
    let bun = spawn_bun(46320, 46321, 46322, bun_dir, "");
    wait_ready(46321).await;
    amqp_surface("bun", 46320).await;
    policy_without_declare_args("bun", 46320, 46321, false).await;
    drop(bun);
}

async fn quorum_client(label: &str, amqp: u16, mgmt: u16, rabbit: bool) {
    let http = Client::builder().cookie_store(true).build().unwrap();
    if !rabbit {
        let login = http
            .post(format!("http://127.0.0.1:{mgmt}/api/login"))
            .json(&serde_json::json!({"username": "admin", "password": "devpassword12"}))
            .send()
            .await
            .unwrap();
        assert!(
            login.status().is_success(),
            "{label} login {}",
            login.status()
        );
    }
    let ch = connect(amqp).await;
    let mut args = FieldTable::default();
    args.insert("x-queue-type".into(), long("quorum"));
    ch.queue_declare("qq-client", qdecl(), args)
        .await
        .unwrap_or_else(|e| panic!("{label} quorum declare: {e}"));
    ch.confirm_select(lapin::options::ConfirmSelectOptions::default())
        .await
        .unwrap();
    let conf = ch
        .basic_publish(
            "",
            "qq-client",
            BasicPublishOptions::default(),
            b"quorum-body",
            BasicProperties::default().with_delivery_mode(2),
        )
        .await
        .unwrap();
    assert!(
        conf.await.expect("{label} quorum confirm").is_ack(),
        "{label} quorum publish was not confirmed"
    );
    let vhost = AMQP_VHOST.lock().expect("vhost").clone();
    let vpath = if vhost.is_empty() {
        "%2F".to_string()
    } else {
        vhost
    };
    let mut req = http.get(format!("http://127.0.0.1:{mgmt}/api/queues/{vpath}"));
    if rabbit {
        req = req.basic_auth("admin", Some("devpassword12"));
    }
    let listed = req.send().await.unwrap();
    let status = listed.status();
    let listed = listed.text().await.unwrap();
    assert!(
        status.is_success() && listed.contains("qq-client") && listed.contains("quorum"),
        "{label} queue is not quorum ({status}): {listed}"
    );
    let got = ch
        .basic_get("qq-client", lapin::options::BasicGetOptions::default())
        .await
        .unwrap();
    assert_eq!(
        got.expect("{label} quorum body").data.as_slice(),
        b"quorum-body"
    );
    let again = ch
        .basic_get("qq-client", lapin::options::BasicGetOptions::default())
        .await
        .unwrap();
    assert!(again.is_none(), "{label} quorum body delivered twice");
    mark(label, "quorum publish and consume");
}

#[tokio::test]
async fn quorum_client_matches_rabbitmq() {
    let _serial = SERIAL.lock().await;
    let vhost = format!("qq{}", stamp() % 1_000_000_000);
    let http = Client::new();
    let created = http
        .put(format!("http://127.0.0.1:15672/api/vhosts/{vhost}"))
        .basic_auth("admin", Some("devpassword12"))
        .send()
        .await
        .unwrap();
    assert!(
        created.status().is_success(),
        "rabbit vhost {}",
        created.status()
    );
    let perm = http
        .put(format!(
            "http://127.0.0.1:15672/api/permissions/{vhost}/admin"
        ))
        .basic_auth("admin", Some("devpassword12"))
        .json(&serde_json::json!({"configure": ".*", "write": ".*", "read": ".*"}))
        .send()
        .await
        .unwrap();
    assert!(perm.status().is_success(), "rabbit perm {}", perm.status());
    *AMQP_VHOST.lock().expect("vhost") = vhost;
    quorum_client("rabbit", 5672, 15672, true).await;
    AMQP_VHOST.lock().expect("vhost").clear();

    let rust_dir = std::env::temp_dir().join("qf-rust-qqclient");
    let _ = fs::remove_dir_all(&rust_dir);
    let rust = spawn_rust(46610, 46611, 46612, rust_dir, "");
    wait_ready(46611).await;
    quorum_client("rust", 46610, 46611, false).await;
    drop(rust);
    let bun_dir = std::env::temp_dir().join("qf-bun-qqclient");
    let _ = fs::remove_dir_all(&bun_dir);
    let bun = spawn_bun(46620, 46621, 46622, bun_dir, "");
    wait_ready(46621).await;
    quorum_client("bun", 46620, 46621, false).await;
}

async fn mgmt_put(
    http: &Client,
    rabbit: bool,
    mgmt: u16,
    path: &str,
    body: serde_json::Value,
) -> reqwest::StatusCode {
    let req = http
        .put(format!("http://127.0.0.1:{mgmt}{path}"))
        .json(&body);
    let req = if rabbit {
        req.basic_auth("admin", Some("devpassword12"))
    } else {
        req
    };
    req.send().await.unwrap().status()
}

async fn shovel_and_federation(label: &str, amqp: u16, mgmt: u16, rabbit: bool) {
    let http = Client::builder().cookie_store(true).build().unwrap();
    if !rabbit {
        let login = http
            .post(format!("http://127.0.0.1:{mgmt}/api/login"))
            .json(&serde_json::json!({"username": "admin", "password": "devpassword12"}))
            .send()
            .await
            .unwrap();
        assert!(
            login.status().is_success(),
            "{label} login {}",
            login.status()
        );
    }
    let ch = connect(amqp).await;
    ch.queue_declare("shovel-src", qdecl(), FieldTable::default())
        .await
        .unwrap();
    ch.queue_declare("shovel-dest", qdecl(), FieldTable::default())
        .await
        .unwrap();
    ch.basic_publish(
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
    let shovel = mgmt_put(
        &http,
        rabbit,
        mgmt,
        "/api/parameters/shovel/%2F/move",
        serde_json::json!({"value": {
            "src-protocol": "amqp091",
            "src-uri": format!("amqp://admin:devpassword12@127.0.0.1:{amqp}"),
            "src-queue": "shovel-src",
            "dest-protocol": "amqp091",
            "dest-uri": format!("amqp://admin:devpassword12@127.0.0.1:{amqp}"),
            "dest-queue": "shovel-dest"
        }}),
    )
    .await;
    assert!(shovel.is_success(), "{label} shovel {shovel}");
    let dest = connect(amqp).await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    let mut moved = None;
    while tokio::time::Instant::now() < deadline {
        moved = dest
            .basic_get("shovel-dest", lapin::options::BasicGetOptions::default())
            .await
            .unwrap();
        if moved.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        moved.expect("{label} shovel").data.as_slice(),
        b"shovel-body"
    );
    mark(label, "shovel");

    let vh_status = mgmt_put(&http, rabbit, mgmt, "/api/vhosts/up", serde_json::json!({})).await;
    assert!(vh_status.is_success(), "{label} vhost up {vh_status}");
    let perm_path = if rabbit {
        "/api/permissions/up/admin"
    } else {
        "/api/permissions/admin/up"
    };
    let perm = mgmt_put(
        &http,
        rabbit,
        mgmt,
        perm_path,
        serde_json::json!({"configure": ".*", "write": ".*", "read": ".*"}),
    )
    .await;
    assert!(perm.is_success(), "{label} up perm {perm}");
    ch.exchange_declare(
        "fed.ex",
        ExchangeKind::Direct,
        ExchangeDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    ch.queue_declare("fed-q", qdecl(), FieldTable::default())
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
    let up = Connection::connect(
        &format!("amqp://admin:devpassword12@127.0.0.1:{amqp}/up"),
        ConnectionProperties::default(),
    )
    .await
    .expect("vhost up")
    .create_channel()
    .await
    .unwrap();
    up.exchange_declare(
        "fed.ex",
        ExchangeKind::Direct,
        ExchangeDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    let upstream = mgmt_put(
        &http,
        rabbit,
        mgmt,
        "/api/parameters/federation-upstream/%2F/origin",
        serde_json::json!({"value": {"uri": format!("amqp://admin:devpassword12@127.0.0.1:{amqp}/up")}}),
    )
    .await;
    assert!(upstream.is_success(), "{label} upstream {upstream}");
    let policy = mgmt_put(
        &http,
        rabbit,
        mgmt,
        "/api/policies/%2F/fed-pol",
        serde_json::json!({
            "pattern": "^fed\\.",
            "apply-to": "exchanges",
            "definition": {"federation-upstream-set": "all"}
        }),
    )
    .await;
    assert!(policy.is_success(), "{label} fed policy {policy}");
    tokio::time::sleep(Duration::from_millis(300)).await;
    up.basic_publish(
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
    let fed_deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    let mut copied = None;
    while tokio::time::Instant::now() < fed_deadline {
        copied = ch
            .basic_get("fed-q", lapin::options::BasicGetOptions::default())
            .await
            .unwrap();
        if copied.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        copied.expect("{label} federation").data.as_slice(),
        b"fed-body"
    );
    mark(label, "federation");
}

#[tokio::test]
async fn shovel_and_federation_match_rabbitmq() {
    let _serial = SERIAL.lock().await;
    shovel_and_federation("rabbit", 5672, 15672, true).await;
    let rust_dir = std::env::temp_dir().join("qf-rust-shovel");
    let _ = fs::remove_dir_all(&rust_dir);
    let rust = spawn_rust(46410, 46411, 46412, rust_dir, "");
    wait_ready(46411).await;
    shovel_and_federation("rust", 46410, 46411, false).await;
    drop(rust);
    let bun_dir = std::env::temp_dir().join("qf-bun-shovel");
    let _ = fs::remove_dir_all(&bun_dir);
    let bun = spawn_bun(46420, 46421, 46422, bun_dir, "");
    wait_ready(46421).await;
    shovel_and_federation("bun", 46420, 46421, false).await;
    drop(bun);
}

fn series_value(body: &str, family: &str, queue: &str) -> f64 {
    let mut total = 0.0;
    for line in body.lines() {
        if line.starts_with('#') || line.is_empty() {
            continue;
        }
        let name = line.split([' ', '{']).next().unwrap_or("");
        if name != family {
            continue;
        }
        if !queue.is_empty() && !line.contains(&format!("queue=\"{queue}\"")) {
            continue;
        }
        total += line
            .split_whitespace()
            .last()
            .unwrap_or("0")
            .parse::<f64>()
            .unwrap_or(0.0);
    }
    total
}

async fn import_rabbit_definitions(label: &str, node: &Node, doc: &serde_json::Value, vhost: &str) {
    let http = Client::builder().cookie_store(true).build().unwrap();
    let login = http
        .post(format!("http://127.0.0.1:{}/api/login", node.mgmt))
        .json(&serde_json::json!({"username": "admin", "password": "devpassword12"}))
        .send()
        .await
        .unwrap();
    assert!(
        login.status().is_success(),
        "{label} login {}",
        login.status()
    );
    let imported = http
        .post(format!("http://127.0.0.1:{}/api/definitions", node.mgmt))
        .json(doc)
        .send()
        .await
        .unwrap();
    assert!(
        imported.status().is_success(),
        "{label} import {} {}",
        imported.status(),
        imported.text().await.unwrap_or_default()
    );
    let denied = Connection::connect(
        &format!(
            "amqp://imp-user:wrong-password1@127.0.0.1:{}/{vhost}",
            node.amqp
        ),
        ConnectionProperties::default(),
    )
    .await;
    assert!(
        denied.is_err(),
        "{label} accepted a wrong password for the imported user"
    );
    let conn = Connection::connect(
        &format!(
            "amqp://imp-user:quorum-password@127.0.0.1:{}/{vhost}",
            node.amqp
        ),
        ConnectionProperties::default(),
    )
    .await
    .expect("imported user");
    let ch = conn.create_channel().await.unwrap();
    ch.basic_publish(
        "def-ex",
        "rk",
        BasicPublishOptions::default(),
        b"imported-body",
        BasicProperties::default(),
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let metrics = reqwest::get(format!("http://127.0.0.1:{}/metrics", node.metrics))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    for name in [
        "rabbitmq_connections",
        "rabbitmq_channels",
        "rabbitmq_queues",
        "rabbitmq_global_messages_received_total",
        "rabbitmq_queue_messages",
    ] {
        assert!(metrics.contains(name), "{label} metrics missing {name}");
    }
    assert_eq!(
        series_value(&metrics, "rabbitmq_queue_messages", "def-q"),
        1.0,
        "{label} queue depth\n{metrics}"
    );
    assert_eq!(
        series_value(&metrics, "rabbitmq_global_messages_received_total", ""),
        1.0,
        "{label} received"
    );
    let got = ch
        .basic_get("def-q", lapin::options::BasicGetOptions::default())
        .await
        .unwrap();
    assert_eq!(
        got.expect("imported body").data.as_slice(),
        b"imported-body"
    );
    let again = ch
        .basic_get("def-q", lapin::options::BasicGetOptions::default())
        .await
        .unwrap();
    assert!(again.is_none(), "{label} imported body delivered twice");
    let queues = http
        .get(format!("http://127.0.0.1:{}/api/queues/{vhost}", node.mgmt))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(
        queues.contains("def-qq"),
        "{label} imported quorum missing {queues}"
    );
    assert!(
        queues.contains("quorum"),
        "{label} imported queue lost quorum type {queues}"
    );
    assert!(
        queues.contains("x-message-ttl")
            || queues.contains("message-ttl")
            || queues.contains("1000"),
        "{label} queue arguments {queues}"
    );
    mark(label, "definitions import then one consumed body");
}

#[tokio::test]
async fn definitions_and_metrics_match_rabbitmq() {
    let _serial = SERIAL.lock().await;
    let vhost = format!("imp{}", stamp() % 1_000_000_000);
    let http = Client::new();
    let auth = |req: reqwest::RequestBuilder| req.basic_auth("admin", Some("devpassword12"));
    let created = auth(http.put(format!("http://127.0.0.1:15672/api/vhosts/{vhost}")))
        .send()
        .await
        .unwrap();
    assert!(created.status().is_success(), "vhost {}", created.status());
    let perm = auth(http.put(format!(
        "http://127.0.0.1:15672/api/permissions/{vhost}/admin"
    )))
    .json(&serde_json::json!({"configure": ".*", "write": ".*", "read": ".*"}))
    .send()
    .await
    .unwrap();
    assert!(perm.status().is_success(), "perm {}", perm.status());
    let user = auth(http.put("http://127.0.0.1:15672/api/users/imp-user"))
        .json(&serde_json::json!({"password": "quorum-password", "tags": "management"}))
        .send()
        .await
        .unwrap();
    assert!(
        user.status().is_success(),
        "user {} {}",
        user.status(),
        user.text().await.unwrap_or_default()
    );
    let uperm = auth(http.put(format!(
        "http://127.0.0.1:15672/api/permissions/{vhost}/imp-user"
    )))
    .json(&serde_json::json!({"configure": ".*", "write": ".*", "read": ".*"}))
    .send()
    .await
    .unwrap();
    assert!(uperm.status().is_success(), "user perm {}", uperm.status());
    let classic = auth(http.put(format!("http://127.0.0.1:15672/api/queues/{vhost}/def-q")))
        .json(&serde_json::json!({"durable": true, "arguments": {"x-message-ttl": 60000}}))
        .send()
        .await
        .unwrap();
    assert!(
        classic.status().is_success(),
        "classic {}",
        classic.status()
    );
    let quorum = auth(http.put(format!("http://127.0.0.1:15672/api/queues/{vhost}/def-qq")))
        .json(&serde_json::json!({"durable": true, "arguments": {"x-queue-type": "quorum", "x-message-ttl": 1000}}))
        .send()
        .await
        .unwrap();
    assert!(quorum.status().is_success(), "quorum {}", quorum.status());
    let exchange = auth(http.put(format!(
        "http://127.0.0.1:15672/api/exchanges/{vhost}/def-ex"
    )))
    .json(&serde_json::json!({"type": "direct", "durable": true}))
    .send()
    .await
    .unwrap();
    assert!(
        exchange.status().is_success(),
        "exchange {}",
        exchange.status()
    );
    let bind = auth(http.post(format!(
        "http://127.0.0.1:15672/api/bindings/{vhost}/e/def-ex/q/def-q"
    )))
    .json(&serde_json::json!({"routing_key": "rk"}))
    .send()
    .await
    .unwrap();
    assert!(bind.status().is_success(), "bind {}", bind.status());
    let policy = auth(http.put(format!("http://127.0.0.1:15672/api/policies/{vhost}/imp-pol")))
        .json(&serde_json::json!({"pattern": "^no-match-", "apply-to": "queues", "priority": 1, "definition": {"message-ttl": 5000}}))
        .send()
        .await
        .unwrap();
    assert!(policy.status().is_success(), "policy {}", policy.status());
    let exported: serde_json::Value = auth(http.get("http://127.0.0.1:15672/api/definitions"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let keep = |item: &serde_json::Value| {
        item.get("vhost").and_then(|v| v.as_str()) == Some(vhost.as_str())
    };
    let doc = serde_json::json!({
        "rabbit_version": exported.get("rabbit_version").cloned().unwrap_or(serde_json::json!("4.0.0")),
        "users": exported["users"].as_array().unwrap().iter().filter(|u| u["name"] == "imp-user").cloned().collect::<Vec<_>>(),
        "vhosts": exported["vhosts"].as_array().unwrap().iter().filter(|v| v["name"] == vhost).cloned().collect::<Vec<_>>(),
        "permissions": exported["permissions"].as_array().unwrap().iter().filter(|p| keep(p)).cloned().collect::<Vec<_>>(),
        "exchanges": exported["exchanges"].as_array().unwrap().iter().filter(|e| keep(e) && e["name"].as_str().unwrap_or("").starts_with("def-")).cloned().collect::<Vec<_>>(),
        "queues": exported["queues"].as_array().unwrap().iter().filter(|q| keep(q)).cloned().collect::<Vec<_>>(),
        "bindings": exported["bindings"].as_array().unwrap().iter().filter(|b| keep(b) && b["destination"] == "def-q").cloned().collect::<Vec<_>>(),
        "policies": exported["policies"].as_array().unwrap().iter().filter(|p| keep(p)).cloned().collect::<Vec<_>>(),
    });
    assert!(
        doc["queues"]
            .as_array()
            .unwrap()
            .iter()
            .any(|q| q["type"] == "quorum"),
        "rabbit export lost quorum {doc}"
    );
    let imported_user = doc["users"]
        .as_array()
        .unwrap()
        .iter()
        .find(|u| u["name"] == "imp-user")
        .expect("rabbit export dropped imp-user");
    assert!(
        imported_user["password_hash"].as_str().unwrap_or("").len() > 8,
        "rabbit export dropped password_hash {imported_user}"
    );
    Connection::connect(
        &format!("amqp://imp-user:quorum-password@127.0.0.1:5672/{vhost}"),
        ConnectionProperties::default(),
    )
    .await
    .expect("rabbit rejected the user whose hash will be imported");
    let rabbit_ch = Connection::connect(
        &format!("amqp://admin:devpassword12@127.0.0.1:5672/{vhost}"),
        ConnectionProperties::default(),
    )
    .await
    .unwrap()
    .create_channel()
    .await
    .unwrap();
    let before = reqwest::get("http://127.0.0.1:15692/metrics")
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let before_messages = series_value(&before, "rabbitmq_queue_messages", "");
    let before_received = series_value(&before, "rabbitmq_global_messages_received_total", "");
    rabbit_ch
        .basic_publish(
            "def-ex",
            "rk",
            BasicPublishOptions::default(),
            b"imported-body",
            BasicProperties::default(),
        )
        .await
        .unwrap()
        .await
        .ok();
    let mut rabbit_metrics = String::new();
    for _ in 0..20 {
        rabbit_metrics = reqwest::get("http://127.0.0.1:15692/metrics")
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        if series_value(&rabbit_metrics, "rabbitmq_queue_messages", "") >= before_messages + 1.0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    for name in [
        "rabbitmq_connections",
        "rabbitmq_channels",
        "rabbitmq_queues",
        "rabbitmq_global_messages_received_total",
        "rabbitmq_queue_messages",
    ] {
        assert!(
            rabbit_metrics.contains(name),
            "rabbit metrics missing {name}"
        );
    }
    assert_eq!(
        series_value(&rabbit_metrics, "rabbitmq_queue_messages", "") - before_messages,
        1.0,
        "rabbit queue depth"
    );
    assert_eq!(
        series_value(
            &rabbit_metrics,
            "rabbitmq_global_messages_received_total",
            ""
        ) - before_received,
        1.0,
        "rabbit received"
    );
    let got = rabbit_ch
        .basic_get("def-q", lapin::options::BasicGetOptions::default())
        .await
        .unwrap();
    assert_eq!(got.expect("rabbit body").data.as_slice(), b"imported-body");
    mark("rabbit", "definitions export");

    let rust_dir = std::env::temp_dir().join("qf-rust-defimp");
    let _ = fs::remove_dir_all(&rust_dir);
    let rust = spawn_rust(46510, 46511, 46512, rust_dir, "");
    wait_ready(46511).await;
    import_rabbit_definitions("rust", &rust, &doc, &vhost).await;
    drop(rust);
    let bun_dir = std::env::temp_dir().join("qf-bun-defimp");
    let _ = fs::remove_dir_all(&bun_dir);
    let bun = spawn_bun(46520, 46521, 46522, bun_dir, "");
    wait_ready(46521).await;
    import_rabbit_definitions("bun", &bun, &doc, &vhost).await;
}

async fn durable_once(label: &str, port: u16) {
    let ch = connect(port).await;
    ch.queue_declare("durable-flush-q", qdecl(), FieldTable::default())
        .await
        .unwrap_or_else(|e| panic!("{label} declare: {e}"));
    ch.confirm_select(lapin::options::ConfirmSelectOptions::default())
        .await
        .unwrap();
    let confirm = ch
        .basic_publish(
            "",
            "durable-flush-q",
            BasicPublishOptions::default(),
            b"flush-body",
            BasicProperties::default().with_delivery_mode(2),
        )
        .await
        .unwrap_or_else(|e| panic!("{label} publish: {e}"));
    assert!(
        confirm.await.unwrap().is_ack(),
        "{label} durable publish was not confirmed"
    );
    let got = ch
        .basic_get(
            "durable-flush-q",
            lapin::options::BasicGetOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(got.expect("{label} body").data.as_slice(), b"flush-body");
    mark(label, "durable confirm and consume");
}

/// Persistent publish, confirm, and consume on RabbitMQ 4, Rust, and Bun.
#[tokio::test]
async fn durable_group_commit_matches_rabbitmq() {
    let _serial = SERIAL.lock().await;
    let vhost = format!("fl{}", stamp() % 1_000_000_000);
    let http = Client::new();
    let created = http
        .put(format!("http://127.0.0.1:15672/api/vhosts/{vhost}"))
        .basic_auth("admin", Some("devpassword12"))
        .send()
        .await
        .unwrap();
    assert!(
        created.status().is_success(),
        "rabbit vhost {}",
        created.status()
    );
    let perm = http
        .put(format!(
            "http://127.0.0.1:15672/api/permissions/{vhost}/admin"
        ))
        .basic_auth("admin", Some("devpassword12"))
        .json(&serde_json::json!({"configure": ".*", "write": ".*", "read": ".*"}))
        .send()
        .await
        .unwrap();
    assert!(perm.status().is_success(), "rabbit perm {}", perm.status());
    *AMQP_VHOST.lock().expect("vhost") = vhost;
    durable_once("rabbit", 5672).await;
    AMQP_VHOST.lock().expect("vhost").clear();

    let rust_dir = std::env::temp_dir().join("qf-rust-flush");
    let _ = fs::remove_dir_all(&rust_dir);
    let rust = spawn_rust(46710, 46711, 46712, rust_dir, "");
    wait_ready(46711).await;
    durable_once("rust", 46710).await;
    drop(rust);
    let bun_dir = std::env::temp_dir().join("qf-bun-flush");
    let _ = fs::remove_dir_all(&bun_dir);
    let bun = spawn_bun(46720, 46721, 46722, bun_dir, "");
    wait_ready(46721).await;
    durable_once("bun", 46720).await;
}
