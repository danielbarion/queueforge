//! Engine-local persistence of the RabbitMQ 4.3 plugin features: a broker
//! process is killed (SIGKILL, no graceful shutdown) and started again on the
//! same data directory. Delayed messages, MQTT retained messages, dynamic
//! shovels and federation links must all come back. Also covers shovels
//! with AMQP 1.0 endpoints and a stream source.

use std::fs;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use lapin::options::{
    BasicGetOptions, BasicPublishOptions, ConfirmSelectOptions, ExchangeDeclareOptions, QueueBindOptions,
    QueueDeclareOptions,
};
use lapin::types::{AMQPValue, FieldTable, LongString};
use lapin::{BasicProperties, Channel, Connection, ConnectionProperties, ExchangeKind};
use reqwest::Client;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

struct Broker {
    child: Option<Child>,
    data: PathBuf,
    amqp: u16,
    mgmt: u16,
    metrics: u16,
    mqtt: u16,
}

impl Broker {
    fn start(base: u16, name: &str) -> Self {
        let data = std::env::temp_dir().join(format!("qf-persist-{name}-{base}"));
        let _ = fs::remove_dir_all(&data);
        fs::create_dir_all(&data).unwrap();
        let mut b = Broker { child: None, data, amqp: base, mgmt: base + 1, metrics: base + 2, mqtt: base + 3 };
        b.spawn();
        b
    }

    fn spawn(&mut self) {
        let cfg = self.data.join("qf.toml");
        fs::write(
            &cfg,
            format!(
                r#"[listeners]
amqp = "127.0.0.1:{}"
management = "127.0.0.1:{}"
metrics = "127.0.0.1:{}"
mqtt = "127.0.0.1:{}"
[data]
dir = "{}"
fsync_policy = "every_n_ms"
fsync_interval_ms = 10
[logging]
level = "warn"
"#,
                self.amqp,
                self.mgmt,
                self.metrics,
                self.mqtt,
                self.data.join("store").display()
            ),
        )
        .unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_queueforge"))
            .arg("--config")
            .arg(&cfg)
            .arg("--dev-bootstrap")
            .spawn()
            .expect("spawn queueforge");
        self.child = Some(child);
    }

    /// Kill without a graceful shutdown, then start on the same data.
    async fn crash_and_restart(&mut self) {
        if let Some(mut c) = self.child.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        self.spawn();
        self.ready().await;
    }

    async fn ready(&self) {
        let url = format!("http://127.0.0.1:{}/readyz", self.mgmt);
        let client = Client::new();
        for _ in 0..200 {
            if let Ok(res) = client.get(&url).send().await {
                if res.status().is_success() {
                    // The AMQP listener binds just after readiness.
                    if TcpStream::connect(("127.0.0.1", self.amqp)).await.is_ok() {
                        return;
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("broker on {} did not become ready", self.mgmt);
    }

    async fn channel(&self) -> (Connection, Channel) {
        let conn = Connection::connect(
            &format!("amqp://admin:devpassword12@127.0.0.1:{}/%2f", self.amqp),
            ConnectionProperties::default(),
        )
        .await
        .expect("connect");
        let ch = conn.create_channel().await.unwrap();
        ch.confirm_select(ConfirmSelectOptions::default()).await.unwrap();
        (conn, ch)
    }

    async fn http(&self) -> Client {
        let client = Client::builder().cookie_store(true).build().unwrap();
        let res = client
            .post(format!("http://127.0.0.1:{}/api/login", self.mgmt))
            .json(&json!({"username": "admin", "password": "devpassword12"}))
            .send()
            .await
            .unwrap();
        assert!(res.status().is_success(), "login {}", res.status());
        client
    }

    async fn put(&self, path: &str, body: Value) {
        let res = self.http().await.put(format!("http://127.0.0.1:{}{path}", self.mgmt)).json(&body).send().await.unwrap();
        let status = res.status();
        assert!(status.is_success(), "PUT {path} -> {status} {}", res.text().await.unwrap_or_default());
    }

    async fn get(&self, path: &str) -> Value {
        self.http().await.get(format!("http://127.0.0.1:{}{path}", self.mgmt)).send().await.unwrap().json().await.unwrap()
    }
}

impl Drop for Broker {
    fn drop(&mut self) {
        if let Some(mut c) = self.child.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
        let _ = fs::remove_dir_all(&self.data);
    }
}

fn durable() -> QueueDeclareOptions {
    QueueDeclareOptions { durable: true, ..Default::default() }
}

async fn publish(ch: &Channel, exchange: &str, key: &str, body: &[u8], headers: FieldTable) {
    ch.basic_publish(exchange, key, BasicPublishOptions::default(), body, BasicProperties::default().with_delivery_mode(2).with_headers(headers))
        .await
        .unwrap()
        .await
        .unwrap();
}

/// Poll `queue` until a message arrives or `within` passes.
async fn get_body(ch: &Channel, queue: &str, within: Duration) -> Option<Vec<u8>> {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        if let Ok(Some(m)) = ch.basic_get(queue, BasicGetOptions { no_ack: true }).await {
            return Some(m.delivery.data);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    None
}

#[tokio::test]
async fn delayed_messages_survive_a_crash() {
    let mut b = Broker::start(31200, "delayed");
    b.ready().await;
    let (_c, ch) = b.channel().await;
    let mut args = FieldTable::default();
    args.insert("x-delayed-type".into(), AMQPValue::LongString(LongString::from(b"direct".to_vec())));
    let opts = ExchangeDeclareOptions { durable: true, ..Default::default() };
    ch.exchange_declare("dx", ExchangeKind::Custom("x-delayed-message".into()), opts, args).await.unwrap();
    ch.queue_declare("delayed-q", durable(), FieldTable::default()).await.unwrap();
    ch.queue_bind("delayed-q", "dx", "k", QueueBindOptions::default(), FieldTable::default()).await.unwrap();
    let mut headers = FieldTable::default();
    headers.insert("x-delay".into(), AMQPValue::LongInt(2500));
    publish(&ch, "dx", "k", b"held", headers).await;
    assert!(get_body(&ch, "delayed-q", Duration::from_millis(300)).await.is_none(), "delivered before its delay");
    drop(ch);
    b.crash_and_restart().await;
    let (_c, ch) = b.channel().await;
    assert_eq!(get_body(&ch, "delayed-q", Duration::from_secs(8)).await.as_deref(), Some(&b"held"[..]));
    // Routed once: the stored row is gone. Delivery is at least once, so a
    // kill before the row delete and the get are on disk may route it again;
    // give both a moment first.
    tokio::time::sleep(Duration::from_millis(500)).await;
    b.crash_and_restart().await;
    let (_c, ch) = b.channel().await;
    assert!(get_body(&ch, "delayed-q", Duration::from_secs(1)).await.is_none(), "delivered twice");
}

fn mqtt_str(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(&(s.len() as u16).to_be_bytes());
    out.extend_from_slice(s.as_bytes());
}

fn mqtt_packet(first: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![first];
    let mut n = body.len();
    loop {
        let mut byte = (n % 128) as u8;
        n /= 128;
        if n > 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if n == 0 {
            break;
        }
    }
    out.extend_from_slice(body);
    out
}

async fn mqtt_connect(port: u16, client_id: &str) -> TcpStream {
    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let mut body = Vec::new();
    mqtt_str(&mut body, "MQTT");
    body.extend_from_slice(&[4, 0xc2, 0, 60]);
    mqtt_str(&mut body, client_id);
    mqtt_str(&mut body, "admin");
    mqtt_str(&mut body, "devpassword12");
    s.write_all(&mqtt_packet(0x10, &body)).await.unwrap();
    let mut ack = [0u8; 4];
    s.read_exact(&mut ack).await.unwrap();
    assert_eq!(ack, [0x20, 2, 0, 0], "CONNACK");
    s
}

#[tokio::test]
async fn mqtt_retained_messages_survive_a_crash() {
    let mut b = Broker::start(31220, "retained");
    b.ready().await;
    let mut pubc = mqtt_connect(b.mqtt, "persist-pub").await;
    let mut body = Vec::new();
    mqtt_str(&mut body, "home/door");
    body.extend_from_slice(&[0, 1]);
    body.extend_from_slice(b"locked");
    // QoS 1, retain.
    pubc.write_all(&mqtt_packet(0x33, &body)).await.unwrap();
    let mut puback = [0u8; 4];
    pubc.read_exact(&mut puback).await.unwrap();
    assert_eq!(puback[0], 0x40);
    drop(pubc);
    // The store writer runs off the session; give it a moment before the kill.
    tokio::time::sleep(Duration::from_millis(300)).await;
    b.crash_and_restart().await;
    let mut sub = mqtt_connect(b.mqtt, "persist-sub").await;
    let mut body = vec![0, 7];
    mqtt_str(&mut body, "home/door");
    body.push(0);
    sub.write_all(&mqtt_packet(0x82, &body)).await.unwrap();
    let mut got = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut buf = [0u8; 1024];
    while Instant::now() < deadline && !got.windows(6).any(|w| w == b"locked") {
        match tokio::time::timeout(Duration::from_millis(500), sub.read(&mut buf)).await {
            Ok(Ok(n)) if n > 0 => got.extend_from_slice(&buf[..n]),
            _ => {}
        }
    }
    let at = got.iter().position(|b| b >> 4 == 3).expect("a PUBLISH after the restart");
    assert_eq!(got[at] & 1, 1, "sent as retained");
    assert!(got.windows(6).any(|w| w == b"locked"));
}

#[tokio::test]
async fn shovels_survive_a_crash() {
    let mut b = Broker::start(31240, "shovel");
    b.ready().await;
    let (_c, ch) = b.channel().await;
    for q in ["shovel-in", "shovel-out"] {
        ch.queue_declare(q, durable(), FieldTable::default()).await.unwrap();
    }
    b.put(
        "/api/parameters/shovel/%2F/mover",
        json!({"value": {"src-protocol": "amqp091", "src-uri": "amqp://", "src-queue": "shovel-in",
                         "dest-protocol": "amqp091", "dest-uri": "amqp://", "dest-queue": "shovel-out"}}),
    )
    .await;
    publish(&ch, "", "shovel-in", b"before", FieldTable::default()).await;
    assert_eq!(get_body(&ch, "shovel-out", Duration::from_secs(5)).await.as_deref(), Some(&b"before"[..]));
    drop(ch);
    b.crash_and_restart().await;
    let (_c, ch) = b.channel().await;
    publish(&ch, "", "shovel-in", b"after", FieldTable::default()).await;
    // At least once: a copy moved just before the kill may come again first.
    let mut seen = Vec::new();
    while let Some(body) = get_body(&ch, "shovel-out", Duration::from_secs(8)).await {
        let done = body == b"after";
        seen.push(body);
        if done {
            break;
        }
    }
    assert_eq!(seen.last().map(Vec::as_slice), Some(&b"after"[..]), "{seen:?}");
    let listed = b.get("/api/shovels/%2F").await;
    let row = listed.as_array().unwrap().iter().find(|r| r["name"] == "mover").expect("listed");
    assert_eq!(row["type"], "dynamic");
    assert_eq!(row["state"], "running");
}

#[tokio::test]
async fn federation_links_survive_a_crash() {
    let mut b = Broker::start(31260, "federation");
    b.ready().await;
    let (_c, ch) = b.channel().await;
    let opts = ExchangeDeclareOptions { durable: true, ..Default::default() };
    ch.exchange_declare("fed-up", ExchangeKind::Topic, opts, FieldTable::default()).await.unwrap();
    ch.exchange_declare("fed-down", ExchangeKind::Topic, opts, FieldTable::default()).await.unwrap();
    ch.queue_declare("fed-sink", durable(), FieldTable::default()).await.unwrap();
    ch.queue_bind("fed-sink", "fed-down", "a.#", QueueBindOptions::default(), FieldTable::default()).await.unwrap();
    b.put("/api/parameters/federation-upstream/%2F/origin", json!({"value": {"uri": "amqp://", "exchange": "fed-up"}})).await;
    b.put(
        "/api/policies/%2F/fed",
        json!({"pattern": "^fed-down$", "apply-to": "exchanges", "definition": {"federation-upstream": "origin"}}),
    )
    .await;
    let flows = |ch: Channel| async move {
        for _ in 0..80 {
            publish(&ch, "fed-up", "a.b", b"federated", FieldTable::default()).await;
            if let Some(body) = get_body(&ch, "fed-sink", Duration::from_millis(150)).await {
                return body;
            }
        }
        panic!("nothing federated");
    };
    assert_eq!(flows(ch.clone()).await, b"federated");
    drop(ch);
    b.crash_and_restart().await;
    let (_c, ch) = b.channel().await;
    // Drain copies sent just before the crash, then check the link again.
    while get_body(&ch, "fed-sink", Duration::from_millis(100)).await.is_some() {}
    assert_eq!(flows(ch.clone()).await, b"federated");
    let links = b.get("/api/federation-links/%2F").await;
    let link = links.as_array().unwrap().iter().find(|l| l["upstream"] == "origin").expect("link listed");
    assert_eq!(link["type"], "exchange");
    assert_eq!(link["exchange"], "fed-down");
    assert_eq!(link["upstream_exchange"], "fed-up");
    assert_eq!(link["status"], "running");
    // Routing keys the downstream does not bind stay upstream.
    while get_body(&ch, "fed-sink", Duration::from_millis(200)).await.is_some() {}
    publish(&ch, "fed-up", "z.z", b"unbound", FieldTable::default()).await;
    assert!(get_body(&ch, "fed-sink", Duration::from_millis(600)).await.is_none());
}

#[tokio::test]
async fn shovels_move_messages_over_amqp_1_0_and_from_a_stream() {
    let b = Broker::start(31280, "shovel-amqp10");
    b.ready().await;
    let (_c, ch) = b.channel().await;
    for q in ["a10-in", "a10-mid", "a10-out"] {
        ch.queue_declare(q, durable(), FieldTable::default()).await.unwrap();
    }
    let mut stream = FieldTable::default();
    stream.insert("x-queue-type".into(), AMQPValue::LongString(LongString::from(b"stream".to_vec())));
    ch.queue_declare("a10-stream", durable(), stream).await.unwrap();
    // AMQP 1.0 source, 0-9-1 destination; then 0-9-1 source, 1.0 destination.
    b.put(
        "/api/parameters/shovel/%2F/from-a10",
        json!({"value": {"src-protocol": "amqp10", "src-uri": "amqp://", "src-address": "/queues/a10-in",
                         "dest-protocol": "amqp091", "dest-uri": "amqp://", "dest-queue": "a10-mid"}}),
    )
    .await;
    b.put(
        "/api/parameters/shovel/%2F/to-a10",
        json!({"value": {"src-protocol": "amqp091", "src-uri": "amqp://", "src-queue": "a10-mid",
                         "dest-protocol": "amqp10", "dest-uri": "amqp://", "dest-address": "a10-out"}}),
    )
    .await;
    let mut headers = FieldTable::default();
    headers.insert("trace".into(), AMQPValue::LongString(LongString::from(b"kept".to_vec())));
    publish(&ch, "", "a10-in", b"over-1.0", headers).await;
    let deadline = Instant::now() + Duration::from_secs(8);
    let mut got = None;
    while Instant::now() < deadline && got.is_none() {
        if let Ok(Some(m)) = ch.basic_get("a10-out", BasicGetOptions { no_ack: true }).await {
            got = Some(m);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let m = got.expect("moved through both shovels");
    assert_eq!(m.delivery.data, b"over-1.0");
    let trace = m.delivery.properties.headers().as_ref().and_then(|h| h.inner().get("trace").cloned());
    assert!(matches!(trace, Some(AMQPValue::LongString(s)) if s.as_bytes() == b"kept"), "header lost");
    // A stream source starts where src-consumer-args says.
    publish(&ch, "", "a10-stream", b"s1", FieldTable::default()).await;
    publish(&ch, "", "a10-stream", b"s2", FieldTable::default()).await;
    b.put(
        "/api/parameters/shovel/%2F/from-stream",
        json!({"value": {"src-protocol": "amqp091", "src-uri": "amqp://", "src-queue": "a10-stream",
                         "src-consumer-args": {"x-stream-offset": "first"}, "src-prefetch-count": 10,
                         "dest-protocol": "amqp091", "dest-uri": "amqp://", "dest-queue": "a10-out"}}),
    )
    .await;
    assert_eq!(get_body(&ch, "a10-out", Duration::from_secs(8)).await.as_deref(), Some(&b"s1"[..]));
    assert_eq!(get_body(&ch, "a10-out", Duration::from_secs(4)).await.as_deref(), Some(&b"s2"[..]));
    let listed = b.get("/api/shovels").await;
    let names: Vec<&str> = listed.as_array().unwrap().iter().filter_map(|r| r["name"].as_str()).collect();
    assert_eq!(names, ["from-a10", "from-stream", "to-a10"]);
}

