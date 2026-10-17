//! RabbitMQ 4.3 parity, round 2: shovel and federation links over TLS
//! (`amqps://` with RabbitMQ's URI query parameters), shovel
//! `src-delete-after = "queue-length"`, federation `trust-user-id`, and the
//! MQTT 5 DISCONNECT packets a server sends. Each test runs its own broker
//! process with an AMQPS listener on a certificate made with openssl.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use lapin::options::{BasicGetOptions, BasicPublishOptions, ConfirmSelectOptions, ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions};
use lapin::types::{FieldTable, ShortString};
use lapin::{BasicProperties, Channel, Connection, ConnectionProperties, ExchangeKind};
use reqwest::Client;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

fn openssl(dir: &Path, args: &[&str]) {
    let out = Command::new("openssl").args(args).current_dir(dir).output().expect("run openssl");
    assert!(out.status.success(), "openssl {args:?}: {}", String::from_utf8_lossy(&out.stderr));
}

/// A CA, a server certificate for localhost / 127.0.0.1 and a client certificate.
fn make_pki(dir: &Path) {
    fs::write(dir.join("san.ext"), "subjectAltName=DNS:localhost,IP:127.0.0.1\n").unwrap();
    openssl(dir, &["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", "ca.key", "-out", "ca.pem", "-days", "2", "-subj", "/CN=qf-link-ca"]);
    openssl(dir, &["req", "-newkey", "rsa:2048", "-nodes", "-keyout", "server.key", "-out", "server.csr", "-subj", "/CN=localhost"]);
    openssl(
        dir,
        &["x509", "-req", "-in", "server.csr", "-CA", "ca.pem", "-CAkey", "ca.key", "-CAcreateserial", "-out", "server.pem", "-days", "2", "-extfile", "san.ext"],
    );
    openssl(dir, &["req", "-newkey", "rsa:2048", "-nodes", "-keyout", "client.key", "-out", "client.csr", "-subj", "/CN=admin"]);
    openssl(dir, &["x509", "-req", "-in", "client.csr", "-CA", "ca.pem", "-CAkey", "ca.key", "-CAcreateserial", "-out", "client.pem", "-days", "2"]);
}

struct Broker {
    child: Option<Child>,
    data: PathBuf,
    amqp: u16,
    amqps: u16,
    mgmt: u16,
    mqtt: u16,
}

impl Broker {
    async fn start(base: u16, name: &str) -> Self {
        let data = std::env::temp_dir().join(format!("qf-round2-{name}-{base}"));
        let _ = fs::remove_dir_all(&data);
        fs::create_dir_all(&data).unwrap();
        make_pki(&data);
        let (amqp, amqps, mgmt, metrics, mqtt) = (base, base + 1, base + 2, base + 3, base + 4);
        let cfg = data.join("qf.toml");
        fs::write(
            &cfg,
            format!(
                r#"[listeners]
amqp = "127.0.0.1:{amqp}"
amqps = "127.0.0.1:{amqps}"
management = "127.0.0.1:{mgmt}"
metrics = "127.0.0.1:{metrics}"
mqtt = "127.0.0.1:{mqtt}"
[data]
dir = "{store}"
fsync_policy = "every_n_ms"
fsync_interval_ms = 10
[logging]
level = "warn"
[tls]
enabled = false
cert_path = "{pki}/server.pem"
key_path = "{pki}/server.key"
ca_path = "{pki}/ca.pem"
"#,
                store = data.join("store").display(),
                pki = data.display(),
            ),
        )
        .unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_queueforge"))
            .arg("--config")
            .arg(&cfg)
            .arg("--dev-bootstrap")
            .spawn()
            .expect("spawn queueforge");
        let b = Broker { child: Some(child), data, amqp, amqps, mgmt, mqtt };
        b.ready().await;
        b
    }

    fn pki(&self, file: &str) -> String {
        self.data.join(file).display().to_string()
    }

    async fn ready(&self) {
        let url = format!("http://127.0.0.1:{}/readyz", self.mgmt);
        let client = Client::new();
        for _ in 0..300 {
            if let Ok(res) = client.get(&url).send().await {
                if res.status().is_success()
                    && TcpStream::connect(("127.0.0.1", self.amqp)).await.is_ok()
                    && TcpStream::connect(("127.0.0.1", self.amqps)).await.is_ok()
                {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("broker on {} did not become ready", self.mgmt);
    }

    async fn channel_as(&self, user: &str, pass: &str) -> (Connection, Channel) {
        let conn = Connection::connect(&format!("amqp://{user}:{pass}@127.0.0.1:{}/%2f", self.amqp), ConnectionProperties::default())
            .await
            .expect("connect");
        let ch = conn.create_channel().await.unwrap();
        ch.confirm_select(ConfirmSelectOptions::default()).await.unwrap();
        (conn, ch)
    }

    async fn channel(&self) -> (Connection, Channel) {
        self.channel_as("admin", "devpassword12").await
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

    async fn get(&self, path: &str) -> (u16, Value) {
        let res = self.http().await.get(format!("http://127.0.0.1:{}{path}", self.mgmt)).send().await.unwrap();
        let status = res.status().as_u16();
        (status, res.json().await.unwrap_or(Value::Null))
    }

    async fn shovel(&self, name: &str) -> Option<Value> {
        let (_, all) = self.get("/api/shovels/%2F").await;
        all.as_array()?.iter().find(|r| r["name"] == name).cloned()
    }

    fn sigterm(&self) {
        if let Some(c) = &self.child {
            let _ = Command::new("kill").arg("-TERM").arg(c.id().to_string()).status();
        }
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

async fn publish(ch: &Channel, exchange: &str, key: &str, body: &[u8], props: BasicProperties) {
    ch.basic_publish(exchange, key, BasicPublishOptions::default(), body, props).await.unwrap().await.unwrap();
}

async fn get(ch: &Channel, queue: &str, within: Duration) -> Option<lapin::message::BasicGetMessage> {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        if let Ok(Some(m)) = ch.basic_get(queue, BasicGetOptions { no_ack: true }).await {
            return Some(m);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    None
}

async fn get_body(ch: &Channel, queue: &str, within: Duration) -> Option<Vec<u8>> {
    get(ch, queue, within).await.map(|m| m.delivery.data)
}

/// A shovel from a local queue to `dest` (a full URI) and its destination queue.
async fn shovel_to(b: &Broker, name: &str, protocol: &str, dest: String) {
    let mut value = json!({"src-protocol": "amqp091", "src-uri": "amqp://", "src-queue": format!("{name}-in"),
                           "dest-protocol": protocol, "dest-uri": dest, "reconnect-delay": 1});
    if protocol == "amqp10" {
        value["dest-address"] = json!(format!("/queues/{name}-out"));
    } else {
        value["dest-queue"] = json!(format!("{name}-out"));
    }
    b.put(&format!("/api/parameters/shovel/%2F/{name}"), json!({ "value": value })).await;
}

#[tokio::test]
async fn shovels_and_federation_dial_amqps_with_rabbitmq_tls_options() {
    let b = Broker::start(33300, "tls").await;
    let (_c, ch) = b.channel().await;
    let ca = b.pki("ca.pem");
    let (cert, key) = (b.pki("client.pem"), b.pki("client.key"));
    let base = format!("amqps://admin:devpassword12@127.0.0.1:{}/%2F", b.amqps);
    let cases: Vec<(&str, &str, String, bool)> = vec![
        // Verified against the given CA, with a client certificate.
        ("tls-ca", "amqp091", format!("{base}?cacertfile={ca}&certfile={cert}&keyfile={key}&verify=verify_peer"), true),
        // The certificate is checked against the SNI name instead of the host.
        ("tls-sni", "amqp091", format!("{base}?cacertfile={ca}&server_name_indication=localhost"), true),
        ("tls-none", "amqp091", format!("{base}?verify=verify_none"), true),
        ("tls-a10", "amqp10", format!("{base}?cacertfile={ca}"), true),
        // verify_peer with the system roots does not trust the test CA.
        ("tls-system", "amqp091", base.clone(), false),
        ("tls-badname", "amqp091", format!("{base}?cacertfile={ca}&server_name_indication=wrong.example"), false),
    ];
    for (name, _, _, _) in &cases {
        for q in ["in", "out"] {
            ch.queue_declare(&format!("{name}-{q}"), durable(), FieldTable::default()).await.unwrap();
        }
    }
    for (name, protocol, uri, _) in &cases {
        shovel_to(&b, name, protocol, uri.clone()).await;
        publish(&ch, "", &format!("{name}-in"), name.as_bytes(), BasicProperties::default()).await;
    }
    for (name, _, _, works) in &cases {
        let got = get_body(&ch, &format!("{name}-out"), Duration::from_secs(if *works { 8 } else { 2 })).await;
        if *works {
            assert_eq!(got.as_deref(), Some(name.as_bytes()), "{name} did not move over TLS");
            assert_eq!(b.shovel(name).await.unwrap()["state"], "running", "{name}");
        } else {
            assert!(got.is_none(), "{name} moved without a trusted certificate");
            let row = b.shovel(name).await.unwrap();
            assert_ne!(row["state"], "running", "{name}: {row}");
        }
    }
    // The password stays out of the reported URI.
    let row = b.shovel("tls-ca").await.unwrap();
    assert!(!row["dest_uri"].as_str().unwrap().contains("devpassword12"), "{row}");

    // Exchange federation from an amqps:// upstream.
    let opts = ExchangeDeclareOptions { durable: true, ..Default::default() };
    ch.exchange_declare("tls-up", ExchangeKind::Topic, opts, FieldTable::default()).await.unwrap();
    ch.exchange_declare("tls-down", ExchangeKind::Topic, opts, FieldTable::default()).await.unwrap();
    ch.queue_declare("tls-sink", durable(), FieldTable::default()).await.unwrap();
    ch.queue_bind("tls-sink", "tls-down", "#", QueueBindOptions::default(), FieldTable::default()).await.unwrap();
    b.put("/api/parameters/federation-upstream/%2F/tls-origin", json!({"value": {"uri": format!("{base}?cacertfile={ca}"), "exchange": "tls-up"}}))
        .await;
    b.put("/api/policies/%2F/tls-fed", json!({"pattern": "^tls-down$", "apply-to": "exchanges", "definition": {"federation-upstream": "tls-origin"}}))
        .await;
    let mut federated = None;
    for _ in 0..60 {
        publish(&ch, "tls-up", "a.b", b"over-tls", BasicProperties::default()).await;
        federated = get_body(&ch, "tls-sink", Duration::from_millis(200)).await;
        if federated.is_some() {
            break;
        }
    }
    assert_eq!(federated.as_deref(), Some(&b"over-tls"[..]));
    let (_, links) = b.get("/api/federation-links/%2F").await;
    let link = links.as_array().unwrap().iter().find(|l| l["upstream"] == "tls-origin").expect("link listed").clone();
    assert_eq!(link["status"], "running");
    assert!(link["uri"].as_str().unwrap().starts_with("amqps://"), "{link}");
}

#[tokio::test]
async fn a_queue_length_shovel_moves_what_was_there_then_deletes_itself() {
    let b = Broker::start(33320, "qlen").await;
    let (_c, ch) = b.channel().await;
    for q in ["ql-in", "ql-out", "ql-empty-in", "ql-empty-out"] {
        ch.queue_declare(q, durable(), FieldTable::default()).await.unwrap();
    }
    for i in 0..3 {
        publish(&ch, "", "ql-in", format!("m{i}").as_bytes(), BasicProperties::default()).await;
    }
    b.put(
        "/api/parameters/shovel/%2F/ql",
        json!({"value": {"src-uri": "amqp://", "src-queue": "ql-in", "dest-uri": "amqp://", "dest-queue": "ql-out",
                         "src-delete-after": "queue-length"}}),
    )
    .await;
    let mut moved = Vec::new();
    while moved.len() < 3 {
        let body = get_body(&ch, "ql-out", Duration::from_secs(5)).await.expect("the messages present at start move");
        moved.push(String::from_utf8(body).unwrap());
    }
    assert_eq!(moved, ["m0", "m1", "m2"]);
    // Then the shovel is gone, runtime parameter included.
    let deadline = Instant::now() + Duration::from_secs(5);
    while b.shovel("ql").await.is_some() && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(b.shovel("ql").await.is_none(), "the shovel deletes itself");
    let (_, params) = b.get("/api/parameters/shovel/%2F").await;
    assert!(!params.as_array().unwrap().iter().any(|p| p["name"] == "ql"), "stored definition removed: {params}");
    // Later messages stay where they are.
    publish(&ch, "", "ql-in", b"late", BasicProperties::default()).await;
    assert!(get_body(&ch, "ql-out", Duration::from_millis(800)).await.is_none());
    assert_eq!(get_body(&ch, "ql-in", Duration::from_secs(1)).await.as_deref(), Some(&b"late"[..]));
    // An empty source: nothing to move, the shovel goes at once.
    b.put(
        "/api/parameters/shovel/%2F/ql-empty",
        json!({"value": {"src-uri": "amqp://", "src-queue": "ql-empty-in", "dest-uri": "amqp://", "dest-queue": "ql-empty-out",
                         "src-delete-after": "queue-length"}}),
    )
    .await;
    let deadline = Instant::now() + Duration::from_secs(5);
    while b.shovel("ql-empty").await.is_some() && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(b.shovel("ql-empty").await.is_none());
    publish(&ch, "", "ql-empty-in", b"stays", BasicProperties::default()).await;
    assert!(get_body(&ch, "ql-empty-out", Duration::from_millis(800)).await.is_none());
}

#[tokio::test]
async fn federation_trust_user_id_keeps_or_clears_the_validated_user_id() {
    let b = Broker::start(33340, "trust").await;
    b.put("/api/users/alice", json!({"password": "alice-secret", "tags": ""})).await;
    b.put("/api/permissions/%2F/alice", json!({"configure": ".*", "write": ".*", "read": ".*"})).await;
    let (_c, ch) = b.channel().await;
    let opts = ExchangeDeclareOptions { durable: true, ..Default::default() };
    ch.exchange_declare("ut-up", ExchangeKind::Topic, opts, FieldTable::default()).await.unwrap();
    for (down, trust) in [("ut-down-cleared", false), ("ut-down-kept", true)] {
        ch.exchange_declare(down, ExchangeKind::Topic, opts, FieldTable::default()).await.unwrap();
        ch.queue_declare(&format!("{down}-sink"), durable(), FieldTable::default()).await.unwrap();
        ch.queue_bind(&format!("{down}-sink"), down, "#", QueueBindOptions::default(), FieldTable::default()).await.unwrap();
        b.put(&format!("/api/parameters/federation-upstream/%2F/{down}-up"), json!({"value": {"uri": "amqp://", "exchange": "ut-up", "trust-user-id": trust}}))
            .await;
        b.put(
            &format!("/api/policies/%2F/{down}"),
            json!({"pattern": format!("^{down}$"), "apply-to": "exchanges", "definition": {"federation-upstream": format!("{down}-up")}}),
        )
        .await;
    }
    // alice's messages carry her validated user-id; the links log in as the administrator.
    let (_a, alice) = b.channel_as("alice", "alice-secret").await;
    let props = BasicProperties::default().with_user_id(ShortString::from("alice"));
    let mut got: [Option<lapin::message::BasicGetMessage>; 2] = [None, None];
    for _ in 0..60 {
        publish(&alice, "ut-up", "a.b", b"from-alice", props.clone()).await;
        for (i, down) in ["ut-down-cleared", "ut-down-kept"].iter().enumerate() {
            if got[i].is_none() {
                got[i] = get(&ch, &format!("{down}-sink"), Duration::from_millis(100)).await;
            }
        }
        if got.iter().all(Option::is_some) {
            break;
        }
    }
    let [cleared, kept] = got;
    let cleared = cleared.expect("forwarded without trust-user-id");
    assert_eq!(cleared.delivery.data, b"from-alice");
    assert_eq!(cleared.delivery.properties.user_id(), &None, "user-id is cleared without trust-user-id");
    let kept = kept.expect("forwarded with trust-user-id");
    assert_eq!(kept.delivery.properties.user_id().as_ref().map(|u| u.as_str()), Some("alice"));
    let (_, links) = b.get("/api/federation-links/%2F").await;
    for l in links.as_array().unwrap() {
        assert_eq!(l["status"], "running", "{l}");
    }
}

// ---------- MQTT 5 ----------

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

fn connect_packet(client_id: &str, user: &str, pass: &str, keepalive: u16) -> Vec<u8> {
    let mut body = Vec::new();
    mqtt_str(&mut body, "MQTT");
    body.push(5);
    body.push(0xc2);
    body.extend_from_slice(&keepalive.to_be_bytes());
    body.push(0);
    mqtt_str(&mut body, client_id);
    mqtt_str(&mut body, user);
    mqtt_str(&mut body, pass);
    mqtt_packet(0x10, &body)
}

/// Next packet: first byte and body. `None` on timeout or close.
async fn read_packet(s: &mut TcpStream, within: Duration) -> Option<(u8, Vec<u8>)> {
    tokio::time::timeout(within, async {
        let first = s.read_u8().await.ok()?;
        let mut len = 0usize;
        let mut mult = 1usize;
        loop {
            let b = s.read_u8().await.ok()?;
            len += usize::from(b & 0x7f) * mult;
            if b & 0x80 == 0 {
                break;
            }
            mult *= 128;
        }
        let mut body = vec![0u8; len];
        s.read_exact(&mut body).await.ok()?;
        Some((first, body))
    })
    .await
    .ok()
    .flatten()
}

async fn mqtt5(port: u16, client_id: &str, user: &str, pass: &str, keepalive: u16) -> TcpStream {
    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    s.write_all(&connect_packet(client_id, user, pass, keepalive)).await.unwrap();
    let (first, body) = read_packet(&mut s, Duration::from_secs(5)).await.expect("CONNACK");
    assert_eq!(first, 0x20);
    assert_eq!(body[1], 0, "CONNACK reason");
    s
}

/// The reason code of the DISCONNECT the server sends next.
async fn disconnect_reason(s: &mut TcpStream, within: Duration) -> Option<u8> {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        let (first, body) = read_packet(s, deadline - Instant::now()).await?;
        if first == 0xe0 {
            return Some(body.first().copied().unwrap_or(0));
        }
    }
    None
}

fn publish_packet(topic: &str, props: &[u8], payload: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();
    mqtt_str(&mut body, topic);
    body.push(props.len() as u8);
    body.extend_from_slice(props);
    body.extend_from_slice(payload);
    mqtt_packet(0x30, &body)
}

#[tokio::test]
async fn mqtt_5_clients_get_a_disconnect_with_the_reason() {
    let b = Broker::start(33360, "mqtt5").await;
    let (port, wait) = (b.mqtt, Duration::from_secs(5));

    // Session taken over by a second connection with the same client id.
    let mut first = mqtt5(port, "dup-id", "admin", "devpassword12", 60).await;
    let _second = mqtt5(port, "dup-id", "admin", "devpassword12", 60).await;
    assert_eq!(disconnect_reason(&mut first, wait).await, Some(0x8e), "session taken over");

    // Keep-alive timeout: 1 s keep-alive, then silence.
    let mut idle = mqtt5(port, "idle-id", "admin", "devpassword12", 1).await;
    assert_eq!(disconnect_reason(&mut idle, wait).await, Some(0x8d), "keep-alive timeout");

    // Topic alias above the CONNACK topic-alias-maximum.
    let mut c = mqtt5(port, "alias-id", "admin", "devpassword12", 60).await;
    c.write_all(&publish_packet("a/b", &[0x23, 0x00, 200], b"x")).await.unwrap();
    assert_eq!(disconnect_reason(&mut c, wait).await, Some(0x94), "topic alias invalid");

    // An alias that was never set, with an empty topic: protocol error.
    let mut c = mqtt5(port, "alias2-id", "admin", "devpassword12", 60).await;
    c.write_all(&publish_packet("", &[0x23, 0x00, 3], b"x")).await.unwrap();
    assert_eq!(disconnect_reason(&mut c, wait).await, Some(0x82), "unknown alias");

    // A second CONNECT on the same connection.
    let mut c = mqtt5(port, "twice-id", "admin", "devpassword12", 60).await;
    c.write_all(&connect_packet("twice-id", "admin", "devpassword12", 60)).await.unwrap();
    assert_eq!(disconnect_reason(&mut c, wait).await, Some(0x82), "second CONNECT");

    // A PUBLISH whose topic length runs past the packet: malformed.
    let mut c = mqtt5(port, "short-id", "admin", "devpassword12", 60).await;
    c.write_all(&mqtt_packet(0x30, &[0x00, 0x20, b'a'])).await.unwrap();
    assert_eq!(disconnect_reason(&mut c, wait).await, Some(0x81), "malformed packet");

    // A QoS 0 publish the user may not write.
    b.put("/api/users/mq-reader", json!({"password": "reader-secret", "tags": ""})).await;
    b.put("/api/permissions/%2F/mq-reader", json!({"configure": ".*", "write": "^$", "read": ".*"})).await;
    let mut c = mqtt5(port, "reader-id", "mq-reader", "reader-secret", 60).await;
    c.write_all(&publish_packet("a/b", &[], b"x")).await.unwrap();
    assert_eq!(disconnect_reason(&mut c, wait).await, Some(0x87), "not authorized");

    // Graceful shutdown.
    let mut c = mqtt5(port, "stay-id", "admin", "devpassword12", 60).await;
    b.sigterm();
    assert_eq!(disconnect_reason(&mut c, wait).await, Some(0x8b), "server shutting down");
}
