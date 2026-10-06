//! Two real `queueforge` processes with separate data directories.

use std::process::{Child, Command};
use std::time::Duration;

use futures_lite::StreamExt;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, BasicQosOptions,
    ConfirmSelectOptions, ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions,
};
use lapin::types::{AMQPValue, FieldTable, ShortString};
use lapin::{BasicProperties, Connection, ConnectionProperties, ExchangeKind};
use queueforge_broker::queue_home;
use queueforge_core::ClusterMember;
use tokio::time::timeout;

struct Node {
    child: Child,
    amqp: u16,
    mgmt: u16,
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn spawn_node(
    dir: &std::path::Path,
    node_id: &str,
    amqp: u16,
    mgmt: u16,
    metrics: u16,
    cluster_listen: u16,
    peers: &[(String, u16)],
) -> Node {
    let cfg = dir.join(format!("{node_id}.toml"));
    let data = dir.join(node_id);
    std::fs::create_dir_all(&data).unwrap();
    let members: String = peers
        .iter()
        .map(|(id, port)| format!("  {{ id = \"{id}\", addr = \"127.0.0.1:{port}\" }},\n"))
        .collect();
    let cluster = if peers.is_empty() {
        String::new()
    } else {
        format!(
            "[cluster]\nnode_id = \"{node_id}\"\nlisten = \"127.0.0.1:{cluster_listen}\"\nmembers = [\n{members}]\n"
        )
    };
    std::fs::write(
        &cfg,
        format!(
            r#"[listeners]
amqp = "127.0.0.1:{amqp}"
management = "127.0.0.1:{mgmt}"
metrics = "127.0.0.1:{metrics}"

[data]
dir = "{data}"
fsync_policy = "always"

{cluster}"#,
            data = data.display()
        ),
    )
    .unwrap();
    // CARGO_BIN_EXE is compiled into this integration test. Touching this call
    // after the workspace moved under rust/ forces cargo to bake the new path.
    let child = Command::new(env!("CARGO_BIN_EXE_queueforge"))
        .arg("--config")
        .arg(&cfg)
        .arg("--dev-bootstrap")
        .spawn()
        .expect("spawn queueforge");
    Node { child, amqp, mgmt }
}

async fn wait_ready(node: &mut Node) {
    let url = format!("http://127.0.0.1:{}/readyz", node.mgmt);
    let client = reqwest::Client::new();
    for _ in 0..100 {
        if let Ok(res) = client.get(&url).send().await {
            if res.status().is_success() {
                return;
            }
        }
        if node.child.try_wait().ok().flatten().is_some() {
            panic!("node on {} exited before ready", node.amqp);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("node on {} did not become ready", node.amqp);
}

async fn amqp(node: &Node) -> lapin::Channel {
    let conn = Connection::connect(
        &format!("amqp://admin:devpassword12@127.0.0.1:{}/%2f", node.amqp),
        ConnectionProperties::default(),
    )
    .await
    .expect("amqp connect");
    conn.create_channel().await.expect("channel")
}

#[tokio::test]
async fn single_node_config_still_serves_amqp() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = spawn_node(dir.path(), "solo", 27861, 37861, 37862, 0, &[]);
    wait_ready(&mut node).await;
    let ch = amqp(&node).await;
    ch.queue_declare(
        "solo-q",
        QueueDeclareOptions {
            durable: true,
            ..QueueDeclareOptions::default()
        },
        FieldTable::default(),
    )
    .await
    .expect("declare");
    ch.basic_publish(
        "",
        "solo-q",
        BasicPublishOptions::default(),
        b"solo",
        BasicProperties::default(),
    )
    .await
    .expect("publish")
    .await
    .ok();
    let mut consumer = ch
        .basic_consume(
            "solo-q",
            "c",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("consume");
    let delivery = timeout(Duration::from_secs(5), consumer.next())
        .await
        .expect("timeout")
        .expect("closed")
        .expect("delivery");
    assert_eq!(delivery.data.as_slice(), b"solo");
}

#[tokio::test]
async fn declare_on_one_node_is_usable_on_the_other() {
    let dir = tempfile::tempdir().unwrap();
    let peers = vec![("a".into(), 27971u16), ("b".into(), 27972u16)];
    let mut a = spawn_node(dir.path(), "a", 27961, 37961, 37962, 27971, &peers);
    let mut b = spawn_node(dir.path(), "b", 27963, 37963, 37964, 27972, &peers);
    wait_ready(&mut a).await;
    wait_ready(&mut b).await;
    tokio::time::sleep(Duration::from_millis(400)).await;

    let ch_a = amqp(&a).await;
    ch_a.exchange_declare(
        "ex",
        ExchangeKind::Direct,
        ExchangeDeclareOptions {
            durable: true,
            ..ExchangeDeclareOptions::default()
        },
        FieldTable::default(),
    )
    .await
    .expect("exchange");
    ch_a.queue_declare(
        "shared",
        QueueDeclareOptions {
            durable: true,
            ..QueueDeclareOptions::default()
        },
        FieldTable::default(),
    )
    .await
    .expect("queue");
    ch_a.queue_bind(
        "shared",
        "ex",
        "rk",
        QueueBindOptions::default(),
        FieldTable::default(),
    )
    .await
    .expect("bind");

    let ch_b = amqp(&b).await;
    ch_b.queue_declare(
        "shared",
        QueueDeclareOptions {
            passive: true,
            durable: true,
            ..QueueDeclareOptions::default()
        },
        FieldTable::default(),
    )
    .await
    .expect("passive declare on the other node");

    ch_a.basic_publish(
        "ex",
        "rk",
        BasicPublishOptions::default(),
        b"cross",
        BasicProperties::default().with_delivery_mode(2),
    )
    .await
    .expect("publish")
    .await
    .ok();

    let mut consumer = ch_b
        .basic_consume(
            "shared",
            "c",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("consume on peer");
    let delivery = timeout(Duration::from_secs(5), consumer.next())
        .await
        .expect("delivery timeout")
        .expect("closed")
        .expect("delivery");
    assert_eq!(delivery.data.as_slice(), b"cross");
    delivery.ack(BasicAckOptions::default()).await.expect("ack");
    drop(consumer);

    ch_a.confirm_select(ConfirmSelectOptions::default())
        .await
        .expect("confirm");
    ch_a.basic_publish(
        "ex",
        "rk",
        BasicPublishOptions::default(),
        b"kept",
        BasicProperties::default().with_delivery_mode(2),
    )
    .await
    .expect("publish kept")
    .await
    .expect("confirm kept");

    let members = vec![
        ClusterMember {
            id: "a".into(),
            addr: "127.0.0.1:27971".parse().unwrap(),
        },
        ClusterMember {
            id: "b".into(),
            addr: "127.0.0.1:27972".parse().unwrap(),
        },
    ];
    let home = queue_home(&members, "/", "shared");
    let home_node = if home == "a" { &mut a } else { &mut b };
    let _ = home_node.child.kill();
    let _ = home_node.child.wait();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let other = if home == "a" { &b } else { &a };
    let ch = amqp(other).await;
    let failed = ch
        .basic_get("shared", lapin::options::BasicGetOptions::default())
        .await;
    assert!(
        failed.is_err(),
        "queue op must fail while the home node is down"
    );

    let mut home_again = if home == "a" {
        spawn_node(dir.path(), "a", 27961, 37961, 37962, 27971, &peers)
    } else {
        spawn_node(dir.path(), "b", 27963, 37963, 37964, 27972, &peers)
    };
    wait_ready(&mut home_again).await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    let ch = amqp(&home_again).await;
    let mut consumer = ch
        .basic_consume(
            "shared",
            "after",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("consume after home restart");
    let mut saw_kept = false;
    for _ in 0..4 {
        let delivery = timeout(Duration::from_secs(5), consumer.next())
            .await
            .expect("kept message missing after home restart")
            .expect("closed")
            .expect("delivery");
        let body = delivery.data.clone();
        delivery.ack(BasicAckOptions::default()).await.expect("ack");
        if body.as_slice() == b"kept" {
            saw_kept = true;
            break;
        }
    }
    assert!(
        saw_kept,
        "durable message confirmed on the home must survive restart"
    );
}

#[tokio::test]
async fn cross_node_headers_alternate_tx_and_prefetch() {
    let dir = tempfile::tempdir().unwrap();
    let peers = vec![("a".into(), 28071u16), ("b".into(), 28072u16)];
    let mut a = spawn_node(dir.path(), "a", 28061, 38061, 38062, 28071, &peers);
    let mut b = spawn_node(dir.path(), "b", 28063, 38063, 38064, 28072, &peers);
    wait_ready(&mut a).await;
    wait_ready(&mut b).await;
    tokio::time::sleep(Duration::from_millis(400)).await;

    let members = vec![
        ClusterMember {
            id: "a".into(),
            addr: "127.0.0.1:28071".parse().unwrap(),
        },
        ClusterMember {
            id: "b".into(),
            addr: "127.0.0.1:28072".parse().unwrap(),
        },
    ];
    let mut queue_name = String::new();
    for i in 0..80 {
        let candidate = format!("pq{i}");
        if queue_home(&members, "/", &candidate) == "a" {
            queue_name = candidate;
            break;
        }
    }
    assert!(!queue_name.is_empty());

    let home = amqp(&a).await;
    let peer = amqp(&b).await;
    home.queue_declare(
        &queue_name,
        QueueDeclareOptions {
            durable: true,
            ..QueueDeclareOptions::default()
        },
        FieldTable::default(),
    )
    .await
    .expect("home queue");
    peer.basic_qos(0, BasicQosOptions { global: false })
        .await
        .expect("qos");
    let mut consumer = peer
        .basic_consume(
            &queue_name,
            "wide",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("non-home consume");
    for n in 0..300u32 {
        home.basic_publish(
            "",
            &queue_name,
            BasicPublishOptions::default(),
            n.to_string().as_bytes(),
            BasicProperties::default(),
        )
        .await
        .expect("publish")
        .await
        .ok();
    }
    let mut got = 0u32;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    while got <= 256 && tokio::time::Instant::now() < deadline {
        match timeout(Duration::from_millis(200), consumer.next()).await {
            Ok(Some(Ok(_))) => got += 1,
            _ => break,
        }
    }
    assert!(
        got > 256,
        "prefetch 0 on a non-home connection delivered {got}"
    );

    let ch = amqp(&b).await;
    ch.exchange_declare(
        "hdr",
        ExchangeKind::Headers,
        ExchangeDeclareOptions {
            durable: true,
            ..ExchangeDeclareOptions::default()
        },
        FieldTable::default(),
    )
    .await
    .expect("headers exchange");
    ch.queue_declare(
        "hq",
        QueueDeclareOptions {
            durable: true,
            ..QueueDeclareOptions::default()
        },
        FieldTable::default(),
    )
    .await
    .expect("hq");
    let mut args = FieldTable::default();
    args.insert(
        ShortString::from("x-match"),
        AMQPValue::LongString("all".into()),
    );
    args.insert(
        ShortString::from("color"),
        AMQPValue::LongString("blue".into()),
    );
    ch.queue_bind("hq", "hdr", "", QueueBindOptions::default(), args)
        .await
        .expect("bind headers");
    tokio::time::sleep(Duration::from_millis(300)).await;
    let publisher = amqp(&a).await;
    let mut headers = FieldTable::default();
    headers.insert(
        ShortString::from("color"),
        AMQPValue::LongString("blue".into()),
    );
    publisher
        .basic_publish(
            "hdr",
            "",
            BasicPublishOptions::default(),
            b"hdr-body",
            BasicProperties::default().with_headers(headers),
        )
        .await
        .expect("publish headers")
        .await
        .ok();
    let mut hdr_consumer = ch
        .basic_consume(
            "hq",
            "hc",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("consume headers");
    let delivery = timeout(Duration::from_secs(5), hdr_consumer.next())
        .await
        .expect("headers timeout")
        .expect("closed")
        .expect("headers delivery");
    assert_eq!(delivery.data.as_slice(), b"hdr-body");

    ch.queue_declare(
        "anyq",
        QueueDeclareOptions {
            durable: true,
            ..QueueDeclareOptions::default()
        },
        FieldTable::default(),
    )
    .await
    .expect("anyq");
    let mut any_args = FieldTable::default();
    any_args.insert(
        ShortString::from("x-match"),
        AMQPValue::LongString("any".into()),
    );
    any_args.insert(
        ShortString::from("color"),
        AMQPValue::LongString("blue".into()),
    );
    any_args.insert(ShortString::from("size"), AMQPValue::LongString("l".into()));
    ch.queue_bind("anyq", "hdr", "", QueueBindOptions::default(), any_args)
        .await
        .expect("bind any");
    tokio::time::sleep(Duration::from_millis(200)).await;
    let mut only_color = FieldTable::default();
    only_color.insert(
        ShortString::from("color"),
        AMQPValue::LongString("blue".into()),
    );
    publisher
        .basic_publish(
            "hdr",
            "",
            BasicPublishOptions::default(),
            b"any-body",
            BasicProperties::default().with_headers(only_color),
        )
        .await
        .expect("publish any")
        .await
        .ok();
    let mut any_consumer = publisher
        .basic_consume(
            "anyq",
            "anyc",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("consume any");
    let delivery = timeout(Duration::from_secs(5), any_consumer.next())
        .await
        .expect("any timeout")
        .expect("closed")
        .expect("any delivery");
    assert_eq!(delivery.data.as_slice(), b"any-body");

    let mut alt_args = FieldTable::default();
    alt_args.insert(
        ShortString::from("alternate-exchange"),
        AMQPValue::LongString("alt".into()),
    );
    publisher
        .exchange_declare(
            "main",
            ExchangeKind::Direct,
            ExchangeDeclareOptions {
                durable: true,
                ..ExchangeDeclareOptions::default()
            },
            alt_args,
        )
        .await
        .expect("main");
    publisher
        .exchange_declare(
            "alt",
            ExchangeKind::Fanout,
            ExchangeDeclareOptions {
                durable: true,
                ..ExchangeDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("alt");
    publisher
        .queue_declare(
            "altq",
            QueueDeclareOptions {
                durable: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("altq");
    publisher
        .queue_bind(
            "altq",
            "alt",
            "",
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("bind alt");
    tokio::time::sleep(Duration::from_millis(300)).await;
    ch.basic_publish(
        "main",
        "missing",
        BasicPublishOptions::default(),
        b"via-alt",
        BasicProperties::default(),
    )
    .await
    .expect("publish alt")
    .await
    .ok();
    let mut alt_consumer = ch
        .basic_consume(
            "altq",
            "ac",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("consume alt");
    let delivery = timeout(Duration::from_secs(5), alt_consumer.next())
        .await
        .expect("alt timeout")
        .expect("closed")
        .expect("alt delivery");
    assert_eq!(delivery.data.as_slice(), b"via-alt");

    publisher
        .exchange_declare(
            "topics",
            ExchangeKind::Topic,
            ExchangeDeclareOptions {
                durable: true,
                ..ExchangeDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("topic exchange");
    publisher
        .queue_declare(
            "tq",
            QueueDeclareOptions {
                durable: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("tq");
    publisher
        .queue_bind(
            "tq",
            "topics",
            "orders.*",
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("bind topic");
    tokio::time::sleep(Duration::from_millis(200)).await;
    ch.basic_publish(
        "topics",
        "orders.new",
        BasicPublishOptions::default(),
        b"topic-body",
        BasicProperties::default(),
    )
    .await
    .expect("publish topic")
    .await
    .ok();
    let mut topic_consumer = ch
        .basic_consume(
            "tq",
            "tc",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("consume topic");
    let delivery = timeout(Duration::from_secs(5), topic_consumer.next())
        .await
        .expect("topic timeout")
        .expect("closed")
        .expect("topic delivery");
    assert_eq!(delivery.data.as_slice(), b"topic-body");

    ch.queue_declare(
        "txq",
        QueueDeclareOptions {
            durable: true,
            ..QueueDeclareOptions::default()
        },
        FieldTable::default(),
    )
    .await
    .expect("txq");
    ch.tx_select().await.expect("tx");
    ch.basic_publish(
        "",
        "txq",
        BasicPublishOptions::default(),
        b"hidden",
        BasicProperties::default(),
    )
    .await
    .expect("tx publish")
    .await
    .ok();
    ch.tx_rollback().await.expect("rollback");
    let mut tx_consumer = ch
        .basic_consume(
            "txq",
            "txc",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("consume txq");
    let hidden = timeout(Duration::from_millis(400), tx_consumer.next()).await;
    assert!(hidden.is_err(), "rolled back publish must stay invisible");
    ch.basic_publish(
        "",
        "txq",
        BasicPublishOptions::default(),
        b"shown",
        BasicProperties::default(),
    )
    .await
    .expect("tx publish 2")
    .await
    .ok();
    ch.tx_commit().await.expect("commit");
    let delivery = timeout(Duration::from_secs(5), tx_consumer.next())
        .await
        .expect("commit timeout")
        .expect("closed")
        .expect("commit delivery");
    assert_eq!(delivery.data.as_slice(), b"shown");
}

#[tokio::test]
async fn cross_node_publish_consume_window() {
    let dir = tempfile::tempdir().unwrap();
    let peers = vec![("a".into(), 28171u16), ("b".into(), 28172u16)];
    let mut a = spawn_node(dir.path(), "a", 28161, 38161, 38162, 28171, &peers);
    let mut b = spawn_node(dir.path(), "b", 28163, 38163, 38164, 28172, &peers);
    wait_ready(&mut a).await;
    wait_ready(&mut b).await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    let members = vec![
        ClusterMember {
            id: "a".into(),
            addr: "127.0.0.1:28171".parse().unwrap(),
        },
        ClusterMember {
            id: "b".into(),
            addr: "127.0.0.1:28172".parse().unwrap(),
        },
    ];
    let mut queue_name = String::new();
    for i in 0..80 {
        let candidate = format!("win{i}");
        if queue_home(&members, "/", &candidate) == "b" {
            queue_name = candidate;
            break;
        }
    }
    let setup = amqp(&b).await;
    setup
        .queue_declare(
            &queue_name,
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("queue");
    let publisher = amqp(&a).await;
    let consumer_ch = amqp(&b).await;
    consumer_ch
        .basic_qos(1000, BasicQosOptions { global: false })
        .await
        .expect("qos");
    let mut consumer = consumer_ch
        .basic_consume(
            &queue_name,
            "win",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("consume");
    let start = tokio::time::Instant::now();
    let mut published = 0u64;
    let mut consumed = 0u64;
    while start.elapsed() < Duration::from_secs(5) {
        publisher
            .basic_publish(
                "",
                &queue_name,
                BasicPublishOptions::default(),
                b"w",
                BasicProperties::default(),
            )
            .await
            .expect("publish")
            .await
            .ok();
        published += 1;
        while let Ok(Some(Ok(delivery))) = timeout(Duration::from_millis(1), consumer.next()).await
        {
            delivery.ack(BasicAckOptions::default()).await.ok();
            consumed += 1;
        }
    }
    let drain_until = tokio::time::Instant::now() + Duration::from_secs(2);
    while tokio::time::Instant::now() < drain_until {
        match timeout(Duration::from_millis(50), consumer.next()).await {
            Ok(Some(Ok(delivery))) => {
                delivery.ack(BasicAckOptions::default()).await.ok();
                consumed += 1;
            }
            _ => break,
        }
    }
    eprintln!("published={published} consumed={consumed}");
    assert!(
        published > 0 && consumed > 0,
        "published={published} consumed={consumed}"
    );
}

#[tokio::test]
async fn consumers_on_both_nodes_receive_from_one_queue() {
    let dir = tempfile::tempdir().unwrap();
    let peers = vec![("a".into(), 28271u16), ("b".into(), 28272u16)];
    let mut a = spawn_node(dir.path(), "a", 28261, 38261, 38262, 28271, &peers);
    let mut b = spawn_node(dir.path(), "b", 28263, 38263, 38264, 28272, &peers);
    wait_ready(&mut a).await;
    wait_ready(&mut b).await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    let members = vec![
        ClusterMember {
            id: "a".into(),
            addr: "127.0.0.1:28271".parse().unwrap(),
        },
        ClusterMember {
            id: "b".into(),
            addr: "127.0.0.1:28272".parse().unwrap(),
        },
    ];
    let mut queue_name = String::new();
    for i in 0..80 {
        let candidate = format!("both{i}");
        if queue_home(&members, "/", &candidate) == "a" {
            queue_name = candidate;
            break;
        }
    }
    let setup = amqp(&a).await;
    setup
        .queue_declare(
            &queue_name,
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("queue");
    let home = amqp(&a).await;
    let peer = amqp(&b).await;
    let mut home_consumer = home
        .basic_consume(
            &queue_name,
            "home",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("home consume");
    let mut peer_consumer = peer
        .basic_consume(
            &queue_name,
            "peer",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("peer consume must not collide with the home session");
    let publisher = amqp(&a).await;
    for n in 0..8u8 {
        publisher
            .basic_publish(
                "",
                &queue_name,
                BasicPublishOptions::default(),
                &[n],
                BasicProperties::default(),
            )
            .await
            .expect("publish")
            .await
            .ok();
    }
    let mut got_home = 0;
    let mut got_peer = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while (got_home == 0 || got_peer == 0) && tokio::time::Instant::now() < deadline {
        if let Ok(Some(Ok(delivery))) =
            timeout(Duration::from_millis(100), home_consumer.next()).await
        {
            delivery.ack(BasicAckOptions::default()).await.ok();
            got_home += 1;
        }
        if let Ok(Some(Ok(delivery))) =
            timeout(Duration::from_millis(100), peer_consumer.next()).await
        {
            delivery.ack(BasicAckOptions::default()).await.ok();
            got_peer += 1;
        }
    }
    assert!(
        got_home > 0 && got_peer > 0,
        "home={got_home} peer={got_peer}"
    );
}

#[tokio::test]
async fn user_created_on_one_node_opens_amqp_on_the_other() {
    let dir = tempfile::tempdir().unwrap();
    let peers = vec![("a".into(), 28371u16), ("b".into(), 28372u16)];
    let mut a = spawn_node(dir.path(), "a", 28361, 38361, 38362, 28371, &peers);
    let mut b = spawn_node(dir.path(), "b", 28363, 38363, 38364, 28372, &peers);
    wait_ready(&mut a).await;
    wait_ready(&mut b).await;
    tokio::time::sleep(Duration::from_millis(400)).await;

    let http = reqwest::Client::builder()
        .cookie_store(true)
        .build()
        .unwrap();
    let login = http
        .post(format!("http://127.0.0.1:{}/api/login", a.mgmt))
        .json(&serde_json::json!({"username": "admin", "password": "devpassword12"}))
        .send()
        .await
        .expect("login");
    assert!(
        login.status().is_success(),
        "admin login {}",
        login.status()
    );
    let created = http
        .put(format!("http://127.0.0.1:{}/api/users/alice", a.mgmt))
        .json(&serde_json::json!({"password": "alicepassword1", "tags": ["management"]}))
        .send()
        .await
        .expect("create user");
    assert!(
        created.status().is_success(),
        "create user {}",
        created.status()
    );
    let perm = http
        .put(format!(
            "http://127.0.0.1:{}/api/permissions/alice/%2F",
            a.mgmt
        ))
        .json(&serde_json::json!({"configure": ".*", "write": ".*", "read": ".*"}))
        .send()
        .await
        .expect("permission");
    assert!(perm.status().is_success(), "permission {}", perm.status());

    let conn = Connection::connect(
        &format!("amqp://alice:alicepassword1@127.0.0.1:{}/%2f", b.amqp),
        ConnectionProperties::default(),
    )
    .await
    .expect("alice must authenticate on the peer");
    conn.create_channel().await.expect("channel");
}
