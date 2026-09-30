//! Expanded integration coverage (PR 18): publisher confirms, durable restart,
//! DLX routing, priority order + restart, and management definitions export.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use futures_lite::stream::StreamExt;
use lapin::options::*;
use lapin::types::FieldTable;
use lapin::types::{AMQPValue, ShortString};
use lapin::{BasicProperties, Connection, ConnectionProperties, ExchangeKind};
use queueforge_auth::{AuthService, BootstrapMode, DEV_BOOTSTRAP_PASSWORD, DEV_BOOTSTRAP_USER};
use queueforge_broker::{start_amqp_listener, ConnectionParams};
use queueforge_core::{
    DurabilityPolicy, FsyncPolicy, MemoryTracker, QueueMetaStore, QueueRegistry,
};
use queueforge_mgmt::ConnectionTracker;
use queueforge_store::{recover_durable_queues, MetadataStore, RecoveryConfig, WalFactory};
use tempfile::TempDir;
use tokio_executor_trait::Tokio as TokioExecutor;
use tokio_reactor_trait::Tokio as TokioReactor;

struct TestBroker {
    listener: queueforge_broker::AmqpListener,
    port: u16,
    _store: Arc<MetadataStore>,
    queues: Arc<QueueRegistry>,
}

async fn start_test_broker_at(data_dir: &std::path::Path) -> TestBroker {
    let store = MetadataStore::open(data_dir).expect("open store");
    let auth = AuthService::new(&store);
    let _ = auth
        .bootstrap_admin_if_empty(BootstrapMode::DevFallback)
        .expect("bootstrap");

    let store = Arc::new(store);
    let policy = DurabilityPolicy::from_parts(FsyncPolicy::Always, 100, 1);
    let factory = Arc::new(WalFactory::new(store.data_dir(), 4 * 1024 * 1024));
    let queues = QueueRegistry::shared_with_durability(
        Arc::clone(&store) as Arc<dyn QueueMetaStore>,
        MemoryTracker::shared(),
        factory,
        policy,
    );
    let router = Arc::new(store.bootstrap_router().expect("router"));
    queues.set_dlx(Arc::new(queueforge_core::DlxRouter::new(
        Arc::clone(&router),
        Arc::downgrade(&queues),
    )));

    recover_durable_queues(
        store.as_ref(),
        queues.as_ref(),
        &RecoveryConfig {
            wal_segment_max_bytes: 4 * 1024 * 1024,
            durability_policy: policy,
            local_node: None,
        },
    )
    .await
    .expect("recovery");

    let listener = start_amqp_listener(
        "127.0.0.1:0".parse().unwrap(),
        Arc::clone(&store),
        Arc::clone(&queues),
        router,
        ConnectionTracker::shared(),
        ConnectionParams::default(),
    )
    .await
    .expect("bind");
    let port = listener.local_addr.port();
    TestBroker {
        listener,
        port,
        _store: store,
        queues,
    }
}

fn amqp_url(port: u16) -> String {
    format!("amqp://{DEV_BOOTSTRAP_USER}:{DEV_BOOTSTRAP_PASSWORD}@127.0.0.1:{port}/%2f")
}

async fn connect(port: u16) -> Connection {
    let options = ConnectionProperties::default()
        .with_executor(TokioExecutor::current())
        .with_reactor(TokioReactor);
    tokio::time::timeout(
        Duration::from_secs(10),
        Connection::connect(&amqp_url(port), options),
    )
    .await
    .expect("connect timeout")
    .expect("connect")
}

async fn shutdown_broker(broker: TestBroker) {
    let TestBroker {
        listener,
        _store,
        queues,
        port: _,
    } = broker;
    listener.graceful_stop(Duration::from_secs(5)).await;
    queues.shutdown_all().await;
    drop(queues);
    match Arc::try_unwrap(_store) {
        Ok(s) => s.close(),
        Err(s) => drop(s),
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
}

#[tokio::test]
async fn confirms_ack_on_publish() {
    let tmp = TempDir::new().unwrap();
    let broker = start_test_broker_at(tmp.path()).await;
    let conn = connect(broker.port).await;
    let ch = conn.create_channel().await.expect("channel");
    ch.confirm_select(ConfirmSelectOptions::default())
        .await
        .expect("confirm.select");
    ch.queue_declare(
        "conf-q",
        QueueDeclareOptions {
            durable: true,
            ..QueueDeclareOptions::default()
        },
        FieldTable::default(),
    )
    .await
    .expect("declare");

    let confirm = ch
        .basic_publish(
            "",
            "conf-q",
            BasicPublishOptions::default(),
            b"confirmed",
            BasicProperties::default().with_delivery_mode(2),
        )
        .await
        .expect("publish");
    let conf = tokio::time::timeout(Duration::from_secs(5), confirm)
        .await
        .expect("confirm timeout")
        .expect("confirm");
    assert!(conf.is_ack(), "expected publisher confirm ack");

    ch.close(200, "bye").await.ok();
    conn.close(200, "bye").await.ok();
    shutdown_broker(broker).await;
}

#[tokio::test]
async fn durable_restart_retains_persistent_messages() {
    let tmp = TempDir::new().unwrap();
    let data_path: PathBuf = tmp.path().to_path_buf();

    let broker1 = start_test_broker_at(&data_path).await;
    {
        let conn = connect(broker1.port).await;
        let ch = conn.create_channel().await.expect("channel");
        ch.queue_declare(
            "dur-q",
            QueueDeclareOptions {
                durable: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("declare");
        ch.confirm_select(ConfirmSelectOptions::default())
            .await
            .expect("confirm");
        let conf = ch
            .basic_publish(
                "",
                "dur-q",
                BasicPublishOptions::default(),
                b"survive-me",
                BasicProperties::default().with_delivery_mode(2),
            )
            .await
            .expect("publish");
        let ack = tokio::time::timeout(Duration::from_secs(5), conf)
            .await
            .expect("timeout")
            .expect("confirm");
        assert!(ack.is_ack());
        ch.close(200, "bye").await.ok();
        conn.close(200, "bye").await.ok();
    }
    shutdown_broker(broker1).await;

    let broker2 = start_test_broker_at(&data_path).await;
    let conn = connect(broker2.port).await;
    let ch = conn.create_channel().await.expect("ch");
    let get = ch
        .basic_get("dur-q", BasicGetOptions { no_ack: true })
        .await
        .expect("get");
    let delivery = get.expect("message present after restart");
    assert_eq!(delivery.data.as_slice(), b"survive-me");
    ch.close(200, "bye").await.ok();
    conn.close(200, "bye").await.ok();
    shutdown_broker(broker2).await;
}

#[tokio::test]
async fn dlx_routes_rejected_messages() {
    let tmp = TempDir::new().unwrap();
    let broker = start_test_broker_at(tmp.path()).await;
    let conn = connect(broker.port).await;
    let ch = conn.create_channel().await.expect("channel");

    ch.exchange_declare(
        "dlx.ex",
        ExchangeKind::Fanout,
        ExchangeDeclareOptions {
            durable: true,
            ..ExchangeDeclareOptions::default()
        },
        FieldTable::default(),
    )
    .await
    .expect("dlx exchange");

    ch.queue_declare(
        "dead",
        QueueDeclareOptions {
            durable: true,
            ..QueueDeclareOptions::default()
        },
        FieldTable::default(),
    )
    .await
    .expect("dead q");
    ch.queue_bind(
        "dead",
        "dlx.ex",
        "",
        QueueBindOptions::default(),
        FieldTable::default(),
    )
    .await
    .expect("bind dead");

    let mut args = FieldTable::default();
    args.insert(
        ShortString::from("x-dead-letter-exchange"),
        AMQPValue::LongString("dlx.ex".into()),
    );
    ch.queue_declare(
        "src-dlx",
        QueueDeclareOptions {
            durable: true,
            ..QueueDeclareOptions::default()
        },
        args,
    )
    .await
    .expect("src");

    ch.basic_publish(
        "",
        "src-dlx",
        BasicPublishOptions::default(),
        b"poison",
        BasicProperties::default(),
    )
    .await
    .expect("publish");

    let mut consumer = ch
        .basic_consume(
            "src-dlx",
            "c1",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("consume");
    let delivery = tokio::time::timeout(Duration::from_secs(5), consumer.next())
        .await
        .expect("timeout")
        .expect("stream")
        .expect("delivery");
    delivery
        .acker
        .nack(BasicNackOptions {
            requeue: false,
            multiple: false,
        })
        .await
        .expect("nack");

    let mut found = false;
    for _ in 0..50 {
        if let Ok(Some(d)) = ch.basic_get("dead", BasicGetOptions { no_ack: true }).await {
            assert_eq!(d.data.as_slice(), b"poison");
            found = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(found, "message should be dead-lettered");

    ch.close(200, "bye").await.ok();
    conn.close(200, "bye").await.ok();
    shutdown_broker(broker).await;
}

#[tokio::test]
async fn priority_order_and_restart() {
    let tmp = TempDir::new().unwrap();
    let data_path: PathBuf = tmp.path().to_path_buf();

    let broker1 = start_test_broker_at(&data_path).await;
    {
        let conn = connect(broker1.port).await;
        let ch = conn.create_channel().await.expect("channel");

        let mut args = FieldTable::default();
        args.insert(
            ShortString::from("x-max-priority"),
            AMQPValue::ShortShortUInt(9),
        );
        ch.queue_declare(
            "prio-q",
            QueueDeclareOptions {
                durable: true,
                ..QueueDeclareOptions::default()
            },
            args,
        )
        .await
        .expect("declare prio");

        ch.confirm_select(ConfirmSelectOptions::default())
            .await
            .expect("confirm");

        for (body, prio) in [(b"p0" as &[u8], 0u8), (b"p9", 9), (b"p5", 5)] {
            let conf = ch
                .basic_publish(
                    "",
                    "prio-q",
                    BasicPublishOptions::default(),
                    body,
                    BasicProperties::default()
                        .with_delivery_mode(2)
                        .with_priority(prio),
                )
                .await
                .expect("publish");
            let ack = tokio::time::timeout(Duration::from_secs(5), conf)
                .await
                .expect("timeout")
                .expect("confirm");
            assert!(ack.is_ack());
        }

        async fn get_body(ch: &lapin::Channel) -> Vec<u8> {
            let d = ch
                .basic_get("prio-q", BasicGetOptions { no_ack: true })
                .await
                .expect("get")
                .expect("msg");
            d.data.to_vec()
        }
        assert_eq!(get_body(&ch).await, b"p9");
        assert_eq!(get_body(&ch).await, b"p5");
        assert_eq!(get_body(&ch).await, b"p0");

        // Leave durable messages for restart.
        for (body, prio) in [(b"r1" as &[u8], 1u8), (b"r8", 8)] {
            let conf = ch
                .basic_publish(
                    "",
                    "prio-q",
                    BasicPublishOptions::default(),
                    body,
                    BasicProperties::default()
                        .with_delivery_mode(2)
                        .with_priority(prio),
                )
                .await
                .expect("publish");
            let _ = tokio::time::timeout(Duration::from_secs(5), conf)
                .await
                .expect("timeout")
                .expect("confirm");
        }

        ch.close(200, "bye").await.ok();
        conn.close(200, "bye").await.ok();
    }
    shutdown_broker(broker1).await;

    let broker2 = start_test_broker_at(&data_path).await;
    let conn = connect(broker2.port).await;
    let ch = conn.create_channel().await.expect("ch");
    let first = ch
        .basic_get("prio-q", BasicGetOptions { no_ack: true })
        .await
        .expect("get")
        .expect("msg");
    assert_eq!(
        first.data.as_slice(),
        b"r8",
        "highest remaining priority first after restart"
    );
    ch.close(200, "bye").await.ok();
    conn.close(200, "bye").await.ok();
    shutdown_broker(broker2).await;
}

#[tokio::test]
async fn definitions_export_includes_resources() {
    use queueforge_metrics::ReadyFlag;
    use queueforge_mgmt::{
        start_server as start_mgmt_server, ConnectionTracker, MgmtConfig, MgmtState,
    };

    let dir = TempDir::new().unwrap();
    let store = MetadataStore::open(dir.path()).unwrap();
    let auth = AuthService::new(&store);
    assert!(auth
        .bootstrap_admin_if_empty(BootstrapMode::DevFallback)
        .unwrap());
    let store = Arc::new(store);
    let queues = QueueRegistry::shared(
        Arc::clone(&store) as Arc<dyn QueueMetaStore>,
        MemoryTracker::shared(),
    );
    let router = Arc::new(store.bootstrap_router().unwrap());
    let connections = ConnectionTracker::shared();
    let ready = ReadyFlag::new();
    ready.set_ready(true);

    queues
        .declare(
            "/",
            "defs-q",
            queueforge_core::QueueDeclareOpts {
                durable: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let state = MgmtState::new(
        Arc::clone(&store),
        Arc::clone(&queues),
        Arc::clone(&router),
        Arc::clone(&connections),
        ready,
        MgmtConfig {
            cookie_secure: false,
            product_version: "0.1.0-test".into(),
            trusted_proxy_cidrs: Vec::new(),
        },
    );
    let server = start_mgmt_server("127.0.0.1:0".parse().unwrap(), state, None)
        .await
        .unwrap();
    let addr = server.local_addr;

    let client = reqwest::Client::builder()
        .cookie_store(true)
        .build()
        .unwrap();
    let login = client
        .post(format!("http://{addr}/api/login"))
        .json(&serde_json::json!({
            "username": DEV_BOOTSTRAP_USER,
            "password": DEV_BOOTSTRAP_PASSWORD,
        }))
        .send()
        .await
        .unwrap();
    assert!(login.status().is_success(), "login {}", login.status());

    let defs = client
        .get(format!("http://{addr}/api/definitions"))
        .send()
        .await
        .unwrap();
    assert!(defs.status().is_success());
    let json: serde_json::Value = defs.json().await.unwrap();
    assert!(
        json["queues"]
            .as_array()
            .unwrap()
            .iter()
            .any(|q| q["name"] == "defs-q"),
        "definitions should include declared queue"
    );
    assert!(json["vhosts"]
        .as_array()
        .unwrap()
        .iter()
        .any(|v| v["name"] == "/"));

    server.abort();
}
