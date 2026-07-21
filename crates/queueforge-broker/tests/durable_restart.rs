//! Integration: publish durable+persistent, restart broker, consume with redelivered.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use futures_lite::stream::StreamExt;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, QueueDeclareOptions,
};
use lapin::types::FieldTable;
use lapin::{BasicProperties, Connection, ConnectionProperties};
use queueforge_auth::{AuthService, BootstrapMode, DEV_BOOTSTRAP_PASSWORD, DEV_BOOTSTRAP_USER};
use queueforge_broker::{start_amqp_listener, ConnectionParams};
use queueforge_core::{
    DurabilityPolicy, FsyncPolicy, MemoryTracker, QueueKey, QueueMetaStore, QueueRegistry,
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

    recover_durable_queues(
        store.as_ref(),
        queues.as_ref(),
        &RecoveryConfig {
            wal_segment_max_bytes: 4 * 1024 * 1024,
            durability_policy: policy,
        },
    )
    .await
    .expect("recovery");

    let router = Arc::new(store.bootstrap_router().expect("bootstrap router"));
    let listener = start_amqp_listener(
        "127.0.0.1:0".parse().unwrap(),
        Arc::clone(&store),
        Arc::clone(&queues),
        router,
        ConnectionTracker::shared(),
        ConnectionParams::default(),
    )
    .await
    .expect("bind amqp");
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
    .expect("lapin connect")
}

async fn shutdown_broker(broker: TestBroker) {
    // Mirror production ordered drain: stop accept + connection.close, queue
    // Shutdown/fsync, then close redb.
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
    // Brief pause so OS releases file locks (redb).
    tokio::time::sleep(Duration::from_millis(100)).await;
}

#[tokio::test]
async fn durable_publish_survives_broker_restart() {
    let tmp = TempDir::new().expect("tempdir");
    let data_path: PathBuf = tmp.path().to_path_buf();

    // ── Phase 1: publish durable persistent message ───────────────────
    let broker1 = start_test_broker_at(&data_path).await;
    let port1 = broker1.port;

    {
        let conn = connect(port1).await;
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
        .expect("declare durable queue");

        ch.basic_publish(
            "",
            "dur-q",
            BasicPublishOptions::default(),
            b"survive-me",
            BasicProperties::default().with_delivery_mode(2),
        )
        .await
        .expect("publish")
        .await
        .expect("publish confirm wait");

        // Always fsync policy: durable_done already completed before publish returns.
        ch.close(200, "bye").await.ok();
        conn.close(200, "bye").await.ok();
    }

    shutdown_broker(broker1).await;

    // ── Phase 2: restart on same data dir, consume ────────────────────
    let broker2 = start_test_broker_at(&data_path).await;
    let port2 = broker2.port;

    assert!(
        broker2.queues.get(&QueueKey::new("/", "dur-q")).is_some(),
        "durable queue must be restored after restart"
    );

    {
        let conn = connect(port2).await;
        let ch = conn.create_channel().await.expect("channel");
        ch.queue_declare(
            "dur-q",
            QueueDeclareOptions {
                durable: true,
                passive: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("passive declare after restart");

        let mut consumer = ch
            .basic_consume(
                "dur-q",
                "ctag",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .expect("consume");

        let delivery = tokio::time::timeout(Duration::from_secs(5), consumer.next())
            .await
            .expect("consume timeout")
            .expect("consumer closed")
            .expect("delivery error");

        assert_eq!(delivery.data.as_slice(), b"survive-me");
        assert!(
            delivery.redelivered,
            "recovered messages must be redelivered=true"
        );

        delivery.ack(BasicAckOptions::default()).await.expect("ack");

        ch.close(200, "bye").await.ok();
        conn.close(200, "bye").await.ok();
    }

    shutdown_broker(broker2).await;
}
