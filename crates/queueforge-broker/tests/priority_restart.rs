//! Integration: priority order (p=9 before p=0) and restart preserves lanes.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use futures_lite::stream::StreamExt;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, QueueDeclareOptions,
};
use lapin::types::{AMQPValue, FieldTable, ShortString};
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
    for key in broker.queues.list_keys() {
        if let Some(h) = broker.queues.get(&key) {
            let (tx, rx) = tokio::sync::oneshot::channel();
            let _ =
                h.tx.send(queueforge_core::QueueCmd::Shutdown { reply: tx })
                    .await;
            let _ = tokio::time::timeout(Duration::from_secs(2), rx).await;
        }
    }
    let TestBroker {
        listener,
        _store,
        queues: _,
        port: _,
    } = broker;
    listener.abort();
    drop(_store);
    tokio::time::sleep(Duration::from_millis(100)).await;
}

fn max_priority_args(max: i32) -> FieldTable {
    let mut args = FieldTable::default();
    args.insert(ShortString::from("x-max-priority"), AMQPValue::LongInt(max));
    args
}

#[tokio::test]
async fn priority_order_p9_before_p0() {
    let tmp = TempDir::new().expect("tempdir");
    let broker = start_test_broker_at(tmp.path()).await;
    let port = broker.port;

    {
        let conn = connect(port).await;
        let ch = conn.create_channel().await.expect("channel");
        ch.queue_declare(
            "prio-q",
            QueueDeclareOptions::default(),
            max_priority_args(9),
        )
        .await
        .expect("declare priority queue");

        // Publish low then high (non-persistent is fine for in-process order).
        ch.basic_publish(
            "",
            "prio-q",
            BasicPublishOptions::default(),
            b"priority-0",
            BasicProperties::default().with_priority(0),
        )
        .await
        .expect("publish p0")
        .await
        .expect("confirm p0");

        ch.basic_publish(
            "",
            "prio-q",
            BasicPublishOptions::default(),
            b"priority-9",
            BasicProperties::default().with_priority(9),
        )
        .await
        .expect("publish p9")
        .await
        .expect("confirm p9");

        let mut consumer = ch
            .basic_consume(
                "prio-q",
                "ctag",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .expect("consume");

        let d1 = tokio::time::timeout(Duration::from_secs(5), consumer.next())
            .await
            .expect("timeout")
            .expect("closed")
            .expect("err");
        assert_eq!(
            d1.data.as_slice(),
            b"priority-9",
            "higher priority must be delivered first"
        );
        d1.ack(BasicAckOptions::default()).await.expect("ack1");

        let d2 = tokio::time::timeout(Duration::from_secs(5), consumer.next())
            .await
            .expect("timeout")
            .expect("closed")
            .expect("err");
        assert_eq!(d2.data.as_slice(), b"priority-0");
        d2.ack(BasicAckOptions::default()).await.expect("ack2");

        ch.close(200, "bye").await.ok();
        conn.close(200, "bye").await.ok();
    }

    shutdown_broker(broker).await;
}

#[tokio::test]
async fn priority_order_survives_broker_restart() {
    let tmp = TempDir::new().expect("tempdir");
    let data_path: PathBuf = tmp.path().to_path_buf();

    // ── Phase 1: durable priority queue, publish p=0 then p=9 ─────────
    let broker1 = start_test_broker_at(&data_path).await;
    let port1 = broker1.port;

    {
        let conn = connect(port1).await;
        let ch = conn.create_channel().await.expect("channel");
        ch.queue_declare(
            "prio-dur",
            QueueDeclareOptions {
                durable: true,
                ..QueueDeclareOptions::default()
            },
            max_priority_args(9),
        )
        .await
        .expect("declare durable priority queue");

        ch.basic_publish(
            "",
            "prio-dur",
            BasicPublishOptions::default(),
            b"p0-body",
            BasicProperties::default()
                .with_delivery_mode(2)
                .with_priority(0),
        )
        .await
        .expect("publish")
        .await
        .expect("confirm");

        ch.basic_publish(
            "",
            "prio-dur",
            BasicPublishOptions::default(),
            b"p9-body",
            BasicProperties::default()
                .with_delivery_mode(2)
                .with_priority(9),
        )
        .await
        .expect("publish")
        .await
        .expect("confirm");

        ch.close(200, "bye").await.ok();
        conn.close(200, "bye").await.ok();
    }

    shutdown_broker(broker1).await;

    // ── Phase 2: restart; high priority must still deliver first ──────
    let broker2 = start_test_broker_at(&data_path).await;
    let port2 = broker2.port;

    assert!(
        broker2
            .queues
            .get(&QueueKey::new("/", "prio-dur"))
            .is_some(),
        "durable priority queue must be restored"
    );

    {
        let conn = connect(port2).await;
        let ch = conn.create_channel().await.expect("channel");
        ch.queue_declare(
            "prio-dur",
            QueueDeclareOptions {
                durable: true,
                passive: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("passive declare");

        let mut consumer = ch
            .basic_consume(
                "prio-dur",
                "ctag",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .expect("consume");

        let d1 = tokio::time::timeout(Duration::from_secs(5), consumer.next())
            .await
            .expect("timeout")
            .expect("closed")
            .expect("err");
        assert_eq!(
            d1.data.as_slice(),
            b"p9-body",
            "restart must re-lane so p=9 delivers before p=0"
        );
        assert!(d1.redelivered, "recovered messages redelivered=true");
        d1.ack(BasicAckOptions::default()).await.expect("ack");

        let d2 = tokio::time::timeout(Duration::from_secs(5), consumer.next())
            .await
            .expect("timeout")
            .expect("closed")
            .expect("err");
        assert_eq!(d2.data.as_slice(), b"p0-body");
        d2.ack(BasicAckOptions::default()).await.expect("ack");

        ch.close(200, "bye").await.ok();
        conn.close(200, "bye").await.ok();
    }

    shutdown_broker(broker2).await;
}
