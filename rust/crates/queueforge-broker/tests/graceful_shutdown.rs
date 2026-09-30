//! Integration: ordered graceful drain — readyz 503, connection.close, WAL fsync, redb close.

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
use queueforge_metrics::{install_recorder, start_server, ReadyFlag};
use queueforge_mgmt::ConnectionTracker;
use queueforge_store::{recover_durable_queues, MetadataStore, RecoveryConfig, WalFactory};
use tempfile::TempDir;
use tokio_executor_trait::Tokio as TokioExecutor;
use tokio_reactor_trait::Tokio as TokioReactor;

struct TestStack {
    _dir: TempDir,
    store: Arc<MetadataStore>,
    queues: Arc<QueueRegistry>,
    listener: queueforge_broker::AmqpListener,
    metrics: Option<queueforge_metrics::MetricsServer>,
    ready: ReadyFlag,
    amqp_port: u16,
    metrics_addr: Option<std::net::SocketAddr>,
}

/// Install metrics recorder once per process (global).
fn metrics_handle() -> Option<metrics_exporter_prometheus::PrometheusHandle> {
    static ONCE: std::sync::OnceLock<Option<metrics_exporter_prometheus::PrometheusHandle>> =
        std::sync::OnceLock::new();
    ONCE.get_or_init(|| install_recorder().ok()).clone()
}

async fn start_stack() -> TestStack {
    let dir = TempDir::new().expect("tempdir");
    let store = MetadataStore::open(dir.path()).expect("open store");
    let auth = AuthService::new(&store);
    assert!(auth
        .bootstrap_admin_if_empty(BootstrapMode::DevFallback)
        .expect("bootstrap"));

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
            local_node: None,
        },
    )
    .await
    .expect("recovery");

    let ready = ReadyFlag::new();
    let (metrics, metrics_addr) = if let Some(handle) = metrics_handle() {
        let server = start_server("127.0.0.1:0".parse().unwrap(), handle, ready.clone())
            .await
            .expect("metrics");
        let addr = server.local_addr;
        (Some(server), Some(addr))
    } else {
        (None, None)
    };

    ready.set_ready(true);

    let router = Arc::new(store.bootstrap_router().expect("router"));
    let listener = start_amqp_listener(
        "127.0.0.1:0".parse().unwrap(),
        Arc::clone(&store),
        Arc::clone(&queues),
        router,
        ConnectionTracker::shared(),
        ConnectionParams::default(),
    )
    .await
    .expect("amqp");
    let amqp_port = listener.local_addr.port();

    TestStack {
        _dir: dir,
        store,
        queues,
        listener,
        metrics,
        ready,
        amqp_port,
        metrics_addr,
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

async fn get_readyz(addr: std::net::SocketAddr) -> u16 {
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("http://{addr}/readyz"))
        .send()
        .await
        .expect("readyz");
    resp.status().as_u16()
}

/// Ordered drain: readyz goes 503, open connection is closed, durable msg survives reopen.
#[tokio::test]
async fn graceful_drain_readyz_and_durable_fsync() {
    let stack = start_stack().await;
    let amqp_port = stack.amqp_port;
    let metrics_addr = stack.metrics_addr;

    if let Some(addr) = metrics_addr {
        assert_eq!(get_readyz(addr).await, 200, "ready before drain");
    }

    // Publish durable persistent message.
    {
        let conn = connect(amqp_port).await;
        let ch = conn.create_channel().await.expect("channel");
        ch.queue_declare(
            "drain-q",
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
            "drain-q",
            BasicPublishOptions::default(),
            b"drained-ok",
            BasicProperties::default().with_delivery_mode(2),
        )
        .await
        .expect("publish")
        .await
        .expect("confirm");

        ch.close(200, "bye").await.ok();
        conn.close(200, "bye").await.ok();
    }

    // Open a live connection that drain must force-close.
    let live = connect(amqp_port).await;
    assert!(live.status().connected());
    assert!(
        stack.listener.tracker.active() >= 1,
        "expected at least one tracked connection"
    );

    // Begin ordered shutdown (same sequence as main).
    stack.ready.set_ready(false);
    if let Some(addr) = metrics_addr {
        assert_eq!(
            get_readyz(addr).await,
            503,
            "readyz must be 503 during shutdown"
        );
    }

    let data_dir = stack.store.data_dir().to_path_buf();
    let TestStack {
        _dir,
        store,
        queues,
        listener,
        metrics,
        ready: _,
        amqp_port: _,
        metrics_addr: _,
    } = stack;

    listener.graceful_stop(Duration::from_secs(5)).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    queues.shutdown_all().await;
    assert!(queues.is_empty());
    drop(queues);

    if let Some(m) = metrics {
        m.abort();
    }

    match Arc::try_unwrap(store) {
        Ok(s) => s.close(),
        Err(s) => drop(s),
    }
    drop(live);

    tokio::time::sleep(Duration::from_millis(150)).await;

    // Reopen: durable message must be present (fsync on queue Shutdown).
    let store2 = MetadataStore::open(&data_dir).expect("reopen store");
    let store2 = Arc::new(store2);
    let policy = DurabilityPolicy::from_parts(FsyncPolicy::Always, 100, 1);
    let factory = Arc::new(WalFactory::new(store2.data_dir(), 4 * 1024 * 1024));
    let queues2 = QueueRegistry::shared_with_durability(
        Arc::clone(&store2) as Arc<dyn QueueMetaStore>,
        MemoryTracker::shared(),
        factory,
        policy,
    );
    recover_durable_queues(
        store2.as_ref(),
        queues2.as_ref(),
        &RecoveryConfig {
            wal_segment_max_bytes: 4 * 1024 * 1024,
            durability_policy: policy,
            local_node: None,
        },
    )
    .await
    .expect("recovery after drain");

    assert!(
        queues2.get(&QueueKey::new("/", "drain-q")).is_some(),
        "durable queue restored after graceful drain"
    );

    let router = Arc::new(store2.bootstrap_router().expect("router"));
    let listener2 = start_amqp_listener(
        "127.0.0.1:0".parse().unwrap(),
        store2,
        Arc::clone(&queues2),
        router,
        ConnectionTracker::shared(),
        ConnectionParams::default(),
    )
    .await
    .expect("amqp2");
    let port2 = listener2.local_addr.port();

    {
        let conn = connect(port2).await;
        let ch = conn.create_channel().await.expect("ch");
        let mut consumer = ch
            .basic_consume(
                "drain-q",
                "ctag",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .expect("consume");

        let delivery = tokio::time::timeout(Duration::from_secs(5), consumer.next())
            .await
            .expect("timeout")
            .expect("closed")
            .expect("err");
        assert_eq!(delivery.data.as_slice(), b"drained-ok");
        delivery.ack(BasicAckOptions::default()).await.ok();
        ch.close(200, "bye").await.ok();
        conn.close(200, "bye").await.ok();
    }

    listener2.graceful_stop(Duration::from_secs(2)).await;
    queues2.shutdown_all().await;
}

/// After graceful_stop, new AMQP connects must fail (accept loop gone).
#[tokio::test]
async fn graceful_stop_rejects_new_connections() {
    let stack = start_stack().await;
    let port = stack.amqp_port;

    stack.ready.set_ready(false);
    let TestStack {
        _dir,
        store,
        queues,
        listener,
        metrics,
        ..
    } = stack;

    listener.graceful_stop(Duration::from_secs(2)).await;
    queues.shutdown_all().await;
    drop(queues);
    if let Some(m) = metrics {
        m.abort();
    }
    match Arc::try_unwrap(store) {
        Ok(s) => s.close(),
        Err(s) => drop(s),
    }

    let options = ConnectionProperties::default()
        .with_executor(TokioExecutor::current())
        .with_reactor(TokioReactor);
    let result = tokio::time::timeout(
        Duration::from_secs(3),
        Connection::connect(&amqp_url(port), options),
    )
    .await;

    match result {
        Ok(Err(_)) => {}
        Err(_) => {}
        Ok(Ok(conn)) => {
            let _ = conn.close(200, "nope").await;
            panic!("new connection must not succeed after accept loop stopped");
        }
    }
}

/// Tracker active count goes to zero after graceful_stop with an open client.
#[tokio::test]
async fn graceful_stop_drains_open_connection() {
    let stack = start_stack().await;
    let port = stack.amqp_port;
    let live = connect(port).await;
    assert!(stack.listener.tracker.active() >= 1);

    let tracker = stack.listener.tracker.clone();
    let TestStack {
        listener,
        queues,
        store,
        metrics,
        _dir,
        ..
    } = stack;

    let start = std::time::Instant::now();
    let drained = listener.graceful_stop(Duration::from_secs(5)).await;
    let elapsed = start.elapsed();
    assert!(drained, "single cooperative client must drain");
    assert_eq!(
        tracker.active(),
        0,
        "connection guards must be gone after graceful_stop"
    );
    assert!(
        elapsed < Duration::from_secs(2),
        "drain of one client should be well under timeout, took {elapsed:?}"
    );

    // Client should observe the server-initiated close.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !live.status().connected(),
        "lapin connection should not stay connected after server drain"
    );

    let report = queues.shutdown_all().await;
    assert!(report.is_clean(), "report={report:?}");
    drop(queues);
    if let Some(m) = metrics {
        m.abort();
    }
    match Arc::try_unwrap(store) {
        Ok(s) => s.close(),
        Err(s) => drop(s),
    }
}
