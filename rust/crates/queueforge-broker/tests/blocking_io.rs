//! Metadata transactions for AMQP authz and management list/overview run off
//! the Tokio worker.

use std::sync::Arc;
use std::time::Duration;

use lapin::options::QueueDeclareOptions;
use lapin::types::FieldTable;
use lapin::{Connection, ConnectionProperties};
use queueforge_auth::{AuthService, BootstrapMode, DEV_BOOTSTRAP_PASSWORD, DEV_BOOTSTRAP_USER};
use queueforge_broker::{start_amqp_listener, ConnectionParams};
use queueforge_core::{MemoryTracker, QueueMetaStore, QueueRegistry, UserTag};
use queueforge_mgmt::{ConnectionTracker, MgmtConfig, MgmtState};
use queueforge_store::{store_scheduler_stats, track_store_scheduler, MetadataStore};
use tempfile::TempDir;

use tokio_executor_trait::Tokio as TokioExecutor;
use tokio_reactor_trait::Tokio as TokioReactor;

#[tokio::test]
async fn permission_and_overview_use_blocking_pool() {
    let dir = TempDir::new().expect("tempdir");
    let store = MetadataStore::open(dir.path()).expect("open store");
    let auth = AuthService::new(&store);
    assert!(auth
        .bootstrap_admin_if_empty(BootstrapMode::DevFallback)
        .expect("bootstrap"));
    auth.create_user("nobody", "nobody-password", vec![UserTag::Management])
        .expect("create user");
    auth.set_permission("nobody", "/", "", "", "")
        .expect("permission");

    let store = Arc::new(store);
    let queues = QueueRegistry::shared(
        Arc::clone(&store) as Arc<dyn QueueMetaStore>,
        MemoryTracker::shared(),
    );
    let router = Arc::new(store.bootstrap_router().expect("router"));
    let connections = ConnectionTracker::shared();
    let listener = start_amqp_listener(
        "127.0.0.1:0".parse().unwrap(),
        Arc::clone(&store),
        Arc::clone(&queues),
        Arc::clone(&router),
        Arc::clone(&connections),
        ConnectionParams::default(),
    )
    .await
    .expect("amqp");
    let amqp_port = listener.local_addr.port();

    let ready = queueforge_metrics::ReadyFlag::new();
    ready.set_ready(true);
    let mgmt = MgmtState::new(
        Arc::clone(&store),
        queues,
        router,
        connections,
        ready,
        MgmtConfig::default(),
    );
    let http = queueforge_mgmt::start_server("127.0.0.1:0".parse().unwrap(), mgmt, None)
        .await
        .expect("http");
    let http_addr = http.local_addr;

    let admin = amqp_connect(amqp_port, DEV_BOOTSTRAP_USER, DEV_BOOTSTRAP_PASSWORD).await;
    let ch = admin.create_channel().await.expect("admin channel");
    ch.queue_declare(
        "tracked-q",
        QueueDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .expect("admin declare is allowed");

    let client = reqwest::Client::builder()
        .cookie_store(true)
        .build()
        .expect("http client");
    let base = format!("http://{http_addr}");
    let login = client
        .post(format!("{base}/api/login"))
        .json(&serde_json::json!({
            "username": DEV_BOOTSTRAP_USER,
            "password": DEV_BOOTSTRAP_PASSWORD,
        }))
        .send()
        .await
        .expect("login");
    let login_status = login.status();
    let login_body = login.text().await.unwrap_or_default();
    assert!(
        login_status.is_success(),
        "login status {login_status} body {login_body}"
    );

    track_store_scheduler(true);

    ch.basic_publish(
        "",
        "tracked-q",
        lapin::options::BasicPublishOptions::default(),
        b"ping",
        lapin::BasicProperties::default(),
    )
    .await
    .expect("publish is allowed")
    .await
    .expect("confirm");

    let nobody = amqp_connect(amqp_port, "nobody", "nobody-password").await;
    let denied = nobody.create_channel().await.expect("nobody channel");
    let declare_denied = denied
        .queue_declare(
            "nope",
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await;
    assert!(
        declare_denied.is_err(),
        "user with an empty configure regex must be denied"
    );

    let overview = client
        .get(format!("{base}/api/overview"))
        .send()
        .await
        .expect("overview");
    assert!(overview.status().is_success());
    let overview_body = overview.text().await.expect("overview body");
    assert!(overview_body.contains("QueueForge"), "{overview_body}");

    let queues_body = client
        .get(format!("{base}/api/queues/%2F"))
        .send()
        .await
        .expect("list queues");
    assert!(queues_body.status().is_success());
    let queues_text = queues_body.text().await.expect("queues body");
    assert!(queues_text.contains("tracked-q"), "{queues_text}");

    let stats = store_scheduler_stats();
    track_store_scheduler(false);
    assert_eq!(
        stats.on_worker, 0,
        "metadata transactions ran on a Tokio worker: {stats:?}"
    );
    assert!(
        stats.off_worker > 0,
        "expected blocking-pool metadata transactions, got {stats:?}"
    );

    admin.close(200, "bye").await.ok();
    nobody.close(200, "bye").await.ok();
    listener.abort();
}

async fn amqp_connect(port: u16, user: &str, pass: &str) -> Connection {
    let uri = format!("amqp://{user}:{pass}@127.0.0.1:{port}/%2f");
    let options = ConnectionProperties::default()
        .with_executor(TokioExecutor::current())
        .with_reactor(TokioReactor);
    tokio::time::timeout(Duration::from_secs(10), Connection::connect(&uri, options))
        .await
        .expect("connect timed out")
        .expect("connect")
}
