//! Integration smoke: lapin connects and opens a channel against a live broker.

use std::sync::Arc;
use std::time::Duration;

use lapin::{Connection, ConnectionProperties};
use queueforge_auth::{AuthService, BootstrapMode, DEV_BOOTSTRAP_PASSWORD, DEV_BOOTSTRAP_USER};
use queueforge_broker::{
    start_amqp_listener, start_amqp_listener_with_limits, ConnectionLimiter, ConnectionParams,
};
use queueforge_core::{MemoryTracker, QueueKey, QueueMetaStore, QueueRegistry};
use queueforge_mgmt::ConnectionTracker;
use queueforge_store::MetadataStore;
use tempfile::TempDir;
use tokio_executor_trait::Tokio as TokioExecutor;
use tokio_reactor_trait::Tokio as TokioReactor;

struct TestBroker {
    _dir: TempDir,
    listener: queueforge_broker::AmqpListener,
    port: u16,
    queues: Arc<QueueRegistry>,
}

/// Boot a minimal AMQP listener with dev-bootstrap admin on a random port.
async fn start_test_broker() -> TestBroker {
    let dir = TempDir::new().expect("tempdir");
    let store = MetadataStore::open(dir.path()).expect("open store");
    let auth = AuthService::new(&store);
    assert!(auth
        .bootstrap_admin_if_empty(BootstrapMode::DevFallback)
        .expect("bootstrap"));

    let store = Arc::new(store);
    let queues = QueueRegistry::shared(
        Arc::clone(&store) as Arc<dyn QueueMetaStore>,
        MemoryTracker::shared(),
    );
    let router = Arc::new(store.bootstrap_router().expect("bootstrap router"));
    let connections = ConnectionTracker::shared();
    let listener = start_amqp_listener(
        "127.0.0.1:0".parse().unwrap(),
        store,
        Arc::clone(&queues),
        router,
        connections,
        ConnectionParams::default(),
    )
    .await
    .expect("bind amqp");
    let port = listener.local_addr.port();
    TestBroker {
        _dir: dir,
        listener,
        port,
        queues,
    }
}

fn amqp_url(port: u16, user: &str, pass: &str) -> String {
    // vhost `/` is URL-encoded as `%2f`
    format!("amqp://{user}:{pass}@127.0.0.1:{port}/%2f")
}

#[tokio::test]
async fn lapin_connect_and_open_channel() {
    let broker = start_test_broker().await;
    let port = broker.port;

    let uri = amqp_url(port, DEV_BOOTSTRAP_USER, DEV_BOOTSTRAP_PASSWORD);
    let options = ConnectionProperties::default()
        .with_executor(TokioExecutor::current())
        .with_reactor(TokioReactor);

    let conn = tokio::time::timeout(Duration::from_secs(10), Connection::connect(&uri, options))
        .await
        .expect("connect timed out")
        .expect("lapin connect");

    assert!(conn.status().connected());

    let channel = tokio::time::timeout(Duration::from_secs(5), conn.create_channel())
        .await
        .expect("create_channel timed out")
        .expect("create_channel");

    assert!(channel.status().connected());

    // Graceful close.
    channel.close(200, "bye").await.ok();
    conn.close(200, "bye").await.ok();

    broker.listener.abort();
}

#[tokio::test]
async fn lapin_bad_credentials_are_rejected() {
    let broker = start_test_broker().await;
    let port = broker.port;

    let uri = amqp_url(port, DEV_BOOTSTRAP_USER, "wrong-password-xx");
    let options = ConnectionProperties::default()
        .with_executor(TokioExecutor::current())
        .with_reactor(TokioReactor);

    let result =
        tokio::time::timeout(Duration::from_secs(10), Connection::connect(&uri, options)).await;

    match result {
        Ok(Err(_)) => {
            // Expected: authentication failure.
        }
        Ok(Ok(conn)) => {
            // Some lapin versions surface auth failure after connect handshake.
            // Attempting to open a channel should fail; either way drop the conn.
            let ch = conn.create_channel().await;
            assert!(ch.is_err(), "bad credentials must not open a channel");
            let _ = conn.close(200, "nope").await;
            panic!("connect unexpectedly succeeded with bad credentials");
        }
        Err(_) => panic!("connect timed out (expected auth failure)"),
    }

    broker.listener.abort();
}

/// Client `heartbeat=0` must disable heartbeats (Issue 1): connection stays up
/// without the server forcing a close for "missed" heartbeats.
#[tokio::test]
async fn lapin_heartbeat_zero_disables() {
    let broker = start_test_broker().await;
    let port = broker.port;

    let uri = format!(
        "amqp://{DEV_BOOTSTRAP_USER}:{DEV_BOOTSTRAP_PASSWORD}@127.0.0.1:{port}/%2f?heartbeat=0"
    );
    let options = ConnectionProperties::default()
        .with_executor(TokioExecutor::current())
        .with_reactor(TokioReactor);

    let conn = tokio::time::timeout(Duration::from_secs(10), Connection::connect(&uri, options))
        .await
        .expect("connect timed out")
        .expect("lapin connect with heartbeat=0");

    assert!(conn.status().connected());
    let channel = conn.create_channel().await.expect("create_channel");
    assert!(channel.status().connected());

    // Stay idle briefly; with heartbeat disabled the server must not force-close.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        conn.status().connected(),
        "heartbeat=0 must not force-close"
    );

    channel.close(200, "bye").await.ok();
    conn.close(200, "bye").await.ok();
    broker.listener.abort();
}

#[tokio::test]
async fn lapin_queue_declare_and_delete() {
    use lapin::options::{QueueDeclareOptions, QueueDeleteOptions};
    use lapin::types::FieldTable;

    let broker = start_test_broker().await;
    let port = broker.port;

    let uri = amqp_url(port, DEV_BOOTSTRAP_USER, DEV_BOOTSTRAP_PASSWORD);
    let options = ConnectionProperties::default()
        .with_executor(TokioExecutor::current())
        .with_reactor(TokioReactor);

    let conn = tokio::time::timeout(Duration::from_secs(10), Connection::connect(&uri, options))
        .await
        .expect("connect timed out")
        .expect("lapin connect");
    let channel = conn.create_channel().await.expect("create_channel");

    let queue = tokio::time::timeout(
        Duration::from_secs(5),
        channel.queue_declare(
            "pr7a-test-q",
            QueueDeclareOptions {
                durable: false,
                exclusive: false,
                auto_delete: false,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        ),
    )
    .await
    .expect("declare timed out")
    .expect("queue.declare");
    assert_eq!(queue.name().as_str(), "pr7a-test-q");

    let deleted = channel
        .queue_delete("pr7a-test-q", QueueDeleteOptions::default())
        .await
        .expect("queue.delete");
    assert_eq!(deleted, 0);

    channel.close(200, "bye").await.ok();
    conn.close(200, "bye").await.ok();
    broker.listener.abort();
}

#[tokio::test]
async fn lapin_publish_consume_ack() {
    use futures_lite::stream::StreamExt;
    use lapin::options::{
        BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, QueueDeclareOptions,
    };
    use lapin::types::FieldTable;
    use lapin::BasicProperties;

    let broker = start_test_broker().await;
    let port = broker.port;

    let uri = amqp_url(port, DEV_BOOTSTRAP_USER, DEV_BOOTSTRAP_PASSWORD);
    let options = ConnectionProperties::default()
        .with_executor(TokioExecutor::current())
        .with_reactor(TokioReactor);

    let conn = tokio::time::timeout(Duration::from_secs(10), Connection::connect(&uri, options))
        .await
        .expect("connect timed out")
        .expect("lapin connect");
    let channel = conn.create_channel().await.expect("create_channel");

    channel
        .queue_declare(
            "pr7b-smoke",
            QueueDeclareOptions {
                durable: false,
                exclusive: false,
                auto_delete: false,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("queue.declare");

    // Publish via default exchange (routing key = queue name).
    channel
        .basic_publish(
            "",
            "pr7b-smoke",
            BasicPublishOptions::default(),
            b"hello-pr7b",
            BasicProperties::default(),
        )
        .await
        .expect("basic.publish")
        .await
        .expect("publish confirm wait");

    let mut consumer = channel
        .basic_consume(
            "pr7b-smoke",
            "ctag-smoke",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("basic.consume");

    let delivery = tokio::time::timeout(Duration::from_secs(5), consumer.next())
        .await
        .expect("consume timed out")
        .expect("consumer closed")
        .expect("delivery error");

    assert_eq!(delivery.data.as_slice(), b"hello-pr7b");
    delivery
        .ack(BasicAckOptions::default())
        .await
        .expect("basic.ack");

    channel.close(200, "bye").await.ok();
    conn.close(200, "bye").await.ok();
    broker.listener.abort();
}

#[tokio::test]
async fn lapin_fanout_exchange_bind_publish() {
    use futures_lite::stream::StreamExt;
    use lapin::options::{
        BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, ExchangeDeclareOptions,
        QueueBindOptions, QueueDeclareOptions,
    };
    use lapin::types::FieldTable;
    use lapin::{BasicProperties, ExchangeKind};

    let broker = start_test_broker().await;
    let port = broker.port;

    let uri = amqp_url(port, DEV_BOOTSTRAP_USER, DEV_BOOTSTRAP_PASSWORD);
    let options = ConnectionProperties::default()
        .with_executor(TokioExecutor::current())
        .with_reactor(TokioReactor);

    let conn = Connection::connect(&uri, options)
        .await
        .expect("lapin connect");
    let channel = conn.create_channel().await.expect("create_channel");

    channel
        .exchange_declare(
            "pr8.fanout",
            ExchangeKind::Fanout,
            ExchangeDeclareOptions {
                durable: false,
                auto_delete: false,
                ..ExchangeDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("exchange.declare fanout");

    for name in ["pr8.fa", "pr8.fb"] {
        channel
            .queue_declare(
                name,
                QueueDeclareOptions {
                    durable: false,
                    exclusive: false,
                    auto_delete: false,
                    ..QueueDeclareOptions::default()
                },
                FieldTable::default(),
            )
            .await
            .expect("queue.declare");
        channel
            .queue_bind(
                name,
                "pr8.fanout",
                "",
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .expect("queue.bind");
    }

    channel
        .basic_publish(
            "pr8.fanout",
            "ignored-rk",
            BasicPublishOptions::default(),
            b"fanout-msg",
            BasicProperties::default(),
        )
        .await
        .expect("basic.publish")
        .await
        .expect("publish confirm wait");

    for name in ["pr8.fa", "pr8.fb"] {
        let mut consumer = channel
            .basic_consume(
                name,
                &format!("ctag-{name}"),
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .expect("basic.consume");
        let delivery = tokio::time::timeout(Duration::from_secs(5), consumer.next())
            .await
            .expect("consume timed out")
            .expect("consumer closed")
            .expect("delivery error");
        assert_eq!(delivery.data.as_slice(), b"fanout-msg");
        delivery
            .ack(BasicAckOptions::default())
            .await
            .expect("basic.ack");
    }

    channel.close(200, "bye").await.ok();
    conn.close(200, "bye").await.ok();
    broker.listener.abort();
}

#[tokio::test]
async fn lapin_topic_exchange_matching() {
    use futures_lite::stream::StreamExt;
    use lapin::options::{
        BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, ExchangeDeclareOptions,
        QueueBindOptions, QueueDeclareOptions,
    };
    use lapin::types::FieldTable;
    use lapin::{BasicProperties, ExchangeKind};

    let broker = start_test_broker().await;
    let port = broker.port;

    let uri = amqp_url(port, DEV_BOOTSTRAP_USER, DEV_BOOTSTRAP_PASSWORD);
    let options = ConnectionProperties::default()
        .with_executor(TokioExecutor::current())
        .with_reactor(TokioReactor);

    let conn = Connection::connect(&uri, options)
        .await
        .expect("lapin connect");
    let channel = conn.create_channel().await.expect("create_channel");

    channel
        .exchange_declare(
            "pr8.topic",
            ExchangeKind::Topic,
            ExchangeDeclareOptions {
                durable: false,
                auto_delete: false,
                ..ExchangeDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("exchange.declare topic");

    channel
        .queue_declare(
            "pr8.logs",
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("queue.declare");
    channel
        .queue_bind(
            "pr8.logs",
            "pr8.topic",
            "logs.*.error",
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("queue.bind");

    channel
        .queue_declare(
            "pr8.all",
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("queue.declare");
    channel
        .queue_bind(
            "pr8.all",
            "pr8.topic",
            "logs.#",
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("queue.bind");

    // Matches both * and #
    channel
        .basic_publish(
            "pr8.topic",
            "logs.app.error",
            BasicPublishOptions::default(),
            b"topic-hit",
            BasicProperties::default(),
        )
        .await
        .expect("basic.publish")
        .await
        .expect("publish confirm wait");

    // Matches only #
    channel
        .basic_publish(
            "pr8.topic",
            "logs.app.warn.detail",
            BasicPublishOptions::default(),
            b"topic-hash-only",
            BasicProperties::default(),
        )
        .await
        .expect("basic.publish")
        .await
        .expect("publish confirm wait");

    // Consume from logs.*
    let mut c_logs = channel
        .basic_consume(
            "pr8.logs",
            "ctag-logs",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("consume logs");
    let d1 = tokio::time::timeout(Duration::from_secs(5), c_logs.next())
        .await
        .expect("timeout")
        .expect("closed")
        .expect("err");
    assert_eq!(d1.data.as_slice(), b"topic-hit");
    d1.ack(BasicAckOptions::default()).await.ok();

    // all should have two messages
    let mut c_all = channel
        .basic_consume(
            "pr8.all",
            "ctag-all",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("consume all");
    let mut bodies = Vec::new();
    for _ in 0..2 {
        let d = tokio::time::timeout(Duration::from_secs(5), c_all.next())
            .await
            .expect("timeout")
            .expect("closed")
            .expect("err");
        bodies.push(d.data.clone());
        d.ack(BasicAckOptions::default()).await.ok();
    }
    assert!(bodies.iter().any(|b| b.as_slice() == b"topic-hit"));
    assert!(bodies.iter().any(|b| b.as_slice() == b"topic-hash-only"));

    channel.close(200, "bye").await.ok();
    conn.close(200, "bye").await.ok();
    broker.listener.abort();
}

#[tokio::test]
async fn lapin_direct_bind_and_unbind() {
    use futures_lite::stream::StreamExt;
    use lapin::options::{
        BasicConsumeOptions, BasicPublishOptions, ExchangeDeclareOptions, QueueBindOptions,
        QueueDeclareOptions,
    };
    use lapin::types::FieldTable;
    use lapin::{BasicProperties, ExchangeKind};

    let broker = start_test_broker().await;
    let port = broker.port;

    let uri = amqp_url(port, DEV_BOOTSTRAP_USER, DEV_BOOTSTRAP_PASSWORD);
    let options = ConnectionProperties::default()
        .with_executor(TokioExecutor::current())
        .with_reactor(TokioReactor);

    let conn = Connection::connect(&uri, options)
        .await
        .expect("lapin connect");
    let channel = conn.create_channel().await.expect("create_channel");

    channel
        .exchange_declare(
            "pr8.direct",
            ExchangeKind::Direct,
            ExchangeDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("exchange.declare");
    channel
        .queue_declare(
            "pr8.dq",
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("queue.declare");
    channel
        .queue_bind(
            "pr8.dq",
            "pr8.direct",
            "rk.one",
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("queue.bind");

    channel
        .basic_publish(
            "pr8.direct",
            "rk.one",
            BasicPublishOptions::default(),
            b"bound",
            BasicProperties::default(),
        )
        .await
        .expect("publish")
        .await
        .expect("confirm");

    let mut consumer = channel
        .basic_consume(
            "pr8.dq",
            "ctag-d",
            BasicConsumeOptions {
                no_ack: true,
                ..BasicConsumeOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("consume");
    let d = tokio::time::timeout(Duration::from_secs(5), consumer.next())
        .await
        .expect("timeout")
        .expect("closed")
        .expect("err");
    assert_eq!(d.data.as_slice(), b"bound");

    channel
        .queue_unbind("pr8.dq", "pr8.direct", "rk.one", FieldTable::default())
        .await
        .expect("queue.unbind");

    // After unbind, non-mandatory publish is dropped (no delivery).
    channel
        .basic_publish(
            "pr8.direct",
            "rk.one",
            BasicPublishOptions::default(),
            b"unbound",
            BasicProperties::default(),
        )
        .await
        .expect("publish")
        .await
        .expect("confirm");

    let timed = tokio::time::timeout(Duration::from_millis(400), consumer.next()).await;
    assert!(
        timed.is_err(),
        "no delivery expected after unbind for non-mandatory publish"
    );

    channel.close(200, "bye").await.ok();
    conn.close(200, "bye").await.ok();
    broker.listener.abort();
}

#[tokio::test]
async fn lapin_mandatory_return_unroutable() {
    use lapin::options::{BasicPublishOptions, ExchangeDeclareOptions};
    use lapin::types::FieldTable;
    use lapin::{BasicProperties, ExchangeKind};

    let broker = start_test_broker().await;
    let port = broker.port;

    let uri = amqp_url(port, DEV_BOOTSTRAP_USER, DEV_BOOTSTRAP_PASSWORD);
    let options = ConnectionProperties::default()
        .with_executor(TokioExecutor::current())
        .with_reactor(TokioReactor);

    let conn = Connection::connect(&uri, options)
        .await
        .expect("lapin connect");
    let channel = conn.create_channel().await.expect("create_channel");

    channel
        .exchange_declare(
            "pr8.mandatory",
            ExchangeKind::Direct,
            ExchangeDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("exchange.declare");

    // No bindings → mandatory publish must basic.return 312 and keep channel open.
    channel
        .basic_publish(
            "pr8.mandatory",
            "no.such.rk",
            BasicPublishOptions {
                mandatory: true,
                immediate: false,
            },
            b"return-me",
            BasicProperties::default(),
        )
        .await
        .expect("basic.publish")
        .await
        .expect("publish future");

    // Allow the return content frames to land in lapin's non-confirm buffer.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let returned = channel
        .wait_for_confirms()
        .await
        .expect("wait_for_confirms drains returned messages");
    assert!(
        !returned.is_empty(),
        "expected basic.return for mandatory unroutable"
    );
    assert_eq!(returned[0].reply_code, 312, "NO_ROUTE");
    assert!(channel.status().connected(), "channel must stay open");

    channel.close(200, "bye").await.ok();
    conn.close(200, "bye").await.ok();
    broker.listener.abort();
}

#[tokio::test]
async fn lapin_multi_dest_partial_unavailable_closes_channel() {
    use lapin::options::{
        BasicPublishOptions, ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions,
    };
    use lapin::types::FieldTable;
    use lapin::{BasicProperties, ExchangeKind};

    let broker = start_test_broker().await;
    let port = broker.port;
    let queues = Arc::clone(&broker.queues);

    let uri = amqp_url(port, DEV_BOOTSTRAP_USER, DEV_BOOTSTRAP_PASSWORD);
    let options = ConnectionProperties::default()
        .with_executor(TokioExecutor::current())
        .with_reactor(TokioReactor);

    let conn = Connection::connect(&uri, options)
        .await
        .expect("lapin connect");
    let channel = conn.create_channel().await.expect("create_channel");

    channel
        .exchange_declare(
            "pr8.partial",
            ExchangeKind::Fanout,
            ExchangeDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("exchange.declare");

    for name in ["pr8.ok", "pr8.dead"] {
        channel
            .queue_declare(name, QueueDeclareOptions::default(), FieldTable::default())
            .await
            .expect("queue.declare");
        channel
            .queue_bind(
                name,
                "pr8.partial",
                "",
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .expect("queue.bind");
    }

    // Mark one bound queue unavailable (panic isolation) so multi-dest wait-all
    // must surface partial failure as channel 541 — not silent subset success.
    let dead = queues
        .get(&QueueKey::new("/", "pr8.dead"))
        .expect("dead queue registered");
    dead.info.mark_unavailable();

    let publish = channel
        .basic_publish(
            "pr8.partial",
            "",
            BasicPublishOptions::default(),
            b"partial",
            BasicProperties::default(),
        )
        .await
        .expect("basic.publish send");

    // Publish may complete from the client's view; the channel should then close
    // with 541 once the server finishes wait-all and detects the failed dest.
    let _ = publish.await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Channel should no longer be usable after partial multi-dest failure.
    assert!(
        !channel.status().connected()
            || channel
                .queue_declare(
                    "pr8.after-fail",
                    QueueDeclareOptions::default(),
                    FieldTable::default(),
                )
                .await
                .is_err(),
        "partial multi-dest failure must close the channel (541)"
    );

    conn.close(200, "bye").await.ok();
    broker.listener.abort();
}

#[tokio::test]
async fn lapin_publisher_confirms_ack() {
    use futures_lite::stream::StreamExt;
    use lapin::options::{
        BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, ConfirmSelectOptions,
        QueueDeclareOptions,
    };
    use lapin::types::FieldTable;
    use lapin::BasicProperties;

    let broker = start_test_broker().await;
    let port = broker.port;

    let uri = amqp_url(port, DEV_BOOTSTRAP_USER, DEV_BOOTSTRAP_PASSWORD);
    let options = ConnectionProperties::default()
        .with_executor(TokioExecutor::current())
        .with_reactor(TokioReactor);

    let conn = Connection::connect(&uri, options)
        .await
        .expect("lapin connect");
    let channel = conn.create_channel().await.expect("create_channel");

    channel
        .confirm_select(ConfirmSelectOptions::default())
        .await
        .expect("confirm.select");

    channel
        .queue_declare(
            "pr10-confirm",
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("queue.declare");

    let confirm = channel
        .basic_publish(
            "",
            "pr10-confirm",
            BasicPublishOptions::default(),
            b"confirmed-payload",
            BasicProperties::default(),
        )
        .await
        .expect("basic.publish")
        .await
        .expect("await publisher confirm");
    assert!(
        confirm.is_ack(),
        "successful enqueue must map EnqueueCompletion to basic.ack"
    );

    // Second publish gets sequence 2; still acked.
    let confirm2 = channel
        .basic_publish(
            "",
            "pr10-confirm",
            BasicPublishOptions::default(),
            b"second",
            BasicProperties::default(),
        )
        .await
        .expect("basic.publish 2")
        .await
        .expect("confirm 2");
    assert!(confirm2.is_ack());

    let mut consumer = channel
        .basic_consume(
            "pr10-confirm",
            "ctag-confirm",
            BasicConsumeOptions {
                no_ack: false,
                ..BasicConsumeOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("consume");
    let d = tokio::time::timeout(Duration::from_secs(5), consumer.next())
        .await
        .expect("timeout")
        .expect("closed")
        .expect("err");
    assert_eq!(d.data.as_slice(), b"confirmed-payload");
    d.ack(BasicAckOptions::default())
        .await
        .expect("consumer ack");

    channel.close(200, "bye").await.ok();
    conn.close(200, "bye").await.ok();
    broker.listener.abort();
}

#[tokio::test]
async fn lapin_publisher_confirms_nack_on_partial_failure() {
    use lapin::options::{
        BasicPublishOptions, ConfirmSelectOptions, ExchangeDeclareOptions, QueueBindOptions,
        QueueDeclareOptions,
    };
    use lapin::types::FieldTable;
    use lapin::{BasicProperties, ExchangeKind};

    let broker = start_test_broker().await;
    let port = broker.port;
    let queues = Arc::clone(&broker.queues);

    let uri = amqp_url(port, DEV_BOOTSTRAP_USER, DEV_BOOTSTRAP_PASSWORD);
    let options = ConnectionProperties::default()
        .with_executor(TokioExecutor::current())
        .with_reactor(TokioReactor);

    let conn = Connection::connect(&uri, options)
        .await
        .expect("lapin connect");
    let channel = conn.create_channel().await.expect("create_channel");

    channel
        .confirm_select(ConfirmSelectOptions::default())
        .await
        .expect("confirm.select");

    channel
        .exchange_declare(
            "pr10.partial",
            ExchangeKind::Fanout,
            ExchangeDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("exchange.declare");

    for name in ["pr10.ok", "pr10.dead"] {
        channel
            .queue_declare(name, QueueDeclareOptions::default(), FieldTable::default())
            .await
            .expect("queue.declare");
        channel
            .queue_bind(
                name,
                "pr10.partial",
                "",
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .expect("queue.bind");
    }

    let dead = queues
        .get(&QueueKey::new("/", "pr10.dead"))
        .expect("dead queue registered");
    dead.info.mark_unavailable();

    let confirm = channel
        .basic_publish(
            "pr10.partial",
            "",
            BasicPublishOptions::default(),
            b"partial-confirm",
            BasicProperties::default(),
        )
        .await
        .expect("basic.publish send")
        .await
        .expect("publisher confirm future");

    assert!(
        confirm.is_nack(),
        "partial multi-dest failure with confirms on must basic.nack (not channel 541)"
    );
    assert!(
        channel.status().connected(),
        "channel must stay open after confirm-mode nack"
    );

    // Channel remains usable: declare after nack must succeed.
    channel
        .queue_declare(
            "pr10.after-nack",
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("channel still open after nack");

    channel.close(200, "bye").await.ok();
    conn.close(200, "bye").await.ok();
    broker.listener.abort();
}

#[tokio::test]
async fn lapin_publisher_confirms_durable_ack() {
    use lapin::options::{BasicPublishOptions, ConfirmSelectOptions, QueueDeclareOptions};
    use lapin::types::FieldTable;
    use lapin::BasicProperties;

    let broker = start_test_broker().await;
    let port = broker.port;

    let uri = amqp_url(port, DEV_BOOTSTRAP_USER, DEV_BOOTSTRAP_PASSWORD);
    let options = ConnectionProperties::default()
        .with_executor(TokioExecutor::current())
        .with_reactor(TokioReactor);

    let conn = Connection::connect(&uri, options)
        .await
        .expect("lapin connect");
    let channel = conn.create_channel().await.expect("create_channel");

    channel
        .confirm_select(ConfirmSelectOptions::default())
        .await
        .expect("confirm.select");

    channel
        .queue_declare(
            "pr10-durable",
            QueueDeclareOptions {
                durable: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("declare durable");

    // Persistent publish: confirm must wait EnqueueCompletion.durable_done (fsync).
    let confirm = channel
        .basic_publish(
            "",
            "pr10-durable",
            BasicPublishOptions::default(),
            b"durable-confirmed",
            BasicProperties::default().with_delivery_mode(2),
        )
        .await
        .expect("publish")
        .await
        .expect("confirm wait");
    assert!(
        confirm.is_ack(),
        "durable persistent publish must ack after durable_done"
    );

    channel.close(200, "bye").await.ok();
    conn.close(200, "bye").await.ok();
    broker.listener.abort();
}

/// Broker with a tight max_connections and custom max_message_bytes.
async fn start_test_broker_with_limits(max_connections: u32, max_message_bytes: u64) -> TestBroker {
    let dir = TempDir::new().expect("tempdir");
    let store = MetadataStore::open(dir.path()).expect("open store");
    let auth = AuthService::new(&store);
    assert!(auth
        .bootstrap_admin_if_empty(BootstrapMode::DevFallback)
        .expect("bootstrap"));

    let store = Arc::new(store);
    let queues = QueueRegistry::shared(
        Arc::clone(&store) as Arc<dyn QueueMetaStore>,
        MemoryTracker::shared(),
    );
    let router = Arc::new(store.bootstrap_router().expect("bootstrap router"));
    let connections = ConnectionTracker::shared();
    let params = ConnectionParams {
        max_message_bytes,
        ..ConnectionParams::default()
    };
    let limiter = ConnectionLimiter::shared(max_connections);
    let listener = start_amqp_listener_with_limits(
        "127.0.0.1:0".parse().unwrap(),
        store,
        Arc::clone(&queues),
        router,
        connections,
        params,
        Some(limiter),
        None,
    )
    .await
    .expect("bind amqp");
    let port = listener.local_addr.port();
    TestBroker {
        _dir: dir,
        listener,
        port,
        queues,
    }
}

#[tokio::test]
async fn max_connections_refuses_extra() {
    let broker = start_test_broker_with_limits(1, 16 * 1024 * 1024).await;
    let port = broker.port;

    let uri = amqp_url(port, DEV_BOOTSTRAP_USER, DEV_BOOTSTRAP_PASSWORD);
    let options = ConnectionProperties::default()
        .with_executor(TokioExecutor::current())
        .with_reactor(TokioReactor);

    let conn1 = tokio::time::timeout(
        Duration::from_secs(10),
        Connection::connect(&uri, options.clone()),
    )
    .await
    .expect("connect timed out")
    .expect("first connect should succeed");

    // Second connection should be refused (TCP drop / connect error).
    let conn2 =
        tokio::time::timeout(Duration::from_secs(5), Connection::connect(&uri, options)).await;
    match conn2 {
        Ok(Ok(_c)) => {
            panic!("second connection unexpectedly succeeded under max_connections=1");
        }
        Ok(Err(_)) | Err(_) => {
            // Expected: refused or timed out.
        }
    }

    conn1.close(200, "bye").await.ok();
}

#[tokio::test]
async fn max_message_bytes_rejected() {
    use lapin::options::{BasicPublishOptions, QueueDeclareOptions};
    use lapin::types::FieldTable;
    use lapin::BasicProperties;

    let broker = start_test_broker_with_limits(100, 64).await;
    let port = broker.port;
    let uri = amqp_url(port, DEV_BOOTSTRAP_USER, DEV_BOOTSTRAP_PASSWORD);
    let options = ConnectionProperties::default()
        .with_executor(TokioExecutor::current())
        .with_reactor(TokioReactor);

    let conn = tokio::time::timeout(Duration::from_secs(10), Connection::connect(&uri, options))
        .await
        .expect("connect timed out")
        .expect("lapin connect");
    let channel = conn.create_channel().await.expect("channel");

    channel
        .queue_declare(
            "tiny-limit-q",
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("declare");

    let body = vec![b'x'; 128]; // exceeds max_message_bytes=64
    let pub_result = channel
        .basic_publish(
            "",
            "tiny-limit-q",
            BasicPublishOptions::default(),
            &body,
            BasicProperties::default(),
        )
        .await;

    // Server closes the channel with 406 PRECONDITION_FAILED after the content
    // header; lapin may error on publish or later on the confirm / channel state.
    let mut channel_died = false;
    match pub_result {
        Err(_) => channel_died = true,
        Ok(confirm) => {
            let _ = tokio::time::timeout(Duration::from_secs(2), confirm).await;
            // Poll channel status briefly.
            for _ in 0..40 {
                if !channel.status().connected() {
                    channel_died = true;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
    assert!(
        channel_died,
        "oversized message should close channel or fail publish"
    );

    conn.close(200, "bye").await.ok();
}
