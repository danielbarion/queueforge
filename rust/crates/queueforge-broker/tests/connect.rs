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

#[tokio::test]
async fn lapin_application_header_round_trip() {
    use futures_lite::stream::StreamExt;
    use lapin::options::{BasicConsumeOptions, BasicPublishOptions, QueueDeclareOptions};
    use lapin::types::{AMQPValue, FieldTable, ShortString};
    use lapin::BasicProperties;

    let broker = start_test_broker().await;
    let conn = connect_test(broker.port).await;
    let channel = conn.create_channel().await.expect("channel");
    channel
        .queue_declare(
            "hdr-q",
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("declare");

    let mut headers = FieldTable::default();
    headers.insert(
        ShortString::from("trace-id"),
        AMQPValue::LongString("abc-123".into()),
    );
    channel
        .basic_publish(
            "",
            "hdr-q",
            BasicPublishOptions::default(),
            b"with-header",
            BasicProperties::default().with_headers(headers),
        )
        .await
        .expect("publish")
        .await
        .expect("confirm");

    let mut consumer = channel
        .basic_consume(
            "hdr-q",
            "ctag-hdr",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("consume");
    let delivery = tokio::time::timeout(Duration::from_secs(5), consumer.next())
        .await
        .expect("timeout")
        .expect("closed")
        .expect("delivery");
    let table = delivery.properties.headers().clone().expect("headers");
    match table.inner().get(&ShortString::from("trace-id")) {
        Some(AMQPValue::LongString(s)) => assert_eq!(s.as_bytes(), b"abc-123"),
        other => panic!("trace-id missing or wrong type: {other:?}"),
    }
    conn.close(200, "bye").await.ok();
    broker.listener.abort();
}

#[tokio::test]
async fn lapin_channel_flow_holds_publish() {
    use lapin::options::{BasicPublishOptions, QueueDeclareOptions};
    use lapin::types::FieldTable;
    use lapin::BasicProperties;

    let broker = start_test_broker().await;
    let conn = connect_test(broker.port).await;
    let channel = conn.create_channel().await.expect("channel");
    channel
        .queue_declare(
            "flow-q",
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("declare");

    // lapin will not write a publish while it considers the channel flowed
    // off, so this checks the server answers flow and then accepts a publish
    // once flow is on again.
    tokio::time::timeout(
        Duration::from_secs(3),
        channel.channel_flow(lapin::options::ChannelFlowOptions { active: false }),
    )
    .await
    .expect("flow off timed out")
    .expect("flow off");
    tokio::time::timeout(
        Duration::from_secs(3),
        channel.channel_flow(lapin::options::ChannelFlowOptions { active: true }),
    )
    .await
    .expect("flow on timed out")
    .expect("flow on");
    channel
        .basic_publish(
            "",
            "flow-q",
            BasicPublishOptions::default(),
            b"after-flow",
            BasicProperties::default(),
        )
        .await
        .expect("publish")
        .await
        .expect("confirm");
    let info = channel
        .queue_declare(
            "flow-q",
            QueueDeclareOptions {
                passive: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("passive");
    assert!(info.message_count() >= 1, "publish after flow resumed");
    conn.close(200, "bye").await.ok();
    broker.listener.abort();
}

#[tokio::test]
async fn lapin_global_qos_caps_two_channels() {
    use futures_lite::stream::StreamExt;
    use lapin::options::{
        BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, BasicQosOptions,
        QueueDeclareOptions,
    };
    use lapin::types::FieldTable;
    use lapin::BasicProperties;

    let broker = start_test_broker().await;
    let conn = connect_test(broker.port).await;
    let setup = conn.create_channel().await.expect("setup");
    setup
        .queue_declare(
            "gqos-q",
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("declare");

    let ch_a = conn.create_channel().await.expect("ch a");
    ch_a.basic_qos(1, BasicQosOptions { global: true })
        .await
        .expect("qos");

    let mut cons_a = ch_a
        .basic_consume(
            "gqos-q",
            "a",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("consume a");
    let mut cons_b = ch_a
        .basic_consume(
            "gqos-q",
            "b",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("consume b");

    for body in [b"one".as_slice(), b"two".as_slice()] {
        setup
            .basic_publish(
                "",
                "gqos-q",
                BasicPublishOptions::default(),
                body,
                BasicProperties::default(),
            )
            .await
            .expect("publish")
            .await
            .expect("confirm");
    }

    let first = tokio::time::timeout(Duration::from_secs(3), async {
        tokio::select! {
            d = cons_a.next() => d,
            d = cons_b.next() => d,
        }
    })
    .await
    .expect("first delivery")
    .expect("stream")
    .expect("delivery");

    let second = tokio::time::timeout(Duration::from_millis(300), async {
        tokio::select! {
            d = cons_a.next() => d,
            d = cons_b.next() => d,
        }
    })
    .await;
    assert!(
        second.is_err(),
        "channel-global prefetch 1 should hold the second delivery"
    );

    first.ack(BasicAckOptions::default()).await.expect("ack");
    let after = tokio::time::timeout(Duration::from_secs(3), async {
        tokio::select! {
            d = cons_a.next() => d,
            d = cons_b.next() => d,
        }
    })
    .await
    .expect("second delivery after ack");
    assert!(after.is_some());

    conn.close(200, "bye").await.ok();
    broker.listener.abort();
}

async fn connect_test(port: u16) -> Connection {
    let uri = amqp_url(port, DEV_BOOTSTRAP_USER, DEV_BOOTSTRAP_PASSWORD);
    let options = ConnectionProperties::default()
        .with_executor(TokioExecutor::current())
        .with_reactor(TokioReactor);
    tokio::time::timeout(Duration::from_secs(10), Connection::connect(&uri, options))
        .await
        .expect("connect timed out")
        .expect("lapin connect")
}

#[tokio::test]
async fn two_connections_consume_one_queue() {
    use futures_lite::stream::StreamExt;
    use lapin::options::{
        BasicConsumeOptions, BasicPublishOptions, BasicQosOptions, QueueDeclareOptions,
    };
    use lapin::types::FieldTable;
    use lapin::BasicProperties;

    let broker = start_test_broker().await;
    let conn_a = connect_test(broker.port).await;
    let conn_b = connect_test(broker.port).await;
    let ch_a = conn_a.create_channel().await.expect("channel a");
    let ch_b = conn_b.create_channel().await.expect("channel b");
    ch_a.queue_declare(
        "shared-q",
        QueueDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .expect("declare");
    let qos = BasicQosOptions { global: false };
    ch_a.basic_qos(1, qos).await.expect("qos a");
    ch_b.basic_qos(1, qos).await.expect("qos b");
    let mut cons_a = ch_a
        .basic_consume(
            "shared-q",
            "conn-a",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("consume a");
    let mut cons_b = ch_b
        .basic_consume(
            "shared-q",
            "conn-b",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("consume b");

    for body in [b"one".as_slice(), b"two".as_slice()] {
        ch_a.basic_publish(
            "",
            "shared-q",
            BasicPublishOptions::default(),
            body,
            BasicProperties::default(),
        )
        .await
        .expect("publish")
        .await
        .expect("confirm");
    }

    let mut saw_a = false;
    let mut saw_b = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline && !(saw_a && saw_b) {
        tokio::select! {
            biased;
            d = cons_a.next() => {
                d.expect("consumer a closed").expect("delivery a");
                saw_a = true;
            }
            d = cons_b.next() => {
                d.expect("consumer b closed").expect("delivery b");
                saw_b = true;
            }
            _ = tokio::time::sleep_until(deadline) => break,
        }
    }
    assert!(saw_a && saw_b, "both connections must receive a delivery");
    conn_a.close(200, "bye").await.ok();
    conn_b.close(200, "bye").await.ok();
    broker.listener.abort();
}

#[tokio::test]
async fn flow_inactive_holds_publish_until_resume() {
    use lapin::options::{BasicPublishOptions, QueueDeclareOptions};
    use lapin::types::FieldTable;
    use lapin::BasicProperties;

    let broker = start_test_broker().await;
    let conn = connect_test(broker.port).await;
    let channel = conn.create_channel().await.expect("channel");
    let inspect = conn.create_channel().await.expect("inspect");
    channel
        .queue_declare(
            "flow-hold",
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("declare");

    tokio::time::timeout(
        Duration::from_secs(3),
        channel.channel_flow(lapin::options::ChannelFlowOptions { active: false }),
    )
    .await
    .expect("flow off timed out")
    .expect("flow off");

    let published = tokio::time::timeout(Duration::from_secs(3), async {
        channel
            .basic_publish(
                "",
                "flow-hold",
                BasicPublishOptions::default(),
                b"held",
                BasicProperties::default(),
            )
            .await
    })
    .await
    .expect("publish write timed out")
    .expect("publish write");
    let _ = tokio::time::timeout(Duration::from_millis(200), published).await;
    tokio::time::sleep(Duration::from_millis(150)).await;

    let held = inspect
        .queue_declare(
            "flow-hold",
            QueueDeclareOptions {
                passive: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("passive while held");
    assert_eq!(
        held.message_count(),
        0,
        "publish must not be stored while flow is off"
    );

    tokio::time::timeout(
        Duration::from_secs(3),
        channel.channel_flow(lapin::options::ChannelFlowOptions { active: true }),
    )
    .await
    .expect("flow on timed out")
    .expect("flow on");
    tokio::time::sleep(Duration::from_millis(200)).await;

    let resumed = inspect
        .queue_declare(
            "flow-hold",
            QueueDeclareOptions {
                passive: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("passive after resume");
    assert!(
        resumed.message_count() >= 1,
        "held publish must be stored after flow resumes"
    );
    conn.close(200, "bye").await.ok();
    broker.listener.abort();
}

#[tokio::test]
async fn per_consumer_prefetch_is_not_shared() {
    use futures_lite::stream::StreamExt;
    use lapin::options::{
        BasicConsumeOptions, BasicPublishOptions, BasicQosOptions, QueueDeclareOptions,
    };
    use lapin::types::FieldTable;
    use lapin::BasicProperties;

    let broker = start_test_broker().await;
    let conn = connect_test(broker.port).await;
    let ch = conn.create_channel().await.expect("channel");
    ch.queue_declare("per-c", QueueDeclareOptions::default(), FieldTable::default())
        .await
        .expect("declare");
    ch.basic_qos(1, BasicQosOptions { global: false })
        .await
        .expect("qos");
    let mut a = ch
        .basic_consume(
            "per-c",
            "ca",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("a");
    let mut b = ch
        .basic_consume(
            "per-c",
            "cb",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("b");
    for body in [b"one".as_slice(), b"two".as_slice()] {
        ch.basic_publish(
            "",
            "per-c",
            BasicPublishOptions::default(),
            body,
            BasicProperties::default(),
        )
        .await
        .expect("publish")
        .await
        .expect("confirm");
    }
    let mut saw_a = false;
    let mut saw_b = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while tokio::time::Instant::now() < deadline && !(saw_a && saw_b) {
        tokio::select! {
            d = a.next() => {
                d.expect("a closed").expect("a delivery");
                saw_a = true;
            }
            d = b.next() => {
                d.expect("b closed").expect("b delivery");
                saw_b = true;
            }
        }
    }
    assert!(saw_a && saw_b, "each consumer gets its own prefetch of 1");
    conn.close(200, "bye").await.ok();
    broker.listener.abort();
}

#[tokio::test]
async fn global_qos_zero_uses_server_default_across_channels() {
    use futures_lite::stream::StreamExt;
    use lapin::options::{
        BasicConsumeOptions, BasicPublishOptions, BasicQosOptions, QueueDeclareOptions,
    };
    use lapin::types::FieldTable;
    use lapin::BasicProperties;

    let broker = start_test_broker().await;
    let conn = connect_test(broker.port).await;
    let setup = conn.create_channel().await.expect("setup");
    setup
        .queue_declare(
            "gqos-default",
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("declare");
    let ch_a = conn.create_channel().await.expect("a");
    ch_a.basic_qos(0, BasicQosOptions { global: false })
        .await
        .expect("qos");
    let mut cons_a = ch_a
        .basic_consume(
            "gqos-default",
            "ga",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("consume a");
    let mut cons_b = ch_a
        .basic_consume(
            "gqos-default",
            "gb",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("consume b");

    const PUBLISHED: u32 = 300;
    for i in 0..PUBLISHED {
        let body = i.to_string();
        setup
            .basic_publish(
                "",
                "gqos-default",
                BasicPublishOptions::default(),
                body.as_bytes(),
                BasicProperties::default(),
            )
            .await
            .expect("publish")
            .await
            .expect("confirm");
    }

    let mut received = 0u32;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while tokio::time::Instant::now() < deadline {
        tokio::select! {
            biased;
            d = cons_a.next() => {
                if d.transpose().ok().flatten().is_some() {
                    received += 1;
                }
            }
            d = cons_b.next() => {
                if d.transpose().ok().flatten().is_some() {
                    received += 1;
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(50)) => {}
        }
    }
    assert!(
        received > 256,
        "prefetch 0 is unlimited and must deliver more than 256 unacked messages, got {received}"
    );

    conn.close(200, "bye").await.ok();
    broker.listener.abort();
}

#[tokio::test]
async fn global_qos_one_leaves_second_message_ready() {
    use futures_lite::stream::StreamExt;
    use lapin::options::{
        BasicConsumeOptions, BasicPublishOptions, BasicQosOptions, QueueDeclareOptions,
    };
    use lapin::types::FieldTable;
    use lapin::BasicProperties;

    let broker = start_test_broker().await;
    let conn = connect_test(broker.port).await;
    let setup = conn.create_channel().await.expect("setup");
    setup
        .queue_declare(
            "gqos-hold",
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("declare");
    let ch_a = conn.create_channel().await.expect("a");
    ch_a.basic_qos(1, BasicQosOptions { global: true })
        .await
        .expect("qos");
    let mut cons_a = ch_a
        .basic_consume(
            "gqos-hold",
            "ha",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("consume a");
    let mut cons_b = ch_a
        .basic_consume(
            "gqos-hold",
            "hb",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("consume b");
    for body in [b"one".as_slice(), b"two".as_slice()] {
        setup
            .basic_publish(
                "",
                "gqos-hold",
                BasicPublishOptions::default(),
                body,
                BasicProperties::default(),
            )
            .await
            .expect("publish")
            .await
            .expect("confirm");
    }

    let first = tokio::time::timeout(Duration::from_secs(3), async {
        tokio::select! {
            d = cons_a.next() => d,
            d = cons_b.next() => d,
        }
    })
    .await
    .expect("first delivery")
    .expect("stream")
    .expect("delivery");
    let _ = first;
    let second = tokio::time::timeout(Duration::from_millis(400), async {
        tokio::select! {
            d = cons_a.next() => d,
            d = cons_b.next() => d,
        }
    })
    .await;
    assert!(second.is_err(), "second delivery must stay unacked-capped");

    let info = setup
        .queue_declare(
            "gqos-hold",
            QueueDeclareOptions {
                passive: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("passive");
    assert_eq!(
        info.message_count(),
        1,
        "the second message must sit ready, not spin through deliver/nack"
    );

    conn.close(200, "bye").await.ok();
    broker.listener.abort();
}

#[tokio::test]
async fn failed_exclusive_consume_does_not_keep_global_credit() {
    use futures_lite::stream::StreamExt;
    use lapin::options::{
        BasicConsumeOptions, BasicPublishOptions, BasicQosOptions, QueueDeclareOptions,
    };
    use lapin::types::FieldTable;
    use lapin::BasicProperties;

    let broker = start_test_broker().await;
    let setup = connect_test(broker.port).await;
    let decl = setup.create_channel().await.expect("declare channel");
    for name in ["q-exclusive", "q-other"] {
        decl.queue_declare(name, QueueDeclareOptions::default(), FieldTable::default())
            .await
            .expect("declare");
    }

    let holder = connect_test(broker.port).await;
    let hold_ch = holder.create_channel().await.expect("holder");
    hold_ch
        .basic_qos(1, BasicQosOptions { global: true })
        .await
        .expect("holder qos");
    let _holder_consumer = hold_ch
        .basic_consume(
            "q-exclusive",
            "holder",
            BasicConsumeOptions {
                exclusive: true,
                ..BasicConsumeOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("holder consume");

    let conn = connect_test(broker.port).await;
    let failed = conn.create_channel().await.expect("failed channel");
    failed
        .basic_qos(1, BasicQosOptions { global: true })
        .await
        .expect("qos");
    let key = queueforge_core::QueueKey::new("/", "q-exclusive");
    let handle = broker.queues.get(&key).expect("queue");
    let (tx, rx) = tokio::sync::oneshot::channel();
    handle
        .tx
        .send(queueforge_core::QueueCmd::Stats { reply: tx })
        .await
        .expect("stats");
    let stats = rx.await.expect("stats reply");
    assert_eq!(stats.consumer_count, 1, "holder must be registered, {stats:?}");

    let rejected = failed
        .basic_consume(
            "q-exclusive",
            "second-exclusive",
            BasicConsumeOptions {
                exclusive: true,
                ..BasicConsumeOptions::default()
            },
            FieldTable::default(),
        )
        .await;
    assert!(
        rejected.is_err(),
        "second exclusive consumer must be rejected: {rejected:?}"
    );

    let other = conn.create_channel().await.expect("other channel");
    other
        .basic_qos(1, BasicQosOptions { global: true })
        .await
        .expect("other qos");
    let mut consumer = other
        .basic_consume(
            "q-other",
            "other",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("consume other");
    decl.basic_publish(
        "",
        "q-other",
        BasicPublishOptions::default(),
        b"still-credited",
        BasicProperties::default(),
    )
    .await
    .expect("publish")
    .await
    .expect("confirm");

    let delivery = tokio::time::timeout(Duration::from_secs(3), consumer.next())
        .await
        .expect("delivery timed out; failed consume kept the global credit")
        .expect("consumer closed")
        .expect("delivery");
    assert_eq!(delivery.data.as_slice(), b"still-credited");

    setup.close(200, "bye").await.ok();
    holder.close(200, "bye").await.ok();
    conn.close(200, "bye").await.ok();
    broker.listener.abort();
}

#[tokio::test]
async fn headers_exchange_and_alternate_exchange() {
    use futures_lite::stream::StreamExt;
    use lapin::options::{
        BasicConsumeOptions, BasicPublishOptions, ExchangeDeclareOptions, QueueBindOptions,
        QueueDeclareOptions,
    };
    use lapin::types::{AMQPValue, FieldTable, ShortString};
    use lapin::{BasicProperties, ExchangeKind};

    let broker = start_test_broker().await;
    let conn = connect_test(broker.port).await;
    let ch = conn.create_channel().await.expect("channel");

    ch.exchange_declare(
        "headers-ex",
        ExchangeKind::Headers,
        ExchangeDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .expect("headers exchange");
    ch.queue_declare("all-q", QueueDeclareOptions::default(), FieldTable::default())
        .await
        .expect("all-q");
    ch.queue_declare("any-q", QueueDeclareOptions::default(), FieldTable::default())
        .await
        .expect("any-q");

    let mut all_args = FieldTable::default();
    all_args.insert(
        ShortString::from("x-match"),
        AMQPValue::LongString("all".into()),
    );
    all_args.insert(
        ShortString::from("format"),
        AMQPValue::LongString("json".into()),
    );
    all_args.insert(
        ShortString::from("kind"),
        AMQPValue::LongString("order".into()),
    );
    ch.queue_bind(
        "all-q",
        "headers-ex",
        "ignored-routing-key",
        QueueBindOptions::default(),
        all_args,
    )
    .await
    .expect("bind all");

    let mut any_args = FieldTable::default();
    any_args.insert(
        ShortString::from("x-match"),
        AMQPValue::LongString("any".into()),
    );
    any_args.insert(
        ShortString::from("format"),
        AMQPValue::LongString("json".into()),
    );
    ch.queue_bind("any-q", "headers-ex", "", QueueBindOptions::default(), any_args)
        .await
        .expect("bind any");

    let mut all_consumer = ch
        .basic_consume(
            "all-q",
            "all-c",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("consume all");
    let mut any_consumer = ch
        .basic_consume(
            "any-q",
            "any-c",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("consume any");

    let mut both = FieldTable::default();
    both.insert(
        ShortString::from("format"),
        AMQPValue::LongString("json".into()),
    );
    both.insert(
        ShortString::from("kind"),
        AMQPValue::LongString("order".into()),
    );
    ch.basic_publish(
        "headers-ex",
        "not-used",
        BasicPublishOptions::default(),
        b"both",
        BasicProperties::default().with_headers(both),
    )
    .await
    .expect("publish both")
    .await
    .ok();

    let all_delivery = tokio::time::timeout(Duration::from_secs(3), all_consumer.next())
        .await
        .expect("all timeout")
        .expect("all closed")
        .expect("all delivery");
    assert_eq!(all_delivery.data.as_slice(), b"both");
    let any_delivery = tokio::time::timeout(Duration::from_secs(3), any_consumer.next())
        .await
        .expect("any timeout")
        .expect("any closed")
        .expect("any delivery");
    assert_eq!(any_delivery.data.as_slice(), b"both");

    let mut only_kind = FieldTable::default();
    only_kind.insert(
        ShortString::from("kind"),
        AMQPValue::LongString("order".into()),
    );
    ch.basic_publish(
        "headers-ex",
        "",
        BasicPublishOptions::default(),
        b"partial",
        BasicProperties::default().with_headers(only_kind),
    )
    .await
    .expect("publish partial")
    .await
    .ok();
    let partial_all = tokio::time::timeout(Duration::from_millis(300), all_consumer.next()).await;
    assert!(partial_all.is_err(), "x-match=all must reject a partial header set");
    let partial_any = tokio::time::timeout(Duration::from_millis(300), any_consumer.next()).await;
    assert!(
        partial_any.is_err(),
        "x-match=any must not match a different header"
    );

    let mut alt_args = FieldTable::default();
    alt_args.insert(
        ShortString::from("alternate-exchange"),
        AMQPValue::LongString("alt-ex".into()),
    );
    ch.exchange_declare(
        "main-ex",
        ExchangeKind::Direct,
        ExchangeDeclareOptions::default(),
        alt_args,
    )
    .await
    .expect("main");
    ch.exchange_declare(
        "alt-ex",
        ExchangeKind::Fanout,
        ExchangeDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .expect("alt");
    ch.queue_declare("alt-q", QueueDeclareOptions::default(), FieldTable::default())
        .await
        .expect("alt-q");
    ch.queue_bind(
        "alt-q",
        "alt-ex",
        "",
        QueueBindOptions::default(),
        FieldTable::default(),
    )
    .await
    .expect("bind alt");
    let mut alt_consumer = ch
        .basic_consume(
            "alt-q",
            "alt-c",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("consume alt");
    ch.basic_publish(
        "main-ex",
        "no-such-key",
        BasicPublishOptions {
            mandatory: true,
            immediate: false,
        },
        b"via-alt",
        BasicProperties::default(),
    )
    .await
    .expect("publish alt")
    .await
    .ok();
    let alt_delivery = tokio::time::timeout(Duration::from_secs(3), alt_consumer.next())
        .await
        .expect("alt timeout")
        .expect("alt closed")
        .expect("alt delivery");
    assert_eq!(alt_delivery.data.as_slice(), b"via-alt");

    let mut cycle_main = FieldTable::default();
    cycle_main.insert(
        ShortString::from("alternate-exchange"),
        AMQPValue::LongString("cycle-b".into()),
    );
    let mut cycle_b = FieldTable::default();
    cycle_b.insert(
        ShortString::from("alternate-exchange"),
        AMQPValue::LongString("cycle-a".into()),
    );
    ch.exchange_declare(
        "cycle-a",
        ExchangeKind::Direct,
        ExchangeDeclareOptions::default(),
        cycle_main,
    )
    .await
    .expect("cycle-a");
    ch.exchange_declare(
        "cycle-b",
        ExchangeKind::Direct,
        ExchangeDeclareOptions::default(),
        cycle_b,
    )
    .await
    .expect("cycle-b");
    ch.queue_declare(
        "cycle-q",
        QueueDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .expect("cycle-q");
    ch.basic_publish(
        "cycle-a",
        "nowhere",
        BasicPublishOptions {
            mandatory: true,
            immediate: false,
        },
        b"cycle",
        BasicProperties::default(),
    )
    .await
    .expect("publish cycle")
    .await
    .ok();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    let mut returned = false;
    while tokio::time::Instant::now() < deadline {
        let batch = ch.wait_for_confirms().await.expect("returns");
        if batch.iter().any(|msg| msg.data.as_slice() == b"cycle") {
            returned = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(returned, "a cycled alternate-exchange must basic.return the body");
    let parked = ch
        .queue_declare(
            "cycle-q",
            QueueDeclareOptions {
                passive: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("passive cycle-q");
    assert_eq!(parked.message_count(), 0, "cycle must not enqueue the body");

    conn.close(200, "bye").await.ok();
    broker.listener.abort();
}

#[tokio::test]
async fn tx_commit_is_visible_and_rollback_is_not() {
    use futures_lite::stream::StreamExt;
    use lapin::options::{BasicConsumeOptions, BasicPublishOptions, QueueDeclareOptions};
    use lapin::types::FieldTable;
    use lapin::BasicProperties;

    let broker = start_test_broker().await;
    let conn = connect_test(broker.port).await;
    let ch = conn.create_channel().await.expect("channel");
    ch.queue_declare("tx-q", QueueDeclareOptions::default(), FieldTable::default())
        .await
        .expect("declare");
    let mut consumer = ch
        .basic_consume(
            "tx-q",
            "tx-c",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("consume");

    ch.tx_select().await.expect("select");
    ch.basic_publish(
        "",
        "tx-q",
        BasicPublishOptions::default(),
        b"hidden",
        BasicProperties::default(),
    )
    .await
    .expect("publish")
    .await
    .ok();
    assert!(
        tokio::time::timeout(Duration::from_millis(300), consumer.next())
            .await
            .is_err(),
        "publish must stay invisible until commit"
    );
    ch.tx_rollback().await.expect("rollback");
    assert!(
        tokio::time::timeout(Duration::from_millis(200), consumer.next())
            .await
            .is_err(),
        "rollback must drop the publish"
    );

    ch.basic_publish(
        "",
        "tx-q",
        BasicPublishOptions::default(),
        b"committed",
        BasicProperties::default(),
    )
    .await
    .expect("publish 2")
    .await
    .ok();
    ch.tx_commit().await.expect("commit");
    let delivery = tokio::time::timeout(Duration::from_secs(3), consumer.next())
        .await
        .expect("commit timeout")
        .expect("closed")
        .expect("delivery");
    assert_eq!(delivery.data.as_slice(), b"committed");
    delivery
        .ack(lapin::options::BasicAckOptions::default())
        .await
        .expect("ack committed");
    ch.tx_commit().await.expect("commit the ack");

    ch.tx_select().await.expect("select declare");
    ch.queue_declare(
        "tx-immediate",
        QueueDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .expect("declare inside tx");
    ch.tx_rollback().await.expect("rollback declare");
    ch.queue_declare(
        "tx-immediate",
        QueueDeclareOptions {
            passive: true,
            ..QueueDeclareOptions::default()
        },
        FieldTable::default(),
    )
    .await
    .expect("declare survived rollback");

    // Ack inside a transaction must not remove the message until commit.
    let probe = conn.create_channel().await.expect("probe");
    ch.basic_publish(
        "",
        "tx-q",
        BasicPublishOptions::default(),
        b"ack-me",
        BasicProperties::default(),
    )
    .await
    .expect("publish ack-me")
    .await
    .ok();
    ch.tx_commit().await.expect("commit ack-me");
    let held = tokio::time::timeout(Duration::from_secs(3), consumer.next())
        .await
        .expect("ack-me timeout")
        .expect("closed")
        .expect("ack-me delivery");
    assert_eq!(held.data.as_slice(), b"ack-me");
    ch.tx_select().await.expect("select ack");
    held.ack(lapin::options::BasicAckOptions::default())
        .await
        .expect("ack in tx");
    assert!(
        probe
            .basic_get("tx-q", lapin::options::BasicGetOptions::default())
            .await
            .expect("get during ack tx")
            .is_none(),
        "ack in an open transaction must leave the message unacked"
    );
    ch.tx_rollback().await.expect("rollback ack");
    ch.close(200, "requeue").await.ok();
    let after_ack_rollback = probe
        .basic_get("tx-q", lapin::options::BasicGetOptions::default())
        .await
        .expect("get after ack rollback")
        .expect("rolled-back ack must leave the message in the queue");
    assert_eq!(after_ack_rollback.data.as_slice(), b"ack-me");
    after_ack_rollback
        .ack(lapin::options::BasicAckOptions::default())
        .await
        .expect("clear ack-me");

    // Reject inside a transaction must not requeue until commit.
    let ch = conn.create_channel().await.expect("reject channel");
    ch.basic_publish(
        "",
        "tx-q",
        BasicPublishOptions::default(),
        b"reject-me",
        BasicProperties::default(),
    )
    .await
    .expect("publish reject-me")
    .await
    .ok();
    let mut reject_consumer = ch
        .basic_consume(
            "tx-q",
            "tx-reject",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("consume reject");
    let reject_delivery = tokio::time::timeout(Duration::from_secs(3), reject_consumer.next())
        .await
        .expect("reject-me timeout")
        .expect("closed")
        .expect("reject-me delivery");
    assert_eq!(reject_delivery.data.as_slice(), b"reject-me");
    ch.tx_select().await.expect("select reject");
    reject_delivery
        .reject(lapin::options::BasicRejectOptions { requeue: true })
        .await
        .expect("reject in tx");
    assert!(
        probe
            .basic_get("tx-q", lapin::options::BasicGetOptions::default())
            .await
            .expect("get during reject tx")
            .is_none(),
        "reject in an open transaction must not requeue yet"
    );
    ch.tx_rollback().await.expect("rollback reject");
    ch.close(200, "requeue reject").await.ok();
    let after_reject_rollback = probe
        .basic_get("tx-q", lapin::options::BasicGetOptions::default())
        .await
        .expect("get after reject rollback")
        .expect("rolled-back reject must leave the message in the queue");
    assert_eq!(after_reject_rollback.data.as_slice(), b"reject-me");

    conn.close(200, "bye").await.ok();
    broker.listener.abort();
}
