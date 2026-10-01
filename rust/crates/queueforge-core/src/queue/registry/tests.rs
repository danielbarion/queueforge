//! Registry declare, delete, durability, and shutdown tests.

use super::*;
use crate::queue::meta::NoopMetaStore;
use std::collections::HashMap as StdHashMap;
use std::sync::Mutex;

/// In-memory meta store for durable persist tests.
#[derive(Default)]
struct MemMeta {
    queues: Mutex<StdHashMap<(String, String), Queue>>,
}

impl QueueMetaStore for MemMeta {
    fn create_queue(&self, queue: &Queue) -> Result<()> {
        let mut g = self.queues.lock().unwrap();
        let key = (queue.vhost.to_string(), queue.name.to_string());
        if g.contains_key(&key) {
            return Err(Error::AlreadyExists(format!(
                "queue {}/{}",
                queue.vhost, queue.name
            )));
        }
        g.insert(key, queue.clone());
        Ok(())
    }

    fn put_queue(&self, queue: &Queue) -> Result<()> {
        let mut g = self.queues.lock().unwrap();
        g.insert(
            (queue.vhost.to_string(), queue.name.to_string()),
            queue.clone(),
        );
        Ok(())
    }

    fn get_queue(&self, vhost: &str, name: &str) -> Result<Option<Queue>> {
        let g = self.queues.lock().unwrap();
        Ok(g.get(&(vhost.to_string(), name.to_string())).cloned())
    }

    fn delete_queue(&self, vhost: &str, name: &str) -> Result<bool> {
        let mut g = self.queues.lock().unwrap();
        Ok(g.remove(&(vhost.to_string(), name.to_string())).is_some())
    }
}

fn test_registry() -> QueueRegistry {
    QueueRegistry::new(Arc::new(NoopMetaStore), MemoryTracker::shared()).with_mailbox_capacity(16)
}

fn mem_registry() -> (Arc<MemMeta>, QueueRegistry) {
    let meta = Arc::new(MemMeta::default());
    let reg = QueueRegistry::new(
        Arc::clone(&meta) as Arc<dyn QueueMetaStore>,
        MemoryTracker::shared(),
    )
    .with_mailbox_capacity(16);
    (meta, reg)
}

#[tokio::test]
async fn declare_lookup_delete() {
    let reg = test_registry();
    let opts = QueueDeclareOpts::default();
    let declared = reg.declare("/", "orders", opts).await.expect("declare");
    assert_eq!(declared.handle.info.key.name.as_str(), "orders");
    assert!(declared.handle.is_available());
    assert_eq!(reg.len(), 1);

    let key = QueueKey::new("/", "orders");
    let got = reg.get(&key).expect("lookup");
    assert_eq!(got.info.key.name.as_str(), "orders");

    let msgs = reg.delete(&key, false, false).await.expect("delete");
    assert_eq!(msgs, 0);
    assert!(reg.get(&key).is_none());
    assert!(reg.is_empty());
}

#[tokio::test]
async fn declare_idempotent_same_props() {
    let reg = test_registry();
    let opts = QueueDeclareOpts {
        durable: true,
        ..QueueDeclareOpts::default()
    };
    let _ = reg.declare("/", "q1", opts.clone()).await.unwrap();
    let again = reg.declare("/", "q1", opts).await.unwrap();
    assert_eq!(again.handle.info.key.name.as_str(), "q1");
    assert_eq!(reg.len(), 1);
}

#[tokio::test]
async fn declare_rejects_property_mismatch() {
    let reg = test_registry();
    let _ = reg
        .declare("/", "q1", QueueDeclareOpts::default())
        .await
        .unwrap();
    let err = reg
        .declare(
            "/",
            "q1",
            QueueDeclareOpts {
                durable: true,
                ..QueueDeclareOpts::default()
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::PreconditionFailed(_)));
}

#[tokio::test]
async fn passive_missing_is_not_found() {
    let reg = test_registry();
    let err = reg
        .declare(
            "/",
            "nope",
            QueueDeclareOpts {
                passive: true,
                ..QueueDeclareOpts::default()
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::NotFound(_)));
}

#[tokio::test]
async fn server_named_queue() {
    let reg = test_registry();
    let a = reg
        .declare("/", "", QueueDeclareOpts::default())
        .await
        .unwrap();
    let b = reg
        .declare("/", "", QueueDeclareOpts::default())
        .await
        .unwrap();
    assert!(a.handle.info.key.name.starts_with("amq.gen-"));
    assert!(b.handle.info.key.name.starts_with("amq.gen-"));
    assert_ne!(
        a.handle.info.key.name.as_str(),
        b.handle.info.key.name.as_str(),
        "concurrent empty-name declares must not collide"
    );
    assert_eq!(reg.len(), 2);
}

#[tokio::test]
async fn durable_persists_to_meta() {
    let (meta, reg) = mem_registry();
    let _ = reg
        .declare(
            "/",
            "dur",
            QueueDeclareOpts {
                durable: true,
                ..QueueDeclareOpts::default()
            },
        )
        .await
        .unwrap();
    let stored = meta.get_queue("/", "dur").unwrap().expect("stored");
    assert!(stored.durable);

    let key = QueueKey::new("/", "dur");
    let _ = reg.delete(&key, false, false).await.unwrap();
    assert!(meta.get_queue("/", "dur").unwrap().is_none());
}

#[tokio::test]
async fn transient_not_persisted() {
    let (meta, reg) = mem_registry();
    let _ = reg
        .declare("/", "tmp", QueueDeclareOpts::default())
        .await
        .unwrap();
    assert!(meta.get_queue("/", "tmp").unwrap().is_none());
}

#[tokio::test]
async fn actor_panic_marks_unavailable_no_restart() {
    let reg = test_registry();
    let declared = reg
        .declare("/", "boom", QueueDeclareOpts::default())
        .await
        .unwrap();
    let handle = declared.handle;

    // Enqueue so the queue holds reserved memory before panic.
    let msg = Arc::new(crate::queue::Message {
        exchange: CompactString::from(""),
        routing_key: CompactString::from("boom"),
        body: bytes::Bytes::from_static(b"leak-check"),
        persistent: false,
        redelivered: false,
        content_type: None,
        content_encoding: None,
        correlation_id: None,
        message_id: None,
        reply_to: None,
        expiration: None,
        app_id: None,
        user_id: None,
        type_: None,
        priority: None,
        timestamp: None,
        expires_unix_ms: None,
        headers: Default::default(),
    });
    let tracked = msg.tracked_bytes();
    let before = reg.memory.tracked_bytes();
    let (reply_tx, reply_rx) = oneshot::channel();
    handle
        .tx
        .send(QueueCmd::Enqueue {
            msg,
            reply: reply_tx,
        })
        .await
        .expect("enqueue");
    reply_rx.await.unwrap().expect("enqueue ok");
    assert!(reg.memory.tracked_bytes() >= before + tracked);
    assert!(handle.info.reserved_bytes() >= tracked);

    handle
        .tx
        .send(QueueCmd::TestPanic)
        .await
        .expect("send panic");

    // Wait until supervisor marks unavailable.
    let mut ok = false;
    for _ in 0..50 {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        if !handle.is_available() {
            ok = true;
            break;
        }
    }
    assert!(ok, "expected unavailable after panic");

    // Issue 1: panic must release residual watermark reservation.
    for _ in 0..50 {
        if reg.memory.tracked_bytes() == before && handle.info.reserved_bytes() == 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert_eq!(
        handle.info.reserved_bytes(),
        0,
        "per-queue reservation must be released after panic"
    );
    assert_eq!(
        reg.memory.tracked_bytes(),
        before,
        "global tracked_bytes must not stay inflated after queue panic"
    );

    // Still registered — no silent remove/restart.
    assert!(reg.get(&QueueKey::new("/", "boom")).is_some());
    assert_eq!(reg.len(), 1);

    // Active redeclare must surface unavailable.
    let err = reg
        .declare("/", "boom", QueueDeclareOpts::default())
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Unavailable(_)));

    // Passive declare must also surface unavailable (Issue 3).
    let err = reg
        .declare(
            "/",
            "boom",
            QueueDeclareOpts {
                passive: true,
                ..QueueDeclareOpts::default()
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Unavailable(_)));
}

#[tokio::test]
async fn delete_missing_is_not_found() {
    let reg = test_registry();
    let err = reg
        .delete(&QueueKey::new("/", "missing"), false, false)
        .await
        .unwrap_err();
    assert!(matches!(err, Error::NotFound(_)));
}

#[tokio::test]
async fn exclusive_owner_lock() {
    let reg = test_registry();
    let _ = reg
        .declare(
            "/",
            "ex",
            QueueDeclareOpts {
                exclusive: true,
                exclusive_owner: Some(CompactString::from("conn-a")),
                ..QueueDeclareOpts::default()
            },
        )
        .await
        .unwrap();
    let err = reg
        .declare(
            "/",
            "ex",
            QueueDeclareOpts {
                exclusive: true,
                exclusive_owner: Some(CompactString::from("conn-b")),
                ..QueueDeclareOpts::default()
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::ResourceLocked(_)));
}

#[tokio::test]
async fn concurrent_durable_declare_and_delete_meta_consistent() {
    let (meta, reg) = mem_registry();
    let reg = Arc::new(reg);
    let key = QueueKey::new("/", "race-dd");

    // Seed a durable queue, then thrash declare ‖ delete.
    let _ = reg
        .declare(
            "/",
            "race-dd",
            QueueDeclareOpts {
                durable: true,
                ..QueueDeclareOpts::default()
            },
        )
        .await
        .unwrap();

    let mut joins = Vec::new();
    for i in 0..20 {
        let reg_d = Arc::clone(&reg);
        let reg_c = Arc::clone(&reg);
        let key_d = key.clone();
        joins.push(tokio::spawn(async move {
            let _ = reg_d.delete(&key_d, false, false).await;
        }));
        joins.push(tokio::spawn(async move {
            let _ = reg_c
                .declare(
                    "/",
                    "race-dd",
                    QueueDeclareOpts {
                        durable: true,
                        ..QueueDeclareOpts::default()
                    },
                )
                .await;
            let _ = i;
        }));
    }
    for j in joins {
        let _ = j.await;
    }

    // Invariant: if registry has a durable live entry, meta row must exist.
    // If registry is empty, meta must be absent (no orphan durable definition).
    match reg.get(&key) {
        Some(h) => {
            assert!(h.info.durable);
            assert!(
                meta.get_queue("/", "race-dd").unwrap().is_some(),
                "live durable actor must have meta row"
            );
        }
        None => {
            assert!(
                meta.get_queue("/", "race-dd").unwrap().is_none(),
                "no orphan durable meta when queue absent"
            );
        }
    }
}

#[tokio::test]
async fn concurrent_durable_vs_transient_no_orphan_meta() {
    let (meta, reg) = mem_registry();
    let reg = Arc::new(reg);

    let mut joins = Vec::new();
    for _ in 0..16 {
        let r1 = Arc::clone(&reg);
        let r2 = Arc::clone(&reg);
        joins.push(tokio::spawn(async move {
            let _ = r1
                .declare(
                    "/",
                    "race-props",
                    QueueDeclareOpts {
                        durable: true,
                        ..QueueDeclareOpts::default()
                    },
                )
                .await;
        }));
        joins.push(tokio::spawn(async move {
            let _ = r2
                .declare(
                    "/",
                    "race-props",
                    QueueDeclareOpts {
                        durable: false,
                        ..QueueDeclareOpts::default()
                    },
                )
                .await;
        }));
    }
    for j in joins {
        let _ = j.await;
    }

    let key = QueueKey::new("/", "race-props");
    match reg.get(&key) {
        Some(h) if h.info.durable => {
            assert!(meta.get_queue("/", "race-props").unwrap().is_some());
        }
        Some(h) => {
            // Transient winner: must not leave durable meta from a loser.
            assert!(!h.info.durable);
            assert!(
                meta.get_queue("/", "race-props").unwrap().is_none(),
                "transient live queue must not have orphan durable meta"
            );
        }
        None => {
            assert!(meta.get_queue("/", "race-props").unwrap().is_none());
        }
    }
}

#[tokio::test]
async fn concurrent_double_delete_second_is_not_found() {
    let reg = Arc::new(test_registry());
    let _ = reg
        .declare("/", "once", QueueDeclareOpts::default())
        .await
        .unwrap();
    let key = QueueKey::new("/", "once");

    let r1 = Arc::clone(&reg);
    let r2 = Arc::clone(&reg);
    let k1 = key.clone();
    let k2 = key.clone();
    let (a, b) = tokio::join!(
        async move { r1.delete(&k1, false, false).await },
        async move { r2.delete(&k2, false, false).await },
    );

    let oks = [a.is_ok(), b.is_ok()].into_iter().filter(|x| *x).count();
    let not_founds = [a, b]
        .into_iter()
        .filter(|r| matches!(r, Err(Error::NotFound(_))))
        .count();
    assert_eq!(oks, 1, "exactly one delete succeeds");
    assert_eq!(not_founds, 1, "loser is NotFound");
    assert!(reg.is_empty());
}

#[tokio::test]
async fn durable_vs_transient_sequential_mismatch() {
    let (meta, reg) = mem_registry();
    let _ = reg
        .declare(
            "/",
            "mix",
            QueueDeclareOpts {
                durable: true,
                ..QueueDeclareOpts::default()
            },
        )
        .await
        .unwrap();
    let err = reg
        .declare("/", "mix", QueueDeclareOpts::default())
        .await
        .unwrap_err();
    assert!(matches!(err, Error::PreconditionFailed(_)));
    // Durable meta still present for the live durable queue.
    assert!(meta.get_queue("/", "mix").unwrap().is_some());
}

#[tokio::test]
async fn dlx_routes_on_nack_no_requeue() {
    use crate::domain::{Binding, Exchange, ExchangeType};
    use crate::queue::args::QueueArgs;
    use crate::queue::cmd::{ConsumerSessionId, Message, QueueCmd, QueueDelivery};
    use crate::queue::dlx::DlxRouter;
    use crate::router::ExchangeRouter;
    use bytes::Bytes;
    use compact_str::CompactString;
    use std::sync::Arc;
    use tokio::sync::{mpsc, oneshot};

    let reg = Arc::new(test_registry());
    let router = Arc::new(ExchangeRouter::new());
    router.put_exchange(Exchange::new("/", "dlx", ExchangeType::Fanout));
    reg.set_dlx(Arc::new(DlxRouter::new(
        Arc::clone(&router),
        Arc::downgrade(&reg),
    )));

    // Dead-letter destination.
    let _ = reg
        .declare("/", "dead", QueueDeclareOpts::default())
        .await
        .unwrap();
    router.bind(Binding::new("/", "dlx", "dead", "")).unwrap();

    // Source with DLX.
    let src = reg
        .declare(
            "/",
            "src",
            QueueDeclareOpts {
                args: QueueArgs {
                    dead_letter_exchange: Some(CompactString::from("dlx")),
                    ..QueueArgs::default()
                },
                ..QueueDeclareOpts::default()
            },
        )
        .await
        .unwrap();

    let msg = Arc::new(Message {
        exchange: CompactString::from(""),
        routing_key: CompactString::from("src"),
        body: Bytes::from_static(b"poison"),
        persistent: false,
        redelivered: false,
        content_type: None,
        content_encoding: None,
        correlation_id: None,
        message_id: None,
        reply_to: None,
        expiration: None,
        app_id: None,
        user_id: None,
        type_: None,
        priority: None,
        timestamp: None,
        expires_unix_ms: None,
        headers: Default::default(),
    });
    let (reply_tx, reply_rx) = oneshot::channel();
    src.handle
        .tx
        .send(QueueCmd::Enqueue {
            msg,
            reply: reply_tx,
        })
        .await
        .unwrap();
    let _ = reply_rx.await.unwrap().unwrap();

    let (dtx, mut drx) = mpsc::channel::<QueueDelivery>(64);
    let (reg_tx, reg_rx) = oneshot::channel();
    src.handle
        .tx
        .send(QueueCmd::RegisterConsumer {
            session: ConsumerSessionId(1),
            no_ack: false,
            exclusive: false,
            priority: 0,
            initial_credit: Some(1),
            deliver_tx: dtx,
            reply: reg_tx,
        })
        .await
        .unwrap();
    reg_rx.await.unwrap().unwrap();
    let d = drx.recv().await.unwrap();

    src.handle
        .tx
        .send(QueueCmd::Nack {
            id: d.delivery_id,
            requeue: false,
        })
        .await
        .unwrap();

    // Message should appear on dead queue with x-death.
    let dead = reg.get(&QueueKey::new("/", "dead")).unwrap();
    let mut got = None;
    for _ in 0..50 {
        let (gtx, grx) = oneshot::channel();
        dead.tx
            .send(QueueCmd::Get {
                no_ack: true,
                reply: gtx,
            })
            .await
            .unwrap();
        if let Some((_, qm, _)) = grx.await.unwrap() {
            got = Some(qm);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let qm = got.expect("dead-lettered message on dead queue");
    assert_eq!(qm.message.body.as_ref(), b"poison");
    assert_eq!(qm.message.headers.deaths.len(), 1);
    assert_eq!(qm.message.headers.deaths[0].queue.as_str(), "src");
    assert_eq!(
        qm.message.headers.deaths[0].reason,
        crate::queue::dlx::DeathReason::Rejected
    );
}

#[tokio::test]
async fn shutdown_all_stops_actors_and_clears_registry() {
    let reg = test_registry();
    let _ = reg
        .declare("/", "a", QueueDeclareOpts::default())
        .await
        .unwrap();
    let _ = reg
        .declare("/", "b", QueueDeclareOpts::default())
        .await
        .unwrap();
    assert_eq!(reg.len(), 2);

    let report = reg.shutdown_all().await;
    assert!(report.is_clean(), "report={report:?}");
    assert_eq!(report.queues, 2);
    assert!(reg.is_empty());
    assert!(reg.get(&QueueKey::new("/", "a")).is_none());
}
