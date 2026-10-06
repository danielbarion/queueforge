//! Cluster home selection and quorum-append wire tests.

use queueforge_core::{Queue, QueueType};

use super::forward::normalize_queue_json;
use super::quorum::{peer_append_durable, peers_for_quorum_append};
use super::{catchup_satisfied, record_unreachable, *};

#[test]
fn quorum_catchup_waits_for_every_other_member() {
    let members = vec!["a".to_string(), "b".to_string(), "c".to_string()];
    let none = std::collections::HashSet::new();
    let down = std::collections::HashSet::new();
    assert!(!catchup_satisfied("a", &members, &none, &down));
    let mut heard = std::collections::HashSet::new();
    heard.insert("b".to_string());
    assert!(!catchup_satisfied("a", &members, &heard, &down));
    heard.insert("c".to_string());
    assert!(catchup_satisfied("a", &members, &heard, &down));
    let alone = vec!["a".to_string()];
    assert!(catchup_satisfied("a", &alone, &none, &down));
}

#[test]
fn quorum_catchup_skips_a_member_after_five_refused_dials() {
    let members = vec!["a".to_string(), "b".to_string(), "c".to_string()];
    let mut heard = std::collections::HashSet::new();
    heard.insert("b".to_string());
    let mut down = std::collections::HashSet::new();
    let mut fails = std::collections::HashMap::new();
    for _ in 0..4 {
        assert!(!record_unreachable(&mut fails, &mut down, "c"));
        assert!(!catchup_satisfied("a", &members, &heard, &down));
    }
    assert!(record_unreachable(&mut fails, &mut down, "c"));
    assert!(catchup_satisfied("a", &members, &heard, &down));
}

#[test]
fn home_is_stable_and_covers_both_nodes() {
    let members = vec![
        ClusterMember {
            id: "b".into(),
            addr: "127.0.0.1:2".parse().unwrap(),
        },
        ClusterMember {
            id: "a".into(),
            addr: "127.0.0.1:1".parse().unwrap(),
        },
    ];
    assert_eq!(
        queue_home(&members, "/", "orders"),
        queue_home(&members, "/", "orders")
    );
    let mut seen = std::collections::HashSet::new();
    for i in 0..40 {
        seen.insert(queue_home(&members, "/", &format!("q{i}")));
    }
    assert!(seen.len() > 1, "both nodes should home some queues");
}

#[test]
fn bun_queue_record_decodes_as_a_rust_quorum_queue() {
    let body = serde_json::json!({
        "vhost": "/",
        "name": "orders",
        "durable": true,
        "exclusive": false,
        "autoDelete": false,
        "home": "a",
        "args": { "x-queue-type": "quorum", "x-delivery-limit": 20 }
    });
    let queue: Queue =
        serde_json::from_value(normalize_queue_json(&body)).expect("bun queue record");
    assert_eq!(queue.name.as_str(), "orders");
    assert!(!queue.auto_delete);
    assert_eq!(queue.home.as_deref(), Some("a"));
    assert_eq!(queue.args.queue_type, Some(QueueType::Quorum));
    assert_eq!(queue.args.delivery_limit, Some(20));
}

#[test]
fn quorum_append_v1_round_trip_keeps_the_body() {
    let mut message = Message::blank();
    message.body = Bytes::from_static(b"mixed-body");
    message.persistent = true;
    message.message_id = Some(CompactString::from("m1"));
    message.routing_key = CompactString::from("orders");
    let encoded = encode_quorum_append(&QueueKey::new("/", "orders"), &message);
    assert_eq!(encoded["v"], 1);
    let (key, decoded) = decode_quorum_append(&encoded).expect("decode v1");
    assert_eq!(key.name.as_str(), "orders");
    assert_eq!(decoded.body.as_ref(), b"mixed-body");
    assert_eq!(decoded.message_id.as_deref(), Some("m1"));
    let bun_shape = serde_json::json!({
        "vhost": "/",
        "queue": "orders",
        "qid": "m1",
        "body": BASE64.encode(b"mixed-body"),
        "persistent": true,
        "routingKey": "orders"
    });
    let (_, from_bun) = decode_quorum_append(&bun_shape).expect("decode bun shape");
    assert_eq!(from_bun.body.as_ref(), b"mixed-body");
}

fn sample_reply(ok: bool) -> Msg {
    Msg {
        id: 1,
        op: "quorum_append".into(),
        ok,
        error: if ok {
            String::new()
        } else {
            "append failed".into()
        },
        payload: Value::Null,
        v: 1,
        node_id: "peer".into(),
        from: "peer".into(),
        kind: String::new(),
    }
}

#[test]
fn failed_peer_reply_is_not_a_durable_copy() {
    let failed = Ok(sample_reply(false));
    let accepted = Ok(sample_reply(true));
    let down = Err(Error::Unavailable("cluster peer is down".into()));
    assert!(!peer_append_durable(&failed));
    assert!(!peer_append_durable(&down));
    assert!(peer_append_durable(&accepted));
    let copies = [
        if peer_append_durable(&failed) {
            MemberCopy::Durable
        } else {
            MemberCopy::MemoryOnly
        },
        if peer_append_durable(&down) {
            MemberCopy::Durable
        } else {
            MemberCopy::MemoryOnly
        },
        MemberCopy::Durable,
    ];
    assert!(
        !durable_majority(3, &copies),
        "an ok:false reply must not satisfy a durable majority"
    );
}

#[test]
fn lagging_extra_peer_is_skipped_once_a_majority_peer_is_available() {
    let peers = vec!["b".to_string(), "c".to_string()];
    let mut inflight = std::collections::HashMap::new();
    let both = peers_for_quorum_append(&peers, &inflight, 1, 32);
    assert_eq!(both, vec!["b", "c"]);
    inflight.insert("c".to_string(), 32);
    let skipped = peers_for_quorum_append(&peers, &inflight, 1, 32);
    assert_eq!(
        skipped,
        vec!["b"],
        "c is past the cap and b already covers the peer copy a majority needs"
    );
    inflight.insert("b".to_string(), 100);
    inflight.insert("c".to_string(), 100);
    let required = peers_for_quorum_append(&peers, &inflight, 1, 32);
    assert_eq!(
        required,
        vec!["b"],
        "the less loaded peer is still contacted when every peer is past the cap"
    );
    let only = vec!["c".to_string()];
    let forced = peers_for_quorum_append(&only, &inflight, 1, 32);
    assert_eq!(
        forced,
        vec!["c"],
        "the only peer is contacted even past the cap"
    );
}
