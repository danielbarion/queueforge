//! Cluster home selection and quorum-append wire tests.

use super::quorum::peer_append_durable;
use super::*;

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
