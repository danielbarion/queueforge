//! Method id and encode/decode round-trips.

use super::*;
use crate::error::Error;
use crate::frame::Frame;
use crate::types::{Encoder, FieldTable, FieldValue};

fn roundtrip(m: Method) {
    let encoded = m.encode().expect("encode");
    let decoded = Method::decode(&encoded).expect("decode");
    assert_eq!(
        decoded,
        m,
        "roundtrip mismatch for class={} method={}",
        m.class_id(),
        m.method_id()
    );
    // frame wrapper
    let frame = m.to_frame(1).expect("to_frame");
    assert_eq!(frame.channel, 1);
    let again = Method::from_frame(&frame).expect("from_frame");
    assert_eq!(again, m);
}

#[test]
fn class_method_ids() {
    assert_eq!(connection::CLASS_ID, 10);
    assert_eq!(channel::CLASS_ID, 20);
    assert_eq!(exchange::CLASS_ID, 40);
    assert_eq!(queue::CLASS_ID, 50);
    assert_eq!(basic::CLASS_ID, 60);
    assert_eq!(confirm::CLASS_ID, 85);

    assert_eq!(connection::Start::METHOD_ID, 10);
    assert_eq!(connection::StartOk::METHOD_ID, 11);
    assert_eq!(connection::Tune::METHOD_ID, 30);
    assert_eq!(connection::Open::METHOD_ID, 40);
    assert_eq!(connection::Close::METHOD_ID, 50);
    assert_eq!(basic::Publish::METHOD_ID, 40);
    assert_eq!(basic::Deliver::METHOD_ID, 60);
    assert_eq!(basic::Ack::METHOD_ID, 80);
    assert_eq!(basic::Nack::METHOD_ID, 120);
    assert_eq!(confirm::Select::METHOD_ID, 10);
    assert_eq!(confirm::SelectOk::METHOD_ID, 11);
}

#[test]
fn confirm_select_roundtrip() {
    roundtrip(Method::ConfirmSelect(confirm::Select { nowait: false }));
    roundtrip(Method::ConfirmSelect(confirm::Select { nowait: true }));
    roundtrip(Method::ConfirmSelectOk(confirm::SelectOk));
}

#[test]
fn connection_start_roundtrip() {
    let mut props = FieldTable::new();
    props.insert("product", FieldValue::long_str("QueueForge"));
    props.insert("version", FieldValue::long_str("0.1.0"));
    props.insert(
        "capabilities",
        FieldValue::Table(FieldTable::from_pairs([
            ("publisher_confirms", FieldValue::Bool(true)),
            ("consumer_cancel_notify", FieldValue::Bool(true)),
            ("basic.nack", FieldValue::Bool(true)),
        ])),
    );

    roundtrip(Method::ConnectionStart(connection::Start {
        version_major: 0,
        version_minor: 9,
        server_properties: props,
        mechanisms: b"PLAIN AMQPLAIN".to_vec(),
        locales: b"en_US".to_vec(),
    }));
}

#[test]
fn connection_start_ok_plain_auth() {
    let mut props = FieldTable::new();
    props.insert("product", FieldValue::long_str("test-client"));
    // PLAIN: \0username\0password
    let response = b"\0admin\0s3cret".to_vec();
    roundtrip(Method::ConnectionStartOk(connection::StartOk {
        client_properties: props,
        mechanism: "PLAIN".into(),
        response,
        locale: "en_US".into(),
    }));
}

#[test]
fn connection_tune_open_close_roundtrip() {
    roundtrip(Method::ConnectionTune(connection::Tune {
        channel_max: 2047,
        frame_max: 131_072,
        heartbeat: 60,
    }));
    roundtrip(Method::ConnectionTuneOk(connection::TuneOk {
        channel_max: 2047,
        frame_max: 131_072,
        heartbeat: 60,
    }));
    roundtrip(Method::ConnectionOpen(connection::Open::new("/")));
    roundtrip(Method::ConnectionOpenOk(connection::OpenOk::new()));
    roundtrip(Method::ConnectionClose(connection::Close {
        reply_code: 200,
        reply_text: "OK".into(),
        class_id: 0,
        method_id: 0,
    }));
    roundtrip(Method::ConnectionCloseOk(connection::CloseOk));
}

#[test]
fn channel_methods_roundtrip() {
    roundtrip(Method::ChannelOpen(channel::Open::new()));
    roundtrip(Method::ChannelOpenOk(channel::OpenOk::new()));
    roundtrip(Method::ChannelFlow(channel::Flow { active: true }));
    roundtrip(Method::ChannelFlowOk(channel::FlowOk { active: false }));
    roundtrip(Method::ChannelClose(channel::Close {
        reply_code: 404,
        reply_text: "NOT_FOUND".into(),
        class_id: 50,
        method_id: 10,
    }));
    roundtrip(Method::ChannelCloseOk(channel::CloseOk));
}

#[test]
fn queue_declare_with_flags_and_args() {
    let mut args = FieldTable::new();
    args.insert("x-max-priority", FieldValue::I32(10));
    roundtrip(Method::QueueDeclare(queue::Declare {
        reserved_1: 0,
        queue: "orders".into(),
        passive: false,
        durable: true,
        exclusive: false,
        auto_delete: false,
        no_wait: false,
        arguments: args,
    }));
    roundtrip(Method::QueueDeclareOk(queue::DeclareOk {
        queue: "orders".into(),
        message_count: 3,
        consumer_count: 1,
    }));
}

#[test]
fn queue_bind_unbind_purge_delete() {
    roundtrip(Method::QueueBind(queue::Bind {
        reserved_1: 0,
        queue: "q".into(),
        exchange: "ex".into(),
        routing_key: "rk".into(),
        no_wait: false,
        arguments: FieldTable::new(),
    }));
    roundtrip(Method::QueueBindOk(queue::BindOk));
    roundtrip(Method::QueueUnbind(queue::Unbind {
        reserved_1: 0,
        queue: "q".into(),
        exchange: "ex".into(),
        routing_key: "rk".into(),
        arguments: FieldTable::new(),
    }));
    roundtrip(Method::QueueUnbindOk(queue::UnbindOk));
    roundtrip(Method::QueuePurge(queue::Purge {
        reserved_1: 0,
        queue: "q".into(),
        no_wait: false,
    }));
    roundtrip(Method::QueuePurgeOk(queue::PurgeOk { message_count: 7 }));
    roundtrip(Method::QueueDelete(queue::Delete {
        reserved_1: 0,
        queue: "q".into(),
        if_unused: true,
        if_empty: true,
        no_wait: false,
    }));
    roundtrip(Method::QueueDeleteOk(queue::DeleteOk { message_count: 0 }));
}

#[test]
fn exchange_declare_and_bind() {
    roundtrip(Method::ExchangeDeclare(exchange::Declare {
        reserved_1: 0,
        exchange: "logs".into(),
        kind: "topic".into(),
        passive: false,
        durable: true,
        auto_delete: false,
        internal: false,
        no_wait: false,
        arguments: FieldTable::new(),
    }));
    roundtrip(Method::ExchangeDeclareOk(exchange::DeclareOk));
    roundtrip(Method::ExchangeBind(exchange::Bind {
        reserved_1: 0,
        destination: "dst".into(),
        source: "src".into(),
        routing_key: "#".into(),
        no_wait: false,
        arguments: FieldTable::new(),
    }));
    roundtrip(Method::ExchangeDelete(exchange::Delete {
        reserved_1: 0,
        exchange: "logs".into(),
        if_unused: true,
        no_wait: false,
    }));
    roundtrip(Method::ExchangeDeleteOk(exchange::DeleteOk));
}

#[test]
fn basic_publish_consume_deliver_ack() {
    roundtrip(Method::BasicQos(basic::Qos {
        prefetch_size: 0,
        prefetch_count: 50,
        global: false,
    }));
    roundtrip(Method::BasicQosOk(basic::QosOk));

    roundtrip(Method::BasicConsume(basic::Consume {
        reserved_1: 0,
        queue: "orders".into(),
        consumer_tag: String::new(),
        no_local: false,
        no_ack: false,
        exclusive: false,
        no_wait: false,
        arguments: FieldTable::new(),
    }));
    roundtrip(Method::BasicConsumeOk(basic::ConsumeOk {
        consumer_tag: "ctag-1".into(),
    }));

    roundtrip(Method::BasicPublish(basic::Publish {
        reserved_1: 0,
        exchange: String::new(),
        routing_key: "orders".into(),
        mandatory: true,
        immediate: false,
    }));

    roundtrip(Method::BasicDeliver(basic::Deliver {
        consumer_tag: "ctag-1".into(),
        delivery_tag: 1,
        redelivered: false,
        exchange: String::new(),
        routing_key: "orders".into(),
    }));

    roundtrip(Method::BasicAck(basic::Ack {
        delivery_tag: 1,
        multiple: false,
    }));
    roundtrip(Method::BasicNack(basic::Nack {
        delivery_tag: 2,
        multiple: true,
        requeue: true,
    }));
    roundtrip(Method::BasicReject(basic::Reject {
        delivery_tag: 3,
        requeue: false,
    }));
}

#[test]
fn basic_get_return_cancel_recover() {
    roundtrip(Method::BasicGet(basic::Get {
        reserved_1: 0,
        queue: "q".into(),
        no_ack: false,
    }));
    roundtrip(Method::BasicGetOk(basic::GetOk {
        delivery_tag: 9,
        redelivered: true,
        exchange: "ex".into(),
        routing_key: "rk".into(),
        message_count: 4,
    }));
    roundtrip(Method::BasicGetEmpty(basic::GetEmpty::new()));
    roundtrip(Method::BasicReturn(basic::Return {
        reply_code: 312,
        reply_text: "NO_ROUTE".into(),
        exchange: String::new(),
        routing_key: "missing".into(),
    }));
    roundtrip(Method::BasicCancel(basic::Cancel {
        consumer_tag: "ctag-1".into(),
        no_wait: false,
    }));
    roundtrip(Method::BasicCancelOk(basic::CancelOk {
        consumer_tag: "ctag-1".into(),
    }));
    roundtrip(Method::BasicRecover(basic::Recover { requeue: true }));
    roundtrip(Method::BasicRecoverOk(basic::RecoverOk));
}

#[test]
fn unknown_method_errors() {
    // class 10 method 99
    let payload = {
        let mut enc = Encoder::new();
        enc.write_short(10);
        enc.write_short(99);
        enc.finish()
    };
    assert!(matches!(
        Method::decode(&payload),
        Err(Error::UnknownMethod {
            class_id: 10,
            method_id: 99
        })
    ));
}

#[test]
fn truncated_method_errors() {
    // class+method only, missing tune fields
    let payload = {
        let mut enc = Encoder::new();
        enc.write_short(10);
        enc.write_short(30);
        enc.finish()
    };
    assert!(matches!(
        Method::decode(&payload),
        Err(Error::TruncatedMethod { .. })
    ));
}

#[test]
fn connection_start_wire_ids_prefix() {
    let m = Method::ConnectionStart(connection::Start {
        version_major: 0,
        version_minor: 9,
        server_properties: FieldTable::new(),
        mechanisms: b"PLAIN".to_vec(),
        locales: b"en_US".to_vec(),
    });
    let bytes = m.encode().unwrap();
    // class 10, method 10
    assert_eq!(&bytes[0..4], &[0x00, 0x0A, 0x00, 0x0A]);
    assert_eq!(bytes[4], 0); // version-major
    assert_eq!(bytes[5], 9); // version-minor
}

#[test]
fn queue_declare_flags_bit_packing() {
    // durable=true, exclusive=true → bits 1 and 2 set → 0b0000_0110
    let m = Method::QueueDeclare(queue::Declare {
        reserved_1: 0,
        queue: "q".into(),
        passive: false,
        durable: true,
        exclusive: true,
        auto_delete: false,
        no_wait: false,
        arguments: FieldTable::new(),
    });
    let bytes = m.encode().unwrap();
    // class(2)+method(2)+reserved(2)+shortstr "q"(2) = 8, then flags octet
    // 00 32 00 0A | 00 00 | 01 71 | flags | table len
    assert_eq!(&bytes[0..4], &[0x00, 0x32, 0x00, 0x0A]); // class 50, method 10
    assert_eq!(&bytes[4..6], &[0x00, 0x00]); // reserved
    assert_eq!(&bytes[6..8], &[0x01, b'q']);
    assert_eq!(bytes[8], 0b0000_0110);
}

#[test]
fn handshake_sequence_frame_encode() {
    // Smoke: encode a typical server connection.start as a channel-0 frame.
    let start = Method::ConnectionStart(connection::Start {
        version_major: 0,
        version_minor: 9,
        server_properties: FieldTable::new(),
        mechanisms: b"PLAIN".to_vec(),
        locales: b"en_US".to_vec(),
    });
    let frame = start.to_frame(0).unwrap();
    let wire = frame.encode().unwrap();
    let (decoded_frame, n) = Frame::decode(&wire).unwrap();
    assert_eq!(n, wire.len());
    let decoded = Method::from_frame(&decoded_frame).unwrap();
    assert_eq!(decoded, start);
}

#[test]
fn from_frame_rejects_non_method() {
    let hb = Frame::heartbeat();
    assert!(matches!(
        Method::from_frame(&hb),
        Err(Error::ExpectedMethodFrame(8))
    ));
    let body = Frame::body(1, vec![1, 2, 3]);
    assert!(matches!(
        Method::from_frame(&body),
        Err(Error::ExpectedMethodFrame(3))
    ));
}

/// Golden method payloads (exact byte strings) for third-party interop confidence.
#[test]
fn golden_connection_start_minimal() {
    let m = Method::ConnectionStart(connection::Start {
        version_major: 0,
        version_minor: 9,
        server_properties: FieldTable::new(),
        mechanisms: b"PLAIN".to_vec(),
        locales: b"en_US".to_vec(),
    });
    // class=10 method=10 | ver 0,9 | empty table | longstr PLAIN | longstr en_US
    let expected: &[u8] = &[
        0x00, 0x0A, 0x00, 0x0A, //
        0x00, 0x09, //
        0x00, 0x00, 0x00, 0x00, // empty server-properties
        0x00, 0x00, 0x00, 0x05, b'P', b'L', b'A', b'I', b'N', //
        0x00, 0x00, 0x00, 0x05, b'e', b'n', b'_', b'U', b'S',
    ];
    assert_eq!(m.encode().unwrap(), expected);
    assert_eq!(Method::decode(expected).unwrap(), m);
}

#[test]
fn golden_queue_declare_durable() {
    let m = Method::QueueDeclare(queue::Declare {
        reserved_1: 0,
        queue: "q".into(),
        passive: false,
        durable: true,
        exclusive: false,
        auto_delete: false,
        no_wait: false,
        arguments: FieldTable::new(),
    });
    // class=50 method=10 | ticket 0 | "q" | flags durable only (0x02) | empty table
    let expected: &[u8] = &[
        0x00, 0x32, 0x00, 0x0A, //
        0x00, 0x00, //
        0x01, b'q', //
        0x02, // bit1 = durable
        0x00, 0x00, 0x00, 0x00,
    ];
    assert_eq!(m.encode().unwrap(), expected);
    assert_eq!(Method::decode(expected).unwrap(), m);
}

#[test]
fn golden_connection_start_ok_plain() {
    let m = Method::ConnectionStartOk(connection::StartOk {
        client_properties: FieldTable::new(),
        mechanism: "PLAIN".into(),
        response: b"\0admin\0s3cret".to_vec(),
        locale: "en_US".into(),
    });
    // class=10 method=11 | empty table | shortstr PLAIN | longstr \0admin\0s3cret | shortstr en_US
    // SASL response is \0 admin \0 s3cret = 1+5+1+6 = 13 bytes
    let expected: &[u8] = &[
        0x00, 0x0A, 0x00, 0x0B, //
        0x00, 0x00, 0x00, 0x00, // empty client-properties
        0x05, b'P', b'L', b'A', b'I', b'N', //
        0x00, 0x00, 0x00, 0x0D, // 13-byte SASL response
        0x00, b'a', b'd', b'm', b'i', b'n', 0x00, b's', b'3', b'c', b'r', b'e', b't', //
        0x05, b'e', b'n', b'_', b'U', b'S',
    ];
    assert_eq!(m.encode().unwrap(), expected);
    assert_eq!(Method::decode(expected).unwrap(), m);
}
