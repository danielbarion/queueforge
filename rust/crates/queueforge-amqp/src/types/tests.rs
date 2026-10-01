//! Wire-type round-trips, including the RabbitMQ field-table dialect.

use super::*;
use crate::error::Error;

#[test]
fn shortstr_roundtrip() {
    let mut enc = Encoder::new();
    enc.write_shortstr("hello").unwrap();
    let buf = enc.finish();
    assert_eq!(buf, b"\x05hello");
    let mut dec = Decoder::new(&buf);
    assert_eq!(dec.read_shortstr().unwrap(), "hello");
    dec.finish().unwrap();
}

#[test]
fn longstr_binary_roundtrip() {
    let payload = b"\0user\0pass";
    let mut enc = Encoder::new();
    enc.write_longstr(payload).unwrap();
    let buf = enc.finish();
    let mut dec = Decoder::new(&buf);
    assert_eq!(dec.read_longstr().unwrap(), payload);
    dec.finish().unwrap();
}

#[test]
fn bits_pack_lsb_first() {
    // five bits: 1,0,1,1,0 → octet 0b00001101 = 0x0D
    let mut enc = Encoder::new();
    enc.write_bit(true);
    enc.write_bit(false);
    enc.write_bit(true);
    enc.write_bit(true);
    enc.write_bit(false);
    enc.write_shortstr("x").unwrap(); // force align
    let buf = enc.finish();
    assert_eq!(buf[0], 0b0000_1101);
    assert_eq!(&buf[1..], b"\x01x");

    let mut dec = Decoder::new(&buf);
    assert!(dec.read_bit().unwrap());
    assert!(!dec.read_bit().unwrap());
    assert!(dec.read_bit().unwrap());
    assert!(dec.read_bit().unwrap());
    assert!(!dec.read_bit().unwrap());
    assert_eq!(dec.read_shortstr().unwrap(), "x");
    dec.finish().unwrap();
}

#[test]
fn table_roundtrip_common_types() {
    let mut table = FieldTable::new();
    table.insert("product", FieldValue::long_str("QueueForge"));
    table.insert("version", FieldValue::short_str("0.1.0"));
    table.insert("platform", FieldValue::long_str("rust"));
    table.insert(
        "capabilities",
        FieldValue::Table(FieldTable::from_pairs([
            ("publisher_confirms", FieldValue::Bool(true)),
            ("basic.nack", FieldValue::Bool(true)),
        ])),
    );
    table.insert("count", FieldValue::I32(42));
    table.insert("priority", FieldValue::I16(5));
    table.insert("delivery_count", FieldValue::I64(1));
    table.insert("none", FieldValue::Void);
    table.insert("blob", FieldValue::Bytes(vec![1, 2, 3]));

    let mut enc = Encoder::new();
    enc.write_table(&table).unwrap();
    let buf = enc.finish();

    let mut dec = Decoder::new(&buf);
    let decoded = dec.read_table().unwrap();
    dec.finish().unwrap();
    assert_eq!(decoded, table);

    // RabbitMQ dialect wire tags for integers / strings.
    let mut enc = Encoder::new();
    enc.write_field_value(&FieldValue::I16(-2)).unwrap();
    enc.write_field_value(&FieldValue::I64(99)).unwrap();
    enc.write_field_value(&FieldValue::long_str("hi")).unwrap();
    let tags = enc.finish();
    assert_eq!(tags[0], b's'); // i16
    assert_eq!(tags[3], b'l'); // i64
    assert_eq!(tags[12], b'S'); // longstr
}

#[test]
fn rabbitmq_dialect_s_is_i16_not_shortstr() {
    // Wire: tag 's' + i16 BE 0x0007 — must not be read as short-string.
    let wire = [b's', 0x00, 0x07];
    let mut dec = Decoder::new(&wire);
    assert_eq!(dec.read_field_value().unwrap(), FieldValue::I16(7));
    dec.finish().unwrap();
}

#[test]
fn rabbitmq_dialect_accepts_official_u_and_l() {
    let mut wire = vec![b'U'];
    wire.extend_from_slice(&(-3i16).to_be_bytes());
    wire.push(b'L');
    wire.extend_from_slice(&42i64.to_be_bytes());
    let mut dec = Decoder::new(&wire);
    assert_eq!(dec.read_field_value().unwrap(), FieldValue::I16(-3));
    assert_eq!(dec.read_field_value().unwrap(), FieldValue::I64(42));
    dec.finish().unwrap();
}

#[test]
fn rabbitmq_client_properties_table_golden() {
    // Minimal client-properties table in RabbitMQ dialect (as lapin/RMQ emit):
    //   product: S "lapin"
    //   capabilities: F { basic.nack: t true }
    // Hand-built expected wire body (after 4-byte table length prefix).
    let mut table = FieldTable::new();
    table.insert("product", FieldValue::long_str("lapin"));
    table.insert(
        "capabilities",
        FieldValue::Table(FieldTable::from_pairs([(
            "basic.nack",
            FieldValue::Bool(true),
        )])),
    );

    let mut enc = Encoder::new();
    enc.write_table(&table).unwrap();
    let buf = enc.finish();

    // length (4) + entries
    // "product" shortstr = 07 product
    // S + len 5 + lapin
    // "capabilities" = 0C capabilities
    // F + nested table len + "basic.nack" + t 01
    // body = product(8+10) + capabilities(13+1+4+13) = 18 + 31 = 49
    let expected: &[u8] = &[
        0x00, 0x00, 0x00, 0x31, // table body length = 49
        0x07, b'p', b'r', b'o', b'd', b'u', b'c', b't', //
        b'S', 0x00, 0x00, 0x00, 0x05, b'l', b'a', b'p', b'i', b'n', //
        0x0C, b'c', b'a', b'p', b'a', b'b', b'i', b'l', b'i', b't', b'i', b'e', b's', //
        b'F', 0x00, 0x00, 0x00, 0x0D, // nested table len 13
        0x0A, b'b', b'a', b's', b'i', b'c', b'.', b'n', b'a', b'c', b'k', //
        b't', 0x01,
    ];
    assert_eq!(buf, expected);

    let mut dec = Decoder::new(&buf);
    let decoded = dec.read_table().unwrap();
    dec.finish().unwrap();
    assert_eq!(decoded, table);
}

#[test]
fn shortstr_too_long_rejected() {
    let s = "x".repeat(256);
    let mut enc = Encoder::new();
    assert!(matches!(
        enc.write_shortstr(&s),
        Err(Error::InvalidShortstr(256))
    ));
}

#[test]
fn shortstr_decode_past_buffer() {
    // claims length 5 but only 2 bytes follow
    let buf = [0x05u8, b'a', b'b'];
    let mut dec = Decoder::new(&buf);
    assert!(matches!(
        dec.read_shortstr(),
        Err(Error::TruncatedMethod { need: 3, have: 2 })
    ));
}

#[test]
fn longstr_decode_past_buffer() {
    // claims length 10, only 1 byte follows
    let buf = [0x00, 0x00, 0x00, 0x0A, 0xFF];
    let mut dec = Decoder::new(&buf);
    assert!(matches!(
        dec.read_longstr(),
        Err(Error::TruncatedMethod { need: 9, have: 1 })
    ));
}

#[test]
fn unknown_field_value_tag() {
    let buf = b"Z";
    let mut dec = Decoder::new(buf);
    assert!(matches!(
        dec.read_field_value(),
        Err(Error::UnknownFieldValueType(b'Z'))
    ));
}

#[test]
fn table_length_mismatch_overread() {
    // table claims 2 body bytes but body is empty after length
    let buf = [0x00, 0x00, 0x00, 0x02];
    let mut dec = Decoder::new(&buf);
    assert!(matches!(
        dec.read_table(),
        Err(Error::TruncatedMethod { .. })
    ));
}

#[test]
fn table_length_underrun_trailing_inside() {
    // claims 4 body bytes: complete empty-name + void pair uses 2, then a
    // second name length 0 with no type tag left inside the declared length.
    let buf = [
        0x00, 0x00, 0x00, 0x04, // len 4
        0x00, b'V', // pair 1
        0x00, 0x00, // incomplete next pair
    ];
    let mut dec = Decoder::new(&buf);
    let err = dec.read_table().unwrap_err();
    assert!(
        matches!(
            err,
            Error::InvalidTableLength(4)
                | Error::TruncatedMethod { .. }
                | Error::InvalidShortstr(_)
                | Error::UnknownFieldValueType(_)
        ),
        "unexpected err: {err:?}"
    );
}

#[test]
fn u64_out_of_i64_range_rejected() {
    let mut enc = Encoder::new();
    assert!(matches!(
        enc.write_field_value(&FieldValue::U64(u64::MAX)),
        Err(Error::ValueOutOfRange("u64 as i64"))
    ));
}
