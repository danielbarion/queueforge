//! AMQP 0-9-1 wire types: primitives, field tables, and bit packing.
//! The encoder and decoder live in sibling modules.
//!
//! Encoding rules follow the AMQP 0-9-1 specification for method arguments:
//! - Integers are big-endian.
//! - Consecutive `bit` fields pack into octets, least-significant bit first.
//! - A non-bit field forces octet alignment (remaining bits in the current
//!   bit-octet are discarded on decode / left zero on encode).
//! - `shortstr` = octet length + content (max 255).
//! - `longstr` = long length + content.
//! - `table` = long length + field-value-pairs (name shortstr + typed value).
//!
//! # Field-table dialect (RabbitMQ / lapin interop)
//!
//! Field-value **type tags** use the **RabbitMQ / QPid table dialect**, not the
//! literal AMQP 0-9-1 XML mapping, because production clients (including lapin)
//! and RabbitMQ itself use this dialect for `peer-properties` and method
//! `arguments`:
//!
//! | Tag | Encode | Decode |
//! |-----|--------|--------|
//! | `t` | bool | bool |
//! | `b` / `B` | i8 / u8 | i8 / u8 |
//! | `s` | **i16** (not short-string) | i16 |
//! | `u` | u16 | u16 |
//! | `I` / `i` | i32 / u32 | i32 / u32 |
//! | `l` | **i64** | i64 |
//! | `S` | long-string (only string form) | long-string |
//! | `T` / `F` / `A` / `V` / `x` | timestamp / table / array / void / bytes | same |
//!
//! On decode we also accept the rare official-XML tags where unambiguous:
//! `U` → i16, `L` → i64 (RabbitMQ accepts `L` as signed long-long as well).
//! There is no short-string field-value in this dialect; [`FieldValue::ShortString`]
//! encodes as `S`. Unsigned 64-bit values have no RabbitMQ tag and only encode
//! when they fit in `i64` (as `l`).

/// Maximum length of an AMQP `shortstr` (octet length prefix).
pub const SHORTSTR_MAX: usize = 255;

/// Ordered AMQP field table (`peer-properties`, method `arguments`, etc.).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct FieldTable {
    /// Name/value pairs in wire order.
    pub entries: Vec<(String, FieldValue)>,
}

impl FieldTable {
    /// Empty table.
    pub fn new() -> Self {
        Self::default()
    }

    /// Build from an iterator of pairs.
    pub fn from_pairs<I, K>(iter: I) -> Self
    where
        I: IntoIterator<Item = (K, FieldValue)>,
        K: Into<String>,
    {
        Self {
            entries: iter.into_iter().map(|(k, v)| (k.into(), v)).collect(),
        }
    }

    /// Number of entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// True when the table has no entries.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Insert or replace by name (appends if missing).
    pub fn insert(&mut self, name: impl Into<String>, value: FieldValue) {
        let name = name.into();
        if let Some((_, slot)) = self.entries.iter_mut().find(|(k, _)| k == &name) {
            *slot = value;
        } else {
            self.entries.push((name, value));
        }
    }

    /// Look up a value by name.
    pub fn get(&self, name: &str) -> Option<&FieldValue> {
        self.entries.iter().find(|(k, _)| k == name).map(|(_, v)| v)
    }
}

/// A typed AMQP field-table / field-array value.
///
/// Wire tags follow the **RabbitMQ table dialect** (see module docs).
#[derive(Debug, Clone, PartialEq)]
pub enum FieldValue {
    /// `t` — boolean.
    Bool(bool),
    /// `b` — short-short-int.
    I8(i8),
    /// `B` — short-short-uint.
    U8(u8),
    /// `s` — short-int (RabbitMQ dialect; official XML used `U`).
    I16(i16),
    /// `u` — short-uint.
    U16(u16),
    /// `I` — long-int.
    I32(i32),
    /// `i` — long-uint.
    U32(u32),
    /// `l` — long-long-int (RabbitMQ dialect; official XML used `L` for signed).
    I64(i64),
    /// Unsigned 64-bit integer.
    ///
    /// RabbitMQ has no unsigned long-long table tag. Encoding succeeds only when
    /// the value fits in `i64` (emitted as `l`); decoding always yields [`Self::I64`].
    U64(u64),
    /// `f` — float.
    F32(f32),
    /// `d` — double.
    F64(f64),
    /// `D` — decimal-value (scale, unscaled signed long).
    Decimal {
        /// Number of digits after the decimal point.
        scale: u8,
        /// Unscaled signed integer value.
        value: i32,
    },
    /// Short-string **logical** value.
    ///
    /// RabbitMQ tables have no short-string field-value tag (`s` is i16). This
    /// variant is encoded as long-string (`S`) for interop; decode of `S`
    /// produces [`Self::LongString`].
    ShortString(String),
    /// `S` — long-string (binary-safe); the only string form on the wire.
    LongString(Vec<u8>),
    /// `A` — field-array.
    Array(Vec<FieldValue>),
    /// `T` — timestamp (posix seconds, 64-bit).
    Timestamp(u64),
    /// `F` — nested field-table.
    Table(FieldTable),
    /// `V` — void / null.
    Void,
    /// `x` — byte array (RabbitMQ extension, common in client properties).
    Bytes(Vec<u8>),
}

impl FieldValue {
    /// Convenience: long-string from a UTF-8 `&str` (wire tag `S`).
    pub fn long_str(s: impl Into<String>) -> Self {
        Self::LongString(s.into().into_bytes())
    }

    /// Convenience: UTF-8 string stored as long-string (wire tag `S`).
    ///
    /// Prefer this over [`Self::ShortString`] for table values; RabbitMQ has no
    /// short-string field-value tag.
    pub fn short_str(s: impl Into<String>) -> Self {
        Self::LongString(s.into().into_bytes())
    }
}

mod decode;
mod encode;

pub use decode::Decoder;
pub use encode::Encoder;

#[cfg(test)]
mod tests;
