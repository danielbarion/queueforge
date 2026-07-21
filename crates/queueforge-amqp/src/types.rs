//! AMQP 0-9-1 wire types: primitives, field tables, and bit packing.
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

use crate::error::{Error, Result};

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

/// Streaming encoder for AMQP method arguments and table content.
#[derive(Debug, Default)]
pub struct Encoder {
    buf: Vec<u8>,
    /// Bits already written into the open bit-octet (`0` = no open group).
    bits_in_octet: u8,
}

impl Encoder {
    /// Create an empty encoder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create with a capacity hint.
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            buf: Vec::with_capacity(cap),
            bits_in_octet: 0,
        }
    }

    /// Finish encoding and return the buffer.
    pub fn finish(self) -> Vec<u8> {
        self.buf
    }

    /// Borrow the encoded bytes so far.
    pub fn as_slice(&self) -> &[u8] {
        &self.buf
    }

    /// Current length in bytes.
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    /// True when nothing has been written.
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// Align to the next octet boundary (close any open bit group).
    fn align(&mut self) {
        self.bits_in_octet = 0;
    }

    /// Write a single AMQP bit (packed LSB-first within an octet).
    pub fn write_bit(&mut self, value: bool) {
        if self.bits_in_octet == 0 {
            self.buf.push(0);
        }
        if value {
            let idx = self.buf.len() - 1;
            self.buf[idx] |= 1 << self.bits_in_octet;
        }
        self.bits_in_octet += 1;
        if self.bits_in_octet == 8 {
            self.bits_in_octet = 0;
        }
    }

    /// Write an octet.
    pub fn write_octet(&mut self, value: u8) {
        self.align();
        self.buf.push(value);
    }

    /// Write a 16-bit big-endian integer.
    pub fn write_short(&mut self, value: u16) {
        self.align();
        self.buf.extend_from_slice(&value.to_be_bytes());
    }

    /// Write a 32-bit big-endian integer.
    pub fn write_long(&mut self, value: u32) {
        self.align();
        self.buf.extend_from_slice(&value.to_be_bytes());
    }

    /// Write a 64-bit big-endian integer.
    pub fn write_longlong(&mut self, value: u64) {
        self.align();
        self.buf.extend_from_slice(&value.to_be_bytes());
    }

    /// Write a short string (`shortstr`).
    pub fn write_shortstr(&mut self, value: &str) -> Result<()> {
        if value.len() > SHORTSTR_MAX {
            return Err(Error::InvalidShortstr(value.len()));
        }
        self.align();
        self.buf.push(value.len() as u8);
        self.buf.extend_from_slice(value.as_bytes());
        Ok(())
    }

    /// Write a long string (`longstr`) — binary-safe.
    pub fn write_longstr(&mut self, value: &[u8]) -> Result<()> {
        let len = u32::try_from(value.len()).map_err(|_| Error::InvalidLongstr(value.len()))?;
        self.align();
        self.buf.extend_from_slice(&len.to_be_bytes());
        self.buf.extend_from_slice(value);
        Ok(())
    }

    /// Write a field table.
    pub fn write_table(&mut self, table: &FieldTable) -> Result<()> {
        self.align();
        // Encode entries into a temporary buffer to know the length prefix.
        let mut inner = Encoder::with_capacity(64);
        for (name, value) in &table.entries {
            inner.write_shortstr(name)?;
            inner.write_field_value(value)?;
        }
        let bytes = inner.finish();
        let len = u32::try_from(bytes.len()).map_err(|_| Error::InvalidTableLength(bytes.len()))?;
        self.buf.extend_from_slice(&len.to_be_bytes());
        self.buf.extend_from_slice(&bytes);
        Ok(())
    }

    /// Write a typed field value (type tag + payload).
    pub fn write_field_value(&mut self, value: &FieldValue) -> Result<()> {
        self.align();
        match value {
            FieldValue::Bool(v) => {
                self.buf.push(b't');
                self.buf.push(u8::from(*v));
            }
            FieldValue::I8(v) => {
                self.buf.push(b'b');
                self.buf.push(*v as u8);
            }
            FieldValue::U8(v) => {
                self.buf.push(b'B');
                self.buf.push(*v);
            }
            FieldValue::I16(v) => {
                // RabbitMQ dialect: `s` = signed short (not short-string).
                self.buf.push(b's');
                self.buf.extend_from_slice(&v.to_be_bytes());
            }
            FieldValue::U16(v) => {
                self.buf.push(b'u');
                self.buf.extend_from_slice(&v.to_be_bytes());
            }
            FieldValue::I32(v) => {
                self.buf.push(b'I');
                self.buf.extend_from_slice(&v.to_be_bytes());
            }
            FieldValue::U32(v) => {
                self.buf.push(b'i');
                self.buf.extend_from_slice(&v.to_be_bytes());
            }
            FieldValue::I64(v) => {
                // RabbitMQ dialect: `l` = signed long-long.
                self.buf.push(b'l');
                self.buf.extend_from_slice(&v.to_be_bytes());
            }
            FieldValue::U64(v) => {
                // No unsigned 64-bit tag in RabbitMQ; emit as signed `l` when safe.
                let signed = i64::try_from(*v).map_err(|_| Error::ValueOutOfRange("u64 as i64"))?;
                self.buf.push(b'l');
                self.buf.extend_from_slice(&signed.to_be_bytes());
            }
            FieldValue::F32(v) => {
                self.buf.push(b'f');
                self.buf.extend_from_slice(&v.to_bits().to_be_bytes());
            }
            FieldValue::F64(v) => {
                self.buf.push(b'd');
                self.buf.extend_from_slice(&v.to_bits().to_be_bytes());
            }
            FieldValue::Decimal { scale, value: dec } => {
                self.buf.push(b'D');
                self.buf.push(*scale);
                self.buf.extend_from_slice(&dec.to_be_bytes());
            }
            FieldValue::ShortString(s) => {
                // RabbitMQ has no short-string field-value; use long-string `S`.
                self.buf.push(b'S');
                let len = u32::try_from(s.len()).map_err(|_| Error::InvalidLongstr(s.len()))?;
                self.buf.extend_from_slice(&len.to_be_bytes());
                self.buf.extend_from_slice(s.as_bytes());
            }
            FieldValue::LongString(bytes) => {
                self.buf.push(b'S');
                let len =
                    u32::try_from(bytes.len()).map_err(|_| Error::InvalidLongstr(bytes.len()))?;
                self.buf.extend_from_slice(&len.to_be_bytes());
                self.buf.extend_from_slice(bytes);
            }
            FieldValue::Array(items) => {
                self.buf.push(b'A');
                let mut inner = Encoder::with_capacity(32);
                for item in items {
                    inner.write_field_value(item)?;
                }
                let bytes = inner.finish();
                let len = u32::try_from(bytes.len())
                    .map_err(|_| Error::InvalidTableLength(bytes.len()))?;
                self.buf.extend_from_slice(&len.to_be_bytes());
                self.buf.extend_from_slice(&bytes);
            }
            FieldValue::Timestamp(ts) => {
                self.buf.push(b'T');
                self.buf.extend_from_slice(&ts.to_be_bytes());
            }
            FieldValue::Table(table) => {
                self.buf.push(b'F');
                // write_table will align again (no-op) and write length+body
                // But we already pushed 'F'; write table body with length.
                let mut inner = Encoder::with_capacity(64);
                for (name, val) in &table.entries {
                    inner.write_shortstr(name)?;
                    inner.write_field_value(val)?;
                }
                let bytes = inner.finish();
                let len = u32::try_from(bytes.len())
                    .map_err(|_| Error::InvalidTableLength(bytes.len()))?;
                self.buf.extend_from_slice(&len.to_be_bytes());
                self.buf.extend_from_slice(&bytes);
            }
            FieldValue::Void => {
                self.buf.push(b'V');
            }
            FieldValue::Bytes(bytes) => {
                self.buf.push(b'x');
                let len =
                    u32::try_from(bytes.len()).map_err(|_| Error::InvalidLongstr(bytes.len()))?;
                self.buf.extend_from_slice(&len.to_be_bytes());
                self.buf.extend_from_slice(bytes);
            }
        }
        Ok(())
    }
}

/// Streaming decoder for AMQP method arguments and table content.
#[derive(Debug)]
pub struct Decoder<'a> {
    data: &'a [u8],
    pos: usize,
    bit_buf: u8,
    bits_left: u8,
}

impl<'a> Decoder<'a> {
    /// Create a decoder over `data`.
    pub fn new(data: &'a [u8]) -> Self {
        Self {
            data,
            pos: 0,
            bit_buf: 0,
            bits_left: 0,
        }
    }

    /// Bytes remaining (ignores partial bit state).
    pub fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.pos)
    }

    /// True when all bytes have been consumed (bit residue allowed / ignored).
    pub fn is_empty(&self) -> bool {
        self.pos >= self.data.len()
    }

    /// Number of bytes consumed from the input.
    pub fn position(&self) -> usize {
        self.pos
    }

    /// Discard any remaining bits in the current bit-octet.
    fn align(&mut self) {
        self.bits_left = 0;
    }

    fn need(&self, n: usize) -> Result<()> {
        if self.remaining() < n {
            return Err(Error::TruncatedMethod {
                need: n - self.remaining(),
                have: self.remaining(),
            });
        }
        Ok(())
    }

    /// Ensure the entire payload was consumed.
    pub fn finish(self) -> Result<()> {
        if self.pos < self.data.len() {
            return Err(Error::TrailingBytes(self.data.len() - self.pos));
        }
        Ok(())
    }

    /// Read a packed bit.
    pub fn read_bit(&mut self) -> Result<bool> {
        if self.bits_left == 0 {
            self.need(1)?;
            self.bit_buf = self.data[self.pos];
            self.pos += 1;
            self.bits_left = 8;
        }
        let bit = (self.bit_buf & 1) != 0;
        self.bit_buf >>= 1;
        self.bits_left -= 1;
        Ok(bit)
    }

    /// Read an octet.
    pub fn read_octet(&mut self) -> Result<u8> {
        self.align();
        self.need(1)?;
        let v = self.data[self.pos];
        self.pos += 1;
        Ok(v)
    }

    /// Read a 16-bit big-endian integer.
    pub fn read_short(&mut self) -> Result<u16> {
        self.align();
        self.need(2)?;
        let v = u16::from_be_bytes([self.data[self.pos], self.data[self.pos + 1]]);
        self.pos += 2;
        Ok(v)
    }

    /// Read a 32-bit big-endian integer.
    pub fn read_long(&mut self) -> Result<u32> {
        self.align();
        self.need(4)?;
        let v = u32::from_be_bytes([
            self.data[self.pos],
            self.data[self.pos + 1],
            self.data[self.pos + 2],
            self.data[self.pos + 3],
        ]);
        self.pos += 4;
        Ok(v)
    }

    /// Read a 64-bit big-endian integer.
    pub fn read_longlong(&mut self) -> Result<u64> {
        self.align();
        self.need(8)?;
        let mut b = [0u8; 8];
        b.copy_from_slice(&self.data[self.pos..self.pos + 8]);
        self.pos += 8;
        Ok(u64::from_be_bytes(b))
    }

    /// Read a short string.
    pub fn read_shortstr(&mut self) -> Result<String> {
        self.align();
        let len = self.read_octet()? as usize;
        self.need(len)?;
        let bytes = &self.data[self.pos..self.pos + len];
        self.pos += len;
        let s = std::str::from_utf8(bytes).map_err(|_| Error::InvalidUtf8)?;
        Ok(s.to_owned())
    }

    /// Read a long string (binary-safe).
    pub fn read_longstr(&mut self) -> Result<Vec<u8>> {
        self.align();
        let len = self.read_long()? as usize;
        self.need(len)?;
        let bytes = self.data[self.pos..self.pos + len].to_vec();
        self.pos += len;
        Ok(bytes)
    }

    /// Read a field table.
    pub fn read_table(&mut self) -> Result<FieldTable> {
        self.align();
        let len = self.read_long()? as usize;
        self.need(len)?;
        let end = self.pos + len;
        let mut entries = Vec::new();
        while self.pos < end {
            // Nested decoder over remaining table body so need() stays accurate.
            if self.pos >= end {
                break;
            }
            let name = self.read_shortstr_within(end)?;
            let value = self.read_field_value_within(end)?;
            entries.push((name, value));
        }
        if self.pos != end {
            // Over-read or under-read of declared table length.
            return Err(Error::InvalidTableLength(len));
        }
        Ok(FieldTable { entries })
    }

    fn read_shortstr_within(&mut self, end: usize) -> Result<String> {
        self.align();
        if self.pos >= end {
            return Err(Error::TruncatedMethod { need: 1, have: 0 });
        }
        let len = self.data[self.pos] as usize;
        self.pos += 1;
        if self.pos + len > end {
            return Err(Error::InvalidShortstr(len));
        }
        let bytes = &self.data[self.pos..self.pos + len];
        self.pos += len;
        let s = std::str::from_utf8(bytes).map_err(|_| Error::InvalidUtf8)?;
        Ok(s.to_owned())
    }

    fn ensure_within(&self, end: usize, n: usize) -> Result<()> {
        let have = end.saturating_sub(self.pos);
        if have < n {
            return Err(Error::TruncatedMethod {
                need: n - have,
                have,
            });
        }
        Ok(())
    }

    fn read_field_value_within(&mut self, end: usize) -> Result<FieldValue> {
        self.align();
        self.ensure_within(end, 1)?;
        let tag = self.data[self.pos];
        self.pos += 1;
        self.read_field_value_body(tag, end)
    }

    /// Read a field value (type tag + payload) unbounded by a table length.
    pub fn read_field_value(&mut self) -> Result<FieldValue> {
        self.align();
        let tag = self.read_octet()?;
        let end = self.data.len();
        self.read_field_value_body(tag, end)
    }

    fn read_field_value_body(&mut self, tag: u8, end: usize) -> Result<FieldValue> {
        match tag {
            b't' => {
                self.ensure_within(end, 1)?;
                let v = self.data[self.pos];
                self.pos += 1;
                Ok(FieldValue::Bool(v != 0))
            }
            b'b' => {
                self.ensure_within(end, 1)?;
                let v = self.data[self.pos] as i8;
                self.pos += 1;
                Ok(FieldValue::I8(v))
            }
            b'B' => {
                self.ensure_within(end, 1)?;
                let v = self.data[self.pos];
                self.pos += 1;
                Ok(FieldValue::U8(v))
            }
            // Official XML tag `U` (short-int) — accept for compatibility.
            b'U' => {
                self.ensure_within(end, 2)?;
                let v = i16::from_be_bytes([self.data[self.pos], self.data[self.pos + 1]]);
                self.pos += 2;
                Ok(FieldValue::I16(v))
            }
            b'u' => {
                self.ensure_within(end, 2)?;
                let v = u16::from_be_bytes([self.data[self.pos], self.data[self.pos + 1]]);
                self.pos += 2;
                Ok(FieldValue::U16(v))
            }
            b'I' => {
                self.ensure_within(end, 4)?;
                let v = i32::from_be_bytes([
                    self.data[self.pos],
                    self.data[self.pos + 1],
                    self.data[self.pos + 2],
                    self.data[self.pos + 3],
                ]);
                self.pos += 4;
                Ok(FieldValue::I32(v))
            }
            b'i' => {
                self.ensure_within(end, 4)?;
                let v = u32::from_be_bytes([
                    self.data[self.pos],
                    self.data[self.pos + 1],
                    self.data[self.pos + 2],
                    self.data[self.pos + 3],
                ]);
                self.pos += 4;
                Ok(FieldValue::U32(v))
            }
            // Official XML `L` and RabbitMQ `l`/`L` all mean signed long-long.
            b'L' | b'l' => {
                self.ensure_within(end, 8)?;
                let mut b = [0u8; 8];
                b.copy_from_slice(&self.data[self.pos..self.pos + 8]);
                self.pos += 8;
                Ok(FieldValue::I64(i64::from_be_bytes(b)))
            }
            b'f' => {
                self.ensure_within(end, 4)?;
                let bits = u32::from_be_bytes([
                    self.data[self.pos],
                    self.data[self.pos + 1],
                    self.data[self.pos + 2],
                    self.data[self.pos + 3],
                ]);
                self.pos += 4;
                Ok(FieldValue::F32(f32::from_bits(bits)))
            }
            b'd' => {
                self.ensure_within(end, 8)?;
                let mut b = [0u8; 8];
                b.copy_from_slice(&self.data[self.pos..self.pos + 8]);
                self.pos += 8;
                Ok(FieldValue::F64(f64::from_bits(u64::from_be_bytes(b))))
            }
            b'D' => {
                self.ensure_within(end, 5)?;
                let scale = self.data[self.pos];
                let value = i32::from_be_bytes([
                    self.data[self.pos + 1],
                    self.data[self.pos + 2],
                    self.data[self.pos + 3],
                    self.data[self.pos + 4],
                ]);
                self.pos += 5;
                Ok(FieldValue::Decimal { scale, value })
            }
            // RabbitMQ dialect: `s` is signed short (i16), not short-string.
            b's' => {
                self.ensure_within(end, 2)?;
                let v = i16::from_be_bytes([self.data[self.pos], self.data[self.pos + 1]]);
                self.pos += 2;
                Ok(FieldValue::I16(v))
            }
            b'S' => {
                self.ensure_within(end, 4)?;
                let len = u32::from_be_bytes([
                    self.data[self.pos],
                    self.data[self.pos + 1],
                    self.data[self.pos + 2],
                    self.data[self.pos + 3],
                ]) as usize;
                self.pos += 4;
                self.ensure_within(end, len)?;
                let bytes = self.data[self.pos..self.pos + len].to_vec();
                self.pos += len;
                Ok(FieldValue::LongString(bytes))
            }
            b'A' => {
                self.ensure_within(end, 4)?;
                let len = u32::from_be_bytes([
                    self.data[self.pos],
                    self.data[self.pos + 1],
                    self.data[self.pos + 2],
                    self.data[self.pos + 3],
                ]) as usize;
                self.pos += 4;
                self.ensure_within(end, len)?;
                let arr_end = self.pos + len;
                let mut items = Vec::new();
                while self.pos < arr_end {
                    items.push(self.read_field_value_within(arr_end)?);
                }
                if self.pos != arr_end {
                    return Err(Error::InvalidTableLength(len));
                }
                Ok(FieldValue::Array(items))
            }
            b'T' => {
                self.ensure_within(end, 8)?;
                let mut b = [0u8; 8];
                b.copy_from_slice(&self.data[self.pos..self.pos + 8]);
                self.pos += 8;
                Ok(FieldValue::Timestamp(u64::from_be_bytes(b)))
            }
            b'F' => {
                // Nested table — reuse read_table but constrained.
                self.ensure_within(end, 4)?;
                let len = u32::from_be_bytes([
                    self.data[self.pos],
                    self.data[self.pos + 1],
                    self.data[self.pos + 2],
                    self.data[self.pos + 3],
                ]) as usize;
                self.pos += 4;
                self.ensure_within(end, len)?;
                let table_end = self.pos + len;
                let mut entries = Vec::new();
                while self.pos < table_end {
                    let name = self.read_shortstr_within(table_end)?;
                    let value = self.read_field_value_within(table_end)?;
                    entries.push((name, value));
                }
                if self.pos != table_end {
                    return Err(Error::InvalidTableLength(len));
                }
                Ok(FieldValue::Table(FieldTable { entries }))
            }
            b'V' => Ok(FieldValue::Void),
            b'x' => {
                self.ensure_within(end, 4)?;
                let len = u32::from_be_bytes([
                    self.data[self.pos],
                    self.data[self.pos + 1],
                    self.data[self.pos + 2],
                    self.data[self.pos + 3],
                ]) as usize;
                self.pos += 4;
                self.ensure_within(end, len)?;
                let bytes = self.data[self.pos..self.pos + len].to_vec();
                self.pos += len;
                Ok(FieldValue::Bytes(bytes))
            }
            other => Err(Error::UnknownFieldValueType(other)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
