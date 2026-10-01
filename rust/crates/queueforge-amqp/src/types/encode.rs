//! Streaming encoder for AMQP method arguments and field tables.

use super::{FieldTable, FieldValue, SHORTSTR_MAX};
use crate::error::{Error, Result};

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
