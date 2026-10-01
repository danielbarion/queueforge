//! Streaming decoder for AMQP method arguments and field tables.

use super::{FieldTable, FieldValue};
use crate::error::{Error, Result};

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
