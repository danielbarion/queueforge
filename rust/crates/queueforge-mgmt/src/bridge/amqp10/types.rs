//! The AMQP 1.0 type system: a [`V`] tree, decoded from and encoded to the
//! wire. Only the shapes the broker uses are encoded compactly; everything
//! decodes.

#[derive(Clone, Debug, PartialEq)]
pub enum V {
    Null,
    Bool(bool),
    Ubyte(u8),
    Ushort(u16),
    Uint(u32),
    Ulong(u64),
    Byte(i8),
    Short(i16),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    Char(char),
    Timestamp(i64),
    Uuid([u8; 16]),
    Binary(Vec<u8>),
    Str(String),
    Sym(String),
    List(Vec<V>),
    Map(Vec<(V, V)>),
    /// Elements all share one constructor; only symbols and strings are encoded.
    Array(Vec<V>),
    Described(Box<V>, Box<V>),
    /// decimal32/64/128, kept as raw bytes.
    Decimal(Vec<u8>),
}

impl V {
    pub fn described(code: u64, value: V) -> V {
        V::Described(Box::new(V::Ulong(code)), Box::new(value))
    }

    pub fn sym(s: &str) -> V {
        V::Sym(s.to_string())
    }

    /// A described value's descriptor as a numeric code; symbolic descriptors are mapped.
    pub fn code(&self) -> Option<u64> {
        let V::Described(d, _) = self else { return None };
        match d.as_ref() {
            V::Ulong(n) => Some(*n),
            V::Uint(n) => Some(u64::from(*n)),
            V::Ubyte(n) => Some(u64::from(*n)),
            V::Sym(s) => match s.as_str() {
                "amqp:accepted:list" => Some(0x24),
                "amqp:rejected:list" => Some(0x25),
                "amqp:released:list" => Some(0x26),
                "amqp:modified:list" => Some(0x27),
                "amqp:source:list" => Some(0x28),
                "amqp:target:list" => Some(0x29),
                "amqp:data:binary" => Some(0x75),
                _ => None,
            },
            _ => None,
        }
    }

    /// The value inside a described type, or the value itself.
    pub fn inner(&self) -> &V {
        match self {
            V::Described(_, v) => v,
            v => v,
        }
    }

    /// Field `i` of a list; missing trailing fields are null.
    pub fn field(&self, i: usize) -> &V {
        match self.inner() {
            V::List(items) => items.get(i).unwrap_or(&V::Null),
            _ => &V::Null,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, V::Null)
    }

    pub fn as_u64(&self) -> Option<u64> {
        match self {
            V::Ubyte(n) => Some(u64::from(*n)),
            V::Ushort(n) => Some(u64::from(*n)),
            V::Uint(n) => Some(u64::from(*n)),
            V::Ulong(n) => Some(*n),
            V::Byte(n) => u64::try_from(*n).ok(),
            V::Short(n) => u64::try_from(*n).ok(),
            V::Int(n) => u64::try_from(*n).ok(),
            V::Long(n) => u64::try_from(*n).ok(),
            _ => None,
        }
    }

    pub fn as_u32(&self) -> Option<u32> {
        self.as_u64().and_then(|n| u32::try_from(n).ok())
    }

    pub fn as_bool(&self) -> bool {
        matches!(self, V::Bool(true))
    }

    /// Strings and symbols as text.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            V::Str(s) | V::Sym(s) => Some(s),
            _ => None,
        }
    }
}

pub struct Decoder<'a> {
    b: &'a [u8],
    pub at: usize,
}

type R<T> = Result<T, String>;

impl<'a> Decoder<'a> {
    pub fn new(b: &'a [u8]) -> Self {
        Decoder { b, at: 0 }
    }

    pub fn more(&self) -> bool {
        self.at < self.b.len()
    }

    fn take(&mut self, n: usize) -> R<&'a [u8]> {
        if self.at + n > self.b.len() {
            return Err("amqp10: short value".into());
        }
        let out = &self.b[self.at..self.at + n];
        self.at += n;
        Ok(out)
    }

    fn u8(&mut self) -> R<u8> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> R<u32> {
        let b = self.take(4)?;
        Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn arr<const N: usize>(&mut self) -> R<[u8; N]> {
        let b = self.take(N)?;
        let mut out = [0u8; N];
        out.copy_from_slice(b);
        Ok(out)
    }

    pub fn value(&mut self) -> R<V> {
        let code = self.u8()?;
        if code == 0x00 {
            let d = self.value()?;
            let v = self.value()?;
            return Ok(V::Described(Box::new(d), Box::new(v)));
        }
        self.body(code)
    }

    fn text(&mut self, n: usize) -> R<String> {
        String::from_utf8(self.take(n)?.to_vec()).map_err(|_| "amqp10: bad utf-8".to_string())
    }

    fn body(&mut self, code: u8) -> R<V> {
        Ok(match code {
            0x40 => V::Null,
            0x41 => V::Bool(true),
            0x42 => V::Bool(false),
            0x56 => V::Bool(self.u8()? != 0),
            0x43 => V::Uint(0),
            0x44 => V::Ulong(0),
            0x50 => V::Ubyte(self.u8()?),
            0x51 => V::Byte(self.u8()? as i8),
            0x52 => V::Uint(u32::from(self.u8()?)),
            0x53 => V::Ulong(u64::from(self.u8()?)),
            0x54 => V::Int(i32::from(self.u8()? as i8)),
            0x55 => V::Long(i64::from(self.u8()? as i8)),
            0x60 => V::Ushort(u16::from_be_bytes(self.arr()?)),
            0x61 => V::Short(i16::from_be_bytes(self.arr()?)),
            0x70 => V::Uint(self.u32()?),
            0x71 => V::Int(i32::from_be_bytes(self.arr()?)),
            0x72 => V::Float(f32::from_be_bytes(self.arr()?)),
            0x73 => V::Char(char::from_u32(self.u32()?).unwrap_or('\u{fffd}')),
            0x74 => V::Decimal(self.take(4)?.to_vec()),
            0x80 => V::Ulong(u64::from_be_bytes(self.arr()?)),
            0x81 => V::Long(i64::from_be_bytes(self.arr()?)),
            0x82 => V::Double(f64::from_be_bytes(self.arr()?)),
            0x83 => V::Timestamp(i64::from_be_bytes(self.arr()?)),
            0x84 => V::Decimal(self.take(8)?.to_vec()),
            0x94 => V::Decimal(self.take(16)?.to_vec()),
            0x98 => V::Uuid(self.arr()?),
            0xa0 => {
                let n = self.u8()? as usize;
                V::Binary(self.take(n)?.to_vec())
            }
            0xb0 => {
                let n = self.u32()? as usize;
                V::Binary(self.take(n)?.to_vec())
            }
            0xa1 => {
                let n = self.u8()? as usize;
                V::Str(self.text(n)?)
            }
            0xb1 => {
                let n = self.u32()? as usize;
                V::Str(self.text(n)?)
            }
            0xa3 => {
                let n = self.u8()? as usize;
                V::Sym(self.text(n)?)
            }
            0xb3 => {
                let n = self.u32()? as usize;
                V::Sym(self.text(n)?)
            }
            0x45 => V::List(Vec::new()),
            0xc0 | 0xd0 | 0xc1 | 0xd1 => {
                let wide = code & 0x10 != 0;
                let count = if wide {
                    self.u32()?;
                    self.u32()? as usize
                } else {
                    self.u8()?;
                    self.u8()? as usize
                };
                let mut items = Vec::with_capacity(count.min(1024));
                for _ in 0..count {
                    items.push(self.value()?);
                }
                if code == 0xc1 || code == 0xd1 {
                    let mut pairs = Vec::with_capacity(items.len() / 2);
                    let mut it = items.into_iter();
                    while let (Some(k), Some(v)) = (it.next(), it.next()) {
                        pairs.push((k, v));
                    }
                    V::Map(pairs)
                } else {
                    V::List(items)
                }
            }
            0xe0 | 0xf0 => {
                let count = if code == 0xf0 {
                    self.u32()?;
                    self.u32()? as usize
                } else {
                    self.u8()?;
                    self.u8()? as usize
                };
                let mut elem = self.u8()?;
                let mut descriptor = None;
                if elem == 0x00 {
                    descriptor = Some(self.value()?);
                    elem = self.u8()?;
                }
                let mut items = Vec::with_capacity(count.min(1024));
                for _ in 0..count {
                    let item = self.body(elem)?;
                    items.push(match &descriptor {
                        Some(d) => V::Described(Box::new(d.clone()), Box::new(item)),
                        None => item,
                    });
                }
                V::Array(items)
            }
            other => return Err(format!("amqp10: unknown type 0x{other:02x}")),
        })
    }
}

#[derive(Default)]
pub struct Encoder {
    pub buf: Vec<u8>,
}

impl Encoder {
    pub fn new() -> Self {
        Encoder { buf: Vec::with_capacity(128) }
    }

    pub fn value(&mut self, v: &V) {
        let b = &mut self.buf;
        match v {
            V::Null => b.push(0x40),
            V::Bool(true) => b.push(0x41),
            V::Bool(false) => b.push(0x42),
            V::Ubyte(n) => b.extend([0x50, *n]),
            V::Byte(n) => b.extend([0x51, *n as u8]),
            V::Ushort(n) => {
                b.push(0x60);
                b.extend(n.to_be_bytes());
            }
            V::Short(n) => {
                b.push(0x61);
                b.extend(n.to_be_bytes());
            }
            V::Uint(0) => b.push(0x43),
            V::Uint(n) if *n < 256 => b.extend([0x52, *n as u8]),
            V::Uint(n) => {
                b.push(0x70);
                b.extend(n.to_be_bytes());
            }
            V::Ulong(0) => b.push(0x44),
            V::Ulong(n) if *n < 256 => b.extend([0x53, *n as u8]),
            V::Ulong(n) => {
                b.push(0x80);
                b.extend(n.to_be_bytes());
            }
            V::Int(n) if (-128..128).contains(n) => b.extend([0x54, *n as i8 as u8]),
            V::Int(n) => {
                b.push(0x71);
                b.extend(n.to_be_bytes());
            }
            V::Long(n) if (-128..128).contains(n) => b.extend([0x55, *n as i8 as u8]),
            V::Long(n) => {
                b.push(0x81);
                b.extend(n.to_be_bytes());
            }
            V::Float(n) => {
                b.push(0x72);
                b.extend(n.to_be_bytes());
            }
            V::Double(n) => {
                b.push(0x82);
                b.extend(n.to_be_bytes());
            }
            V::Char(c) => {
                b.push(0x73);
                b.extend((*c as u32).to_be_bytes());
            }
            V::Timestamp(n) => {
                b.push(0x83);
                b.extend(n.to_be_bytes());
            }
            V::Uuid(u) => {
                b.push(0x98);
                b.extend(u);
            }
            V::Decimal(raw) => {
                b.push(match raw.len() {
                    4 => 0x74,
                    8 => 0x84,
                    _ => 0x94,
                });
                b.extend(raw);
            }
            V::Binary(x) => self.variable(x, 0xa0, 0xb0),
            V::Str(s) => self.variable(s.as_bytes(), 0xa1, 0xb1),
            V::Sym(s) => self.variable(s.as_bytes(), 0xa3, 0xb3),
            V::List(items) if items.is_empty() => b.push(0x45),
            V::List(items) => self.compound(items.iter(), items.len(), 0xc0, 0xd0),
            V::Map(pairs) => {
                let flat: Vec<&V> = pairs.iter().flat_map(|(k, v)| [k, v]).collect();
                let n = flat.len();
                self.compound(flat.into_iter(), n, 0xc1, 0xd1);
            }
            V::Array(items) => self.array(items),
            V::Described(d, v) => {
                b.push(0x00);
                self.value(d);
                self.value(v);
            }
        }
    }

    fn variable(&mut self, x: &[u8], small: u8, large: u8) {
        if x.len() < 256 {
            self.buf.extend([small, x.len() as u8]);
        } else {
            self.buf.push(large);
            self.buf.extend((x.len() as u32).to_be_bytes());
        }
        self.buf.extend(x);
    }

    fn compound<'v>(&mut self, items: impl Iterator<Item = &'v V>, count: usize, small: u8, large: u8) {
        let mut inner = Encoder::new();
        for item in items {
            inner.value(item);
        }
        if inner.buf.len() + 1 < 256 && count < 256 {
            self.buf.extend([small, (inner.buf.len() + 1) as u8, count as u8]);
        } else {
            self.buf.push(large);
            self.buf.extend(((inner.buf.len() + 4) as u32).to_be_bytes());
            self.buf.extend((count as u32).to_be_bytes());
        }
        self.buf.extend(inner.buf);
    }

    /// Arrays of strings or symbols, with the wide element constructor.
    fn array(&mut self, items: &[V]) {
        let mut inner = Vec::new();
        let mut ctor = 0xb3u8;
        for item in items {
            let (c, s) = match item {
                V::Sym(s) => (0xb3, s),
                V::Str(s) => (0xb1, s),
                _ => continue,
            };
            ctor = c;
            inner.extend((s.len() as u32).to_be_bytes());
            inner.extend(s.as_bytes());
        }
        self.buf.push(0xf0);
        self.buf.extend(((inner.len() + 5) as u32).to_be_bytes());
        self.buf.extend((items.len() as u32).to_be_bytes());
        self.buf.push(ctor);
        self.buf.extend(inner);
    }
}

pub fn encode(v: &V) -> Vec<u8> {
    let mut e = Encoder::new();
    e.value(v);
    e.buf
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_a_performative() {
        let v = V::described(
            0x12,
            V::List(vec![
                V::Str("link".into()),
                V::Uint(3),
                V::Bool(true),
                V::Ubyte(0),
                V::Null,
                V::described(0x28, V::List(vec![V::Str("/queues/q".into())])),
                V::Map(vec![(V::sym("k"), V::Long(-5))]),
                V::Array(vec![V::sym("a"), V::sym("bc")]),
                V::Binary(vec![9; 300]),
            ]),
        );
        let bytes = encode(&v);
        let back = Decoder::new(&bytes).value().unwrap();
        assert_eq!(back.code(), Some(0x12));
        assert_eq!(back.field(1).as_u32(), Some(3));
        assert_eq!(back.field(5).field(0).as_str(), Some("/queues/q"));
        assert_eq!(back.field(7), &V::Array(vec![V::sym("a"), V::sym("bc")]));
        assert_eq!(back.field(8), &V::Binary(vec![9; 300]));
    }
}
