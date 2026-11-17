//! Kafka protocol primitives for the non-flexible request and response
//! versions the consumer client speaks: big-endian fixed-width integers,
//! `i16`-length strings, `i32`-length bytes and arrays, and the zig-zag
//! varints of the v2 record format.

/// A malformed or truncated protocol message.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("malformed Kafka protocol message: {0}")]
pub struct WireError(pub String);

pub type WireResult<T> = Result<T, WireError>;

/// Builds a request body.
#[derive(Debug, Default)]
pub struct Encoder {
    buf: Vec<u8>,
}

impl Encoder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn i8(&mut self, v: i8) -> &mut Self {
        self.buf.push(v as u8);
        self
    }

    pub fn i16(&mut self, v: i16) -> &mut Self {
        self.buf.extend_from_slice(&v.to_be_bytes());
        self
    }

    pub fn i32(&mut self, v: i32) -> &mut Self {
        self.buf.extend_from_slice(&v.to_be_bytes());
        self
    }

    pub fn i64(&mut self, v: i64) -> &mut Self {
        self.buf.extend_from_slice(&v.to_be_bytes());
        self
    }

    pub fn bool(&mut self, v: bool) -> &mut Self {
        self.i8(i8::from(v))
    }

    /// A non-null `STRING` (`i16` length).
    pub fn string(&mut self, s: &str) -> &mut Self {
        self.i16(s.len().min(i16::MAX as usize) as i16);
        self.buf.extend_from_slice(s.as_bytes());
        self
    }

    /// A `NULLABLE_STRING`: `-1` length for null.
    pub fn nullable_string(&mut self, s: Option<&str>) -> &mut Self {
        match s {
            Some(s) => self.string(s),
            None => self.i16(-1),
        }
    }

    /// `BYTES` (`i32` length).
    pub fn bytes(&mut self, b: &[u8]) -> &mut Self {
        self.i32(b.len().min(i32::MAX as usize) as i32);
        self.buf.extend_from_slice(b);
        self
    }

    /// An `ARRAY`: `i32` count, then each item.
    pub fn array<T>(&mut self, items: &[T], mut item: impl FnMut(&mut Self, &T)) -> &mut Self {
        self.i32(items.len().min(i32::MAX as usize) as i32);
        for it in items {
            item(self, it);
        }
        self
    }

    /// A zig-zag `VARINT`.
    pub fn varint(&mut self, v: i32) -> &mut Self {
        self.varlong(i64::from(v))
    }

    /// A zig-zag `VARLONG`.
    pub fn varlong(&mut self, v: i64) -> &mut Self {
        let mut z = ((v << 1) ^ (v >> 63)) as u64;
        loop {
            let byte = (z & 0x7f) as u8;
            z >>= 7;
            if z == 0 {
                self.buf.push(byte);
                return self;
            }
            self.buf.push(byte | 0x80);
        }
    }

    pub fn raw(&mut self, b: &[u8]) -> &mut Self {
        self.buf.extend_from_slice(b);
        self
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.buf
    }
}

/// Reads a response body.
#[derive(Debug)]
pub struct Decoder<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Decoder<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    /// The bytes not read yet, consuming them.
    pub fn rest(&mut self) -> &'a [u8] {
        let rest = &self.buf[self.pos..];
        self.pos = self.buf.len();
        rest
    }

    /// The bytes not read yet, without consuming them.
    pub fn peek_rest(&self) -> &'a [u8] {
        &self.buf[self.pos..]
    }

    pub fn take(&mut self, n: usize) -> WireResult<&'a [u8]> {
        if n > self.remaining() {
            return Err(WireError(format!(
                "needed {n} bytes, {} left",
                self.remaining()
            )));
        }
        let out = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(out)
    }

    fn fixed<const N: usize>(&mut self) -> WireResult<[u8; N]> {
        let mut out = [0u8; N];
        out.copy_from_slice(self.take(N)?);
        Ok(out)
    }

    pub fn i8(&mut self) -> WireResult<i8> {
        Ok(self.fixed::<1>()?[0] as i8)
    }

    pub fn i16(&mut self) -> WireResult<i16> {
        Ok(i16::from_be_bytes(self.fixed()?))
    }

    pub fn i32(&mut self) -> WireResult<i32> {
        Ok(i32::from_be_bytes(self.fixed()?))
    }

    pub fn u32(&mut self) -> WireResult<u32> {
        Ok(u32::from_be_bytes(self.fixed()?))
    }

    pub fn i64(&mut self) -> WireResult<i64> {
        Ok(i64::from_be_bytes(self.fixed()?))
    }

    pub fn bool(&mut self) -> WireResult<bool> {
        Ok(self.i8()? != 0)
    }

    pub fn nullable_string(&mut self) -> WireResult<Option<String>> {
        let len = self.i16()?;
        if len < 0 {
            return Ok(None);
        }
        let raw = self.take(len as usize)?;
        String::from_utf8(raw.to_vec())
            .map(Some)
            .map_err(|_| WireError("string is not UTF-8".to_string()))
    }

    pub fn string(&mut self) -> WireResult<String> {
        self.nullable_string()?
            .ok_or_else(|| WireError("unexpected null string".to_string()))
    }

    pub fn nullable_bytes(&mut self) -> WireResult<Option<&'a [u8]>> {
        let len = self.i32()?;
        if len < 0 {
            return Ok(None);
        }
        self.take(len as usize).map(Some)
    }

    /// An `ARRAY` of items read by `item`; a null array reads as empty.
    pub fn array<T>(
        &mut self,
        mut item: impl FnMut(&mut Self) -> WireResult<T>,
    ) -> WireResult<Vec<T>> {
        let len = self.i32()?;
        if len <= 0 {
            return Ok(Vec::new());
        }
        // Every item takes at least one byte: a count past what is left is
        // corrupt, never a reason to allocate.
        if len as usize > self.remaining() {
            return Err(WireError(format!(
                "array of {len} items in {} bytes",
                self.remaining()
            )));
        }
        let mut out = Vec::with_capacity(len as usize);
        for _ in 0..len {
            out.push(item(self)?);
        }
        Ok(out)
    }

    pub fn varlong(&mut self) -> WireResult<i64> {
        let mut z: u64 = 0;
        for shift in (0..64).step_by(7) {
            let byte = self.i8()? as u8;
            z |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(((z >> 1) as i64) ^ -((z & 1) as i64));
            }
        }
        Err(WireError("varint longer than 10 bytes".to_string()))
    }

    pub fn varint(&mut self) -> WireResult<i32> {
        let v = self.varlong()?;
        i32::try_from(v).map_err(|_| WireError(format!("varint {v} out of range")))
    }

    /// Varint-length bytes of the record format: `-1` is null.
    pub fn var_bytes(&mut self) -> WireResult<Option<&'a [u8]>> {
        let len = self.varint()?;
        if len < 0 {
            return Ok(None);
        }
        self.take(len as usize).map(Some)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varints_round_trip_with_zigzag() {
        for v in [
            0i64,
            1,
            -1,
            63,
            -64,
            64,
            300,
            -300,
            i32::MAX as i64,
            i64::MIN,
            i64::MAX,
        ] {
            let mut e = Encoder::new();
            e.varlong(v);
            let bytes = e.into_bytes();
            assert_eq!(Decoder::new(&bytes).varlong().unwrap(), v, "{v}");
        }
        // Zig-zag: -1 is 0x01, 1 is 0x02, 300 is 0xd8 0x04.
        let mut e = Encoder::new();
        e.varint(-1).varint(1).varint(300);
        assert_eq!(e.into_bytes(), vec![0x01, 0x02, 0xd8, 0x04]);
    }

    #[test]
    fn strings_bytes_and_arrays_read_back() {
        let mut e = Encoder::new();
        e.string("topic")
            .nullable_string(None)
            .bytes(b"abc")
            .array(&[1i32, 2, 3], |e, v| {
                e.i32(*v);
            });
        let bytes = e.into_bytes();
        let mut d = Decoder::new(&bytes);
        assert_eq!(d.string().unwrap(), "topic");
        assert_eq!(d.nullable_string().unwrap(), None);
        assert_eq!(d.nullable_bytes().unwrap(), Some(&b"abc"[..]));
        assert_eq!(d.array(|d| d.i32()).unwrap(), vec![1, 2, 3]);
        assert_eq!(d.remaining(), 0);
    }

    #[test]
    fn truncated_and_oversized_input_is_an_error() {
        assert!(Decoder::new(&[0, 0, 0]).i32().is_err());
        // An array claiming more items than bytes left.
        let mut e = Encoder::new();
        e.i32(1_000_000);
        let bytes = e.into_bytes();
        assert!(Decoder::new(&bytes).array(|d| d.i8()).is_err());
    }
}
