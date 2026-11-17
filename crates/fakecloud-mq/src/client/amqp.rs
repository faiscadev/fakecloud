//! An AMQP 0-9-1 client, enough to declare, publish to and consume from a
//! RabbitMQ queue with manual acknowledgement, on a single channel.

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use super::ClientError;

const FRAME_METHOD: u8 = 1;
const FRAME_HEADER: u8 = 2;
const FRAME_BODY: u8 = 3;
const FRAME_HEARTBEAT: u8 = 8;
const FRAME_END: u8 = 0xCE;
/// The one channel this client opens.
const CHANNEL: u16 = 1;
/// The largest frame this client asks for.
const CLIENT_FRAME_MAX: u32 = 131_072;

const CONNECTION: u16 = 10;
const CHANNEL_CLASS: u16 = 20;
const QUEUE: u16 = 50;
const BASIC: u16 = 60;
const CONFIRM: u16 = 85;

/// An AMQP field value, as RabbitMQ encodes them.
#[derive(Debug, Clone, PartialEq)]
pub enum FieldValue {
    Bool(bool),
    I8(i8),
    U8(u8),
    I16(i16),
    U16(u16),
    I32(i32),
    U32(u32),
    I64(i64),
    F32(f32),
    F64(f64),
    /// `(scale, value)`: `value / 10^scale`.
    Decimal(u8, u32),
    LongString(Vec<u8>),
    Array(Vec<FieldValue>),
    /// Seconds since the epoch.
    Timestamp(u64),
    Table(FieldTable),
    Void,
    Bytes(Vec<u8>),
}

/// An AMQP field table, in wire order.
pub type FieldTable = Vec<(String, FieldValue)>;

/// The `basic` content-class properties of a message.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct BasicProperties {
    pub content_type: Option<String>,
    pub content_encoding: Option<String>,
    pub headers: Option<FieldTable>,
    pub delivery_mode: Option<u8>,
    pub priority: Option<u8>,
    pub correlation_id: Option<String>,
    pub reply_to: Option<String>,
    pub expiration: Option<String>,
    pub message_id: Option<String>,
    /// Seconds since the epoch.
    pub timestamp: Option<u64>,
    pub kind: Option<String>,
    pub user_id: Option<String>,
    pub app_id: Option<String>,
    pub cluster_id: Option<String>,
}

/// A message the broker delivered to this client's consumer.
#[derive(Debug, Clone, PartialEq)]
pub struct Delivery {
    pub delivery_tag: u64,
    pub redelivered: bool,
    pub exchange: String,
    pub routing_key: String,
    pub properties: BasicProperties,
    pub body: Vec<u8>,
}

struct Frame {
    kind: u8,
    channel: u16,
    payload: Vec<u8>,
}

struct PendingDelivery {
    delivery: Delivery,
    body_size: Option<u64>,
}

/// An open AMQP 0-9-1 connection with one open channel.
pub struct AmqpConnection {
    stream: TcpStream,
    buf: Vec<u8>,
    frame_max: u32,
    pending: Option<PendingDelivery>,
    confirms: bool,
}

impl AmqpConnection {
    /// Connect to `addr` (`host:port`), log in to virtual host `vhost` as
    /// `username` (SASL `PLAIN`) and open the channel. Refused credentials or
    /// a virtual host the user may not use are [`ClientError::Auth`].
    pub async fn connect(
        addr: &str,
        vhost: &str,
        username: &str,
        password: &str,
    ) -> Result<Self, ClientError> {
        let stream = TcpStream::connect(addr).await?;
        let _ = stream.set_nodelay(true);
        let mut conn = Self {
            stream,
            buf: Vec::new(),
            frame_max: CLIENT_FRAME_MAX,
            pending: None,
            confirms: false,
        };
        conn.stream.write_all(b"AMQP\x00\x00\x09\x01").await?;
        conn.handshake(vhost, username, password)
            .await
            .map_err(|e| match e {
                // RabbitMQ closes the connection with 403 ACCESS_REFUSED for bad
                // credentials and 530 NOT_ALLOWED for a forbidden vhost.
                ClientError::Broker(m) if m.starts_with("403") || m.starts_with("530") => {
                    ClientError::Auth(m)
                }
                other => other,
            })?;
        Ok(conn)
    }

    async fn handshake(
        &mut self,
        vhost: &str,
        username: &str,
        password: &str,
    ) -> Result<(), ClientError> {
        self.expect_method(0, CONNECTION, 10).await?;
        let mut enc = Enc::method(CONNECTION, 11);
        enc.table(&vec![
            (
                "product".to_string(),
                FieldValue::LongString(b"fakecloud".to_vec()),
            ),
            (
                "capabilities".to_string(),
                FieldValue::Table(vec![
                    (
                        "authentication_failure_close".to_string(),
                        FieldValue::Bool(true),
                    ),
                    ("consumer_cancel_notify".to_string(), FieldValue::Bool(true)),
                    ("publisher_confirms".to_string(), FieldValue::Bool(true)),
                ]),
            ),
        ]);
        enc.shortstr("PLAIN");
        let mut response = vec![0u8];
        response.extend_from_slice(username.as_bytes());
        response.push(0);
        response.extend_from_slice(password.as_bytes());
        enc.longstr(&response);
        enc.shortstr("en_US");
        self.send_method(0, enc).await?;

        let mut tune = self.expect_method(0, CONNECTION, 30).await?;
        let channel_max = tune.u16()?;
        let server_frame_max = tune.u32()?;
        self.frame_max = match server_frame_max {
            0 => CLIENT_FRAME_MAX,
            n => n.min(CLIENT_FRAME_MAX),
        };
        let mut enc = Enc::method(CONNECTION, 31);
        enc.u16(channel_max);
        enc.u32(self.frame_max);
        // No heartbeats: the broker then never drops a quiet consumer.
        enc.u16(0);
        self.send_method(0, enc).await?;

        let mut enc = Enc::method(CONNECTION, 40);
        enc.shortstr(vhost);
        enc.shortstr("");
        enc.bits(&[false]);
        self.send_method(0, enc).await?;
        self.expect_method(0, CONNECTION, 41).await?;

        let mut enc = Enc::method(CHANNEL_CLASS, 10);
        enc.shortstr("");
        self.send_method(CHANNEL, enc).await?;
        self.expect_method(CHANNEL, CHANNEL_CLASS, 11).await?;
        Ok(())
    }

    /// Let the broker have at most `prefetch` unacknowledged deliveries out
    /// to this channel's consumers.
    pub async fn qos(&mut self, prefetch: u16) -> Result<(), ClientError> {
        let mut enc = Enc::method(BASIC, 10);
        enc.u32(0);
        enc.u16(prefetch);
        enc.bits(&[false]);
        self.send_method(CHANNEL, enc).await?;
        self.expect_method(CHANNEL, BASIC, 11).await?;
        Ok(())
    }

    /// Declare queue `name` (creating it if it does not exist).
    pub async fn declare_queue(&mut self, name: &str, durable: bool) -> Result<(), ClientError> {
        let mut enc = Enc::method(QUEUE, 10);
        enc.u16(0);
        enc.shortstr(name);
        // passive, durable, exclusive, auto-delete, no-wait
        enc.bits(&[false, durable, false, false, false]);
        enc.table(&Vec::new());
        self.send_method(CHANNEL, enc).await?;
        self.expect_method(CHANNEL, QUEUE, 11).await?;
        Ok(())
    }

    /// Put the channel in publisher-confirm mode: [`Self::publish`] then
    /// waits for the broker to confirm each message.
    pub async fn enable_confirms(&mut self) -> Result<(), ClientError> {
        let mut enc = Enc::method(CONFIRM, 10);
        enc.bits(&[false]);
        self.send_method(CHANNEL, enc).await?;
        self.expect_method(CHANNEL, CONFIRM, 11).await?;
        self.confirms = true;
        Ok(())
    }

    /// Publish `body` with `properties` to `exchange` under `routing_key`
    /// (the default exchange `""` routes to the queue of that name).
    pub async fn publish(
        &mut self,
        exchange: &str,
        routing_key: &str,
        properties: &BasicProperties,
        body: &[u8],
    ) -> Result<(), ClientError> {
        let mut enc = Enc::method(BASIC, 40);
        enc.u16(0);
        enc.shortstr(exchange);
        enc.shortstr(routing_key);
        enc.bits(&[false, false]);
        self.send_method(CHANNEL, enc).await?;

        let mut header = Enc(Vec::new());
        header.u16(BASIC);
        header.u16(0);
        header.u64(body.len() as u64);
        encode_properties(&mut header, properties);
        self.send_frame(FRAME_HEADER, CHANNEL, &header.0).await?;
        let chunk = (self.frame_max as usize).saturating_sub(8).max(1);
        for part in body.chunks(chunk) {
            self.send_frame(FRAME_BODY, CHANNEL, part).await?;
        }
        if self.confirms {
            let mut ack = self.expect_method_any(CHANNEL, BASIC, &[80, 120]).await?;
            if ack.1 == 120 {
                return Err(ClientError::Broker(
                    "the broker refused the published message".to_string(),
                ));
            }
            let _ = ack.0.u64()?;
        }
        Ok(())
    }

    /// Start consuming `queue` with manual acknowledgement; returns the
    /// consumer tag. A queue that does not exist closes the channel with
    /// `404 NOT_FOUND`, reported as [`ClientError::Broker`].
    pub async fn consume(&mut self, queue: &str) -> Result<String, ClientError> {
        let mut enc = Enc::method(BASIC, 20);
        enc.u16(0);
        enc.shortstr(queue);
        enc.shortstr("");
        // no-local, no-ack, exclusive, no-wait
        enc.bits(&[false, false, false, false]);
        enc.table(&Vec::new());
        self.send_method(CHANNEL, enc).await?;
        let mut ok = self.expect_method(CHANNEL, BASIC, 21).await?;
        ok.shortstr()
    }

    /// Acknowledge delivery `delivery_tag`.
    pub async fn ack(&mut self, delivery_tag: u64) -> Result<(), ClientError> {
        let mut enc = Enc::method(BASIC, 80);
        enc.u64(delivery_tag);
        enc.bits(&[false]);
        self.send_method(CHANNEL, enc).await
    }

    /// Reject delivery `delivery_tag`; with `requeue` the broker puts it back
    /// on its queue and redelivers it (marked redelivered).
    pub async fn nack(&mut self, delivery_tag: u64, requeue: bool) -> Result<(), ClientError> {
        let mut enc = Enc::method(BASIC, 120);
        enc.u64(delivery_tag);
        enc.bits(&[false, requeue]);
        self.send_method(CHANNEL, enc).await
    }

    /// The next message delivered to this channel's consumer.
    pub async fn next_delivery(&mut self) -> Result<Delivery, ClientError> {
        loop {
            if let Some(done) = self.completed_delivery() {
                return Ok(done);
            }
            let frame = self.read_frame().await?;
            match frame.kind {
                FRAME_HEARTBEAT => {}
                FRAME_METHOD => {
                    let mut dec = Dec::new(frame.payload);
                    let (class, method) = (dec.u16()?, dec.u16()?);
                    self.check_close(frame.channel, class, method, &mut dec)
                        .await?;
                    match (class, method) {
                        (BASIC, 60) => {
                            let _consumer_tag = dec.shortstr()?;
                            let delivery_tag = dec.u64()?;
                            let redelivered = dec.u8()? & 1 == 1;
                            let exchange = dec.shortstr()?;
                            let routing_key = dec.shortstr()?;
                            self.pending = Some(PendingDelivery {
                                delivery: Delivery {
                                    delivery_tag,
                                    redelivered,
                                    exchange,
                                    routing_key,
                                    properties: BasicProperties::default(),
                                    body: Vec::new(),
                                },
                                body_size: None,
                            });
                        }
                        (BASIC, 30) => {
                            return Err(ClientError::Broker(
                                "the broker cancelled the consumer".to_string(),
                            ))
                        }
                        _ => {}
                    }
                }
                FRAME_HEADER => {
                    let mut dec = Dec::new(frame.payload);
                    let _class = dec.u16()?;
                    let _weight = dec.u16()?;
                    let size = dec.u64()?;
                    let properties = decode_properties(&mut dec)?;
                    let pending = self.pending.as_mut().ok_or_else(|| {
                        ClientError::Protocol("content header without a delivery".to_string())
                    })?;
                    pending.delivery.properties = properties;
                    pending.body_size = Some(size);
                }
                FRAME_BODY => {
                    let pending = self.pending.as_mut().ok_or_else(|| {
                        ClientError::Protocol("content body without a delivery".to_string())
                    })?;
                    pending.delivery.body.extend_from_slice(&frame.payload);
                }
                other => return Err(ClientError::Protocol(format!("unknown frame type {other}"))),
            }
        }
    }

    fn completed_delivery(&mut self) -> Option<Delivery> {
        let done = self.pending.as_ref().is_some_and(|p| {
            p.body_size
                .is_some_and(|size| p.delivery.body.len() as u64 >= size)
        });
        if done {
            self.pending.take().map(|p| p.delivery)
        } else {
            None
        }
    }

    /// Close the connection politely.
    pub async fn close(mut self) {
        let mut enc = Enc::method(CONNECTION, 50);
        enc.u16(200);
        enc.shortstr("bye");
        enc.u16(0);
        enc.u16(0);
        if self.send_method(0, enc).await.is_ok() {
            let _ = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                self.expect_method(0, CONNECTION, 51),
            )
            .await;
        }
        let _ = self.stream.shutdown().await;
    }

    /// Answer a `Connection.Close` / `Channel.Close` from the broker and turn
    /// it into an error naming its reply code and text.
    async fn check_close(
        &mut self,
        channel: u16,
        class: u16,
        method: u16,
        dec: &mut Dec,
    ) -> Result<(), ClientError> {
        let close_ok = match (class, method) {
            (CONNECTION, 50) => (CONNECTION, 51),
            (CHANNEL_CLASS, 40) => (CHANNEL_CLASS, 41),
            _ => return Ok(()),
        };
        let code = dec.u16().unwrap_or(0);
        let text = dec.shortstr().unwrap_or_default();
        let _ = self
            .send_method(channel, Enc::method(close_ok.0, close_ok.1))
            .await;
        Err(ClientError::Broker(format!("{code} {text}")))
    }

    async fn expect_method(
        &mut self,
        channel: u16,
        class: u16,
        method: u16,
    ) -> Result<Dec, ClientError> {
        Ok(self.expect_method_any(channel, class, &[method]).await?.0)
    }

    /// Read frames until method `class.<one of methods>` arrives on
    /// `channel`; returns its arguments and which method it was.
    async fn expect_method_any(
        &mut self,
        channel: u16,
        class: u16,
        methods: &[u16],
    ) -> Result<(Dec, u16), ClientError> {
        loop {
            let frame = self.read_frame().await?;
            if frame.kind == FRAME_HEARTBEAT {
                continue;
            }
            if frame.kind != FRAME_METHOD {
                return Err(ClientError::Protocol(format!(
                    "expected a method frame, got frame type {}",
                    frame.kind
                )));
            }
            let mut dec = Dec::new(frame.payload);
            let (c, m) = (dec.u16()?, dec.u16()?);
            self.check_close(frame.channel, c, m, &mut dec).await?;
            if frame.channel == channel && c == class && methods.contains(&m) {
                return Ok((dec, m));
            }
            return Err(ClientError::Protocol(format!(
                "expected method {class}.{methods:?}, got {c}.{m}"
            )));
        }
    }

    async fn read_frame(&mut self) -> Result<Frame, ClientError> {
        loop {
            if let Some(frame) = parse_frame(&mut self.buf)? {
                return Ok(frame);
            }
            let mut chunk = [0u8; 16384];
            let n = self.stream.read(&mut chunk).await?;
            if n == 0 {
                return Err(ClientError::Closed);
            }
            self.buf.extend_from_slice(&chunk[..n]);
        }
    }

    async fn send_method(&mut self, channel: u16, enc: Enc) -> Result<(), ClientError> {
        self.send_frame(FRAME_METHOD, channel, &enc.0).await
    }

    async fn send_frame(
        &mut self,
        kind: u8,
        channel: u16,
        payload: &[u8],
    ) -> Result<(), ClientError> {
        let mut out = Vec::with_capacity(payload.len() + 8);
        out.push(kind);
        out.extend_from_slice(&channel.to_be_bytes());
        out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        out.extend_from_slice(payload);
        out.push(FRAME_END);
        self.stream.write_all(&out).await?;
        Ok(())
    }
}

fn parse_frame(buf: &mut Vec<u8>) -> Result<Option<Frame>, ClientError> {
    if buf.len() < 7 {
        return Ok(None);
    }
    let kind = buf[0];
    let channel = u16::from_be_bytes([buf[1], buf[2]]);
    let size = u32::from_be_bytes([buf[3], buf[4], buf[5], buf[6]]) as usize;
    if buf.len() < 7 + size + 1 {
        return Ok(None);
    }
    if buf[7 + size] != FRAME_END {
        return Err(ClientError::Protocol(
            "frame without its end marker".to_string(),
        ));
    }
    let payload = buf[7..7 + size].to_vec();
    buf.drain(..7 + size + 1);
    Ok(Some(Frame {
        kind,
        channel,
        payload,
    }))
}

/// Encoder for method arguments and content headers.
struct Enc(Vec<u8>);

impl Enc {
    fn method(class: u16, method: u16) -> Self {
        let mut e = Enc(Vec::new());
        e.u16(class);
        e.u16(method);
        e
    }
    fn u8(&mut self, v: u8) {
        self.0.push(v);
    }
    fn u16(&mut self, v: u16) {
        self.0.extend_from_slice(&v.to_be_bytes());
    }
    fn u32(&mut self, v: u32) {
        self.0.extend_from_slice(&v.to_be_bytes());
    }
    fn u64(&mut self, v: u64) {
        self.0.extend_from_slice(&v.to_be_bytes());
    }
    fn shortstr(&mut self, s: &str) {
        let bytes = &s.as_bytes()[..s.len().min(255)];
        self.u8(bytes.len() as u8);
        self.0.extend_from_slice(bytes);
    }
    fn longstr(&mut self, b: &[u8]) {
        self.u32(b.len() as u32);
        self.0.extend_from_slice(b);
    }
    /// Consecutive bit arguments, packed into octets.
    fn bits(&mut self, bits: &[bool]) {
        for chunk in bits.chunks(8) {
            let mut octet = 0u8;
            for (i, b) in chunk.iter().enumerate() {
                if *b {
                    octet |= 1 << i;
                }
            }
            self.u8(octet);
        }
    }
    fn table(&mut self, table: &FieldTable) {
        let mut inner = Enc(Vec::new());
        for (k, v) in table {
            inner.shortstr(k);
            inner.value(v);
        }
        self.longstr(&inner.0);
    }
    fn value(&mut self, v: &FieldValue) {
        match v {
            FieldValue::Bool(b) => {
                self.u8(b't');
                self.u8(u8::from(*b));
            }
            FieldValue::I8(n) => {
                self.u8(b'b');
                self.0.extend_from_slice(&n.to_be_bytes());
            }
            FieldValue::U8(n) => {
                self.u8(b'B');
                self.u8(*n);
            }
            FieldValue::I16(n) => {
                self.u8(b's');
                self.0.extend_from_slice(&n.to_be_bytes());
            }
            FieldValue::U16(n) => {
                self.u8(b'u');
                self.u16(*n);
            }
            FieldValue::I32(n) => {
                self.u8(b'I');
                self.0.extend_from_slice(&n.to_be_bytes());
            }
            FieldValue::U32(n) => {
                self.u8(b'i');
                self.u32(*n);
            }
            FieldValue::I64(n) => {
                self.u8(b'l');
                self.0.extend_from_slice(&n.to_be_bytes());
            }
            FieldValue::F32(n) => {
                self.u8(b'f');
                self.0.extend_from_slice(&n.to_be_bytes());
            }
            FieldValue::F64(n) => {
                self.u8(b'd');
                self.0.extend_from_slice(&n.to_be_bytes());
            }
            FieldValue::Decimal(scale, n) => {
                self.u8(b'D');
                self.u8(*scale);
                self.u32(*n);
            }
            FieldValue::LongString(b) => {
                self.u8(b'S');
                self.longstr(b);
            }
            FieldValue::Array(items) => {
                self.u8(b'A');
                let mut inner = Enc(Vec::new());
                for item in items {
                    inner.value(item);
                }
                self.longstr(&inner.0);
            }
            FieldValue::Timestamp(t) => {
                self.u8(b'T');
                self.u64(*t);
            }
            FieldValue::Table(t) => {
                self.u8(b'F');
                self.table(t);
            }
            FieldValue::Void => self.u8(b'V'),
            FieldValue::Bytes(b) => {
                self.u8(b'x');
                self.longstr(b);
            }
        }
    }
}

/// Decoder over a frame payload.
struct Dec {
    data: Vec<u8>,
    pos: usize,
}

impl Dec {
    fn new(data: Vec<u8>) -> Self {
        Self { data, pos: 0 }
    }
    fn take(&mut self, n: usize) -> Result<&[u8], ClientError> {
        if self.pos + n > self.data.len() {
            return Err(ClientError::Protocol("truncated frame".to_string()));
        }
        let out = &self.data[self.pos..self.pos + n];
        self.pos += n;
        Ok(out)
    }
    fn u8(&mut self) -> Result<u8, ClientError> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16, ClientError> {
        let b = self.take(2)?;
        Ok(u16::from_be_bytes([b[0], b[1]]))
    }
    fn u32(&mut self) -> Result<u32, ClientError> {
        let b = self.take(4)?;
        Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }
    fn u64(&mut self) -> Result<u64, ClientError> {
        let b = self.take(8)?;
        let mut a = [0u8; 8];
        a.copy_from_slice(b);
        Ok(u64::from_be_bytes(a))
    }
    fn shortstr(&mut self) -> Result<String, ClientError> {
        let n = self.u8()? as usize;
        Ok(String::from_utf8_lossy(self.take(n)?).into_owned())
    }
    fn longstr(&mut self) -> Result<Vec<u8>, ClientError> {
        let n = self.u32()? as usize;
        Ok(self.take(n)?.to_vec())
    }
    fn table(&mut self) -> Result<FieldTable, ClientError> {
        let mut inner = Dec::new(self.longstr()?);
        let mut out = Vec::new();
        while inner.pos < inner.data.len() {
            let key = inner.shortstr()?;
            let value = inner.value()?;
            out.push((key, value));
        }
        Ok(out)
    }
    fn value(&mut self) -> Result<FieldValue, ClientError> {
        let tag = self.u8()?;
        Ok(match tag {
            b't' => FieldValue::Bool(self.u8()? != 0),
            b'b' => FieldValue::I8(self.u8()? as i8),
            b'B' => FieldValue::U8(self.u8()?),
            b's' => FieldValue::I16(self.u16()? as i16),
            b'u' => FieldValue::U16(self.u16()?),
            b'I' => FieldValue::I32(self.u32()? as i32),
            b'i' => FieldValue::U32(self.u32()?),
            b'l' => FieldValue::I64(self.u64()? as i64),
            b'f' => FieldValue::F32(f32::from_bits(self.u32()?)),
            b'd' => FieldValue::F64(f64::from_bits(self.u64()?)),
            b'D' => {
                let scale = self.u8()?;
                FieldValue::Decimal(scale, self.u32()?)
            }
            b'S' => FieldValue::LongString(self.longstr()?),
            b'A' => {
                let mut inner = Dec::new(self.longstr()?);
                let mut items = Vec::new();
                while inner.pos < inner.data.len() {
                    items.push(inner.value()?);
                }
                FieldValue::Array(items)
            }
            b'T' => FieldValue::Timestamp(self.u64()?),
            b'F' => FieldValue::Table(self.table()?),
            b'V' => FieldValue::Void,
            b'x' => FieldValue::Bytes(self.longstr()?),
            other => {
                return Err(ClientError::Protocol(format!(
                    "unknown field value type {:?}",
                    other as char
                )))
            }
        })
    }
}

/// Property flag bits of the `basic` class, highest first.
const PROPERTY_BITS: [u16; 14] = [
    1 << 15,
    1 << 14,
    1 << 13,
    1 << 12,
    1 << 11,
    1 << 10,
    1 << 9,
    1 << 8,
    1 << 7,
    1 << 6,
    1 << 5,
    1 << 4,
    1 << 3,
    1 << 2,
];

fn encode_properties(enc: &mut Enc, p: &BasicProperties) {
    let present = [
        p.content_type.is_some(),
        p.content_encoding.is_some(),
        p.headers.is_some(),
        p.delivery_mode.is_some(),
        p.priority.is_some(),
        p.correlation_id.is_some(),
        p.reply_to.is_some(),
        p.expiration.is_some(),
        p.message_id.is_some(),
        p.timestamp.is_some(),
        p.kind.is_some(),
        p.user_id.is_some(),
        p.app_id.is_some(),
        p.cluster_id.is_some(),
    ];
    let flags = present
        .iter()
        .zip(PROPERTY_BITS)
        .filter(|(set, _)| **set)
        .fold(0u16, |acc, (_, bit)| acc | bit);
    enc.u16(flags);
    let short = |enc: &mut Enc, v: &Option<String>| {
        if let Some(s) = v {
            enc.shortstr(s);
        }
    };
    short(enc, &p.content_type);
    short(enc, &p.content_encoding);
    if let Some(h) = &p.headers {
        enc.table(h);
    }
    if let Some(v) = p.delivery_mode {
        enc.u8(v);
    }
    if let Some(v) = p.priority {
        enc.u8(v);
    }
    short(enc, &p.correlation_id);
    short(enc, &p.reply_to);
    short(enc, &p.expiration);
    short(enc, &p.message_id);
    if let Some(v) = p.timestamp {
        enc.u64(v);
    }
    short(enc, &p.kind);
    short(enc, &p.user_id);
    short(enc, &p.app_id);
    short(enc, &p.cluster_id);
}

fn decode_properties(dec: &mut Dec) -> Result<BasicProperties, ClientError> {
    let flags = dec.u16()?;
    let has = |i: usize| flags & PROPERTY_BITS[i] != 0;
    let mut p = BasicProperties::default();
    let short = |dec: &mut Dec, set: bool| -> Result<Option<String>, ClientError> {
        if set {
            dec.shortstr().map(Some)
        } else {
            Ok(None)
        }
    };
    p.content_type = short(dec, has(0))?;
    p.content_encoding = short(dec, has(1))?;
    if has(2) {
        p.headers = Some(dec.table()?);
    }
    if has(3) {
        p.delivery_mode = Some(dec.u8()?);
    }
    if has(4) {
        p.priority = Some(dec.u8()?);
    }
    p.correlation_id = short(dec, has(5))?;
    p.reply_to = short(dec, has(6))?;
    p.expiration = short(dec, has(7))?;
    p.message_id = short(dec, has(8))?;
    if has(9) {
        p.timestamp = Some(dec.u64()?);
    }
    p.kind = short(dec, has(10))?;
    p.user_id = short(dec, has(11))?;
    p.app_id = short(dec, has(12))?;
    p.cluster_id = short(dec, has(13))?;
    Ok(p)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn properties_round_trip() {
        let props = BasicProperties {
            content_type: Some("text/plain".to_string()),
            headers: Some(vec![
                ("a".to_string(), FieldValue::LongString(b"x".to_vec())),
                ("n".to_string(), FieldValue::I32(-7)),
                (
                    "nested".to_string(),
                    FieldValue::Table(vec![("b".to_string(), FieldValue::Bool(true))]),
                ),
                (
                    "list".to_string(),
                    FieldValue::Array(vec![FieldValue::I64(1), FieldValue::Void]),
                ),
            ]),
            delivery_mode: Some(2),
            priority: Some(4),
            expiration: Some("60000".to_string()),
            timestamp: Some(1_700_000_000),
            user_id: Some("guest".to_string()),
            ..Default::default()
        };
        let mut enc = Enc(Vec::new());
        encode_properties(&mut enc, &props);
        let mut dec = Dec::new(enc.0.clone());
        assert_eq!(decode_properties(&mut dec).unwrap(), props);
        assert_eq!(dec.pos, enc.0.len());
    }

    #[test]
    fn frames_parse_only_when_whole() {
        let mut wire = vec![FRAME_HEARTBEAT, 0, 0, 0, 0, 0, 0, FRAME_END];
        wire.extend_from_slice(&[FRAME_BODY, 0, 1, 0, 0, 0, 2, b'h', b'i', FRAME_END]);
        let mut buf = Vec::new();
        let mut frames = Vec::new();
        for byte in wire {
            buf.push(byte);
            while let Some(frame) = parse_frame(&mut buf).unwrap() {
                frames.push(frame);
            }
        }
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].kind, FRAME_HEARTBEAT);
        assert_eq!(frames[1].channel, 1);
        assert_eq!(frames[1].payload, b"hi");
    }

    #[test]
    fn bits_pack_into_octets() {
        let mut enc = Enc(Vec::new());
        enc.bits(&[false, true, false, true]);
        assert_eq!(enc.0, vec![0b1010]);
    }
}
