//! A STOMP 1.2 client, enough to produce to and consume from an ActiveMQ
//! queue with client acknowledgement.

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use super::ClientError;

/// One STOMP frame: command, headers in wire order (unescaped), body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StompFrame {
    pub command: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl StompFrame {
    /// The value of header `name`. A repeated header's first occurrence wins,
    /// as STOMP 1.2 specifies.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }
}

/// An authenticated STOMP 1.2 session.
pub struct StompConnection {
    stream: TcpStream,
    buf: FrameBuffer,
}

impl StompConnection {
    /// Connect to `addr` (`host:port`) and log in as `login`. A broker that
    /// refuses the credentials answers `ERROR`, reported as
    /// [`ClientError::Auth`].
    pub async fn connect(addr: &str, login: &str, passcode: &str) -> Result<Self, ClientError> {
        let stream = TcpStream::connect(addr).await?;
        let _ = stream.set_nodelay(true);
        let mut conn = Self {
            stream,
            buf: FrameBuffer::default(),
        };
        // CONNECT headers are never escaped (STOMP 1.2, "Value Encoding").
        let connect = format!(
            "CONNECT\naccept-version:1.2\nhost:/\nheart-beat:0,0\nlogin:{login}\npasscode:{passcode}\n\n\0"
        );
        conn.stream.write_all(connect.as_bytes()).await?;
        let reply = conn.read_frame().await?;
        match reply.command.as_str() {
            "CONNECTED" => Ok(conn),
            "ERROR" => Err(ClientError::Auth(error_text(&reply))),
            other => Err(ClientError::Protocol(format!(
                "expected CONNECTED, got {other}"
            ))),
        }
    }

    /// Subscribe to `destination` as subscription `id` with acknowledgement
    /// mode `ack` (`auto`, `client`, `client-individual`) and any extra
    /// broker-specific headers (`activemq.prefetchSize`).
    pub async fn subscribe(
        &mut self,
        id: &str,
        destination: &str,
        ack: &str,
        extra: &[(&str, &str)],
    ) -> Result<(), ClientError> {
        let mut headers = vec![("id", id), ("destination", destination), ("ack", ack)];
        headers.extend_from_slice(extra);
        self.write_frame("SUBSCRIBE", &headers, b"").await
    }

    /// Send `body` to `destination` with `headers`. ActiveMQ stores a frame
    /// with a `content-length` header as a bytes message and one without as a
    /// text message (whose body then may not hold a NUL byte). With a
    /// `receipt` header the call waits for the broker's `RECEIPT`, so the
    /// message is on the broker when it returns.
    pub async fn send(
        &mut self,
        destination: &str,
        headers: &[(&str, &str)],
        body: &[u8],
    ) -> Result<(), ClientError> {
        let mut all = vec![("destination", destination)];
        all.extend_from_slice(headers);
        let receipt = headers
            .iter()
            .find(|(k, _)| *k == "receipt")
            .map(|(_, v)| v.to_string());
        self.write_frame("SEND", &all, body).await?;
        if let Some(receipt) = receipt {
            loop {
                let frame = self.read_frame().await?;
                match frame.command.as_str() {
                    "RECEIPT" if frame.header("receipt-id") == Some(receipt.as_str()) => {
                        return Ok(())
                    }
                    "ERROR" => return Err(ClientError::Broker(error_text(&frame))),
                    _ => {}
                }
            }
        }
        Ok(())
    }

    /// Acknowledge the message whose `MESSAGE` frame carried `ack_id` in its
    /// `ack` header.
    pub async fn ack(&mut self, ack_id: &str) -> Result<(), ClientError> {
        self.write_frame("ACK", &[("id", ack_id)], b"").await
    }

    /// Read the next frame other than a heart-beat. An `ERROR` frame is
    /// returned like any other; callers decide what it means.
    pub async fn read_frame(&mut self) -> Result<StompFrame, ClientError> {
        loop {
            if let Some(frame) = self.buf.parse()? {
                return Ok(frame);
            }
            let mut chunk = [0u8; 8192];
            let n = self.stream.read(&mut chunk).await?;
            if n == 0 {
                return Err(ClientError::Closed);
            }
            self.buf.bytes.extend_from_slice(&chunk[..n]);
        }
    }

    /// Read the next `MESSAGE` frame. An `ERROR` frame ends the session.
    pub async fn next_message(&mut self) -> Result<StompFrame, ClientError> {
        loop {
            let frame = self.read_frame().await?;
            match frame.command.as_str() {
                "MESSAGE" => return Ok(frame),
                "ERROR" => return Err(ClientError::Broker(error_text(&frame))),
                _ => {}
            }
        }
    }

    /// Say goodbye and close the connection.
    pub async fn disconnect(mut self) {
        let _ = self.write_frame("DISCONNECT", &[], b"").await;
        let _ = self.stream.shutdown().await;
    }

    async fn write_frame(
        &mut self,
        command: &str,
        headers: &[(&str, &str)],
        body: &[u8],
    ) -> Result<(), ClientError> {
        let mut out = Vec::with_capacity(64 + body.len());
        out.extend_from_slice(command.as_bytes());
        out.push(b'\n');
        for (k, v) in headers {
            out.extend_from_slice(escape(k).as_bytes());
            out.push(b':');
            out.extend_from_slice(escape(v).as_bytes());
            out.push(b'\n');
        }
        out.push(b'\n');
        out.extend_from_slice(body);
        out.push(0);
        self.stream.write_all(&out).await?;
        Ok(())
    }
}

fn error_text(frame: &StompFrame) -> String {
    let body = String::from_utf8_lossy(&frame.body).trim().to_string();
    match frame.header("message") {
        Some(m) if body.is_empty() => m.to_string(),
        Some(m) => format!("{m}: {body}"),
        None => body,
    }
}

/// STOMP 1.2 header value escaping.
fn escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            ':' => out.push_str("\\c"),
            c => out.push(c),
        }
    }
    out
}

fn unescape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('n') => out.push('\n'),
            Some('r') => out.push('\r'),
            Some('c') => out.push(':'),
            Some('\\') => out.push('\\'),
            Some(other) => {
                out.push('\\');
                out.push(other);
            }
            None => out.push('\\'),
        }
    }
    out
}

/// The header block of the frame being read.
struct Head {
    body_start: usize,
    command: String,
    headers: Vec<(String, String)>,
    content_length: Option<usize>,
}

/// Bytes read off the connection, parsed into frames incrementally: the
/// header block is parsed once, and the searches for its end and for a
/// body's NUL resume where the previous read left them, so a large frame
/// arriving in many reads costs linear time. A frame over
/// [`super::MAX_FRAME_BYTES`] is [`ClientError::FrameTooLarge`].
#[derive(Default)]
struct FrameBuffer {
    bytes: Vec<u8>,
    head: Option<Head>,
    /// How far the current search has got without finding its terminator.
    scan: usize,
}

impl FrameBuffer {
    /// Parse one frame off the front of the buffer, consuming it, or `None`
    /// when the buffer does not hold a whole frame yet. Leading end-of-line
    /// bytes (heart-beats and the optional EOLs after a frame's NUL) are
    /// skipped.
    fn parse(&mut self) -> Result<Option<StompFrame>, ClientError> {
        if self.head.is_none() {
            let start = self
                .bytes
                .iter()
                .position(|b| *b != b'\n' && *b != b'\r')
                .unwrap_or(self.bytes.len());
            if start > 0 {
                self.bytes.drain(..start);
                self.scan = self.scan.saturating_sub(start);
            }
            if self.bytes.is_empty() {
                return Ok(None);
            }
            // The header block ends at the first blank line (`\n\n` or
            // `\n\r\n`); the frame starts with a non-EOL byte, so a blank
            // line always follows a newline.
            let from = self.scan.max(1);
            let end = (from..self.bytes.len()).find(|&i| {
                self.bytes[i] == b'\n'
                    && (self.bytes[i - 1] == b'\n'
                        || (i >= 2 && self.bytes[i - 1] == b'\r' && self.bytes[i - 2] == b'\n'))
            });
            let Some(end) = end else {
                self.scan = self.bytes.len();
                return self.check_size(0).map(|()| None);
            };
            self.head = Some(parse_head(&self.bytes[..end], end + 1)?);
            self.scan = end + 1;
        }
        let head = self.head.as_ref().expect("header block parsed");
        let body_start = head.body_start;
        let body_end = match head.content_length {
            Some(len) => {
                if len > super::MAX_FRAME_BYTES {
                    return Err(ClientError::FrameTooLarge(len));
                }
                if self.bytes.len() < body_start + len + 1 {
                    return Ok(None);
                }
                if self.bytes[body_start + len] != 0 {
                    return Err(ClientError::Protocol(
                        "frame body is not NUL-terminated at its content-length".to_string(),
                    ));
                }
                body_start + len
            }
            None => {
                let from = self.scan.max(body_start);
                match self.bytes[from..].iter().position(|b| *b == 0) {
                    Some(n) => from + n,
                    None => {
                        self.scan = self.bytes.len();
                        return self.check_size(body_start).map(|()| None);
                    }
                }
            }
        };
        let head = self.head.take().expect("header block parsed");
        let body = self.bytes[body_start..body_end].to_vec();
        self.bytes.drain(..body_end + 1);
        self.scan = 0;
        Ok(Some(StompFrame {
            command: head.command,
            headers: head.headers,
            body,
        }))
    }

    /// Refuse a partial frame that has already grown past the limit.
    fn check_size(&self, body_start: usize) -> Result<(), ClientError> {
        let size = self.bytes.len() - body_start;
        if size > super::MAX_FRAME_BYTES {
            return Err(ClientError::FrameTooLarge(size));
        }
        Ok(())
    }
}

/// Parse a frame's header block (its command and header lines, without the
/// blank line ending it).
fn parse_head(block: &[u8], body_start: usize) -> Result<Head, ClientError> {
    let mut lines = block.split(|b| *b == b'\n').map(|line| {
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        String::from_utf8_lossy(line).into_owned()
    });
    let command = lines
        .next()
        .filter(|c| !c.is_empty())
        .ok_or_else(|| ClientError::Protocol("frame without a command".to_string()))?;
    let unescaped = command != "CONNECTED";
    let headers: Vec<(String, String)> = lines
        .filter(|line| !line.is_empty())
        .map(|line| {
            let (k, v) = line.split_once(':').unwrap_or((line.as_str(), ""));
            if unescaped {
                (unescape(k), unescape(v))
            } else {
                (k.to_string(), v.to_string())
            }
        })
        .collect();
    let content_length = headers
        .iter()
        .find(|(k, _)| k == "content-length")
        .and_then(|(_, v)| v.trim().parse::<usize>().ok());
    Ok(Head {
        body_start,
        command,
        headers,
        content_length,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_frames_split_across_reads() {
        let wire = b"\nMESSAGE\nmessage-id:ID\\c1\ndestination:/queue/a\n\nhello\0\nMESSAGE\ncontent-length:3\n\na\0b\0";
        let mut buf = FrameBuffer::default();
        let mut frames = Vec::new();
        for byte in wire {
            buf.bytes.push(*byte);
            while let Some(frame) = buf.parse().unwrap() {
                frames.push(frame);
            }
        }
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].command, "MESSAGE");
        assert_eq!(frames[0].header("message-id"), Some("ID:1"));
        assert_eq!(frames[0].body, b"hello");
        // A content-length body may hold NUL bytes.
        assert_eq!(frames[1].body, b"a\0b");
        assert!(buf.bytes.is_empty());
    }

    #[test]
    fn a_large_text_frame_parses_as_it_streams_in() {
        let body = vec![b'x'; 3 * 1024 * 1024];
        let mut wire = b"MESSAGE\r\nmessage-id:1\r\n\r\n".to_vec();
        wire.extend_from_slice(&body);
        wire.push(0);
        let mut buf = FrameBuffer::default();
        let mut frames = Vec::new();
        for chunk in wire.chunks(8192) {
            buf.bytes.extend_from_slice(chunk);
            if let Some(frame) = buf.parse().unwrap() {
                frames.push(frame);
            } else {
                // A partial body is never rescanned from its start.
                assert_eq!(buf.scan, buf.bytes.len());
            }
        }
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].header("message-id"), Some("1"));
        assert_eq!(frames[0].body.len(), body.len());
    }

    #[test]
    fn a_frame_over_the_limit_is_refused() {
        let mut buf = FrameBuffer::default();
        buf.bytes.extend_from_slice(b"MESSAGE\n\n");
        buf.bytes.resize(super::super::MAX_FRAME_BYTES + 64, b'x');
        assert!(matches!(buf.parse(), Err(ClientError::FrameTooLarge(_))));

        let mut buf = FrameBuffer::default();
        buf.bytes
            .extend_from_slice(format!("MESSAGE\ncontent-length:{}\n\n", 1 << 30).as_bytes());
        assert!(matches!(buf.parse(), Err(ClientError::FrameTooLarge(_))));
    }

    #[test]
    fn header_values_round_trip_through_escaping() {
        let raw = "a:b\\c\nd\re";
        assert_eq!(unescape(&escape(raw)), raw);
    }

    #[test]
    fn first_repeated_header_wins() {
        let mut buf = FrameBuffer {
            bytes: b"MESSAGE\nfoo:1\nfoo:2\n\n\0".to_vec(),
            ..Default::default()
        };
        let frame = buf.parse().unwrap().unwrap();
        assert_eq!(frame.header("foo"), Some("1"));
    }
}
