//! Minimal SMTP client used as an opt-in outbound relay for SES
//! SendEmail / SendEmailV2 / SendRawEmail. Wired only when the
//! `FAKECLOUD_SES_SMTP_RELAY` env var is set (e.g.
//! `smtp://localhost:1025` to drop mail at a local Mailpit / MailHog
//! instance for development).
//!
//! Plain SMTP only — no TLS, no auth. Failures are logged and
//! swallowed; the existing in-memory `sent_emails` store remains the
//! source of truth for tests and introspection.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::time::Duration;

/// Parse `smtp://host:port` (or `host:port`) into `(host, port)`.
pub(crate) fn parse_relay_url(url: &str) -> Option<(String, u16)> {
    let s = url.trim().strip_prefix("smtp://").unwrap_or(url);
    let (host, port) = s.rsplit_once(':')?;
    let port: u16 = port.parse().ok()?;
    Some((host.to_string(), port))
}

pub struct OutboundMail<'a> {
    pub from: &'a str,
    pub to: &'a [String],
    pub cc: &'a [String],
    pub bcc: &'a [String],
    pub subject: Option<&'a str>,
    pub text_body: Option<&'a str>,
    pub html_body: Option<&'a str>,
}

/// Best-effort SMTP delivery. Returns `Err` only for caller visibility
/// in tests; production callers fire-and-forget via tracing.
pub fn relay(url: &str, mail: &OutboundMail<'_>) -> Result<(), String> {
    let (host, port) = parse_relay_url(url).ok_or_else(|| format!("invalid relay URL: {url}"))?;
    let mut stream = TcpStream::connect((host.as_str(), port))
        .map_err(|e| format!("connect to {host}:{port} failed: {e}"))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .map_err(|e| format!("set_read_timeout: {e}"))?;
    let mut reader = BufReader::new(
        stream
            .try_clone()
            .map_err(|e| format!("clone TcpStream: {e}"))?,
    );

    expect_code(&mut reader, "220")?;
    write_line(&mut stream, "HELO fakecloud")?;
    expect_code(&mut reader, "250")?;

    write_line(&mut stream, &format!("MAIL FROM:<{}>", mail.from))?;
    expect_code(&mut reader, "250")?;

    for addr in mail.to.iter().chain(mail.cc.iter()).chain(mail.bcc.iter()) {
        write_line(&mut stream, &format!("RCPT TO:<{addr}>"))?;
        expect_code(&mut reader, "250")?;
    }

    write_line(&mut stream, "DATA")?;
    expect_code(&mut reader, "354")?;

    let mut data = String::new();
    data.push_str(&format!("From: {}\r\n", mail.from));
    if !mail.to.is_empty() {
        data.push_str(&format!("To: {}\r\n", mail.to.join(", ")));
    }
    if !mail.cc.is_empty() {
        data.push_str(&format!("Cc: {}\r\n", mail.cc.join(", ")));
    }
    if let Some(s) = mail.subject {
        data.push_str(&format!("Subject: {s}\r\n"));
    }
    if let (Some(text), Some(html)) = (mail.text_body, mail.html_body) {
        data.push_str("MIME-Version: 1.0\r\n");
        data.push_str("Content-Type: multipart/alternative; boundary=\"fc-boundary\"\r\n\r\n");
        data.push_str("--fc-boundary\r\n");
        data.push_str("Content-Type: text/plain; charset=UTF-8\r\n\r\n");
        data.push_str(text);
        data.push_str("\r\n--fc-boundary\r\n");
        data.push_str("Content-Type: text/html; charset=UTF-8\r\n\r\n");
        data.push_str(html);
        data.push_str("\r\n--fc-boundary--\r\n");
    } else if let Some(h) = mail.html_body {
        data.push_str("MIME-Version: 1.0\r\n");
        data.push_str("Content-Type: text/html; charset=UTF-8\r\n\r\n");
        data.push_str(h);
    } else if let Some(t) = mail.text_body {
        data.push_str("Content-Type: text/plain; charset=UTF-8\r\n\r\n");
        data.push_str(t);
    } else {
        data.push_str("\r\n");
    }
    data.push_str("\r\n.\r\n");
    stream
        .write_all(data.as_bytes())
        .map_err(|e| format!("write DATA: {e}"))?;
    expect_code(&mut reader, "250")?;

    write_line(&mut stream, "QUIT")?;
    let _ = read_line(&mut reader);
    Ok(())
}

fn write_line(stream: &mut TcpStream, line: &str) -> Result<(), String> {
    stream
        .write_all(line.as_bytes())
        .map_err(|e| format!("write {line}: {e}"))?;
    stream
        .write_all(b"\r\n")
        .map_err(|e| format!("write CRLF: {e}"))
}

fn read_line<R: Read>(reader: &mut BufReader<R>) -> Result<String, String> {
    let mut buf = String::new();
    reader
        .read_line(&mut buf)
        .map_err(|e| format!("read_line: {e}"))?;
    Ok(buf)
}

fn expect_code<R: Read>(reader: &mut BufReader<R>, code: &str) -> Result<(), String> {
    let line = read_line(reader)?;
    if !line.starts_with(code) {
        return Err(format!("expected {code}, got: {}", line.trim_end()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_relay_url_with_scheme() {
        assert_eq!(
            parse_relay_url("smtp://mailpit:1025"),
            Some(("mailpit".to_string(), 1025))
        );
    }

    #[test]
    fn parse_relay_url_without_scheme() {
        assert_eq!(
            parse_relay_url("localhost:2525"),
            Some(("localhost".to_string(), 2525))
        );
    }

    #[test]
    fn parse_relay_url_rejects_missing_port() {
        assert_eq!(parse_relay_url("smtp://host"), None);
    }

    #[test]
    fn relay_fails_when_no_listener() {
        // Random high port unlikely to have a listener.
        let err = relay(
            "smtp://127.0.0.1:1",
            &OutboundMail {
                from: "a@b",
                to: &["x@y".to_string()],
                cc: &[],
                bcc: &[],
                subject: None,
                text_body: None,
                html_body: None,
            },
        );
        assert!(err.is_err());
    }
}

#[cfg(test)]
mod integration {
    use super::*;
    use std::io::{BufRead, BufReader, Write};
    use std::net::{TcpListener, TcpStream};
    use std::thread;

    /// Trivial SMTP server stub. Acks every command + DATA, captures
    /// the raw conversation for assertions.
    fn spawn_stub() -> (u16, std::sync::mpsc::Receiver<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let (tx, rx) = std::sync::mpsc::channel();
        thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            handle(stream, tx);
        });
        (port, rx)
    }

    fn handle(mut stream: TcpStream, tx: std::sync::mpsc::Sender<Vec<String>>) {
        let mut log = Vec::new();
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        stream.write_all(b"220 stub\r\n").unwrap();
        let mut in_data = false;
        loop {
            let mut line = String::new();
            if reader.read_line(&mut line).unwrap_or(0) == 0 {
                break;
            }
            let trimmed = line.trim_end().to_string();
            log.push(trimmed.clone());
            if in_data {
                if trimmed == "." {
                    stream.write_all(b"250 ok\r\n").unwrap();
                    in_data = false;
                }
                continue;
            }
            let up = trimmed.to_uppercase();
            if up.starts_with("HELO") || up.starts_with("EHLO") {
                stream.write_all(b"250 hello\r\n").unwrap();
            } else if up.starts_with("MAIL FROM") || up.starts_with("RCPT TO") {
                stream.write_all(b"250 ok\r\n").unwrap();
            } else if up == "DATA" {
                stream.write_all(b"354 send data\r\n").unwrap();
                in_data = true;
            } else if up == "QUIT" {
                stream.write_all(b"221 bye\r\n").unwrap();
                break;
            } else {
                stream.write_all(b"250 ok\r\n").unwrap();
            }
        }
        tx.send(log).ok();
    }

    #[test]
    fn relay_sends_mail_to_stub_smtp_server() {
        let (port, rx) = spawn_stub();
        let url = format!("smtp://127.0.0.1:{port}");
        relay(
            &url,
            &OutboundMail {
                from: "sender@example.com",
                to: &["recipient@example.com".to_string()],
                cc: &[],
                bcc: &[],
                subject: Some("Hello"),
                text_body: Some("Body line 1"),
                html_body: None,
            },
        )
        .unwrap();
        let log = rx.recv_timeout(std::time::Duration::from_secs(2)).unwrap();
        assert!(log.iter().any(|l| l.starts_with("HELO")));
        assert!(log
            .iter()
            .any(|l| l.starts_with("MAIL FROM:<sender@example.com>")));
        assert!(log
            .iter()
            .any(|l| l.starts_with("RCPT TO:<recipient@example.com>")));
        assert!(log.iter().any(|l| l == "Subject: Hello"));
        assert!(log.iter().any(|l| l == "Body line 1"));
    }
}
