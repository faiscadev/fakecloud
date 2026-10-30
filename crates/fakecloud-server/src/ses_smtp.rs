//! Minimal SMTP listener that accepts mail authenticated with IAM
//! ServiceSpecificCredentials issued for `ses.amazonaws.com`. Off by
//! default; opt in by setting `FAKECLOUD_SES_SMTP_PORT` (e.g. `2525`).
//!
//! Implements the subset of RFC 5321 / RFC 4954 that real AWS SES SMTP
//! clients use: EHLO/HELO, AUTH PLAIN, AUTH LOGIN, MAIL FROM, RCPT TO,
//! DATA, RSET, NOOP, QUIT. STARTTLS is not implemented — fakecloud is a
//! dev/test emulator, run it on localhost.
//!
//! On `DATA` completion the message is recorded in SES state as a
//! `SentEmail` with `raw_data` populated, mirroring what `SendRawEmail`
//! produces. Tests can introspect it via the existing SES retrieval
//! endpoints.

use std::sync::Arc;

use base64::Engine;
use chrono::Utc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, info, warn};

use fakecloud_iam::SharedIamState;
use fakecloud_ses::{SentEmail, SharedSesState, SmtpSubmission};

const SES_SERVICE_NAME: &str = "ses.amazonaws.com";

/// Spawn the listener if `FAKECLOUD_SES_SMTP_PORT` is set in the
/// environment. Returns immediately when the variable is unset or
/// unparseable.
pub fn maybe_spawn(iam: SharedIamState, ses: SharedSesState) {
    let Ok(raw) = std::env::var("FAKECLOUD_SES_SMTP_PORT") else {
        return;
    };
    let Ok(port) = raw.parse::<u16>() else {
        warn!(
            value = %raw,
            "FAKECLOUD_SES_SMTP_PORT is set but not a valid port; SMTP listener disabled"
        );
        return;
    };
    spawn(iam, ses, port);
}

pub fn spawn(iam: SharedIamState, ses: SharedSesState, port: u16) {
    tokio::spawn(async move {
        let bind = format!("0.0.0.0:{port}");
        let listener = match TcpListener::bind(&bind).await {
            Ok(l) => l,
            Err(e) => {
                warn!(error = ?e, %bind, "SES SMTP listener bind failed");
                return;
            }
        };
        info!(%bind, "SES SMTP listener started");
        let iam = Arc::new(iam);
        let ses = Arc::new(ses);
        loop {
            let (stream, peer) = match listener.accept().await {
                Ok(s) => s,
                Err(e) => {
                    debug!(error = ?e, "smtp accept error");
                    continue;
                }
            };
            let iam = iam.clone();
            let ses = ses.clone();
            tokio::spawn(async move {
                if let Err(e) = handle(stream, &iam, &ses).await {
                    debug!(?peer, error = ?e, "smtp session ended with error");
                }
            });
        }
    });
}

#[derive(Default)]
struct Session {
    authenticated: Option<AuthIdent>,
    mail_from: Option<String>,
    rcpt_to: Vec<String>,
    pending_auth: Option<AuthChallenge>,
}

#[derive(Clone)]
struct AuthIdent {
    account_id: String,
    /// IAM service-specific credential username used to authenticate this
    /// session. Recorded on each SMTP submission for introspection.
    service_user_name: String,
}

enum AuthChallenge {
    LoginUsername,
    LoginPassword(String),
    PlainBlob,
}

async fn handle(
    stream: TcpStream,
    iam: &SharedIamState,
    ses: &SharedSesState,
) -> std::io::Result<()> {
    let (rd, mut wr) = stream.into_split();
    let mut rd = BufReader::new(rd);
    wr.write_all(b"220 fakecloud-ses ESMTP ready\r\n").await?;
    let mut s = Session::default();
    let mut line = String::new();
    loop {
        line.clear();
        let n = rd.read_line(&mut line).await?;
        if n == 0 {
            break;
        }
        let trimmed = line.trim_end_matches(['\r', '\n']);

        if let Some(ch) = s.pending_auth.take() {
            handle_auth_continuation(ch, trimmed, iam, &mut s, &mut wr).await?;
            continue;
        }

        let cmd_upper = trimmed.to_ascii_uppercase();
        if cmd_upper.starts_with("EHLO") || cmd_upper.starts_with("HELO") {
            wr.write_all(b"250-fakecloud-ses\r\n250-AUTH PLAIN LOGIN\r\n250 OK\r\n")
                .await?;
        } else if cmd_upper.starts_with("AUTH PLAIN") {
            let arg = trimmed.get(10..).map(str::trim).unwrap_or("");
            if arg.is_empty() {
                s.pending_auth = Some(AuthChallenge::PlainBlob);
                wr.write_all(b"334 \r\n").await?;
            } else {
                finish_plain_auth(arg, iam, &mut s, &mut wr).await?;
            }
        } else if cmd_upper.starts_with("AUTH LOGIN") {
            s.pending_auth = Some(AuthChallenge::LoginUsername);
            wr.write_all(b"334 VXNlcm5hbWU6\r\n").await?; // base64("Username:")
        } else if cmd_upper.starts_with("MAIL FROM") {
            if s.authenticated.is_none() {
                wr.write_all(b"530 5.7.0 Authentication required\r\n")
                    .await?;
                continue;
            }
            s.mail_from = extract_addr(trimmed);
            s.rcpt_to.clear();
            wr.write_all(b"250 OK\r\n").await?;
        } else if cmd_upper.starts_with("RCPT TO") {
            if s.authenticated.is_none() {
                wr.write_all(b"530 5.7.0 Authentication required\r\n")
                    .await?;
                continue;
            }
            match extract_addr(trimmed) {
                Some(addr) => {
                    s.rcpt_to.push(addr);
                    wr.write_all(b"250 OK\r\n").await?;
                }
                None => {
                    wr.write_all(b"501 5.5.4 syntax: RCPT TO:<addr>\r\n")
                        .await?;
                }
            }
        } else if cmd_upper.starts_with("DATA") {
            let Some(ident) = s.authenticated.clone() else {
                wr.write_all(b"530 5.7.0 Authentication required\r\n")
                    .await?;
                continue;
            };
            if s.mail_from.is_none() || s.rcpt_to.is_empty() {
                wr.write_all(b"503 5.5.1 RCPT first\r\n").await?;
                continue;
            }
            wr.write_all(b"354 End data with <CR><LF>.<CR><LF>\r\n")
                .await?;
            let data = read_data(&mut rd).await?;
            let from = s.mail_from.clone().unwrap_or_default();
            let to = std::mem::take(&mut s.rcpt_to);
            // Extract the auth user from IAM (the SMTP service-specific
            // username) so the introspection endpoint can show which
            // credential submitted the message.
            let message_id = store_email(
                ses,
                &ident.account_id,
                from,
                to,
                data,
                &ident.service_user_name,
            );
            s.mail_from = None;
            wr.write_all(format!("250 OK queued as {message_id}\r\n").as_bytes())
                .await?;
        } else if cmd_upper.starts_with("RSET") {
            s.mail_from = None;
            s.rcpt_to.clear();
            wr.write_all(b"250 OK\r\n").await?;
        } else if cmd_upper.starts_with("NOOP") {
            wr.write_all(b"250 OK\r\n").await?;
        } else if cmd_upper.starts_with("QUIT") {
            wr.write_all(b"221 2.0.0 bye\r\n").await?;
            break;
        } else {
            wr.write_all(b"502 5.5.2 command not recognized\r\n")
                .await?;
        }
    }
    Ok(())
}

async fn handle_auth_continuation(
    ch: AuthChallenge,
    line: &str,
    iam: &SharedIamState,
    s: &mut Session,
    wr: &mut tokio::net::tcp::OwnedWriteHalf,
) -> std::io::Result<()> {
    match ch {
        AuthChallenge::LoginUsername => {
            s.pending_auth = Some(AuthChallenge::LoginPassword(line.to_string()));
            wr.write_all(b"334 UGFzc3dvcmQ6\r\n").await?; // base64("Password:")
        }
        AuthChallenge::LoginPassword(user_b64) => {
            let user = decode_b64_string(&user_b64);
            let pass = decode_b64_string(line);
            match lookup_credential(iam, &user, &pass) {
                Some(ident) => {
                    s.authenticated = Some(ident);
                    wr.write_all(b"235 2.7.0 Authentication successful\r\n")
                        .await?;
                }
                None => {
                    wr.write_all(b"535 5.7.8 Authentication credentials invalid\r\n")
                        .await?;
                }
            }
        }
        AuthChallenge::PlainBlob => {
            finish_plain_auth(line, iam, s, wr).await?;
        }
    }
    Ok(())
}

async fn finish_plain_auth(
    arg: &str,
    iam: &SharedIamState,
    s: &mut Session,
    wr: &mut tokio::net::tcp::OwnedWriteHalf,
) -> std::io::Result<()> {
    match decode_auth_plain(arg, iam) {
        Some(ident) => {
            s.authenticated = Some(ident);
            wr.write_all(b"235 2.7.0 Authentication successful\r\n")
                .await?;
        }
        None => {
            wr.write_all(b"535 5.7.8 Authentication credentials invalid\r\n")
                .await?;
        }
    }
    Ok(())
}

async fn read_data(rd: &mut BufReader<tokio::net::tcp::OwnedReadHalf>) -> std::io::Result<String> {
    let mut data = String::new();
    let mut buf = String::new();
    loop {
        buf.clear();
        let n = rd.read_line(&mut buf).await?;
        if n == 0 {
            break;
        }
        if buf == ".\r\n" || buf == ".\n" {
            break;
        }
        let unstuffed = if let Some(rest) = buf.strip_prefix("..") {
            rest.to_string()
        } else {
            buf.clone()
        };
        data.push_str(&unstuffed);
    }
    Ok(data)
}

fn extract_addr(line: &str) -> Option<String> {
    let lt = line.find('<')?;
    let rest = &line[lt + 1..];
    let gt = rest.find('>')?;
    Some(rest[..gt].to_string())
}

fn decode_b64_string(s: &str) -> String {
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(s.trim())
        .unwrap_or_default();
    String::from_utf8(bytes).unwrap_or_default()
}

fn decode_auth_plain(arg: &str, iam: &SharedIamState) -> Option<AuthIdent> {
    let raw = base64::engine::general_purpose::STANDARD
        .decode(arg.trim())
        .ok()?;
    // RFC 4616: [authzid] NUL authcid NUL passwd
    let parts: Vec<&[u8]> = raw.splitn(3, |b| *b == 0).collect();
    if parts.len() != 3 {
        return None;
    }
    let user = std::str::from_utf8(parts[1]).ok()?;
    let pass = std::str::from_utf8(parts[2]).ok()?;
    lookup_credential(iam, user, pass)
}

fn lookup_credential(
    iam: &SharedIamState,
    service_user: &str,
    password: &str,
) -> Option<AuthIdent> {
    let accounts = iam.read();
    for (account_id, state) in accounts.iter() {
        for creds in state.service_specific_credentials.values() {
            for c in creds {
                if c.service_name == SES_SERVICE_NAME
                    && c.status == "Active"
                    && c.service_user_name == service_user
                    && c.service_password == password
                {
                    return Some(AuthIdent {
                        account_id: account_id.to_string(),
                        service_user_name: c.service_user_name.clone(),
                    });
                }
            }
        }
    }
    None
}

fn store_email(
    ses: &SharedSesState,
    account_id: &str,
    from: String,
    to: Vec<String>,
    data: String,
    auth_user: &str,
) -> String {
    let message_id = format!("smtp-{:032x}", rand::random::<u128>());
    let now = Utc::now();
    let subject = fakecloud_ses::mime::message_subject(data.as_bytes());
    let (text_body, html_body) = fakecloud_ses::mime::message_bodies(data.as_bytes());
    let raw_size_bytes = data.len();
    let sent = SentEmail {
        message_id: message_id.clone(),
        from: from.clone(),
        to: to.clone(),
        cc: Vec::new(),
        bcc: Vec::new(),
        subject: subject.clone(),
        html_body,
        text_body,
        raw_data: Some(data),
        template_name: None,
        template_data: None,
        dkim_signature: None,
        headers: Vec::new(),
        timestamp: now,
        email_tags: Vec::new(),
        delivery_insights: Vec::new(),
    };
    let submission = SmtpSubmission {
        message_id: message_id.clone(),
        from,
        to,
        subject,
        raw_size_bytes,
        received_at: now,
        auth_user: auth_user.to_string(),
    };
    if std::env::var_os("FAKECLOUD_SES_SMTP_RELAY").is_some() {
        // The relay client is blocking; keep it off the listener's runtime.
        let relayed = sent.clone();
        tokio::task::spawn_blocking(move || fakecloud_ses::smtp_relay::relay_sent_email(&relayed));
    }
    let mut accounts = ses.write();
    let state = accounts.get_or_create(account_id);
    state.sent_emails.push(sent);
    state.smtp_submissions.push(submission);
    message_id
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_addr_strips_brackets() {
        assert_eq!(
            extract_addr("MAIL FROM:<alice@example.com>"),
            Some("alice@example.com".into())
        );
        assert_eq!(
            extract_addr("RCPT TO: <bob@example.com> SIZE=10"),
            Some("bob@example.com".into())
        );
    }

    #[test]
    fn extract_addr_rejects_missing_brackets() {
        assert_eq!(extract_addr("MAIL FROM:alice@example.com"), None);
    }

    #[test]
    fn auth_plain_blob_decodes_user_and_password() {
        // "\0alice\0secret"
        let blob = base64::engine::general_purpose::STANDARD.encode(b"\0alice\0secret");
        let raw = base64::engine::general_purpose::STANDARD
            .decode(blob)
            .unwrap();
        let parts: Vec<&[u8]> = raw.splitn(3, |b| *b == 0).collect();
        assert_eq!(parts.len(), 3);
        assert_eq!(parts[1], b"alice");
        assert_eq!(parts[2], b"secret");
    }
}
