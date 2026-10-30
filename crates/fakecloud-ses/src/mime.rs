//! RFC 5322 / MIME parsing and building for fakecloud SES.
//!
//! Parsing: `RawMail` decodes the base64 raw message supplied to
//! `SendRawEmail` (v1) or `SendEmail` with `Content.Raw` (v2) into the plain
//! MIME that is recorded and relayed, and reads its header block: unfolded
//! header lookup, RFC 2047 decoding of `Subject`, `X-SES-*` headers, and
//! address fields. `message_bodies` extracts the text and HTML bodies of a
//! plain MIME message. Address fields and API address members are validated as
//! RFC 5322 mailboxes or groups via `mail-parser` (`parse_address_list`,
//! `mailbox_address`, `api_recipients`).
//!
//! Building: `build_message` renders the outgoing MIME message.
//! AWS SES `TestRenderEmailTemplate` returns the fully-formed MIME message
//! that would be sent on `SendTemplatedEmail`. The previous implementation
//! emitted only `Subject` + `MIME-Version` + `Content-Type` + body, which
//! standard parsers (`mail-parser`, Python `email`, Node `mailparser`)
//! accept loosely but reject for missing `Date`, `Message-ID`, and proper
//! multipart structure when both text and HTML bodies are present.
//!
//! This module produces RFC-5322-compliant messages with:
//! - `Date` (RFC 2822), `Message-ID` (UUID), `MIME-Version: 1.0`
//! - `Subject` (RFC 2047 encoded-word when non-ASCII)
//! - `multipart/alternative` with proper boundary when both `text` and
//!   `html` parts are present, single-part otherwise
//! - `Content-Transfer-Encoding: quoted-printable` for non-ASCII bodies
//!   or HTML, `7bit` for plain ASCII text

use base64::Engine;
use chrono::Utc;
use mail_parser::MimeHeaders;
use uuid::Uuid;

#[derive(Debug, Clone, Copy)]
pub(crate) struct InvalidAddress;

/// Shared raw-message metadata for both SES APIs. Header parsing stops at the
/// body; address fields are validated only when used as the effective envelope.
pub(crate) struct RawMail {
    bytes: Vec<u8>,
    headers: Vec<(String, String)>,
}

impl RawMail {
    pub(crate) fn parse(encoded: &str) -> Result<Self, base64::DecodeError> {
        let bytes = base64::engine::general_purpose::STANDARD.decode(encoded)?;
        Ok(Self::from_mime(bytes))
    }

    /// Read the header block of an already-decoded (plain MIME) message.
    pub(crate) fn from_mime(bytes: Vec<u8>) -> Self {
        let text = String::from_utf8_lossy(&bytes);
        let mut headers: Vec<(String, String)> = Vec::new();
        for line in text.lines() {
            if line.is_empty() {
                break;
            }
            if line.starts_with([' ', '\t']) {
                if let Some((_, value)) = headers.last_mut() {
                    value.push(' ');
                    value.push_str(line.trim());
                }
            } else if let Some((name, value)) = line.split_once(':') {
                // Obsolete RFC 5322 syntax (section 4.5) allows whitespace
                // between the field name and the colon (`To : a@b.com`).
                headers.push((name.trim_end().to_string(), value.trim().to_string()));
            }
        }
        for (_, value) in &mut headers {
            *value = value.trim().to_string();
        }
        Self { bytes, headers }
    }

    /// The decoded message as plain MIME text, the form SES records and sends.
    pub(crate) fn mime(&self) -> String {
        String::from_utf8_lossy(&self.bytes).into_owned()
    }

    /// The message's `(text, html)` bodies; see `message_bodies`.
    pub(crate) fn bodies(&self) -> (Option<String>, Option<String>) {
        message_bodies(&self.bytes)
    }

    pub(crate) fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    /// The `Subject` header with RFC 2047 encoded-words decoded, as SES
    /// reports it for raw sends.
    pub(crate) fn subject(&self) -> Option<String> {
        let value = self.header("Subject")?;
        // The library field parser flushes its final token on the line terminator.
        let field = format!("{value}\r\n");
        match mail_parser::parsers::MessageStream::new(field.as_bytes()).parse_unstructured() {
            mail_parser::HeaderValue::Text(text) => Some(text.into_owned()),
            _ => Some(String::new()),
        }
    }

    pub(crate) fn addresses(&self, name: &str) -> Result<Vec<String>, InvalidAddress> {
        let mut addresses = Vec::new();
        for (_, value) in self
            .headers
            .iter()
            .filter(|(key, _)| key.eq_ignore_ascii_case(name))
        {
            addresses.extend(parse_address_list(value)?);
        }
        Ok(addresses)
    }

    pub(crate) fn message_tags(&self) -> Vec<(String, String)> {
        self.header("X-SES-MESSAGE-TAGS")
            .map(|value| {
                value
                    .split(',')
                    .filter_map(|tag| {
                        let (name, value) = tag.split_once('=')?;
                        Some((name.trim().to_string(), value.trim().to_string()))
                    })
                    .collect()
            })
            .unwrap_or_default()
    }
}

/// The body of a plain MIME message: everything after the blank line that
/// ends the header block (empty when there is none).
pub(crate) fn body_section(raw: &str) -> &str {
    let crlf = raw.find("\r\n\r\n").map(|i| i + 4);
    let lf = raw.find("\n\n").map(|i| i + 2);
    match (crlf, lf) {
        (Some(a), Some(b)) => &raw[a.min(b)..],
        (Some(i), None) | (None, Some(i)) => &raw[i..],
        (None, None) => "",
    }
}

/// The decoded `Subject` of a plain MIME message (as `RawMail::subject`).
pub fn message_subject(raw: &[u8]) -> Option<String> {
    RawMail::from_mime(raw.to_vec()).subject()
}

/// The first `text/plain` and first `text/html` body of a plain MIME message,
/// transfer-decoded (quoted-printable, base64) and converted from their
/// charset. Attachments are never bodies; a body type the message does not
/// carry is `None` rather than converted from the other one.
pub fn message_bodies(raw: &[u8]) -> (Option<String>, Option<String>) {
    let Some(message) = mail_parser::MessageParser::new().parse(raw) else {
        return (None, None);
    };
    let text = message
        .text_bodies()
        .find(|part| {
            matches!(part.body, mail_parser::PartType::Text(_))
                && part.content_type().is_none_or(|ct| {
                    ct.ctype().eq_ignore_ascii_case("text")
                        && ct
                            .subtype()
                            .is_none_or(|sub| sub.eq_ignore_ascii_case("plain"))
                })
        })
        .and_then(|part| part.text_contents())
        .map(str::to_string);
    let html = message
        .html_bodies()
        .find(|part| part.is_text_html())
        .and_then(|part| part.text_contents())
        .map(str::to_string);
    (text, html)
}

/// Parse a raw RFC 5322 address field. Empty groups are permitted, but every
/// mailbox must have an addr-spec: name-only entries must never be filtered out.
pub(crate) fn parse_address_list(value: &str) -> Result<Vec<String>, InvalidAddress> {
    parse_address_field(value).map(|(_, addresses)| addresses)
}

/// An API address member is exactly one mailbox, never a group or a list.
pub(crate) fn mailbox_address(value: &str) -> Option<String> {
    let (group, mut addresses) = parse_address_field(value).ok()?;
    if group || addresses.len() != 1 {
        return None;
    }
    addresses.pop()
}

pub(crate) fn api_recipients<'a>(
    values: impl IntoIterator<Item = &'a str>,
) -> Result<Vec<String>, InvalidAddress> {
    let addresses = values
        .into_iter()
        .map(|value| mailbox_address(value).ok_or(InvalidAddress))
        .collect::<Result<Vec<_>, _>>()?;
    if addresses.is_empty() {
        Err(InvalidAddress)
    } else {
        Ok(addresses)
    }
}

fn parse_address_field(value: &str) -> Result<(bool, Vec<String>), InvalidAddress> {
    let (protected, quoted_locals) = normalize_address_field(value)?;
    // The library field parser flushes its final token on the line terminator.
    let field = format!("{protected}\r\n");
    let mail_parser::HeaderValue::Address(parsed) =
        mail_parser::parsers::MessageStream::new(field.as_bytes()).parse_address()
    else {
        return Err(InvalidAddress);
    };
    let group = matches!(parsed, mail_parser::Address::Group(_));
    let addresses = parsed
        .into_list()
        .into_iter()
        .map(|address| {
            let address = address.address.ok_or(InvalidAddress)?;
            let (local, domain) = address.rsplit_once('@').ok_or(InvalidAddress)?;
            let local = quoted_locals
                .iter()
                .find(|(token, _)| token == local)
                .map_or(local, |(_, original)| original.as_str());
            if !valid_addr_spec(local, domain) {
                return Err(InvalidAddress);
            }
            Ok(format!("{local}@{domain}"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok((group, addresses))
}

/// mail-parser 0.10 treats a bare quoted local part as a display name, and
/// unescapes one inside angle brackets. Protect only quoted tokens followed by
/// '@', then restore their exact spelling. The library still parses names,
/// groups and mailbox boundaries. Skip group-boundary CFWS comments that the
/// library represents as addressless members; leave comments inside mailboxes
/// to the library. Actual name-only entries remain present and are rejected.
fn normalize_address_field(value: &str) -> Result<(String, Vec<(String, String)>), InvalidAddress> {
    if value.bytes().any(|b| (b < b' ' && b != b'\t') || b == 127) {
        return Err(InvalidAddress);
    }
    let mut prefix = format!("fakecloud-quoted-local-{}-", Uuid::new_v4().simple());
    while value.contains(&prefix) {
        prefix = format!("fakecloud-quoted-local-{}-", Uuid::new_v4().simple());
    }
    let bytes = value.as_bytes();
    let mut out = String::new();
    let mut locals = Vec::new();
    let (mut i, mut copied, mut comments) = (0, 0, 0usize);
    let (mut angle, mut group, mut literal) = (false, false, false);
    let (mut group_cfws, mut skip_comment) = (false, false);
    while i < bytes.len() {
        if comments == 0
            && !bytes[i].is_ascii_whitespace()
            && bytes[i] != b'('
            && !matches!(bytes[i], b':' | b';')
        {
            group_cfws = false;
        }
        match bytes[i] {
            b'\\' if comments > 0 => {
                i += 2;
                continue;
            }
            b'(' => {
                if comments == 0 {
                    skip_comment = group_cfws;
                    if skip_comment {
                        out.push_str(&value[copied..i]);
                        out.push(' ');
                    }
                }
                comments += 1;
            }
            b')' => {
                if comments == 0 {
                    return Err(InvalidAddress);
                }
                comments -= 1;
                if comments == 0 && skip_comment {
                    copied = i + 1;
                }
            }
            b'<' if comments == 0 => {
                if angle {
                    return Err(InvalidAddress);
                }
                angle = true;
            }
            b'>' if comments == 0 => {
                if !angle {
                    return Err(InvalidAddress);
                }
                angle = false;
            }
            b'[' if comments == 0 && !angle => literal = true,
            b']' if comments == 0 && !angle => literal = false,
            b':' if comments == 0 && !angle && !literal => {
                if group {
                    return Err(InvalidAddress);
                }
                group = true;
                group_cfws = true;
            }
            b';' if comments == 0 && !angle && !literal => {
                if !group {
                    return Err(InvalidAddress);
                }
                group = false;
                group_cfws = true;
            }
            b'"' if comments == 0 => {
                let start = i;
                i += 1;
                while i < bytes.len() && bytes[i] != b'"' {
                    i += if bytes[i] == b'\\' { 2 } else { 1 };
                }
                if i >= bytes.len() {
                    return Err(InvalidAddress);
                }
                if i < bytes.len() && bytes.get(i + 1) == Some(&b'@') {
                    let token = format!("{prefix}{}", locals.len());
                    out.push_str(&value[copied..start]);
                    out.push_str(&token);
                    locals.push((token, value[start..=i].to_string()));
                    copied = i + 1;
                }
            }
            _ => {}
        }
        i += 1;
    }
    // The library is best-effort; unclosed groups/comments are not valid empty
    // groups and must not be admitted just because another field has a mailbox.
    if comments != 0 || angle || group || literal {
        return Err(InvalidAddress);
    }
    out.push_str(&value[copied..]);
    Ok((out, locals))
}

fn valid_addr_spec(local: &str, domain: &str) -> bool {
    if domain.is_empty()
        || !domain
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b".-[]:".contains(&b))
    {
        return false;
    }
    if local.starts_with('"') && local.ends_with('"') && local.len() >= 2 {
        let mut escaped = false;
        for b in local.as_bytes()[1..local.len() - 1].iter().copied() {
            if !(b' '..=b'~').contains(&b) || (b == b'"' && !escaped) {
                return false;
            }
            if escaped {
                escaped = false;
            } else if b == b'\\' {
                escaped = true;
            }
        }
        !escaped
    } else {
        local.split('.').all(|atom| {
            !atom.is_empty()
                && atom
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-/=?^_`{|}~".contains(&b))
        })
    }
}

/// Inputs for `build_message`.
pub struct MimeInputs<'a> {
    pub subject: &'a str,
    pub text: Option<&'a str>,
    pub html: Option<&'a str>,
}

/// Build an RFC 5322 / MIME message from the given subject and bodies.
pub fn build_message(input: &MimeInputs<'_>) -> String {
    let mut headers = String::new();
    headers.push_str(&format!("Date: {}\r\n", rfc2822_now()));
    headers.push_str(&format!(
        "Message-ID: <{}@fakecloud.local>\r\n",
        Uuid::new_v4().simple()
    ));
    headers.push_str(&format!("Subject: {}\r\n", encode_header(input.subject)));
    headers.push_str("MIME-Version: 1.0\r\n");

    match (input.text, input.html) {
        (Some(text), Some(html)) => {
            let boundary = format!("=_fakecloud_{}", Uuid::new_v4().simple());
            headers.push_str(&format!(
                "Content-Type: multipart/alternative; boundary=\"{}\"\r\n\r\n",
                boundary
            ));
            let mut body = String::new();
            push_part(&mut body, &boundary, "text/plain; charset=UTF-8", text);
            push_part(&mut body, &boundary, "text/html; charset=UTF-8", html);
            body.push_str(&format!("--{}--\r\n", boundary));
            headers + &body
        }
        (None, Some(html)) => single_part(headers, "text/html; charset=UTF-8", html),
        (Some(text), None) => single_part(headers, "text/plain; charset=UTF-8", text),
        (None, None) => single_part(headers, "text/plain; charset=UTF-8", ""),
    }
}

fn single_part(mut headers: String, content_type: &str, body: &str) -> String {
    let (encoded_body, encoding) = encode_body(body);
    headers.push_str(&format!("Content-Type: {}\r\n", content_type));
    headers.push_str(&format!("Content-Transfer-Encoding: {}\r\n\r\n", encoding));
    headers.push_str(&encoded_body);
    headers
}

fn push_part(out: &mut String, boundary: &str, content_type: &str, body: &str) {
    let (encoded_body, encoding) = encode_body(body);
    out.push_str(&format!("--{}\r\n", boundary));
    out.push_str(&format!("Content-Type: {}\r\n", content_type));
    out.push_str(&format!("Content-Transfer-Encoding: {}\r\n\r\n", encoding));
    out.push_str(&encoded_body);
    if !encoded_body.ends_with("\r\n") {
        out.push_str("\r\n");
    }
}

/// Quoted-printable for any non-ASCII body or HTML; 7bit for plain ASCII text.
fn encode_body(body: &str) -> (String, &'static str) {
    if body.is_ascii() {
        (normalize_crlf(body), "7bit")
    } else {
        (quoted_printable_encode(body), "quoted-printable")
    }
}

/// Normalize any mix of `\r\n`, `\r`, `\n` line endings to canonical `\r\n`
/// without doubling existing CRLFs.
fn normalize_crlf(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let bytes = input.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'\r' => {
                out.push_str("\r\n");
                if i + 1 < bytes.len() && bytes[i + 1] == b'\n' {
                    i += 2;
                } else {
                    i += 1;
                }
            }
            b'\n' => {
                out.push_str("\r\n");
                i += 1;
            }
            b => {
                out.push(b as char);
                i += 1;
            }
        }
    }
    out
}

fn quoted_printable_encode(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut line_len = 0;
    let bytes = input.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let byte = bytes[i];
        // Hard line breaks: emit literal CRLF, not =0A / =0D. Collapse any
        // mix of \r, \n, \r\n into a single CRLF.
        if byte == b'\r' || byte == b'\n' {
            out.push_str("\r\n");
            line_len = 0;
            if byte == b'\r' && i + 1 < bytes.len() && bytes[i + 1] == b'\n' {
                i += 2;
            } else {
                i += 1;
            }
            continue;
        }
        let needs_encoding = matches!(byte, 0..=31 | 61 | 127..=255) && byte != b'\t';
        let chunk: String = if needs_encoding {
            format!("={:02X}", byte)
        } else {
            (byte as char).to_string()
        };
        // Soft line break before 76th char per RFC 2045 §6.7.
        if line_len + chunk.len() > 75 {
            out.push_str("=\r\n");
            line_len = 0;
        }
        out.push_str(&chunk);
        line_len += chunk.len();
        i += 1;
    }
    out
}

/// RFC 2047 encoded-word for non-ASCII headers; raw otherwise.
/// Render an unstructured header value (e.g. `Subject`) for the wire. Plain
/// printable ASCII passes through; anything else, including CR/LF that would
/// otherwise inject header lines, becomes an RFC 2047 encoded-word.
pub(crate) fn encode_header(value: &str) -> String {
    if value
        .bytes()
        .all(|b| b == b'\t' || (0x20..0x7f).contains(&b))
    {
        value.to_string()
    } else {
        let b64 = base64::engine::general_purpose::STANDARD.encode(value.as_bytes());
        format!("=?UTF-8?B?{}?=", b64)
    }
}

fn rfc2822_now() -> String {
    Utc::now().format("%a, %d %b %Y %H:%M:%S +0000").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw(text: &str) -> RawMail {
        RawMail::parse(&base64::engine::general_purpose::STANDARD.encode(text)).unwrap()
    }

    #[test]
    fn raw_subject_decodes_rfc2047_including_folded_words() {
        let mail = raw("Subject: =?UTF-8?B?T2zDoQ==?=\r\n =?UTF-8?Q?_mundo?=\r\n\r\nBody");
        assert_eq!(mail.subject().as_deref(), Some("Ol\u{e1} mundo"));
        assert_eq!(
            raw("Subject: plain text\r\n\r\n").subject().as_deref(),
            Some("plain text")
        );
        assert_eq!(raw("To: a@b.com\r\n\r\n").subject(), None);
    }

    #[test]
    fn raw_header_names_tolerate_whitespace_before_colon() {
        let mail = raw("To : a@example.com\r\nCc\t: b@example.com\r\n\r\n");
        assert_eq!(mail.addresses("To").unwrap(), ["a@example.com"]);
        assert_eq!(mail.addresses("Cc").unwrap(), ["b@example.com"]);
    }

    #[test]
    fn optional_group_comments_preserve_comments_inside_mailboxes() {
        assert_eq!(
            parse_address_list("Team: Chris <c@(host)example.com>, good@example.com; (team)")
                .unwrap(),
            ["c@example.com", "good@example.com"]
        );
        assert_eq!(
            parse_address_list("Team: good@example.com; (one) (two)").unwrap(),
            ["good@example.com"]
        );
        assert_eq!(
            parse_address_list("Empty: (nobody); (end), good@example.com").unwrap(),
            ["good@example.com"]
        );
        assert!(parse_address_list("good@example.com, not-an-address (label)").is_err());
    }

    #[test]
    fn admission_quoted_mailbox_normalization_is_idempotent() {
        for (value, want) in [
            (r#""a,b"@example.com"#, r#""a,b"@example.com"#),
            (r#"<"a,b"@example.com>"#, r#""a,b"@example.com"#),
            (r#"Name <"a,b"@example.com>"#, r#""a,b"@example.com"#),
            (r#""a b"@example.com"#, r#""a b"@example.com"#),
            (r#""a\"b"@example.com"#, r#""a\"b"@example.com"#),
            (r#""Last, First" <user@example.com>"#, "user@example.com"),
        ] {
            let parsed = mailbox_address(value).expect(value);
            assert_eq!(parsed, want, "{value}");
            assert_eq!(mailbox_address(&parsed).as_deref(), Some(want));
        }
    }

    #[test]
    fn admission_raw_groups_preserve_multiple_quoted_local_parts() {
        let want = vec![
            r#""a,b"@example.com"#,
            r#""a\"b"@example.com"#,
            "normal@example.com",
        ];
        let raw = r#"Empty:;, Team: "a,b"@example.com, Escaped <"a\"b"@example.com>;, normal@example.com (Name)"#;
        let parsed = parse_address_list(raw).unwrap();
        assert_eq!(parsed, want);
        for address in parsed {
            assert_eq!(mailbox_address(&address).as_deref(), Some(address.as_str()));
        }
        assert!(parse_address_list(r#"good@example.com, "Display name only""#).is_err());
        assert!(mailbox_address("One: good@example.com;").is_none());
    }

    #[test]
    fn ascii_text_only_uses_7bit() {
        let mime = build_message(&MimeInputs {
            subject: "hello",
            text: Some("plain body"),
            html: None,
        });
        assert!(mime.contains("Subject: hello\r\n"));
        assert!(mime.contains("Content-Type: text/plain; charset=UTF-8\r\n"));
        assert!(mime.contains("Content-Transfer-Encoding: 7bit\r\n"));
        assert!(mime.contains("plain body"));
        assert!(mime.contains("Date: "));
        assert!(mime.contains("Message-ID: <"));
    }

    #[test]
    fn ascii_html_only_uses_html_part() {
        let mime = build_message(&MimeInputs {
            subject: "hi",
            text: None,
            html: Some("<p>x</p>"),
        });
        assert!(mime.contains("Content-Type: text/html; charset=UTF-8\r\n"));
        assert!(mime.contains("<p>x</p>"));
    }

    #[test]
    fn both_parts_use_multipart_alternative() {
        let mime = build_message(&MimeInputs {
            subject: "hi",
            text: Some("plain"),
            html: Some("<p>x</p>"),
        });
        assert!(mime.contains("multipart/alternative; boundary=\"=_fakecloud_"));
        assert!(mime.contains("Content-Type: text/plain; charset=UTF-8\r\n"));
        assert!(mime.contains("Content-Type: text/html; charset=UTF-8\r\n"));
        assert!(mime.contains("plain"));
        assert!(mime.contains("<p>x</p>"));
    }

    #[test]
    fn non_ascii_subject_uses_encoded_word() {
        let mime = build_message(&MimeInputs {
            subject: "héllo",
            text: Some("body"),
            html: None,
        });
        assert!(mime.contains("Subject: =?UTF-8?B?"));
    }

    #[test]
    fn header_encoding_never_emits_line_breaks() {
        let encoded = encode_header("x\r\nBcc: evil@x");
        assert!(!encoded.contains(['\r', '\n']));
        assert!(encoded.starts_with("=?UTF-8?B?"));
        let mime = build_message(&MimeInputs {
            subject: "x\r\nBcc: evil@x",
            text: Some("body"),
            html: None,
        });
        assert!(!mime.contains("\r\nBcc:"));
        assert_eq!(encode_header("plain subject"), "plain subject");
    }

    #[test]
    fn non_ascii_body_uses_quoted_printable() {
        let mime = build_message(&MimeInputs {
            subject: "x",
            text: Some("café"),
            html: None,
        });
        assert!(mime.contains("Content-Transfer-Encoding: quoted-printable\r\n"));
        // 'é' is two bytes in UTF-8 (0xC3 0xA9), each must be percent-escaped.
        assert!(mime.contains("=C3=A9"));
    }

    #[test]
    fn quoted_printable_emits_crlf_for_newlines_not_0a() {
        let qp = quoted_printable_encode("café\nlatte");
        assert!(qp.contains("=C3=A9\r\nlatte"));
        assert!(!qp.contains("=0A"));
    }

    #[test]
    fn quoted_printable_collapses_crlf_lf_cr_to_canonical_crlf() {
        let qp = quoted_printable_encode("a\r\nb\nc\rdé");
        assert_eq!(qp, "a\r\nb\r\nc\r\nd=C3=A9");
    }

    #[test]
    fn normalize_crlf_does_not_double_existing_crlf() {
        assert_eq!(normalize_crlf("a\r\nb"), "a\r\nb");
        assert_eq!(normalize_crlf("a\nb"), "a\r\nb");
        assert_eq!(normalize_crlf("a\rb"), "a\r\nb");
        assert_eq!(normalize_crlf("a\r\n\r\nb"), "a\r\n\r\nb");
        assert_eq!(normalize_crlf("a\n\nb"), "a\r\n\r\nb");
    }

    #[test]
    fn raw_mail_exposes_the_decoded_message_as_plain_mime() {
        let mail = raw("Subject: Hi\r\n\r\nBody");
        assert_eq!(mail.mime(), "Subject: Hi\r\n\r\nBody");
    }

    #[test]
    fn bodies_of_multipart_alternative_are_transfer_and_charset_decoded() {
        let mime = concat!(
            "From: a@example.com\r\n",
            "Subject: Hi\r\n",
            "MIME-Version: 1.0\r\n",
            "Content-Type: multipart/alternative; boundary=\"b1\"\r\n",
            "\r\n",
            "--b1\r\n",
            "Content-Type: text/plain; charset=iso-8859-1\r\n",
            "Content-Transfer-Encoding: quoted-printable\r\n",
            "\r\n",
            "Ol=E1 mundo=\r\n",
            " soft break\r\n",
            "--b1\r\n",
            "Content-Type: text/html; charset=utf-8\r\n",
            "Content-Transfer-Encoding: base64\r\n",
            "\r\n",
            "PHA+T2zDoTwvcD4=\r\n",
            "--b1--\r\n",
        );
        let (text, html) = message_bodies(mime.as_bytes());
        assert_eq!(text.as_deref(), Some("Ol\u{e1} mundo soft break"));
        assert_eq!(html.as_deref(), Some("<p>Ol\u{e1}</p>"));
    }

    #[test]
    fn attachments_are_not_bodies_and_missing_types_stay_none() {
        let mime = concat!(
            "Subject: Report\r\n",
            "Content-Type: multipart/mixed; boundary=\"m\"\r\n",
            "\r\n",
            "--m\r\n",
            "Content-Type: text/html\r\n",
            "\r\n",
            "<b>see attached</b>\r\n",
            "--m\r\n",
            "Content-Type: text/plain; name=\"notes.txt\"\r\n",
            "Content-Disposition: attachment; filename=\"notes.txt\"\r\n",
            "\r\n",
            "attached notes\r\n",
            "--m--\r\n",
        );
        let (text, html) = message_bodies(mime.as_bytes());
        // The only text/plain part is an attachment, and the HTML body is
        // never converted into a text one.
        assert_eq!(text, None);
        assert_eq!(html.as_deref(), Some("<b>see attached</b>"));
    }

    #[test]
    fn single_part_message_without_content_type_is_a_text_body() {
        let (text, html) = message_bodies(b"Subject: Hi\r\n\r\nplain body");
        assert_eq!(text.as_deref(), Some("plain body"));
        assert_eq!(html, None);
    }

    #[test]
    fn body_section_starts_after_the_header_block() {
        assert_eq!(
            body_section("A: 1\r\nB: 2\r\n\r\nbody\r\n\r\nmore"),
            "body\r\n\r\nmore"
        );
        assert_eq!(body_section("A: 1\n\nbody"), "body");
        assert_eq!(body_section("A: 1\r\n"), "");
    }

    #[test]
    fn message_subject_decodes_encoded_words() {
        assert_eq!(
            message_subject(b"Subject: =?UTF-8?B?T2zDoQ==?=\r\n\r\nx").as_deref(),
            Some("Ol\u{e1}")
        );
    }
}
