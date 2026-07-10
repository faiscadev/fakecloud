//! AWS Signature Version 4 parsing and verification.
//!
//! Used in two modes:
//!
//! 1. **Routing** — lightweight parse of the Authorization header or presigned
//!    query string to extract the access key ID, region, and service. Always
//!    on, used by dispatch to route requests regardless of whether signatures
//!    are being verified.
//!
//! 2. **Verification** — reconstructs the canonical request, derives the
//!    signing key from the access key's secret, and compares the computed
//!    signature against the one the client sent. Opt-in via
//!    `--verify-sigv4` / `FAKECLOUD_VERIFY_SIGV4`.
//!
//! The canonical request + string-to-sign + signing-key derivation follows
//! the AWS SigV4 specification at
//! <https://docs.aws.amazon.com/general/latest/gr/sigv4_signing.html>.
//! S3 is the only service that single-encodes the path; all others
//! double-encode.

use chrono::{DateTime, TimeZone, Utc};
use hmac::{Hmac, Mac};
use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

type HmacSha256 = Hmac<Sha256>;

/// Lightweight view of a parsed SigV4 Authorization header or presigned URL.
/// Used for request routing (access key → principal, region, service) without
/// requiring cryptographic verification.
#[derive(Debug, Clone)]
pub struct SigV4Info {
    pub access_key: String,
    pub region: String,
    pub service: String,
}

/// Full parse of a SigV4-signed request. Carries everything needed to
/// reconstruct the canonical request and re-derive the signature.
#[derive(Debug, Clone)]
pub struct ParsedSigV4 {
    pub access_key: String,
    /// 8-char `YYYYMMDD` date part of the credential scope.
    pub date_stamp: String,
    pub region: String,
    pub service: String,
    /// Lowercased, semicolon-separated list of signed headers
    /// (e.g. `host;x-amz-content-sha256;x-amz-date`).
    pub signed_headers: Vec<String>,
    /// Hex-encoded signature the client sent.
    pub signature: String,
    /// `X-Amz-Date` / `x-amz-date` value in `YYYYMMDDTHHMMSSZ` form.
    pub amz_date: String,
    /// True if the request was signed via presigned URL query parameters
    /// rather than the `Authorization` header.
    pub is_presigned: bool,
}

impl ParsedSigV4 {
    /// Borrow-view the routing subset of a full parse.
    pub fn as_info(&self) -> SigV4Info {
        SigV4Info {
            access_key: self.access_key.clone(),
            region: self.region.clone(),
            service: self.service.clone(),
        }
    }
}

/// Reasons a SigV4 verification can fail. Each variant maps onto the
/// AWS-shape error the caller should return.
#[derive(Debug, thiserror::Error)]
pub enum SigV4Error {
    /// Request was signed more than 15 minutes from the server's clock.
    /// Maps to AWS `RequestTimeTooSkewed`.
    #[error("request time {signed} is outside the allowed 15-minute window from {server}")]
    RequestTimeTooSkewed {
        signed: DateTime<Utc>,
        server: DateTime<Utc>,
    },
    /// `Authorization` header or presigned URL was not a well-formed
    /// AWS4-HMAC-SHA256 signature. Maps to AWS `IncompleteSignature` or
    /// `InvalidSignatureException`.
    #[error("malformed SigV4 signature: {0}")]
    Malformed(&'static str),
    /// The computed signature did not match the signature the client sent.
    /// Maps to AWS `SignatureDoesNotMatch`.
    #[error("signature does not match")]
    SignatureMismatch,
    /// `X-Amz-Date` / credential-scope date could not be parsed.
    #[error("invalid x-amz-date: {0}")]
    InvalidDate(&'static str),
    /// Presigned URL is past its `X-Amz-Expires` lifetime. Maps to AWS
    /// `AccessDenied` ("Request has expired").
    #[error("presigned URL signed at {signed} has expired (X-Amz-Expires={expires_secs}s) relative to {server}")]
    PresignedUrlExpired {
        signed: DateTime<Utc>,
        server: DateTime<Utc>,
        expires_secs: i64,
    },
    /// Presigned `X-Amz-Expires` was absent or outside the 1..=604800 range.
    #[error("invalid X-Amz-Expires value {0} (must be 1..=604800 seconds)")]
    InvalidPresignExpires(i64),
}

/// Maximum allowed drift between the request's `X-Amz-Date` and the server's
/// wall clock. Matches the 15-minute window AWS uses.
pub const CLOCK_SKEW_SECONDS: i64 = 15 * 60;

/// Maximum `X-Amz-Expires` AWS accepts for a presigned URL: 7 days.
pub const MAX_PRESIGN_EXPIRES_SECONDS: i64 = 7 * 24 * 60 * 60;

/// Extract the `X-Amz-Expires` value (seconds) from a presigned request's
/// query string. Returns `DateParseError` when absent or not an integer, so a
/// presigned request that omits it is rejected rather than treated as
/// never-expiring (bug-audit 2026-05-28, 5.2).
fn presigned_expires_secs(query: &str) -> Result<i64, SigV4Error> {
    query
        .split('&')
        .find_map(|pair| {
            let (k, v) = pair.split_once('=')?;
            (k == "X-Amz-Expires").then(|| v.to_string())
        })
        .and_then(|v| v.parse::<i64>().ok())
        .ok_or(SigV4Error::Malformed("missing or invalid X-Amz-Expires"))
}

/// Legacy routing-only parse. Extracts access key, region, and service from
/// either an `Authorization: AWS4-HMAC-SHA256 …` header or a presigned URL's
/// `X-Amz-Credential` component.
///
/// Returns `None` when the input isn't a recognizable SigV4 credential.
pub fn parse_sigv4(auth_header: &str) -> Option<SigV4Info> {
    parse_header_credential(auth_header)
}

fn parse_header_credential(auth_header: &str) -> Option<SigV4Info> {
    let auth = auth_header.strip_prefix("AWS4-HMAC-SHA256 ")?;
    let credential_start = auth.find("Credential=")?;
    let credential_value = &auth[credential_start + "Credential=".len()..];
    // Real SigV4 headers always carry `SignedHeaders` and `Signature` after
    // the credential, separated by commas. Some clients (and our conformance
    // probe historically) send only the credential scope — accept that too
    // so service routing still works rather than falling through to the
    // catch-all API Gateway handler.
    let credential = match credential_value.find(',') {
        Some(end) => &credential_value[..end],
        None => credential_value,
    };
    parse_credential_scope(credential)
}

fn parse_credential_scope(credential: &str) -> Option<SigV4Info> {
    let parts: Vec<&str> = credential.split('/').collect();
    if parts.len() != 5 || parts[4] != "aws4_request" {
        return None;
    }
    Some(SigV4Info {
        access_key: parts[0].to_string(),
        region: parts[2].to_string(),
        service: parts[3].to_string(),
    })
}

/// Full parse of an `Authorization: AWS4-HMAC-SHA256 …` header.
///
/// Returns `None` if the header is missing required components. The returned
/// value is suitable for both routing (`as_info`) and signature verification
/// (`verify`). The caller must also supply the request's `X-Amz-Date` header
/// because it isn't embedded in the credential — it's carried separately.
pub fn parse_sigv4_header(auth_header: &str, amz_date: Option<&str>) -> Option<ParsedSigV4> {
    let auth = auth_header.strip_prefix("AWS4-HMAC-SHA256 ")?;

    // Each field is comma-separated and may or may not be preceded by a space.
    let mut credential: Option<&str> = None;
    let mut signed_headers: Option<&str> = None;
    let mut signature: Option<&str> = None;
    for part in auth.split(',') {
        let part = part.trim();
        if let Some(v) = part.strip_prefix("Credential=") {
            credential = Some(v);
        } else if let Some(v) = part.strip_prefix("SignedHeaders=") {
            signed_headers = Some(v);
        } else if let Some(v) = part.strip_prefix("Signature=") {
            signature = Some(v);
        }
    }

    let credential = credential?;
    let signed_headers = signed_headers?;
    let signature = signature?;
    let scope = parse_credential_scope(credential)?;
    let date_stamp = credential.split('/').nth(1)?.to_string();
    let signed_headers: Vec<String> = signed_headers
        .split(';')
        .map(|s| s.to_ascii_lowercase())
        .collect();
    if signed_headers.is_empty() {
        return None;
    }

    Some(ParsedSigV4 {
        access_key: scope.access_key,
        date_stamp,
        region: scope.region,
        service: scope.service,
        signed_headers,
        signature: signature.to_string(),
        amz_date: amz_date?.to_string(),
        is_presigned: false,
    })
}

/// Full parse of a presigned URL's SigV4 query parameters.
///
/// Expects to be given the full query-parameter map. Returns `None` when the
/// required `X-Amz-*` parameters are missing or malformed.
pub fn parse_sigv4_presigned(
    query: &std::collections::HashMap<String, String>,
) -> Option<ParsedSigV4> {
    if query.get("X-Amz-Algorithm").map(|s| s.as_str()) != Some("AWS4-HMAC-SHA256") {
        return None;
    }
    let credential = query.get("X-Amz-Credential")?;
    let scope = parse_credential_scope(credential)?;
    let date_stamp = credential.split('/').nth(1)?.to_string();
    let signed_headers = query.get("X-Amz-SignedHeaders")?;
    let signed_headers: Vec<String> = signed_headers
        .split(';')
        .map(|s| s.to_ascii_lowercase())
        .collect();
    let signature = query.get("X-Amz-Signature")?.clone();
    let amz_date = query.get("X-Amz-Date")?.clone();

    Some(ParsedSigV4 {
        access_key: scope.access_key,
        date_stamp,
        region: scope.region,
        service: scope.service,
        signed_headers,
        signature,
        amz_date,
        is_presigned: true,
    })
}

/// The fields of an incoming HTTP request relevant to SigV4 verification.
///
/// Held separately from the full HTTP request so tests can build synthetic
/// requests without constructing an axum/hyper payload.
#[derive(Debug, Clone)]
pub struct VerifyRequest<'a> {
    pub method: &'a str,
    /// URI path as-received (already URL-decoded once by the HTTP framework).
    pub path: &'a str,
    /// Query string without the leading `?`. For presigned URLs the
    /// `X-Amz-Signature` parameter is removed before signing.
    pub query: &'a str,
    /// Lowercased header name → header value. Multi-valued headers should be
    /// joined by `", "`. Built by [`headers_from_http`] for real requests.
    pub headers: &'a [(String, String)],
    /// Full request body. Required unless `X-Amz-Content-Sha256` is set.
    pub body: &'a [u8],
}

/// Flatten a [`http::HeaderMap`] into the lowercase key/value slice
/// [`VerifyRequest`] expects. Multi-valued headers are joined with `", "`.
pub fn headers_from_http(headers: &http::HeaderMap) -> Vec<(String, String)> {
    let mut out: std::collections::BTreeMap<String, Vec<String>> = Default::default();
    for (name, value) in headers.iter() {
        let key = name.as_str().to_ascii_lowercase();
        if let Ok(v) = value.to_str() {
            out.entry(key).or_default().push(v.to_string());
        }
    }
    out.into_iter().map(|(k, vs)| (k, vs.join(", "))).collect()
}

/// Cryptographically verify that the parsed SigV4 signature matches the
/// incoming request under the given secret access key.
///
/// The verification procedure is the canonical AWS SigV4 flow:
///
/// 1. Parse the `X-Amz-Date` and check it's within `CLOCK_SKEW_SECONDS` of
///    `now`.
/// 2. Build the canonical request (method, canonical URI, canonical query
///    string, canonical headers, signed headers, hashed payload).
/// 3. Derive the string-to-sign.
/// 4. Derive the signing key from the secret access key via the four-step
///    HMAC chain (`date → region → service → aws4_request`).
/// 5. Compute the expected HMAC-SHA256 signature and compare it against
///    `parsed.signature` in constant time.
pub fn verify(
    parsed: &ParsedSigV4,
    req: &VerifyRequest<'_>,
    secret_access_key: &str,
    now: DateTime<Utc>,
) -> Result<(), SigV4Error> {
    // 1. Time-validity check. Header-signed requests must be within the fixed
    // clock-skew window; presigned URLs instead live for their own
    // `X-Amz-Expires` lifetime measured from the signing time (bug-audit
    // 2026-05-28, 5.2: X-Amz-Expires was never enforced, so a 1-second URL
    // lived the full 15-minute skew window and a legitimate 7-day URL died
    // after 15 minutes).
    let signed_at = parse_amz_date(&parsed.amz_date)?;
    if parsed.is_presigned {
        let expires = presigned_expires_secs(req.query)?;
        if !(1..=MAX_PRESIGN_EXPIRES_SECONDS).contains(&expires) {
            return Err(SigV4Error::InvalidPresignExpires(expires));
        }
        // The URL is valid from signing time until signing time + expires.
        // A small negative drift (clock skew before signing) is tolerated via
        // the same window AWS allows; anything past the lifetime is rejected.
        let age = (now - signed_at).num_seconds();
        if age < -CLOCK_SKEW_SECONDS || age > expires {
            return Err(SigV4Error::PresignedUrlExpired {
                signed: signed_at,
                server: now,
                expires_secs: expires,
            });
        }
    } else {
        let drift = (now - signed_at).num_seconds().abs();
        if drift > CLOCK_SKEW_SECONDS {
            return Err(SigV4Error::RequestTimeTooSkewed {
                signed: signed_at,
                server: now,
            });
        }
    }

    // 2. Canonical request.
    let canonical_request = build_canonical_request(parsed, req)?;
    let hashed_canonical = hex::encode(Sha256::digest(canonical_request.as_bytes()));

    // 3. String to sign.
    let credential_scope = format!(
        "{}/{}/{}/aws4_request",
        parsed.date_stamp, parsed.region, parsed.service
    );
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{}\n{}",
        parsed.amz_date, credential_scope, hashed_canonical
    );

    // 4. Signing key.
    let signing_key = derive_signing_key(
        secret_access_key,
        &parsed.date_stamp,
        &parsed.region,
        &parsed.service,
    );

    // 5. Expected signature.
    let expected = hmac_sha256(&signing_key, string_to_sign.as_bytes());
    let expected_hex = hex::encode(expected);

    if constant_time_eq(expected_hex.as_bytes(), parsed.signature.as_bytes()) {
        Ok(())
    } else {
        Err(SigV4Error::SignatureMismatch)
    }
}

/// Parse the `YYYYMMDDTHHMMSSZ` basic-ISO-8601 form AWS uses.
fn parse_amz_date(s: &str) -> Result<DateTime<Utc>, SigV4Error> {
    let naive = chrono::NaiveDateTime::parse_from_str(s, "%Y%m%dT%H%M%SZ")
        .map_err(|_| SigV4Error::InvalidDate("expected YYYYMMDDTHHMMSSZ"))?;
    Utc.from_local_datetime(&naive)
        .single()
        .ok_or(SigV4Error::InvalidDate("ambiguous datetime"))
}

/// The SigV4 URI-encoding character set: everything except unreserved
/// characters (`A-Za-z0-9-_.~`). Same set for all AWS services.
const SIGV4_URI_ENCODE: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'!')
    .add(b'"')
    .add(b'#')
    .add(b'$')
    .add(b'%')
    .add(b'&')
    .add(b'\'')
    .add(b'(')
    .add(b')')
    .add(b'*')
    .add(b'+')
    .add(b',')
    .add(b'/')
    .add(b':')
    .add(b';')
    .add(b'<')
    .add(b'=')
    .add(b'>')
    .add(b'?')
    .add(b'@')
    .add(b'[')
    .add(b'\\')
    .add(b']')
    .add(b'^')
    .add(b'`')
    .add(b'{')
    .add(b'|')
    .add(b'}');

fn sigv4_encode(s: &str) -> String {
    utf8_percent_encode(s, SIGV4_URI_ENCODE).to_string()
}

/// Canonicalize the URI path. S3 encodes each segment once; all other
/// services encode twice.
fn canonical_uri(path: &str, service: &str) -> String {
    if path.is_empty() {
        return "/".to_string();
    }
    // Split on '/' so the separators themselves aren't encoded.
    let encoded: Vec<String> = path
        .split('/')
        .map(|seg| {
            let once = sigv4_encode(seg);
            if service == "s3" {
                once
            } else {
                sigv4_encode(&once)
            }
        })
        .collect();
    encoded.join("/")
}

/// Canonicalize the query string per SigV4: URL-decode + re-encode each
/// key/value, sort by key then value, exclude `X-Amz-Signature` for
/// presigned URLs.
fn canonical_query(query: &str, is_presigned: bool) -> String {
    if query.is_empty() {
        return String::new();
    }
    let mut pairs: Vec<(String, String)> = query
        .split('&')
        .filter_map(|kv| {
            if kv.is_empty() {
                return None;
            }
            let (k, v) = match kv.split_once('=') {
                Some((k, v)) => (k, v),
                None => (kv, ""),
            };
            // Decode then re-encode to normalize.
            let k_dec = percent_decode(k);
            let v_dec = percent_decode(v);
            if is_presigned && k_dec == "X-Amz-Signature" {
                return None;
            }
            Some((sigv4_encode(&k_dec), sigv4_encode(&v_dec)))
        })
        .collect();
    pairs.sort();
    pairs
        .into_iter()
        .map(|(k, v)| format!("{}={}", k, v))
        .collect::<Vec<_>>()
        .join("&")
}

fn percent_decode(s: &str) -> String {
    percent_encoding::percent_decode_str(s)
        .decode_utf8_lossy()
        .into_owned()
}

fn build_canonical_request(
    parsed: &ParsedSigV4,
    req: &VerifyRequest<'_>,
) -> Result<String, SigV4Error> {
    let method = req.method.to_ascii_uppercase();
    let canonical_path = canonical_uri(req.path, &parsed.service);
    let canonical_qs = canonical_query(req.query, parsed.is_presigned);

    // Canonical headers: lowercased name, trimmed ASCII-collapsed value,
    // sorted by name, only those listed in `signed_headers`.
    let header_map: BTreeMap<String, String> = req
        .headers
        .iter()
        .map(|(k, v)| (k.to_ascii_lowercase(), collapse_ws(v)))
        .collect();
    let mut canonical_headers = String::new();
    for name in &parsed.signed_headers {
        let value = header_map.get(name).ok_or(SigV4Error::Malformed(
            "signed header not present in request",
        ))?;
        canonical_headers.push_str(name);
        canonical_headers.push(':');
        canonical_headers.push_str(value);
        canonical_headers.push('\n');
    }
    let signed_headers_list = parsed.signed_headers.join(";");

    // Payload hash: prefer `x-amz-content-sha256` when present. S3's special
    // values (`UNSIGNED-PAYLOAD`, `STREAMING-*`) flow through as-is and are
    // matched against the client's signature without rehashing.
    let payload_hash = if parsed.is_presigned {
        // Presigned URLs always sign the empty payload hash marker on GET,
        // or `UNSIGNED-PAYLOAD` on PUT. AWS sets `x-amz-content-sha256` to
        // `UNSIGNED-PAYLOAD` for presigned PUT; match that.
        header_map
            .get("x-amz-content-sha256")
            .cloned()
            .unwrap_or_else(|| "UNSIGNED-PAYLOAD".to_string())
    } else if let Some(h) = header_map.get("x-amz-content-sha256") {
        h.clone()
    } else {
        hex::encode(Sha256::digest(req.body))
    };

    Ok(format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        method, canonical_path, canonical_qs, canonical_headers, signed_headers_list, payload_hash
    ))
}

/// Trim and collapse internal runs of whitespace per the SigV4 spec.
fn collapse_ws(v: &str) -> String {
    let mut out = String::with_capacity(v.len());
    let mut in_ws = false;
    for ch in v.trim().chars() {
        if ch == ' ' || ch == '\t' {
            if !in_ws {
                out.push(' ');
                in_ws = true;
            }
        } else {
            out.push(ch);
            in_ws = false;
        }
    }
    out
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn derive_signing_key(secret: &str, date: &str, region: &str, service: &str) -> Vec<u8> {
    let k_secret = format!("AWS4{}", secret);
    let k_date = hmac_sha256(k_secret.as_bytes(), date.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    hmac_sha256(&k_service, b"aws4_request")
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    use subtle::ConstantTimeEq;
    a.ct_eq(b).into()
}

/// Compute a hex-encoded SigV4 signature for an arbitrary string-to-sign.
///
/// Used by wire formats that don't fit the header/query SigV4 grammar
/// [`verify`] handles — notably S3 POST-Policy (browser-form) uploads, whose
/// "string to sign" is the base64-encoded policy document rather than a
/// canonical-request hash. The signing-key derivation (date -> region ->
/// service -> `aws4_request`) is identical; only the string being signed
/// differs, so this exposes just that derivation + final HMAC rather than
/// duplicating it at each call site.
pub fn sign_string(
    secret_access_key: &str,
    date_stamp: &str,
    region: &str,
    service: &str,
    string_to_sign: &str,
) -> String {
    let signing_key = derive_signing_key(secret_access_key, date_stamp, region, service);
    hex::encode(hmac_sha256(&signing_key, string_to_sign.as_bytes()))
}

/// Verify a client-supplied hex signature against the one derived from
/// `string_to_sign`, in constant time.
///
/// Companion to [`sign_string`] for the S3 POST-Policy path. The comparison
/// uses the same constant-time primitive as the header/query SigV4 path so a
/// signature check can't leak via timing. `sign_string` emits lowercase hex;
/// the provided signature is lowercased first (case normalization is not
/// secret-dependent) so a valid uppercase-hex signature still matches.
pub fn verify_signature(
    secret_access_key: &str,
    date_stamp: &str,
    region: &str,
    service: &str,
    string_to_sign: &str,
    provided_signature: &str,
) -> bool {
    let expected = sign_string(secret_access_key, date_stamp, region, service, string_to_sign);
    constant_time_eq(
        expected.as_bytes(),
        provided_signature.to_ascii_lowercase().as_bytes(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    // AWS documentation's canonical example from
    // https://docs.aws.amazon.com/IAM/latest/UserGuide/create-signed-request.html
    // GetSessionToken request, us-east-1, iam service.
    // Obviously-synthetic secret — real AWS example strings trip GitHub's
    // secret-scanning push protection.
    const AWS_EXAMPLE_SECRET: &str = "testtesttesttesttesttesttesttesttesttest";
    const AWS_EXAMPLE_ACCESS_KEY: &str = "AKIAIOSFODNN7EXAMPLE";

    #[test]
    fn parse_valid_header() {
        let header = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260101/us-east-1/sqs/aws4_request, SignedHeaders=host;x-amz-date, Signature=abc123";
        let info = parse_sigv4(header).unwrap();
        assert_eq!(info.access_key, "AKIAIOSFODNN7EXAMPLE");
        assert_eq!(info.region, "us-east-1");
        assert_eq!(info.service, "sqs");
    }

    #[test]
    fn parse_credential_only_no_trailing_parts() {
        let header =
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260101/us-east-1/rds/aws4_request";
        let info = parse_sigv4(header).unwrap();
        assert_eq!(info.service, "rds");
        assert_eq!(info.region, "us-east-1");
    }

    #[test]
    fn parse_full_header_extracts_all_fields() {
        let header = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260101/us-east-1/sqs/aws4_request, SignedHeaders=host;x-amz-date, Signature=deadbeef";
        let parsed = parse_sigv4_header(header, Some("20260101T000000Z")).unwrap();
        assert_eq!(parsed.access_key, "AKIAIOSFODNN7EXAMPLE");
        assert_eq!(parsed.date_stamp, "20260101");
        assert_eq!(parsed.signed_headers, vec!["host", "x-amz-date"]);
        assert_eq!(parsed.signature, "deadbeef");
        assert!(!parsed.is_presigned);
    }

    #[test]
    fn parse_presigned_query_extracts_all_fields() {
        let mut q = std::collections::HashMap::new();
        q.insert(
            "X-Amz-Algorithm".to_string(),
            "AWS4-HMAC-SHA256".to_string(),
        );
        q.insert(
            "X-Amz-Credential".to_string(),
            "AKIAIOSFODNN7EXAMPLE/20260101/us-east-1/s3/aws4_request".to_string(),
        );
        q.insert("X-Amz-Date".to_string(), "20260101T000000Z".to_string());
        q.insert("X-Amz-SignedHeaders".to_string(), "host".to_string());
        q.insert("X-Amz-Signature".to_string(), "cafe".to_string());
        let parsed = parse_sigv4_presigned(&q).unwrap();
        assert_eq!(parsed.service, "s3");
        assert!(parsed.is_presigned);
        assert_eq!(parsed.signature, "cafe");
    }

    #[test]
    fn returns_none_for_invalid() {
        assert!(parse_sigv4("Bearer token123").is_none());
        assert!(parse_sigv4("").is_none());
    }

    #[test]
    fn canonical_uri_non_s3_double_encodes() {
        assert_eq!(canonical_uri("/foo bar", "iam"), "/foo%2520bar");
        // path with slash stays as-is
        assert_eq!(canonical_uri("/a/b", "iam"), "/a/b");
    }

    #[test]
    fn canonical_uri_s3_single_encodes() {
        assert_eq!(canonical_uri("/foo bar", "s3"), "/foo%20bar");
    }

    #[test]
    fn canonical_query_sorts_and_drops_presigned_signature() {
        let q = "X-Amz-Signature=ignored&B=2&A=1";
        assert_eq!(canonical_query(q, true), "A=1&B=2");
        assert_eq!(canonical_query(q, false), "A=1&B=2&X-Amz-Signature=ignored");
    }

    #[test]
    fn derive_signing_key_is_deterministic_and_stable() {
        // Regression guard: fix the derivation output for a known set of
        // inputs so any future refactor that changes the HMAC chain is
        // caught. The value is the hex HMAC-SHA256 of `aws4_request` under
        // the four-step AWS4 → date → region → service → aws4_request chain,
        // computed by this same function — the goal is stability over time,
        // not agreement with an external reference.
        let key = derive_signing_key(AWS_EXAMPLE_SECRET, "20150830", "us-east-1", "iam");
        assert_eq!(
            hex::encode(&key),
            "0d041c02f01817181204845091e3445c37d6f6b200833f52d34d682b2005918a"
        );
        // Sanity: swapping any input changes the output.
        let diff = derive_signing_key(AWS_EXAMPLE_SECRET, "20150831", "us-east-1", "iam");
        assert_ne!(key, diff);
    }

    #[test]
    fn verify_rejects_skewed_clock() {
        // Signature content is irrelevant here; clock check runs first.
        let parsed = ParsedSigV4 {
            access_key: "X".into(),
            date_stamp: "20260101".into(),
            region: "us-east-1".into(),
            service: "iam".into(),
            signed_headers: vec!["host".into()],
            signature: "00".into(),
            amz_date: "20260101T000000Z".into(),
            is_presigned: false,
        };
        let req = VerifyRequest {
            method: "GET",
            path: "/",
            query: "",
            headers: &[("host".into(), "iam.amazonaws.com".into())],
            body: b"",
        };
        let server_now = Utc.with_ymd_and_hms(2026, 1, 1, 1, 0, 0).unwrap();
        let result = verify(&parsed, &req, AWS_EXAMPLE_SECRET, server_now);
        assert!(matches!(
            result,
            Err(SigV4Error::RequestTimeTooSkewed { .. })
        ));
    }

    #[test]
    fn verify_round_trip_matches_self_computed_signature() {
        // Build a request, compute the signature using the same derivation
        // `verify` uses, then assert `verify` accepts it.
        let secret = AWS_EXAMPLE_SECRET;
        let date_stamp = "20260101";
        let amz_date = "20260101T120000Z";
        let region = "us-east-1";
        let service = "iam";
        let method = "GET";
        let path = "/";
        let query = "Action=GetUser&Version=2010-05-08";
        let headers = vec![
            ("host".to_string(), "iam.amazonaws.com".to_string()),
            ("x-amz-date".to_string(), amz_date.to_string()),
        ];
        let body: &[u8] = b"";

        // Build canonical request manually, matching `build_canonical_request`.
        let canonical_request = {
            let mut parsed = ParsedSigV4 {
                access_key: AWS_EXAMPLE_ACCESS_KEY.into(),
                date_stamp: date_stamp.into(),
                region: region.into(),
                service: service.into(),
                signed_headers: vec!["host".into(), "x-amz-date".into()],
                signature: String::new(),
                amz_date: amz_date.into(),
                is_presigned: false,
            };
            let req = VerifyRequest {
                method,
                path,
                query,
                headers: &headers,
                body,
            };
            let cr = build_canonical_request(&parsed, &req).unwrap();
            parsed.signature = {
                let scope = format!("{}/{}/{}/aws4_request", date_stamp, region, service);
                let sts = format!(
                    "AWS4-HMAC-SHA256\n{}\n{}\n{}",
                    amz_date,
                    scope,
                    hex::encode(Sha256::digest(cr.as_bytes()))
                );
                let sk = derive_signing_key(secret, date_stamp, region, service);
                hex::encode(hmac_sha256(&sk, sts.as_bytes()))
            };
            parsed
        };

        let req = VerifyRequest {
            method,
            path,
            query,
            headers: &headers,
            body,
        };
        let now = Utc.with_ymd_and_hms(2026, 1, 1, 12, 0, 30).unwrap();
        verify(&canonical_request, &req, secret, now).unwrap();
    }

    #[test]
    fn verify_rejects_tampered_body() {
        let secret = AWS_EXAMPLE_SECRET;
        let date_stamp = "20260101";
        let amz_date = "20260101T120000Z";
        let region = "us-east-1";
        let service = "iam";
        let method = "POST";
        let path = "/";
        let query = "";
        let headers = vec![
            ("host".to_string(), "iam.amazonaws.com".to_string()),
            ("x-amz-date".to_string(), amz_date.to_string()),
        ];
        let original_body: &[u8] = b"Action=ListUsers&Version=2010-05-08";

        // Sign the original body.
        let mut parsed = ParsedSigV4 {
            access_key: AWS_EXAMPLE_ACCESS_KEY.into(),
            date_stamp: date_stamp.into(),
            region: region.into(),
            service: service.into(),
            signed_headers: vec!["host".into(), "x-amz-date".into()],
            signature: String::new(),
            amz_date: amz_date.into(),
            is_presigned: false,
        };
        let signing_req = VerifyRequest {
            method,
            path,
            query,
            headers: &headers,
            body: original_body,
        };
        let cr = build_canonical_request(&parsed, &signing_req).unwrap();
        let scope = format!("{}/{}/{}/aws4_request", date_stamp, region, service);
        let sts = format!(
            "AWS4-HMAC-SHA256\n{}\n{}\n{}",
            amz_date,
            scope,
            hex::encode(Sha256::digest(cr.as_bytes()))
        );
        let sk = derive_signing_key(secret, date_stamp, region, service);
        parsed.signature = hex::encode(hmac_sha256(&sk, sts.as_bytes()));

        // Verify against a tampered body.
        let tampered = VerifyRequest {
            method,
            path,
            query,
            headers: &headers,
            body: b"Action=DeleteUser&Version=2010-05-08",
        };
        let now = Utc.with_ymd_and_hms(2026, 1, 1, 12, 0, 30).unwrap();
        assert!(matches!(
            verify(&parsed, &tampered, secret, now),
            Err(SigV4Error::SignatureMismatch)
        ));
    }

    #[test]
    fn collapse_ws_normalizes_runs() {
        assert_eq!(collapse_ws("  foo   bar  "), "foo bar");
        assert_eq!(collapse_ws("foo\tbar"), "foo bar");
    }
}
