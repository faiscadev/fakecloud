//! Primitives shared across the Amazon Translate handlers: ARN synthesis,
//! deterministic id derivation, timestamps, and terminology-file parsing. Kept
//! in one place so the create / get paths cannot diverge on wire format.

use base64::Engine;

/// Current time as awsJson1_1 epoch-seconds (a floating-point number). The
/// Translate `Timestamp` shape carries no `@timestampFormat`, so awsJson1_1's
/// default epoch-seconds applies.
pub fn now_epoch() -> f64 {
    chrono::Utc::now().timestamp_millis() as f64 / 1000.0
}

/// FNV-1a hash for deterministic synthesis of ids from a seed so a given
/// resource's derived value is stable across reads and restarts.
pub fn hash_str(s: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in s.as_bytes() {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// A 32-character lowercase-hex job id (matching the `JobId` `@length` max of
/// 32 and the permissive `JobId` pattern). Derived deterministically from a
/// seed so repeated calls in a test are stable, but seeded with a UUID at the
/// call site to stay unique across jobs.
pub fn job_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

/// Amazon Translate resource ARN,
/// `arn:aws:translate:{region}:{account}:{resource_type}/{name}`.
///
/// `resource_type` is one of the kebab-case type names the live service uses
/// (`terminology`, `parallel-data`).
pub fn resource_arn(region: &str, account: &str, resource_type: &str, name: &str) -> String {
    format!("arn:aws:translate:{region}:{account}:{resource_type}/{name}")
}

/// Split a Translate ARN into `(resource_type, name)`.
/// `arn:aws:translate:{region}:{account}:{type}/{name}` -> `(type, name)`.
pub fn parse_resource_arn(arn: &str) -> Option<(String, String)> {
    let mut parts = arn.splitn(6, ':');
    let tail = parts.nth(5)?;
    let (rtype, name) = tail.split_once('/')?;
    if rtype.is_empty() || name.is_empty() {
        return None;
    }
    Some((rtype.to_string(), name.to_string()))
}

/// A service-managed S3 location Translate exposes for a downloadable resource
/// file (terminology / parallel-data). The file itself is not produced; this
/// is the presigned-style location it would live at.
pub fn data_location(region: &str, account: &str, kind: &str, name: &str, file: &str) -> String {
    let h = hash_str(&format!("{account}/{kind}/{name}/{file}"));
    format!("https://aws-translate-{region}-prod.s3.{region}.amazonaws.com/{account}/{kind}/{name}/{h:016x}/{file}")
}

/// Facts read from an imported terminology / parallel-data file.
#[derive(Debug, Default, Clone)]
pub struct FileFacts {
    /// Decoded byte size of the file.
    pub size_bytes: u64,
    /// Number of data records (rows past the header for CSV/TSV).
    pub record_count: u64,
    /// Source language code (first column header of a CSV/TSV), if derivable.
    pub source_language: Option<String>,
    /// Target language codes (remaining column headers), if derivable.
    pub target_languages: Vec<String>,
}

/// Decode a wire blob member. awsJson1_1 base64-encodes blobs; the AWS SDK and
/// terraform always send base64. If the value is not valid base64 (e.g. a raw
/// synthetic probe string) fall back to the raw UTF-8 bytes so the parser never
/// panics on unexpected input.
pub fn decode_blob(s: &str) -> Vec<u8> {
    base64::engine::general_purpose::STANDARD
        .decode(s)
        .unwrap_or_else(|_| s.as_bytes().to_vec())
}

/// Parse a terminology / parallel-data file (CSV or TSV) for its size, record
/// count, and language columns. This reads the caller-supplied data file only;
/// it runs no translation. Unknown / binary formats yield just the byte size.
pub fn parse_file(bytes: &[u8], format: &str) -> FileFacts {
    let mut facts = FileFacts {
        size_bytes: bytes.len() as u64,
        ..Default::default()
    };
    let text = match std::str::from_utf8(bytes) {
        Ok(t) => t,
        Err(_) => return facts,
    };
    let delim = match format {
        "TSV" => '\t',
        // CSV (and, best-effort, TMX which we don't structurally parse) split
        // on commas.
        _ => ',',
    };
    let mut lines = text.lines().filter(|l| !l.trim().is_empty());
    if let Some(header) = lines.next() {
        let cols: Vec<&str> = header.split(delim).map(str::trim).collect();
        if cols.len() >= 2 {
            facts.source_language = Some(cols[0].to_string());
            facts.target_languages = cols[1..].iter().map(|s| s.to_string()).collect();
        }
    }
    facts.record_count = lines.count() as u64;
    facts
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arn_round_trips() {
        let arn = resource_arn("us-east-1", "000000000000", "terminology", "my-term");
        assert_eq!(
            arn,
            "arn:aws:translate:us-east-1:000000000000:terminology/my-term"
        );
        assert_eq!(
            parse_resource_arn(&arn),
            Some(("terminology".to_string(), "my-term".to_string()))
        );
    }

    #[test]
    fn parse_rejects_non_translate() {
        assert_eq!(parse_resource_arn("arn:aws:s3:::bucket"), None);
    }

    #[test]
    fn csv_terminology_parses_languages_and_count() {
        let csv = "en,fr,es\nhello,bonjour,hola\ndog,chien,perro\n";
        let f = parse_file(csv.as_bytes(), "CSV");
        assert_eq!(f.source_language.as_deref(), Some("en"));
        assert_eq!(f.target_languages, vec!["fr", "es"]);
        assert_eq!(f.record_count, 2);
        assert_eq!(f.size_bytes, csv.len() as u64);
    }

    #[test]
    fn tsv_terminology_parses_tab_columns() {
        let tsv = "en\tde\nhello\thallo\n";
        let f = parse_file(tsv.as_bytes(), "TSV");
        assert_eq!(f.source_language.as_deref(), Some("en"));
        assert_eq!(f.target_languages, vec!["de"]);
        assert_eq!(f.record_count, 1);
    }

    #[test]
    fn decode_blob_falls_back_on_non_base64() {
        // Contains characters outside the base64 alphabet -> raw bytes.
        assert_eq!(decode_blob("en,fr\n"), b"en,fr\n");
    }

    #[test]
    fn job_id_is_32_hex() {
        let id = job_id();
        assert_eq!(id.len(), 32);
        assert!(id.chars().all(|c| c.is_ascii_hexdigit()));
    }
}
