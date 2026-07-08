//! Primitives shared across the Amazon Comprehend handlers: ARN synthesis,
//! deterministic id derivation, and timestamps. Kept in one place so the
//! create / describe paths cannot diverge on wire format.

/// Current time as awsJson1_1 epoch-seconds (a floating-point number). The
/// Comprehend `Timestamp` shape carries no `@timestampFormat`, so awsJson1_1's
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

/// A stable, AWS-shaped 32-hex-character id (as used for job ids) derived from a
/// seed, or a fresh random one when `seed` is empty.
pub fn hex32(seed: &str) -> String {
    if seed.is_empty() {
        return uuid::Uuid::new_v4().simple().to_string();
    }
    let a = hash_str(seed);
    let b = hash_str(&format!("{seed}/salt"));
    format!("{a:016x}{b:016x}")
}

/// Amazon Comprehend resource ARN,
/// `arn:aws:comprehend:{region}:{account}:{resource_type}/{tail}`.
///
/// `resource_type` is one of the type names the live service uses
/// (`document-classifier`, `entity-recognizer`, `document-classifier-endpoint`,
/// `entity-recognizer-endpoint`, `flywheel`, `sentiment-detection-job`, ...).
pub fn resource_arn(region: &str, account: &str, resource_type: &str, tail: &str) -> String {
    format!("arn:aws:comprehend:{region}:{account}:{resource_type}/{tail}")
}

/// Split a Comprehend ARN into `(resource_type, tail)`.
/// `arn:aws:comprehend:{region}:{account}:{type}/{tail}` -> `(type, tail)`.
/// `tail` keeps any further `/`-separated segments (version, dataset, ...).
pub fn parse_resource_arn(arn: &str) -> Option<(String, String)> {
    let mut parts = arn.splitn(6, ':');
    let tail = parts.nth(5)?;
    let (rtype, rest) = tail.split_once('/')?;
    if rtype.is_empty() || rest.is_empty() {
        return None;
    }
    Some((rtype.to_string(), rest.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arn_round_trips() {
        let arn = resource_arn("us-east-1", "000000000000", "flywheel", "my-fw");
        assert_eq!(
            arn,
            "arn:aws:comprehend:us-east-1:000000000000:flywheel/my-fw"
        );
        assert_eq!(
            parse_resource_arn(&arn),
            Some(("flywheel".to_string(), "my-fw".to_string()))
        );
    }

    #[test]
    fn arn_keeps_versioned_tail() {
        let arn = "arn:aws:comprehend:us-east-1:000000000000:document-classifier/name/version/v1";
        assert_eq!(
            parse_resource_arn(arn),
            Some((
                "document-classifier".to_string(),
                "name/version/v1".to_string()
            ))
        );
    }

    #[test]
    fn parse_rejects_non_comprehend() {
        assert_eq!(parse_resource_arn("arn:aws:s3:::bucket"), None);
    }

    #[test]
    fn hex32_is_stable_and_shaped() {
        let a = hex32("seed");
        let b = hex32("seed");
        assert_eq!(a, b);
        assert_eq!(a.len(), 32);
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
    }
}
