//! Primitives shared across the Amazon Transcribe handlers: ARN synthesis,
//! deterministic output-URI derivation, and timestamps. Kept in one place so
//! the create / get paths cannot diverge on wire format.

/// Current time as awsJson1_1 epoch-seconds (a floating-point number). The
/// Transcribe `DateTime` shape carries no `@timestampFormat`, so awsJson1_1's
/// default epoch-seconds applies.
pub fn now_epoch() -> f64 {
    chrono::Utc::now().timestamp_millis() as f64 / 1000.0
}

/// FNV-1a hash for deterministic synthesis of ids / URIs from a seed so a given
/// resource's derived value is stable across reads and restarts.
pub fn hash_str(s: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in s.as_bytes() {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// Amazon Transcribe resource ARN,
/// `arn:aws:transcribe:{region}:{account}:{resource_type}/{name}`.
///
/// `resource_type` is one of the kebab-case type names the live service uses
/// (`transcription-job`, `vocabulary`, `vocabulary-filter`, `language-model`,
/// `medical-transcription-job`, `medical-vocabulary`, `call-analytics-job`,
/// `call-analytics-category`, `medical-scribe-job`).
pub fn resource_arn(region: &str, account: &str, resource_type: &str, name: &str) -> String {
    format!("arn:aws:transcribe:{region}:{account}:{resource_type}/{name}")
}

/// Split a Transcribe ARN into `(resource_type, name)`.
/// `arn:aws:transcribe:{region}:{account}:{type}/{name}` -> `(type, name)`.
pub fn parse_resource_arn(arn: &str) -> Option<(String, String)> {
    // Everything after the 5th colon is `{type}/{name}`.
    let mut parts = arn.splitn(6, ':');
    let tail = parts.nth(5)?;
    let (rtype, name) = tail.split_once('/')?;
    if rtype.is_empty() || name.is_empty() {
        return None;
    }
    Some((rtype.to_string(), name.to_string()))
}

/// The transcript-output URI a completed job exposes when the caller supplied
/// an output bucket. `https://s3.{region}.amazonaws.com/{bucket}/{key}`.
pub fn bucket_output_uri(region: &str, bucket: &str, key: &str) -> String {
    format!("https://s3.{region}.amazonaws.com/{bucket}/{key}")
}

/// The service-managed transcript-output URI AWS returns when the caller did
/// not supply an output bucket. The transcript itself is not produced (see the
/// crate-level ASR gap note); this is the location it would live at.
pub fn service_managed_uri(region: &str, account: &str, job_name: &str, file: &str) -> String {
    let token = format!("{:032x}", hash_str(&format!("{account}/{job_name}/{file}")));
    let uuid = format!(
        "{}-{}-{}-{}-{}",
        &token[0..8],
        &token[8..12],
        &token[12..16],
        &token[16..20],
        &token[20..32]
    );
    format!("https://s3.{region}.amazonaws.com/aws-transcribe-{region}-prod/{account}/{job_name}/{uuid}/{file}")
}

/// A download URI for a stored vocabulary / vocabulary-filter file.
pub fn download_uri(region: &str, account: &str, resource_type: &str, name: &str) -> String {
    let h = hash_str(&format!("{account}/{resource_type}/{name}"));
    format!("https://s3.{region}.amazonaws.com/aws-transcribe-{region}-prod/{account}/{resource_type}/{name}/{h:016x}.txt")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arn_round_trips() {
        let arn = resource_arn("us-east-1", "000000000000", "transcription-job", "my-job");
        assert_eq!(
            arn,
            "arn:aws:transcribe:us-east-1:000000000000:transcription-job/my-job"
        );
        assert_eq!(
            parse_resource_arn(&arn),
            Some(("transcription-job".to_string(), "my-job".to_string()))
        );
    }

    #[test]
    fn parse_rejects_non_transcribe() {
        assert_eq!(parse_resource_arn("arn:aws:s3:::bucket"), None);
    }

    #[test]
    fn service_managed_uri_is_stable() {
        let a = service_managed_uri("us-east-1", "000000000000", "j", "asrOutput.json");
        let b = service_managed_uri("us-east-1", "000000000000", "j", "asrOutput.json");
        assert_eq!(a, b);
        assert!(a.contains("aws-transcribe-us-east-1-prod"));
    }
}
