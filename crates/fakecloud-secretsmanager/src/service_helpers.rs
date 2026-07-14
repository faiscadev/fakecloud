use super::*;

/// Classify whether a proposed write collides with an existing
/// version. AWS uses `ClientRequestToken` as a client-side idempotency
/// key, so a repeat write of the exact same payload is a success but a
/// repeat with a different payload is a `ResourceExistsException`.
///
/// `existing_plaintext` is the existing version's decrypted secret
/// string — callers compute this via the KMS hook before invoking so
/// the comparison happens on plaintext, not on stored ciphertext.
pub(crate) fn check_secret_version_idempotency(
    versions: &BTreeMap<String, SecretVersion>,
    version_id: &str,
    existing_plaintext: Option<String>,
    secret_string: &Option<String>,
    secret_binary: &Option<Vec<u8>>,
) -> VersionIdempotency {
    let Some(existing) = versions.get(version_id) else {
        return VersionIdempotency::NotFound;
    };
    if &existing_plaintext == secret_string && &existing.secret_binary == secret_binary {
        VersionIdempotency::Match
    } else {
        VersionIdempotency::Conflict
    }
}

/// Demote the version currently holding `AWSCURRENT` to `AWSPREVIOUS`, enforcing
/// the single-`AWSPREVIOUS` invariant: strip `AWSPREVIOUS` from every other
/// version so exactly one version ever carries it.
///
/// AWS keeps at most one `AWSPREVIOUS` staging label per secret. The naive
/// "remove AWSCURRENT, add AWSPREVIOUS to the old version" logic leaves the
/// previously-previous version still tagged `AWSPREVIOUS`, so two versions end
/// up with the label and `GetSecretValue(AWSPREVIOUS)` returns an arbitrary one.
/// This helper centralizes the correct behaviour already used by
/// `PutSecretValue` / `UpdateSecretVersionStage` so `UpdateSecret`,
/// `RotateSecret` and scheduled rotation share it.
pub(crate) fn demote_current_to_previous(
    versions: &mut BTreeMap<String, SecretVersion>,
    old_vid: &str,
) {
    // Strip AWSPREVIOUS from any other version first so `old_vid` becomes the
    // sole holder.
    for (id, v) in versions.iter_mut() {
        if id != old_vid {
            v.stages.retain(|s| s != "AWSPREVIOUS");
        }
    }
    if let Some(old_v) = versions.get_mut(old_vid) {
        old_v.stages.retain(|s| s != "AWSCURRENT");
        if !old_v.stages.contains(&"AWSPREVIOUS".to_string()) {
            old_v.stages.push("AWSPREVIOUS".to_string());
        }
    }
}

/// Decode the optional `SecretBinary` field. Returns an error when the field is
/// present but not valid base64, matching AWS which rejects an undecodable
/// `SecretBinary` rather than silently dropping it (which would let a caller
/// think binary was stored when it was not).
pub(crate) fn parse_secret_binary(body: &Value) -> Result<Option<Vec<u8>>, AwsServiceError> {
    match body["SecretBinary"].as_str() {
        None => Ok(None),
        Some(s) => base64_decode(s).map(Some).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "Invalid base64: the value of the SecretBinary field could not be decoded.",
            )
        }),
    }
}

/// Whether a secret scheduled for deletion has passed its recovery window and
/// should be treated as permanently gone. AWS purges a secret once the recovery
/// window elapses; a lazy check on read reproduces that without a background
/// reaper.
pub(crate) fn secret_recovery_window_elapsed(secret: &Secret, now: chrono::DateTime<Utc>) -> bool {
    secret.deleted && secret.deletion_date.map(|d| now >= d).unwrap_or(false)
}

/// Reject secret names outside the AWS-permitted charset. AWS allows only
/// `[A-Za-z0-9/_+=.@-]` in a secret name.
pub(crate) fn validate_secret_name_charset(name: &str) -> Result<(), AwsServiceError> {
    if name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || "/_+=.@-".contains(c))
    {
        Ok(())
    } else {
        Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameterException",
            "Invalid name. Name must contain only alphanumeric characters or the characters /_+=.@-",
        ))
    }
}

/// Actions that mutate Secrets Manager state.
pub(crate) fn is_mutating_action(action: &str) -> bool {
    matches!(
        action,
        "CreateSecret"
            | "PutSecretValue"
            | "UpdateSecret"
            | "DeleteSecret"
            | "RestoreSecret"
            | "TagResource"
            | "UntagResource"
            | "RotateSecret"
            | "CancelRotateSecret"
            | "UpdateSecretVersionStage"
            | "PutResourcePolicy"
            | "DeleteResourcePolicy"
            | "ReplicateSecretToRegions"
            | "RemoveRegionsFromReplication"
            | "StopReplicationToReplica"
    )
}

pub(crate) fn require_secret_id(body: &Value) -> Result<String, AwsServiceError> {
    let id = body["SecretId"].as_str().ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameterException",
            "SecretId is required",
        )
    })?;
    validate_string_length("secretId", id, 1, 2048)?;
    Ok(id.to_string())
}

pub(crate) fn parse_tags(tags_val: &Value) -> Vec<(String, String)> {
    tags_val
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|t| {
                    let key = t["Key"].as_str()?;
                    let value = t["Value"].as_str()?;
                    Some((key.to_string(), value.to_string()))
                })
                .collect()
        })
        .unwrap_or_default()
}

pub(crate) fn tags_to_json(tags: &[(String, String)]) -> Vec<Value> {
    tags.iter()
        .map(|(k, v)| json!({"Key": k, "Value": v}))
        .collect()
}

/// Split text into words for secret name filtering.
/// Splits on special characters (/ - _ + = . @) and camelCase.
/// If multiple different special characters are present, doesn't split.
/// Spaces are always split on first.
pub(crate) fn split_words(text: &str) -> Vec<String> {
    // First split on whitespace, then apply word splitting to each part
    let mut all_words = Vec::new();
    for space_part in text.split_whitespace() {
        all_words.extend(split_words_no_space(space_part));
    }
    all_words
}

pub(crate) fn split_words_no_space(text: &str) -> Vec<String> {
    let special_chars = ['/', '-', '_', '+', '=', '.', '@'];

    // Check if text is just a special char
    if text.len() == 1 && special_chars.contains(&text.chars().next().unwrap_or(' ')) {
        return vec![];
    }

    // Find which special chars are present
    let present: Vec<char> = special_chars
        .iter()
        .filter(|&&c| text.contains(c))
        .copied()
        .collect();

    if present.len() > 1 {
        // Multiple different special chars: don't split
        return vec![text.to_string()];
    }

    if present.len() == 1 {
        let ch = present[0];
        let parts: Vec<&str> = text.split(ch).filter(|s| !s.is_empty()).collect();
        let mut result = Vec::new();
        for part in parts {
            result.extend(split_by_uppercase(part));
        }
        return result;
    }

    // No special chars: split by uppercase
    split_by_uppercase(text)
}

/// Split a string by the pattern: a non-lowercase char followed by one or more lowercase chars.
/// Equivalent to Python regex: re.split(r"([^a-z][a-z]+)", s)
pub(crate) fn split_by_uppercase(text: &str) -> Vec<String> {
    // Implement the equivalent of Python's re.split(r"([^a-z][a-z]+)", text)
    // re.split with capturing group returns: [before, match, between, match, ..., after]
    let chars: Vec<char> = text.chars().collect();
    let mut words = Vec::new();
    let mut last_end = 0;
    let mut i = 0;

    while i < chars.len() {
        // Try to find pattern: [^a-z][a-z]+
        if !chars[i].is_ascii_lowercase()
            && i + 1 < chars.len()
            && chars[i + 1].is_ascii_lowercase()
        {
            // Text before this match (between previous match end and this match start)
            if i > last_end {
                let between: String = chars[last_end..i].iter().collect();
                let trimmed = between.trim().to_string();
                if !trimmed.is_empty() {
                    words.push(trimmed);
                }
            }

            // The match itself
            let start = i;
            i += 2;
            while i < chars.len() && chars[i].is_ascii_lowercase() {
                i += 1;
            }
            let word: String = chars[start..i].iter().collect();
            let trimmed = word.trim().to_string();
            if !trimmed.is_empty() {
                words.push(trimmed);
            }
            last_end = i;
        } else {
            i += 1;
        }
    }

    // Text after last match
    if last_end < chars.len() {
        let after: String = chars[last_end..].iter().collect();
        let trimmed = after.trim().to_string();
        if !trimmed.is_empty() {
            words.push(trimmed);
        }
    }

    words
}

/// Match a pattern against a value.
/// - match_prefix=true: simple prefix match on the full string
/// - match_prefix=false: split both into words, all pattern words must prefix-match some value word
pub(crate) fn match_pattern(
    pattern: &str,
    value: &str,
    match_prefix: bool,
    case_sensitive: bool,
) -> bool {
    if match_prefix {
        if case_sensitive {
            value.starts_with(pattern)
        } else {
            value.to_lowercase().starts_with(&pattern.to_lowercase())
        }
    } else {
        let mut pattern_words = split_words(pattern);
        if pattern_words.is_empty() {
            return false;
        }
        let mut value_words = split_words(value);
        if !case_sensitive {
            pattern_words = pattern_words.iter().map(|w| w.to_lowercase()).collect();
            value_words = value_words.iter().map(|w| w.to_lowercase()).collect();
        }
        for pw in &pattern_words {
            if !value_words.iter().any(|vw| vw.starts_with(pw.as_str())) {
                return false;
            }
        }
        true
    }
}

/// The main matcher: check patterns against a list of strings.
/// Supports negation (!pattern), prefix matching, and case sensitivity.
pub(crate) fn matcher(
    patterns: &[&str],
    strings: &[&str],
    match_prefix: bool,
    case_sensitive: bool,
) -> bool {
    // First check negated patterns
    for pattern in patterns.iter().filter(|p| p.starts_with('!')) {
        let inner = &pattern[1..];
        for s in strings {
            if !match_pattern(inner, s, match_prefix, case_sensitive) {
                return true;
            }
        }
    }

    // Then check positive patterns
    for pattern in patterns.iter().filter(|p| !p.starts_with('!')) {
        for s in strings {
            if match_pattern(pattern, s, match_prefix, case_sensitive) {
                return true;
            }
        }
    }
    false
}

/// Name filter: prefix match, case sensitive
pub(crate) fn filter_name(secret: &Secret, values: &[&str]) -> bool {
    matcher(values, &[secret.name.as_str()], true, true)
}

/// Description filter: word match, case insensitive
pub(crate) fn filter_description(secret: &Secret, values: &[&str]) -> bool {
    match secret.description.as_deref() {
        Some(desc) if !desc.is_empty() => matcher(values, &[desc], false, false),
        _ => false,
    }
}

/// Tag key filter: prefix match, case sensitive
pub(crate) fn filter_tag_key(secret: &Secret, values: &[&str]) -> bool {
    if secret.tags.is_empty() {
        return false;
    }
    let keys: Vec<&str> = secret.tags.iter().map(|(k, _)| k.as_str()).collect();
    matcher(values, &keys, true, true)
}

/// Tag value filter: prefix match, case sensitive
pub(crate) fn filter_tag_value(secret: &Secret, values: &[&str]) -> bool {
    if secret.tags.is_empty() {
        return false;
    }
    let vals: Vec<&str> = secret.tags.iter().map(|(_, v)| v.as_str()).collect();
    matcher(values, &vals, true, true)
}

/// All filter: word match, case insensitive, across all fields
pub(crate) fn filter_all(secret: &Secret, values: &[&str]) -> bool {
    let mut attributes: Vec<&str> = vec![secret.name.as_str()];
    if let Some(ref desc) = secret.description {
        if !desc.is_empty() {
            attributes.push(desc.as_str());
        }
    }
    for (k, v) in &secret.tags {
        attributes.push(k.as_str());
        attributes.push(v.as_str());
    }
    matcher(values, &attributes, false, false)
}

pub(crate) fn simple_random() -> usize {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};
    let s = RandomState::new();
    let mut hasher = s.build_hasher();
    hasher.write_usize(0);
    hasher.finish() as usize
}

pub(crate) fn base64_decode(input: &str) -> Option<Vec<u8>> {
    let table = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut buf = Vec::new();
    let mut bits: u32 = 0;
    let mut count = 0;
    for &b in input.as_bytes() {
        if b == b'=' || b == b'\n' || b == b'\r' {
            continue;
        }
        let val = table.iter().position(|&c| c == b)? as u32;
        bits = (bits << 6) | val;
        count += 1;
        if count == 4 {
            buf.push((bits >> 16) as u8);
            buf.push((bits >> 8) as u8);
            buf.push(bits as u8);
            bits = 0;
            count = 0;
        }
    }
    match count {
        2 => {
            bits <<= 12;
            buf.push((bits >> 16) as u8);
        }
        3 => {
            bits <<= 6;
            buf.push((bits >> 16) as u8);
            buf.push((bits >> 8) as u8);
        }
        _ => {}
    }
    Some(buf)
}

pub(crate) fn base64_encode(input: &[u8]) -> String {
    let table = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut result = String::new();
    for chunk in input.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let triple = (b0 << 16) | (b1 << 8) | b2;
        result.push(table[((triple >> 18) & 0x3F) as usize] as char);
        result.push(table[((triple >> 12) & 0x3F) as usize] as char);
        if chunk.len() > 1 {
            result.push(table[((triple >> 6) & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
        if chunk.len() > 2 {
            result.push(table[(triple & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
    }
    result
}
