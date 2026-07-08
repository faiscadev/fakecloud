//! Primitives shared across the AWS Amplify (`amplify`) handlers: ARN
//! synthesis, deterministic id derivation, timestamps, and default-domain /
//! webhook-URL construction. Kept in one place so the create / get paths cannot
//! diverge on wire format.

use base64::Engine as _;
use uuid::Uuid;

/// The AWS Amplify app ARN, `arn:aws:amplify:{region}:{account}:apps/{app_id}`.
pub fn app_arn(region: &str, account: &str, app_id: &str) -> String {
    format!("arn:aws:amplify:{region}:{account}:apps/{app_id}")
}

/// The branch ARN, `.../apps/{app_id}/branches/{branch}`.
pub fn branch_arn(region: &str, account: &str, app_id: &str, branch: &str) -> String {
    format!("arn:aws:amplify:{region}:{account}:apps/{app_id}/branches/{branch}")
}

/// The domain-association ARN, `.../apps/{app_id}/domains/{domain}`.
pub fn domain_arn(region: &str, account: &str, app_id: &str, domain: &str) -> String {
    format!("arn:aws:amplify:{region}:{account}:apps/{app_id}/domains/{domain}")
}

/// The webhook ARN, `.../apps/{app_id}/webhooks/{webhook_id}`.
pub fn webhook_arn(region: &str, account: &str, app_id: &str, webhook_id: &str) -> String {
    format!("arn:aws:amplify:{region}:{account}:apps/{app_id}/webhooks/{webhook_id}")
}

/// The backend-environment ARN, `.../apps/{app_id}/backendenvironments/{name}`.
pub fn backend_environment_arn(region: &str, account: &str, app_id: &str, name: &str) -> String {
    format!("arn:aws:amplify:{region}:{account}:apps/{app_id}/backendenvironments/{name}")
}

/// The job ARN, `.../apps/{app_id}/branches/{branch}/jobs/{job_id}`.
pub fn job_arn(region: &str, account: &str, app_id: &str, branch: &str, job_id: &str) -> String {
    format!("arn:aws:amplify:{region}:{account}:apps/{app_id}/branches/{branch}/jobs/{job_id}")
}

/// FNV-1a hash for deterministic synthesis of ids / hosts from a seed so a
/// given resource's derived value is stable across reads and restarts.
pub fn hash_str(s: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in s.as_bytes() {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// Generate a fresh Amplify app id of AWS's `^d[a-z0-9]+$` form: a `d` prefix
/// followed by 24 lowercase base-36 characters (well within the 1..=20... —
/// AWS app ids are 14 chars, so we emit `d` + 13 chars = 14 total).
pub fn new_app_id() -> String {
    let raw = Uuid::new_v4().simple().to_string();
    // uuid simple is 32 lowercase hex chars (`[0-9a-f]`), matching `[a-z0-9]`.
    format!("d{}", &raw[..13])
}

/// Generate a fresh webhook id (a UUID; Amplify's `WebhookId` pattern accepts
/// any string).
pub fn new_webhook_id() -> String {
    Uuid::new_v4().to_string()
}

/// The default `*.amplifyapp.com` domain AWS assigns every app.
pub fn default_domain(app_id: &str) -> String {
    format!("{app_id}.amplifyapp.com")
}

/// The webhook trigger URL AWS provisions, of the form
/// `https://webhooks.amplify.{region}.amazonaws.com/prod/webhooks?id={id}&token={token}&operation=startbuild`.
pub fn webhook_url(region: &str, webhook_id: &str) -> String {
    let token = Uuid::new_v4().simple().to_string();
    format!(
        "https://webhooks.amplify.{region}.amazonaws.com/prod/webhooks?id={webhook_id}&token={token}&operation=startbuild"
    )
}

/// A branch thumbnail URL (a presigned-style URL over the Amplify CDN).
pub fn thumbnail_url(region: &str, app_id: &str, branch: &str) -> String {
    let h = hash_str(&format!("{app_id}/{branch}"));
    format!("https://aws-amplify-prod-{region}-artifacts.s3.{region}.amazonaws.com/{app_id}/{branch}/thumbnail-{h:016x}.png")
}

/// The DNS record a subdomain resolves to (a CNAME onto the app's default
/// domain).
pub fn subdomain_dns_record(app_id: &str) -> String {
    format!("CNAME {}", default_domain(app_id))
}

/// The ACM certificate-verification DNS record AWS surfaces while a domain is
/// being verified.
pub fn certificate_verification_dns_record(domain: &str) -> String {
    let h = hash_str(domain);
    format!("_{h:016x}.{domain}. CNAME _{h:016x}.acm-validations.aws.")
}

/// A presigned S3 upload URL for a manual deployment file.
pub fn upload_url(region: &str, app_id: &str, branch: &str, key: &str) -> String {
    let h = hash_str(&format!("{app_id}/{branch}/{key}"));
    format!("https://aws-amplify-prod-{region}-deployments.s3.{region}.amazonaws.com/{app_id}/{branch}/{key}?X-Amz-Signature={h:016x}")
}

/// A presigned access-log download URL.
pub fn access_log_url(region: &str, app_id: &str, domain: &str) -> String {
    let h = hash_str(&format!("{app_id}/{domain}"));
    format!("https://aws-amplify-prod-{region}-accesslogs.s3.{region}.amazonaws.com/{app_id}/{domain}/access-logs-{h:016x}.csv?X-Amz-Signature={h:016x}")
}

/// Encode an artifact id so `GetArtifactUrl` can resolve it back to its
/// `(app_id, branch, job_id, file_name)` without a lookup table. AWS artifact
/// ids are opaque; ours are a reversible base64url blob.
pub fn encode_artifact_id(app_id: &str, branch: &str, job_id: &str, file_name: &str) -> String {
    let raw = format!("{app_id}\x1f{branch}\x1f{job_id}\x1f{file_name}");
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(raw.as_bytes())
}

/// Reverse [`encode_artifact_id`]. Returns `None` for an id we did not mint.
pub fn decode_artifact_id(id: &str) -> Option<(String, String, String, String)> {
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(id.as_bytes())
        .ok()?;
    let s = String::from_utf8(bytes).ok()?;
    let parts: Vec<&str> = s.split('\x1f').collect();
    if parts.len() != 4 {
        return None;
    }
    Some((
        parts[0].to_string(),
        parts[1].to_string(),
        parts[2].to_string(),
        parts[3].to_string(),
    ))
}

/// The artifact download URL for a build artifact.
pub fn artifact_url(region: &str, app_id: &str, branch: &str, job_id: &str, file: &str) -> String {
    let h = hash_str(&format!("{app_id}/{branch}/{job_id}/{file}"));
    format!("https://aws-amplify-prod-{region}-artifacts.s3.{region}.amazonaws.com/{app_id}/{branch}/{job_id}/{file}?X-Amz-Signature={h:016x}")
}

/// Current time as restJson1 epoch-seconds (a floating-point number). Amplify's
/// timestamp members carry no `@timestampFormat`, so restJson1's default
/// epoch-seconds applies -- the AWS SDK parses the numeric value.
pub fn now_epoch() -> f64 {
    chrono::Utc::now().timestamp_millis() as f64 / 1000.0
}

/// Parse an Amplify `ResourceArn` into `(app_id, Option<branch>)` for the
/// tagging operations. Only apps and branches carry tags in the model.
///
/// - `arn:aws:amplify:{region}:{account}:apps/{app_id}` -> `(app_id, None)`
/// - `.../apps/{app_id}/branches/{branch}` -> `(app_id, Some(branch))`
pub fn parse_resource_arn(arn: &str) -> Option<(String, Option<String>)> {
    let (_, rest) = arn.split_once(":apps/")?;
    if rest.is_empty() {
        return None;
    }
    match rest.split_once("/branches/") {
        Some((app_id, branch)) if !app_id.is_empty() && !branch.is_empty() => {
            Some((app_id.to_string(), Some(branch.to_string())))
        }
        Some(_) => None,
        None => {
            // Bare `apps/{app_id}` (no trailing segments).
            if rest.contains('/') {
                None
            } else {
                Some((rest.to_string(), None))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn app_id_matches_aws_pattern() {
        let id = new_app_id();
        assert!(id.starts_with('d'));
        assert!(!id.is_empty() && id.len() <= 20);
        assert!(id
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit()));
    }

    #[test]
    fn artifact_id_round_trips() {
        let id = encode_artifact_id("d123", "main", "5", "buildLogs.txt");
        assert_eq!(
            decode_artifact_id(&id),
            Some((
                "d123".to_string(),
                "main".to_string(),
                "5".to_string(),
                "buildLogs.txt".to_string()
            ))
        );
        assert_eq!(decode_artifact_id("not-a-real-id!!!"), None);
    }

    #[test]
    fn parse_resource_arn_app_and_branch() {
        assert_eq!(
            parse_resource_arn("arn:aws:amplify:us-east-1:000000000000:apps/d123"),
            Some(("d123".to_string(), None))
        );
        assert_eq!(
            parse_resource_arn("arn:aws:amplify:us-east-1:000000000000:apps/d123/branches/main"),
            Some(("d123".to_string(), Some("main".to_string())))
        );
        assert_eq!(parse_resource_arn("arn:aws:s3:::bucket"), None);
    }
}
