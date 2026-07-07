//! Primitives shared across the Amazon MWAA (`mwaa`) handlers: ARN synthesis,
//! deterministic id/host derivation, timestamps, and token minting. Kept in one
//! place so the create/get/token paths cannot diverge on wire format.

use base64::Engine as _;
use uuid::Uuid;

/// The MWAA environment ARN. MWAA's `arnNamespace` is `airflow`, so the ARN is
/// `arn:aws:airflow:{region}:{account}:environment/{name}` -- note the resource
/// separator is a `/` (not `:`).
pub fn environment_arn(region: &str, account: &str, name: &str) -> String {
    format!("arn:aws:airflow:{region}:{account}:environment/{name}")
}

/// The environment `Name` embedded in an MWAA environment ARN, i.e. the segment
/// after `environment/`. Returns `None` for a malformed ARN.
pub fn name_from_arn(arn: &str) -> Option<&str> {
    arn.rsplit_once(":environment/").map(|(_, n)| n)
}

/// The region embedded in an MWAA environment ARN (the 4th colon-delimited
/// field, `arn:aws:airflow:<region>:...`).
pub fn region_from_arn(arn: &str) -> Option<&str> {
    arn.split(':').nth(3).filter(|s| !s.is_empty())
}

/// FNV-1a hash for deterministic synthesis of ids / hostnames from a name so a
/// given environment's URL is stable across reads and restarts.
pub fn hash_str(s: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in s.as_bytes() {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// Current time as restJson1 epoch-seconds (a floating-point number). MWAA's
/// timestamp members (`CreatedAt`) carry no `@timestampFormat`, so restJson1's
/// default epoch-seconds applies -- the AWS SDK parses the numeric value.
pub fn now_epoch() -> f64 {
    chrono::Utc::now().timestamp_millis() as f64 / 1000.0
}

/// The Airflow web-server hostname for an environment, of the AWS form
/// `{hex}.c{n}.{region}.airflow.amazonaws.com`. Deterministic in the
/// account+region+name so `GetEnvironment`, `CreateCliToken`, and
/// `CreateWebLoginToken` all agree.
pub fn webserver_hostname(account: &str, region: &str, name: &str) -> String {
    let h = hash_str(&format!("{account}/{region}/{name}"));
    // A 32-hex-char host id and a small numeric cell suffix, mirroring AWS's
    // `<uuid-without-dashes>.c<n>.<region>.airflow.amazonaws.com`.
    format!(
        "{:016x}{:016x}.c{}.{}.airflow.amazonaws.com",
        h,
        h.rotate_left(32).wrapping_mul(0x9e37_79b9_7f4a_7c15),
        h % 90 + 10,
        region
    )
}

/// The full `WebserverUrl` (an `https://` URL over the web-server hostname).
pub fn webserver_url(account: &str, region: &str, name: &str) -> String {
    format!("https://{}", webserver_hostname(account, region, name))
}

/// The MWAA service-linked role ARN AWS creates for every environment.
pub fn service_role_arn(account: &str) -> String {
    format!("arn:aws:iam::{account}:role/aws-service-role/airflow.amazonaws.com/AWSServiceRoleForAmazonMWAA")
}

/// The Celery executor SQS queue URL AWS provisions for an environment.
pub fn celery_executor_queue(account: &str, region: &str, name: &str) -> String {
    let h = hash_str(&format!("celery/{account}/{region}/{name}"));
    format!("https://sqs.{region}.amazonaws.com/{account}/airflow-celery-{h:016x}")
}

/// A VPC endpoint service name of AWS's `com.amazonaws.vpce.{region}.vpce-svc-{id}`
/// form, derived deterministically from the environment + a role discriminator.
pub fn vpc_endpoint_service(region: &str, name: &str, role: &str) -> String {
    let h = hash_str(&format!("{role}/{region}/{name}"));
    format!("com.amazonaws.vpce.{region}.vpce-svc-{h:017x}")
}

/// Mint a short-lived access token. AWS returns opaque base64url-ish tokens for
/// `CreateCliToken` / `CreateWebLoginToken`; we return a fresh random one of the
/// same shape per call (they are single-use and not verified server-side here).
pub fn mint_token() -> String {
    let raw = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(raw.as_bytes())
}
