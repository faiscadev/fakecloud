//! Primitives shared across the AWS Elemental MediaConvert (`mediaconvert`)
//! handlers: ARN synthesis, deterministic id derivation, timestamps, and the
//! default account-specific endpoint URL. Kept in one place so the create / get
//! paths cannot diverge on wire format.

use rand::Rng;

/// The queue ARN, `arn:aws:mediaconvert:{region}:{account}:queues/{name}`.
pub fn queue_arn(region: &str, account: &str, name: &str) -> String {
    format!("arn:aws:mediaconvert:{region}:{account}:queues/{name}")
}

/// The preset ARN, `arn:aws:mediaconvert:{region}:{account}:presets/{name}`.
pub fn preset_arn(region: &str, account: &str, name: &str) -> String {
    format!("arn:aws:mediaconvert:{region}:{account}:presets/{name}")
}

/// The job-template ARN,
/// `arn:aws:mediaconvert:{region}:{account}:jobTemplates/{name}`.
pub fn job_template_arn(region: &str, account: &str, name: &str) -> String {
    format!("arn:aws:mediaconvert:{region}:{account}:jobTemplates/{name}")
}

/// The job ARN, `arn:aws:mediaconvert:{region}:{account}:jobs/{id}`.
pub fn job_arn(region: &str, account: &str, id: &str) -> String {
    format!("arn:aws:mediaconvert:{region}:{account}:jobs/{id}")
}

/// Generate a fresh MediaConvert job id of AWS's `{epoch-millis}-{6 base-36}`
/// form (e.g. `1699999999999-abc123`), stable and unique per creation.
pub fn new_job_id() -> String {
    let millis = chrono::Utc::now().timestamp_millis();
    let mut rng = rand::thread_rng();
    let suffix: String = (0..6)
        .map(|_| {
            let n: u8 = rng.gen_range(0..36);
            if n < 10 {
                (b'0' + n) as char
            } else {
                (b'a' + (n - 10)) as char
            }
        })
        .collect();
    format!("{millis}-{suffix}")
}

/// A jobs-query id (an opaque token).
pub fn new_query_id() -> String {
    let millis = chrono::Utc::now().timestamp_millis();
    let mut rng = rand::thread_rng();
    let suffix: String = (0..8)
        .map(|_| {
            let n: u8 = rng.gen_range(0..36);
            if n < 10 {
                (b'0' + n) as char
            } else {
                (b'a' + (n - 10)) as char
            }
        })
        .collect();
    format!("{millis}-{suffix}")
}

/// Current time as restJson1 epoch-seconds (a floating-point number).
/// MediaConvert's timestamp members are `@timestampFormat("epoch-seconds")`
/// (the `__timestampUnix` shape), so the AWS SDK parses the numeric value.
pub fn now_epoch() -> f64 {
    chrono::Utc::now().timestamp_millis() as f64 / 1000.0
}

/// The account-specific endpoint URL `DescribeEndpoints` advertises. When the
/// server was started with an explicit endpoint (the usual case) we echo it so
/// the returned URL points back at this fakecloud host; otherwise we synthesise
/// the canonical regional MediaConvert host.
pub fn endpoint_url(configured: &str, region: &str) -> String {
    if configured.is_empty() {
        format!("https://mediaconvert.{region}.amazonaws.com")
    } else {
        configured.trim_end_matches('/').to_string()
    }
}
