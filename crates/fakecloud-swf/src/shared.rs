//! Primitives shared across the Amazon SWF handlers: ARN synthesis, run-id
//! minting, timestamps, and the type/token key derivation. Kept in one place so
//! the register / describe / poll paths cannot diverge on wire format.

/// Current time as awsJson1_0 epoch-seconds (a floating-point number). SWF's
/// `Timestamp` shape carries no `@timestampFormat`, so awsJson1_0's default
/// epoch-seconds applies (SWF returns e.g. `1420070400.123`).
pub fn now_epoch() -> f64 {
    chrono::Utc::now().timestamp_millis() as f64 / 1000.0
}

/// A fresh, AWS-shaped SWF run id: a base64url-ish opaque token AWS returns from
/// `StartWorkflowExecution`. We mint a stable random 43-ish char id; the exact
/// alphabet is opaque to clients, they only echo it back.
pub fn new_run_id() -> String {
    // Two v4 UUIDs (simple, no dashes) give a 64-hex opaque token, matching the
    // opaque-string contract SWF run ids satisfy.
    format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    )
}

/// A fresh opaque task token (decision or activity). Clients treat it as opaque.
pub fn new_task_token() -> String {
    format!(
        "AAAAK{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    )
}

/// Amazon SWF domain ARN, `arn:aws:swf:{region}:{account}:/domain/{name}`.
pub fn domain_arn(region: &str, account: &str, name: &str) -> String {
    format!("arn:aws:swf:{region}:{account}:/domain/{name}")
}

/// Internal storage key for an activity or workflow type: `name` + a unit
/// separator + `version`. The separator can never appear in a Smithy `Name` or
/// `Version` value, so the key round-trips unambiguously.
pub fn type_key(name: &str, version: &str) -> String {
    format!("{name}\u{1}{version}")
}

/// Split a [`type_key`] back into `(name, version)`.
pub fn split_type_key(key: &str) -> Option<(&str, &str)> {
    key.split_once('\u{1}')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_key_round_trips() {
        let k = type_key("send-email", "1.0");
        assert_eq!(split_type_key(&k), Some(("send-email", "1.0")));
    }

    #[test]
    fn domain_arn_shape() {
        assert_eq!(
            domain_arn("us-east-1", "000000000000", "orders"),
            "arn:aws:swf:us-east-1:000000000000:/domain/orders"
        );
    }

    #[test]
    fn run_ids_are_unique_and_shaped() {
        let a = new_run_id();
        let b = new_run_id();
        assert_ne!(a, b);
        assert_eq!(a.len(), 64);
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
    }
}
