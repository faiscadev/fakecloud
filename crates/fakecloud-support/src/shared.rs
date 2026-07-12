//! Primitives shared across the AWS Support handlers: id synthesis, the
//! submitter identity, and ISO-8601 timestamps. Kept in one place so the
//! create / describe paths cannot diverge on wire format.

use serde_json::Value;

/// Current time as an ISO-8601 UTC string with millisecond precision, e.g.
/// `2013-08-23T20:10:32.000Z`. The Support `TimeCreated` / `ExpiryTime` shapes
/// carry no `@timestampFormat`, and the live service returns ISO-8601 strings.
pub fn iso_now() -> String {
    chrono::Utc::now()
        .format("%Y-%m-%dT%H:%M:%S%.3fZ")
        .to_string()
}

/// The current UTC year, used in the `case-{account}-{year}-{hex}` case id.
pub fn current_year() -> i32 {
    chrono::Utc::now()
        .format("%Y")
        .to_string()
        .parse()
        .unwrap_or(2013)
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

/// A fresh 32-hex-character random id (used for the case-id tail).
pub fn hex32() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

/// AWS-shaped Support case id: `case-{account}-{year}-{16 hex}`.
pub fn new_case_id(account: &str, year: i32) -> String {
    let tail = &hex32()[..16];
    format!("case-{account}-{year}-{tail}")
}

/// A 10-digit numeric `displayId` derived from the case id so it round-trips.
pub fn display_id(case_id: &str) -> String {
    let n = hash_str(case_id) % 9_000_000_000 + 1_000_000_000;
    n.to_string()
}

/// A fresh attachment-set id (`as-{32 hex}`), matching the live service's
/// opaque token shape.
pub fn new_attachment_set_id() -> String {
    format!("as-{}", hex32())
}

/// A fresh attachment id (`attachment-{32 hex}`).
pub fn new_attachment_id() -> String {
    format!("attachment-{}", hex32())
}

/// Read a string member from a request body.
pub fn str_member<'a>(body: &'a Value, name: &str) -> Option<&'a str> {
    body.get(name).and_then(Value::as_str)
}
