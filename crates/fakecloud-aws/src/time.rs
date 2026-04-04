use chrono::{DateTime, Utc};

/// ISO 8601 format used by most AWS APIs.
pub fn iso8601(dt: &DateTime<Utc>) -> String {
    dt.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
}

/// Epoch seconds, used by some AWS APIs.
pub fn epoch_seconds(dt: &DateTime<Utc>) -> f64 {
    dt.timestamp() as f64 + (dt.timestamp_subsec_millis() as f64 / 1000.0)
}
