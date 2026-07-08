//! Primitives shared across the Amazon Timestream handlers: ARN synthesis,
//! id minting, timestamps, endpoint-host derivation, and the internal
//! `database\u{1}table` key. Kept in one place so the write and query paths
//! cannot diverge on wire format.

use fakecloud_core::service::AwsRequest;

/// Current time as awsJson1_0 epoch-seconds (a floating-point number).
/// Timestream's `Date` / `Time` shapes carry no `@timestampFormat`, so
/// awsJson1_0's default epoch-seconds applies.
pub fn now_epoch() -> f64 {
    chrono::Utc::now().timestamp_millis() as f64 / 1000.0
}

/// Amazon Timestream database ARN,
/// `arn:aws:timestream:{region}:{account}:database/{name}`.
pub fn database_arn(region: &str, account: &str, name: &str) -> String {
    format!("arn:aws:timestream:{region}:{account}:database/{name}")
}

/// Amazon Timestream table ARN,
/// `arn:aws:timestream:{region}:{account}:database/{db}/table/{name}`.
pub fn table_arn(region: &str, account: &str, database: &str, table: &str) -> String {
    format!("arn:aws:timestream:{region}:{account}:database/{database}/table/{table}")
}

/// Amazon Timestream scheduled-query ARN,
/// `arn:aws:timestream:{region}:{account}:scheduled-query/{name}-{suffix}`.
pub fn scheduled_query_arn(region: &str, account: &str, name: &str, suffix: &str) -> String {
    format!("arn:aws:timestream:{region}:{account}:scheduled-query/{name}-{suffix}")
}

/// A fresh opaque id (query id, batch-load task id, ARN suffix). Clients treat
/// these as opaque tokens they only echo back.
pub fn new_id() -> String {
    uuid::Uuid::new_v4().simple().to_string().to_uppercase()
}

/// Internal storage key for a table within a database: `database` + a unit
/// separator + `table`. The separator can never appear in a Timestream
/// `ResourceName`, so the key round-trips unambiguously.
pub fn table_key(database: &str, table: &str) -> String {
    format!("{database}\u{1}{table}")
}

/// The bare host (`host[:port]`, no scheme) that `DescribeEndpoints` should
/// advertise so a client's follow-up calls come back to this fakecloud server.
/// Derived from the request `Host` header (the address the client actually
/// dialed); falls back to the canonical regional Timestream ingest host when no
/// `Host` header is present.
pub fn endpoint_address(req: &AwsRequest) -> String {
    req.headers
        .get("host")
        .and_then(|v| v.to_str().ok())
        .filter(|h| !h.is_empty())
        .map(|h| h.to_string())
        .unwrap_or_else(|| format!("ingest.timestream.{}.amazonaws.com", req.region))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn database_arn_shape() {
        assert_eq!(
            database_arn("us-east-1", "000000000000", "metrics"),
            "arn:aws:timestream:us-east-1:000000000000:database/metrics"
        );
    }

    #[test]
    fn table_arn_shape() {
        assert_eq!(
            table_arn("us-east-1", "000000000000", "metrics", "cpu"),
            "arn:aws:timestream:us-east-1:000000000000:database/metrics/table/cpu"
        );
    }

    #[test]
    fn table_key_round_trips() {
        let k = table_key("metrics", "cpu");
        assert_eq!(k.split_once('\u{1}'), Some(("metrics", "cpu")));
    }
}
