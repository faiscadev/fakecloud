use std::sync::Arc;

use fakecloud_core::delivery::FirehoseDelivery;
use fakecloud_persistence::S3Store;
use fakecloud_s3::SharedS3State;

use crate::service::FirehoseService;
use crate::state::SharedFirehoseState;

/// Lets non-Firehose services route records into a delivery stream
/// without taking a direct dep on the firehose crate. CloudWatch Logs
/// subscription filters use this for `arn:aws:firehose:` destinations.
pub struct FirehoseDeliveryImpl {
    inner: Arc<FirehoseService>,
}

impl FirehoseDeliveryImpl {
    pub fn new(state: SharedFirehoseState, s3: SharedS3State) -> Self {
        let svc = FirehoseService::new(state).with_s3(s3);
        Self {
            inner: Arc::new(svc),
        }
    }

    /// Wire the durable S3 store after construction. The delivery hook is built
    /// during startup before the S3 store exists, so the store is set later so
    /// records delivered via the CloudWatch-Logs subscription path are written
    /// through to disk and survive a restart.
    pub fn set_s3_store(&self, store: Arc<dyn S3Store>) {
        self.inner.set_s3_store(store);
    }
}

impl FirehoseDelivery for FirehoseDeliveryImpl {
    fn put_record(&self, delivery_stream_arn: &str, data: &[u8]) {
        let Some((account, region, stream_name)) = parse_arn(delivery_stream_arn) else {
            tracing::warn!(arn = %delivery_stream_arn, "invalid firehose ARN");
            return;
        };
        if let Err(err) =
            self.inner
                .deliver_records(&account, &region, &stream_name, vec![data.to_vec()])
        {
            tracing::warn!(
                arn = %delivery_stream_arn,
                error = ?err,
                "firehose delivery failed"
            );
        }
    }
}

fn parse_arn(arn: &str) -> Option<(String, String, String)> {
    // arn:aws:firehose:<region>:<account>:deliverystream/<name>
    let parts: Vec<&str> = arn.splitn(6, ':').collect();
    if parts.len() < 6 || parts[0] != "arn" || parts[2] != "firehose" {
        return None;
    }
    let stream_name = parts[5].strip_prefix("deliverystream/")?;
    Some((
        parts[4].to_string(),
        parts[3].to_string(),
        stream_name.to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_arn_extracts_components() {
        let parsed = parse_arn("arn:aws:firehose:us-east-1:123456789012:deliverystream/logs-to-s3");
        assert_eq!(
            parsed,
            Some((
                "123456789012".to_string(),
                "us-east-1".to_string(),
                "logs-to-s3".to_string(),
            ))
        );
    }

    #[test]
    fn parse_arn_rejects_non_firehose_arns() {
        assert!(parse_arn("arn:aws:s3:::bucket").is_none());
        assert!(parse_arn("not-an-arn").is_none());
        assert!(parse_arn("arn:aws:firehose:us-east-1:111:stream/foo").is_none());
    }
}
