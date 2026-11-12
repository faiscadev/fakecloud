//! `S3Service` `notification` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl S3Service {
    // ---- Notification ----

    pub(crate) fn put_bucket_notification(
        &self,
        account_id: &str,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body_str = std::str::from_utf8(&req.body).unwrap_or("").to_string();
        let region = self
            .state
            .read()
            .get(account_id)
            .and_then(|s| s.buckets.get(bucket))
            .map(|b| b.region.clone())
            .ok_or_else(|| no_such_bucket(bucket))?;
        self.validate_queue_destinations(account_id, &region, bucket, &body_str)?;
        let mut accts = self.state.write();
        let state = accts.get_or_create(account_id);
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        // Check if EventBridgeConfiguration XML element is present (opening tag or self-closing)
        b.eventbridge_enabled = body_str.contains("<EventBridgeConfiguration");
        // Auto-generate Id for each configuration element if missing
        let normalized = normalize_notification_ids(&body_str);
        b.notification_config = Some(normalized.clone());
        let meta = bucket_meta_snapshot(b);
        self.store
            .put_bucket_meta(bucket, &meta)
            .map_err(crate::service::persistence_error)?;
        self.store
            .put_bucket_subresource(bucket, BucketSubresource::Notification, &normalized)
            .map_err(crate::service::persistence_error)?;
        Ok(empty_response(StatusCode::OK))
    }

    /// S3 validates each SQS destination of a notification configuration by
    /// sending it a test message as the S3 service principal, for the
    /// bucket. A queue whose encryption key does not let S3 encrypt (one
    /// under the AWS-managed `aws/sqs` key, whose policy admits only callers
    /// of the account) fails the request, recorded as 400 `InvalidArgument`
    /// "Unable to validate the following destination configurations" naming
    /// each queue (`ArgumentName<n>`) with "The message to the SSE queue
    /// could not be encrypted using KMS" (`ArgumentValue<n>`).
    fn validate_queue_destinations(
        &self,
        account_id: &str,
        region: &str,
        bucket: &str,
        config: &str,
    ) -> Result<(), AwsServiceError> {
        let bucket_arn = fakecloud_aws::arn::Arn::s3_in(region, bucket).to_string();
        let mut failures = Vec::new();
        for target in crate::service::notifications::parse_notification_config(config) {
            if !matches!(
                target.target_type,
                crate::service::notifications::NotificationTargetType::Sqs
            ) {
                continue;
            }
            let sender = fakecloud_core::delivery::DeliverySender::Service {
                principal: "s3.amazonaws.com",
                source_arn: &bucket_arn,
                source_account: account_id,
            };
            if let Err(fakecloud_core::delivery::SqsDeliveryError::Kms(_)) =
                self.delivery.check_sqs_sender(sender, &target.arn)
            {
                failures.push(target.arn.clone());
            }
        }
        if failures.is_empty() {
            return Ok(());
        }
        let mut fields = Vec::new();
        for (i, arn) in failures.into_iter().enumerate() {
            fields.push((format!("ArgumentName{}", i + 1), arn));
            fields.push((
                format!("ArgumentValue{}", i + 1),
                "The message to the SSE queue could not be encrypted using KMS".to_string(),
            ));
        }
        Err(AwsServiceError::aws_error_with_fields(
            StatusCode::BAD_REQUEST,
            "InvalidArgument",
            "Unable to validate the following destination configurations",
            fields,
        ))
    }

    pub(crate) fn get_bucket_notification(
        &self,
        account_id: &str,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accts = self.state.read();
        let __empty = crate::state::S3State::new(account_id, "us-east-1");
        let state = accts.get(account_id).unwrap_or(&__empty);
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        let mut body = match &b.notification_config {
            Some(config) => config.clone(),
            None => "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
                     <NotificationConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
                     </NotificationConfiguration>"
                .to_string(),
        };
        // Ensure EventBridgeConfiguration is in response if enabled
        if b.eventbridge_enabled && !body.contains("EventBridgeConfiguration") {
            if let Some(pos) = body.find("</NotificationConfiguration>") {
                body.insert_str(pos, "<EventBridgeConfiguration/>");
            }
        }
        Ok(s3_xml(StatusCode::OK, body))
    }
}
