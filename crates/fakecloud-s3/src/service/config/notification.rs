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
