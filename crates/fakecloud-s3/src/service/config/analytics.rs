//! `S3Service` `analytics` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl S3Service {
    // ---- Analytics / Intelligent-Tiering / Metrics configurations ----

    pub(crate) fn put_bucket_analytics_config(
        &self,
        account_id: &str,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        store_named_config(self, account_id, req, bucket, ConfigKind::Analytics)
    }

    pub(crate) fn get_bucket_analytics_config(
        &self,
        account_id: &str,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        get_named_config(self, account_id, req, bucket, ConfigKind::Analytics)
    }

    pub(crate) fn delete_bucket_analytics_config(
        &self,
        account_id: &str,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        delete_named_config(self, account_id, req, bucket, ConfigKind::Analytics)
    }

    pub(crate) fn list_bucket_analytics_configurations(
        &self,
        account_id: &str,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        list_named_config(self, account_id, bucket, ConfigKind::Analytics)
    }
}
