//! The handler side of [`super::route`]: run the operation a request was
//! classified as. Every arm is one operation; nothing here re-reads the query
//! to pick one.

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};
use http::StatusCode;

use super::route::S3Op;
use super::{extract_xml_value, S3Service};

impl S3Service {
    /// Run `op` for `req`, on bucket `b` and key `k` (empty when the
    /// operation addresses none), in `account_id` (the bucket owner's) or,
    /// for the operations that act in the caller's own account,
    /// `caller_account`.
    pub(super) async fn route_operation(
        &self,
        op: S3Op,
        req: &AwsRequest,
        caller_account: &str,
        account_id: &str,
        b: &str,
        k: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        // The query member an upload operation names its upload with; the
        // classification only routes here when it is present.
        let upload_id = || {
            req.query_params
                .get("uploadId")
                .cloned()
                .unwrap_or_default()
        };
        // Recorded: an upload that does not exist is NoSuchUpload before the
        // part number is looked at (dispatch checks it before authorizing,
        // too); the handlers check the number's range.
        if let Some(missing) = self.missing_upload(req, op) {
            return Err(missing);
        }
        let part_number = || -> Result<i64, AwsServiceError> {
            let raw = req
                .query_params
                .get("partNumber")
                .map(String::as_str)
                .unwrap_or_default();
            raw.parse::<i64>()
                .map_err(|_| super::multipart::invalid_part_number(raw))
        };
        use S3Op::*;
        match op {
            ListBuckets => self.list_buckets(caller_account, req),
            ListDirectoryBuckets => self.list_directory_buckets(caller_account, req),
            WriteGetObjectResponse => self.write_get_object_response(account_id, req),

            // Multipart uploads.
            CreateMultipartUpload => self.create_multipart_upload(account_id, req, b, k),
            CompleteMultipartUpload => {
                self.complete_multipart_upload(account_id, req, b, k, &upload_id())
            }
            UploadPart => {
                self.upload_part(account_id, req, b, k, &upload_id(), part_number()?)
                    .await
            }
            UploadPartCopy => {
                self.upload_part_copy(account_id, req, b, k, &upload_id(), part_number()?)
            }
            AbortMultipartUpload => self.abort_multipart_upload(account_id, b, k, &upload_id()),
            ListMultipartUploads => self.list_multipart_uploads(account_id, b, &req.query_params),
            ListParts => self.list_parts(account_id, req, b, k, &upload_id()),

            // Buckets.
            CreateBucket => self.create_bucket(caller_account, req, b),
            DeleteBucket => self.delete_bucket(account_id, req, b),
            HeadBucket => self.head_bucket(account_id, b),
            ListObjects => self.list_objects_or_website_index(account_id, req, b),
            ListObjectsV2 => self.list_objects_v2(account_id, req, b),
            ListObjectVersions => self.list_object_versions(account_id, req, b),
            GetBucketLocation => self.get_bucket_location(account_id, b),
            CreateSession => self.create_session(account_id, req, b),
            DeleteObjects => self.delete_objects(account_id, req, b),
            PostObject => self.post_object(account_id, req, b).await,
            GetBucketTagging => self.get_bucket_tagging(account_id, req, b),
            PutBucketTagging => self.put_bucket_tagging(account_id, req, b),
            DeleteBucketTagging => self.delete_bucket_tagging(account_id, req, b),
            GetBucketAcl => self.get_bucket_acl(account_id, req, b),
            PutBucketAcl => self.put_bucket_acl(account_id, req, b),
            GetBucketVersioning => self.get_bucket_versioning(account_id, b),
            PutBucketVersioning => self.put_bucket_versioning(account_id, req, b),
            GetBucketCors => self.get_bucket_cors(account_id, b),
            PutBucketCors => self.put_bucket_cors(account_id, req, b),
            DeleteBucketCors => self.delete_bucket_cors(account_id, b),
            GetBucketNotificationConfiguration => self.get_bucket_notification(account_id, b),
            PutBucketNotificationConfiguration => self.put_bucket_notification(account_id, req, b),
            GetBucketWebsite => self.get_bucket_website(account_id, b),
            PutBucketWebsite => self.put_bucket_website(account_id, req, b),
            DeleteBucketWebsite => self.delete_bucket_website(account_id, b),
            GetBucketAccelerateConfiguration => self.get_bucket_accelerate(account_id, b),
            PutBucketAccelerateConfiguration => self.put_bucket_accelerate(account_id, req, b),
            GetPublicAccessBlock => self.get_public_access_block(account_id, b),
            PutPublicAccessBlock => self.put_public_access_block(account_id, req, b),
            DeletePublicAccessBlock => self.delete_public_access_block(account_id, b),
            GetBucketEncryption => self.get_bucket_encryption(account_id, b),
            PutBucketEncryption => self.put_bucket_encryption(account_id, req, b),
            DeleteBucketEncryption => self.delete_bucket_encryption(account_id, b),
            GetBucketLifecycleConfiguration => self.get_bucket_lifecycle(account_id, b),
            PutBucketLifecycleConfiguration => self.put_bucket_lifecycle(account_id, req, b),
            DeleteBucketLifecycle => self.delete_bucket_lifecycle(account_id, b),
            GetBucketLogging => self.get_bucket_logging(account_id, b),
            PutBucketLogging => self.put_bucket_logging(account_id, req, b),
            GetBucketPolicy => self.get_bucket_policy(account_id, b),
            PutBucketPolicy => self.put_bucket_policy(account_id, req, b),
            DeleteBucketPolicy => self.delete_bucket_policy(account_id, b),
            GetBucketPolicyStatus => self.get_bucket_policy_status(account_id, b),
            GetObjectLockConfiguration => self.get_object_lock_configuration(account_id, b),
            PutObjectLockConfiguration => self.put_object_lock_config(account_id, req, b),
            GetBucketReplication => self.get_bucket_replication(account_id, b),
            PutBucketReplication => self.put_bucket_replication(account_id, req, b),
            DeleteBucketReplication => self.delete_bucket_replication(account_id, b),
            GetBucketOwnershipControls => self.get_bucket_ownership_controls(account_id, b),
            PutBucketOwnershipControls => self.put_bucket_ownership_controls(account_id, req, b),
            DeleteBucketOwnershipControls => self.delete_bucket_ownership_controls(account_id, b),
            GetBucketInventoryConfiguration => self.get_bucket_inventory(account_id, req, b),
            ListBucketInventoryConfigurations => {
                self.list_bucket_inventory_configurations(account_id, b)
            }
            PutBucketInventoryConfiguration => self.put_bucket_inventory(account_id, req, b),
            DeleteBucketInventoryConfiguration => self.delete_bucket_inventory(account_id, req, b),
            GetBucketAnalyticsConfiguration => self.get_bucket_analytics_config(account_id, req, b),
            ListBucketAnalyticsConfigurations => {
                self.list_bucket_analytics_configurations(account_id, b)
            }
            PutBucketAnalyticsConfiguration => self.put_bucket_analytics_config(account_id, req, b),
            DeleteBucketAnalyticsConfiguration => {
                self.delete_bucket_analytics_config(account_id, req, b)
            }
            GetBucketIntelligentTieringConfiguration => {
                self.get_bucket_intelligent_tiering_config(account_id, req, b)
            }
            ListBucketIntelligentTieringConfigurations => {
                self.list_bucket_intelligent_tiering_configurations(account_id, b)
            }
            PutBucketIntelligentTieringConfiguration => {
                self.put_bucket_intelligent_tiering_config(account_id, req, b)
            }
            DeleteBucketIntelligentTieringConfiguration => {
                self.delete_bucket_intelligent_tiering_config(account_id, req, b)
            }
            GetBucketMetricsConfiguration => self.get_bucket_metrics_config(account_id, req, b),
            ListBucketMetricsConfigurations => {
                self.list_bucket_metrics_configurations(account_id, b)
            }
            PutBucketMetricsConfiguration => self.put_bucket_metrics_config(account_id, req, b),
            DeleteBucketMetricsConfiguration => {
                self.delete_bucket_metrics_config(account_id, req, b)
            }
            GetBucketRequestPayment => self.get_bucket_request_payment(account_id, b),
            PutBucketRequestPayment => self.put_bucket_request_payment(account_id, req, b),
            GetBucketAbac => self.get_bucket_abac(account_id, b),
            PutBucketAbac => self.put_bucket_abac(account_id, req, b),
            GetBucketMetadataConfiguration => self.get_bucket_metadata_config(account_id, b),
            CreateBucketMetadataConfiguration => {
                self.create_bucket_metadata_config(account_id, req, b)
            }
            DeleteBucketMetadataConfiguration => self.delete_bucket_metadata_config(account_id, b),
            GetBucketMetadataTableConfiguration => {
                self.get_bucket_metadata_table_config(account_id, b)
            }
            CreateBucketMetadataTableConfiguration => {
                self.create_bucket_metadata_table_config(account_id, req, b)
            }
            DeleteBucketMetadataTableConfiguration => {
                self.delete_bucket_metadata_table_config(account_id, b)
            }
            UpdateBucketMetadataInventoryTableConfiguration => {
                self.update_bucket_metadata_inventory_table(account_id, req, b)
            }
            UpdateBucketMetadataJournalTableConfiguration => {
                self.update_bucket_metadata_journal_table(account_id, req, b)
            }
            UpdateBucketMetadataAnnotationTableConfiguration => {
                self.update_bucket_metadata_annotation_table_configuration(account_id, req, b)
            }

            // Objects.
            PutObject => self.put_object(account_id, req, b, k).await,
            CopyObject => self.copy_object(account_id, req, b, k),
            GetObject => self.get_object_or_website_error(account_id, req, b, k),
            HeadObject => {
                let result = self.head_object(account_id, req, b, k);
                let result = self.hide_missing_key(req, b, result);
                self.shape_lock_headers(req, b, k, result)
            }
            DeleteObject => self.delete_object(account_id, req, b, k),
            GetObjectAttributes => {
                let result = self.get_object_attributes(account_id, req, b, k);
                self.hide_missing_key(req, b, result)
            }
            GetObjectTorrent => self.get_object_torrent(account_id, req, b, k),
            RestoreObject => self.restore_object(account_id, req, b, k),
            SelectObjectContent => self.select_object_content(account_id, req, b, k),
            RenameObject => self.rename_object(account_id, req, b, k),
            UpdateObjectEncryption => self.update_object_encryption(account_id, req, b, k),
            GetObjectTagging => self.get_object_tagging(account_id, req, b, k),
            PutObjectTagging => self.put_object_tagging(account_id, req, b, k),
            DeleteObjectTagging => self.delete_object_tagging(account_id, req, b, k),
            GetObjectAcl => self.get_object_acl(account_id, req, b, k),
            PutObjectAcl => self.put_object_acl(account_id, req, b, k),
            GetObjectRetention => self.get_object_retention(account_id, req, b, k),
            PutObjectRetention => self.put_object_retention(account_id, req, b, k),
            GetObjectLegalHold => self.get_object_legal_hold(account_id, req, b, k),
            PutObjectLegalHold => self.put_object_legal_hold(account_id, req, b, k),
            GetObjectAnnotation => self.get_object_annotation(account_id, req, b, k),
            ListObjectAnnotations => self.list_object_annotations(account_id, req, b, k),
            PutObjectAnnotation => self.put_object_annotation(account_id, req, b, k),
            DeleteObjectAnnotation => self.delete_object_annotation(account_id, req, b, k),
        }
    }

    /// For an upload part or a part listing, the NoSuchUpload error when the
    /// bucket exists but holds no such upload of this key. All this tells a
    /// caller is whether the upload exists, which S3 answers before
    /// authorizing the request (recorded, anonymously).
    pub(super) fn missing_upload(&self, req: &AwsRequest, op: S3Op) -> Option<AwsServiceError> {
        if !matches!(
            op,
            S3Op::UploadPart | S3Op::UploadPartCopy | S3Op::ListParts
        ) {
            return None;
        }
        let bucket = req.path_segments.first()?;
        let key = super::request_key(req)?;
        let upload_id = req.query_params.get("uploadId")?;
        let accounts = self.state.read();
        let owner = accounts.find_account(|s| s.buckets.contains_key(bucket.as_str()))?;
        let b = accounts.get(owner)?.buckets.get(bucket.as_str())?;
        let exists = b
            .multipart_uploads
            .get(upload_id)
            .is_some_and(|upload| upload.key == key);
        (!exists).then(|| super::no_such_upload(upload_id))
    }

    /// ListObjects, or for a bucket with a website configuration and a
    /// request with no query at all, its index document.
    fn list_objects_or_website_index(
        &self,
        account_id: &str,
        req: &AwsRequest,
        b: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let website_config = req
            .query_params
            .is_empty()
            .then(|| self.website_config(account_id, req, b))
            .flatten();
        let index = website_config.as_deref().and_then(|config| {
            extract_xml_value(config, "Suffix").or_else(|| {
                extract_xml_value(config, "IndexDocument").and_then(|inner| {
                    let open = "<Suffix>";
                    let close = "</Suffix>";
                    let s = inner.find(open)? + open.len();
                    let e = inner.find(close)?;
                    Some(inner[s..e].trim().to_string())
                })
            })
        });
        match (index, website_config) {
            (Some(index), Some(config)) => {
                self.serve_website_object(account_id, req, b, &index, &config)
            }
            _ => self.list_objects_v1(account_id, req, b),
        }
    }

    /// GetObject. A missing object of a bucket with a website configuration
    /// gets its error document instead, with the status the caller would get:
    /// 404, or 403 for a caller that may not list the bucket (S3 serves the
    /// custom error document for 4XX errors).
    fn get_object_or_website_error(
        &self,
        account_id: &str,
        req: &AwsRequest,
        b: &str,
        k: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let result = self.get_object(account_id, req, b, k);
        let missing = matches!(&result, Err(e) if e.code() == "NoSuchKey");
        let result = self.hide_missing_key(req, b, result);
        let result = self.shape_lock_headers(req, b, k, result);
        if !missing {
            return result;
        }
        let status = result
            .as_ref()
            .err()
            .map_or(StatusCode::NOT_FOUND, |e| e.status());
        let error_key = self
            .website_config(account_id, req, b)
            .and_then(|config| website_error_key(&config));
        match error_key {
            Some(error_key) => self.serve_website_error(account_id, req, b, &error_key, status),
            None => result,
        }
    }

    /// The bucket's website configuration, if any.
    fn website_config(&self, account_id: &str, req: &AwsRequest, b: &str) -> Option<String> {
        let accounts = self.state.read();
        let empty = crate::state::S3State::new(account_id, &req.region);
        let state = accounts.get(account_id).unwrap_or(&empty);
        state
            .buckets
            .get(b)
            .and_then(|bucket| bucket.website_config.clone())
    }
}

/// The error document's key of a website configuration.
pub(super) fn website_error_key(config: &str) -> Option<String> {
    extract_xml_value(config, "ErrorDocument")
        .and_then(|inner| {
            let open = "<Key>";
            let close = "</Key>";
            let s = inner.find(open)? + open.len();
            let e = inner.find(close)?;
            Some(inner[s..e].trim().to_string())
        })
        .or_else(|| extract_xml_value(config, "Key"))
}
