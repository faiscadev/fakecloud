//! `S3Service` `website` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl S3Service {
    /// Serve a bucket's website index document. The request was authorized
    /// as a listing of the bucket; the document is an object read, so the
    /// caller must also be allowed `s3:GetObject` on it, as on any GetObject
    /// (else 403). A missing index falls back to the error document.
    pub(crate) fn serve_website_object(
        &self,
        account_id: &str,
        req: &AwsRequest,
        bucket: &str,
        key: &str,
        error_key: Option<&str>,
    ) -> Result<AwsResponse, AwsServiceError> {
        if !self.caller_may(req, "GetObject", bucket, Some(key)) {
            return Err(access_denied());
        }
        let result = self.get_object(account_id, req, bucket, key);
        if result.is_err() {
            if let Some(error_key) = error_key {
                return self.serve_website_error(
                    account_id,
                    req,
                    bucket,
                    error_key,
                    StatusCode::NOT_FOUND,
                );
            }
        }
        self.shape_lock_headers(req, bucket, key, result)
    }

    /// Serve the website error document with `status` (404, or 403 for a
    /// missing key hidden from a caller who may not list the bucket). The
    /// document is an object read of its own: a caller not allowed
    /// `s3:GetObject` on it gets a plain 403, and its headers are shaped like
    /// any GetObject's. A missing error document answers `status` too, so a
    /// caller who may not list the bucket learns nothing from it.
    pub(crate) fn serve_website_error(
        &self,
        account_id: &str,
        req: &AwsRequest,
        bucket: &str,
        error_key: &str,
        status: StatusCode,
    ) -> Result<AwsResponse, AwsServiceError> {
        if !self.caller_may(req, "GetObject", bucket, Some(error_key)) {
            return Err(access_denied());
        }
        match self.get_object(account_id, req, bucket, error_key) {
            Ok(mut resp) => {
                resp.status = status;
                self.shape_lock_headers(req, bucket, error_key, Ok(resp))
            }
            Err(_) if status == StatusCode::FORBIDDEN => Err(access_denied()),
            Err(e) => Err(e),
        }
    }
}

fn access_denied() -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::FORBIDDEN, "AccessDenied", "Access Denied")
}
