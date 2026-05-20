//! `S3Service` `accelerate` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl S3Service {
    // ---- Accelerate ----

    pub(crate) fn put_bucket_accelerate(
        &self,
        account_id: &str,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        if bucket.contains('.') {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidRequest",
                "S3 Transfer Acceleration is not supported for buckets with periods (.) in their names",
            ));
        }
        let body_str = std::str::from_utf8(&req.body).unwrap_or("");
        let status = extract_xml_value(body_str, "Status");
        let mut accts = self.state.write();
        let state = accts.get_or_create(account_id);
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        // Validate status
        if let Some(ref s) = status {
            if s != "Enabled" && s != "Suspended" {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "MalformedXML",
                    "The XML you provided was not well-formed or did not validate against our published schema",
                ));
            }
        }
        // Suspending a never-configured bucket is a no-op
        if status.as_deref() == Some("Suspended") && b.accelerate_status.is_none() {
            return Ok(empty_response(StatusCode::OK));
        }
        b.accelerate_status = status;
        let meta = bucket_meta_snapshot(b);
        self.store
            .put_bucket_meta(bucket, &meta)
            .map_err(crate::service::persistence_error)?;
        Ok(empty_response(StatusCode::OK))
    }

    pub(crate) fn get_bucket_accelerate(
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
        let status_xml = match &b.accelerate_status {
            Some(s) => format!("<Status>{s}</Status>"),
            None => String::new(),
        };
        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <AccelerateConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             {status_xml}\
             </AccelerateConfiguration>"
        );
        Ok(s3_xml(StatusCode::OK, body))
    }
}
