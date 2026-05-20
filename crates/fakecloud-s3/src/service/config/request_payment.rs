//! `S3Service` `request_payment` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl S3Service {
    // ---- Request Payment ----

    pub(crate) fn put_bucket_request_payment(
        &self,
        account_id: &str,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body_str = std::str::from_utf8(&req.body).unwrap_or("").to_string();
        {
            let mut accts = self.state.write();
            let state = accts.get_or_create(account_id);
            let b = state
                .buckets
                .get_mut(bucket)
                .ok_or_else(|| no_such_bucket(bucket))?;
            b.request_payment = Some(body_str.clone());
        }
        self.store
            .put_bucket_subresource(bucket, BucketSubresource::RequestPayment, &body_str)
            .map_err(crate::service::persistence_error)?;
        Ok(empty_response(StatusCode::OK))
    }

    pub(crate) fn get_bucket_request_payment(
        &self,
        account_id: &str,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accts = self.state.read();
        let empty = crate::state::S3State::new(account_id, "us-east-1");
        let state = accts.get(account_id).unwrap_or(&empty);
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        let payer = b
            .request_payment
            .as_deref()
            .and_then(|x| extract_xml_value(x, "Payer"))
            .unwrap_or_else(|| "BucketOwner".to_string());
        let body = format!(
            "<RequestPaymentConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Payer>{}</Payer></RequestPaymentConfiguration>",
            xml_escape(&payer)
        );
        Ok(s3_xml(StatusCode::OK, body))
    }
}
