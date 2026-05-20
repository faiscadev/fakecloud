//! `S3Service` `website` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl S3Service {
    /// Serve a website object (index or error document) from the bucket.
    pub(crate) fn serve_website_object(
        &self,
        account_id: &str,
        req: &AwsRequest,
        bucket: &str,
        key: &str,
        website_config: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let result = self.get_object(account_id, req, bucket, key);
        if result.is_err() {
            // If index doc doesn't exist either, try error document
            if let Some(error_key) = extract_xml_value(website_config, "ErrorDocument")
                .and_then(|inner| {
                    let open = "<Key>";
                    let close = "</Key>";
                    let s = inner.find(open)? + open.len();
                    let e = inner.find(close)?;
                    Some(inner[s..e].trim().to_string())
                })
                .or_else(|| extract_xml_value(website_config, "Key"))
            {
                return self.serve_website_error(account_id, req, bucket, &error_key);
            }
        }
        result
    }

    /// Serve the website error document with a 404 status.
    pub(crate) fn serve_website_error(
        &self,
        account_id: &str,
        req: &AwsRequest,
        bucket: &str,
        error_key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        match self.get_object(account_id, req, bucket, error_key) {
            Ok(mut resp) => {
                resp.status = StatusCode::NOT_FOUND;
                Ok(resp)
            }
            Err(e) => Err(e),
        }
    }
}
