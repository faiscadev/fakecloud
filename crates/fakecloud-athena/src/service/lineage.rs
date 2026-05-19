//! `AthenaService` `lineage` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl AthenaService {
    pub(super) fn get_resource_dashboard(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // Smithy: ResourceARN @required, targets AmazonResourceName @length(1,1011).
        validate_required_string_len(&body, "ResourceARN", 1, 1011)?;
        // Smithy GetResourceDashboardOutput: { Url: String (required) }.
        Ok(AwsResponse::ok_json(json!({
            "Url": "https://console.aws.amazon.com/athena/home#/dashboard",
        })))
    }
}
