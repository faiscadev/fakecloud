//! `OrganizationsService` `policy_types` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl OrganizationsService {
    pub(super) fn enable_all_features(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        self.require_member_management(&guard, &req.account_id)?;
        let org = guard.as_mut().expect("management gate proved Some");
        org.enable_all_features();
        // AWS returns a Handshake here; we synthesize a minimal accepted
        // shape so SDKs can deserialize the response.
        let handshake = json!({
            "Id": "h-enableallfeatures",
            "Arn": format!(
                "arn:aws:organizations::{}:handshake/{}/enable-all-features/h-enableallfeatures",
                org.management_account_id, org.org_id
            ),
            "Action": "ENABLE_ALL_FEATURES",
            "State": "ACCEPTED",
            "Parties": [],
            "Resources": [],
        });
        Ok(AwsResponse::ok_json(json!({ "Handshake": handshake })))
    }
}
