//! `OrganizationsService` `org` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl OrganizationsService {
    pub(super) fn create_organization(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let feature_set = body
            .get("FeatureSet")
            .and_then(|v| v.as_str())
            .unwrap_or(FEATURE_SET_ALL);
        // FeatureSet is an enum: only ALL and CONSOLIDATED_BILLING are valid.
        // Anything else is a malformed request (InvalidInputException), which
        // is what AWS returns for an out-of-enum value.
        if feature_set != FEATURE_SET_ALL && feature_set != FEATURE_SET_CONSOLIDATED_BILLING {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidInputException",
                format!("FeatureSet must be one of [ALL, CONSOLIDATED_BILLING], got {feature_set}"),
            ));
        }

        let mut guard = self.state.write();
        if guard.is_some() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "AlreadyInOrganizationException",
                "The AWS account is already a member of an organization.",
            ));
        }
        let mut org = OrganizationState::bootstrap(&req.account_id);
        // A CONSOLIDATED_BILLING org has no policy management; reflect the
        // requested feature set and drop the auto-enabled SCP type.
        if feature_set == FEATURE_SET_CONSOLIDATED_BILLING {
            org.feature_set = FEATURE_SET_CONSOLIDATED_BILLING.to_string();
            org.enabled_policy_types.clear();
        }
        let resp_value = organization_payload(&org);
        *guard = Some(org);
        Ok(AwsResponse::ok_json(json!({ "Organization": resp_value })))
    }

    pub(super) fn describe_organization(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let org = guard.as_ref().ok_or_else(organizations_not_in_use)?;
        // AWS scopes DescribeOrganization to members of the organization.
        // Non-members must not learn that an org exists at all — return
        // the same `AWSOrganizationsNotInUseException` the no-org path
        // returns so org metadata doesn't leak across account boundaries.
        if !org.accounts.contains_key(&req.account_id) {
            return Err(organizations_not_in_use());
        }
        Ok(AwsResponse::ok_json(
            json!({ "Organization": organization_payload(org) }),
        ))
    }

    pub(super) fn delete_organization(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let org = guard.as_ref().ok_or_else(organizations_not_in_use)?;
        // Non-members get the same "not in use" error as callers in a
        // process with no org at all — they should not be able to tell
        // the difference.
        if !org.accounts.contains_key(&req.account_id) {
            return Err(organizations_not_in_use());
        }
        if !org.is_management(&req.account_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::FORBIDDEN,
                "AccessDeniedException",
                "Only the management account can delete the organization.",
            ));
        }
        // Match AWS: delete fails if any member accounts besides the
        // management account remain. In Batch 1 only the management is
        // enrolled, so this check is a no-op; Batch 2 starts populating
        // real member accounts.
        let non_mgmt = org
            .accounts
            .keys()
            .filter(|id| id.as_str() != org.management_account_id)
            .count();
        if non_mgmt > 0 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "OrganizationNotEmptyException",
                "The organization still has member accounts. Remove them first.",
            ));
        }
        *guard = None;
        Ok(AwsResponse::ok_json(Value::Null))
    }
}
