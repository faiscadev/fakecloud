//! `StsService` `caller` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl StsService {
    pub(super) fn get_caller_identity(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        // Prefer the pre-resolved principal that dispatch attached via the
        // credential resolver — avoids re-parsing the Authorization header
        // and re-walking IamState. Falls back to the account-root identity
        // when the caller didn't present resolvable credentials (e.g. the
        // `test` root bypass, or no auth header at all).
        if let Some(principal) = req.principal.as_ref() {
            let xml = xml_responses::get_caller_identity_response(
                &principal.account_id,
                &principal.arn,
                &principal.user_id,
                &req.request_id,
            );
            return Ok(AwsResponse::xml(StatusCode::OK, xml));
        }

        // F4: real GetCallerIdentity rejects calls that arrive with
        // neither a resolvable principal nor an Authorization header.
        // AWS returns `MissingAuthenticationTokenException` (HTTP 403)
        // in that case. We keep the unsigned-but-account-scoped path
        // (the `test` root bypass and similar smoke probes) by falling
        // back to root only when an Authorization header was present
        // but didn't resolve to a stored principal.
        let has_auth_header = req.headers.contains_key("authorization")
            || req.headers.contains_key("x-amz-security-token");
        if !has_auth_header {
            return Err(AwsServiceError::aws_error(
                StatusCode::FORBIDDEN,
                "MissingAuthenticationTokenException",
                "Request is missing Authentication Token",
            ));
        }

        let accounts = self.state.read();
        let account_id = accounts.default_account_id();
        let partition = partition_for_region(&req.region);
        let arn = format!("arn:{}:iam::{}:root", partition, account_id);
        let user_id = "FKIAIOSFODNN7EXAMPLE";
        let xml =
            xml_responses::get_caller_identity_response(account_id, &arn, user_id, &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    pub(super) fn get_access_key_info(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let access_key_id = req.query_params.get("AccessKeyId").ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MissingParameter",
                "The request must contain the parameter AccessKeyId",
            )
        })?;
        validate_string_length("accessKeyId", access_key_id, 16, 128)?;

        // Try to resolve account from known access keys across all accounts
        let accounts = self.state.read();
        let mut resolved_account_id = None;
        for (acct_id, acct_state) in accounts.iter() {
            if acct_state
                .access_keys
                .values()
                .flatten()
                .any(|k| k.access_key_id == *access_key_id)
            {
                resolved_account_id = Some(acct_id.to_string());
                break;
            }
            if let Some(ci) = acct_state.credential_identities.get(access_key_id.as_str()) {
                resolved_account_id = Some(ci.account_id.clone());
                break;
            }
        }
        let account_id =
            resolved_account_id.unwrap_or_else(|| accounts.default_account_id().to_string());

        let xml = xml_responses::get_access_key_info_response(&account_id, &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }
}
