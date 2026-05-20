//! `CognitoService` `signout` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl CognitoService {
    pub(crate) fn global_sign_out(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let access_token = require_str(&body, "AccessToken")?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        // Look up user from access token
        let token_data = state.access_tokens.get(access_token).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotAuthorizedException",
                "Invalid access token.",
            )
        })?;
        let pool_id = token_data.user_pool_id.clone();
        let username = token_data.username.clone();

        // Invalidate all refresh tokens for this user
        state
            .refresh_tokens
            .retain(|_, v| !(v.user_pool_id == pool_id && v.username == username));

        // Invalidate all access tokens for this user
        state
            .access_tokens
            .retain(|_, v| !(v.user_pool_id == pool_id && v.username == username));

        Ok(AwsResponse::ok_json(json!({})))
    }
}
