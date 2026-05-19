//! `StsService` `session` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl StsService {
    pub(super) fn get_session_token(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        // Validate optional DurationSeconds (used below for expiration)
        if let Some(ds) = req.query_params.get("DurationSeconds") {
            let v = ds.parse::<i64>().map_err(|_| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationError",
                    format!(
                        "Value '{}' at 'durationSeconds' failed to satisfy constraint: \
                         Member must be a valid integer",
                        ds
                    ),
                )
            })?;
            validate_range_i64("durationSeconds", v, 900, 129600)?;
        }

        // Validate and accept optional MFA SerialNumber (no verification in emulator)
        validate_optional_string_length(
            "serialNumber",
            req.query_params.get("SerialNumber").map(|s| s.as_str()),
            9,
            256,
        )?;
        let _serial_number = req.query_params.get("SerialNumber").cloned();

        // Validate and accept optional MFA TokenCode (no verification in emulator)
        validate_optional_string_length(
            "tokenCode",
            req.query_params.get("TokenCode").map(|s| s.as_str()),
            6,
            6,
        )?;
        let _token_code = req.query_params.get("TokenCode").cloned();

        // Compute expiration from DurationSeconds (default 43200s / 12 hours)
        let expiration_at = compute_expiration_at(req, DEFAULT_SESSION_TOKEN_DURATION)?;
        let expiration = format_expiration(expiration_at);

        // Resolve the calling principal so the temporary credential is tied
        // to a real identity that SigV4 verification and IAM enforcement can
        // look up later. Falls back to the account root when the caller
        // isn't a known IAM user — matches how `GetSessionToken` behaves
        // against AWS with root credentials.
        let partition = partition_for_region(&req.region);
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let (principal_arn, user_id, account_id) =
            if let Some(akid) = extract_access_key(req).as_deref() {
                if let Some(lookup) = state.credential_secret_readonly(akid) {
                    (lookup.principal_arn, lookup.user_id, lookup.account_id)
                } else {
                    (
                        format!("arn:{}:iam::{}:root", partition, state.account_id),
                        state.account_id.clone(),
                        state.account_id.clone(),
                    )
                }
            } else {
                (
                    format!("arn:{}:iam::{}:root", partition, state.account_id),
                    state.account_id.clone(),
                    state.account_id.clone(),
                )
            };

        let creds = StsCredentials::generate();
        state.credential_identities.insert(
            creds.access_key_id.clone(),
            CredentialIdentity {
                arn: principal_arn.clone(),
                user_id: user_id.clone(),
                account_id: account_id.clone(),
            },
        );
        // GetSessionToken does not accept a Policy parameter per AWS
        // docs, so session_policies is always empty for this operation.
        let mfa_present_for_session = req.query_params.contains_key("SerialNumber")
            && req.query_params.contains_key("TokenCode");
        state.sts_temp_credentials.insert(
            creds.access_key_id.clone(),
            StsTempCredential {
                access_key_id: creds.access_key_id.clone(),
                secret_access_key: creds.secret_access_key.clone(),
                session_token: creds.session_token.clone(),
                principal_arn,
                user_id,
                account_id,
                expiration: expiration_at,
                session_policies: Vec::new(),
                mfa_present: mfa_present_for_session,
                issued_at: Utc::now(),
                // GetSessionToken doesn't federate.
                federated_provider: None,
            },
        );

        let xml = xml_responses::get_session_token_response(&creds, &expiration, &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    pub(super) fn decode_authorization_message(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let encoded_message = req.query_params.get("EncodedMessage").ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MissingParameter",
                "The request must contain the parameter EncodedMessage",
            )
        })?;
        validate_string_length("encodedMessage", encoded_message, 1, 10240)?;

        // Round-trip the deflated/base64'd token produced by
        // `auth_message::encode_deny`. Tokens that don't decode are
        // rejected with `InvalidAuthorizationMessageException`,
        // matching how AWS reports a corrupted blob.
        let decoded_message =
            crate::auth_message::decode_message(encoded_message).map_err(|why| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidAuthorizationMessageException",
                    why,
                )
            })?;
        let xml =
            xml_responses::decode_authorization_message_response(&decoded_message, &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }
}
