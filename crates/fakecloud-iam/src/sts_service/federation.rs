//! `StsService` `federation` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl StsService {
    pub(super) fn get_federation_token(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = req.query_params.get("Name").ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MissingParameter",
                "The request must contain the parameter Name",
            )
        })?;
        sts_validate_string_length("name", name, 2, 32)?;

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
            sts_validate_range_i64("durationSeconds", v, 900, 129600)?;
        }

        // Validate and store optional policy
        validate_optional_string_length(
            "policy",
            req.query_params.get("Policy").map(|s| s.as_str()),
            1,
            2048,
        )?;
        let policy = req.query_params.get("Policy").cloned();

        // Compute expiration from DurationSeconds (default 43200s / 12 hours)
        let expiration_at = compute_expiration_at(req, DEFAULT_FEDERATION_TOKEN_DURATION)?;
        let expiration = format_expiration(expiration_at);

        let partition = partition_for_region(&req.region);
        let creds = StsCredentials::generate();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let session_policies = collect_session_policies(req, state);
        let account_id = state.account_id.clone();
        let federated_user_arn = format!(
            "arn:{}:sts::{}:federated-user/{}",
            partition, account_id, name
        );
        let federated_user_id = format!("{}:{}", account_id, name);

        state.credential_identities.insert(
            creds.access_key_id.clone(),
            CredentialIdentity {
                arn: federated_user_arn.clone(),
                user_id: federated_user_id.clone(),
                account_id: account_id.clone(),
            },
        );
        state.sts_temp_credentials.insert(
            creds.access_key_id.clone(),
            StsTempCredential {
                access_key_id: creds.access_key_id.clone(),
                secret_access_key: creds.secret_access_key.clone(),
                session_token: creds.session_token.clone(),
                principal_arn: federated_user_arn,
                user_id: federated_user_id,
                account_id: account_id.clone(),
                expiration: expiration_at,
                session_policies,
                mfa_present: false,
                issued_at: Utc::now(),
                // GetFederationToken yields a federated user, not a federated
                // provider — `aws:FederatedProvider` stays absent.
                federated_provider: None,
            },
        );

        let xml = xml_responses::get_federation_token_response(
            &creds,
            name,
            &account_id,
            partition,
            &expiration,
            policy.as_deref(),
            &req.request_id,
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    /// `GetWebIdentityToken`: mint a signed (well, structurally-valid)
    /// JWT representing the caller. The Smithy op declares only
    /// `JWTPayloadSizeExceededException`, `OutboundWebIdentityFederationDisabledException`,
    /// and `SessionDurationEscalationException` — no input-validation
    /// error shape. So we accept whatever audience / algorithm /
    /// duration the caller asked for and emit a token.
    ///
    /// The returned JWT is a base64url(header).base64url(payload).
    /// base64url("fakecloud-stub") triple — real callers exchanging it
    /// against `AssumeRoleWithWebIdentity` against the same fakecloud
    /// instance will get back credentials because that op already
    /// tolerates unsigned tokens.
    pub(super) fn get_web_identity_token(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        use base64::engine::general_purpose::URL_SAFE_NO_PAD as b64;
        use base64::Engine as _;

        // The op declares no input-validation shape, but
        // `SessionDurationEscalationException` fits a duration-bounds
        // violation, so route every Smithy-modeled @required / @length /
        // @range violation through it. Probes only require *some*
        // declared 4xx for negative variants.
        let audiences = collect_audiences(req);
        if audiences.is_empty() {
            return Err(sts_web_identity_error("Audience is required"));
        }
        let duration_seconds = match req.query_params.get("DurationSeconds") {
            Some(raw) => match raw.parse::<i64>() {
                Ok(v) if (60..=3600).contains(&v) => v,
                _ => {
                    return Err(sts_web_identity_error(
                        "DurationSeconds must be between 60 and 3600",
                    ))
                }
            },
            None => 300,
        };
        let signing_alg = req
            .query_params
            .get("SigningAlgorithm")
            .cloned()
            .ok_or_else(|| sts_web_identity_error("SigningAlgorithm is required"))?;
        // `jwtAlgorithmType` is @length 5..=5.
        if signing_alg.len() != 5 {
            return Err(sts_web_identity_error(
                "SigningAlgorithm must be exactly 5 characters (e.g. RS256, ES384)",
            ));
        }

        let issued_at = Utc::now();
        let expiration_at = issued_at + chrono::Duration::seconds(duration_seconds);

        let principal_arn = req
            .principal
            .as_ref()
            .map(|p| p.arn.clone())
            .unwrap_or_else(|| {
                let accounts = self.state.read();
                let account_id = accounts.default_account_id();
                let partition = partition_for_region(&req.region);
                format!("arn:{partition}:iam::{account_id}:root")
            });

        let header = serde_json::json!({
            "alg": signing_alg,
            "typ": "JWT",
            "kid": "fakecloud-sts-stub",
        });
        let mut payload = serde_json::json!({
            // Partition-aware iss: AWS GovCloud and China regions use
            // different STS hostnames; hardcoding `.amazonaws.com`
            // produces issuer values that OIDC providers in those
            // partitions won't trust.
            "iss": sts_issuer_url(&req.region),
            "sub": principal_arn,
            "aud": audiences,
            "iat": issued_at.timestamp(),
            "exp": expiration_at.timestamp(),
            "nbf": issued_at.timestamp(),
        });
        // Custom tag claims: every `Tags.member.N.Key` / `.Value` pair
        // becomes a top-level JWT claim, matching the AWS contract.
        for i in 1..=50 {
            let kkey = format!("Tags.member.{i}.Key");
            let vkey = format!("Tags.member.{i}.Value");
            match (req.query_params.get(&kkey), req.query_params.get(&vkey)) {
                (Some(k), Some(v)) => {
                    payload[k] = serde_json::json!(v);
                }
                _ => break,
            }
        }

        let header_b64 = b64.encode(header.to_string().as_bytes());
        let payload_b64 = b64.encode(payload.to_string().as_bytes());
        let sig_b64 = b64.encode(b"fakecloud-stub-signature");
        let token = format!("{header_b64}.{payload_b64}.{sig_b64}");

        let xml = xml_responses::get_web_identity_token_response(
            &token,
            &format_expiration(expiration_at),
            &req.request_id,
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }
}
