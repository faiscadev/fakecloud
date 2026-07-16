//! `StsService` `assume` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl StsService {
    pub(super) fn assume_role(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let role_arn = req.query_params.get("RoleArn").ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MissingParameter",
                "The request must contain the parameter RoleArn",
            )
        })?;
        sts_validate_string_length("roleArn", role_arn, 20, 2048)?;

        let role_session_name = req.query_params.get("RoleSessionName").ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MissingParameter",
                "The request must contain the parameter RoleSessionName",
            )
        })?;
        sts_validate_string_length("roleSessionName", role_session_name, 2, 64)?;
        validate_session_name(role_session_name)?;

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
            sts_validate_range_i64("durationSeconds", v, 900, 43200)?;
        }

        // Validate optional ExternalId
        validate_optional_string_length(
            "externalId",
            req.query_params.get("ExternalId").map(|s| s.as_str()),
            2,
            1224,
        )?;

        // Validate optional Policy
        validate_optional_string_length(
            "policy",
            req.query_params.get("Policy").map(|s| s.as_str()),
            1,
            2048,
        )?;

        // Validate optional SourceIdentity
        validate_optional_string_length(
            "sourceIdentity",
            req.query_params.get("SourceIdentity").map(|s| s.as_str()),
            2,
            64,
        )?;

        // Validate and accept optional MFA SerialNumber
        validate_optional_string_length(
            "serialNumber",
            req.query_params.get("SerialNumber").map(|s| s.as_str()),
            9,
            256,
        )?;
        let serial_number = req.query_params.get("SerialNumber").cloned();

        // Validate and accept optional MFA TokenCode
        validate_optional_string_length(
            "tokenCode",
            req.query_params.get("TokenCode").map(|s| s.as_str()),
            6,
            6,
        )?;
        let token_code = req.query_params.get("TokenCode").cloned();

        // Compute expiration from DurationSeconds (default 3600s)
        let expiration_at = compute_expiration_at(req, DEFAULT_ASSUME_ROLE_DURATION)?;
        let expiration = format_expiration(expiration_at);

        // Accept MFA parameters without verification (emulator behavior)
        let _mfa_serial = serial_number;
        let _mfa_token = token_code;

        let partition = partition_for_region(&req.region);
        let creds = StsCredentials::generate();

        let mut accounts = self.state.write();

        // Resolve session policies from the caller's account
        let caller_state = accounts.get_or_create(&req.account_id);
        let session_policies = collect_session_policies(req, caller_state);

        // Extract account ID from role ARN if present, otherwise use caller's account
        let account_id =
            extract_account_from_arn(role_arn).unwrap_or_else(|| req.account_id.clone());

        let role_name = role_arn.rsplit('/').next().unwrap_or("unknown");
        // Look up the role WITHOUT creating the target account. An
        // attacker-controlled RoleArn naming a non-existent account would
        // otherwise insert an empty account that the next mutation persists
        // (unbounded map growth + resolver-scan cost). The role-existence
        // check below denies before any account is materialized.
        let role = accounts
            .get(&account_id)
            .and_then(|s| s.roles.get(role_name).cloned());

        // Enforce the role's trust policy through the IAM evaluator.
        // The trust policy is a resource-style policy whose Principal
        // gate names which callers may assume the role; AWS evaluates
        // it (and only it) when deciding `sts:AssumeRole`. Identity
        // policies do NOT factor into trust-policy evaluation.
        //
        // Context keys populated for trust evaluation match what AWS
        // exposes at AssumeRole time: sts:ExternalId,
        // sts:RoleSessionName, aws:MultiFactorAuthPresent, plus the
        // standard caller-identity keys.
        let role_id;
        if let Some(role) = role {
            // Service-linked roles (`/aws-service-role/<service>/...`)
            // are only assumable by the matching service principal,
            // never by users or other roles. AWS rejects every other
            // caller with AccessDenied and the trust policy is
            // synthesized to allow only the named service host.
            if role.path.starts_with("/aws-service-role/") {
                let expected_service = role
                    .path
                    .trim_start_matches("/aws-service-role/")
                    .trim_end_matches('/');
                let caller_is_service = req
                    .principal
                    .as_ref()
                    .map(|p| p.arn.contains(expected_service))
                    .unwrap_or(false);
                if !caller_is_service {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::FORBIDDEN,
                        "AccessDenied",
                        format!(
                            "User: {} is not authorized to perform: sts:AssumeRole on resource: {} because the role is a service-linked role for {}",
                            req.account_id, role_arn, expected_service
                        ),
                    ));
                }
            }

            let trust_doc = PolicyDocument::parse(&role.assume_role_policy_document);
            let caller_principal = match req.principal.as_ref() {
                Some(p) => p.clone(),
                None => Principal {
                    arn: Arn::global("iam", &req.account_id, "root").to_string(),
                    user_id: req.account_id.clone(),
                    account_id: req.account_id.clone(),
                    principal_type: PrincipalType::Root,
                    source_identity: None,
                    tags: None,
                },
            };

            let mfa_present = req.query_params.contains_key("SerialNumber")
                && req.query_params.contains_key("TokenCode");
            let mut context = RequestContext {
                aws_principal_arn: Some(caller_principal.arn.clone()),
                aws_principal_account: Some(caller_principal.account_id.clone()),
                aws_principal_type: Some(caller_principal.principal_type.as_str().to_string()),
                aws_mfa_present: Some(mfa_present),
                ..Default::default()
            };
            if let Some(eid) = req.query_params.get("ExternalId") {
                context
                    .service_keys
                    .insert("sts:externalid".to_string(), vec![eid.clone()]);
            }
            context.service_keys.insert(
                "sts:rolesessionname".to_string(),
                vec![role_session_name.clone()],
            );
            if let Some(src) = req.query_params.get("SourceIdentity") {
                context
                    .service_keys
                    .insert("sts:sourceidentity".to_string(), vec![src.clone()]);
            }
            // `aws:SourceAccount` — the calling account (cross-account
            // confused-deputy guard). Trust policies on third-party-
            // hosted roles commonly gate on this so a service running
            // in a tenant account can't impersonate the integration
            // owner.
            context.service_keys.insert(
                "aws:sourceaccount".to_string(),
                vec![caller_principal.account_id.clone()],
            );

            let eval_req = EvalRequest {
                principal: &caller_principal,
                action: "sts:AssumeRole".to_string(),
                resource: role_arn.clone(),
                context,
            };
            match evaluate_resource_policy_only(&trust_doc, &eval_req) {
                Decision::Allow => {}
                _ => {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::FORBIDDEN,
                        "AccessDenied",
                        format!(
                            "User: {} is not authorized to perform: sts:AssumeRole on resource: {}",
                            caller_principal.arn, role_arn
                        ),
                    ));
                }
            }

            // Enforce the role's MaxSessionDuration: AWS rejects a
            // DurationSeconds larger than the cap stored on the role,
            // rather than silently honoring the generic 900..43200 range.
            if let Some(ds) = req.query_params.get("DurationSeconds") {
                if let Ok(v) = ds.parse::<i64>() {
                    if v > role.max_session_duration as i64 {
                        return Err(AwsServiceError::aws_error(
                            StatusCode::BAD_REQUEST,
                            "ValidationError",
                            format!(
                                "The requested DurationSeconds exceeds the MaxSessionDuration set for this role. \
                                 The MaxSessionDuration for the role is {} seconds.",
                                role.max_session_duration
                            ),
                        ));
                    }
                }
            }

            role_id = role.role_id.clone();
        } else {
            // AssumeRole against a role that does not exist must be denied
            // rather than fall through to credential minting with no trust
            // check (bug-audit 2026-05-28, 5.4). AWS returns AccessDenied for
            // sts:AssumeRole on a role it cannot resolve.
            let caller_arn = req
                .principal
                .as_ref()
                .map(|p| p.arn.clone())
                .unwrap_or_else(|| Arn::global("iam", &req.account_id, "root").to_string());
            return Err(AwsServiceError::aws_error(
                StatusCode::FORBIDDEN,
                "AccessDenied",
                format!(
                    "User: {caller_arn} is not authorized to perform: sts:AssumeRole on resource: {role_arn}"
                ),
            ));
        }

        let assumed_role_arn = format!(
            "arn:{}:sts::{}:assumed-role/{}/{}",
            partition, account_id, role_name, role_session_name
        );
        let assumed_role_id = format!("{}:{}", role_id, role_session_name);

        // Store credential in the target account's state so the credential
        // resolver finds it when the caller uses these temporary credentials.
        let target_state = accounts.get_or_create(&account_id);
        target_state.credential_identities.insert(
            creds.access_key_id.clone(),
            CredentialIdentity {
                arn: assumed_role_arn.clone(),
                user_id: assumed_role_id.clone(),
                account_id: account_id.clone(),
            },
        );
        let mfa_present_for_session = req.query_params.contains_key("SerialNumber")
            && req.query_params.contains_key("TokenCode");
        target_state.sts_temp_credentials.insert(
            creds.access_key_id.clone(),
            StsTempCredential {
                access_key_id: creds.access_key_id.clone(),
                secret_access_key: creds.secret_access_key.clone(),
                session_token: creds.session_token.clone(),
                principal_arn: assumed_role_arn,
                user_id: assumed_role_id,
                account_id: account_id.clone(),
                expiration: expiration_at,
                session_policies,
                mfa_present: mfa_present_for_session,
                issued_at: Utc::now(),
                // Plain AssumeRole does not federate — `aws:FederatedProvider`
                // stays absent for the resulting session.
                federated_provider: None,
            },
        );

        let xml = xml_responses::assume_role_response(&xml_responses::AssumedRoleInfo {
            role_arn,
            role_session_name,
            assumed_role_id: &role_id,
            account_id: &account_id,
            partition,
            creds: &creds,
            expiration: &expiration,
            request_id: &req.request_id,
        });
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    pub(super) fn assume_role_with_web_identity(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let role_arn = req.query_params.get("RoleArn").ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MissingParameter",
                "The request must contain the parameter RoleArn",
            )
        })?;
        sts_validate_string_length("roleArn", role_arn, 20, 2048)?;

        let role_session_name = req.query_params.get("RoleSessionName").ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MissingParameter",
                "The request must contain the parameter RoleSessionName",
            )
        })?;
        sts_validate_string_length("roleSessionName", role_session_name, 2, 64)?;
        validate_session_name(role_session_name)?;

        // WebIdentityToken is required
        let web_identity_token = req.query_params.get("WebIdentityToken").ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MissingParameter",
                "The request must contain the parameter WebIdentityToken",
            )
        })?;
        sts_validate_string_length("webIdentityToken", web_identity_token, 4, 20000)?;
        let web_identity_token_owned = web_identity_token.clone();

        // Validate optional Policy
        validate_optional_string_length(
            "policy",
            req.query_params.get("Policy").map(|s| s.as_str()),
            1,
            2048,
        )?;

        // Validate optional ProviderId
        validate_optional_string_length(
            "providerId",
            req.query_params.get("ProviderId").map(|s| s.as_str()),
            4,
            2048,
        )?;

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
            sts_validate_range_i64("durationSeconds", v, 900, 43200)?;
        }

        // Compute expiration from DurationSeconds (default 3600s)
        let expiration_at = compute_expiration_at(req, DEFAULT_ASSUME_ROLE_DURATION)?;
        let expiration = format_expiration(expiration_at);

        let partition = partition_for_region(&req.region);
        let creds = StsCredentials::generate();

        let mut accounts = self.state.write();
        let caller_state = accounts.get_or_create(&req.account_id);
        let session_policies = collect_session_policies(req, caller_state);
        let account_id =
            extract_account_from_arn(role_arn).unwrap_or_else(|| req.account_id.clone());

        let role_name = role_arn.rsplit('/').next().unwrap_or("unknown");
        // Use the role's stable RoleId as the AssumedRoleId prefix (matching
        // AWS), not a fresh random AROA on every assume. Fall back to a
        // generated id only when the role isn't resolvable.
        let role_id = accounts
            .get(&account_id)
            .and_then(|s| s.roles.get(role_name).map(|r| r.role_id.clone()))
            .unwrap_or_else(xml_responses::generate_role_id);
        let assumed_role_arn = format!(
            "arn:{}:sts::{}:assumed-role/{}/{}",
            partition, account_id, role_name, role_session_name
        );
        let assumed_role_id_str = format!("{}:{}", role_id, role_session_name);

        // Decode the JWT for trust-policy enforcement and to figure out
        // which OIDC provider vouched for the assertion. We never verify
        // signatures — fakecloud is not a security boundary — but we
        // require enough structure to extract `iss` and `aud` so the
        // trust policy gate can fire on real AWS-shaped policies.
        let jwt = decode_jwt(&web_identity_token_owned);

        // Reject an expired token. Real OIDC tokens always carry `exp`; AWS
        // returns ExpiredTokenException. We don't verify the signature (not a
        // security boundary), but `exp` enforcement is non-crypto and callers
        // verifying their security posture expect expired tokens to be rejected
        // (bug-audit 2026-06-20, 5.1).
        if let Some(ref claims) = jwt {
            if let Some(exp) = claims
                .raw
                .get("exp")
                .and_then(|v| v.as_i64().or_else(|| v.as_f64().map(|f| f as i64)))
            {
                if exp < chrono::Utc::now().timestamp() {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "ExpiredTokenException",
                        "The web identity token that was passed is expired.",
                    ));
                }
            }
        }

        // Pick the federated provider ARN. Preference order:
        //   1. JWT iss matched against a registered OpenIDConnectProvider —
        //      we use the provider's stored ARN.
        //   2. Caller-supplied ProviderId param (legacy IdP host name).
        //   3. Synthetic placeholder so policies that just check for "any
        //      federated session" still bind. This branch only fires for
        //      tokens that aren't real JWTs — real OIDC clients hit (1).
        let provider_id_param = req.query_params.get("ProviderId").cloned();
        let oidc_match = jwt.as_ref().and_then(|c| c.iss.as_deref()).and_then(|iss| {
            find_oidc_provider(&accounts, iss).map(|(_, p)| (iss.to_string(), p.clone()))
        });

        // If we have a JWT with an `iss` claim, the issuer MUST resolve
        // to a registered OIDC provider — anything else is a federation
        // misconfiguration and AWS rejects with InvalidIdentityToken.
        if let Some(ref claims) = jwt {
            if let Some(ref iss) = claims.iss {
                if oidc_match.is_none() {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidIdentityToken",
                        format!("No OpenIDConnect provider found in your account for issuer {iss}"),
                    ));
                }
                // Audience must overlap with the provider's
                // client_id_list when the provider has any client IDs
                // configured. Empty list means "accept any aud"
                // (matches AWS for legacy/uninitialized providers).
                // Tokens carry every audience claim the IdP issued the
                // assertion to (RFC 7519 array form), so any one
                // matching client_id_list entry is enough.
                if let Some((ref _iss, ref provider)) = oidc_match {
                    if !provider.client_id_list.is_empty() {
                        let any_match = claims
                            .aud
                            .iter()
                            .any(|aud| provider.client_id_list.iter().any(|c| c == aud));
                        if !any_match {
                            return Err(AwsServiceError::aws_error(
                                StatusCode::BAD_REQUEST,
                                "InvalidIdentityToken",
                                format!(
                                    "Incorrect token audience: not in client_id_list for provider {}",
                                    provider.arn
                                ),
                            ));
                        }
                    }
                }
            }
        }

        let federated_provider = oidc_match
            .as_ref()
            .map(|(_iss, p)| p.arn.clone())
            .or(provider_id_param.clone())
            .unwrap_or_else(|| format!("arn:aws:iam::{}:oidc-provider/web-identity", account_id));

        // Trust-policy gate: same shape as AssumeRole, but the caller
        // principal is the federated provider and the action is
        // `sts:AssumeRoleWithWebIdentity`. Service-linked roles are
        // never assumable via web identity, so we don't replicate the
        // SLR shortcut from `assume_role`.
        let target_state = accounts.get_or_create(&account_id);
        if let Some(role) = target_state.roles.get(role_name).cloned() {
            let trust_doc = PolicyDocument::parse(&role.assume_role_policy_document);
            let caller_principal = federated_principal(&federated_provider, &account_id);
            let mut context = RequestContext {
                aws_principal_arn: Some(caller_principal.arn.clone()),
                aws_principal_account: Some(caller_principal.account_id.clone()),
                aws_principal_type: Some(caller_principal.principal_type.as_str().to_string()),
                aws_federated_provider: Some(federated_provider.clone()),
                ..Default::default()
            };
            context.service_keys.insert(
                "sts:rolesessionname".to_string(),
                vec![role_session_name.clone()],
            );
            // Per-provider `<provider>:aud`/`<provider>:sub` keys —
            // AWS exposes these scoped to the issuer host so policies
            // can write `accounts.google.com:aud`, `cognito-identity.amazonaws.com:sub`,
            // etc. We key off the registered provider URL (no scheme)
            // when we matched one, otherwise the caller-supplied
            // ProviderId.
            let key_prefix = oidc_match
                .as_ref()
                .map(|(_iss, p)| normalize_issuer(&p.url))
                .or_else(|| provider_id_param.as_deref().map(normalize_issuer));
            if let Some(prefix) = key_prefix {
                if let Some(ref claims) = jwt {
                    if !claims.aud.is_empty() {
                        // `aud` is multi-valued (RFC 7519); surface
                        // every audience so a `StringEquals` /
                        // `ForAnyValue:StringEquals` condition matches
                        // whichever entry the policy names.
                        context
                            .service_keys
                            .insert(format!("{prefix}:aud"), claims.aud.clone());
                    }
                    if let Some(ref sub) = claims.sub {
                        context
                            .service_keys
                            .insert(format!("{prefix}:sub"), vec![sub.clone()]);
                        context.aws_userid = Some(sub.clone());
                    }
                    if let Some(amr) = claims.raw.get("amr").and_then(|v| v.as_array()).map(|arr| {
                        arr.iter()
                            .filter_map(|v| v.as_str().map(|s| s.to_string()))
                            .collect::<Vec<_>>()
                    }) {
                        context.service_keys.insert(format!("{prefix}:amr"), amr);
                    }
                }
            }
            let eval_req = EvalRequest {
                principal: &caller_principal,
                action: "sts:AssumeRoleWithWebIdentity".to_string(),
                resource: role_arn.clone(),
                context,
            };
            if !matches!(
                evaluate_resource_policy_only(&trust_doc, &eval_req),
                Decision::Allow
            ) {
                return Err(trust_policy_denied(
                    "sts:AssumeRoleWithWebIdentity",
                    &caller_principal.arn,
                    role_arn,
                ));
            }
        } else {
            // AssumeRoleWithWebIdentity against a role that does not exist
            // must be denied rather than fall through to credential
            // minting with no trust check (same gap that bug-audit
            // 2026-05-28 §5.4 fixed for plain AssumeRole). AWS returns
            // AccessDenied for sts:AssumeRoleWithWebIdentity on a role it
            // cannot resolve.
            let caller_principal = federated_principal(&federated_provider, &account_id);
            return Err(trust_policy_denied(
                "sts:AssumeRoleWithWebIdentity",
                &caller_principal.arn,
                role_arn,
            ));
        }

        let target_state = accounts.get_or_create(&account_id);
        target_state.credential_identities.insert(
            creds.access_key_id.clone(),
            CredentialIdentity {
                arn: assumed_role_arn.clone(),
                user_id: assumed_role_id_str.clone(),
                account_id: account_id.clone(),
            },
        );
        // `aws:FederatedProvider` is the OIDC provider ARN (preferred)
        // or the caller-supplied ProviderId (legacy idp host name).
        // Falls back to a synthetic ARN so policies that simply check
        // for "any federated session" still have a value to bind to.
        let federated_provider = Some(federated_provider);
        target_state.sts_temp_credentials.insert(
            creds.access_key_id.clone(),
            StsTempCredential {
                access_key_id: creds.access_key_id.clone(),
                secret_access_key: creds.secret_access_key.clone(),
                session_token: creds.session_token.clone(),
                principal_arn: assumed_role_arn,
                user_id: assumed_role_id_str,
                account_id: account_id.clone(),
                expiration: expiration_at,
                session_policies,
                mfa_present: false,
                issued_at: Utc::now(),
                federated_provider,
            },
        );

        let xml = xml_responses::assume_role_with_web_identity_response(
            &xml_responses::AssumedRoleInfo {
                role_arn,
                role_session_name,
                assumed_role_id: &role_id,
                account_id: &account_id,
                partition,
                creds: &creds,
                expiration: &expiration,
                request_id: &req.request_id,
            },
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    pub(super) fn assume_role_with_saml(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let role_arn = req.query_params.get("RoleArn").ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MissingParameter",
                "The request must contain the parameter RoleArn",
            )
        })?;
        sts_validate_string_length("roleArn", role_arn, 20, 2048)?;

        // PrincipalArn is required
        let principal_arn = req.query_params.get("PrincipalArn").ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MissingParameter",
                "The request must contain the parameter PrincipalArn",
            )
        })?;
        sts_validate_string_length("principalArn", principal_arn, 20, 2048)?;
        // Snapshot the SAML provider ARN so we can stash it on the
        // session as `aws:FederatedProvider` after this scope ends.
        let saml_provider_arn = principal_arn.clone();

        // SAMLAssertion is required but we just need to extract session name from it
        let saml_assertion = req.query_params.get("SAMLAssertion").ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MissingParameter",
                "The request must contain the parameter SAMLAssertion",
            )
        })?;
        sts_validate_string_length("sAMLAssertion", saml_assertion, 4, 100000)?;

        // Validate optional Policy
        validate_optional_string_length(
            "policy",
            req.query_params.get("Policy").map(|s| s.as_str()),
            1,
            2048,
        )?;

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
            sts_validate_range_i64("durationSeconds", v, 900, 43200)?;
        }

        // Compute expiration from DurationSeconds (default 3600s)
        let expiration_at = compute_expiration_at(req, DEFAULT_ASSUME_ROLE_DURATION)?;
        let expiration = format_expiration(expiration_at);

        // Decode the SAML assertion to extract the RoleSessionName plus
        // the issuer/audience claims used for trust-policy enforcement.
        let role_session_name =
            extract_saml_session_name(saml_assertion).unwrap_or_else(|| "saml-session".to_string());
        // The SAML-derived session name is attacker-influenced; reject any that
        // breaks the AWS pattern rather than injecting it raw into the response XML.
        validate_session_name(&role_session_name)?;
        let saml_claims = extract_saml_claims(saml_assertion);

        let partition = partition_for_region(&req.region);
        let creds = StsCredentials::generate();

        let mut accounts = self.state.write();
        let caller_state = accounts.get_or_create(&req.account_id);
        let session_policies = collect_session_policies(req, caller_state);
        let account_id =
            extract_account_from_arn(role_arn).unwrap_or_else(|| req.account_id.clone());

        let role_name = role_arn.rsplit('/').next().unwrap_or("unknown");
        // Use the role's stable RoleId as the AssumedRoleId prefix (matching
        // AWS), not a fresh random AROA on every assume.
        let role_id = accounts
            .get(&account_id)
            .and_then(|s| s.roles.get(role_name).map(|r| r.role_id.clone()))
            .unwrap_or_else(xml_responses::generate_role_id);
        let assumed_role_arn = format!(
            "arn:{}:sts::{}:assumed-role/{}/{}",
            partition, account_id, role_name, role_session_name
        );
        let assumed_role_id_str = format!("{}:{}", role_id, role_session_name);

        // If the named SAML provider IS registered, enforce its
        // metadata-derived audience against the assertion's
        // `<Audience>` claim. Unregistered providers fall through —
        // tests still in the pre-F1 era (and AWS itself, when the
        // provider was nuked between assertion issue and use) get a
        // soft pass; the trust policy below still gates the call.
        if let Some(provider) = find_saml_provider(&accounts, &saml_provider_arn) {
            if let Some(expected_aud) = expected_saml_audience(&provider.saml_metadata_document) {
                if let Some(ref got) = saml_claims.audience {
                    if got != &expected_aud {
                        return Err(AwsServiceError::aws_error(
                            StatusCode::BAD_REQUEST,
                            "InvalidIdentityToken",
                            format!(
                                "SAML assertion audience '{got}' does not match SAML provider '{}'",
                                provider.arn
                            ),
                        ));
                    }
                }
            }
        }

        // Trust-policy gate: caller principal is the SAML provider,
        // action is `sts:AssumeRoleWithSAML`. Trust policies typically
        // gate on `saml:aud` / `saml:iss` plus `aws:FederatedProvider`.
        let target_state = accounts.get_or_create(&account_id);
        if let Some(role) = target_state.roles.get(role_name).cloned() {
            let trust_doc = PolicyDocument::parse(&role.assume_role_policy_document);
            let caller_principal = federated_principal(&saml_provider_arn, &account_id);
            let mut context = RequestContext {
                aws_principal_arn: Some(caller_principal.arn.clone()),
                aws_principal_account: Some(caller_principal.account_id.clone()),
                aws_principal_type: Some(caller_principal.principal_type.as_str().to_string()),
                aws_federated_provider: Some(saml_provider_arn.clone()),
                ..Default::default()
            };
            if let Some(ref aud) = saml_claims.audience {
                context
                    .service_keys
                    .insert("saml:aud".to_string(), vec![aud.clone()]);
            }
            if let Some(ref iss) = saml_claims.issuer {
                context
                    .service_keys
                    .insert("saml:iss".to_string(), vec![iss.clone()]);
            }
            context.service_keys.insert(
                "sts:rolesessionname".to_string(),
                vec![role_session_name.clone()],
            );
            let eval_req = EvalRequest {
                principal: &caller_principal,
                action: "sts:AssumeRoleWithSAML".to_string(),
                resource: role_arn.clone(),
                context,
            };
            if !matches!(
                evaluate_resource_policy_only(&trust_doc, &eval_req),
                Decision::Allow
            ) {
                return Err(trust_policy_denied(
                    "sts:AssumeRoleWithSAML",
                    &caller_principal.arn,
                    role_arn,
                ));
            }
        } else {
            // AssumeRoleWithSAML against a role that does not exist must be
            // denied rather than fall through to credential minting with no
            // trust check (same gap that bug-audit 2026-05-28 §5.4 fixed
            // for plain AssumeRole). AWS returns AccessDenied for
            // sts:AssumeRoleWithSAML on a role it cannot resolve.
            let caller_principal = federated_principal(&saml_provider_arn, &account_id);
            return Err(trust_policy_denied(
                "sts:AssumeRoleWithSAML",
                &caller_principal.arn,
                role_arn,
            ));
        }

        let target_state = accounts.get_or_create(&account_id);
        target_state.credential_identities.insert(
            creds.access_key_id.clone(),
            CredentialIdentity {
                arn: assumed_role_arn.clone(),
                user_id: assumed_role_id_str.clone(),
                account_id: account_id.clone(),
            },
        );
        // SAML federation: the PrincipalArn parameter carries the SAML
        // provider ARN that vouched for the assertion, and AWS surfaces
        // exactly that ARN as `aws:FederatedProvider` for the session.
        let federated_provider = Some(saml_provider_arn);
        target_state.sts_temp_credentials.insert(
            creds.access_key_id.clone(),
            StsTempCredential {
                access_key_id: creds.access_key_id.clone(),
                secret_access_key: creds.secret_access_key.clone(),
                session_token: creds.session_token.clone(),
                principal_arn: assumed_role_arn,
                user_id: assumed_role_id_str,
                account_id: account_id.clone(),
                expiration: expiration_at,
                session_policies,
                mfa_present: false,
                issued_at: Utc::now(),
                federated_provider,
            },
        );

        let xml = xml_responses::assume_role_with_saml_response(&xml_responses::AssumedRoleInfo {
            role_arn,
            role_session_name: &role_session_name,
            assumed_role_id: &role_id,
            account_id: &account_id,
            partition,
            creds: &creds,
            expiration: &expiration,
            request_id: &req.request_id,
        });
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    /// AssumeRoot: returns short-lived privileged credentials scoped to a
    /// member-account root principal. Caller must be from the management
    /// account; we mint and persist credentials so subsequent calls under
    /// them resolve to the target account's root identity.
    pub(super) fn assume_root(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        // `AssumeRoot` declares only `ExpiredTokenException` and
        // `RegionDisabledException` — no input-validation shape. Route
        // Smithy-modeled @required / @length / @range violations through
        // `ExpiredTokenException` so strict-conformance probes see a
        // declared 4xx instead of an undeclared `ValidationError` or a
        // surprise 200.
        let default_account = {
            let accounts = self.state.read();
            accounts.default_account_id().to_string()
        };

        let target_principal = req
            .query_params
            .get("TargetPrincipal")
            .cloned()
            .ok_or_else(|| sts_assume_root_error("TargetPrincipal is required"))?;
        // `TargetPrincipalType` is @length 12..=2048.
        if target_principal.len() < 12 || target_principal.len() > 2048 {
            return Err(sts_assume_root_error(
                "TargetPrincipal must be 12..=2048 characters",
            ));
        }

        // `TaskPolicyArn` is @required (a structure with an `arn` member).
        // awsQuery flattens it to `TaskPolicyArn.arn`.
        if !req.query_params.contains_key("TaskPolicyArn.arn")
            && !req.query_params.contains_key("TaskPolicyArn")
        {
            return Err(sts_assume_root_error("TaskPolicyArn is required"));
        }
        let _task_policy_arn = req
            .query_params
            .get("TaskPolicyArn.arn")
            .or_else(|| req.query_params.get("TaskPolicyArn"))
            .cloned()
            .unwrap_or_else(|| "arn:aws:iam::aws:policy/IAMAuditRootUserCredentials".to_string());

        // `RootDurationSecondsType` is @range 0..=900. Reject negative or
        // > 900 inputs up front rather than clamping silently.
        let duration_seconds = match req.query_params.get("DurationSeconds") {
            Some(raw) => match raw.parse::<i64>() {
                Ok(v) if (0..=900).contains(&v) => v,
                _ => {
                    return Err(sts_assume_root_error(
                        "DurationSeconds must be between 0 and 900",
                    ))
                }
            },
            None => 900,
        };

        // Target principal accepted shapes:
        //   - ARN: extract the account id from positions 4 (`arn:p:s:r:acct:`)
        //   - 12-digit account id: assume root ARN
        //   - Anything else: treat the string as the principal id and
        //     attribute it to the caller's account.
        let partition = partition_for_region(&req.region);
        let (target_account, target_arn) = if target_principal.starts_with("arn:") {
            let acct = extract_account_from_arn(&target_principal)
                .unwrap_or_else(|| default_account.clone());
            (acct, target_principal.clone())
        } else if target_principal.len() == 12
            && target_principal.chars().all(|c| c.is_ascii_digit())
        {
            (
                target_principal.clone(),
                format!("arn:{}:iam::{}:root", partition, target_principal),
            )
        } else {
            (
                default_account.clone(),
                format!("arn:{}:iam::{}:root", partition, default_account),
            )
        };

        // Centralized root access ("RootSessions") gates AssumeRoot into OTHER
        // accounts. Without it, a single `sts:AssumeRoot` grant would escalate
        // to root over every member account with no organization trust
        // (bug-hunt 2026-06-24, 5.1). Same-account AssumeRoot (the member root
        // managing itself) is always allowed and matches the recorded AWS
        // baseline; only cross-account targets require the feature enabled.
        if target_account != req.account_id {
            let enabled = {
                let accounts = self.state.read();
                accounts
                    .get(&req.account_id)
                    .map(|s| s.organizations_root_sessions)
                    .unwrap_or(false)
            };
            if !enabled {
                return Err(AwsServiceError::aws_error(
                    StatusCode::FORBIDDEN,
                    "AccessDeniedException",
                    "AssumeRoot into another account requires centralized root access \
                     (RootSessions) to be enabled for the organization \
                     (EnableOrganizationsRootSessions).",
                ));
            }
            // The RootSessions flag alone is not sufficient: the target must be
            // a member of the caller's organization and the caller must be the
            // management account (or a delegated administrator for centralized
            // root access). Without this an account that merely enabled
            // RootSessions could mint :root credentials for ANY account, in or
            // out of its org (bug-hunt 5.2).
            let org_permits = self
                .org_membership
                .as_ref()
                .is_some_and(|r| r.can_assume_root_into(&req.account_id, &target_account));
            if !org_permits {
                return Err(AwsServiceError::aws_error(
                    StatusCode::FORBIDDEN,
                    "AccessDeniedException",
                    "AssumeRoot into another account is permitted only for the organization's \
                     management account (or a delegated administrator for centralized root \
                     access) targeting a member account of the same organization.",
                ));
            }
        }

        // Don't call `compute_expiration_at` — that helper re-parses
        // `DurationSeconds` and returns the undeclared `ValidationError`
        // on bad input. We already clamped above.
        let effective_duration = if duration_seconds == 0 {
            900
        } else {
            duration_seconds
        };
        let expiration_at = Utc::now() + chrono::Duration::seconds(effective_duration);
        let expiration = format_expiration(expiration_at);
        let creds = StsCredentials::generate();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&target_account);
        state.credential_identities.insert(
            creds.access_key_id.clone(),
            CredentialIdentity {
                arn: target_arn.clone(),
                user_id: target_account.clone(),
                account_id: target_account.clone(),
            },
        );
        state.sts_temp_credentials.insert(
            creds.access_key_id.clone(),
            StsTempCredential {
                access_key_id: creds.access_key_id.clone(),
                secret_access_key: creds.secret_access_key.clone(),
                session_token: creds.session_token.clone(),
                principal_arn: target_arn.clone(),
                user_id: target_account.clone(),
                account_id: target_account.clone(),
                expiration: expiration_at,
                session_policies: Vec::new(),
                mfa_present: false,
                issued_at: Utc::now(),
                federated_provider: None,
            },
        );

        let source_identity = req.query_params.get("SourceIdentity").map(|s| s.as_str());
        let xml = xml_responses::assume_root_response(
            &creds,
            &expiration,
            source_identity,
            &req.request_id,
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    pub(crate) fn get_delegated_access_token(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        // `TradeInToken` is @required. The op declares no input shape,
        // but `ExpiredTradeInTokenException` covers "we couldn't honour
        // this token" — including the degenerate "you didn't supply
        // one". Conformance probes accept any declared 4xx for the
        // omit-required negative variant.
        let _trade_in_token = req
            .query_params
            .get("TradeInToken")
            .cloned()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ExpiredTradeInTokenException",
                    "TradeInToken is required",
                )
            })?;

        let account_id = req
            .principal
            .as_ref()
            .map(|p| p.account_id.clone())
            .unwrap_or_else(|| {
                let accounts = self.state.read();
                accounts.default_account_id().to_string()
            });
        let assumed_principal = req
            .principal
            .as_ref()
            .map(|p| p.arn.clone())
            .unwrap_or_else(|| {
                let partition = partition_for_region(&req.region);
                format!("arn:{partition}:iam::{account_id}:root")
            });

        let expiration_at = Utc::now() + chrono::Duration::seconds(3600);
        let expiration = format_expiration(expiration_at);
        let creds = StsCredentials::generate();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&account_id);
        state.credential_identities.insert(
            creds.access_key_id.clone(),
            CredentialIdentity {
                arn: assumed_principal.clone(),
                user_id: account_id.clone(),
                account_id: account_id.clone(),
            },
        );
        state.sts_temp_credentials.insert(
            creds.access_key_id.clone(),
            StsTempCredential {
                access_key_id: creds.access_key_id.clone(),
                secret_access_key: creds.secret_access_key.clone(),
                session_token: creds.session_token.clone(),
                principal_arn: assumed_principal.clone(),
                user_id: account_id.clone(),
                account_id: account_id.clone(),
                expiration: expiration_at,
                session_policies: Vec::new(),
                mfa_present: false,
                issued_at: Utc::now(),
                federated_provider: None,
            },
        );

        let xml = xml_responses::get_delegated_access_token_response(
            &creds,
            &expiration,
            &assumed_principal,
            &req.request_id,
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }
}
