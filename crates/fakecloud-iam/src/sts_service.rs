use async_trait::async_trait;
use chrono::Utc;
use http::StatusCode;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_core::validation::*;

use crate::state::{CredentialIdentity, SharedIamState};
use crate::xml_responses::{self, StsCredentials};

/// Default duration for AssumeRole and similar operations (1 hour).
const DEFAULT_ASSUME_ROLE_DURATION: i64 = 3600;

/// Default duration for GetSessionToken (12 hours).
const DEFAULT_SESSION_TOKEN_DURATION: i64 = 43200;

/// Default duration for GetFederationToken (12 hours).
const DEFAULT_FEDERATION_TOKEN_DURATION: i64 = 43200;

/// Compute an ISO 8601 expiration timestamp from an optional DurationSeconds parameter.
fn compute_expiration(req: &AwsRequest, default_duration: i64) -> Result<String, AwsServiceError> {
    let duration = if let Some(ds) = req.query_params.get("DurationSeconds") {
        ds.parse::<i64>().map_err(|_| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationError",
                format!(
                    "Value '{}' at 'durationSeconds' failed to satisfy constraint: \
                     Member must be a valid integer",
                    ds
                ),
            )
        })?
    } else {
        default_duration
    };
    let expiration = Utc::now() + chrono::Duration::seconds(duration);
    Ok(expiration.format("%Y-%m-%dT%H:%M:%SZ").to_string())
}

pub struct StsService {
    state: SharedIamState,
}

impl StsService {
    pub fn new(state: SharedIamState) -> Self {
        Self { state }
    }
}

#[async_trait]
impl AwsService for StsService {
    fn service_name(&self) -> &str {
        "sts"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        match req.action.as_str() {
            "GetCallerIdentity" => self.get_caller_identity(&req),
            "AssumeRole" => self.assume_role(&req),
            "AssumeRoleWithWebIdentity" => self.assume_role_with_web_identity(&req),
            "AssumeRoleWithSAML" => self.assume_role_with_saml(&req),
            "GetSessionToken" => self.get_session_token(&req),
            "GetFederationToken" => self.get_federation_token(&req),
            "GetAccessKeyInfo" => self.get_access_key_info(&req),
            "DecodeAuthorizationMessage" => self.decode_authorization_message(&req),
            _ => Err(AwsServiceError::action_not_implemented("sts", &req.action)),
        }
    }

    fn supported_actions(&self) -> &[&str] {
        &[
            "GetCallerIdentity",
            "AssumeRole",
            "AssumeRoleWithWebIdentity",
            "AssumeRoleWithSAML",
            "GetSessionToken",
            "GetFederationToken",
            "GetAccessKeyInfo",
            "DecodeAuthorizationMessage",
        ]
    }
}

/// Get the AWS partition from a region string.
fn partition_for_region(region: &str) -> &str {
    if region.starts_with("cn-") {
        "aws-cn"
    } else if region.starts_with("us-iso-") {
        "aws-iso"
    } else if region.starts_with("us-isob-") {
        "aws-iso-b"
    } else if region.starts_with("us-isof-") {
        "aws-iso-f"
    } else if region.starts_with("eu-isoe-") {
        "aws-iso-e"
    } else {
        "aws"
    }
}

/// Extract the caller's access key from the SigV4 Authorization header.
fn extract_access_key(req: &AwsRequest) -> Option<String> {
    let auth = req.headers.get("authorization")?.to_str().ok()?;
    let info = fakecloud_aws::sigv4::parse_sigv4(auth)?;
    Some(info.access_key)
}

impl StsService {
    fn get_caller_identity(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let partition = partition_for_region(&req.region);

        // Check if caller has credentials that map to a known identity
        if let Some(access_key) = extract_access_key(req) {
            // First check credential_identities (assumed roles, etc.)
            if let Some(identity) = state.credential_identities.get(&access_key) {
                let xml = xml_responses::get_caller_identity_response(
                    &identity.account_id,
                    &identity.arn,
                    &identity.user_id,
                    &req.request_id,
                );
                return Ok(AwsResponse::xml(StatusCode::OK, xml));
            }

            // Then check IAM user access keys
            for keys in state.access_keys.values() {
                for key in keys {
                    if key.access_key_id == access_key {
                        if let Some(user) = state.users.get(&key.user_name) {
                            let xml = xml_responses::get_caller_identity_response(
                                &state.account_id,
                                &user.arn,
                                &user.user_id,
                                &req.request_id,
                            );
                            return Ok(AwsResponse::xml(StatusCode::OK, xml));
                        }
                    }
                }
            }
        }

        // Default identity — matches real AWS root credentials
        let arn = format!("arn:{}:iam::{}:root", partition, state.account_id);
        let user_id = "FKIAIOSFODNN7EXAMPLE";
        let xml = xml_responses::get_caller_identity_response(
            &state.account_id,
            &arn,
            user_id,
            &req.request_id,
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn assume_role(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let role_arn = req.query_params.get("RoleArn").ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MissingParameter",
                "The request must contain the parameter RoleArn",
            )
        })?;
        validate_string_length("roleArn", role_arn, 20, 2048)?;

        let role_session_name = req.query_params.get("RoleSessionName").ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MissingParameter",
                "The request must contain the parameter RoleSessionName",
            )
        })?;
        validate_string_length("roleSessionName", role_session_name, 2, 64)?;

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
            validate_range_i64("durationSeconds", v, 900, 43200)?;
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
        let expiration = compute_expiration(req, DEFAULT_ASSUME_ROLE_DURATION)?;

        // Accept MFA parameters without verification (emulator behavior)
        let _mfa_serial = serial_number;
        let _mfa_token = token_code;

        let partition = partition_for_region(&req.region);
        let creds = StsCredentials::generate();

        let mut state = self.state.write();

        // Extract account ID from role ARN if present, otherwise use default
        let account_id =
            extract_account_from_arn(role_arn).unwrap_or_else(|| state.account_id.clone());

        // Try to find the role in state to get its role_id
        let role_name = role_arn.rsplit('/').next().unwrap_or("unknown");
        let role_id = state
            .roles
            .get(role_name)
            .map(|r| r.role_id.clone())
            .unwrap_or_else(xml_responses::generate_role_id);

        let assumed_role_arn = format!(
            "arn:{}:sts::{}:assumed-role/{}/{}",
            partition, account_id, role_name, role_session_name
        );
        let assumed_role_id = format!("{}:{}", role_id, role_session_name);

        // Store credential identity for GetCallerIdentity lookups
        state.credential_identities.insert(
            creds.access_key_id.clone(),
            CredentialIdentity {
                arn: assumed_role_arn,
                user_id: assumed_role_id,
                account_id: account_id.clone(),
            },
        );

        let xml = xml_responses::assume_role_response(
            role_arn,
            role_session_name,
            &role_id,
            &account_id,
            partition,
            &creds,
            &expiration,
            &req.request_id,
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn assume_role_with_web_identity(
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
        validate_string_length("roleArn", role_arn, 20, 2048)?;

        let role_session_name = req.query_params.get("RoleSessionName").ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MissingParameter",
                "The request must contain the parameter RoleSessionName",
            )
        })?;
        validate_string_length("roleSessionName", role_session_name, 2, 64)?;

        // WebIdentityToken is required
        let web_identity_token = req.query_params.get("WebIdentityToken").ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MissingParameter",
                "The request must contain the parameter WebIdentityToken",
            )
        })?;
        validate_string_length("webIdentityToken", web_identity_token, 4, 20000)?;
        let _web_identity_token = web_identity_token.clone();

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
            validate_range_i64("durationSeconds", v, 900, 43200)?;
        }

        // Compute expiration from DurationSeconds (default 3600s)
        let expiration = compute_expiration(req, DEFAULT_ASSUME_ROLE_DURATION)?;

        let partition = partition_for_region(&req.region);
        let creds = StsCredentials::generate();
        let role_id = xml_responses::generate_role_id();

        let mut state = self.state.write();
        let account_id =
            extract_account_from_arn(role_arn).unwrap_or_else(|| state.account_id.clone());

        let role_name = role_arn.rsplit('/').next().unwrap_or("unknown");
        let assumed_role_arn = format!(
            "arn:{}:sts::{}:assumed-role/{}/{}",
            partition, account_id, role_name, role_session_name
        );
        let assumed_role_id_str = format!("{}:{}", role_id, role_session_name);

        state.credential_identities.insert(
            creds.access_key_id.clone(),
            CredentialIdentity {
                arn: assumed_role_arn,
                user_id: assumed_role_id_str,
                account_id: account_id.clone(),
            },
        );

        let xml = xml_responses::assume_role_with_web_identity_response(
            role_arn,
            role_session_name,
            &account_id,
            partition,
            &creds,
            &role_id,
            &expiration,
            &req.request_id,
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn assume_role_with_saml(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let role_arn = req.query_params.get("RoleArn").ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MissingParameter",
                "The request must contain the parameter RoleArn",
            )
        })?;
        validate_string_length("roleArn", role_arn, 20, 2048)?;

        // PrincipalArn is required
        let principal_arn = req.query_params.get("PrincipalArn").ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MissingParameter",
                "The request must contain the parameter PrincipalArn",
            )
        })?;
        validate_string_length("principalArn", principal_arn, 20, 2048)?;
        let _principal_arn = principal_arn.clone();

        // SAMLAssertion is required but we just need to extract session name from it
        let saml_assertion = req.query_params.get("SAMLAssertion").ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MissingParameter",
                "The request must contain the parameter SAMLAssertion",
            )
        })?;
        validate_string_length("sAMLAssertion", saml_assertion, 4, 100000)?;

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
            validate_range_i64("durationSeconds", v, 900, 43200)?;
        }

        // Compute expiration from DurationSeconds (default 3600s)
        let expiration = compute_expiration(req, DEFAULT_ASSUME_ROLE_DURATION)?;

        // Decode the SAML assertion to extract the RoleSessionName
        let role_session_name =
            extract_saml_session_name(saml_assertion).unwrap_or_else(|| "saml-session".to_string());

        let partition = partition_for_region(&req.region);
        let creds = StsCredentials::generate();
        let role_id = xml_responses::generate_role_id();

        let mut state = self.state.write();
        let account_id =
            extract_account_from_arn(role_arn).unwrap_or_else(|| state.account_id.clone());

        let role_name = role_arn.rsplit('/').next().unwrap_or("unknown");
        let assumed_role_arn = format!(
            "arn:{}:sts::{}:assumed-role/{}/{}",
            partition, account_id, role_name, &role_session_name
        );
        let assumed_role_id_str = format!("{}:{}", role_id, role_session_name);

        state.credential_identities.insert(
            creds.access_key_id.clone(),
            CredentialIdentity {
                arn: assumed_role_arn,
                user_id: assumed_role_id_str,
                account_id: account_id.clone(),
            },
        );

        let xml = xml_responses::assume_role_with_saml_response(
            role_arn,
            &role_session_name,
            &account_id,
            partition,
            &creds,
            &role_id,
            &expiration,
            &req.request_id,
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn get_session_token(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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
        let expiration = compute_expiration(req, DEFAULT_SESSION_TOKEN_DURATION)?;

        let xml = xml_responses::get_session_token_response(&expiration, &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn get_federation_token(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let name = req.query_params.get("Name").ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MissingParameter",
                "The request must contain the parameter Name",
            )
        })?;
        validate_string_length("name", name, 2, 32)?;

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

        // Validate and store optional policy
        validate_optional_string_length(
            "policy",
            req.query_params.get("Policy").map(|s| s.as_str()),
            1,
            2048,
        )?;
        let policy = req.query_params.get("Policy").cloned();

        // Compute expiration from DurationSeconds (default 43200s / 12 hours)
        let expiration = compute_expiration(req, DEFAULT_FEDERATION_TOKEN_DURATION)?;

        let partition = partition_for_region(&req.region);
        let state = self.state.read();
        let xml = xml_responses::get_federation_token_response(
            name,
            &state.account_id,
            partition,
            &expiration,
            policy.as_deref(),
            &req.request_id,
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn decode_authorization_message(
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

        let decoded_message =
            r#"{"allowed":true,"explicitDeny":false,"matchedStatements":{"items":[]}}"#;
        let xml =
            xml_responses::decode_authorization_message_response(decoded_message, &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn get_access_key_info(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let access_key_id = req.query_params.get("AccessKeyId").ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MissingParameter",
                "The request must contain the parameter AccessKeyId",
            )
        })?;
        validate_string_length("accessKeyId", access_key_id, 16, 128)?;

        // Try to resolve account from known access keys, fall back to default
        let state = self.state.read();
        let account_id = state
            .access_keys
            .values()
            .flatten()
            .find(|k| k.access_key_id == *access_key_id)
            .map(|_| state.account_id.clone())
            .or_else(|| {
                state
                    .credential_identities
                    .get(access_key_id.as_str())
                    .map(|ci| ci.account_id.clone())
            })
            .unwrap_or_else(|| state.account_id.clone());

        let xml = xml_responses::get_access_key_info_response(&account_id, &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }
}

/// Extract account ID from an ARN like `arn:aws:iam::123456789012:role/name`.
fn extract_account_from_arn(arn: &str) -> Option<String> {
    let parts: Vec<&str> = arn.split(':').collect();
    if parts.len() >= 5 && !parts[4].is_empty() {
        Some(parts[4].to_string())
    } else {
        None
    }
}

/// Extract the RoleSessionName from a base64-encoded SAML assertion.
fn extract_saml_session_name(saml_b64: &str) -> Option<String> {
    use base64::Engine;
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(saml_b64)
        .ok()?;
    let xml_str = String::from_utf8(decoded).ok()?;

    // Look for the RoleSessionName attribute value in the SAML XML.
    let role_session_attr = "https://aws.amazon.com/SAML/Attributes/RoleSessionName";
    let pos = xml_str.find(role_session_attr)?;

    // Find the AttributeValue after this position
    let after = &xml_str[pos..];
    let av_start = after.find("AttributeValue")?;
    let after_av = &after[av_start..];
    // Skip past the closing >
    let gt_pos = after_av.find('>')?;
    let value_start = &after_av[gt_pos + 1..];
    // Find end of value (next < which starts the closing tag)
    let lt_pos = value_start.find('<')?;
    let value = value_start[..lt_pos].trim();

    if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_for_region() {
        assert_eq!(partition_for_region("us-east-1"), "aws");
        assert_eq!(partition_for_region("eu-west-1"), "aws");
        assert_eq!(partition_for_region("cn-north-1"), "aws-cn");
        assert_eq!(partition_for_region("cn-northwest-1"), "aws-cn");
        assert_eq!(partition_for_region("us-isob-east-1"), "aws-iso-b");
        assert_eq!(partition_for_region("us-iso-east-1"), "aws-iso");
    }

    #[test]
    fn test_extract_account_from_arn() {
        assert_eq!(
            extract_account_from_arn("arn:aws:iam::123456789012:role/test"),
            Some("123456789012".to_string())
        );
        assert_eq!(
            extract_account_from_arn("arn:aws:iam::111111111111:role/test"),
            Some("111111111111".to_string())
        );
        assert_eq!(extract_account_from_arn("invalid"), None);
    }

    #[test]
    fn test_extract_saml_session_name() {
        use base64::Engine;
        let xml = r#"<?xml version="1.0"?><samlp:Response><Assertion><AttributeStatement><Attribute Name="https://aws.amazon.com/SAML/Attributes/RoleSessionName"><AttributeValue>testuser</AttributeValue></Attribute></AttributeStatement></Assertion></samlp:Response>"#;
        let encoded = base64::engine::general_purpose::STANDARD.encode(xml.as_bytes());
        assert_eq!(
            extract_saml_session_name(&encoded),
            Some("testuser".to_string())
        );
    }

    #[test]
    fn test_extract_saml_session_name_with_namespace() {
        use base64::Engine;
        let xml = r#"<?xml version="1.0"?><samlp:Response><saml:Assertion><saml:AttributeStatement><saml:Attribute Name="https://aws.amazon.com/SAML/Attributes/RoleSessionName"><saml:AttributeValue>testuser</saml:AttributeValue></saml:Attribute></saml:AttributeStatement></saml:Assertion></samlp:Response>"#;
        let encoded = base64::engine::general_purpose::STANDARD.encode(xml.as_bytes());
        assert_eq!(
            extract_saml_session_name(&encoded),
            Some("testuser".to_string())
        );
    }

    #[test]
    fn test_session_token_format() {
        let token = xml_responses::generate_session_token();
        assert_eq!(token.len(), 356);
        assert!(token.starts_with("FQoGZXIvYXdzE"));
    }

    #[test]
    fn test_access_key_id_format() {
        let key = xml_responses::generate_access_key_id();
        assert_eq!(key.len(), 20);
        assert!(key.starts_with("FSIA"));
    }

    #[test]
    fn test_secret_access_key_format() {
        let key = xml_responses::generate_secret_access_key();
        assert_eq!(key.len(), 40);
    }

    #[test]
    fn test_role_id_format() {
        let id = xml_responses::generate_role_id();
        assert_eq!(id.len(), 21);
        assert!(id.starts_with("AROA"));
    }

    #[test]
    fn test_decode_authorization_message() {
        use crate::state::IamState;
        use parking_lot::RwLock;
        use std::collections::HashMap;
        use std::sync::Arc;

        let state: SharedIamState = Arc::new(RwLock::new(IamState::new("123456789012")));
        let service = StsService::new(state);

        let mut params = HashMap::new();
        params.insert(
            "EncodedMessage".to_string(),
            "some-encoded-message".to_string(),
        );

        let req = make_test_request(params);
        let resp = service.decode_authorization_message(&req).unwrap();
        let body = std::str::from_utf8(&resp.body).unwrap();
        assert!(body.contains("DecodedMessage"));
        assert!(body.contains("allowed"));
        assert!(body.contains("matchedStatements"));
    }

    #[test]
    fn test_decode_authorization_message_missing_param() {
        use crate::state::IamState;
        use parking_lot::RwLock;
        use std::collections::HashMap;
        use std::sync::Arc;

        let state: SharedIamState = Arc::new(RwLock::new(IamState::new("123456789012")));
        let service = StsService::new(state);

        let req = make_test_request(HashMap::new());
        let result = service.decode_authorization_message(&req);
        assert!(result.is_err());
        let err = result.err().unwrap();
        let msg = format!("{:?}", err);
        assert!(msg.contains("EncodedMessage"));
    }

    fn make_test_request(params: std::collections::HashMap<String, String>) -> AwsRequest {
        AwsRequest {
            service: "sts".into(),
            action: "Test".into(),
            region: "us-east-1".into(),
            account_id: "123456789012".into(),
            request_id: "test".into(),
            headers: http::HeaderMap::new(),
            query_params: params,
            body: Default::default(),
            path_segments: vec![],
            raw_path: "/".into(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: true,
            access_key_id: None,
        }
    }

    fn parse_expiration(s: &str) -> chrono::DateTime<Utc> {
        chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%SZ")
            .expect("valid timestamp")
            .and_utc()
    }

    #[test]
    fn test_compute_expiration_with_duration() {
        use std::collections::HashMap;

        let mut params = HashMap::new();
        params.insert("DurationSeconds".to_string(), "1800".to_string());
        let req = make_test_request(params);

        let now = Utc::now();
        let exp_str = compute_expiration(&req, 3600).unwrap();
        let exp_utc = parse_expiration(&exp_str);

        // Should be ~1800s from now (using provided DurationSeconds, not default)
        let diff = (exp_utc - now).num_seconds();
        assert!(
            (1798..=1802).contains(&diff),
            "expected ~1800s duration, got {diff}s"
        );
    }

    #[test]
    fn test_compute_expiration_default() {
        use std::collections::HashMap;

        let req = make_test_request(HashMap::new());

        let now = Utc::now();
        let exp_str = compute_expiration(&req, 43200).unwrap();
        let exp_utc = parse_expiration(&exp_str);

        // Should be ~43200s (12 hours) from now using default
        let diff = (exp_utc - now).num_seconds();
        assert!(
            (43198..=43202).contains(&diff),
            "expected ~43200s duration, got {diff}s"
        );
    }

    #[test]
    fn test_compute_expiration_uses_provided_not_default() {
        use std::collections::HashMap;

        let mut params = HashMap::new();
        params.insert("DurationSeconds".to_string(), "900".to_string());
        let req = make_test_request(params);

        let before = Utc::now();
        let exp_str = compute_expiration(&req, 43200).unwrap();
        let exp_utc = parse_expiration(&exp_str);

        // Should use 900s, not the default 43200s
        let expected = before + chrono::Duration::seconds(900);
        let diff = (exp_utc - expected).num_seconds().abs();
        assert!(
            diff <= 2,
            "expected ~900s duration, got diff={diff}s from expected"
        );
    }
}
