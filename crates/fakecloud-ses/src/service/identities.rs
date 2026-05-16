use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::EmailIdentity;
use crate::state::SesState;

use super::SesV2Service;

/// Expected DNS records the customer must publish for SES to accept the
/// configured mail-from domain. Mirrors the JSON shape SES returns in
/// `GetEmailIdentity.MailFromAttributes.MailFromDomainDnsRecords`.
pub(crate) fn mail_from_dns_records(domain: &str, region: &str) -> Vec<Value> {
    if domain.is_empty() {
        return Vec::new();
    }
    vec![
        json!({
            "Name": domain,
            "Type": "MX",
            "Value": format!("10 feedback-smtp.{region}.amazonses.com"),
        }),
        json!({
            "Name": domain,
            "Type": "TXT",
            "Value": "\"v=spf1 include:amazonses.com ~all\"",
        }),
    ]
}

impl SesV2Service {
    pub(super) fn create_email_identity(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let identity_name = match body["EmailIdentity"].as_str() {
            Some(name) => name.to_string(),
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "EmailIdentity is required",
                ));
            }
        };
        if identity_name.is_empty() {
            return Ok(Self::json_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "EmailIdentity must not be empty",
            ));
        }

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        if state.identities.contains_key(&identity_name) {
            return Ok(Self::json_error(
                StatusCode::CONFLICT,
                "AlreadyExistsException",
                &format!("Identity {} already exists", identity_name),
            ));
        }

        let identity_type = if identity_name.contains('@') {
            "EMAIL_ADDRESS"
        } else {
            "DOMAIN"
        };

        // Honor the optional `ConfigurationSetName` from the input — the
        // Smithy model round-trips it on GetEmailIdentity, so dropping it
        // here is a real input-drop bug.
        let configuration_set_name = body["ConfigurationSetName"].as_str().map(|s| s.to_string());

        // Easy DKIM auto-provisions a fresh RSA-2048 keypair when the
        // identity is created so SendEmail can stamp DKIM-Signature
        // headers without a follow-up PutEmailIdentityDkimSigningAttributes.
        let (priv_pem, pub_b64) = crate::dkim::generate_easy_dkim_keypair();
        let identity = EmailIdentity {
            identity_name: identity_name.clone(),
            identity_type: identity_type.to_string(),
            verified: true,
            created_at: Utc::now(),
            dkim_signing_enabled: true,
            dkim_signing_attributes_origin: "AWS_SES".to_string(),
            dkim_domain_signing_private_key: Some(priv_pem),
            dkim_domain_signing_selector: Some("fakecloudses".to_string()),
            dkim_next_signing_key_length: Some("RSA_2048_BIT".to_string()),
            dkim_public_key_b64: Some(pub_b64),
            email_forwarding_enabled: true,
            mail_from_domain: None,
            mail_from_behavior_on_mx_failure: "USE_DEFAULT_VALUE".to_string(),
            mail_from_domain_status: "NotStarted".to_string(),
            configuration_set_name,
        };

        state.identities.insert(identity_name.clone(), identity);

        // Persist Tags via the per-ARN tag map TagResource/ListTagsForResource
        // use, so the round-trip echo for Tags is honored end-to-end.
        if let Some(tags_arr) = body["Tags"].as_array() {
            let arn = format!(
                "arn:aws:ses:{}:{}:identity/{}",
                req.region, req.account_id, identity_name
            );
            let tag_map = state.tags.entry(arn).or_default();
            for tag in tags_arr {
                if let (Some(k), Some(v)) = (tag["Key"].as_str(), tag["Value"].as_str()) {
                    tag_map.insert(k.to_string(), v.to_string());
                }
            }
        }

        let response = json!({
            "IdentityType": identity_type,
            "VerifiedForSendingStatus": true,
            "DkimAttributes": {
                "SigningEnabled": true,
                "Status": "SUCCESS",
                "Tokens": [
                    "token1",
                    "token2",
                    "token3",
                ],
            },
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    pub(super) fn list_email_identities(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let empty = SesState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let identities: Vec<Value> = state
            .identities
            .values()
            .map(|id| {
                json!({
                    "IdentityType": id.identity_type,
                    "IdentityName": id.identity_name,
                    "SendingEnabled": true,
                })
            })
            .collect();

        let response = json!({
            "EmailIdentities": identities,
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    pub(super) fn get_email_identity(
        &self,
        identity_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let region = state.region.clone();
        let identity = match state.identities.get_mut(identity_name) {
            Some(id) => id,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Identity {} does not exist", identity_name),
                ));
            }
        };

        let mail_from_domain = identity.mail_from_domain.clone().unwrap_or_default();
        // Auto-advance Pending -> Success on next read; matches real SES once
        // it observes the expected MX/TXT records on the configured mail-from
        // domain. Admin endpoint flips back to Failed for tests.
        if identity.mail_from_domain_status == "Pending" && !mail_from_domain.is_empty() {
            identity.mail_from_domain_status = "Success".to_string();
        }
        if mail_from_domain.is_empty() {
            identity.mail_from_domain_status = "NotStarted".to_string();
        }
        let mail_from_status = identity.mail_from_domain_status.clone();
        let behavior = identity.mail_from_behavior_on_mx_failure.clone();
        let mail_from_dns = mail_from_dns_records(&mail_from_domain, &region);

        let mut dkim_attrs = json!({
            "SigningEnabled": identity.dkim_signing_enabled,
            "Status": "SUCCESS",
            "SigningAttributesOrigin": identity.dkim_signing_attributes_origin,
            "Tokens": ["token1", "token2", "token3"],
        });
        if let Some(ref selector) = identity.dkim_domain_signing_selector {
            dkim_attrs["LastKeyGenerationTimestamp"] = json!(identity.created_at.timestamp());
            dkim_attrs["CurrentSigningKeyLength"] = json!(identity
                .dkim_next_signing_key_length
                .as_deref()
                .unwrap_or("RSA_2048_BIT"));
            dkim_attrs["NextSigningKeyLength"] = json!(identity
                .dkim_next_signing_key_length
                .as_deref()
                .unwrap_or("RSA_2048_BIT"));
            dkim_attrs["DomainSigningSelector"] = json!(selector);
        }
        let mut response = json!({
            "IdentityType": identity.identity_type,
            "VerifiedForSendingStatus": true,
            "FeedbackForwardingStatus": identity.email_forwarding_enabled,
            "DkimAttributes": dkim_attrs,
            "MailFromAttributes": {
                "MailFromDomain": mail_from_domain,
                "MailFromDomainStatus": mail_from_status,
                "BehaviorOnMxFailure": behavior,
            },
            "Tags": [],
        });
        if !mail_from_dns.is_empty() {
            response["MailFromAttributes"]["MailFromDomainDnsRecords"] = json!(mail_from_dns);
        }

        let configuration_set_name = identity.configuration_set_name.clone();
        // Release the &mut on `identity` before borrowing `state.tags`.
        let _ = identity;
        if let Some(cs) = configuration_set_name {
            response["ConfigurationSetName"] = json!(cs);
        }

        // Surface stored tags from the per-ARN tag map. Echoing keeps
        // the round-trip honest: anything CreateEmailIdentity/TagResource
        // wrote is visible on the read side.
        let arn = format!(
            "arn:aws:ses:{}:{}:identity/{}",
            req.region, req.account_id, identity_name
        );
        if let Some(tag_map) = state.tags.get(&arn) {
            response["Tags"] =
                Value::Array(fakecloud_core::tags::tags_to_json(tag_map, "Key", "Value"));
        }

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    pub(super) fn delete_email_identity(
        &self,
        identity_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        if state.identities.remove(identity_name).is_none() {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("Identity {} does not exist", identity_name),
            ));
        }

        // Remove tags for this identity
        let arn = format!(
            "arn:aws:ses:{}:{}:identity/{}",
            req.region, req.account_id, identity_name
        );
        state.tags.remove(&arn);

        // Remove policies for this identity
        state.identity_policies.remove(identity_name);

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    // --- Email Identity Policy operations ---

    pub(super) fn create_email_identity_policy(
        &self,
        identity_name: &str,
        policy_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        Self::require_nonempty("EmailIdentity", identity_name)?;
        Self::require_nonempty("PolicyName", policy_name)?;
        // Reject unsubstituted URI template placeholders (e.g. SDK fed
        // an empty or absent PolicyName and the literal "{PolicyName}"
        // remained in the URL).
        if policy_name.starts_with('{') && policy_name.ends_with('}') {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "PolicyName is required",
            ));
        }
        if policy_name.is_empty() || policy_name.len() > 64 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "PolicyName length must be between 1 and 64",
            ));
        }
        // Smithy regex: `^[a-zA-Z0-9_-]+$`
        if !policy_name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "PolicyName must match pattern [a-zA-Z0-9_-]+",
            ));
        }
        let body: Value = Self::parse_body(req)?;

        let policy = match body["Policy"].as_str() {
            Some(p) => p.to_string(),
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "Policy is required",
                ));
            }
        };

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        if !state.identities.contains_key(identity_name) {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("Identity {} does not exist", identity_name),
            ));
        }

        let policies = state
            .identity_policies
            .entry(identity_name.to_string())
            .or_default();

        if policies.contains_key(policy_name) {
            return Ok(Self::json_error(
                StatusCode::CONFLICT,
                "AlreadyExistsException",
                &format!("Policy {} already exists", policy_name),
            ));
        }

        policies.insert(policy_name.to_string(), policy);

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(super) fn get_email_identity_policies(
        &self,
        identity_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let empty = SesState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

        if !state.identities.contains_key(identity_name) {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("Identity {} does not exist", identity_name),
            ));
        }

        let policies = state
            .identity_policies
            .get(identity_name)
            .cloned()
            .unwrap_or_default();

        let policies_json: Value = policies
            .into_iter()
            .map(|(k, v)| (k, Value::String(v)))
            .collect::<serde_json::Map<String, Value>>()
            .into();

        let response = json!({
            "Policies": policies_json,
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    pub(super) fn update_email_identity_policy(
        &self,
        identity_name: &str,
        policy_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;

        let policy = match body["Policy"].as_str() {
            Some(p) => p.to_string(),
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "Policy is required",
                ));
            }
        };

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        if !state.identities.contains_key(identity_name) {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("Identity {} does not exist", identity_name),
            ));
        }

        let policies = state
            .identity_policies
            .entry(identity_name.to_string())
            .or_default();

        if !policies.contains_key(policy_name) {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("Policy {} does not exist", policy_name),
            ));
        }

        policies.insert(policy_name.to_string(), policy);

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(super) fn delete_email_identity_policy(
        &self,
        identity_name: &str,
        policy_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        if !state.identities.contains_key(identity_name) {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("Identity {} does not exist", identity_name),
            ));
        }

        let policies = state
            .identity_policies
            .entry(identity_name.to_string())
            .or_default();

        if policies.remove(policy_name).is_none() {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("Policy {} does not exist", policy_name),
            ));
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    // --- Identity Attribute operations ---

    pub(super) fn put_email_identity_dkim_attributes(
        &self,
        identity_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        let identity = match state.identities.get_mut(identity_name) {
            Some(id) => id,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Identity {} does not exist", identity_name),
                ));
            }
        };

        if let Some(enabled) = body["SigningEnabled"].as_bool() {
            identity.dkim_signing_enabled = enabled;
        }
        // Lazily provision an Easy DKIM keypair the moment signing is
        // enabled if no caller-supplied key is on file. Mirrors how real
        // SES auto-generates the per-identity keypair on enable so the
        // next SendEmail can stamp a real DKIM-Signature.
        if identity.dkim_signing_enabled && identity.dkim_domain_signing_private_key.is_none() {
            let (priv_pem, pub_b64) = crate::dkim::generate_easy_dkim_keypair();
            identity.dkim_domain_signing_private_key = Some(priv_pem);
            identity.dkim_public_key_b64 = Some(pub_b64);
            if identity.dkim_domain_signing_selector.is_none() {
                identity.dkim_domain_signing_selector = Some("fakecloudses".to_string());
            }
            if identity.dkim_next_signing_key_length.is_none() {
                identity.dkim_next_signing_key_length = Some("RSA_2048_BIT".to_string());
            }
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(super) fn put_email_identity_dkim_signing_attributes(
        &self,
        identity_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        let identity = match state.identities.get_mut(identity_name) {
            Some(id) => id,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Identity {} does not exist", identity_name),
                ));
            }
        };

        let origin = body["SigningAttributesOrigin"]
            .as_str()
            .unwrap_or(&identity.dkim_signing_attributes_origin)
            .to_string();
        identity.dkim_signing_attributes_origin = origin.clone();

        if let Some(attrs) = body.get("SigningAttributes") {
            if let Some(key) = attrs["DomainSigningPrivateKey"].as_str() {
                identity.dkim_domain_signing_private_key = Some(key.to_string());
                identity.dkim_public_key_b64 = None;
            }
            if let Some(selector) = attrs["DomainSigningSelector"].as_str() {
                identity.dkim_domain_signing_selector = Some(selector.to_string());
            }
            if let Some(length) = attrs["NextSigningKeyLength"].as_str() {
                identity.dkim_next_signing_key_length = Some(length.to_string());
            }
        }

        // Easy DKIM: AWS_SES origin without a caller-supplied key triggers
        // generation of a fresh RSA-2048 keypair. The public half is what
        // SES would publish via the `*.dkim.amazonses.com` CNAME chain.
        if origin == "AWS_SES" && identity.dkim_domain_signing_private_key.is_none() {
            let (priv_pem, pub_b64) = crate::dkim::generate_easy_dkim_keypair();
            identity.dkim_domain_signing_private_key = Some(priv_pem);
            identity.dkim_public_key_b64 = Some(pub_b64);
            if identity.dkim_domain_signing_selector.is_none() {
                identity.dkim_domain_signing_selector = Some("fakecloudses".to_string());
            }
        }

        let response = json!({
            "DkimStatus": "SUCCESS",
            "DkimTokens": ["token1", "token2", "token3"],
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    pub(super) fn put_email_identity_feedback_attributes(
        &self,
        identity_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        let identity = match state.identities.get_mut(identity_name) {
            Some(id) => id,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Identity {} does not exist", identity_name),
                ));
            }
        };

        if let Some(enabled) = body["EmailForwardingEnabled"].as_bool() {
            identity.email_forwarding_enabled = enabled;
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(super) fn put_email_identity_mail_from_attributes(
        &self,
        identity_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        let identity = match state.identities.get_mut(identity_name) {
            Some(id) => id,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Identity {} does not exist", identity_name),
                ));
            }
        };

        if let Some(domain) = body["MailFromDomain"].as_str() {
            let trimmed = domain.trim();
            if trimmed.is_empty() {
                identity.mail_from_domain = None;
                identity.mail_from_domain_status = "NotStarted".to_string();
            } else {
                identity.mail_from_domain = Some(trimmed.to_string());
                identity.mail_from_domain_status = "Pending".to_string();
            }
        }
        if let Some(behavior) = body["BehaviorOnMxFailure"].as_str() {
            identity.mail_from_behavior_on_mx_failure = behavior.to_string();
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(super) fn put_email_identity_configuration_set_attributes(
        &self,
        identity_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        let identity = match state.identities.get_mut(identity_name) {
            Some(id) => id,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Identity {} does not exist", identity_name),
                ));
            }
        };

        identity.configuration_set_name =
            body["ConfigurationSetName"].as_str().map(|s| s.to_string());

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }
}
