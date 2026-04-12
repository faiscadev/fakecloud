use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::EmailIdentity;

use super::SesV2Service;

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

        let mut state = self.state.write();

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

        let identity = EmailIdentity {
            identity_name: identity_name.clone(),
            identity_type: identity_type.to_string(),
            verified: true,
            created_at: Utc::now(),
            dkim_signing_enabled: true,
            dkim_signing_attributes_origin: "AWS_SES".to_string(),
            dkim_domain_signing_private_key: None,
            dkim_domain_signing_selector: None,
            dkim_next_signing_key_length: None,
            email_forwarding_enabled: true,
            mail_from_domain: None,
            mail_from_behavior_on_mx_failure: "USE_DEFAULT_VALUE".to_string(),
            configuration_set_name: None,
        };

        state.identities.insert(identity_name, identity);

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

    pub(super) fn list_email_identities(&self) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
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
    ) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let identity = match state.identities.get(identity_name) {
            Some(id) => id,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Identity {} does not exist", identity_name),
                ));
            }
        };

        let mail_from_domain = identity.mail_from_domain.as_deref().unwrap_or("");
        let mail_from_status = if mail_from_domain.is_empty() {
            "FAILED"
        } else {
            "SUCCESS"
        };

        let mut response = json!({
            "IdentityType": identity.identity_type,
            "VerifiedForSendingStatus": true,
            "FeedbackForwardingStatus": identity.email_forwarding_enabled,
            "DkimAttributes": {
                "SigningEnabled": identity.dkim_signing_enabled,
                "Status": "SUCCESS",
                "SigningAttributesOrigin": identity.dkim_signing_attributes_origin,
                "Tokens": [
                    "token1",
                    "token2",
                    "token3",
                ],
            },
            "MailFromAttributes": {
                "MailFromDomain": mail_from_domain,
                "MailFromDomainStatus": mail_from_status,
                "BehaviorOnMxFailure": identity.mail_from_behavior_on_mx_failure,
            },
            "Tags": [],
        });

        if let Some(ref cs) = identity.configuration_set_name {
            response["ConfigurationSetName"] = json!(cs);
        }

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    pub(super) fn delete_email_identity(
        &self,
        identity_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();

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

        let mut state = self.state.write();

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
    ) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();

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

        let mut state = self.state.write();

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
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();

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
        let mut state = self.state.write();

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

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(super) fn put_email_identity_dkim_signing_attributes(
        &self,
        identity_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let mut state = self.state.write();

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

        if let Some(origin) = body["SigningAttributesOrigin"].as_str() {
            identity.dkim_signing_attributes_origin = origin.to_string();
        }

        if let Some(attrs) = body.get("SigningAttributes") {
            if let Some(key) = attrs["DomainSigningPrivateKey"].as_str() {
                identity.dkim_domain_signing_private_key = Some(key.to_string());
            }
            if let Some(selector) = attrs["DomainSigningSelector"].as_str() {
                identity.dkim_domain_signing_selector = Some(selector.to_string());
            }
            if let Some(length) = attrs["NextSigningKeyLength"].as_str() {
                identity.dkim_next_signing_key_length = Some(length.to_string());
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
        let mut state = self.state.write();

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
        let mut state = self.state.write();

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
            identity.mail_from_domain = Some(domain.to_string());
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
        let mut state = self.state.write();

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
