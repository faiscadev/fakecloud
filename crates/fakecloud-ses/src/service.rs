use async_trait::async_trait;
use chrono::Utc;
use http::{Method, StatusCode};
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::{ConfigurationSet, EmailIdentity, EmailTemplate, SentEmail, SharedSesState};

pub struct SesV2Service {
    state: SharedSesState,
}

impl SesV2Service {
    pub fn new(state: SharedSesState) -> Self {
        Self { state }
    }

    /// Determine the action from the HTTP method and path segments.
    /// SES v2 uses REST-style routing with base path /v2/email/:
    ///   GET    /v2/email/account                         -> GetAccount
    ///   POST   /v2/email/identities                      -> CreateEmailIdentity
    ///   GET    /v2/email/identities                      -> ListEmailIdentities
    ///   GET    /v2/email/identities/{id}                 -> GetEmailIdentity
    ///   DELETE /v2/email/identities/{id}                 -> DeleteEmailIdentity
    ///   POST   /v2/email/configuration-sets              -> CreateConfigurationSet
    ///   GET    /v2/email/configuration-sets              -> ListConfigurationSets
    ///   GET    /v2/email/configuration-sets/{name}       -> GetConfigurationSet
    ///   DELETE /v2/email/configuration-sets/{name}       -> DeleteConfigurationSet
    ///   POST   /v2/email/templates                       -> CreateEmailTemplate
    ///   GET    /v2/email/templates                       -> ListEmailTemplates
    ///   GET    /v2/email/templates/{name}                -> GetEmailTemplate
    ///   PUT    /v2/email/templates/{name}                -> UpdateEmailTemplate
    ///   DELETE /v2/email/templates/{name}                -> DeleteEmailTemplate
    ///   POST   /v2/email/outbound-emails                 -> SendEmail
    ///   POST   /v2/email/outbound-bulk-emails            -> SendBulkEmail
    fn resolve_action(req: &AwsRequest) -> Option<(&str, Option<String>)> {
        let segs = &req.path_segments;

        // Expect first two segments to be "v2" and "email"
        if segs.len() < 3 || segs[0] != "v2" || segs[1] != "email" {
            return None;
        }

        // URL-decode the resource name (e.g. test%40example.com -> test@example.com)
        let resource = segs.get(3).map(|s| {
            percent_encoding::percent_decode_str(s)
                .decode_utf8_lossy()
                .into_owned()
        });

        match (req.method.clone(), segs.len()) {
            // /v2/email/account
            (Method::GET, 3) if segs[2] == "account" => Some(("GetAccount", None)),

            // /v2/email/identities
            (Method::POST, 3) if segs[2] == "identities" => Some(("CreateEmailIdentity", None)),
            (Method::GET, 3) if segs[2] == "identities" => Some(("ListEmailIdentities", None)),
            // /v2/email/identities/{id}
            (Method::GET, 4) if segs[2] == "identities" => Some(("GetEmailIdentity", resource)),
            (Method::DELETE, 4) if segs[2] == "identities" => {
                Some(("DeleteEmailIdentity", resource))
            }

            // /v2/email/configuration-sets
            (Method::POST, 3) if segs[2] == "configuration-sets" => {
                Some(("CreateConfigurationSet", None))
            }
            (Method::GET, 3) if segs[2] == "configuration-sets" => {
                Some(("ListConfigurationSets", None))
            }
            // /v2/email/configuration-sets/{name}
            (Method::GET, 4) if segs[2] == "configuration-sets" => {
                Some(("GetConfigurationSet", resource))
            }
            (Method::DELETE, 4) if segs[2] == "configuration-sets" => {
                Some(("DeleteConfigurationSet", resource))
            }

            // /v2/email/templates
            (Method::POST, 3) if segs[2] == "templates" => Some(("CreateEmailTemplate", None)),
            (Method::GET, 3) if segs[2] == "templates" => Some(("ListEmailTemplates", None)),
            // /v2/email/templates/{name}
            (Method::GET, 4) if segs[2] == "templates" => Some(("GetEmailTemplate", resource)),
            (Method::PUT, 4) if segs[2] == "templates" => Some(("UpdateEmailTemplate", resource)),
            (Method::DELETE, 4) if segs[2] == "templates" => {
                Some(("DeleteEmailTemplate", resource))
            }

            // /v2/email/outbound-emails
            (Method::POST, 3) if segs[2] == "outbound-emails" => Some(("SendEmail", None)),

            // /v2/email/outbound-bulk-emails
            (Method::POST, 3) if segs[2] == "outbound-bulk-emails" => Some(("SendBulkEmail", None)),

            _ => None,
        }
    }

    fn parse_body(req: &AwsRequest) -> Result<Value, AwsServiceError> {
        serde_json::from_slice(&req.body).map_err(|_| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "Invalid JSON in request body",
            )
        })
    }

    fn json_error(status: StatusCode, code: &str, message: &str) -> AwsResponse {
        let body = json!({
            "__type": code,
            "message": message,
        });
        AwsResponse::json(status, body.to_string())
    }

    fn get_account(&self) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let response = json!({
            "DedicatedIpAutoWarmupEnabled": false,
            "EnforcementStatus": "HEALTHY",
            "ProductionAccessEnabled": true,
            "SendQuota": {
                "Max24HourSend": 50000.0,
                "MaxSendRate": 14.0,
                "SentLast24Hours": state.sent_emails.len() as f64,
            },
            "SendingEnabled": true,
        });
        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn create_email_identity(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    fn list_email_identities(&self) -> Result<AwsResponse, AwsServiceError> {
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

    fn get_email_identity(&self, identity_name: &str) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let _identity = match state.identities.get(identity_name) {
            Some(id) => id,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Identity {} does not exist", identity_name),
                ));
            }
        };

        let response = json!({
            "IdentityType": _identity.identity_type,
            "VerifiedForSendingStatus": true,
            "FeedbackForwardingStatus": true,
            "DkimAttributes": {
                "SigningEnabled": true,
                "Status": "SUCCESS",
                "Tokens": [
                    "token1",
                    "token2",
                    "token3",
                ],
            },
            "MailFromAttributes": {
                "MailFromDomain": "",
                "MailFromDomainStatus": "FAILED",
                "BehaviorOnMxFailure": "USE_DEFAULT_VALUE",
            },
            "Tags": [],
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn delete_email_identity(&self, identity_name: &str) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();

        if state.identities.remove(identity_name).is_none() {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("Identity {} does not exist", identity_name),
            ));
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn create_configuration_set(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let name = match body["ConfigurationSetName"].as_str() {
            Some(n) => n.to_string(),
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "ConfigurationSetName is required",
                ));
            }
        };

        let mut state = self.state.write();

        if state.configuration_sets.contains_key(&name) {
            return Ok(Self::json_error(
                StatusCode::CONFLICT,
                "AlreadyExistsException",
                &format!("Configuration set {} already exists", name),
            ));
        }

        state
            .configuration_sets
            .insert(name.clone(), ConfigurationSet { name });

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn list_configuration_sets(&self) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let sets: Vec<Value> = state
            .configuration_sets
            .keys()
            .map(|name| json!(name))
            .collect();

        let response = json!({
            "ConfigurationSets": sets,
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn get_configuration_set(&self, name: &str) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();

        if !state.configuration_sets.contains_key(name) {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("Configuration set {} does not exist", name),
            ));
        }

        let response = json!({
            "ConfigurationSetName": name,
            "DeliveryOptions": {
                "TlsPolicy": "OPTIONAL",
            },
            "ReputationOptions": {
                "ReputationMetricsEnabled": false,
            },
            "SendingOptions": {
                "SendingEnabled": true,
            },
            "Tags": [],
            "TrackingOptions": {},
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn delete_configuration_set(&self, name: &str) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();

        if state.configuration_sets.remove(name).is_none() {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("Configuration set {} does not exist", name),
            ));
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn create_email_template(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let template_name = match body["TemplateName"].as_str() {
            Some(n) => n.to_string(),
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "TemplateName is required",
                ));
            }
        };

        let mut state = self.state.write();

        if state.templates.contains_key(&template_name) {
            return Ok(Self::json_error(
                StatusCode::CONFLICT,
                "AlreadyExistsException",
                &format!("Template {} already exists", template_name),
            ));
        }

        let template = EmailTemplate {
            template_name: template_name.clone(),
            subject: body["TemplateContent"]["Subject"]
                .as_str()
                .map(|s| s.to_string()),
            html_body: body["TemplateContent"]["Html"]
                .as_str()
                .map(|s| s.to_string()),
            text_body: body["TemplateContent"]["Text"]
                .as_str()
                .map(|s| s.to_string()),
            created_at: Utc::now(),
        };

        state.templates.insert(template_name, template);

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn list_email_templates(&self) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let templates: Vec<Value> = state
            .templates
            .values()
            .map(|t| {
                json!({
                    "TemplateName": t.template_name,
                    "CreatedTimestamp": t.created_at.timestamp() as f64,
                })
            })
            .collect();

        let response = json!({
            "TemplatesMetadata": templates,
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn get_email_template(&self, name: &str) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let template = match state.templates.get(name) {
            Some(t) => t,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Template {} does not exist", name),
                ));
            }
        };

        let response = json!({
            "TemplateName": template.template_name,
            "TemplateContent": {
                "Subject": template.subject,
                "Html": template.html_body,
                "Text": template.text_body,
            },
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn update_email_template(
        &self,
        name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let mut state = self.state.write();

        let template = match state.templates.get_mut(name) {
            Some(t) => t,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Template {} does not exist", name),
                ));
            }
        };

        if let Some(subject) = body["TemplateContent"]["Subject"].as_str() {
            template.subject = Some(subject.to_string());
        }
        if let Some(html) = body["TemplateContent"]["Html"].as_str() {
            template.html_body = Some(html.to_string());
        }
        if let Some(text) = body["TemplateContent"]["Text"].as_str() {
            template.text_body = Some(text.to_string());
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn delete_email_template(&self, name: &str) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();

        if state.templates.remove(name).is_none() {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("Template {} does not exist", name),
            ));
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn send_email(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;

        if !body["Content"].is_object()
            || (!body["Content"]["Simple"].is_object()
                && !body["Content"]["Raw"].is_object()
                && !body["Content"]["Template"].is_object())
        {
            return Ok(Self::json_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "Content is required and must contain Simple, Raw, or Template",
            ));
        }

        let from = body["FromEmailAddress"].as_str().unwrap_or("").to_string();

        let to = extract_string_array(&body["Destination"]["ToAddresses"]);
        let cc = extract_string_array(&body["Destination"]["CcAddresses"]);
        let bcc = extract_string_array(&body["Destination"]["BccAddresses"]);

        let (subject, html_body, text_body, raw_data, template_name, template_data) =
            if body["Content"]["Simple"].is_object() {
                let simple = &body["Content"]["Simple"];
                let subject = simple["Subject"]["Data"].as_str().map(|s| s.to_string());
                let html = simple["Body"]["Html"]["Data"]
                    .as_str()
                    .map(|s| s.to_string());
                let text = simple["Body"]["Text"]["Data"]
                    .as_str()
                    .map(|s| s.to_string());
                (subject, html, text, None, None, None)
            } else if body["Content"]["Raw"].is_object() {
                let raw = body["Content"]["Raw"]["Data"]
                    .as_str()
                    .map(|s| s.to_string());
                (None, None, None, raw, None, None)
            } else if body["Content"]["Template"].is_object() {
                let tmpl = &body["Content"]["Template"];
                let tmpl_name = tmpl["TemplateName"].as_str().map(|s| s.to_string());
                let tmpl_data = tmpl["TemplateData"].as_str().map(|s| s.to_string());
                (None, None, None, None, tmpl_name, tmpl_data)
            } else {
                (None, None, None, None, None, None)
            };

        let message_id = uuid::Uuid::new_v4().to_string();

        let sent = SentEmail {
            message_id: message_id.clone(),
            from,
            to,
            cc,
            bcc,
            subject,
            html_body,
            text_body,
            raw_data,
            template_name,
            template_data,
            timestamp: Utc::now(),
        };

        self.state.write().sent_emails.push(sent);

        let response = json!({
            "MessageId": message_id,
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn send_bulk_email(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;

        let from = body["FromEmailAddress"].as_str().unwrap_or("").to_string();

        let entries = match body["BulkEmailEntries"].as_array() {
            Some(arr) if !arr.is_empty() => arr.clone(),
            _ => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "BulkEmailEntries is required and must not be empty",
                ));
            }
        };

        let mut results = Vec::new();

        for entry in &entries {
            let to = extract_string_array(&entry["Destination"]["ToAddresses"]);
            let cc = extract_string_array(&entry["Destination"]["CcAddresses"]);
            let bcc = extract_string_array(&entry["Destination"]["BccAddresses"]);

            let message_id = uuid::Uuid::new_v4().to_string();

            let template_name = body["DefaultContent"]["Template"]["TemplateName"]
                .as_str()
                .map(|s| s.to_string());
            let template_data = entry["ReplacementEmailContent"]["ReplacementTemplate"]
                ["ReplacementTemplateData"]
                .as_str()
                .or_else(|| body["DefaultContent"]["Template"]["TemplateData"].as_str())
                .map(|s| s.to_string());

            let sent = SentEmail {
                message_id: message_id.clone(),
                from: from.clone(),
                to,
                cc,
                bcc,
                subject: None,
                html_body: None,
                text_body: None,
                raw_data: None,
                template_name,
                template_data,
                timestamp: Utc::now(),
            };

            self.state.write().sent_emails.push(sent);

            results.push(json!({
                "Status": "SUCCESS",
                "MessageId": message_id,
            }));
        }

        let response = json!({
            "BulkEmailEntryResults": results,
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }
}

fn extract_string_array(value: &Value) -> Vec<String> {
    value
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default()
}

#[async_trait]
impl fakecloud_core::service::AwsService for SesV2Service {
    fn service_name(&self) -> &str {
        "ses"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let (action, resource_name) = Self::resolve_action(&req).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "UnknownOperationException",
                format!("Unknown operation: {} {}", req.method, req.raw_path),
            )
        })?;

        match action {
            "GetAccount" => self.get_account(),
            "CreateEmailIdentity" => self.create_email_identity(&req),
            "ListEmailIdentities" => self.list_email_identities(),
            "GetEmailIdentity" => self.get_email_identity(resource_name.as_deref().unwrap_or("")),
            "DeleteEmailIdentity" => {
                self.delete_email_identity(resource_name.as_deref().unwrap_or(""))
            }
            "CreateConfigurationSet" => self.create_configuration_set(&req),
            "ListConfigurationSets" => self.list_configuration_sets(),
            "GetConfigurationSet" => {
                self.get_configuration_set(resource_name.as_deref().unwrap_or(""))
            }
            "DeleteConfigurationSet" => {
                self.delete_configuration_set(resource_name.as_deref().unwrap_or(""))
            }
            "CreateEmailTemplate" => self.create_email_template(&req),
            "ListEmailTemplates" => self.list_email_templates(),
            "GetEmailTemplate" => self.get_email_template(resource_name.as_deref().unwrap_or("")),
            "UpdateEmailTemplate" => {
                self.update_email_template(resource_name.as_deref().unwrap_or(""), &req)
            }
            "DeleteEmailTemplate" => {
                self.delete_email_template(resource_name.as_deref().unwrap_or(""))
            }
            "SendEmail" => self.send_email(&req),
            "SendBulkEmail" => self.send_bulk_email(&req),
            _ => Err(AwsServiceError::action_not_implemented("ses", action)),
        }
    }

    fn supported_actions(&self) -> &[&str] {
        &[
            "GetAccount",
            "CreateEmailIdentity",
            "ListEmailIdentities",
            "GetEmailIdentity",
            "DeleteEmailIdentity",
            "CreateConfigurationSet",
            "ListConfigurationSets",
            "GetConfigurationSet",
            "DeleteConfigurationSet",
            "CreateEmailTemplate",
            "ListEmailTemplates",
            "GetEmailTemplate",
            "UpdateEmailTemplate",
            "DeleteEmailTemplate",
            "SendEmail",
            "SendBulkEmail",
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::SesState;
    use bytes::Bytes;
    use fakecloud_core::service::AwsService;
    use http::{HeaderMap, Method};
    use parking_lot::RwLock;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn make_state() -> SharedSesState {
        Arc::new(RwLock::new(SesState::new("123456789012", "us-east-1")))
    }

    fn make_request(method: Method, path: &str, body: &str) -> AwsRequest {
        let path_segments: Vec<String> = path
            .split('/')
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect();
        AwsRequest {
            service: "ses".to_string(),
            action: String::new(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            request_id: "test-request-id".to_string(),
            headers: HeaderMap::new(),
            query_params: HashMap::new(),
            body: Bytes::from(body.to_string()),
            path_segments,
            raw_path: path.to_string(),
            method,
            is_query_protocol: false,
            access_key_id: None,
        }
    }

    #[tokio::test]
    async fn test_identity_lifecycle() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        // Create identity
        let req = make_request(
            Method::POST,
            "/v2/email/identities",
            r#"{"EmailIdentity": "test@example.com"}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["VerifiedForSendingStatus"], true);
        assert_eq!(body["IdentityType"], "EMAIL_ADDRESS");

        // List identities
        let req = make_request(Method::GET, "/v2/email/identities", "");
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["EmailIdentities"].as_array().unwrap().len(), 1);

        // Get identity
        let req = make_request(Method::GET, "/v2/email/identities/test%40example.com", "");
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["VerifiedForSendingStatus"], true);
        assert_eq!(body["DkimAttributes"]["Status"], "SUCCESS");

        // Delete identity
        let req = make_request(
            Method::DELETE,
            "/v2/email/identities/test%40example.com",
            "",
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        // Verify deleted
        let req = make_request(Method::GET, "/v2/email/identities/test%40example.com", "");
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_domain_identity() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::POST,
            "/v2/email/identities",
            r#"{"EmailIdentity": "example.com"}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["IdentityType"], "DOMAIN");
    }

    #[tokio::test]
    async fn test_duplicate_identity() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::POST,
            "/v2/email/identities",
            r#"{"EmailIdentity": "test@example.com"}"#,
        );
        svc.handle(req).await.unwrap();

        let req = make_request(
            Method::POST,
            "/v2/email/identities",
            r#"{"EmailIdentity": "test@example.com"}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn test_template_lifecycle() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        // Create template
        let req = make_request(
            Method::POST,
            "/v2/email/templates",
            r#"{"TemplateName": "welcome", "TemplateContent": {"Subject": "Welcome", "Html": "<h1>Hi</h1>", "Text": "Hi"}}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        // Get template
        let req = make_request(Method::GET, "/v2/email/templates/welcome", "");
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["TemplateName"], "welcome");
        assert_eq!(body["TemplateContent"]["Subject"], "Welcome");

        // Update template
        let req = make_request(
            Method::PUT,
            "/v2/email/templates/welcome",
            r#"{"TemplateContent": {"Subject": "Updated Welcome"}}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        // Verify update
        let req = make_request(Method::GET, "/v2/email/templates/welcome", "");
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["TemplateContent"]["Subject"], "Updated Welcome");

        // List templates
        let req = make_request(Method::GET, "/v2/email/templates", "");
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["TemplatesMetadata"].as_array().unwrap().len(), 1);

        // Delete template
        let req = make_request(Method::DELETE, "/v2/email/templates/welcome", "");
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        // Verify deleted
        let req = make_request(Method::GET, "/v2/email/templates/welcome", "");
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_send_email() {
        let state = make_state();
        let svc = SesV2Service::new(state.clone());

        let req = make_request(
            Method::POST,
            "/v2/email/outbound-emails",
            r#"{
                "FromEmailAddress": "sender@example.com",
                "Destination": {
                    "ToAddresses": ["recipient@example.com"]
                },
                "Content": {
                    "Simple": {
                        "Subject": {"Data": "Test Subject"},
                        "Body": {
                            "Text": {"Data": "Hello world"},
                            "Html": {"Data": "<p>Hello world</p>"}
                        }
                    }
                }
            }"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["MessageId"].as_str().is_some());

        // Verify stored
        let s = state.read();
        assert_eq!(s.sent_emails.len(), 1);
        assert_eq!(s.sent_emails[0].from, "sender@example.com");
        assert_eq!(s.sent_emails[0].to, vec!["recipient@example.com"]);
        assert_eq!(s.sent_emails[0].subject.as_deref(), Some("Test Subject"));
    }

    #[tokio::test]
    async fn test_get_account() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(Method::GET, "/v2/email/account", "");
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["SendingEnabled"], true);
        assert!(body["SendQuota"]["Max24HourSend"].as_f64().unwrap() > 0.0);
    }

    #[tokio::test]
    async fn test_configuration_set_lifecycle() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        // Create
        let req = make_request(
            Method::POST,
            "/v2/email/configuration-sets",
            r#"{"ConfigurationSetName": "my-config"}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        // Get
        let req = make_request(Method::GET, "/v2/email/configuration-sets/my-config", "");
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["ConfigurationSetName"], "my-config");

        // List
        let req = make_request(Method::GET, "/v2/email/configuration-sets", "");
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["ConfigurationSets"].as_array().unwrap().len(), 1);

        // Delete
        let req = make_request(Method::DELETE, "/v2/email/configuration-sets/my-config", "");
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        // Verify deleted
        let req = make_request(Method::GET, "/v2/email/configuration-sets/my-config", "");
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::NOT_FOUND);
    }
}
