use async_trait::async_trait;
use chrono::Utc;
use http::{Method, StatusCode};
use serde_json::{json, Value};
use std::collections::HashMap;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::{
    ConfigurationSet, Contact, ContactList, EmailIdentity, EmailTemplate, EventDestination,
    SentEmail, SharedSesState, SuppressedDestination, Topic, TopicPreference,
};

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
    ///   POST   /v2/email/tags                            -> TagResource
    ///   DELETE /v2/email/tags                            -> UntagResource
    ///   GET    /v2/email/tags                            -> ListTagsForResource
    ///   POST   /v2/email/contact-lists                   -> CreateContactList
    ///   GET    /v2/email/contact-lists                   -> ListContactLists
    ///   GET    /v2/email/contact-lists/{name}            -> GetContactList
    ///   PUT    /v2/email/contact-lists/{name}            -> UpdateContactList
    ///   DELETE /v2/email/contact-lists/{name}            -> DeleteContactList
    ///   POST   /v2/email/contact-lists/{name}/contacts   -> CreateContact
    ///   GET    /v2/email/contact-lists/{name}/contacts   -> ListContacts
    ///   GET    /v2/email/contact-lists/{name}/contacts/{email} -> GetContact
    ///   PUT    /v2/email/contact-lists/{name}/contacts/{email} -> UpdateContact
    ///   DELETE /v2/email/contact-lists/{name}/contacts/{email} -> DeleteContact
    ///   PUT    /v2/email/suppression/addresses            -> PutSuppressedDestination
    ///   GET    /v2/email/suppression/addresses            -> ListSuppressedDestinations
    ///   GET    /v2/email/suppression/addresses/{email}    -> GetSuppressedDestination
    ///   DELETE /v2/email/suppression/addresses/{email}    -> DeleteSuppressedDestination
    ///   POST   /v2/email/configuration-sets/{name}/event-destinations -> CreateConfigurationSetEventDestination
    ///   GET    /v2/email/configuration-sets/{name}/event-destinations -> GetConfigurationSetEventDestinations
    ///   PUT    /v2/email/configuration-sets/{name}/event-destinations/{dest} -> UpdateConfigurationSetEventDestination
    ///   DELETE /v2/email/configuration-sets/{name}/event-destinations/{dest} -> DeleteConfigurationSetEventDestination
    ///   POST   /v2/email/identities/{id}/policies/{policy} -> CreateEmailIdentityPolicy
    ///   GET    /v2/email/identities/{id}/policies         -> GetEmailIdentityPolicies
    ///   PUT    /v2/email/identities/{id}/policies/{policy} -> UpdateEmailIdentityPolicy
    ///   DELETE /v2/email/identities/{id}/policies/{policy} -> DeleteEmailIdentityPolicy
    fn resolve_action(req: &AwsRequest) -> Option<(&str, Option<String>, Option<String>)> {
        let segs = &req.path_segments;

        // Expect first two segments to be "v2" and "email"
        if segs.len() < 3 || segs[0] != "v2" || segs[1] != "email" {
            return None;
        }

        // URL-decode the resource name (e.g. test%40example.com -> test@example.com)
        let decode = |s: &str| {
            percent_encoding::percent_decode_str(s)
                .decode_utf8_lossy()
                .into_owned()
        };
        let resource = segs.get(3).map(|s| decode(s));

        match (req.method.clone(), segs.len()) {
            // /v2/email/account
            (Method::GET, 3) if segs[2] == "account" => Some(("GetAccount", None, None)),

            // /v2/email/identities
            (Method::POST, 3) if segs[2] == "identities" => {
                Some(("CreateEmailIdentity", None, None))
            }
            (Method::GET, 3) if segs[2] == "identities" => {
                Some(("ListEmailIdentities", None, None))
            }
            // /v2/email/identities/{id}
            (Method::GET, 4) if segs[2] == "identities" => {
                Some(("GetEmailIdentity", resource, None))
            }
            (Method::DELETE, 4) if segs[2] == "identities" => {
                Some(("DeleteEmailIdentity", resource, None))
            }

            // /v2/email/configuration-sets
            (Method::POST, 3) if segs[2] == "configuration-sets" => {
                Some(("CreateConfigurationSet", None, None))
            }
            (Method::GET, 3) if segs[2] == "configuration-sets" => {
                Some(("ListConfigurationSets", None, None))
            }
            // /v2/email/configuration-sets/{name}
            (Method::GET, 4) if segs[2] == "configuration-sets" => {
                Some(("GetConfigurationSet", resource, None))
            }
            (Method::DELETE, 4) if segs[2] == "configuration-sets" => {
                Some(("DeleteConfigurationSet", resource, None))
            }

            // /v2/email/templates
            (Method::POST, 3) if segs[2] == "templates" => {
                Some(("CreateEmailTemplate", None, None))
            }
            (Method::GET, 3) if segs[2] == "templates" => Some(("ListEmailTemplates", None, None)),
            // /v2/email/templates/{name}
            (Method::GET, 4) if segs[2] == "templates" => {
                Some(("GetEmailTemplate", resource, None))
            }
            (Method::PUT, 4) if segs[2] == "templates" => {
                Some(("UpdateEmailTemplate", resource, None))
            }
            (Method::DELETE, 4) if segs[2] == "templates" => {
                Some(("DeleteEmailTemplate", resource, None))
            }

            // /v2/email/outbound-emails
            (Method::POST, 3) if segs[2] == "outbound-emails" => Some(("SendEmail", None, None)),

            // /v2/email/outbound-bulk-emails
            (Method::POST, 3) if segs[2] == "outbound-bulk-emails" => {
                Some(("SendBulkEmail", None, None))
            }

            // /v2/email/contact-lists
            (Method::POST, 3) if segs[2] == "contact-lists" => {
                Some(("CreateContactList", None, None))
            }
            (Method::GET, 3) if segs[2] == "contact-lists" => {
                Some(("ListContactLists", None, None))
            }
            // /v2/email/contact-lists/{name}
            (Method::GET, 4) if segs[2] == "contact-lists" => {
                Some(("GetContactList", resource, None))
            }
            (Method::PUT, 4) if segs[2] == "contact-lists" => {
                Some(("UpdateContactList", resource, None))
            }
            (Method::DELETE, 4) if segs[2] == "contact-lists" => {
                Some(("DeleteContactList", resource, None))
            }
            // /v2/email/tags
            (Method::POST, 3) if segs[2] == "tags" => Some(("TagResource", None, None)),
            (Method::DELETE, 3) if segs[2] == "tags" => Some(("UntagResource", None, None)),
            (Method::GET, 3) if segs[2] == "tags" => Some(("ListTagsForResource", None, None)),

            // /v2/email/contact-lists/{name}/contacts
            (Method::POST, 5) if segs[2] == "contact-lists" && segs[4] == "contacts" => {
                Some(("CreateContact", resource, None))
            }
            (Method::GET, 5) if segs[2] == "contact-lists" && segs[4] == "contacts" => {
                Some(("ListContacts", resource, None))
            }
            // /v2/email/contact-lists/{name}/contacts/list (SDK sends POST for ListContacts)
            (Method::POST, 6)
                if segs[2] == "contact-lists" && segs[4] == "contacts" && segs[5] == "list" =>
            {
                Some(("ListContacts", resource, None))
            }
            // /v2/email/contact-lists/{name}/contacts/{email}
            (Method::GET, 6) if segs[2] == "contact-lists" && segs[4] == "contacts" => {
                Some(("GetContact", resource, Some(decode(&segs[5]))))
            }
            (Method::PUT, 6) if segs[2] == "contact-lists" && segs[4] == "contacts" => {
                Some(("UpdateContact", resource, Some(decode(&segs[5]))))
            }
            (Method::DELETE, 6) if segs[2] == "contact-lists" && segs[4] == "contacts" => {
                Some(("DeleteContact", resource, Some(decode(&segs[5]))))
            }

            // /v2/email/suppression/addresses
            (Method::PUT, 4) if segs[2] == "suppression" && segs[3] == "addresses" => {
                Some(("PutSuppressedDestination", None, None))
            }
            (Method::GET, 4) if segs[2] == "suppression" && segs[3] == "addresses" => {
                Some(("ListSuppressedDestinations", None, None))
            }
            // /v2/email/suppression/addresses/{email}
            (Method::GET, 5) if segs[2] == "suppression" && segs[3] == "addresses" => {
                Some(("GetSuppressedDestination", Some(decode(&segs[4])), None))
            }
            (Method::DELETE, 5) if segs[2] == "suppression" && segs[3] == "addresses" => {
                Some(("DeleteSuppressedDestination", Some(decode(&segs[4])), None))
            }

            // /v2/email/configuration-sets/{name}/event-destinations
            (Method::POST, 5)
                if segs[2] == "configuration-sets" && segs[4] == "event-destinations" =>
            {
                Some(("CreateConfigurationSetEventDestination", resource, None))
            }
            (Method::GET, 5)
                if segs[2] == "configuration-sets" && segs[4] == "event-destinations" =>
            {
                Some(("GetConfigurationSetEventDestinations", resource, None))
            }
            // /v2/email/configuration-sets/{name}/event-destinations/{dest-name}
            (Method::PUT, 6)
                if segs[2] == "configuration-sets" && segs[4] == "event-destinations" =>
            {
                Some((
                    "UpdateConfigurationSetEventDestination",
                    resource,
                    Some(decode(&segs[5])),
                ))
            }
            (Method::DELETE, 6)
                if segs[2] == "configuration-sets" && segs[4] == "event-destinations" =>
            {
                Some((
                    "DeleteConfigurationSetEventDestination",
                    resource,
                    Some(decode(&segs[5])),
                ))
            }

            // /v2/email/identities/{id}/policies
            (Method::GET, 5) if segs[2] == "identities" && segs[4] == "policies" => {
                Some(("GetEmailIdentityPolicies", resource, None))
            }
            // /v2/email/identities/{id}/policies/{policy-name}
            (Method::POST, 6) if segs[2] == "identities" && segs[4] == "policies" => Some((
                "CreateEmailIdentityPolicy",
                resource,
                Some(decode(&segs[5])),
            )),
            (Method::PUT, 6) if segs[2] == "identities" && segs[4] == "policies" => Some((
                "UpdateEmailIdentityPolicy",
                resource,
                Some(decode(&segs[5])),
            )),
            (Method::DELETE, 6) if segs[2] == "identities" && segs[4] == "policies" => Some((
                "DeleteEmailIdentityPolicy",
                resource,
                Some(decode(&segs[5])),
            )),

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
                "SentLast24Hours": state.sent_emails.iter()
                    .filter(|e| e.timestamp > Utc::now() - chrono::Duration::hours(24))
                    .count() as f64,
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

    fn delete_email_identity(
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

    fn delete_configuration_set(
        &self,
        name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();

        if state.configuration_sets.remove(name).is_none() {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("Configuration set {} does not exist", name),
            ));
        }

        // Remove tags for this configuration set
        let arn = format!(
            "arn:aws:ses:{}:{}:configuration-set/{}",
            req.region, req.account_id, name
        );
        state.tags.remove(&arn);

        // Remove event destinations for this configuration set
        state.event_destinations.remove(name);

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

    // --- Contact List operations ---

    fn create_contact_list(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let name = match body["ContactListName"].as_str() {
            Some(n) => n.to_string(),
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "ContactListName is required",
                ));
            }
        };

        let mut state = self.state.write();

        if state.contact_lists.contains_key(&name) {
            return Ok(Self::json_error(
                StatusCode::CONFLICT,
                "AlreadyExistsException",
                &format!("List with name {} already exists.", name),
            ));
        }

        let topics = parse_topics(&body["Topics"]);
        let description = body["Description"].as_str().map(|s| s.to_string());
        let now = Utc::now();

        state.contact_lists.insert(
            name.clone(),
            ContactList {
                contact_list_name: name.clone(),
                description,
                topics,
                created_at: now,
                last_updated_at: now,
            },
        );
        state.contacts.insert(name, HashMap::new());

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn get_contact_list(&self, name: &str) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let list = match state.contact_lists.get(name) {
            Some(l) => l,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("List with name {} does not exist.", name),
                ));
            }
        };

        let topics: Vec<Value> = list
            .topics
            .iter()
            .map(|t| {
                json!({
                    "TopicName": t.topic_name,
                    "DisplayName": t.display_name,
                    "Description": t.description,
                    "DefaultSubscriptionStatus": t.default_subscription_status,
                })
            })
            .collect();

        let response = json!({
            "ContactListName": list.contact_list_name,
            "Description": list.description,
            "Topics": topics,
            "CreatedTimestamp": list.created_at.timestamp() as f64,
            "LastUpdatedTimestamp": list.last_updated_at.timestamp() as f64,
            "Tags": [],
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn list_contact_lists(&self) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let lists: Vec<Value> = state
            .contact_lists
            .values()
            .map(|l| {
                json!({
                    "ContactListName": l.contact_list_name,
                    "LastUpdatedTimestamp": l.last_updated_at.timestamp() as f64,
                })
            })
            .collect();

        let response = json!({
            "ContactLists": lists,
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn update_contact_list(
        &self,
        name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let mut state = self.state.write();

        let list = match state.contact_lists.get_mut(name) {
            Some(l) => l,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("List with name {} does not exist.", name),
                ));
            }
        };

        if let Some(desc) = body.get("Description") {
            list.description = desc.as_str().map(|s| s.to_string());
        }
        if body.get("Topics").is_some() {
            list.topics = parse_topics(&body["Topics"]);
        }
        list.last_updated_at = Utc::now();

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn delete_contact_list(
        &self,
        name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();

        if state.contact_lists.remove(name).is_none() {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("List with name {} does not exist.", name),
            ));
        }

        // Also delete all contacts in this list
        state.contacts.remove(name);

        // Remove tags for this contact list
        let arn = format!(
            "arn:aws:ses:{}:{}:contact-list/{}",
            req.region, req.account_id, name
        );
        state.tags.remove(&arn);

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    // --- Contact operations ---

    fn create_contact(
        &self,
        list_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let email = match body["EmailAddress"].as_str() {
            Some(e) => e.to_string(),
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "EmailAddress is required",
                ));
            }
        };

        let mut state = self.state.write();

        if !state.contact_lists.contains_key(list_name) {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("List with name {} does not exist.", list_name),
            ));
        }

        let contacts = state.contacts.entry(list_name.to_string()).or_default();

        if contacts.contains_key(&email) {
            return Ok(Self::json_error(
                StatusCode::CONFLICT,
                "AlreadyExistsException",
                &format!("Contact already exists in list {}", list_name),
            ));
        }

        let topic_preferences = parse_topic_preferences(&body["TopicPreferences"]);
        let unsubscribe_all = body["UnsubscribeAll"].as_bool().unwrap_or(false);
        let attributes_data = body["AttributesData"].as_str().map(|s| s.to_string());
        let now = Utc::now();

        contacts.insert(
            email.clone(),
            Contact {
                email_address: email,
                topic_preferences,
                unsubscribe_all,
                attributes_data,
                created_at: now,
                last_updated_at: now,
            },
        );

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn get_contact(&self, list_name: &str, email: &str) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();

        if !state.contact_lists.contains_key(list_name) {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("List with name {} does not exist.", list_name),
            ));
        }

        let contact = state.contacts.get(list_name).and_then(|m| m.get(email));

        let contact = match contact {
            Some(c) => c,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Contact {} does not exist in list {}", email, list_name),
                ));
            }
        };

        // Build TopicDefaultPreferences from the contact list's topics
        let list = state.contact_lists.get(list_name).unwrap();
        let topic_default_preferences: Vec<Value> = list
            .topics
            .iter()
            .map(|t| {
                json!({
                    "TopicName": t.topic_name,
                    "SubscriptionStatus": t.default_subscription_status,
                })
            })
            .collect();

        let topic_preferences: Vec<Value> = contact
            .topic_preferences
            .iter()
            .map(|tp| {
                json!({
                    "TopicName": tp.topic_name,
                    "SubscriptionStatus": tp.subscription_status,
                })
            })
            .collect();

        let mut response = json!({
            "ContactListName": list_name,
            "EmailAddress": contact.email_address,
            "TopicPreferences": topic_preferences,
            "TopicDefaultPreferences": topic_default_preferences,
            "UnsubscribeAll": contact.unsubscribe_all,
            "CreatedTimestamp": contact.created_at.timestamp() as f64,
            "LastUpdatedTimestamp": contact.last_updated_at.timestamp() as f64,
        });

        if let Some(ref attrs) = contact.attributes_data {
            response["AttributesData"] = json!(attrs);
        }

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn list_contacts(&self, list_name: &str) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();

        if !state.contact_lists.contains_key(list_name) {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("List with name {} does not exist.", list_name),
            ));
        }

        let contacts: Vec<Value> = state
            .contacts
            .get(list_name)
            .map(|m| {
                m.values()
                    .map(|c| {
                        let topic_prefs: Vec<Value> = c
                            .topic_preferences
                            .iter()
                            .map(|tp| {
                                json!({
                                    "TopicName": tp.topic_name,
                                    "SubscriptionStatus": tp.subscription_status,
                                })
                            })
                            .collect();

                        // Build TopicDefaultPreferences from the list's topics
                        let list = state.contact_lists.get(list_name).unwrap();
                        let topic_defaults: Vec<Value> = list
                            .topics
                            .iter()
                            .map(|t| {
                                json!({
                                    "TopicName": t.topic_name,
                                    "SubscriptionStatus": t.default_subscription_status,
                                })
                            })
                            .collect();

                        json!({
                            "EmailAddress": c.email_address,
                            "TopicPreferences": topic_prefs,
                            "TopicDefaultPreferences": topic_defaults,
                            "UnsubscribeAll": c.unsubscribe_all,
                            "LastUpdatedTimestamp": c.last_updated_at.timestamp() as f64,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();

        let response = json!({
            "Contacts": contacts,
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn update_contact(
        &self,
        list_name: &str,
        email: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let mut state = self.state.write();

        if !state.contact_lists.contains_key(list_name) {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("List with name {} does not exist.", list_name),
            ));
        }

        let contact = state
            .contacts
            .get_mut(list_name)
            .and_then(|m| m.get_mut(email));

        let contact = match contact {
            Some(c) => c,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Contact {} does not exist in list {}", email, list_name),
                ));
            }
        };

        if body.get("TopicPreferences").is_some() {
            contact.topic_preferences = parse_topic_preferences(&body["TopicPreferences"]);
        }
        if let Some(unsub) = body["UnsubscribeAll"].as_bool() {
            contact.unsubscribe_all = unsub;
        }
        if let Some(attrs) = body.get("AttributesData") {
            contact.attributes_data = attrs.as_str().map(|s| s.to_string());
        }
        contact.last_updated_at = Utc::now();

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn delete_contact(&self, list_name: &str, email: &str) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();

        if !state.contact_lists.contains_key(list_name) {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("List with name {} does not exist.", list_name),
            ));
        }

        let removed = state
            .contacts
            .get_mut(list_name)
            .and_then(|m| m.remove(email));

        if removed.is_none() {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("Contact {} does not exist in list {}", email, list_name),
            ));
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    // --- Tag operations ---

    /// Validate that a resource ARN refers to an existing resource.
    /// Returns `None` if the resource exists, or `Some(error_response)` if not.
    fn validate_resource_arn(&self, arn: &str) -> Option<AwsResponse> {
        let state = self.state.read();

        // Parse ARN: arn:aws:ses:{region}:{account}:{resource-type}/{name}
        let parts: Vec<&str> = arn.split(':').collect();
        if parts.len() < 6 {
            return Some(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("Resource not found: {arn}"),
            ));
        }

        let resource = parts[5..].join(":");
        let found = if let Some(name) = resource.strip_prefix("identity/") {
            state.identities.contains_key(name)
        } else if let Some(name) = resource.strip_prefix("configuration-set/") {
            state.configuration_sets.contains_key(name)
        } else if let Some(name) = resource.strip_prefix("contact-list/") {
            state.contact_lists.contains_key(name)
        } else {
            false
        };

        if found {
            None
        } else {
            Some(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("Resource not found: {arn}"),
            ))
        }
    }

    fn tag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;

        let arn = match body["ResourceArn"].as_str() {
            Some(a) => a.to_string(),
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "ResourceArn is required",
                ));
            }
        };

        let tags_arr = match body["Tags"].as_array() {
            Some(arr) => arr,
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "Tags is required",
                ));
            }
        };

        if let Some(resp) = self.validate_resource_arn(&arn) {
            return Ok(resp);
        }

        let mut state = self.state.write();
        let tag_map = state.tags.entry(arn).or_default();
        for tag in tags_arr {
            if let (Some(k), Some(v)) = (tag["Key"].as_str(), tag["Value"].as_str()) {
                tag_map.insert(k.to_string(), v.to_string());
            }
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn untag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        // ResourceArn and TagKeys come as query params
        let arn = match req.query_params.get("ResourceArn") {
            Some(a) => a.to_string(),
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "ResourceArn is required",
                ));
            }
        };

        if let Some(resp) = self.validate_resource_arn(&arn) {
            return Ok(resp);
        }

        // Parse TagKeys from raw query string (supports repeated params)
        let tag_keys: Vec<String> = form_urlencoded::parse(req.raw_query.as_bytes())
            .filter(|(k, _)| k == "TagKeys")
            .map(|(_, v)| v.into_owned())
            .collect();

        let mut state = self.state.write();
        if let Some(tag_map) = state.tags.get_mut(&arn) {
            for key in &tag_keys {
                tag_map.remove(key);
            }
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn list_tags_for_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arn = match req.query_params.get("ResourceArn") {
            Some(a) => a.to_string(),
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "ResourceArn is required",
                ));
            }
        };

        if let Some(resp) = self.validate_resource_arn(&arn) {
            return Ok(resp);
        }

        let state = self.state.read();
        let tags = state.tags.get(&arn);
        let tags_json = match tags {
            Some(t) => fakecloud_core::tags::tags_to_json(t, "Key", "Value"),
            None => vec![],
        };

        let response = json!({
            "Tags": tags_json,
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    // --- Suppression List operations ---

    fn put_suppressed_destination(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let email = match body["EmailAddress"].as_str() {
            Some(e) => e.to_string(),
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "EmailAddress is required",
                ));
            }
        };
        let reason = match body["Reason"].as_str() {
            Some(r) if r == "BOUNCE" || r == "COMPLAINT" => r.to_string(),
            Some(_) => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "Reason must be BOUNCE or COMPLAINT",
                ));
            }
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "Reason is required",
                ));
            }
        };

        let mut state = self.state.write();
        state.suppressed_destinations.insert(
            email.clone(),
            SuppressedDestination {
                email_address: email,
                reason,
                last_update_time: Utc::now(),
            },
        );

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn get_suppressed_destination(&self, email: &str) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let dest = match state.suppressed_destinations.get(email) {
            Some(d) => d,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("{} is not on the suppression list", email),
                ));
            }
        };

        let response = json!({
            "SuppressedDestination": {
                "EmailAddress": dest.email_address,
                "Reason": dest.reason,
                "LastUpdateTime": dest.last_update_time.timestamp() as f64,
            }
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn delete_suppressed_destination(&self, email: &str) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();
        if state.suppressed_destinations.remove(email).is_none() {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("{} is not on the suppression list", email),
            ));
        }
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn list_suppressed_destinations(&self) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let summaries: Vec<Value> = state
            .suppressed_destinations
            .values()
            .map(|d| {
                json!({
                    "EmailAddress": d.email_address,
                    "Reason": d.reason,
                    "LastUpdateTime": d.last_update_time.timestamp() as f64,
                })
            })
            .collect();

        let response = json!({
            "SuppressedDestinationSummaries": summaries,
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    // --- Event Destination operations ---

    fn create_configuration_set_event_destination(
        &self,
        config_set_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;

        let state_read = self.state.read();
        if !state_read.configuration_sets.contains_key(config_set_name) {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("Configuration set {} does not exist", config_set_name),
            ));
        }
        drop(state_read);

        let dest_name = match body["EventDestinationName"].as_str() {
            Some(n) => n.to_string(),
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "EventDestinationName is required",
                ));
            }
        };

        let event_dest = parse_event_destination_definition(&dest_name, &body["EventDestination"]);

        let mut state = self.state.write();
        let dests = state
            .event_destinations
            .entry(config_set_name.to_string())
            .or_default();

        if dests.iter().any(|d| d.name == dest_name) {
            return Ok(Self::json_error(
                StatusCode::CONFLICT,
                "AlreadyExistsException",
                &format!("Event destination {} already exists", dest_name),
            ));
        }

        dests.push(event_dest);

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn get_configuration_set_event_destinations(
        &self,
        config_set_name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();

        if !state.configuration_sets.contains_key(config_set_name) {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("Configuration set {} does not exist", config_set_name),
            ));
        }

        let dests = state
            .event_destinations
            .get(config_set_name)
            .cloned()
            .unwrap_or_default();

        let dests_json: Vec<Value> = dests.iter().map(event_destination_to_json).collect();

        let response = json!({
            "EventDestinations": dests_json,
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn update_configuration_set_event_destination(
        &self,
        config_set_name: &str,
        dest_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;

        let mut state = self.state.write();

        if !state.configuration_sets.contains_key(config_set_name) {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("Configuration set {} does not exist", config_set_name),
            ));
        }

        let dests = state
            .event_destinations
            .entry(config_set_name.to_string())
            .or_default();

        let existing = match dests.iter_mut().find(|d| d.name == dest_name) {
            Some(d) => d,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Event destination {} does not exist", dest_name),
                ));
            }
        };

        let updated = parse_event_destination_definition(dest_name, &body["EventDestination"]);
        *existing = updated;

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn delete_configuration_set_event_destination(
        &self,
        config_set_name: &str,
        dest_name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();

        if !state.configuration_sets.contains_key(config_set_name) {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("Configuration set {} does not exist", config_set_name),
            ));
        }

        let dests = state
            .event_destinations
            .entry(config_set_name.to_string())
            .or_default();

        let len_before = dests.len();
        dests.retain(|d| d.name != dest_name);

        if dests.len() == len_before {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("Event destination {} does not exist", dest_name),
            ));
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    // --- Email Identity Policy operations ---

    fn create_email_identity_policy(
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

    fn get_email_identity_policies(
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

    fn update_email_identity_policy(
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

    fn delete_email_identity_policy(
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

fn parse_topics(value: &Value) -> Vec<Topic> {
    value
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|v| {
                    let topic_name = v["TopicName"].as_str()?.to_string();
                    let display_name = v["DisplayName"].as_str().unwrap_or("").to_string();
                    let description = v["Description"].as_str().unwrap_or("").to_string();
                    let default_subscription_status = v["DefaultSubscriptionStatus"]
                        .as_str()
                        .unwrap_or("OPT_OUT")
                        .to_string();
                    Some(Topic {
                        topic_name,
                        display_name,
                        description,
                        default_subscription_status,
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

fn parse_topic_preferences(value: &Value) -> Vec<TopicPreference> {
    value
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|v| {
                    let topic_name = v["TopicName"].as_str()?.to_string();
                    let subscription_status = v["SubscriptionStatus"]
                        .as_str()
                        .unwrap_or("OPT_OUT")
                        .to_string();
                    Some(TopicPreference {
                        topic_name,
                        subscription_status,
                    })
                })
                .collect()
        })
        .unwrap_or_default()
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

fn parse_event_destination_definition(name: &str, def: &Value) -> EventDestination {
    let enabled = def["Enabled"].as_bool().unwrap_or(false);
    let matching_event_types = extract_string_array(&def["MatchingEventTypes"]);
    let kinesis_firehose_destination = def
        .get("KinesisFirehoseDestination")
        .filter(|v| v.is_object())
        .cloned();
    let cloud_watch_destination = def
        .get("CloudWatchDestination")
        .filter(|v| v.is_object())
        .cloned();
    let sns_destination = def.get("SnsDestination").filter(|v| v.is_object()).cloned();
    let event_bridge_destination = def
        .get("EventBridgeDestination")
        .filter(|v| v.is_object())
        .cloned();
    let pinpoint_destination = def
        .get("PinpointDestination")
        .filter(|v| v.is_object())
        .cloned();

    EventDestination {
        name: name.to_string(),
        enabled,
        matching_event_types,
        kinesis_firehose_destination,
        cloud_watch_destination,
        sns_destination,
        event_bridge_destination,
        pinpoint_destination,
    }
}

fn event_destination_to_json(dest: &EventDestination) -> Value {
    let mut obj = json!({
        "Name": dest.name,
        "Enabled": dest.enabled,
        "MatchingEventTypes": dest.matching_event_types,
    });
    if let Some(ref v) = dest.kinesis_firehose_destination {
        obj["KinesisFirehoseDestination"] = v.clone();
    }
    if let Some(ref v) = dest.cloud_watch_destination {
        obj["CloudWatchDestination"] = v.clone();
    }
    if let Some(ref v) = dest.sns_destination {
        obj["SnsDestination"] = v.clone();
    }
    if let Some(ref v) = dest.event_bridge_destination {
        obj["EventBridgeDestination"] = v.clone();
    }
    if let Some(ref v) = dest.pinpoint_destination {
        obj["PinpointDestination"] = v.clone();
    }
    obj
}

#[async_trait]
impl fakecloud_core::service::AwsService for SesV2Service {
    fn service_name(&self) -> &str {
        "ses"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let (action, resource_name, sub_resource) =
            Self::resolve_action(&req).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "UnknownOperationException",
                    format!("Unknown operation: {} {}", req.method, req.raw_path),
                )
            })?;

        let res = resource_name.as_deref().unwrap_or("");
        let sub = sub_resource.as_deref().unwrap_or("");

        match action {
            "GetAccount" => self.get_account(),
            "CreateEmailIdentity" => self.create_email_identity(&req),
            "ListEmailIdentities" => self.list_email_identities(),
            "GetEmailIdentity" => self.get_email_identity(res),
            "DeleteEmailIdentity" => self.delete_email_identity(res, &req),
            "CreateConfigurationSet" => self.create_configuration_set(&req),
            "ListConfigurationSets" => self.list_configuration_sets(),
            "GetConfigurationSet" => self.get_configuration_set(res),
            "DeleteConfigurationSet" => self.delete_configuration_set(res, &req),
            "CreateEmailTemplate" => self.create_email_template(&req),
            "ListEmailTemplates" => self.list_email_templates(),
            "GetEmailTemplate" => self.get_email_template(res),
            "UpdateEmailTemplate" => self.update_email_template(res, &req),
            "DeleteEmailTemplate" => self.delete_email_template(res),
            "SendEmail" => self.send_email(&req),
            "SendBulkEmail" => self.send_bulk_email(&req),
            "TagResource" => self.tag_resource(&req),
            "UntagResource" => self.untag_resource(&req),
            "ListTagsForResource" => self.list_tags_for_resource(&req),
            "CreateContactList" => self.create_contact_list(&req),
            "GetContactList" => self.get_contact_list(res),
            "ListContactLists" => self.list_contact_lists(),
            "UpdateContactList" => self.update_contact_list(res, &req),
            "DeleteContactList" => self.delete_contact_list(res, &req),
            "CreateContact" => self.create_contact(res, &req),
            "GetContact" => self.get_contact(res, sub),
            "ListContacts" => self.list_contacts(res),
            "UpdateContact" => self.update_contact(res, sub, &req),
            "DeleteContact" => self.delete_contact(res, sub),
            "PutSuppressedDestination" => self.put_suppressed_destination(&req),
            "GetSuppressedDestination" => self.get_suppressed_destination(res),
            "DeleteSuppressedDestination" => self.delete_suppressed_destination(res),
            "ListSuppressedDestinations" => self.list_suppressed_destinations(),
            "CreateConfigurationSetEventDestination" => {
                self.create_configuration_set_event_destination(res, &req)
            }
            "GetConfigurationSetEventDestinations" => {
                self.get_configuration_set_event_destinations(res)
            }
            "UpdateConfigurationSetEventDestination" => {
                self.update_configuration_set_event_destination(res, sub, &req)
            }
            "DeleteConfigurationSetEventDestination" => {
                self.delete_configuration_set_event_destination(res, sub)
            }
            "CreateEmailIdentityPolicy" => self.create_email_identity_policy(res, sub, &req),
            "GetEmailIdentityPolicies" => self.get_email_identity_policies(res),
            "UpdateEmailIdentityPolicy" => self.update_email_identity_policy(res, sub, &req),
            "DeleteEmailIdentityPolicy" => self.delete_email_identity_policy(res, sub),
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
            "TagResource",
            "UntagResource",
            "ListTagsForResource",
            "CreateContactList",
            "GetContactList",
            "ListContactLists",
            "UpdateContactList",
            "DeleteContactList",
            "CreateContact",
            "GetContact",
            "ListContacts",
            "UpdateContact",
            "DeleteContact",
            "PutSuppressedDestination",
            "GetSuppressedDestination",
            "DeleteSuppressedDestination",
            "ListSuppressedDestinations",
            "CreateConfigurationSetEventDestination",
            "GetConfigurationSetEventDestinations",
            "UpdateConfigurationSetEventDestination",
            "DeleteConfigurationSetEventDestination",
            "CreateEmailIdentityPolicy",
            "GetEmailIdentityPolicies",
            "UpdateEmailIdentityPolicy",
            "DeleteEmailIdentityPolicy",
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
        make_request_with_query(method, path, body, "", HashMap::new())
    }

    fn make_request_with_query(
        method: Method,
        path: &str,
        body: &str,
        raw_query: &str,
        query_params: HashMap<String, String>,
    ) -> AwsRequest {
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
            query_params,
            body: Bytes::from(body.to_string()),
            path_segments,
            raw_path: path.to_string(),
            raw_query: raw_query.to_string(),
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

    #[tokio::test]
    async fn test_send_email_raw_content() {
        let state = make_state();
        let svc = SesV2Service::new(state.clone());

        let req = make_request(
            Method::POST,
            "/v2/email/outbound-emails",
            r#"{
                "FromEmailAddress": "sender@example.com",
                "Destination": {
                    "ToAddresses": ["to@example.com"]
                },
                "Content": {
                    "Raw": {
                        "Data": "From: sender@example.com\r\nTo: to@example.com\r\nSubject: Raw\r\n\r\nBody"
                    }
                }
            }"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["MessageId"].as_str().is_some());

        let s = state.read();
        assert_eq!(s.sent_emails.len(), 1);
        assert!(s.sent_emails[0].raw_data.is_some());
        assert!(
            s.sent_emails[0].subject.is_none(),
            "Raw emails should not have parsed subject"
        );
    }

    #[tokio::test]
    async fn test_send_email_template_content() {
        let state = make_state();
        let svc = SesV2Service::new(state.clone());

        let req = make_request(
            Method::POST,
            "/v2/email/outbound-emails",
            r#"{
                "FromEmailAddress": "sender@example.com",
                "Destination": {
                    "ToAddresses": ["to@example.com"]
                },
                "Content": {
                    "Template": {
                        "TemplateName": "welcome",
                        "TemplateData": "{\"name\": \"Alice\"}"
                    }
                }
            }"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        let s = state.read();
        assert_eq!(s.sent_emails.len(), 1);
        assert_eq!(s.sent_emails[0].template_name.as_deref(), Some("welcome"));
        assert_eq!(
            s.sent_emails[0].template_data.as_deref(),
            Some("{\"name\": \"Alice\"}")
        );
    }

    #[tokio::test]
    async fn test_send_email_missing_content() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::POST,
            "/v2/email/outbound-emails",
            r#"{"FromEmailAddress": "sender@example.com", "Destination": {"ToAddresses": ["to@example.com"]}}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_send_email_with_cc_and_bcc() {
        let state = make_state();
        let svc = SesV2Service::new(state.clone());

        let req = make_request(
            Method::POST,
            "/v2/email/outbound-emails",
            r#"{
                "FromEmailAddress": "sender@example.com",
                "Destination": {
                    "ToAddresses": ["to@example.com"],
                    "CcAddresses": ["cc@example.com"],
                    "BccAddresses": ["bcc@example.com"]
                },
                "Content": {
                    "Simple": {
                        "Subject": {"Data": "Test"},
                        "Body": {"Text": {"Data": "Hello"}}
                    }
                }
            }"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        let s = state.read();
        assert_eq!(s.sent_emails[0].cc, vec!["cc@example.com"]);
        assert_eq!(s.sent_emails[0].bcc, vec!["bcc@example.com"]);
    }

    #[tokio::test]
    async fn test_send_bulk_email() {
        let state = make_state();
        let svc = SesV2Service::new(state.clone());

        let req = make_request(
            Method::POST,
            "/v2/email/outbound-bulk-emails",
            r#"{
                "FromEmailAddress": "sender@example.com",
                "DefaultContent": {
                    "Template": {
                        "TemplateName": "bulk-template",
                        "TemplateData": "{\"default\": true}"
                    }
                },
                "BulkEmailEntries": [
                    {"Destination": {"ToAddresses": ["a@example.com"]}},
                    {"Destination": {"ToAddresses": ["b@example.com"]}}
                ]
            }"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let results = body["BulkEmailEntryResults"].as_array().unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0]["Status"], "SUCCESS");
        assert_eq!(results[1]["Status"], "SUCCESS");

        let s = state.read();
        assert_eq!(s.sent_emails.len(), 2);
        assert_eq!(s.sent_emails[0].to, vec!["a@example.com"]);
        assert_eq!(s.sent_emails[1].to, vec!["b@example.com"]);
    }

    #[tokio::test]
    async fn test_send_bulk_email_empty_entries() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::POST,
            "/v2/email/outbound-bulk-emails",
            r#"{"FromEmailAddress": "s@example.com", "BulkEmailEntries": []}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_delete_nonexistent_identity() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::DELETE,
            "/v2/email/identities/nobody%40example.com",
            "",
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_duplicate_configuration_set() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::POST,
            "/v2/email/configuration-sets",
            r#"{"ConfigurationSetName": "dup-config"}"#,
        );
        svc.handle(req).await.unwrap();

        let req = make_request(
            Method::POST,
            "/v2/email/configuration-sets",
            r#"{"ConfigurationSetName": "dup-config"}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn test_duplicate_template() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::POST,
            "/v2/email/templates",
            r#"{"TemplateName": "dup-tmpl", "TemplateContent": {}}"#,
        );
        svc.handle(req).await.unwrap();

        let req = make_request(
            Method::POST,
            "/v2/email/templates",
            r#"{"TemplateName": "dup-tmpl", "TemplateContent": {}}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn test_delete_nonexistent_template() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(Method::DELETE, "/v2/email/templates/nope", "");
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_delete_nonexistent_configuration_set() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(Method::DELETE, "/v2/email/configuration-sets/nope", "");
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_unknown_route() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(Method::GET, "/v2/email/unknown-resource", "");
        let result = svc.handle(req).await;
        assert!(result.is_err(), "Unknown route should return error");
    }

    #[tokio::test]
    async fn test_update_nonexistent_template() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::PUT,
            "/v2/email/templates/nonexistent",
            r#"{"TemplateContent": {"Subject": "Updated"}}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_invalid_json_body() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(Method::POST, "/v2/email/identities", "not valid json {{{");
        let result = svc.handle(req).await;
        assert!(result.is_err(), "Invalid JSON body should return error");
    }

    #[tokio::test]
    async fn test_create_identity_missing_name() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(Method::POST, "/v2/email/identities", r#"{}"#);
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::BAD_REQUEST);
    }

    // --- Contact List tests ---

    #[tokio::test]
    async fn test_contact_list_lifecycle() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        // Create contact list with topics
        let req = make_request(
            Method::POST,
            "/v2/email/contact-lists",
            r#"{
                "ContactListName": "my-list",
                "Description": "Test list",
                "Topics": [
                    {
                        "TopicName": "newsletters",
                        "DisplayName": "Newsletters",
                        "Description": "Weekly newsletters",
                        "DefaultSubscriptionStatus": "OPT_IN"
                    }
                ]
            }"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        // Get contact list
        let req = make_request(Method::GET, "/v2/email/contact-lists/my-list", "{}");
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["ContactListName"], "my-list");
        assert_eq!(body["Description"], "Test list");
        assert_eq!(body["Topics"][0]["TopicName"], "newsletters");
        assert_eq!(body["Topics"][0]["DefaultSubscriptionStatus"], "OPT_IN");
        assert!(body["CreatedTimestamp"].as_f64().is_some());
        assert!(body["LastUpdatedTimestamp"].as_f64().is_some());

        // List contact lists
        let req = make_request(Method::GET, "/v2/email/contact-lists", "{}");
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["ContactLists"].as_array().unwrap().len(), 1);
        assert_eq!(body["ContactLists"][0]["ContactListName"], "my-list");

        // Update contact list
        let req = make_request(
            Method::PUT,
            "/v2/email/contact-lists/my-list",
            r#"{
                "Description": "Updated description",
                "Topics": [
                    {
                        "TopicName": "newsletters",
                        "DisplayName": "Updated Newsletters",
                        "Description": "Updated desc",
                        "DefaultSubscriptionStatus": "OPT_OUT"
                    },
                    {
                        "TopicName": "promotions",
                        "DisplayName": "Promotions",
                        "Description": "Promo emails",
                        "DefaultSubscriptionStatus": "OPT_OUT"
                    }
                ]
            }"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        // Verify update
        let req = make_request(Method::GET, "/v2/email/contact-lists/my-list", "{}");
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Description"], "Updated description");
        assert_eq!(body["Topics"].as_array().unwrap().len(), 2);

        // Delete contact list
        let req = make_request(Method::DELETE, "/v2/email/contact-lists/my-list", "");
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        // Verify deleted
        let req = make_request(Method::GET, "/v2/email/contact-lists/my-list", "{}");
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_duplicate_contact_list() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::POST,
            "/v2/email/contact-lists",
            r#"{"ContactListName": "dup-list"}"#,
        );
        svc.handle(req).await.unwrap();

        let req = make_request(
            Method::POST,
            "/v2/email/contact-lists",
            r#"{"ContactListName": "dup-list"}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn test_contact_list_not_found() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(Method::GET, "/v2/email/contact-lists/nonexistent", "{}");
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::NOT_FOUND);
    }

    // --- Contact tests ---

    #[tokio::test]
    async fn test_contact_lifecycle() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        // Create contact list first
        let req = make_request(
            Method::POST,
            "/v2/email/contact-lists",
            r#"{
                "ContactListName": "my-list",
                "Topics": [
                    {
                        "TopicName": "newsletters",
                        "DisplayName": "Newsletters",
                        "Description": "Weekly newsletters",
                        "DefaultSubscriptionStatus": "OPT_OUT"
                    }
                ]
            }"#,
        );
        svc.handle(req).await.unwrap();

        // Create contact
        let req = make_request(
            Method::POST,
            "/v2/email/contact-lists/my-list/contacts",
            r#"{
                "EmailAddress": "user@example.com",
                "TopicPreferences": [
                    {"TopicName": "newsletters", "SubscriptionStatus": "OPT_IN"}
                ],
                "UnsubscribeAll": false
            }"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        // Get contact
        let req = make_request(
            Method::GET,
            "/v2/email/contact-lists/my-list/contacts/user%40example.com",
            "{}",
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["EmailAddress"], "user@example.com");
        assert_eq!(body["ContactListName"], "my-list");
        assert_eq!(body["UnsubscribeAll"], false);
        assert_eq!(body["TopicPreferences"][0]["TopicName"], "newsletters");
        assert_eq!(body["TopicPreferences"][0]["SubscriptionStatus"], "OPT_IN");
        assert_eq!(
            body["TopicDefaultPreferences"][0]["SubscriptionStatus"],
            "OPT_OUT"
        );
        assert!(body["CreatedTimestamp"].as_f64().is_some());

        // List contacts
        let req = make_request(
            Method::GET,
            "/v2/email/contact-lists/my-list/contacts",
            "{}",
        );
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Contacts"].as_array().unwrap().len(), 1);
        assert_eq!(body["Contacts"][0]["EmailAddress"], "user@example.com");

        // Update contact
        let req = make_request(
            Method::PUT,
            "/v2/email/contact-lists/my-list/contacts/user%40example.com",
            r#"{
                "TopicPreferences": [
                    {"TopicName": "newsletters", "SubscriptionStatus": "OPT_OUT"}
                ],
                "UnsubscribeAll": true
            }"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        // Verify update
        let req = make_request(
            Method::GET,
            "/v2/email/contact-lists/my-list/contacts/user%40example.com",
            "{}",
        );
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["UnsubscribeAll"], true);
        assert_eq!(body["TopicPreferences"][0]["SubscriptionStatus"], "OPT_OUT");

        // Delete contact
        let req = make_request(
            Method::DELETE,
            "/v2/email/contact-lists/my-list/contacts/user%40example.com",
            "",
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        // Verify deleted
        let req = make_request(
            Method::GET,
            "/v2/email/contact-lists/my-list/contacts/user%40example.com",
            "{}",
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_duplicate_contact() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::POST,
            "/v2/email/contact-lists",
            r#"{"ContactListName": "my-list"}"#,
        );
        svc.handle(req).await.unwrap();

        let req = make_request(
            Method::POST,
            "/v2/email/contact-lists/my-list/contacts",
            r#"{"EmailAddress": "dup@example.com"}"#,
        );
        svc.handle(req).await.unwrap();

        let req = make_request(
            Method::POST,
            "/v2/email/contact-lists/my-list/contacts",
            r#"{"EmailAddress": "dup@example.com"}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn test_contact_in_nonexistent_list() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::POST,
            "/v2/email/contact-lists/no-such-list/contacts",
            r#"{"EmailAddress": "user@example.com"}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_get_nonexistent_contact() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::POST,
            "/v2/email/contact-lists",
            r#"{"ContactListName": "my-list"}"#,
        );
        svc.handle(req).await.unwrap();

        let req = make_request(
            Method::GET,
            "/v2/email/contact-lists/my-list/contacts/nobody%40example.com",
            "{}",
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_delete_contact_list_cascades_contacts() {
        let state = make_state();
        let svc = SesV2Service::new(state.clone());

        // Create list and contact
        let req = make_request(
            Method::POST,
            "/v2/email/contact-lists",
            r#"{"ContactListName": "my-list"}"#,
        );
        svc.handle(req).await.unwrap();

        let req = make_request(
            Method::POST,
            "/v2/email/contact-lists/my-list/contacts",
            r#"{"EmailAddress": "user@example.com"}"#,
        );
        svc.handle(req).await.unwrap();

        // Delete the contact list
        let req = make_request(Method::DELETE, "/v2/email/contact-lists/my-list", "");
        svc.handle(req).await.unwrap();

        // Verify contacts map is cleaned up
        let s = state.read();
        assert!(!s.contacts.contains_key("my-list"));
    }

    #[tokio::test]
    async fn test_tag_resource() {
        let state = make_state();
        let svc = SesV2Service::new(state.clone());

        // Create an identity
        let req = make_request(
            Method::POST,
            "/v2/email/identities",
            r#"{"EmailIdentity": "test@example.com"}"#,
        );
        svc.handle(req).await.unwrap();

        // Tag it
        let req = make_request(
            Method::POST,
            "/v2/email/tags",
            r#"{"ResourceArn": "arn:aws:ses:us-east-1:123456789012:identity/test@example.com", "Tags": [{"Key": "env", "Value": "prod"}, {"Key": "team", "Value": "backend"}]}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        // List tags
        let mut qp = HashMap::new();
        qp.insert(
            "ResourceArn".to_string(),
            "arn:aws:ses:us-east-1:123456789012:identity/test@example.com".to_string(),
        );
        let req = make_request_with_query(
            Method::GET,
            "/v2/email/tags",
            "",
            "ResourceArn=arn%3Aaws%3Ases%3Aus-east-1%3A123456789012%3Aidentity%2Ftest%40example.com",
            qp,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let tags = body["Tags"].as_array().unwrap();
        assert_eq!(tags.len(), 2);
    }

    #[tokio::test]
    async fn test_untag_resource() {
        let state = make_state();
        let svc = SesV2Service::new(state.clone());

        // Create an identity
        let req = make_request(
            Method::POST,
            "/v2/email/identities",
            r#"{"EmailIdentity": "test@example.com"}"#,
        );
        svc.handle(req).await.unwrap();

        let arn = "arn:aws:ses:us-east-1:123456789012:identity/test@example.com";

        // Tag it
        let req = make_request(
            Method::POST,
            "/v2/email/tags",
            &format!(
                r#"{{"ResourceArn": "{arn}", "Tags": [{{"Key": "env", "Value": "prod"}}, {{"Key": "team", "Value": "backend"}}]}}"#
            ),
        );
        svc.handle(req).await.unwrap();

        // Untag - remove "env"
        let mut qp = HashMap::new();
        qp.insert("ResourceArn".to_string(), arn.to_string());
        qp.insert("TagKeys".to_string(), "env".to_string());
        let raw_query = format!("ResourceArn={}&TagKeys=env", urlencoded(arn));
        let req = make_request_with_query(Method::DELETE, "/v2/email/tags", "", &raw_query, qp);
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        // Verify only "team" remains
        let s = state.read();
        let tags = s.tags.get(arn).unwrap();
        assert_eq!(tags.len(), 1);
        assert_eq!(tags.get("team").unwrap(), "backend");
    }

    #[tokio::test]
    async fn test_tag_nonexistent_resource() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::POST,
            "/v2/email/tags",
            r#"{"ResourceArn": "arn:aws:ses:us-east-1:123456789012:identity/nope", "Tags": [{"Key": "k", "Value": "v"}]}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_delete_identity_removes_tags() {
        let state = make_state();
        let svc = SesV2Service::new(state.clone());

        let req = make_request(
            Method::POST,
            "/v2/email/identities",
            r#"{"EmailIdentity": "test@example.com"}"#,
        );
        svc.handle(req).await.unwrap();

        let arn = "arn:aws:ses:us-east-1:123456789012:identity/test@example.com";
        let req = make_request(
            Method::POST,
            "/v2/email/tags",
            &format!(r#"{{"ResourceArn": "{arn}", "Tags": [{{"Key": "k", "Value": "v"}}]}}"#),
        );
        svc.handle(req).await.unwrap();

        // Delete identity
        let req = make_request(
            Method::DELETE,
            "/v2/email/identities/test%40example.com",
            "",
        );
        svc.handle(req).await.unwrap();

        // Tags should be gone
        let s = state.read();
        assert!(!s.tags.contains_key(arn));
    }

    #[tokio::test]
    async fn test_delete_config_set_removes_tags() {
        let state = make_state();
        let svc = SesV2Service::new(state.clone());

        let req = make_request(
            Method::POST,
            "/v2/email/configuration-sets",
            r#"{"ConfigurationSetName": "my-config"}"#,
        );
        svc.handle(req).await.unwrap();

        let arn = "arn:aws:ses:us-east-1:123456789012:configuration-set/my-config";
        let req = make_request(
            Method::POST,
            "/v2/email/tags",
            &format!(r#"{{"ResourceArn": "{arn}", "Tags": [{{"Key": "k", "Value": "v"}}]}}"#),
        );
        svc.handle(req).await.unwrap();

        // Delete config set
        let req = make_request(Method::DELETE, "/v2/email/configuration-sets/my-config", "");
        svc.handle(req).await.unwrap();

        let s = state.read();
        assert!(!s.tags.contains_key(arn));
    }

    #[tokio::test]
    async fn test_delete_contact_list_removes_tags() {
        let state = make_state();
        let svc = SesV2Service::new(state.clone());

        let req = make_request(
            Method::POST,
            "/v2/email/contact-lists",
            r#"{"ContactListName": "my-list"}"#,
        );
        svc.handle(req).await.unwrap();

        let arn = "arn:aws:ses:us-east-1:123456789012:contact-list/my-list";
        let req = make_request(
            Method::POST,
            "/v2/email/tags",
            &format!(r#"{{"ResourceArn": "{arn}", "Tags": [{{"Key": "k", "Value": "v"}}]}}"#),
        );
        svc.handle(req).await.unwrap();

        // Delete contact list
        let req = make_request(Method::DELETE, "/v2/email/contact-lists/my-list", "");
        svc.handle(req).await.unwrap();

        let s = state.read();
        assert!(!s.tags.contains_key(arn));
    }

    fn urlencoded(s: &str) -> String {
        form_urlencoded::byte_serialize(s.as_bytes()).collect()
    }

    // --- Suppression List tests ---

    #[tokio::test]
    async fn test_suppressed_destination_lifecycle() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        // Put suppressed destination
        let req = make_request(
            Method::PUT,
            "/v2/email/suppression/addresses",
            r#"{"EmailAddress": "bounce@example.com", "Reason": "BOUNCE"}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        // Get suppressed destination
        let req = make_request(
            Method::GET,
            "/v2/email/suppression/addresses/bounce%40example.com",
            "",
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(
            body["SuppressedDestination"]["EmailAddress"],
            "bounce@example.com"
        );
        assert_eq!(body["SuppressedDestination"]["Reason"], "BOUNCE");
        assert!(body["SuppressedDestination"]["LastUpdateTime"]
            .as_f64()
            .is_some());

        // List suppressed destinations
        let req = make_request(Method::GET, "/v2/email/suppression/addresses", "");
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(
            body["SuppressedDestinationSummaries"]
                .as_array()
                .unwrap()
                .len(),
            1
        );

        // Delete suppressed destination
        let req = make_request(
            Method::DELETE,
            "/v2/email/suppression/addresses/bounce%40example.com",
            "",
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        // Verify deleted
        let req = make_request(
            Method::GET,
            "/v2/email/suppression/addresses/bounce%40example.com",
            "",
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_suppressed_destination_complaint() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::PUT,
            "/v2/email/suppression/addresses",
            r#"{"EmailAddress": "complaint@example.com", "Reason": "COMPLAINT"}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        let req = make_request(
            Method::GET,
            "/v2/email/suppression/addresses/complaint%40example.com",
            "",
        );
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["SuppressedDestination"]["Reason"], "COMPLAINT");
    }

    #[tokio::test]
    async fn test_suppressed_destination_invalid_reason() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::PUT,
            "/v2/email/suppression/addresses",
            r#"{"EmailAddress": "bad@example.com", "Reason": "INVALID"}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_suppressed_destination_upsert() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        // Put with BOUNCE
        let req = make_request(
            Method::PUT,
            "/v2/email/suppression/addresses",
            r#"{"EmailAddress": "user@example.com", "Reason": "BOUNCE"}"#,
        );
        svc.handle(req).await.unwrap();

        // Put again with COMPLAINT (upsert)
        let req = make_request(
            Method::PUT,
            "/v2/email/suppression/addresses",
            r#"{"EmailAddress": "user@example.com", "Reason": "COMPLAINT"}"#,
        );
        svc.handle(req).await.unwrap();

        let req = make_request(
            Method::GET,
            "/v2/email/suppression/addresses/user%40example.com",
            "",
        );
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["SuppressedDestination"]["Reason"], "COMPLAINT");
    }

    #[tokio::test]
    async fn test_delete_nonexistent_suppressed_destination() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::DELETE,
            "/v2/email/suppression/addresses/nobody%40example.com",
            "",
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::NOT_FOUND);
    }

    // --- Event Destination tests ---

    #[tokio::test]
    async fn test_event_destination_lifecycle() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        // Create config set first
        let req = make_request(
            Method::POST,
            "/v2/email/configuration-sets",
            r#"{"ConfigurationSetName": "my-config"}"#,
        );
        svc.handle(req).await.unwrap();

        // Create event destination
        let req = make_request(
            Method::POST,
            "/v2/email/configuration-sets/my-config/event-destinations",
            r#"{
                "EventDestinationName": "my-dest",
                "EventDestination": {
                    "Enabled": true,
                    "MatchingEventTypes": ["SEND", "BOUNCE"],
                    "SnsDestination": {"TopicArn": "arn:aws:sns:us-east-1:123456789012:my-topic"}
                }
            }"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        // Get event destinations
        let req = make_request(
            Method::GET,
            "/v2/email/configuration-sets/my-config/event-destinations",
            "",
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let dests = body["EventDestinations"].as_array().unwrap();
        assert_eq!(dests.len(), 1);
        assert_eq!(dests[0]["Name"], "my-dest");
        assert_eq!(dests[0]["Enabled"], true);
        assert_eq!(dests[0]["MatchingEventTypes"], json!(["SEND", "BOUNCE"]));
        assert_eq!(
            dests[0]["SnsDestination"]["TopicArn"],
            "arn:aws:sns:us-east-1:123456789012:my-topic"
        );

        // Update event destination
        let req = make_request(
            Method::PUT,
            "/v2/email/configuration-sets/my-config/event-destinations/my-dest",
            r#"{
                "EventDestination": {
                    "Enabled": false,
                    "MatchingEventTypes": ["DELIVERY"],
                    "SnsDestination": {"TopicArn": "arn:aws:sns:us-east-1:123456789012:updated-topic"}
                }
            }"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        // Verify update
        let req = make_request(
            Method::GET,
            "/v2/email/configuration-sets/my-config/event-destinations",
            "",
        );
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let dests = body["EventDestinations"].as_array().unwrap();
        assert_eq!(dests[0]["Enabled"], false);
        assert_eq!(dests[0]["MatchingEventTypes"], json!(["DELIVERY"]));

        // Delete event destination
        let req = make_request(
            Method::DELETE,
            "/v2/email/configuration-sets/my-config/event-destinations/my-dest",
            "",
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        // Verify deleted
        let req = make_request(
            Method::GET,
            "/v2/email/configuration-sets/my-config/event-destinations",
            "",
        );
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["EventDestinations"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_event_destination_config_set_not_found() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::POST,
            "/v2/email/configuration-sets/nonexistent/event-destinations",
            r#"{
                "EventDestinationName": "dest",
                "EventDestination": {"Enabled": true, "MatchingEventTypes": ["SEND"]}
            }"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_event_destination_duplicate() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::POST,
            "/v2/email/configuration-sets",
            r#"{"ConfigurationSetName": "my-config"}"#,
        );
        svc.handle(req).await.unwrap();

        let body = r#"{
            "EventDestinationName": "dup-dest",
            "EventDestination": {"Enabled": true, "MatchingEventTypes": ["SEND"]}
        }"#;

        let req = make_request(
            Method::POST,
            "/v2/email/configuration-sets/my-config/event-destinations",
            body,
        );
        svc.handle(req).await.unwrap();

        let req = make_request(
            Method::POST,
            "/v2/email/configuration-sets/my-config/event-destinations",
            body,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn test_update_nonexistent_event_destination() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::POST,
            "/v2/email/configuration-sets",
            r#"{"ConfigurationSetName": "my-config"}"#,
        );
        svc.handle(req).await.unwrap();

        let req = make_request(
            Method::PUT,
            "/v2/email/configuration-sets/my-config/event-destinations/nonexistent",
            r#"{"EventDestination": {"Enabled": true, "MatchingEventTypes": ["SEND"]}}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_delete_nonexistent_event_destination() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::POST,
            "/v2/email/configuration-sets",
            r#"{"ConfigurationSetName": "my-config"}"#,
        );
        svc.handle(req).await.unwrap();

        let req = make_request(
            Method::DELETE,
            "/v2/email/configuration-sets/my-config/event-destinations/nonexistent",
            "",
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_event_destinations_cleaned_on_config_set_delete() {
        let state = make_state();
        let svc = SesV2Service::new(state.clone());

        let req = make_request(
            Method::POST,
            "/v2/email/configuration-sets",
            r#"{"ConfigurationSetName": "my-config"}"#,
        );
        svc.handle(req).await.unwrap();

        let req = make_request(
            Method::POST,
            "/v2/email/configuration-sets/my-config/event-destinations",
            r#"{
                "EventDestinationName": "dest1",
                "EventDestination": {"Enabled": true, "MatchingEventTypes": ["SEND"]}
            }"#,
        );
        svc.handle(req).await.unwrap();

        let req = make_request(Method::DELETE, "/v2/email/configuration-sets/my-config", "");
        svc.handle(req).await.unwrap();

        let s = state.read();
        assert!(!s.event_destinations.contains_key("my-config"));
    }

    // --- Email Identity Policy tests ---

    #[tokio::test]
    async fn test_identity_policy_lifecycle() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        // Create identity first
        let req = make_request(
            Method::POST,
            "/v2/email/identities",
            r#"{"EmailIdentity": "test@example.com"}"#,
        );
        svc.handle(req).await.unwrap();

        // Create policy
        let policy_doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"ses:SendEmail","Resource":"*"}]}"#;
        let req = make_request(
            Method::POST,
            "/v2/email/identities/test%40example.com/policies/my-policy",
            &format!(
                r#"{{"Policy": {}}}"#,
                serde_json::to_string(policy_doc).unwrap()
            ),
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        // Get policies
        let req = make_request(
            Method::GET,
            "/v2/email/identities/test%40example.com/policies",
            "",
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["Policies"]["my-policy"].is_string());
        assert_eq!(body["Policies"]["my-policy"].as_str().unwrap(), policy_doc);

        // Update policy
        let updated_doc = r#"{"Version":"2012-10-17","Statement":[]}"#;
        let req = make_request(
            Method::PUT,
            "/v2/email/identities/test%40example.com/policies/my-policy",
            &format!(
                r#"{{"Policy": {}}}"#,
                serde_json::to_string(updated_doc).unwrap()
            ),
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        // Verify update
        let req = make_request(
            Method::GET,
            "/v2/email/identities/test%40example.com/policies",
            "",
        );
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Policies"]["my-policy"].as_str().unwrap(), updated_doc);

        // Delete policy
        let req = make_request(
            Method::DELETE,
            "/v2/email/identities/test%40example.com/policies/my-policy",
            "",
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        // Verify deleted
        let req = make_request(
            Method::GET,
            "/v2/email/identities/test%40example.com/policies",
            "",
        );
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["Policies"].as_object().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_identity_policy_identity_not_found() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::POST,
            "/v2/email/identities/nonexistent%40example.com/policies/my-policy",
            r#"{"Policy": "{}"}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_identity_policy_duplicate() {
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
            "/v2/email/identities/test%40example.com/policies/my-policy",
            r#"{"Policy": "{}"}"#,
        );
        svc.handle(req).await.unwrap();

        let req = make_request(
            Method::POST,
            "/v2/email/identities/test%40example.com/policies/my-policy",
            r#"{"Policy": "{}"}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn test_update_nonexistent_policy() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::POST,
            "/v2/email/identities",
            r#"{"EmailIdentity": "test@example.com"}"#,
        );
        svc.handle(req).await.unwrap();

        let req = make_request(
            Method::PUT,
            "/v2/email/identities/test%40example.com/policies/nonexistent",
            r#"{"Policy": "{}"}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_delete_nonexistent_policy() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::POST,
            "/v2/email/identities",
            r#"{"EmailIdentity": "test@example.com"}"#,
        );
        svc.handle(req).await.unwrap();

        let req = make_request(
            Method::DELETE,
            "/v2/email/identities/test%40example.com/policies/nonexistent",
            "",
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_policies_cleaned_on_identity_delete() {
        let state = make_state();
        let svc = SesV2Service::new(state.clone());

        let req = make_request(
            Method::POST,
            "/v2/email/identities",
            r#"{"EmailIdentity": "test@example.com"}"#,
        );
        svc.handle(req).await.unwrap();

        let req = make_request(
            Method::POST,
            "/v2/email/identities/test%40example.com/policies/my-policy",
            r#"{"Policy": "{}"}"#,
        );
        svc.handle(req).await.unwrap();

        let req = make_request(
            Method::DELETE,
            "/v2/email/identities/test%40example.com",
            "",
        );
        svc.handle(req).await.unwrap();

        let s = state.read();
        assert!(!s.identity_policies.contains_key("test@example.com"));
    }
}
