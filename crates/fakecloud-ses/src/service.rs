use async_trait::async_trait;
use chrono::Utc;
use http::{Method, StatusCode};
use serde_json::{json, Value};
use std::collections::HashMap;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::{
    ConfigurationSet, Contact, ContactList, EmailIdentity, EmailTemplate, SentEmail,
    SharedSesState, Topic, TopicPreference,
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
            // /v2/email/contact-lists/{name}/contacts
            (Method::POST, 5) if segs[2] == "contact-lists" && segs[4] == "contacts" => {
                Some(("CreateContact", resource, None))
            }
            (Method::GET, 5) if segs[2] == "contact-lists" && segs[4] == "contacts" => {
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

    fn delete_contact_list(&self, name: &str) -> Result<AwsResponse, AwsServiceError> {
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
            "DeleteEmailIdentity" => self.delete_email_identity(res),
            "CreateConfigurationSet" => self.create_configuration_set(&req),
            "ListConfigurationSets" => self.list_configuration_sets(),
            "GetConfigurationSet" => self.get_configuration_set(res),
            "DeleteConfigurationSet" => self.delete_configuration_set(res),
            "CreateEmailTemplate" => self.create_email_template(&req),
            "ListEmailTemplates" => self.list_email_templates(),
            "GetEmailTemplate" => self.get_email_template(res),
            "UpdateEmailTemplate" => self.update_email_template(res, &req),
            "DeleteEmailTemplate" => self.delete_email_template(res),
            "SendEmail" => self.send_email(&req),
            "SendBulkEmail" => self.send_bulk_email(&req),
            "CreateContactList" => self.create_contact_list(&req),
            "GetContactList" => self.get_contact_list(res),
            "ListContactLists" => self.list_contact_lists(),
            "UpdateContactList" => self.update_contact_list(res, &req),
            "DeleteContactList" => self.delete_contact_list(res),
            "CreateContact" => self.create_contact(res, &req),
            "GetContact" => self.get_contact(res, sub),
            "ListContacts" => self.list_contacts(res),
            "UpdateContact" => self.update_contact(res, sub, &req),
            "DeleteContact" => self.delete_contact(res, sub),
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
}
