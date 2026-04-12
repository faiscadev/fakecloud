use std::collections::HashMap;

use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::{Contact, ContactList};

use super::{parse_topic_preferences, parse_topics, SesV2Service};

impl SesV2Service {
    pub(super) fn create_contact_list(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn get_contact_list(&self, name: &str) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn list_contact_lists(&self) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn update_contact_list(
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

    pub(super) fn delete_contact_list(
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

    pub(super) fn create_contact(
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

    pub(super) fn get_contact(
        &self,
        list_name: &str,
        email: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn list_contacts(&self, list_name: &str) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn update_contact(
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

    pub(super) fn delete_contact(
        &self,
        list_name: &str,
        email: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
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
}
