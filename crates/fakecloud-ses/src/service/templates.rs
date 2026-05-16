use std::collections::HashMap;

use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::EmailTemplate;
use crate::state::SesState;

use super::SesV2Service;

impl SesV2Service {
    pub(super) fn create_email_template(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
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
        if template_name.is_empty() {
            return Ok(Self::json_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "TemplateName must not be empty",
            ));
        }

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

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

        state.templates.insert(template_name.clone(), template);

        // Persist Tags via the per-ARN tag map. The Smithy
        // `GetEmailTemplateResponse` round-trips Tags, so dropping them
        // on Create is a real input-drop bug.
        if let Some(tags_arr) = body["Tags"].as_array() {
            let arn = format!(
                "arn:aws:ses:{}:{}:template/{}",
                req.region, req.account_id, template_name
            );
            let tag_map = state.tags.entry(arn).or_default();
            for tag in tags_arr {
                if let (Some(k), Some(v)) = (tag["Key"].as_str(), tag["Value"].as_str()) {
                    tag_map.insert(k.to_string(), v.to_string());
                }
            }
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(super) fn list_email_templates(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let empty = SesState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
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

    pub(super) fn get_email_template(
        &self,
        name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let empty = SesState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
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

        let mut response = json!({
            "TemplateName": template.template_name,
            "TemplateContent": {
                "Subject": template.subject,
                "Html": template.html_body,
                "Text": template.text_body,
            },
        });

        let arn = format!(
            "arn:aws:ses:{}:{}:template/{}",
            req.region, req.account_id, name
        );
        if let Some(tag_map) = state.tags.get(&arn) {
            response["Tags"] =
                Value::Array(fakecloud_core::tags::tags_to_json(tag_map, "Key", "Value"));
        }

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    pub(super) fn update_email_template(
        &self,
        name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

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

    pub(super) fn delete_email_template(
        &self,
        name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        if state.templates.remove(name).is_none() {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("Template {} does not exist", name),
            ));
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(super) fn test_render_email_template(
        &self,
        template_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;

        let template_data_str = match body["TemplateData"].as_str() {
            Some(d) => d.to_string(),
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "TemplateData is required",
                ));
            }
        };

        let accounts = self.state.read();
        let empty = SesState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let template = match state.templates.get(template_name) {
            Some(t) => t,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Template {} does not exist", template_name),
                ));
            }
        };

        let rendered = render_template(template, &template_data_str);
        let mime = crate::mime::build_message(&crate::mime::MimeInputs {
            subject: rendered.subject.as_deref().unwrap_or(""),
            text: rendered.text.as_deref(),
            html: rendered.html.as_deref(),
        });

        let response = json!({
            "RenderedTemplate": mime,
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }
}

/// Result of rendering an `EmailTemplate` with caller-supplied JSON
/// substitutions. All three fields are pre-substituted strings.
pub struct RenderedTemplate {
    pub subject: Option<String>,
    pub html: Option<String>,
    pub text: Option<String>,
}

/// Render an `EmailTemplate`'s subject/html/text by substituting
/// `{{ key }}` placeholders with values from `template_data_str` (a
/// JSON object). Falls back to empty data when the JSON is malformed.
pub fn render_template(template: &EmailTemplate, template_data_str: &str) -> RenderedTemplate {
    let data: HashMap<String, Value> = serde_json::from_str(template_data_str).unwrap_or_default();
    let substitute = |text: &str| -> String {
        let mut result = text.to_string();
        for (key, value) in &data {
            let placeholder = format!("{{{{{}}}}}", key);
            let replacement = match value {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            result = result.replace(&placeholder, &replacement);
        }
        result
    };
    RenderedTemplate {
        subject: template.subject.as_deref().map(&substitute),
        html: template.html_body.as_deref().map(&substitute),
        text: template.text_body.as_deref().map(&substitute),
    }
}
