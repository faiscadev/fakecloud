use std::collections::HashMap;

use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::EmailTemplate;

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

    pub(super) fn list_email_templates(&self) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn get_email_template(&self, name: &str) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn update_email_template(
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

    pub(super) fn delete_email_template(&self, name: &str) -> Result<AwsResponse, AwsServiceError> {
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

        let state = self.state.read();
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

        // Parse template data JSON
        let data: HashMap<String, Value> =
            serde_json::from_str(&template_data_str).unwrap_or_default();

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

        let rendered_subject = template
            .subject
            .as_deref()
            .map(&substitute)
            .unwrap_or_default();
        let rendered_html = template.html_body.as_deref().map(&substitute);
        let rendered_text = template.text_body.as_deref().map(&substitute);

        // Build a simplified MIME message
        let mut mime = format!("Subject: {}\r\n", rendered_subject);
        mime.push_str("MIME-Version: 1.0\r\n");
        mime.push_str("Content-Type: text/html; charset=UTF-8\r\n");
        mime.push_str("\r\n");
        if let Some(ref html) = rendered_html {
            mime.push_str(html);
        } else if let Some(ref text) = rendered_text {
            mime.push_str(text);
        }

        let response = json!({
            "RenderedTemplate": mime,
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }
}
