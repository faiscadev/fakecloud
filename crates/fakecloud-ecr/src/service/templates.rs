//! `EcrService` `templates` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl EcrService {
    pub(super) fn create_repository_creation_template(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        use crate::state::{EncryptionConfiguration as Enc, RepositoryCreationTemplate};
        let body = request.json_body();
        let prefix = req_str(&body, "prefix")?.to_string();
        validate_template_prefix(&prefix)?;
        let applied_for: Vec<String> = body
            .get("appliedFor")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let image_tag_mutability = opt_str(&body, "imageTagMutability")
            .unwrap_or("MUTABLE")
            .to_string();
        let resource_tags = body
            .get("resourceTags")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let encryption = body.get("encryptionConfiguration").map(|v| Enc {
            encryption_type: v
                .get("encryptionType")
                .and_then(|x| x.as_str())
                .unwrap_or("AES256")
                .to_string(),
            kms_key: v
                .get("kmsKey")
                .and_then(|x| x.as_str())
                .map(|s| s.to_string()),
        });
        let account = target_account_id(request, &body);
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&account);
        if state.repository_creation_templates.contains_key(&prefix) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "TemplateAlreadyExistsException",
                format!(
                    "A repository creation template with the prefix '{prefix}' already exists."
                ),
            ));
        }
        let now = Utc::now();
        let tpl = RepositoryCreationTemplate {
            prefix: prefix.clone(),
            description: opt_str(&body, "description").map(|s| s.to_string()),
            image_tag_mutability,
            applied_for,
            resource_tags,
            created_at: now,
            updated_at: now,
            custom_role_arn: opt_str(&body, "customRoleArn").map(|s| s.to_string()),
            repository_policy: opt_str(&body, "repositoryPolicy").map(|s| s.to_string()),
            lifecycle_policy: opt_str(&body, "lifecyclePolicy").map(|s| s.to_string()),
            encryption_configuration: encryption,
        };
        state
            .repository_creation_templates
            .insert(prefix, tpl.clone());
        Ok(AwsResponse::ok_json(json!({
            "registryId": state.account_id,
            "repositoryCreationTemplate": template_to_json(&tpl),
        })))
    }

    pub(super) fn delete_repository_creation_template(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let prefix = req_str(&body, "prefix")?.to_string();
        validate_template_prefix(&prefix)?;
        let account = target_account_id(request, &body);
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&account);
        let removed = state
            .repository_creation_templates
            .remove(&prefix)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "TemplateNotFoundException",
                    format!("No repository creation template with prefix '{prefix}' exists."),
                )
            })?;
        Ok(AwsResponse::ok_json(json!({
            "registryId": state.account_id,
            "repositoryCreationTemplate": template_to_json(&removed),
        })))
    }

    pub(super) fn describe_repository_creation_templates(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        validate_max_results(&body)?;
        let prefixes: Vec<String> = body
            .get("prefixes")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let account = target_account_id(request, &body);
        let accounts = self.state.read();
        let state = accounts.get(&account);
        let tpls: Vec<Value> = state
            .map(|s| {
                s.repository_creation_templates
                    .values()
                    .filter(|t| prefixes.is_empty() || prefixes.contains(&t.prefix))
                    .map(template_to_json)
                    .collect()
            })
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({
            "registryId": state.map(|s| s.account_id.clone()).unwrap_or_default(),
            "repositoryCreationTemplates": tpls,
        })))
    }

    pub(super) fn update_repository_creation_template(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let prefix = req_str(&body, "prefix")?.to_string();
        validate_template_prefix(&prefix)?;
        let account = target_account_id(request, &body);
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&account);
        let tpl = state
            .repository_creation_templates
            .get_mut(&prefix)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "TemplateNotFoundException",
                    format!("No repository creation template with prefix '{prefix}' exists."),
                )
            })?;
        if let Some(desc) = opt_str(&body, "description") {
            tpl.description = Some(desc.to_string());
        }
        if let Some(mutability) = opt_str(&body, "imageTagMutability") {
            tpl.image_tag_mutability = mutability.to_string();
        }
        if let Some(arr) = body.get("appliedFor").and_then(|v| v.as_array()) {
            tpl.applied_for = arr
                .iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect();
        }
        if let Some(arr) = body.get("resourceTags").and_then(|v| v.as_array()) {
            tpl.resource_tags = arr.clone();
        }
        // customRoleArn / repositoryPolicy / lifecyclePolicy /
        // encryptionConfiguration were dropped on update (bug-audit
        // 2026-06-20, 1.24).
        if let Some(role) = opt_str(&body, "customRoleArn") {
            tpl.custom_role_arn = Some(role.to_string());
        }
        if let Some(p) = opt_str(&body, "repositoryPolicy") {
            tpl.repository_policy = Some(p.to_string());
        }
        if let Some(p) = opt_str(&body, "lifecyclePolicy") {
            tpl.lifecycle_policy = Some(p.to_string());
        }
        if let Some(v) = body.get("encryptionConfiguration") {
            use crate::state::EncryptionConfiguration as Enc;
            tpl.encryption_configuration = Some(Enc {
                encryption_type: v
                    .get("encryptionType")
                    .and_then(|x| x.as_str())
                    .unwrap_or("AES256")
                    .to_string(),
                kms_key: v
                    .get("kmsKey")
                    .and_then(|x| x.as_str())
                    .map(|s| s.to_string()),
            });
        }
        tpl.updated_at = Utc::now();
        Ok(AwsResponse::ok_json(json!({
            "registryId": state.account_id,
            "repositoryCreationTemplate": template_to_json(tpl),
        })))
    }
}
