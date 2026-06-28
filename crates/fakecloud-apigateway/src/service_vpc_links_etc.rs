// Auto-extracted from service.rs as part of carryover service.rs split.

#![allow(clippy::too_many_arguments)]

use serde_json::{json, Value};
use std::collections::BTreeMap;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use super::*;

impl ApiGatewayService {
    pub(super) fn create_vpc_link(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let id = make_id();
        let mut value = req.json_body();
        if let Some(o) = value.as_object_mut() {
            o.insert("id".to_string(), Value::String(id.clone()));
            o.insert("status".to_string(), Value::String("AVAILABLE".to_string()));
        }
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        state.vpc_links.insert(id, value.clone());
        ok_status(StatusCode::CREATED, value)
    }

    pub(super) fn get_vpc_link(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = params.get("vpcLinkId").cloned().unwrap_or_default();
        let accounts = self.state.read();
        let v = accounts
            .get(&request_account(req))
            .and_then(|s| s.vpc_links.get(&id))
            .cloned()
            .ok_or_else(|| not_found("VpcLink not found"))?;
        ok(v)
    }

    pub(super) fn get_vpc_links(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let items: Vec<Value> = accounts
            .get(&request_account(req))
            .map(|s| s.vpc_links.values().cloned().collect())
            .unwrap_or_default();
        ok(json!({"item": items}))
    }

    pub(super) fn delete_vpc_link(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = params.get("vpcLinkId").cloned().unwrap_or_default();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        if state.vpc_links.remove(&id).is_none() {
            return Err(not_found("VpcLink not found"));
        }
        ok_no_content()
    }

    pub(super) fn update_vpc_link(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = params.get("vpcLinkId").cloned().unwrap_or_default();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        let v = state
            .vpc_links
            .get_mut(&id)
            .ok_or_else(|| not_found("VpcLink not found"))?;
        apply_patch_operations(req, |_op, path, value| {
            if let Some(o) = v.as_object_mut() {
                o.insert(path.trim_start_matches('/').to_string(), value.clone());
            }
        });
        ok(v.clone())
    }

    pub(super) fn create_domain_name(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let domain = body
            .get("domainName")
            .and_then(Value::as_str)
            .ok_or_else(|| bad_request("domainName is required"))?
            .to_string();
        let mut value = body.clone();
        if let Some(o) = value.as_object_mut() {
            // DomainName output shape (Smithy) excludes input-only certificate
            // bodies — strip before storing/returning so list/get/get-many
            // responses validate.
            for k in [
                "certificateBody",
                "certificatePrivateKey",
                "certificateChain",
            ] {
                o.remove(k);
            }
            o.insert(
                "regionalDomainName".to_string(),
                Value::String(format!("{domain}.fakecloud")),
            );
            o.insert(
                "domainNameStatus".to_string(),
                Value::String("AVAILABLE".to_string()),
            );
        }
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        state.domain_names.insert(domain, value.clone());
        ok_status(StatusCode::CREATED, value)
    }

    pub(super) fn get_domain_name(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let d = params.get("domainName").cloned().unwrap_or_default();
        let accounts = self.state.read();
        let v = accounts
            .get(&request_account(req))
            .and_then(|s| s.domain_names.get(&d))
            .cloned()
            .ok_or_else(|| not_found("DomainName not found"))?;
        ok(v)
    }

    pub(super) fn get_domain_names(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let items: Vec<Value> = accounts
            .get(&request_account(req))
            .map(|s| s.domain_names.values().cloned().collect())
            .unwrap_or_default();
        ok(json!({"item": items}))
    }

    pub(super) fn delete_domain_name(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let d = params.get("domainName").cloned().unwrap_or_default();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        if state.domain_names.remove(&d).is_none() {
            return Err(not_found("DomainName not found"));
        }
        state.base_path_mappings.remove(&d);
        ok_no_content()
    }

    pub(super) fn update_domain_name(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let d = params.get("domainName").cloned().unwrap_or_default();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        let v = state
            .domain_names
            .get_mut(&d)
            .ok_or_else(|| not_found("DomainName not found"))?;
        apply_patch_operations(req, |_op, path, value| {
            if let Some(o) = v.as_object_mut() {
                o.insert(path.trim_start_matches('/').to_string(), value.clone());
            }
        });
        ok(v.clone())
    }

    /// Cross-account access associations for private custom domain
    /// names. Stored as opaque JSON envelopes keyed by ARN. AWS uses
    /// these to model "account A can attach the private domain owned by
    /// account B"; fakecloud's multi-account model is ARN-routed and
    /// trusts the request, so we round-trip the envelope.
    pub(super) fn create_dnaa(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = make_id();
        let arn = format!(
            "arn:aws:apigateway:{}:{}:/domainnameaccessassociations/{}",
            req.region, req.account_id, id
        );
        let mut entry = body.clone();
        if let Some(obj) = entry.as_object_mut() {
            obj.insert(
                "domainNameAccessAssociationArn".to_string(),
                Value::String(arn.clone()),
            );
        }
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        state
            .domain_name_access_associations
            .insert(arn.clone(), entry.clone());
        ok_status(StatusCode::CREATED, entry)
    }

    pub(super) fn get_dnaas(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let items: Vec<Value> = accounts
            .get(&request_account(req))
            .map(|s| {
                s.domain_name_access_associations
                    .values()
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        ok(json!({"item": items}))
    }

    pub(super) fn delete_dnaa(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = params
            .get("domainNameAccessAssociationArn")
            .cloned()
            .unwrap_or_default();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        if state.domain_name_access_associations.remove(&arn).is_none() {
            return Err(not_found("DomainNameAccessAssociation not found"));
        }
        ok_no_content()
    }

    pub(super) fn reject_dnaa(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        // Reject deletes (rejects) the association on the receiving
        // account side. The arn arrives as a query string parameter.
        // AWS returns 202 with no body.
        let arn = params
            .get("domainNameAccessAssociationArn")
            .cloned()
            .unwrap_or_default();
        if arn.is_empty() {
            return Err(bad_request("domainNameAccessAssociationArn is required"));
        }
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        state.domain_name_access_associations.remove(&arn);
        ok_no_content()
    }

    /// Bulk import API keys from a CSV/JSON payload. fakecloud accepts
    /// both shapes; the v1 client typically POSTs a CSV blob with one
    /// `name,key,description,enabled` row per key. We parse what we can
    /// and create real `ApiKey` entries.
    pub(super) fn import_api_keys(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = String::from_utf8_lossy(req.body.as_ref()).to_string();
        let mut ids: Vec<String> = Vec::new();
        let mut warnings: Vec<String> = Vec::new();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        // Try JSON first.
        if let Ok(parsed) = serde_json::from_str::<Value>(&body) {
            if let Some(arr) = parsed.as_array() {
                for entry in arr {
                    let id = make_id();
                    let key = ApiKey {
                        id: id.clone(),
                        name: entry
                            .get("name")
                            .and_then(Value::as_str)
                            .unwrap_or("")
                            .to_string(),
                        description: entry
                            .get("description")
                            .and_then(Value::as_str)
                            .map(String::from),
                        enabled: entry
                            .get("enabled")
                            .and_then(Value::as_bool)
                            .unwrap_or(true),
                        value: entry
                            .get("value")
                            .and_then(Value::as_str)
                            .unwrap_or(&id)
                            .to_string(),
                        created_date: chrono::Utc::now(),
                        last_updated_date: chrono::Utc::now(),
                        stage_keys: Vec::new(),
                        tags: BTreeMap::new(),
                        customer_id: entry
                            .get("customerId")
                            .and_then(Value::as_str)
                            .map(String::from),
                    };
                    state.api_keys.insert(id.clone(), key);
                    ids.push(id);
                }
            } else {
                warnings.push("expected JSON array of api key entries".to_string());
            }
        } else {
            // CSV fallback: name,key,description,enabled
            for (n, line) in body.lines().enumerate() {
                let trimmed = line.trim();
                if trimmed.is_empty() || trimmed.starts_with('#') {
                    continue;
                }
                let cols: Vec<&str> = trimmed.split(',').collect();
                if cols.len() < 2 {
                    warnings.push(format!("line {n}: malformed CSV row"));
                    continue;
                }
                let id = make_id();
                let key = ApiKey {
                    id: id.clone(),
                    name: cols[0].to_string(),
                    description: cols.get(2).map(|s| s.to_string()),
                    enabled: cols
                        .get(3)
                        .map(|s| !matches!(s.trim().to_ascii_lowercase().as_str(), "false" | "0"))
                        .unwrap_or(true),
                    value: cols[1].to_string(),
                    created_date: chrono::Utc::now(),
                    last_updated_date: chrono::Utc::now(),
                    stage_keys: Vec::new(),
                    tags: BTreeMap::new(),
                    customer_id: None,
                };
                state.api_keys.insert(id.clone(), key);
                ids.push(id);
            }
        }
        ok_status(
            StatusCode::CREATED,
            json!({
                "ids": ids,
                "warnings": warnings,
            }),
        )
    }

    /// Bulk import documentation parts under a given REST API.
    pub(super) fn import_documentation_parts(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api_id = params.get("restApiId").cloned().unwrap_or_default();
        let body = req.json_body();
        let mut ids: Vec<String> = Vec::new();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        let parts = state.documentation_parts.entry(api_id.clone()).or_default();
        let entries = body
            .get("documentationParts")
            .or(Some(&body))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        for entry in entries {
            let id = make_id();
            let mut value = entry.clone();
            if let Some(obj) = value.as_object_mut() {
                obj.insert("id".to_string(), Value::String(id.clone()));
            }
            parts.insert(id.clone(), value);
            ids.push(id);
        }
        ok(json!({
            "ids": ids,
            "warnings": Vec::<String>::new(),
        }))
    }

    pub(super) fn create_base_path_mapping(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let domain = params.get("domainName").cloned().unwrap_or_default();
        let body = req.json_body();
        let base_path = body
            .get("basePath")
            .and_then(Value::as_str)
            .unwrap_or("(none)")
            .to_string();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        state
            .base_path_mappings
            .entry(domain)
            .or_default()
            .insert(base_path.clone(), body.clone());
        ok_status(StatusCode::CREATED, body)
    }

    pub(super) fn get_base_path_mapping(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let d = params.get("domainName").cloned().unwrap_or_default();
        let bp = params.get("basePath").cloned().unwrap_or_default();
        let accounts = self.state.read();
        let v = accounts
            .get(&request_account(req))
            .and_then(|s| s.base_path_mappings.get(&d))
            .and_then(|m| m.get(&bp))
            .cloned()
            .ok_or_else(|| not_found("BasePathMapping not found"))?;
        ok(v)
    }

    pub(super) fn get_base_path_mappings(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let d = params.get("domainName").cloned().unwrap_or_default();
        let accounts = self.state.read();
        let items: Vec<Value> = accounts
            .get(&request_account(req))
            .and_then(|s| s.base_path_mappings.get(&d))
            .map(|m| m.values().cloned().collect())
            .unwrap_or_default();
        ok(json!({"item": items}))
    }

    pub(super) fn delete_base_path_mapping(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let d = params.get("domainName").cloned().unwrap_or_default();
        let bp = params.get("basePath").cloned().unwrap_or_default();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        let map = state
            .base_path_mappings
            .get_mut(&d)
            .ok_or_else(|| not_found("BasePathMapping not found"))?;
        if map.remove(&bp).is_none() {
            return Err(not_found("BasePathMapping not found"));
        }
        ok_no_content()
    }

    pub(super) fn update_base_path_mapping(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let d = params.get("domainName").cloned().unwrap_or_default();
        let bp = params.get("basePath").cloned().unwrap_or_default();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        let map = state
            .base_path_mappings
            .get_mut(&d)
            .ok_or_else(|| not_found("BasePathMapping not found"))?;
        let v = map
            .get_mut(&bp)
            .ok_or_else(|| not_found("BasePathMapping not found"))?;
        apply_patch_operations(req, |_op, path, value| {
            if let Some(o) = v.as_object_mut() {
                o.insert(path.trim_start_matches('/').to_string(), value.clone());
            }
        });
        ok(v.clone())
    }

    pub(super) fn generate_client_cert(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = make_id();
        let mut value = req.json_body();
        let now = chrono::Utc::now();
        if !value.is_object() {
            value = json!({});
        }
        if let Some(o) = value.as_object_mut() {
            o.insert("clientCertificateId".to_string(), Value::String(id.clone()));
            o.insert(
                "createdDate".to_string(),
                Value::Number(serde_json::Number::from(now.timestamp())),
            );
            o.insert(
                "expirationDate".to_string(),
                Value::Number(serde_json::Number::from(
                    (now + chrono::Duration::days(365)).timestamp(),
                )),
            );
            o.insert(
                "pemEncodedCertificate".to_string(),
                Value::String(generate_client_certificate_pem()),
            );
        }
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        state.client_certificates.insert(id, value.clone());
        ok_status(StatusCode::CREATED, value)
    }

    pub(super) fn get_client_cert(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = params
            .get("clientCertificateId")
            .cloned()
            .unwrap_or_default();
        let accounts = self.state.read();
        let v = accounts
            .get(&request_account(req))
            .and_then(|s| s.client_certificates.get(&id))
            .cloned()
            .ok_or_else(|| not_found("ClientCertificate not found"))?;
        ok(v)
    }

    pub(super) fn get_client_certs(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let items: Vec<Value> = accounts
            .get(&request_account(req))
            .map(|s| s.client_certificates.values().cloned().collect())
            .unwrap_or_default();
        ok(json!({"item": items}))
    }

    pub(super) fn delete_client_cert(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = params
            .get("clientCertificateId")
            .cloned()
            .unwrap_or_default();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        if state.client_certificates.remove(&id).is_none() {
            return Err(not_found("ClientCertificate not found"));
        }
        ok_no_content()
    }

    pub(super) fn update_client_cert(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = params
            .get("clientCertificateId")
            .cloned()
            .unwrap_or_default();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        let v = state
            .client_certificates
            .get_mut(&id)
            .ok_or_else(|| not_found("ClientCertificate not found"))?;
        apply_patch_operations(req, |_op, path, value| {
            if let Some(o) = v.as_object_mut() {
                o.insert(path.trim_start_matches('/').to_string(), value.clone());
            }
        });
        ok(v.clone())
    }
}

/// Generate a real self-signed X.509 certificate (PEM) for the API Gateway
/// client certificate, so callers configuring backend mTLS trust get a
/// parseable cert instead of a placeholder string.
fn generate_client_certificate_pem() -> String {
    rcgen::generate_simple_self_signed(vec!["apigateway-client.amazonaws.com".to_string()])
        .map(|c| c.cert.pem())
        .unwrap_or_else(|_| {
            "-----BEGIN CERTIFICATE-----\nfakecloud-stub\n-----END CERTIFICATE-----".to_string()
        })
}
