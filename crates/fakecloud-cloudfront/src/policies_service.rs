//! Handlers for Batch 2 policy resources. The CRUD shape is identical
//! across all five resource types — every one of them parses a `*Config`
//! XML body, stores an ETag-versioned record, and emits a small
//! `<Resource><Id/><LastModifiedTime/><Config/></Resource>` envelope.
//! These handlers are split into their own module to keep `service.rs`
//! focused on the distribution / invalidation surface.

use std::collections::HashMap;

use chrono::Utc;
use http::{HeaderMap, StatusCode};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::policies::{
    empty, not_found, precondition_failed, render_oac, render_simple_policy, require_if_match,
    rfc3339, route_id, touch_account, xml_with_etag, CachePolicyConfig,
    ContinuousDeploymentPolicyConfig, OriginAccessControlConfig, OriginRequestPolicyConfig,
    PolicyView, ResponseHeadersPolicyConfig, StoredCachePolicy, StoredContinuousDeploymentPolicy,
    StoredOriginAccessControl, StoredOriginRequestPolicy, StoredResponseHeadersPolicy,
};
use crate::router::Route;
use crate::service::{
    aws_error, esc, generate_id_with_prefix, invalid_argument, xml_response, CloudFrontService,
    DEFAULT_ACCOUNT,
};
use crate::xml_io;

// ─── Origin Access Control ────────────────────────────────────────────

impl CloudFrontService {
    pub(crate) fn create_origin_access_control(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let cfg: OriginAccessControlConfig = xml_io::from_xml_root(&req.body)
            .map_err(|e| invalid_argument(format!("invalid OriginAccessControlConfig XML: {e}")))?;
        if cfg.name.is_empty() {
            return Err(invalid_argument(
                "OriginAccessControlConfig.Name is required",
            ));
        }
        let id = generate_id_with_prefix("E");
        let etag = generate_id_with_prefix("E");
        let stored = StoredOriginAccessControl {
            id: id.clone(),
            etag: etag.clone(),
            config: cfg,
        };
        let mut state = self.state.write();
        state
            .accounts
            .entry(DEFAULT_ACCOUNT.to_string())
            .or_default()
            .origin_access_controls
            .insert(id.clone(), stored.clone());
        drop(state);
        let body = render_oac(&stored);
        Ok(xml_with_etag(StatusCode::CREATED, body, &etag, Some(&id)))
    }

    pub(crate) fn get_origin_access_control(
        &self,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = route_id(route, "OriginAccessControl")?;
        let oac = self
            .shared_state()
            .read()
            .accounts
            .get(DEFAULT_ACCOUNT)
            .and_then(|a| a.origin_access_controls.get(&id).cloned())
            .ok_or_else(|| not_found("OriginAccessControl", &id))?;
        let body = render_oac(&oac);
        Ok(xml_with_etag(StatusCode::OK, body, &oac.etag, None))
    }

    pub(crate) fn get_origin_access_control_config(
        &self,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = route_id(route, "OriginAccessControl")?;
        let oac = self
            .shared_state()
            .read()
            .accounts
            .get(DEFAULT_ACCOUNT)
            .and_then(|a| a.origin_access_controls.get(&id).cloned())
            .ok_or_else(|| not_found("OriginAccessControl", &id))?;
        let body = quick_xml::se::to_string_with_root("OriginAccessControlConfig", &oac.config)
            .map(|inner| {
                format!(
                    "{XML_DECL}{}",
                    inner.replacen(
                        "<OriginAccessControlConfig>",
                        &format!(
                            "<OriginAccessControlConfig xmlns=\"{NS}\">",
                            NS = crate::NAMESPACE
                        ),
                        1,
                    ),
                    XML_DECL = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                )
            })
            .map_err(|e| {
                aws_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "InternalError",
                    format!("xml encode failed: {e}"),
                )
            })?;
        Ok(xml_with_etag(StatusCode::OK, body, &oac.etag, None))
    }

    pub(crate) fn update_origin_access_control(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = route_id(route, "OriginAccessControl")?;
        let if_match = require_if_match(req)?;
        let cfg: OriginAccessControlConfig = xml_io::from_xml_root(&req.body)
            .map_err(|e| invalid_argument(format!("invalid OriginAccessControlConfig XML: {e}")))?;
        if cfg.name.is_empty() {
            return Err(invalid_argument(
                "OriginAccessControlConfig.Name is required",
            ));
        }
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| not_found("OriginAccessControl", &id))?;
        let oac = account
            .origin_access_controls
            .get_mut(&id)
            .ok_or_else(|| not_found("OriginAccessControl", &id))?;
        if oac.etag != if_match {
            return Err(precondition_failed());
        }
        oac.config = cfg;
        oac.etag = generate_id_with_prefix("E");
        let snapshot = oac.clone();
        drop(state);
        let body = render_oac(&snapshot);
        Ok(xml_with_etag(StatusCode::OK, body, &snapshot.etag, None))
    }

    pub(crate) fn delete_origin_access_control(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = route_id(route, "OriginAccessControl")?;
        let if_match = require_if_match(req)?;
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| not_found("OriginAccessControl", &id))?;
        {
            let oac = account
                .origin_access_controls
                .get(&id)
                .ok_or_else(|| not_found("OriginAccessControl", &id))?;
            if oac.etag != if_match {
                return Err(precondition_failed());
            }
        }
        account.origin_access_controls.remove(&id);
        Ok(empty(StatusCode::NO_CONTENT))
    }

    pub(crate) fn list_origin_access_controls(
        &self,
        _req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let mut items: Vec<StoredOriginAccessControl> = state
            .accounts
            .values()
            .flat_map(|a| a.origin_access_controls.values().cloned())
            .collect();
        drop(state);
        items.sort_by(|a, b| a.id.cmp(&b.id));
        let mut body = String::new();
        body.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
        body.push_str(&format!(
            "<OriginAccessControlList xmlns=\"{NS}\">",
            NS = crate::NAMESPACE
        ));
        body.push_str("<Marker></Marker>");
        body.push_str("<MaxItems>100</MaxItems>");
        body.push_str("<IsTruncated>false</IsTruncated>");
        body.push_str(&format!("<Quantity>{}</Quantity>", items.len()));
        if !items.is_empty() {
            body.push_str("<Items>");
            for o in &items {
                body.push_str("<OriginAccessControlSummary>");
                body.push_str(&format!("<Id>{}</Id>", esc(&o.id)));
                body.push_str(&format!(
                    "<Description>{}</Description>",
                    esc(o.config.description.as_deref().unwrap_or(""))
                ));
                body.push_str(&format!("<Name>{}</Name>", esc(&o.config.name)));
                body.push_str(&format!(
                    "<SigningProtocol>{}</SigningProtocol>",
                    esc(&o.config.signing_protocol)
                ));
                body.push_str(&format!(
                    "<SigningBehavior>{}</SigningBehavior>",
                    esc(&o.config.signing_behavior)
                ));
                body.push_str(&format!(
                    "<OriginAccessControlOriginType>{}</OriginAccessControlOriginType>",
                    esc(&o.config.origin_access_control_origin_type)
                ));
                body.push_str("</OriginAccessControlSummary>");
            }
            body.push_str("</Items>");
        }
        body.push_str("</OriginAccessControlList>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }
}

// ─── Cache Policy ─────────────────────────────────────────────────────

impl CloudFrontService {
    pub(crate) fn create_cache_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let cfg: CachePolicyConfig = xml_io::from_xml_root(&req.body)
            .map_err(|e| invalid_argument(format!("invalid CachePolicyConfig XML: {e}")))?;
        if cfg.name.is_empty() {
            return Err(invalid_argument("CachePolicyConfig.Name is required"));
        }
        touch_account(&self.state, DEFAULT_ACCOUNT);
        let id = generate_id_with_prefix("C");
        let etag = generate_id_with_prefix("E");
        let stored = StoredCachePolicy {
            id: id.clone(),
            etag: etag.clone(),
            last_modified_time: Utc::now(),
            config: cfg,
            policy_type: "custom".to_string(),
        };
        let mut state = self.state.write();
        state
            .accounts
            .entry(DEFAULT_ACCOUNT.to_string())
            .or_default()
            .cache_policies
            .insert(id.clone(), stored.clone());
        drop(state);
        let body = render_simple_policy(PolicyView::from(stored), "CachePolicy");
        Ok(xml_with_etag(StatusCode::CREATED, body, &etag, Some(&id)))
    }

    pub(crate) fn get_cache_policy(&self, route: &Route) -> Result<AwsResponse, AwsServiceError> {
        let id = route_id(route, "CachePolicy")?;
        touch_account(&self.state, DEFAULT_ACCOUNT);
        let cp = self
            .shared_state()
            .read()
            .accounts
            .get(DEFAULT_ACCOUNT)
            .and_then(|a| a.cache_policies.get(&id).cloned())
            .ok_or_else(|| not_found("CachePolicy", &id))?;
        let body = render_simple_policy(PolicyView::from(cp.clone()), "CachePolicy");
        Ok(xml_with_etag(StatusCode::OK, body, &cp.etag, None))
    }

    pub(crate) fn get_cache_policy_config(
        &self,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = route_id(route, "CachePolicy")?;
        touch_account(&self.state, DEFAULT_ACCOUNT);
        let cp = self
            .shared_state()
            .read()
            .accounts
            .get(DEFAULT_ACCOUNT)
            .and_then(|a| a.cache_policies.get(&id).cloned())
            .ok_or_else(|| not_found("CachePolicy", &id))?;
        let body = config_xml("CachePolicyConfig", &cp.config)?;
        Ok(xml_with_etag(StatusCode::OK, body, &cp.etag, None))
    }

    pub(crate) fn update_cache_policy(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = route_id(route, "CachePolicy")?;
        let if_match = require_if_match(req)?;
        let cfg: CachePolicyConfig = xml_io::from_xml_root(&req.body)
            .map_err(|e| invalid_argument(format!("invalid CachePolicyConfig XML: {e}")))?;
        if cfg.name.is_empty() {
            return Err(invalid_argument("CachePolicyConfig.Name is required"));
        }
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| not_found("CachePolicy", &id))?;
        let cp = account
            .cache_policies
            .get_mut(&id)
            .ok_or_else(|| not_found("CachePolicy", &id))?;
        if cp.policy_type == "managed" {
            return Err(aws_error(
                StatusCode::FORBIDDEN,
                "IllegalUpdate",
                "Managed cache policies cannot be updated",
            ));
        }
        if cp.etag != if_match {
            return Err(precondition_failed());
        }
        cp.config = cfg;
        cp.etag = generate_id_with_prefix("E");
        cp.last_modified_time = Utc::now();
        let snapshot = cp.clone();
        drop(state);
        let body = render_simple_policy(PolicyView::from(snapshot.clone()), "CachePolicy");
        Ok(xml_with_etag(StatusCode::OK, body, &snapshot.etag, None))
    }

    pub(crate) fn delete_cache_policy(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = route_id(route, "CachePolicy")?;
        let if_match = require_if_match(req)?;
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| not_found("CachePolicy", &id))?;
        {
            let cp = account
                .cache_policies
                .get(&id)
                .ok_or_else(|| not_found("CachePolicy", &id))?;
            if cp.policy_type == "managed" {
                return Err(aws_error(
                    StatusCode::FORBIDDEN,
                    "IllegalDelete",
                    "Managed cache policies cannot be deleted",
                ));
            }
            if cp.etag != if_match {
                return Err(precondition_failed());
            }
        }
        account.cache_policies.remove(&id);
        Ok(empty(StatusCode::NO_CONTENT))
    }

    pub(crate) fn list_cache_policies(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        touch_account(&self.state, DEFAULT_ACCOUNT);
        let filter = validate_policy_type_query(&req.raw_query)?;
        let state = self.state.read();
        let mut items: Vec<StoredCachePolicy> = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .map(|a| a.cache_policies.values().cloned().collect())
            .unwrap_or_default();
        drop(state);
        if let Some(t) = filter.as_deref() {
            items.retain(|p| p.policy_type == t);
        }
        items.sort_by(|a, b| a.config.name.cmp(&b.config.name));
        let mut body = String::new();
        body.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
        body.push_str(&format!(
            "<CachePolicyList xmlns=\"{NS}\">",
            NS = crate::NAMESPACE
        ));
        body.push_str("<MaxItems>100</MaxItems>");
        body.push_str(&format!("<Quantity>{}</Quantity>", items.len()));
        if !items.is_empty() {
            body.push_str("<Items>");
            for cp in &items {
                body.push_str("<CachePolicySummary>");
                body.push_str(&format!("<Type>{}</Type>", esc(&cp.policy_type)));
                body.push_str("<CachePolicy>");
                body.push_str(&format!("<Id>{}</Id>", esc(&cp.id)));
                body.push_str(&format!(
                    "<LastModifiedTime>{}</LastModifiedTime>",
                    rfc3339(&cp.last_modified_time)
                ));
                body.push_str(
                    &quick_xml::se::to_string_with_root("CachePolicyConfig", &cp.config)
                        .unwrap_or_default(),
                );
                body.push_str("</CachePolicy>");
                body.push_str("</CachePolicySummary>");
            }
            body.push_str("</Items>");
        }
        body.push_str("</CachePolicyList>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }
}

// ─── Origin Request Policy ────────────────────────────────────────────

impl CloudFrontService {
    pub(crate) fn create_origin_request_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let cfg: OriginRequestPolicyConfig = xml_io::from_xml_root(&req.body)
            .map_err(|e| invalid_argument(format!("invalid OriginRequestPolicyConfig XML: {e}")))?;
        if cfg.name.is_empty() {
            return Err(invalid_argument(
                "OriginRequestPolicyConfig.Name is required",
            ));
        }
        touch_account(&self.state, DEFAULT_ACCOUNT);
        let id = generate_id_with_prefix("O");
        let etag = generate_id_with_prefix("E");
        let stored = StoredOriginRequestPolicy {
            id: id.clone(),
            etag: etag.clone(),
            last_modified_time: Utc::now(),
            config: cfg,
            policy_type: "custom".to_string(),
        };
        let mut state = self.state.write();
        state
            .accounts
            .entry(DEFAULT_ACCOUNT.to_string())
            .or_default()
            .origin_request_policies
            .insert(id.clone(), stored.clone());
        drop(state);
        let body = render_simple_policy(PolicyView::from(stored), "OriginRequestPolicy");
        Ok(xml_with_etag(StatusCode::CREATED, body, &etag, Some(&id)))
    }

    pub(crate) fn get_origin_request_policy(
        &self,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = route_id(route, "OriginRequestPolicy")?;
        touch_account(&self.state, DEFAULT_ACCOUNT);
        let p = self
            .shared_state()
            .read()
            .accounts
            .get(DEFAULT_ACCOUNT)
            .and_then(|a| a.origin_request_policies.get(&id).cloned())
            .ok_or_else(|| not_found("OriginRequestPolicy", &id))?;
        let body = render_simple_policy(PolicyView::from(p.clone()), "OriginRequestPolicy");
        Ok(xml_with_etag(StatusCode::OK, body, &p.etag, None))
    }

    pub(crate) fn get_origin_request_policy_config(
        &self,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = route_id(route, "OriginRequestPolicy")?;
        touch_account(&self.state, DEFAULT_ACCOUNT);
        let p = self
            .shared_state()
            .read()
            .accounts
            .get(DEFAULT_ACCOUNT)
            .and_then(|a| a.origin_request_policies.get(&id).cloned())
            .ok_or_else(|| not_found("OriginRequestPolicy", &id))?;
        let body = config_xml("OriginRequestPolicyConfig", &p.config)?;
        Ok(xml_with_etag(StatusCode::OK, body, &p.etag, None))
    }

    pub(crate) fn update_origin_request_policy(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = route_id(route, "OriginRequestPolicy")?;
        let if_match = require_if_match(req)?;
        let cfg: OriginRequestPolicyConfig = xml_io::from_xml_root(&req.body)
            .map_err(|e| invalid_argument(format!("invalid OriginRequestPolicyConfig XML: {e}")))?;
        if cfg.name.is_empty() {
            return Err(invalid_argument(
                "OriginRequestPolicyConfig.Name is required",
            ));
        }
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| not_found("OriginRequestPolicy", &id))?;
        let p = account
            .origin_request_policies
            .get_mut(&id)
            .ok_or_else(|| not_found("OriginRequestPolicy", &id))?;
        if p.policy_type == "managed" {
            return Err(aws_error(
                StatusCode::FORBIDDEN,
                "IllegalUpdate",
                "Managed origin request policies cannot be updated",
            ));
        }
        if p.etag != if_match {
            return Err(precondition_failed());
        }
        p.config = cfg;
        p.etag = generate_id_with_prefix("E");
        p.last_modified_time = Utc::now();
        let snapshot = p.clone();
        drop(state);
        let body = render_simple_policy(PolicyView::from(snapshot.clone()), "OriginRequestPolicy");
        Ok(xml_with_etag(StatusCode::OK, body, &snapshot.etag, None))
    }

    pub(crate) fn delete_origin_request_policy(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = route_id(route, "OriginRequestPolicy")?;
        let if_match = require_if_match(req)?;
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| not_found("OriginRequestPolicy", &id))?;
        {
            let p = account
                .origin_request_policies
                .get(&id)
                .ok_or_else(|| not_found("OriginRequestPolicy", &id))?;
            if p.policy_type == "managed" {
                return Err(aws_error(
                    StatusCode::FORBIDDEN,
                    "IllegalDelete",
                    "Managed origin request policies cannot be deleted",
                ));
            }
            if p.etag != if_match {
                return Err(precondition_failed());
            }
        }
        account.origin_request_policies.remove(&id);
        Ok(empty(StatusCode::NO_CONTENT))
    }

    pub(crate) fn list_origin_request_policies(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        touch_account(&self.state, DEFAULT_ACCOUNT);
        let filter = validate_policy_type_query(&req.raw_query)?;
        let state = self.state.read();
        let mut items: Vec<StoredOriginRequestPolicy> = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .map(|a| a.origin_request_policies.values().cloned().collect())
            .unwrap_or_default();
        drop(state);
        if let Some(t) = filter.as_deref() {
            items.retain(|p| p.policy_type == t);
        }
        items.sort_by(|a, b| a.config.name.cmp(&b.config.name));
        let mut body = String::new();
        body.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
        body.push_str(&format!(
            "<OriginRequestPolicyList xmlns=\"{NS}\">",
            NS = crate::NAMESPACE
        ));
        body.push_str("<MaxItems>100</MaxItems>");
        body.push_str(&format!("<Quantity>{}</Quantity>", items.len()));
        if !items.is_empty() {
            body.push_str("<Items>");
            for p in &items {
                body.push_str("<OriginRequestPolicySummary>");
                body.push_str(&format!("<Type>{}</Type>", esc(&p.policy_type)));
                body.push_str("<OriginRequestPolicy>");
                body.push_str(&format!("<Id>{}</Id>", esc(&p.id)));
                body.push_str(&format!(
                    "<LastModifiedTime>{}</LastModifiedTime>",
                    rfc3339(&p.last_modified_time)
                ));
                body.push_str(
                    &quick_xml::se::to_string_with_root("OriginRequestPolicyConfig", &p.config)
                        .unwrap_or_default(),
                );
                body.push_str("</OriginRequestPolicy>");
                body.push_str("</OriginRequestPolicySummary>");
            }
            body.push_str("</Items>");
        }
        body.push_str("</OriginRequestPolicyList>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }
}

// ─── Response Headers Policy ──────────────────────────────────────────

impl CloudFrontService {
    pub(crate) fn create_response_headers_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let cfg: ResponseHeadersPolicyConfig = xml_io::from_xml_root(&req.body).map_err(|e| {
            invalid_argument(format!("invalid ResponseHeadersPolicyConfig XML: {e}"))
        })?;
        if cfg.name.is_empty() {
            return Err(invalid_argument(
                "ResponseHeadersPolicyConfig.Name is required",
            ));
        }
        touch_account(&self.state, DEFAULT_ACCOUNT);
        let id = generate_id_with_prefix("R");
        let etag = generate_id_with_prefix("E");
        let stored = StoredResponseHeadersPolicy {
            id: id.clone(),
            etag: etag.clone(),
            last_modified_time: Utc::now(),
            config: cfg,
            policy_type: "custom".to_string(),
        };
        let mut state = self.state.write();
        state
            .accounts
            .entry(DEFAULT_ACCOUNT.to_string())
            .or_default()
            .response_headers_policies
            .insert(id.clone(), stored.clone());
        drop(state);
        let body = render_simple_policy(PolicyView::from(stored), "ResponseHeadersPolicy");
        Ok(xml_with_etag(StatusCode::CREATED, body, &etag, Some(&id)))
    }

    pub(crate) fn get_response_headers_policy(
        &self,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = route_id(route, "ResponseHeadersPolicy")?;
        touch_account(&self.state, DEFAULT_ACCOUNT);
        let p = self
            .shared_state()
            .read()
            .accounts
            .get(DEFAULT_ACCOUNT)
            .and_then(|a| a.response_headers_policies.get(&id).cloned())
            .ok_or_else(|| not_found("ResponseHeadersPolicy", &id))?;
        let body = render_simple_policy(PolicyView::from(p.clone()), "ResponseHeadersPolicy");
        Ok(xml_with_etag(StatusCode::OK, body, &p.etag, None))
    }

    pub(crate) fn get_response_headers_policy_config(
        &self,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = route_id(route, "ResponseHeadersPolicy")?;
        touch_account(&self.state, DEFAULT_ACCOUNT);
        let p = self
            .shared_state()
            .read()
            .accounts
            .get(DEFAULT_ACCOUNT)
            .and_then(|a| a.response_headers_policies.get(&id).cloned())
            .ok_or_else(|| not_found("ResponseHeadersPolicy", &id))?;
        let body = config_xml("ResponseHeadersPolicyConfig", &p.config)?;
        Ok(xml_with_etag(StatusCode::OK, body, &p.etag, None))
    }

    pub(crate) fn update_response_headers_policy(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = route_id(route, "ResponseHeadersPolicy")?;
        let if_match = require_if_match(req)?;
        let cfg: ResponseHeadersPolicyConfig = xml_io::from_xml_root(&req.body).map_err(|e| {
            invalid_argument(format!("invalid ResponseHeadersPolicyConfig XML: {e}"))
        })?;
        if cfg.name.is_empty() {
            return Err(invalid_argument(
                "ResponseHeadersPolicyConfig.Name is required",
            ));
        }
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| not_found("ResponseHeadersPolicy", &id))?;
        let p = account
            .response_headers_policies
            .get_mut(&id)
            .ok_or_else(|| not_found("ResponseHeadersPolicy", &id))?;
        if p.policy_type == "managed" {
            return Err(aws_error(
                StatusCode::FORBIDDEN,
                "IllegalUpdate",
                "Managed response headers policies cannot be updated",
            ));
        }
        if p.etag != if_match {
            return Err(precondition_failed());
        }
        p.config = cfg;
        p.etag = generate_id_with_prefix("E");
        p.last_modified_time = Utc::now();
        let snapshot = p.clone();
        drop(state);
        let body =
            render_simple_policy(PolicyView::from(snapshot.clone()), "ResponseHeadersPolicy");
        Ok(xml_with_etag(StatusCode::OK, body, &snapshot.etag, None))
    }

    pub(crate) fn delete_response_headers_policy(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = route_id(route, "ResponseHeadersPolicy")?;
        let if_match = require_if_match(req)?;
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| not_found("ResponseHeadersPolicy", &id))?;
        {
            let p = account
                .response_headers_policies
                .get(&id)
                .ok_or_else(|| not_found("ResponseHeadersPolicy", &id))?;
            if p.policy_type == "managed" {
                return Err(aws_error(
                    StatusCode::FORBIDDEN,
                    "IllegalDelete",
                    "Managed response headers policies cannot be deleted",
                ));
            }
            if p.etag != if_match {
                return Err(precondition_failed());
            }
        }
        account.response_headers_policies.remove(&id);
        Ok(empty(StatusCode::NO_CONTENT))
    }

    pub(crate) fn list_response_headers_policies(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        touch_account(&self.state, DEFAULT_ACCOUNT);
        let filter = validate_policy_type_query(&req.raw_query)?;
        let state = self.state.read();
        let mut items: Vec<StoredResponseHeadersPolicy> = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .map(|a| a.response_headers_policies.values().cloned().collect())
            .unwrap_or_default();
        drop(state);
        if let Some(t) = filter.as_deref() {
            items.retain(|p| p.policy_type == t);
        }
        items.sort_by(|a, b| a.config.name.cmp(&b.config.name));
        let mut body = String::new();
        body.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
        body.push_str(&format!(
            "<ResponseHeadersPolicyList xmlns=\"{NS}\">",
            NS = crate::NAMESPACE
        ));
        body.push_str("<MaxItems>100</MaxItems>");
        body.push_str(&format!("<Quantity>{}</Quantity>", items.len()));
        if !items.is_empty() {
            body.push_str("<Items>");
            for p in &items {
                body.push_str("<ResponseHeadersPolicySummary>");
                body.push_str(&format!("<Type>{}</Type>", esc(&p.policy_type)));
                body.push_str("<ResponseHeadersPolicy>");
                body.push_str(&format!("<Id>{}</Id>", esc(&p.id)));
                body.push_str(&format!(
                    "<LastModifiedTime>{}</LastModifiedTime>",
                    rfc3339(&p.last_modified_time)
                ));
                body.push_str(
                    &quick_xml::se::to_string_with_root("ResponseHeadersPolicyConfig", &p.config)
                        .unwrap_or_default(),
                );
                body.push_str("</ResponseHeadersPolicy>");
                body.push_str("</ResponseHeadersPolicySummary>");
            }
            body.push_str("</Items>");
        }
        body.push_str("</ResponseHeadersPolicyList>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }
}

// ─── Continuous Deployment Policy ─────────────────────────────────────

impl CloudFrontService {
    pub(crate) fn create_continuous_deployment_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let cfg: ContinuousDeploymentPolicyConfig =
            xml_io::from_xml_root(&req.body).map_err(|e| {
                invalid_argument(format!("invalid ContinuousDeploymentPolicyConfig XML: {e}"))
            })?;
        let id = generate_id_with_prefix("D");
        let etag = generate_id_with_prefix("E");
        let stored = StoredContinuousDeploymentPolicy {
            id: id.clone(),
            etag: etag.clone(),
            last_modified_time: Utc::now(),
            config: cfg,
        };
        let mut state = self.state.write();
        state
            .accounts
            .entry(DEFAULT_ACCOUNT.to_string())
            .or_default()
            .continuous_deployment_policies
            .insert(id.clone(), stored.clone());
        drop(state);
        let body = render_simple_policy(PolicyView::from(stored), "ContinuousDeploymentPolicy");
        Ok(xml_with_etag(StatusCode::CREATED, body, &etag, Some(&id)))
    }

    pub(crate) fn get_continuous_deployment_policy(
        &self,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = route_id(route, "ContinuousDeploymentPolicy")?;
        let cdp = self
            .shared_state()
            .read()
            .accounts
            .get(DEFAULT_ACCOUNT)
            .and_then(|a| a.continuous_deployment_policies.get(&id).cloned())
            .ok_or_else(|| not_found("ContinuousDeploymentPolicy", &id))?;
        let body =
            render_simple_policy(PolicyView::from(cdp.clone()), "ContinuousDeploymentPolicy");
        Ok(xml_with_etag(StatusCode::OK, body, &cdp.etag, None))
    }

    pub(crate) fn get_continuous_deployment_policy_config(
        &self,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = route_id(route, "ContinuousDeploymentPolicy")?;
        let cdp = self
            .shared_state()
            .read()
            .accounts
            .get(DEFAULT_ACCOUNT)
            .and_then(|a| a.continuous_deployment_policies.get(&id).cloned())
            .ok_or_else(|| not_found("ContinuousDeploymentPolicy", &id))?;
        let body = config_xml("ContinuousDeploymentPolicyConfig", &cdp.config)?;
        Ok(xml_with_etag(StatusCode::OK, body, &cdp.etag, None))
    }

    pub(crate) fn update_continuous_deployment_policy(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = route_id(route, "ContinuousDeploymentPolicy")?;
        let if_match = require_if_match(req)?;
        let cfg: ContinuousDeploymentPolicyConfig =
            xml_io::from_xml_root(&req.body).map_err(|e| {
                invalid_argument(format!("invalid ContinuousDeploymentPolicyConfig XML: {e}"))
            })?;
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| not_found("ContinuousDeploymentPolicy", &id))?;
        let cdp = account
            .continuous_deployment_policies
            .get_mut(&id)
            .ok_or_else(|| not_found("ContinuousDeploymentPolicy", &id))?;
        if cdp.etag != if_match {
            return Err(precondition_failed());
        }
        cdp.config = cfg;
        cdp.etag = generate_id_with_prefix("E");
        cdp.last_modified_time = Utc::now();
        let snapshot = cdp.clone();
        drop(state);
        let body = render_simple_policy(
            PolicyView::from(snapshot.clone()),
            "ContinuousDeploymentPolicy",
        );
        Ok(xml_with_etag(StatusCode::OK, body, &snapshot.etag, None))
    }

    pub(crate) fn delete_continuous_deployment_policy(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = route_id(route, "ContinuousDeploymentPolicy")?;
        let if_match = require_if_match(req)?;
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| not_found("ContinuousDeploymentPolicy", &id))?;
        {
            let cdp = account
                .continuous_deployment_policies
                .get(&id)
                .ok_or_else(|| not_found("ContinuousDeploymentPolicy", &id))?;
            if cdp.etag != if_match {
                return Err(precondition_failed());
            }
        }
        account.continuous_deployment_policies.remove(&id);
        Ok(empty(StatusCode::NO_CONTENT))
    }

    pub(crate) fn list_continuous_deployment_policies(
        &self,
        _req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let mut items: Vec<StoredContinuousDeploymentPolicy> = state
            .accounts
            .values()
            .flat_map(|a| a.continuous_deployment_policies.values().cloned())
            .collect();
        drop(state);
        items.sort_by(|a, b| a.id.cmp(&b.id));
        let mut body = String::new();
        body.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
        body.push_str(&format!(
            "<ContinuousDeploymentPolicyList xmlns=\"{NS}\">",
            NS = crate::NAMESPACE
        ));
        body.push_str("<MaxItems>100</MaxItems>");
        body.push_str(&format!("<Quantity>{}</Quantity>", items.len()));
        if !items.is_empty() {
            body.push_str("<Items>");
            for cdp in &items {
                body.push_str("<ContinuousDeploymentPolicySummary>");
                body.push_str("<ContinuousDeploymentPolicy>");
                body.push_str(&format!("<Id>{}</Id>", esc(&cdp.id)));
                body.push_str(&format!(
                    "<LastModifiedTime>{}</LastModifiedTime>",
                    rfc3339(&cdp.last_modified_time)
                ));
                body.push_str(
                    &quick_xml::se::to_string_with_root(
                        "ContinuousDeploymentPolicyConfig",
                        &cdp.config,
                    )
                    .unwrap_or_default(),
                );
                body.push_str("</ContinuousDeploymentPolicy>");
                body.push_str("</ContinuousDeploymentPolicySummary>");
            }
            body.push_str("</Items>");
        }
        body.push_str("</ContinuousDeploymentPolicyList>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }
}

// ─── Helpers ──────────────────────────────────────────────────────────

fn config_xml<T: serde::Serialize>(root: &str, cfg: &T) -> Result<String, AwsServiceError> {
    let inner = quick_xml::se::to_string_with_root(root, cfg).map_err(|e| {
        aws_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalError",
            format!("xml encode failed: {e}"),
        )
    })?;
    let stamped = inner.replacen(
        &format!("<{root}>"),
        &format!("<{root} xmlns=\"{NS}\">", NS = crate::NAMESPACE),
        1,
    );
    Ok(format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>{stamped}"
    ))
}

fn parse_type_filter(query: &str) -> Option<String> {
    let pairs: HashMap<&str, &str> = query.split('&').filter_map(|p| p.split_once('=')).collect();
    pairs.get("Type").map(|v| v.to_string())
}

/// Validate the `Type` query parameter against the
/// CachePolicyType / OriginRequestPolicyType / ResponseHeadersPolicyType
/// enum (all three share the same `managed` | `custom` value space). When
/// present and unknown, return InvalidArgument; absent or valid is OK.
pub(crate) fn validate_policy_type_query(
    query: &str,
) -> Result<Option<String>, fakecloud_core::service::AwsServiceError> {
    let filter = parse_type_filter(query);
    if let Some(ref v) = filter {
        if v != "managed" && v != "custom" {
            return Err(crate::service::invalid_argument(format!(
                "Type must be one of 'managed' or 'custom', got '{v}'"
            )));
        }
    }
    Ok(filter)
}
