// Handlers for CloudFront DistributionTenant ops (12 ops). Same
// REST-XML conventions as Distribution: ETag-based concurrency,
// IDs prefixed with `dt-`, ARNs under `arn:aws:cloudfront::`.

use chrono::Utc;
use http::header::LOCATION;
use http::{HeaderMap, StatusCode};
use uuid::Uuid;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::policies::{
    not_found, precondition_failed, require_if_match, rfc3339, route_id, xml_with_etag,
};
use crate::router::Route;
use crate::service::{
    aws_error, esc, generate_id_with_prefix, invalid_argument, xml_response, CloudFrontService,
    DEFAULT_ACCOUNT,
};
use crate::state::Tag;
use crate::tenants::{
    StoredDistributionTenant, StoredTenantInvalidation, TenantCustomizations,
    TenantGeoRestrictionCustomization, TenantManagedCertificateRequest, TenantParameter,
    TenantWebAclCustomization,
};
use crate::xml_io;

const NS: &str = crate::NAMESPACE;
const XML_DECL: &str = r#"<?xml version="1.0" encoding="UTF-8"?>"#;

#[derive(Debug, Default, serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
struct CreateDistributionTenantRequest {
    pub distribution_id: String,
    pub name: String,
    #[serde(default)]
    pub domains: Option<DomainItems>,
    #[serde(default)]
    pub connection_group_id: Option<String>,
    #[serde(default)]
    pub enabled: Option<bool>,
    #[serde(default)]
    pub parameters: Option<ParametersReq>,
    #[serde(default)]
    pub customizations: Option<CustomizationsReq>,
    #[serde(default)]
    pub managed_certificate_request: Option<ManagedCertificateRequestReq>,
    #[serde(default)]
    pub tags: Option<TagsReq>,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
struct UpdateDistributionTenantRequest {
    #[serde(default)]
    pub distribution_id: Option<String>,
    #[serde(default)]
    pub domains: Option<DomainItems>,
    #[serde(default)]
    pub connection_group_id: Option<String>,
    #[serde(default)]
    pub enabled: Option<bool>,
    #[serde(default)]
    pub parameters: Option<ParametersReq>,
    #[serde(default)]
    pub customizations: Option<CustomizationsReq>,
    #[serde(default)]
    pub managed_certificate_request: Option<ManagedCertificateRequestReq>,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ParametersReq {
    #[serde(default, rename = "Parameter")]
    pub parameter: Vec<ParameterReq>,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ParameterReq {
    pub name: String,
    #[serde(default)]
    pub value: String,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
struct CustomizationsReq {
    #[serde(default)]
    pub web_acl: Option<WebAclReq>,
    #[serde(default)]
    pub certificate: Option<CertificateReq>,
    #[serde(default)]
    pub geo_restrictions: Option<GeoRestrictionsReq>,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
struct WebAclReq {
    #[serde(default)]
    pub action: String,
    #[serde(default, rename = "Arn")]
    pub arn: Option<String>,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
struct CertificateReq {
    #[serde(rename = "Arn")]
    pub arn: String,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GeoRestrictionsReq {
    #[serde(default)]
    pub restriction_type: String,
    #[serde(default)]
    pub locations: Option<LocationsReq>,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
struct LocationsReq {
    #[serde(default, rename = "Location")]
    pub location: Vec<String>,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ManagedCertificateRequestReq {
    #[serde(default)]
    pub validation_token_host: Option<String>,
    #[serde(default)]
    pub primary_domain_name: Option<String>,
    #[serde(default)]
    pub certificate_transparency_logging_preference: Option<String>,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
struct TagsReq {
    #[serde(default)]
    pub items: Option<TagItemsReq>,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
struct TagItemsReq {
    #[serde(default, rename = "Tag")]
    pub tag: Vec<TagReq>,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
struct TagReq {
    pub key: String,
    #[serde(default)]
    pub value: Option<String>,
}

#[derive(Debug, Default, serde::Deserialize)]
struct DomainItems {
    #[serde(default, rename = "member")]
    pub members: Vec<DomainItem>,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
struct DomainItem {
    pub domain: String,
}

#[derive(Debug, Default, serde::Deserialize)]
struct AssociateWebAclRequest {
    #[serde(rename = "WebACLArn")]
    pub web_acl_arn: String,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
struct InvalidationBatchRequest {
    pub paths: PathsItems,
    pub caller_reference: String,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
struct PathsItems {
    #[serde(default)]
    pub items: Option<PathItems>,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
struct PathItems {
    #[serde(default, rename = "Path")]
    pub path: Vec<String>,
}

impl CloudFrontService {
    pub(crate) fn create_distribution_tenant(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let parsed: CreateDistributionTenantRequest =
            xml_io::from_xml_root(&req.body).map_err(|e| {
                invalid_argument(format!("invalid CreateDistributionTenantRequest XML: {e}"))
            })?;
        if parsed.distribution_id.is_empty() {
            return Err(invalid_argument("DistributionId is required"));
        }
        if parsed.name.is_empty() {
            return Err(invalid_argument("Name is required"));
        }
        let mut state = self.state.write();
        let account = state
            .accounts
            .entry(DEFAULT_ACCOUNT.to_string())
            .or_default();
        if account
            .distribution_tenants
            .values()
            .any(|t| t.name == parsed.name)
        {
            return Err(aws_error(
                StatusCode::CONFLICT,
                "EntityAlreadyExists",
                format!("DistributionTenant {} already exists", parsed.name),
            ));
        }
        let id = generate_tenant_id();
        let arn = format!(
            "arn:aws:cloudfront::{}:distribution-tenant/{}",
            DEFAULT_ACCOUNT, id
        );
        let etag = generate_id_with_prefix("E");
        let now = Utc::now();
        let domains = parsed
            .domains
            .map(|d| d.members.into_iter().map(|i| i.domain).collect())
            .unwrap_or_default();
        let customizations = parsed.customizations.map(convert_customizations);
        let web_acl_arn = customizations
            .as_ref()
            .and_then(|c| c.web_acl.as_ref())
            .and_then(|w| w.arn.clone());
        let stored = StoredDistributionTenant {
            id: id.clone(),
            arn: arn.clone(),
            name: parsed.name,
            distribution_id: parsed.distribution_id,
            domains,
            connection_group_id: parsed.connection_group_id,
            web_acl_arn,
            enabled: parsed.enabled.unwrap_or(true),
            status: "InProgress".to_string(),
            etag: etag.clone(),
            created_time: now,
            last_modified_time: now,
            parameters: convert_parameters(parsed.parameters),
            customizations,
            managed_certificate_request: parsed
                .managed_certificate_request
                .map(convert_managed_cert_request),
        };
        account
            .distribution_tenants
            .insert(id.clone(), stored.clone());
        if let Some(tags) = parsed.tags {
            let converted: Vec<Tag> = tags
                .items
                .map(|i| {
                    i.tag
                        .into_iter()
                        .map(|t| Tag {
                            key: t.key,
                            value: t.value,
                        })
                        .collect()
                })
                .unwrap_or_default();
            if !converted.is_empty() {
                account.tags.entry(arn).or_default().extend(converted);
            }
        }
        drop(state);
        self.schedule_distribution_tenant_deploy(id.clone());
        let body = render_distribution_tenant(&stored);
        Ok(xml_with_etag(StatusCode::CREATED, body, &etag, Some(&id)))
    }

    pub(crate) fn get_distribution_tenant(
        &self,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = route_id(route, "DistributionTenant")?;
        let state = self.state.read();
        let t = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .and_then(|a| a.distribution_tenants.get(&id).cloned())
            .ok_or_else(|| not_found("DistributionTenant", &id))?;
        drop(state);
        let body = render_distribution_tenant(&t);
        Ok(xml_with_etag(StatusCode::OK, body, &t.etag, None))
    }

    pub(crate) fn get_distribution_tenant_by_domain(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let domain = req
            .query_params
            .get("domain")
            .or_else(|| req.query_params.get("Domain"))
            .cloned()
            .ok_or_else(|| invalid_argument("Domain query parameter is required"))?;
        let state = self.state.read();
        let t = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .and_then(|a| {
                a.distribution_tenants
                    .values()
                    .find(|t| t.domains.iter().any(|d| d == &domain))
                    .cloned()
            })
            .ok_or_else(|| not_found("DistributionTenant", &domain))?;
        drop(state);
        let body = render_distribution_tenant(&t);
        Ok(xml_with_etag(StatusCode::OK, body, &t.etag, None))
    }

    pub(crate) fn update_distribution_tenant(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = route_id(route, "DistributionTenant")?;
        let if_match = require_if_match(req)?;
        let parsed: UpdateDistributionTenantRequest =
            xml_io::from_xml_root(&req.body).map_err(|e| {
                invalid_argument(format!("invalid UpdateDistributionTenantRequest XML: {e}"))
            })?;
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| not_found("DistributionTenant", &id))?;
        let t = account
            .distribution_tenants
            .get_mut(&id)
            .ok_or_else(|| not_found("DistributionTenant", &id))?;
        if t.etag != if_match {
            return Err(precondition_failed());
        }
        if let Some(d) = parsed.distribution_id {
            t.distribution_id = d;
        }
        if let Some(d) = parsed.domains {
            t.domains = d.members.into_iter().map(|i| i.domain).collect();
        }
        if let Some(c) = parsed.connection_group_id {
            t.connection_group_id = Some(c);
        }
        if let Some(e) = parsed.enabled {
            t.enabled = e;
        }
        if let Some(p) = parsed.parameters {
            t.parameters = convert_parameters(Some(p));
        }
        if let Some(c) = parsed.customizations {
            let converted = convert_customizations(c);
            t.web_acl_arn = converted
                .web_acl
                .as_ref()
                .and_then(|w| w.arn.clone())
                .or_else(|| t.web_acl_arn.clone());
            t.customizations = Some(converted);
        }
        if let Some(m) = parsed.managed_certificate_request {
            t.managed_certificate_request = Some(convert_managed_cert_request(m));
        }
        t.etag = generate_id_with_prefix("E");
        t.last_modified_time = Utc::now();
        // UpdateDistributionTenant kicks off a fresh edge propagation; mirror
        // the Distribution lifecycle by flipping back to InProgress.
        t.status = "InProgress".to_string();
        let snap = t.clone();
        drop(state);
        self.schedule_distribution_tenant_deploy(id.clone());
        let body = render_distribution_tenant(&snap);
        Ok(xml_with_etag(StatusCode::OK, body, &snap.etag, None))
    }

    pub(crate) fn delete_distribution_tenant(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = route_id(route, "DistributionTenant")?;
        let if_match = require_if_match(req)?;
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| not_found("DistributionTenant", &id))?;
        let t = account
            .distribution_tenants
            .get(&id)
            .ok_or_else(|| not_found("DistributionTenant", &id))?;
        if t.etag != if_match {
            return Err(precondition_failed());
        }
        if t.enabled {
            return Err(aws_error(
                StatusCode::PRECONDITION_FAILED,
                "ResourceInUse",
                "DistributionTenant must be disabled before delete",
            ));
        }
        let arn = t.arn.clone();
        account.distribution_tenants.remove(&id);
        account
            .tenant_invalidations
            .retain(|_, inv| inv.tenant_id != id);
        account.tags.remove(&arn);
        drop(state);
        Ok(crate::policies::empty(StatusCode::NO_CONTENT))
    }

    pub(crate) fn list_distribution_tenants(
        &self,
        _req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let mut items: Vec<StoredDistributionTenant> = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .map(|a| a.distribution_tenants.values().cloned().collect())
            .unwrap_or_default();
        drop(state);
        items.sort_by(|a, b| a.id.cmp(&b.id));
        let body = render_tenant_list(&items, "ListDistributionTenantsResult");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    pub(crate) fn list_distribution_tenants_by_customization(
        &self,
        _req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let mut items: Vec<StoredDistributionTenant> = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .map(|a| a.distribution_tenants.values().cloned().collect())
            .unwrap_or_default();
        drop(state);
        items.sort_by(|a, b| a.id.cmp(&b.id));
        let body = render_tenant_list(&items, "ListDistributionTenantsByCustomizationResult");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    pub(crate) fn associate_distribution_tenant_web_acl(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = route_id(route, "DistributionTenant")?;
        let if_match = require_if_match(req)?;
        let parsed: AssociateWebAclRequest = xml_io::from_xml_root(&req.body).map_err(|e| {
            invalid_argument(format!(
                "invalid AssociateDistributionTenantWebACLRequest XML: {e}"
            ))
        })?;
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| not_found("DistributionTenant", &id))?;
        let t = account
            .distribution_tenants
            .get_mut(&id)
            .ok_or_else(|| not_found("DistributionTenant", &id))?;
        if t.etag != if_match {
            return Err(precondition_failed());
        }
        t.web_acl_arn = Some(parsed.web_acl_arn.clone());
        // Mirror the association into Customizations.WebAcl so GetDistribution
        // Tenant reflects the active WebACL, matching AWS.
        let customizations = t.customizations.get_or_insert_with(Default::default);
        let web_acl = customizations.web_acl.get_or_insert_with(Default::default);
        if web_acl.action.is_empty() {
            web_acl.action = "override".to_string();
        }
        web_acl.arn = Some(parsed.web_acl_arn);
        t.etag = generate_id_with_prefix("E");
        t.last_modified_time = Utc::now();
        let snap = t.clone();
        drop(state);
        let body = render_associate_web_acl(&snap);
        Ok(xml_with_etag(StatusCode::OK, body, &snap.etag, None))
    }

    pub(crate) fn disassociate_distribution_tenant_web_acl(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = route_id(route, "DistributionTenant")?;
        let if_match = require_if_match(req)?;
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| not_found("DistributionTenant", &id))?;
        let t = account
            .distribution_tenants
            .get_mut(&id)
            .ok_or_else(|| not_found("DistributionTenant", &id))?;
        if t.etag != if_match {
            return Err(precondition_failed());
        }
        t.web_acl_arn = None;
        if let Some(c) = t.customizations.as_mut() {
            c.web_acl = None;
        }
        t.etag = generate_id_with_prefix("E");
        t.last_modified_time = Utc::now();
        let snap = t.clone();
        drop(state);
        let body = render_disassociate_web_acl(&snap);
        Ok(xml_with_etag(StatusCode::OK, body, &snap.etag, None))
    }

    pub(crate) fn create_invalidation_for_distribution_tenant(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let tenant_id = route_id(route, "DistributionTenant")?;
        let parsed: InvalidationBatchRequest = xml_io::from_xml_root(&req.body)
            .map_err(|e| invalid_argument(format!("invalid InvalidationBatch XML: {e}")))?;
        if parsed.caller_reference.is_empty() {
            return Err(invalid_argument("CallerReference is required"));
        }
        let paths = parsed.paths.items.map(|i| i.path).unwrap_or_default();
        if paths.is_empty() {
            return Err(invalid_argument(
                "InvalidationBatch.Paths must be non-empty",
            ));
        }
        let mut state = self.state.write();
        let account = state.entry(DEFAULT_ACCOUNT);
        if !account.distribution_tenants.contains_key(&tenant_id) {
            return Err(not_found("DistributionTenant", &tenant_id));
        }
        let id = generate_invalidation_id();
        let stored = StoredTenantInvalidation {
            id: id.clone(),
            tenant_id: tenant_id.clone(),
            status: "Completed".to_string(),
            create_time: Utc::now(),
            paths,
            caller_reference: parsed.caller_reference,
        };
        account
            .tenant_invalidations
            .insert(id.clone(), stored.clone());
        drop(state);
        let body = render_tenant_invalidation(&stored);
        let mut headers = HeaderMap::new();
        if let Ok(v) = http::HeaderValue::from_str(&format!(
            "/2020-05-31/distribution-tenant/{tenant_id}/invalidation/{}",
            stored.id
        )) {
            headers.insert(LOCATION, v);
        }
        Ok(xml_response(StatusCode::CREATED, body, headers))
    }

    pub(crate) fn get_invalidation_for_distribution_tenant(
        &self,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let tenant_id = route
            .id
            .as_deref()
            .ok_or_else(|| invalid_argument("missing distribution tenant id"))?;
        let inv_id = route
            .second_id
            .as_deref()
            .ok_or_else(|| invalid_argument("missing invalidation id"))?;
        let state = self.state.read();
        let account = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .ok_or_else(|| not_found("Invalidation", inv_id))?;
        if !account.distribution_tenants.contains_key(tenant_id) {
            return Err(not_found("DistributionTenant", tenant_id));
        }
        let inv = account
            .tenant_invalidations
            .get(inv_id)
            .filter(|i| i.tenant_id == tenant_id)
            .ok_or_else(|| not_found("Invalidation", inv_id))?
            .clone();
        drop(state);
        let body = render_tenant_invalidation(&inv);
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    pub(crate) fn list_invalidations_for_distribution_tenant(
        &self,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let tenant_id = route
            .id
            .as_deref()
            .ok_or_else(|| invalid_argument("missing distribution tenant id"))?;
        let state = self.state.read();
        let account = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .ok_or_else(|| not_found("DistributionTenant", tenant_id))?;
        if !account.distribution_tenants.contains_key(tenant_id) {
            return Err(not_found("DistributionTenant", tenant_id));
        }
        let mut items: Vec<&StoredTenantInvalidation> = account
            .tenant_invalidations
            .values()
            .filter(|i| i.tenant_id == tenant_id)
            .collect();
        items.sort_by_key(|a| a.create_time);
        let body = render_tenant_invalidation_list(&items);
        drop(state);
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }
}

fn generate_tenant_id() -> String {
    let raw = Uuid::new_v4().simple().to_string().to_uppercase();
    format!("DT{}", &raw[..12])
}

fn generate_invalidation_id() -> String {
    let raw = Uuid::new_v4().simple().to_string().to_uppercase();
    format!("I{}", &raw[..13])
}

fn render_distribution_tenant(t: &StoredDistributionTenant) -> String {
    let mut out = String::with_capacity(512);
    out.push_str(XML_DECL);
    out.push_str(&format!("<DistributionTenant xmlns=\"{NS}\">"));
    push_tenant_inner(&mut out, t);
    out.push_str("</DistributionTenant>");
    out
}

fn push_tenant_inner(out: &mut String, t: &StoredDistributionTenant) {
    out.push_str(&format!("<Id>{}</Id>", esc(&t.id)));
    out.push_str(&format!(
        "<DistributionId>{}</DistributionId>",
        esc(&t.distribution_id)
    ));
    out.push_str(&format!("<Name>{}</Name>", esc(&t.name)));
    out.push_str(&format!("<Arn>{}</Arn>", esc(&t.arn)));
    out.push_str(&format!("<Status>{}</Status>", esc(&t.status)));
    out.push_str(&format!("<Enabled>{}</Enabled>", t.enabled));
    out.push_str(&format!(
        "<CreatedTime>{}</CreatedTime>",
        rfc3339(&t.created_time)
    ));
    out.push_str(&format!(
        "<LastModifiedTime>{}</LastModifiedTime>",
        rfc3339(&t.last_modified_time)
    ));
    if let Some(c) = &t.connection_group_id {
        out.push_str(&format!(
            "<ConnectionGroupId>{}</ConnectionGroupId>",
            esc(c)
        ));
    }
    out.push_str("<Domains>");
    for d in &t.domains {
        out.push_str("<DomainResult>");
        out.push_str(&format!("<Domain>{}</Domain>", esc(d)));
        out.push_str("<Status>active</Status>");
        out.push_str("</DomainResult>");
    }
    out.push_str("</Domains>");
    if !t.parameters.is_empty() {
        out.push_str("<Parameters>");
        for p in &t.parameters {
            out.push_str("<Parameter>");
            out.push_str(&format!("<Name>{}</Name>", esc(&p.name)));
            out.push_str(&format!("<Value>{}</Value>", esc(&p.value)));
            out.push_str("</Parameter>");
        }
        out.push_str("</Parameters>");
    }
    if let Some(c) = &t.customizations {
        out.push_str("<Customizations>");
        if let Some(w) = &c.web_acl {
            out.push_str("<WebAcl>");
            out.push_str(&format!("<Action>{}</Action>", esc(&w.action)));
            if let Some(arn) = &w.arn {
                out.push_str(&format!("<Arn>{}</Arn>", esc(arn)));
            }
            out.push_str("</WebAcl>");
        }
        if let Some(cert) = &c.certificate {
            out.push_str("<Certificate>");
            out.push_str(&format!("<Arn>{}</Arn>", esc(cert)));
            out.push_str("</Certificate>");
        }
        if let Some(g) = &c.geo_restrictions {
            out.push_str("<GeoRestrictions>");
            out.push_str(&format!(
                "<RestrictionType>{}</RestrictionType>",
                esc(&g.restriction_type)
            ));
            out.push_str("<Locations>");
            for l in &g.locations {
                out.push_str(&format!("<Location>{}</Location>", esc(l)));
            }
            out.push_str("</Locations>");
            out.push_str("</GeoRestrictions>");
        }
        out.push_str("</Customizations>");
    }
}

fn convert_parameters(req: Option<ParametersReq>) -> Vec<TenantParameter> {
    req.map(|p| {
        p.parameter
            .into_iter()
            .map(|r| TenantParameter {
                name: r.name,
                value: r.value,
            })
            .collect()
    })
    .unwrap_or_default()
}

fn convert_customizations(req: CustomizationsReq) -> TenantCustomizations {
    TenantCustomizations {
        web_acl: req.web_acl.map(|w| TenantWebAclCustomization {
            action: w.action,
            arn: w.arn,
        }),
        certificate: req.certificate.map(|c| c.arn),
        geo_restrictions: req
            .geo_restrictions
            .map(|g| TenantGeoRestrictionCustomization {
                restriction_type: g.restriction_type,
                locations: g.locations.map(|l| l.location).unwrap_or_default(),
            }),
    }
}

fn convert_managed_cert_request(
    req: ManagedCertificateRequestReq,
) -> TenantManagedCertificateRequest {
    TenantManagedCertificateRequest {
        validation_token_host: req.validation_token_host,
        primary_domain_name: req.primary_domain_name,
        certificate_transparency_logging_preference: req
            .certificate_transparency_logging_preference,
    }
}

fn render_tenant_list(items: &[StoredDistributionTenant], wrapper: &str) -> String {
    let mut out = String::with_capacity(512);
    out.push_str(XML_DECL);
    out.push_str(&format!("<{wrapper} xmlns=\"{NS}\">"));
    out.push_str("<NextMarker></NextMarker>");
    out.push_str("<DistributionTenantList>");
    for t in items {
        out.push_str("<DistributionTenantSummary>");
        push_tenant_inner(&mut out, t);
        out.push_str("</DistributionTenantSummary>");
    }
    out.push_str("</DistributionTenantList>");
    out.push_str(&format!("</{wrapper}>"));
    out
}

fn render_associate_web_acl(t: &StoredDistributionTenant) -> String {
    let mut out = String::with_capacity(256);
    out.push_str(XML_DECL);
    out.push_str(&format!(
        "<AssociateDistributionTenantWebACLResult xmlns=\"{NS}\">"
    ));
    out.push_str(&format!("<Id>{}</Id>", esc(&t.id)));
    if let Some(arn) = &t.web_acl_arn {
        out.push_str(&format!("<WebACLArn>{}</WebACLArn>", esc(arn)));
    }
    out.push_str("</AssociateDistributionTenantWebACLResult>");
    out
}

fn render_disassociate_web_acl(t: &StoredDistributionTenant) -> String {
    let mut out = String::with_capacity(256);
    out.push_str(XML_DECL);
    out.push_str(&format!(
        "<DisassociateDistributionTenantWebACLResult xmlns=\"{NS}\">"
    ));
    out.push_str(&format!("<Id>{}</Id>", esc(&t.id)));
    out.push_str("</DisassociateDistributionTenantWebACLResult>");
    out
}

fn render_tenant_invalidation(inv: &StoredTenantInvalidation) -> String {
    let mut out = String::with_capacity(512);
    out.push_str(XML_DECL);
    out.push_str(&format!("<Invalidation xmlns=\"{NS}\">"));
    out.push_str(&format!("<Id>{}</Id>", esc(&inv.id)));
    out.push_str(&format!("<Status>{}</Status>", esc(&inv.status)));
    out.push_str(&format!(
        "<CreateTime>{}</CreateTime>",
        rfc3339(&inv.create_time)
    ));
    out.push_str("<InvalidationBatch>");
    out.push_str(&format!(
        "<CallerReference>{}</CallerReference>",
        esc(&inv.caller_reference)
    ));
    out.push_str("<Paths>");
    out.push_str(&format!("<Quantity>{}</Quantity>", inv.paths.len()));
    out.push_str("<Items>");
    for p in &inv.paths {
        out.push_str(&format!("<Path>{}</Path>", esc(p)));
    }
    out.push_str("</Items>");
    out.push_str("</Paths>");
    out.push_str("</InvalidationBatch>");
    out.push_str("</Invalidation>");
    out
}

fn render_tenant_invalidation_list(items: &[&StoredTenantInvalidation]) -> String {
    let mut out = String::with_capacity(1024);
    out.push_str(XML_DECL);
    out.push_str(&format!("<InvalidationList xmlns=\"{NS}\">"));
    out.push_str("<Marker></Marker>");
    out.push_str("<MaxItems>100</MaxItems>");
    out.push_str("<IsTruncated>false</IsTruncated>");
    out.push_str(&format!("<Quantity>{}</Quantity>", items.len()));
    if !items.is_empty() {
        out.push_str("<Items>");
        for inv in items {
            out.push_str("<InvalidationSummary>");
            out.push_str(&format!("<Id>{}</Id>", esc(&inv.id)));
            out.push_str(&format!(
                "<CreateTime>{}</CreateTime>",
                rfc3339(&inv.create_time)
            ));
            out.push_str(&format!("<Status>{}</Status>", esc(&inv.status)));
            out.push_str("</InvalidationSummary>");
        }
        out.push_str("</Items>");
    }
    out.push_str("</InvalidationList>");
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::CloudFrontAccounts;
    use bytes::Bytes;
    use fakecloud_core::service::AwsService;
    use http::HeaderValue;
    use parking_lot::RwLock;
    use std::sync::Arc;

    fn svc() -> CloudFrontService {
        CloudFrontService::new(Arc::new(RwLock::new(CloudFrontAccounts::new())))
    }

    fn req(method: http::Method, path: &str, body: &str, if_match: Option<&str>) -> AwsRequest {
        let mut headers = HeaderMap::new();
        if let Some(v) = if_match {
            headers.insert(http::header::IF_MATCH, HeaderValue::from_str(v).unwrap());
        }
        AwsRequest {
            service: "cloudfront".into(),
            action: String::new(),
            region: "us-east-1".into(),
            account_id: DEFAULT_ACCOUNT.into(),
            request_id: Uuid::new_v4().to_string(),
            headers,
            query_params: std::collections::HashMap::new(),
            body_stream: parking_lot::Mutex::new(None),
            body: Bytes::from(body.to_string()),
            path_segments: path
                .split('/')
                .filter(|s| !s.is_empty())
                .map(String::from)
                .collect(),
            raw_path: path.into(),
            raw_query: String::new(),
            method,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    fn body_str(resp: &AwsResponse) -> String {
        match &resp.body {
            fakecloud_core::service::ResponseBody::Bytes(b) => {
                String::from_utf8(b.to_vec()).unwrap()
            }
            _ => panic!("expected bytes body"),
        }
    }

    async fn create_tenant_full(svc: &CloudFrontService, name: &str) -> (String, String) {
        let body = format!(
            r#"<?xml version="1.0"?>
<CreateDistributionTenantRequest xmlns="{NS}">
  <DistributionId>E123</DistributionId>
  <Name>{name}</Name>
  <Parameters>
    <Parameter><Name>tenantId</Name><Value>acme</Value></Parameter>
  </Parameters>
  <Customizations>
    <Certificate><Arn>arn:aws:acm::cert/xyz</Arn></Certificate>
    <GeoRestrictions>
      <RestrictionType>whitelist</RestrictionType>
      <Locations><Location>US</Location><Location>CA</Location></Locations>
    </GeoRestrictions>
  </Customizations>
  <ManagedCertificateRequest>
    <PrimaryDomainName>acme.example.com</PrimaryDomainName>
    <ValidationTokenHost>self-hosted</ValidationTokenHost>
  </ManagedCertificateRequest>
  <Tags>
    <Items><Tag><Key>env</Key><Value>prod</Value></Tag></Items>
  </Tags>
</CreateDistributionTenantRequest>"#
        );
        let resp = svc
            .handle(req(
                http::Method::POST,
                "/2020-05-31/distribution-tenant",
                &body,
                None,
            ))
            .await
            .unwrap();
        assert_eq!(resp.status, StatusCode::CREATED);
        let xml = body_str(&resp);
        let id = xml
            .split("<Id>")
            .nth(1)
            .unwrap()
            .split("</Id>")
            .next()
            .unwrap()
            .to_string();
        let etag = resp
            .headers
            .get(http::header::ETAG)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        (id, etag)
    }

    #[tokio::test]
    async fn create_tenant_persists_parameters_customizations_and_tags() {
        // Finding #6: Parameters/Customizations/ManagedCertificateRequest/Tags
        // survive create and are echoed on Get.
        let svc = svc();
        let (id, _etag) = create_tenant_full(&svc, "acme-tenant").await;

        let get = svc
            .handle(req(
                http::Method::GET,
                &format!("/2020-05-31/distribution-tenant/{id}"),
                "",
                None,
            ))
            .await
            .unwrap();
        let xml = body_str(&get);
        assert!(
            xml.contains("<Name>tenantId</Name>") && xml.contains("<Value>acme</Value>"),
            "parameters dropped: {xml}"
        );
        assert!(
            xml.contains("<Certificate><Arn>arn:aws:acm::cert/xyz</Arn></Certificate>"),
            "certificate customization dropped: {xml}"
        );
        assert!(
            xml.contains("<RestrictionType>whitelist</RestrictionType>")
                && xml.contains("<Location>US</Location>"),
            "geo restrictions dropped: {xml}"
        );

        // Tags persisted for ListTagsForResource lookup.
        let arn = format!(
            "arn:aws:cloudfront::{}:distribution-tenant/{}",
            DEFAULT_ACCOUNT, id
        );
        let state = svc.shared_state();
        let guard = state.read();
        let tags = guard
            .get(DEFAULT_ACCOUNT)
            .and_then(|a| a.tags.get(&arn))
            .cloned()
            .unwrap_or_default();
        assert!(
            tags.iter()
                .any(|t| t.key == "env" && t.value.as_deref() == Some("prod")),
            "tenant tags dropped: {tags:?}"
        );
        // Managed certificate request persisted.
        let mcr = guard
            .get(DEFAULT_ACCOUNT)
            .and_then(|a| a.distribution_tenants.get(&id))
            .and_then(|t| t.managed_certificate_request.clone());
        assert_eq!(
            mcr.and_then(|m| m.primary_domain_name),
            Some("acme.example.com".to_string())
        );
    }

    #[tokio::test]
    async fn associate_web_acl_surfaces_on_get_distribution_tenant() {
        // Finding #5: WebACLArn is visible on GetDistributionTenant after
        // AssociateDistributionTenantWebACL.
        let svc = svc();
        let (id, etag) = create_tenant_full(&svc, "waf-tenant").await;
        let web_acl = "arn:aws:wafv2:us-east-1:000:global/webacl/x/1";
        let assoc_body = format!(
            r#"<?xml version="1.0"?>
<AssociateDistributionTenantWebACLRequest xmlns="{NS}">
  <WebACLArn>{web_acl}</WebACLArn>
</AssociateDistributionTenantWebACLRequest>"#
        );
        let assoc = svc
            .handle(req(
                http::Method::PUT,
                &format!("/2020-05-31/distribution-tenant/{id}/associate-web-acl"),
                &assoc_body,
                Some(&etag),
            ))
            .await
            .unwrap();
        assert_eq!(assoc.status, StatusCode::OK);

        let get = svc
            .handle(req(
                http::Method::GET,
                &format!("/2020-05-31/distribution-tenant/{id}"),
                "",
                None,
            ))
            .await
            .unwrap();
        let xml = body_str(&get);
        assert!(
            xml.contains(&format!("<Arn>{web_acl}</Arn>")) && xml.contains("<WebAcl>"),
            "WebACL not surfaced on GetDistributionTenant: {xml}"
        );
    }
}
