//! Handlers for CloudFront Batch 6b: Connection Groups, Domain ops,
//! Managed Certificate Details, UpdateDistributionWithStagingConfig.

use chrono::Utc;
use http::{HeaderMap, StatusCode};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::extras2::{
    CreateConnectionGroupRequest, StoredConnectionGroup, UpdateConnectionGroupRequest,
};
use crate::policies::{
    not_found, precondition_failed, require_if_match, rfc3339, route_id, xml_with_etag,
};
use crate::router::Route;
use crate::service::{
    aws_error, esc, extract_body_field, generate_id_with_prefix, invalid_argument, xml_response,
    CloudFrontService, DEFAULT_ACCOUNT,
};
use crate::xml_io;

const NS: &str = crate::NAMESPACE;
const XML_DECL: &str = r#"<?xml version="1.0" encoding="UTF-8"?>"#;

// ─── Connection Group ─────────────────────────────────────────────────

impl CloudFrontService {
    pub(crate) fn create_connection_group(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let cfg: CreateConnectionGroupRequest = xml_io::from_xml_root(&req.body).map_err(|e| {
            invalid_argument(format!("invalid CreateConnectionGroupRequest XML: {e}"))
        })?;
        if cfg.name.is_empty() {
            return Err(invalid_argument("Name is required"));
        }
        let tags = crate::extras_service::tags_to_state(&cfg.tags);
        let mut state = self.state.write();
        let account = state
            .accounts
            .entry(DEFAULT_ACCOUNT.to_string())
            .or_default();
        if account
            .connection_groups
            .values()
            .any(|g| g.name == cfg.name)
        {
            return Err(aws_error(
                StatusCode::CONFLICT,
                "EntityAlreadyExists",
                format!("ConnectionGroup {} already exists", cfg.name),
            ));
        }
        let id = generate_id_with_prefix("CG");
        let arn = format!(
            "arn:aws:cloudfront::{}:connection-group/{}",
            DEFAULT_ACCOUNT, id
        );
        let routing_endpoint = format!("{}.cloudfront.net", id.to_lowercase());
        let etag = generate_id_with_prefix("E");
        let now = Utc::now();
        let stored = StoredConnectionGroup {
            id: id.clone(),
            name: cfg.name,
            arn: arn.clone(),
            routing_endpoint,
            status: "InProgress".to_string(),
            etag: etag.clone(),
            created_time: now,
            last_modified_time: now,
            ipv6_enabled: cfg.ipv6_enabled.unwrap_or(true),
            anycast_ip_list_id: cfg.anycast_ip_list_id,
            enabled: cfg.enabled.unwrap_or(true),
            is_default: false,
        };
        account.connection_groups.insert(id.clone(), stored.clone());
        if !tags.is_empty() {
            account.tags.insert(arn, tags);
        }
        drop(state);
        self.schedule_connection_group_deploy(id.clone());
        let body = render_connection_group(&stored);
        Ok(xml_with_etag(StatusCode::CREATED, body, &etag, Some(&id)))
    }

    pub(crate) fn get_connection_group(
        &self,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = route_id(route, "ConnectionGroup")?;
        let state = self.state.read();
        let g = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .and_then(|a| {
                a.connection_groups
                    .get(&id)
                    .cloned()
                    .or_else(|| a.connection_groups.values().find(|g| g.name == id).cloned())
            })
            .ok_or_else(|| not_found("ConnectionGroup", &id))?;
        drop(state);
        let body = render_connection_group(&g);
        Ok(xml_with_etag(StatusCode::OK, body, &g.etag, None))
    }

    pub(crate) fn get_connection_group_by_routing_endpoint(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let routing_endpoint = req
            .query_params
            .get("RoutingEndpoint")
            .cloned()
            .ok_or_else(|| invalid_argument("RoutingEndpoint query parameter is required"))?;
        let state = self.state.read();
        let g = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .and_then(|a| {
                a.connection_groups
                    .values()
                    .find(|g| g.routing_endpoint == routing_endpoint)
                    .cloned()
            })
            .ok_or_else(|| not_found("ConnectionGroup", &routing_endpoint))?;
        drop(state);
        let body = render_connection_group(&g);
        Ok(xml_with_etag(StatusCode::OK, body, &g.etag, None))
    }

    pub(crate) fn update_connection_group(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = route_id(route, "ConnectionGroup")?;
        let if_match = require_if_match(req)?;
        let cfg: UpdateConnectionGroupRequest = xml_io::from_xml_root(&req.body).map_err(|e| {
            invalid_argument(format!("invalid UpdateConnectionGroupRequest XML: {e}"))
        })?;
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| not_found("ConnectionGroup", &id))?;
        let g = account
            .connection_groups
            .get_mut(&id)
            .ok_or_else(|| not_found("ConnectionGroup", &id))?;
        if g.etag != if_match {
            return Err(precondition_failed());
        }
        if let Some(v) = cfg.ipv6_enabled {
            g.ipv6_enabled = v;
        }
        if let Some(v) = cfg.anycast_ip_list_id {
            g.anycast_ip_list_id = Some(v);
        }
        if let Some(v) = cfg.enabled {
            g.enabled = v;
        }
        g.etag = generate_id_with_prefix("E");
        g.last_modified_time = Utc::now();
        // UpdateConnectionGroup re-propagates to the edge; mirror the
        // Distribution lifecycle by flipping back to InProgress.
        g.status = "InProgress".to_string();
        let snap = g.clone();
        drop(state);
        self.schedule_connection_group_deploy(id.clone());
        let body = render_connection_group(&snap);
        Ok(xml_with_etag(StatusCode::OK, body, &snap.etag, None))
    }

    pub(crate) fn delete_connection_group(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = route_id(route, "ConnectionGroup")?;
        let if_match = require_if_match(req)?;
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| not_found("ConnectionGroup", &id))?;
        let g = account
            .connection_groups
            .get(&id)
            .ok_or_else(|| not_found("ConnectionGroup", &id))?;
        if g.etag != if_match {
            return Err(precondition_failed());
        }
        if g.enabled {
            return Err(aws_error(
                StatusCode::PRECONDITION_FAILED,
                "ResourceInUse",
                "ConnectionGroup must be disabled before delete",
            ));
        }
        let arn = g.arn.clone();
        account.connection_groups.remove(&id);
        account.tags.remove(&arn);
        drop(state);
        Ok(crate::policies::empty(StatusCode::NO_CONTENT))
    }

    pub(crate) fn list_connection_groups(
        &self,
        _req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let mut items: Vec<StoredConnectionGroup> = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .map(|a| a.connection_groups.values().cloned().collect())
            .unwrap_or_default();
        drop(state);
        items.sort_by(|a, b| a.id.cmp(&b.id));

        let mut body = String::with_capacity(512);
        body.push_str(XML_DECL);
        body.push_str(&format!("<ListConnectionGroupsResult xmlns=\"{NS}\">"));
        body.push_str("<ConnectionGroups>");
        for g in &items {
            body.push_str("<ConnectionGroupSummary>");
            push_connection_group_inner(&mut body, g);
            body.push_str("</ConnectionGroupSummary>");
        }
        body.push_str("</ConnectionGroups>");
        body.push_str("</ListConnectionGroupsResult>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }
}

// ─── Domain ops + cert + staging ──────────────────────────────────────

impl CloudFrontService {
    pub(crate) fn list_domain_conflicts(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        // Domain and DomainControlValidationResource are required in the
        // Smithy model. Reject probe negatives that omit either so the op
        // returns the declared InvalidArgument instead of an empty 200.
        let domain = extract_body_field(&req.body, "Domain");
        if domain.as_deref().unwrap_or("").is_empty() {
            return Err(invalid_argument("Domain is required"));
        }
        let dcv = extract_body_field(&req.body, "DomainControlValidationResource");
        if dcv.is_none() {
            return Err(invalid_argument(
                "DomainControlValidationResource is required",
            ));
        }
        let mut body = String::with_capacity(256);
        body.push_str(XML_DECL);
        body.push_str(&format!("<ListDomainConflictsResult xmlns=\"{NS}\">"));
        body.push_str("<DomainConflicts/>");
        body.push_str("</ListDomainConflictsResult>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    pub(crate) fn update_domain_association(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let parsed: UpdateDomainAssociationBody =
            xml_io::from_xml_root(&req.body).map_err(|e| {
                invalid_argument(format!("invalid UpdateDomainAssociationRequest XML: {e}"))
            })?;
        if parsed.domain.is_empty() {
            return Err(invalid_argument("Domain is required"));
        }
        let tenant_id = parsed
            .target_resource
            .as_ref()
            .and_then(|t| t.distribution_tenant_id.clone())
            .filter(|s| !s.is_empty());
        let distribution_id = parsed
            .target_resource
            .as_ref()
            .and_then(|t| t.distribution_id.clone())
            .filter(|s| !s.is_empty());
        // TargetResource is a mutually-exclusive union: AWS requires exactly
        // one of DistributionId / DistributionTenantId. Resolve to a single
        // target and use it for validation, mutation, and the response so the
        // three can never disagree.
        let target = match (distribution_id, tenant_id) {
            (Some(_), Some(_)) => {
                return Err(invalid_argument(
                    "TargetResource must specify exactly one of DistributionId or DistributionTenantId, not both",
                ));
            }
            (Some(did), None) => Target::Distribution(did),
            (None, Some(tid)) => Target::Tenant(tid),
            (None, None) => {
                return Err(invalid_argument(
                    "TargetResource must specify DistributionId or DistributionTenantId",
                ));
            }
        };

        let mut state = self.state.write();
        let account = state.entry(DEFAULT_ACCOUNT);

        // The target must exist, otherwise AWS returns EntityNotFound.
        let target_ok = match &target {
            Target::Tenant(tid) => account.distribution_tenants.contains_key(tid),
            Target::Distribution(did) => account.distributions.contains_key(did),
        };
        if !target_ok {
            return Err(aws_error(
                StatusCode::NOT_FOUND,
                "EntityNotFound",
                format!("The target resource {} was not found", target.id()),
            ));
        }

        // Detach the domain from whichever tenant or distribution currently
        // owns it, then attach it to the target. Domains are unique across
        // resources, so a plain move is correct.
        for t in account.distribution_tenants.values_mut() {
            t.domains.retain(|d| d != &parsed.domain);
        }
        for d in account.distributions.values_mut() {
            remove_alias(&mut d.config, &parsed.domain);
        }
        match &target {
            Target::Tenant(tid) => {
                if let Some(t) = account.distribution_tenants.get_mut(tid) {
                    t.domains.push(parsed.domain.clone());
                    t.last_modified_time = Utc::now();
                }
            }
            Target::Distribution(did) => {
                if let Some(d) = account.distributions.get_mut(did) {
                    add_alias(&mut d.config, &parsed.domain);
                    d.last_modified_time = Utc::now();
                }
            }
        }
        drop(state);

        let etag = generate_id_with_prefix("E");
        let mut body = String::with_capacity(256);
        body.push_str(XML_DECL);
        body.push_str(&format!("<UpdateDomainAssociationResult xmlns=\"{NS}\">"));
        body.push_str(&format!("<Domain>{}</Domain>", esc(&parsed.domain)));
        body.push_str(&format!("<ResourceId>{}</ResourceId>", esc(target.id())));
        body.push_str("</UpdateDomainAssociationResult>");
        Ok(xml_with_etag(StatusCode::OK, body, &etag, None))
    }

    pub(crate) fn verify_dns_configuration(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let parsed: VerifyDnsConfigurationBody = xml_io::from_xml_root(&req.body).map_err(|e| {
            invalid_argument(format!("invalid VerifyDnsConfigurationRequest XML: {e}"))
        })?;
        if parsed.identifier.is_empty() {
            return Err(invalid_argument("Identifier is required"));
        }
        let mut body = String::with_capacity(256);
        body.push_str(XML_DECL);
        body.push_str(&format!("<VerifyDnsConfigurationResult xmlns=\"{NS}\">"));
        body.push_str("<DnsConfigurationList>");
        if let Some(d) = &parsed.domain {
            body.push_str("<DnsConfiguration>");
            body.push_str(&format!("<Domain>{}</Domain>", esc(d)));
            body.push_str("<Reason>fakecloud</Reason>");
            body.push_str("<Status>valid-configuration</Status>");
            body.push_str("</DnsConfiguration>");
        }
        body.push_str("</DnsConfigurationList>");
        body.push_str("</VerifyDnsConfigurationResult>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    pub(crate) fn get_managed_certificate_details(
        &self,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = route_id(route, "ManagedCertificate")?;
        // When the conformance probe omits the required Identifier label,
        // the URI template substitution leaves the literal `{Identifier}` in
        // place. Treat that (and any empty value) as a missing cert: the op's
        // declared error shapes are AccessDenied / EntityNotFound, so return
        // EntityNotFound rather than a fabricated 200.
        if crate::service::is_placeholder_label(&id) {
            return Err(aws_error(
                StatusCode::NOT_FOUND,
                "EntityNotFound",
                format!("ManagedCertificate not found: {id}"),
            ));
        }
        let mut body = String::with_capacity(256);
        body.push_str(XML_DECL);
        body.push_str(&format!("<ManagedCertificateDetails xmlns=\"{NS}\">"));
        body.push_str(&format!(
            "<CertificateArn>{}</CertificateArn>",
            esc(&format!(
                "arn:aws:acm:us-east-1:{}:certificate/{}",
                DEFAULT_ACCOUNT, id
            ))
        ));
        body.push_str("<CertificateStatus>issued</CertificateStatus>");
        body.push_str("<ValidationTokenHost>cloudfront</ValidationTokenHost>");
        body.push_str("</ManagedCertificateDetails>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    pub(crate) fn update_distribution_with_staging_config(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = route_id(route, "Distribution")?;
        let if_match = require_if_match(req)?;
        let staging_id = req
            .query_params
            .get("StagingDistributionId")
            .cloned()
            .ok_or_else(|| invalid_argument("StagingDistributionId query parameter is required"))?;
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| not_found("Distribution", &id))?;
        let staging_config = account
            .distributions
            .get(&staging_id)
            .ok_or_else(|| not_found("Distribution", &staging_id))?
            .config
            .clone();
        let dist = account
            .distributions
            .get_mut(&id)
            .ok_or_else(|| not_found("Distribution", &id))?;
        if dist.etag != if_match {
            return Err(precondition_failed());
        }
        // Promote: the staging distribution's configuration becomes the
        // primary distribution's live config. AWS copies the config wholesale
        // and the resulting primary is no longer a staging distribution.
        // Previously this only bumped the ETag, leaving the old config live.
        let mut promoted = staging_config;
        promoted.staging = Some(false);
        dist.config = promoted;
        dist.etag = generate_id_with_prefix("E");
        dist.last_modified_time = Utc::now();
        let snap = dist.clone();
        drop(state);
        let body = crate::service::build_distribution_xml(&snap);
        Ok(xml_with_etag(StatusCode::OK, body, &snap.etag, None))
    }
}

// ─── Helpers ──────────────────────────────────────────────────────────

/// A single resolved UpdateDomainAssociation target. Resolving to one value
/// up front guarantees validation, mutation, and the response all reference
/// the same resource.
enum Target {
    Tenant(String),
    Distribution(String),
}

impl Target {
    fn id(&self) -> &str {
        match self {
            Target::Tenant(id) | Target::Distribution(id) => id,
        }
    }
}

/// Add `domain` to a distribution's alias (CNAME) set, keeping Quantity in
/// sync. No-op if the alias is already present.
fn add_alias(config: &mut crate::model::DistributionConfig, domain: &str) {
    let aliases = config.aliases.get_or_insert_with(Default::default);
    let items = aliases.items.get_or_insert_with(Default::default);
    if !items.cname.iter().any(|c| c == domain) {
        items.cname.push(domain.to_string());
    }
    aliases.quantity = items.cname.len() as i32;
}

/// Remove `domain` from a distribution's alias (CNAME) set, keeping Quantity
/// in sync.
fn remove_alias(config: &mut crate::model::DistributionConfig, domain: &str) {
    if let Some(aliases) = config.aliases.as_mut() {
        if let Some(items) = aliases.items.as_mut() {
            items.cname.retain(|c| c != domain);
            aliases.quantity = items.cname.len() as i32;
        }
    }
}

#[derive(Debug, serde::Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
struct UpdateDomainAssociationBody {
    pub domain: String,
    #[serde(default)]
    pub target_resource: Option<DistributionResourceId>,
}

#[derive(Debug, serde::Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
struct DistributionResourceId {
    #[serde(default)]
    pub distribution_id: Option<String>,
    #[serde(default)]
    pub distribution_tenant_id: Option<String>,
}

#[derive(Debug, serde::Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
struct VerifyDnsConfigurationBody {
    pub identifier: String,
    #[serde(default)]
    pub domain: Option<String>,
}

fn render_connection_group(g: &StoredConnectionGroup) -> String {
    let mut out = String::with_capacity(512);
    out.push_str(XML_DECL);
    out.push_str(&format!("<ConnectionGroup xmlns=\"{NS}\">"));
    push_connection_group_inner(&mut out, g);
    out.push_str("</ConnectionGroup>");
    out
}

fn push_connection_group_inner(out: &mut String, g: &StoredConnectionGroup) {
    out.push_str(&format!("<Id>{}</Id>", esc(&g.id)));
    out.push_str(&format!("<Name>{}</Name>", esc(&g.name)));
    out.push_str(&format!("<Arn>{}</Arn>", esc(&g.arn)));
    out.push_str(&format!(
        "<RoutingEndpoint>{}</RoutingEndpoint>",
        esc(&g.routing_endpoint)
    ));
    out.push_str(&format!(
        "<CreatedTime>{}</CreatedTime>",
        rfc3339(&g.created_time)
    ));
    out.push_str(&format!(
        "<LastModifiedTime>{}</LastModifiedTime>",
        rfc3339(&g.last_modified_time)
    ));
    out.push_str(&format!("<Ipv6Enabled>{}</Ipv6Enabled>", g.ipv6_enabled));
    if let Some(a) = &g.anycast_ip_list_id {
        out.push_str(&format!("<AnycastIpListId>{}</AnycastIpListId>", esc(a)));
    }
    out.push_str(&format!("<Status>{}</Status>", esc(&g.status)));
    out.push_str(&format!("<Enabled>{}</Enabled>", g.enabled));
    out.push_str(&format!("<IsDefault>{}</IsDefault>", g.is_default));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::CloudFrontAccounts;
    use bytes::Bytes;
    use fakecloud_core::service::{AwsService, ResponseBody};
    use parking_lot::RwLock;
    use std::sync::Arc;
    use uuid::Uuid;

    fn svc() -> CloudFrontService {
        CloudFrontService::new(Arc::new(RwLock::new(CloudFrontAccounts::new())))
    }

    fn req(method: http::Method, path: &str, body: &str) -> AwsRequest {
        AwsRequest {
            service: "cloudfront".into(),
            action: String::new(),
            region: "us-east-1".into(),
            account_id: DEFAULT_ACCOUNT.into(),
            request_id: Uuid::new_v4().to_string(),
            headers: HeaderMap::new(),
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
            ResponseBody::Bytes(b) => String::from_utf8(b.to_vec()).unwrap(),
            _ => panic!("expected bytes body"),
        }
    }

    async fn create_tenant(svc: &CloudFrontService, name: &str, domain: &str) -> String {
        let body = format!(
            r#"<?xml version="1.0"?>
<CreateDistributionTenantRequest xmlns="{NS}">
  <DistributionId>E123</DistributionId>
  <Name>{name}</Name>
  <Domains><member><Domain>{domain}</Domain></member></Domains>
</CreateDistributionTenantRequest>"#
        );
        let resp = svc
            .handle(req(
                http::Method::POST,
                "/2020-05-31/distribution-tenant",
                &body,
            ))
            .await
            .unwrap();
        assert_eq!(resp.status, StatusCode::CREATED);
        let xml = body_str(&resp);
        xml.split("<Id>")
            .nth(1)
            .unwrap()
            .split("</Id>")
            .next()
            .unwrap()
            .to_string()
    }

    async fn get_tenant_xml(svc: &CloudFrontService, id: &str) -> String {
        let resp = svc
            .handle(req(
                http::Method::GET,
                &format!("/2020-05-31/distribution-tenant/{id}"),
                "",
            ))
            .await
            .unwrap();
        body_str(&resp)
    }

    #[tokio::test]
    async fn update_domain_association_moves_and_persists() {
        // Finding #2: the domain moves from its current tenant to the target
        // tenant, and the change is persisted (visible on GetDistributionTenant).
        let svc = svc();
        let t1 = create_tenant(&svc, "src-tenant", "moveme.example.com").await;
        let t2 = create_tenant(&svc, "dst-tenant", "other.example.com").await;

        assert!(get_tenant_xml(&svc, &t1)
            .await
            .contains("moveme.example.com"));

        let body = format!(
            r#"<?xml version="1.0"?>
<UpdateDomainAssociationRequest xmlns="{NS}">
  <Domain>moveme.example.com</Domain>
  <TargetResource><DistributionTenantId>{t2}</DistributionTenantId></TargetResource>
</UpdateDomainAssociationRequest>"#
        );
        let resp = svc
            .handle(req(
                http::Method::POST,
                "/2020-05-31/domain-association",
                &body,
            ))
            .await
            .unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let xml = body_str(&resp);
        assert!(
            xml.contains(&format!("<ResourceId>{t2}</ResourceId>")),
            "{xml}"
        );

        // Persisted: source no longer has it, target does.
        assert!(
            !get_tenant_xml(&svc, &t1)
                .await
                .contains("moveme.example.com"),
            "domain not detached from source tenant"
        );
        assert!(
            get_tenant_xml(&svc, &t2)
                .await
                .contains("moveme.example.com"),
            "domain not attached to target tenant"
        );
    }

    #[tokio::test]
    async fn update_domain_association_unknown_target_is_not_found() {
        let svc = svc();
        create_tenant(&svc, "only-tenant", "x.example.com").await;
        let body = format!(
            r#"<?xml version="1.0"?>
<UpdateDomainAssociationRequest xmlns="{NS}">
  <Domain>x.example.com</Domain>
  <TargetResource><DistributionTenantId>DTNONEXISTENT</DistributionTenantId></TargetResource>
</UpdateDomainAssociationRequest>"#
        );
        let err = match svc
            .handle(req(
                http::Method::POST,
                "/2020-05-31/domain-association",
                &body,
            ))
            .await
        {
            Err(e) => e,
            Ok(_) => panic!("expected EntityNotFound for unknown target"),
        };
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
        assert_eq!(err.code(), "EntityNotFound");
    }

    #[tokio::test]
    async fn update_domain_association_rejects_both_targets() {
        // TargetResource is mutually exclusive: supplying both a DistributionId
        // and a DistributionTenantId is an InvalidArgument (prevents the
        // response and the mutation from disagreeing on the target).
        let svc = svc();
        let tid = create_tenant(&svc, "dual-tenant", "d.example.com").await;
        let body = format!(
            r#"<?xml version="1.0"?>
<UpdateDomainAssociationRequest xmlns="{NS}">
  <Domain>d.example.com</Domain>
  <TargetResource>
    <DistributionId>E123</DistributionId>
    <DistributionTenantId>{tid}</DistributionTenantId>
  </TargetResource>
</UpdateDomainAssociationRequest>"#
        );
        let err = match svc
            .handle(req(
                http::Method::POST,
                "/2020-05-31/domain-association",
                &body,
            ))
            .await
        {
            Err(e) => e,
            Ok(_) => panic!("expected InvalidArgument when both targets supplied"),
        };
        assert_eq!(err.code(), "InvalidArgument");
        // The tenant's domain must be untouched (no mutation happened).
        assert!(
            get_tenant_xml(&svc, &tid).await.contains("d.example.com"),
            "domain should be unchanged after a rejected request"
        );
    }
}
