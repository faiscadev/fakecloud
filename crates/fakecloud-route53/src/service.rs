//! Route 53 REST-XML service implementation.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use http::{HeaderMap, StatusCode};
use parking_lot::RwLock;
use uuid::Uuid;

use fakecloud_aws::arn::Arn;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError, ResponseBody};

use crate::model::{
    AssociateVpcRequest, ChangeCidrCollectionRequest, ChangeResourceRecordSetsRequest,
    ChangeTagsForResourceRequest, CreateCidrCollectionRequest, CreateHealthCheckRequest,
    CreateHostedZoneRequest, CreateKeySigningKeyRequest, CreateQueryLoggingConfigRequest,
    CreateReusableDelegationSetRequest, CreateTrafficPolicyInstanceRequest,
    CreateTrafficPolicyRequest, CreateTrafficPolicyVersionRequest, HealthCheckConfig,
    ListTagsForResourcesRequest, ResourceRecordSet, UpdateHealthCheckRequest,
    UpdateHostedZoneCommentRequest, UpdateHostedZoneFeaturesRequest,
    UpdateTrafficPolicyCommentRequest, UpdateTrafficPolicyInstanceRequest, VpcAuthorizationRequest,
    VPC,
};
use crate::router::{route, Route};
use crate::state::{
    AccountState, HealthCheckStatus, Route53Accounts, SharedRoute53State, StoredChange,
    StoredCidrCollection, StoredHealthCheck, StoredHostedZone, StoredKeySigningKey,
    StoredQueryLoggingConfig, StoredReusableDelegationSet, StoredTrafficPolicy,
    StoredTrafficPolicyInstance,
};
use crate::xml_io;

pub(crate) const DEFAULT_ACCOUNT: &str = "000000000000";
pub(crate) const NS: &str = crate::NAMESPACE;
const XML_DECL: &str = r#"<?xml version="1.0" encoding="UTF-8"?>"#;

const SUPPORTED_ACTIONS: &[&str] = &[
    "CreateHostedZone",
    "GetHostedZone",
    "DeleteHostedZone",
    "ListHostedZones",
    "ListHostedZonesByName",
    "GetHostedZoneCount",
    "UpdateHostedZoneComment",
    "UpdateHostedZoneFeatures",
    "GetHostedZoneLimit",
    "ChangeResourceRecordSets",
    "ListResourceRecordSets",
    "GetChange",
    "TestDNSAnswer",
    "CreateHealthCheck",
    "GetHealthCheck",
    "UpdateHealthCheck",
    "DeleteHealthCheck",
    "ListHealthChecks",
    "GetHealthCheckCount",
    "GetHealthCheckStatus",
    "GetHealthCheckLastFailureReason",
    "GetCheckerIpRanges",
    "CreateTrafficPolicy",
    "CreateTrafficPolicyVersion",
    "GetTrafficPolicy",
    "UpdateTrafficPolicyComment",
    "DeleteTrafficPolicy",
    "ListTrafficPolicies",
    "ListTrafficPolicyVersions",
    "CreateTrafficPolicyInstance",
    "GetTrafficPolicyInstance",
    "UpdateTrafficPolicyInstance",
    "DeleteTrafficPolicyInstance",
    "ListTrafficPolicyInstances",
    "ListTrafficPolicyInstancesByHostedZone",
    "ListTrafficPolicyInstancesByPolicy",
    "GetTrafficPolicyInstanceCount",
    "GetDNSSEC",
    "EnableHostedZoneDNSSEC",
    "DisableHostedZoneDNSSEC",
    "CreateKeySigningKey",
    "DeleteKeySigningKey",
    "ActivateKeySigningKey",
    "DeactivateKeySigningKey",
    "CreateQueryLoggingConfig",
    "GetQueryLoggingConfig",
    "DeleteQueryLoggingConfig",
    "ListQueryLoggingConfigs",
    "CreateCidrCollection",
    "ChangeCidrCollection",
    "DeleteCidrCollection",
    "ListCidrCollections",
    "ListCidrLocations",
    "ListCidrBlocks",
    "AssociateVPCWithHostedZone",
    "DisassociateVPCFromHostedZone",
    "CreateVPCAssociationAuthorization",
    "DeleteVPCAssociationAuthorization",
    "ListVPCAssociationAuthorizations",
    "ListHostedZonesByVPC",
    "CreateReusableDelegationSet",
    "GetReusableDelegationSet",
    "DeleteReusableDelegationSet",
    "ListReusableDelegationSets",
    "GetReusableDelegationSetLimit",
    "ListGeoLocations",
    "GetGeoLocation",
    "GetAccountLimit",
    "ChangeTagsForResource",
    "ListTagsForResource",
    "ListTagsForResources",
];

pub struct Route53Service {
    pub(crate) state: SharedRoute53State,
    /// Optional CloudWatch Logs state. When wired, every TestDNSAnswer
    /// call against a zone with an active QueryLoggingConfig appends a
    /// query log record to the configured log group, mirroring real
    /// Route 53's query logging delivery.
    pub(crate) logs_state: Option<fakecloud_logs::SharedLogsState>,
    /// Optional ELBv2 state. When wired, alias records targeting an
    /// ELB DNS name (e.g. `my-lb-123.us-east-1.elb.amazonaws.com`)
    /// resolve to the load balancer's actual A/AAAA addresses pulled
    /// from `LoadBalancerAddress`.
    pub(crate) elbv2_state: Option<fakecloud_elbv2::SharedElbv2State>,
    /// Optional CloudFront state. When wired, alias records targeting
    /// a CloudFront `<id>.cloudfront.net` domain resolve only when the
    /// distribution exists.
    pub(crate) cloudfront_state: Option<fakecloud_cloudfront::SharedCloudFrontState>,
    /// Optional S3 state. When wired, alias records targeting an S3
    /// website endpoint (`<bucket>.s3-website-<region>.amazonaws.com`)
    /// or virtual-hosted bucket endpoint resolve only when the bucket
    /// exists.
    pub(crate) s3_state: Option<fakecloud_s3::SharedS3State>,
}

impl Route53Service {
    pub fn new(state: SharedRoute53State) -> Self {
        Self {
            state,
            logs_state: None,
            elbv2_state: None,
            cloudfront_state: None,
            s3_state: None,
        }
    }

    /// Wire CloudWatch Logs so `TestDNSAnswer` calls against a zone
    /// with an active QueryLoggingConfig forward their query record
    /// into the configured log group.
    pub fn with_logs(mut self, logs: fakecloud_logs::SharedLogsState) -> Self {
        self.logs_state = Some(logs);
        self
    }

    /// Wire ELBv2 state so `TestDNSAnswer` resolves alias records that
    /// point at an ELB DNS name to the load balancer's real addresses.
    pub fn with_elbv2(mut self, elbv2: fakecloud_elbv2::SharedElbv2State) -> Self {
        self.elbv2_state = Some(elbv2);
        self
    }

    /// Wire CloudFront state so `TestDNSAnswer` only resolves alias
    /// records that point at an existing CloudFront distribution.
    pub fn with_cloudfront(mut self, cf: fakecloud_cloudfront::SharedCloudFrontState) -> Self {
        self.cloudfront_state = Some(cf);
        self
    }

    /// Wire S3 state so `TestDNSAnswer` only resolves alias records
    /// that point at an existing S3 bucket / website endpoint.
    pub fn with_s3(mut self, s3: fakecloud_s3::SharedS3State) -> Self {
        self.s3_state = Some(s3);
        self
    }

    pub fn shared_state(&self) -> SharedRoute53State {
        Arc::clone(&self.state)
    }
}

impl Default for Route53Service {
    fn default() -> Self {
        Self::new(Arc::new(RwLock::new(Route53Accounts::new())))
    }
}

/// Bundle of DNSSEC RRSIG fields returned by
/// `Route53Service::sign_rrset_with_zone_ksk`. Carries enough context
/// (algorithm, key tag, validity window, signer) for an admin caller
/// to assemble a full RRSIG wire record.
#[derive(Debug, Clone)]
pub struct DnssecSignature {
    pub signature_b64: String,
    pub algorithm: u8,
    pub key_tag: u16,
    pub signer_name: String,
    pub inception: u32,
    pub expiration: u32,
    pub labels: u8,
    pub original_ttl: u32,
    pub rrset_type: String,
}

#[async_trait]
impl AwsService for Route53Service {
    fn service_name(&self) -> &str {
        "route53"
    }

    fn supported_actions(&self) -> &[&str] {
        SUPPORTED_ACTIONS
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let resolved = match route(&req.method, &req.raw_path, &req.raw_query) {
            Some(r) => r,
            None => {
                return Err(aws_error(
                    StatusCode::NOT_FOUND,
                    "InvalidArgument",
                    format!("Unknown Route 53 route: {} {}", req.method, req.raw_path),
                ));
            }
        };

        match resolved.action {
            "CreateHostedZone" => self.create_hosted_zone(&req),
            "GetHostedZone" => self.get_hosted_zone(&resolved),
            "DeleteHostedZone" => self.delete_hosted_zone(&resolved),
            "ListHostedZones" => self.list_hosted_zones(&req),
            "ListHostedZonesByName" => self.list_hosted_zones_by_name(&req),
            "GetHostedZoneCount" => self.get_hosted_zone_count(),
            "UpdateHostedZoneComment" => self.update_hosted_zone_comment(&req, &resolved),
            "UpdateHostedZoneFeatures" => self.update_hosted_zone_features(&req, &resolved),
            "GetHostedZoneLimit" => self.get_hosted_zone_limit(&resolved),
            "ChangeResourceRecordSets" => self.change_resource_record_sets(&req, &resolved),
            "ListResourceRecordSets" => self.list_resource_record_sets(&req, &resolved),
            "GetChange" => self.get_change(&resolved),
            "TestDNSAnswer" => self.test_dns_answer(&req),
            "CreateHealthCheck" => self.create_health_check(&req),
            "GetHealthCheck" => self.get_health_check(&resolved),
            "UpdateHealthCheck" => self.update_health_check(&req, &resolved),
            "DeleteHealthCheck" => self.delete_health_check(&resolved),
            "ListHealthChecks" => self.list_health_checks(&req),
            "GetHealthCheckCount" => self.get_health_check_count(),
            "GetHealthCheckStatus" => self.get_health_check_status(&resolved),
            "GetHealthCheckLastFailureReason" => {
                self.get_health_check_last_failure_reason(&resolved)
            }
            "GetCheckerIpRanges" => self.get_checker_ip_ranges(),
            "CreateTrafficPolicy" => self.create_traffic_policy(&req),
            "CreateTrafficPolicyVersion" => self.create_traffic_policy_version(&req, &resolved),
            "GetTrafficPolicy" => self.get_traffic_policy(&resolved),
            "UpdateTrafficPolicyComment" => self.update_traffic_policy_comment(&req, &resolved),
            "DeleteTrafficPolicy" => self.delete_traffic_policy(&resolved),
            "ListTrafficPolicies" => self.list_traffic_policies(&req),
            "ListTrafficPolicyVersions" => self.list_traffic_policy_versions(&req, &resolved),
            "CreateTrafficPolicyInstance" => self.create_traffic_policy_instance(&req),
            "GetTrafficPolicyInstance" => self.get_traffic_policy_instance(&resolved),
            "UpdateTrafficPolicyInstance" => self.update_traffic_policy_instance(&req, &resolved),
            "DeleteTrafficPolicyInstance" => self.delete_traffic_policy_instance(&resolved),
            "ListTrafficPolicyInstances" => self.list_traffic_policy_instances(&req),
            "ListTrafficPolicyInstancesByHostedZone" => {
                self.list_traffic_policy_instances_by_hosted_zone(&req)
            }
            "ListTrafficPolicyInstancesByPolicy" => {
                self.list_traffic_policy_instances_by_policy(&req)
            }
            "GetTrafficPolicyInstanceCount" => self.get_traffic_policy_instance_count(),
            "GetDNSSEC" => self.get_dnssec(&resolved),
            "EnableHostedZoneDNSSEC" => self.enable_hosted_zone_dnssec(&resolved),
            "DisableHostedZoneDNSSEC" => self.disable_hosted_zone_dnssec(&resolved),
            "CreateKeySigningKey" => self.create_key_signing_key(&req),
            "DeleteKeySigningKey" => self.delete_key_signing_key(&resolved),
            "ActivateKeySigningKey" => self.activate_key_signing_key(&resolved),
            "DeactivateKeySigningKey" => self.deactivate_key_signing_key(&resolved),
            "CreateQueryLoggingConfig" => self.create_query_logging_config(&req),
            "GetQueryLoggingConfig" => self.get_query_logging_config(&resolved),
            "DeleteQueryLoggingConfig" => self.delete_query_logging_config(&resolved),
            "ListQueryLoggingConfigs" => self.list_query_logging_configs(&req),
            "CreateCidrCollection" => self.create_cidr_collection(&req),
            "ChangeCidrCollection" => self.change_cidr_collection(&req, &resolved),
            "DeleteCidrCollection" => self.delete_cidr_collection(&resolved),
            "ListCidrCollections" => self.list_cidr_collections(&req),
            "ListCidrLocations" => self.list_cidr_locations(&req, &resolved),
            "ListCidrBlocks" => self.list_cidr_blocks(&req, &resolved),
            "AssociateVPCWithHostedZone" => self.associate_vpc_with_hosted_zone(&req, &resolved),
            "DisassociateVPCFromHostedZone" => {
                self.disassociate_vpc_from_hosted_zone(&req, &resolved)
            }
            "CreateVPCAssociationAuthorization" => {
                self.create_vpc_association_authorization(&req, &resolved)
            }
            "DeleteVPCAssociationAuthorization" => {
                self.delete_vpc_association_authorization(&req, &resolved)
            }
            "ListVPCAssociationAuthorizations" => {
                self.list_vpc_association_authorizations(&req, &resolved)
            }
            "ListHostedZonesByVPC" => self.list_hosted_zones_by_vpc(&req),
            "CreateReusableDelegationSet" => self.create_reusable_delegation_set(&req),
            "GetReusableDelegationSet" => self.get_reusable_delegation_set(&resolved),
            "DeleteReusableDelegationSet" => self.delete_reusable_delegation_set(&resolved),
            "ListReusableDelegationSets" => self.list_reusable_delegation_sets(&req),
            "GetReusableDelegationSetLimit" => self.get_reusable_delegation_set_limit(&resolved),
            "ListGeoLocations" => self.list_geo_locations(&req),
            "GetGeoLocation" => self.get_geo_location(&req),
            "GetAccountLimit" => self.get_account_limit(&resolved),
            "ChangeTagsForResource" => self.change_tags_for_resource(&req, &resolved),
            "ListTagsForResource" => self.list_tags_for_resource(&resolved),
            "ListTagsForResources" => self.list_tags_for_resources(&req, &resolved),
            other => Err(aws_error(
                StatusCode::NOT_IMPLEMENTED,
                "InvalidAction",
                format!("Route 53 action {other} is not implemented yet"),
            )),
        }
    }
}

// ─── Hosted Zone handlers ────────────────────────────────────────────

impl Route53Service {
    fn create_hosted_zone(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let cfg: CreateHostedZoneRequest = xml_io::from_xml_root(&req.body)
            .map_err(|e| invalid_argument(format!("invalid CreateHostedZoneRequest XML: {e}")))?;
        if cfg.name.is_empty() {
            return Err(invalid_argument("Name is required"));
        }
        if cfg.caller_reference.is_empty() {
            return Err(invalid_argument("CallerReference is required"));
        }
        let mut name = cfg.name.clone();
        if !name.ends_with('.') {
            name.push('.');
        }
        let private_zone = cfg
            .hosted_zone_config
            .as_ref()
            .and_then(|c| c.private_zone)
            .unwrap_or(false);
        if private_zone && cfg.vpc.is_none() {
            return Err(invalid_argument("Private hosted zone must include a VPC"));
        }
        let comment = cfg
            .hosted_zone_config
            .as_ref()
            .and_then(|c| c.comment.clone());

        let mut state = self.state.write();
        let account = state
            .accounts
            .entry(DEFAULT_ACCOUNT.to_string())
            .or_default();
        if account
            .hosted_zones
            .values()
            .any(|z| z.caller_reference == cfg.caller_reference)
        {
            return Err(aws_error(
                StatusCode::CONFLICT,
                "HostedZoneAlreadyExists",
                format!(
                    "A hosted zone with the same caller reference already exists: {}",
                    cfg.caller_reference
                ),
            ));
        }
        let id = generate_zone_id();
        let now = Utc::now();
        let name_servers = synth_name_servers(&id);
        let vpcs = cfg.vpc.into_iter().collect();
        let default_records = if private_zone {
            Vec::new()
        } else {
            default_zone_records(&name, &name_servers)
        };
        let zone = StoredHostedZone {
            id: id.clone(),
            name: name.clone(),
            caller_reference: cfg.caller_reference,
            comment,
            private_zone,
            features: None,
            vpcs,
            delegation_set_id: cfg.delegation_set_id,
            name_servers: name_servers.clone(),
            created_time: now,
            resource_record_sets: default_records,
        };
        account.hosted_zones.insert(id.clone(), zone.clone());

        let change_id = generate_change_id();
        let change = StoredChange {
            id: change_id.clone(),
            status: "PENDING".to_string(),
            submitted_at: now,
            comment: Some(format!("CreateHostedZone {}", id)),
            read_count: 0,
        };
        account.changes.insert(change_id.clone(), change.clone());
        drop(state);

        let mut body = String::with_capacity(1024);
        body.push_str(XML_DECL);
        body.push_str(&format!("<CreateHostedZoneResponse xmlns=\"{NS}\">"));
        push_hosted_zone(&mut body, &zone);
        push_change_info(&mut body, &change);
        body.push_str("<DelegationSet>");
        if let Some(id) = &zone.delegation_set_id {
            body.push_str(&format!("<Id>{}</Id>", esc(id)));
        }
        body.push_str("<NameServers>");
        for ns in &zone.name_servers {
            body.push_str(&format!("<NameServer>{}</NameServer>", esc(ns)));
        }
        body.push_str("</NameServers>");
        body.push_str("</DelegationSet>");
        if !zone.vpcs.is_empty() {
            push_vpc_block(&mut body, "VPC", &zone.vpcs[0]);
        }
        body.push_str("</CreateHostedZoneResponse>");

        let mut headers = HeaderMap::new();
        if let Ok(loc) = http::HeaderValue::from_str(&format!("/2013-04-01/hostedzone/{}", zone.id))
        {
            headers.insert(http::header::LOCATION, loc);
        }
        Ok(xml_response(StatusCode::CREATED, body, headers))
    }

    fn get_hosted_zone(&self, route: &Route) -> Result<AwsResponse, AwsServiceError> {
        let id = require_id(route)?;
        let id = strip_zone_prefix(&id);
        let state = self.state.read();
        let account = state.accounts.get(DEFAULT_ACCOUNT);
        let zone = account
            .and_then(|a| a.hosted_zones.get(&id).cloned())
            .ok_or_else(|| no_such_hosted_zone(&id))?;
        drop(state);
        let mut body = String::with_capacity(1024);
        body.push_str(XML_DECL);
        body.push_str(&format!("<GetHostedZoneResponse xmlns=\"{NS}\">"));
        push_hosted_zone(&mut body, &zone);
        body.push_str("<DelegationSet>");
        if let Some(id) = &zone.delegation_set_id {
            body.push_str(&format!("<Id>{}</Id>", esc(id)));
        }
        body.push_str("<NameServers>");
        for ns in &zone.name_servers {
            body.push_str(&format!("<NameServer>{}</NameServer>", esc(ns)));
        }
        body.push_str("</NameServers>");
        body.push_str("</DelegationSet>");
        if !zone.vpcs.is_empty() {
            body.push_str("<VPCs>");
            for v in &zone.vpcs {
                push_vpc_block(&mut body, "VPC", v);
            }
            body.push_str("</VPCs>");
        }
        body.push_str("</GetHostedZoneResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    fn delete_hosted_zone(&self, route: &Route) -> Result<AwsResponse, AwsServiceError> {
        let id = require_id(route)?;
        let id = strip_zone_prefix(&id);
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| no_such_hosted_zone(&id))?;
        let zone = account
            .hosted_zones
            .get(&id)
            .ok_or_else(|| no_such_hosted_zone(&id))?;
        if zone
            .resource_record_sets
            .iter()
            .any(|r| !is_default_record(r, &zone.name))
        {
            return Err(aws_error(
                StatusCode::BAD_REQUEST,
                "HostedZoneNotEmpty",
                format!("HostedZone {} has user-managed resource record sets", id),
            ));
        }
        account.hosted_zones.remove(&id);
        let change_id = generate_change_id();
        let change = StoredChange {
            id: change_id.clone(),
            status: "PENDING".to_string(),
            submitted_at: Utc::now(),
            comment: Some(format!("DeleteHostedZone {}", id)),
            read_count: 0,
        };
        account.changes.insert(change_id.clone(), change.clone());
        drop(state);

        let mut body = String::with_capacity(256);
        body.push_str(XML_DECL);
        body.push_str(&format!("<DeleteHostedZoneResponse xmlns=\"{NS}\">"));
        push_change_info(&mut body, &change);
        body.push_str("</DeleteHostedZoneResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    fn list_hosted_zones(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        validate_query_constraints(
            &req.query_params,
            &[
                QueryConstraint::StrLen {
                    key: "marker",
                    min: 0,
                    max: 64,
                },
                QueryConstraint::StrLen {
                    key: "delegationsetid",
                    min: 0,
                    max: 32,
                },
                QueryConstraint::Enum {
                    key: "hostedzonetype",
                    allowed: &["PrivateHostedZone"],
                },
                MAX_ITEMS_CONSTRAINT,
            ],
        )?;
        let state = self.state.read();
        let mut zones: Vec<StoredHostedZone> = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .map(|a| a.hosted_zones.values().cloned().collect())
            .unwrap_or_default();
        drop(state);
        zones.sort_by(|a, b| a.id.cmp(&b.id));
        let mut body = String::with_capacity(1024);
        body.push_str(XML_DECL);
        body.push_str(&format!("<ListHostedZonesResponse xmlns=\"{NS}\">"));
        body.push_str("<HostedZones>");
        for z in &zones {
            push_hosted_zone(&mut body, z);
        }
        body.push_str("</HostedZones>");
        body.push_str("<MaxItems>100</MaxItems>");
        body.push_str("<IsTruncated>false</IsTruncated>");
        body.push_str("</ListHostedZonesResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    fn list_hosted_zones_by_name(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        validate_query_constraints(
            &req.query_params,
            &[
                QueryConstraint::StrLen {
                    key: "dnsname",
                    min: 0,
                    max: 1024,
                },
                QueryConstraint::StrLen {
                    key: "hostedzoneid",
                    min: 0,
                    max: 32,
                },
                MAX_ITEMS_CONSTRAINT,
            ],
        )?;
        let dns_name = req.query_params.get("dnsname").cloned();
        let state = self.state.read();
        let mut zones: Vec<StoredHostedZone> = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .map(|a| a.hosted_zones.values().cloned().collect())
            .unwrap_or_default();
        drop(state);
        zones.sort_by(|a, b| a.name.cmp(&b.name));
        if let Some(name) = &dns_name {
            let normalized = if name.ends_with('.') {
                name.clone()
            } else {
                format!("{name}.")
            };
            zones.retain(|z| z.name >= normalized);
        }
        let mut body = String::with_capacity(1024);
        body.push_str(XML_DECL);
        body.push_str(&format!("<ListHostedZonesByNameResponse xmlns=\"{NS}\">"));
        body.push_str("<HostedZones>");
        for z in &zones {
            push_hosted_zone(&mut body, z);
        }
        body.push_str("</HostedZones>");
        if let Some(name) = &dns_name {
            body.push_str(&format!("<DNSName>{}</DNSName>", esc(name)));
        }
        body.push_str("<MaxItems>100</MaxItems>");
        body.push_str("<IsTruncated>false</IsTruncated>");
        body.push_str("</ListHostedZonesByNameResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    fn get_hosted_zone_count(&self) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let count = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .map(|a| a.hosted_zones.len())
            .unwrap_or(0);
        drop(state);
        let mut body = String::with_capacity(128);
        body.push_str(XML_DECL);
        body.push_str(&format!("<GetHostedZoneCountResponse xmlns=\"{NS}\">"));
        body.push_str(&format!("<HostedZoneCount>{}</HostedZoneCount>", count));
        body.push_str("</GetHostedZoneCountResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    fn update_hosted_zone_comment(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = require_id(route)?;
        let id = strip_zone_prefix(&id);
        let cfg: UpdateHostedZoneCommentRequest =
            xml_io::from_xml_root(&req.body).map_err(|e| {
                invalid_argument(format!("invalid UpdateHostedZoneCommentRequest XML: {e}"))
            })?;
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| no_such_hosted_zone(&id))?;
        let zone = account
            .hosted_zones
            .get_mut(&id)
            .ok_or_else(|| no_such_hosted_zone(&id))?;
        zone.comment = cfg.comment;
        let snap = zone.clone();
        drop(state);
        let mut body = String::with_capacity(512);
        body.push_str(XML_DECL);
        body.push_str(&format!("<UpdateHostedZoneCommentResponse xmlns=\"{NS}\">"));
        push_hosted_zone(&mut body, &snap);
        body.push_str("</UpdateHostedZoneCommentResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    fn update_hosted_zone_features(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = require_id(route)?;
        let id = strip_zone_prefix(&id);
        let cfg: UpdateHostedZoneFeaturesRequest =
            xml_io::from_xml_root(&req.body).map_err(|e| {
                invalid_argument(format!("invalid UpdateHostedZoneFeaturesRequest XML: {e}"))
            })?;
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| no_such_hosted_zone(&id))?;
        let zone = account
            .hosted_zones
            .get_mut(&id)
            .ok_or_else(|| no_such_hosted_zone(&id))?;
        zone.features = Some(crate::model::HostedZoneFeatures {
            enable_accelerated_recovery: cfg.enable_accelerated_recovery,
        });
        let snap = zone.clone();
        drop(state);
        let mut body = String::with_capacity(256);
        body.push_str(XML_DECL);
        body.push_str(&format!(
            "<UpdateHostedZoneFeaturesResponse xmlns=\"{NS}\">"
        ));
        push_hosted_zone(&mut body, &snap);
        body.push_str("</UpdateHostedZoneFeaturesResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    fn get_hosted_zone_limit(&self, route: &Route) -> Result<AwsResponse, AwsServiceError> {
        let id = require_id(route)?;
        let id = strip_zone_prefix(&id);
        let lim_type = route
            .second_id
            .clone()
            .ok_or_else(|| invalid_argument("limit Type is required"))?;
        let state = self.state.read();
        let zone = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .and_then(|a| a.hosted_zones.get(&id).cloned())
            .ok_or_else(|| no_such_hosted_zone(&id))?;
        drop(state);
        let (value, count) = match lim_type.as_str() {
            "MAX_RRSETS_BY_ZONE" => (10000_u64, zone.resource_record_sets.len() as u64),
            "MAX_VPCS_ASSOCIATED_BY_ZONE" => (300_u64, zone.vpcs.len() as u64),
            other => {
                return Err(invalid_argument(format!(
                    "Unknown hosted zone limit type: {other}"
                )));
            }
        };
        let mut body = String::with_capacity(256);
        body.push_str(XML_DECL);
        body.push_str(&format!("<GetHostedZoneLimitResponse xmlns=\"{NS}\">"));
        body.push_str(&format!(
            "<Limit><Type>{}</Type><Value>{}</Value></Limit>",
            esc(&lim_type),
            value
        ));
        body.push_str(&format!("<Count>{}</Count>", count));
        body.push_str("</GetHostedZoneLimitResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }
}

// ─── Resource Record Set handlers ────────────────────────────────────

impl Route53Service {
    fn change_resource_record_sets(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = require_id(route)?;
        let id = strip_zone_prefix(&id);
        let cfg: ChangeResourceRecordSetsRequest =
            xml_io::from_xml_root(&req.body).map_err(|e| {
                invalid_argument(format!("invalid ChangeResourceRecordSetsRequest XML: {e}"))
            })?;
        if cfg.change_batch.changes.change.is_empty() {
            return Err(invalid_argument("ChangeBatch.Changes is empty"));
        }
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| no_such_hosted_zone(&id))?;
        let zone = account
            .hosted_zones
            .get_mut(&id)
            .ok_or_else(|| no_such_hosted_zone(&id))?;
        // AWS applies a ChangeBatch atomically: either every change succeeds
        // or none do. Stage the mutations against a clone first; only swap
        // the live record set in once every action validates.
        let mut working = zone.resource_record_sets.clone();
        for ch in &cfg.change_batch.changes.change {
            let action = ch.action.to_uppercase();
            let rec = normalize_rrset(&ch.resource_record_set);
            match action.as_str() {
                "CREATE" => {
                    if working.iter().any(|r| rrset_matches(r, &rec)) {
                        return Err(invalid_change_batch(format!(
                            "Tried to create resource record set [name='{}', type='{}'] but it already exists",
                            rec.name, rec.record_type
                        )));
                    }
                    working.push(rec);
                }
                "UPSERT" => {
                    let pos = working.iter().position(|r| rrset_matches(r, &rec));
                    if let Some(p) = pos {
                        working[p] = rec;
                    } else {
                        working.push(rec);
                    }
                }
                "DELETE" => {
                    let pos = working.iter().position(|r| rrset_matches(r, &rec));
                    let p = pos.ok_or_else(|| {
                        invalid_change_batch(format!(
                            "Tried to delete resource record set [name='{}', type='{}'] but it was not found",
                            rec.name, rec.record_type
                        ))
                    })?;
                    if is_default_record(&working[p], &zone.name) {
                        return Err(invalid_change_batch(
                            "Cannot delete default SOA or NS record",
                        ));
                    }
                    working.remove(p);
                }
                other => {
                    return Err(invalid_change_batch(format!(
                        "Unknown change action: {other}"
                    )));
                }
            }
        }
        zone.resource_record_sets = working;
        let change_id = generate_change_id();
        let change = StoredChange {
            id: change_id.clone(),
            status: "PENDING".to_string(),
            submitted_at: Utc::now(),
            comment: cfg.change_batch.comment,
            read_count: 0,
        };
        account.changes.insert(change_id.clone(), change.clone());
        drop(state);
        let mut body = String::with_capacity(256);
        body.push_str(XML_DECL);
        body.push_str(&format!(
            "<ChangeResourceRecordSetsResponse xmlns=\"{NS}\">"
        ));
        push_change_info(&mut body, &change);
        body.push_str("</ChangeResourceRecordSetsResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    fn list_resource_record_sets(
        &self,
        _req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = require_id(route)?;
        let id = strip_zone_prefix(&id);
        let state = self.state.read();
        let zone = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .and_then(|a| a.hosted_zones.get(&id).cloned())
            .ok_or_else(|| no_such_hosted_zone(&id))?;
        drop(state);
        let mut body = String::with_capacity(1024);
        body.push_str(XML_DECL);
        body.push_str(&format!("<ListResourceRecordSetsResponse xmlns=\"{NS}\">"));
        body.push_str("<ResourceRecordSets>");
        for r in &zone.resource_record_sets {
            push_rrset(&mut body, r);
        }
        body.push_str("</ResourceRecordSets>");
        body.push_str("<IsTruncated>false</IsTruncated>");
        body.push_str("<MaxItems>100</MaxItems>");
        body.push_str("</ListResourceRecordSetsResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }
}

// ─── Change tracking + DNS test ──────────────────────────────────────

impl Route53Service {
    fn get_change(&self, route: &Route) -> Result<AwsResponse, AwsServiceError> {
        let id = require_id(route)?;
        // Mirror real Route 53's eventual-consistency window. Two
        // signals flip a change from PENDING to INSYNC, whichever
        // fires first:
        //   * Wall-clock age >= `PROPAGATION_AGE_SECS` since
        //     `submitted_at`. Real AWS converges in ~60s; we use a
        //     short window (default 1s, override via
        //     `FAKECLOUD_ROUTE53_PROPAGATION_SECS`) so polling tests
        //     observe the transition without dragging out wall-clock
        //     time.
        //   * `PROPAGATION_READS` GetChange polls. Lets tests that
        //     can't sleep (sync drivers, deterministic test runners)
        //     still drive the transition by polling.
        const PROPAGATION_READS: u32 = 5;
        let propagation_secs = std::env::var("FAKECLOUD_ROUTE53_PROPAGATION_SECS")
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(1);
        let change = {
            let mut state = self.state.write();
            let account = state.accounts.get_mut(DEFAULT_ACCOUNT).ok_or_else(|| {
                aws_error(
                    StatusCode::NOT_FOUND,
                    "NoSuchChange",
                    format!("Change {} not found", id),
                )
            })?;
            let stored = account.changes.get_mut(&id).ok_or_else(|| {
                aws_error(
                    StatusCode::NOT_FOUND,
                    "NoSuchChange",
                    format!("Change {} not found", id),
                )
            })?;
            stored.read_count = stored.read_count.saturating_add(1);
            let age_secs = (Utc::now() - stored.submitted_at).num_seconds();
            if stored.status == "PENDING"
                && (age_secs >= propagation_secs || stored.read_count >= PROPAGATION_READS)
            {
                stored.status = "INSYNC".to_string();
            }
            stored.clone()
        };
        let mut body = String::with_capacity(256);
        body.push_str(XML_DECL);
        body.push_str(&format!("<GetChangeResponse xmlns=\"{NS}\">"));
        push_change_info(&mut body, &change);
        body.push_str("</GetChangeResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    fn test_dns_answer(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let zone_id = req
            .query_params
            .get("hostedzoneid")
            .cloned()
            .ok_or_else(|| invalid_argument("hostedzoneid query parameter is required"))?;
        let record_name = req
            .query_params
            .get("recordname")
            .cloned()
            .ok_or_else(|| invalid_argument("recordname query parameter is required"))?;
        let record_type = req
            .query_params
            .get("recordtype")
            .cloned()
            .ok_or_else(|| invalid_argument("recordtype query parameter is required"))?;
        let resolver_ip = req
            .query_params
            .get("resolverip")
            .cloned()
            .unwrap_or_else(|| "8.8.8.8".to_string());
        let edns0_subnet = req.query_params.get("edns0clientsubnetip").cloned();
        let dnssec_requested = req
            .query_params
            .get("dnssec")
            .map(|v| matches!(v.as_str(), "true" | "1"))
            .unwrap_or(false);
        let zone_id = strip_zone_prefix(&zone_id);
        let state = self.state.read();
        let account = state.accounts.get(DEFAULT_ACCOUNT);
        let zone = account
            .and_then(|a| a.hosted_zones.get(&zone_id).cloned())
            .ok_or_else(|| no_such_hosted_zone(&zone_id))?;
        let health_checks = account.map(|a| a.health_checks.clone()).unwrap_or_default();
        // Capture DNSSEC + query-logging context while we still hold
        // the read guard so we can drop it before doing any work that
        // re-enters state (or cross-calls into fakecloud-logs).
        let dnssec_signing = account
            .and_then(|a| a.dnssec_status.get(&zone_id).cloned())
            .map(|s| s == "SIGNING")
            .unwrap_or(false);
        let active_ksk: Option<StoredKeySigningKey> = account.and_then(|a| {
            a.key_signing_keys
                .values()
                .filter(|k| k.hosted_zone_id == zone_id && k.status.eq_ignore_ascii_case("ACTIVE"))
                .min_by(|a, b| a.name.cmp(&b.name))
                .cloned()
        });
        let query_log_arn = account.and_then(|a| {
            a.query_logging_configs
                .values()
                .find(|c| c.hosted_zone_id == zone_id)
                .map(|c| c.cloud_watch_logs_log_group_arn.clone())
        });
        drop(state);
        let normalized_name = if record_name.ends_with('.') {
            record_name.clone()
        } else {
            format!("{record_name}.")
        };

        let candidates: Vec<&crate::model::ResourceRecordSet> = zone
            .resource_record_sets
            .iter()
            .filter(|r| r.name == normalized_name && r.record_type == record_type)
            .collect();

        let original_ttl = candidates.iter().filter_map(|c| c.ttl).min().unwrap_or(300) as u32;

        let alias_lookup = AliasLookup {
            elbv2: self.elbv2_state.as_ref(),
            cloudfront: self.cloudfront_state.as_ref(),
            s3: self.s3_state.as_ref(),
        };
        let answers: Vec<String> = if candidates.is_empty() {
            Vec::new()
        } else {
            resolve_routing_policy(
                &candidates,
                &health_checks,
                edns0_subnet.as_deref(),
                &alias_lookup,
            )
        };

        // Compute RRSIG when DNSSEC is on and the caller asked for it
        // (via `?dnssec=true`). Real Route 53's TestDNSAnswer doesn't
        // include RRSIGs by default, so gating behind the flag keeps
        // existing test fixtures stable.
        let rrsig_b64 = if dnssec_requested && dnssec_signing && !answers.is_empty() {
            active_ksk.as_ref().and_then(|ksk| {
                self.compute_rrsig_for_answers(
                    &zone.name,
                    &normalized_name,
                    &record_type,
                    original_ttl,
                    &answers,
                    ksk,
                )
            })
        } else {
            None
        };

        let mut body = String::with_capacity(512);
        body.push_str(XML_DECL);
        body.push_str(&format!("<TestDNSAnswerResponse xmlns=\"{NS}\">"));
        body.push_str(&format!("<Nameserver>{}</Nameserver>", esc(&resolver_ip)));
        body.push_str(&format!("<RecordName>{}</RecordName>", esc(&record_name)));
        body.push_str(&format!("<RecordType>{}</RecordType>", esc(&record_type)));
        body.push_str("<RecordData>");
        for v in &answers {
            body.push_str(&format!("<RecordDataEntry>{}</RecordDataEntry>", esc(v)));
        }
        body.push_str("</RecordData>");
        body.push_str("<ResponseCode>NOERROR</ResponseCode>");
        body.push_str(&format!(
            "<Protocol>{}</Protocol>",
            if edns0_subnet.is_some() {
                "EDNS0"
            } else {
                "UDP"
            }
        ));
        // Fakecloud extension: surface DNSSEC RRSIG bytes when the
        // caller asked for them. Real Route 53 doesn't expose this
        // field; it sits inside the namespace under
        // `<DnssecSignatures>` so AWS SDKs that don't model it just
        // ignore it.
        if let Some(sig) = &rrsig_b64 {
            body.push_str("<DnssecSignatures>");
            body.push_str(&format!(
                "<Algorithm>{}</Algorithm>",
                crate::dnssec::DNSSEC_ALGORITHM
            ));
            if let Some(ksk) = &active_ksk {
                body.push_str(&format!("<KeyTag>{}</KeyTag>", ksk.key_tag));
            }
            body.push_str(&format!("<Signature>{}</Signature>", esc(sig)));
            body.push_str("</DnssecSignatures>");
        }
        body.push_str("</TestDNSAnswerResponse>");

        // Query log delivery — best-effort. If logs state isn't wired
        // (unit-test harness, persistence-only build) we silently
        // skip; a real server always wires it via `with_logs`.
        if let (Some(logs_state), Some(arn)) = (&self.logs_state, query_log_arn) {
            if let Some((account_id, region, group_name)) = parse_log_group_arn(&arn) {
                let response_code = if answers.is_empty() {
                    "NXDOMAIN"
                } else {
                    "NOERROR"
                };
                let protocol = if edns0_subnet.is_some() {
                    "EDNS0"
                } else {
                    "UDP"
                };
                // Real Route 53 query log format (space-separated):
                //   <version> <ts> <zone_id> <name> <type> <rcode>
                //   <protocol> <edge_location> <client_ip> <ecs_subnet>
                let now = Utc::now();
                let line = format!(
                    "1.0 {ts} {zone} {name} {rtype} {rcode} {proto} FAKECLOUD {client} {ecs}",
                    ts = now.format("%Y-%m-%dT%H:%M:%S%.3fZ"),
                    zone = zone_id,
                    name = normalized_name,
                    rtype = record_type,
                    rcode = response_code,
                    proto = protocol,
                    client = resolver_ip,
                    ecs = edns0_subnet.unwrap_or_else(|| "-".to_string()),
                );
                let stream_name = format!("FAKECLOUD/{}", now.format("%Y/%m/%d"));
                fakecloud_logs::ingest::append_events(
                    logs_state,
                    &account_id,
                    &region,
                    &group_name,
                    &stream_name,
                    &[fakecloud_logs::ingest::IngestEvent {
                        timestamp_ms: now.timestamp_millis(),
                        message: line,
                    }],
                );
            }
        }

        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    /// Sign the canonical RRset for `answers` with `ksk`. Returns
    /// base64-encoded raw `r||s` (64 bytes for ECDSA-P256). `None`
    /// when the key material is unavailable (e.g., legacy snapshots
    /// pre-dating the DNSSEC fields).
    fn compute_rrsig_for_answers(
        &self,
        zone_name: &str,
        owner_name: &str,
        record_type: &str,
        ttl: u32,
        answers: &[String],
        ksk: &StoredKeySigningKey,
    ) -> Option<String> {
        if ksk.private_key_pem.is_empty() {
            return None;
        }
        let rtype_code = crate::dnssec::type_code(record_type)?;
        let rdatas: Vec<Vec<u8>> = answers
            .iter()
            .map(|v| crate::dnssec::encode_rdata(record_type, v))
            .collect();
        let canonical = crate::dnssec::canonical_rrset_bytes(
            owner_name,
            rtype_code,
            crate::dnssec::CLASS_IN,
            ttl,
            &rdatas,
        );
        let now = Utc::now().timestamp() as u32;
        let inception = now;
        let expiration = now.saturating_add(30 * 24 * 60 * 60); // 30 days
        let header = crate::dnssec::RrsigHeader {
            rtype: rtype_code,
            algorithm: crate::dnssec::DNSSEC_ALGORITHM,
            labels: crate::dnssec::label_count(owner_name),
            original_ttl: ttl,
            sig_expiration: expiration,
            sig_inception: inception,
            key_tag: ksk.key_tag as u16,
            signer_name: zone_name,
        };
        let signed = crate::dnssec::rrsig_signed_data(&header, &canonical);
        let sig = crate::dnssec::sign_with_pkcs8_pem(&ksk.private_key_pem, &signed);
        Some(crate::dnssec::b64(&sig))
    }
}

// ─── Health Check handlers ───────────────────────────────────────────

impl Route53Service {
    fn create_health_check(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let cfg: CreateHealthCheckRequest = xml_io::from_xml_root(&req.body)
            .map_err(|e| invalid_argument(format!("invalid CreateHealthCheckRequest XML: {e}")))?;
        if cfg.caller_reference.is_empty() {
            return Err(invalid_argument("CallerReference is required"));
        }
        if cfg.health_check_config.health_check_type.is_empty() {
            return Err(invalid_argument("HealthCheckConfig.Type is required"));
        }
        let mut state = self.state.write();
        let account = state
            .accounts
            .entry(DEFAULT_ACCOUNT.to_string())
            .or_default();
        if let Some(existing) = account
            .health_checks
            .values()
            .find(|h| h.caller_reference == cfg.caller_reference)
        {
            return Err(aws_error(
                StatusCode::CONFLICT,
                "HealthCheckAlreadyExists",
                format!(
                    "A health check with the same CallerReference already exists: {} (id={})",
                    cfg.caller_reference, existing.id
                ),
            ));
        }
        let id = generate_health_check_id();
        let stored = StoredHealthCheck {
            id: id.clone(),
            caller_reference: cfg.caller_reference,
            version: 1,
            config: cfg.health_check_config,
            created_time: Utc::now(),
            status: HealthCheckStatus::Success,
            last_failure_reason: None,
        };
        account.health_checks.insert(id.clone(), stored.clone());
        drop(state);
        let mut body = String::with_capacity(1024);
        body.push_str(XML_DECL);
        body.push_str(&format!("<CreateHealthCheckResponse xmlns=\"{NS}\">"));
        push_health_check(&mut body, &stored);
        body.push_str("</CreateHealthCheckResponse>");
        let mut headers = HeaderMap::new();
        if let Ok(loc) =
            http::HeaderValue::from_str(&format!("/2013-04-01/healthcheck/{}", stored.id))
        {
            headers.insert(http::header::LOCATION, loc);
        }
        Ok(xml_response(StatusCode::CREATED, body, headers))
    }

    fn get_health_check(&self, route: &Route) -> Result<AwsResponse, AwsServiceError> {
        let id = require_id(route)?;
        let state = self.state.read();
        let hc = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .and_then(|a| a.health_checks.get(&id).cloned())
            .ok_or_else(|| no_such_health_check(&id))?;
        drop(state);
        let mut body = String::with_capacity(512);
        body.push_str(XML_DECL);
        body.push_str(&format!("<GetHealthCheckResponse xmlns=\"{NS}\">"));
        push_health_check(&mut body, &hc);
        body.push_str("</GetHealthCheckResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    fn update_health_check(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = require_id(route)?;
        let cfg: UpdateHealthCheckRequest = xml_io::from_xml_root(&req.body)
            .map_err(|e| invalid_argument(format!("invalid UpdateHealthCheckRequest XML: {e}")))?;
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| no_such_health_check(&id))?;
        let hc = account
            .health_checks
            .get_mut(&id)
            .ok_or_else(|| no_such_health_check(&id))?;
        if let Some(client_version) = cfg.health_check_version {
            if client_version != hc.version {
                return Err(aws_error(
                    StatusCode::CONFLICT,
                    "HealthCheckVersionMismatch",
                    format!(
                        "Provided HealthCheckVersion ({}) does not match the current version ({})",
                        client_version, hc.version
                    ),
                ));
            }
        }
        if let Some(v) = cfg.ip_address {
            hc.config.ip_address = Some(v);
        }
        if let Some(v) = cfg.port {
            hc.config.port = Some(v);
        }
        if let Some(v) = cfg.resource_path {
            hc.config.resource_path = Some(v);
        }
        if let Some(v) = cfg.fully_qualified_domain_name {
            hc.config.fully_qualified_domain_name = Some(v);
        }
        if let Some(v) = cfg.search_string {
            hc.config.search_string = Some(v);
        }
        if let Some(v) = cfg.failure_threshold {
            hc.config.failure_threshold = Some(v);
        }
        if let Some(v) = cfg.inverted {
            hc.config.inverted = Some(v);
        }
        if let Some(v) = cfg.disabled {
            hc.config.disabled = Some(v);
        }
        if let Some(v) = cfg.health_threshold {
            hc.config.health_threshold = Some(v);
        }
        if let Some(v) = cfg.child_health_checks {
            hc.config.child_health_checks = Some(v);
        }
        if let Some(v) = cfg.enable_sni {
            hc.config.enable_sni = Some(v);
        }
        if let Some(v) = cfg.regions {
            hc.config.regions = Some(v);
        }
        if let Some(v) = cfg.alarm_identifier {
            hc.config.alarm_identifier = Some(v);
        }
        if let Some(v) = cfg.insufficient_data_health_status {
            hc.config.insufficient_data_health_status = Some(v);
        }
        if let Some(reset) = cfg.reset_elements {
            for name in reset.resettable_element_name {
                match name.as_str() {
                    "ChildHealthChecks" => hc.config.child_health_checks = None,
                    "FullyQualifiedDomainName" => hc.config.fully_qualified_domain_name = None,
                    "Regions" => hc.config.regions = None,
                    "ResourcePath" => hc.config.resource_path = None,
                    _ => {}
                }
            }
        }
        hc.version += 1;
        let snap = hc.clone();
        drop(state);
        let mut body = String::with_capacity(512);
        body.push_str(XML_DECL);
        body.push_str(&format!("<UpdateHealthCheckResponse xmlns=\"{NS}\">"));
        push_health_check(&mut body, &snap);
        body.push_str("</UpdateHealthCheckResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    fn delete_health_check(&self, route: &Route) -> Result<AwsResponse, AwsServiceError> {
        let id = require_id(route)?;
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| no_such_health_check(&id))?;
        if !account.health_checks.contains_key(&id) {
            return Err(no_such_health_check(&id));
        }
        // Real Route 53 returns HealthCheckInUse if any record set still
        // references the health check. Mirror that across all hosted zones.
        for zone in account.hosted_zones.values() {
            for rrset in &zone.resource_record_sets {
                if rrset.health_check_id.as_deref() == Some(id.as_str()) {
                    return Err(aws_error(
                        StatusCode::BAD_REQUEST,
                        "HealthCheckInUse",
                        format!(
                            "Health check {} is in use by record set {} ({}) in zone {}",
                            id, rrset.name, rrset.record_type, zone.id
                        ),
                    ));
                }
            }
        }
        account.health_checks.remove(&id);
        drop(state);
        let mut body = String::with_capacity(128);
        body.push_str(XML_DECL);
        body.push_str(&format!("<DeleteHealthCheckResponse xmlns=\"{NS}\"/>"));
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    fn list_health_checks(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        validate_query_constraints(
            &req.query_params,
            &[
                QueryConstraint::StrLen {
                    key: "marker",
                    min: 0,
                    max: 64,
                },
                MAX_ITEMS_CONSTRAINT,
            ],
        )?;
        let marker = req.query_params.get("marker").cloned();
        let max_items: usize = req
            .query_params
            .get("maxitems")
            .and_then(|s| s.parse().ok())
            .unwrap_or(100);
        let state = self.state.read();
        let mut hcs: Vec<StoredHealthCheck> = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .map(|a| a.health_checks.values().cloned().collect())
            .unwrap_or_default();
        drop(state);
        hcs.sort_by(|a, b| a.id.cmp(&b.id));
        let start = match &marker {
            Some(m) => hcs
                .iter()
                .position(|h| h.id.as_str() >= m.as_str())
                .unwrap_or(hcs.len()),
            None => 0,
        };
        let slice: Vec<StoredHealthCheck> =
            hcs.iter().skip(start).take(max_items).cloned().collect();
        let next_marker = if start + slice.len() < hcs.len() {
            Some(hcs[start + slice.len()].id.clone())
        } else {
            None
        };
        let mut body = String::with_capacity(1024);
        body.push_str(XML_DECL);
        body.push_str(&format!("<ListHealthChecksResponse xmlns=\"{NS}\">"));
        body.push_str("<HealthChecks>");
        for hc in &slice {
            push_health_check(&mut body, hc);
        }
        body.push_str("</HealthChecks>");
        if let Some(m) = &marker {
            body.push_str(&format!("<Marker>{}</Marker>", esc(m)));
        } else {
            body.push_str("<Marker></Marker>");
        }
        body.push_str(&format!("<MaxItems>{}</MaxItems>", max_items));
        body.push_str(&format!(
            "<IsTruncated>{}</IsTruncated>",
            next_marker.is_some()
        ));
        if let Some(nm) = &next_marker {
            body.push_str(&format!("<NextMarker>{}</NextMarker>", esc(nm)));
        }
        body.push_str("</ListHealthChecksResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    fn get_health_check_count(&self) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let count = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .map(|a| a.health_checks.len())
            .unwrap_or(0);
        drop(state);
        let mut body = String::with_capacity(128);
        body.push_str(XML_DECL);
        body.push_str(&format!("<GetHealthCheckCountResponse xmlns=\"{NS}\">"));
        body.push_str(&format!("<HealthCheckCount>{}</HealthCheckCount>", count));
        body.push_str("</GetHealthCheckCountResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    fn get_health_check_status(&self, route: &Route) -> Result<AwsResponse, AwsServiceError> {
        let id = require_id(route)?;
        let state = self.state.read();
        let hc = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .and_then(|a| a.health_checks.get(&id).cloned())
            .ok_or_else(|| no_such_health_check(&id))?;
        drop(state);
        let status_text = render_status_line(hc.status, hc.last_failure_reason.as_deref());
        let now = rfc3339(&Utc::now());
        let mut body = String::with_capacity(512);
        body.push_str(XML_DECL);
        body.push_str(&format!("<GetHealthCheckStatusResponse xmlns=\"{NS}\">"));
        body.push_str("<HealthCheckObservations>");
        for region in checker_regions() {
            body.push_str("<HealthCheckObservation>");
            body.push_str(&format!("<Region>{}</Region>", esc(region)));
            body.push_str(&format!(
                "<IPAddress>{}</IPAddress>",
                esc(&checker_ip_for_region(region))
            ));
            body.push_str("<StatusReport>");
            body.push_str(&format!("<Status>{}</Status>", esc(&status_text)));
            body.push_str(&format!("<CheckedTime>{}</CheckedTime>", now));
            body.push_str("</StatusReport>");
            body.push_str("</HealthCheckObservation>");
        }
        body.push_str("</HealthCheckObservations>");
        body.push_str("</GetHealthCheckStatusResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    /// Admin: flip a health check's status and optional last-failure
    /// reason. Powers the
    /// `POST /_fakecloud/route53/health-checks/{id}/status` endpoint
    /// in fakecloud-server. Returns `false` if the id doesn't exist.
    /// When `status = Success`, `last_failure_reason` is ignored and
    /// any prior reason is preserved so a later read of
    /// `GetHealthCheckLastFailureReason` still surfaces the historical
    /// observation. When `status` is one of the failure-flavoured
    /// variants (`Failure`, `Timeout`, `DnsError`) and
    /// `last_failure_reason` is `Some`, the stored reason is
    /// overwritten; `None` leaves the prior reason untouched.
    pub fn set_health_check_status(
        &self,
        id: &str,
        status: HealthCheckStatus,
        last_failure_reason: Option<String>,
    ) -> bool {
        let mut state = self.state.write();
        let Some(account) = state.accounts.get_mut(DEFAULT_ACCOUNT) else {
            return false;
        };
        let Some(hc) = account.health_checks.get_mut(id) else {
            return false;
        };
        hc.status = status;
        let is_failure_flavoured = matches!(
            status,
            HealthCheckStatus::Failure | HealthCheckStatus::Timeout | HealthCheckStatus::DnsError
        );
        if is_failure_flavoured && last_failure_reason.is_some() {
            hc.last_failure_reason = last_failure_reason;
        }
        true
    }

    /// Look up the public DNSSEC material for a zone's first ACTIVE
    /// KSK (sorted by name). Returns the DNSKEY public key, computed
    /// key tag, DS digest hex, and the KSK record so admin endpoints
    /// can surface a stable DNSSEC chain-of-trust to test code.
    /// Returns `None` if the zone has no ACTIVE KSK.
    pub fn dnssec_material_for_zone(
        &self,
        zone_id: &str,
    ) -> Option<(StoredKeySigningKey, Vec<u8>, u16, String)> {
        let state = self.state.read();
        let account = state.accounts.get(DEFAULT_ACCOUNT)?;
        let zone_id_clean = strip_zone_prefix(zone_id);
        // Existence check — return None when the zone is unknown
        // rather than synthesising bogus material.
        account.hosted_zones.get(&zone_id_clean)?;
        let ksk = account
            .key_signing_keys
            .values()
            .filter(|k| {
                k.hosted_zone_id == zone_id_clean && k.status.eq_ignore_ascii_case("ACTIVE")
            })
            .min_by(|a, b| a.name.cmp(&b.name))
            .cloned()?;
        let material = crate::dnssec::derive_keypair(&ksk.hosted_zone_id, &ksk.name);
        let key_tag = ksk.key_tag as u16;
        let ds_hex = ksk.ds_digest_hex.clone();
        Some((ksk, material.dnskey_public_key, key_tag, ds_hex))
    }

    /// Sign an arbitrary RRset with the zone's first ACTIVE KSK.
    /// Returns the base64 RRSIG bytes plus the tag/algorithm so the
    /// caller can construct a full RRSIG record. `None` when the zone
    /// or active KSK is missing or the record type isn't recognised.
    pub fn sign_rrset_with_zone_ksk(
        &self,
        zone_id: &str,
        owner_name: &str,
        record_type: &str,
        ttl: u32,
        rdata_values: &[String],
    ) -> Option<DnssecSignature> {
        let zone_id_clean = strip_zone_prefix(zone_id);
        let state = self.state.read();
        let account = state.accounts.get(DEFAULT_ACCOUNT)?;
        let zone = account.hosted_zones.get(&zone_id_clean)?;
        let zone_name = zone.name.clone();
        let ksk = account
            .key_signing_keys
            .values()
            .filter(|k| {
                k.hosted_zone_id == zone_id_clean && k.status.eq_ignore_ascii_case("ACTIVE")
            })
            .min_by(|a, b| a.name.cmp(&b.name))
            .cloned()?;
        drop(state);
        let rtype_code = crate::dnssec::type_code(record_type)?;
        let normalized_owner = if owner_name.ends_with('.') {
            owner_name.to_string()
        } else {
            format!("{owner_name}.")
        };
        let rdatas: Vec<Vec<u8>> = rdata_values
            .iter()
            .map(|v| crate::dnssec::encode_rdata(record_type, v))
            .collect();
        let canonical = crate::dnssec::canonical_rrset_bytes(
            &normalized_owner,
            rtype_code,
            crate::dnssec::CLASS_IN,
            ttl,
            &rdatas,
        );
        let now = Utc::now().timestamp() as u32;
        let inception = now;
        let expiration = now.saturating_add(30 * 24 * 60 * 60);
        let labels = crate::dnssec::label_count(&normalized_owner);
        let header = crate::dnssec::RrsigHeader {
            rtype: rtype_code,
            algorithm: crate::dnssec::DNSSEC_ALGORITHM,
            labels,
            original_ttl: ttl,
            sig_expiration: expiration,
            sig_inception: inception,
            key_tag: ksk.key_tag as u16,
            signer_name: &zone_name,
        };
        let signed = crate::dnssec::rrsig_signed_data(&header, &canonical);
        let sig = crate::dnssec::sign_with_pkcs8_pem(&ksk.private_key_pem, &signed);
        Some(DnssecSignature {
            signature_b64: crate::dnssec::b64(&sig),
            algorithm: crate::dnssec::DNSSEC_ALGORITHM,
            key_tag: ksk.key_tag as u16,
            signer_name: zone_name,
            inception,
            expiration,
            labels,
            original_ttl: ttl,
            rrset_type: record_type.to_string(),
        })
    }

    fn get_health_check_last_failure_reason(
        &self,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = require_id(route)?;
        let state = self.state.read();
        let hc = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .and_then(|a| a.health_checks.get(&id).cloned())
            .ok_or_else(|| no_such_health_check(&id))?;
        drop(state);
        let mut body = String::with_capacity(256);
        body.push_str(XML_DECL);
        body.push_str(&format!(
            "<GetHealthCheckLastFailureReasonResponse xmlns=\"{NS}\">"
        ));
        body.push_str("<HealthCheckObservations>");
        if let Some(reason) = hc.last_failure_reason.as_deref() {
            let now = rfc3339(&Utc::now());
            for region in checker_regions() {
                body.push_str("<HealthCheckObservation>");
                body.push_str(&format!("<Region>{}</Region>", esc(region)));
                body.push_str(&format!(
                    "<IPAddress>{}</IPAddress>",
                    esc(&checker_ip_for_region(region))
                ));
                body.push_str("<StatusReport>");
                body.push_str(&format!("<Status>{}</Status>", esc(reason)));
                body.push_str(&format!("<CheckedTime>{}</CheckedTime>", now));
                body.push_str("</StatusReport>");
                body.push_str("</HealthCheckObservation>");
            }
        }
        body.push_str("</HealthCheckObservations>");
        body.push_str("</GetHealthCheckLastFailureReasonResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    fn get_checker_ip_ranges(&self) -> Result<AwsResponse, AwsServiceError> {
        let mut body = String::with_capacity(512);
        body.push_str(XML_DECL);
        body.push_str(&format!("<GetCheckerIpRangesResponse xmlns=\"{NS}\">"));
        body.push_str("<CheckerIpRanges>");
        for cidr in CHECKER_IP_RANGES {
            body.push_str(&format!("<member>{}</member>", esc(cidr)));
        }
        body.push_str("</CheckerIpRanges>");
        body.push_str("</GetCheckerIpRangesResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }
}

// ─── Traffic Policy handlers ─────────────────────────────────────────

impl Route53Service {
    fn create_traffic_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let cfg: CreateTrafficPolicyRequest = xml_io::from_xml_root(&req.body).map_err(|e| {
            invalid_argument(format!("invalid CreateTrafficPolicyRequest XML: {e}"))
        })?;
        if cfg.name.is_empty() {
            return Err(invalid_argument("Name is required"));
        }
        if cfg.document.is_empty() {
            return Err(invalid_argument("Document is required"));
        }
        let policy_type = infer_policy_type(&cfg.document);
        let mut state = self.state.write();
        let account = state
            .accounts
            .entry(DEFAULT_ACCOUNT.to_string())
            .or_default();
        // Match real Route 53: name uniqueness applies across all versions
        // of every existing policy. Checking only version == 1 would let a
        // duplicate name slip through whenever the v1 row had been deleted
        // but later versions remained.
        if account
            .traffic_policies
            .values()
            .any(|p| p.name == cfg.name)
        {
            return Err(aws_error(
                StatusCode::CONFLICT,
                "TrafficPolicyAlreadyExists",
                format!("A traffic policy named '{}' already exists", cfg.name),
            ));
        }
        let id = generate_traffic_policy_id();
        let stored = StoredTrafficPolicy {
            id: id.clone(),
            version: 1,
            name: cfg.name,
            policy_type,
            document: cfg.document,
            comment: cfg.comment,
            created_time: Utc::now(),
        };
        account
            .traffic_policies
            .insert((id.clone(), 1), stored.clone());
        drop(state);
        let mut body = String::with_capacity(512);
        body.push_str(XML_DECL);
        body.push_str(&format!("<CreateTrafficPolicyResponse xmlns=\"{NS}\">"));
        push_traffic_policy(&mut body, &stored);
        body.push_str("</CreateTrafficPolicyResponse>");
        let mut headers = HeaderMap::new();
        if let Ok(loc) = http::HeaderValue::from_str(&format!(
            "/2013-04-01/trafficpolicy/{}/{}",
            stored.id, stored.version
        )) {
            headers.insert(http::header::LOCATION, loc);
        }
        Ok(xml_response(StatusCode::CREATED, body, headers))
    }

    fn create_traffic_policy_version(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = require_id(route)?;
        let cfg: CreateTrafficPolicyVersionRequest =
            xml_io::from_xml_root(&req.body).map_err(|e| {
                invalid_argument(format!(
                    "invalid CreateTrafficPolicyVersionRequest XML: {e}"
                ))
            })?;
        if cfg.document.is_empty() {
            return Err(invalid_argument("Document is required"));
        }
        let policy_type = infer_policy_type(&cfg.document);
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| no_such_traffic_policy(&id))?;
        let existing_versions: Vec<i64> = account
            .traffic_policies
            .keys()
            .filter(|(pid, _)| pid == &id)
            .map(|(_, v)| *v)
            .collect();
        if existing_versions.is_empty() {
            return Err(no_such_traffic_policy(&id));
        }
        let next_version = existing_versions.iter().max().copied().unwrap_or(0) + 1;
        // Borrow name from the latest existing version so the new one stays consistent.
        let (name, original_comment) = {
            let latest_v = existing_versions.iter().max().copied().unwrap();
            let p = account
                .traffic_policies
                .get(&(id.clone(), latest_v))
                .unwrap();
            (p.name.clone(), p.comment.clone())
        };
        let stored = StoredTrafficPolicy {
            id: id.clone(),
            version: next_version,
            name,
            policy_type,
            document: cfg.document,
            comment: cfg.comment.or(original_comment),
            created_time: Utc::now(),
        };
        account
            .traffic_policies
            .insert((id.clone(), next_version), stored.clone());
        drop(state);
        let mut body = String::with_capacity(512);
        body.push_str(XML_DECL);
        body.push_str(&format!(
            "<CreateTrafficPolicyVersionResponse xmlns=\"{NS}\">"
        ));
        push_traffic_policy(&mut body, &stored);
        body.push_str("</CreateTrafficPolicyVersionResponse>");
        let mut headers = HeaderMap::new();
        if let Ok(loc) = http::HeaderValue::from_str(&format!(
            "/2013-04-01/trafficpolicy/{}/{}",
            stored.id, stored.version
        )) {
            headers.insert(http::header::LOCATION, loc);
        }
        Ok(xml_response(StatusCode::CREATED, body, headers))
    }

    fn get_traffic_policy(&self, route: &Route) -> Result<AwsResponse, AwsServiceError> {
        let id = require_id(route)?;
        let version = require_version(route)?;
        let state = self.state.read();
        let p = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .and_then(|a| a.traffic_policies.get(&(id.clone(), version)).cloned())
            .ok_or_else(|| no_such_traffic_policy(&id))?;
        drop(state);
        let mut body = String::with_capacity(512);
        body.push_str(XML_DECL);
        body.push_str(&format!("<GetTrafficPolicyResponse xmlns=\"{NS}\">"));
        push_traffic_policy(&mut body, &p);
        body.push_str("</GetTrafficPolicyResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    fn update_traffic_policy_comment(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = require_id(route)?;
        let version = require_version(route)?;
        let cfg: UpdateTrafficPolicyCommentRequest =
            xml_io::from_xml_root(&req.body).map_err(|e| {
                invalid_argument(format!(
                    "invalid UpdateTrafficPolicyCommentRequest XML: {e}"
                ))
            })?;
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| no_such_traffic_policy(&id))?;
        let p = account
            .traffic_policies
            .get_mut(&(id.clone(), version))
            .ok_or_else(|| no_such_traffic_policy(&id))?;
        p.comment = Some(cfg.comment);
        let snap = p.clone();
        drop(state);
        let mut body = String::with_capacity(512);
        body.push_str(XML_DECL);
        body.push_str(&format!(
            "<UpdateTrafficPolicyCommentResponse xmlns=\"{NS}\">"
        ));
        push_traffic_policy(&mut body, &snap);
        body.push_str("</UpdateTrafficPolicyCommentResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    fn delete_traffic_policy(&self, route: &Route) -> Result<AwsResponse, AwsServiceError> {
        let id = require_id(route)?;
        let version = require_version(route)?;
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| no_such_traffic_policy(&id))?;
        if !account
            .traffic_policies
            .contains_key(&(id.clone(), version))
        {
            return Err(no_such_traffic_policy(&id));
        }
        // TrafficPolicyInUse if any instance still references this (id, version).
        if account
            .traffic_policy_instances
            .values()
            .any(|i| i.traffic_policy_id == id && i.traffic_policy_version == version)
        {
            return Err(aws_error(
                StatusCode::BAD_REQUEST,
                "TrafficPolicyInUse",
                format!("Traffic policy {}/{} is in use by an instance", id, version),
            ));
        }
        account.traffic_policies.remove(&(id, version));
        drop(state);
        let mut body = String::with_capacity(128);
        body.push_str(XML_DECL);
        body.push_str(&format!("<DeleteTrafficPolicyResponse xmlns=\"{NS}\"/>"));
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    fn list_traffic_policies(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        validate_query_constraints(
            &req.query_params,
            &[
                QueryConstraint::StrLen {
                    key: "trafficpolicyid",
                    min: 1,
                    max: 36,
                },
                MAX_ITEMS_CONSTRAINT,
            ],
        )?;
        let marker = req.query_params.get("trafficpolicyid").cloned();
        let max_items: usize = req
            .query_params
            .get("maxitems")
            .and_then(|s| s.parse().ok())
            .unwrap_or(100);
        let state = self.state.read();
        // Group by policy id; emit only the latest version of each.
        let mut latest: BTreeMap<String, StoredTrafficPolicy> = BTreeMap::new();
        let mut counts: BTreeMap<String, i64> = BTreeMap::new();
        if let Some(account) = state.accounts.get(DEFAULT_ACCOUNT) {
            for p in account.traffic_policies.values() {
                let entry = latest.entry(p.id.clone()).or_insert_with(|| p.clone());
                if p.version > entry.version {
                    *entry = p.clone();
                }
                *counts.entry(p.id.clone()).or_insert(0) += 1;
            }
        }
        drop(state);
        let mut summaries: Vec<StoredTrafficPolicy> = latest.into_values().collect();
        summaries.sort_by(|a, b| a.id.cmp(&b.id));
        let start = match &marker {
            Some(m) => summaries
                .iter()
                .position(|p| p.id.as_str() >= m.as_str())
                .unwrap_or(summaries.len()),
            None => 0,
        };
        let slice: Vec<&StoredTrafficPolicy> =
            summaries.iter().skip(start).take(max_items).collect();
        let next_marker = if start + slice.len() < summaries.len() {
            Some(summaries[start + slice.len()].id.clone())
        } else {
            None
        };
        let mut body = String::with_capacity(1024);
        body.push_str(XML_DECL);
        body.push_str(&format!("<ListTrafficPoliciesResponse xmlns=\"{NS}\">"));
        body.push_str("<TrafficPolicySummaries>");
        for p in &slice {
            push_traffic_policy_summary(&mut body, p, counts.get(&p.id).copied().unwrap_or(1));
        }
        body.push_str("</TrafficPolicySummaries>");
        body.push_str(&format!(
            "<IsTruncated>{}</IsTruncated>",
            next_marker.is_some()
        ));
        body.push_str(&format!("<MaxItems>{}</MaxItems>", max_items));
        if let Some(nm) = &next_marker {
            body.push_str(&format!(
                "<TrafficPolicyIdMarker>{}</TrafficPolicyIdMarker>",
                esc(nm)
            ));
        }
        body.push_str("</ListTrafficPoliciesResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    fn list_traffic_policy_versions(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = require_id(route)?;
        let marker: Option<i64> = req
            .query_params
            .get("trafficpolicyversion")
            .and_then(|s| s.parse().ok());
        let max_items: usize = req
            .query_params
            .get("maxitems")
            .and_then(|s| s.parse().ok())
            .unwrap_or(100);
        let state = self.state.read();
        let mut versions: Vec<StoredTrafficPolicy> = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .map(|a| {
                a.traffic_policies
                    .values()
                    .filter(|p| p.id == id)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        drop(state);
        if versions.is_empty() {
            return Err(no_such_traffic_policy(&id));
        }
        versions.sort_by_key(|p| p.version);
        let start = match marker {
            Some(m) => versions
                .iter()
                .position(|p| p.version >= m)
                .unwrap_or(versions.len()),
            None => 0,
        };
        let slice: Vec<&StoredTrafficPolicy> =
            versions.iter().skip(start).take(max_items).collect();
        let next_marker = if start + slice.len() < versions.len() {
            Some(versions[start + slice.len()].version)
        } else {
            None
        };
        let mut body = String::with_capacity(1024);
        body.push_str(XML_DECL);
        body.push_str(&format!(
            "<ListTrafficPolicyVersionsResponse xmlns=\"{NS}\">"
        ));
        body.push_str("<TrafficPolicies>");
        for p in &slice {
            push_traffic_policy(&mut body, p);
        }
        body.push_str("</TrafficPolicies>");
        body.push_str(&format!(
            "<IsTruncated>{}</IsTruncated>",
            next_marker.is_some()
        ));
        body.push_str(&format!("<MaxItems>{}</MaxItems>", max_items));
        if let Some(nm) = next_marker {
            body.push_str(&format!(
                "<TrafficPolicyVersionMarker>{}</TrafficPolicyVersionMarker>",
                nm
            ));
        } else {
            body.push_str("<TrafficPolicyVersionMarker></TrafficPolicyVersionMarker>");
        }
        body.push_str("</ListTrafficPolicyVersionsResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }
}

// ─── Traffic Policy Instance handlers ────────────────────────────────

impl Route53Service {
    fn create_traffic_policy_instance(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let cfg: CreateTrafficPolicyInstanceRequest =
            xml_io::from_xml_root(&req.body).map_err(|e| {
                invalid_argument(format!(
                    "invalid CreateTrafficPolicyInstanceRequest XML: {e}"
                ))
            })?;
        if cfg.hosted_zone_id.is_empty() || cfg.name.is_empty() || cfg.traffic_policy_id.is_empty()
        {
            return Err(invalid_argument(
                "HostedZoneId, Name, and TrafficPolicyId are required",
            ));
        }
        let zone_id = strip_zone_prefix(&cfg.hosted_zone_id);
        let mut state = self.state.write();
        let account = state
            .accounts
            .entry(DEFAULT_ACCOUNT.to_string())
            .or_default();
        if !account.hosted_zones.contains_key(&zone_id) {
            return Err(no_such_hosted_zone(&zone_id));
        }
        let policy = account
            .traffic_policies
            .get(&(cfg.traffic_policy_id.clone(), cfg.traffic_policy_version))
            .cloned()
            .ok_or_else(|| no_such_traffic_policy(&cfg.traffic_policy_id))?;
        let mut name = cfg.name.clone();
        if !name.ends_with('.') {
            name.push('.');
        }
        // Real Route 53 rejects a duplicate (HostedZoneId, Name, Type) instance
        // with TrafficPolicyInstanceAlreadyExists. Mirror that.
        if account.traffic_policy_instances.values().any(|i| {
            i.hosted_zone_id == zone_id
                && i.name == name
                && i.traffic_policy_type == policy.policy_type
        }) {
            return Err(aws_error(
                StatusCode::CONFLICT,
                "TrafficPolicyInstanceAlreadyExists",
                format!(
                    "A traffic policy instance for {} ({}) already exists in zone {}",
                    name, policy.policy_type, zone_id
                ),
            ));
        }
        let id = generate_traffic_policy_instance_id();
        let stored = StoredTrafficPolicyInstance {
            id: id.clone(),
            hosted_zone_id: zone_id,
            name,
            ttl: cfg.ttl,
            state: "Applied".to_string(),
            message: String::new(),
            traffic_policy_id: cfg.traffic_policy_id,
            traffic_policy_version: cfg.traffic_policy_version,
            traffic_policy_type: policy.policy_type,
            created_time: Utc::now(),
        };
        account
            .traffic_policy_instances
            .insert(id.clone(), stored.clone());
        drop(state);
        let mut body = String::with_capacity(512);
        body.push_str(XML_DECL);
        body.push_str(&format!(
            "<CreateTrafficPolicyInstanceResponse xmlns=\"{NS}\">"
        ));
        push_traffic_policy_instance(&mut body, &stored);
        body.push_str("</CreateTrafficPolicyInstanceResponse>");
        let mut headers = HeaderMap::new();
        if let Ok(loc) =
            http::HeaderValue::from_str(&format!("/2013-04-01/trafficpolicyinstance/{}", stored.id))
        {
            headers.insert(http::header::LOCATION, loc);
        }
        Ok(xml_response(StatusCode::CREATED, body, headers))
    }

    fn get_traffic_policy_instance(&self, route: &Route) -> Result<AwsResponse, AwsServiceError> {
        let id = require_id(route)?;
        let state = self.state.read();
        let i = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .and_then(|a| a.traffic_policy_instances.get(&id).cloned())
            .ok_or_else(|| no_such_traffic_policy_instance(&id))?;
        drop(state);
        let mut body = String::with_capacity(512);
        body.push_str(XML_DECL);
        body.push_str(&format!(
            "<GetTrafficPolicyInstanceResponse xmlns=\"{NS}\">"
        ));
        push_traffic_policy_instance(&mut body, &i);
        body.push_str("</GetTrafficPolicyInstanceResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    fn update_traffic_policy_instance(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = require_id(route)?;
        let cfg: UpdateTrafficPolicyInstanceRequest =
            xml_io::from_xml_root(&req.body).map_err(|e| {
                invalid_argument(format!(
                    "invalid UpdateTrafficPolicyInstanceRequest XML: {e}"
                ))
            })?;
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| no_such_traffic_policy_instance(&id))?;
        let policy = account
            .traffic_policies
            .get(&(cfg.traffic_policy_id.clone(), cfg.traffic_policy_version))
            .cloned()
            .ok_or_else(|| no_such_traffic_policy(&cfg.traffic_policy_id))?;
        let i = account
            .traffic_policy_instances
            .get_mut(&id)
            .ok_or_else(|| no_such_traffic_policy_instance(&id))?;
        i.ttl = cfg.ttl;
        i.traffic_policy_id = cfg.traffic_policy_id;
        i.traffic_policy_version = cfg.traffic_policy_version;
        i.traffic_policy_type = policy.policy_type;
        i.state = "Applied".to_string();
        let snap = i.clone();
        drop(state);
        let mut body = String::with_capacity(512);
        body.push_str(XML_DECL);
        body.push_str(&format!(
            "<UpdateTrafficPolicyInstanceResponse xmlns=\"{NS}\">"
        ));
        push_traffic_policy_instance(&mut body, &snap);
        body.push_str("</UpdateTrafficPolicyInstanceResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    fn delete_traffic_policy_instance(
        &self,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = require_id(route)?;
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| no_such_traffic_policy_instance(&id))?;
        if account.traffic_policy_instances.remove(&id).is_none() {
            return Err(no_such_traffic_policy_instance(&id));
        }
        drop(state);
        let mut body = String::with_capacity(128);
        body.push_str(XML_DECL);
        body.push_str(&format!(
            "<DeleteTrafficPolicyInstanceResponse xmlns=\"{NS}\"/>"
        ));
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    fn list_traffic_policy_instances(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        validate_query_constraints(
            &req.query_params,
            &[
                QueryConstraint::StrLen {
                    key: "hostedzoneid",
                    min: 0,
                    max: 32,
                },
                QueryConstraint::StrLen {
                    key: "trafficpolicyinstancename",
                    min: 0,
                    max: 1024,
                },
                QueryConstraint::Enum {
                    key: "trafficpolicyinstancetype",
                    allowed: &RR_TYPES,
                },
                MAX_ITEMS_CONSTRAINT,
            ],
        )?;
        let max_items: usize = req
            .query_params
            .get("maxitems")
            .and_then(|s| s.parse().ok())
            .unwrap_or(100);
        let state = self.state.read();
        let mut instances: Vec<StoredTrafficPolicyInstance> = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .map(|a| a.traffic_policy_instances.values().cloned().collect())
            .unwrap_or_default();
        drop(state);
        instances.sort_by(|a, b| a.id.cmp(&b.id));
        let slice: Vec<&StoredTrafficPolicyInstance> = instances.iter().take(max_items).collect();
        let next_marker = if slice.len() < instances.len() {
            Some(instances[slice.len()].id.clone())
        } else {
            None
        };
        let mut body = String::with_capacity(1024);
        body.push_str(XML_DECL);
        body.push_str(&format!(
            "<ListTrafficPolicyInstancesResponse xmlns=\"{NS}\">"
        ));
        body.push_str("<TrafficPolicyInstances>");
        for i in &slice {
            push_traffic_policy_instance(&mut body, i);
        }
        body.push_str("</TrafficPolicyInstances>");
        body.push_str(&format!(
            "<IsTruncated>{}</IsTruncated>",
            next_marker.is_some()
        ));
        body.push_str(&format!("<MaxItems>{}</MaxItems>", max_items));
        if let Some(nm) = &next_marker {
            body.push_str(&format!(
                "<TrafficPolicyInstanceNameMarker>{}</TrafficPolicyInstanceNameMarker>",
                esc(nm)
            ));
        }
        body.push_str("</ListTrafficPolicyInstancesResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    fn list_traffic_policy_instances_by_hosted_zone(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let zone_id = req
            .query_params
            .get("id")
            .cloned()
            .ok_or_else(|| invalid_argument("id query parameter is required"))?;
        let zone_id = strip_zone_prefix(&zone_id);
        let max_items: usize = req
            .query_params
            .get("maxitems")
            .and_then(|s| s.parse().ok())
            .unwrap_or(100);
        let state = self.state.read();
        let account = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .ok_or_else(|| no_such_hosted_zone(&zone_id))?;
        if !account.hosted_zones.contains_key(&zone_id) {
            return Err(no_such_hosted_zone(&zone_id));
        }
        let mut instances: Vec<StoredTrafficPolicyInstance> = account
            .traffic_policy_instances
            .values()
            .filter(|i| i.hosted_zone_id == zone_id)
            .cloned()
            .collect();
        drop(state);
        instances.sort_by(|a, b| a.id.cmp(&b.id));
        let slice: Vec<&StoredTrafficPolicyInstance> = instances.iter().take(max_items).collect();
        let truncated = slice.len() < instances.len();
        let mut body = String::with_capacity(1024);
        body.push_str(XML_DECL);
        body.push_str(&format!(
            "<ListTrafficPolicyInstancesByHostedZoneResponse xmlns=\"{NS}\">"
        ));
        body.push_str("<TrafficPolicyInstances>");
        for i in &slice {
            push_traffic_policy_instance(&mut body, i);
        }
        body.push_str("</TrafficPolicyInstances>");
        body.push_str(&format!("<IsTruncated>{}</IsTruncated>", truncated));
        body.push_str(&format!("<MaxItems>{}</MaxItems>", max_items));
        if truncated {
            body.push_str(&format!(
                "<TrafficPolicyInstanceNameMarker>{}</TrafficPolicyInstanceNameMarker>",
                esc(&instances[slice.len()].id)
            ));
        }
        body.push_str("</ListTrafficPolicyInstancesByHostedZoneResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    fn list_traffic_policy_instances_by_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        validate_query_constraints(
            &req.query_params,
            &[
                QueryConstraint::StrLen {
                    key: "id",
                    min: 1,
                    max: 36,
                },
                QueryConstraint::IntRange {
                    key: "version",
                    min: 1,
                    max: 1000,
                },
                QueryConstraint::StrLen {
                    key: "hostedzoneid",
                    min: 0,
                    max: 32,
                },
                QueryConstraint::StrLen {
                    key: "trafficpolicyinstancename",
                    min: 0,
                    max: 1024,
                },
                QueryConstraint::Enum {
                    key: "trafficpolicyinstancetype",
                    allowed: &RR_TYPES,
                },
                MAX_ITEMS_CONSTRAINT,
            ],
        )?;
        let policy_id = req
            .query_params
            .get("id")
            .cloned()
            .ok_or_else(|| invalid_argument("id query parameter is required"))?;
        // `version` is `@required` in the Smithy model. Match AWS by rejecting
        // requests that omit it (a `negative_omit_TrafficPolicyVersion` probe
        // strips the query param entirely; without this guard the handler
        // would silently treat the filter as "any version" and return 200).
        let version_raw = req
            .query_params
            .get("version")
            .cloned()
            .ok_or_else(|| invalid_argument("version query parameter is required"))?;
        let version: i64 = version_raw.parse().map_err(|_| {
            invalid_argument(format!(
                "'version' value '{version_raw}' is not a valid integer"
            ))
        })?;
        let max_items: usize = req
            .query_params
            .get("maxitems")
            .and_then(|s| s.parse().ok())
            .unwrap_or(100);
        let state = self.state.read();
        let mut instances: Vec<StoredTrafficPolicyInstance> = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .map(|a| {
                a.traffic_policy_instances
                    .values()
                    .filter(|i| {
                        i.traffic_policy_id == policy_id && i.traffic_policy_version == version
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        drop(state);
        instances.sort_by(|a, b| a.id.cmp(&b.id));
        let slice: Vec<&StoredTrafficPolicyInstance> = instances.iter().take(max_items).collect();
        let truncated = slice.len() < instances.len();
        let mut body = String::with_capacity(1024);
        body.push_str(XML_DECL);
        body.push_str(&format!(
            "<ListTrafficPolicyInstancesByPolicyResponse xmlns=\"{NS}\">"
        ));
        body.push_str("<TrafficPolicyInstances>");
        for i in &slice {
            push_traffic_policy_instance(&mut body, i);
        }
        body.push_str("</TrafficPolicyInstances>");
        body.push_str(&format!("<IsTruncated>{}</IsTruncated>", truncated));
        body.push_str(&format!("<MaxItems>{}</MaxItems>", max_items));
        if truncated {
            body.push_str(&format!(
                "<TrafficPolicyInstanceNameMarker>{}</TrafficPolicyInstanceNameMarker>",
                esc(&instances[slice.len()].id)
            ));
        }
        body.push_str("</ListTrafficPolicyInstancesByPolicyResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    fn get_traffic_policy_instance_count(&self) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let count = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .map(|a| a.traffic_policy_instances.len())
            .unwrap_or(0);
        drop(state);
        let mut body = String::with_capacity(128);
        body.push_str(XML_DECL);
        body.push_str(&format!(
            "<GetTrafficPolicyInstanceCountResponse xmlns=\"{NS}\">"
        ));
        body.push_str(&format!(
            "<TrafficPolicyInstanceCount>{}</TrafficPolicyInstanceCount>",
            count
        ));
        body.push_str("</GetTrafficPolicyInstanceCountResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }
}

// ─── DNSSEC + KSK handlers ───────────────────────────────────────────

impl Route53Service {
    fn get_dnssec(&self, route: &Route) -> Result<AwsResponse, AwsServiceError> {
        let zone_id = strip_zone_prefix(&require_id(route)?);
        let state = self.state.read();
        let account = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .ok_or_else(|| no_such_hosted_zone(&zone_id))?;
        if !account.hosted_zones.contains_key(&zone_id) {
            return Err(no_such_hosted_zone(&zone_id));
        }
        let status = account
            .dnssec_status
            .get(&zone_id)
            .cloned()
            .unwrap_or_else(|| "NOT_SIGNING".to_string());
        let ksks: Vec<StoredKeySigningKey> = account
            .key_signing_keys
            .values()
            .filter(|k| k.hosted_zone_id == zone_id)
            .cloned()
            .collect();
        drop(state);
        let mut body = String::with_capacity(512);
        body.push_str(XML_DECL);
        body.push_str(&format!("<GetDNSSECResponse xmlns=\"{NS}\">"));
        body.push_str("<Status>");
        body.push_str(&format!(
            "<ServeSignature>{}</ServeSignature>",
            esc(&status)
        ));
        body.push_str("</Status>");
        body.push_str("<KeySigningKeys>");
        for k in &ksks {
            // KeySigningKeys list members lack `xmlName`, so the AWS SDK
            // expects the default `<member>` element name.
            body.push_str("<member>");
            push_key_signing_key_inner(&mut body, k);
            body.push_str("</member>");
        }
        body.push_str("</KeySigningKeys>");
        body.push_str("</GetDNSSECResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    fn enable_hosted_zone_dnssec(&self, route: &Route) -> Result<AwsResponse, AwsServiceError> {
        let zone_id = strip_zone_prefix(&require_id(route)?);
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| no_such_hosted_zone(&zone_id))?;
        if !account.hosted_zones.contains_key(&zone_id) {
            return Err(no_such_hosted_zone(&zone_id));
        }
        account
            .dnssec_status
            .insert(zone_id.clone(), "SIGNING".to_string());
        let change = StoredChange {
            id: generate_change_id(),
            status: "PENDING".to_string(),
            submitted_at: Utc::now(),
            comment: Some(format!("EnableHostedZoneDNSSEC {}", zone_id)),
            read_count: 0,
        };
        account.changes.insert(change.id.clone(), change.clone());
        drop(state);
        let mut body = String::with_capacity(256);
        body.push_str(XML_DECL);
        body.push_str(&format!("<EnableHostedZoneDNSSECResponse xmlns=\"{NS}\">"));
        push_change_info(&mut body, &change);
        body.push_str("</EnableHostedZoneDNSSECResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    fn disable_hosted_zone_dnssec(&self, route: &Route) -> Result<AwsResponse, AwsServiceError> {
        let zone_id = strip_zone_prefix(&require_id(route)?);
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| no_such_hosted_zone(&zone_id))?;
        if !account.hosted_zones.contains_key(&zone_id) {
            return Err(no_such_hosted_zone(&zone_id));
        }
        account
            .dnssec_status
            .insert(zone_id.clone(), "NOT_SIGNING".to_string());
        let change = StoredChange {
            id: generate_change_id(),
            status: "PENDING".to_string(),
            submitted_at: Utc::now(),
            comment: Some(format!("DisableHostedZoneDNSSEC {}", zone_id)),
            read_count: 0,
        };
        account.changes.insert(change.id.clone(), change.clone());
        drop(state);
        let mut body = String::with_capacity(256);
        body.push_str(XML_DECL);
        body.push_str(&format!("<DisableHostedZoneDNSSECResponse xmlns=\"{NS}\">"));
        push_change_info(&mut body, &change);
        body.push_str("</DisableHostedZoneDNSSECResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    fn create_key_signing_key(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let cfg: CreateKeySigningKeyRequest = xml_io::from_xml_root(&req.body).map_err(|e| {
            invalid_argument(format!("invalid CreateKeySigningKeyRequest XML: {e}"))
        })?;
        if cfg.caller_reference.is_empty()
            || cfg.hosted_zone_id.is_empty()
            || cfg.key_management_service_arn.is_empty()
            || cfg.name.is_empty()
            || cfg.status.is_empty()
        {
            return Err(invalid_argument(
                "CallerReference, HostedZoneId, KeyManagementServiceArn, Name, Status all required",
            ));
        }
        let zone_id = strip_zone_prefix(&cfg.hosted_zone_id);
        let mut state = self.state.write();
        let account = state
            .accounts
            .entry(DEFAULT_ACCOUNT.to_string())
            .or_default();
        if !account.hosted_zones.contains_key(&zone_id) {
            return Err(no_such_hosted_zone(&zone_id));
        }
        // Real Route 53 enforces unique KSK Name per zone and unique KMS ARN per zone.
        if account
            .key_signing_keys
            .contains_key(&(zone_id.clone(), cfg.name.clone()))
        {
            return Err(aws_error(
                StatusCode::CONFLICT,
                "KeySigningKeyAlreadyExists",
                format!(
                    "A key-signing key named '{}' already exists in zone {}",
                    cfg.name, zone_id
                ),
            ));
        }
        let now = Utc::now();
        // Derive a deterministic ECDSA P-256 keypair so persistence reloads
        // the same DNSKEY/DS material, then compute the standard DNSSEC
        // key tag and DS digest for the parent zone to publish.
        let key_material = crate::dnssec::derive_keypair(&zone_id, &cfg.name);
        let key_tag = crate::dnssec::key_tag_for(&key_material.dnskey_public_key);
        let zone_name = account
            .hosted_zones
            .get(&zone_id)
            .map(|z| z.name.clone())
            .unwrap_or_else(|| ".".to_string());
        let ds_digest_hex =
            crate::dnssec::ds_digest_sha256(&zone_name, key_tag, &key_material.dnskey_public_key);
        let ksk = StoredKeySigningKey {
            hosted_zone_id: zone_id.clone(),
            name: cfg.name.clone(),
            kms_arn: cfg.key_management_service_arn,
            status: cfg.status,
            caller_reference: cfg.caller_reference,
            created_date: now,
            last_modified_date: now,
            key_tag: key_tag as i32,
            private_key_pem: key_material.private_key_pem,
            public_key_der: key_material.public_key_der,
            ds_digest_hex,
        };
        account
            .key_signing_keys
            .insert((zone_id.clone(), cfg.name.clone()), ksk.clone());
        let change = StoredChange {
            id: generate_change_id(),
            status: "PENDING".to_string(),
            submitted_at: now,
            comment: Some(format!("CreateKeySigningKey {}/{}", zone_id, cfg.name)),
            read_count: 0,
        };
        account.changes.insert(change.id.clone(), change.clone());
        drop(state);
        let mut body = String::with_capacity(512);
        body.push_str(XML_DECL);
        body.push_str(&format!("<CreateKeySigningKeyResponse xmlns=\"{NS}\">"));
        push_change_info(&mut body, &change);
        body.push_str("<KeySigningKey>");
        push_key_signing_key_inner(&mut body, &ksk);
        body.push_str("</KeySigningKey>");
        body.push_str("</CreateKeySigningKeyResponse>");
        let mut headers = HeaderMap::new();
        if let Ok(loc) = http::HeaderValue::from_str(&format!(
            "/2013-04-01/keysigningkey/{}/{}",
            zone_id, ksk.name
        )) {
            headers.insert(http::header::LOCATION, loc);
        }
        Ok(xml_response(StatusCode::CREATED, body, headers))
    }

    fn delete_key_signing_key(&self, route: &Route) -> Result<AwsResponse, AwsServiceError> {
        let (zone_id, name) = require_zone_and_name(route)?;
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| no_such_key_signing_key(&zone_id, &name))?;
        let ksk = account
            .key_signing_keys
            .get(&(zone_id.clone(), name.clone()))
            .ok_or_else(|| no_such_key_signing_key(&zone_id, &name))?;
        // Real Route 53 requires Status == INACTIVE before delete.
        if ksk.status.eq_ignore_ascii_case("ACTIVE") {
            return Err(aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidKeySigningKeyStatus",
                format!(
                    "KeySigningKey {}/{} must be deactivated before deletion",
                    zone_id, name
                ),
            ));
        }
        account
            .key_signing_keys
            .remove(&(zone_id.clone(), name.clone()));
        let change = StoredChange {
            id: generate_change_id(),
            status: "PENDING".to_string(),
            submitted_at: Utc::now(),
            comment: Some(format!("DeleteKeySigningKey {}/{}", zone_id, name)),
            read_count: 0,
        };
        account.changes.insert(change.id.clone(), change.clone());
        drop(state);
        let mut body = String::with_capacity(256);
        body.push_str(XML_DECL);
        body.push_str(&format!("<DeleteKeySigningKeyResponse xmlns=\"{NS}\">"));
        push_change_info(&mut body, &change);
        body.push_str("</DeleteKeySigningKeyResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    fn activate_key_signing_key(&self, route: &Route) -> Result<AwsResponse, AwsServiceError> {
        self.set_ksk_status(route, "ACTIVE", "ActivateKeySigningKey")
    }

    fn deactivate_key_signing_key(&self, route: &Route) -> Result<AwsResponse, AwsServiceError> {
        self.set_ksk_status(route, "INACTIVE", "DeactivateKeySigningKey")
    }

    fn set_ksk_status(
        &self,
        route: &Route,
        status: &str,
        op: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let (zone_id, name) = require_zone_and_name(route)?;
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| no_such_key_signing_key(&zone_id, &name))?;
        let ksk = account
            .key_signing_keys
            .get_mut(&(zone_id.clone(), name.clone()))
            .ok_or_else(|| no_such_key_signing_key(&zone_id, &name))?;
        ksk.status = status.to_string();
        ksk.last_modified_date = Utc::now();
        let change = StoredChange {
            id: generate_change_id(),
            status: "PENDING".to_string(),
            submitted_at: Utc::now(),
            comment: Some(format!("{} {}/{}", op, zone_id, name)),
            read_count: 0,
        };
        account.changes.insert(change.id.clone(), change.clone());
        drop(state);
        let mut body = String::with_capacity(256);
        body.push_str(XML_DECL);
        body.push_str(&format!("<{op}Response xmlns=\"{NS}\">"));
        push_change_info(&mut body, &change);
        body.push_str(&format!("</{op}Response>"));
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }
}

// ─── Query Logging handlers ──────────────────────────────────────────

impl Route53Service {
    fn create_query_logging_config(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let cfg: CreateQueryLoggingConfigRequest =
            xml_io::from_xml_root(&req.body).map_err(|e| {
                invalid_argument(format!("invalid CreateQueryLoggingConfigRequest XML: {e}"))
            })?;
        if cfg.hosted_zone_id.is_empty() || cfg.cloud_watch_logs_log_group_arn.is_empty() {
            return Err(invalid_argument(
                "HostedZoneId and CloudWatchLogsLogGroupArn are required",
            ));
        }
        let zone_id = strip_zone_prefix(&cfg.hosted_zone_id);
        let mut state = self.state.write();
        let account = state
            .accounts
            .entry(DEFAULT_ACCOUNT.to_string())
            .or_default();
        if let Some(zone) = account.hosted_zones.get(&zone_id) {
            if zone.private_zone {
                return Err(invalid_argument(
                    "Query logging is only supported for public hosted zones",
                ));
            }
        } else {
            return Err(no_such_hosted_zone(&zone_id));
        }
        // One config per zone.
        if account
            .query_logging_configs
            .values()
            .any(|c| c.hosted_zone_id == zone_id)
        {
            return Err(aws_error(
                StatusCode::CONFLICT,
                "QueryLoggingConfigAlreadyExists",
                format!("A query logging config already exists for zone {}", zone_id),
            ));
        }
        let id = Uuid::new_v4().to_string();
        let stored = StoredQueryLoggingConfig {
            id: id.clone(),
            hosted_zone_id: zone_id,
            cloud_watch_logs_log_group_arn: cfg.cloud_watch_logs_log_group_arn,
        };
        account
            .query_logging_configs
            .insert(id.clone(), stored.clone());
        drop(state);
        let mut body = String::with_capacity(256);
        body.push_str(XML_DECL);
        body.push_str(&format!(
            "<CreateQueryLoggingConfigResponse xmlns=\"{NS}\">"
        ));
        push_query_logging_config(&mut body, &stored);
        body.push_str("</CreateQueryLoggingConfigResponse>");
        let mut headers = HeaderMap::new();
        if let Ok(loc) =
            http::HeaderValue::from_str(&format!("/2013-04-01/queryloggingconfig/{}", stored.id))
        {
            headers.insert(http::header::LOCATION, loc);
        }
        Ok(xml_response(StatusCode::CREATED, body, headers))
    }

    fn get_query_logging_config(&self, route: &Route) -> Result<AwsResponse, AwsServiceError> {
        let id = require_id(route)?;
        let state = self.state.read();
        let cfg = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .and_then(|a| a.query_logging_configs.get(&id).cloned())
            .ok_or_else(|| no_such_query_logging_config(&id))?;
        drop(state);
        let mut body = String::with_capacity(256);
        body.push_str(XML_DECL);
        body.push_str(&format!("<GetQueryLoggingConfigResponse xmlns=\"{NS}\">"));
        push_query_logging_config(&mut body, &cfg);
        body.push_str("</GetQueryLoggingConfigResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    fn delete_query_logging_config(&self, route: &Route) -> Result<AwsResponse, AwsServiceError> {
        let id = require_id(route)?;
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| no_such_query_logging_config(&id))?;
        if account.query_logging_configs.remove(&id).is_none() {
            return Err(no_such_query_logging_config(&id));
        }
        drop(state);
        let mut body = String::with_capacity(128);
        body.push_str(XML_DECL);
        body.push_str(&format!(
            "<DeleteQueryLoggingConfigResponse xmlns=\"{NS}\"/>"
        ));
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    fn list_query_logging_configs(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        validate_query_constraints(
            &req.query_params,
            &[
                QueryConstraint::StrLen {
                    key: "hostedzoneid",
                    min: 0,
                    max: 32,
                },
                QueryConstraint::StrLen {
                    key: "nexttoken",
                    min: 0,
                    max: 1024,
                },
                MAX_RESULTS_CONSTRAINT,
            ],
        )?;
        let zone_filter = req.query_params.get("hostedzoneid").cloned();
        let max_items: usize = req
            .query_params
            .get("maxresults")
            .and_then(|s| s.parse().ok())
            .unwrap_or(100);
        let state = self.state.read();
        let mut configs: Vec<StoredQueryLoggingConfig> = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .map(|a| a.query_logging_configs.values().cloned().collect())
            .unwrap_or_default();
        drop(state);
        if let Some(zid) = zone_filter {
            let z = strip_zone_prefix(&zid);
            configs.retain(|c| c.hosted_zone_id == z);
        }
        configs.sort_by(|a, b| a.id.cmp(&b.id));
        let slice: Vec<&StoredQueryLoggingConfig> = configs.iter().take(max_items).collect();
        let next = if slice.len() < configs.len() {
            Some(configs[slice.len()].id.clone())
        } else {
            None
        };
        let mut body = String::with_capacity(512);
        body.push_str(XML_DECL);
        body.push_str(&format!("<ListQueryLoggingConfigsResponse xmlns=\"{NS}\">"));
        body.push_str("<QueryLoggingConfigs>");
        for c in &slice {
            push_query_logging_config(&mut body, c);
        }
        body.push_str("</QueryLoggingConfigs>");
        if let Some(n) = &next {
            body.push_str(&format!("<NextToken>{}</NextToken>", esc(n)));
        }
        body.push_str("</ListQueryLoggingConfigsResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }
}

// ─── CIDR Collection handlers ────────────────────────────────────────

impl Route53Service {
    fn create_cidr_collection(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let cfg: CreateCidrCollectionRequest = xml_io::from_xml_root(&req.body).map_err(|e| {
            invalid_argument(format!("invalid CreateCidrCollectionRequest XML: {e}"))
        })?;
        if cfg.name.is_empty() || cfg.caller_reference.is_empty() {
            return Err(invalid_argument("Name and CallerReference are required"));
        }
        let mut state = self.state.write();
        let account = state
            .accounts
            .entry(DEFAULT_ACCOUNT.to_string())
            .or_default();
        if account
            .cidr_collections
            .values()
            .any(|c| c.name == cfg.name)
        {
            return Err(aws_error(
                StatusCode::CONFLICT,
                "CidrCollectionAlreadyExistsException",
                format!("A CIDR collection named '{}' already exists", cfg.name),
            ));
        }
        let id = Uuid::new_v4().to_string();
        let arn =
            Arn::global("route53", DEFAULT_ACCOUNT, &format!("cidrcollection/{id}")).to_string();
        let stored = StoredCidrCollection {
            id: id.clone(),
            name: cfg.name,
            arn: arn.clone(),
            version: 1,
            caller_reference: cfg.caller_reference,
            locations: BTreeMap::new(),
        };
        account.cidr_collections.insert(id.clone(), stored.clone());
        drop(state);
        let mut body = String::with_capacity(512);
        body.push_str(XML_DECL);
        body.push_str(&format!("<CreateCidrCollectionResponse xmlns=\"{NS}\">"));
        push_cidr_collection_full(&mut body, &stored);
        body.push_str("<Location>");
        body.push_str(&format!("<Arn>{}</Arn>", esc(&arn)));
        body.push_str("</Location>");
        body.push_str("</CreateCidrCollectionResponse>");
        let mut headers = HeaderMap::new();
        if let Ok(loc) =
            http::HeaderValue::from_str(&format!("/2013-04-01/cidrcollection/{}", stored.id))
        {
            headers.insert(http::header::LOCATION, loc);
        }
        Ok(xml_response(StatusCode::CREATED, body, headers))
    }

    fn change_cidr_collection(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = require_id(route)?;
        let cfg: ChangeCidrCollectionRequest = xml_io::from_xml_root(&req.body).map_err(|e| {
            invalid_argument(format!("invalid ChangeCidrCollectionRequest XML: {e}"))
        })?;
        if cfg.changes.change.is_empty() {
            return Err(invalid_argument("Changes must contain at least one entry"));
        }
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| no_such_cidr_collection(&id))?;
        let coll = account
            .cidr_collections
            .get_mut(&id)
            .ok_or_else(|| no_such_cidr_collection(&id))?;
        if let Some(client_v) = cfg.collection_version {
            if client_v != coll.version {
                return Err(aws_error(
                    StatusCode::CONFLICT,
                    "CidrCollectionVersionMismatchException",
                    format!(
                        "CollectionVersion ({}) does not match the current ({})",
                        client_v, coll.version
                    ),
                ));
            }
        }
        // Stage changes against a clone so a later invalid change rolls
        // everything back atomically.
        let mut working = coll.locations.clone();
        for ch in &cfg.changes.change {
            match ch.action.to_uppercase().as_str() {
                "PUT" => {
                    let entry = working.entry(ch.location_name.clone()).or_default();
                    for cidr in &ch.cidr_list.cidr {
                        if !entry.contains(cidr) {
                            entry.push(cidr.clone());
                        }
                    }
                    entry.sort();
                }
                "DELETE_IF_EXISTS" => {
                    if let Some(entry) = working.get_mut(&ch.location_name) {
                        entry.retain(|c| !ch.cidr_list.cidr.contains(c));
                        if entry.is_empty() {
                            working.remove(&ch.location_name);
                        }
                    }
                }
                other => {
                    return Err(invalid_argument(format!(
                        "Unknown CIDR change action: {other}"
                    )));
                }
            }
        }
        coll.locations = working;
        coll.version += 1;
        drop(state);
        let mut body = String::with_capacity(128);
        body.push_str(XML_DECL);
        body.push_str(&format!("<ChangeCidrCollectionResponse xmlns=\"{NS}\">"));
        body.push_str(&format!("<Id>{}</Id>", esc(&id)));
        body.push_str("</ChangeCidrCollectionResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    fn delete_cidr_collection(&self, route: &Route) -> Result<AwsResponse, AwsServiceError> {
        let id = require_id(route)?;
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| no_such_cidr_collection(&id))?;
        let coll = account
            .cidr_collections
            .get(&id)
            .ok_or_else(|| no_such_cidr_collection(&id))?;
        if !coll.locations.is_empty() {
            return Err(aws_error(
                StatusCode::BAD_REQUEST,
                "CidrCollectionInUseException",
                format!(
                    "CIDR collection {} still contains {} location(s)",
                    id,
                    coll.locations.len()
                ),
            ));
        }
        account.cidr_collections.remove(&id);
        drop(state);
        let mut body = String::with_capacity(128);
        body.push_str(XML_DECL);
        body.push_str(&format!("<DeleteCidrCollectionResponse xmlns=\"{NS}\"/>"));
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    fn list_cidr_collections(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        validate_query_constraints(
            &req.query_params,
            &[
                QueryConstraint::StrLen {
                    key: "nexttoken",
                    min: 0,
                    max: 1024,
                },
                MAX_RESULTS_CONSTRAINT,
            ],
        )?;
        let max_items: usize = req
            .query_params
            .get("maxresults")
            .and_then(|s| s.parse().ok())
            .unwrap_or(100);
        let state = self.state.read();
        let mut colls: Vec<StoredCidrCollection> = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .map(|a| a.cidr_collections.values().cloned().collect())
            .unwrap_or_default();
        drop(state);
        colls.sort_by(|a, b| a.id.cmp(&b.id));
        let slice: Vec<&StoredCidrCollection> = colls.iter().take(max_items).collect();
        let next = if slice.len() < colls.len() {
            Some(colls[slice.len()].id.clone())
        } else {
            None
        };
        let mut body = String::with_capacity(512);
        body.push_str(XML_DECL);
        body.push_str(&format!("<ListCidrCollectionsResponse xmlns=\"{NS}\">"));
        body.push_str("<CidrCollections>");
        for c in &slice {
            // CollectionSummaries.member has no xmlName trait, so AWS
            // SDKs deserialize members from the default `<member>` element.
            body.push_str("<member>");
            body.push_str(&format!("<Arn>{}</Arn>", esc(&c.arn)));
            body.push_str(&format!("<Id>{}</Id>", esc(&c.id)));
            body.push_str(&format!("<Name>{}</Name>", esc(&c.name)));
            body.push_str(&format!("<Version>{}</Version>", c.version));
            body.push_str("</member>");
        }
        body.push_str("</CidrCollections>");
        if let Some(n) = &next {
            body.push_str(&format!("<NextToken>{}</NextToken>", esc(n)));
        }
        body.push_str("</ListCidrCollectionsResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    fn list_cidr_locations(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = require_id(route)?;
        let max_items: usize = req
            .query_params
            .get("maxresults")
            .and_then(|s| s.parse().ok())
            .unwrap_or(100);
        let state = self.state.read();
        let coll = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .and_then(|a| a.cidr_collections.get(&id).cloned())
            .ok_or_else(|| no_such_cidr_collection(&id))?;
        drop(state);
        let mut names: Vec<String> = coll.locations.keys().cloned().collect();
        names.sort();
        let slice: Vec<&String> = names.iter().take(max_items).collect();
        let next = if slice.len() < names.len() {
            Some(names[slice.len()].clone())
        } else {
            None
        };
        let mut body = String::with_capacity(512);
        body.push_str(XML_DECL);
        body.push_str(&format!("<ListCidrLocationsResponse xmlns=\"{NS}\">"));
        body.push_str("<CidrLocations>");
        for n in &slice {
            body.push_str("<member>");
            body.push_str(&format!("<LocationName>{}</LocationName>", esc(n)));
            body.push_str("</member>");
        }
        body.push_str("</CidrLocations>");
        if let Some(n) = &next {
            body.push_str(&format!("<NextToken>{}</NextToken>", esc(n)));
        }
        body.push_str("</ListCidrLocationsResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    fn list_cidr_blocks(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = require_id(route)?;
        let location_name = req.query_params.get("location").cloned();
        let max_items: usize = req
            .query_params
            .get("maxresults")
            .and_then(|s| s.parse().ok())
            .unwrap_or(100);
        let state = self.state.read();
        let coll = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .and_then(|a| a.cidr_collections.get(&id).cloned())
            .ok_or_else(|| no_such_cidr_collection(&id))?;
        drop(state);
        let blocks: Vec<(String, String)> = coll
            .locations
            .iter()
            .filter(|(n, _)| location_name.as_ref().is_none_or(|name| name == *n))
            .flat_map(|(n, blocks)| blocks.iter().map(move |b| (n.clone(), b.clone())))
            .collect();
        let slice: Vec<&(String, String)> = blocks.iter().take(max_items).collect();
        let next = if slice.len() < blocks.len() {
            Some(blocks[slice.len()].1.clone())
        } else {
            None
        };
        let mut body = String::with_capacity(512);
        body.push_str(XML_DECL);
        body.push_str(&format!("<ListCidrBlocksResponse xmlns=\"{NS}\">"));
        body.push_str("<CidrBlocks>");
        for (loc, cidr) in &slice {
            body.push_str("<member>");
            body.push_str(&format!("<CidrBlock>{}</CidrBlock>", esc(cidr)));
            body.push_str(&format!("<LocationName>{}</LocationName>", esc(loc)));
            body.push_str("</member>");
        }
        body.push_str("</CidrBlocks>");
        if let Some(n) = &next {
            body.push_str(&format!("<NextToken>{}</NextToken>", esc(n)));
        }
        body.push_str("</ListCidrBlocksResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────

#[path = "helpers.rs"]
mod helpers;
use helpers::*;

/// Bundle of cross-service shared state references passed into the
/// routing-policy resolver so alias targets can resolve to the actual
/// underlying ELB / CloudFront / S3 endpoints. Each field is optional —
/// when a service isn't wired (unit-test, persistence-only build) the
/// resolver falls back to a deterministic synthetic IP so existing
/// tests stay stable.
pub(crate) struct AliasLookup<'a> {
    pub(crate) elbv2: Option<&'a fakecloud_elbv2::SharedElbv2State>,
    pub(crate) cloudfront: Option<&'a fakecloud_cloudfront::SharedCloudFrontState>,
    pub(crate) s3: Option<&'a fakecloud_s3::SharedS3State>,
}

/// Resolve a TestDNSAnswer query against the candidate RRsets honoring
/// the routing policy fields each set may carry. Mirrors real Route 53.
///
/// * Failover (PRIMARY+SECONDARY) — primary if healthy, otherwise secondary.
/// * Multi-value answer — up to 8 healthy values combined.
/// * Weighted — pick proportional to weight, deterministically keyed by subnet.
/// * Latency — pick the record whose region matches the client's region.
/// * Geolocation — match country/continent against record's GeoLocation.
/// * Alias targets — resolve into the wired ELB/CloudFront/S3 state when
///   present and fall back to a deterministic synthetic IP otherwise.
/// * Default (no routing fields) — first healthy record's values.
fn resolve_routing_policy(
    candidates: &[&crate::model::ResourceRecordSet],
    health_checks: &std::collections::BTreeMap<String, crate::state::StoredHealthCheck>,
    edns0_subnet: Option<&str>,
    alias_lookup: &AliasLookup<'_>,
) -> Vec<String> {
    let rr_values = |r: &crate::model::ResourceRecordSet| -> Vec<String> {
        if let Some(alias) = r.alias_target.as_ref() {
            return resolve_alias_target(alias, &r.record_type, alias_lookup);
        }
        r.resource_records
            .as_ref()
            .map(|rr| rr.resource_record.iter().map(|x| x.value.clone()).collect())
            .unwrap_or_default()
    };
    fn is_healthy(
        r: &crate::model::ResourceRecordSet,
        health_checks: &std::collections::BTreeMap<String, crate::state::StoredHealthCheck>,
    ) -> bool {
        match r.health_check_id.as_ref() {
            None => true,
            Some(id) => health_checks
                .get(id)
                .map(|hc| matches!(hc.status, crate::state::HealthCheckStatus::Success))
                .unwrap_or(true),
        }
    }
    fn subnet_hash(subnet: Option<&str>) -> u64 {
        subnet
            .map(|s| {
                let mut h: u64 = 0xcbf29ce484222325;
                for b in s.bytes() {
                    h ^= b as u64;
                    h = h.wrapping_mul(0x100000001b3);
                }
                h
            })
            .unwrap_or(0)
    }

    if candidates.iter().any(|r| r.failover.is_some()) {
        let primary = candidates
            .iter()
            .find(|r| r.failover.as_deref() == Some("PRIMARY"));
        let secondary = candidates
            .iter()
            .find(|r| r.failover.as_deref() == Some("SECONDARY"));
        if let Some(p) = primary {
            if is_healthy(p, health_checks) {
                return rr_values(p);
            }
        }
        if let Some(s) = secondary {
            return rr_values(s);
        }
        return primary.map(|p| rr_values(p)).unwrap_or_default();
    }

    if candidates
        .iter()
        .any(|r| r.multi_value_answer == Some(true))
    {
        return candidates
            .iter()
            .filter(|r| is_healthy(r, health_checks))
            .flat_map(|r| rr_values(r))
            .take(8)
            .collect();
    }

    if candidates.iter().any(|r| r.weight.is_some()) {
        let healthy: Vec<&&crate::model::ResourceRecordSet> = candidates
            .iter()
            .filter(|r| is_healthy(r, health_checks) && r.weight.is_some())
            .collect();
        let total: i64 = healthy.iter().map(|r| r.weight.unwrap_or(0)).sum();
        if total == 0 || healthy.is_empty() {
            return candidates.first().map(|r| rr_values(r)).unwrap_or_default();
        }
        let mut pick = (subnet_hash(edns0_subnet) % total as u64) as i64;
        for r in &healthy {
            let w = r.weight.unwrap_or(0);
            if pick < w {
                return rr_values(r);
            }
            pick -= w;
        }
        return rr_values(healthy[0]);
    }

    // Latency-based: pick the record whose region matches the client subnet's
    // inferred region. Fall back to the closest region by hash distance.
    if candidates
        .iter()
        .any(|r| r.region.is_some() && r.geo_location.is_none())
    {
        let healthy: Vec<&&crate::model::ResourceRecordSet> = candidates
            .iter()
            .filter(|r| is_healthy(r, health_checks))
            .collect();
        if healthy.is_empty() {
            return Vec::new();
        }
        let client_region = infer_region_from_subnet(edns0_subnet);
        if let Some(r) = healthy
            .iter()
            .find(|r| r.region.as_deref() == Some(client_region.as_str()))
        {
            return rr_values(r);
        }
        // No exact region match — fall back to a deterministic pick.
        let idx = (subnet_hash(edns0_subnet) as usize) % healthy.len();
        return rr_values(healthy[idx]);
    }

    // Geolocation: match country/continent of client against record GeoLocation.
    // A record with GeoLocation { country_code: "*" } (or no GeoLocation) acts
    // as the default. Real Route 53 uses a record with no GeoLocation as the
    // default; we match either form.
    if candidates.iter().any(|r| r.geo_location.is_some()) {
        let healthy: Vec<&&crate::model::ResourceRecordSet> = candidates
            .iter()
            .filter(|r| is_healthy(r, health_checks))
            .collect();
        if healthy.is_empty() {
            return Vec::new();
        }
        let (client_country, client_continent) = infer_geo_from_subnet(edns0_subnet);
        // 1) Exact country match.
        if let Some(r) = healthy.iter().find(|r| {
            r.geo_location
                .as_ref()
                .and_then(|g| g.country_code.as_deref())
                .map(|c| c == client_country)
                .unwrap_or(false)
        }) {
            return rr_values(r);
        }
        // 2) Continent match.
        if let Some(r) = healthy.iter().find(|r| {
            r.geo_location
                .as_ref()
                .and_then(|g| g.continent_code.as_deref())
                .map(|c| c == client_continent)
                .unwrap_or(false)
        }) {
            return rr_values(r);
        }
        // 3) Default record: GeoLocation with country_code="*" or no GeoLocation.
        if let Some(r) = healthy.iter().find(|r| {
            r.geo_location
                .as_ref()
                .and_then(|g| g.country_code.as_deref())
                .map(|c| c == "*")
                .unwrap_or(false)
        }) {
            return rr_values(r);
        }
        if let Some(r) = healthy.iter().find(|r| r.geo_location.is_none()) {
            return rr_values(r);
        }
        // 4) Last resort: first healthy.
        return rr_values(healthy[0]);
    }

    candidates
        .iter()
        .find(|r| is_healthy(r, health_checks))
        .or_else(|| candidates.first())
        .map(|r| rr_values(r))
        .unwrap_or_default()
}

/// Resolve an alias target to record values. Real Route 53 follows the
/// alias to the underlying ELB / CloudFront / S3 endpoint and returns
/// A / AAAA records. For fakecloud's TestDNSAnswer we cross-call into
/// the appropriate crate's state when wired (`with_elbv2`,
/// `with_cloudfront`, `with_s3`) and fall back to a deterministic
/// synthetic IP per DNS name when the target service isn't wired or
/// the resource isn't found. Non-A/AAAA aliases surface the alias
/// hostname directly, matching real Route 53 behaviour for CNAME-style
/// aliases.
fn resolve_alias_target(
    alias: &crate::model::AliasTarget,
    record_type: &str,
    lookup: &AliasLookup<'_>,
) -> Vec<String> {
    let name = alias.dns_name.trim_end_matches('.');
    if record_type == "A" || record_type == "AAAA" {
        // 1) Try ELB lookup: `<name>-<suffix>.<region>.elb.amazonaws.com`.
        if let Some(addrs) = lookup_elbv2_addresses(name, record_type, lookup.elbv2) {
            return addrs;
        }
        // 2) CloudFront: `<id>.cloudfront.net` — only resolve when the
        //    distribution is known.
        if let Some(addrs) = lookup_cloudfront_addresses(name, record_type, lookup.cloudfront) {
            return addrs;
        }
        // 3) S3: `<bucket>.s3-website-<region>.amazonaws.com` /
        //    `<bucket>.s3.<region>.amazonaws.com` — only resolve when
        //    the bucket exists.
        if let Some(addrs) = lookup_s3_addresses(name, record_type, lookup.s3) {
            return addrs;
        }
        // 4) Fallback: deterministic IP derived from the name. Keeps
        //    pre-cross-call test fixtures stable and gives unit tests
        //    a stable value when state isn't wired.
        return synthetic_alias_addresses(name, record_type);
    }
    // Non-A/AAAA aliases (CNAME, etc.) surface the alias hostname directly.
    vec![name.to_string()]
}

fn lookup_elbv2_addresses(
    name: &str,
    record_type: &str,
    state: Option<&fakecloud_elbv2::SharedElbv2State>,
) -> Option<Vec<String>> {
    // ELB DNS names look like:
    //   `<scheme>-?<lb-name>-<suffix>.<region>.elb.amazonaws.com`
    // (`internal-` prefix for private LBs). Match the suffix and pull
    // addresses straight off the matching `LoadBalancer`.
    if !name.ends_with(".elb.amazonaws.com") {
        return None;
    }
    let state = state?;
    let lower = name.to_ascii_lowercase();
    let st = state.read();
    for (_acct, account) in st.iter() {
        for lb in account.load_balancers.values() {
            if lb.dns_name.trim_end_matches('.').to_ascii_lowercase() == lower {
                let mut out: Vec<String> = Vec::new();
                for az in &lb.availability_zones {
                    for addr in &az.load_balancer_addresses {
                        if record_type == "AAAA" {
                            if let Some(v6) = addr.ipv6_address.as_ref() {
                                if !v6.is_empty() {
                                    out.push(v6.clone());
                                }
                            }
                        } else if let Some(v4) = addr.ip_address.as_ref() {
                            if !v4.is_empty() {
                                out.push(v4.clone());
                            }
                        } else if let Some(v4) = addr.private_ipv4_address.as_ref() {
                            if !v4.is_empty() {
                                out.push(v4.clone());
                            }
                        }
                    }
                }
                if out.is_empty() {
                    // LB exists but has no concrete addresses recorded;
                    // synthesise one off the DNS name so the alias still
                    // resolves to something stable.
                    return Some(synthetic_alias_addresses(name, record_type));
                }
                return Some(out);
            }
        }
    }
    None
}

fn lookup_cloudfront_addresses(
    name: &str,
    record_type: &str,
    state: Option<&fakecloud_cloudfront::SharedCloudFrontState>,
) -> Option<Vec<String>> {
    if !name.ends_with(".cloudfront.net") {
        return None;
    }
    let state = state?;
    let lower = name.to_ascii_lowercase();
    let id = lower.trim_end_matches(".cloudfront.net");
    let st = state.read();
    for account in st.accounts.values() {
        if account.distributions.values().any(|d| {
            d.id.eq_ignore_ascii_case(id)
                || d.domain_name.trim_end_matches('.').to_ascii_lowercase() == lower
        }) {
            return Some(synthetic_alias_addresses(name, record_type));
        }
        if account
            .streaming_distributions
            .values()
            .any(|d| d.domain_name.trim_end_matches('.').to_ascii_lowercase() == lower)
        {
            return Some(synthetic_alias_addresses(name, record_type));
        }
    }
    None
}

fn lookup_s3_addresses(
    name: &str,
    record_type: &str,
    state: Option<&fakecloud_s3::SharedS3State>,
) -> Option<Vec<String>> {
    let lower = name.to_ascii_lowercase();
    // Pattern: `<bucket>.s3-website[-.]<region>.amazonaws.com` or
    //          `<bucket>.s3[.<region>].amazonaws.com`.
    let bucket = if let Some(idx) = lower.find(".s3-website") {
        &lower[..idx]
    } else if let Some(idx) = lower.find(".s3.") {
        &lower[..idx]
    } else if let Some(idx) = lower.find(".s3-") {
        &lower[..idx]
    } else {
        return None;
    };
    if bucket.is_empty() {
        return None;
    }
    let state = state?;
    let st = state.read();
    for (_acct, s3) in st.iter() {
        if s3.buckets.contains_key(bucket) {
            return Some(synthetic_alias_addresses(name, record_type));
        }
    }
    None
}

/// Build a deterministic IPv4 (or IPv6) address from a DNS name. Used
/// as the fallback when an alias target's underlying service either
/// isn't wired into the Route 53 service or doesn't know about the
/// resource. Addresses sit inside the IETF documentation ranges
/// (`198.51.x.x` and `2001:db8::/32`) so they never overlap real
/// production endpoints.
fn synthetic_alias_addresses(name: &str, record_type: &str) -> Vec<String> {
    let h = {
        let mut h: u64 = 0xcbf29ce484222325;
        for b in name.bytes() {
            h ^= b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
        h
    };
    if record_type == "AAAA" {
        let a = (h & 0xffff) as u16;
        let b = ((h >> 16) & 0xffff) as u16;
        let c = ((h >> 32) & 0xffff) as u16;
        let d = ((h >> 48) & 0xffff) as u16;
        return vec![format!("2001:db8:{a:x}:{b:x}::{c:x}:{d:x}")];
    }
    let oct2 = ((h >> 16) & 0xff) as u8;
    let oct3 = ((h >> 8) & 0xff) as u8;
    let oct4 = (h & 0xff) as u8;
    vec![format!(
        "198.51.{}.{oct4}",
        ((oct2 as u16) << 8 | oct3 as u16) % 256
    )]
}

/// Infer an AWS region from a client subnet IP. Used by latency-based
/// routing. Real Route 53 uses BGP-derived geolocation; fakecloud uses a
/// deterministic /8-prefix mapping that is stable across calls so tests can
/// pin a request to a specific region.
fn infer_region_from_subnet(subnet: Option<&str>) -> String {
    let Some(s) = subnet else {
        return "us-east-1".to_string();
    };
    let first_octet: u32 = s
        .split('.')
        .next()
        .and_then(|o| o.parse().ok())
        .unwrap_or(0);
    match first_octet {
        0..=63 => "us-east-1".to_string(),
        64..=127 => "us-west-2".to_string(),
        128..=159 => "eu-west-1".to_string(),
        160..=191 => "eu-central-1".to_string(),
        192..=223 => "ap-southeast-1".to_string(),
        _ => "ap-northeast-1".to_string(),
    }
}

/// Infer (country_code, continent_code) from a client subnet IP. Used by
/// geolocation routing. Stable mapping keyed by the first octet so tests
/// can pin a request to a specific country.
fn infer_geo_from_subnet(subnet: Option<&str>) -> (String, String) {
    let Some(s) = subnet else {
        return ("US".to_string(), "NA".to_string());
    };
    let first_octet: u32 = s
        .split('.')
        .next()
        .and_then(|o| o.parse().ok())
        .unwrap_or(0);
    match first_octet {
        0..=63 => ("US".to_string(), "NA".to_string()),
        64..=127 => ("CA".to_string(), "NA".to_string()),
        128..=159 => ("GB".to_string(), "EU".to_string()),
        160..=191 => ("DE".to_string(), "EU".to_string()),
        192..=223 => ("SG".to_string(), "AS".to_string()),
        _ => ("JP".to_string(), "AS".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::HealthCheckConfig;
    use crate::state::{HealthCheckStatus, Route53Accounts, StoredHealthCheck};

    fn svc_with_health_check(id: &str) -> Route53Service {
        let state = Arc::new(RwLock::new(Route53Accounts::default()));
        {
            let mut s = state.write();
            let account = s.accounts.entry(DEFAULT_ACCOUNT.to_string()).or_default();
            account.health_checks.insert(
                id.to_string(),
                StoredHealthCheck {
                    id: id.to_string(),
                    caller_reference: "ref".to_string(),
                    version: 1,
                    config: HealthCheckConfig::default(),
                    created_time: Utc::now(),
                    status: HealthCheckStatus::Success,
                    last_failure_reason: None,
                },
            );
        }
        Route53Service::new(state)
    }

    #[test]
    fn set_health_check_status_flips_status_and_failure_reason() {
        let svc = svc_with_health_check("hc-1");
        assert!(svc.set_health_check_status(
            "hc-1",
            HealthCheckStatus::Failure,
            Some("Endpoint timed out".to_string()),
        ));
        let st = svc.state.read();
        let hc = &st.accounts.get(DEFAULT_ACCOUNT).unwrap().health_checks["hc-1"];
        assert_eq!(hc.status, HealthCheckStatus::Failure);
        assert_eq!(
            hc.last_failure_reason.as_deref().unwrap(),
            "Endpoint timed out"
        );
    }

    #[test]
    fn set_health_check_status_returns_false_for_unknown_id() {
        let svc = svc_with_health_check("hc-1");
        assert!(!svc.set_health_check_status("ghost", HealthCheckStatus::Failure, None,));
    }

    fn rrset(value: &str) -> crate::model::ResourceRecordSet {
        crate::model::ResourceRecordSet {
            name: "x.example.com.".to_string(),
            record_type: "A".to_string(),
            ttl: Some(60),
            resource_records: Some(crate::model::ResourceRecords {
                resource_record: vec![crate::model::ResourceRecord {
                    value: value.to_string(),
                }],
            }),
            ..Default::default()
        }
    }

    fn empty_lookup() -> AliasLookup<'static> {
        AliasLookup {
            elbv2: None,
            cloudfront: None,
            s3: None,
        }
    }

    #[test]
    fn routing_policy_failover_picks_secondary_when_primary_unhealthy() {
        let mut p = rrset("1.1.1.1");
        p.failover = Some("PRIMARY".to_string());
        p.health_check_id = Some("hc-down".to_string());
        let mut s = rrset("2.2.2.2");
        s.failover = Some("SECONDARY".to_string());
        let mut hcs = std::collections::BTreeMap::new();
        hcs.insert(
            "hc-down".to_string(),
            StoredHealthCheck {
                id: "hc-down".to_string(),
                caller_reference: "r".to_string(),
                version: 1,
                config: HealthCheckConfig::default(),
                created_time: Utc::now(),
                status: HealthCheckStatus::Failure,
                last_failure_reason: Some("connection refused".to_string()),
            },
        );
        let answers = resolve_routing_policy(&[&p, &s], &hcs, None, &empty_lookup());
        assert_eq!(answers, vec!["2.2.2.2".to_string()]);
    }

    #[test]
    fn routing_policy_multivalue_returns_only_healthy() {
        let mut a = rrset("1.1.1.1");
        a.multi_value_answer = Some(true);
        a.health_check_id = Some("hc-down".to_string());
        let mut b = rrset("2.2.2.2");
        b.multi_value_answer = Some(true);
        let mut c = rrset("3.3.3.3");
        c.multi_value_answer = Some(true);
        let mut hcs = std::collections::BTreeMap::new();
        hcs.insert(
            "hc-down".to_string(),
            StoredHealthCheck {
                id: "hc-down".to_string(),
                caller_reference: "r".to_string(),
                version: 1,
                config: HealthCheckConfig::default(),
                created_time: Utc::now(),
                status: HealthCheckStatus::Failure,
                last_failure_reason: None,
            },
        );
        let answers = resolve_routing_policy(&[&a, &b, &c], &hcs, None, &empty_lookup());
        assert_eq!(answers, vec!["2.2.2.2".to_string(), "3.3.3.3".to_string()]);
    }

    #[test]
    fn routing_policy_weighted_picks_deterministically_by_subnet() {
        let mut a = rrset("1.1.1.1");
        a.weight = Some(10);
        let mut b = rrset("2.2.2.2");
        b.weight = Some(90);
        let hcs = std::collections::BTreeMap::new();
        // Same subnet should always produce the same answer.
        let one = resolve_routing_policy(&[&a, &b], &hcs, Some("203.0.113.5"), &empty_lookup());
        let two = resolve_routing_policy(&[&a, &b], &hcs, Some("203.0.113.5"), &empty_lookup());
        assert_eq!(one, two);
        assert_eq!(one.len(), 1);
    }

    #[test]
    fn routing_policy_default_returns_first_records() {
        let a = rrset("1.1.1.1");
        let hcs = std::collections::BTreeMap::new();
        let answers = resolve_routing_policy(&[&a], &hcs, None, &empty_lookup());
        assert_eq!(answers, vec!["1.1.1.1".to_string()]);
    }

    #[test]
    fn routing_policy_latency_picks_record_matching_inferred_region() {
        let mut a = rrset("1.1.1.1");
        a.region = Some("us-east-1".to_string());
        let mut b = rrset("2.2.2.2");
        b.region = Some("eu-west-1".to_string());
        let hcs = std::collections::BTreeMap::new();
        // First-octet 0..=63 -> us-east-1 in `infer_region_from_subnet`.
        let us = resolve_routing_policy(&[&a, &b], &hcs, Some("10.0.0.1"), &empty_lookup());
        assert_eq!(us, vec!["1.1.1.1".to_string()]);
        // First-octet 128..=159 -> eu-west-1.
        let eu = resolve_routing_policy(&[&a, &b], &hcs, Some("130.0.0.1"), &empty_lookup());
        assert_eq!(eu, vec!["2.2.2.2".to_string()]);
    }

    #[test]
    fn routing_policy_geolocation_uses_country_then_continent_then_default() {
        let mut us = rrset("1.1.1.1");
        us.geo_location = Some(crate::model::GeoLocation {
            continent_code: None,
            country_code: Some("US".to_string()),
            subdivision_code: None,
        });
        let mut eu_default = rrset("2.2.2.2");
        eu_default.geo_location = Some(crate::model::GeoLocation {
            continent_code: Some("EU".to_string()),
            country_code: None,
            subdivision_code: None,
        });
        let mut star = rrset("9.9.9.9");
        star.geo_location = Some(crate::model::GeoLocation {
            continent_code: None,
            country_code: Some("*".to_string()),
            subdivision_code: None,
        });
        let hcs = std::collections::BTreeMap::new();
        // 0..=63 -> US country.
        let r1 = resolve_routing_policy(
            &[&us, &eu_default, &star],
            &hcs,
            Some("10.0.0.1"),
            &empty_lookup(),
        );
        assert_eq!(r1, vec!["1.1.1.1".to_string()]);
        // 128..=159 -> GB country (no exact GB record), continent EU matches.
        let r2 = resolve_routing_policy(
            &[&us, &eu_default, &star],
            &hcs,
            Some("130.0.0.1"),
            &empty_lookup(),
        );
        assert_eq!(r2, vec!["2.2.2.2".to_string()]);
        // 192..=223 -> SG / AS — falls back to the `*` default record.
        let r3 = resolve_routing_policy(
            &[&us, &eu_default, &star],
            &hcs,
            Some("200.0.0.1"),
            &empty_lookup(),
        );
        assert_eq!(r3, vec!["9.9.9.9".to_string()]);
    }

    #[test]
    fn alias_target_falls_back_to_synthetic_ip_when_state_not_wired() {
        let alias = crate::model::AliasTarget {
            hosted_zone_id: "Z3DZXE0EXAMPLE".to_string(),
            dns_name: "my-lb-1234567890.us-east-1.elb.amazonaws.com.".to_string(),
            evaluate_target_health: false,
        };
        let answers = resolve_alias_target(&alias, "A", &empty_lookup());
        assert_eq!(answers.len(), 1);
        // Synthetic fallback uses 198.51.x.x (documentation range).
        assert!(
            answers[0].starts_with("198.51."),
            "expected documentation IPv4, got {}",
            answers[0]
        );
    }

    #[test]
    fn alias_target_resolves_elbv2_load_balancer_to_real_addresses() {
        let elbv2_state: fakecloud_elbv2::SharedElbv2State = std::sync::Arc::new(
            parking_lot::RwLock::new(fakecloud_elbv2::Elbv2Accounts::new()),
        );
        {
            let mut st = elbv2_state.write();
            let account = st.get_or_create("000000000000");
            account.load_balancers.insert(
                "lb-1".to_string(),
                fakecloud_elbv2::LoadBalancer {
                    arn: "arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app/my-lb/abcdef".to_string(),
                    name: "my-lb".to_string(),
                    dns_name: "my-lb-9999999999.us-east-1.elb.amazonaws.com".to_string(),
                    canonical_hosted_zone_id: "Z35SXDOTRQ7X7K".to_string(),
                    created_time: Utc::now(),
                    scheme: "internet-facing".to_string(),
                    vpc_id: "vpc-1".to_string(),
                    state_code: "active".to_string(),
                    state_reason: None,
                    lb_type: "application".to_string(),
                    availability_zones: vec![fakecloud_elbv2::AvailabilityZone {
                        zone_name: "us-east-1a".to_string(),
                        subnet_id: "subnet-1".to_string(),
                        outpost_id: None,
                        load_balancer_addresses: vec![fakecloud_elbv2::LoadBalancerAddress {
                            ip_address: Some("203.0.113.10".to_string()),
                            allocation_id: None,
                            private_ipv4_address: None,
                            ipv6_address: Some("2001:db8::1".to_string()),
                            ipv4_prefix: None,
                            ipv6_prefix: None,
                        }],
                        source_nat_ipv6_prefixes: vec![],
                    }],
                    security_groups: vec![],
                    ip_address_type: "ipv4".to_string(),
                    customer_owned_ipv4_pool: None,
                    enforce_security_group_inbound_rules_on_private_link_traffic: None,
                    enable_prefix_for_ipv6_source_nat: None,
                    ipv4_ipam_pool_id: None,
                    tags: vec![],
                    attributes: std::collections::BTreeMap::new(),
                    minimum_capacity_units: None,
                    bound_port: None,
                },
            );
        }
        let lookup = AliasLookup {
            elbv2: Some(&elbv2_state),
            cloudfront: None,
            s3: None,
        };
        let alias = crate::model::AliasTarget {
            hosted_zone_id: "Z35SXDOTRQ7X7K".to_string(),
            dns_name: "my-lb-9999999999.us-east-1.elb.amazonaws.com.".to_string(),
            evaluate_target_health: false,
        };
        let v4 = resolve_alias_target(&alias, "A", &lookup);
        assert_eq!(v4, vec!["203.0.113.10".to_string()]);
        let v6 = resolve_alias_target(&alias, "AAAA", &lookup);
        assert_eq!(v6, vec!["2001:db8::1".to_string()]);
    }

    #[test]
    fn get_change_starts_pending_then_flips_after_threshold_reads() {
        use crate::state::StoredChange;
        let state = Arc::new(RwLock::new(Route53Accounts::default()));
        {
            let mut s = state.write();
            let account = s.accounts.entry(DEFAULT_ACCOUNT.to_string()).or_default();
            account.changes.insert(
                "C123".to_string(),
                StoredChange::pending("C123".to_string(), Utc::now(), Some("test".to_string())),
            );
        }
        let svc = Route53Service::new(state);
        let route =
            crate::router::route(&http::Method::GET, "/2013-04-01/change/C123", "").unwrap();

        for i in 1..=5 {
            svc.get_change(&route).unwrap();
            let st = svc.state.read();
            let stored = &st.accounts.get(DEFAULT_ACCOUNT).unwrap().changes["C123"];
            if i < 5 {
                assert_eq!(stored.status, "PENDING", "read {i}: expected PENDING");
            } else {
                assert_eq!(stored.status, "INSYNC", "read {i}: expected INSYNC");
            }
        }
    }

    #[test]
    fn get_change_unknown_id_returns_404() {
        let svc = Route53Service::new(Arc::new(RwLock::new(Route53Accounts::default())));
        let route =
            crate::router::route(&http::Method::GET, "/2013-04-01/change/CGHOST", "").unwrap();
        let err = match svc.get_change(&route) {
            Err(e) => e,
            Ok(_) => panic!("expected NoSuchChange"),
        };
        assert_eq!(err.code(), "NoSuchChange");
    }

    #[test]
    fn set_health_check_status_preserves_existing_reason_when_none() {
        let svc = svc_with_health_check("hc-1");
        svc.set_health_check_status(
            "hc-1",
            HealthCheckStatus::Failure,
            Some("connect() timed out".to_string()),
        );
        // Flipping back to Success must not clobber the historical
        // failure reason — GetHealthCheckLastFailureReason still returns
        // it after recovery, mirroring real Route 53.
        svc.set_health_check_status("hc-1", HealthCheckStatus::Success, None);
        let st = svc.state.read();
        let hc = &st.accounts.get(DEFAULT_ACCOUNT).unwrap().health_checks["hc-1"];
        assert_eq!(hc.status, HealthCheckStatus::Success);
        assert_eq!(
            hc.last_failure_reason.as_deref().unwrap(),
            "connect() timed out"
        );
    }

    #[test]
    fn get_health_check_status_returns_success_by_default() {
        let svc = svc_with_health_check("hc-default");
        let route = crate::router::route(
            &http::Method::GET,
            "/2013-04-01/healthcheck/hc-default/status",
            "",
        )
        .unwrap();
        let resp = svc.get_health_check_status(&route).unwrap();
        let body = std::str::from_utf8(resp.body.expect_bytes())
            .unwrap()
            .to_string();
        assert!(
            body.contains("<Status>Success: HTTP Status Code 200</Status>"),
            "body: {body}"
        );
    }

    #[test]
    fn get_health_check_status_reflects_state_failure() {
        let svc = svc_with_health_check("hc-down");
        {
            let mut state = svc.state.write();
            let hc = state
                .accounts
                .get_mut(DEFAULT_ACCOUNT)
                .unwrap()
                .health_checks
                .get_mut("hc-down")
                .unwrap();
            hc.status = HealthCheckStatus::Failure;
            hc.last_failure_reason = Some("test".to_string());
        }
        let route = crate::router::route(
            &http::Method::GET,
            "/2013-04-01/healthcheck/hc-down/status",
            "",
        )
        .unwrap();
        let resp = svc.get_health_check_status(&route).unwrap();
        let body = std::str::from_utf8(resp.body.expect_bytes())
            .unwrap()
            .to_string();
        assert!(
            body.contains("<Status>Failure: test</Status>"),
            "body: {body}"
        );
    }

    #[test]
    fn get_health_check_status_failure_without_reason_renders_canned_descriptor() {
        let svc = svc_with_health_check("hc-bare");
        {
            let mut state = svc.state.write();
            let hc = state
                .accounts
                .get_mut(DEFAULT_ACCOUNT)
                .unwrap()
                .health_checks
                .get_mut("hc-bare")
                .unwrap();
            hc.status = HealthCheckStatus::Failure;
            hc.last_failure_reason = None;
        }
        let route = crate::router::route(
            &http::Method::GET,
            "/2013-04-01/healthcheck/hc-bare/status",
            "",
        )
        .unwrap();
        let resp = svc.get_health_check_status(&route).unwrap();
        let body = std::str::from_utf8(resp.body.expect_bytes())
            .unwrap()
            .to_string();
        assert!(
            body.contains("<Status>Failure: Endpoint unreachable</Status>"),
            "body: {body}"
        );
    }

    #[test]
    fn get_health_check_status_renders_timeout_dns_insufficient_unknown() {
        let svc = svc_with_health_check("hc-flavours");
        let cases = [
            (
                HealthCheckStatus::Timeout,
                None,
                "<Status>Failure: Connection timed out</Status>",
            ),
            (
                HealthCheckStatus::Timeout,
                Some("custom timeout msg".to_string()),
                "<Status>Failure: custom timeout msg</Status>",
            ),
            (
                HealthCheckStatus::DnsError,
                None,
                "<Status>Failure: DNS resolution failed</Status>",
            ),
            (
                HealthCheckStatus::InsufficientDataPoints,
                None,
                "<Status>InsufficientDataPoints</Status>",
            ),
            (HealthCheckStatus::Unknown, None, "<Status>Unknown</Status>"),
        ];
        for (status, reason, expected) in cases {
            {
                let mut state = svc.state.write();
                let hc = state
                    .accounts
                    .get_mut(DEFAULT_ACCOUNT)
                    .unwrap()
                    .health_checks
                    .get_mut("hc-flavours")
                    .unwrap();
                hc.status = status;
                hc.last_failure_reason = reason.clone();
            }
            let route = crate::router::route(
                &http::Method::GET,
                "/2013-04-01/healthcheck/hc-flavours/status",
                "",
            )
            .unwrap();
            let resp = svc.get_health_check_status(&route).unwrap();
            let body = std::str::from_utf8(resp.body.expect_bytes())
                .unwrap()
                .to_string();
            assert!(
                body.contains(expected),
                "status={status:?} reason={reason:?} body: {body}"
            );
        }
    }

    #[test]
    fn set_health_check_status_records_reason_for_timeout_and_dns_error() {
        let svc = svc_with_health_check("hc-flav");
        assert!(svc.set_health_check_status(
            "hc-flav",
            HealthCheckStatus::Timeout,
            Some("upstream silent".to_string()),
        ));
        {
            let st = svc.state.read();
            let hc = &st.accounts.get(DEFAULT_ACCOUNT).unwrap().health_checks["hc-flav"];
            assert_eq!(hc.status, HealthCheckStatus::Timeout);
            assert_eq!(
                hc.last_failure_reason.as_deref().unwrap(),
                "upstream silent"
            );
        }
        assert!(svc.set_health_check_status(
            "hc-flav",
            HealthCheckStatus::DnsError,
            Some("NXDOMAIN".to_string()),
        ));
        {
            let st = svc.state.read();
            let hc = &st.accounts.get(DEFAULT_ACCOUNT).unwrap().health_checks["hc-flav"];
            assert_eq!(hc.status, HealthCheckStatus::DnsError);
            assert_eq!(hc.last_failure_reason.as_deref().unwrap(), "NXDOMAIN");
        }
        // InsufficientDataPoints / Unknown must not clobber prior reason
        // even when a reason is supplied (those flavours aren't
        // failure-flavoured).
        assert!(svc.set_health_check_status(
            "hc-flav",
            HealthCheckStatus::InsufficientDataPoints,
            Some("ignored".to_string()),
        ));
        {
            let st = svc.state.read();
            let hc = &st.accounts.get(DEFAULT_ACCOUNT).unwrap().health_checks["hc-flav"];
            assert_eq!(hc.status, HealthCheckStatus::InsufficientDataPoints);
            assert_eq!(hc.last_failure_reason.as_deref().unwrap(), "NXDOMAIN");
        }
    }

    // ─── TestDNSAnswer routing-policy E2E tests (U2) ──────────────────

    fn svc_with_zone(records: Vec<crate::model::ResourceRecordSet>) -> (Route53Service, String) {
        let state = Arc::new(RwLock::new(Route53Accounts::default()));
        let zone_id = "Z123ABC".to_string();
        {
            let mut s = state.write();
            let account = s.accounts.entry(DEFAULT_ACCOUNT.to_string()).or_default();
            account.hosted_zones.insert(
                zone_id.clone(),
                crate::state::StoredHostedZone {
                    id: zone_id.clone(),
                    name: "example.com.".to_string(),
                    caller_reference: "ref".to_string(),
                    comment: None,
                    private_zone: false,
                    features: None,
                    vpcs: vec![],
                    delegation_set_id: None,
                    name_servers: vec![],
                    created_time: Utc::now(),
                    resource_record_sets: records,
                },
            );
        }
        (Route53Service::new(state), zone_id)
    }

    fn req_for_dns(zone_id: &str, name: &str, rtype: &str, edns0: Option<&str>) -> AwsRequest {
        let mut params: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();
        params.insert("hostedzoneid".to_string(), zone_id.to_string());
        params.insert("recordname".to_string(), name.to_string());
        params.insert("recordtype".to_string(), rtype.to_string());
        if let Some(s) = edns0 {
            params.insert("edns0clientsubnetip".to_string(), s.to_string());
        }
        let raw_path = "/2013-04-01/testdnsanswer".to_string();
        let segs: Vec<String> = raw_path
            .trim_start_matches('/')
            .split('/')
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect();
        AwsRequest {
            service: "route53".to_string(),
            action: "TestDNSAnswer".to_string(),
            region: "us-east-1".to_string(),
            account_id: DEFAULT_ACCOUNT.to_string(),
            request_id: "test".to_string(),
            headers: HeaderMap::new(),
            query_params: params,
            body: Bytes::new(),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: segs,
            raw_path,
            raw_query: String::new(),
            method: http::Method::GET,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    fn extract_record_data(body: &str) -> Vec<String> {
        let mut out = Vec::new();
        let mut rest = body;
        while let Some(start) = rest.find("<RecordDataEntry>") {
            rest = &rest[start + "<RecordDataEntry>".len()..];
            let Some(end) = rest.find("</RecordDataEntry>") else {
                break;
            };
            out.push(rest[..end].to_string());
            rest = &rest[end + "</RecordDataEntry>".len()..];
        }
        out
    }

    #[test]
    fn test_dns_answer_simple_returns_records() {
        let r = rrset("1.2.3.4");
        let (svc, zid) = svc_with_zone(vec![r]);
        let req = req_for_dns(&zid, "x.example.com", "A", None);
        let resp = svc.test_dns_answer(&req).unwrap();
        let body = std::str::from_utf8(resp.body.expect_bytes())
            .unwrap()
            .to_string();
        assert_eq!(extract_record_data(&body), vec!["1.2.3.4".to_string()]);
    }

    #[test]
    fn test_dns_answer_weighted_picks_proportional_to_weight() {
        let mut a = rrset("1.1.1.1");
        a.weight = Some(1);
        a.set_identifier = Some("light".to_string());
        let mut b = rrset("9.9.9.9");
        b.weight = Some(99);
        b.set_identifier = Some("heavy".to_string());
        let (svc, zid) = svc_with_zone(vec![a, b]);
        // Sweep distinct subnets to exercise the weighted distribution.
        let mut heavy = 0usize;
        let mut light = 0usize;
        for i in 0..200 {
            let subnet = format!("203.0.113.{}", i % 200);
            let req = req_for_dns(&zid, "x.example.com", "A", Some(&subnet));
            let resp = svc.test_dns_answer(&req).unwrap();
            let body = std::str::from_utf8(resp.body.expect_bytes())
                .unwrap()
                .to_string();
            let data = extract_record_data(&body);
            assert_eq!(data.len(), 1);
            if data[0] == "9.9.9.9" {
                heavy += 1;
            } else if data[0] == "1.1.1.1" {
                light += 1;
            }
        }
        // With 99:1 weight ratio, heavy should dominate by a large margin.
        assert!(
            heavy > 10 * light,
            "expected heavy-weighted record to dominate, got heavy={heavy} light={light}"
        );
    }

    #[test]
    fn test_dns_answer_failover_uses_primary_when_healthy() {
        let mut p = rrset("10.0.0.1");
        p.failover = Some("PRIMARY".to_string());
        p.health_check_id = Some("hc-up".to_string());
        let mut s = rrset("10.0.0.2");
        s.failover = Some("SECONDARY".to_string());
        let (svc, zid) = svc_with_zone(vec![p, s]);
        // Seed the health check as healthy.
        {
            let mut st = svc.state.write();
            let acct = st.accounts.get_mut(DEFAULT_ACCOUNT).unwrap();
            acct.health_checks.insert(
                "hc-up".to_string(),
                StoredHealthCheck {
                    id: "hc-up".to_string(),
                    caller_reference: "r".to_string(),
                    version: 1,
                    config: HealthCheckConfig::default(),
                    created_time: Utc::now(),
                    status: HealthCheckStatus::Success,
                    last_failure_reason: None,
                },
            );
        }
        let req = req_for_dns(&zid, "x.example.com", "A", None);
        let resp = svc.test_dns_answer(&req).unwrap();
        let body = std::str::from_utf8(resp.body.expect_bytes())
            .unwrap()
            .to_string();
        assert_eq!(extract_record_data(&body), vec!["10.0.0.1".to_string()]);
    }

    #[test]
    fn test_dns_answer_failover_falls_back_to_secondary_when_primary_unhealthy() {
        let mut p = rrset("10.0.0.1");
        p.failover = Some("PRIMARY".to_string());
        p.health_check_id = Some("hc-flip".to_string());
        let mut s = rrset("10.0.0.2");
        s.failover = Some("SECONDARY".to_string());
        let (svc, zid) = svc_with_zone(vec![p, s]);
        {
            let mut st = svc.state.write();
            let acct = st.accounts.get_mut(DEFAULT_ACCOUNT).unwrap();
            acct.health_checks.insert(
                "hc-flip".to_string(),
                StoredHealthCheck {
                    id: "hc-flip".to_string(),
                    caller_reference: "r".to_string(),
                    version: 1,
                    config: HealthCheckConfig::default(),
                    created_time: Utc::now(),
                    status: HealthCheckStatus::Success,
                    last_failure_reason: None,
                },
            );
        }
        // Flip primary's health to failure via the public helper.
        assert!(svc.set_health_check_status(
            "hc-flip",
            HealthCheckStatus::Failure,
            Some("simulated outage".to_string()),
        ));
        let req = req_for_dns(&zid, "x.example.com", "A", None);
        let resp = svc.test_dns_answer(&req).unwrap();
        let body = std::str::from_utf8(resp.body.expect_bytes())
            .unwrap()
            .to_string();
        assert_eq!(extract_record_data(&body), vec!["10.0.0.2".to_string()]);
    }

    #[test]
    fn test_dns_answer_multivalue_returns_up_to_8_healthy() {
        // Case 1: 10 records, 6 healthy → 6 returned.
        let mut records = Vec::new();
        for i in 0..6 {
            let mut r = rrset(&format!("10.0.0.{i}"));
            r.multi_value_answer = Some(true);
            r.set_identifier = Some(format!("h-{i}"));
            records.push(r);
        }
        for i in 6..10 {
            let mut r = rrset(&format!("10.0.0.{i}"));
            r.multi_value_answer = Some(true);
            r.set_identifier = Some(format!("u-{i}"));
            r.health_check_id = Some("hc-down".to_string());
            records.push(r);
        }
        let (svc, zid) = svc_with_zone(records);
        {
            let mut st = svc.state.write();
            let acct = st.accounts.get_mut(DEFAULT_ACCOUNT).unwrap();
            acct.health_checks.insert(
                "hc-down".to_string(),
                StoredHealthCheck {
                    id: "hc-down".to_string(),
                    caller_reference: "r".to_string(),
                    version: 1,
                    config: HealthCheckConfig::default(),
                    created_time: Utc::now(),
                    status: HealthCheckStatus::Failure,
                    last_failure_reason: None,
                },
            );
        }
        let req = req_for_dns(&zid, "x.example.com", "A", None);
        let resp = svc.test_dns_answer(&req).unwrap();
        let body = std::str::from_utf8(resp.body.expect_bytes())
            .unwrap()
            .to_string();
        assert_eq!(extract_record_data(&body).len(), 6);

        // Case 2: 12 healthy → cap at 8.
        let mut records = Vec::new();
        for i in 0..12 {
            let mut r = rrset(&format!("10.0.1.{i}"));
            r.multi_value_answer = Some(true);
            r.set_identifier = Some(format!("h-{i}"));
            records.push(r);
        }
        let (svc2, zid2) = svc_with_zone(records);
        let req2 = req_for_dns(&zid2, "x.example.com", "A", None);
        let resp2 = svc2.test_dns_answer(&req2).unwrap();
        let body2 = std::str::from_utf8(resp2.body.expect_bytes())
            .unwrap()
            .to_string();
        assert_eq!(extract_record_data(&body2).len(), 8);
    }

    #[test]
    fn test_dns_answer_geolocation_matches_country_to_record() {
        let mut us = rrset("1.0.0.1");
        us.set_identifier = Some("us".to_string());
        us.geo_location = Some(crate::model::GeoLocation {
            country_code: Some("US".to_string()),
            ..Default::default()
        });
        let mut default = rrset("9.0.0.9");
        default.set_identifier = Some("default".to_string());
        default.geo_location = Some(crate::model::GeoLocation {
            country_code: Some("*".to_string()),
            ..Default::default()
        });
        let (svc, zid) = svc_with_zone(vec![us, default]);

        // EDNS0 IP `10.x.y.z` infers country "US" → should hit the US record.
        let req = req_for_dns(&zid, "x.example.com", "A", Some("10.0.0.1"));
        let resp = svc.test_dns_answer(&req).unwrap();
        let body = std::str::from_utf8(resp.body.expect_bytes())
            .unwrap()
            .to_string();
        assert_eq!(extract_record_data(&body), vec!["1.0.0.1".to_string()]);

        // EDNS0 IP `200.x.y.z` infers SG (continent AS) → no country/continent
        // match, falls back to the default record.
        let req2 = req_for_dns(&zid, "x.example.com", "A", Some("200.0.0.1"));
        let resp2 = svc.test_dns_answer(&req2).unwrap();
        let body2 = std::str::from_utf8(resp2.body.expect_bytes())
            .unwrap()
            .to_string();
        assert_eq!(extract_record_data(&body2), vec!["9.0.0.9".to_string()]);
    }

    #[test]
    fn test_dns_answer_alias_target_synthesizes_record() {
        let mut r = rrset("ignored");
        r.resource_records = None;
        r.alias_target = Some(crate::model::AliasTarget {
            hosted_zone_id: "Z2FDTNDATAQYW2".to_string(),
            dns_name: "example-lb-1234.us-east-1.elb.amazonaws.com.".to_string(),
            evaluate_target_health: false,
        });
        let (svc, zid) = svc_with_zone(vec![r]);
        let req = req_for_dns(&zid, "x.example.com", "A", None);
        let resp = svc.test_dns_answer(&req).unwrap();
        let body = std::str::from_utf8(resp.body.expect_bytes())
            .unwrap()
            .to_string();
        let data = extract_record_data(&body);
        assert_eq!(data.len(), 1);
        // Synthesised IPv4 in 198.51.0.0/16.
        assert!(
            data[0].starts_with("198.51."),
            "expected synthesised A in 198.51.0.0/16, got {}",
            data[0]
        );
    }
}
