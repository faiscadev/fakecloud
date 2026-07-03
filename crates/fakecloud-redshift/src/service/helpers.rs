//! Shared helpers for the Redshift awsQuery handlers: XML rendering, param
//! parsing, pagination, tag handling, and typed error constructors.

use http::StatusCode;

use fakecloud_core::query::{query_metadata_only_xml, query_response_xml};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::Tag;

/// Redshift's Query-protocol XML namespace.
pub(crate) const NS: &str = "http://redshift.amazonaws.com/doc/2012-12-01/";

/// Wrap `inner` in a `<{Action}Response><{Action}Result>…` envelope.
pub(crate) fn xml_resp(action: &str, inner: String, request_id: &str) -> AwsResponse {
    AwsResponse::xml(
        StatusCode::OK,
        query_response_xml(action, NS, &inner, request_id),
    )
}

/// Response with only `<ResponseMetadata>` — for ops with no output members
/// (e.g. DeleteTags, RebootCluster-style acknowledgements that echo nothing).
pub(crate) fn xml_metadata_only(action: &str, request_id: &str) -> AwsResponse {
    AwsResponse::xml(
        StatusCode::OK,
        query_metadata_only_xml(action, NS, request_id),
    )
}

pub(crate) fn xml_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '&' => out.push_str("&amp;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            _ => out.push(c),
        }
    }
    out
}

/// `<Name>escaped(value)</Name>`.
pub(crate) fn tag_elem(name: &str, value: &str) -> String {
    format!("<{name}>{}</{name}>", xml_escape(value))
}

/// Optional `<Name>value</Name>` — emitted only when `value` is `Some`.
pub(crate) fn opt_elem(name: &str, value: Option<&str>) -> String {
    match value {
        Some(v) => tag_elem(name, v),
        None => String::new(),
    }
}

pub(crate) fn param(req: &AwsRequest, name: &str) -> Option<String> {
    req.query_params
        .get(name)
        .cloned()
        .filter(|v| !v.is_empty())
}

pub(crate) fn param_or(req: &AwsRequest, name: &str, default: &str) -> String {
    param(req, name).unwrap_or_else(|| default.to_string())
}

pub(crate) fn bool_param(req: &AwsRequest, name: &str) -> Option<bool> {
    param(req, name).map(|v| v.eq_ignore_ascii_case("true"))
}

pub(crate) fn int_param(req: &AwsRequest, name: &str) -> Option<i32> {
    param(req, name).and_then(|v| v.parse().ok())
}

pub(crate) fn long_param(req: &AwsRequest, name: &str) -> Option<i64> {
    param(req, name).and_then(|v| v.parse().ok())
}

/// Collect a flat scalar list serialized as `{prefix}.member.N` (also accepts
/// the alternate `{prefix}.{alt}.N` wire form some SDKs emit).
pub(crate) fn member_list(req: &AwsRequest, prefix: &str, alt: &str) -> Vec<String> {
    let mut out = Vec::new();
    for wire in [format!("{prefix}.member"), format!("{prefix}.{alt}")] {
        for n in 1..=200 {
            match req.query_params.get(&format!("{wire}.{n}")) {
                Some(v) => out.push(v.clone()),
                None => break,
            }
        }
        if !out.is_empty() {
            break;
        }
    }
    out
}

/// Parse `Tags.member.N.Key` / `.Value` into a tag list.
pub(crate) fn parse_tags(req: &AwsRequest) -> Vec<Tag> {
    let mut out = Vec::new();
    for wire in ["Tags.member", "Tags.Tag"] {
        for n in 1..=50 {
            match req.query_params.get(&format!("{wire}.{n}.Key")) {
                Some(k) => {
                    let value = req
                        .query_params
                        .get(&format!("{wire}.{n}.Value"))
                        .cloned()
                        .unwrap_or_default();
                    out.push(Tag {
                        key: k.clone(),
                        value,
                    });
                }
                None => break,
            }
        }
        if !out.is_empty() {
            break;
        }
    }
    out
}

/// Render a `<Tags><Tag><Key/><Value/></Tag>…</Tags>` block.
pub(crate) fn render_tags(tags: &[Tag]) -> String {
    if tags.is_empty() {
        return "<Tags/>".to_string();
    }
    let inner: String = tags
        .iter()
        .map(|t| {
            format!(
                "<Tag><Key>{}</Key><Value>{}</Value></Tag>",
                xml_escape(&t.key),
                xml_escape(&t.value)
            )
        })
        .collect();
    format!("<Tags>{inner}</Tags>")
}

/// Redshift paginates with an opaque `Marker` and a `MaxRecords` page size.
/// We model the marker as a numeric offset — SDKs treat it as opaque and just
/// round-trip it. Returns the page slice and the next marker (if truncated).
pub(crate) fn paginate<'a, T>(items: &'a [T], req: &AwsRequest) -> (Vec<&'a T>, Option<String>) {
    let offset: usize = param(req, "Marker")
        .and_then(|m| m.parse().ok())
        .unwrap_or(0);
    let max: usize = int_param(req, "MaxRecords")
        .filter(|n| *n > 0)
        .map(|n| n as usize)
        .unwrap_or(100)
        .min(100);
    let end = (offset + max).min(items.len());
    let page: Vec<&T> = if offset < items.len() {
        items[offset..end].iter().collect()
    } else {
        Vec::new()
    };
    let next = if end < items.len() {
        Some(end.to_string())
    } else {
        None
    };
    (page, next)
}

/// `<Marker>…</Marker>` when a next page exists.
pub(crate) fn render_marker(next: Option<String>) -> String {
    match next {
        Some(m) => tag_elem("Marker", &m),
        None => String::new(),
    }
}

// ───────────────────────── error constructors ─────────────────────────

fn err(status: u16, code: &str, msg: String) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::from_u16(status).unwrap(), code, msg)
}

pub(crate) fn cluster_not_found(id: &str) -> AwsServiceError {
    err(404, "ClusterNotFound", format!("Cluster {id} not found."))
}

pub(crate) fn cluster_already_exists(id: &str) -> AwsServiceError {
    err(
        400,
        "ClusterAlreadyExists",
        format!("Cluster {id} already exists."),
    )
}

pub(crate) fn parameter_group_not_found(id: &str) -> AwsServiceError {
    err(
        404,
        "ClusterParameterGroupNotFound",
        format!("ClusterParameterGroup {id} not found."),
    )
}

pub(crate) fn parameter_group_already_exists(id: &str) -> AwsServiceError {
    err(
        400,
        "ClusterParameterGroupAlreadyExists",
        format!("ClusterParameterGroup {id} already exists."),
    )
}

pub(crate) fn security_group_not_found(id: &str) -> AwsServiceError {
    err(
        404,
        "ClusterSecurityGroupNotFound",
        format!("ClusterSecurityGroup {id} not found."),
    )
}

pub(crate) fn security_group_already_exists(id: &str) -> AwsServiceError {
    err(
        400,
        "ClusterSecurityGroupAlreadyExists",
        format!("ClusterSecurityGroup {id} already exists."),
    )
}

pub(crate) fn subnet_group_not_found(id: &str) -> AwsServiceError {
    err(
        400,
        "ClusterSubnetGroupNotFoundFault",
        format!("ClusterSubnetGroup {id} not found."),
    )
}

pub(crate) fn subnet_group_already_exists(id: &str) -> AwsServiceError {
    err(
        400,
        "ClusterSubnetGroupAlreadyExists",
        format!("ClusterSubnetGroup {id} already exists."),
    )
}

pub(crate) fn snapshot_not_found(id: &str) -> AwsServiceError {
    err(
        404,
        "ClusterSnapshotNotFound",
        format!("ClusterSnapshot {id} not found."),
    )
}

pub(crate) fn snapshot_already_exists(id: &str) -> AwsServiceError {
    err(
        400,
        "ClusterSnapshotAlreadyExists",
        format!("ClusterSnapshot {id} already exists."),
    )
}

pub(crate) fn subscription_not_found(id: &str) -> AwsServiceError {
    err(
        404,
        "SubscriptionNotFound",
        format!("Subscription {id} not found."),
    )
}

pub(crate) fn subscription_already_exists(id: &str) -> AwsServiceError {
    err(
        400,
        "SubscriptionAlreadyExist",
        format!("Subscription {id} already exists."),
    )
}

pub(crate) fn hsm_client_certificate_not_found(id: &str) -> AwsServiceError {
    err(
        400,
        "HsmClientCertificateNotFoundFault",
        format!("HsmClientCertificate {id} not found."),
    )
}

pub(crate) fn hsm_client_certificate_already_exists(id: &str) -> AwsServiceError {
    err(
        400,
        "HsmClientCertificateAlreadyExistsFault",
        format!("HsmClientCertificate {id} already exists."),
    )
}

pub(crate) fn hsm_configuration_not_found(id: &str) -> AwsServiceError {
    err(
        400,
        "HsmConfigurationNotFoundFault",
        format!("HsmConfiguration {id} not found."),
    )
}

pub(crate) fn hsm_configuration_already_exists(id: &str) -> AwsServiceError {
    err(
        400,
        "HsmConfigurationAlreadyExistsFault",
        format!("HsmConfiguration {id} already exists."),
    )
}

pub(crate) fn snapshot_copy_grant_not_found(id: &str) -> AwsServiceError {
    err(
        400,
        "SnapshotCopyGrantNotFoundFault",
        format!("SnapshotCopyGrant {id} not found."),
    )
}

pub(crate) fn snapshot_copy_grant_already_exists(id: &str) -> AwsServiceError {
    err(
        400,
        "SnapshotCopyGrantAlreadyExistsFault",
        format!("SnapshotCopyGrant {id} already exists."),
    )
}

pub(crate) fn snapshot_schedule_not_found(id: &str) -> AwsServiceError {
    err(
        400,
        "SnapshotScheduleNotFound",
        format!("SnapshotSchedule {id} not found."),
    )
}

pub(crate) fn snapshot_schedule_already_exists(id: &str) -> AwsServiceError {
    err(
        400,
        "SnapshotScheduleAlreadyExists",
        format!("SnapshotSchedule {id} already exists."),
    )
}

pub(crate) fn scheduled_action_not_found(id: &str) -> AwsServiceError {
    err(
        400,
        "ScheduledActionNotFound",
        format!("ScheduledAction {id} not found."),
    )
}

pub(crate) fn scheduled_action_already_exists(id: &str) -> AwsServiceError {
    err(
        400,
        "ScheduledActionAlreadyExists",
        format!("ScheduledAction {id} already exists."),
    )
}

pub(crate) fn usage_limit_not_found(id: &str) -> AwsServiceError {
    err(
        404,
        "UsageLimitNotFound",
        format!("UsageLimit {id} not found."),
    )
}

pub(crate) fn usage_limit_already_exists(id: &str) -> AwsServiceError {
    err(
        400,
        "UsageLimitAlreadyExists",
        format!("UsageLimit {id} already exists."),
    )
}

pub(crate) fn endpoint_not_found(id: &str) -> AwsServiceError {
    err(404, "EndpointNotFound", format!("Endpoint {id} not found."))
}

pub(crate) fn endpoint_already_exists(id: &str) -> AwsServiceError {
    err(
        400,
        "EndpointAlreadyExists",
        format!("Endpoint {id} already exists."),
    )
}

pub(crate) fn endpoint_authorization_already_exists(account: &str) -> AwsServiceError {
    err(
        400,
        "EndpointAuthorizationAlreadyExists",
        format!("Endpoint authorization for account {account} already exists."),
    )
}

pub(crate) fn endpoint_authorization_not_found(account: &str) -> AwsServiceError {
    err(
        404,
        "EndpointAuthorizationNotFound",
        format!("Endpoint authorization for account {account} not found."),
    )
}

pub(crate) fn invalid_authorization_state(msg: impl Into<String>) -> AwsServiceError {
    err(400, "InvalidAuthorizationState", msg.into())
}

pub(crate) fn authentication_profile_not_found(id: &str) -> AwsServiceError {
    err(
        404,
        "AuthenticationProfileNotFoundFault",
        format!("AuthenticationProfile {id} not found."),
    )
}

pub(crate) fn authentication_profile_already_exists(id: &str) -> AwsServiceError {
    err(
        400,
        "AuthenticationProfileAlreadyExistsFault",
        format!("AuthenticationProfile {id} already exists."),
    )
}

pub(crate) fn integration_not_found(id: &str) -> AwsServiceError {
    err(
        404,
        "IntegrationNotFoundFault",
        format!("Integration {id} not found."),
    )
}

pub(crate) fn integration_already_exists(id: &str) -> AwsServiceError {
    err(
        400,
        "IntegrationAlreadyExistsFault",
        format!("Integration {id} already exists."),
    )
}

pub(crate) fn idc_application_not_found(id: &str) -> AwsServiceError {
    err(
        404,
        "RedshiftIdcApplicationNotExists",
        format!("RedshiftIdcApplication {id} not found."),
    )
}

pub(crate) fn idc_application_already_exists(id: &str) -> AwsServiceError {
    err(
        400,
        "RedshiftIdcApplicationAlreadyExists",
        format!("RedshiftIdcApplication {id} already exists."),
    )
}

pub(crate) fn custom_domain_not_found(id: &str) -> AwsServiceError {
    err(
        404,
        "CustomDomainAssociationNotFoundFault",
        format!("CustomDomainAssociation {id} not found."),
    )
}

pub(crate) fn resource_not_found(msg: impl Into<String>) -> AwsServiceError {
    err(404, "ResourceNotFoundFault", msg.into())
}

pub(crate) fn invalid_datashare(msg: impl Into<String>) -> AwsServiceError {
    err(400, "InvalidDataShareFault", msg.into())
}
