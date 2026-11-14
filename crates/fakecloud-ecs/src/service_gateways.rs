// ECS gateways: shared, gateway-managed ingress built on host-routed
// Application Load Balancers. A gateway is associated with clusters; every
// ACTIVE service in an associated cluster whose `gatewayConnectors` select
// the gateway's access type gets a gateway route to the connector's port.
//
// The gateway's load balancers, listeners, rules, target groups, security
// groups and certificates are identities the gateway owns and reports; they
// are not provisioned as resources in the ELBv2, EC2 or ACM services.

use chrono::Utc;
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};

use fakecloud_core::pagination::paginate_checked;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use super::*;
use crate::state::{Gateway, GatewayClusterAssociation, GatewayLoadBalancer, TaskDefinition};

const GATEWAY_STATUSES: &[&str] = &[
    "ACTIVE",
    "UPDATE_IN_PROGRESS",
    "UPDATE_COMPLETE",
    "UPDATE_FAILED",
    "DELETED",
];
const ASSOCIATION_STATUSES: &[&str] = &["ACTIVE", "DISASSOCIATING", "INACTIVE"];
const ROUTE_STATUSES: &[&str] = &[
    "ACTIVE",
    "CREATE_IN_PROGRESS",
    "CREATE_FAILED",
    "UPDATE_IN_PROGRESS",
    "UPDATE_FAILED",
    "DELETE_IN_PROGRESS",
    "DELETE_FAILED",
];
const ACCESS_TYPES: &[&str] = &["PUBLIC", "PRIVATE"];

/// A gateway route: one gateway, one associated cluster, one service
/// connector port. Routes are derived from the current gateways,
/// associations and services, so they appear and disappear with them.
#[derive(Clone, Debug)]
pub(crate) struct GatewayRouteView {
    pub gateway_route_arn: String,
    pub gateway_arn: String,
    pub cluster_arn: String,
    pub service_arn: String,
    pub port_name: String,
    pub access_type: String,
    pub service_url: String,
    route_id: String,
    region: String,
    account: String,
    https: bool,
    test_traffic: bool,
    load_balancer: GatewayLoadBalancer,
}

impl GatewayRouteView {
    pub fn status(&self) -> &'static str {
        "ACTIVE"
    }

    fn certificate_arn(&self) -> Option<String> {
        self.https.then(|| {
            let h = hex_digest(&format!("{}|certificate", self.route_id));
            fakecloud_aws::arn::Arn::regional(
                "acm",
                &self.region,
                &self.account,
                &format!(
                    "certificate/{}-{}-{}-{}-{}",
                    &h[0..8],
                    &h[8..12],
                    &h[12..16],
                    &h[16..20],
                    &h[20..32]
                ),
            )
            .to_string()
        })
    }

    fn rule_arn(&self, suffix: &str) -> String {
        let id = &hex_digest(&format!("{}|{suffix}", self.route_id))[..16];
        format!(
            "{}/{id}",
            self.load_balancer
                .listener_arn
                .replacen(":listener/", ":listener-rule/", 1)
        )
    }

    fn target_group_arn(&self, suffix: &str) -> String {
        let id = &hex_digest(&format!("{}|{suffix}", self.route_id))[..16];
        fakecloud_aws::arn::Arn::regional(
            "elasticloadbalancing",
            &self.region,
            &self.account,
            &format!("targetgroup/ecs-gw-{suffix}-{}/{id}", &self.route_id[..8]),
        )
        .to_string()
    }

    fn load_balancer_security_group_arn(&self) -> String {
        security_group_arn(
            &self.region,
            &self.account,
            &self.load_balancer.security_group_id,
        )
    }

    fn service_security_group_arn(&self) -> String {
        let id = format!(
            "sg-{}",
            &hex_digest(&format!("{}|service-sg", self.route_id))[..17]
        );
        security_group_arn(&self.region, &self.account, &id)
    }

    fn summary_json(&self) -> Value {
        json!({
            "gatewayRouteArn": self.gateway_route_arn,
            "gatewayArn": self.gateway_arn,
            "clusterArn": self.cluster_arn,
            "serviceArn": self.service_arn,
            "serviceContainerPortName": self.port_name,
            "status": self.status(),
        })
    }

    fn detail_json(&self) -> Value {
        let mut alb = Map::new();
        if let Some(cert) = self.certificate_arn() {
            alb.insert("acmCertificateArn".into(), json!(cert));
        }
        alb.insert(
            "loadBalancerArn".into(),
            json!(self.load_balancer.load_balancer_arn),
        );
        alb.insert("listenerArn".into(), json!(self.load_balancer.listener_arn));
        alb.insert("listenerRuleArn".into(), json!(self.rule_arn("rule")));
        if self.test_traffic {
            alb.insert(
                "testListenerRuleArns".into(),
                json!([self.rule_arn("test-rule")]),
            );
        }
        let sgr = format!(
            "sgr-{}",
            &hex_digest(&format!("{}|sgr", self.route_id))[..17]
        );
        alb.insert(
            "loadBalancerSecurityGroups".into(),
            json!([{
                "arn": self.load_balancer_security_group_arn(),
                "ruleArns": [fakecloud_aws::arn::Arn::regional(
                    "ec2",
                    &self.region,
                    &self.account,
                    &format!("security-group-rule/{sgr}"),
                )
                .to_string()],
            }]),
        );
        alb.insert(
            "serviceSecurityGroupArn".into(),
            json!(self.service_security_group_arn()),
        );
        json!({
            "gatewayRouteArn": self.gateway_route_arn,
            "serviceArn": self.service_arn,
            "serviceContainerPortName": self.port_name,
            "status": self.status(),
            "serviceUrl": self.service_url,
            "resources": { "hostSharedAlb": [Value::Object(alb)] },
        })
    }

    /// The route as a `GatewayConnection` on a service revision's
    /// `ecsManagedResources`.
    pub fn connection_json(&self) -> Value {
        let mut alb = Map::new();
        if let Some(cert) = self.certificate_arn() {
            alb.insert("acmCertificateArn".into(), json!(cert));
        }
        alb.insert(
            "loadBalancerArn".into(),
            json!(self.load_balancer.load_balancer_arn),
        );
        alb.insert(
            "loadBalancerSecurityGroupArns".into(),
            json!([self.load_balancer_security_group_arn()]),
        );
        alb.insert("listenerArn".into(), json!(self.load_balancer.listener_arn));
        alb.insert("listenerRuleArn".into(), json!(self.rule_arn("rule")));
        alb.insert("targetGroupArn".into(), json!(self.target_group_arn("tg")));
        if self.test_traffic {
            alb.insert(
                "testListenerRuleArn".into(),
                json!(self.rule_arn("test-rule")),
            );
            alb.insert(
                "alternateTargetGroupArn".into(),
                json!(self.target_group_arn("alt")),
            );
        }
        json!({
            "gatewayArn": self.gateway_arn,
            "gatewayRouteArn": self.gateway_route_arn,
            "portName": self.port_name,
            "serviceUrl": self.service_url,
            "accessType": self.access_type,
            "status": self.status(),
            "resources": { "hostSharedAlb": [Value::Object(alb)] },
        })
    }
}

fn hex_digest(seed: &str) -> String {
    let digest = Sha256::digest(seed.as_bytes());
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

fn security_group_arn(region: &str, account: &str, id: &str) -> String {
    fakecloud_aws::arn::Arn::regional("ec2", region, account, &format!("security-group/{id}"))
        .to_string()
}

/// `(region, account)` of an ARN.
fn arn_scope(arn: &str) -> (String, String) {
    let mut parts = arn.split(':');
    let region = parts.nth(3).unwrap_or_default().to_string();
    let account = parts.next().unwrap_or_default().to_string();
    (region, account)
}

fn connector_access_types(connector: &Value) -> Vec<String> {
    connector
        .get("filters")
        .and_then(|f| f.get("accessTypes"))
        .and_then(|v| v.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

/// Every gateway route in `state`, sorted by route ARN. A route exists for
/// each non-deleted gateway, each of its ACTIVE cluster associations, each
/// ACTIVE service in that cluster and each of the service's gateway
/// connectors whose `filters.accessTypes` (all access types when empty)
/// include the gateway's access type.
pub(crate) fn gateway_routes(state: &EcsState) -> Vec<GatewayRouteView> {
    let mut routes = Vec::new();
    for gateway in state.gateways.values() {
        if gateway.status == "DELETED" {
            continue;
        }
        let Some(load_balancer) = gateway.load_balancers.first().cloned() else {
            continue;
        };
        let (region, account) = arn_scope(&gateway.gateway_arn);
        let https = gateway
            .host_shared_alb
            .get("protocol")
            .and_then(|v| v.as_str())
            != Some("HTTP");
        let test_traffic = gateway
            .host_shared_alb
            .get("testTrafficRules")
            .is_some_and(|v| !v.is_null());
        for assoc in state.gateway_cluster_associations.values() {
            if assoc.gateway_arn != gateway.gateway_arn || assoc.status != "ACTIVE" {
                continue;
            }
            for service in state.services.values() {
                if service.cluster_arn != assoc.cluster_arn || service.status != "ACTIVE" {
                    continue;
                }
                for connector in &service.gateway_connectors {
                    let Some(port_name) = connector.get("portName").and_then(|v| v.as_str()) else {
                        continue;
                    };
                    let filter = connector_access_types(connector);
                    if !filter.is_empty() && !filter.contains(&gateway.access_type) {
                        continue;
                    }
                    let route_id = hex_digest(&format!(
                        "{}|{}|{}",
                        gateway.gateway_arn, service.service_arn, port_name
                    ))[..32]
                        .to_string();
                    let gateway_route_arn = crate::state::ecs_arn(
                        &region,
                        &account,
                        &format!(
                            "gateway-route/{}/{}/{route_id}",
                            gateway.name, gateway.gateway_id
                        ),
                    );
                    let service_url = format!(
                        "{}://{}-{}.{}.gateway.ecs.{region}.on.aws",
                        if https { "https" } else { "http" },
                        service.service_name.to_ascii_lowercase(),
                        &route_id[..8],
                        &gateway.gateway_id[..12],
                    );
                    routes.push(GatewayRouteView {
                        gateway_route_arn,
                        gateway_arn: gateway.gateway_arn.clone(),
                        cluster_arn: assoc.cluster_arn.clone(),
                        service_arn: service.service_arn.clone(),
                        port_name: port_name.to_string(),
                        access_type: gateway.access_type.clone(),
                        service_url,
                        route_id,
                        region: region.clone(),
                        account: account.clone(),
                        https,
                        test_traffic,
                        load_balancer: load_balancer.clone(),
                    });
                }
            }
        }
    }
    routes.sort_by(|a, b| a.gateway_route_arn.cmp(&b.gateway_route_arn));
    routes
}

/// Validate a CreateService / UpdateService `gatewayConnectors` list against
/// the task definition the service runs.
pub(crate) fn validate_gateway_connectors(
    connectors: &[Value],
    td: &TaskDefinition,
) -> Result<(), AwsServiceError> {
    if connectors.len() > 1 {
        return Err(invalid_parameter(
            "A service supports no more than one gateway connector.",
        ));
    }
    let port_names: Vec<&str> = td
        .container_definitions
        .iter()
        .filter_map(|cd| cd.get("portMappings").and_then(|v| v.as_array()))
        .flatten()
        .filter_map(|pm| pm.get("name").and_then(|v| v.as_str()))
        .collect();
    for connector in connectors {
        let port_name = connector
            .get("portName")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| invalid_parameter("Gateway connector portName is required."))?;
        if !port_names.contains(&port_name) {
            return Err(invalid_parameter(format!(
                "The gateway connector portName '{port_name}' does not match a named port mapping in the task definition {}.",
                td.task_definition_arn
            )));
        }
        if let Some(hc) = connector.get("healthCheck").filter(|v| !v.is_null()) {
            validate_enum_opt(hc, "healthCheckProtocol", &["HTTP", "HTTPS"])?;
            for (field, min, max) in [
                ("healthCheckPort", 1, 65535),
                ("healthCheckTimeoutSeconds", 2, 120),
                ("healthCheckIntervalSeconds", 5, 300),
                ("healthyThresholdCount", 2, 10),
                ("unhealthyThresholdCount", 2, 10),
            ] {
                check_int_range(hc, field, min, max)?;
            }
        }
        if let Some(types) = connector
            .get("filters")
            .and_then(|f| f.get("accessTypes"))
            .filter(|v| !v.is_null())
        {
            validate_enum_list(types, "filters.accessTypes", ACCESS_TYPES)?;
        }
    }
    Ok(())
}

fn check_int_range(body: &Value, field: &str, min: i64, max: i64) -> Result<(), AwsServiceError> {
    match body.get(field).filter(|v| !v.is_null()) {
        None => Ok(()),
        Some(v) => match v.as_i64() {
            Some(n) if (min..=max).contains(&n) => Ok(()),
            _ => Err(invalid_parameter(format!(
                "{field} must be between {min} and {max}."
            ))),
        },
    }
}

/// A list of unique enum values, at most one entry per allowed value.
fn validate_enum_list(
    value: &Value,
    field: &str,
    allowed: &[&str],
) -> Result<Vec<String>, AwsServiceError> {
    let arr = value
        .as_array()
        .ok_or_else(|| invalid_parameter(format!("{field} must be a list.")))?;
    let mut out: Vec<String> = Vec::new();
    for v in arr {
        let s = v.as_str().filter(|s| allowed.contains(s)).ok_or_else(|| {
            invalid_parameter(format!(
                "Invalid value in {field}. Valid values: {}",
                allowed.join(", ")
            ))
        })?;
        if out.iter().any(|o| o == s) {
            return Err(invalid_parameter(format!(
                "{field} must not contain duplicate values."
            )));
        }
        out.push(s.to_string());
    }
    Ok(out)
}

fn status_filter(body: &Value, allowed: &[&str]) -> Result<Vec<String>, AwsServiceError> {
    match body.get("status").filter(|v| !v.is_null()) {
        None => Ok(Vec::new()),
        Some(v) => validate_enum_list(v, "status", allowed),
    }
}

fn page_params(body: &Value) -> Result<(usize, Option<String>), AwsServiceError> {
    let max_results = match body.get("maxResults").filter(|v| !v.is_null()) {
        None => 100,
        Some(v) => match v.as_i64() {
            Some(n) if (1..=100).contains(&n) => n as usize,
            _ => return Err(invalid_parameter("maxResults must be between 1 and 100.")),
        },
    };
    let next_token = opt_str(body, "nextToken")
        .filter(|s| !s.is_empty())
        .map(String::from);
    Ok((max_results, next_token))
}

fn paginate<T: Clone>(
    items: &[T],
    next_token: Option<&str>,
    max_results: usize,
) -> Result<(Vec<T>, Option<String>), AwsServiceError> {
    paginate_checked(items, next_token, max_results)
        .map_err(|_| invalid_parameter("Invalid nextToken"))
}

fn gateway_resource_not_found(what: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        http::StatusCode::BAD_REQUEST,
        "ResourceNotFoundException",
        format!("The specified resource was not found: {what}"),
    )
}

fn gateway_not_found(ident: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        http::StatusCode::BAD_REQUEST,
        "GatewayNotFoundException",
        format!("The gateway {ident} was not found."),
    )
}

/// Resolve a gateway name or ARN to its storage key. By name, a live gateway
/// wins over deleted ones (a deleted gateway's name can be reused).
fn find_gateway<'a>(
    state: &'a EcsState,
    ident: &str,
    include_deleted: bool,
) -> Option<&'a Gateway> {
    if ident.starts_with("arn:") {
        return state
            .gateways
            .get(ident)
            .filter(|g| include_deleted || g.status != "DELETED");
    }
    let live = state
        .gateways
        .values()
        .find(|g| g.name == ident && g.status != "DELETED");
    if live.is_some() || !include_deleted {
        return live;
    }
    state
        .gateways
        .values()
        .filter(|g| g.name == ident)
        .max_by_key(|g| g.updated_at)
}

fn live_gateway_arn(state: &EcsState, ident: &str) -> Result<String, AwsServiceError> {
    find_gateway(state, ident, false)
        .map(|g| g.gateway_arn.clone())
        .ok_or_else(|| gateway_not_found(ident))
}

fn validate_gateway_name(name: &str) -> Result<(), AwsServiceError> {
    if name.is_empty()
        || name.len() > 255
        || !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        return Err(invalid_parameter(
            "Gateway name must be 1-255 characters and may contain only letters, numbers, hyphens and underscores.",
        ));
    }
    Ok(())
}

fn validate_role_arn(arn: &str) -> Result<(), AwsServiceError> {
    let ok = arn.starts_with("arn:")
        && arn.split(':').nth(2) == Some("iam")
        && arn
            .split(':')
            .nth(5)
            .is_some_and(|r| r.starts_with("role/") && r.len() > "role/".len());
    if !ok {
        return Err(invalid_parameter(format!(
            "infrastructureRoleArn '{arn}' is not a valid IAM role ARN."
        )));
    }
    Ok(())
}

fn validate_tags(body: &Value) -> Result<(), AwsServiceError> {
    let Some(tags) = body.get("tags").and_then(|v| v.as_array()) else {
        return Ok(());
    };
    if tags.len() > 50 {
        return Err(invalid_parameter("A gateway can have at most 50 tags."));
    }
    for tag in tags {
        let key = tag.get("key").and_then(|v| v.as_str()).unwrap_or("");
        let value = tag.get("value").and_then(|v| v.as_str()).unwrap_or("");
        if key.is_empty() || key.chars().count() > 128 {
            return Err(invalid_parameter(
                "Tag keys must be between 1 and 128 characters.",
            ));
        }
        if value.chars().count() > 256 {
            return Err(invalid_parameter(
                "Tag values must be at most 256 characters.",
            ));
        }
    }
    Ok(())
}

/// Validate the fields CreateGateway and UpdateGateway share on a
/// `hostSharedAlb` configuration.
fn validate_alb_common(alb: &Value) -> Result<(), AwsServiceError> {
    validate_enum_opt(alb, "httpDesync", &["monitor", "defensive", "strictest"])?;
    check_int_range(alb, "maxAlbCount", 1, 20)?;
    if let Some(logs) = alb.get("accessLogs").filter(|v| !v.is_null()) {
        let bucket = logs
            .get("destination")
            .and_then(|d| d.get("s3"))
            .and_then(|s3| s3.get("bucket"))
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty());
        if bucket.is_none() {
            return Err(invalid_parameter(
                "accessLogs destination must specify an s3 bucket.",
            ));
        }
    }
    if let Some(rules) = alb.get("testTrafficRules").filter(|v| !v.is_null()) {
        let header = rules.get("header").filter(|v| !v.is_null());
        if let Some(header) = header {
            let name_ok = header
                .get("name")
                .and_then(|v| v.as_str())
                .is_some_and(|s| !s.is_empty());
            let values_ok = header
                .get("values")
                .and_then(|v| v.as_array())
                .is_some_and(|a| !a.is_empty() && a.iter().all(|v| v.is_string()));
            if !name_ok || !values_ok {
                return Err(invalid_parameter(
                    "testTrafficRules header requires a name and at least one value.",
                ));
            }
        }
    }
    if let Some(net) = alb.get("networkConfiguration").filter(|v| !v.is_null()) {
        if !net.is_object() {
            return Err(invalid_parameter("networkConfiguration must be an object."));
        }
    }
    Ok(())
}

/// Validate a CreateGateway `hostSharedAlb` configuration and apply AWS's
/// defaults (internet-facing HTTPS on 443, `defensive` desync mitigation,
/// IPv4 addressing).
fn normalize_create_alb(alb: &Value) -> Result<Value, AwsServiceError> {
    let Some(obj) = alb.as_object() else {
        return Err(invalid_parameter("hostSharedAlb must be an object."));
    };
    validate_alb_common(alb)?;
    validate_enum_opt(alb, "scheme", &["internet-facing", "internal"])?;
    validate_enum_opt(alb, "protocol", &["HTTP", "HTTPS"])?;
    check_int_range(alb, "port", 1, 65535)?;
    if let Some(net) = alb.get("networkConfiguration").filter(|v| !v.is_null()) {
        validate_enum_opt(
            net,
            "ipAddressType",
            &["ipv4", "dualstack", "dualstack-without-public-ipv4"],
        )?;
        if let Some(subnets) = net.get("subnets").filter(|v| !v.is_null()) {
            let ok = subnets
                .as_array()
                .is_some_and(|a| a.iter().all(|s| s.is_string()));
            if !ok {
                return Err(invalid_parameter("subnets must be a list of subnet IDs."));
            }
        }
    }
    let mut out = obj.clone();
    out.retain(|_, v| !v.is_null());
    out.entry("scheme")
        .or_insert_with(|| json!("internet-facing"));
    out.entry("protocol").or_insert_with(|| json!("HTTPS"));
    let default_port = if out.get("protocol").and_then(|v| v.as_str()) == Some("HTTP") {
        80
    } else {
        443
    };
    out.entry("port").or_insert_with(|| json!(default_port));
    out.entry("httpDesync")
        .or_insert_with(|| json!("defensive"));
    let net = out
        .entry("networkConfiguration")
        .or_insert_with(|| json!({}));
    if let Some(net) = net.as_object_mut() {
        net.entry("ipAddressType").or_insert_with(|| json!("ipv4"));
    }
    Ok(Value::Object(out))
}

/// The single member of a `GatewayConfiguration`-style union, which must be
/// `hostSharedAlb`.
fn host_shared_alb_member(configuration: &Value) -> Result<&Value, AwsServiceError> {
    let obj = configuration
        .as_object()
        .ok_or_else(|| invalid_parameter("configuration must be an object."))?;
    if let Some(other) = obj.keys().find(|k| k.as_str() != "hostSharedAlb") {
        return Err(invalid_parameter(format!(
            "Unsupported gateway configuration type '{other}'. Valid values: hostSharedAlb"
        )));
    }
    obj.get("hostSharedAlb")
        .filter(|v| !v.is_null())
        .ok_or_else(|| invalid_parameter("configuration must specify hostSharedAlb."))
}

fn new_load_balancer(gateway: &Gateway, index: usize) -> GatewayLoadBalancer {
    let (region, account) = arn_scope(&gateway.gateway_arn);
    let seed = hex_digest(&format!("{}|alb|{index}", gateway.gateway_arn));
    let lb_name = format!("ecs-gw-{}-{index}", &gateway.gateway_id[..16]);
    let lb_resource = format!("app/{lb_name}/{}", &seed[..16]);
    let load_balancer_arn = fakecloud_aws::arn::Arn::regional(
        "elasticloadbalancing",
        &region,
        &account,
        &format!("loadbalancer/{lb_resource}"),
    )
    .to_string();
    let listener_arn = fakecloud_aws::arn::Arn::regional(
        "elasticloadbalancing",
        &region,
        &account,
        &format!("listener/{lb_resource}/{}", &seed[16..32]),
    )
    .to_string();
    let security_group_id = gateway
        .host_shared_alb
        .get("networkConfiguration")
        .and_then(|n| n.get("inboundSecurityGroup"))
        .and_then(|v| v.as_str())
        .map(String::from)
        .unwrap_or_else(|| format!("sg-{}", &seed[32..49]));
    GatewayLoadBalancer {
        load_balancer_arn,
        listener_arn,
        security_group_id,
    }
}

fn epoch(t: chrono::DateTime<Utc>) -> f64 {
    t.timestamp_millis() as f64 / 1000.0
}

fn gateway_json(g: &Gateway) -> Value {
    let mut alb = g.host_shared_alb.as_object().cloned().unwrap_or_default();
    alb.insert("currentAlbCount".into(), json!(g.load_balancers.len()));
    let mut map = Map::new();
    map.insert("gatewayArn".into(), json!(g.gateway_arn));
    map.insert("name".into(), json!(g.name));
    map.insert("status".into(), json!(g.status));
    if let Some(ref reason) = g.status_reason {
        map.insert("statusReason".into(), json!(reason));
    }
    map.insert(
        "infrastructureRoleArn".into(),
        json!(g.infrastructure_role_arn),
    );
    map.insert("deletionProtection".into(), json!(g.deletion_protection));
    map.insert(
        "configuration".into(),
        json!({ "hostSharedAlb": Value::Object(alb) }),
    );
    map.insert("accessType".into(), json!(g.access_type));
    map.insert(
        "propagateTags".into(),
        json!(g.propagate_tags.as_deref().unwrap_or("NONE")),
    );
    map.insert("tags".into(), tags_json(&g.tags));
    map.insert("createdAt".into(), json!(epoch(g.created_at)));
    map.insert("updatedAt".into(), json!(epoch(g.updated_at)));
    Value::Object(map)
}

fn gateway_brief_json(g: &Gateway, status: &str) -> Value {
    json!({
        "gatewayArn": g.gateway_arn,
        "name": g.name,
        "status": status,
    })
}

fn association_json(a: &GatewayClusterAssociation, status: &str) -> Value {
    json!({
        "gatewayArn": a.gateway_arn,
        "clusterArn": a.cluster_arn,
        "status": status,
    })
}

/// Resolve a cluster name or ARN to an ACTIVE cluster's `(name, arn)`.
fn active_cluster(
    state: &EcsState,
    cluster_ref: &str,
) -> Result<(String, String), AwsServiceError> {
    let name = EcsState::resolve_cluster_name(Some(cluster_ref));
    state
        .clusters
        .get(&name)
        .filter(|c| c.status == "ACTIVE")
        .map(|c| (name.clone(), c.cluster_arn.clone()))
        .ok_or_else(|| cluster_not_found(&name))
}

impl EcsService {
    pub(super) fn create_gateway(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let name = req_str(&body, "name")?.to_string();
        validate_gateway_name(&name)?;
        let infrastructure_role_arn = req_str(&body, "infrastructureRoleArn")?.to_string();
        validate_role_arn(&infrastructure_role_arn)?;
        let configuration = body
            .get("configuration")
            .filter(|v| !v.is_null())
            .ok_or_else(|| client_exception("Missing required field: configuration"))?;
        let host_shared_alb = normalize_create_alb(host_shared_alb_member(configuration)?)?;
        validate_enum_opt(&body, "propagateTags", &["NONE", "GATEWAY"])?;
        validate_tags(&body)?;
        let deletion_protection = body
            .get("deletionProtection")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let propagate_tags = opt_str(&body, "propagateTags").map(String::from);
        let client_token = opt_str(&body, "clientToken")
            .filter(|s| !s.is_empty())
            .map(String::from);
        let tags = parse_tags(&body);

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);

        if let Some(existing) = state
            .gateways
            .values()
            .find(|g| g.name == name && g.status != "DELETED")
        {
            // A retried CreateGateway with the same client token returns the
            // gateway the first call created.
            if client_token.is_some() && existing.client_token == client_token {
                return Ok(AwsResponse::ok_json(gateway_brief_json(
                    existing,
                    &existing.status,
                )));
            }
            return Err(client_exception(format!(
                "A gateway named {name} already exists."
            )));
        }
        if let Some(token) = &client_token {
            if state
                .gateways
                .values()
                .any(|g| g.status != "DELETED" && g.client_token.as_deref() == Some(token))
            {
                return Err(client_exception(
                    "The client token was already used to create a different gateway.",
                ));
            }
        }

        let gateway_id = uuid::Uuid::new_v4().simple().to_string();
        let gateway_arn = crate::state::ecs_arn(
            &request.region,
            &state.account_id,
            &format!("gateway/{name}/{gateway_id}"),
        );
        let access_type =
            if host_shared_alb.get("scheme").and_then(|v| v.as_str()) == Some("internal") {
                "PRIVATE"
            } else {
                "PUBLIC"
            };
        let now = Utc::now();
        let mut gateway = Gateway {
            name,
            gateway_id,
            gateway_arn: gateway_arn.clone(),
            status: "ACTIVE".into(),
            status_reason: None,
            infrastructure_role_arn,
            deletion_protection,
            host_shared_alb,
            access_type: access_type.into(),
            propagate_tags,
            tags,
            client_token,
            load_balancers: Vec::new(),
            created_at: now,
            updated_at: now,
        };
        gateway.load_balancers.push(new_load_balancer(&gateway, 0));
        let out = gateway_brief_json(&gateway, &gateway.status);
        state.gateways.insert(gateway_arn, gateway);
        Ok(AwsResponse::ok_json(out))
    }

    pub(super) fn describe_gateway(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let ident = req_str(&body, "gateway")?;
        let accounts = self.state.read();
        let gateway = accounts
            .get(&request.account_id)
            .and_then(|s| find_gateway(s, ident, true))
            .ok_or_else(|| gateway_not_found(ident))?;
        Ok(AwsResponse::ok_json(
            json!({ "gateway": gateway_json(gateway) }),
        ))
    }

    pub(super) fn update_gateway(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let ident = req_str(&body, "gateway")?;
        if let Some(role) = opt_str(&body, "infrastructureRoleArn") {
            validate_role_arn(role)?;
        }
        validate_enum_opt(&body, "propagateTags", &["NONE", "GATEWAY"])?;
        let alb_update = match body.get("configuration").filter(|v| !v.is_null()) {
            Some(cfg) => {
                let alb = host_shared_alb_member(cfg)?;
                if !alb.is_object() {
                    return Err(invalid_parameter("hostSharedAlb must be an object."));
                }
                validate_alb_common(alb)?;
                Some(alb.clone())
            }
            None => None,
        };

        let mut accounts = self.state.write();
        let state = accounts
            .get_mut(&request.account_id)
            .ok_or_else(|| gateway_not_found(ident))?;
        let arn = live_gateway_arn(state, ident)?;
        let gateway = state.gateways.get_mut(&arn).unwrap();

        if let Some(alb) = &alb_update {
            if let Some(n) = alb.get("maxAlbCount").and_then(|v| v.as_i64()) {
                if (n as usize) < gateway.load_balancers.len() {
                    return Err(invalid_parameter(format!(
                        "maxAlbCount cannot be lower than the gateway's current load balancer count ({}).",
                        gateway.load_balancers.len()
                    )));
                }
            }
        }

        if let Some(role) = opt_str(&body, "infrastructureRoleArn") {
            gateway.infrastructure_role_arn = role.to_string();
        }
        if let Some(b) = body.get("deletionProtection").and_then(|v| v.as_bool()) {
            gateway.deletion_protection = b;
        }
        if let Some(p) = opt_str(&body, "propagateTags") {
            gateway.propagate_tags = Some(p.to_string());
        }
        if let Some(alb) = alb_update {
            let stored = gateway
                .host_shared_alb
                .as_object_mut()
                .expect("stored hostSharedAlb is an object");
            for (key, value) in alb.as_object().unwrap() {
                if value.is_null() {
                    continue;
                }
                if key == "networkConfiguration" {
                    let net = stored
                        .entry("networkConfiguration")
                        .or_insert_with(|| json!({}));
                    if let (Some(net), Some(update)) = (net.as_object_mut(), value.as_object()) {
                        for (k, v) in update {
                            if !v.is_null() {
                                net.insert(k.clone(), v.clone());
                            }
                        }
                    }
                    if let Some(sg) = value.get("inboundSecurityGroup").and_then(|v| v.as_str()) {
                        for lb in gateway.load_balancers.iter_mut() {
                            lb.security_group_id = sg.to_string();
                        }
                    }
                } else {
                    stored.insert(key.clone(), value.clone());
                }
            }
        }
        // The update is applied before the call returns: the response
        // reports it in progress, the gateway itself settles to
        // UPDATE_COMPLETE.
        gateway.status = "UPDATE_COMPLETE".into();
        gateway.status_reason = None;
        gateway.updated_at = Utc::now();
        Ok(AwsResponse::ok_json(gateway_brief_json(
            gateway,
            "UPDATE_IN_PROGRESS",
        )))
    }

    pub(super) fn delete_gateway(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let ident = req_str(&body, "gateway")?;
        let mut accounts = self.state.write();
        let state = accounts
            .get_mut(&request.account_id)
            .ok_or_else(|| gateway_not_found(ident))?;
        let arn = live_gateway_arn(state, ident)?;
        let gateway = &state.gateways[&arn];
        if gateway.deletion_protection {
            return Err(client_exception(format!(
                "The gateway {} has deletion protection enabled. Disable deletion protection before deleting the gateway.",
                gateway.name
            )));
        }
        if state
            .gateway_cluster_associations
            .values()
            .any(|a| a.gateway_arn == arn && a.status != "INACTIVE")
        {
            return Err(client_exception(format!(
                "The gateway {} is still associated with one or more clusters. Disassociate it from every cluster before deleting it.",
                gateway.name
            )));
        }
        let gateway = state.gateways.get_mut(&arn).unwrap();
        gateway.status = "DELETED".into();
        gateway.status_reason = None;
        gateway.updated_at = Utc::now();
        Ok(AwsResponse::ok_json(gateway_brief_json(gateway, "DELETED")))
    }

    pub(super) fn list_gateways(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let statuses = status_filter(&body, GATEWAY_STATUSES)?;
        let names: Vec<String> = body
            .get("names")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let (max_results, next_token) = page_params(&body)?;
        let accounts = self.state.read();
        let mut gateways: Vec<Value> = Vec::new();
        if let Some(state) = accounts.get(&request.account_id) {
            let mut matched: Vec<&Gateway> = state
                .gateways
                .values()
                .filter(|g| {
                    if statuses.is_empty() {
                        g.status != "DELETED"
                    } else {
                        statuses.contains(&g.status)
                    }
                })
                .filter(|g| names.is_empty() || names.contains(&g.name))
                .collect();
            matched.sort_by(|a, b| {
                a.created_at
                    .cmp(&b.created_at)
                    .then_with(|| a.gateway_arn.cmp(&b.gateway_arn))
            });
            gateways = matched
                .into_iter()
                .map(|g| {
                    json!({
                        "gatewayArn": g.gateway_arn,
                        "name": g.name,
                        "status": g.status,
                        "createdAt": epoch(g.created_at),
                    })
                })
                .collect();
        }
        let (page, token) = paginate(&gateways, next_token.as_deref(), max_results)?;
        let mut out = json!({ "gateways": page });
        if let Some(t) = token {
            out["nextToken"] = json!(t);
        }
        Ok(AwsResponse::ok_json(out))
    }

    pub(super) fn associate_gateway_with_cluster(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let ident = req_str(&body, "gateway")?;
        let cluster_ref = req_str(&body, "cluster")?;
        let mut accounts = self.state.write();
        let state = accounts
            .get_mut(&request.account_id)
            .ok_or_else(|| gateway_not_found(ident))?;
        let gateway_arn = live_gateway_arn(state, ident)?;
        let (cluster_name, cluster_arn) = active_cluster(state, cluster_ref)?;
        let key = EcsState::gateway_association_key(&gateway_arn, &cluster_arn);
        if let Some(existing) = state.gateway_cluster_associations.get(&key) {
            if existing.status != "INACTIVE" {
                return Err(client_exception(format!(
                    "The gateway is already associated with the cluster {cluster_name} (status {}).",
                    existing.status
                )));
            }
        }
        let assoc = GatewayClusterAssociation {
            gateway_arn,
            cluster_arn,
            cluster_name,
            status: "ACTIVE".into(),
            updated_at: Utc::now(),
        };
        let out = association_json(&assoc, "ACTIVE");
        state.gateway_cluster_associations.insert(key, assoc);
        Ok(AwsResponse::ok_json(out))
    }

    pub(super) fn disassociate_gateway_from_cluster(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let ident = req_str(&body, "gateway")?;
        let cluster_ref = req_str(&body, "cluster")?;
        let mut accounts = self.state.write();
        let state = accounts
            .get_mut(&request.account_id)
            .ok_or_else(|| gateway_not_found(ident))?;
        let gateway_arn = live_gateway_arn(state, ident)?;
        let cluster_name = EcsState::resolve_cluster_name(Some(cluster_ref));
        let cluster_arn = state
            .clusters
            .get(&cluster_name)
            .map(|c| c.cluster_arn.clone())
            .ok_or_else(|| cluster_not_found(&cluster_name))?;
        let key = EcsState::gateway_association_key(&gateway_arn, &cluster_arn);
        if !state
            .gateway_cluster_associations
            .get(&key)
            .is_some_and(|a| a.status == "ACTIVE")
        {
            return Err(gateway_resource_not_found(&format!(
                "association between {gateway_arn} and {cluster_arn}"
            )));
        }
        // The association drains while its routes are torn down; route
        // teardown completes before this call returns, so the stored
        // association is INACTIVE and only a call that removed routes
        // reports DISASSOCIATING.
        let had_routes = gateway_routes(state)
            .iter()
            .any(|r| r.gateway_arn == gateway_arn && r.cluster_arn == cluster_arn);
        let assoc = state.gateway_cluster_associations.get_mut(&key).unwrap();
        assoc.status = "INACTIVE".into();
        assoc.updated_at = Utc::now();
        let reported = if had_routes {
            "DISASSOCIATING"
        } else {
            "INACTIVE"
        };
        Ok(AwsResponse::ok_json(association_json(assoc, reported)))
    }

    pub(super) fn list_gateway_cluster_associations(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let statuses = status_filter(&body, ASSOCIATION_STATUSES)?;
        let (max_results, next_token) = page_params(&body)?;
        let accounts = self.state.read();
        let mut rows: Vec<Value> = Vec::new();
        if let Some(state) = accounts.get(&request.account_id) {
            let gateway_filter: Option<Option<String>> = opt_str(&body, "gateway")
                .map(|g| find_gateway(state, g, true).map(|g| g.gateway_arn.clone()));
            let cluster_filter =
                opt_str(&body, "cluster").map(|c| EcsState::resolve_cluster_name(Some(c)));
            let mut matched: Vec<&GatewayClusterAssociation> = state
                .gateway_cluster_associations
                .values()
                .filter(|a| match &gateway_filter {
                    None => true,
                    Some(arn) => arn.as_deref() == Some(a.gateway_arn.as_str()),
                })
                .filter(|a| {
                    cluster_filter
                        .as_deref()
                        .is_none_or(|c| c == a.cluster_name)
                })
                .filter(|a| statuses.is_empty() || statuses.contains(&a.status))
                .collect();
            matched.sort_by(|a, b| {
                (a.gateway_arn.as_str(), a.cluster_arn.as_str())
                    .cmp(&(b.gateway_arn.as_str(), b.cluster_arn.as_str()))
            });
            rows = matched
                .into_iter()
                .map(|a| association_json(a, &a.status))
                .collect();
        }
        let (page, token) = paginate(&rows, next_token.as_deref(), max_results)?;
        let mut out = json!({ "gatewayClusterAssociations": page });
        if let Some(t) = token {
            out["nextToken"] = json!(t);
        }
        Ok(AwsResponse::ok_json(out))
    }

    pub(super) fn list_gateway_routes(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let statuses = status_filter(&body, ROUTE_STATUSES)?;
        let (max_results, next_token) = page_params(&body)?;
        let accounts = self.state.read();
        let state = accounts.get(&request.account_id);
        let gateway_arn = match opt_str(&body, "gateway") {
            Some(g) => Some(
                state
                    .and_then(|s| find_gateway(s, g, true))
                    .map(|g| g.gateway_arn.clone())
                    .ok_or_else(|| gateway_not_found(g))?,
            ),
            None => None,
        };
        let cluster_filter =
            opt_str(&body, "cluster").map(|c| EcsState::resolve_cluster_name(Some(c)));
        let service_filter = opt_str(&body, "service");
        let port_filter = opt_str(&body, "serviceContainerPortName");
        let mut rows: Vec<Value> = Vec::new();
        if let Some(state) = state {
            rows = gateway_routes(state)
                .into_iter()
                .filter(|r| gateway_arn.as_deref().is_none_or(|g| g == r.gateway_arn))
                .filter(|r| {
                    cluster_filter
                        .as_deref()
                        .is_none_or(|c| EcsState::resolve_cluster_name(Some(&r.cluster_arn)) == c)
                })
                .filter(|r| {
                    service_filter.is_none_or(|s| {
                        if s.starts_with("arn:") {
                            s == r.service_arn
                        } else {
                            service_name_from_ref(&r.service_arn) == s
                        }
                    })
                })
                .filter(|r| port_filter.is_none_or(|p| p == r.port_name))
                .filter(|r| statuses.is_empty() || statuses.iter().any(|s| s == r.status()))
                .map(|r| r.summary_json())
                .collect();
        }
        let (page, token) = paginate(&rows, next_token.as_deref(), max_results)?;
        let mut out = json!({ "gatewayRoutes": page });
        if let Some(t) = token {
            out["nextToken"] = json!(t);
        }
        Ok(AwsResponse::ok_json(out))
    }

    pub(super) fn describe_gateway_route(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let arn = req_str(&body, "gatewayRouteArn")?;
        let accounts = self.state.read();
        let route = accounts
            .get(&request.account_id)
            .and_then(|s| {
                gateway_routes(s)
                    .into_iter()
                    .find(|r| r.gateway_route_arn == arn)
            })
            .ok_or_else(|| gateway_resource_not_found(arn))?;
        Ok(AwsResponse::ok_json(
            json!({ "gatewayRoute": route.detail_json() }),
        ))
    }

    pub(super) fn describe_gateway_resources(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let ident = req_str(&body, "gateway")?;
        let (max_results, next_token) = page_params(&body)?;
        let accounts = self.state.read();
        let state = accounts
            .get(&request.account_id)
            .ok_or_else(|| gateway_not_found(ident))?;
        let gateway = find_gateway(state, ident, false).ok_or_else(|| gateway_not_found(ident))?;
        let routes = gateway_routes(state);
        let albs: Vec<Value> = gateway
            .load_balancers
            .iter()
            .map(|lb| {
                let route_arns: Vec<&str> = routes
                    .iter()
                    .filter(|r| {
                        r.gateway_arn == gateway.gateway_arn
                            && r.load_balancer.load_balancer_arn == lb.load_balancer_arn
                    })
                    .map(|r| r.gateway_route_arn.as_str())
                    .collect();
                json!({
                    "loadBalancerArn": lb.load_balancer_arn,
                    "listenerArn": lb.listener_arn,
                    "securityGroup": lb.security_group_id,
                    "gatewayRouteArns": route_arns,
                })
            })
            .collect();
        let (page, token) = paginate(&albs, next_token.as_deref(), max_results)?;
        let mut out = json!({
            "gatewayResources": {
                "gatewayArn": gateway.gateway_arn,
                "resources": { "hostSharedAlb": page },
            }
        });
        if let Some(t) = token {
            out["nextToken"] = json!(t);
        }
        Ok(AwsResponse::ok_json(out))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn td_with_port(name: &str) -> TaskDefinition {
        TaskDefinition {
            family: "web".into(),
            revision: 1,
            task_definition_arn: "arn:aws:ecs:us-east-1:123456789012:task-definition/web:1".into(),
            container_definitions: vec![json!({
                "name": "app",
                "image": "nginx",
                "portMappings": [{"containerPort": 80, "name": name}],
            })],
            status: "ACTIVE".into(),
            task_role_arn: None,
            execution_role_arn: None,
            network_mode: None,
            requires_compatibilities: vec![],
            compatibilities: vec![],
            cpu: None,
            memory: None,
            pid_mode: None,
            ipc_mode: None,
            volumes: vec![],
            placement_constraints: vec![],
            proxy_configuration: None,
            inference_accelerators: vec![],
            ephemeral_storage: None,
            runtime_platform: None,
            requires_attributes: vec![],
            registered_at: Utc::now(),
            registered_by: None,
            deregistered_at: None,
            tags: vec![],
            enable_fault_injection: None,
        }
    }

    #[test]
    fn connectors_must_name_a_task_definition_port() {
        let td = td_with_port("web");
        assert!(validate_gateway_connectors(&[json!({"portName": "web"})], &td).is_ok());
        assert!(validate_gateway_connectors(&[json!({"portName": "api"})], &td).is_err());
        assert!(validate_gateway_connectors(&[json!({})], &td).is_err());
        let two = [json!({"portName": "web"}), json!({"portName": "web"})];
        assert!(validate_gateway_connectors(&two, &td).is_err());
    }

    #[test]
    fn connector_health_check_and_filters_are_validated() {
        let td = td_with_port("web");
        let bad_port = json!({"portName": "web", "healthCheck": {"healthCheckPort": 0}});
        assert!(validate_gateway_connectors(&[bad_port], &td).is_err());
        let bad_proto = json!({"portName": "web", "healthCheck": {"healthCheckProtocol": "TCP"}});
        assert!(validate_gateway_connectors(&[bad_proto], &td).is_err());
        let dup = json!({"portName": "web", "filters": {"accessTypes": ["PUBLIC", "PUBLIC"]}});
        assert!(validate_gateway_connectors(&[dup], &td).is_err());
        let ok = json!({
            "portName": "web",
            "healthCheck": {"healthCheckProtocol": "HTTP", "healthCheckPath": "/", "healthyThresholdCount": 3},
            "filters": {"accessTypes": ["PRIVATE"]},
        });
        assert!(validate_gateway_connectors(&[ok], &td).is_ok());
    }

    #[test]
    fn create_alb_defaults_follow_protocol() {
        let https = normalize_create_alb(&json!({})).unwrap();
        assert_eq!(https["scheme"], "internet-facing");
        assert_eq!(https["protocol"], "HTTPS");
        assert_eq!(https["port"], 443);
        assert_eq!(https["httpDesync"], "defensive");
        assert_eq!(https["networkConfiguration"]["ipAddressType"], "ipv4");
        let http = normalize_create_alb(&json!({"protocol": "HTTP"})).unwrap();
        assert_eq!(http["port"], 80);
        assert!(normalize_create_alb(&json!({"scheme": "public"})).is_err());
        assert!(normalize_create_alb(&json!({"port": 70000})).is_err());
        assert!(normalize_create_alb(&json!({"maxAlbCount": 0})).is_err());
    }

    #[test]
    fn configuration_union_requires_host_shared_alb() {
        assert!(host_shared_alb_member(&json!({})).is_err());
        assert!(host_shared_alb_member(&json!({"other": {}})).is_err());
        assert!(host_shared_alb_member(&json!({"hostSharedAlb": {}})).is_ok());
    }

    fn service() -> EcsService {
        use fakecloud_core::multi_account::MultiAccountState;
        let accounts: MultiAccountState<EcsState> =
            MultiAccountState::new("000000000000", "us-east-1", "http://localhost:4566");
        let state = std::sync::Arc::new(parking_lot::RwLock::new(accounts));
        EcsService::new(state)
    }

    fn call(svc: &EcsService, action: &str, body: Value) -> Result<Value, AwsServiceError> {
        let req = AwsRequest {
            service: "ecs".into(),
            action: action.into(),
            region: "us-east-1".into(),
            account_id: "000000000000".into(),
            request_id: "req".into(),
            headers: http::HeaderMap::new(),
            query_params: std::collections::HashMap::new(),
            body: bytes::Bytes::from(serde_json::to_vec(&body).unwrap()),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: Vec::new(),
            raw_path: "/".into(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        };
        let resp = futures_lite_block_on(svc.handle_deferring_snapshot(req))?;
        Ok(serde_json::from_slice(resp.body.expect_bytes()).unwrap())
    }

    fn futures_lite_block_on<F: std::future::Future>(f: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(f)
    }

    fn create_gateway_body(name: &str, scheme: &str) -> Value {
        json!({
            "name": name,
            "infrastructureRoleArn": "arn:aws:iam::000000000000:role/infra",
            "configuration": {"hostSharedAlb": {"scheme": scheme}},
        })
    }

    /// Cluster `c`, task definition `web` exposing port `web`, and a public
    /// gateway `gw` associated with `c`.
    fn setup(svc: &EcsService) -> String {
        call(svc, "CreateCluster", json!({"clusterName": "c"})).unwrap();
        call(
            svc,
            "RegisterTaskDefinition",
            json!({"family": "web", "containerDefinitions": [{
                "name": "app", "image": "nginx",
                "portMappings": [{"containerPort": 80, "name": "web"}],
            }]}),
        )
        .unwrap();
        let gw = call(
            svc,
            "CreateGateway",
            create_gateway_body("gw", "internet-facing"),
        )
        .unwrap();
        call(
            svc,
            "AssociateGatewayWithCluster",
            json!({"gateway": "gw", "cluster": "c"}),
        )
        .unwrap();
        gw["gatewayArn"].as_str().unwrap().to_string()
    }

    fn routes(svc: &EcsService, body: Value) -> Vec<Value> {
        call(svc, "ListGatewayRoutes", body).unwrap()["gatewayRoutes"]
            .as_array()
            .unwrap()
            .clone()
    }

    #[test]
    fn service_connectors_yield_routes_that_follow_the_service() {
        let svc = service();
        let gw_arn = setup(&svc);
        assert!(routes(&svc, json!({})).is_empty());
        let created = call(
            &svc,
            "CreateService",
            json!({"cluster": "c", "serviceName": "api", "taskDefinition": "web",
                   "desiredCount": 0, "gatewayConnectors": [{"portName": "web"}]}),
        )
        .unwrap();
        let service_arn = created["service"]["serviceArn"]
            .as_str()
            .unwrap()
            .to_string();
        let listed = routes(&svc, json!({"service": "api"}));
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0]["gatewayArn"], gw_arn);
        let route_arn = listed[0]["gatewayRouteArn"].as_str().unwrap().to_string();
        assert!(route_arn.contains(":gateway-route/gw/"));

        // The revision reports the route as a gateway-managed connection.
        let revs = call(
            &svc,
            "DescribeServiceRevisions",
            json!({"serviceRevisionArns": [format!("{service_arn}:1")]}),
        )
        .unwrap();
        let rev = &revs["serviceRevisions"][0];
        assert_eq!(rev["gatewayConnectors"][0]["portName"], "web");
        let conn = &rev["ecsManagedResources"]["gatewayConnections"][0];
        assert_eq!(conn["gatewayRouteArn"], route_arn.as_str());
        assert_eq!(conn["accessType"], "PUBLIC");
        assert!(conn["resources"]["hostSharedAlb"][0]["targetGroupArn"].is_string());

        // Gateway resources list the route on the managed ALB.
        let res = call(&svc, "DescribeGatewayResources", json!({"gateway": "gw"})).unwrap();
        assert_eq!(
            res["gatewayResources"]["resources"]["hostSharedAlb"][0]["gatewayRouteArns"][0],
            route_arn.as_str()
        );

        // Removing the connector removes the route and mints a revision.
        call(
            &svc,
            "UpdateService",
            json!({"cluster": "c", "service": "api", "gatewayConnectors": []}),
        )
        .unwrap();
        assert!(routes(&svc, json!({})).is_empty());
        let err = call(
            &svc,
            "DescribeGatewayRoute",
            json!({"gatewayRouteArn": route_arn}),
        )
        .unwrap_err();
        assert_eq!(err.code(), "ResourceNotFoundException");
        let revs = call(
            &svc,
            "DescribeServiceRevisions",
            json!({"serviceRevisionArns": [format!("{service_arn}:2")]}),
        )
        .unwrap();
        assert_eq!(revs["serviceRevisions"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn access_type_filters_select_gateways() {
        let svc = service();
        setup(&svc);
        call(
            &svc,
            "CreateGateway",
            create_gateway_body("private", "internal"),
        )
        .unwrap();
        call(
            &svc,
            "AssociateGatewayWithCluster",
            json!({"gateway": "private", "cluster": "c"}),
        )
        .unwrap();
        call(
            &svc,
            "CreateService",
            json!({"cluster": "c", "serviceName": "api", "taskDefinition": "web", "desiredCount": 0,
                   "gatewayConnectors": [{"portName": "web", "filters": {"accessTypes": ["PRIVATE"]}}]}),
        )
        .unwrap();
        let listed = routes(&svc, json!({}));
        assert_eq!(listed.len(), 1);
        assert!(listed[0]["gatewayArn"]
            .as_str()
            .unwrap()
            .contains(":gateway/private/"));
        assert_eq!(routes(&svc, json!({"gateway": "gw"})).len(), 0);
    }

    #[test]
    fn create_service_rejects_unknown_connector_port() {
        let svc = service();
        setup(&svc);
        let err = call(
            &svc,
            "CreateService",
            json!({"cluster": "c", "serviceName": "api", "taskDefinition": "web",
                   "desiredCount": 0, "gatewayConnectors": [{"portName": "nope"}]}),
        )
        .unwrap_err();
        assert_eq!(err.code(), "InvalidParameterException");
    }

    #[test]
    fn gateway_lifecycle_and_delete_guards() {
        let svc = service();
        let gw_arn = setup(&svc);
        // Associated: refuse delete.
        let err = call(&svc, "DeleteGateway", json!({"gateway": "gw"})).unwrap_err();
        assert_eq!(err.code(), "ClientException");
        // Re-associating an active association is refused.
        let err = call(
            &svc,
            "AssociateGatewayWithCluster",
            json!({"gateway": "gw", "cluster": "c"}),
        )
        .unwrap_err();
        assert_eq!(err.code(), "ClientException");
        // Unknown cluster.
        let err = call(
            &svc,
            "AssociateGatewayWithCluster",
            json!({"gateway": "gw", "cluster": "missing"}),
        )
        .unwrap_err();
        assert_eq!(err.code(), "ClusterNotFoundException");

        let out = call(
            &svc,
            "DisassociateGatewayFromCluster",
            json!({"gateway": "gw", "cluster": "c"}),
        )
        .unwrap();
        assert_eq!(out["status"], "INACTIVE");
        let err = call(
            &svc,
            "DisassociateGatewayFromCluster",
            json!({"gateway": "gw", "cluster": "c"}),
        )
        .unwrap_err();
        assert_eq!(err.code(), "ResourceNotFoundException");
        let inactive = call(
            &svc,
            "ListGatewayClusterAssociations",
            json!({"status": ["INACTIVE"]}),
        )
        .unwrap();
        assert_eq!(
            inactive["gatewayClusterAssociations"]
                .as_array()
                .unwrap()
                .len(),
            1
        );

        // Deletion protection blocks delete until it is turned off.
        call(
            &svc,
            "UpdateGateway",
            json!({"gateway": "gw", "deletionProtection": true}),
        )
        .unwrap();
        let err = call(&svc, "DeleteGateway", json!({"gateway": "gw"})).unwrap_err();
        assert_eq!(err.code(), "ClientException");
        let described = call(&svc, "DescribeGateway", json!({"gateway": "gw"})).unwrap();
        assert_eq!(described["gateway"]["status"], "UPDATE_COMPLETE");
        call(
            &svc,
            "UpdateGateway",
            json!({"gateway": "gw", "deletionProtection": false}),
        )
        .unwrap();
        let deleted = call(&svc, "DeleteGateway", json!({"gateway": "gw"})).unwrap();
        assert_eq!(deleted["status"], "DELETED");

        // Deleted: still describable, gone for mutations, hidden by default.
        let described = call(&svc, "DescribeGateway", json!({"gateway": gw_arn})).unwrap();
        assert_eq!(described["gateway"]["status"], "DELETED");
        let err = call(&svc, "UpdateGateway", json!({"gateway": "gw"})).unwrap_err();
        assert_eq!(err.code(), "GatewayNotFoundException");
        let listed = call(&svc, "ListGateways", json!({})).unwrap();
        assert!(listed["gateways"].as_array().unwrap().is_empty());
        let listed = call(&svc, "ListGateways", json!({"status": ["DELETED"]})).unwrap();
        assert_eq!(listed["gateways"].as_array().unwrap().len(), 1);

        // The name can be reused, with a new ARN.
        let again = call(&svc, "CreateGateway", create_gateway_body("gw", "internal")).unwrap();
        assert_ne!(again["gatewayArn"], gw_arn.as_str());
    }

    #[test]
    fn create_gateway_is_idempotent_per_client_token_and_rejects_duplicates() {
        let svc = service();
        let mut body = create_gateway_body("dup", "internet-facing");
        body["clientToken"] = json!("tok");
        let a = call(&svc, "CreateGateway", body.clone()).unwrap();
        let b = call(&svc, "CreateGateway", body).unwrap();
        assert_eq!(a["gatewayArn"], b["gatewayArn"]);
        let err = call(
            &svc,
            "CreateGateway",
            create_gateway_body("dup", "internet-facing"),
        )
        .unwrap_err();
        assert_eq!(err.code(), "ClientException");
    }

    #[test]
    fn gateways_are_taggable() {
        let svc = service();
        let created = call(&svc, "CreateGateway", create_gateway_body("t", "internal")).unwrap();
        let arn = created["gatewayArn"].as_str().unwrap();
        call(
            &svc,
            "TagResource",
            json!({"resourceArn": arn, "tags": [{"key": "k", "value": "v"}]}),
        )
        .unwrap();
        let tags = call(&svc, "ListTagsForResource", json!({"resourceArn": arn})).unwrap();
        assert_eq!(tags["tags"][0]["key"], "k");
        let described = call(&svc, "DescribeGateway", json!({"gateway": arn})).unwrap();
        assert_eq!(described["gateway"]["tags"][0]["value"], "v");
        assert_eq!(described["gateway"]["accessType"], "PRIVATE");
    }

    #[test]
    fn update_gateway_merges_network_configuration_and_moves_alb_security_group() {
        let svc = service();
        call(&svc, "CreateGateway", create_gateway_body("u", "internal")).unwrap();
        call(
            &svc,
            "UpdateGateway",
            json!({"gateway": "u", "configuration": {"hostSharedAlb": {
                "networkConfiguration": {"inboundSecurityGroup": "sg-0abc"},
            }}}),
        )
        .unwrap();
        let described = call(&svc, "DescribeGateway", json!({"gateway": "u"})).unwrap();
        let net = &described["gateway"]["configuration"]["hostSharedAlb"]["networkConfiguration"];
        assert_eq!(net["inboundSecurityGroup"], "sg-0abc");
        assert_eq!(net["ipAddressType"], "ipv4");
        let res = call(&svc, "DescribeGatewayResources", json!({"gateway": "u"})).unwrap();
        assert_eq!(
            res["gatewayResources"]["resources"]["hostSharedAlb"][0]["securityGroup"],
            "sg-0abc"
        );
    }

    #[test]
    fn role_arn_must_be_an_iam_role() {
        assert!(validate_role_arn("arn:aws:iam::123456789012:role/infra").is_ok());
        assert!(validate_role_arn("arn:aws:iam::123456789012:user/me").is_err());
        assert!(validate_role_arn("infra").is_err());
    }
}
