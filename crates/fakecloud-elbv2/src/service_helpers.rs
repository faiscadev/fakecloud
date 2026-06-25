use super::*;

pub(crate) fn xml_resp(action: &str, inner: String, request_id: &str) -> AwsResponse {
    let xml = query_response_xml(action, NS, &inner, request_id);
    AwsResponse::xml(StatusCode::OK, xml)
}

pub(crate) fn xml_metadata_only(action: &str, request_id: &str) -> AwsResponse {
    // The ELBv2 Query protocol always wraps the response in a `<{Action}Result>`
    // element (empty for actions with no output members). Omitting it makes the
    // AWS SDK fail to deserialize with "{Action}Result node not found" -- e.g.
    // DeleteLoadBalancer, which the Terraform provider's destroy relies on.
    let xml = query_response_xml(action, NS, "", request_id);
    AwsResponse::xml(StatusCode::OK, xml)
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

pub(crate) fn parse_member_list(req: &AwsRequest, prefix: &str) -> Vec<String> {
    let mut out = Vec::new();
    for n in 1..=100 {
        let key = format!("{prefix}.member.{n}");
        if let Some(v) = req.query_params.get(&key) {
            out.push(v.clone());
        } else {
            break;
        }
    }
    out
}

pub(crate) fn parse_subnet_mappings(req: &AwsRequest) -> Vec<SubnetMapping> {
    let mut out = Vec::new();
    for n in 1..=20 {
        let prefix = format!("SubnetMappings.member.{n}");
        let subnet_id = req.query_params.get(&format!("{prefix}.SubnetId")).cloned();
        let allocation_id = req
            .query_params
            .get(&format!("{prefix}.AllocationId"))
            .cloned();
        if subnet_id.is_none() && allocation_id.is_none() {
            break;
        }
        out.push(SubnetMapping {
            subnet_id,
            allocation_id,
        });
    }
    out
}

pub(crate) fn parse_tags(req: &AwsRequest) -> Vec<Tag> {
    let mut out = Vec::new();
    for n in 1..=50 {
        let key = req
            .query_params
            .get(&format!("Tags.member.{n}.Key"))
            .cloned();
        if let Some(k) = key {
            let v = req
                .query_params
                .get(&format!("Tags.member.{n}.Value"))
                .cloned()
                .unwrap_or_default();
            out.push(Tag { key: k, value: v });
        } else {
            break;
        }
    }
    out
}

pub(crate) fn validate_ip_address_type(ipt: &str) -> Result<(), AwsServiceError> {
    if !matches!(ipt, "ipv4" | "dualstack" | "dualstack-without-public-ipv4") {
        return Err(invalid_param(format!(
            "IpAddressType must be one of ipv4|dualstack|dualstack-without-public-ipv4, got '{ipt}'"
        )));
    }
    Ok(())
}

pub(crate) fn validate_lb_name(name: &str) -> Result<(), AwsServiceError> {
    if name.is_empty() || name.len() > 32 {
        return Err(invalid_param(
            "LoadBalancer name must be between 1 and 32 characters",
        ));
    }
    if !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '-') {
        return Err(invalid_param(
            "LoadBalancer name must contain only ASCII letters, digits, and hyphens",
        ));
    }
    if name.starts_with('-') || name.ends_with('-') || name.starts_with("internal-") {
        return Err(invalid_param(
            "LoadBalancer name cannot start or end with hyphen, and cannot start with 'internal-'",
        ));
    }
    Ok(())
}

/// Generic "bad input" error for ELBv2 control-plane ops. ELBv2 doesn't
/// model a top-level `ValidationException`; the closest declared shape on
/// most create/modify ops is `InvalidConfigurationRequestException`
/// (wire code `InvalidConfigurationRequest`). Strict-Smithy probes reject
/// the legacy awsQuery `ValidationError` because no op declares it.
pub(crate) fn invalid_param(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "InvalidConfigurationRequest",
        msg.into(),
    )
}

/// Reject when an integer query param falls outside `[min, max]`. Mirrors
/// Smithy `@range` constraints — used by ops whose negative probes flip
/// boundary fields and expect the same rejection AWS does up front.
pub(crate) fn validate_range_i32(
    req: &AwsRequest,
    field: &str,
    min: i32,
    max: i32,
) -> Result<(), AwsServiceError> {
    if let Some(raw) = req.query_params.get(field) {
        match raw.parse::<i64>() {
            Ok(v) if v >= min as i64 && v <= max as i64 => Ok(()),
            _ => Err(invalid_param(format!(
                "{field} must be between {min} and {max}, got '{raw}'"
            ))),
        }
    } else {
        Ok(())
    }
}

/// Variant of `invalid_param` for target register/deregister paths. Both
/// ops declare `InvalidTargetException` (wire `InvalidTarget`); use that
/// when the supplied target list is malformed.
pub(crate) fn invalid_target(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "InvalidTarget", msg.into())
}

/// Listener protocols accepted by ELBv2: HTTP/HTTPS for ALB, TCP/UDP/
/// TCP_UDP/TLS for NLB, GENEVE for GWLB. Anything else gets rejected
/// up front rather than silently stored — real ELBv2 returns
/// ValidationError when callers pass nonsense like "FOO".
pub(crate) fn validate_listener_protocol(protocol: &str) -> Result<(), AwsServiceError> {
    const VALID: &[&str] = &["HTTP", "HTTPS", "TCP", "UDP", "TCP_UDP", "TLS", "GENEVE"];
    if !VALID.contains(&protocol) {
        return Err(invalid_param(format!(
            "Protocol '{protocol}' is not valid. Valid values: {}",
            VALID.join(", ")
        )));
    }
    Ok(())
}

/// Listener port must be in the TCP/UDP range 1-65535. Real ELBv2
/// rejects 0 and anything outside u16 with a ValidationError.
pub(crate) fn validate_listener_port(port: i32) -> Result<(), AwsServiceError> {
    if !(1..=65535).contains(&port) {
        return Err(invalid_param(format!(
            "Port '{port}' must be between 1 and 65535"
        )));
    }
    Ok(())
}

/// Each load balancer type accepts only a subset of listener
/// protocols, and GWLB pins the port to GENEVE's standard `6081`.
/// AWS rejects mismatches up front - e.g. an ALB cannot listen on
/// raw TCP, an NLB cannot listen on HTTP, and a GWLB cannot listen
/// on anything except GENEVE/6081.
///
/// `protocol` and `port` are optional: callers should still call
/// [`validate_listener_protocol`] / [`validate_listener_port`] for
/// the standalone shape checks; this helper only enforces the
/// per-type matrix.
pub(crate) fn validate_listener_protocol_port_for_lb_type(
    lb_type: &str,
    protocol: Option<&str>,
    port: Option<i32>,
) -> Result<(), AwsServiceError> {
    let allowed: &[&str] = match lb_type {
        "application" => &["HTTP", "HTTPS"],
        "network" => &["TCP", "UDP", "TCP_UDP", "TLS"],
        "gateway" => &["GENEVE"],
        // Unknown LB type - fall back to permissive; the caller has
        // already gated on the enum.
        _ => return Ok(()),
    };
    if let Some(p) = protocol {
        if !allowed.contains(&p) {
            return Err(invalid_param(format!(
                "Protocol '{p}' is not valid for {lb_type} load balancers. Valid values: {}",
                allowed.join(", ")
            )));
        }
    }
    if lb_type == "gateway" {
        if let Some(port) = port {
            if port != 6081 {
                return Err(invalid_param(format!(
                    "Port '{port}' is not valid for gateway load balancers. GENEVE listeners must use port 6081"
                )));
            }
        }
    }
    Ok(())
}

/// `ipv6.enable_prefix_for_source_nat` is the only LB attribute
/// whose value is restricted to a tight set of bool-ish strings.
/// AWS accepts `true`/`false` and the `on`/`off` aliases used by
/// the corresponding `EnablePrefixForIpv6SourceNat` create-time
/// enum. Anything else returns a `ValidationError` rather than
/// being silently round-tripped.
pub(crate) fn validate_ipv6_source_nat_value(value: &str) -> Result<(), AwsServiceError> {
    if !matches!(value, "true" | "false" | "on" | "off") {
        return Err(invalid_param(format!(
            "ipv6.enable_prefix_for_source_nat must be one of true|false|on|off, got '{value}'"
        )));
    }
    Ok(())
}

pub(crate) fn lb_not_found(arn: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "LoadBalancerNotFound",
        format!("Load balancer '{arn}' not found"),
    )
}

pub(crate) fn build_lb_arn(
    region: &str,
    account_id: &str,
    lb_type: &str,
    name: &str,
    suffix: &str,
) -> String {
    let prefix = match lb_type {
        "network" => "net",
        "gateway" => "gwy",
        _ => "app",
    };
    format!(
        "arn:aws:elasticloadbalancing:{region}:{account_id}:loadbalancer/{prefix}/{name}/{suffix}"
    )
}

pub(crate) fn build_dns_name(
    name: &str,
    _lb_type: &str,
    scheme: &str,
    region: &str,
    suffix: &str,
) -> String {
    let scheme_part = if scheme == "internal" {
        "internal-"
    } else {
        ""
    };
    format!("{scheme_part}{name}-{suffix}.{region}.elb.amazonaws.com")
}

pub(crate) fn render_lb_xml(lb: &LoadBalancer) -> String {
    let azs_xml = lb
        .availability_zones
        .iter()
        .map(render_az_xml)
        .collect::<String>();
    let sg_xml = lb
        .security_groups
        .iter()
        .map(|s| format!("<member>{}</member>", xml_escape(s)))
        .collect::<String>();
    let state_reason = lb
        .state_reason
        .as_deref()
        .map(|r| format!("<Reason>{}</Reason>", xml_escape(r)))
        .unwrap_or_default();
    let coip = lb
        .customer_owned_ipv4_pool
        .as_deref()
        .map(|s| {
            format!(
                "<CustomerOwnedIpv4Pool>{}</CustomerOwnedIpv4Pool>",
                xml_escape(s)
            )
        })
        .unwrap_or_default();
    let enforce = lb
        .enforce_security_group_inbound_rules_on_private_link_traffic
        .as_deref()
        .map(|s| {
            format!(
                "<EnforceSecurityGroupInboundRulesOnPrivateLinkTraffic>{}</EnforceSecurityGroupInboundRulesOnPrivateLinkTraffic>",
                xml_escape(s)
            )
        })
        .unwrap_or_default();
    let nat6 = lb
        .enable_prefix_for_ipv6_source_nat
        .as_deref()
        .map(|s| {
            format!(
                "<EnablePrefixForIpv6SourceNat>{}</EnablePrefixForIpv6SourceNat>",
                xml_escape(s)
            )
        })
        .unwrap_or_default();
    let ipam = lb
        .ipv4_ipam_pool_id
        .as_deref()
        .map(|s| {
            format!(
                "<IpamPools><Ipv4IpamPoolId>{}</Ipv4IpamPoolId></IpamPools>",
                xml_escape(s)
            )
        })
        .unwrap_or_default();
    let min_cap = lb
        .minimum_capacity_units
        .map(|v| {
            format!(
                "<MinimumLoadBalancerCapacity><CapacityUnits>{v}</CapacityUnits></MinimumLoadBalancerCapacity>"
            )
        })
        .unwrap_or_default();
    format!(
        "<LoadBalancerArn>{arn}</LoadBalancerArn>\
         <DNSName>{dns}</DNSName>\
         <CanonicalHostedZoneId>{zone}</CanonicalHostedZoneId>\
         <CreatedTime>{created}</CreatedTime>\
         <LoadBalancerName>{name}</LoadBalancerName>\
         <Scheme>{scheme}</Scheme>\
         <VpcId>{vpc}</VpcId>\
         <State><Code>{code}</Code>{state_reason}</State>\
         <Type>{ty}</Type>\
         <AvailabilityZones>{azs}</AvailabilityZones>\
         <SecurityGroups>{sgs}</SecurityGroups>\
         <IpAddressType>{ipt}</IpAddressType>{coip}{enforce}{nat6}{ipam}{min_cap}",
        arn = xml_escape(&lb.arn),
        dns = xml_escape(&lb.dns_name),
        zone = xml_escape(&lb.canonical_hosted_zone_id),
        created = lb
            .created_time
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        name = xml_escape(&lb.name),
        scheme = xml_escape(&lb.scheme),
        vpc = xml_escape(&lb.vpc_id),
        code = xml_escape(&lb.state_code),
        ty = xml_escape(&lb.lb_type),
        azs = azs_xml,
        sgs = sg_xml,
        ipt = xml_escape(&lb.ip_address_type),
    )
}

pub(crate) fn render_az_xml(az: &AvailabilityZone) -> String {
    let addresses = az
        .load_balancer_addresses
        .iter()
        .map(render_address_xml)
        .collect::<String>();
    let prefixes = az
        .source_nat_ipv6_prefixes
        .iter()
        .map(|p| format!("<member>{}</member>", xml_escape(p)))
        .collect::<String>();
    let outpost = az
        .outpost_id
        .as_deref()
        .map(|o| format!("<OutpostId>{}</OutpostId>", xml_escape(o)))
        .unwrap_or_default();
    format!(
        "<member><ZoneName>{zone}</ZoneName><SubnetId>{subnet}</SubnetId>{outpost}<LoadBalancerAddresses>{addrs}</LoadBalancerAddresses><SourceNatIpv6Prefixes>{prefixes}</SourceNatIpv6Prefixes></member>",
        zone = xml_escape(&az.zone_name),
        subnet = xml_escape(&az.subnet_id),
        addrs = addresses,
    )
}

pub(crate) fn render_address_xml(addr: &LoadBalancerAddress) -> String {
    let mut s = String::from("<member>");
    if let Some(ip) = &addr.ip_address {
        s.push_str(&format!("<IpAddress>{}</IpAddress>", xml_escape(ip)));
    }
    if let Some(a) = &addr.allocation_id {
        s.push_str(&format!("<AllocationId>{}</AllocationId>", xml_escape(a)));
    }
    if let Some(p) = &addr.private_ipv4_address {
        s.push_str(&format!(
            "<PrivateIPv4Address>{}</PrivateIPv4Address>",
            xml_escape(p)
        ));
    }
    if let Some(p) = &addr.ipv6_address {
        s.push_str(&format!("<IPv6Address>{}</IPv6Address>", xml_escape(p)));
    }
    if let Some(p) = &addr.ipv4_prefix {
        s.push_str(&format!("<IPv4Prefix>{}</IPv4Prefix>", xml_escape(p)));
    }
    if let Some(p) = &addr.ipv6_prefix {
        s.push_str(&format!("<IPv6Prefix>{}</IPv6Prefix>", xml_escape(p)));
    }
    s.push_str("</member>");
    s
}

pub(crate) fn az_for_subnet(region: &str, subnet_id: &str) -> String {
    let suffix = match subnet_id
        .chars()
        .fold(0u32, |a, c| a.wrapping_add(c as u32))
        % 6
    {
        0 => 'a',
        1 => 'b',
        2 => 'c',
        3 => 'd',
        4 => 'e',
        _ => 'f',
    };
    format!("{region}{suffix}")
}

pub(crate) fn alphanumeric_id(len: usize) -> String {
    let raw = uuid::Uuid::new_v4().simple().to_string();
    raw.chars().take(len).collect()
}

pub(crate) fn trust_store_not_found(arn: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "TrustStoreNotFound",
        format!("Trust store '{arn}' not found"),
    )
}

pub(crate) fn default_lb_attributes(lb_type: &str) -> BTreeMap<String, String> {
    let mut m = BTreeMap::new();
    m.insert("idle_timeout.timeout_seconds".to_string(), "60".to_string());
    m.insert(
        "deletion_protection.enabled".to_string(),
        "false".to_string(),
    );
    m.insert(
        "load_balancing.cross_zone.enabled".to_string(),
        "true".to_string(),
    );
    m.insert("access_logs.s3.enabled".to_string(), "false".to_string());
    m.insert("access_logs.s3.bucket".to_string(), String::new());
    m.insert("access_logs.s3.prefix".to_string(), String::new());
    if lb_type == "application" {
        m.insert("routing.http2.enabled".to_string(), "true".to_string());
        m.insert(
            "routing.http.drop_invalid_header_fields.enabled".to_string(),
            "false".to_string(),
        );
        m.insert(
            "routing.http.preserve_host_header.enabled".to_string(),
            "false".to_string(),
        );
    }
    m
}

pub(crate) fn merge_lb_attributes(
    lb_type: &str,
    stored: &BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    let mut m = default_lb_attributes(lb_type);
    for (k, v) in stored {
        m.insert(k.clone(), v.clone());
    }
    m
}

pub(crate) fn capacity_reservation_xml(lb: &crate::state::LoadBalancer) -> String {
    let units = lb
        .minimum_capacity_units
        .map(|v| {
            format!(
                "<MinimumLoadBalancerCapacity><CapacityUnits>{v}</CapacityUnits></MinimumLoadBalancerCapacity>"
            )
        })
        .unwrap_or_default();
    format!("<LastModifiedTime>{}</LastModifiedTime>{units}<DecreaseRequestsRemaining>0</DecreaseRequestsRemaining>", lb.created_time.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
}

pub(crate) fn render_trust_store_xml(ts: &crate::state::TrustStore) -> String {
    format!(
        "<TrustStoreArn>{arn}</TrustStoreArn>\
         <Name>{name}</Name>\
         <Status>{status}</Status>\
         <NumberOfCaCertificates>{num}</NumberOfCaCertificates>\
         <TotalRevokedEntries>{rev}</TotalRevokedEntries>",
        arn = xml_escape(&ts.arn),
        name = xml_escape(&ts.name),
        status = xml_escape(&ts.status),
        num = ts.number_of_ca_certificates,
        rev = ts.total_revoked_entries,
    )
}

pub(crate) fn listener_not_found(arn: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ListenerNotFound",
        format!("Listener '{arn}' not found"),
    )
}

pub(crate) fn rule_not_found(arn: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "RuleNotFound",
        format!("Rule '{arn}' not found"),
    )
}

pub(crate) fn parse_certificates(req: &AwsRequest) -> Vec<crate::state::Certificate> {
    let mut out = Vec::new();
    for n in 1..=25 {
        let arn = req
            .query_params
            .get(&format!("Certificates.member.{n}.CertificateArn"))
            .cloned();
        if let Some(arn) = arn {
            let is_default = req
                .query_params
                .get(&format!("Certificates.member.{n}.IsDefault"))
                .map(|s| s == "true")
                .unwrap_or(false);
            out.push(crate::state::Certificate {
                certificate_arn: arn,
                is_default,
            });
        } else {
            break;
        }
    }
    out
}

pub(crate) fn parse_actions(req: &AwsRequest, prefix: &str) -> Vec<crate::state::Action> {
    let mut out = Vec::new();
    for n in 1..=10 {
        let p = format!("{prefix}.member.{n}");
        let action_type = req.query_params.get(&format!("{p}.Type")).cloned();
        if let Some(t) = action_type {
            let target_group_arn = req
                .query_params
                .get(&format!("{p}.TargetGroupArn"))
                .cloned();
            let order = req
                .query_params
                .get(&format!("{p}.Order"))
                .and_then(|s| s.parse().ok());
            let redirect = parse_redirect(req, &p);
            let fixed_response = parse_fixed_response(req, &p);
            let forward = parse_forward(req, &p);
            out.push(crate::state::Action {
                action_type: t,
                target_group_arn,
                order,
                redirect,
                fixed_response,
                forward,
                authenticate_cognito: None,
                authenticate_oidc: None,
            });
        } else {
            break;
        }
    }
    out
}

pub(crate) fn parse_redirect(
    req: &AwsRequest,
    prefix: &str,
) -> Option<crate::state::RedirectConfig> {
    let status = req
        .query_params
        .get(&format!("{prefix}.RedirectConfig.StatusCode"))
        .cloned()?;
    Some(crate::state::RedirectConfig {
        protocol: req
            .query_params
            .get(&format!("{prefix}.RedirectConfig.Protocol"))
            .cloned(),
        port: req
            .query_params
            .get(&format!("{prefix}.RedirectConfig.Port"))
            .cloned(),
        host: req
            .query_params
            .get(&format!("{prefix}.RedirectConfig.Host"))
            .cloned(),
        path: req
            .query_params
            .get(&format!("{prefix}.RedirectConfig.Path"))
            .cloned(),
        query: req
            .query_params
            .get(&format!("{prefix}.RedirectConfig.Query"))
            .cloned(),
        status_code: status,
    })
}

pub(crate) fn parse_fixed_response(
    req: &AwsRequest,
    prefix: &str,
) -> Option<crate::state::FixedResponseConfig> {
    let status = req
        .query_params
        .get(&format!("{prefix}.FixedResponseConfig.StatusCode"))
        .cloned()?;
    Some(crate::state::FixedResponseConfig {
        message_body: req
            .query_params
            .get(&format!("{prefix}.FixedResponseConfig.MessageBody"))
            .cloned(),
        status_code: status,
        content_type: req
            .query_params
            .get(&format!("{prefix}.FixedResponseConfig.ContentType"))
            .cloned(),
    })
}

pub(crate) fn parse_forward(req: &AwsRequest, prefix: &str) -> Option<crate::state::ForwardConfig> {
    let mut target_groups = Vec::new();
    for n in 1..=5 {
        let key = format!("{prefix}.ForwardConfig.TargetGroups.member.{n}.TargetGroupArn");
        if let Some(arn) = req.query_params.get(&key) {
            let weight = req
                .query_params
                .get(&format!(
                    "{prefix}.ForwardConfig.TargetGroups.member.{n}.Weight"
                ))
                .and_then(|s| s.parse().ok());
            target_groups.push(crate::state::TargetGroupTuple {
                target_group_arn: arn.clone(),
                weight,
            });
        } else {
            break;
        }
    }
    if target_groups.is_empty() {
        return None;
    }
    Some(crate::state::ForwardConfig {
        target_groups,
        stickiness: None,
    })
}

pub(crate) fn parse_mutual_authentication(
    req: &AwsRequest,
) -> Option<crate::state::MutualAuthentication> {
    let mode = req.query_params.get("MutualAuthentication.Mode").cloned()?;
    Some(crate::state::MutualAuthentication {
        mode: Some(mode),
        trust_store_arn: req
            .query_params
            .get("MutualAuthentication.TrustStoreArn")
            .cloned(),
        ignore_client_certificate_expiry: req
            .query_params
            .get("MutualAuthentication.IgnoreClientCertificateExpiry")
            .map(|s| s == "true"),
        trust_store_association_status: None,
        advertise_trust_store_ca_names: req
            .query_params
            .get("MutualAuthentication.AdvertiseTrustStoreCaNames")
            .cloned(),
    })
}

pub(crate) fn parse_conditions(req: &AwsRequest) -> Vec<crate::state::RuleCondition> {
    let mut out = Vec::new();
    for n in 1..=10 {
        let p = format!("Conditions.member.{n}");
        let field = req.query_params.get(&format!("{p}.Field")).cloned();
        if let Some(field) = field {
            out.push(crate::state::RuleCondition {
                field,
                values: parse_member_list(req, &format!("{p}.Values")),
                host_header_values: parse_member_list(req, &format!("{p}.HostHeaderConfig.Values")),
                path_pattern_values: parse_member_list(
                    req,
                    &format!("{p}.PathPatternConfig.Values"),
                ),
                http_header_name: req
                    .query_params
                    .get(&format!("{p}.HttpHeaderConfig.HttpHeaderName"))
                    .cloned(),
                http_header_values: parse_member_list(req, &format!("{p}.HttpHeaderConfig.Values")),
                query_string_values: Vec::new(),
                http_request_method_values: parse_member_list(
                    req,
                    &format!("{p}.HttpRequestMethodConfig.Values"),
                ),
                source_ip_values: parse_member_list(req, &format!("{p}.SourceIpConfig.Values")),
            });
        } else {
            break;
        }
    }
    out
}

pub(crate) fn render_listener_xml(l: &crate::state::Listener) -> String {
    let port = l
        .port
        .map(|p| format!("<Port>{p}</Port>"))
        .unwrap_or_default();
    let proto = l
        .protocol
        .as_deref()
        .map(|p| format!("<Protocol>{}</Protocol>", xml_escape(p)))
        .unwrap_or_default();
    let ssl = l
        .ssl_policy
        .as_deref()
        .map(|p| format!("<SslPolicy>{}</SslPolicy>", xml_escape(p)))
        .unwrap_or_default();
    let cert_xml = l
        .certificates
        .iter()
        .map(render_certificate_xml)
        .collect::<String>();
    let actions_xml = l
        .default_actions
        .iter()
        .map(render_action_xml)
        .collect::<String>();
    let alpn_xml = l
        .alpn_policy
        .iter()
        .map(|p| format!("<member>{}</member>", xml_escape(p)))
        .collect::<String>();
    let ma_xml = l
        .mutual_authentication
        .as_ref()
        .map(render_mutual_auth_xml)
        .unwrap_or_default();
    format!(
        "<ListenerArn>{arn}</ListenerArn>\
         <LoadBalancerArn>{lb}</LoadBalancerArn>\
         {port}{proto}\
         <Certificates>{cert_xml}</Certificates>\
         {ssl}\
         <DefaultActions>{actions_xml}</DefaultActions>\
         <AlpnPolicy>{alpn_xml}</AlpnPolicy>\
         {ma_xml}",
        arn = xml_escape(&l.arn),
        lb = xml_escape(&l.load_balancer_arn),
    )
}

pub(crate) fn render_certificate_xml(c: &crate::state::Certificate) -> String {
    format!(
        "<member><CertificateArn>{}</CertificateArn><IsDefault>{}</IsDefault></member>",
        xml_escape(&c.certificate_arn),
        c.is_default
    )
}

pub(crate) fn render_action_xml(a: &crate::state::Action) -> String {
    let order = a
        .order
        .map(|o| format!("<Order>{o}</Order>"))
        .unwrap_or_default();
    let tg = a
        .target_group_arn
        .as_deref()
        .map(|t| format!("<TargetGroupArn>{}</TargetGroupArn>", xml_escape(t)))
        .unwrap_or_default();
    let redirect = a
        .redirect
        .as_ref()
        .map(|r| {
            let mut s = String::from("<RedirectConfig>");
            if let Some(p) = &r.protocol {
                s.push_str(&format!("<Protocol>{}</Protocol>", xml_escape(p)));
            }
            if let Some(p) = &r.port {
                s.push_str(&format!("<Port>{}</Port>", xml_escape(p)));
            }
            if let Some(p) = &r.host {
                s.push_str(&format!("<Host>{}</Host>", xml_escape(p)));
            }
            if let Some(p) = &r.path {
                s.push_str(&format!("<Path>{}</Path>", xml_escape(p)));
            }
            if let Some(p) = &r.query {
                s.push_str(&format!("<Query>{}</Query>", xml_escape(p)));
            }
            s.push_str(&format!(
                "<StatusCode>{}</StatusCode>",
                xml_escape(&r.status_code)
            ));
            s.push_str("</RedirectConfig>");
            s
        })
        .unwrap_or_default();
    let fixed = a
        .fixed_response
        .as_ref()
        .map(|f| {
            let mb = f
                .message_body
                .as_deref()
                .map(|m| format!("<MessageBody>{}</MessageBody>", xml_escape(m)))
                .unwrap_or_default();
            let ct = f
                .content_type
                .as_deref()
                .map(|c| format!("<ContentType>{}</ContentType>", xml_escape(c)))
                .unwrap_or_default();
            format!(
                "<FixedResponseConfig>{mb}<StatusCode>{}</StatusCode>{ct}</FixedResponseConfig>",
                xml_escape(&f.status_code)
            )
        })
        .unwrap_or_default();
    let forward = a
        .forward
        .as_ref()
        .map(|f| {
            let groups = f
                .target_groups
                .iter()
                .map(|g| {
                    let weight = g
                        .weight
                        .map(|w| format!("<Weight>{w}</Weight>"))
                        .unwrap_or_default();
                    format!(
                        "<member><TargetGroupArn>{}</TargetGroupArn>{weight}</member>",
                        xml_escape(&g.target_group_arn)
                    )
                })
                .collect::<String>();
            format!("<ForwardConfig><TargetGroups>{groups}</TargetGroups></ForwardConfig>")
        })
        .unwrap_or_default();
    format!(
        "<member><Type>{ty}</Type>{tg}{order}{redirect}{fixed}{forward}</member>",
        ty = xml_escape(&a.action_type),
    )
}

pub(crate) fn render_mutual_auth_xml(ma: &crate::state::MutualAuthentication) -> String {
    let mode = ma
        .mode
        .as_deref()
        .map(|m| format!("<Mode>{}</Mode>", xml_escape(m)))
        .unwrap_or_default();
    let ts = ma
        .trust_store_arn
        .as_deref()
        .map(|t| format!("<TrustStoreArn>{}</TrustStoreArn>", xml_escape(t)))
        .unwrap_or_default();
    let ig = ma
        .ignore_client_certificate_expiry
        .map(|b| format!("<IgnoreClientCertificateExpiry>{b}</IgnoreClientCertificateExpiry>"))
        .unwrap_or_default();
    let adv = ma
        .advertise_trust_store_ca_names
        .as_deref()
        .map(|a| {
            format!(
                "<AdvertiseTrustStoreCaNames>{}</AdvertiseTrustStoreCaNames>",
                xml_escape(a)
            )
        })
        .unwrap_or_default();
    format!("<MutualAuthentication>{mode}{ts}{ig}{adv}</MutualAuthentication>")
}

pub(crate) fn render_rule_xml(r: &crate::state::Rule) -> String {
    let conditions_xml = r
        .conditions
        .iter()
        .map(|c| {
            let values = c
                .values
                .iter()
                .map(|v| format!("<member>{}</member>", xml_escape(v)))
                .collect::<String>();
            format!(
                "<member><Field>{}</Field><Values>{values}</Values></member>",
                xml_escape(&c.field)
            )
        })
        .collect::<String>();
    let actions_xml = r.actions.iter().map(render_action_xml).collect::<String>();
    format!(
        "<RuleArn>{arn}</RuleArn>\
         <Priority>{priority}</Priority>\
         <Conditions>{conditions_xml}</Conditions>\
         <Actions>{actions_xml}</Actions>\
         <IsDefault>{is_default}</IsDefault>",
        arn = xml_escape(&r.arn),
        priority = xml_escape(&r.priority),
        is_default = r.is_default,
    )
}

pub(crate) fn validate_tg_name(name: &str) -> Result<(), AwsServiceError> {
    if name.is_empty() || name.len() > 32 {
        return Err(invalid_param(
            "TargetGroup name must be between 1 and 32 characters",
        ));
    }
    if !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '-') {
        return Err(invalid_param(
            "TargetGroup name must contain only ASCII letters, digits, and hyphens",
        ));
    }
    if name.starts_with('-') || name.ends_with('-') {
        return Err(invalid_param(
            "TargetGroup name cannot start or end with a hyphen",
        ));
    }
    Ok(())
}

pub(crate) fn target_group_not_found(arn: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "TargetGroupNotFound",
        format!("Target group '{arn}' not found"),
    )
}

pub(crate) fn render_target_group_xml(tg: &crate::state::TargetGroup) -> String {
    let lb_xml = tg
        .load_balancer_arns
        .iter()
        .map(|a| format!("<member>{}</member>", xml_escape(a)))
        .collect::<String>();
    let port = tg
        .port
        .map(|p| format!("<Port>{p}</Port>"))
        .unwrap_or_default();
    let proto = tg
        .protocol
        .as_deref()
        .map(|p| format!("<Protocol>{}</Protocol>", xml_escape(p)))
        .unwrap_or_default();
    let vpc = tg
        .vpc_id
        .as_deref()
        .map(|v| format!("<VpcId>{}</VpcId>", xml_escape(v)))
        .unwrap_or_default();
    let proto_ver = tg
        .protocol_version
        .as_deref()
        .map(|p| format!("<ProtocolVersion>{}</ProtocolVersion>", xml_escape(p)))
        .unwrap_or_default();
    let hc_proto = tg
        .health_check_protocol
        .as_deref()
        .map(|p| {
            format!(
                "<HealthCheckProtocol>{}</HealthCheckProtocol>",
                xml_escape(p)
            )
        })
        .unwrap_or_default();
    let hc_port = tg
        .health_check_port
        .as_deref()
        .map(|p| format!("<HealthCheckPort>{}</HealthCheckPort>", xml_escape(p)))
        .unwrap_or_default();
    let hc_path = tg
        .health_check_path
        .as_deref()
        .map(|p| format!("<HealthCheckPath>{}</HealthCheckPath>", xml_escape(p)))
        .unwrap_or_default();
    let matcher = match (
        tg.matcher_http_code.as_deref(),
        tg.matcher_grpc_code.as_deref(),
    ) {
        (Some(http), Some(grpc)) => format!(
            "<Matcher><HttpCode>{}</HttpCode><GrpcCode>{}</GrpcCode></Matcher>",
            xml_escape(http),
            xml_escape(grpc)
        ),
        (Some(http), None) => {
            format!(
                "<Matcher><HttpCode>{}</HttpCode></Matcher>",
                xml_escape(http)
            )
        }
        (None, Some(grpc)) => {
            format!(
                "<Matcher><GrpcCode>{}</GrpcCode></Matcher>",
                xml_escape(grpc)
            )
        }
        (None, None) => String::new(),
    };
    format!(
        "<TargetGroupArn>{arn}</TargetGroupArn>\
         <TargetGroupName>{name}</TargetGroupName>\
         {proto}{port}{vpc}{proto_ver}\
         <TargetType>{tt}</TargetType>\
         <IpAddressType>{ipt}</IpAddressType>\
         <HealthCheckEnabled>{hce}</HealthCheckEnabled>\
         {hc_proto}{hc_port}{hc_path}\
         <HealthCheckIntervalSeconds>{hci}</HealthCheckIntervalSeconds>\
         <HealthCheckTimeoutSeconds>{hct}</HealthCheckTimeoutSeconds>\
         <HealthyThresholdCount>{ht}</HealthyThresholdCount>\
         <UnhealthyThresholdCount>{uht}</UnhealthyThresholdCount>\
         {matcher}\
         <LoadBalancerArns>{lb_xml}</LoadBalancerArns>",
        arn = xml_escape(&tg.arn),
        name = xml_escape(&tg.name),
        tt = xml_escape(&tg.target_type),
        ipt = xml_escape(&tg.ip_address_type),
        hce = tg.health_check_enabled,
        hci = tg.health_check_interval_seconds,
        hct = tg.health_check_timeout_seconds,
        ht = tg.healthy_threshold_count,
        uht = tg.unhealthy_threshold_count,
    )
}

pub(crate) fn parse_target_descriptions(req: &AwsRequest) -> Vec<crate::state::TargetDescription> {
    let mut out = Vec::new();
    for n in 1..=100 {
        let id = req
            .query_params
            .get(&format!("Targets.member.{n}.Id"))
            .cloned();
        if let Some(id) = id {
            let port = req
                .query_params
                .get(&format!("Targets.member.{n}.Port"))
                .and_then(|s| s.parse().ok());
            let az = req
                .query_params
                .get(&format!("Targets.member.{n}.AvailabilityZone"))
                .cloned();
            out.push(crate::state::TargetDescription {
                id,
                port,
                availability_zone: az,
                health: if crate::prober::probes_enabled() {
                    crate::state::TargetHealth::initial()
                } else {
                    crate::state::TargetHealth::default()
                },
                consecutive_success: 0,
                consecutive_failure: 0,
                last_probe_at: None,
            });
        } else {
            break;
        }
    }
    out
}

pub(crate) fn parse_attributes(req: &AwsRequest) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for n in 1..=50 {
        let key = req
            .query_params
            .get(&format!("Attributes.member.{n}.Key"))
            .cloned();
        if let Some(k) = key {
            let v = req
                .query_params
                .get(&format!("Attributes.member.{n}.Value"))
                .cloned()
                .unwrap_or_default();
            out.push((k, v));
        } else {
            break;
        }
    }
    out
}

pub(crate) fn render_attributes_xml(attrs: &BTreeMap<String, String>) -> String {
    let entries = attrs
        .iter()
        .map(|(k, v)| {
            format!(
                "<member><Key>{}</Key><Value>{}</Value></member>",
                xml_escape(k),
                xml_escape(v)
            )
        })
        .collect::<String>();
    format!("<Attributes>{entries}</Attributes>")
}

pub(crate) fn default_target_group_attributes() -> BTreeMap<String, String> {
    let mut m = BTreeMap::new();
    m.insert(
        "deregistration_delay.timeout_seconds".to_string(),
        "300".to_string(),
    );
    m.insert("stickiness.enabled".to_string(), "false".to_string());
    m.insert("stickiness.type".to_string(), "lb_cookie".to_string());
    m.insert(
        "stickiness.lb_cookie.duration_seconds".to_string(),
        "86400".to_string(),
    );
    m.insert(
        "load_balancing.algorithm.type".to_string(),
        "round_robin".to_string(),
    );
    m.insert("slow_start.duration_seconds".to_string(), "0".to_string());
    m
}

pub(crate) fn apply_tags(
    st: &mut crate::state::Elbv2State,
    arn: &str,
    new_tags: &[Tag],
) -> Result<(), AwsServiceError> {
    let target = resolve_taggable(st, arn)?;
    for nt in new_tags {
        target.retain(|t| t.key != nt.key);
        target.push(nt.clone());
    }
    Ok(())
}

pub(crate) fn remove_tag_keys(
    st: &mut crate::state::Elbv2State,
    arn: &str,
    keys: &[String],
) -> Result<(), AwsServiceError> {
    let target = resolve_taggable(st, arn)?;
    target.retain(|t| !keys.contains(&t.key));
    Ok(())
}

pub(crate) fn lookup_tags<'a>(st: &'a crate::state::Elbv2State, arn: &str) -> &'a [Tag] {
    if let Some(lb) = st.load_balancers.get(arn) {
        return &lb.tags;
    }
    if let Some(tg) = st.target_groups.get(arn) {
        return &tg.tags;
    }
    if let Some(l) = st.listeners.get(arn) {
        return &l.tags;
    }
    if let Some(r) = st.rules.get(arn) {
        return &r.tags;
    }
    if let Some(ts) = st.trust_stores.get(arn) {
        return &ts.tags;
    }
    &[]
}

pub(crate) fn resolve_taggable<'a>(
    st: &'a mut crate::state::Elbv2State,
    arn: &str,
) -> Result<&'a mut Vec<Tag>, AwsServiceError> {
    if st.load_balancers.contains_key(arn) {
        return Ok(&mut st.load_balancers.get_mut(arn).unwrap().tags);
    }
    if st.target_groups.contains_key(arn) {
        return Ok(&mut st.target_groups.get_mut(arn).unwrap().tags);
    }
    if st.listeners.contains_key(arn) {
        return Ok(&mut st.listeners.get_mut(arn).unwrap().tags);
    }
    if st.rules.contains_key(arn) {
        return Ok(&mut st.rules.get_mut(arn).unwrap().tags);
    }
    if st.trust_stores.contains_key(arn) {
        return Ok(&mut st.trust_stores.get_mut(arn).unwrap().tags);
    }
    Err(taggable_not_found(arn))
}

/// Map an unresolved tag-target ARN to the specific *NotFound error declared
/// on the tag operations (AddTags / RemoveTags / DescribeTags all share the
/// same Listener/LoadBalancer/Rule/TargetGroup/TrustStore NotFound set).
/// awsQuery probes reject a generic "ResourceNotFound" because it isn't in
/// any op's Smithy errors list; the closest declared shape comes from the
/// ARN resource type.
pub(crate) fn taggable_not_found(arn: &str) -> AwsServiceError {
    let (code, label) = if arn.contains(":loadbalancer/") {
        ("LoadBalancerNotFound", "Load balancer")
    } else if arn.contains(":targetgroup/") {
        ("TargetGroupNotFound", "Target group")
    } else if arn.contains(":listener-rule/") {
        ("RuleNotFound", "Rule")
    } else if arn.contains(":listener/") {
        ("ListenerNotFound", "Listener")
    } else if arn.contains(":truststore/") {
        ("TrustStoreNotFound", "Trust store")
    } else {
        // ARN didn't match any known ELBv2 resource type. The tag ops only
        // declare specific *NotFound shapes — pick LoadBalancerNotFound as
        // the catch-all since it's declared on every tag op and matches
        // what AWS returns for unparseable ARNs.
        ("LoadBalancerNotFound", "Resource")
    };
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        code,
        format!("{label} '{arn}' not found"),
    )
}
