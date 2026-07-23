use super::*;

pub(crate) fn _account_helper(_state: &mut Route53Accounts) -> &mut AccountState {
    unreachable!()
}

pub(crate) fn require_id(route: &Route) -> Result<String, AwsServiceError> {
    route
        .id
        .clone()
        .ok_or_else(|| invalid_argument("missing id in URI"))
}

pub(crate) fn strip_zone_prefix(id: &str) -> String {
    if let Some(rest) = id.strip_prefix("/hostedzone/") {
        rest.to_string()
    } else if let Some(rest) = id.strip_prefix("hostedzone/") {
        rest.to_string()
    } else {
        id.to_string()
    }
}

pub(crate) fn synth_name_servers(_id: &str) -> Vec<String> {
    vec![
        "ns-2048.awsdns-64.com".to_string(),
        "ns-2049.awsdns-65.net".to_string(),
        "ns-2050.awsdns-66.org".to_string(),
        "ns-2051.awsdns-67.co.uk".to_string(),
    ]
}

pub(crate) fn default_zone_records(name: &str, name_servers: &[String]) -> Vec<ResourceRecordSet> {
    let mut soa_value = String::new();
    soa_value.push_str(&name_servers[0]);
    soa_value.push_str(" awsdns-hostmaster.amazon.com. 1 7200 900 1209600 86400");
    let soa = ResourceRecordSet {
        name: name.to_string(),
        record_type: "SOA".to_string(),
        ttl: Some(900),
        resource_records: Some(crate::model::ResourceRecords {
            resource_record: vec![crate::model::ResourceRecord { value: soa_value }],
        }),
        ..Default::default()
    };
    let ns = ResourceRecordSet {
        name: name.to_string(),
        record_type: "NS".to_string(),
        ttl: Some(172800),
        resource_records: Some(crate::model::ResourceRecords {
            resource_record: name_servers
                .iter()
                .map(|n| crate::model::ResourceRecord { value: n.clone() })
                .collect(),
        }),
        ..Default::default()
    };
    vec![soa, ns]
}

pub(crate) fn normalize_rrset(r: &ResourceRecordSet) -> ResourceRecordSet {
    let mut out = r.clone();
    // Route 53 lowercases record names on store and matches them
    // case-insensitively. Normalizing to lowercase here keeps stored records
    // canonical (so `ListResourceRecordSets` doesn't echo mixed case back and
    // cause a Terraform perpetual diff) and makes CREATE duplicate-detection
    // and DELETE/UPSERT matching case-insensitive.
    out.name = out.name.to_ascii_lowercase();
    if !out.name.ends_with('.') {
        out.name.push('.');
    }
    out
}

pub(crate) fn rrset_matches(a: &ResourceRecordSet, b: &ResourceRecordSet) -> bool {
    // Compare names case-insensitively — AWS treats DNS names as
    // case-insensitive, and older stored records (e.g. default SOA/NS seeded
    // from a mixed-case zone name) may not be lowercased.
    a.name.eq_ignore_ascii_case(&b.name)
        && a.record_type == b.record_type
        && a.set_identifier == b.set_identifier
}

/// Sorted multiset of a record set's values (resource records + an alias
/// target rendered as `ALIAS <zone>:<dns>`), used to compare whether two
/// record sets carry the same data. Route 53's `DELETE` change action
/// requires the caller to submit the record set's *current* values (and
/// TTL) exactly; a name+type+set-identifier match alone is insufficient.
fn rrset_value_key(r: &ResourceRecordSet) -> Vec<String> {
    let mut vals: Vec<String> = r
        .resource_records
        .as_ref()
        .map(|rr| rr.resource_record.iter().map(|x| x.value.clone()).collect())
        .unwrap_or_default();
    if let Some(at) = &r.alias_target {
        // The alias `EvaluateTargetHealth` flag is part of the record set's
        // current values: a DELETE must submit it exactly, so include it in the
        // value key rather than matching on hosted-zone-id + dns-name alone.
        vals.push(format!(
            "ALIAS {}:{}:{}",
            at.hosted_zone_id, at.dns_name, at.evaluate_target_health
        ));
    }
    vals.sort();
    vals
}

/// True when two record sets carry identical TTL and value data, in
/// addition to name/type/set-identifier. Used to gate `DELETE`.
pub(crate) fn rrset_values_match(current: &ResourceRecordSet, want: &ResourceRecordSet) -> bool {
    // Alias records carry no TTL, so Route 53 ignores TTL when matching an
    // alias delete. For a standard record a DELETE must specify the TTL and it
    // must match exactly — omitting it is a mismatch, not a wildcard.
    let ttl_ok = if current.alias_target.is_some() {
        true
    } else {
        current.ttl == want.ttl
    };
    ttl_ok && rrset_value_key(current) == rrset_value_key(want)
}

/// Validate a record set submitted to `ChangeResourceRecordSets` for a
/// `CREATE`/`UPSERT`. Enforces the three checks AWS applies that fakecloud
/// previously skipped: the record name must sit within the zone, the type
/// must be a known RR type, and value-bearing types must carry either
/// `ResourceRecords` or an `AliasTarget`.
pub(crate) fn validate_rrset_in_zone(
    r: &ResourceRecordSet,
    zone_name: &str,
) -> Result<(), AwsServiceError> {
    // `r.name` and `zone_name` are already normalized to a trailing dot.
    let in_zone = r.name == zone_name
        || r.name
            .to_ascii_lowercase()
            .ends_with(&format!(".{}", zone_name.to_ascii_lowercase()));
    if !in_zone {
        return Err(invalid_change_batch(format!(
            "RRSet with DNS name {} is not permitted in zone {}",
            r.name, zone_name
        )));
    }
    if !RR_TYPES.contains(&r.record_type.as_str()) {
        return Err(invalid_change_batch(format!(
            "Invalid resource record set type: {}",
            r.record_type
        )));
    }
    let has_values = r
        .resource_records
        .as_ref()
        .map(|rr| !rr.resource_record.is_empty())
        .unwrap_or(false);
    if !has_values && r.alias_target.is_none() {
        return Err(invalid_change_batch(format!(
            "Invalid resource record set [name='{}', type='{}']: must specify either ResourceRecords or AliasTarget",
            r.name, r.record_type
        )));
    }
    // AliasTarget is mutually exclusive with both ResourceRecords and TTL: an
    // alias record's target is resolved dynamically, so AWS rejects a record
    // that also carries literal values or a TTL with InvalidChangeBatch.
    if r.alias_target.is_some() && has_values {
        return Err(invalid_change_batch(format!(
            "Invalid resource record set [name='{}', type='{}']: may not specify both AliasTarget and ResourceRecords",
            r.name, r.record_type
        )));
    }
    if r.alias_target.is_some() && r.ttl.is_some() {
        return Err(invalid_change_batch(format!(
            "Invalid resource record set [name='{}', type='{}']: may not specify a TTL for an alias record",
            r.name, r.record_type
        )));
    }
    Ok(())
}

pub(crate) fn is_default_record(r: &ResourceRecordSet, zone_name: &str) -> bool {
    r.name == zone_name && (r.record_type == "SOA" || r.record_type == "NS")
}

/// Reversed-label sort key for a DNS name. Route 53 orders record sets and
/// zones by RFC 4034 canonical name ordering: compare labels right-to-left, and
/// within a label octet-by-octet where the end of a label sorts before any
/// octet.
///
/// The labels are reversed and joined with NUL (`0x00`) rather than `.` so that
/// a label boundary sorts before every label octet. Joining with `.` (`0x2E`)
/// mis-ranks any label containing an octet below `.` — e.g. `-` (`0x2D`) — so
/// `a-.example.com.` would wrongly sort after `x.a.example.com.`.
pub(crate) fn reverse_dns_key(name: &str) -> String {
    let trimmed = name.trim_end_matches('.').to_ascii_lowercase();
    let mut labels: Vec<&str> = trimmed.split('.').collect();
    labels.reverse();
    labels.join("\0")
}

/// Materialize a traffic-policy document into the record set AWS creates for
/// a traffic policy instance. Real Route 53 expands the policy's rule/endpoint
/// DAG into one or more record sets; fakecloud resolves the leaf endpoint
/// values into a single record set carrying the instance name, type, TTL, the
/// endpoint values, and the `TrafficPolicyInstanceId` link, so the instance's
/// DNS shows up in `ListResourceRecordSets` exactly like any other record.
pub(crate) fn materialize_policy_rrset(
    document: &str,
    name: &str,
    record_type: &str,
    ttl: i64,
    instance_id: &str,
) -> ResourceRecordSet {
    let values = extract_endpoint_values(document);
    ResourceRecordSet {
        name: name.to_string(),
        record_type: record_type.to_string(),
        ttl: Some(ttl),
        resource_records: if values.is_empty() {
            None
        } else {
            Some(crate::model::ResourceRecords {
                resource_record: values
                    .into_iter()
                    .map(|value| crate::model::ResourceRecord { value })
                    .collect(),
            })
        },
        traffic_policy_instance_id: Some(instance_id.to_string()),
        ..Default::default()
    }
}

/// Collect the leaf `Value` of every endpoint declared in a traffic policy
/// document. Value endpoints carry a literal record value; ELB/S3/CloudFront
/// endpoints carry the target DNS name as their `Value` — either way the
/// `Value` is the record data to publish.
fn extract_endpoint_values(document: &str) -> Vec<String> {
    let Ok(v) = serde_json::from_str::<serde_json::Value>(document) else {
        return Vec::new();
    };
    let Some(endpoints) = v.get("Endpoints").and_then(|e| e.as_object()) else {
        return Vec::new();
    };
    let mut out: Vec<String> = endpoints
        .values()
        .filter_map(|ep| ep.get("Value").and_then(|x| x.as_str()).map(String::from))
        .collect();
    out.sort();
    out
}

pub(crate) fn push_hosted_zone(out: &mut String, z: &StoredHostedZone) {
    out.push_str("<HostedZone>");
    out.push_str(&format!("<Id>/hostedzone/{}</Id>", esc(&z.id)));
    out.push_str(&format!("<Name>{}</Name>", esc(&z.name)));
    out.push_str(&format!(
        "<CallerReference>{}</CallerReference>",
        esc(&z.caller_reference)
    ));
    out.push_str("<Config>");
    if let Some(c) = &z.comment {
        out.push_str(&format!("<Comment>{}</Comment>", esc(c)));
    }
    out.push_str(&format!("<PrivateZone>{}</PrivateZone>", z.private_zone));
    out.push_str("</Config>");
    out.push_str(&format!(
        "<ResourceRecordSetCount>{}</ResourceRecordSetCount>",
        z.resource_record_sets.len()
    ));
    out.push_str("</HostedZone>");
}

pub(crate) fn push_change_info(out: &mut String, c: &StoredChange) {
    out.push_str("<ChangeInfo>");
    out.push_str(&format!("<Id>/change/{}</Id>", esc(&c.id)));
    out.push_str(&format!("<Status>{}</Status>", esc(&c.status)));
    out.push_str(&format!(
        "<SubmittedAt>{}</SubmittedAt>",
        rfc3339(&c.submitted_at)
    ));
    if let Some(cm) = &c.comment {
        out.push_str(&format!("<Comment>{}</Comment>", esc(cm)));
    }
    out.push_str("</ChangeInfo>");
}

pub(crate) fn push_vpc_block(out: &mut String, root: &str, v: &crate::model::VPC) {
    out.push_str(&format!("<{root}>"));
    if let Some(id) = &v.vpc_id {
        out.push_str(&format!("<VPCId>{}</VPCId>", esc(id)));
    }
    if let Some(r) = &v.vpc_region {
        out.push_str(&format!("<VPCRegion>{}</VPCRegion>", esc(r)));
    }
    out.push_str(&format!("</{root}>"));
}

pub(crate) fn push_rrset(out: &mut String, r: &ResourceRecordSet) {
    out.push_str("<ResourceRecordSet>");
    out.push_str(&format!("<Name>{}</Name>", esc(&r.name)));
    out.push_str(&format!("<Type>{}</Type>", esc(&r.record_type)));
    if let Some(s) = &r.set_identifier {
        out.push_str(&format!("<SetIdentifier>{}</SetIdentifier>", esc(s)));
    }
    if let Some(w) = r.weight {
        out.push_str(&format!("<Weight>{}</Weight>", w));
    }
    if let Some(reg) = &r.region {
        out.push_str(&format!("<Region>{}</Region>", esc(reg)));
    }
    if let Some(g) = &r.geo_location {
        out.push_str("<GeoLocation>");
        if let Some(v) = &g.continent_code {
            out.push_str(&format!("<ContinentCode>{}</ContinentCode>", esc(v)));
        }
        if let Some(v) = &g.country_code {
            out.push_str(&format!("<CountryCode>{}</CountryCode>", esc(v)));
        }
        if let Some(v) = &g.subdivision_code {
            out.push_str(&format!("<SubdivisionCode>{}</SubdivisionCode>", esc(v)));
        }
        out.push_str("</GeoLocation>");
    }
    if let Some(f) = &r.failover {
        out.push_str(&format!("<Failover>{}</Failover>", esc(f)));
    }
    if let Some(m) = r.multi_value_answer {
        out.push_str(&format!("<MultiValueAnswer>{}</MultiValueAnswer>", m));
    }
    if let Some(t) = r.ttl {
        out.push_str(&format!("<TTL>{}</TTL>", t));
    }
    if let Some(rr) = &r.resource_records {
        out.push_str("<ResourceRecords>");
        for v in &rr.resource_record {
            out.push_str("<ResourceRecord>");
            out.push_str(&format!("<Value>{}</Value>", esc(&v.value)));
            out.push_str("</ResourceRecord>");
        }
        out.push_str("</ResourceRecords>");
    }
    if let Some(at) = &r.alias_target {
        out.push_str("<AliasTarget>");
        out.push_str(&format!(
            "<HostedZoneId>{}</HostedZoneId>",
            esc(&at.hosted_zone_id)
        ));
        out.push_str(&format!("<DNSName>{}</DNSName>", esc(&at.dns_name)));
        out.push_str(&format!(
            "<EvaluateTargetHealth>{}</EvaluateTargetHealth>",
            at.evaluate_target_health
        ));
        out.push_str("</AliasTarget>");
    }
    if let Some(h) = &r.health_check_id {
        out.push_str(&format!("<HealthCheckId>{}</HealthCheckId>", esc(h)));
    }
    if let Some(t) = &r.traffic_policy_instance_id {
        out.push_str(&format!(
            "<TrafficPolicyInstanceId>{}</TrafficPolicyInstanceId>",
            esc(t)
        ));
    }
    if let Some(c) = &r.cidr_routing_config {
        out.push_str("<CidrRoutingConfig>");
        out.push_str(&format!(
            "<CollectionId>{}</CollectionId>",
            esc(&c.collection_id)
        ));
        out.push_str(&format!(
            "<LocationName>{}</LocationName>",
            esc(&c.location_name)
        ));
        out.push_str("</CidrRoutingConfig>");
    }
    if let Some(g) = &r.geo_proximity_location {
        out.push_str("<GeoProximityLocation>");
        if let Some(v) = &g.aws_region {
            out.push_str(&format!("<AWSRegion>{}</AWSRegion>", esc(v)));
        }
        if let Some(v) = &g.local_zone_group {
            out.push_str(&format!("<LocalZoneGroup>{}</LocalZoneGroup>", esc(v)));
        }
        if let Some(c) = &g.coordinates {
            out.push_str("<Coordinates>");
            out.push_str(&format!("<Latitude>{}</Latitude>", esc(&c.latitude)));
            out.push_str(&format!("<Longitude>{}</Longitude>", esc(&c.longitude)));
            out.push_str("</Coordinates>");
        }
        if let Some(b) = g.bias {
            out.push_str(&format!("<Bias>{}</Bias>", b));
        }
        out.push_str("</GeoProximityLocation>");
    }
    out.push_str("</ResourceRecordSet>");
}

pub(crate) fn esc(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            _ => out.push(c),
        }
    }
    out
}

pub(crate) fn generate_zone_id() -> String {
    let raw = Uuid::new_v4().simple().to_string().to_uppercase();
    format!("Z{}", &raw[..14])
}

pub(crate) fn generate_change_id() -> String {
    let raw = Uuid::new_v4().simple().to_string().to_uppercase();
    format!("C{}", &raw[..14])
}

pub(crate) fn generate_health_check_id() -> String {
    Uuid::new_v4().to_string()
}

pub(crate) fn push_health_check(out: &mut String, hc: &StoredHealthCheck) {
    out.push_str("<HealthCheck>");
    out.push_str(&format!("<Id>{}</Id>", esc(&hc.id)));
    out.push_str(&format!(
        "<CallerReference>{}</CallerReference>",
        esc(&hc.caller_reference)
    ));
    push_health_check_config(out, &hc.config);
    out.push_str(&format!(
        "<HealthCheckVersion>{}</HealthCheckVersion>",
        hc.version
    ));
    out.push_str("</HealthCheck>");
}

pub(crate) fn push_health_check_config(out: &mut String, c: &HealthCheckConfig) {
    out.push_str("<HealthCheckConfig>");
    if let Some(v) = &c.ip_address {
        out.push_str(&format!("<IPAddress>{}</IPAddress>", esc(v)));
    }
    if let Some(v) = c.port {
        out.push_str(&format!("<Port>{}</Port>", v));
    }
    out.push_str(&format!("<Type>{}</Type>", esc(&c.health_check_type)));
    if let Some(v) = &c.resource_path {
        out.push_str(&format!("<ResourcePath>{}</ResourcePath>", esc(v)));
    }
    if let Some(v) = &c.fully_qualified_domain_name {
        out.push_str(&format!(
            "<FullyQualifiedDomainName>{}</FullyQualifiedDomainName>",
            esc(v)
        ));
    }
    if let Some(v) = &c.search_string {
        out.push_str(&format!("<SearchString>{}</SearchString>", esc(v)));
    }
    if let Some(v) = c.request_interval {
        out.push_str(&format!("<RequestInterval>{}</RequestInterval>", v));
    }
    if let Some(v) = c.failure_threshold {
        out.push_str(&format!("<FailureThreshold>{}</FailureThreshold>", v));
    }
    if let Some(v) = c.measure_latency {
        out.push_str(&format!("<MeasureLatency>{}</MeasureLatency>", v));
    }
    if let Some(v) = c.inverted {
        out.push_str(&format!("<Inverted>{}</Inverted>", v));
    }
    if let Some(v) = c.disabled {
        out.push_str(&format!("<Disabled>{}</Disabled>", v));
    }
    if let Some(v) = c.health_threshold {
        out.push_str(&format!("<HealthThreshold>{}</HealthThreshold>", v));
    }
    if let Some(child) = &c.child_health_checks {
        out.push_str("<ChildHealthChecks>");
        for id in &child.child_health_check {
            out.push_str(&format!("<ChildHealthCheck>{}</ChildHealthCheck>", esc(id)));
        }
        out.push_str("</ChildHealthChecks>");
    }
    if let Some(v) = c.enable_sni {
        out.push_str(&format!("<EnableSNI>{}</EnableSNI>", v));
    }
    if let Some(regs) = &c.regions {
        out.push_str("<Regions>");
        for r in &regs.region {
            out.push_str(&format!("<Region>{}</Region>", esc(r)));
        }
        out.push_str("</Regions>");
    }
    if let Some(a) = &c.alarm_identifier {
        out.push_str("<AlarmIdentifier>");
        out.push_str(&format!("<Region>{}</Region>", esc(&a.region)));
        out.push_str(&format!("<Name>{}</Name>", esc(&a.name)));
        out.push_str("</AlarmIdentifier>");
    }
    if let Some(v) = &c.insufficient_data_health_status {
        out.push_str(&format!(
            "<InsufficientDataHealthStatus>{}</InsufficientDataHealthStatus>",
            esc(v)
        ));
    }
    if let Some(v) = &c.routing_control_arn {
        out.push_str(&format!(
            "<RoutingControlArn>{}</RoutingControlArn>",
            esc(v)
        ));
    }
    out.push_str("</HealthCheckConfig>");
}

pub(crate) fn no_such_health_check(id: &str) -> AwsServiceError {
    aws_error(
        StatusCode::NOT_FOUND,
        "NoSuchHealthCheck",
        format!("No health check found with ID: {id}"),
    )
}

// Public Route 53 health checker IP ranges (subset of the published list).
// fakecloud is offline so the exact set doesn't matter; SDKs only check
// the structure of the response.
pub(crate) const CHECKER_IP_RANGES: &[&str] = &[
    "15.177.0.0/18",
    "15.177.64.0/18",
    "15.177.128.0/18",
    "15.177.192.0/18",
    "54.183.255.128/26",
    "54.228.16.0/26",
    "54.232.40.64/26",
    "54.241.32.64/26",
    "54.243.31.192/26",
    "54.244.52.192/26",
    "54.245.168.0/26",
    "54.248.220.0/26",
    "54.250.253.192/26",
    "54.251.31.128/26",
    "54.252.79.128/26",
    "54.252.254.192/26",
    "54.255.254.192/26",
    "107.23.255.0/26",
    "176.34.159.192/26",
    "177.71.207.128/26",
];

pub(crate) fn checker_regions() -> &'static [&'static str] {
    &[
        "us-east-1",
        "us-west-1",
        "us-west-2",
        "eu-west-1",
        "ap-southeast-1",
        "ap-southeast-2",
        "ap-northeast-1",
        "sa-east-1",
    ]
}

pub(crate) fn checker_ip_for_region(region: &str) -> String {
    // Route 53 reports a per-region checker IP. The exact value isn't
    // observable to clients in any way that matters for fakecloud, so use
    // a deterministic offset into the documented ranges.
    let idx = region.bytes().fold(0usize, |a, b| a + b as usize) % CHECKER_IP_RANGES.len();
    CHECKER_IP_RANGES[idx]
        .split('/')
        .next()
        .unwrap_or("0.0.0.0")
        .to_string()
}

pub(crate) fn rfc3339(t: &chrono::DateTime<chrono::Utc>) -> String {
    t.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

/// Format the `<Status>` element returned by `GetHealthCheckStatus`.
///
/// Mirrors the strings real Route 53 surfaces: a `Success`/`Failure`
/// prefix with a free-form descriptor after a colon. When the caller
/// records an explicit `last_failure_reason` for a failing check, that
/// reason is concatenated; otherwise we fall back to a per-status
/// canned descriptor that matches what AWS produces for that flavour.
pub(crate) fn render_status_line(
    status: crate::state::HealthCheckStatus,
    last_failure_reason: Option<&str>,
) -> String {
    use crate::state::HealthCheckStatus;
    match status {
        HealthCheckStatus::Success => "Success: HTTP Status Code 200".to_string(),
        HealthCheckStatus::Failure => match last_failure_reason {
            Some(reason) if !reason.is_empty() => format!("Failure: {reason}"),
            _ => "Failure: Endpoint unreachable".to_string(),
        },
        HealthCheckStatus::Timeout => match last_failure_reason {
            Some(reason) if !reason.is_empty() => format!("Failure: {reason}"),
            _ => "Failure: Connection timed out".to_string(),
        },
        HealthCheckStatus::DnsError => match last_failure_reason {
            Some(reason) if !reason.is_empty() => format!("Failure: {reason}"),
            _ => "Failure: DNS resolution failed".to_string(),
        },
        HealthCheckStatus::InsufficientDataPoints => "InsufficientDataPoints".to_string(),
        HealthCheckStatus::Unknown => "Unknown".to_string(),
    }
}

pub(crate) fn invalid_argument(msg: impl Into<String>) -> AwsServiceError {
    aws_error(StatusCode::BAD_REQUEST, "InvalidInput", msg)
}

pub(crate) fn invalid_change_batch(msg: impl Into<String>) -> AwsServiceError {
    aws_error(StatusCode::BAD_REQUEST, "InvalidChangeBatch", msg)
}

pub(crate) fn no_such_hosted_zone(id: &str) -> AwsServiceError {
    aws_error(
        StatusCode::NOT_FOUND,
        "NoSuchHostedZone",
        format!("No hosted zone found with ID: {id}"),
    )
}

pub(crate) fn aws_error(
    status: StatusCode,
    code: impl Into<String>,
    msg: impl Into<String>,
) -> AwsServiceError {
    AwsServiceError::aws_error(status, code.into(), msg)
}

pub(crate) fn xml_response(status: StatusCode, body: String, headers: HeaderMap) -> AwsResponse {
    AwsResponse {
        status,
        content_type: "text/xml".to_string(),
        body: ResponseBody::Bytes(Bytes::from(body)),
        headers,
    }
}

// ─── Traffic Policy helpers ──────────────────────────────────────────

pub(crate) fn require_version(route: &Route) -> Result<i64, AwsServiceError> {
    route
        .second_id
        .as_ref()
        .ok_or_else(|| invalid_argument("missing version in URI"))
        .and_then(|v| {
            v.parse::<i64>()
                .map_err(|_| invalid_argument(format!("invalid version: {v}")))
        })
}

pub(crate) fn generate_traffic_policy_id() -> String {
    Uuid::new_v4().to_string()
}

pub(crate) fn generate_traffic_policy_instance_id() -> String {
    Uuid::new_v4().to_string()
}

/// Inspect a traffic policy document JSON for `RecordType` to seed the
/// `TrafficPolicy.Type` field. Defaults to `A` if the document is empty
/// or doesn't declare one.
pub(crate) fn infer_policy_type(doc: &str) -> String {
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(doc) {
        if let Some(rt) = v.get("RecordType").and_then(|x| x.as_str()) {
            return rt.to_string();
        }
    }
    "A".to_string()
}

pub(crate) fn no_such_traffic_policy(id: &str) -> AwsServiceError {
    aws_error(
        StatusCode::NOT_FOUND,
        "NoSuchTrafficPolicy",
        format!("No traffic policy found with ID: {id}"),
    )
}

pub(crate) fn no_such_traffic_policy_instance(id: &str) -> AwsServiceError {
    aws_error(
        StatusCode::NOT_FOUND,
        "NoSuchTrafficPolicyInstance",
        format!("No traffic policy instance found with ID: {id}"),
    )
}

pub(crate) fn push_traffic_policy(out: &mut String, p: &StoredTrafficPolicy) {
    out.push_str("<TrafficPolicy>");
    out.push_str(&format!("<Id>{}</Id>", esc(&p.id)));
    out.push_str(&format!("<Version>{}</Version>", p.version));
    out.push_str(&format!("<Name>{}</Name>", esc(&p.name)));
    out.push_str(&format!("<Type>{}</Type>", esc(&p.policy_type)));
    out.push_str(&format!("<Document>{}</Document>", esc(&p.document)));
    if let Some(c) = &p.comment {
        out.push_str(&format!("<Comment>{}</Comment>", esc(c)));
    }
    out.push_str("</TrafficPolicy>");
}

pub(crate) fn push_traffic_policy_summary(
    out: &mut String,
    p: &StoredTrafficPolicy,
    traffic_policy_count: i64,
) {
    out.push_str("<TrafficPolicySummary>");
    out.push_str(&format!("<Id>{}</Id>", esc(&p.id)));
    out.push_str(&format!("<Name>{}</Name>", esc(&p.name)));
    out.push_str(&format!("<Type>{}</Type>", esc(&p.policy_type)));
    out.push_str(&format!("<LatestVersion>{}</LatestVersion>", p.version));
    out.push_str(&format!(
        "<TrafficPolicyCount>{}</TrafficPolicyCount>",
        traffic_policy_count
    ));
    out.push_str("</TrafficPolicySummary>");
}

pub(crate) fn push_traffic_policy_instance(out: &mut String, i: &StoredTrafficPolicyInstance) {
    out.push_str("<TrafficPolicyInstance>");
    out.push_str(&format!("<Id>{}</Id>", esc(&i.id)));
    out.push_str(&format!(
        "<HostedZoneId>{}</HostedZoneId>",
        esc(&i.hosted_zone_id)
    ));
    out.push_str(&format!("<Name>{}</Name>", esc(&i.name)));
    out.push_str(&format!("<TTL>{}</TTL>", i.ttl));
    out.push_str(&format!("<State>{}</State>", esc(&i.state)));
    out.push_str(&format!("<Message>{}</Message>", esc(&i.message)));
    out.push_str(&format!(
        "<TrafficPolicyId>{}</TrafficPolicyId>",
        esc(&i.traffic_policy_id)
    ));
    out.push_str(&format!(
        "<TrafficPolicyVersion>{}</TrafficPolicyVersion>",
        i.traffic_policy_version
    ));
    out.push_str(&format!(
        "<TrafficPolicyType>{}</TrafficPolicyType>",
        esc(&i.traffic_policy_type)
    ));
    out.push_str("</TrafficPolicyInstance>");
}

// ─── DNSSEC + Query Logging + CIDR helpers ───────────────────────────

pub(crate) fn require_zone_and_name(route: &Route) -> Result<(String, String), AwsServiceError> {
    let zone = require_id(route)?;
    let name = route
        .second_id
        .clone()
        .ok_or_else(|| invalid_argument("missing name in URI"))?;
    Ok((strip_zone_prefix(&zone), name))
}

pub(crate) fn no_such_key_signing_key(zone_id: &str, name: &str) -> AwsServiceError {
    aws_error(
        StatusCode::NOT_FOUND,
        "NoSuchKeySigningKey",
        format!("No key-signing key {}/{} found", zone_id, name),
    )
}

pub(crate) fn no_such_query_logging_config(id: &str) -> AwsServiceError {
    aws_error(
        StatusCode::NOT_FOUND,
        "NoSuchQueryLoggingConfig",
        format!("No query logging config found with ID: {id}"),
    )
}

pub(crate) fn no_such_cidr_collection(id: &str) -> AwsServiceError {
    aws_error(
        StatusCode::NOT_FOUND,
        "NoSuchCidrCollectionException",
        format!("No CIDR collection found with ID: {id}"),
    )
}

pub(crate) fn push_key_signing_key_inner(out: &mut String, k: &StoredKeySigningKey) {
    out.push_str(&format!("<Name>{}</Name>", esc(&k.name)));
    out.push_str(&format!("<KmsArn>{}</KmsArn>", esc(&k.kms_arn)));
    out.push_str("<Flag>257</Flag>");
    out.push_str(
        "<SigningAlgorithmMnemonic>ECDSAP256SHA256</SigningAlgorithmMnemonic>\
         <SigningAlgorithmType>13</SigningAlgorithmType>\
         <DigestAlgorithmMnemonic>SHA-256</DigestAlgorithmMnemonic>\
         <DigestAlgorithmType>2</DigestAlgorithmType>",
    );
    out.push_str(&format!("<KeyTag>{}</KeyTag>", k.key_tag));
    // Re-derive the (deterministic) DNSSEC key material so the response carries
    // the DNSKEY public key, the DNSKEY/DS presentation records, and the DS
    // digest the Terraform resource asserts. AWS reports the DS digest in upper
    // case hex; the DNSKEY/DS records use the standard zone-file presentation.
    let material = crate::dnssec::derive_keypair(&k.hosted_zone_id, &k.name);
    let public_key_b64 = crate::dnssec::b64(&material.dnskey_public_key);
    let digest_upper = k.ds_digest_hex.to_uppercase();
    let dnskey_record = format!("257 3 13 {public_key_b64}");
    let ds_record = format!("{} 13 2 {}", k.key_tag, digest_upper);
    out.push_str(&format!("<PublicKey>{}</PublicKey>", esc(&public_key_b64)));
    out.push_str(&format!(
        "<DigestValue>{}</DigestValue>",
        esc(&digest_upper)
    ));
    out.push_str(&format!(
        "<DNSKEYRecord>{}</DNSKEYRecord>",
        esc(&dnskey_record)
    ));
    out.push_str(&format!("<DSRecord>{}</DSRecord>", esc(&ds_record)));
    out.push_str(&format!("<Status>{}</Status>", esc(&k.status)));
    out.push_str(&format!(
        "<CreatedDate>{}</CreatedDate>",
        rfc3339(&k.created_date)
    ));
    out.push_str(&format!(
        "<LastModifiedDate>{}</LastModifiedDate>",
        rfc3339(&k.last_modified_date)
    ));
}

pub(crate) fn push_query_logging_config(out: &mut String, c: &StoredQueryLoggingConfig) {
    out.push_str("<QueryLoggingConfig>");
    out.push_str(&format!("<Id>{}</Id>", esc(&c.id)));
    out.push_str(&format!(
        "<HostedZoneId>{}</HostedZoneId>",
        esc(&c.hosted_zone_id)
    ));
    out.push_str(&format!(
        "<CloudWatchLogsLogGroupArn>{}</CloudWatchLogsLogGroupArn>",
        esc(&c.cloud_watch_logs_log_group_arn)
    ));
    out.push_str("</QueryLoggingConfig>");
}

pub(crate) fn push_cidr_collection_full(out: &mut String, c: &StoredCidrCollection) {
    out.push_str("<Collection>");
    out.push_str(&format!("<Arn>{}</Arn>", esc(&c.arn)));
    out.push_str(&format!("<Id>{}</Id>", esc(&c.id)));
    out.push_str(&format!("<Name>{}</Name>", esc(&c.name)));
    out.push_str(&format!("<Version>{}</Version>", c.version));
    out.push_str("</Collection>");
}

// ─── VPC Association handlers ────────────────────────────────────────

impl Route53Service {
    pub(crate) fn associate_vpc_with_hosted_zone(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = strip_zone_prefix(&require_id(route)?);
        let cfg: AssociateVpcRequest = xml_io::from_xml_root(&req.body)
            .map_err(|e| invalid_argument(format!("invalid AssociateVPCRequest XML: {e}")))?;
        let vpc = cfg.vpc;
        require_vpc(&vpc)?;
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| no_such_hosted_zone(&id))?;
        let zone = account
            .hosted_zones
            .get_mut(&id)
            .ok_or_else(|| no_such_hosted_zone(&id))?;
        if !zone.private_zone {
            return Err(aws_error(
                StatusCode::BAD_REQUEST,
                "PublicZoneVPCAssociation",
                format!("HostedZone {id} is a public zone; cannot associate a VPC"),
            ));
        }
        if zone.vpcs.iter().any(|v| same_vpc(v, &vpc)) {
            return Err(aws_error(
                StatusCode::CONFLICT,
                "ConflictingDomainExists",
                format!(
                    "VPC {} is already associated with hosted zone {id}",
                    vpc.vpc_id.clone().unwrap_or_default()
                ),
            ));
        }
        // For cross-account associations Route 53 requires a prior
        // CreateVPCAssociationAuthorization. For same-account associations
        // (the only kind fakecloud has — single DEFAULT_ACCOUNT) the
        // authorization is implicit, so we don't gate on it. Once we model
        // multi-account ownership of zones we can wire the authorization
        // check in.
        zone.vpcs.push(vpc);
        let now = Utc::now();
        let change_id = generate_change_id();
        let change = StoredChange {
            id: change_id.clone(),
            status: "PENDING".to_string(),
            submitted_at: now,
            comment: cfg.comment,
            read_count: 0,
        };
        account.changes.insert(change_id.clone(), change.clone());
        drop(state);
        let mut body = String::with_capacity(256);
        body.push_str(XML_DECL);
        body.push_str(&format!(
            "<AssociateVPCWithHostedZoneResponse xmlns=\"{NS}\">"
        ));
        push_change_info(&mut body, &change);
        body.push_str("</AssociateVPCWithHostedZoneResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    pub(crate) fn disassociate_vpc_from_hosted_zone(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = strip_zone_prefix(&require_id(route)?);
        let cfg: AssociateVpcRequest = xml_io::from_xml_root(&req.body)
            .map_err(|e| invalid_argument(format!("invalid DisassociateVPCRequest XML: {e}")))?;
        let vpc = cfg.vpc;
        require_vpc(&vpc)?;
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| no_such_hosted_zone(&id))?;
        let zone = account
            .hosted_zones
            .get_mut(&id)
            .ok_or_else(|| no_such_hosted_zone(&id))?;
        let Some(pos) = zone.vpcs.iter().position(|v| same_vpc(v, &vpc)) else {
            return Err(aws_error(
                StatusCode::NOT_FOUND,
                "VPCAssociationNotFound",
                format!(
                    "VPC {} is not associated with hosted zone {id}",
                    vpc.vpc_id.clone().unwrap_or_default()
                ),
            ));
        };
        if zone.vpcs.len() == 1 {
            return Err(aws_error(
                StatusCode::BAD_REQUEST,
                "LastVPCAssociation",
                format!("Cannot remove the last VPC association from private hosted zone {id}"),
            ));
        }
        zone.vpcs.remove(pos);
        let now = Utc::now();
        let change_id = generate_change_id();
        let change = StoredChange {
            id: change_id.clone(),
            status: "PENDING".to_string(),
            submitted_at: now,
            comment: cfg.comment,
            read_count: 0,
        };
        account.changes.insert(change_id.clone(), change.clone());
        drop(state);
        let mut body = String::with_capacity(256);
        body.push_str(XML_DECL);
        body.push_str(&format!(
            "<DisassociateVPCFromHostedZoneResponse xmlns=\"{NS}\">"
        ));
        push_change_info(&mut body, &change);
        body.push_str("</DisassociateVPCFromHostedZoneResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    pub(crate) fn create_vpc_association_authorization(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = strip_zone_prefix(&require_id(route)?);
        let cfg: VpcAuthorizationRequest = xml_io::from_xml_root(&req.body).map_err(|e| {
            invalid_argument(format!(
                "invalid CreateVPCAssociationAuthorizationRequest XML: {e}"
            ))
        })?;
        let vpc = cfg.vpc;
        require_vpc(&vpc)?;
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| no_such_hosted_zone(&id))?;
        let zone = account
            .hosted_zones
            .get(&id)
            .ok_or_else(|| no_such_hosted_zone(&id))?;
        if !zone.private_zone {
            return Err(aws_error(
                StatusCode::BAD_REQUEST,
                "PublicZoneVPCAssociation",
                format!("HostedZone {id} is a public zone; cannot authorize a VPC association"),
            ));
        }
        let entry = account.vpc_authorizations.entry(id.clone()).or_default();
        if !entry.iter().any(|v| same_vpc(v, &vpc)) {
            entry.push(vpc.clone());
        }
        drop(state);
        let mut body = String::with_capacity(256);
        body.push_str(XML_DECL);
        body.push_str(&format!(
            "<CreateVPCAssociationAuthorizationResponse xmlns=\"{NS}\">"
        ));
        body.push_str(&format!("<HostedZoneId>{}</HostedZoneId>", esc(&id)));
        push_vpc_block(&mut body, "VPC", &vpc);
        body.push_str("</CreateVPCAssociationAuthorizationResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    pub(crate) fn delete_vpc_association_authorization(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = strip_zone_prefix(&require_id(route)?);
        let cfg: VpcAuthorizationRequest = xml_io::from_xml_root(&req.body).map_err(|e| {
            invalid_argument(format!(
                "invalid DeleteVPCAssociationAuthorizationRequest XML: {e}"
            ))
        })?;
        let vpc = cfg.vpc;
        require_vpc(&vpc)?;
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| no_such_hosted_zone(&id))?;
        if !account.hosted_zones.contains_key(&id) {
            return Err(no_such_hosted_zone(&id));
        }
        let entry = account.vpc_authorizations.entry(id.clone()).or_default();
        let before = entry.len();
        entry.retain(|v| !same_vpc(v, &vpc));
        if entry.len() == before {
            return Err(aws_error(
                StatusCode::NOT_FOUND,
                "VPCAssociationAuthorizationNotFound",
                format!(
                    "VPC {} is not authorized for hosted zone {id}",
                    vpc.vpc_id.clone().unwrap_or_default()
                ),
            ));
        }
        if entry.is_empty() {
            account.vpc_authorizations.remove(&id);
        }
        drop(state);
        let mut body = String::with_capacity(128);
        body.push_str(XML_DECL);
        body.push_str(&format!(
            "<DeleteVPCAssociationAuthorizationResponse xmlns=\"{NS}\"/>"
        ));
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    pub(crate) fn list_vpc_association_authorizations(
        &self,
        _req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = strip_zone_prefix(&require_id(route)?);
        let state = self.state.read();
        let account = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .ok_or_else(|| no_such_hosted_zone(&id))?;
        if !account.hosted_zones.contains_key(&id) {
            return Err(no_such_hosted_zone(&id));
        }
        let vpcs: Vec<VPC> = account
            .vpc_authorizations
            .get(&id)
            .cloned()
            .unwrap_or_default();
        drop(state);
        let mut body = String::with_capacity(512);
        body.push_str(XML_DECL);
        body.push_str(&format!(
            "<ListVPCAssociationAuthorizationsResponse xmlns=\"{NS}\">"
        ));
        body.push_str(&format!("<HostedZoneId>{}</HostedZoneId>", esc(&id)));
        body.push_str("<VPCs>");
        for v in &vpcs {
            push_vpc_block(&mut body, "VPC", v);
        }
        body.push_str("</VPCs>");
        body.push_str("</ListVPCAssociationAuthorizationsResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    pub(crate) fn list_hosted_zones_by_vpc(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        validate_query_constraints(
            &req.query_params,
            &[
                QueryConstraint::StrLen {
                    key: "vpcid",
                    min: 0,
                    max: 1024,
                },
                QueryConstraint::StrLen {
                    key: "vpcregion",
                    min: 1,
                    max: 64,
                },
                QueryConstraint::Enum {
                    key: "vpcregion",
                    allowed: &VPC_REGIONS,
                },
                QueryConstraint::StrLen {
                    key: "nexttoken",
                    min: 0,
                    max: 1024,
                },
                MAX_ITEMS_CONSTRAINT,
            ],
        )?;
        let vpc_id = req
            .query_params
            .get("vpcid")
            .cloned()
            .ok_or_else(|| invalid_argument("vpcid query parameter is required"))?;
        let vpc_region = req
            .query_params
            .get("vpcregion")
            .cloned()
            .ok_or_else(|| invalid_argument("vpcregion query parameter is required"))?;
        let state = self.state.read();
        let mut summaries: Vec<(String, String)> = Vec::new();
        if let Some(account) = state.accounts.get(DEFAULT_ACCOUNT) {
            for z in account.hosted_zones.values() {
                if z.vpcs.iter().any(|v| {
                    v.vpc_id.as_deref() == Some(vpc_id.as_str())
                        && v.vpc_region.as_deref() == Some(vpc_region.as_str())
                }) {
                    summaries.push((z.id.clone(), z.name.clone()));
                }
            }
        }
        drop(state);
        summaries.sort_by(|a, b| a.0.cmp(&b.0));
        // Honor maxitems + nexttoken instead of returning the whole set with a
        // hardcoded MaxItems=100. Resume after the inbound token (last zone id).
        let max_items: usize = req
            .query_params
            .get("maxitems")
            .and_then(|s| s.parse().ok())
            .unwrap_or(100);
        let start = match req.query_params.get("nexttoken") {
            Some(t) => summaries
                .iter()
                .position(|(id, _)| id.as_str() > t.as_str())
                .unwrap_or(summaries.len()),
            None => 0,
        };
        let rest = &summaries[start..];
        let page: Vec<&(String, String)> = rest.iter().take(max_items).collect();
        // Token is the LAST id emitted on this page; the next request resumes
        // strictly after it (`id > token`). Emitting the first *excluded* id
        // here would skip it, since resume is strict-greater.
        let next_token = if page.len() < rest.len() {
            page.last().map(|(id, _)| id.clone())
        } else {
            None
        };
        let mut body = String::with_capacity(512);
        body.push_str(XML_DECL);
        body.push_str(&format!("<ListHostedZonesByVPCResponse xmlns=\"{NS}\">"));
        body.push_str("<HostedZoneSummaries>");
        for (id, name) in &page {
            body.push_str("<HostedZoneSummary>");
            // HostedZoneId in a summary is the bare id (no `/hostedzone/`
            // prefix); the SDK validates the id pattern and rejects the prefix.
            body.push_str(&format!("<HostedZoneId>{}</HostedZoneId>", esc(id)));
            body.push_str(&format!("<Name>{}</Name>", esc(name)));
            body.push_str("<Owner>");
            body.push_str(&format!("<OwningAccount>{DEFAULT_ACCOUNT}</OwningAccount>",));
            body.push_str("</Owner>");
            body.push_str("</HostedZoneSummary>");
        }
        body.push_str("</HostedZoneSummaries>");
        body.push_str(&format!("<MaxItems>{max_items}</MaxItems>"));
        if let Some(t) = &next_token {
            body.push_str(&format!("<NextToken>{}</NextToken>", esc(t)));
        }
        body.push_str("</ListHostedZonesByVPCResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }
}

// ─── Reusable Delegation Set handlers ────────────────────────────────

impl Route53Service {
    pub(crate) fn create_reusable_delegation_set(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let cfg: CreateReusableDelegationSetRequest =
            xml_io::from_xml_root(&req.body).map_err(|e| {
                invalid_argument(format!(
                    "invalid CreateReusableDelegationSetRequest XML: {e}"
                ))
            })?;
        if cfg.caller_reference.is_empty() {
            return Err(invalid_argument("CallerReference is required"));
        }
        let mut state = self.state.write();
        let account = state
            .accounts
            .entry(DEFAULT_ACCOUNT.to_string())
            .or_default();
        if account
            .reusable_delegation_sets
            .values()
            .any(|d| d.caller_reference == cfg.caller_reference)
        {
            return Err(aws_error(
                StatusCode::CONFLICT,
                "DelegationSetAlreadyCreated",
                format!(
                    "A delegation set with caller reference {} already exists",
                    cfg.caller_reference
                ),
            ));
        }
        let id = generate_delegation_set_id();
        // When the caller supplies an existing hosted zone, reuse that
        // zone's authoritative name servers — that's the documented Route
        // 53 behavior for "promote a zone's delegation set to a reusable
        // one." Without that lookup, an unknown HostedZoneId silently
        // succeeds, which is what Cubic flagged.
        let name_servers = if let Some(hosted_zone_id) = cfg.hosted_zone_id.as_deref() {
            let hosted_zone_id = strip_zone_prefix(hosted_zone_id);
            account
                .hosted_zones
                .get(&hosted_zone_id)
                .ok_or_else(|| no_such_hosted_zone(&hosted_zone_id))?
                .name_servers
                .clone()
        } else {
            synth_name_servers(&id)
        };
        let ds = StoredReusableDelegationSet {
            id: id.clone(),
            caller_reference: cfg.caller_reference,
            name_servers,
        };
        account
            .reusable_delegation_sets
            .insert(id.clone(), ds.clone());
        drop(state);

        let mut body = String::with_capacity(512);
        body.push_str(XML_DECL);
        body.push_str(&format!(
            "<CreateReusableDelegationSetResponse xmlns=\"{NS}\">"
        ));
        push_delegation_set(&mut body, &ds);
        body.push_str("</CreateReusableDelegationSetResponse>");

        let mut headers = HeaderMap::new();
        if let Ok(loc) = http::HeaderValue::from_str(&format!("/2013-04-01/delegationset/{id}")) {
            headers.insert(http::header::LOCATION, loc);
        }
        Ok(xml_response(StatusCode::CREATED, body, headers))
    }

    pub(crate) fn get_reusable_delegation_set(
        &self,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = require_id(route)?;
        let state = self.state.read();
        let ds = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .and_then(|a| a.reusable_delegation_sets.get(&id).cloned())
            .ok_or_else(|| no_such_delegation_set(&id))?;
        drop(state);
        let mut body = String::with_capacity(512);
        body.push_str(XML_DECL);
        body.push_str(&format!(
            "<GetReusableDelegationSetResponse xmlns=\"{NS}\">"
        ));
        push_delegation_set(&mut body, &ds);
        body.push_str("</GetReusableDelegationSetResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    pub(crate) fn delete_reusable_delegation_set(
        &self,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = require_id(route)?;
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| no_such_delegation_set(&id))?;
        if !account.reusable_delegation_sets.contains_key(&id) {
            return Err(no_such_delegation_set(&id));
        }
        if account
            .hosted_zones
            .values()
            .any(|z| z.delegation_set_id.as_deref() == Some(id.as_str()))
        {
            return Err(aws_error(
                StatusCode::BAD_REQUEST,
                "DelegationSetInUse",
                format!("Delegation set {id} is in use by one or more hosted zones"),
            ));
        }
        account.reusable_delegation_sets.remove(&id);
        drop(state);
        let mut body = String::with_capacity(128);
        body.push_str(XML_DECL);
        body.push_str(&format!(
            "<DeleteReusableDelegationSetResponse xmlns=\"{NS}\"/>"
        ));
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    pub(crate) fn list_reusable_delegation_sets(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
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
        let state = self.state.read();
        let mut sets: Vec<StoredReusableDelegationSet> = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .map(|a| a.reusable_delegation_sets.values().cloned().collect())
            .unwrap_or_default();
        drop(state);
        sets.sort_by(|a, b| a.id.cmp(&b.id));
        let mut body = String::with_capacity(512);
        body.push_str(XML_DECL);
        body.push_str(&format!(
            "<ListReusableDelegationSetsResponse xmlns=\"{NS}\">"
        ));
        body.push_str("<DelegationSets>");
        for d in &sets {
            push_delegation_set(&mut body, d);
        }
        body.push_str("</DelegationSets>");
        body.push_str("<Marker></Marker>");
        body.push_str("<IsTruncated>false</IsTruncated>");
        body.push_str("<MaxItems>100</MaxItems>");
        body.push_str("</ListReusableDelegationSetsResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    pub(crate) fn get_reusable_delegation_set_limit(
        &self,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = require_id(route)?;
        let lim_type = route
            .second_id
            .clone()
            .ok_or_else(|| invalid_argument("limit Type is required"))?;
        let state = self.state.read();
        let account = state.accounts.get(DEFAULT_ACCOUNT);
        if !account
            .map(|a| a.reusable_delegation_sets.contains_key(&id))
            .unwrap_or(false)
        {
            return Err(no_such_delegation_set(&id));
        }
        let count = account
            .map(|a| {
                a.hosted_zones
                    .values()
                    .filter(|z| z.delegation_set_id.as_deref() == Some(id.as_str()))
                    .count() as u64
            })
            .unwrap_or(0);
        drop(state);
        if lim_type != "MAX_ZONES_BY_REUSABLE_DELEGATION_SET" {
            return Err(invalid_argument(format!(
                "Unknown reusable delegation set limit type: {lim_type}"
            )));
        }
        let mut body = String::with_capacity(256);
        body.push_str(XML_DECL);
        body.push_str(&format!(
            "<GetReusableDelegationSetLimitResponse xmlns=\"{NS}\">"
        ));
        body.push_str(&format!(
            "<Limit><Type>{}</Type><Value>500</Value></Limit>",
            esc(&lim_type)
        ));
        body.push_str(&format!("<Count>{count}</Count>"));
        body.push_str("</GetReusableDelegationSetLimitResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }
}

// ─── Geo Locations + Account Limits ─────────────────────────────────

impl Route53Service {
    pub(crate) fn list_geo_locations(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        validate_query_constraints(
            &req.query_params,
            &[
                QueryConstraint::StrLen {
                    key: "startcontinentcode",
                    min: 2,
                    max: 2,
                },
                QueryConstraint::StrLen {
                    key: "startcountrycode",
                    min: 1,
                    max: 2,
                },
                QueryConstraint::StrLen {
                    key: "startsubdivisioncode",
                    min: 1,
                    max: 3,
                },
                MAX_ITEMS_CONSTRAINT,
            ],
        )?;
        let start_continent = req.query_params.get("startcontinentcode").cloned();
        let start_country = req.query_params.get("startcountrycode").cloned();
        let start_subdivision = req.query_params.get("startsubdivisioncode").cloned();
        let max_items: usize = req
            .query_params
            .get("maxitems")
            .and_then(|s| s.parse().ok())
            .unwrap_or(100);
        let mut filtered: Vec<GeoLocationEntry> = geo_locations()
            .iter()
            .filter(|g| {
                if let Some(c) = &start_continent {
                    if g.continent_code < c.as_str() {
                        return false;
                    }
                }
                if let Some(c) = &start_country {
                    if g.country_code < c.as_str() {
                        return false;
                    }
                }
                if let Some(c) = &start_subdivision {
                    if g.subdivision_code < c.as_str() {
                        return false;
                    }
                }
                true
            })
            .cloned()
            .collect();
        // Compute the next-page marker from the *filtered* list, not from
        // the unfiltered catalogue. With non-empty start parameters the
        // unfiltered offset would point at the wrong row entirely.
        let next = if filtered.len() > max_items {
            filtered.get(max_items).cloned()
        } else {
            None
        };
        let truncated = next.is_some();
        if truncated {
            filtered.truncate(max_items);
        }
        let mut body = String::with_capacity(2048);
        body.push_str(XML_DECL);
        body.push_str(&format!("<ListGeoLocationsResponse xmlns=\"{NS}\">"));
        body.push_str("<GeoLocationDetailsList>");
        for g in &filtered {
            push_geo_location_details(&mut body, g);
        }
        body.push_str("</GeoLocationDetailsList>");
        body.push_str(&format!("<IsTruncated>{truncated}</IsTruncated>"));
        if let Some(n) = &next {
            if !n.continent_code.is_empty() {
                body.push_str(&format!(
                    "<NextContinentCode>{}</NextContinentCode>",
                    esc(n.continent_code)
                ));
            }
            if !n.country_code.is_empty() {
                body.push_str(&format!(
                    "<NextCountryCode>{}</NextCountryCode>",
                    esc(n.country_code)
                ));
            }
            if !n.subdivision_code.is_empty() {
                body.push_str(&format!(
                    "<NextSubdivisionCode>{}</NextSubdivisionCode>",
                    esc(n.subdivision_code)
                ));
            }
        }
        body.push_str(&format!("<MaxItems>{max_items}</MaxItems>"));
        body.push_str("</ListGeoLocationsResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    pub(crate) fn get_geo_location(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let continent = req.query_params.get("continentcode").cloned();
        let country = req.query_params.get("countrycode").cloned();
        let subdivision = req.query_params.get("subdivisioncode").cloned();
        if continent.is_none() && country.is_none() {
            return Err(invalid_argument(
                "Either continentcode or countrycode must be supplied",
            ));
        }
        let entry = geo_locations().iter().find(|g| {
            let c = continent.as_deref().unwrap_or("");
            let co = country.as_deref().unwrap_or("");
            let sd = subdivision.as_deref().unwrap_or("");
            g.continent_code == c && g.country_code == co && g.subdivision_code == sd
        });
        let g = match entry {
            Some(g) => g,
            None => {
                return Err(aws_error(
                    StatusCode::NOT_FOUND,
                    "NoSuchGeoLocation",
                    "The geo location requested is not supported".to_string(),
                ));
            }
        };
        let mut body = String::with_capacity(256);
        body.push_str(XML_DECL);
        body.push_str(&format!("<GetGeoLocationResponse xmlns=\"{NS}\">"));
        push_geo_location_details(&mut body, g);
        body.push_str("</GetGeoLocationResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    pub(crate) fn get_account_limit(&self, route: &Route) -> Result<AwsResponse, AwsServiceError> {
        let lim_type = require_id(route)?;
        let state = self.state.read();
        let account = state.accounts.get(DEFAULT_ACCOUNT);
        let (value, count) = match lim_type.as_str() {
            "MAX_HEALTH_CHECKS_BY_OWNER" => (
                200_u64,
                account.map(|a| a.health_checks.len() as u64).unwrap_or(0),
            ),
            "MAX_HOSTED_ZONES_BY_OWNER" => (
                500_u64,
                account.map(|a| a.hosted_zones.len() as u64).unwrap_or(0),
            ),
            "MAX_REUSABLE_DELEGATION_SETS_BY_OWNER" => (
                100_u64,
                account
                    .map(|a| a.reusable_delegation_sets.len() as u64)
                    .unwrap_or(0),
            ),
            "MAX_TRAFFIC_POLICIES_BY_OWNER" => {
                let mut policies = std::collections::HashSet::new();
                if let Some(a) = account {
                    for (id, _) in a.traffic_policies.keys().map(|k| (k.0.clone(), 0)) {
                        policies.insert(id);
                    }
                }
                (50_u64, policies.len() as u64)
            }
            "MAX_TRAFFIC_POLICY_INSTANCES_BY_OWNER" => (
                5_u64,
                account
                    .map(|a| a.traffic_policy_instances.len() as u64)
                    .unwrap_or(0),
            ),
            other => {
                return Err(invalid_argument(format!(
                    "Unknown account limit type: {other}"
                )));
            }
        };
        drop(state);
        let mut body = String::with_capacity(256);
        body.push_str(XML_DECL);
        body.push_str(&format!("<GetAccountLimitResponse xmlns=\"{NS}\">"));
        body.push_str(&format!(
            "<Limit><Type>{}</Type><Value>{}</Value></Limit>",
            esc(&lim_type),
            value
        ));
        body.push_str(&format!("<Count>{count}</Count>"));
        body.push_str("</GetAccountLimitResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }
}

// ─── Tag handlers ────────────────────────────────────────────────────

impl Route53Service {
    pub(crate) fn change_tags_for_resource(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let (res_type, res_id) = require_tag_target(route)?;
        let cfg: ChangeTagsForResourceRequest = xml_io::from_xml_root(&req.body).map_err(|e| {
            invalid_argument(format!("invalid ChangeTagsForResourceRequest XML: {e}"))
        })?;
        let mut state = self.state.write();
        let account = state
            .accounts
            .entry(DEFAULT_ACCOUNT.to_string())
            .or_default();
        if !tag_target_exists(account, &res_type, &res_id) {
            return Err(no_such_tag_target(&res_type, &res_id));
        }
        let bag = account
            .tags
            .entry((res_type.clone(), res_id.clone()))
            .or_default();
        if let Some(adds) = &cfg.add_tags {
            for tag in &adds.tag {
                let key = match &tag.key {
                    Some(k) if !k.is_empty() => k.clone(),
                    _ => return Err(invalid_argument("Tag.Key is required")),
                };
                bag.insert(key, tag.value.clone().unwrap_or_default());
            }
        }
        if let Some(removes) = &cfg.remove_tag_keys {
            for key in &removes.key {
                bag.remove(key);
            }
        }
        if bag.is_empty() {
            account.tags.remove(&(res_type.clone(), res_id.clone()));
        }
        drop(state);
        let mut body = String::with_capacity(128);
        body.push_str(XML_DECL);
        body.push_str(&format!("<ChangeTagsForResourceResponse xmlns=\"{NS}\"/>"));
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    pub(crate) fn list_tags_for_resource(
        &self,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let (res_type, res_id) = require_tag_target(route)?;
        let state = self.state.read();
        let account = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .ok_or_else(|| no_such_tag_target(&res_type, &res_id))?;
        if !tag_target_exists(account, &res_type, &res_id) {
            return Err(no_such_tag_target(&res_type, &res_id));
        }
        let bag = account
            .tags
            .get(&(res_type.clone(), res_id.clone()))
            .cloned()
            .unwrap_or_default();
        drop(state);
        let mut body = String::with_capacity(512);
        body.push_str(XML_DECL);
        body.push_str(&format!("<ListTagsForResourceResponse xmlns=\"{NS}\">"));
        push_resource_tag_set(&mut body, &res_type, &res_id, &bag);
        body.push_str("</ListTagsForResourceResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    pub(crate) fn list_tags_for_resources(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let res_type = require_id(route)?;
        if res_type != "healthcheck" && res_type != "hostedzone" {
            return Err(invalid_argument(format!(
                "Unknown tag resource type: {res_type}"
            )));
        }
        let cfg: ListTagsForResourcesRequest = xml_io::from_xml_root(&req.body).map_err(|e| {
            invalid_argument(format!("invalid ListTagsForResourcesRequest XML: {e}"))
        })?;
        if cfg.resource_ids.resource_id.is_empty() {
            return Err(invalid_argument("ResourceIds must contain at least one ID"));
        }
        let state = self.state.read();
        let account = state.accounts.get(DEFAULT_ACCOUNT);
        let mut sets: Vec<(String, BTreeMap<String, String>)> = Vec::new();
        for id in &cfg.resource_ids.resource_id {
            if let Some(a) = account {
                if !tag_target_exists(a, &res_type, id) {
                    return Err(no_such_tag_target(&res_type, id));
                }
                let bag = a
                    .tags
                    .get(&(res_type.clone(), id.clone()))
                    .cloned()
                    .unwrap_or_default();
                sets.push((id.clone(), bag));
            } else {
                return Err(no_such_tag_target(&res_type, id));
            }
        }
        drop(state);
        let mut body = String::with_capacity(1024);
        body.push_str(XML_DECL);
        body.push_str(&format!("<ListTagsForResourcesResponse xmlns=\"{NS}\">"));
        body.push_str("<ResourceTagSets>");
        for (id, bag) in &sets {
            push_resource_tag_set(&mut body, &res_type, id, bag);
        }
        body.push_str("</ResourceTagSets>");
        body.push_str("</ListTagsForResourcesResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }
}

// ─── Batch 5 helpers ─────────────────────────────────────────────────

pub(crate) fn require_vpc(v: &VPC) -> Result<(), AwsServiceError> {
    if v.vpc_id.as_deref().unwrap_or("").is_empty() {
        return Err(invalid_argument("VPC.VPCId is required"));
    }
    if v.vpc_region.as_deref().unwrap_or("").is_empty() {
        return Err(invalid_argument("VPC.VPCRegion is required"));
    }
    Ok(())
}

pub(crate) fn same_vpc(a: &VPC, b: &VPC) -> bool {
    a.vpc_id == b.vpc_id && a.vpc_region == b.vpc_region
}

pub(crate) fn push_delegation_set(out: &mut String, d: &StoredReusableDelegationSet) {
    out.push_str("<DelegationSet>");
    out.push_str(&format!("<Id>{}</Id>", esc(&d.id)));
    out.push_str(&format!(
        "<CallerReference>{}</CallerReference>",
        esc(&d.caller_reference)
    ));
    out.push_str("<NameServers>");
    for ns in &d.name_servers {
        out.push_str(&format!("<NameServer>{}</NameServer>", esc(ns)));
    }
    out.push_str("</NameServers>");
    out.push_str("</DelegationSet>");
}

pub(crate) fn generate_delegation_set_id() -> String {
    let raw = Uuid::new_v4().simple().to_string().to_uppercase();
    format!("N{}", &raw[..14])
}

pub(crate) fn no_such_delegation_set(id: &str) -> AwsServiceError {
    aws_error(
        StatusCode::NOT_FOUND,
        "NoSuchDelegationSet",
        format!("No reusable delegation set found with ID: {id}"),
    )
}

pub(crate) fn require_tag_target(route: &Route) -> Result<(String, String), AwsServiceError> {
    let res_type = require_id(route)?;
    let res_id = route
        .second_id
        .clone()
        .ok_or_else(|| invalid_argument("ResourceId is required"))?;
    if res_type != "healthcheck" && res_type != "hostedzone" {
        return Err(invalid_argument(format!(
            "Unknown tag resource type: {res_type}"
        )));
    }
    Ok((res_type, res_id))
}

pub(crate) fn tag_target_exists(account: &AccountState, res_type: &str, res_id: &str) -> bool {
    match res_type {
        "healthcheck" => account.health_checks.contains_key(res_id),
        "hostedzone" => account.hosted_zones.contains_key(res_id),
        _ => false,
    }
}

pub(crate) fn no_such_tag_target(res_type: &str, res_id: &str) -> AwsServiceError {
    let code = match res_type {
        "healthcheck" => "NoSuchHealthCheck",
        "hostedzone" => "NoSuchHostedZone",
        _ => "NoSuchResource",
    };
    aws_error(
        StatusCode::NOT_FOUND,
        code,
        format!("No {res_type} found with ID: {res_id}"),
    )
}

pub(crate) fn push_resource_tag_set(
    out: &mut String,
    res_type: &str,
    res_id: &str,
    bag: &BTreeMap<String, String>,
) {
    out.push_str("<ResourceTagSet>");
    out.push_str(&format!("<ResourceType>{}</ResourceType>", esc(res_type)));
    out.push_str(&format!("<ResourceId>{}</ResourceId>", esc(res_id)));
    out.push_str("<Tags>");
    let mut pairs: Vec<(&String, &String)> = bag.iter().collect();
    pairs.sort_by(|a, b| a.0.cmp(b.0));
    for (k, v) in pairs {
        out.push_str("<Tag>");
        out.push_str(&format!("<Key>{}</Key>", esc(k)));
        out.push_str(&format!("<Value>{}</Value>", esc(v)));
        out.push_str("</Tag>");
    }
    out.push_str("</Tags>");
    out.push_str("</ResourceTagSet>");
}

#[derive(Debug, Clone)]
pub(crate) struct GeoLocationEntry {
    continent_code: &'static str,
    continent_name: &'static str,
    country_code: &'static str,
    country_name: &'static str,
    subdivision_code: &'static str,
    subdivision_name: &'static str,
}

pub(crate) fn push_geo_location_details(out: &mut String, g: &GeoLocationEntry) {
    out.push_str("<GeoLocationDetails>");
    if !g.continent_code.is_empty() {
        out.push_str(&format!(
            "<ContinentCode>{}</ContinentCode>",
            esc(g.continent_code)
        ));
    }
    if !g.continent_name.is_empty() {
        out.push_str(&format!(
            "<ContinentName>{}</ContinentName>",
            esc(g.continent_name)
        ));
    }
    if !g.country_code.is_empty() {
        out.push_str(&format!(
            "<CountryCode>{}</CountryCode>",
            esc(g.country_code)
        ));
    }
    if !g.country_name.is_empty() {
        out.push_str(&format!(
            "<CountryName>{}</CountryName>",
            esc(g.country_name)
        ));
    }
    if !g.subdivision_code.is_empty() {
        out.push_str(&format!(
            "<SubdivisionCode>{}</SubdivisionCode>",
            esc(g.subdivision_code)
        ));
    }
    if !g.subdivision_name.is_empty() {
        out.push_str(&format!(
            "<SubdivisionName>{}</SubdivisionName>",
            esc(g.subdivision_name)
        ));
    }
    out.push_str("</GeoLocationDetails>");
}

/// A representative slice of the geographic locations Route 53 supports
/// for geolocation routing. fakecloud does not track every ISO-3166-1
/// country (Route 53 supports 200+) — this list is enough to exercise
/// continent / country / subdivision pagination with deterministic
/// values across CI runs. `*` is the AWS-documented fallback "default
/// resource" continent code.
pub(crate) fn geo_locations() -> &'static [GeoLocationEntry] {
    &[
        GeoLocationEntry {
            continent_code: "AF",
            continent_name: "Africa",
            country_code: "",
            country_name: "",
            subdivision_code: "",
            subdivision_name: "",
        },
        GeoLocationEntry {
            continent_code: "AN",
            continent_name: "Antarctica",
            country_code: "",
            country_name: "",
            subdivision_code: "",
            subdivision_name: "",
        },
        GeoLocationEntry {
            continent_code: "AS",
            continent_name: "Asia",
            country_code: "",
            country_name: "",
            subdivision_code: "",
            subdivision_name: "",
        },
        GeoLocationEntry {
            continent_code: "EU",
            continent_name: "Europe",
            country_code: "",
            country_name: "",
            subdivision_code: "",
            subdivision_name: "",
        },
        GeoLocationEntry {
            continent_code: "NA",
            continent_name: "North America",
            country_code: "",
            country_name: "",
            subdivision_code: "",
            subdivision_name: "",
        },
        GeoLocationEntry {
            continent_code: "OC",
            continent_name: "Oceania",
            country_code: "",
            country_name: "",
            subdivision_code: "",
            subdivision_name: "",
        },
        GeoLocationEntry {
            continent_code: "SA",
            continent_name: "South America",
            country_code: "",
            country_name: "",
            subdivision_code: "",
            subdivision_name: "",
        },
        GeoLocationEntry {
            continent_code: "",
            continent_name: "",
            country_code: "*",
            country_name: "Default",
            subdivision_code: "",
            subdivision_name: "",
        },
        GeoLocationEntry {
            continent_code: "",
            continent_name: "",
            country_code: "BR",
            country_name: "Brazil",
            subdivision_code: "",
            subdivision_name: "",
        },
        GeoLocationEntry {
            continent_code: "",
            continent_name: "",
            country_code: "CA",
            country_name: "Canada",
            subdivision_code: "",
            subdivision_name: "",
        },
        GeoLocationEntry {
            continent_code: "",
            continent_name: "",
            country_code: "DE",
            country_name: "Germany",
            subdivision_code: "",
            subdivision_name: "",
        },
        GeoLocationEntry {
            continent_code: "",
            continent_name: "",
            country_code: "FR",
            country_name: "France",
            subdivision_code: "",
            subdivision_name: "",
        },
        GeoLocationEntry {
            continent_code: "",
            continent_name: "",
            country_code: "GB",
            country_name: "United Kingdom",
            subdivision_code: "",
            subdivision_name: "",
        },
        GeoLocationEntry {
            continent_code: "",
            continent_name: "",
            country_code: "JP",
            country_name: "Japan",
            subdivision_code: "",
            subdivision_name: "",
        },
        GeoLocationEntry {
            continent_code: "",
            continent_name: "",
            country_code: "US",
            country_name: "United States",
            subdivision_code: "",
            subdivision_name: "",
        },
        GeoLocationEntry {
            continent_code: "",
            continent_name: "",
            country_code: "US",
            country_name: "United States",
            subdivision_code: "CA",
            subdivision_name: "California",
        },
        GeoLocationEntry {
            continent_code: "",
            continent_name: "",
            country_code: "US",
            country_name: "United States",
            subdivision_code: "NY",
            subdivision_name: "New York",
        },
        GeoLocationEntry {
            continent_code: "",
            continent_name: "",
            country_code: "US",
            country_name: "United States",
            subdivision_code: "TX",
            subdivision_name: "Texas",
        },
        GeoLocationEntry {
            continent_code: "",
            continent_name: "",
            country_code: "US",
            country_name: "United States",
            subdivision_code: "WA",
            subdivision_name: "Washington",
        },
    ]
}

/// Parse a CloudWatch Logs log-group ARN into `(account_id, region,
/// log_group_name)`. Returns `None` when the ARN doesn't match the
/// expected service-prefixed shape so the query-log delivery path
/// silently no-ops on garbage input rather than panicking.
///
/// Accepted shape:
///
/// ```text
/// arn:aws:logs:<region>:<account>:log-group:<group-name>[:*]
/// ```
/// Wire enumeration of every Route 53 resource record set type
/// (`smithy.api#enumValue` literals from the `RRType` shape). Used by
/// list endpoints that take an `RRType` filter via `@httpQuery` so
/// unknown values are rejected the same way AWS rejects them.
pub(crate) const RR_TYPES: [&str; 17] = [
    "SOA", "A", "TXT", "NS", "CNAME", "MX", "NAPTR", "PTR", "SRV", "SPF", "AAAA", "CAA", "DS",
    "TLSA", "SSHFP", "SVCB", "HTTPS",
];

/// Wire enumeration of every `VPCRegion` value. Route 53 accepts VPCs in
/// any of these regions; anything else must be rejected so the conformance
/// probe sees an `InvalidInput` instead of a happy-path 200.
pub(crate) const VPC_REGIONS: [&str; 46] = [
    "us-east-1",
    "us-east-2",
    "us-west-1",
    "us-west-2",
    "eu-west-1",
    "eu-west-2",
    "eu-west-3",
    "eu-central-1",
    "eu-central-2",
    "ap-east-1",
    "me-south-1",
    "us-gov-west-1",
    "us-gov-east-1",
    "us-iso-east-1",
    "us-iso-west-1",
    "us-isob-east-1",
    "me-central-1",
    "ap-southeast-1",
    "ap-southeast-2",
    "ap-southeast-3",
    "ap-south-1",
    "ap-south-2",
    "ap-northeast-1",
    "ap-northeast-2",
    "ap-northeast-3",
    "eu-north-1",
    "sa-east-1",
    "ca-central-1",
    "cn-north-1",
    "cn-northwest-1",
    "af-south-1",
    "eu-south-1",
    "eu-south-2",
    "ap-southeast-4",
    "il-central-1",
    "ca-west-1",
    "ap-southeast-5",
    "mx-central-1",
    "us-isof-south-1",
    "us-isof-east-1",
    "ap-southeast-7",
    "ap-east-2",
    "eu-isoe-west-1",
    "ap-southeast-6",
    "us-isob-west-1",
    "eusc-de-east-1",
];

/// Shared `maxitems` constraint: Route 53 advertises a per-page ceiling
/// of 100 across its `maxitems`-paginated list APIs and rejects values
/// outside `[1, 100]` (or any non-integer) with `InvalidInput`. Reuse
/// this from every `Max*`-paginated handler so an out-of-range
/// `?maxitems=-1` doesn't silently fall back to the default of 100.
pub(crate) const MAX_ITEMS_CONSTRAINT: QueryConstraint = QueryConstraint::IntRange {
    key: "maxitems",
    min: 1,
    max: 100,
};

/// Same as [`MAX_ITEMS_CONSTRAINT`] but for the few endpoints that
/// expose the parameter as `?maxresults=` (Route 53's CIDR collection
/// and query-logging list APIs).
pub(crate) const MAX_RESULTS_CONSTRAINT: QueryConstraint = QueryConstraint::IntRange {
    key: "maxresults",
    min: 1,
    max: 100,
};

/// Constraint spec for a single Route 53 list-style query parameter.
///
/// Route 53 reads pagination markers, filters, and per-page sizes from the
/// URL's query string (`@httpQuery` bindings in the Smithy model). The
/// negative-conformance probes inject out-of-range values for those query
/// parameters (e.g. a 1025-char `marker=`, an empty `startcountrycode=`,
/// `version=0`, an unknown `trafficpolicyinstancetype=ZZZ`). AWS rejects
/// such requests with `InvalidInput`; we mirror that here so the probes
/// see the same wire response.
///
/// `min`/`max` apply when the parameter is present (an absent param is
/// always valid — these are all optional in the model). `min = 0` means
/// the empty string is allowed; `min > 0` requires at least one char.
pub(crate) enum QueryConstraint {
    /// String value: length must satisfy `min..=max` inclusive.
    StrLen {
        key: &'static str,
        min: usize,
        max: usize,
    },
    /// Integer value: must parse and satisfy `min..=max` inclusive.
    IntRange {
        key: &'static str,
        min: i64,
        max: i64,
    },
    /// String value: must be one of `allowed` (case-sensitive, matching
    /// the Smithy `@enumValue` wire forms).
    Enum {
        key: &'static str,
        allowed: &'static [&'static str],
    },
}

/// Validate each `@httpQuery`-bound parameter against its Smithy constraints.
///
/// Returns `Err(InvalidInput)` on the first violation. Centralising the
/// checks keeps the eleven list handlers small while ensuring every
/// negative variant generated by the conformance harness hits an honest
/// 4xx instead of a happy-path 200 with an empty body.
pub(crate) fn validate_query_constraints(
    query_params: &std::collections::HashMap<String, String>,
    constraints: &[QueryConstraint],
) -> Result<(), AwsServiceError> {
    for c in constraints {
        match c {
            QueryConstraint::StrLen { key, min, max } => {
                if let Some(v) = query_params.get(*key) {
                    let len = v.chars().count();
                    if len < *min {
                        return Err(invalid_argument(format!(
                            "'{key}' value '{v}' at length {len} below minimum field length {min}"
                        )));
                    }
                    if len > *max {
                        return Err(invalid_argument(format!(
                            "'{key}' value at length {len} exceeds maximum field length {max}"
                        )));
                    }
                }
            }
            QueryConstraint::IntRange { key, min, max } => {
                if let Some(v) = query_params.get(*key) {
                    let parsed: i64 = v.parse().map_err(|_| {
                        invalid_argument(format!("'{key}' value '{v}' is not a valid integer"))
                    })?;
                    if parsed < *min || parsed > *max {
                        return Err(invalid_argument(format!(
                            "'{key}' value {parsed} outside allowed range [{min}, {max}]"
                        )));
                    }
                }
            }
            QueryConstraint::Enum { key, allowed } => {
                if let Some(v) = query_params.get(*key) {
                    if !allowed.contains(&v.as_str()) {
                        return Err(invalid_argument(format!(
                            "'{key}' value '{v}' is not one of {allowed:?}"
                        )));
                    }
                }
            }
        }
    }
    Ok(())
}

pub(crate) fn parse_log_group_arn(arn: &str) -> Option<(String, String, String)> {
    // arn:aws:logs:us-east-1:000000000000:log-group:/route53/qlog
    let mut parts = arn.splitn(7, ':');
    let p0 = parts.next()?;
    let p1 = parts.next()?;
    let p2 = parts.next()?;
    let region = parts.next()?;
    let account = parts.next()?;
    let p5 = parts.next()?;
    let group_with_optional_suffix = parts.next()?;
    if p0 != "arn" || p1 != "aws" || p2 != "logs" || p5 != "log-group" {
        return None;
    }
    // Trim a trailing `:*` if the caller used the wildcard form.
    let group = group_with_optional_suffix
        .trim_end_matches(":*")
        .to_string();
    if group.is_empty() {
        return None;
    }
    Some((account.to_string(), region.to_string(), group))
}

#[cfg(test)]
mod log_arn_tests {
    use super::parse_log_group_arn;

    #[test]
    fn parses_basic_arn() {
        let parsed =
            parse_log_group_arn("arn:aws:logs:us-east-1:000000000000:log-group:/route53/qlog")
                .unwrap();
        assert_eq!(parsed.0, "000000000000");
        assert_eq!(parsed.1, "us-east-1");
        assert_eq!(parsed.2, "/route53/qlog");
    }

    #[test]
    fn parses_wildcard_suffix() {
        let parsed =
            parse_log_group_arn("arn:aws:logs:eu-west-1:111111111111:log-group:/route53/qlog:*")
                .unwrap();
        assert_eq!(parsed.2, "/route53/qlog");
    }

    #[test]
    fn rejects_non_logs_arn() {
        assert!(parse_log_group_arn("arn:aws:s3:::my-bucket").is_none());
        assert!(parse_log_group_arn("not-an-arn").is_none());
    }
}

#[cfg(test)]
mod hardening_tests {
    use super::*;

    fn rr(vals: &[&str]) -> Option<crate::model::ResourceRecords> {
        Some(crate::model::ResourceRecords {
            resource_record: vals
                .iter()
                .map(|v| crate::model::ResourceRecord {
                    value: v.to_string(),
                })
                .collect(),
        })
    }

    #[test]
    fn reverse_dns_key_reverses_labels() {
        // Labels are reversed and joined with NUL.
        assert_eq!(reverse_dns_key("www.example.com."), "com\0example\0www");
        assert_eq!(reverse_dns_key("example.com"), "com\0example");
        assert_eq!(reverse_dns_key("WWW.Example.COM"), "com\0example\0www");
        // Zone apex sorts before its subdomains under the reversed key.
        assert!(reverse_dns_key("example.com.") < reverse_dns_key("a.example.com."));
    }

    // RFC 4034: end-of-label sorts before any octet, so a label that is a proper
    // prefix of a sibling sorts first even when the sibling's next octet is
    // below '.' (0x2E). `a` < `a-` -> `a.example.com.` < `a-.example.com.`.
    #[test]
    fn reverse_dns_key_orders_end_of_label_before_low_octet() {
        assert!(reverse_dns_key("a.example.com.") < reverse_dns_key("a-.example.com."));
        // And a deeper name under `a` still sorts before the `a-` sibling.
        assert!(reverse_dns_key("z.a.example.com.") < reverse_dns_key("a-.example.com."));
    }

    #[test]
    fn rrset_values_match_requires_exact_values_and_ttl() {
        let cur = ResourceRecordSet {
            name: "a.example.com.".into(),
            record_type: "A".into(),
            ttl: Some(300),
            resource_records: rr(&["1.2.3.4"]),
            ..Default::default()
        };
        let same = ResourceRecordSet {
            resource_records: rr(&["1.2.3.4"]),
            ..cur.clone()
        };
        let diff_val = ResourceRecordSet {
            resource_records: rr(&["9.9.9.9"]),
            ..cur.clone()
        };
        let diff_ttl = ResourceRecordSet {
            ttl: Some(60),
            ..cur.clone()
        };
        assert!(rrset_values_match(&cur, &same));
        assert!(!rrset_values_match(&cur, &diff_val));
        assert!(!rrset_values_match(&cur, &diff_ttl));

        // A DELETE that omits TTL for a standard record does NOT match — Route
        // 53 requires the TTL on a non-alias delete.
        let no_ttl = ResourceRecordSet {
            ttl: None,
            resource_records: rr(&["1.2.3.4"]),
            ..cur.clone()
        };
        assert!(!rrset_values_match(&cur, &no_ttl));

        // Alias records have no TTL; TTL is ignored when matching an alias.
        let alias_cur = ResourceRecordSet {
            name: "alias.example.com.".into(),
            record_type: "A".into(),
            ttl: None,
            resource_records: None,
            alias_target: Some(crate::model::AliasTarget {
                hosted_zone_id: "Z1".into(),
                dns_name: "target.example.com.".into(),
                evaluate_target_health: false,
            }),
            ..Default::default()
        };
        let alias_want = ResourceRecordSet {
            ttl: None,
            ..alias_cur.clone()
        };
        assert!(rrset_values_match(&alias_cur, &alias_want));
    }

    #[test]
    fn validate_rrset_rejects_out_of_zone_bad_type_and_empty() {
        let good = ResourceRecordSet {
            name: "a.example.com.".into(),
            record_type: "A".into(),
            resource_records: rr(&["1.2.3.4"]),
            ..Default::default()
        };
        assert!(validate_rrset_in_zone(&good, "example.com.").is_ok());

        let out_of_zone = ResourceRecordSet {
            name: "a.other.org.".into(),
            ..good.clone()
        };
        assert!(validate_rrset_in_zone(&out_of_zone, "example.com.").is_err());

        let bad_type = ResourceRecordSet {
            record_type: "ZZZ".into(),
            ..good.clone()
        };
        assert!(validate_rrset_in_zone(&bad_type, "example.com.").is_err());

        let empty = ResourceRecordSet {
            name: "a.example.com.".into(),
            record_type: "A".into(),
            resource_records: None,
            alias_target: None,
            ..Default::default()
        };
        assert!(validate_rrset_in_zone(&empty, "example.com.").is_err());
    }

    #[test]
    fn materialize_policy_rrset_extracts_endpoint_values() {
        let doc = r#"{"AWSPolicyFormatVersion":"2015-10-01","RecordType":"A","Endpoints":{"a":{"Type":"value","Value":"1.1.1.1"},"b":{"Type":"value","Value":"2.2.2.2"}},"StartEndpoint":"a"}"#;
        let rs = materialize_policy_rrset(doc, "app.example.com.", "A", 60, "tpi-1");
        assert_eq!(rs.traffic_policy_instance_id.as_deref(), Some("tpi-1"));
        assert_eq!(rs.ttl, Some(60));
        let vals: Vec<String> = rs
            .resource_records
            .unwrap()
            .resource_record
            .into_iter()
            .map(|r| r.value)
            .collect();
        assert_eq!(vals, vec!["1.1.1.1".to_string(), "2.2.2.2".to_string()]);
    }
}
