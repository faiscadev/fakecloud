use super::*;

/// Validate a single `PutEvents` entry's required fields (`Source`,
/// `DetailType`, `Detail`) and that `Detail` is a well-formed JSON
/// object. Returns the JSON error body AWS surfaces in the matching
/// `Entries[]` slot on failure.
pub(crate) fn validate_put_events_entry(
    source: &str,
    detail_type: &str,
    detail: &str,
) -> Result<(), Value> {
    if source.is_empty() {
        return Err(json!({
            "ErrorCode": "InvalidArgument",
            "ErrorMessage": "Parameter Source is not valid. Reason: Source is a required argument.",
        }));
    }
    if detail_type.is_empty() {
        return Err(json!({
            "ErrorCode": "InvalidArgument",
            "ErrorMessage": "Parameter DetailType is not valid. Reason: DetailType is a required argument.",
        }));
    }
    if detail.is_empty() {
        return Err(json!({
            "ErrorCode": "InvalidArgument",
            "ErrorMessage": "Parameter Detail is not valid. Reason: Detail is a required argument.",
        }));
    }
    // AWS requires Detail to be a well-formed JSON *object* (`{...}`).
    // A syntactically valid but non-object payload (e.g. `"a string"`,
    // `123`, or `[...]`) is rejected with MalformedDetail, matching the
    // real service; previously any valid JSON was accepted.
    match serde_json::from_str::<Value>(detail) {
        Ok(v) if v.is_object() => {}
        _ => {
            return Err(json!({
                "ErrorCode": "MalformedDetail",
                "ErrorMessage": "Detail is malformed.",
            }));
        }
    }
    Ok(())
}

/// Parse an entry's `Time` field, tolerating the three formats AWS
/// accepts (RFC 3339 string, fractional seconds as a float, integer
/// seconds). Falls back to "now" if the field is absent or
/// unparseable, which matches the real service.
/// Re-stamp the region segment of an ARN with the caller's request region.
///
/// The default event bus is created at account-state bootstrap, which only
/// has access to the server's frozen startup region — not the caller's
/// credential-scope region. Custom buses created through `CreateEventBus`
/// already carry the request region, so this rewrite is idempotent for them
/// and only corrects the bootstrap default bus when a client is configured
/// for a non-default region. ARNs that don't have the expected
/// `arn:partition:service:region:account:resource` shape are returned
/// unchanged.
pub(crate) fn arn_with_request_region(arn: &str, region: &str) -> String {
    let parts: Vec<&str> = arn.splitn(6, ':').collect();
    if parts.len() == 6 && parts[0] == "arn" {
        format!(
            "{}:{}:{}:{}:{}:{}",
            parts[0], parts[1], parts[2], region, parts[4], parts[5]
        )
    } else {
        arn.to_string()
    }
}

pub(crate) fn parse_put_events_time(raw: &Value) -> DateTime<Utc> {
    if let Some(s) = raw.as_str() {
        return DateTime::parse_from_rfc3339(s)
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now());
    }
    if let Some(ts) = raw.as_f64() {
        return DateTime::from_timestamp(ts as i64, ((ts.fract()) * 1_000_000_000.0) as u32)
            .unwrap_or_else(Utc::now);
    }
    if let Some(ts) = raw.as_i64() {
        return DateTime::from_timestamp(ts, 0).unwrap_or_else(Utc::now);
    }
    Utc::now()
}

/// Actions that mutate EventBridge state.
pub(crate) fn is_mutating_action(action: &str) -> bool {
    matches!(
        action,
        "CreateEventBus"
            | "DeleteEventBus"
            | "UpdateEventBus"
            | "PutRule"
            | "DeleteRule"
            | "EnableRule"
            | "DisableRule"
            | "PutTargets"
            | "RemoveTargets"
            | "PutEvents"
            | "PutPermission"
            | "RemovePermission"
            | "TagResource"
            | "UntagResource"
            | "CreateArchive"
            | "UpdateArchive"
            | "DeleteArchive"
            | "CreateConnection"
            | "UpdateConnection"
            | "DeleteConnection"
            | "DeauthorizeConnection"
            | "CreateApiDestination"
            | "UpdateApiDestination"
            | "DeleteApiDestination"
            | "StartReplay"
            | "CancelReplay"
            | "CreatePartnerEventSource"
            | "DeletePartnerEventSource"
            | "ActivateEventSource"
            | "DeactivateEventSource"
            | "PutPartnerEvents"
            | "CreateEndpoint"
            | "DeleteEndpoint"
            | "UpdateEndpoint"
    )
}

pub(crate) fn parse_tags(body: &Value) -> BTreeMap<String, String> {
    let mut tags = BTreeMap::new();
    if let Some(arr) = body["Tags"].as_array() {
        for tag in arr {
            if let (Some(key), Some(val)) = (tag["Key"].as_str(), tag["Value"].as_str()) {
                tags.insert(key.to_string(), val.to_string());
            }
        }
    }
    tags
}

pub fn parse_target(target: &Value) -> EventTarget {
    EventTarget {
        id: target["Id"].as_str().unwrap_or("").to_string(),
        arn: target["Arn"].as_str().unwrap_or("").to_string(),
        input: target["Input"].as_str().map(|s| s.to_string()),
        input_path: target["InputPath"].as_str().map(|s| s.to_string()),
        input_transformer: target.get("InputTransformer").cloned(),
        sqs_parameters: target.get("SqsParameters").cloned(),
        role_arn: target["RoleArn"].as_str().map(|s| s.to_string()),
        dead_letter_config: target.get("DeadLetterConfig").cloned(),
        retry_policy: target.get("RetryPolicy").cloned(),
        ecs_parameters: target.get("EcsParameters").cloned(),
        batch_parameters: target.get("BatchParameters").cloned(),
        kinesis_parameters: target.get("KinesisParameters").cloned(),
        redshift_data_parameters: target.get("RedshiftDataParameters").cloned(),
        http_parameters: target.get("HttpParameters").cloned(),
        sage_maker_pipeline_parameters: target.get("SageMakerPipelineParameters").cloned(),
        app_sync_parameters: target.get("AppSyncParameters").cloned(),
        run_command_parameters: target.get("RunCommandParameters").cloned(),
    }
}

pub(crate) fn target_to_json(t: &EventTarget) -> Value {
    let mut obj = json!({ "Id": t.id, "Arn": t.arn });
    if let Some(ref input) = t.input {
        obj["Input"] = json!(input);
    }
    if let Some(ref input_path) = t.input_path {
        obj["InputPath"] = json!(input_path);
    }
    if let Some(ref it) = t.input_transformer {
        obj["InputTransformer"] = it.clone();
    }
    if let Some(ref sp) = t.sqs_parameters {
        obj["SqsParameters"] = sp.clone();
    }
    if let Some(ref ra) = t.role_arn {
        obj["RoleArn"] = json!(ra);
    }
    if let Some(ref dlc) = t.dead_letter_config {
        obj["DeadLetterConfig"] = dlc.clone();
    }
    if let Some(ref rp) = t.retry_policy {
        obj["RetryPolicy"] = rp.clone();
    }
    if let Some(ref p) = t.ecs_parameters {
        obj["EcsParameters"] = p.clone();
    }
    if let Some(ref p) = t.batch_parameters {
        obj["BatchParameters"] = p.clone();
    }
    if let Some(ref p) = t.kinesis_parameters {
        obj["KinesisParameters"] = p.clone();
    }
    if let Some(ref p) = t.redshift_data_parameters {
        obj["RedshiftDataParameters"] = p.clone();
    }
    if let Some(ref p) = t.http_parameters {
        obj["HttpParameters"] = p.clone();
    }
    if let Some(ref p) = t.sage_maker_pipeline_parameters {
        obj["SageMakerPipelineParameters"] = p.clone();
    }
    if let Some(ref p) = t.app_sync_parameters {
        obj["AppSyncParameters"] = p.clone();
    }
    if let Some(ref p) = t.run_command_parameters {
        obj["RunCommandParameters"] = p.clone();
    }
    obj
}

pub(crate) fn find_tags_mut<'a>(
    state: &'a mut crate::state::EventBridgeState,
    arn: &str,
) -> Result<&'a mut BTreeMap<String, String>, AwsServiceError> {
    // Check buses
    for bus in state.buses.values_mut() {
        if bus.arn == arn {
            return Ok(&mut bus.tags);
        }
    }
    // Check rules
    for rule in state.rules.values_mut() {
        if rule.arn == arn {
            return Ok(&mut rule.tags);
        }
    }

    // Parse ARN to give better error messages
    let error_msg = if arn.contains(":rule/") {
        // Extract rule name and bus from ARN
        let parts: Vec<&str> = arn.rsplitn(2, ":rule/").collect();
        if let Some(rule_path) = parts.first() {
            if let Some((bus, rule_name)) = rule_path.rsplit_once('/') {
                format!("Rule {rule_name} does not exist on EventBus {bus}.")
            } else {
                format!("Rule {} does not exist on EventBus default.", rule_path)
            }
        } else {
            format!("Resource {arn} not found.")
        }
    } else {
        format!("Resource {arn} not found.")
    };

    Err(AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ResourceNotFoundException",
        error_msg,
    ))
}

pub(crate) fn find_tags<'a>(
    state: &'a crate::state::EventBridgeState,
    arn: &str,
) -> Result<&'a BTreeMap<String, String>, AwsServiceError> {
    for bus in state.buses.values() {
        if bus.arn == arn {
            return Ok(&bus.tags);
        }
    }
    for rule in state.rules.values() {
        if rule.arn == arn {
            return Ok(&rule.tags);
        }
    }

    let error_msg = if arn.contains(":rule/") {
        let parts: Vec<&str> = arn.rsplitn(2, ":rule/").collect();
        if let Some(rule_path) = parts.first() {
            if let Some((bus, rule_name)) = rule_path.rsplit_once('/') {
                format!("Rule {rule_name} does not exist on EventBus {bus}.")
            } else {
                format!("Rule {} does not exist on EventBus default.", rule_path)
            }
        } else {
            format!("Resource {arn} not found.")
        }
    } else {
        format!("Resource {arn} not found.")
    };

    Err(AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ResourceNotFoundException",
        error_msg,
    ))
}

pub(crate) fn validate_event_pattern(pattern: &str) -> Result<(), AwsServiceError> {
    let parsed: Value = serde_json::from_str(pattern).map_err(|_| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidEventPatternException",
            "Event pattern is not valid. Reason: Invalid JSON",
        )
    })?;

    validate_pattern_values(&parsed, "")?;
    Ok(())
}

pub(crate) fn validate_pattern_values(value: &Value, path: &str) -> Result<(), AwsServiceError> {
    match value {
        Value::Object(obj) => {
            for (key, val) in obj {
                let new_path = if path.is_empty() {
                    key.clone()
                } else {
                    format!("{path}.{key}")
                };
                match val {
                    Value::Object(_) => validate_pattern_values(val, &new_path)?,
                    Value::Array(items) => {
                        // Validate matcher objects that appear in a leaf list
                        // (e.g. `{"numeric": [...]}`), rejecting malformed
                        // ones at PutRule/TestEventPattern time.
                        for item in items {
                            if let Value::Object(m) = item {
                                if let Some(num) = m.get("numeric") {
                                    validate_numeric_matcher(num, &new_path)?;
                                }
                            }
                        }
                    }
                    _ => {
                        return Err(AwsServiceError::aws_error(
                            StatusCode::BAD_REQUEST,
                            "InvalidEventPatternException",
                            format!(
                                "Event pattern is not valid. Reason: '{}' must be an object or an array",
                                key
                            ),
                        ));
                    }
                }
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

pub(crate) fn build_auth_params_response(auth_type: &str, params: &Value) -> Value {
    let mut resp = match auth_type {
        "API_KEY" => {
            let mut resp = json!({});
            if let Some(api_key) = params.get("ApiKeyAuthParameters") {
                resp["ApiKeyAuthParameters"] = json!({
                    "ApiKeyName": api_key["ApiKeyName"],
                });
            }
            resp
        }
        "BASIC" => {
            let mut resp = json!({});
            if let Some(basic) = params.get("BasicAuthParameters") {
                resp["BasicAuthParameters"] = json!({
                    "Username": basic["Username"],
                });
            }
            resp
        }
        "OAUTH_CLIENT_CREDENTIALS" => {
            let mut resp = json!({});
            if let Some(oauth) = params.get("OAuthParameters") {
                resp["OAuthParameters"] = json!({
                    "AuthorizationEndpoint": oauth["AuthorizationEndpoint"],
                    "HttpMethod": oauth["HttpMethod"],
                    "ClientParameters": {
                        "ClientID": oauth.get("ClientParameters").and_then(|c| c.get("ClientID")),
                    },
                });
            }
            resp
        }
        _ => return params.clone(),
    };

    // Echo the connection-level InvocationHttpParameters (additional custom
    // headers / query-string / body parameters merged into every invocation).
    // Previously dropped, which made DescribeConnection lose everything the
    // caller configured beyond the auth block. Secret values are hidden on
    // describe (matching AWS), but keys + IsValueSecret flags round-trip.
    if let Some(inv) = params.get("InvocationHttpParameters") {
        resp["InvocationHttpParameters"] = sanitize_invocation_http_params(inv);
    }
    resp
}

/// Redact secret values from a connection's `InvocationHttpParameters` for
/// read responses. Each parameter keeps its `Key` and `IsValueSecret` flag;
/// the `Value` is only echoed when the parameter is explicitly non-secret.
fn sanitize_invocation_http_params(inv: &Value) -> Value {
    let mut out = json!({});
    for field in [
        "HeaderParameters",
        "QueryStringParameters",
        "BodyParameters",
    ] {
        if let Some(arr) = inv.get(field).and_then(|v| v.as_array()) {
            let sanitized: Vec<Value> = arr
                .iter()
                .map(|p| {
                    let is_secret = p
                        .get("IsValueSecret")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(true);
                    let mut entry = json!({
                        "Key": p.get("Key").cloned().unwrap_or(Value::Null),
                        "IsValueSecret": is_secret,
                    });
                    if !is_secret {
                        if let Some(val) = p.get("Value") {
                            entry["Value"] = val.clone();
                        }
                    }
                    entry
                })
                .collect();
            out[field] = json!(sanitized);
        }
    }
    out
}

/// Match an event against an EventBridge event pattern.
///
/// `id` and `time` are the event's generated envelope fields; they are
/// woven into the synthetic match event (alongside the constant
/// `version` of `"0"`) so patterns targeting `id`/`time`/`version` are
/// matchable, which they previously were not.
#[allow(clippy::too_many_arguments)]
pub(crate) fn matches_pattern(
    pattern_json: Option<&str>,
    source: &str,
    detail_type: &str,
    detail: &str,
    account: &str,
    region: &str,
    resources: &[String],
    id: &str,
    time: &str,
) -> bool {
    let pattern_json = match pattern_json {
        Some(p) => p,
        None => return true,
    };

    let pattern: Value = match serde_json::from_str(pattern_json) {
        Ok(v) => v,
        Err(_) => return false,
    };

    if !pattern.is_object() {
        return false;
    }

    let detail_value: Value = serde_json::from_str(detail).unwrap_or(json!({}));
    let event = json!({
        "version": "0",
        "id": id,
        "source": source,
        "detail-type": detail_type,
        "detail": detail_value,
        "account": account,
        "region": region,
        "resources": resources,
        "time": time,
    });

    matches_value(&pattern, &event)
}

pub(crate) fn matches_value(pattern: &Value, event_value: &Value) -> bool {
    match pattern {
        Value::Object(obj) => {
            // All sibling keys are ANDed together, and `$or` is a
            // sibling-level alternation group that is *also* ANDed with the
            // rest of the object. Previously `$or` short-circuited and the
            // sibling constraints were never evaluated, so a pattern like
            // `{"source": ["aws.ec2"], "$or": [...]}` matched events from any
            // source as long as one alternative matched.
            for (key, sub_pattern) in obj {
                if key == "$or" {
                    continue;
                }
                let sub_value = &event_value[key];
                if !matches_value(sub_pattern, sub_value) {
                    return false;
                }
            }
            if let Some(Value::Array(alternatives)) = obj.get("$or") {
                if !alternatives
                    .iter()
                    .any(|alt| matches_value(alt, event_value))
                {
                    return false;
                }
            }
            true
        }
        // A content-filter list matches if ANY of its elements matches the
        // event value.
        Value::Array(arr) => arr
            .iter()
            .any(|elem| matches_content_filter(elem, event_value)),
        _ => false,
    }
}

/// Evaluate a single content-filter element against an event value, applying
/// the correct array-vs-matcher semantics.
///
/// Positive matchers (`prefix`, `suffix`, `exists`, plain equality, ...) match
/// if the whole value matches (so presence matchers like `exists` see the
/// array) or, for an array-valued field, if ANY element matches.
///
/// Negation matchers (`anything-but`) invert this: the filter matches only when
/// NONE of the event values are excluded. Against an array field that means
/// EVERY element must individually satisfy the negation. Feeding the whole array
/// to the scalar matcher (as positive matchers do) would always pass -- a
/// scalar forbidden value never equals the whole array -- silently bypassing
/// the filter and over-delivering to targets.
fn matches_content_filter(elem: &Value, event_value: &Value) -> bool {
    if is_negation_matcher(elem) {
        match event_value {
            Value::Array(items) => items.iter().all(|item| matches_single(elem, item)),
            other => matches_single(elem, other),
        }
    } else {
        matches_single(elem, event_value)
            || matches!(event_value, Value::Array(items)
                if items.iter().any(|item| matches_single(elem, item)))
    }
}

/// A content-filter element is a negation matcher when it is an object carrying
/// an `anything-but` key (in any of its sub-forms: string, list, number, or a
/// nested inner matcher).
fn is_negation_matcher(elem: &Value) -> bool {
    matches!(elem, Value::Object(obj) if obj.contains_key("anything-but"))
}

pub(crate) fn matches_single(pattern_elem: &Value, event_value: &Value) -> bool {
    match pattern_elem {
        Value::Object(obj) => {
            if let Some(prefix_val) = obj.get("prefix") {
                if let (Some(prefix), Some(actual)) = (prefix_val.as_str(), event_value.as_str()) {
                    return actual.starts_with(prefix);
                }
                return false;
            }
            if let Some(suffix_val) = obj.get("suffix") {
                if let (Some(suffix), Some(actual)) = (suffix_val.as_str(), event_value.as_str()) {
                    return actual.ends_with(suffix);
                }
                return false;
            }
            if let Some(eqic_val) = obj.get("equals-ignore-case") {
                if let (Some(expected), Some(actual)) = (eqic_val.as_str(), event_value.as_str()) {
                    return expected.eq_ignore_ascii_case(actual);
                }
                return false;
            }
            if let Some(cidr_val) = obj.get("cidr") {
                if let (Some(cidr), Some(actual)) = (cidr_val.as_str(), event_value.as_str()) {
                    return cidr_matches(cidr, actual);
                }
                return false;
            }
            if let Some(wild_val) = obj.get("wildcard") {
                if let (Some(pattern), Some(actual)) = (wild_val.as_str(), event_value.as_str()) {
                    return wildcard_matches(pattern, actual);
                }
                return false;
            }
            if let Some(exists_val) = obj.get("exists") {
                let should_exist = exists_val.as_bool().unwrap_or(true);
                let does_exist = !event_value.is_null();
                return should_exist == does_exist;
            }
            if let Some(anything_but_val) = obj.get("anything-but") {
                return match anything_but_val {
                    Value::String(s) => event_value.as_str() != Some(s.as_str()),
                    Value::Array(arr) => !arr.iter().any(|v| values_equal(v, event_value)),
                    Value::Number(_) => event_value != anything_but_val,
                    // Nested-matcher negation, e.g.
                    // {"anything-but": {"prefix": "x"}} matches when the
                    // field does NOT start with "x". Each inner matcher
                    // accepts a single string or a list of strings; `cidr`
                    // and `wildcard` are supported too. An unknown/malformed
                    // inner matcher is conservatively treated as excluding
                    // the event rather than always delivering it (which the
                    // previous `else => true` did).
                    Value::Object(nested) => matches_anything_but_nested(nested, event_value),
                    _ => true,
                };
            }
            if let Some(numeric_val) = obj.get("numeric") {
                return matches_numeric(numeric_val, event_value);
            }
            false
        }
        _ => values_equal(pattern_elem, event_value),
    }
}

/// Evaluate a nested `anything-but` matcher (e.g.
/// `{"anything-but": {"prefix": "x"}}`). Returns `true` when the field does
/// NOT satisfy the inner matcher. Each inner matcher accepts a single string
/// or a list of strings. Unknown or malformed inner matchers return `false`
/// (exclude the event) so a filter the caller intended never over-delivers.
fn matches_anything_but_nested(
    nested: &serde_json::Map<String, Value>,
    event_value: &Value,
) -> bool {
    let fv = event_value.as_str();
    // Apply `pred` to a single-string or list-of-strings inner value and
    // return whether ANY entry matched. A non-string inner value is
    // malformed and reported as "no decision" via None.
    fn any_str_hit(v: &Value, pred: &dyn Fn(&str) -> bool) -> Option<bool> {
        match v {
            Value::String(s) => Some(pred(s)),
            Value::Array(a) => Some(a.iter().filter_map(|x| x.as_str()).any(pred)),
            _ => None,
        }
    }

    if let Some(p) = nested.get("prefix") {
        match any_str_hit(p, &|s| fv.is_some_and(|x| x.starts_with(s))) {
            Some(hit) => !hit,
            None => false,
        }
    } else if let Some(p) = nested.get("suffix") {
        match any_str_hit(p, &|s| fv.is_some_and(|x| x.ends_with(s))) {
            Some(hit) => !hit,
            None => false,
        }
    } else if let Some(p) = nested.get("equals-ignore-case") {
        match any_str_hit(p, &|s| fv.is_some_and(|x| x.eq_ignore_ascii_case(s))) {
            Some(hit) => !hit,
            None => false,
        }
    } else if let Some(p) = nested.get("wildcard") {
        match any_str_hit(p, &|s| fv.is_some_and(|x| wildcard_matches(s, x))) {
            Some(hit) => !hit,
            None => false,
        }
    } else if let Some(c) = nested.get("cidr").and_then(|v| v.as_str()) {
        !fv.is_some_and(|x| cidr_matches(c, x))
    } else {
        // Unknown / unsupported nested matcher: cannot evaluate, so
        // conservatively exclude rather than deliver.
        false
    }
}

/// `wildcard` matcher: `*` matches any run of characters (including empty);
/// `\*` is a literal asterisk.
pub(crate) fn wildcard_matches(pattern: &str, actual: &str) -> bool {
    let mut segments: Vec<String> = Vec::new();
    let mut current = String::new();
    let mut chars = pattern.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            if let Some(next) = chars.next() {
                current.push(next);
            }
        } else if c == '*' {
            segments.push(std::mem::take(&mut current));
        } else {
            current.push(c);
        }
    }
    segments.push(current);

    if segments.len() == 1 {
        return segments[0] == actual;
    }

    let mut pos = 0;
    let first = &segments[0];
    if !actual[pos..].starts_with(first.as_str()) {
        return false;
    }
    pos += first.len();

    let last_idx = segments.len() - 1;
    for (i, seg) in segments.iter().enumerate().skip(1) {
        if i == last_idx {
            // The trailing segment must match the tail.
            if !actual[pos..].ends_with(seg.as_str()) {
                return false;
            }
            return actual.len().saturating_sub(pos) >= seg.len();
        }
        match actual[pos..].find(seg.as_str()) {
            Some(idx) => pos += idx + seg.len(),
            None => return false,
        }
    }
    true
}

/// IPv4 CIDR membership test for the `cidr` filter.
pub(crate) fn cidr_matches(cidr: &str, actual: &str) -> bool {
    let (net_str, prefix_str) = match cidr.split_once('/') {
        Some(parts) => parts,
        None => return false,
    };
    let prefix: u32 = match prefix_str.parse() {
        Ok(p) if p <= 32 => p,
        _ => return false,
    };
    let net = match parse_ipv4(net_str) {
        Some(n) => n,
        None => return false,
    };
    let value = match parse_ipv4(actual) {
        Some(v) => v,
        None => return false,
    };
    if prefix == 0 {
        return true;
    }
    let mask = u32::MAX << (32 - prefix);
    (net & mask) == (value & mask)
}

fn parse_ipv4(s: &str) -> Option<u32> {
    let mut parts = s.split('.');
    let mut result: u32 = 0;
    for _ in 0..4 {
        let octet: u32 = parts.next()?.parse().ok()?;
        if octet > 255 {
            return None;
        }
        result = (result << 8) | octet;
    }
    if parts.next().is_some() {
        return None;
    }
    Some(result)
}

/// For each archive on `event_bus_name` whose event pattern matches the
/// event, append a clone of it to the archive's stored events and bump
/// the archive's counters.
#[allow(clippy::too_many_arguments)]
pub(crate) fn archive_matching_event(
    state: &mut crate::state::EventBridgeState,
    event: &PutEvent,
    event_bus_name: &str,
    source: &str,
    detail_type: &str,
    detail: &str,
    account_id: &str,
    region: &str,
    resources: &[String],
) {
    let archive_keys: Vec<String> = state.archives.keys().cloned().collect();
    for akey in archive_keys {
        let (archive_bus, archive_pattern, archive_enabled) = {
            let a = &state.archives[&akey];
            (
                state.resolve_bus_name(&a.event_source_arn),
                a.event_pattern.clone(),
                a.state == "ENABLED",
            )
        };
        if archive_bus != event_bus_name || !archive_enabled {
            continue;
        }
        let pattern_matches = matches_pattern(
            archive_pattern.as_deref(),
            source,
            detail_type,
            detail,
            account_id,
            region,
            resources,
            &event.event_id,
            &event.time.to_rfc3339(),
        );
        if !pattern_matches {
            continue;
        }
        if let Some(archive) = state.archives.get_mut(&akey) {
            archive.event_count += 1;
            archive.size_bytes += detail.len() as i64;
            archive.events.push(event.clone());
        }
    }
}

/// Walk the named archive, filter events into the replay window, then
/// fan out each event against rules on `bus_name` to collect its
/// matching targets. Returns only events that matched at least one
/// target.
#[allow(clippy::too_many_arguments)]
pub(crate) fn collect_replay_events_with_targets(
    state: &crate::state::EventBridgeState,
    archive_name: &str,
    bus_name: &str,
    event_start_time: DateTime<Utc>,
    event_end_time: DateTime<Utc>,
    account_id: &str,
    region: &str,
) -> Vec<(PutEvent, Vec<EventTarget>)> {
    let Some(archive) = state.archives.get(archive_name) else {
        return Vec::new();
    };

    let replay_events: Vec<PutEvent> = archive
        .events
        .iter()
        .filter(|e| e.time >= event_start_time && e.time < event_end_time)
        .cloned()
        .collect();

    let mut events_to_deliver: Vec<(PutEvent, Vec<EventTarget>)> = Vec::new();
    for event in replay_events {
        let matching_targets: Vec<EventTarget> = state
            .rules
            .values()
            .filter(|r| {
                r.event_bus_name == bus_name
                    && r.state == "ENABLED"
                    && matches_pattern(
                        r.event_pattern.as_deref(),
                        &event.source,
                        &event.detail_type,
                        &event.detail,
                        account_id,
                        region,
                        &event.resources,
                        &event.event_id,
                        &event.time.to_rfc3339(),
                    )
            })
            .flat_map(|r| r.targets.clone())
            .collect();

        if !matching_targets.is_empty() {
            events_to_deliver.push((event, matching_targets));
        }
    }
    events_to_deliver
}

pub(crate) fn matches_numeric(numeric_arr: &Value, event_value: &Value) -> bool {
    let arr = match numeric_arr.as_array() {
        Some(a) => a,
        None => return false,
    };
    // An empty matcher (`{"numeric": []}`) or one with a dangling trailing
    // operator (odd length) is invalid and must not match anything; the
    // old `while i + 1 < arr.len()` loop silently accepted both, so an
    // empty matcher matched every value.
    if arr.is_empty() || arr.len() % 2 != 0 {
        return false;
    }
    let actual = match event_value.as_f64() {
        Some(n) => n,
        None => return false,
    };
    let mut i = 0;
    while i < arr.len() {
        let op = match arr[i].as_str() {
            Some(s) => s,
            None => return false,
        };
        let threshold = match arr[i + 1].as_f64() {
            Some(n) => n,
            None => return false,
        };
        let ok = match op {
            ">" => actual > threshold,
            ">=" => actual >= threshold,
            "<" => actual < threshold,
            "<=" => actual <= threshold,
            // Exact double equality, matching AWS. The previous absolute
            // `f64::EPSILON` comparison was wrong at large magnitudes (e.g.
            // 1e18 vs 1e18 + 1000 compared equal).
            "=" => actual == threshold,
            _ => return false,
        };
        if !ok {
            return false;
        }
        i += 2;
    }
    true
}

/// Validate the shape of a `{"numeric": [...]}` matcher at pattern-validation
/// time so malformed matchers surface as `InvalidEventPatternException`
/// rather than silently never (or always) matching.
fn validate_numeric_matcher(numeric: &Value, path: &str) -> Result<(), AwsServiceError> {
    let invalid = |reason: &str| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidEventPatternException",
            format!("Event pattern is not valid. Reason: {reason} at '{path}'"),
        )
    };
    let arr = numeric
        .as_array()
        .ok_or_else(|| invalid("\"numeric\" must be an array"))?;
    if arr.is_empty() || arr.len() % 2 != 0 {
        return Err(invalid("\"numeric\" must be operator/value pairs"));
    }
    let mut i = 0;
    while i < arr.len() {
        let op = arr[i]
            .as_str()
            .ok_or_else(|| invalid("\"numeric\" operator must be a string"))?;
        if !matches!(op, ">" | ">=" | "<" | "<=" | "=") {
            return Err(invalid("unsupported \"numeric\" operator"));
        }
        if arr[i + 1].as_f64().is_none() {
            return Err(invalid("\"numeric\" value must be a number"));
        }
        i += 2;
    }
    Ok(())
}

pub(crate) fn values_equal(a: &Value, b: &Value) -> bool {
    a == b
}

/// Resolve a simple JSON path like `$.detail.name` against an event JSON value.
pub(crate) fn resolve_json_path(event: &Value, path: &str) -> Option<Value> {
    let path = path.strip_prefix('$').unwrap_or(path);
    let mut current = event;
    for segment in path.split('.') {
        if segment.is_empty() {
            continue;
        }
        current = current.get(segment)?;
    }
    Some(current.clone())
}

/// Apply an EventBridge InputTransformer to an event.
///
/// Besides user-defined `InputPathsMap` variables, EventBridge exposes a set
/// of predefined reserved variables that are resolved here:
///   * `<aws.events.event.json>` — the whole matched event as JSON.
///   * `<aws.events.event.ingestion-time>` — the event's `time` field.
///   * `<aws.events.rule-arn>` — the ARN of the matching rule (when known).
pub(crate) fn apply_input_transformer(
    transformer: &Value,
    event: &Value,
    rule_arn: Option<&str>,
) -> String {
    let input_paths_map = transformer
        .get("InputPathsMap")
        .and_then(|v| v.as_object())
        .cloned()
        .unwrap_or_default();
    let template = transformer
        .get("InputTemplate")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    // Resolve all input paths
    let mut resolved: HashMap<String, Value> = HashMap::new();
    for (var_name, path_val) in &input_paths_map {
        if let Some(path_str) = path_val.as_str() {
            if let Some(val) = resolve_json_path(event, path_str) {
                resolved.insert(var_name.clone(), val);
            }
        }
    }

    // Replace <varName> placeholders in template
    let mut result = template;
    for (var_name, val) in &resolved {
        let placeholder = format!("<{var_name}>");
        let replacement = match val {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        result = result.replace(&placeholder, &replacement);
    }

    // Resolve EventBridge's predefined reserved variables.
    if result.contains("<aws.events.event.json>") {
        result = result.replace("<aws.events.event.json>", &event.to_string());
    }
    if result.contains("<aws.events.event.ingestion-time>") {
        let ingestion_time = event
            .get("time")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| Utc::now().to_rfc3339());
        result = result.replace("<aws.events.event.ingestion-time>", &ingestion_time);
    }
    if let Some(arn) = rule_arn {
        result = result.replace("<aws.events.rule-arn>", arn);
    }

    result
}

pub(crate) fn missing(name: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ValidationException",
        format!("The request must contain the parameter {name}"),
    )
}

/// Extract a Lambda function name from its ARN.
///
/// Handles both unqualified (`arn:aws:lambda:region:account:function:NAME`)
/// and qualified (`arn:aws:lambda:region:account:function:NAME:alias`) ARNs.
pub(crate) fn function_name_from_arn(arn: &str) -> &str {
    let parts: Vec<&str> = arn.split(':').collect();
    if parts.len() >= 7 && parts[5] == "function" {
        parts[6]
    } else {
        arn
    }
}

/// Spawn a background task to invoke a Lambda function via ContainerRuntime.
/// This is fire-and-forget: EventBridge delivery is asynchronous.
pub(crate) fn invoke_lambda_async(
    container_runtime: &Option<Arc<ContainerRuntime>>,
    lambda_state: &Option<SharedLambdaState>,
    function_arn: &str,
    payload: &str,
) {
    let runtime = match container_runtime {
        Some(rt) => rt.clone(),
        None => return,
    };
    let lambda_state = match lambda_state {
        Some(ls) => ls.clone(),
        None => return,
    };
    let func_name = function_name_from_arn(function_arn).to_string();
    let payload = payload.as_bytes().to_vec();

    tokio::spawn(async move {
        let resolved = {
            let accounts = lambda_state.read();
            let state = accounts.default_ref();
            state.functions.get(&func_name).cloned().map(|func| {
                let mut layer_zips: Vec<Vec<u8>> = Vec::with_capacity(func.layers.len());
                for attached in &func.layers {
                    if let Some(bytes) = fakecloud_lambda::extras::parse_layer_version_arn(
                        &attached.arn,
                    )
                    .and_then(|(acct, name, ver)| {
                        accounts
                            .get(&acct)
                            .and_then(|s| s.layers.get(&name))
                            .and_then(|l| l.versions.iter().find(|v| v.version == ver))
                            .and_then(|v| v.code_zip.clone())
                    }) {
                        layer_zips.push(bytes);
                    }
                }
                (func, layer_zips)
            })
        };
        let (func, layer_zips) = match resolved {
            Some(pair) => pair,
            None => {
                tracing::warn!(
                    function = %func_name,
                    "EventBridge Lambda target not found, skipping invocation"
                );
                return;
            }
        };
        match runtime.invoke(&func, &payload, &layer_zips).await {
            Ok(_) => {
                tracing::info!(function = %func_name, "EventBridge Lambda invocation succeeded");
            }
            Err(e) => {
                tracing::warn!(
                    function = %func_name,
                    error = %e,
                    "EventBridge Lambda invocation failed"
                );
            }
        }
    });
}

/// Deliver an EventBridge event to CloudWatch Logs by writing a log event
/// to the appropriate log group and stream.
pub(crate) fn deliver_to_logs(
    logs_state: &SharedLogsState,
    log_group_arn: &str,
    payload: &str,
    timestamp: chrono::DateTime<chrono::Utc>,
) {
    // Extract log group name from ARN: arn:aws:logs:region:account:log-group:NAME
    // or just the name if it's not an ARN
    let group_name = if log_group_arn.contains(":log-group:") {
        log_group_arn
            .split(":log-group:")
            .nth(1)
            .unwrap_or(log_group_arn)
            .trim_end_matches(":*")
    } else {
        log_group_arn
    };

    let stream_name = "events".to_string();
    let ts_millis = timestamp.timestamp_millis();

    let mut accounts = logs_state.write();
    let state = accounts.default_mut();
    let region = state.region.clone();
    let account_id = state.account_id.clone();

    // Auto-create log group and stream if they don't exist
    let group = state
        .log_groups
        .entry(group_name.to_string())
        .or_insert_with(|| fakecloud_logs::LogGroup {
            name: group_name.to_string(),
            arn: Arn::new(
                "logs",
                &region,
                &account_id,
                &format!("log-group:{group_name}"),
            )
            .to_string(),
            creation_time: ts_millis,
            retention_in_days: None,
            kms_key_id: None,
            tags: std::collections::BTreeMap::new(),
            log_streams: std::collections::BTreeMap::new(),
            stored_bytes: 0,
            subscription_filters: Vec::new(),
            data_protection_policy: None,
            index_policies: Vec::new(),
            transformer: None,
            deletion_protection: false,
            log_group_class: Some("STANDARD".to_string()),
        });

    let stream = group
        .log_streams
        .entry(stream_name.clone())
        .or_insert_with(|| fakecloud_logs::LogStream {
            name: stream_name,
            arn: format!("{}:log-stream:events", group.arn),
            creation_time: ts_millis,
            first_event_timestamp: None,
            last_event_timestamp: None,
            last_ingestion_time: None,
            upload_sequence_token: "1".to_string(),
            events: Vec::new(),
        });

    let next_seq = stream.events.iter().map(|e| e.seq).max().unwrap_or(0) + 1;
    stream.events.push(fakecloud_logs::LogEvent {
        timestamp: ts_millis,
        message: payload.to_string(),
        ingestion_time: ts_millis,
        seq: next_seq,
    });
    stream.last_event_timestamp = Some(ts_millis);
    stream.last_ingestion_time = Some(ts_millis);
    if stream.first_event_timestamp.is_none() {
        stream.first_event_timestamp = Some(ts_millis);
    }
}

/// Apply connection auth parameters to an outgoing HTTP request.
pub(crate) fn apply_connection_auth(
    mut builder: reqwest::RequestBuilder,
    conn: &Connection,
) -> reqwest::RequestBuilder {
    match conn.authorization_type.as_str() {
        "API_KEY" => {
            if let Some(params) = conn.auth_parameters.get("ApiKeyAuthParameters") {
                if let (Some(name), Some(value)) = (
                    params["ApiKeyName"].as_str(),
                    params["ApiKeyValue"].as_str(),
                ) {
                    builder = builder.header(name, value);
                }
            }
        }
        "BASIC" => {
            if let Some(params) = conn.auth_parameters.get("BasicAuthParameters") {
                if let (Some(user), Some(pass)) =
                    (params["Username"].as_str(), params["Password"].as_str())
                {
                    builder = builder.basic_auth(user, Some(pass));
                }
            }
        }
        "OAUTH_CLIENT_CREDENTIALS" => {
            // For OAuth, in a real implementation we'd exchange credentials for a token.
            // Here we pass client credentials as basic auth as a reasonable approximation.
            if let Some(params) = conn.auth_parameters.get("OAuthParameters") {
                if let (Some(client_id), Some(client_secret)) = (
                    params["ClientParameters"]["ClientID"].as_str(),
                    params["ClientParameters"]["ClientSecret"].as_str(),
                ) {
                    builder = builder.basic_auth(client_id, Some(client_secret));
                }
            }
        }
        _ => {}
    }
    builder
}

/// Context shared by both put_events (direct) and put_event_in_account
/// (cross-service) when dispatching matched targets. Optional state
/// handles let cross-service callers (which may not be wired with full
/// service plumbing) gracefully degrade — e.g. Lambda dispatch becomes
/// a fire-and-forget log unless `lambda_state` is wired.
pub(crate) struct EventDispatchContext<'a> {
    pub(crate) state: &'a crate::state::SharedEventBridgeState,
    pub(crate) delivery: &'a std::sync::Arc<fakecloud_core::delivery::DeliveryBus>,
    pub(crate) lambda_state: Option<&'a fakecloud_lambda::SharedLambdaState>,
    pub(crate) logs_state: Option<&'a fakecloud_logs::SharedLogsState>,
    pub(crate) container_runtime:
        &'a Option<std::sync::Arc<fakecloud_lambda::runtime::ContainerRuntime>>,
    pub(crate) account_id: &'a str,
    pub(crate) region: &'a str,
}

/// Single-target dispatch shared by direct PutEvents and cross-service
/// put_event_in_account so both honour the same target shape (SQS/SNS/
/// Lambda/Logs/Kinesis/StepFunctions/ApiDestination/HTTP) and the same
/// InputTransformer + InputPath body resolution.
pub(crate) fn dispatch_event_target(
    ctx: &EventDispatchContext,
    target: &crate::state::EventTarget,
    event_json: &Value,
    event_id: &str,
    detail_type: &str,
    rule_arn: Option<&str>,
) {
    let arn = &target.arn;
    let event_str = event_json.to_string();
    let body_str = if let Some(ref transformer) = target.input_transformer {
        apply_input_transformer(transformer, event_json, rule_arn)
    } else if let Some(ref input) = target.input {
        input.clone()
    } else if let Some(ref input_path) = target.input_path {
        resolve_json_path(event_json, input_path)
            .map(|v| v.to_string())
            .unwrap_or_else(|| event_str.clone())
    } else {
        event_str.clone()
    };

    if arn.contains(":sqs:") {
        let group_id = target
            .sqs_parameters
            .as_ref()
            .and_then(|p| p["MessageGroupId"].as_str())
            .map(|s| s.to_string());
        if group_id.is_some() {
            ctx.delivery.send_to_sqs_with_attrs(
                arn,
                &body_str,
                &HashMap::new(),
                group_id.as_deref(),
                None,
            );
        } else {
            ctx.delivery.send_to_sqs(arn, &body_str, &HashMap::new());
        }
    } else if arn.contains(":sns:") {
        ctx.delivery
            .publish_to_sns(arn, &body_str, Some(detail_type));
    } else if arn.contains(":lambda:") {
        tracing::info!(
            function_arn = %arn,
            payload = %body_str,
            "EventBridge delivering to Lambda function"
        );
        let now = chrono::Utc::now();
        {
            let mut accounts = ctx.state.write();
            let s = accounts.get_or_create(ctx.account_id);
            s.lambda_invocations.push(crate::state::LambdaInvocation {
                function_arn: arn.clone(),
                payload: body_str.clone(),
                timestamp: now,
            });
        }
        if let Some(ls) = ctx.lambda_state {
            ls.write()
                .default_mut()
                .invocations
                .push(fakecloud_lambda::LambdaInvocation {
                    function_arn: arn.clone(),
                    payload: body_str.clone(),
                    timestamp: now,
                    source: "aws:events".to_string(),
                });
        }
        invoke_lambda_async(
            ctx.container_runtime,
            &ctx.lambda_state.cloned(),
            arn,
            &body_str,
        );
    } else if arn.contains(":logs:") {
        tracing::info!(
            log_group_arn = %arn,
            payload = %body_str,
            "EventBridge delivering to CloudWatch Logs"
        );
        let now = chrono::Utc::now();
        {
            let mut accounts = ctx.state.write();
            let s = accounts.get_or_create(ctx.account_id);
            s.log_deliveries.push(crate::state::LogDelivery {
                log_group_arn: arn.clone(),
                payload: body_str.clone(),
                timestamp: now,
            });
        }
        if let Some(log_state) = ctx.logs_state {
            deliver_to_logs(log_state, arn, &body_str, now);
        }
    } else if arn.contains(":kinesis:") {
        tracing::info!(
            stream_arn = %arn,
            "EventBridge delivering to Kinesis stream"
        );
        ctx.delivery.send_to_kinesis(arn, &body_str, event_id);
    } else if arn.contains(":states:") {
        tracing::info!(
            state_machine_arn = %arn,
            "EventBridge delivering to Step Functions"
        );
        ctx.delivery.start_stepfunctions_execution(arn, &body_str);
        let mut accounts = ctx.state.write();
        let s = accounts.get_or_create(ctx.account_id);
        s.step_function_executions
            .push(crate::state::StepFunctionExecution {
                state_machine_arn: arn.clone(),
                payload: body_str.clone(),
                timestamp: chrono::Utc::now(),
            });
    } else if arn.contains(":api-destination/") {
        let accounts = ctx.state.read();
        let empty = crate::state::EventBridgeState::new(ctx.account_id, ctx.region);
        let s = accounts.get(ctx.account_id).unwrap_or(&empty);
        let dest = s.api_destinations.values().find(|d| d.arn == *arn).cloned();
        let conn = dest.as_ref().and_then(|d| {
            s.connections
                .values()
                .find(|c| c.arn == d.connection_arn)
                .cloned()
        });
        drop(accounts);
        if let Some(dest) = dest {
            let url = dest.invocation_endpoint;
            let method = dest.http_method;
            let payload = body_str.clone();
            tokio::spawn(async move {
                let client = reqwest::Client::new();
                let mut req_builder = match method.as_str() {
                    "GET" => client.get(&url),
                    "PUT" => client.put(&url),
                    "DELETE" => client.delete(&url),
                    "PATCH" => client.patch(&url),
                    "HEAD" => client.head(&url),
                    _ => client.post(&url),
                };
                req_builder = req_builder.header("Content-Type", "application/json");
                if let Some(conn) = conn {
                    req_builder = apply_connection_auth(req_builder, &conn);
                }
                let result = req_builder.body(payload).send().await;
                if let Err(e) = result {
                    tracing::warn!(
                        endpoint = %url,
                        error = %e,
                        "EventBridge ApiDestination delivery failed"
                    );
                }
            });
        }
    } else if arn.starts_with("https://") || arn.starts_with("http://") {
        let url = arn.clone();
        let payload = body_str.clone();
        tokio::spawn(async move {
            let client = reqwest::Client::new();
            let result = client
                .post(&url)
                .header("Content-Type", "application/json")
                .body(payload)
                .send()
                .await;
            if let Err(e) = result {
                tracing::warn!(
                    endpoint = %url,
                    error = %e,
                    "EventBridge HTTP target delivery failed"
                );
            }
        });
    }
}
