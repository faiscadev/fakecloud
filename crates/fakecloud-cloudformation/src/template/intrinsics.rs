//! `intrinsics` concerns from template.rs (audit-2026-05-19).

use super::*;

/// Render a CFN intrinsic value (Ref to a parameter, plain string, etc.)
/// as a string for Fn::Equals comparison.
pub(super) fn stringify_value(value: &Value, parameters: &BTreeMap<String, String>) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::Object(m) => {
            if let Some(name) = m.get("Ref").and_then(|v| v.as_str()) {
                if let Some(p) = parameters.get(name) {
                    return p.clone();
                }
                return name.to_string();
            }
            value.to_string()
        }
        _ => value.to_string(),
    }
}

/// Substitute a pseudo-parameter with the value provided through the
/// stack `parameters` map (keyed by the same `AWS::*` name). When the
/// caller hasn't supplied a value, fall back to the canonical default
/// for that parameter (commercial partition / us-east-1 / empty list).
pub(super) fn pseudo_value(name: &str, parameters: &BTreeMap<String, String>) -> Option<Value> {
    // AWS::NotificationARNs is array-typed; the seed encodes it as a
    // JSON array string so it round-trips through the string-keyed
    // parameters map cleanly. Falls back to the default empty list when
    // the seed is missing or malformed.
    if name == "AWS::NotificationARNs" {
        if let Some(raw) = parameters.get(name) {
            if let Ok(parsed) = serde_json::from_str::<Vec<String>>(raw) {
                return Some(Value::Array(
                    parsed.into_iter().map(Value::String).collect(),
                ));
            }
        }
        return Some(Value::Array(Vec::new()));
    }
    if let Some(v) = parameters.get(name) {
        return Some(Value::String(v.clone()));
    }
    let region = parameters
        .get("AWS::Region")
        .map(String::as_str)
        .unwrap_or("us-east-1");
    match name {
        // Partition + URLSuffix mirror real CFN: derive from the request
        // region so a stack in `cn-north-1` lands `aws-cn` /
        // `amazonaws.com.cn`, and `us-gov-west-1` lands `aws-us-gov`.
        "AWS::Partition" => Some(Value::String(partition_for_region(region).to_string())),
        "AWS::URLSuffix" => Some(Value::String(url_suffix_for_region(region).to_string())),
        "AWS::Region" => Some(Value::String(region.to_string())),
        // NoValue is a sentinel: emit a private marker object so the
        // post-resolution `strip_no_value` walk can drop the parent
        // property entirely. CloudFormation removes the key from the
        // resolved object rather than leaving a JSON null behind.
        "AWS::NoValue" => Some(no_value_sentinel()),
        _ => None,
    }
}

/// Build a fresh `AWS::NoValue` sentinel object. See
/// [`NO_VALUE_SENTINEL_KEY`].
pub(super) fn no_value_sentinel() -> Value {
    let mut m = serde_json::Map::new();
    m.insert(NO_VALUE_SENTINEL_KEY.to_string(), Value::Bool(true));
    Value::Object(m)
}

/// Return true when `value` is the `AWS::NoValue` sentinel emitted by
/// `pseudo_value` (or by an `Fn::If` branch that resolved to it).
pub(super) fn is_no_value(value: &Value) -> bool {
    value
        .as_object()
        .map(|m| m.len() == 1 && m.contains_key(NO_VALUE_SENTINEL_KEY))
        .unwrap_or(false)
}

/// Recursively walk `value` and drop any object entry / array slot
/// whose resolved content is the `AWS::NoValue` sentinel. A top-level
/// `AWS::NoValue` collapses to `Value::Null` so the caller can detect
/// the empty case (CFN's behavior is to omit the property entirely).
pub(super) fn strip_no_value(value: Value) -> Value {
    match value {
        Value::Object(map) => {
            if is_no_value(&Value::Object(map.clone())) {
                return Value::Null;
            }
            let mut out = serde_json::Map::with_capacity(map.len());
            for (k, v) in map {
                if is_no_value(&v) {
                    continue;
                }
                out.insert(k, strip_no_value(v));
            }
            Value::Object(out)
        }
        Value::Array(arr) => Value::Array(
            arr.into_iter()
                .filter(|v| !is_no_value(v))
                .map(strip_no_value)
                .collect(),
        ),
        other => other,
    }
}

/// Resolve `Ref`, `Fn::GetAtt`, `Fn::Join`, and `Fn::Sub` in property
/// values. Cross-stack `Fn::ImportValue` is not consulted; use
/// `resolve_refs_with_imports` for that. Test-only after the
/// resource-properties path moved to `resolve_refs_full`.
#[cfg(test)]
pub(super) fn resolve_refs(
    value: &Value,
    parameters: &BTreeMap<String, String>,
    _resources: &serde_json::Map<String, Value>,
    resource_physical_ids: &BTreeMap<String, String>,
    resource_attributes: &BTreeMap<String, BTreeMap<String, String>>,
) -> Value {
    resolve_refs_full(
        value,
        parameters,
        _resources,
        resource_physical_ids,
        resource_attributes,
        &BTreeMap::new(),
        &BTreeMap::new(),
    )
}

/// Resolve `Ref`, `Fn::GetAtt`, `Fn::Join`, `Fn::Sub`, and
/// `Fn::ImportValue` in property values.
pub(super) fn resolve_refs_full(
    value: &Value,
    parameters: &BTreeMap<String, String>,
    _resources: &serde_json::Map<String, Value>,
    resource_physical_ids: &BTreeMap<String, String>,
    resource_attributes: &BTreeMap<String, BTreeMap<String, String>>,
    imports: &BTreeMap<String, String>,
    conditions: &BTreeMap<String, bool>,
) -> Value {
    // Fn::If always rewrites to either branch BEFORE descent so we don't
    // try to resolve the unused branch (it may legitimately reference an
    // unconditional resource).
    if let Some(map) = value.as_object() {
        if let Some(arr) = map.get("Fn::If").and_then(|v| v.as_array()) {
            if arr.len() == 3 {
                let cond_name = arr[0].as_str().unwrap_or("");
                let picked = if conditions.get(cond_name).copied().unwrap_or(false) {
                    &arr[1]
                } else {
                    &arr[2]
                };
                return resolve_refs_full(
                    picked,
                    parameters,
                    _resources,
                    resource_physical_ids,
                    resource_attributes,
                    imports,
                    conditions,
                );
            }
        }
    }
    match value {
        Value::Object(map) => {
            if let Some(ref_val) = map.get("Ref") {
                if let Some(ref_name) = ref_val.as_str() {
                    // 1. Pseudo-references go through `pseudo_value`
                    //    first — `AWS::NotificationARNs` is array-typed
                    //    and would otherwise fall through to the
                    //    string-only parameter path and leak its JSON
                    //    encoding into the resolved value.
                    if PSEUDO_REFS.contains(&ref_name) {
                        if let Some(v) = pseudo_value(ref_name, parameters) {
                            return v;
                        }
                        return Value::String(ref_name.to_string());
                    }
                    // 2. Explicit template parameters.
                    if let Some(param_val) = parameters.get(ref_name) {
                        return Value::String(param_val.clone());
                    }
                    // 3. Already-provisioned resource physical IDs.
                    if let Some(physical_id) = resource_physical_ids.get(ref_name) {
                        return Value::String(physical_id.clone());
                    }
                    // 4. Known logical resource in the template but
                    //    not yet provisioned: return the logical ID and
                    //    let incremental provisioning rewrite it.
                    if _resources.contains_key(ref_name) {
                        return Value::String(ref_name.to_string());
                    }
                    // 5. Unknown ref — return as-is (could be a default parameter)
                    return Value::String(ref_name.to_string());
                }
            }
            // Fn::ImportValue: look up an exported value from another stack.
            // Resolves to the empty string when the export name isn't known
            // (callers that need strict failure can pre-validate).
            if let Some(import_val) = map.get("Fn::ImportValue") {
                let resolved = resolve_refs_full(
                    import_val,
                    parameters,
                    _resources,
                    resource_physical_ids,
                    resource_attributes,
                    imports,
                    conditions,
                );
                let key = match &resolved {
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                };
                if let Some(v) = imports.get(&key) {
                    return Value::String(v.clone());
                }
                return Value::String(String::new());
            }
            if let Some(getatt_val) = map.get("Fn::GetAtt") {
                if let Some((logical_id, attr_name)) = parse_getatt(getatt_val) {
                    if let Some(attrs) = resource_attributes.get(&logical_id) {
                        if let Some(attr_value) = attrs.get(&attr_name) {
                            return Value::String(attr_value.clone());
                        }
                    }
                    // Resource not yet provisioned, or attribute unknown.
                    // Surface a placeholder so the consumer can still string-format
                    // it; multi-pass provisioning will retry once attributes land.
                    return Value::String(format!("{logical_id}.{attr_name}"));
                }
            }
            if let Some(join_val) = map.get("Fn::Join") {
                if let Some(arr) = join_val.as_array() {
                    if arr.len() == 2 {
                        let delimiter = arr[0].as_str().unwrap_or("");
                        if let Some(parts) = arr[1].as_array() {
                            let resolved_parts: Vec<String> = parts
                                .iter()
                                .map(|p| {
                                    let resolved = resolve_refs_full(
                                        p,
                                        parameters,
                                        _resources,
                                        resource_physical_ids,
                                        resource_attributes,
                                        imports,
                                        conditions,
                                    );
                                    match resolved {
                                        Value::String(s) => s,
                                        other => other.to_string(),
                                    }
                                })
                                .collect();
                            return Value::String(resolved_parts.join(delimiter));
                        }
                    }
                }
            }
            // Fn::Base64: base64-encode a string (or recursively-resolved
            // value).
            if let Some(b64_val) = map.get("Fn::Base64") {
                let resolved = resolve_refs_full(
                    b64_val,
                    parameters,
                    _resources,
                    resource_physical_ids,
                    resource_attributes,
                    imports,
                    conditions,
                );
                let s = match &resolved {
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                };
                return Value::String(
                    base64::engine::general_purpose::STANDARD.encode(s.as_bytes()),
                );
            }
            // Fn::Length: number of elements in an array, or characters
            // in a string. Real CFN only documents list inputs but
            // accepts strings; we count UTF-8 chars (not bytes) so
            // multi-byte characters count once.
            if let Some(len_val) = map.get("Fn::Length") {
                let resolved = resolve_refs_full(
                    len_val,
                    parameters,
                    _resources,
                    resource_physical_ids,
                    resource_attributes,
                    imports,
                    conditions,
                );
                let n: usize = match &resolved {
                    Value::Array(arr) => arr.len(),
                    Value::String(s) => s.chars().count(),
                    _ => 0,
                };
                return Value::Number(serde_json::Number::from(n));
            }
            // Fn::ToJsonString: serialize a value as a JSON string.
            if let Some(to_json) = map.get("Fn::ToJsonString") {
                let resolved = resolve_refs_full(
                    to_json,
                    parameters,
                    _resources,
                    resource_physical_ids,
                    resource_attributes,
                    imports,
                    conditions,
                );
                let s = serde_json::to_string(&resolved).unwrap_or_default();
                return Value::String(s);
            }
            // Fn::Split: split a string by a delimiter into an array of
            // strings. Args: ["delim", "source"] (source can be a Ref/etc).
            if let Some(split_val) = map.get("Fn::Split") {
                if let Some(arr) = split_val.as_array() {
                    if arr.len() == 2 {
                        let delim = arr[0].as_str().unwrap_or("");
                        let src_resolved = resolve_refs_full(
                            &arr[1],
                            parameters,
                            _resources,
                            resource_physical_ids,
                            resource_attributes,
                            imports,
                            conditions,
                        );
                        let src = match src_resolved {
                            Value::String(s) => s,
                            other => other.to_string(),
                        };
                        let parts: Vec<Value> = src
                            .split(delim)
                            .map(|p| Value::String(p.to_string()))
                            .collect();
                        return Value::Array(parts);
                    }
                }
            }
            // Fn::GetAZs: the availability zones for a region. The argument is
            // usually the empty string (meaning the stack's region), but may be
            // an explicit region literal or `{"Ref":"AWS::Region"}`. Real CFN
            // returns the region's AZ list; without this arm the object fell
            // through unresolved and the ubiquitous `!Select [0, !GetAZs ""]`
            // idiom yielded null (Select got an object, not an array). Matches
            // DescribeAvailabilityZones' a/b/c set.
            if let Some(azs_val) = map.get("Fn::GetAZs") {
                let resolved = resolve_refs_full(
                    azs_val,
                    parameters,
                    _resources,
                    resource_physical_ids,
                    resource_attributes,
                    imports,
                    conditions,
                );
                let region = match &resolved {
                    Value::String(s) if !s.is_empty() => s.clone(),
                    _ => parameters.get("AWS::Region").cloned().unwrap_or_default(),
                };
                let region = if region.is_empty() {
                    "us-east-1".to_string()
                } else {
                    region
                };
                return Value::Array(
                    ["a", "b", "c"]
                        .iter()
                        .map(|s| Value::String(format!("{region}{s}")))
                        .collect(),
                );
            }
            // Fn::Select: pick element at index from an array. Args:
            // [index, list]. The list may itself be an Fn::Split / Ref.
            if let Some(sel_val) = map.get("Fn::Select") {
                if let Some(arr) = sel_val.as_array() {
                    if arr.len() == 2 {
                        let idx_val = resolve_refs_full(
                            &arr[0],
                            parameters,
                            _resources,
                            resource_physical_ids,
                            resource_attributes,
                            imports,
                            conditions,
                        );
                        let list_val = resolve_refs_full(
                            &arr[1],
                            parameters,
                            _resources,
                            resource_physical_ids,
                            resource_attributes,
                            imports,
                            conditions,
                        );
                        let idx: usize = match &idx_val {
                            Value::Number(n) => n.as_u64().unwrap_or(0) as usize,
                            Value::String(s) => s.parse().unwrap_or(0),
                            _ => 0,
                        };
                        // The list argument is normally a real array (an
                        // Fn::Split / list Ref). A list-valued `Fn::GetAtt`
                        // resolves to a JSON-array *string* (attributes are
                        // String-keyed), so parse that back into an array here
                        // before indexing.
                        let owned;
                        let list = match &list_val {
                            Value::Array(a) => Some(a.as_slice()),
                            Value::String(s) => match serde_json::from_str::<Value>(s) {
                                Ok(Value::Array(a)) => {
                                    owned = a;
                                    Some(owned.as_slice())
                                }
                                _ => None,
                            },
                            _ => None,
                        };
                        if let Some(list) = list {
                            if let Some(elt) = list.get(idx) {
                                return elt.clone();
                            }
                        }
                        return Value::Null;
                    }
                }
            }
            // Fn::Cidr: split a CIDR block into N subnets each of a given
            // bit count. Args: [ip_block, count, cidr_bits]. We compute
            // contiguous sub-blocks within an IPv4 range; IPv6 falls
            // through as a string for simplicity.
            if let Some(cidr_val) = map.get("Fn::Cidr") {
                if let Some(arr) = cidr_val.as_array() {
                    if arr.len() == 3 {
                        let block_val = resolve_refs_full(
                            &arr[0],
                            parameters,
                            _resources,
                            resource_physical_ids,
                            resource_attributes,
                            imports,
                            conditions,
                        );
                        let count_val = resolve_refs_full(
                            &arr[1],
                            parameters,
                            _resources,
                            resource_physical_ids,
                            resource_attributes,
                            imports,
                            conditions,
                        );
                        let bits_val = resolve_refs_full(
                            &arr[2],
                            parameters,
                            _resources,
                            resource_physical_ids,
                            resource_attributes,
                            imports,
                            conditions,
                        );
                        let block_str = match &block_val {
                            Value::String(s) => s.clone(),
                            other => other.to_string(),
                        };
                        let count: u32 = match &count_val {
                            Value::Number(n) => n.as_u64().unwrap_or(0) as u32,
                            Value::String(s) => s.parse().unwrap_or(0),
                            _ => 0,
                        };
                        let cidr_bits: u32 = match &bits_val {
                            Value::Number(n) => n.as_u64().unwrap_or(0) as u32,
                            Value::String(s) => s.parse().unwrap_or(0),
                            _ => 0,
                        };
                        if let Some(sub_cidrs) = compute_cidr_subnets(&block_str, count, cidr_bits)
                        {
                            return Value::Array(
                                sub_cidrs.into_iter().map(Value::String).collect(),
                            );
                        }
                    }
                }
            }
            if let Some(sub_val) = map.get("Fn::Sub") {
                // Two CFN-supported shapes:
                //   "Fn::Sub": "literal-${Var}"
                //   "Fn::Sub": ["literal-${Var}", { "Var": <intrinsic> }]
                // The array form lets the template author bind extra
                // variables that aren't template parameters or resource
                // logical IDs. We resolve each binding through
                // `resolve_refs_full` so nested `Ref` / `Fn::GetAtt`
                // works inside the map.
                let (template_str, extra_vars): (Option<&str>, BTreeMap<String, String>) =
                    if let Some(s) = sub_val.as_str() {
                        (Some(s), BTreeMap::new())
                    } else if let Some(arr) = sub_val.as_array() {
                        let str_part = arr.first().and_then(|v| v.as_str());
                        let mut bindings: BTreeMap<String, String> = BTreeMap::new();
                        if let Some(obj) = arr.get(1).and_then(|v| v.as_object()) {
                            for (k, v) in obj {
                                let resolved = resolve_refs_full(
                                    v,
                                    parameters,
                                    _resources,
                                    resource_physical_ids,
                                    resource_attributes,
                                    imports,
                                    conditions,
                                );
                                let s = match resolved {
                                    Value::String(s) => s,
                                    other => other.to_string(),
                                };
                                bindings.insert(k.clone(), s);
                            }
                        }
                        (str_part, bindings)
                    } else {
                        (None, BTreeMap::new())
                    };
                if let Some(s) = template_str {
                    let mut result = s.to_string();
                    // 1. Bindings from the array form take precedence —
                    //    AWS docs spell this out: explicit map wins over
                    //    template parameters with the same name.
                    for (k, v) in &extra_vars {
                        result = result.replace(&format!("${{{k}}}"), v);
                    }
                    // 2. Pseudo-parameters: handle AWS::NoValue by
                    //    swapping in the sentinel string so the surrounding
                    //    string literal still resolves cleanly. The walker
                    //    `strip_no_value` only acts on object/array
                    //    children, so a Fn::Sub that hard-references
                    //    `${AWS::NoValue}` is best-effort: we drop the
                    //    token from the rendered string. Other AWS::*
                    //    pseudo-params resolve via `pseudo_value` with
                    //    region-aware partition/URLSuffix derivation.
                    for pseudo in PSEUDO_REFS {
                        let token = format!("${{{pseudo}}}");
                        if !result.contains(&token) {
                            continue;
                        }
                        if *pseudo == "AWS::NoValue" {
                            // Inside a string, NoValue collapses to empty
                            // — there's no JSON-level key to drop.
                            result = result.replace(&token, "");
                            continue;
                        }
                        if let Some(v) = pseudo_value(pseudo, parameters) {
                            let s = match v {
                                Value::String(s) => s,
                                other => other.to_string(),
                            };
                            result = result.replace(&token, &s);
                        }
                    }
                    // 3. Template parameters (including AWS::Region etc.
                    //    if the caller seeded them).
                    for (k, v) in parameters {
                        result = result.replace(&format!("${{{k}}}"), v);
                    }
                    // 4. Resource physical IDs from already-provisioned
                    //    siblings.
                    for (k, v) in resource_physical_ids {
                        result = result.replace(&format!("${{{k}}}"), v);
                    }
                    // 5. GetAtt-style substitutions: ${LogicalId.AttrName}
                    for (logical, attrs) in resource_attributes {
                        for (attr, value) in attrs {
                            result = result.replace(&format!("${{{logical}.{attr}}}"), value);
                        }
                    }
                    // 6. Unescape `${!Literal}` -> `${Literal}`. `${!` is CFN's
                    //    escape for a literal `${`, so these were deliberately
                    //    never substituted above (the leading `!` prevents any
                    //    key match); strip the `!` now. Without this the escape
                    //    leaked through verbatim and corrupted IAM policy
                    //    variables (`${!aws:username}` stayed literal instead of
                    //    rendering `${aws:username}`).
                    result = result.replace("${!", "${");
                    return Value::String(result);
                }
            }
            // Recurse into object
            let mut new_map = serde_json::Map::new();
            for (k, v) in map {
                new_map.insert(
                    k.clone(),
                    resolve_refs_full(
                        v,
                        parameters,
                        _resources,
                        resource_physical_ids,
                        resource_attributes,
                        imports,
                        conditions,
                    ),
                );
            }
            Value::Object(new_map)
        }
        Value::Array(arr) => Value::Array(
            arr.iter()
                .map(|v| {
                    resolve_refs_full(
                        v,
                        parameters,
                        _resources,
                        resource_physical_ids,
                        resource_attributes,
                        imports,
                        conditions,
                    )
                })
                .collect(),
        ),
        other => other.clone(),
    }
}

/// Carve `ip_block` (eg. `10.0.0.0/16`) into `count` subnet CIDR
/// strings each with a host count of `2^cidr_bits - 2` (matching real
/// `Fn::Cidr`). IPv4 only — returns `None` for IPv6 or malformed
/// inputs, which leaves the value unresolved at the caller.
pub(super) fn compute_cidr_subnets(
    ip_block: &str,
    count: u32,
    cidr_bits: u32,
) -> Option<Vec<String>> {
    // CloudFormation Fn::Cidr documents `count` in 1..=256. Reject
    // out-of-range values before allocating to avoid oversized Vec
    // construction on bad input.
    if !(1..=256).contains(&count) {
        return None;
    }
    let (ip_str, prefix_str) = ip_block.split_once('/')?;
    let prefix: u32 = prefix_str.parse().ok()?;
    let ip: std::net::Ipv4Addr = ip_str.parse().ok()?;
    let base: u32 = ip.into();
    // Subnet size in bits = 32 - new_prefix. Real Fn::Cidr cidr_bits
    // is the host portion length, so new_prefix = 32 - cidr_bits.
    let new_prefix = 32u32.checked_sub(cidr_bits)?;
    if new_prefix <= prefix {
        return None;
    }
    let step: u32 = 1u32 << cidr_bits;
    let mut out = Vec::with_capacity(count as usize);
    for i in 0..count {
        let subnet_base = base.checked_add(step.checked_mul(i)?)?;
        let addr = std::net::Ipv4Addr::from(subnet_base);
        out.push(format!("{addr}/{new_prefix}"));
    }
    Some(out)
}

/// Parse a `Fn::GetAtt` argument. Accepts either the array form
/// `["LogicalId", "Attr"]` (also nested attribute paths joined with `.`)
/// or the short string form `"LogicalId.Attr"`.
pub(super) fn parse_getatt(value: &Value) -> Option<(String, String)> {
    match value {
        Value::Array(arr) if arr.len() >= 2 => {
            let logical_id = arr[0].as_str()?.to_string();
            let parts: Vec<String> = arr[1..]
                .iter()
                .map(|v| match v {
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                })
                .collect();
            Some((logical_id, parts.join(".")))
        }
        Value::String(s) => {
            let (logical_id, attr) = s.split_once('.')?;
            Some((logical_id.to_string(), attr.to_string()))
        }
        _ => None,
    }
}
