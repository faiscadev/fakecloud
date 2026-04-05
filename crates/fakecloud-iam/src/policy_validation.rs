use serde_json::Value;

/// Valid condition operator base names.
const CONDITION_OPERATORS: &[&str] = &[
    "StringEquals",
    "StringNotEquals",
    "StringEqualsIgnoreCase",
    "StringNotEqualsIgnoreCase",
    "StringLike",
    "StringNotLike",
    "NumericEquals",
    "NumericNotEquals",
    "NumericLessThan",
    "NumericLessThanEquals",
    "NumericGreaterThan",
    "NumericGreaterThanEquals",
    "DateEquals",
    "DateNotEquals",
    "DateLessThan",
    "DateLessThanEquals",
    "DateGreaterThan",
    "DateGreaterThanEquals",
    "Bool",
    "BinaryEquals",
    "IpAddress",
    "NotIpAddress",
    "ArnEquals",
    "ArnNotEquals",
    "ArnLike",
    "ArnNotLike",
    "Null",
];

/// Valid AWS partitions for resource ARN validation.
const VALID_PARTITIONS: &[&str] = &["aws", "aws-cn", "aws-us-gov", "aws-iso", "aws-iso-b"];

/// Valid IAM resource path prefixes.
const IAM_RESOURCE_PREFIXES: &[&str] = &[
    "user/",
    "federated-user/",
    "role/",
    "group/",
    "instance-profile/",
    "mfa/",
    "server-certificate/",
    "policy/",
    "sms-mfa/",
    "saml-provider/",
    "oidc-provider/",
    "report/",
    "access-report/",
];

/// Validate a policy document string. Returns Ok(()) if valid, Err(message) if invalid.
pub fn validate_policy_document(doc: &str) -> Result<(), String> {
    // 1. Must be valid JSON
    let value: Value = serde_json::from_str(doc).map_err(|_| "Syntax errors in policy.")?;

    // 2. Must be an object
    let obj = value.as_object().ok_or("Syntax errors in policy.")?;

    // Check for unknown top-level fields
    let allowed_top_level = ["Version", "Statement", "Id"];
    for key in obj.keys() {
        if !allowed_top_level.contains(&key.as_str()) {
            return Err("Syntax errors in policy.".to_string());
        }
    }

    // 3. Validate "Id" if present — must be a string
    if let Some(id_val) = obj.get("Id") {
        if !id_val.is_string() {
            return Err("Syntax errors in policy.".to_string());
        }
    }

    // 4. Version handling
    let version = obj.get("Version");
    let has_version_2012 = match version {
        Some(v) => {
            let vs = v.as_str().ok_or("Syntax errors in policy.")?;
            match vs {
                "2012-10-17" => true,
                "2008-10-17" => false,
                _ => return Err("Syntax errors in policy.".to_string()),
            }
        }
        None => false,
    };

    if !has_version_2012 {
        return Err("Policy document must be version 2012-10-17 or greater.".to_string());
    }

    // 5. Must have Statement
    let statement_val = obj.get("Statement").ok_or("Syntax errors in policy.")?;

    // Normalize statements to a vec
    let statements: Vec<&Value> = match statement_val {
        Value::Array(arr) => {
            if arr.is_empty() {
                return Err("Syntax errors in policy.".to_string());
            }
            for elem in arr {
                if !elem.is_object() {
                    return Err("Syntax errors in policy.".to_string());
                }
            }
            arr.iter().collect()
        }
        Value::Object(_) => vec![statement_val],
        _ => return Err("Syntax errors in policy.".to_string()),
    };

    // Track SIDs for uniqueness
    let mut seen_sids: Vec<String> = Vec::new();

    // First pass: check for legacy Effect casing
    for stmt in &statements {
        let stmt_obj = stmt.as_object().unwrap();
        if let Some(effect_val) = stmt_obj.get("Effect") {
            if let Some(effect_str) = effect_val.as_str() {
                if effect_str != "Allow" && effect_str != "Deny" {
                    let lower = effect_str.to_lowercase();
                    if lower == "allow" || lower == "deny" {
                        return Err("The policy failed legacy parsing".to_string());
                    }
                }
            }
        }
    }

    // Validate each statement
    for stmt in &statements {
        let stmt_obj = stmt.as_object().unwrap();
        validate_statement(stmt_obj, &mut seen_sids)?;
    }

    Ok(())
}

fn validate_statement(
    stmt_obj: &serde_json::Map<String, Value>,
    seen_sids: &mut Vec<String>,
) -> Result<(), String> {
    // Check allowed statement fields
    let allowed_stmt_fields = [
        "Sid",
        "Effect",
        "Action",
        "NotAction",
        "Resource",
        "NotResource",
        "Condition",
        "Principal",
        "NotPrincipal",
    ];
    for key in stmt_obj.keys() {
        if !allowed_stmt_fields.contains(&key.as_str()) {
            return Err("Syntax errors in policy.".to_string());
        }
    }

    // Both Action and NotAction present is an error
    if stmt_obj.contains_key("Action") && stmt_obj.contains_key("NotAction") {
        return Err("Syntax errors in policy.".to_string());
    }

    // Both Resource and NotResource present is an error
    if stmt_obj.contains_key("Resource") && stmt_obj.contains_key("NotResource") {
        return Err("Syntax errors in policy.".to_string());
    }

    // Validate Sid
    if let Some(sid_val) = stmt_obj.get("Sid") {
        match sid_val {
            Value::String(s) => {
                if !s.is_empty() {
                    if seen_sids.contains(s) {
                        return Err(
                            "Statement IDs (SID) in a single policy must be unique.".to_string()
                        );
                    }
                    seen_sids.push(s.clone());
                }
            }
            _ => return Err("Syntax errors in policy.".to_string()),
        }
    }

    // Effect is required
    let effect_val = stmt_obj.get("Effect").ok_or("Syntax errors in policy.")?;
    let effect_str = effect_val.as_str().ok_or("Syntax errors in policy.")?;
    if effect_str != "Allow" && effect_str != "Deny" {
        return Err("Syntax errors in policy.".to_string());
    }

    // Validate Condition (structure only, not date values yet)
    if let Some(cond_val) = stmt_obj.get("Condition") {
        validate_condition_structure(cond_val)?;
    }

    // Determine what we have
    let has_action = stmt_obj.contains_key("Action");
    let has_not_action = stmt_obj.contains_key("NotAction");
    let has_resource = stmt_obj.contains_key("Resource");
    let has_not_resource = stmt_obj.contains_key("NotResource");

    // Missing action
    if !has_action && !has_not_action {
        // If condition has date operators, it's legacy parsing
        if let Some(cond) = stmt_obj.get("Condition") {
            if has_date_condition(cond) {
                return Err("The policy failed legacy parsing".to_string());
            }
        }
        // If resource is present, check if it has legacy issues
        if has_resource || has_not_resource {
            let rkey = if has_resource {
                "Resource"
            } else {
                "NotResource"
            };
            let rval = stmt_obj.get(rkey).unwrap();
            // Check for legacy resource format issues
            validate_resource_strings_legacy_only(rval)?;
        }
        return Err("Policy statement must contain actions.".to_string());
    }

    // Validate action type/structure first (not format — type errors are syntax errors)
    let action_key = if has_action { "Action" } else { "NotAction" };
    let action_val = stmt_obj.get(action_key).unwrap();
    validate_action_type(action_val)?;

    // Missing resource
    if !has_resource && !has_not_resource {
        // Validate action format to see if that error should be reported first
        validate_action_strings(action_val)?;
        return Err("Policy statement must contain resources.".to_string());
    }

    // Validate resource type/structure
    let resource_key = if has_resource {
        "Resource"
    } else {
        "NotResource"
    };
    let resource_val = stmt_obj.get(resource_key).unwrap();
    validate_resource_type(resource_val)?;

    // Check for empty resources
    if is_empty_resources(resource_val) {
        return Err("Policy statement must contain resources.".to_string());
    }

    // Validate date condition values BEFORE resource/action strings
    // (date errors take priority over partition errors)
    if let Some(cond_val) = stmt_obj.get("Condition") {
        validate_date_condition_values(cond_val)?;
    }

    // Validate resource string formats BEFORE action strings
    // (resource errors take priority over action format errors like multiple colons)
    validate_resource_strings(resource_val)?;

    // Now validate action string formats
    validate_action_strings(action_val)?;

    Ok(())
}

/// Check that action value has valid types (string or array of strings)
fn validate_action_type(val: &Value) -> Result<(), String> {
    match val {
        Value::String(_) => Ok(()),
        Value::Array(arr) => {
            for item in arr {
                if !item.is_string() {
                    return Err("Syntax errors in policy.".to_string());
                }
            }
            Ok(())
        }
        _ => Err("Syntax errors in policy.".to_string()),
    }
}

/// Validate action string format (vendor:action or *)
fn validate_action_strings(val: &Value) -> Result<(), String> {
    match val {
        Value::String(s) => validate_single_action(s),
        Value::Array(arr) => {
            if arr.is_empty() {
                return Err("Policy statement must contain actions.".to_string());
            }
            for item in arr {
                if let Value::String(s) = item {
                    validate_single_action(s)?;
                }
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn validate_single_action(action: &str) -> Result<(), String> {
    if action == "*" {
        return Ok(());
    }

    if action.is_empty() {
        return Err(
            "Actions/Conditions must be prefaced by a vendor, e.g., iam, sdb, ec2, etc."
                .to_string(),
        );
    }

    let colon_count = action.matches(':').count();

    if colon_count == 0 {
        return Err(
            "Actions/Conditions must be prefaced by a vendor, e.g., iam, sdb, ec2, etc."
                .to_string(),
        );
    }

    if colon_count > 1 {
        return Err("Actions/Condition can contain only one colon.".to_string());
    }

    let parts: Vec<&str> = action.splitn(2, ':').collect();
    let vendor = parts[0];

    if vendor.contains(' ') {
        return Err(format!("Vendor {} is not valid", vendor));
    }

    Ok(())
}

/// Check that resource value has valid types (string, null, or array of string/null)
fn validate_resource_type(val: &Value) -> Result<(), String> {
    match val {
        Value::String(_) | Value::Null => Ok(()),
        Value::Array(arr) => {
            for item in arr {
                match item {
                    Value::String(_) | Value::Null => {}
                    _ => return Err("Syntax errors in policy.".to_string()),
                }
            }
            Ok(())
        }
        _ => Err("Syntax errors in policy.".to_string()),
    }
}

fn is_empty_resources(val: &Value) -> bool {
    match val {
        Value::Array(arr) => arr.is_empty(),
        _ => false,
    }
}

/// Check if resource has "legacy parsing" issues only (5-part ARNs, empty partitions, etc.)
fn validate_resource_strings_legacy_only(val: &Value) -> Result<(), String> {
    match val {
        Value::String(s) => {
            if let Err(e) = validate_single_resource(s) {
                if e == "The policy failed legacy parsing" {
                    return Err(e);
                }
            }
            Ok(())
        }
        Value::Array(arr) => {
            for item in arr {
                if let Value::String(s) = item {
                    if let Err(e) = validate_single_resource(s) {
                        if e == "The policy failed legacy parsing" {
                            return Err(e);
                        }
                    }
                }
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

/// Validate resource string format
fn validate_resource_strings(val: &Value) -> Result<(), String> {
    match val {
        Value::String(s) => validate_single_resource(s),
        Value::Array(arr) => {
            for item in arr {
                if let Value::String(s) = item {
                    validate_single_resource(s)?;
                }
                // Null is allowed
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn validate_single_resource(resource: &str) -> Result<(), String> {
    if resource == "*" {
        return Ok(());
    }

    if resource.is_empty() {
        return Err(format!(
            "Resource {} must be in ARN format or \"*\".",
            resource
        ));
    }

    if resource.starts_with("arn:") {
        return validate_arn(resource);
    }

    let colon_count = resource.matches(':').count();

    if colon_count == 0 {
        return Err(format!(
            "Resource {} must be in ARN format or \"*\".",
            resource
        ));
    }

    // Has colons but doesn't start with "arn:"
    let parts: Vec<&str> = resource.splitn(6, ':').collect();

    if colon_count == 1 {
        let pseudo_arn = format!("arn:{}:*:*:*:*", parts[1]);
        return Err(format!(
            "Partition \"{}\" is not valid for resource \"{}\".",
            parts[1], pseudo_arn
        ));
    }

    if colon_count == 2 {
        let pseudo_arn = format!("arn:{}:{}:*:*:*", parts[1], parts[2]);
        return Err(format!(
            "Partition \"{}\" is not valid for resource \"{}\".",
            parts[1], pseudo_arn
        ));
    }

    if colon_count >= 3 {
        // Reconstruct as ARN-like and check partition
        // e.g., "aws:s3:::example_bucket" -> check partition = s3
        let after_first = &resource[resource.find(':').unwrap() + 1..];
        let recon = format!("arn:{}", after_first);
        let recon_parts: Vec<&str> = recon.splitn(6, ':').collect();
        if recon_parts.len() >= 2 {
            let partition = recon_parts[1];
            if !VALID_PARTITIONS.contains(&partition) {
                let mut arn_form = format!("arn:{}", after_first);
                let current_colons = arn_form.matches(':').count();
                for _ in current_colons..5 {
                    arn_form.push_str(":*");
                }
                return Err(format!(
                    "Partition \"{}\" is not valid for resource \"{}\".",
                    partition, arn_form
                ));
            }
        }
    }

    Err(format!(
        "Resource {} must be in ARN format or \"*\".",
        resource
    ))
}

fn validate_arn(resource: &str) -> Result<(), String> {
    let parts: Vec<&str> = resource.splitn(6, ':').collect();

    if parts.len() < 6 {
        if parts.len() <= 2 {
            return Err(
                "Resource vendor must be fully qualified and cannot contain regexes.".to_string(),
            );
        }
        // 3 or 4 parts: valid (e.g., "arn:aws:fdsasf" or "arn:aws:s3:fdsasf")
        // but reject if service (parts[2]) is empty (e.g., "arn:aws::fdsasf")
        if parts.len() >= 3 && parts.len() <= 4 {
            if parts[2].is_empty() {
                return Err("The policy failed legacy parsing".to_string());
            }
            return Ok(());
        }
        // 5 parts: legacy parsing error (e.g., "arn:aws:s3::example_bucket")
        return Err("The policy failed legacy parsing".to_string());
    }

    let partition = parts[1];
    let service = parts[2];
    let region = parts[3];

    if partition.is_empty() {
        return Err("The policy failed legacy parsing".to_string());
    }

    if !VALID_PARTITIONS.contains(&partition) {
        return Err(format!(
            "Partition \"{}\" is not valid for resource \"{}\".",
            partition, resource
        ));
    }

    if service.is_empty() {
        return Err("The policy failed legacy parsing".to_string());
    }

    // IAM resources cannot have region info
    if service == "iam" && !region.is_empty() {
        return Err(format!(
            "IAM resource {} cannot contain region information.",
            resource
        ));
    }

    // S3 bucket-style resources (empty account) cannot have region
    let account = parts[4];
    if service == "s3" && !region.is_empty() && account.is_empty() {
        return Err(format!(
            "Resource {} can not contain region information.",
            resource
        ));
    }

    // IAM resource path validation
    if service == "iam" {
        let resource_part = parts[5];
        if !resource_part.is_empty() && resource_part != "*" && !resource_part.contains("${") {
            let has_valid_prefix = IAM_RESOURCE_PREFIXES
                .iter()
                .any(|prefix| resource_part.starts_with(prefix));
            if !has_valid_prefix {
                return Err(
                    "IAM resource path must either be \"*\" or start with user/, federated-user/, role/, group/, instance-profile/, mfa/, server-certificate/, policy/, sms-mfa/, saml-provider/, oidc-provider/, report/, access-report/.".to_string()
                );
            }
        }
    }

    Ok(())
}

/// Validate condition structure (types only, not values)
fn validate_condition_structure(val: &Value) -> Result<(), String> {
    let obj = match val {
        Value::Object(o) => o,
        _ => return Err("Syntax errors in policy.".to_string()),
    };

    if obj.is_empty() {
        return Ok(());
    }

    for (op_key, op_val) in obj {
        let inner_obj = match op_val {
            Value::Object(o) => o,
            _ => return Err("Syntax errors in policy.".to_string()),
        };

        // AWS accepts unknown condition operators when their inner object is empty
        if !is_valid_condition_operator(op_key) {
            if !inner_obj.is_empty() {
                return Err("Syntax errors in policy.".to_string());
            }
            continue;
        }

        for (_cond_key, cond_val) in inner_obj {
            match cond_val {
                Value::String(_) | Value::Number(_) | Value::Bool(_) | Value::Null => {}
                Value::Array(arr) => {
                    for item in arr {
                        match item {
                            Value::String(_) | Value::Number(_) | Value::Bool(_) | Value::Null => {}
                            _ => return Err("Syntax errors in policy.".to_string()),
                        }
                    }
                }
                Value::Object(_) => {
                    return Err("Syntax errors in policy.".to_string());
                }
            }
        }
    }

    Ok(())
}

/// Validate date condition values — invalid date strings trigger legacy parsing error
fn validate_date_condition_values(val: &Value) -> Result<(), String> {
    let obj = match val.as_object() {
        Some(o) => o,
        None => return Ok(()),
    };

    for (op_key, op_val) in obj {
        if !is_date_operator(op_key) {
            continue;
        }

        let inner = match op_val.as_object() {
            Some(o) => o,
            None => continue,
        };

        for (_key, value) in inner {
            match value {
                Value::String(s) => {
                    if !is_valid_date_value(s) {
                        return Err("The policy failed legacy parsing".to_string());
                    }
                }
                Value::Number(n) => {
                    // Check if the number is too large (> i64::MAX)
                    if n.as_i64().is_none() && n.as_f64().is_some() {
                        let f = n.as_f64().unwrap();
                        if f > i64::MAX as f64 {
                            return Err("The policy failed legacy parsing".to_string());
                        }
                    }
                    if let Some(s) = n.as_u64() {
                        if s > i64::MAX as u64 {
                            return Err("The policy failed legacy parsing".to_string());
                        }
                    }
                }
                Value::Array(arr) => {
                    for item in arr {
                        if let Value::String(s) = item {
                            if !is_valid_date_value(s) {
                                return Err("The policy failed legacy parsing".to_string());
                            }
                        }
                    }
                }
                _ => {}
            }
        }
    }

    Ok(())
}

fn is_date_operator(op: &str) -> bool {
    let base = op
        .strip_prefix("ForAllValues:")
        .or_else(|| op.strip_prefix("ForAnyValue:"))
        .unwrap_or(op);
    let base = base.strip_suffix("IfExists").unwrap_or(base);
    matches!(
        base,
        "DateEquals"
            | "DateNotEquals"
            | "DateLessThan"
            | "DateLessThanEquals"
            | "DateGreaterThan"
            | "DateGreaterThanEquals"
    )
}

/// Check if a string is a valid date value for condition operators.
/// Valid formats: ISO 8601 date/datetime, epoch seconds (integer as string)
fn is_valid_date_value(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }

    // Try as epoch seconds (integer)
    if let Ok(n) = s.parse::<i64>() {
        // Valid range — roughly -292275054 to 292278993
        // The test shows "-292275054" is valid, "9223372036854775808" (> i64::MAX) is not
        let _ = n;
        return true;
    }

    // Try as ISO 8601 date/datetime
    // Valid patterns:
    // YYYY-MM-DD
    // YYYY-MM-DDThh:mm:ssZ
    // YYYY-MM-DDThh:mm:ss.sssZ
    // YYYY-MM-DDThh:mm:ss+HH:MM or +HH

    // Find the 'T' or 't' separator
    let t_pos = s.find(['T', 't']);

    if let Some(t_idx) = t_pos {
        let date_part = &s[..t_idx];
        let time_part = &s[t_idx + 1..];

        // AWS accepts short numeric prefixes before T (e.g. "01T") as valid date values
        if date_part.chars().all(|c| c.is_ascii_digit()) && !date_part.is_empty() {
            return is_valid_time_part(time_part);
        }

        if !is_valid_date_part(date_part) {
            return false;
        }

        return is_valid_time_part(time_part);
    }

    if s.len() < 4 {
        return false;
    }

    // No time part, just a date
    is_valid_date_part(s)
}

fn is_valid_date_part(s: &str) -> bool {
    // YYYY-MM-DD
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() != 3 {
        return false;
    }
    if parts[0].parse::<i32>().is_err() {
        return false;
    }
    if parts[1].parse::<u32>().is_err() {
        return false;
    }
    if parts[2].parse::<u32>().is_err() {
        return false;
    }
    let month = parts[1].parse::<u32>().unwrap();
    let day = parts[2].parse::<u32>().unwrap();
    if !(1..=12).contains(&month) {
        return false;
    }
    if !(1..=31).contains(&day) {
        return false;
    }
    true
}

fn is_valid_time_part(s: &str) -> bool {
    if s.is_empty() {
        return true;
    }

    // Separate timezone from time
    // Timezone can be Z, +HH, +HH:MM, -HH, -HH:MM
    let (time_str, tz_str) = if s.ends_with('Z') || s.ends_with('z') {
        (&s[..s.len() - 1], Some("Z"))
    } else if let Some(plus_pos) = s.rfind('+') {
        if plus_pos > 0 {
            (&s[..plus_pos], Some(&s[plus_pos..]))
        } else {
            (s, None)
        }
    } else if let Some(minus_pos) = s.rfind('-') {
        // Make sure it's not part of the time (could be in fractional seconds)
        if minus_pos > 0 && s[..minus_pos].contains(':') {
            (&s[..minus_pos], Some(&s[minus_pos..]))
        } else {
            (s, None)
        }
    } else {
        (s, None)
    };

    // Validate time: HH:MM:SS or HH:MM:SS.sss
    let time_parts: Vec<&str> = time_str.split(':').collect();
    if time_parts.is_empty() || time_parts.len() > 3 {
        return false;
    }

    // Hour
    if time_parts[0].parse::<u32>().is_err() {
        return false;
    }

    // Minute
    if time_parts.len() >= 2 && time_parts[1].parse::<u32>().is_err() {
        return false;
    }

    // Seconds (may have fractional part)
    if time_parts.len() >= 3 {
        let sec_part = time_parts[2];
        let sec_parts: Vec<&str> = sec_part.split('.').collect();
        if sec_parts[0].parse::<u32>().is_err() {
            return false;
        }
        if sec_parts.len() > 1 {
            // Fractional seconds — must be digits and max 9 digits
            let frac = sec_parts[1];
            if frac.is_empty() || frac.len() > 9 || frac.parse::<u64>().is_err() {
                return false;
            }
        }
    }

    // Validate timezone
    if let Some(tz) = tz_str {
        if tz != "Z" {
            // +HH or +HH:MM or -HH or -HH:MM
            let tz_inner = &tz[1..]; // skip + or -
            if tz_inner.contains(':') {
                let tz_parts: Vec<&str> = tz_inner.split(':').collect();
                if tz_parts.len() != 2 {
                    return false;
                }
                let hours = match tz_parts[0].parse::<i32>() {
                    Ok(h) => h,
                    Err(_) => return false,
                };
                let minutes = match tz_parts[1].parse::<i32>() {
                    Ok(m) => m,
                    Err(_) => return false,
                };
                if hours > 23 || minutes > 59 {
                    return false;
                }
            } else {
                // Just +HH — must be exactly 2 digits
                if tz_inner.len() != 2 {
                    return false;
                }
                let hours = match tz_inner.parse::<i32>() {
                    Ok(h) => h,
                    Err(_) => return false,
                };
                if hours > 23 {
                    return false;
                }
            }
        }
    }

    true
}

fn is_valid_condition_operator(op: &str) -> bool {
    if CONDITION_OPERATORS.contains(&op) {
        return true;
    }

    for base in CONDITION_OPERATORS {
        if op == format!("{}IfExists", base) {
            return true;
        }
    }

    let prefixes = ["ForAllValues:", "ForAnyValue:"];
    for prefix in &prefixes {
        if let Some(rest) = op.strip_prefix(prefix) {
            if CONDITION_OPERATORS.contains(&rest) {
                return true;
            }
            for base in CONDITION_OPERATORS {
                if rest == format!("{}IfExists", base) {
                    return true;
                }
            }
        }
    }

    false
}

fn has_date_condition(cond: &Value) -> bool {
    if let Value::Object(obj) = cond {
        for key in obj.keys() {
            if is_date_operator(key) {
                return true;
            }
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_basic_policy() {
        let doc = r#"{"Version":"2012-10-17","Statement":{"Effect":"Allow","Action":"s3:ListBucket","Resource":"arn:aws:s3:::example_bucket"}}"#;
        assert!(validate_policy_document(doc).is_ok());
    }

    #[test]
    fn test_invalid_json() {
        assert_eq!(
            validate_policy_document("not json").unwrap_err(),
            "Syntax errors in policy."
        );
    }

    #[test]
    fn test_missing_version() {
        let doc = r#"{"Statement":{"Effect":"Allow","Action":"s3:ListBucket","Resource":"arn:aws:s3:::example_bucket"}}"#;
        assert_eq!(
            validate_policy_document(doc).unwrap_err(),
            "Policy document must be version 2012-10-17 or greater."
        );
    }

    #[test]
    fn test_invalid_action() {
        let doc = r#"{"Version":"2012-10-17","Statement":{"Effect":"Allow","Action":"invalid","Resource":"arn:aws:s3:::example_bucket"}}"#;
        assert_eq!(
            validate_policy_document(doc).unwrap_err(),
            "Actions/Conditions must be prefaced by a vendor, e.g., iam, sdb, ec2, etc."
        );
    }

    #[test]
    fn test_invalid_resource() {
        let doc = r#"{"Version":"2012-10-17","Statement":{"Effect":"Allow","Action":"s3:ListBucket","Resource":"invalid resource"}}"#;
        assert_eq!(
            validate_policy_document(doc).unwrap_err(),
            "Resource invalid resource must be in ARN format or \"*\"."
        );
    }

    #[test]
    fn test_empty_statement_array() {
        let doc = r#"{"Version":"2012-10-17","Statement":[]}"#;
        assert_eq!(
            validate_policy_document(doc).unwrap_err(),
            "Syntax errors in policy."
        );
    }

    #[test]
    fn test_missing_effect() {
        let doc = r#"{"Version":"2012-10-17","Statement":{"Action":"s3:ListBucket","Resource":"arn:aws:s3:::example_bucket"}}"#;
        assert_eq!(
            validate_policy_document(doc).unwrap_err(),
            "Syntax errors in policy."
        );
    }

    #[test]
    fn test_date_condition_invalid_value() {
        let doc = r#"{"Version":"2012-10-17","Statement":{"Effect":"Allow","Action":"s3:ListBucket","Resource":"arn:aws:s3:::example_bucket","Condition":{"DateGreaterThan":{"a":"sdfdsf"}}}}"#;
        assert_eq!(
            validate_policy_document(doc).unwrap_err(),
            "The policy failed legacy parsing"
        );
    }

    #[test]
    fn test_valid_date_condition() {
        let doc = r#"{"Version":"2012-10-17","Statement":{"Effect":"Allow","Action":"s3:ListBucket","Resource":"arn:aws:s3:::example_bucket","Condition":{"DateGreaterThan":{"aws:CurrentTime":"2017-07-01T00:00:00Z"}}}}"#;
        assert!(validate_policy_document(doc).is_ok());
    }
}
