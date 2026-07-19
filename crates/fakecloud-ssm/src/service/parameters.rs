use std::collections::BTreeMap;

use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_aws::arn::Arn;
use fakecloud_core::pagination::paginate_checked;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};
use fakecloud_core::validation::*;

use crate::state::{ParameterPolicyEvent, SsmParameter, SsmParameterVersion, SsmState};

use super::{missing, missing_with_code, remap_validation_to, SsmService, PARAMETER_VERSION_LIMIT};

/// One parsed entry from the `Policies` JSON array on `PutParameter`.
/// AWS supports three policy types — Expiration deletes the parameter
/// at a specific instant; ExpirationNotification fires an EventBridge
/// event in the run-up to expiry; NoChangeNotification fires when a
/// parameter goes too long without being updated.
#[derive(Debug, Clone)]
pub(crate) enum ParsedPolicy {
    Expiration(chrono::DateTime<Utc>),
    ExpirationNotification { before: i64, unit: PolicyUnit },
    NoChangeNotification { after: i64, unit: PolicyUnit },
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum PolicyUnit {
    Days,
    Hours,
    /// fakecloud-only extension: lets E2E tests trigger notification
    /// thresholds in seconds rather than waiting hours.
    Minutes,
    /// fakecloud-only extension; same rationale as `Minutes`.
    Seconds,
}

impl PolicyUnit {
    fn to_duration(self, n: i64) -> chrono::Duration {
        match self {
            PolicyUnit::Days => chrono::Duration::days(n),
            PolicyUnit::Hours => chrono::Duration::hours(n),
            PolicyUnit::Minutes => chrono::Duration::minutes(n),
            PolicyUnit::Seconds => chrono::Duration::seconds(n),
        }
    }
}

/// Error helper: build the AWS-shaped `InvalidPolicyAttributeException`
/// reply. Used both for malformed JSON and for individual
/// missing/unparseable attributes within a well-formed array.
fn invalid_policy_error(detail: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "InvalidPolicyAttributeException",
        detail.into(),
    )
}

/// Parse and validate the `Policies` JSON string on `PutParameter`.
/// Returns the parsed policy list. Empty/`None` input is OK and yields
/// an empty list. Malformed JSON or unrecognized policy shapes raise
/// `InvalidPolicyAttributeException` to match the real API.
pub(crate) fn parse_policies(policies_str: &str) -> Result<Vec<ParsedPolicy>, AwsServiceError> {
    let value: Value = serde_json::from_str(policies_str)
        .map_err(|e| invalid_policy_error(format!("Policies must be valid JSON: {e}")))?;
    let arr = value
        .as_array()
        .ok_or_else(|| invalid_policy_error("Policies must be a JSON array."))?;

    let mut parsed = Vec::with_capacity(arr.len());
    for (i, policy) in arr.iter().enumerate() {
        let kind = policy["Type"].as_str().ok_or_else(|| {
            invalid_policy_error(format!(
                "Policy at index {i} is missing required field 'Type'."
            ))
        })?;
        let attrs = &policy["Attributes"];
        if !attrs.is_object() {
            return Err(invalid_policy_error(format!(
                "Policy at index {i} is missing required field 'Attributes'."
            )));
        }
        match kind {
            "Expiration" => {
                let ts = attrs["Timestamp"].as_str().ok_or_else(|| {
                    invalid_policy_error(format!(
                        "Expiration policy at index {i} requires Attributes.Timestamp."
                    ))
                })?;
                let dt = chrono::DateTime::parse_from_rfc3339(ts).map_err(|e| {
                    invalid_policy_error(format!(
                        "Expiration policy at index {i} has invalid Timestamp: {e}"
                    ))
                })?;
                parsed.push(ParsedPolicy::Expiration(dt.with_timezone(&Utc)));
            }
            "ExpirationNotification" => {
                let (n, unit) = parse_window_attrs(attrs, "Before", i, kind)?;
                parsed.push(ParsedPolicy::ExpirationNotification { before: n, unit });
            }
            "NoChangeNotification" => {
                let (n, unit) = parse_window_attrs(attrs, "After", i, kind)?;
                parsed.push(ParsedPolicy::NoChangeNotification { after: n, unit });
            }
            other => {
                return Err(invalid_policy_error(format!(
                    "Policy at index {i} has unsupported Type: {other}. \
                     Valid types: Expiration, ExpirationNotification, NoChangeNotification."
                )));
            }
        }
    }
    Ok(parsed)
}

/// Pull a `{<key>: number, "Unit": "Days"|"Hours"}` pair out of a
/// notification policy's `Attributes` block. Used by both
/// ExpirationNotification (`Before`) and NoChangeNotification (`After`).
fn parse_window_attrs(
    attrs: &Value,
    key: &str,
    idx: usize,
    kind: &str,
) -> Result<(i64, PolicyUnit), AwsServiceError> {
    // AWS accepts the numeric attribute as either a JSON number or a
    // stringified integer (the SSM PutParameter docs describe the
    // attribute values as strings, but real callers send both).
    let n = if let Some(n) = attrs[key].as_i64() {
        n
    } else if let Some(s) = attrs[key].as_str() {
        s.parse::<i64>().map_err(|_| {
            invalid_policy_error(format!(
                "{kind} policy at index {idx} has non-numeric {key}: {s}"
            ))
        })?
    } else {
        return Err(invalid_policy_error(format!(
            "{kind} policy at index {idx} requires Attributes.{key}."
        )));
    };
    if n <= 0 {
        return Err(invalid_policy_error(format!(
            "{kind} policy at index {idx} requires {key} > 0."
        )));
    }
    let unit_str = attrs["Unit"].as_str().ok_or_else(|| {
        invalid_policy_error(format!(
            "{kind} policy at index {idx} requires Attributes.Unit."
        ))
    })?;
    let unit = match unit_str {
        "Days" => PolicyUnit::Days,
        "Hours" => PolicyUnit::Hours,
        // fakecloud-only extensions: AWS proper accepts only Days/Hours,
        // but Minutes/Seconds let E2E tests verify notification firing
        // without sitting on the runner for an hour.
        "Minutes" => PolicyUnit::Minutes,
        "Seconds" => PolicyUnit::Seconds,
        other => {
            return Err(invalid_policy_error(format!(
                "{kind} policy at index {idx} has unsupported Unit: {other}. \
                 Valid units: Days, Hours (fakecloud extensions: Minutes, Seconds)."
            )));
        }
    };
    Ok((n, unit))
}

/// Convenience wrapper used by read paths: parse `policies` (if any)
/// and return the Expiration timestamp if one is set. Silently ignores
/// malformed JSON — at this point the param was already accepted, so
/// we don't want to re-fail the read.
fn extract_expiration(policies_str: &str) -> Option<chrono::DateTime<Utc>> {
    parse_policies(policies_str).ok().and_then(|policies| {
        policies.into_iter().find_map(|p| match p {
            ParsedPolicy::Expiration(ts) => Some(ts),
            _ => None,
        })
    })
}

/// Returns true if the parameter has an `Expiration` policy whose timestamp
/// is in the past. AWS deletes expired advanced-tier parameters at the
/// scheduled time; we lazily check on every read instead of running a
/// background sweeper.
pub(crate) fn is_param_expired(p: &SsmParameter) -> bool {
    p.policies
        .as_deref()
        .and_then(extract_expiration)
        .is_some_and(|exp| exp < Utc::now())
}

/// On `PutParameter`, record a "Policy registered" event for each
/// supplied policy so callers can verify (via the admin endpoint) that
/// the server saw and accepted them. Notification policies still
/// require a separate threshold-crossing event; this is the
/// at-creation receipt.
pub(crate) fn record_policy_events(
    state: &mut SsmState,
    name: &str,
    arn: &str,
    policies: &[ParsedPolicy],
) {
    let now = Utc::now();
    for policy in policies {
        let (event_type, message) = match policy {
            ParsedPolicy::Expiration(ts) => (
                "ExpirationRegistered",
                format!(
                    "Parameter {name} registered with Expiration at {}.",
                    ts.to_rfc3339()
                ),
            ),
            ParsedPolicy::ExpirationNotification { before, unit } => (
                "ExpirationNotificationRegistered",
                format!(
                    "Parameter {name} registered ExpirationNotification window of {before} {}.",
                    unit_str(*unit)
                ),
            ),
            ParsedPolicy::NoChangeNotification { after, unit } => (
                "NoChangeNotificationRegistered",
                format!(
                    "Parameter {name} registered NoChangeNotification window of {after} {}.",
                    unit_str(*unit)
                ),
            ),
        };
        state.parameter_policy_events.push(ParameterPolicyEvent {
            parameter_name: name.to_string(),
            parameter_arn: arn.to_string(),
            event_type: event_type.to_string(),
            message,
            created_at: now,
        });
    }
}

fn unit_str(u: PolicyUnit) -> &'static str {
    match u {
        PolicyUnit::Days => "Days",
        PolicyUnit::Hours => "Hours",
        PolicyUnit::Minutes => "Minutes",
        PolicyUnit::Seconds => "Seconds",
    }
}

/// Lazy-fire notification policy events for every parameter in
/// `state`. Called from every read path; cheap when no parameters
/// have policies.
///
/// `ExpirationNotification` fires when `now >= expiration - window`.
/// `NoChangeNotification` fires when `now >= last_modified + window`.
/// Both are guarded by per-parameter "already-fired" flags that get
/// reset on overwrite.
pub(crate) fn tick_policy_notifications(state: &mut SsmState) {
    let now = Utc::now();
    for param in state.parameters.values_mut() {
        let Some(policies_str) = param.policies.as_deref() else {
            continue;
        };
        let Ok(policies) = parse_policies(policies_str) else {
            continue;
        };

        // Find an Expiration timestamp to anchor ExpirationNotification.
        let expiration_at = policies.iter().find_map(|p| match p {
            ParsedPolicy::Expiration(ts) => Some(*ts),
            _ => None,
        });

        for policy in &policies {
            match policy {
                ParsedPolicy::ExpirationNotification { before, unit } => {
                    if param.expiration_notified {
                        continue;
                    }
                    let Some(exp) = expiration_at else { continue };
                    let window = unit.to_duration(*before);
                    if now >= exp - window {
                        state.parameter_policy_events.push(ParameterPolicyEvent {
                            parameter_name: param.name.clone(),
                            parameter_arn: param.arn.clone(),
                            event_type: "ExpirationNotification".to_string(),
                            message: format!(
                                "Parameter {} is within {} {} of its Expiration ({}).",
                                param.name,
                                before,
                                unit_str(*unit),
                                exp.to_rfc3339()
                            ),
                            created_at: now,
                        });
                        param.expiration_notified = true;
                    }
                }
                ParsedPolicy::NoChangeNotification { after, unit } => {
                    if param.no_change_notified {
                        continue;
                    }
                    let window = unit.to_duration(*after);
                    if now >= param.last_modified + window {
                        state.parameter_policy_events.push(ParameterPolicyEvent {
                            parameter_name: param.name.clone(),
                            parameter_arn: param.arn.clone(),
                            event_type: "NoChangeNotification".to_string(),
                            message: format!(
                                "Parameter {} has gone {} {} without an update.",
                                param.name,
                                after,
                                unit_str(*unit)
                            ),
                            created_at: now,
                        });
                        param.no_change_notified = true;
                    }
                }
                ParsedPolicy::Expiration(_) => {}
            }
        }
    }
}

/// Sweep expired parameters out of `state` and record one
/// `Expiration` policy event per deletion. Real AWS deletes expired
/// advanced-tier parameters at the scheduled time, so reads must
/// observe the deletion. Returns the names that were removed for
/// callers that want to log them.
pub(crate) fn purge_expired_params(state: &mut SsmState) -> Vec<String> {
    let now = Utc::now();
    let expired: Vec<String> = state
        .parameters
        .iter()
        .filter(|(_, p)| is_param_expired(p))
        .map(|(name, _)| name.clone())
        .collect();
    for name in &expired {
        if let Some(removed) = state.parameters.remove(name) {
            state.parameter_policy_events.push(ParameterPolicyEvent {
                parameter_name: removed.name.clone(),
                parameter_arn: removed.arn.clone(),
                event_type: "Expiration".to_string(),
                message: format!(
                    "Parameter {} reached its Expiration policy and was deleted.",
                    removed.name
                ),
                created_at: now,
            });
        }
    }
    expired
}

/// Build the JSON `Parameter` body for a historical version of `param`.
/// `Get*Parameter*` returns the same shape whether the lookup landed on
/// the current version or pulled an older one out of the history list,
/// so the only thing this helper has to encode is the SecureString
/// masking rule (mask the value when the caller did not pass
/// `WithDecryption=true`).
fn build_param_history_value(
    param: &SsmParameter,
    hist: &SsmParameterVersion,
    with_decryption: bool,
    region: &str,
) -> Value {
    let mut v = json!({
        "Name": param.name,
        "Type": hist.param_type,
        "Version": hist.version,
        "ARN": rewrite_arn_region(&param.arn, region),
        "LastModifiedDate": hist.last_modified.timestamp_millis() as f64 / 1000.0,
        "DataType": param.data_type,
    });
    if hist.param_type == "SecureString" && !with_decryption {
        // Mask consistently with the current-version representation
        // (`param_to_json`) and GetParameterHistory (`history_entry_json`):
        // return the `kms:<key-id>:<value>` envelope, not a bare `****`, so a
        // client parsing the `kms:` form works whether it pulled the current
        // version or an older one out of history. Use the same key-id fallback
        // as `history_entry_json` so both history paths render identically.
        let key_id = hist.key_id.as_deref().unwrap_or("alias/aws/ssm");
        v["Value"] = json!(format!("kms:{}:{}", key_id, hist.value));
    } else {
        v["Value"] = json!(hist.value);
    }
    v
}

/// Validate a parameter value against an ``AllowedPattern`` regular
/// expression. AWS rejects a ``PutParameter`` whose value does not satisfy the
/// parameter's ``AllowedPattern`` (a ``ValidationException`` that
/// ``put_parameter`` remaps to the declared ``InvalidAllowedPatternException``).
/// The pattern is applied unanchored — AWS's own documented examples carry
/// explicit `^...$` anchors, so the caller is responsible for anchoring; adding
/// our own anchors would wrongly reject values against intentionally-unanchored
/// patterns. A malformed pattern is likewise rejected.
fn validate_value_against_allowed_pattern(
    value: &str,
    pattern: &str,
) -> Result<(), AwsServiceError> {
    if pattern.is_empty() {
        return Ok(());
    }
    let re = regex::Regex::new(pattern).map_err(|_| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            format!("The AllowedPattern {pattern} is not a valid regular expression."),
        )
    })?;
    if re.is_match(value) {
        Ok(())
    } else {
        Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            format!(
                "Parameter value {value} failed to satisfy constraint: \
                 AllowedPattern: {pattern}"
            ),
        ))
    }
}

/// All fields of a ``PutParameter`` request, already parsed and validated.
struct PutParameterInput {
    name: String,
    value: String,
    param_type: Option<String>,
    overwrite: bool,
    description: Option<String>,
    key_id: Option<String>,
    allowed_pattern: Option<String>,
    data_type: String,
    /// Whether the caller explicitly included ``DataType`` in the request — we
    /// only overwrite an existing parameter's ``data_type`` when this is true.
    data_type_explicit: bool,
    tier: String,
    /// Whether the caller explicitly included ``Tier`` in the request. On
    /// overwrite we only change an existing parameter's tier when this is
    /// true; otherwise the existing tier is preserved (matching AWS).
    tier_explicit: bool,
    policies: Option<String>,
    /// Pre-parsed `policies` view, populated by ``from_body`` when the
    /// caller supplied a non-empty Policies array. Notification policies
    /// fan out to ``state.parameter_policy_events`` after the parameter
    /// is committed; Expiration policies drive lazy deletion on read.
    parsed_policies: Vec<ParsedPolicy>,
    tags: Option<Vec<(String, String)>>,
}

impl PutParameterInput {
    fn from_body(body: &Value) -> Result<Self, AwsServiceError> {
        let name = body["Name"]
            .as_str()
            .ok_or_else(|| missing("Name"))?
            .to_string();
        let value = body["Value"]
            .as_str()
            .ok_or_else(|| missing("Value"))?
            .to_string();

        if value.is_empty() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "1 validation error detected: \
                 Value '' at 'value' failed to satisfy constraint: \
                 Member must have length greater than or equal to 1.",
            ));
        }

        let param_type = body["Type"].as_str().map(|s| s.to_string());
        let data_type_explicit = body["DataType"].as_str().is_some();
        let data_type = body["DataType"].as_str().unwrap_or("text").to_string();

        if !["text", "aws:ec2:image"].contains(&data_type.as_str()) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                format!(
                    "The following data type is not supported: {data_type} \
                     (Data type names are all lowercase.)"
                ),
            ));
        }

        if let Some(ref pt) = param_type {
            if !["String", "StringList", "SecureString"].contains(&pt.as_str()) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    format!(
                        "1 validation error detected: Value '{pt}' at 'type' \
                         failed to satisfy constraint: Member must satisfy enum value set: \
                         [SecureString, StringList, String]"
                    ),
                ));
            }
        }

        if let Some(err) = validate_param_name(&name) {
            return Err(err);
        }

        let tags: Option<Vec<(String, String)>> = body["Tags"].as_array().map(|arr| {
            arr.iter()
                .filter_map(|t| {
                    let k = t["Key"].as_str()?;
                    let v = t["Value"].as_str()?;
                    Some((k.to_string(), v.to_string()))
                })
                .collect()
        });

        let tier_explicit = body["Tier"].as_str().is_some();
        let tier = body["Tier"].as_str().unwrap_or("Standard").to_string();

        // Parse + validate the Policies JSON up front so PutParameter
        // can return InvalidPolicyAttributeException without touching
        // state. Empty/absent Policies -> empty list -> no-op.
        //
        // Note: the "Policies require Advanced tier" check is NOT done
        // here because the effective tier depends on whether this is a
        // create (uses the request tier) or an overwrite (may preserve
        // the existing parameter's tier). Both paths enforce it against
        // the effective tier.
        let policies = body["Policies"].as_str().map(|s| s.to_string());
        let parsed_policies = match policies.as_deref() {
            Some(s) if !s.trim().is_empty() => parse_policies(s)?,
            _ => Vec::new(),
        };

        Ok(Self {
            name,
            value,
            param_type,
            overwrite: body["Overwrite"].as_bool().unwrap_or(false),
            description: body["Description"].as_str().map(|s| s.to_string()),
            key_id: body["KeyId"].as_str().map(|s| s.to_string()),
            allowed_pattern: body["AllowedPattern"].as_str().map(|s| s.to_string()),
            data_type,
            data_type_explicit,
            tier,
            tier_explicit,
            policies,
            parsed_policies,
            tags,
        })
    }
}

/// Apply a ``PutParameter`` overwrite to an existing parameter: validate the
/// overwrite-compatible constraints, rotate version history if we're at the
/// version limit, and update the mutable fields.
fn apply_overwrite(
    existing: &mut SsmParameter,
    input: PutParameterInput,
) -> Result<AwsResponse, AwsServiceError> {
    if !input.overwrite {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ParameterAlreadyExists",
            "The parameter already exists. To overwrite this value, set the \
             overwrite option in the request to true.",
        ));
    }

    if input.tags.is_some() {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            "Invalid request: tags and overwrite can't be used together.",
        ));
    }

    // Derive the effective tier: an explicit Tier in the request wins,
    // otherwise the existing parameter's tier is preserved across the
    // overwrite (AWS never silently downgrades a tier when Tier is omitted).
    // The tier/policy conflict is validated earlier in `put_parameter`,
    // before any KMS work, so no re-check is needed here.
    let effective_tier = if input.tier_explicit {
        input.tier.clone()
    } else {
        existing.tier.clone()
    };

    if existing.version >= PARAMETER_VERSION_LIMIT {
        let oldest_version = existing
            .history
            .first()
            .map(|h| h.version)
            .unwrap_or(existing.version);
        let oldest_has_label = existing
            .labels
            .get(&oldest_version)
            .is_some_and(|l| !l.is_empty());

        if oldest_has_label {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ParameterMaxVersionLimitExceeded",
                format!(
                    "You attempted to create a new version of {} by calling \
                     the PutParameter API with the overwrite flag. Version {}, \
                     the oldest version, can't be deleted because it has a label \
                     associated with it. Move the label to another version of the \
                     parameter, and try again.",
                    input.name, oldest_version
                ),
            ));
        }

        if !existing.history.is_empty() {
            let removed = existing.history.remove(0);
            existing.labels.remove(&removed.version);
        }
    }

    let now = Utc::now();
    let current_labels = existing
        .labels
        .get(&existing.version)
        .cloned()
        .unwrap_or_default();
    existing.history.push(SsmParameterVersion {
        value: existing.value.clone(),
        version: existing.version,
        last_modified: existing.last_modified,
        param_type: existing.param_type.clone(),
        description: existing.description.clone(),
        key_id: existing.key_id.clone(),
        labels: current_labels,
    });
    existing.version += 1;
    existing.value = input.value;
    existing.last_modified = now;

    if let Some(pt) = input.param_type {
        existing.param_type = pt;
    }
    if input.description.is_some() {
        existing.description = input.description;
    }
    if input.key_id.is_some() {
        existing.key_id = input.key_id;
    }
    // A new AllowedPattern supplied on overwrite replaces the stored one; the
    // value was already validated against the effective pattern in
    // `put_parameter`. Omitting AllowedPattern preserves the existing one.
    if input.allowed_pattern.is_some() {
        existing.allowed_pattern = input.allowed_pattern;
    }
    if input.data_type_explicit {
        existing.data_type = input.data_type;
    }
    // Replace the policy list whenever the caller explicitly passed
    // Policies. Omitting Policies on overwrite preserves whatever was
    // already attached (matches the SDK behavior of treating policies
    // as an independent property). Reset the emitted-event flags so
    // updated policies and the new value each get a fresh notification
    // window.
    if input.policies.is_some() {
        existing.policies = input.policies;
    }
    // Apply the effective tier computed (and validated) above so the
    // response echoes the correct tier and a Tier upgrade actually
    // takes effect on the stored parameter.
    existing.tier = effective_tier;
    existing.expiration_notified = false;
    existing.no_change_notified = false;

    Ok(AwsResponse::ok_json(json!({
        "Version": existing.version,
        "Tier": existing.tier,
    })))
}

/// Build a brand-new ``SsmParameter`` from a validated input. ``Type`` is
/// required for new parameters; the overwrite path allows it to be omitted.
fn create_new_parameter(
    region: &str,
    account_id: &str,
    input: PutParameterInput,
) -> Result<SsmParameter, AwsServiceError> {
    let param_type = input.param_type.ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            "A parameter type is required when you create a parameter.",
        )
    })?;

    // The tier/policy conflict (policies require Advanced tier) is validated
    // earlier in `put_parameter`, before any KMS encryption work.
    let tag_map = input
        .tags
        .map(|list| list.into_iter().collect::<BTreeMap<_, _>>())
        .unwrap_or_default();

    Ok(SsmParameter {
        arn: param_arn(region, account_id, &input.name),
        name: input.name,
        value: input.value,
        param_type,
        version: 1,
        last_modified: Utc::now(),
        history: Vec::new(),
        labels: BTreeMap::new(),
        tags: tag_map,
        description: input.description,
        allowed_pattern: input.allowed_pattern,
        key_id: input.key_id,
        data_type: input.data_type,
        tier: input.tier,
        policies: input.policies,
        expiration_notified: false,
        no_change_notified: false,
    })
}

impl SsmService {
    pub(super) fn put_parameter(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        // PutParameter's Smithy errors list does not include
        // ValidationException — failures from the shared validate_*
        // helpers are remapped to InvalidAllowedPatternException
        // (the closest declared shape covering generic bad-input).
        let mut input = PutParameterInput::from_body(&req.json_body())
            .map_err(|e| remap_validation_to(e, "InvalidAllowedPatternException"))?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        // A non-overwrite write to a parameter that already exists fails fast
        // with ParameterAlreadyExists, BEFORE the tier/policy validation (which
        // only matters for a write that will actually be applied). Otherwise a
        // duplicate PutParameter without Overwrite could surface the tier/policy
        // error instead of the expected already-exists error.
        if !input.overwrite && lookup_param(&state.parameters, &input.name).is_some() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ParameterAlreadyExists",
                "The parameter already exists. To overwrite this value, set the \
                 overwrite option in the request to true.",
            ));
        }

        // Validate the tier/policy conflict BEFORE any KMS encryption work.
        // Policies are Advanced-tier only; rejecting a Standard-tier param
        // that carries policies must happen ahead of encrypt_secure_value so
        // a bad request never triggers KMS (and never surfaces InvalidKeyId
        // ahead of the real validation error). The effective tier follows the
        // overwrite rules: an explicit Tier wins, else the existing tier is
        // preserved; a new parameter uses the request tier.
        {
            let existing = lookup_param(&state.parameters, &input.name);
            let effective_tier = match existing {
                Some(e) if !input.tier_explicit => e.tier.clone(),
                _ => input.tier.clone(),
            };
            let policies_present = match input.policies.as_deref() {
                Some(s) => !s.trim().is_empty(),
                None => existing
                    .and_then(|e| e.policies.as_deref())
                    .is_some_and(|p| !p.trim().is_empty()),
            };
            if policies_present && effective_tier != "Advanced" {
                return Err(remap_validation_to(
                    AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "ValidationException",
                        "Policies are only supported on Advanced-tier parameters. \
                         Set Tier=Advanced when including Policies.",
                    ),
                    "InvalidAllowedPatternException",
                ));
            }
        }

        // Enforce AllowedPattern against the plaintext value BEFORE any KMS
        // encryption (a SecureString value is validated in the clear). The
        // effective pattern is the one supplied in this request, or — when the
        // request omits it on an overwrite — the pattern already stored on the
        // parameter, matching AWS which keeps validating against a previously
        // set AllowedPattern.
        {
            let effective_pattern = input.allowed_pattern.clone().or_else(|| {
                lookup_param(&state.parameters, &input.name).and_then(|e| e.allowed_pattern.clone())
            });
            if let Some(pattern) = effective_pattern {
                validate_value_against_allowed_pattern(&input.value, &pattern)
                    .map_err(|e| remap_validation_to(e, "InvalidAllowedPatternException"))?;
            }
        }

        // Determine effective param_type for KMS encryption decision.
        // For overwrite, falls back to existing.param_type; for new params,
        // the caller-supplied Type is required.
        let effective_type = match lookup_param(&state.parameters, &input.name) {
            Some(existing) => input
                .param_type
                .clone()
                .unwrap_or_else(|| existing.param_type.clone()),
            None => input.param_type.clone().unwrap_or_default(),
        };
        if effective_type == "SecureString" {
            let key_id_for_enc = input.key_id.as_deref();
            let arn = param_arn(&req.region, &state.account_id, &input.name);
            input.value = self.encrypt_secure_value(
                &req.account_id,
                &req.region,
                &arn,
                key_id_for_enc,
                &input.value,
            )?;
        }

        // Hold on to the parsed policies + identifying metadata before
        // the input is consumed by overwrite/create. We use them after
        // the parameter is stored to fan out notification events.
        let parsed_policies = input.parsed_policies.clone();
        let param_name_for_events = input.name.clone();
        let param_arn_for_events = param_arn(&req.region, &state.account_id, &input.name);

        let resp = if let Some(existing) = lookup_param_mut(&mut state.parameters, &input.name) {
            apply_overwrite(existing, input)
                .map_err(|e| remap_validation_to(e, "InvalidAllowedPatternException"))?
        } else {
            let tier_for_response = input.tier.clone();
            let name = input.name.clone();
            let param = create_new_parameter(&req.region, &state.account_id, input)
                .map_err(|e| remap_validation_to(e, "InvalidAllowedPatternException"))?;
            state.parameters.insert(name, param);
            AwsResponse::ok_json(json!({
                "Version": 1,
                "Tier": tier_for_response,
            }))
        };

        record_policy_events(
            state,
            &param_name_for_events,
            &param_arn_for_events,
            &parsed_policies,
        );

        Ok(resp)
    }

    /// Encrypt a SecureString value via the configured KMS hook.
    ///
    /// When a KMS hook is wired (production server, real e2e tests) the
    /// encrypt call is strict: any failure raises `KMSAccessDeniedException`
    /// rather than silently storing plaintext. Real AWS SSM cannot
    /// PutParameter a SecureString when KMS is unavailable, and silent
    /// plaintext fallback would leak secrets on a misconfigured deployment.
    ///
    /// When no hook is wired the value is returned unchanged. This is
    /// reachable only from in-process unit tests that intentionally skip
    /// KMS wiring; production builds always set a hook via
    /// [`SsmService::with_kms_hook`].
    fn encrypt_secure_value(
        &self,
        account_id: &str,
        region: &str,
        param_arn: &str,
        key_id: Option<&str>,
        plaintext: &str,
    ) -> Result<String, AwsServiceError> {
        let Some(hook) = &self.kms_hook else {
            return Ok(plaintext.to_string());
        };
        let key = key_id.filter(|k| !k.is_empty()).unwrap_or("aws/ssm");
        let mut ctx = std::collections::HashMap::new();
        ctx.insert("PARAMETER_ARN".to_string(), param_arn.to_string());
        hook.encrypt(
            account_id,
            region,
            key,
            plaintext.as_bytes(),
            "ssm.amazonaws.com",
            ctx,
        )
        .map_err(|err| {
            // PutParameter's Smithy errors list does not include
            // KMSAccessDeniedException, so a KMS encrypt failure is
            // surfaced as InvalidKeyId (which is declared).
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidKeyId",
                format!("Unable to encrypt SecureString parameter {param_arn} via KMS: {err}"),
            )
        })
    }

    /// Decrypt a stored SecureString value via the configured KMS hook.
    /// Returns the value unchanged when no hook is wired or the value
    /// can't be decrypted (e.g. snapshots from before the hook was
    /// added). The ciphertext envelope is opaque base64 so we can't
    /// pre-check it with a prefix; rely on `decrypt` to flag malformed
    /// payloads and degrade gracefully.
    pub(crate) fn decrypt_secure_value(
        &self,
        account_id: &str,
        param_arn: &str,
        ciphertext: &str,
    ) -> String {
        let Some(hook) = &self.kms_hook else {
            return ciphertext.to_string();
        };
        let mut ctx = std::collections::HashMap::new();
        ctx.insert("PARAMETER_ARN".to_string(), param_arn.to_string());
        match hook.decrypt(account_id, ciphertext, "ssm.amazonaws.com", ctx) {
            Ok(bytes) => String::from_utf8_lossy(&bytes).to_string(),
            Err(_) => ciphertext.to_string(),
        }
    }

    /// Wrapper around [`param_to_json`] that decrypts SecureString values
    /// via the KMS hook when `with_decryption` is true. Falls through to
    /// the free function when no hook is configured or the param isn't a
    /// SecureString.
    pub(super) fn render_param_to_json(
        &self,
        p: &SsmParameter,
        with_value: bool,
        with_decryption: bool,
        region: &str,
        account_id: &str,
    ) -> Value {
        if with_value
            && with_decryption
            && p.param_type == "SecureString"
            && self.kms_hook.is_some()
        {
            let mut clone = p.clone();
            clone.value = self.decrypt_secure_value(account_id, &p.arn, &p.value);
            return param_to_json(&clone, with_value, with_decryption, region);
        }
        param_to_json(p, with_value, with_decryption, region)
    }

    /// Wrapper around [`build_param_history_value`] that decrypts the
    /// historical SecureString value via the KMS hook when
    /// `with_decryption` is true.
    pub(super) fn render_history_value(
        &self,
        param: &SsmParameter,
        hist: &SsmParameterVersion,
        with_decryption: bool,
        region: &str,
        account_id: &str,
    ) -> Value {
        if with_decryption && hist.param_type == "SecureString" && self.kms_hook.is_some() {
            let mut clone = hist.clone();
            clone.value = self.decrypt_secure_value(account_id, &param.arn, &hist.value);
            return build_param_history_value(param, &clone, with_decryption, region);
        }
        build_param_history_value(param, hist, with_decryption, region)
    }

    /// Resolve a Secrets Manager reference parameter.
    /// Path format: /aws/reference/secretsmanager/{secret-name}
    pub(super) fn resolve_secretsmanager_param(
        &self,
        raw_name: &str,
        secret_name: &str,
        region: &str,
        account_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let sm_state = self.secretsmanager_state.as_ref().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ParameterNotFound",
                format!(
                    "An error occurred (ParameterNotFound) when referencing \
                     Secrets Manager: Secret {raw_name} not found.",
                ),
            )
        })?;

        let sm_accounts = sm_state.read();
        let sm = sm_accounts.default_ref();
        let secret = sm.secrets.get(secret_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ParameterNotFound",
                format!(
                    "An error occurred (ParameterNotFound) when referencing \
                     Secrets Manager: Secret {raw_name} not found.",
                ),
            )
        })?;

        if secret.deleted {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ParameterNotFound",
                format!(
                    "An error occurred (ParameterNotFound) when referencing \
                     Secrets Manager: Secret {raw_name} not found.",
                ),
            ));
        }

        // Get the current version's secret string
        let current_vid = secret.current_version_id.as_ref().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ParameterNotFound",
                format!(
                    "An error occurred (ParameterNotFound) when referencing \
                     Secrets Manager: Secret {raw_name} not found.",
                ),
            )
        })?;
        let version = secret.versions.get(current_vid).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ParameterNotFound",
                format!(
                    "An error occurred (ParameterNotFound) when referencing \
                     Secrets Manager: Secret {raw_name} not found.",
                ),
            )
        })?;

        let value = version.secret_string.as_deref().unwrap_or("").to_string();

        let arn = Arn::new("ssm", region, account_id, &format!("parameter{raw_name}")).to_string();

        Ok(AwsResponse::ok_json(json!({
            "Parameter": {
                "Name": raw_name,
                "Type": "SecureString",
                "Value": value,
                "Version": 0,
                "ARN": arn,
                "LastModifiedDate": version.created_at.timestamp_millis() as f64 / 1000.0,
                "DataType": "text",
                "SourceResult": secret.arn,
            }
        })))
    }

    pub(super) fn get_parameter(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let raw_name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        let with_decryption = body["WithDecryption"].as_bool().unwrap_or(false);

        // Check for Secrets Manager references - require WithDecryption=true
        if raw_name.starts_with("/aws/reference/secretsmanager/") && !with_decryption {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "WithDecryption flag must be True for retrieving a Secret Manager secret.",
            ));
        }

        // Resolve Secrets Manager references via cross-service lookup
        if let Some(secret_name) = raw_name.strip_prefix("/aws/reference/secretsmanager/") {
            return self.resolve_secretsmanager_param(
                raw_name,
                secret_name,
                &req.region,
                &req.account_id,
            );
        }

        // Take the write lock so the lazy-policy sweep (delete expired,
        // emit notifications) can mutate state in line with this read.
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        purge_expired_params(state);
        tick_policy_notifications(state);

        // Handle ARN-style names directly (they contain many colons)
        if raw_name.starts_with("arn:aws:ssm:") {
            let param = resolve_param_by_name_or_arn(state, raw_name)?;
            return Ok(AwsResponse::ok_json(json!({
                "Parameter": self.render_param_to_json(param, true, with_decryption, &req.region, &req.account_id),
            })));
        }

        let (base_name, selector) = parse_param_selector(raw_name);

        // Check for invalid selectors (too many colons)
        if let ParamSelector::Invalid(n) = selector {
            return Err(param_not_found(&n));
        }

        // Try looking up by name or by ARN - use raw_name in error for full context
        let param = resolve_param_by_name_or_arn(state, base_name)
            .map_err(|_| param_not_found(raw_name))?;

        match selector {
            ParamSelector::None => Ok(AwsResponse::ok_json(json!({
                "Parameter": self.render_param_to_json(param, true, with_decryption, &req.region, &req.account_id),
            }))),
            ParamSelector::Version(ver) => {
                if param.version == ver {
                    return Ok(AwsResponse::ok_json(json!({
                        "Parameter": self.render_param_to_json(param, true, with_decryption, &req.region, &req.account_id),
                    })));
                }
                if let Some(hist) = param.history.iter().find(|h| h.version == ver) {
                    let v = self.render_history_value(
                        param,
                        hist,
                        with_decryption,
                        &req.region,
                        &req.account_id,
                    );
                    return Ok(AwsResponse::ok_json(json!({ "Parameter": v })));
                }
                Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ParameterVersionNotFound",
                    format!(
                        "Systems Manager could not find version {} of {}. \
                         Verify the version and try again.",
                        ver, base_name
                    ),
                ))
            }
            ParamSelector::Label(label) => {
                for (ver, labels) in &param.labels {
                    if labels.contains(&label) {
                        if *ver == param.version {
                            return Ok(AwsResponse::ok_json(json!({
                                "Parameter": self.render_param_to_json(param, true, with_decryption, &req.region, &req.account_id),
                            })));
                        }
                        if let Some(hist) = param.history.iter().find(|h| h.version == *ver) {
                            let v = self.render_history_value(
                                param,
                                hist,
                                with_decryption,
                                &req.region,
                                &req.account_id,
                            );
                            return Ok(AwsResponse::ok_json(json!({ "Parameter": v })));
                        }
                    }
                }
                Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ParameterVersionLabelNotFound",
                    format!(
                        "Systems Manager could not find label {} for parameter {}. \
                         Verify the label and try again.",
                        label, base_name
                    ),
                ))
            }
            ParamSelector::Invalid(_) => unreachable!(),
        }
    }

    pub(super) fn get_parameters(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let names = body["Names"].as_array().ok_or_else(|| missing("Names"))?;
        let with_decryption = body["WithDecryption"].as_bool().unwrap_or(false);

        // Validate max 10 names
        if names.len() > 10 {
            let name_strs: Vec<&str> = names.iter().filter_map(|n| n.as_str()).collect();
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                format!(
                    "1 validation error detected: \
                     Value '[{}]' at 'names' failed to satisfy constraint: \
                     Member must have length less than or equal to 10.",
                    name_strs.join(", ")
                ),
            ));
        }

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        purge_expired_params(state);
        tick_policy_notifications(state);
        let mut parameters = Vec::new();
        let mut invalid = Vec::new();
        let mut seen_names = std::collections::HashSet::new();

        for name_val in names {
            if let Some(raw_name) = name_val.as_str() {
                // Deduplicate
                if !seen_names.insert(raw_name.to_string()) {
                    continue;
                }

                // Handle ARN-style names directly. An ARN contains many colons
                // that would otherwise trip parse_param_selector into
                // ParamSelector::Invalid, landing the parameter in
                // InvalidParameters even though GetParameter accepts the same
                // ARN. Apply the same ARN->name normalization here.
                if raw_name.starts_with("arn:aws:ssm:") {
                    match resolve_param_by_name_or_arn(state, raw_name) {
                        Ok(param) => parameters.push(self.render_param_to_json(
                            param,
                            true,
                            with_decryption,
                            &req.region,
                            &req.account_id,
                        )),
                        Err(_) => invalid.push(raw_name.to_string()),
                    }
                    continue;
                }

                let (base_name, selector) = parse_param_selector(raw_name);

                match selector {
                    ParamSelector::Invalid(_) => {
                        invalid.push(raw_name.to_string());
                    }
                    ParamSelector::None => {
                        if let Some(param) = lookup_param(&state.parameters, base_name) {
                            parameters.push(self.render_param_to_json(
                                param,
                                true,
                                with_decryption,
                                &req.region,
                                &req.account_id,
                            ));
                        } else {
                            invalid.push(raw_name.to_string());
                        }
                    }
                    ParamSelector::Version(ver) => {
                        if let Some(param) = lookup_param(&state.parameters, base_name) {
                            if param.version == ver {
                                parameters.push(self.render_param_to_json(
                                    param,
                                    true,
                                    with_decryption,
                                    &req.region,
                                    &req.account_id,
                                ));
                            } else if let Some(hist) =
                                param.history.iter().find(|h| h.version == ver)
                            {
                                parameters.push(self.render_history_value(
                                    param,
                                    hist,
                                    with_decryption,
                                    &req.region,
                                    &req.account_id,
                                ));
                            } else {
                                invalid.push(raw_name.to_string());
                            }
                        } else {
                            invalid.push(raw_name.to_string());
                        }
                    }
                    ParamSelector::Label(ref label) => {
                        if let Some(param) = lookup_param(&state.parameters, base_name) {
                            let mut found = false;
                            for (ver, labels) in &param.labels {
                                if labels.contains(label) {
                                    if *ver == param.version {
                                        parameters.push(self.render_param_to_json(
                                            param,
                                            true,
                                            with_decryption,
                                            &req.region,
                                            &req.account_id,
                                        ));
                                    } else if let Some(hist) =
                                        param.history.iter().find(|h| h.version == *ver)
                                    {
                                        parameters.push(self.render_history_value(
                                            param,
                                            hist,
                                            with_decryption,
                                            &req.region,
                                            &req.account_id,
                                        ));
                                    }
                                    found = true;
                                    break;
                                }
                            }
                            if !found {
                                invalid.push(raw_name.to_string());
                            }
                        } else {
                            invalid.push(raw_name.to_string());
                        }
                    }
                }
            }
        }

        Ok(AwsResponse::ok_json(json!({
            "Parameters": parameters,
            "InvalidParameters": invalid,
        })))
    }

    pub(super) fn get_parameters_by_path(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // GetParametersByPath's Smithy errors list is
        // InvalidFilterKey / InvalidFilterOption / InvalidFilterValue /
        // InvalidKeyId / InvalidNextToken — no ValidationException.
        // Missing/invalid input surfaces as InvalidFilterValue (the
        // generic catch-all of the declared set).
        let path = body["Path"]
            .as_str()
            .ok_or_else(|| missing_with_code("Path", "InvalidFilterValue"))?;
        let recursive = body["Recursive"].as_bool().unwrap_or(false);
        let with_decryption = body["WithDecryption"].as_bool().unwrap_or(false);
        let filters = body["ParameterFilters"].as_array().cloned();
        let max_results = body["MaxResults"].as_i64().unwrap_or(10) as usize;

        // Validate MaxResults
        if max_results > 10 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidFilterValue",
                format!(
                    "1 validation error detected: \
                     Value {} at 'maxResults' failed to satisfy constraint: \
                     Member must have value less than or equal to 10",
                    max_results
                ),
            ));
        }

        // Validate path
        if !is_valid_param_path(path) {
            return Err(remap_validation_to(
                invalid_path_error(path),
                "InvalidFilterValue",
            ));
        }

        // Validate ParameterFilters for by-path (only Type, KeyId, Label, tag:* allowed)
        if let Some(ref f) = filters {
            validate_parameter_filters_by_path(f)
                .map_err(|e| remap_validation_to(e, "InvalidFilterKey"))?;
        }

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        purge_expired_params(state);
        tick_policy_notifications(state);
        let all_params: Vec<&SsmParameter> = state
            .parameters
            .values()
            .filter(|p| param_matches_path(p, path, recursive))
            .filter(|p| apply_parameter_filters(p, filters.as_ref()))
            .collect();

        let (page_params, next_token) =
            paginate_checked(&all_params, body["NextToken"].as_str(), max_results)
                .map_err(|_| super::invalid_next_token())?;
        let parameters: Vec<Value> = page_params
            .iter()
            .map(|p| {
                self.render_param_to_json(p, true, with_decryption, &req.region, &req.account_id)
            })
            .collect();

        let mut resp = json!({ "Parameters": parameters });
        if let Some(token) = next_token {
            resp["NextToken"] = json!(token);
        }

        Ok(AwsResponse::ok_json(resp))
    }

    pub(super) fn delete_parameter(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if remove_param(&mut state.parameters, name).is_none() {
            return Err(param_not_found(name));
        }

        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn delete_parameters(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let names = body["Names"].as_array().ok_or_else(|| missing("Names"))?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let mut deleted = Vec::new();
        let mut invalid = Vec::new();

        for name_val in names {
            if let Some(name) = name_val.as_str() {
                if remove_param(&mut state.parameters, name).is_some() {
                    deleted.push(name.to_string());
                } else {
                    invalid.push(name.to_string());
                }
            }
        }

        Ok(AwsResponse::ok_json(json!({
            "DeletedParameters": deleted,
            "InvalidParameters": invalid,
        })))
    }

    pub(super) fn describe_parameters(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // DescribeParameters declares InvalidFilterKey / InvalidFilterOption /
        // InvalidFilterValue / InvalidNextToken — no ValidationException.
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 50)
            .map_err(|e| remap_validation_to(e, "InvalidFilterValue"))?;
        let param_filters = body["ParameterFilters"].as_array().cloned();
        let old_filters = body["Filters"].as_array().cloned();
        let max_results = body["MaxResults"].as_i64().unwrap_or(10) as usize;

        // Can't use both Filters and ParameterFilters
        if param_filters.is_some() && old_filters.is_some() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidFilterKey",
                "You can use either Filters or ParameterFilters in a single request.",
            ));
        }

        // Validate ParameterFilters
        if let Some(ref filters) = param_filters {
            validate_parameter_filters(filters)
                .map_err(|e| remap_validation_to(e, "InvalidFilterKey"))?;
        }

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        purge_expired_params(state);
        tick_policy_notifications(state);

        // Check if any filter explicitly targets /aws/ prefix paths
        let targets_aws_prefix = param_filters.as_ref().is_some_and(|filters| {
            filters.iter().any(|f| {
                let key = f["Key"].as_str().unwrap_or("");
                if key == "Path" {
                    f["Values"].as_array().is_some_and(|vals| {
                        vals.iter()
                            .any(|v| v.as_str().is_some_and(|s| s.starts_with("/aws")))
                    })
                } else if key == "Name" {
                    f["Values"].as_array().is_some_and(|vals| {
                        vals.iter().any(|v| {
                            v.as_str().is_some_and(|s| {
                                let n = s.strip_prefix('/').unwrap_or(s);
                                n.starts_with("aws/") || n.starts_with("aws")
                            })
                        })
                    })
                } else {
                    false
                }
            })
        });

        let all_params: Vec<&SsmParameter> = state
            .parameters
            .values()
            .filter(|p| {
                // Exclude /aws/ prefix params from user queries unless explicitly targeted
                if !targets_aws_prefix && p.name.starts_with("/aws/") {
                    return false;
                }
                true
            })
            .filter(|p| apply_parameter_filters(p, param_filters.as_ref()))
            .filter(|p| apply_old_filters(p, old_filters.as_ref()))
            .collect();

        let (page_params, next_token) =
            paginate_checked(&all_params, body["NextToken"].as_str(), max_results)
                .map_err(|_| super::invalid_next_token())?;
        let parameters: Vec<Value> = page_params
            .iter()
            .map(|p| param_to_describe_json(p, &req.region))
            .collect();

        let mut resp = json!({ "Parameters": parameters });
        if let Some(token) = next_token {
            resp["NextToken"] = json!(token);
        }

        Ok(AwsResponse::ok_json(resp))
    }

    pub(super) fn get_parameter_history(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        let with_decryption = body["WithDecryption"].as_bool().unwrap_or(false);
        let max_results = body["MaxResults"].as_i64();

        if let Some(mr) = max_results {
            if mr > 50 {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    format!(
                        "1 validation error detected: Value '{mr}' at 'maxResults' \
                         failed to satisfy constraint: Member must have value less than \
                         or equal to 50."
                    ),
                ));
            }
        }
        let max_results = max_results.unwrap_or(50) as usize;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        purge_expired_params(state);
        tick_policy_notifications(state);
        let param = state
            .parameters
            .get(name)
            .ok_or_else(|| param_not_found(name))?;

        let mut all_history: Vec<Value> = param
            .history
            .iter()
            .map(|h| {
                let displayed = if with_decryption && h.param_type == "SecureString" {
                    self.decrypt_secure_value(&req.account_id, &param.arn, &h.value)
                } else {
                    h.value.clone()
                };
                history_entry_json(
                    &param.name,
                    h.version,
                    &h.param_type,
                    &displayed,
                    h.key_id.as_deref(),
                    h.description.as_deref(),
                    h.last_modified.timestamp_millis() as f64 / 1000.0,
                    param.labels.get(&h.version),
                    with_decryption,
                )
            })
            .collect();

        let displayed = if with_decryption && param.param_type == "SecureString" {
            self.decrypt_secure_value(&req.account_id, &param.arn, &param.value)
        } else {
            param.value.clone()
        };
        all_history.push(history_entry_json(
            &param.name,
            param.version,
            &param.param_type,
            &displayed,
            param.key_id.as_deref(),
            param.description.as_deref(),
            param.last_modified.timestamp_millis() as f64 / 1000.0,
            param.labels.get(&param.version),
            with_decryption,
        ));

        let (result, next_token) =
            paginate_checked(&all_history, body["NextToken"].as_str(), max_results)
                .map_err(|_| super::invalid_next_token())?;
        let mut resp = json!({ "Parameters": result });
        if let Some(token) = next_token {
            resp["NextToken"] = json!(token);
        }

        Ok(AwsResponse::ok_json(resp))
    }

    pub(super) fn label_parameter_version(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        let labels = body["Labels"].as_array().ok_or_else(|| missing("Labels"))?;
        let version = if body["ParameterVersion"].is_null() {
            None
        } else {
            Some(body["ParameterVersion"].as_i64().ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "ParameterVersion must be a valid integer",
                )
            })?)
        };

        let label_strings: Vec<String> = labels
            .iter()
            .filter_map(|l| l.as_str().map(|s| s.to_string()))
            .collect();

        validate_label_lengths(&label_strings)?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let param =
            lookup_param_mut(&mut state.parameters, name).ok_or_else(|| param_not_found(name))?;

        let target_version = version.unwrap_or(param.version);

        let version_exists = param.version == target_version
            || param.history.iter().any(|h| h.version == target_version);
        if !version_exists {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ParameterVersionNotFound",
                format!(
                    "Systems Manager could not find version {target_version} of {name}. \
                     Verify the version and try again."
                ),
            ));
        }

        let invalid_labels = collect_invalid_label_content(&label_strings);
        if !invalid_labels.is_empty() {
            return Ok(AwsResponse::ok_json(json!({
                "InvalidLabels": invalid_labels,
                "ParameterVersion": target_version,
            })));
        }

        let current_count = param
            .labels
            .get(&target_version)
            .map(|l| l.len())
            .unwrap_or(0);
        let new_unique = label_strings
            .iter()
            .filter(|l| {
                !param
                    .labels
                    .get(&target_version)
                    .is_some_and(|existing| existing.contains(l))
            })
            .count();

        if current_count + new_unique > 10 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ParameterVersionLabelLimitExceeded",
                "An error occurred (ParameterVersionLabelLimitExceeded) when \
                 calling the LabelParameterVersion operation: \
                 A parameter version can have maximum 10 labels.\
                 Move one or more labels to another version and try again.",
            ));
        }

        // Labels are unique across versions: detach from any other
        // version that already holds one of the new labels, then attach
        // to the target version.
        for existing_labels in param.labels.values_mut() {
            existing_labels.retain(|l| !label_strings.contains(l));
        }
        param.labels.retain(|_, v| !v.is_empty());

        let entry = param.labels.entry(target_version).or_default();
        for label in &label_strings {
            if !entry.contains(label) {
                entry.push(label.clone());
            }
        }

        Ok(AwsResponse::ok_json(json!({
            "InvalidLabels": [],
            "ParameterVersion": target_version,
        })))
    }

    pub(super) fn unlabel_parameter_version(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        let labels = body["Labels"].as_array().ok_or_else(|| missing("Labels"))?;
        let version = body["ParameterVersion"]
            .as_i64()
            .ok_or_else(|| missing("ParameterVersion"))?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let param =
            lookup_param_mut(&mut state.parameters, name).ok_or_else(|| param_not_found(name))?;

        // Validate version exists
        let version_exists =
            param.version == version || param.history.iter().any(|h| h.version == version);
        if !version_exists {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ParameterVersionNotFound",
                format!(
                    "Systems Manager could not find version {version} of {name}. \
                     Verify the version and try again."
                ),
            ));
        }

        let label_strings: Vec<String> = labels
            .iter()
            .filter_map(|l| l.as_str().map(|s| s.to_string()))
            .collect();

        // Find which labels don't exist on this version
        let invalid: Vec<String> = if let Some(existing) = param.labels.get(&version) {
            label_strings
                .iter()
                .filter(|l| !existing.contains(l))
                .cloned()
                .collect()
        } else {
            label_strings.clone()
        };

        // Remove labels
        if let Some(existing) = param.labels.get_mut(&version) {
            existing.retain(|l| !label_strings.contains(l));
        }

        // Clean up empty entries
        param.labels.retain(|_, v| !v.is_empty());

        Ok(AwsResponse::ok_json(json!({
            "InvalidLabels": invalid,
            "RemovedLabels": label_strings.iter().filter(|l| !invalid.contains(l)).collect::<Vec<_>>(),
        })))
    }

    // ===== Document operations =====
}

/// Strip an optional `:<version>` or `:<label>` suffix from a parameter
/// name. Real AWS SSM lets callers reference a specific version via the
/// `name:version` syntax, e.g. `MyParam:1`. The version selector is
/// orthogonal to the parameter's identity — the parameter is stored
/// once and the version selects which value/history entry to return.
fn strip_version_suffix(name: &str) -> &str {
    name.split_once(':').map(|(n, _)| n).unwrap_or(name)
}

/// Normalize a parameter name and resolve it. Tolerates leading slash
/// variants and the `name:version` selector that real AWS accepts.
pub(super) fn lookup_param<'a>(
    parameters: &'a std::collections::BTreeMap<String, SsmParameter>,
    name: &str,
) -> Option<&'a SsmParameter> {
    let bare = strip_version_suffix(name);
    if let Some(p) = parameters.get(bare) {
        return Some(p);
    }
    if let Some(stripped) = bare.strip_prefix('/') {
        parameters.get(stripped)
    } else {
        parameters.get(&format!("/{bare}"))
    }
}

pub(super) fn lookup_param_mut<'a>(
    parameters: &'a mut std::collections::BTreeMap<String, SsmParameter>,
    name: &str,
) -> Option<&'a mut SsmParameter> {
    let bare = strip_version_suffix(name);
    if parameters.contains_key(bare) {
        return parameters.get_mut(bare);
    }
    let alt = if let Some(stripped) = bare.strip_prefix('/') {
        stripped.to_string()
    } else {
        format!("/{bare}")
    };
    parameters.get_mut(&alt)
}

pub(super) fn remove_param(
    parameters: &mut std::collections::BTreeMap<String, SsmParameter>,
    name: &str,
) -> Option<SsmParameter> {
    if let Some(p) = parameters.remove(name) {
        return Some(p);
    }
    let alt = if let Some(stripped) = name.strip_prefix('/') {
        stripped.to_string()
    } else {
        format!("/{name}")
    };
    parameters.remove(&alt)
}

pub(super) fn param_arn(region: &str, account_id: &str, name: &str) -> String {
    let resource = if name.starts_with('/') {
        format!("parameter{name}")
    } else {
        format!("parameter/{name}")
    };
    Arn::new("ssm", region, account_id, &resource).to_string()
}

/// Rewrite the region component of a parameter ARN.
pub(super) fn rewrite_arn_region(arn: &str, region: &str) -> String {
    let parts: Vec<&str> = arn.splitn(6, ':').collect();
    if parts.len() == 6 {
        format!(
            "{}:{}:{}:{}:{}:{}",
            parts[0], parts[1], parts[2], region, parts[4], parts[5]
        )
    } else {
        arn.to_string()
    }
}

pub(super) fn param_to_json(
    p: &SsmParameter,
    with_value: bool,
    with_decryption: bool,
    region: &str,
) -> Value {
    let arn = rewrite_arn_region(&p.arn, region);
    let mut v = json!({
        "Name": p.name,
        "Type": p.param_type,
        "Version": p.version,
        "ARN": arn,
        "LastModifiedDate": p.last_modified.timestamp_millis() as f64 / 1000.0,
        "DataType": p.data_type,
    });
    if with_value {
        if p.param_type == "SecureString" {
            let key_id = p.key_id.as_deref().unwrap_or("alias/aws/ssm");
            if with_decryption {
                // Decrypted: return plain value
                v["Value"] = json!(p.value);
            } else {
                // Not decrypted: return kms:KEY_ID:VALUE placeholder
                v["Value"] = json!(format!("kms:{}:{}", key_id, p.value));
            }
        } else {
            v["Value"] = json!(p.value);
        }
    }
    v
}

pub(super) fn param_to_describe_json(p: &SsmParameter, region: &str) -> Value {
    let arn = rewrite_arn_region(&p.arn, region);
    let mut v = json!({
        "Name": p.name,
        "Type": p.param_type,
        "Version": p.version,
        "ARN": arn,
        "LastModifiedDate": p.last_modified.timestamp_millis() as f64 / 1000.0,
        "LastModifiedUser": "N/A",
        "DataType": p.data_type,
        "Tier": p.tier,
    });
    if let Some(desc) = &p.description {
        v["Description"] = json!(desc);
    }
    if let Some(pattern) = &p.allowed_pattern {
        v["AllowedPattern"] = json!(pattern);
    }
    if let Some(key_id) = &p.key_id {
        v["KeyId"] = json!(key_id);
    }
    // Add policies if present and valid JSON. AWS describes each
    // attached policy as `{PolicyText, PolicyType, PolicyStatus}`,
    // where PolicyText is the original JSON the caller passed in and
    // PolicyType is the `Type` field of that object.
    if let Some(policies_str) = &p.policies {
        if let Ok(parsed) = serde_json::from_str::<Value>(policies_str) {
            if let Some(arr) = parsed.as_array() {
                let policy_objects: Vec<Value> = arr
                    .iter()
                    .map(|policy| {
                        let policy_type = policy["Type"].as_str().unwrap_or("Unknown").to_string();
                        json!({
                            "PolicyText": policy.to_string(),
                            "PolicyType": policy_type,
                            "PolicyStatus": "Finished",
                        })
                    })
                    .collect();
                if !policy_objects.is_empty() {
                    v["Policies"] = json!(policy_objects);
                }
            }
        }
    }
    v
}

/// Validate parameter name restrictions. Returns an error message string on failure.
pub(super) fn validate_param_name(name: &str) -> Option<AwsServiceError> {
    let lower = name.to_lowercase();

    if let Some(stripped) = name.strip_prefix('/') {
        // Path-style names
        let first_segment = stripped.split('/').next().unwrap_or("");
        let first_lower = first_segment.to_lowercase();
        if first_lower.starts_with("aws") {
            return Some(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                format!("No access to reserved parameter name: {name}."),
            ));
        }
        if first_lower.starts_with("ssm") {
            return Some(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "Parameter name: can't be prefixed with \"ssm\" (case-insensitive). \
                 If formed as a path, it can consist of sub-paths divided by slash \
                 symbol; each sub-path can be formed as a mix of letters, numbers \
                 and the following 3 symbols .-_"
                    .to_string(),
            ));
        }
    } else {
        // Non-path names
        if lower.starts_with("aws") || lower.starts_with("ssm") {
            return Some(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "Parameter name: can't be prefixed with \"aws\" or \"ssm\" (case-insensitive)."
                    .to_string(),
            ));
        }
    }
    None
}

/// Parse a parameter name that may include version or label selector.
/// Returns (base_name, selector) where selector can be version number or label string.
pub(super) enum ParamSelector {
    None,
    Version(i64),
    Label(String),
    Invalid(String), // name with too many colons
}

pub(super) fn parse_param_selector(name: &str) -> (&str, ParamSelector) {
    // Check for `:` separator (version or label)
    if let Some(colon_pos) = name.rfind(':') {
        let base = &name[..colon_pos];
        let selector = &name[colon_pos + 1..];

        // Check if there's another colon (invalid)
        if base.contains(':') {
            return (name, ParamSelector::Invalid(name.to_string()));
        }

        if let Ok(version) = selector.parse::<i64>() {
            (base, ParamSelector::Version(version))
        } else {
            (base, ParamSelector::Label(selector.to_string()))
        }
    } else {
        (name, ParamSelector::None)
    }
}

/// Validate a path value for parameter path filters.
pub(super) fn is_valid_param_path(path: &str) -> bool {
    if !path.starts_with('/') {
        return false;
    }
    if path == "//" {
        return false;
    }
    // Each segment between slashes must contain only letters, numbers, . - _
    let segments: Vec<&str> = path.split('/').collect();
    for seg in &segments[1..] {
        if seg.is_empty() {
            continue;
        }
        if !seg
            .chars()
            .all(|c| c.is_alphanumeric() || c == '.' || c == '-' || c == '_')
        {
            return false;
        }
    }
    true
}

/// Full invalid-path error message (matches AWS format).
pub(super) fn invalid_path_error(value: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ValidationException",
        format!(
            "The parameter doesn't meet the parameter name requirements. \
             The parameter name must begin with a forward slash \"/\". \
             It can't be prefixed with \"aws\" or \"ssm\" (case-insensitive). \
             It must use only letters, numbers, or the following symbols: . \
             (period), - (hyphen), _ (underscore). \
             Special characters are not allowed. All sub-paths, if specified, \
             must use the forward slash symbol \"/\". \
             Valid example: /get/parameters2-/by1./path0_. \
             Invalid parameter name: {value}"
        ),
    )
}

/// Validate ParameterFilters for DescribeParameters.
pub(super) fn validate_parameter_filters(filters: &[Value]) -> Result<(), AwsServiceError> {
    let valid_keys = ["Path", "Name", "Type", "KeyId", "Tier"];
    let valid_key_pattern = "tag:.+|Name|Type|KeyId|Path|Label|Tier";

    // Collect structural validation errors first
    let mut errors: Vec<String> = Vec::new();

    for (i, filter) in filters.iter().enumerate() {
        let key = filter["Key"].as_str().unwrap_or("");
        let option = filter["Option"].as_str();
        let values = filter["Values"].as_array();

        // Key must match pattern
        let key_valid = valid_keys.contains(&key) || key.starts_with("tag:") || key == "Label";
        if !key_valid {
            errors.push(format!(
                "Value '{}' at 'parameterFilters.{}.key' failed to satisfy constraint: \
                 Member must satisfy regular expression pattern: {}",
                key,
                i + 1,
                valid_key_pattern
            ));
        }

        // Key length <= 132
        if key.len() > 132 {
            errors.push(format!(
                "Value '{}' at 'parameterFilters.{}.key' failed to satisfy constraint: \
                 Member must have length less than or equal to 132",
                key,
                i + 1
            ));
        }

        // Option length <= 10
        if let Some(opt) = option {
            if opt.len() > 10 {
                errors.push(format!(
                    "Value '{}' at 'parameterFilters.{}.option' failed to satisfy constraint: \
                     Member must have length less than or equal to 10",
                    opt,
                    i + 1
                ));
            }
        }

        // Values length <= 50
        if let Some(vals) = values {
            if vals.len() > 50 {
                let vals_str: Vec<&str> = vals.iter().filter_map(|v| v.as_str()).collect();
                errors.push(format!(
                    "Value '[{}]' at 'parameterFilters.{}.values' failed to satisfy constraint: \
                     Member must have length less than or equal to 50",
                    vals_str.join(", "),
                    i + 1
                ));
            }
            // Each value <= 1024
            for val in vals {
                if let Some(v) = val.as_str() {
                    if v.len() > 1024 {
                        errors.push(format!(
                            "Value '[{}]' at 'parameterFilters.{}.values' failed to satisfy constraint: \
                             Member must have length less than or equal to 1024, \
                             Member must have length greater than or equal to 1",
                            v,
                            i + 1
                        ));
                    }
                }
            }
        }
    }

    if !errors.is_empty() {
        let msg = if errors.len() == 1 {
            format!("1 validation error detected: {}", errors[0])
        } else {
            format!(
                "{} validation errors detected: {}",
                errors.len(),
                errors.join("; ")
            )
        };
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            msg,
        ));
    }

    // Semantic validation (after structural validation passes)

    // Label is not valid for DescribeParameters
    for filter in filters {
        let key = filter["Key"].as_str().unwrap_or("");
        if key == "Label" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "The following filter key is not valid: Label. \
                 Valid filter keys include: [Path, Name, Type, KeyId, Tier]",
            ));
        }
    }

    // Check for missing values (tag: filters are allowed without values - means "tag exists")
    for filter in filters {
        let key = filter["Key"].as_str().unwrap_or("");
        let values = filter["Values"].as_array();
        if !key.starts_with("tag:") && (values.is_none() || values.is_some_and(|v| v.is_empty())) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                format!("The following filter values are missing : null for filter key {key}"),
            ));
        }
    }

    // Check for duplicate keys
    let mut seen_keys = std::collections::HashSet::new();
    for filter in filters {
        let key = filter["Key"].as_str().unwrap_or("");
        if !seen_keys.insert(key) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                format!(
                    "The following filter is duplicated in the request: {key}. \
                     A request can contain only one occurrence of a specific filter."
                ),
            ));
        }
    }

    // Validate per-key constraints
    for filter in filters {
        let key = filter["Key"].as_str().unwrap_or("");
        let option = filter["Option"].as_str();
        let values = filter["Values"].as_array();

        if key == "Path" {
            // Path option must be Recursive or OneLevel, not Equals
            if let Some(opt) = option {
                if opt != "Recursive" && opt != "OneLevel" {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "ValidationException",
                        format!(
                            "The following filter option is not valid: {opt}. \
                             Valid options include: [Recursive, OneLevel]"
                        ),
                    ));
                }
            }

            // Path values can't start with aws or ssm
            if let Some(vals) = values {
                for val in vals {
                    if let Some(v) = val.as_str() {
                        if !is_valid_param_path(v) {
                            return Err(invalid_path_error(v));
                        }
                        let stripped = v.strip_prefix('/').unwrap_or(v);
                        let first_segment = stripped.split('/').next().unwrap_or("");
                        let lower = first_segment.to_lowercase();
                        if lower.starts_with("aws") || lower.starts_with("ssm") {
                            return Err(AwsServiceError::aws_error(
                                StatusCode::BAD_REQUEST,
                                "ValidationException",
                                "Filters for common parameters can't be prefixed with \
                                 \"aws\" or \"ssm\" (case-insensitive).",
                            ));
                        }
                    }
                }
            }
        }

        if key == "Tier" {
            if let Some(vals) = values {
                for val in vals {
                    if let Some(v) = val.as_str() {
                        if !["Standard", "Advanced", "Intelligent-Tiering"].contains(&v) {
                            return Err(AwsServiceError::aws_error(
                                StatusCode::BAD_REQUEST,
                                "ValidationException",
                                format!(
                                    "The following filter value is not valid: {v}. Valid \
                                     values include: [Standard, Advanced, Intelligent-Tiering]"
                                ),
                            ));
                        }
                    }
                }
            }
        }

        if key == "Type" {
            if let Some(vals) = values {
                for val in vals {
                    if let Some(v) = val.as_str() {
                        if !["String", "StringList", "SecureString"].contains(&v) {
                            return Err(AwsServiceError::aws_error(
                                StatusCode::BAD_REQUEST,
                                "ValidationException",
                                format!(
                                    "The following filter value is not valid: {v}. Valid \
                                     values include: [String, StringList, SecureString]"
                                ),
                            ));
                        }
                    }
                }
            }
        }

        if key == "Name" {
            if let Some(opt) = option {
                if !["BeginsWith", "Equals", "Contains"].contains(&opt) {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "ValidationException",
                        format!(
                            "The following filter option is not valid: {opt}. Valid \
                             options include: [BeginsWith, Equals]."
                        ),
                    ));
                }
            }
        }
    }

    Ok(())
}

pub(super) fn validate_parameter_filters_by_path(filters: &[Value]) -> Result<(), AwsServiceError> {
    for filter in filters {
        let key = filter["Key"].as_str().unwrap_or("");
        if !["Type", "KeyId", "Label"].contains(&key) && !key.starts_with("tag:") {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                format!(
                    "The following filter key is not valid: {key}. \
                     Valid filter keys include: [Type, KeyId]."
                ),
            ));
        }
    }
    Ok(())
}

pub(super) fn apply_parameter_filters(param: &SsmParameter, filters: Option<&Vec<Value>>) -> bool {
    let filters = match filters {
        Some(f) => f,
        None => return true,
    };

    for filter in filters {
        let key = match filter["Key"].as_str() {
            Some(k) => k,
            None => continue,
        };
        let option = filter["Option"].as_str().unwrap_or("Equals");
        let values: Vec<&str> = filter["Values"]
            .as_array()
            .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect())
            .unwrap_or_default();

        let matches = match key {
            "Name" => match option {
                "BeginsWith" => values.iter().any(|v| {
                    param.name.starts_with(v) || {
                        // Normalize: /foo matches foo, foo matches /foo
                        let normalized_v = v.strip_prefix('/').unwrap_or(v);
                        let normalized_name = param.name.strip_prefix('/').unwrap_or(&param.name);
                        normalized_name.starts_with(normalized_v)
                    }
                }),
                "Contains" => {
                    // Normalize name to always have leading /
                    let what = if param.name.starts_with('/') {
                        param.name.clone()
                    } else {
                        format!("/{}", param.name)
                    };
                    // Values NOT normalized for Contains (unlike Equals/BeginsWith)
                    values.iter().any(|v| what.contains(v))
                }
                "Equals" => values.iter().any(|v| {
                    param.name == *v || {
                        // Normalize: /foo matches foo, foo matches /foo
                        let normalized_v = v.strip_prefix('/').unwrap_or(v);
                        let normalized_name = param.name.strip_prefix('/').unwrap_or(&param.name);
                        normalized_name == normalized_v
                    }
                }),
                _ => true,
            },
            "Path" => {
                // Default option for Path is OneLevel
                let path_option = if option == "Equals" {
                    "OneLevel"
                } else {
                    option
                };
                match path_option {
                    "Recursive" => values.iter().any(|v| {
                        if *v == "/" {
                            true // All params are under root
                        } else {
                            let prefix = if v.ends_with('/') {
                                v.to_string()
                            } else {
                                format!("{v}/")
                            };
                            param.name.starts_with(&prefix)
                        }
                    }),
                    _ => values.iter().any(|v| {
                        if *v == "/" {
                            // Root level: no-slash params or single-level /params
                            if param.name.starts_with('/') {
                                !param.name[1..].contains('/')
                            } else {
                                !param.name.contains('/')
                            }
                        } else {
                            let prefix = if v.ends_with('/') {
                                v.to_string()
                            } else {
                                format!("{v}/")
                            };
                            param.name.starts_with(&prefix)
                                && !param.name[prefix.len()..].contains('/')
                        }
                    }),
                }
            }
            "Type" => {
                if values.is_empty() {
                    true
                } else {
                    match option {
                        "BeginsWith" => values.iter().any(|v| param.param_type.starts_with(v)),
                        _ => values.iter().any(|v| param.param_type == *v),
                    }
                }
            }
            "KeyId" => {
                // For SecureString params without explicit KeyId, default is alias/aws/ssm
                let effective_key_id = if param.param_type == "SecureString" {
                    Some(
                        param
                            .key_id
                            .as_deref()
                            .unwrap_or("alias/aws/ssm")
                            .to_string(),
                    )
                } else {
                    param.key_id.clone()
                };
                if values.is_empty() {
                    effective_key_id.is_some()
                } else {
                    match option {
                        "BeginsWith" => effective_key_id
                            .as_ref()
                            .is_some_and(|kid| values.iter().any(|v| kid.starts_with(v))),
                        _ => effective_key_id
                            .as_ref()
                            .is_some_and(|kid| values.contains(&kid.as_str())),
                    }
                }
            }
            "Tier" => values.iter().any(|v| param.tier == *v),
            "Label" => {
                let all_labels: Vec<&String> =
                    param.labels.values().flat_map(|v| v.iter()).collect();
                if values.is_empty() {
                    !all_labels.is_empty()
                } else {
                    match option {
                        "BeginsWith" => values
                            .iter()
                            .any(|v| all_labels.iter().any(|l| l.starts_with(v))),
                        "Contains" => values
                            .iter()
                            .any(|v| all_labels.iter().any(|l| l.contains(v))),
                        _ => values
                            .iter()
                            .any(|v| all_labels.iter().any(|l| l.as_str() == *v)),
                    }
                }
            }
            _ if key.starts_with("tag:") => {
                let tag_key = &key[4..];
                if let Some(tag_val) = param.tags.get(tag_key) {
                    if values.is_empty() {
                        true
                    } else {
                        match option {
                            "BeginsWith" => values.iter().any(|v| tag_val.starts_with(v)),
                            "Contains" => values.iter().any(|v| tag_val.contains(v)),
                            _ => values.contains(&tag_val.as_str()),
                        }
                    }
                } else {
                    false
                }
            }
            _ => true,
        };

        if !matches {
            return false;
        }
    }

    true
}

pub(super) fn apply_old_filters(param: &SsmParameter, filters: Option<&Vec<Value>>) -> bool {
    let filters = match filters {
        Some(f) => f,
        None => return true,
    };

    for filter in filters {
        let key = match filter["Key"].as_str() {
            Some(k) => k,
            None => continue,
        };
        let values: Vec<&str> = filter["Values"]
            .as_array()
            .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect())
            .unwrap_or_default();

        let matches = match key {
            "Name" => values.iter().any(|v| param.name.contains(v)),
            "Type" => values.iter().any(|v| param.param_type == *v),
            "KeyId" => param
                .key_id
                .as_ref()
                .is_some_and(|kid| values.contains(&kid.as_str())),
            _ => true,
        };

        if !matches {
            return false;
        }
    }

    true
}

pub(super) fn resolve_param_by_name_or_arn<'a>(
    state: &'a crate::state::SsmState,
    name: &str,
) -> Result<&'a SsmParameter, AwsServiceError> {
    // Direct name lookup with normalization
    if let Some(p) = lookup_param(&state.parameters, name) {
        return Ok(p);
    }

    // ARN lookup: arn:aws:ssm:REGION:ACCOUNT:parameter/NAME
    if name.starts_with("arn:aws:ssm:") {
        if let Some(param_part) = name.split(":parameter").nth(1) {
            if let Some(p) = lookup_param(&state.parameters, param_part) {
                return Ok(p);
            }
        }
    }

    Err(param_not_found(name))
}

pub(super) fn param_not_found(name: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ParameterNotFound",
        format!("Parameter {name} not found."),
    )
}

/// Returns true if `param.name` falls under `path` according to
/// `GetParametersByPath` semantics. Treats `/` as a special "root"
/// query, hides the `/aws/` namespace unless explicitly targeted, and
/// honors the `recursive` flag for non-recursive shallow listings.
fn param_matches_path(param: &SsmParameter, path: &str, recursive: bool) -> bool {
    let targets_aws = path.starts_with("/aws/") || path.starts_with("/aws");
    if !targets_aws && param.name.starts_with("/aws/") {
        return false;
    }

    if path == "/" {
        if recursive {
            return true;
        }
        return if let Some(rest) = param.name.strip_prefix('/') {
            !rest.contains('/')
        } else {
            !param.name.contains('/')
        };
    }

    let prefix = if path.ends_with('/') {
        path.to_string()
    } else {
        format!("{path}/")
    };
    let Some(rest) = param.name.strip_prefix(&prefix) else {
        return false;
    };
    recursive || !rest.contains('/')
}

/// Reject any label longer than 100 characters. AWS reports this with
/// the full label list inline, so we collect display strings even when
/// only one entry is over.
fn validate_label_lengths(labels: &[String]) -> Result<(), AwsServiceError> {
    for label in labels {
        if label.len() > 100 {
            let labels_display: Vec<&str> = labels.iter().map(|s| s.as_str()).collect();
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                format!(
                    "1 validation error detected: \
                     Value '[{}]' at 'labels' failed to satisfy constraint: \
                     Member must satisfy constraint: \
                     [Member must have length less than or equal to 100, Member must \
                     have length greater than or equal to 1]",
                    labels_display.join(", ")
                ),
            ));
        }
    }
    Ok(())
}

/// Collect labels that violate the content rules: reserved `aws`/`ssm`
/// prefix (case-insensitive), starts with a digit, contains `/` or `:`.
/// AWS reports these as `InvalidLabels` in the response rather than as
/// a top-level error.
fn collect_invalid_label_content(labels: &[String]) -> Vec<String> {
    labels
        .iter()
        .filter(|label| {
            let lower = label.to_lowercase();
            lower.starts_with("aws")
                || lower.starts_with("ssm")
                || label.starts_with(|c: char| c.is_ascii_digit())
                || label.contains('/')
                || label.contains(':')
        })
        .cloned()
        .collect()
}

/// Build one history entry JSON for `GetParameterHistory`. Used for both
/// the historical entries (from `param.history`) and the current version
/// — both shapes are identical apart from where the field values come
/// from. Encrypts SecureString values into `kms:<key-id>:<value>` envelopes
/// when `with_decryption` is false, matching the inline encoding the rest
/// of the parameters service uses.
#[allow(clippy::too_many_arguments)]
fn history_entry_json(
    name: &str,
    version: i64,
    param_type: &str,
    value: &str,
    key_id: Option<&str>,
    description: Option<&str>,
    last_modified_secs: f64,
    labels: Option<&Vec<String>>,
    with_decryption: bool,
) -> Value {
    let display_value = if param_type == "SecureString" && !with_decryption {
        let kid = key_id.unwrap_or("alias/aws/ssm");
        format!("kms:{kid}:{value}")
    } else {
        value.to_string()
    };
    let mut entry = json!({
        "Name": name,
        "Value": display_value,
        "Version": version,
        "LastModifiedDate": last_modified_secs,
        "Type": param_type,
    });
    if let Some(desc) = description {
        entry["Description"] = json!(desc);
    }
    if let Some(kid) = key_id {
        entry["KeyId"] = json!(kid);
    }
    entry["Labels"] = json!(labels.cloned().unwrap_or_default());
    entry
}
