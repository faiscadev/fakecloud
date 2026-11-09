//! Lambda Managed Instances: a function's `CapacityProviderConfig`.
//!
//! A function created with a `CapacityProviderConfig` runs on the EC2
//! capacity of a Lambda capacity provider instead of the Lambda (default)
//! fleet. This module validates the config the way `CreateFunction` /
//! `UpdateFunctionConfiguration` do, renders it into `FunctionConfiguration`,
//! and finds the function versions that use a capacity provider (for
//! `ListFunctionVersionsByCapacityProvider`, the in-use refusal of
//! `DeleteCapacityProvider` and the per-provider version quota).

use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::auth::IamMode;
use fakecloud_core::multi_account::MultiRegionState;
use fakecloud_core::service::AwsServiceError;

use crate::extras::constraint_violation;
use crate::state::{FunctionCapacityProviderConfig, LambdaFunction, LambdaState};

/// Smithy `CapacityProviderArn` `@pattern`, quoted verbatim in the
/// validation message.
pub(crate) const CAPACITY_PROVIDER_ARN_PATTERN: &str = "arn:aws[a-zA-Z-]*:lambda:(eusc-)?[a-z]{2}((-gov)|(-iso([a-z]?)))?-[a-z]+-\\d{1}:\\d{12}:capacity-provider:[a-zA-Z0-9-_]+";

/// Smithy `CapacityProviderName` `@pattern` (a bare name or a provider ARN).
pub(crate) const CAPACITY_PROVIDER_NAME_PATTERN: &str = "(arn:aws[a-zA-Z-]*:lambda:(eusc-)?[a-z]{2}((-gov)|(-iso([a-z]?)))?-[a-z]+-\\d{1}:\\d{12}:capacity-provider:[a-zA-Z0-9-_]+)|[a-zA-Z0-9-_]+";

/// Function versions one capacity provider can carry ("Function versions
/// per capacity provider: 100. Cannot be increased.", Lambda Managed
/// Instances quotas).
pub(crate) const MAX_FUNCTION_VERSIONS_PER_CAPACITY_PROVIDER: usize = 100;

/// Default `ExecutionEnvironmentMemoryGiBPerVCpu` when the caller sets none.
const DEFAULT_MEMORY_GIB_PER_VCPU: f64 = 2.0;

/// Smallest function memory on Lambda Managed Instances (2 GB, 1 vCPU).
pub(crate) const MIN_MANAGED_INSTANCES_MEMORY_MB: i64 = 2048;

/// Upper bound of `PerExecutionEnvironmentMaxConcurrency` per vCPU.
const MAX_CONCURRENCY_PER_VCPU: i64 = 64;

const MEMBER: &str = "capacityProviderConfig.lambdaManagedInstancesCapacityProviderConfig";

/// `[a-zA-Z0-9-_]+`.
pub(crate) fn is_bare_name(s: &str) -> bool {
    !s.is_empty()
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

/// The parts of a capacity provider ARN matching
/// [`CAPACITY_PROVIDER_ARN_PATTERN`]: `(partition, region, account, name)`.
pub(crate) fn parse_capacity_provider_arn(arn: &str) -> Option<(&str, &str, &str, &str)> {
    let parts: Vec<&str> = arn.split(':').collect();
    let [prefix, partition, service, region, account, kind, name] = parts.as_slice() else {
        return None;
    };
    let partition_ok = partition
        .strip_prefix("aws")
        .is_some_and(|rest| rest.bytes().all(|b| b.is_ascii_alphabetic() || b == b'-'));
    let ok = *prefix == "arn"
        && partition_ok
        && *service == "lambda"
        && crate::extras::is_lambda_arn_region(region)
        && account.len() == 12
        && account.bytes().all(|b| b.is_ascii_digit())
        && *kind == "capacity-provider"
        && is_bare_name(name)
        && arn.len() <= 140;
    ok.then_some((partition, region, account, name))
}

/// The name a capacity provider reference names: the name itself, or the
/// last segment of a provider ARN.
pub fn name_from_reference(reference: &str) -> &str {
    parse_capacity_provider_arn(reference)
        .map(|(_, _, _, name)| name)
        .unwrap_or(reference)
}

/// A `CapacityProviderName` path label, which takes a bare name or the
/// provider's ARN: the bare name, or the `ValidationException` the label's
/// pattern raises. An ARN naming another account or region resolves to its
/// name too; the lookup in the caller's state then finds no such provider.
pub(crate) fn capacity_provider_name_from_label(label: &str) -> Result<&str, AwsServiceError> {
    if is_bare_name(label) {
        return Ok(label);
    }
    if let Some((_, _, _, name)) = parse_capacity_provider_arn(label) {
        return Ok(name);
    }
    Err(constraint_violation(
        label,
        "capacityProviderName",
        &format!(
            "Member must satisfy regular expression pattern: {CAPACITY_PROVIDER_NAME_PATTERN}"
        ),
    ))
}

fn not_null(member: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ValidationException",
        format!(
            "1 validation error detected: Value null at '{member}' failed to satisfy constraint: Member must not be null"
        ),
    )
}

fn invalid_param(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "InvalidParameterValueException",
        msg,
    )
}

fn validation(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "ValidationException", msg)
}

/// Parse a request's `CapacityProviderConfig` member, applying its Smithy
/// constraints (`ValidationException`, as Lambda's input validation
/// reports them). `None` when the request carries no config.
pub fn parse_config(
    body: &Value,
) -> Result<Option<FunctionCapacityProviderConfig>, AwsServiceError> {
    let Some(cfg) = body.get("CapacityProviderConfig").filter(|v| !v.is_null()) else {
        return Ok(None);
    };
    let inner = cfg
        .get("LambdaManagedInstancesCapacityProviderConfig")
        .filter(|v| v.is_object())
        .ok_or_else(|| not_null(MEMBER))?;
    let arn_member = format!("{MEMBER}.capacityProviderArn");
    let arn = match inner.get("CapacityProviderArn") {
        None | Some(Value::Null) => return Err(not_null(&arn_member)),
        Some(v) => v.as_str().unwrap_or_default().to_string(),
    };
    if parse_capacity_provider_arn(&arn).is_none() {
        return Err(constraint_violation(
            &arn,
            &arn_member,
            &format!(
                "Member must satisfy regular expression pattern: {CAPACITY_PROVIDER_ARN_PATTERN}"
            ),
        ));
    }

    let concurrency = match inner.get("PerExecutionEnvironmentMaxConcurrency") {
        None | Some(Value::Null) => None,
        Some(v) => {
            let member = format!("{MEMBER}.perExecutionEnvironmentMaxConcurrency");
            let n = v.as_i64().ok_or_else(|| {
                constraint_violation(&v.to_string(), &member, "Member must be an integer")
            })?;
            if n < 1 {
                return Err(constraint_violation(
                    &n.to_string(),
                    &member,
                    "Member must have value greater than or equal to 1",
                ));
            }
            if n > 1600 {
                return Err(constraint_violation(
                    &n.to_string(),
                    &member,
                    "Member must have value less than or equal to 1600",
                ));
            }
            Some(n)
        }
    };

    let ratio = match inner.get("ExecutionEnvironmentMemoryGiBPerVCpu") {
        None | Some(Value::Null) => None,
        Some(v) => {
            let member = format!("{MEMBER}.executionEnvironmentMemoryGiBPerVCpu");
            let r = v.as_f64().ok_or_else(|| {
                constraint_violation(&v.to_string(), &member, "Member must be a number")
            })?;
            if r < 2.0 {
                return Err(constraint_violation(
                    &format!("{r:?}"),
                    &member,
                    "Member must have value greater than or equal to 2",
                ));
            }
            if r > 8.0 {
                return Err(constraint_violation(
                    &format!("{r:?}"),
                    &member,
                    "Member must have value less than or equal to 8",
                ));
            }
            Some(r)
        }
    };

    Ok(Some(FunctionCapacityProviderConfig {
        capacity_provider_arn: arn,
        per_execution_environment_max_concurrency: concurrency,
        execution_environment_memory_gib_per_vcpu: ratio,
    }))
}

/// The `CapacityProviderConfig` member of `FunctionConfiguration`.
pub(crate) fn config_json(cfg: &FunctionCapacityProviderConfig) -> Value {
    let mut inner = json!({
        "CapacityProviderArn": cfg.capacity_provider_arn,
        "ExecutionEnvironmentMemoryGiBPerVCpu": effective_ratio(cfg),
    });
    if let Some(n) = cfg.per_execution_environment_max_concurrency {
        inner["PerExecutionEnvironmentMaxConcurrency"] = json!(n);
    }
    json!({ "LambdaManagedInstancesCapacityProviderConfig": inner })
}

fn effective_ratio(cfg: &FunctionCapacityProviderConfig) -> f64 {
    cfg.execution_environment_memory_gib_per_vcpu
        .unwrap_or(DEFAULT_MEMORY_GIB_PER_VCPU)
}

/// Runtimes Lambda Managed Instances supports: Java 21 and later, Python
/// 3.13 and later, Node.js 22 and later, .NET 8 and later and the OS-only
/// `provided.al2023`.
pub(crate) fn runtime_supports_managed_instances(runtime: &str) -> bool {
    fn version_at_least(rest: &str, min: &[u32]) -> bool {
        let nums: Vec<u32> = rest
            .trim_end_matches(".x")
            .split('.')
            .map_while(|p| p.parse().ok())
            .collect();
        !nums.is_empty() && nums.as_slice() >= min
    }
    if runtime == "provided.al2023" {
        return true;
    }
    if let Some(rest) = runtime.strip_prefix("python") {
        return version_at_least(rest, &[3, 13]);
    }
    if let Some(rest) = runtime.strip_prefix("nodejs") {
        return version_at_least(rest, &[22]);
    }
    if let Some(rest) = runtime.strip_prefix("java") {
        // `java8.al2`, `java17.al2023`: the suffix is not part of the version.
        return !rest.contains(".al") && version_at_least(rest, &[21]);
    }
    if let Some(rest) = runtime.strip_prefix("dotnet") {
        return !rest.starts_with("core") && version_at_least(rest, &[8]);
    }
    false
}

/// The function settings a function needs to run on Lambda Managed
/// Instances, checked against the function's final configuration.
pub struct FunctionSettings<'a> {
    pub runtime: &'a str,
    pub package_type: &'a str,
    pub memory_size: i64,
    pub snap_start: Option<&'a Value>,
    pub vpc_config: Option<&'a Value>,
}

/// Refuse settings Lambda Managed Instances cannot run with: SnapStart, a
/// VPC configuration (the capacity provider owns the networking), a runtime
/// outside the supported set, less than 2048 MB of memory, and a
/// memory-to-vCPU ratio other than 2, 4 or 8 GiB (or above the function's
/// memory).
pub(crate) fn validate_settings(
    cfg: &FunctionCapacityProviderConfig,
    settings: &FunctionSettings<'_>,
) -> Result<(), AwsServiceError> {
    let snap_start_on = settings
        .snap_start
        .and_then(|s| s.get("ApplyOn"))
        .and_then(|v| v.as_str())
        .is_some_and(|a| a != "None");
    if snap_start_on {
        return Err(invalid_param(
            "You can't use Lambda Managed Instance functions with SnapStart. Either remove the SnapStart configuration or use a non Lambda Managed Instance function type.",
        ));
    }
    let has_vpc = settings.vpc_config.is_some_and(|v| {
        v.get("SubnetIds")
            .and_then(|s| s.as_array())
            .is_some_and(|s| !s.is_empty())
            || v.get("SecurityGroupIds")
                .and_then(|s| s.as_array())
                .is_some_and(|s| !s.is_empty())
    });
    if has_vpc {
        return Err(invalid_param(
            "You can't use Lambda Managed Instance functions with VPC configurations. Either remove the VPC configuration or use a non Lambda Managed Instance function type.",
        ));
    }
    if settings.package_type != "Image"
        && !settings.runtime.is_empty()
        && !runtime_supports_managed_instances(settings.runtime)
    {
        return Err(invalid_param(format!(
            "Runtime Enum {} does not support specified feature: Lambda Managed Instances",
            settings.runtime
        )));
    }
    if settings.memory_size < MIN_MANAGED_INSTANCES_MEMORY_MB {
        return Err(validation(format!(
            "'MemorySize' value failed to satisfy constraint: Lambda Managed Instance functions must have memory size greater than or equal to {MIN_MANAGED_INSTANCES_MEMORY_MB}"
        )));
    }
    let ratio = effective_ratio(cfg);
    let ratio_mb = ratio * 1024.0;
    if ratio_mb > settings.memory_size as f64 {
        return Err(validation(format!(
            "'ExecutionEnvironmentMemoryGiBPerVCpu' value failed to satisfy constraint: Memory ratio of {ratio_mb:.1}MB per CPU exceeds function's total memory of {}MB. Supported memory-to-CPU ratios are 2GB, 4GB, or 8GB per CPU.",
            settings.memory_size
        )));
    }
    if ![2.0, 4.0, 8.0].contains(&ratio) {
        return Err(invalid_param(
            "The specified value for ExecutionEnvironmentMemoryGiBPerVCpu isn't valid. The value must be 2, 4, or 8 GiB per vCPU.",
        ));
    }
    Ok(())
}

/// Refuse a `PerExecutionEnvironmentMaxConcurrency` above 64 per vCPU of
/// the function (its memory divided by the memory-to-vCPU ratio).
pub(crate) fn validate_concurrency(
    cfg: &FunctionCapacityProviderConfig,
    memory_size: i64,
) -> Result<(), AwsServiceError> {
    let Some(requested) = cfg.per_execution_environment_max_concurrency else {
        return Ok(());
    };
    let vcpus = ((memory_size as f64) / (effective_ratio(cfg) * 1024.0)).floor() as i64;
    let limit = MAX_CONCURRENCY_PER_VCPU * vcpus.max(1);
    if requested > limit {
        return Err(invalid_param(format!(
            "The specified PerExecutionEnvironmentMaxConcurrency exceeds the limit of {MAX_CONCURRENCY_PER_VCPU} per VCpu. For your configuration, the limit is {limit}"
        )));
    }
    Ok(())
}

/// Where a function's capacity provider ARN points, checked against the
/// caller: `(account, name)` of the provider. An ARN in another region (or
/// partition) is refused as invalid. AWS refuses passing another account's
/// capacity provider; as with an execution role in another account, that is
/// refused under `--iam strict`, logged to the IAM audit target under
/// `--iam soft`, and otherwise allowed, the provider being looked up in the
/// account its ARN names.
pub(crate) fn locate_provider<'a>(
    arn: &'a str,
    caller_account: &str,
    region: &str,
    iam_mode: IamMode,
) -> Result<(&'a str, &'a str), AwsServiceError> {
    let (partition, arn_region, account, name) =
        parse_capacity_provider_arn(arn).ok_or_else(|| {
            invalid_param(format!(
                "Capacity provider arn invalid: capacity-provider:{arn}"
            ))
        })?;
    if arn_region != region || partition != fakecloud_aws::arn::partition_for(region) {
        return Err(invalid_param(format!(
            "Capacity provider arn invalid: capacity-provider:{arn}"
        )));
    }
    if account != caller_account && iam_mode.is_enabled() {
        tracing::warn!(
            target: "fakecloud::iam::audit",
            action = "lambda:PassCapacityProvider",
            resource = %arn,
            account = %caller_account,
            mode = %iam_mode,
            "cross-account pass capacity provider denied"
        );
        if iam_mode.is_strict() {
            return Err(AwsServiceError::aws_error(
                StatusCode::FORBIDDEN,
                "AccessDeniedException",
                "Cross-account pass capacity-provider is not allowed.",
            ));
        }
    }
    Ok((account, name))
}

/// `ResourceNotFoundException` for a capacity provider that does not exist,
/// naming it as the caller did.
pub(crate) fn provider_not_found(reference: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::NOT_FOUND,
        "ResourceNotFoundException",
        format!(
            "Capacity provider not found: {reference}. Check that the ARN is correct and the capacity provider exists in this Region."
        ),
    )
}

/// Resolve a function's capacity provider: refuse a misplaced or missing
/// provider (see [`locate_provider`]) and return the config with the ARN
/// the provider itself carries.
pub(crate) fn resolve(
    accounts: &MultiRegionState<LambdaState>,
    cfg: &FunctionCapacityProviderConfig,
    caller_account: &str,
    region: &str,
    iam_mode: IamMode,
) -> Result<FunctionCapacityProviderConfig, AwsServiceError> {
    let (account, name) =
        locate_provider(&cfg.capacity_provider_arn, caller_account, region, iam_mode)?;
    let provider = accounts
        .regional(account, region)
        .and_then(|s| s.capacity_providers.get(name))
        .ok_or_else(|| provider_not_found(&cfg.capacity_provider_arn))?;
    Ok(FunctionCapacityProviderConfig {
        capacity_provider_arn: provider.arn.clone(),
        ..cfg.clone()
    })
}

/// Who assigns a function to a capacity provider: the request's account,
/// region and IAM enforcement mode.
pub struct Caller<'a> {
    pub account: &'a str,
    pub region: &'a str,
    pub iam_mode: IamMode,
}

/// Every check `CreateFunction` runs on a function's capacity provider
/// config (settings, provider location and existence, concurrency, version
/// quota for the `adding` versions the request creates), shared with
/// CloudFormation's `AWS::Lambda::Function`. Returns the config with the
/// provider's canonical ARN. `current` is the function's present config on
/// an update (a version already on the provider takes no new slot).
pub fn validate_function(
    accounts: &MultiRegionState<LambdaState>,
    cfg: &FunctionCapacityProviderConfig,
    settings: &FunctionSettings<'_>,
    caller: &Caller<'_>,
    current: Option<&FunctionCapacityProviderConfig>,
    adding: usize,
) -> Result<FunctionCapacityProviderConfig, AwsServiceError> {
    validate_settings(cfg, settings)?;
    let resolved = resolve(
        accounts,
        cfg,
        caller.account,
        caller.region,
        caller.iam_mode,
    )?;
    validate_concurrency(&resolved, settings.memory_size)?;
    let already_on =
        current.is_some_and(|c| c.capacity_provider_arn == resolved.capacity_provider_arn);
    if !already_on {
        check_version_quota(
            versions_using(accounts, &resolved.capacity_provider_arn).len(),
            adding,
            &resolved.capacity_provider_arn,
        )?;
    }
    Ok(resolved)
}

/// One function version on a capacity provider:
/// `FunctionVersionsByCapacityProviderListItem`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct ProviderFunctionVersion {
    pub function_arn: String,
    pub state: &'static str,
}

fn uses(func: &LambdaFunction, provider_arn: &str) -> bool {
    func.capacity_provider_config
        .as_ref()
        .is_some_and(|c| c.capacity_provider_arn == provider_arn)
}

/// The function versions of one regional state that use `provider_arn`:
/// each function's `$LATEST`, its numbered versions and its
/// `$LATEST.PUBLISHED`, by qualified ARN.
pub(crate) fn versions_in_state(
    state: &LambdaState,
    provider_arn: &str,
) -> Vec<ProviderFunctionVersion> {
    let mut out = Vec::new();
    for func in state.functions.values().filter(|f| uses(f, provider_arn)) {
        out.push(ProviderFunctionVersion {
            function_arn: format!("{}:$LATEST", func.function_arn),
            state: "Active",
        });
    }
    for snaps in state.function_version_snapshots.values() {
        for snap in snaps.values().filter(|f| uses(f, provider_arn)) {
            out.push(ProviderFunctionVersion {
                function_arn: format!("{}:{}", snap.function_arn, snap.version),
                state: "Active",
            });
        }
    }
    for snap in state
        .latest_published
        .values()
        .filter(|f| uses(f, provider_arn))
    {
        out.push(ProviderFunctionVersion {
            function_arn: format!("{}:$LATEST.PUBLISHED", snap.function_arn),
            state: "Active",
        });
    }
    out
}

/// Every function version, in any account, that uses `provider_arn`,
/// ordered by ARN.
pub(crate) fn versions_using(
    accounts: &MultiRegionState<LambdaState>,
    provider_arn: &str,
) -> Vec<ProviderFunctionVersion> {
    let mut out: Vec<ProviderFunctionVersion> = accounts
        .iter_regional()
        .flat_map(|(_, _, state)| versions_in_state(state, provider_arn))
        .collect();
    out.sort();
    out
}

/// Refuse publishing one more version of `func` when it is a Lambda Managed
/// Instances function whose capacity provider already carries the most
/// function versions it can (counted across every account). Shared with
/// CloudFormation's `AWS::Lambda::Version`.
pub fn check_new_version_quota(
    accounts: &MultiRegionState<LambdaState>,
    func: &LambdaFunction,
) -> Result<(), AwsServiceError> {
    let Some(cpc) = &func.capacity_provider_config else {
        return Ok(());
    };
    check_version_quota(
        versions_using(accounts, &cpc.capacity_provider_arn).len(),
        1,
        &cpc.capacity_provider_arn,
    )
}

/// Refuse attaching `adding` more function versions to a provider that
/// already carries `existing`.
pub(crate) fn check_version_quota(
    existing: usize,
    adding: usize,
    provider_arn: &str,
) -> Result<(), AwsServiceError> {
    if existing + adding > MAX_FUNCTION_VERSIONS_PER_CAPACITY_PROVIDER {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "FunctionVersionsPerCapacityProviderLimitExceededException",
            format!(
                "The maximum number of function versions ({MAX_FUNCTION_VERSIONS_PER_CAPACITY_PROVIDER}) for capacity provider {provider_arn} has been exceeded."
            ),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(arn: &str) -> FunctionCapacityProviderConfig {
        FunctionCapacityProviderConfig {
            capacity_provider_arn: arn.to_string(),
            per_execution_environment_max_concurrency: None,
            execution_environment_memory_gib_per_vcpu: None,
        }
    }

    const ARN: &str = "arn:aws:lambda:us-east-1:123456789012:capacity-provider:cp1";

    fn settings(memory: i64) -> FunctionSettings<'static> {
        FunctionSettings {
            runtime: "python3.13",
            package_type: "Zip",
            memory_size: memory,
            snap_start: None,
            vpc_config: None,
        }
    }

    #[test]
    fn arn_pattern_matches_the_model() {
        assert!(parse_capacity_provider_arn(ARN).is_some());
        assert!(parse_capacity_provider_arn(
            "arn:aws-cn:lambda:cn-north-1:123456789012:capacity-provider:x"
        )
        .is_some());
        for bad in [
            "arn:aws:lambda:us-east-1:123456789012:capacity-provider/x",
            "not-an-arn",
            "arn:aws:lambda:us-east-1:12345:capacity-provider:x",
            "arn:aws:s3:us-east-1:123456789012:capacity-provider:x",
            "arn:aws:lambda:us-east-1:123456789012:capacity-provider:a.b",
        ] {
            assert!(parse_capacity_provider_arn(bad).is_none(), "{bad}");
        }
    }

    #[test]
    fn parse_config_reports_constraint_violations_like_lambda() {
        let err = parse_config(&json!({"CapacityProviderConfig": {}})).unwrap_err();
        assert_eq!(err.code(), "ValidationException");
        assert!(err.message().contains(
            "Value null at 'capacityProviderConfig.lambdaManagedInstancesCapacityProviderConfig'"
        ));

        let err = parse_config(&json!({"CapacityProviderConfig": {
            "LambdaManagedInstancesCapacityProviderConfig": {}}}))
        .unwrap_err();
        assert!(err.message().contains("capacityProviderArn' failed"));

        let err = parse_config(&json!({"CapacityProviderConfig": {
            "LambdaManagedInstancesCapacityProviderConfig": {
                "CapacityProviderArn": "arn:aws:lambda:us-east-1:123456789012:capacity-provider/x"}}}))
        .unwrap_err();
        assert!(err.message().contains("regular expression pattern"));

        for (n, needle) in [
            (0, "greater than or equal to 1"),
            (1601, "less than or equal to 1600"),
        ] {
            let err = parse_config(&json!({"CapacityProviderConfig": {
                "LambdaManagedInstancesCapacityProviderConfig": {
                    "CapacityProviderArn": ARN, "PerExecutionEnvironmentMaxConcurrency": n}}}))
            .unwrap_err();
            assert_eq!(err.code(), "ValidationException");
            assert!(err.message().contains(needle), "{}", err.message());
        }
        for (r, needle) in [(1.0, "Value '1.0'"), (9.0, "less than or equal to 8")] {
            let err = parse_config(&json!({"CapacityProviderConfig": {
                "LambdaManagedInstancesCapacityProviderConfig": {
                    "CapacityProviderArn": ARN, "ExecutionEnvironmentMemoryGiBPerVCpu": r}}}))
            .unwrap_err();
            assert!(err.message().contains(needle), "{}", err.message());
        }
        assert_eq!(parse_config(&json!({})).unwrap(), None);
    }

    #[test]
    fn supported_runtimes_follow_the_managed_instances_list() {
        for ok in [
            "python3.13",
            "python3.14",
            "nodejs22.x",
            "nodejs24.x",
            "java21",
            "java25",
            "dotnet8",
            "dotnet10",
            "provided.al2023",
        ] {
            assert!(runtime_supports_managed_instances(ok), "{ok}");
        }
        for bad in [
            "python3.9",
            "python3.12",
            "nodejs20.x",
            "java17",
            "java17.al2023",
            "java8.al2",
            "ruby3.4",
            "provided.al2",
            "dotnetcore3.1",
            "dotnet6",
            "go1.x",
        ] {
            assert!(!runtime_supports_managed_instances(bad), "{bad}");
        }
    }

    #[test]
    fn settings_refuse_what_managed_instances_cannot_run() {
        let c = cfg(ARN);
        assert!(validate_settings(&c, &settings(2048)).is_ok());

        let snap = json!({"ApplyOn": "PublishedVersions"});
        let err = validate_settings(
            &c,
            &FunctionSettings {
                snap_start: Some(&snap),
                ..settings(2048)
            },
        )
        .unwrap_err();
        assert!(err.message().contains("SnapStart"));
        let snap_off = json!({"ApplyOn": "None"});
        assert!(validate_settings(
            &c,
            &FunctionSettings {
                snap_start: Some(&snap_off),
                ..settings(2048)
            }
        )
        .is_ok());

        let vpc = json!({"SubnetIds": ["subnet-1"], "SecurityGroupIds": ["sg-1"]});
        let err = validate_settings(
            &c,
            &FunctionSettings {
                vpc_config: Some(&vpc),
                ..settings(2048)
            },
        )
        .unwrap_err();
        assert!(err.message().contains("VPC configurations"));

        let err = validate_settings(
            &c,
            &FunctionSettings {
                runtime: "ruby3.4",
                ..settings(2048)
            },
        )
        .unwrap_err();
        assert_eq!(
            err.message(),
            "Runtime Enum ruby3.4 does not support specified feature: Lambda Managed Instances"
        );
        // An image function carries no runtime to check.
        assert!(validate_settings(
            &c,
            &FunctionSettings {
                runtime: "",
                package_type: "Image",
                ..settings(2048)
            }
        )
        .is_ok());

        let err = validate_settings(&c, &settings(2047)).unwrap_err();
        assert_eq!(err.code(), "ValidationException");
        assert!(err.message().contains("greater than or equal to 2048"));
    }

    #[test]
    fn memory_ratio_must_fit_the_memory_and_be_2_4_or_8() {
        let with_ratio = |r: f64| FunctionCapacityProviderConfig {
            execution_environment_memory_gib_per_vcpu: Some(r),
            ..cfg(ARN)
        };
        let err = validate_settings(&with_ratio(4.0), &settings(2048)).unwrap_err();
        assert_eq!(err.code(), "ValidationException");
        assert!(err.message().contains(
            "Memory ratio of 4096.0MB per CPU exceeds function's total memory of 2048MB"
        ));
        let err = validate_settings(&with_ratio(2.5), &settings(2048)).unwrap_err();
        assert!(err.message().contains("2560.0MB"));
        let err = validate_settings(&with_ratio(3.0), &settings(8192)).unwrap_err();
        assert_eq!(err.code(), "InvalidParameterValueException");
        assert!(err.message().contains("must be 2, 4, or 8 GiB per vCPU"));
        assert!(validate_settings(&with_ratio(4.0), &settings(4096)).is_ok());
        assert!(validate_settings(&with_ratio(8.0), &settings(8192)).is_ok());
    }

    #[test]
    fn concurrency_is_capped_at_64_per_vcpu() {
        let with = |n: i64| FunctionCapacityProviderConfig {
            per_execution_environment_max_concurrency: Some(n),
            ..cfg(ARN)
        };
        assert!(validate_concurrency(&with(64), 2048).is_ok());
        let err = validate_concurrency(&with(65), 2048).unwrap_err();
        assert!(
            err.message().ends_with("the limit is 64"),
            "{}",
            err.message()
        );
        assert!(validate_concurrency(&with(128), 4096).is_ok());
        assert!(validate_concurrency(&with(129), 4096).is_err());
    }

    #[test]
    fn provider_location_is_checked_against_the_caller() {
        assert_eq!(
            locate_provider(ARN, "123456789012", "us-east-1", IamMode::Strict).unwrap(),
            ("123456789012", "cp1")
        );
        let err = locate_provider(
            "arn:aws:lambda:eu-west-1:123456789012:capacity-provider:x",
            "123456789012",
            "us-east-1",
            IamMode::Off,
        )
        .unwrap_err();
        assert_eq!(err.code(), "InvalidParameterValueException");
        assert_eq!(
            err.message(),
            "Capacity provider arn invalid: capacity-provider:arn:aws:lambda:eu-west-1:123456789012:capacity-provider:x"
        );
        let other = "arn:aws:lambda:us-east-1:111111111111:capacity-provider:x";
        let err = locate_provider(other, "123456789012", "us-east-1", IamMode::Strict).unwrap_err();
        assert_eq!(err.status(), StatusCode::FORBIDDEN);
        assert_eq!(err.code(), "AccessDeniedException");
        assert_eq!(
            locate_provider(other, "123456789012", "us-east-1", IamMode::Off).unwrap(),
            ("111111111111", "x")
        );
    }

    #[test]
    fn name_labels_take_a_name_or_an_arn() {
        assert_eq!(capacity_provider_name_from_label("cp1").unwrap(), "cp1");
        assert_eq!(capacity_provider_name_from_label(ARN).unwrap(), "cp1");
        let err = capacity_provider_name_from_label("bad/name").unwrap_err();
        assert_eq!(err.code(), "ValidationException");
        assert!(err.message().contains("at 'capacityProviderName'"));
    }

    #[test]
    fn config_json_reports_the_effective_ratio() {
        let out = config_json(&cfg(ARN));
        assert_eq!(
            out,
            json!({"LambdaManagedInstancesCapacityProviderConfig": {
                "CapacityProviderArn": ARN,
                "ExecutionEnvironmentMemoryGiBPerVCpu": 2.0,
            }})
        );
        let out = config_json(&FunctionCapacityProviderConfig {
            per_execution_environment_max_concurrency: Some(10),
            execution_environment_memory_gib_per_vcpu: Some(4.0),
            ..cfg(ARN)
        });
        assert_eq!(
            out["LambdaManagedInstancesCapacityProviderConfig"]
                ["PerExecutionEnvironmentMaxConcurrency"],
            10
        );
    }

    #[test]
    fn version_quota_is_100_per_provider() {
        assert!(check_version_quota(99, 1, ARN).is_ok());
        let err = check_version_quota(100, 1, ARN).unwrap_err();
        assert_eq!(
            err.code(),
            "FunctionVersionsPerCapacityProviderLimitExceededException"
        );
    }
}
