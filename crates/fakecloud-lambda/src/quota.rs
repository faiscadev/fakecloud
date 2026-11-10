//! The Service Quotas quota Lambda enforces: "Function and layer storage"
//! (`lambda`/`L-2ACBD22F`, in gigabytes), per account and Region.
//!
//! Lambda keeps a copy of every deployment package it stores: the code of
//! each function's `$LATEST`, of every published version, and of every layer
//! version. Their sizes add up to the storage the quota caps (container-image
//! functions keep their code in ECR and add nothing). A deploy that would take
//! the total past the applied value fails with `CodeStorageExceededException`.
//! Enforcement is opt-in: the quota is only checked once the user switched it
//! on in Service Quotas, and a `LambdaService` without a quota provider
//! enforces nothing.
//!
//! "Concurrent executions" (`L-B99A9384`) is not enforced: cross-service
//! invocations (event source mappings, SNS, S3, EventBridge, ...) run the
//! function without going through the `Invoke` concurrency gate, so the
//! account-wide count of in-flight executions cannot be measured faithfully.
//! Its applied value is still the account limit `GetAccountSettings` reports
//! and `PutFunctionConcurrency` keeps the unreserved minimum against.

use std::sync::Arc;

use fakecloud_core::quota::{QuotaProvider, QuotaUsageSource};
use fakecloud_core::service::AwsServiceError;
use http::StatusCode;

use crate::state::{LambdaFunction, LambdaState, SharedLambdaState};

/// Service code of the Lambda quotas.
pub const SERVICE_CODE: &str = "lambda";
/// "Function and layer storage", in gigabytes.
pub const CODE_STORAGE: &str = "L-2ACBD22F";

const BYTES_PER_GB: f64 = 1024.0 * 1024.0 * 1024.0;

/// The code storage limit of (`account_id`, `region`) in bytes when it is
/// enforced. Resolve it before taking the Lambda state lock.
pub fn enforced_storage_limit(
    provider: Option<&Arc<dyn QuotaProvider>>,
    account_id: &str,
    region: &str,
) -> Option<i64> {
    provider
        .and_then(|p| p.enforced_limit(account_id, region, SERVICE_CODE, CODE_STORAGE))
        .map(gb_to_bytes)
}

fn gb_to_bytes(gb: f64) -> i64 {
    (gb.max(0.0) * BYTES_PER_GB) as i64
}

/// The bytes of code a function record stores: its package size, or nothing
/// for a container image.
pub fn stored_code_size(func: &LambdaFunction) -> i64 {
    if func.package_type == "Image" {
        0
    } else {
        func.code_size.max(0)
    }
}

/// Code storage `state` uses: every function's `$LATEST`, every published
/// version (numbered and `$LATEST.PUBLISHED`) and every layer version.
pub fn code_storage_used(state: &LambdaState) -> i64 {
    let functions: i64 = state.functions.values().map(stored_code_size).sum();
    let versions: i64 = state
        .function_version_snapshots
        .values()
        .flat_map(|versions| versions.values())
        .map(stored_code_size)
        .sum::<i64>()
        + state
            .latest_published
            .values()
            .map(stored_code_size)
            .sum::<i64>();
    let layers: i64 = state
        .layers
        .values()
        .flat_map(|l| l.versions.iter())
        .map(|v| v.code_size.max(0))
        .sum();
    functions + versions + layers
}

fn storage_exceeded() -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "CodeStorageExceededException",
        "Code storage limit exceeded.",
    )
}

/// Refuse a deploy that stores `added` more bytes of code in `state` when that
/// takes the region past the enforced `limit`. Every path that stores new
/// code (a new function, a published version, a layer version) goes through
/// here; a `None` limit accepts anything without measuring.
pub fn check_new_code(
    state: &LambdaState,
    limit: Option<i64>,
    added: i64,
) -> Result<(), AwsServiceError> {
    let Some(limit) = limit else {
        return Ok(());
    };
    if added > 0 && code_storage_used(state) + added > limit {
        return Err(storage_exceeded());
    }
    Ok(())
}

/// Refuse replacing a `$LATEST` record that stores `old_size` bytes with
/// `new` (and, when `publish_copy`, also storing `new`'s code as a published
/// version) when that grows storage past the enforced `limit`. Both `UpdateFunctionCode`
/// and a CloudFormation function update build the updated record first and
/// check it here, so S3-sourced code and container images count the same.
pub fn check_replaced_code(
    state: &LambdaState,
    limit: Option<i64>,
    old_size: i64,
    new: &LambdaFunction,
    publish_copy: bool,
) -> Result<(), AwsServiceError> {
    let Some(limit) = limit else {
        return Ok(());
    };
    let before = code_storage_used(state);
    let new_size = stored_code_size(new);
    let after = before - old_size + new_size + if publish_copy { new_size } else { 0 };
    if after > limit && after > before {
        return Err(storage_exceeded());
    }
    Ok(())
}

/// Refuse a change that stops storing `removed` bytes of code and starts
/// storing `added` when that grows storage past the enforced `limit`.
pub fn check_storage_change(
    state: &LambdaState,
    limit: Option<i64>,
    removed: i64,
    added: i64,
) -> Result<(), AwsServiceError> {
    let Some(limit) = limit else {
        return Ok(());
    };
    let before = code_storage_used(state);
    let after = before - removed + added;
    if after > limit && after > before {
        return Err(storage_exceeded());
    }
    Ok(())
}

/// "Concurrent executions", the account concurrency limit.
pub const CONCURRENT_EXECUTIONS: &str = "L-B99A9384";

/// The AWS default account concurrency limit.
pub const DEFAULT_CONCURRENT_EXECUTIONS: i64 = 1000;
/// The AWS default "Function and layer storage", in bytes (300 GB).
pub const DEFAULT_CODE_STORAGE_BYTES: i64 = 300 * 1024 * 1024 * 1024;

/// `value`, unless it is 0 or less: an unset account setting (a state migrated
/// from an older snapshot carries all-zero settings), not a limit of 0.
pub fn setting_or(value: i64, default: i64) -> i64 {
    if value > 0 {
        value
    } else {
        default
    }
}

/// A stored account setting, unless it is unset (see [`setting_or`]).
fn stored(
    state: &LambdaState,
    pick: impl Fn(&crate::state::AccountSettings) -> i64,
) -> Option<i64> {
    state
        .account_settings
        .as_ref()
        .map(pick)
        .filter(|v| setting_or(*v, 0) > 0)
}

/// The concurrency limit of `state`'s Region: the applied Service Quotas
/// value when Service Quotas is attached (`applied`, so an approved increase
/// counts), else a stored account setting, else the AWS default of 1000.
pub fn concurrency_limit(state: &LambdaState, applied: Option<f64>) -> i64 {
    applied
        .map(|v| v.max(0.0) as i64)
        .or_else(|| stored(state, |s| s.concurrent_executions))
        .unwrap_or(DEFAULT_CONCURRENT_EXECUTIONS)
}

/// The code storage limit of `state`'s Region in bytes, resolved like
/// [`concurrency_limit`] from the applied quota (in GB).
pub fn code_storage_limit(state: &LambdaState, applied_gb: Option<f64>) -> i64 {
    applied_gb
        .map(gb_to_bytes)
        .or_else(|| stored(state, |s| s.total_code_size))
        .unwrap_or(DEFAULT_CODE_STORAGE_BYTES)
}

/// Lambda always leaves part of the account's concurrency unreserved for the
/// functions without reserved concurrency: `PutFunctionConcurrency` refuses
/// a reservation that takes the unreserved pool below 100 (or below the
/// whole limit, for an account whose limit is under 100).
pub fn check_reservation(
    state: &LambdaState,
    function_name: &str,
    requested: i64,
    limit: i64,
) -> Result<(), AwsServiceError> {
    let others: i64 = state
        .function_concurrency
        .iter()
        .filter(|(name, _)| name.as_str() != function_name)
        .map(|(_, n)| *n)
        .sum();
    let minimum = limit.min(100);
    if limit - others - requested < minimum {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameterValueException",
            format!(
                "Specified ReservedConcurrentExecutions for function decreases account's \
                 UnreservedConcurrentExecution below its minimum value of [{minimum}]."
            ),
        ));
    }
    Ok(())
}

/// Reports code storage in gigabytes, the quota's unit, so Service Quotas
/// utilization reports show real usage.
pub struct LambdaQuotaUsage {
    state: SharedLambdaState,
}

impl LambdaQuotaUsage {
    pub fn new(state: SharedLambdaState) -> Arc<Self> {
        Arc::new(Self { state })
    }
}

impl QuotaUsageSource for LambdaQuotaUsage {
    fn service_codes(&self) -> &[&str] {
        &[SERVICE_CODE]
    }

    fn usage(
        &self,
        account_id: &str,
        region: &str,
        service_code: &str,
        quota_code: &str,
    ) -> Option<f64> {
        if (service_code, quota_code) != (SERVICE_CODE, CODE_STORAGE) {
            return None;
        }
        let accounts = self.state.read();
        let bytes = accounts
            .regional(account_id, region)
            .map_or(0, code_storage_used);
        Some(bytes as f64 / BYTES_PER_GB)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::AccountSettings;

    #[test]
    fn concurrency_limit_prefers_the_applied_quota_and_ignores_a_zero_setting() {
        let mut state = LambdaState::new("123456789012", "us-east-1");
        // A state migrated from an older snapshot carries all-zero settings.
        state.account_settings = Some(AccountSettings::default());
        assert_eq!(concurrency_limit(&state, None), 1000);
        assert_eq!(code_storage_limit(&state, None), DEFAULT_CODE_STORAGE_BYTES);
        assert_eq!(concurrency_limit(&state, Some(3000.0)), 3000);
        state.account_settings = Some(AccountSettings {
            concurrent_executions: 500,
            ..Default::default()
        });
        assert_eq!(concurrency_limit(&state, None), 500);
        // An applied (possibly raised) quota wins over the stored setting.
        assert_eq!(concurrency_limit(&state, Some(2000.0)), 2000);
    }

    #[test]
    fn reservation_against_a_migrated_zero_setting_uses_the_default_limit() {
        let mut state = LambdaState::new("123456789012", "us-east-1");
        state.account_settings = Some(AccountSettings::default());
        let limit = concurrency_limit(&state, None);
        assert!(check_reservation(&state, "f", 900, limit).is_ok());
        assert!(check_reservation(&state, "f", 901, limit).is_err());
    }
}
