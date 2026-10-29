//! Cross-service Service Quotas wiring.
//!
//! Service Quotas owns the applied value of every quota (the AWS default,
//! raised by an approved `RequestServiceQuotaIncrease` or an Organizations
//! quota request template, or set directly through the introspection API) and
//! whether fakecloud enforces it. Enforcement is opt-in: a quota is enforced
//! only when the user turned it on, for every quota (`--enforce-quotas`) or
//! for that quota alone. A quota whose capacity AWS scales automatically
//! (Firehose throughput) is additionally enforced only at a value the user set
//! explicitly; its AWS default is never a limit. Services that enforce a quota
//! ask for its limit through [`QuotaProvider::enforced_limit`], so raising a
//! quota through the Service Quotas API changes what the enforcing service
//! accepts.
//!
//! The reverse direction is [`QuotaUsageSource`]: a service that can count its
//! own resources reports the usage behind a quota, which Service Quotas turns
//! into a quota utilization report.

/// Service code of the Amazon VPC quotas.
pub const VPC_SERVICE_CODE: &str = "vpc";
/// `vpc` quota: security groups per network interface.
pub const SECURITY_GROUPS_PER_INTERFACE: &str = "L-2AFB9258";
/// `vpc` quota: inbound or outbound rules per security group.
pub const RULES_PER_SECURITY_GROUP: &str = "L-0EA8095F";
/// AWS default for [`SECURITY_GROUPS_PER_INTERFACE`].
pub const DEFAULT_SECURITY_GROUPS_PER_INTERFACE: usize = 5;
/// AWS default for [`RULES_PER_SECURITY_GROUP`].
pub const DEFAULT_RULES_PER_SECURITY_GROUP: usize = 60;

/// Service code of the Amazon Data Firehose quotas.
pub const FIREHOSE_SERVICE_CODE: &str = "firehose";
/// Combined `PutRecord` and `PutRecordBatch` requests per second per stream.
pub const FIREHOSE_PUT_REQUESTS_PER_SECOND: &str = "L-7681C790";
/// Records per second per Firehose delivery stream.
pub const FIREHOSE_RECORDS_PER_SECOND: &str = "L-17D4EADE";
/// Mebibytes per second per Firehose delivery stream.
pub const FIREHOSE_MIB_PER_SECOND: &str = "L-937C879B";

/// Service code of the IAM quotas.
pub const IAM_SERVICE_CODE: &str = "iam";

/// An IAM `GetAccountSummary` `*Quota` entry backed by a Service Quotas quota.
#[derive(Debug, Clone, Copy)]
pub struct IamSummaryQuota {
    /// The `SummaryMap` key, e.g. `RolesQuota`.
    pub summary_key: &'static str,
    /// The `iam` Service Quotas quota code.
    pub quota_code: &'static str,
    /// The AWS default for a new account.
    pub default: f64,
}

/// The `GetAccountSummary` quota entries IAM reads from Service Quotas, so a
/// quota raised with `RequestServiceQuotaIncrease` shows up in IAM. The other
/// `*Quota` entries (policy sizes, access keys per user, ...) are fixed AWS
/// limits with no Service Quotas counterpart.
pub const IAM_SUMMARY_QUOTAS: &[IamSummaryQuota] = &[
    IamSummaryQuota {
        summary_key: "UsersQuota",
        quota_code: "L-F55AF5E4",
        default: 5000.0,
    },
    IamSummaryQuota {
        summary_key: "GroupsQuota",
        quota_code: "L-F4A5425F",
        default: 300.0,
    },
    IamSummaryQuota {
        summary_key: "ServerCertificatesQuota",
        quota_code: "L-BF35879D",
        default: 20.0,
    },
    IamSummaryQuota {
        summary_key: "PoliciesQuota",
        quota_code: "L-E95E4862",
        default: 1500.0,
    },
    IamSummaryQuota {
        summary_key: "RolesQuota",
        quota_code: "L-FE177D64",
        default: 1000.0,
    },
    IamSummaryQuota {
        summary_key: "InstanceProfilesQuota",
        quota_code: "L-6E65F664",
        default: 1000.0,
    },
    IamSummaryQuota {
        summary_key: "AttachedPoliciesPerGroupQuota",
        quota_code: "L-384571C4",
        default: 10.0,
    },
    IamSummaryQuota {
        summary_key: "AttachedPoliciesPerRoleQuota",
        quota_code: "L-0DA4ABF3",
        default: 20.0,
    },
    IamSummaryQuota {
        summary_key: "AttachedPoliciesPerUserQuota",
        quota_code: "L-4019AD8B",
        default: 10.0,
    },
    IamSummaryQuota {
        summary_key: "AssumeRolePolicySizeQuota",
        quota_code: "L-C07B4B0D",
        default: 2048.0,
    },
];

/// Resolves the applied value of a quota for an account.
pub trait QuotaProvider: Send + Sync {
    /// The applied value of `service_code`/`quota_code` for `account_id` in
    /// `region`, or `None` when the quota is unknown to Service Quotas.
    fn applied_value(
        &self,
        account_id: &str,
        region: &str,
        service_code: &str,
        quota_code: &str,
    ) -> Option<f64>;

    /// The applied value of `service_code`/`quota_code` when fakecloud
    /// enforces that quota for `account_id`, or `None` when it does not (the
    /// quota is not switched on, has no explicit value while AWS scales it
    /// automatically, or is unknown). A service refuses a request that would
    /// take it past the returned limit.
    fn enforced_limit(
        &self,
        account_id: &str,
        region: &str,
        service_code: &str,
        quota_code: &str,
    ) -> Option<f64>;

    /// [`QuotaProvider::enforced_limit`] of several quotas of one service,
    /// in `quota_codes` order, for a service that checks them together.
    fn enforced_limits(
        &self,
        account_id: &str,
        region: &str,
        service_code: &str,
        quota_codes: &[&str],
    ) -> Vec<Option<f64>> {
        quota_codes
            .iter()
            .map(|code| self.enforced_limit(account_id, region, service_code, code))
            .collect()
    }
}

/// Reports how much of a quota an account currently uses.
pub trait QuotaUsageSource: Send + Sync {
    /// The service code (`vpc`, `ec2`, ...) whose quotas this source counts.
    fn service_codes(&self) -> &[&str];

    /// Current usage of `service_code`/`quota_code` for `account_id` in
    /// `region`, or `None` when this source cannot measure that quota.
    fn usage(
        &self,
        account_id: &str,
        region: &str,
        service_code: &str,
        quota_code: &str,
    ) -> Option<f64>;
}

/// The limit of a count quota when it is enforced for `account_id` in
/// `region`: the applied value as a count (a negative value is 0, a value past
/// `usize::MAX` saturates), or `None` when there is no provider or the quota
/// is not enforced. Resolve it before taking the enforcing service's own
/// state lock: the provider takes Service Quotas' locks.
pub fn enforced_count(
    provider: Option<&std::sync::Arc<dyn QuotaProvider>>,
    account_id: &str,
    region: &str,
    service_code: &str,
    quota_code: &str,
) -> Option<usize> {
    provider
        .and_then(|p| p.enforced_limit(account_id, region, service_code, quota_code))
        .map(|v| v.max(0.0) as usize)
}

/// Whether one more resource fits beside `existing` under an enforced `limit`.
/// A `None` limit (not enforced) always has room.
pub fn has_room(limit: Option<usize>, existing: usize) -> bool {
    limit.is_none_or(|limit| existing < limit)
}

/// The failure reason of a resource a quota refused while provisioning it
/// outside the service's API (CloudFormation, Cloud Control): the service's
/// error message, status and code, as a resource handler reports a failed
/// API call.
pub fn refusal_reason(service: &str, err: &crate::service::AwsServiceError) -> String {
    format!(
        "{} (Service: {service}, Status Code: {}, Error Code: {})",
        err.message(),
        err.status().as_u16(),
        err.code()
    )
}

/// A [`QuotaProvider`] with fixed values that enforces every quota it holds,
/// for wiring a service without Service Quotas (unit tests, embedders).
#[derive(Debug, Clone, Default)]
pub struct FixedQuotas {
    quotas: Vec<(String, String, f64)>,
}

impl FixedQuotas {
    /// Enforce `service_code`/`quota_code` at `value`.
    pub fn with(mut self, service_code: &str, quota_code: &str, value: f64) -> Self {
        self.quotas
            .retain(|(s, q, _)| !(s == service_code && q == quota_code));
        self.quotas
            .push((service_code.to_string(), quota_code.to_string(), value));
        self
    }

    /// The two security-group quotas, enforced at their AWS defaults.
    pub fn security_group_defaults() -> Self {
        Self::default()
            .with(
                VPC_SERVICE_CODE,
                SECURITY_GROUPS_PER_INTERFACE,
                DEFAULT_SECURITY_GROUPS_PER_INTERFACE as f64,
            )
            .with(
                VPC_SERVICE_CODE,
                RULES_PER_SECURITY_GROUP,
                DEFAULT_RULES_PER_SECURITY_GROUP as f64,
            )
    }
}

impl QuotaProvider for FixedQuotas {
    fn applied_value(&self, _: &str, _: &str, service_code: &str, quota_code: &str) -> Option<f64> {
        self.quotas
            .iter()
            .find(|(s, q, _)| s == service_code && q == quota_code)
            .map(|(_, _, v)| *v)
    }

    fn enforced_limit(
        &self,
        account_id: &str,
        region: &str,
        service_code: &str,
        quota_code: &str,
    ) -> Option<f64> {
        self.applied_value(account_id, region, service_code, quota_code)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn enforced_count_saturates_and_floors_at_zero() {
        let p: Arc<dyn QuotaProvider> = Arc::new(
            FixedQuotas::default()
                .with("s", "neg", -3.0)
                .with("s", "big", f64::MAX)
                .with("s", "frac", 2.9),
        );
        let get = |code| enforced_count(Some(&p), "a", "r", "s", code);
        assert_eq!(get("neg"), Some(0));
        assert_eq!(get("big"), Some(usize::MAX));
        assert_eq!(get("frac"), Some(2));
        assert_eq!(get("missing"), None);
        assert_eq!(enforced_count(None, "a", "r", "s", "neg"), None);
    }

    #[test]
    fn enforced_limits_resolves_each_code_in_order() {
        let p = FixedQuotas::default()
            .with("s", "a", 1.0)
            .with("s", "b", 2.0);
        assert_eq!(
            p.enforced_limits("acct", "r", "s", &["b", "missing", "a"]),
            vec![Some(2.0), None, Some(1.0)]
        );
        assert_eq!(p.enforced_limits("acct", "r", "other", &["a"]), vec![None]);
        assert!(p.enforced_limits("acct", "r", "s", &[]).is_empty());
    }

    #[test]
    fn has_room_below_the_limit_only() {
        assert!(has_room(None, usize::MAX));
        assert!(has_room(Some(2), 1));
        assert!(!has_room(Some(2), 2));
        assert!(!has_room(Some(0), 0));
    }
}
