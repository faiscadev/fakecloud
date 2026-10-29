//! Which quotas fakecloud enforces, and how increase requests are decided.
//!
//! Enforcement is opt-in. A quota is enforced when, in order of precedence:
//!
//! 1. the account has a per-quota override (`enforce` or `ignore`), else
//! 2. the server has a per-quota override, else
//! 3. the global switch (`--enforce-quotas`) is on.
//!
//! Nothing is enforced by default, so a local test suite that creates more
//! resources than a fresh AWS account allows keeps working until the user
//! asks for limit fidelity. Only quotas a fakecloud service can check
//! ([`QuotaDef::enforceable`]) are ever enforced, and a quota whose capacity
//! AWS scales automatically ([`catalog::AUTO_SCALING`]) only once the account
//! set an explicit applied value for it in the region.
//!
//! The server-wide part lives in [`QuotaSettings`]; per-account overrides live
//! in [`ServiceQuotasData::enforcement`] next to the account's applied values.
//! [`AccountEnforcement`] combines them.

use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use crate::catalog::{self, QuotaDef};
use crate::state::{applied_key, ServiceQuotasData};

/// How `RequestServiceQuotaIncrease` (and template entries applied to new
/// accounts) are decided.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RequestApproval {
    /// Decided on submission: approved unless AWS would refuse the value.
    #[default]
    Auto,
    /// Left `PENDING` until approved or denied through the introspection API,
    /// so code that polls a request's status can be tested.
    Manual,
}

impl RequestApproval {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Manual => "manual",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "auto" => Some(Self::Auto),
            "manual" => Some(Self::Manual),
            _ => None,
        }
    }
}

/// Server-wide quota settings: the global enforcement switch, per-quota
/// overrides that apply to every account, and the request approval mode.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct QuotaSettings {
    /// Enforce every enforceable quota that has no override.
    #[serde(default)]
    pub enforce_all: bool,
    /// Per-quota overrides keyed by [`quota_ref`]: `true` enforces the quota,
    /// `false` ignores it, whatever `enforce_all` says.
    #[serde(default)]
    pub overrides: BTreeMap<String, bool>,
    #[serde(default)]
    pub request_approval: RequestApproval,
}

pub type SharedQuotaSettings = Arc<RwLock<QuotaSettings>>;

/// Why a quota is or is not enforced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnforcementSource {
    /// No fakecloud service checks this quota.
    NotEnforceable,
    /// The account's own override.
    AccountOverride,
    /// A server-wide override for this quota.
    Override,
    /// The global switch.
    Global,
    /// Switched on, but AWS scales this quota's capacity automatically and
    /// the account set no explicit value for it in the region, so there is
    /// no limit to enforce.
    AutoScalingDefault,
}

impl EnforcementSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NotEnforceable => "not_enforceable",
            Self::AccountOverride => "account_override",
            Self::Override => "override",
            Self::Global => "global",
            Self::AutoScalingDefault => "auto_scaling_default",
        }
    }
}

/// `service_code/quota_code`, the key overrides are stored under and the form
/// the CLI flags take.
pub fn quota_ref(def: &QuotaDef) -> String {
    quota_ref_of(def.service_code, def.quota_code)
}

/// [`quota_ref`] from the codes.
pub fn quota_ref_of(service_code: &str, quota_code: &str) -> String {
    format!("{service_code}/{quota_code}")
}

/// Resolve a `service_code/quota_code` reference to an enforceable quota (as
/// `--enforce-quota` / `--ignore-quota` take it) into its override key. The
/// quota must exist in the vendored catalog (decoded here, at startup, only
/// when such a flag is given) and be one a fakecloud service checks.
pub fn resolve_enforceable(reference: &str) -> Result<String, String> {
    let def = crate::catalog::parse_ref(reference).ok_or_else(|| {
        format!(
            "unknown quota {reference:?}: expected SERVICE_CODE/QUOTA_CODE from the Service \
             Quotas catalog"
        )
    })?;
    if !def.enforceable {
        return Err(format!(
            "quota {reference} ({}) is not enforceable: no fakecloud service checks requests \
             against it",
            def.name
        ));
    }
    Ok(quota_ref(def))
}

/// Whether `def` is enforced for the account holding `data` in `region`, and
/// why. See [`AccountEnforcement::of`].
pub fn enforcement(
    settings: &QuotaSettings,
    data: Option<&ServiceQuotasData>,
    region: &str,
    def: &QuotaDef,
) -> (bool, EnforcementSource) {
    AccountEnforcement::new(settings, data, region).of(def.service_code, def.quota_code)
}

/// Effective enforcement for one account in one region, read from one
/// settings and state snapshot: the single place both the
/// [`fakecloud_core::quota::QuotaProvider`] and the introspection view decide
/// whether a quota is enforced.
pub struct AccountEnforcement<'a> {
    settings: &'a QuotaSettings,
    data: Option<&'a ServiceQuotasData>,
    region: &'a str,
}

impl<'a> AccountEnforcement<'a> {
    pub fn new(
        settings: &'a QuotaSettings,
        data: Option<&'a ServiceQuotasData>,
        region: &'a str,
    ) -> Self {
        Self {
            settings,
            data,
            region,
        }
    }

    /// Whether `service_code/quota_code` is enforced, and why: the override
    /// precedence, except that a quota AWS scales automatically
    /// ([`crate::catalog::AUTO_SCALING`]) is enforced only at an applied value
    /// the account set explicitly for the region. The catalog is decoded only
    /// for such a quota once it is switched on, so the request path of a
    /// service that enforces nothing stays cheap.
    pub fn of(&self, service_code: &str, quota_code: &str) -> (bool, EnforcementSource) {
        if !catalog::is_enforceable(service_code, quota_code) {
            return (false, EnforcementSource::NotEnforceable);
        }
        let (on, source) = self.switch(service_code, quota_code);
        if on
            && catalog::is_auto_scaling(service_code, quota_code)
            && catalog::quota(service_code, quota_code)
                .and_then(|def| self.explicit_value(def))
                .is_none()
        {
            return (false, EnforcementSource::AutoScalingDefault);
        }
        (on, source)
    }

    /// The limit fakecloud enforces for `service_code/quota_code`: its
    /// applied value while [`AccountEnforcement::of`] says it is enforced.
    pub fn limit(&self, service_code: &str, quota_code: &str) -> Option<f64> {
        if !self.of(service_code, quota_code).0 {
            return None;
        }
        let def = catalog::quota(service_code, quota_code)?;
        Some(
            self.explicit_value(def)
                .unwrap_or_else(|| catalog::default_value(self.region, def)),
        )
    }

    /// The account switch, else the server-wide override, else the global
    /// switch.
    fn switch(&self, service_code: &str, quota_code: &str) -> (bool, EnforcementSource) {
        let key = quota_ref_of(service_code, quota_code);
        if let Some(on) = self.data.and_then(|d| d.enforcement.get(&key)) {
            return (*on, EnforcementSource::AccountOverride);
        }
        if let Some(on) = self.settings.overrides.get(&key) {
            return (*on, EnforcementSource::Override);
        }
        (self.settings.enforce_all, EnforcementSource::Global)
    }

    /// The applied value the account set for `def` in the region (an
    /// approved request or the introspection API), if any.
    fn explicit_value(&self, def: &QuotaDef) -> Option<f64> {
        let key = applied_key(self.region, def.global, def.service_code, def.quota_code);
        self.data?.applied.get(&key).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog;
    use crate::state::applied_key;

    #[test]
    fn nothing_is_enforced_by_default() {
        for def in catalog::quotas() {
            assert!(!enforcement(&QuotaSettings::default(), None, "us-east-1", def).0);
        }
    }

    #[test]
    fn account_override_beats_server_override_beats_global() {
        let def = catalog::quota(catalog::VPC, catalog::RULES_PER_SECURITY_GROUP).unwrap();
        let mut settings = QuotaSettings {
            enforce_all: true,
            ..Default::default()
        };
        assert_eq!(
            enforcement(&settings, None, "us-east-1", def),
            (true, EnforcementSource::Global)
        );
        settings.overrides.insert(quota_ref(def), false);
        assert_eq!(
            enforcement(&settings, None, "us-east-1", def),
            (false, EnforcementSource::Override)
        );
        let mut data = ServiceQuotasData::default();
        data.enforcement.insert(quota_ref(def), true);
        assert_eq!(
            enforcement(&settings, Some(&data), "us-east-1", def),
            (true, EnforcementSource::AccountOverride)
        );
    }

    #[test]
    fn a_quota_no_service_checks_is_never_enforced() {
        let def = catalog::quota("lambda", "L-B99A9384").unwrap();
        assert!(!def.enforceable);
        let mut settings = QuotaSettings {
            enforce_all: true,
            ..Default::default()
        };
        settings.overrides.insert(quota_ref(def), true);
        assert_eq!(
            enforcement(&settings, None, "us-east-1", def),
            (false, EnforcementSource::NotEnforceable)
        );
    }

    #[test]
    fn an_auto_scaling_quota_is_enforced_only_at_an_explicit_value() {
        use EnforcementSource::*;
        let def = catalog::quota(
            catalog::FIREHOSE_SERVICE_CODE,
            catalog::FIREHOSE_RECORDS_PER_SECOND,
        )
        .unwrap();
        let mut settings = QuotaSettings {
            enforce_all: true,
            ..Default::default()
        };
        let mut data = ServiceQuotasData::default();
        assert_eq!(
            enforcement(&settings, Some(&data), "us-east-2", def),
            (false, AutoScalingDefault)
        );
        assert_eq!(AutoScalingDefault.as_str(), "auto_scaling_default");
        data.applied.insert(
            applied_key("us-east-2", false, def.service_code, def.quota_code),
            9.0,
        );
        let effective = |settings: &QuotaSettings, data: &ServiceQuotasData, region| {
            enforcement(settings, Some(data), region, def)
        };
        assert_eq!(effective(&settings, &data, "us-east-2"), (true, Global));
        assert_eq!(
            effective(&settings, &data, "us-east-1"),
            (false, AutoScalingDefault)
        );
        assert_eq!(
            enforcement(&settings, None, "us-east-2", def),
            (false, AutoScalingDefault)
        );
        // Switched off, the switch says why, value or not.
        settings.overrides.insert(quota_ref(def), false);
        assert_eq!(effective(&settings, &data, "us-east-2"), (false, Override));
        assert_eq!(effective(&settings, &data, "us-east-1"), (false, Override));
        data.enforcement.insert(quota_ref(def), true);
        assert_eq!(
            effective(&settings, &data, "us-east-2"),
            (true, AccountOverride)
        );
        assert_eq!(
            effective(&settings, &data, "us-east-1"),
            (false, AutoScalingDefault)
        );

        let account = AccountEnforcement::new(&settings, Some(&data), "us-east-2");
        assert_eq!(account.limit(def.service_code, def.quota_code), Some(9.0));
        assert_eq!(
            account.limit(def.service_code, catalog::FIREHOSE_MIB_PER_SECOND),
            None
        );
        // Quotas AWS does not scale are enforced at the default.
        assert_eq!(
            account.limit(catalog::VPC, catalog::RULES_PER_SECURITY_GROUP),
            Some(60.0)
        );
        assert_eq!(account.limit("lambda", "L-B99A9384"), None);
    }

    #[test]
    fn enforce_flag_references_resolve_through_the_vendored_data() {
        assert_eq!(
            resolve_enforceable("vpc/L-2AFB9258").unwrap(),
            "vpc/L-2AFB9258"
        );
        // Codes are matched exactly as AWS publishes them.
        assert!(resolve_enforceable("VPC/L-2AFB9258")
            .unwrap_err()
            .contains("unknown quota"));
        assert!(resolve_enforceable("vpc/L-2afb9258")
            .unwrap_err()
            .contains("unknown quota"));
        assert!(resolve_enforceable("vpc")
            .unwrap_err()
            .contains("unknown quota"));
        let err = resolve_enforceable("lambda/L-B99A9384").unwrap_err();
        assert!(err.contains("not enforceable") && err.contains("Concurrent executions"));
        // Every overlay entry is a real quota in the data.
        for (svc, code) in crate::catalog::ENFORCEABLE {
            assert!(resolve_enforceable(&format!("{svc}/{code}")).is_ok());
        }
    }
}
