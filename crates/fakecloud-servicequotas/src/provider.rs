//! Applied-value resolution shared by the Service Quotas API and the
//! [`QuotaProvider`] other services enforce quotas through.

use std::sync::OnceLock;

use chrono::{DateTime, Utc};

use fakecloud_core::multi_account::MultiAccountState;
use fakecloud_core::quota::QuotaProvider;
use fakecloud_organizations::SharedOrganizationsState;
use fakecloud_persistence::SnapshotHook;

use crate::catalog::{self, QuotaDef};
use crate::settings::{AccountEnforcement, RequestApproval, SharedQuotaSettings};
use crate::state::{
    applied_key, QuotaRequest, ServiceQuotasData, SharedServiceQuotasState, TemplateEntry,
};

/// The applied value of `def` for an account: the value an approved increase
/// raised it to, else the AWS default.
pub fn applied_value(data: Option<&ServiceQuotasData>, region: &str, def: &QuotaDef) -> f64 {
    data.and_then(|d| {
        d.applied
            .get(&applied_key(
                region,
                def.global,
                def.service_code,
                def.quota_code,
            ))
            .copied()
    })
    .unwrap_or_else(|| catalog::default_value(region, def))
}

/// The region a quota's ARN and applied value live in: global quotas have
/// none.
pub fn quota_region(region: &str, def: &QuotaDef) -> String {
    if def.global {
        String::new()
    } else {
        region.to_string()
    }
}

/// `arn:<partition>:servicequotas:<region>:<account>:<service>/<quota>`. The
/// region is empty for a global quota and the account is empty for an AWS
/// default (not applied) quota.
pub fn quota_arn(region: &str, account_id: &str, def: &QuotaDef) -> String {
    let resource = format!("{}/{}", def.service_code, def.quota_code);
    if def.global {
        fakecloud_aws::arn::Arn::global_in(region, "servicequotas", account_id, &resource)
            .to_string()
    } else {
        fakecloud_aws::arn::Arn::regional("servicequotas", region, account_id, &resource)
            .to_string()
    }
}

/// The JSON `Requester` AWS reports on a quota request.
pub fn requester(account_id: &str, caller_arn: &str) -> String {
    serde_json::json!({ "accountId": account_id, "callerArn": caller_arn }).to_string()
}

/// A Service Quotas request id: 32 hex characters followed by 8 alphanumerics,
/// the 40-character shape AWS returns.
pub fn new_request_id() -> String {
    use rand::distributions::Alphanumeric;
    use rand::Rng;
    let suffix: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(8)
        .map(char::from)
        .collect();
    format!("{}{suffix}", uuid::Uuid::new_v4().simple())
}

/// Apply the organization's quota request template to `account_id` if the
/// account was created in the organization after the template was
/// associated, exactly once per association.
///
/// AWS applies an associated template to every *new* account created in the
/// organization by submitting each entry as a quota increase request. Doing it
/// on the account's first Service Quotas touch (an API call, or another
/// service enforcing a quota) gives the same observable result: by the time
/// anything reads the account's quotas, the template has been applied and the
/// requests are in its history.
///
/// Under [`RequestApproval::Manual`] the requests are left `PENDING` for the
/// introspection API to decide, like any other request.
pub fn apply_template_if_new(
    state: &SharedServiceQuotasState,
    orgs: &SharedOrganizationsState,
    approval: RequestApproval,
    account_id: &str,
    now: DateTime<Utc>,
) -> bool {
    let (management, joined) = {
        let registry = orgs.read();
        let Some(org) = registry.org_of_account(account_id) else {
            return false;
        };
        if org.management_account_id == account_id {
            return false;
        }
        let joined = org
            .accounts
            .get(account_id)
            .filter(|a| a.joined_method == "CREATED")
            .map(|a| a.joined_timestamp);
        (org.management_account_id.clone(), joined)
    };

    // Which template (if any) is due, read from `guard`. Run once under the
    // read lock so the common case never takes the write lock, and again
    // under the write lock so a template the management account edits or
    // disassociates in between is never applied in its stale form.
    let due =
        |guard: &MultiAccountState<ServiceQuotasData>| -> Option<(String, Vec<TemplateEntry>)> {
            let mgmt = guard.get(&management)?;
            let associated_at = mgmt.template_associated_at?;
            let marker = format!("{management}@{}", associated_at.to_rfc3339());
            if guard
                .get(account_id)
                .and_then(|d| d.template_checked.as_deref())
                == Some(marker.as_str())
            {
                return None;
            }
            let entries = if joined.is_some_and(|j| j >= associated_at) {
                mgmt.template.values().cloned().collect()
            } else {
                Vec::new()
            };
            Some((marker, entries))
        };
    if due(&state.read()).is_none() {
        return false;
    }
    let mut guard = state.write();
    let Some((marker, entries)) = due(&guard) else {
        return false;
    };
    let data = guard.get_or_create(account_id);
    data.template_checked = Some(marker);
    for entry in entries {
        let Some(def) = catalog::quota(&entry.service_code, &entry.quota_code) else {
            continue;
        };
        let region = quota_region(&entry.aws_region, def);
        if applied_value(Some(data), &entry.aws_region, def) >= entry.desired_value {
            continue;
        }
        // Template entries are submitted as ordinary increase requests, so
        // they are decided on the same terms.
        let status = decide_submission(data, &entry.aws_region, def, entry.desired_value, approval);
        let caller =
            fakecloud_aws::arn::Arn::global_in(&entry.aws_region, "iam", &management, "root")
                .to_string();
        let id = new_request_id();
        data.requests.insert(
            id.clone(),
            QuotaRequest {
                id,
                region,
                service_code: def.service_code.to_string(),
                quota_code: def.quota_code.to_string(),
                desired_value: entry.desired_value,
                status: status.to_string(),
                case_id: None,
                created: now,
                last_updated: now,
                requester: requester(account_id, &caller),
                quota_arn: quota_arn(&entry.aws_region, account_id, def),
            },
        );
    }
    true
}

/// Decide a newly submitted increase request and return its status: a value
/// AWS would never approve is `NOT_APPROVED` in either mode; otherwise it is
/// `APPROVED` (and the applied value raised) under automatic approval, or left
/// `PENDING` for the introspection API under manual approval.
pub fn decide_submission(
    data: &mut ServiceQuotasData,
    region: &str,
    def: &QuotaDef,
    desired: f64,
    approval: RequestApproval,
) -> &'static str {
    if !approvable(data, region, def, desired) {
        return "NOT_APPROVED";
    }
    if approval == RequestApproval::Manual {
        return "PENDING";
    }
    data.applied.insert(
        applied_key(region, def.global, def.service_code, def.quota_code),
        desired,
    );
    "APPROVED"
}

/// Whether AWS would approve raising `def` to `desired`: not above the
/// quota's documented maximum, and -- for the two security-group quotas --
/// not pushing groups-per-interface times rules-per-group past 1000.
pub fn approvable(data: &ServiceQuotasData, region: &str, def: &QuotaDef, desired: f64) -> bool {
    if def.max_value.is_some_and(|m| desired > m) {
        return false;
    }
    let other = match (def.service_code, def.quota_code) {
        (catalog::VPC, catalog::SECURITY_GROUPS_PER_INTERFACE) => catalog::RULES_PER_SECURITY_GROUP,
        (catalog::VPC, catalog::RULES_PER_SECURITY_GROUP) => catalog::SECURITY_GROUPS_PER_INTERFACE,
        _ => return true,
    };
    let other_value = catalog::quota(catalog::VPC, other)
        .map(|o| applied_value(Some(data), region, o))
        .unwrap_or(0.0);
    desired * other_value <= catalog::SG_RULES_PRODUCT_LIMIT
}

/// The [`QuotaProvider`] enforcing services resolve applied values through.
pub struct ServiceQuotasProvider {
    state: SharedServiceQuotasState,
    orgs: SharedOrganizationsState,
    settings: SharedQuotaSettings,
    /// Persists Service Quotas state when a lookup applies a template. Set
    /// once the service's snapshot store exists (persistent mode only).
    snapshot_hook: OnceLock<SnapshotHook>,
}

impl ServiceQuotasProvider {
    pub fn new(
        state: SharedServiceQuotasState,
        orgs: SharedOrganizationsState,
        settings: SharedQuotaSettings,
    ) -> Self {
        Self {
            state,
            orgs,
            settings,
            snapshot_hook: OnceLock::new(),
        }
    }

    /// Persist Service Quotas state through `hook` whenever a lookup applies
    /// the organization's template to an account.
    pub fn set_snapshot_hook(&self, hook: SnapshotHook) {
        let _ = self.snapshot_hook.set(hook);
    }
}

impl ServiceQuotasProvider {
    /// Apply the organization's template to `account_id` if it is due,
    /// persisting the change.
    fn apply_template(&self, account_id: &str) {
        let approval = self.settings.read().request_approval;
        if apply_template_if_new(&self.state, &self.orgs, approval, account_id, Utc::now()) {
            // The lookup is synchronous; the snapshot write runs on the
            // runtime the enforcing service is called from.
            if let (Some(hook), Ok(rt)) = (
                self.snapshot_hook.get(),
                tokio::runtime::Handle::try_current(),
            ) {
                rt.spawn(hook());
            }
        }
    }
}

impl QuotaProvider for ServiceQuotasProvider {
    fn applied_value(
        &self,
        account_id: &str,
        region: &str,
        service_code: &str,
        quota_code: &str,
    ) -> Option<f64> {
        let def = catalog::quota(service_code, quota_code)?;
        self.apply_template(account_id);
        let guard = self.state.read();
        Some(applied_value(guard.get(account_id), region, def))
    }

    fn enforced_limit(
        &self,
        account_id: &str,
        region: &str,
        service_code: &str,
        quota_code: &str,
    ) -> Option<f64> {
        self.enforced_limits(account_id, region, service_code, &[quota_code])
            .pop()
            .flatten()
    }

    fn enforced_limits(
        &self,
        account_id: &str,
        region: &str,
        service_code: &str,
        quota_codes: &[&str],
    ) -> Vec<Option<f64>> {
        // Whether a quota is enforced is answered from the static overlay and
        // the settings; the catalog is only decoded for a quota that is.
        if !quota_codes
            .iter()
            .any(|code| catalog::is_enforceable(service_code, code))
        {
            return vec![None; quota_codes.len()];
        }
        // Enforcing touches the account's quotas, which applies a due
        // organization template (see `apply_template_if_new`) before the
        // limits are read.
        self.apply_template(account_id);
        let settings = self.settings.read();
        let guard = self.state.read();
        let account = AccountEnforcement::new(&settings, guard.get(account_id), region);
        quota_codes
            .iter()
            .map(|code| account.limit(service_code, code))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn provider(settings: crate::settings::QuotaSettings) -> ServiceQuotasProvider {
        use parking_lot::RwLock;
        use std::sync::Arc;
        ServiceQuotasProvider::new(
            Arc::new(RwLock::new(MultiAccountState::new(
                "000000000000",
                "us-east-1",
                "",
            ))),
            Arc::new(RwLock::new(Default::default())),
            Arc::new(RwLock::new(settings)),
        )
    }

    #[test]
    fn firehose_provider_reports_regional_defaults_but_enforces_none() {
        let p = provider(Default::default());
        let before = catalog::catalog_reads();
        for code in ["L-7681C790", "L-17D4EADE", "L-937C879B"] {
            assert_eq!(
                p.enforced_limit("000000000000", "us-east-2", "firehose", code),
                None
            );
        }
        assert_eq!(catalog::catalog_reads(), before);
        for (code, high, low) in [
            ("L-7681C790", 2000.0, 1000.0),
            ("L-17D4EADE", 500_000.0, 100_000.0),
            ("L-937C879B", 5.0, 1.0),
        ] {
            for (region, expected) in [
                ("us-east-1", high),
                ("us-west-2", high),
                ("eu-west-1", high),
                ("us-east-2", low),
            ] {
                for account in ["000000000000", "222222222222"] {
                    assert_eq!(
                        p.applied_value(account, region, "firehose", code),
                        Some(expected)
                    );
                    // AWS scales this capacity: its default is no limit.
                    p.settings.write().enforce_all = true;
                    assert_eq!(p.enforced_limit(account, region, "firehose", code), None);
                    p.settings.write().enforce_all = false;
                }
            }
        }
    }

    /// One batched lookup answers exactly what one lookup per code does.
    #[test]
    fn enforced_limits_match_one_lookup_per_code() {
        use fakecloud_core::quota::{
            FIREHOSE_MIB_PER_SECOND as MIB, FIREHOSE_PUT_REQUESTS_PER_SECOND as REQUESTS,
            FIREHOSE_RECORDS_PER_SECOND as RECORDS,
        };
        const ACCT: &str = "000000000000";
        let p = provider(Default::default());
        let codes = [REQUESTS, RECORDS, MIB, "L-14BB0BE7", "L-NOPE"];
        let limits = |p: &ServiceQuotasProvider| {
            for (account, region) in [
                (ACCT, "us-east-1"),
                (ACCT, "us-east-2"),
                ("222222222222", "us-east-2"),
            ] {
                let one_by_one: Vec<_> = codes
                    .iter()
                    .map(|code| p.enforced_limit(account, region, "firehose", code))
                    .collect();
                assert_eq!(
                    p.enforced_limits(account, region, "firehose", &codes),
                    one_by_one,
                    "{account}/{region}"
                );
            }
            p.enforced_limits(ACCT, "us-east-2", "firehose", &codes)
        };
        let explicit = |code: &str, v: f64| {
            p.state
                .write()
                .get_or_create(ACCT)
                .applied
                .insert(applied_key("us-east-2", false, "firehose", code), v);
        };

        assert_eq!(limits(&p), [None; 5]);
        p.settings.write().enforce_all = true;
        assert_eq!(limits(&p), [None; 5]);
        explicit(REQUESTS, 10.0);
        explicit(MIB, 0.5);
        assert_eq!(limits(&p), [Some(10.0), None, Some(0.5), None, None]);
        p.settings
            .write()
            .overrides
            .insert(format!("firehose/{MIB}"), false);
        assert_eq!(limits(&p), [Some(10.0), None, None, None, None]);
        p.settings.write().enforce_all = false;
        explicit(RECORDS, 7.0);
        p.state
            .write()
            .get_or_create(ACCT)
            .enforcement
            .insert(format!("firehose/{RECORDS}"), true);
        assert_eq!(limits(&p), [None, Some(7.0), None, None, None]);
        assert_eq!(
            p.enforced_limits(ACCT, "us-east-2", "firehose", &[]),
            Vec::<Option<f64>>::new()
        );

        // Quotas AWS does not scale are enforced at their applied value.
        p.settings.write().enforce_all = true;
        assert_eq!(
            p.enforced_limits(
                ACCT,
                "us-east-2",
                catalog::VPC,
                &["L-0EA8095F", "L-2AFB9258"]
            ),
            [Some(60.0), Some(5.0)]
        );
    }

    /// With enforcement off, an enforcing service's limit lookup on its
    /// request path never touches (or decodes) the catalog.
    #[test]
    fn unenforced_limit_lookups_do_not_read_the_catalog() {
        let p = provider(Default::default());
        let before = catalog::catalog_reads();
        for (svc, code) in [
            ("s3", "L-DC2B2D3D"),
            (catalog::VPC, "L-F678F1CE"),
            ("iam", "L-FE177D64"),
            ("lambda", "L-B99A9384"),
            ("nope", "L-00000000"),
        ] {
            assert_eq!(
                p.enforced_limit("000000000000", "us-east-1", svc, code),
                None
            );
        }
        assert_eq!(
            p.enforced_limits(
                "000000000000",
                "us-east-1",
                "firehose",
                &["L-7681C790", "L-17D4EADE", "L-937C879B", "L-14BB0BE7"]
            ),
            [None; 4]
        );
        assert_eq!(catalog::catalog_reads(), before);
    }

    #[test]
    fn enforced_limit_returns_the_applied_value_when_enforced() {
        let p = provider(crate::settings::QuotaSettings {
            enforce_all: true,
            ..Default::default()
        });
        assert_eq!(
            p.enforced_limit("000000000000", "eu-west-1", catalog::VPC, "L-F678F1CE"),
            Some(5.0)
        );
        assert_eq!(
            p.enforced_limit("000000000000", "eu-west-1", "lambda", "L-B99A9384"),
            None
        );
    }

    fn def(service: &str, code: &str) -> &'static QuotaDef {
        catalog::quota(service, code).unwrap()
    }

    /// An increase above the documented maximum is not approved; one at it is.
    #[test]
    fn increases_are_capped_at_the_documented_maximum() {
        let data = ServiceQuotasData::default();
        let cases: &[(&str, &str, f64)] = &[
            ("iam", "L-0DA4ABF3", 25.0),
            ("iam", "L-FE177D64", 10000.0),
            ("iam", "L-E95E4862", 10000.0),
            ("iam", "L-858F3967", 700.0),
            ("iam", "L-BF35879D", 20.0),
            (catalog::VPC, "L-93826ACB", 1000.0),
            (catalog::VPC, "L-085A6257", 50.0),
            (catalog::VPC, "L-BB24F6E5", 256000.0),
            (catalog::VPC, "L-CD17FD4B", 512000.0),
        ];
        for &(service, code, max) in cases {
            let d = def(service, code);
            assert!(approvable(&data, "us-east-1", d, max), "{code} at max");
            assert!(
                !approvable(&data, "us-east-1", d, max + 1.0),
                "{code} above max"
            );
        }
    }
}
