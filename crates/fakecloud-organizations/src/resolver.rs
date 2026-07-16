//! Implementation of [`fakecloud_core::auth::ScpResolver`] over the
//! shared organizations state.
//!
//! Walks the OU tree from root down to the account's parent, collecting
//! attachments at every node plus account-direct attachments. The
//! evaluator treats each returned document as a separate gate that
//! must allow, matching AWS's intersect-across-ancestors semantics.
//!
//! Exemptions that return `None` (layer absent, pass-through):
//!
//! - No organization exists for this fakecloud process.
//! - Principal is the management account (AWS never enforces SCPs
//!   against the management account).
//! - Principal is a service-linked role (AWS SLR exemption, same as
//!   permission boundaries).
//! - Principal's account is not enrolled in the organization (stale
//!   credentials from before the org was created, for example).

use std::sync::Arc;

use fakecloud_core::auth::{OrgMembershipResolver, Principal, PrincipalType, ScpResolver};

use crate::state::SharedOrganizationsState;

pub struct OrganizationsScpResolver {
    state: SharedOrganizationsState,
}

impl OrganizationsScpResolver {
    pub fn new(state: SharedOrganizationsState) -> Self {
        Self { state }
    }

    pub fn shared(state: SharedOrganizationsState) -> Arc<dyn ScpResolver> {
        Arc::new(Self::new(state))
    }
}

impl ScpResolver for OrganizationsScpResolver {
    fn scps_for(&self, principal: &Principal) -> Option<Vec<String>> {
        let guard = self.state.read();
        let org = guard.as_ref()?;

        // Management account: always exempt.
        if org.is_management(&principal.account_id) {
            tracing::debug!(
                target: "fakecloud::iam::audit",
                principal_arn = %principal.arn,
                "SCP resolver: management account exempt"
            );
            return None;
        }

        // Service-linked role exemption mirrors the permission-boundary
        // rule: an assumed role whose role name starts with
        // `AWSServiceRoleFor` is exempt from SCP enforcement. AWS
        // rejects attaching SCPs to SLRs at the control plane, so this
        // is defense-in-depth.
        if matches!(principal.principal_type, PrincipalType::AssumedRole)
            && slr_role_name(&principal.arn)
                .map(|n| n.starts_with("AWSServiceRoleFor"))
                .unwrap_or(false)
        {
            tracing::debug!(
                target: "fakecloud::iam::audit",
                principal_arn = %principal.arn,
                "SCP resolver: service-linked role exempt"
            );
            return None;
        }

        let account = match org.accounts.get(&principal.account_id) {
            Some(a) => a,
            None => {
                tracing::debug!(
                    target: "fakecloud::iam::audit",
                    principal_arn = %principal.arn,
                    account = %principal.account_id,
                    "SCP resolver: account not enrolled in organization; SCPs don't apply"
                );
                return None;
            }
        };

        // Walk the OU tree from the account's direct parent up to the
        // root, then reverse so the output is root-OU-first. Collect
        // each target's SCPs into the final ordered chain.
        let mut path: Vec<String> = Vec::new();
        let mut cursor = account.parent_id.clone();
        let mut seen_root = false;
        loop {
            path.push(cursor.clone());
            if cursor == org.root_id {
                seen_root = true;
                break;
            }
            match org.ous.get(&cursor) {
                Some(ou) => cursor = ou.parent_id.clone(),
                None => break,
            }
        }
        if !seen_root {
            // Defensive: if an OU chain doesn't lead to root (shouldn't
            // happen given how we build the tree), skip SCP enforcement
            // with an audit log rather than enforcing a partial chain.
            tracing::debug!(
                target: "fakecloud::iam::audit",
                principal_arn = %principal.arn,
                "SCP resolver: OU path did not reach root; bailing out"
            );
            return None;
        }
        path.reverse();
        // Account-direct attachments come last.
        path.push(principal.account_id.clone());

        // AWS semantics: SCPs attached to the **same** target are
        // unioned (OR across the policies' statements), and the union
        // documents across **different** ancestor levels are then
        // intersected (AND). Our evaluator models each slice entry as
        // an intersection gate, so emit one entry per target level
        // with its per-level union already merged. Identified by cubic.
        let mut docs: Vec<String> = Vec::new();
        for target_id in &path {
            let Some(policy_ids) = org.attachments.get(target_id) else {
                continue;
            };
            if policy_ids.is_empty() {
                continue;
            }
            let mut statements: Vec<serde_json::Value> = Vec::new();
            for pid in policy_ids {
                let Some(policy) = org.policies.get(pid) else {
                    continue;
                };
                let parsed: serde_json::Value = match serde_json::from_str(&policy.content) {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::debug!(
                            target: "fakecloud::iam::audit",
                            policy_id = %pid,
                            error = %e,
                            "SCP content failed to parse at resolve time; skipping policy"
                        );
                        continue;
                    }
                };
                match parsed.get("Statement") {
                    Some(serde_json::Value::Array(arr)) => {
                        statements.extend(arr.iter().cloned());
                    }
                    Some(single @ serde_json::Value::Object(_)) => {
                        statements.push(single.clone());
                    }
                    _ => {
                        tracing::debug!(
                            target: "fakecloud::iam::audit",
                            policy_id = %pid,
                            "SCP has no Statement array; skipping"
                        );
                    }
                }
            }
            if statements.is_empty() {
                continue;
            }
            let merged = serde_json::json!({
                "Version": "2012-10-17",
                "Statement": statements,
            });
            docs.push(merged.to_string());
        }
        Some(docs)
    }
}

/// Extract the bare role name from an `arn:aws:sts::<account>:assumed-role/<name>/<session>` ARN.
fn slr_role_name(arn: &str) -> Option<&str> {
    let rest = arn.splitn(6, ':').nth(5)?;
    let after_prefix = rest.strip_prefix("assumed-role/")?;
    after_prefix.split('/').next()
}

/// Service principal AWS registers for centralized-root-access delegated
/// administration. A member account delegated for this principal may perform
/// `sts:AssumeRoot` on other org members alongside the management account.
const ROOT_ACCESS_SERVICE_PRINCIPAL: &str = "iam.amazonaws.com";

/// [`OrgMembershipResolver`] over the shared organizations state — answers
/// whether the org topology permits a centralized-root (`AssumeRoot`) session.
pub struct OrganizationsMembershipResolver {
    state: SharedOrganizationsState,
}

impl OrganizationsMembershipResolver {
    pub fn new(state: SharedOrganizationsState) -> Self {
        Self { state }
    }

    pub fn shared(state: SharedOrganizationsState) -> Arc<dyn OrgMembershipResolver> {
        Arc::new(Self::new(state))
    }
}

impl OrgMembershipResolver for OrganizationsMembershipResolver {
    fn can_assume_root_into(&self, caller_account: &str, target_account: &str) -> bool {
        let guard = self.state.read();
        let Some(org) = guard.as_ref() else {
            // No organization exists — there is no centralized root access to
            // grant, so cross-account AssumeRoot is never permitted.
            return false;
        };
        // The target must be enrolled in this organization.
        if !org.accounts.contains_key(target_account) {
            return false;
        }
        // The caller must be the management account or a registered delegated
        // administrator for centralized root access.
        if org.is_management(caller_account) {
            return true;
        }
        org.delegated_administrators
            .get(ROOT_ACCESS_SERVICE_PRINCIPAL)
            .is_some_and(|admins| admins.contains_key(caller_account))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::OrganizationState;
    use fakecloud_aws::arn::Arn;
    use parking_lot::RwLock;

    fn shared(org: OrganizationState) -> SharedOrganizationsState {
        Arc::new(RwLock::new(Some(org)))
    }

    fn user_principal(account: &str) -> Principal {
        Principal {
            arn: Arn::global("iam", account, "user/alice").to_string(),
            user_id: "AIDATEST".to_string(),
            account_id: account.to_string(),
            principal_type: PrincipalType::User,
            source_identity: None,
            tags: None,
        }
    }

    #[test]
    fn no_org_returns_none() {
        let state: SharedOrganizationsState = Arc::new(RwLock::new(None));
        let resolver = OrganizationsScpResolver::new(state);
        assert!(resolver.scps_for(&user_principal("111111111111")).is_none());
    }

    #[test]
    fn management_account_is_exempt() {
        let org = OrganizationState::bootstrap("111111111111");
        let resolver = OrganizationsScpResolver::new(shared(org));
        assert!(resolver.scps_for(&user_principal("111111111111")).is_none());
    }

    #[test]
    fn non_member_returns_none() {
        let org = OrganizationState::bootstrap("111111111111");
        let resolver = OrganizationsScpResolver::new(shared(org));
        assert!(resolver.scps_for(&user_principal("999999999999")).is_none());
    }

    #[test]
    fn slr_is_exempt() {
        let mut org = OrganizationState::bootstrap("111111111111");
        org.enroll_account_if_missing("222222222222");
        let resolver = OrganizationsScpResolver::new(shared(org));
        let slr = Principal {
            arn: "arn:aws:sts::222222222222:assumed-role/AWSServiceRoleForLambda/session1"
                .to_string(),
            user_id: "AROATEST".to_string(),
            account_id: "222222222222".to_string(),
            principal_type: PrincipalType::AssumedRole,
            source_identity: None,
            tags: None,
        };
        assert!(resolver.scps_for(&slr).is_none());
    }

    #[test]
    fn member_account_gets_root_scps() {
        let mut org = OrganizationState::bootstrap("111111111111");
        org.enroll_account_if_missing("222222222222");
        let resolver = OrganizationsScpResolver::new(shared(org));
        let docs = resolver.scps_for(&user_principal("222222222222")).unwrap();
        // Root auto-attaches FullAWSAccess.
        assert_eq!(docs.len(), 1);
        assert!(docs[0].contains("\"Action\":\"*\""));
    }

    #[test]
    fn account_in_nested_ou_gets_full_chain() {
        let mut org = OrganizationState::bootstrap("111111111111");
        let root = org.root_id.clone();
        let parent_ou = org.create_ou(&root, "top").unwrap();
        let child_ou = org.create_ou(&parent_ou.id, "child").unwrap();
        org.enroll_account_if_missing("222222222222");
        org.move_account("222222222222", &root, &child_ou.id)
            .unwrap();

        // Attach custom policies at each level.
        let p_root = org
            .create_policy(
                "P_Root",
                "",
                r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}"#,
                crate::state::POLICY_TYPE_SCP,
            )
            .unwrap();
        let p_parent = org
            .create_policy(
                "P_Parent",
                "",
                r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}"#,
                crate::state::POLICY_TYPE_SCP,
            )
            .unwrap();
        let p_child = org
            .create_policy(
                "P_Child",
                "",
                r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}"#,
                crate::state::POLICY_TYPE_SCP,
            )
            .unwrap();
        let p_account = org
            .create_policy(
                "P_Account",
                "",
                r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}"#,
                crate::state::POLICY_TYPE_SCP,
            )
            .unwrap();
        org.attach_policy(&p_root.id, &root).unwrap();
        org.attach_policy(&p_parent.id, &parent_ou.id).unwrap();
        org.attach_policy(&p_child.id, &child_ou.id).unwrap();
        org.attach_policy(&p_account.id, "222222222222").unwrap();

        let resolver = OrganizationsScpResolver::new(shared(org));
        let docs = resolver.scps_for(&user_principal("222222222222")).unwrap();
        // One merged document per level: root (FullAWSAccess + P_Root),
        // parent OU (P_Parent), child OU (P_Child), account (P_Account).
        assert_eq!(docs.len(), 4);
    }

    #[test]
    fn same_target_scps_are_unioned_into_one_document() {
        // Two SCPs attached to the same target must fold their
        // statements into a single document so the evaluator sees one
        // entry for that level. AWS semantics: same-target = OR,
        // across-levels = AND. (Identified by cubic.)
        let mut org = OrganizationState::bootstrap("111111111111");
        let root = org.root_id.clone();
        org.detach_policy(crate::state::FULL_AWS_ACCESS_POLICY_ID, &root)
            .unwrap();
        let a = org
            .create_policy(
                "A",
                "",
                r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}"#,
                crate::state::POLICY_TYPE_SCP,
            )
            .unwrap();
        let b = org
            .create_policy(
                "B",
                "",
                r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:PutObject","Resource":"*"}]}"#,
                crate::state::POLICY_TYPE_SCP,
            )
            .unwrap();
        org.attach_policy(&a.id, &root).unwrap();
        org.attach_policy(&b.id, &root).unwrap();
        org.enroll_account_if_missing("222222222222");
        let resolver = OrganizationsScpResolver::new(shared(org));
        let docs = resolver.scps_for(&user_principal("222222222222")).unwrap();
        assert_eq!(
            docs.len(),
            1,
            "same-target SCPs must merge into one document"
        );
        let merged: serde_json::Value = serde_json::from_str(&docs[0]).unwrap();
        let stmts = merged["Statement"].as_array().unwrap();
        assert_eq!(stmts.len(), 2);
    }

    #[test]
    fn account_with_no_attachments_returns_empty_chain_if_full_aws_access_detached() {
        let mut org = OrganizationState::bootstrap("111111111111");
        let root = org.root_id.clone();
        org.detach_policy(crate::state::FULL_AWS_ACCESS_POLICY_ID, &root)
            .unwrap();
        org.enroll_account_if_missing("222222222222");
        let resolver = OrganizationsScpResolver::new(shared(org));
        let docs = resolver.scps_for(&user_principal("222222222222")).unwrap();
        // No SCPs attached anywhere on the path — evaluator will treat
        // this as deny-all (matches AWS when the last allow-all is
        // detached and nothing else is attached).
        assert!(docs.is_empty());
    }

    // ── OrganizationsMembershipResolver (AssumeRoot gating, §5.2) ──

    #[test]
    fn membership_resolver_denies_when_no_org() {
        let state: SharedOrganizationsState = Arc::new(RwLock::new(None));
        let resolver = OrganizationsMembershipResolver::new(state);
        assert!(!resolver.can_assume_root_into("111111111111", "222222222222"));
    }

    #[test]
    fn membership_resolver_allows_management_into_member() {
        let mut org = OrganizationState::bootstrap("111111111111");
        org.enroll_account_if_missing("222222222222");
        let resolver = OrganizationsMembershipResolver::new(shared(org));
        // Management account -> enrolled member: allowed.
        assert!(resolver.can_assume_root_into("111111111111", "222222222222"));
    }

    #[test]
    fn membership_resolver_denies_non_member_target() {
        let org = OrganizationState::bootstrap("111111111111");
        let resolver = OrganizationsMembershipResolver::new(shared(org));
        // Target 999... is not enrolled -> denied even for the management acct.
        assert!(!resolver.can_assume_root_into("111111111111", "999999999999"));
    }

    #[test]
    fn membership_resolver_denies_non_management_caller() {
        let mut org = OrganizationState::bootstrap("111111111111");
        org.enroll_account_if_missing("222222222222");
        org.enroll_account_if_missing("333333333333");
        let resolver = OrganizationsMembershipResolver::new(shared(org));
        // A plain member (222...) is neither management nor a delegated admin,
        // so it cannot AssumeRoot into a sibling member (333...).
        assert!(!resolver.can_assume_root_into("222222222222", "333333333333"));
    }

    #[test]
    fn membership_resolver_allows_delegated_admin() {
        let mut org = OrganizationState::bootstrap("111111111111");
        org.enroll_account_if_missing("222222222222");
        org.enroll_account_if_missing("333333333333");
        org.delegated_administrators
            .entry(ROOT_ACCESS_SERVICE_PRINCIPAL.to_string())
            .or_default()
            .insert(
                "222222222222".to_string(),
                crate::state::DelegatedAdministrator {
                    account_id: "222222222222".to_string(),
                    service_principal: ROOT_ACCESS_SERVICE_PRINCIPAL.to_string(),
                    registered_at: chrono::Utc::now(),
                },
            );
        let resolver = OrganizationsMembershipResolver::new(shared(org));
        // 222... is a delegated admin for root access -> may target member 333.
        assert!(resolver.can_assume_root_into("222222222222", "333333333333"));
    }
}
