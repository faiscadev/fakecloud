//! STS implementation of [`ResourcePolicyProvider`].
//!
//! For `sts:AssumeRole*`, the "resource policy" is the role's trust
//! policy (`assume_role_policy_document`). This provider looks up the
//! role by ARN in the target account's IAM state and returns the trust
//! policy document so the cross-account evaluator can apply the correct
//! `identity AND resource` semantics.

use std::sync::Arc;

use fakecloud_core::auth::ResourcePolicyProvider;

use crate::state::SharedIamState;

pub struct StsResourcePolicyProvider {
    state: SharedIamState,
}

impl StsResourcePolicyProvider {
    pub fn new(state: SharedIamState) -> Self {
        Self { state }
    }

    pub fn shared(state: SharedIamState) -> Arc<dyn ResourcePolicyProvider> {
        Arc::new(Self::new(state))
    }
}

impl ResourcePolicyProvider for StsResourcePolicyProvider {
    fn resource_policy(&self, service: &str, resource_arn: &str) -> Option<String> {
        if !service.eq_ignore_ascii_case("sts") {
            return None;
        }
        // AssumeRole resource ARN: arn:aws:iam::<account>:role/<name>
        // Extract account and role name from the ARN.
        let parts: Vec<&str> = resource_arn.split(':').collect();
        if parts.len() < 6 {
            return None;
        }
        let account_id = parts[4];
        let accounts = self.state.read();
        let state = accounts.get(account_id)?;
        // Matched on the full ARN, path included: an ARN with the wrong path
        // names no role.
        let role = state.role_by_arn(resource_arn)?;
        Some(role.assume_role_policy_document.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{IamRole, IamState};
    use fakecloud_aws::arn::Arn;
    use fakecloud_core::multi_account::MultiAccountState;
    use parking_lot::RwLock;

    fn make_state_with_role(
        account_id: &str,
        role_name: &str,
        trust_policy: &str,
    ) -> SharedIamState {
        let mut mas = MultiAccountState::<IamState>::new(account_id, "us-east-1", "");
        let state = mas.get_or_create(account_id);
        state.roles.insert(
            role_name.to_string(),
            IamRole {
                role_name: role_name.to_string(),
                role_id: "AROATEST".to_string(),
                arn: Arn::global("iam", account_id, &format!("role/{role_name}")).to_string(),
                path: "/".to_string(),
                assume_role_policy_document: trust_policy.to_string(),
                created_at: chrono::Utc::now(),
                description: None,
                max_session_duration: 3600,
                tags: Vec::new(),
                permissions_boundary: None,
            },
        );
        Arc::new(RwLock::new(mas))
    }

    #[test]
    fn returns_trust_policy_for_sts_role_arn() {
        let trust = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"sts:AssumeRole"}]}"#;
        let state = make_state_with_role("222222222222", "my-role", trust);
        let provider = StsResourcePolicyProvider::new(state);
        assert_eq!(
            provider.resource_policy("sts", "arn:aws:iam::222222222222:role/my-role"),
            Some(trust.to_string())
        );
    }

    #[test]
    fn returns_none_for_non_sts_service() {
        let state = make_state_with_role("222222222222", "r", "{}");
        let provider = StsResourcePolicyProvider::new(state);
        assert_eq!(
            provider.resource_policy("s3", "arn:aws:iam::222222222222:role/r"),
            None
        );
    }

    #[test]
    fn returns_none_for_missing_role() {
        let state = make_state_with_role("222222222222", "existing", "{}");
        let provider = StsResourcePolicyProvider::new(state);
        assert_eq!(
            provider.resource_policy("sts", "arn:aws:iam::222222222222:role/missing"),
            None
        );
    }

    /// A role ARN with the wrong path names no role.
    #[test]
    fn returns_none_for_a_role_arn_with_another_path() {
        let state = make_state_with_role("222222222222", "my-role", "{}");
        let provider = StsResourcePolicyProvider::new(state);
        assert_eq!(
            provider.resource_policy("sts", "arn:aws:iam::222222222222:role/ops/my-role"),
            None
        );
    }

    #[test]
    fn returns_none_for_missing_account() {
        let state = make_state_with_role("111111111111", "r", "{}");
        let provider = StsResourcePolicyProvider::new(state);
        assert_eq!(
            provider.resource_policy("sts", "arn:aws:iam::999999999999:role/r"),
            None
        );
    }
}
