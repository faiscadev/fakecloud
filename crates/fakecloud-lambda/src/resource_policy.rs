//! Lambda implementation of [`ResourcePolicyProvider`].
//!
//! Lambda persists function resource policies as raw JSON in
//! [`crate::state::LambdaFunction::policy`]. Both `AddPermission` and
//! `RemovePermission` write through that field, seeding a canonical
//! `{"Version":"2012-10-17","Statement":[...]}` document so the
//! existing cross-service evaluator path reads it without a
//! Lambda-specific fork. This file is the read-side bridge into the
//! `fakecloud-core::auth::ResourcePolicyProvider` trait.
//!
//! Mirrors [`fakecloud_sns::resource_policy::SnsResourcePolicyProvider`]
//! and [`fakecloud_s3::resource_policy::S3ResourcePolicyProvider`]:
//! single-service gate, ARN parsing, state lookup, return `None` for
//! anything not owned here so composition is safe.

use std::sync::Arc;

use fakecloud_core::auth::ResourcePolicyProvider;

use crate::state::SharedLambdaState;

/// Concrete [`ResourcePolicyProvider`] backed by the in-memory
/// [`crate::state::LambdaState`]. Server bootstrap clone-shares it via
/// [`fakecloud_core::auth::MultiResourcePolicyProvider`] alongside the
/// S3 and SNS providers.
pub struct LambdaResourcePolicyProvider {
    state: SharedLambdaState,
}

impl LambdaResourcePolicyProvider {
    pub fn new(state: SharedLambdaState) -> Self {
        Self { state }
    }

    /// Convenience constructor returning an
    /// `Arc<dyn ResourcePolicyProvider>` so bootstrap can push it
    /// directly into a `MultiResourcePolicyProvider`.
    pub fn shared(state: SharedLambdaState) -> Arc<dyn ResourcePolicyProvider> {
        Arc::new(Self::new(state))
    }
}

impl ResourcePolicyProvider for LambdaResourcePolicyProvider {
    fn resource_policy(&self, service: &str, resource_arn: &str) -> Option<String> {
        if !service.eq_ignore_ascii_case("lambda") {
            return None;
        }
        let function_name = parse_function_name(resource_arn)?;
        // The function lives in the account and region its ARN names:
        // arn:aws:lambda:REGION:ACCOUNT:function:NAME
        let accounts = self.state.read();
        let state = accounts.by_arn(resource_arn)?;
        state
            .functions
            .get(function_name)
            .and_then(|f| f.policy.clone())
    }
}

/// Extract the function name from a Lambda ARN of the form
/// `arn:aws:lambda:REGION:ACCOUNT:function:NAME`. Qualified ARNs
/// (`function:NAME:VERSION` or `function:NAME:ALIAS`) keep the bare
/// function name — `LambdaState::functions` is keyed by unqualified
/// name and resource policies are attached at the function level.
///
/// Returns `None` for anything that isn't a fully-qualified function
/// ARN so the caller short-circuits to "no policy" rather than
/// looking up stray map keys.
fn parse_function_name(arn: &str) -> Option<&str> {
    let rest = fakecloud_aws::arn::arn_resource(arn, "lambda")?;
    // arn:aws:lambda:REGION:ACCOUNT:function:NAME[:QUALIFIER]
    // After the prefix: REGION:ACCOUNT:function:NAME[:QUALIFIER]
    let parts: Vec<&str> = rest.split(':').collect();
    // Expect at least 4 segments: region, account, "function", name.
    if parts.len() < 4 {
        return None;
    }
    let region = parts[0];
    let account = parts[1];
    let resource_type = parts[2];
    let name = parts[3];
    if region.is_empty() || account.is_empty() {
        return None;
    }
    if resource_type != "function" {
        return None;
    }
    if name.is_empty() {
        return None;
    }
    Some(name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{LambdaFunction, LambdaState};
    use chrono::Utc;
    use fakecloud_aws::arn::Arn;
    use parking_lot::RwLock;
    use std::collections::BTreeMap;

    fn func_with_policy(name: &str, policy: Option<&str>) -> LambdaFunction {
        LambdaFunction {
            function_name: name.to_string(),
            function_arn: Arn::new(
                "lambda",
                "us-east-1",
                "123456789012",
                &format!("function:{name}"),
            )
            .to_string(),
            runtime: "python3.12".to_string(),
            role: "arn:aws:iam::123456789012:role/r".to_string(),
            handler: "index.handler".to_string(),
            description: String::new(),
            timeout: 3,
            memory_size: 128,
            code_sha256: String::new(),
            code_size: 0,
            version: "$LATEST".to_string(),
            last_modified: Utc::now(),
            tags: BTreeMap::new(),
            environment: BTreeMap::new(),
            architectures: Vec::new(),
            package_type: "Zip".to_string(),
            code_zip: None,
            image_uri: None,
            policy: policy.map(str::to_string),
            layers: Vec::new(),
            revision_id: "test-rev".to_string(),
            tracing_mode: None,
            kms_key_arn: None,
            ephemeral_storage_size: None,
            vpc_config: None,
            snap_start: None,
            dead_letter_config_arn: None,
            file_system_configs: Vec::new(),
            logging_config: None,
            image_config: None,
            durable_config: None,
            signing_profile_version_arn: None,
            signing_job_arn: None,
            runtime_version_config: None,
            master_arn: None,
            state_reason: None,
            state_reason_code: None,
            last_update_status_reason: None,
            last_update_status_reason_code: None,
            capacity_provider_config: None,
        }
    }

    fn state_with(func: LambdaFunction) -> SharedLambdaState {
        let mut mas: fakecloud_core::multi_account::MultiRegionState<LambdaState> =
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", "");
        mas.regional_mut("123456789012", "us-east-1")
            .functions
            .insert(func.function_name.clone(), func);
        Arc::new(RwLock::new(mas))
    }

    #[test]
    fn parse_function_name_accepts_valid_arn() {
        assert_eq!(
            parse_function_name("arn:aws:lambda:us-east-1:123456789012:function:my-fn"),
            Some("my-fn")
        );
    }

    #[test]
    fn parse_function_name_accepts_china_partition_arn() {
        assert_eq!(
            parse_function_name("arn:aws-cn:lambda:cn-north-1:123456789012:function:my-fn:PROD"),
            Some("my-fn")
        );
    }

    #[test]
    fn parse_function_name_accepts_qualified_arn() {
        // Qualified ARN (version / alias) — function name is still
        // segment 4, qualifier follows as segment 5 and we drop it.
        assert_eq!(
            parse_function_name("arn:aws:lambda:us-east-1:123456789012:function:my-fn:PROD"),
            Some("my-fn")
        );
        assert_eq!(
            parse_function_name("arn:aws:lambda:us-east-1:123456789012:function:my-fn:7"),
            Some("my-fn")
        );
    }

    #[test]
    fn parse_function_name_rejects_malformed() {
        assert_eq!(parse_function_name(""), None);
        assert_eq!(parse_function_name("not-an-arn"), None);
        assert_eq!(parse_function_name("arn:aws:lambda:"), None);
        assert_eq!(parse_function_name("arn:aws:lambda:us-east-1"), None);
        assert_eq!(
            parse_function_name("arn:aws:lambda:us-east-1:123456789012"),
            None
        );
        // Event source mapping ARN — wrong resource type.
        assert_eq!(
            parse_function_name("arn:aws:lambda:us-east-1:123456789012:event-source-mapping:uuid"),
            None
        );
        // Blank region or account.
        assert_eq!(
            parse_function_name("arn:aws:lambda::123456789012:function:f"),
            None
        );
        assert_eq!(
            parse_function_name("arn:aws:lambda:us-east-1::function:f"),
            None
        );
        // Blank function name.
        assert_eq!(
            parse_function_name("arn:aws:lambda:us-east-1:123456789012:function:"),
            None
        );
        // S3-shaped ARN.
        assert_eq!(parse_function_name("arn:aws:s3:::my-bucket"), None);
    }

    #[test]
    fn returns_stored_policy_for_lambda_arn() {
        let doc = r#"{"Version":"2012-10-17","Statement":[]}"#;
        let state = state_with(func_with_policy("my-fn", Some(doc)));
        let provider = LambdaResourcePolicyProvider::new(state);
        assert_eq!(
            provider.resource_policy(
                "lambda",
                "arn:aws:lambda:us-east-1:123456789012:function:my-fn"
            ),
            Some(doc.to_string())
        );
    }

    #[test]
    fn qualified_arn_resolves_to_unqualified_function_policy() {
        // Resource policies live on the function, not on specific
        // version aliases. A qualified ARN must still resolve to the
        // same stored document.
        let doc = r#"{"Statement":[]}"#;
        let state = state_with(func_with_policy("my-fn", Some(doc)));
        let provider = LambdaResourcePolicyProvider::new(state);
        assert_eq!(
            provider.resource_policy(
                "lambda",
                "arn:aws:lambda:us-east-1:123456789012:function:my-fn:PROD"
            ),
            Some(doc.to_string())
        );
    }

    #[test]
    fn returns_none_when_function_has_no_policy() {
        let state = state_with(func_with_policy("my-fn", None));
        let provider = LambdaResourcePolicyProvider::new(state);
        assert_eq!(
            provider.resource_policy(
                "lambda",
                "arn:aws:lambda:us-east-1:123456789012:function:my-fn"
            ),
            None
        );
    }

    #[test]
    fn returns_none_when_function_missing() {
        let state = state_with(func_with_policy("other", Some("{}")));
        let provider = LambdaResourcePolicyProvider::new(state);
        assert_eq!(
            provider.resource_policy(
                "lambda",
                "arn:aws:lambda:us-east-1:123456789012:function:my-fn"
            ),
            None
        );
    }

    #[test]
    fn returns_none_for_non_lambda_service_prefix() {
        let state = state_with(func_with_policy("my-fn", Some("{}")));
        let provider = LambdaResourcePolicyProvider::new(state);
        assert_eq!(
            provider.resource_policy("s3", "arn:aws:lambda:us-east-1:123456789012:function:my-fn"),
            None
        );
        assert_eq!(
            provider.resource_policy(
                "sns",
                "arn:aws:lambda:us-east-1:123456789012:function:my-fn"
            ),
            None
        );
    }

    #[test]
    fn service_prefix_match_is_case_insensitive() {
        let state = state_with(func_with_policy("my-fn", Some("{}")));
        let provider = LambdaResourcePolicyProvider::new(state);
        assert!(provider
            .resource_policy(
                "LAMBDA",
                "arn:aws:lambda:us-east-1:123456789012:function:my-fn"
            )
            .is_some());
    }

    #[test]
    fn shared_constructor_wraps_in_arc() {
        let state = state_with(func_with_policy("my-fn", Some("doc")));
        let arc = LambdaResourcePolicyProvider::shared(state);
        assert_eq!(
            arc.resource_policy(
                "lambda",
                "arn:aws:lambda:us-east-1:123456789012:function:my-fn"
            )
            .as_deref(),
            Some("doc")
        );
    }
}
