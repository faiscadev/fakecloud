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

use crate::state::{LambdaState, SharedLambdaState};

/// Where the resource-based policy of one function qualifier lives, as AWS
/// keeps them: the unqualified function (and `$LATEST`) has one, every
/// published version has its own, and so does every alias -- an alias's
/// policy is not the policy of the version it points to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyQualifier<'q> {
    /// No qualifier, or `$LATEST`.
    Function,
    /// A published version number, or the `$LATEST.PUBLISHED` version of
    /// a Lambda Managed Instances function.
    Version(&'q str),
    /// An alias name.
    Alias(&'q str),
}

impl<'q> PolicyQualifier<'q> {
    pub fn of(qualifier: Option<&'q str>) -> Self {
        match qualifier {
            None | Some("") | Some("$LATEST") => Self::Function,
            Some(q)
                if q == crate::state::LATEST_PUBLISHED || q.bytes().all(|b| b.is_ascii_digit()) =>
            {
                Self::Version(q)
            }
            Some(q) => Self::Alias(q),
        }
    }

    /// The qualifier as it appears in an ARN suffix, if any.
    pub fn suffix(self) -> Option<&'q str> {
        match self {
            Self::Function => None,
            Self::Version(q) | Self::Alias(q) => Some(q),
        }
    }
}

/// A qualifier's policy slot and the revision that guards it.
pub struct PolicySlot<'a> {
    pub policy: &'a mut Option<String>,
    pub revision_id: &'a mut String,
    /// The ARN the slot's statements are scoped to (qualified for a
    /// version or alias).
    pub arn: String,
}

/// Read-only [`PolicySlot`].
pub struct PolicyView<'a> {
    pub policy: Option<&'a str>,
    pub revision_id: &'a str,
}

/// The policy slot of `function_name` at `qualifier`; `None` when the
/// function, version or alias does not exist.
pub fn policy_slot_mut<'a>(
    state: &'a mut LambdaState,
    function_name: &str,
    qualifier: PolicyQualifier<'_>,
) -> Option<PolicySlot<'a>> {
    let live_arn = state.functions.get(function_name)?.function_arn.clone();
    match qualifier {
        PolicyQualifier::Function => {
            let f = state.functions.get_mut(function_name)?;
            Some(PolicySlot {
                policy: &mut f.policy,
                revision_id: &mut f.revision_id,
                arn: live_arn,
            })
        }
        PolicyQualifier::Version(v) => {
            let f = state.version_snapshot_mut(function_name, v)?;
            Some(PolicySlot {
                policy: &mut f.policy,
                revision_id: &mut f.revision_id,
                arn: format!("{live_arn}:{v}"),
            })
        }
        PolicyQualifier::Alias(a) => {
            let alias = state.aliases.get_mut(&format!("{function_name}:{a}"))?;
            Some(PolicySlot {
                policy: &mut alias.policy,
                revision_id: &mut alias.revision_id,
                arn: alias.alias_arn.clone(),
            })
        }
    }
}

/// [`policy_slot_mut`], read-only.
pub fn policy_view<'a>(
    state: &'a LambdaState,
    function_name: &str,
    qualifier: PolicyQualifier<'_>,
) -> Option<PolicyView<'a>> {
    let live = state.functions.get(function_name)?;
    let (policy, revision_id) = match qualifier {
        PolicyQualifier::Function => (&live.policy, &live.revision_id),
        PolicyQualifier::Version(v) => {
            let f = state.version_snapshot(function_name, v)?;
            (&f.policy, &f.revision_id)
        }
        PolicyQualifier::Alias(a) => {
            let alias = state.aliases.get(&format!("{function_name}:{a}"))?;
            (&alias.policy, &alias.revision_id)
        }
    };
    Some(PolicyView {
        policy: policy.as_deref(),
        revision_id,
    })
}

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
        let (function_name, qualifier) = parse_function_ref(resource_arn)?;
        // The function lives in the account and region its ARN names:
        // arn:aws:lambda:REGION:ACCOUNT:function:NAME[:QUALIFIER]. A
        // qualified ARN (an invocation of a version or alias) is governed by
        // that qualifier's own policy, as on AWS.
        let accounts = self.state.read();
        let state = accounts.by_arn(resource_arn)?;
        policy_view(state, function_name, PolicyQualifier::of(qualifier))?
            .policy
            .map(str::to_string)
    }
}

/// [`parse_function_name`] plus the ARN's qualifier, if any.
fn parse_function_ref(arn: &str) -> Option<(&str, Option<&str>)> {
    let name = parse_function_name(arn)?;
    let rest = fakecloud_aws::arn::arn_resource(arn, "lambda")?;
    let qualifier = rest.split(':').nth(4).filter(|q| !q.is_empty());
    Some((name, qualifier))
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
    fn a_qualified_arn_is_governed_by_its_qualifiers_own_policy() {
        // AWS keeps one resource policy per qualifier: the function's does
        // not grant an invocation of an alias or a version, and an alias's
        // is not the policy of the version it points to.
        let function_doc = r#"{"Statement":["function"]}"#;
        let alias_doc = r#"{"Statement":["alias"]}"#;
        let state = state_with(func_with_policy("my-fn", Some(function_doc)));
        {
            let mut accounts = state.write();
            let s = accounts.regional_mut("123456789012", "us-east-1");
            let mut v1 = s.functions["my-fn"].clone();
            v1.policy = None;
            s.function_version_snapshots
                .entry("my-fn".to_string())
                .or_default()
                .insert("1".to_string(), v1);
            s.aliases.insert(
                "my-fn:PROD".to_string(),
                crate::state::FunctionAlias {
                    alias_arn: "arn:aws:lambda:us-east-1:123456789012:function:my-fn:PROD"
                        .to_string(),
                    name: "PROD".to_string(),
                    function_version: "1".to_string(),
                    description: String::new(),
                    revision_id: "r".to_string(),
                    routing_config: None,
                    policy: Some(alias_doc.to_string()),
                },
            );
        }
        let provider = LambdaResourcePolicyProvider::new(state);
        let arn =
            |suffix: &str| format!("arn:aws:lambda:us-east-1:123456789012:function:my-fn{suffix}");
        assert_eq!(
            provider.resource_policy("lambda", &arn("")),
            Some(function_doc.to_string())
        );
        assert_eq!(
            provider.resource_policy("lambda", &arn(":$LATEST")),
            Some(function_doc.to_string())
        );
        assert_eq!(
            provider.resource_policy("lambda", &arn(":PROD")),
            Some(alias_doc.to_string())
        );
        assert_eq!(provider.resource_policy("lambda", &arn(":1")), None);
        assert_eq!(provider.resource_policy("lambda", &arn(":BETA")), None);
    }

    #[test]
    fn latest_published_is_a_version_with_its_own_policy() {
        // A Lambda Managed Instances function's `$LATEST.PUBLISHED` is a
        // version, not an alias: its policy lives on that version.
        assert_eq!(
            PolicyQualifier::of(Some(crate::state::LATEST_PUBLISHED)),
            PolicyQualifier::Version(crate::state::LATEST_PUBLISHED)
        );
        let published_doc = r#"{"Statement":["published"]}"#;
        let state = state_with(func_with_policy("my-fn", None));
        {
            let mut accounts = state.write();
            let s = accounts.regional_mut("123456789012", "us-east-1");
            let mut published = s.functions["my-fn"].clone();
            published.policy = Some(published_doc.to_string());
            s.latest_published.insert("my-fn".to_string(), published);
        }
        let provider = LambdaResourcePolicyProvider::new(state.clone());
        assert_eq!(
            provider.resource_policy(
                "lambda",
                "arn:aws:lambda:us-east-1:123456789012:function:my-fn:$LATEST.PUBLISHED"
            ),
            Some(published_doc.to_string())
        );
        let mut accounts = state.write();
        let s = accounts.regional_mut("123456789012", "us-east-1");
        let slot = policy_slot_mut(
            s,
            "my-fn",
            PolicyQualifier::of(Some(crate::state::LATEST_PUBLISHED)),
        )
        .expect("the $LATEST.PUBLISHED slot");
        assert_eq!(
            slot.arn,
            "arn:aws:lambda:us-east-1:123456789012:function:my-fn:$LATEST.PUBLISHED"
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
