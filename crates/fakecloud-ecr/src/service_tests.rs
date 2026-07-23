use super::validate_repository_name;

#[track_caller]
fn ok(n: &str) {
    validate_repository_name(n).unwrap_or_else(|_| panic!("expected '{n}' to validate"));
}
#[track_caller]
fn bad(n: &str) {
    assert!(
        validate_repository_name(n).is_err(),
        "expected '{n}' to be rejected",
    );
}

#[test]
fn accepts_valid_names() {
    ok("foo");
    ok("foo-bar");
    ok("foo.bar");
    ok("foo_bar");
    ok("foo/bar");
    ok("team/svc");
    ok("a/b/c");
    ok("foo123/bar-baz.qux_q");
}

#[test]
fn rejects_invalid_names() {
    bad("");
    bad("a");
    bad("/foo");
    bad("foo/");
    bad("foo//bar");
    bad("-foo");
    bad("foo-");
    bad("foo--bar");
    bad("foo..bar");
    bad("foo__bar");
    bad("Foo");
    bad("foo bar");
    bad("foo!");
}

// ── Lifecycle policy evaluator ─────────────────────────────────
use super::{evaluate_lifecycle_policy, wildcard_match};
use crate::state::{Image, Repository};
use chrono::Utc;

fn repo_with_images(entries: &[(&str, &[&str], i64)]) -> Repository {
    // entries: (digest, tags, minutes_ago_pushed)
    let mut r = Repository::new("test-repo", "arn".into(), "123", "http://localhost");
    for (digest, tags, minutes_ago) in entries {
        let pushed = Utc::now() - chrono::Duration::minutes(*minutes_ago);
        r.images.insert(
            (*digest).to_string(),
            Image {
                image_digest: (*digest).to_string(),
                image_manifest: String::new(),
                image_manifest_media_type: String::new(),
                artifact_media_type: None,
                image_size_in_bytes: 0,
                image_pushed_at: pushed,
                last_recorded_pull_time: None,
                image_status: "ACTIVE".to_string(),
                last_archived_at: None,
                last_activated_at: None,
                last_in_use_at: None,
                in_use_count: 0,
            },
        );
        for t in *tags {
            r.image_tags.insert((*t).to_string(), (*digest).to_string());
        }
    }
    r
}

#[test]
fn lifecycle_count_more_than_tagged() {
    // Five tagged images; rule says keep newest 2, prune 3.
    let r = repo_with_images(&[
        ("sha256:a", &["v1"], 50),
        ("sha256:b", &["v2"], 40),
        ("sha256:c", &["v3"], 30),
        ("sha256:d", &["v4"], 20),
        ("sha256:e", &["v5"], 10),
    ]);
    let policy = r#"{"rules":[{
        "rulePriority": 1,
        "selection": {"tagStatus":"tagged","countType":"imageCountMoreThan","countNumber":2}
    }]}"#;
    let prune = evaluate_lifecycle_policy(&r, policy);
    assert_eq!(prune.len(), 3);
    assert!(prune.contains(&"sha256:a".to_string()));
    assert!(prune.contains(&"sha256:b".to_string()));
    assert!(prune.contains(&"sha256:c".to_string()));
}

#[test]
fn lifecycle_untagged_only() {
    let r = repo_with_images(&[("sha256:tagged", &["v1"], 60), ("sha256:untag", &[], 30)]);
    let policy = r#"{"rules":[{
        "rulePriority": 1,
        "selection": {"tagStatus":"untagged","countType":"imageCountMoreThan","countNumber":0}
    }]}"#;
    let prune = evaluate_lifecycle_policy(&r, policy);
    assert_eq!(prune, vec!["sha256:untag".to_string()]);
}

#[test]
fn lifecycle_tag_prefix_list() {
    let r = repo_with_images(&[
        ("sha256:a", &["dev-1"], 60),
        ("sha256:b", &["dev-2"], 50),
        ("sha256:c", &["prod-1"], 40),
        ("sha256:d", &["prod-2"], 30),
    ]);
    // Keep newest 1 among dev-*, prune the rest; leave prod-* alone.
    let policy = r#"{"rules":[{
        "rulePriority": 1,
        "selection": {
            "tagStatus":"tagged",
            "tagPrefixList":["dev-"],
            "countType":"imageCountMoreThan",
            "countNumber":1
        }
    }]}"#;
    let prune = evaluate_lifecycle_policy(&r, policy);
    assert_eq!(prune, vec!["sha256:a".to_string()]);
}

#[test]
fn lifecycle_tag_pattern_list_wildcards() {
    let r = repo_with_images(&[
        ("sha256:a", &["release-2024-01"], 60),
        ("sha256:b", &["release-2024-02"], 50),
        ("sha256:c", &["hotfix-2024-02"], 40),
    ]);
    // Match only `release-*`; prune all of them (countNumber=0).
    let policy = r#"{"rules":[{
        "rulePriority": 1,
        "selection": {
            "tagStatus":"tagged",
            "tagPatternList":["release-*"],
            "countType":"imageCountMoreThan",
            "countNumber":0
        }
    }]}"#;
    let prune = evaluate_lifecycle_policy(&r, policy);
    assert_eq!(prune.len(), 2);
    assert!(prune.contains(&"sha256:a".to_string()));
    assert!(prune.contains(&"sha256:b".to_string()));
    assert!(!prune.contains(&"sha256:c".to_string()));
}

#[test]
fn lifecycle_since_image_pushed_days() {
    let r = repo_with_images(&[
        ("sha256:old", &["v1"], 60 * 24 * 10), // 10 days ago
        ("sha256:new", &["v2"], 60 * 24),      // 1 day ago
    ]);
    let policy = r#"{"rules":[{
        "rulePriority": 1,
        "selection": {
            "tagStatus":"any",
            "countType":"sinceImagePushed",
            "countUnit":"days",
            "countNumber":5
        }
    }]}"#;
    let prune = evaluate_lifecycle_policy(&r, policy);
    assert_eq!(prune, vec!["sha256:old".to_string()]);
}

#[test]
fn lifecycle_rule_priority_order() {
    // Priority 1 keeps newest 2 tagged; priority 2 then prunes all
    // remaining tagged > 1 day old. Priority 1 runs first, then 2
    // sees fewer candidates.
    let r = repo_with_images(&[
        ("sha256:a", &["v1"], 60 * 24 * 10),
        ("sha256:b", &["v2"], 60 * 24 * 5),
        ("sha256:c", &["v3"], 60 * 24 * 2),
        ("sha256:d", &["v4"], 60 * 24),
    ]);
    let policy = r#"{"rules":[
        {"rulePriority": 2,
         "selection": {"tagStatus":"any","countType":"sinceImagePushed","countUnit":"days","countNumber":3}},
        {"rulePriority": 1,
         "selection": {"tagStatus":"tagged","countType":"imageCountMoreThan","countNumber":2}}
    ]}"#;
    let prune: std::collections::BTreeSet<String> =
        evaluate_lifecycle_policy(&r, policy).into_iter().collect();
    // Priority 1 (runs first): prunes a + b (keeping newest 2 = c, d).
    // Priority 2: c and d are both < 3 days -> survives.
    assert!(prune.contains("sha256:a"));
    assert!(prune.contains("sha256:b"));
}

#[test]
fn wildcard_match_basics() {
    assert!(wildcard_match("release-*", "release-2024"));
    assert!(wildcard_match("*-stable", "v1-stable"));
    assert!(wildcard_match("a*b*c", "a-something-b-more-c"));
    assert!(wildcard_match("*", "anything"));
    assert!(wildcard_match("exact", "exact"));

    assert!(!wildcard_match("release-*", "rev-2024"));
    assert!(!wildcard_match("*-stable", "v1-beta"));
    assert!(!wildcard_match("exact", "exactly"));
    assert!(!wildcard_match("a*b*c", "a-b"));
}

// ── Registry-level scan-on-push fallback ───────────────────────
use super::{registry_filter_matches, registry_scan_on_push_matches};
use crate::state::{
    RegistryScanningConfiguration, RegistryScanningRule, RepositoryFilter as RegRepositoryFilter,
};

fn rule(freq: &str, filters: Vec<(&str, &str)>) -> RegistryScanningRule {
    RegistryScanningRule {
        scan_frequency: freq.to_string(),
        repository_filters: filters
            .into_iter()
            .map(|(f, t)| RegRepositoryFilter {
                filter: f.to_string(),
                filter_type: t.to_string(),
            })
            .collect(),
    }
}

#[test]
fn registry_scan_matches_when_filter_wildcards_repo() {
    let cfg = RegistryScanningConfiguration {
        scan_type: "BASIC".to_string(),
        rules: vec![rule("SCAN_ON_PUSH", vec![("prod-*", "WILDCARD")])],
    };
    assert!(registry_scan_on_push_matches(&cfg, "prod-api"));
    assert!(!registry_scan_on_push_matches(&cfg, "dev-api"));
}

#[test]
fn registry_scan_matches_when_filter_list_empty() {
    let cfg = RegistryScanningConfiguration {
        scan_type: "BASIC".to_string(),
        rules: vec![rule("SCAN_ON_PUSH", vec![])],
    };
    assert!(registry_scan_on_push_matches(&cfg, "anything"));
}

#[test]
fn registry_scan_skips_continuous_scan_frequency() {
    let cfg = RegistryScanningConfiguration {
        scan_type: "ENHANCED".to_string(),
        rules: vec![rule("CONTINUOUS_SCAN", vec![("*", "WILDCARD")])],
    };
    assert!(!registry_scan_on_push_matches(&cfg, "x"));
}

#[test]
fn registry_filter_rejects_unknown_filter_type() {
    let f = RegRepositoryFilter {
        filter: "x".to_string(),
        filter_type: "REGEX".to_string(),
    };
    assert!(!registry_filter_matches(&f, "x"));
}

#[test]
fn registry_scan_no_rules_no_match() {
    let cfg = RegistryScanningConfiguration::default();
    assert!(!registry_scan_on_push_matches(&cfg, "x"));
}

#[test]
fn repository_filters_match_returns_true_on_empty_list() {
    use super::repository_filters_match;
    assert!(repository_filters_match(&[], "any-repo"));
}

#[test]
fn repository_filters_match_honours_wildcard() {
    use super::repository_filters_match;
    use crate::state::RepositoryFilter;
    let filters = vec![RepositoryFilter {
        filter: "team-a/*".to_string(),
        filter_type: "WILDCARD".to_string(),
    }];
    assert!(repository_filters_match(&filters, "team-a/svc"));
    assert!(!repository_filters_match(&filters, "team-b/svc"));
}

#[test]
fn replicate_image_copies_to_destination_account() {
    use super::EcrService;
    use crate::state::{
        EcrState, Image, ReplicationConfiguration, ReplicationDestination, ReplicationRule,
        Repository,
    };
    use fakecloud_core::multi_account::MultiAccountState;
    use parking_lot::RwLock;
    use std::sync::Arc;

    const SOURCE: &str = "111111111111";
    const TARGET: &str = "222222222222";

    let mut mas: MultiAccountState<EcrState> =
        MultiAccountState::new(SOURCE, "us-east-1", "http://fakecloud:4566");
    let source_state = mas.get_or_create(SOURCE);
    source_state.replication_configuration = Some(ReplicationConfiguration {
        rules: vec![ReplicationRule {
            destinations: vec![ReplicationDestination {
                region: "us-west-2".to_string(),
                registry_id: TARGET.to_string(),
            }],
            repository_filters: Vec::new(),
        }],
    });
    let arn = source_state.repository_arn("us-east-1", "app");
    let mut repo = Repository::new("app", arn, SOURCE, "fakecloud:4566");
    repo.images.insert(
        "sha256:abc".to_string(),
        Image {
            image_digest: "sha256:abc".to_string(),
            image_manifest: "{\"mediaType\":\"x\"}".to_string(),
            image_manifest_media_type: "application/vnd.oci.image.manifest.v1+json".to_string(),
            artifact_media_type: None,
            image_size_in_bytes: 0,
            image_pushed_at: chrono::Utc::now(),
            last_recorded_pull_time: None,
            image_status: "ACTIVE".to_string(),
            last_archived_at: None,
            last_activated_at: None,
            last_in_use_at: None,
            in_use_count: 0,
        },
    );
    source_state.repositories.insert("app".to_string(), repo);

    let state: crate::state::SharedEcrState = Arc::new(RwLock::new(mas));
    let svc = EcrService::new(state.clone());
    svc.replicate_image(SOURCE, "app", "sha256:abc");

    let accounts = state.read();
    let target_state = accounts.get(TARGET).expect("target account created");
    let target_repo = target_state
        .repositories
        .get("app")
        .expect("target repo provisioned");
    assert!(target_repo.images.contains_key("sha256:abc"));

    let source_state = accounts.get(SOURCE).expect("source account intact");
    let source_repo = source_state.repositories.get("app").unwrap();
    let statuses = source_repo
        .replication_statuses
        .get("sha256:abc")
        .expect("status entry recorded");
    assert_eq!(statuses.len(), 1);
    assert_eq!(statuses[0].status, "COMPLETE");
    assert_eq!(statuses[0].registry_id, TARGET);
}

#[test]
fn repository_policy_allows_explicit_principal() {
    use super::repository_policy_allows;
    let policy = r#"{
        "Version": "2012-10-17",
        "Statement": [{
            "Sid": "CrossAccountPull",
            "Effect": "Allow",
            "Principal": {"AWS": "arn:aws:iam::222222222222:root"},
            "Action": ["ecr:BatchGetImage", "ecr:GetDownloadUrlForLayer"],
            "Resource": "*"
        }]
    }"#;
    assert!(repository_policy_allows(
        Some(policy),
        "222222222222",
        "arn:aws:ecr:us-east-1:111111111111:repository/app",
        "ecr:BatchGetImage",
    ));
    // Wrong action -> deny.
    assert!(!repository_policy_allows(
        Some(policy),
        "222222222222",
        "arn:aws:ecr:us-east-1:111111111111:repository/app",
        "ecr:PutImage",
    ));
    // Wrong principal account -> deny.
    assert!(!repository_policy_allows(
        Some(policy),
        "333333333333",
        "arn:aws:ecr:us-east-1:111111111111:repository/app",
        "ecr:BatchGetImage",
    ));
}

#[test]
fn repository_policy_allows_empty_policy_denies() {
    use super::repository_policy_allows;
    assert!(!repository_policy_allows(
        None,
        "222",
        "arn",
        "ecr:PutImage"
    ));
    assert!(!repository_policy_allows(
        Some(""),
        "222",
        "arn",
        "ecr:PutImage"
    ));
}

#[test]
fn check_repo_policy_same_account_passes_without_policy() {
    use super::check_repo_policy;
    // Same-account caller: policy is irrelevant (real ECR falls back to
    // the caller's IAM identity policies for the same-account path).
    let res = check_repo_policy(
        "111111111111",
        "111111111111",
        "arn:aws:ecr:us-east-1:111111111111:repository/app",
        "app",
        None,
        "ecr:BatchGetImage",
    );
    assert!(res.is_ok());
}

#[test]
fn check_repo_policy_cross_account_no_policy_returns_not_found() {
    use super::check_repo_policy;
    let err = check_repo_policy(
        "111111111111",
        "222222222222",
        "arn:aws:ecr:us-east-1:111111111111:repository/app",
        "app",
        None,
        "ecr:BatchGetImage",
    )
    .expect_err("expected error");
    assert_eq!(err.code(), "RepositoryPolicyNotFoundException");
}

#[test]
fn check_repo_policy_cross_account_explicit_allow_passes() {
    use super::check_repo_policy;
    let policy = r#"{
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": "arn:aws:iam::222222222222:root"},
            "Action": "ecr:BatchGetImage",
            "Resource": "*"
        }]
    }"#;
    let res = check_repo_policy(
        "111111111111",
        "222222222222",
        "arn:aws:ecr:us-east-1:111111111111:repository/app",
        "app",
        Some(policy),
        "ecr:BatchGetImage",
    );
    assert!(res.is_ok());
}

#[test]
fn check_repo_policy_cross_account_implicit_deny_returns_access_denied() {
    use super::check_repo_policy;
    // Policy exists but doesn't grant the requested action: ECR returns
    // AccessDeniedException, not RepositoryPolicyNotFoundException.
    let policy = r#"{
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": "arn:aws:iam::333333333333:root"},
            "Action": "ecr:BatchGetImage",
            "Resource": "*"
        }]
    }"#;
    let err = check_repo_policy(
        "111111111111",
        "222222222222",
        "arn:aws:ecr:us-east-1:111111111111:repository/app",
        "app",
        Some(policy),
        "ecr:BatchGetImage",
    )
    .expect_err("expected error");
    assert_eq!(err.code(), "AccessDeniedException");
}

#[test]
fn check_repo_policy_cross_account_explicit_deny_returns_access_denied() {
    use super::check_repo_policy;
    // Explicit Deny in the resource policy beats any matching Allow.
    let policy = r#"{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"AWS": "arn:aws:iam::222222222222:root"},
                "Action": "ecr:BatchGetImage",
                "Resource": "*"
            },
            {
                "Effect": "Deny",
                "Principal": {"AWS": "arn:aws:iam::222222222222:root"},
                "Action": "ecr:BatchGetImage",
                "Resource": "*"
            }
        ]
    }"#;
    let err = check_repo_policy(
        "111111111111",
        "222222222222",
        "arn:aws:ecr:us-east-1:111111111111:repository/app",
        "app",
        Some(policy),
        "ecr:BatchGetImage",
    )
    .expect_err("expected error");
    assert_eq!(err.code(), "AccessDeniedException");
}

// ── Replication: PutImage trigger + DescribeImageReplicationStatus ──

#[cfg(test)]
mod replication_tests {
    use super::super::EcrService;
    use crate::state::{
        EcrState, Image, ReplicationConfiguration, ReplicationDestination, ReplicationRule,
        Repository, RepositoryFilter, SharedEcrState,
    };
    use bytes::Bytes;
    use fakecloud_core::multi_account::MultiAccountState;
    use fakecloud_core::service::AwsRequest;
    use http::{HeaderMap, Method};
    use parking_lot::RwLock;
    use serde_json::{json, Value};
    use std::collections::HashMap;
    use std::sync::Arc;

    fn make_request(action: &str, body: Value) -> AwsRequest {
        AwsRequest {
            service: "ecr".into(),
            action: action.into(),
            region: "us-east-1".into(),
            account_id: "111111111111".into(),
            request_id: "req-1".into(),
            headers: HeaderMap::new(),
            query_params: HashMap::new(),
            body: Bytes::from(serde_json::to_vec(&body).unwrap()),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: vec![],
            raw_path: "/".into(),
            raw_query: String::new(),
            method: Method::POST,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    fn fixture(rules: Vec<ReplicationRule>) -> (EcrService, SharedEcrState) {
        const SOURCE: &str = "111111111111";
        let mut mas: MultiAccountState<EcrState> =
            MultiAccountState::new(SOURCE, "us-east-1", "http://fakecloud:4566");
        let source = mas.get_or_create(SOURCE);
        if !rules.is_empty() {
            source.replication_configuration = Some(ReplicationConfiguration { rules });
        }
        let arn = source.repository_arn("us-east-1", "app");
        let repo = Repository::new("app", arn, SOURCE, "fakecloud:4566");
        source.repositories.insert("app".to_string(), repo);
        let state: SharedEcrState = Arc::new(RwLock::new(mas));
        let svc = EcrService::new(state.clone());
        (svc, state)
    }

    fn seed_image(state: &SharedEcrState, account: &str, repo_name: &str, digest: &str) {
        let mut accounts = state.write();
        let s = accounts.get_mut(account).expect("source account");
        let repo = s.repositories.get_mut(repo_name).expect("source repo");
        repo.images.insert(
            digest.to_string(),
            Image {
                image_digest: digest.to_string(),
                image_manifest: "{\"mediaType\":\"x\"}".to_string(),
                image_manifest_media_type: "application/vnd.oci.image.manifest.v1+json".to_string(),
                artifact_media_type: None,
                image_size_in_bytes: 0,
                image_pushed_at: chrono::Utc::now(),
                last_recorded_pull_time: None,
                image_status: "ACTIVE".to_string(),
                last_archived_at: None,
                last_activated_at: None,
                last_in_use_at: None,
                in_use_count: 0,
            },
        );
    }

    #[test]
    fn update_repository_creation_template_applies_all_fields() {
        // customRoleArn / repositoryPolicy / lifecyclePolicy /
        // encryptionConfiguration were dropped on update (bug-audit
        // 2026-06-20, 1.24).
        let (svc, _state) = fixture(Vec::new());
        svc.create_repository_creation_template(&make_request(
            "CreateRepositoryCreationTemplate",
            json!({"prefix": "team/", "appliedFor": ["PULL_THROUGH_CACHE"]}),
        ))
        .unwrap();

        svc.update_repository_creation_template(&make_request(
            "UpdateRepositoryCreationTemplate",
            json!({
                "prefix": "team/",
                "customRoleArn": "arn:aws:iam::111111111111:role/r",
                "repositoryPolicy": "{\"Version\":\"2012-10-17\"}",
                "lifecyclePolicy": "{\"rules\":[]}",
                "encryptionConfiguration": {"encryptionType": "KMS", "kmsKey": "arn:kms:key"}
            }),
        ))
        .unwrap();

        let resp = svc
            .describe_repository_creation_templates(&make_request(
                "DescribeRepositoryCreationTemplates",
                json!({"prefixes": ["team/"]}),
            ))
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let tpl = &body["repositoryCreationTemplates"][0];
        assert_eq!(
            tpl["customRoleArn"], "arn:aws:iam::111111111111:role/r",
            "{tpl}"
        );
        assert_eq!(tpl["repositoryPolicy"], "{\"Version\":\"2012-10-17\"}");
        assert_eq!(tpl["lifecyclePolicy"], "{\"rules\":[]}");
        assert_eq!(tpl["encryptionConfiguration"]["encryptionType"], "KMS");
        assert_eq!(tpl["encryptionConfiguration"]["kmsKey"], "arn:kms:key");
    }

    #[test]
    fn put_image_triggers_replication_to_configured_destination() {
        const SOURCE: &str = "111111111111";
        const TARGET: &str = "222222222222";
        let rules = vec![ReplicationRule {
            destinations: vec![ReplicationDestination {
                region: "us-west-2".to_string(),
                registry_id: TARGET.to_string(),
            }],
            repository_filters: Vec::new(),
        }];
        let (svc, state) = fixture(rules);
        seed_image(&state, SOURCE, "app", "sha256:trig");

        svc.replicate_image(SOURCE, "app", "sha256:trig");

        let accounts = state.read();
        let target_state = accounts.get(TARGET).expect("target account materialised");
        let target_repo = target_state
            .repositories
            .get("app")
            .expect("target repo provisioned by replication");
        assert!(target_repo.images.contains_key("sha256:trig"));
    }

    #[test]
    fn put_image_records_replication_status_complete() {
        const SOURCE: &str = "111111111111";
        const TARGET: &str = "222222222222";
        let rules = vec![ReplicationRule {
            destinations: vec![ReplicationDestination {
                region: "us-west-2".to_string(),
                registry_id: TARGET.to_string(),
            }],
            repository_filters: Vec::new(),
        }];
        let (svc, state) = fixture(rules);
        seed_image(&state, SOURCE, "app", "sha256:complete");
        svc.replicate_image(SOURCE, "app", "sha256:complete");

        let req = make_request(
            "DescribeImageReplicationStatus",
            json!({
                "repositoryName": "app",
                "imageId": {"imageDigest": "sha256:complete"},
            }),
        );
        let resp = svc.describe_image_replication_status(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let entries = body["replicationStatuses"].as_array().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0]["status"], "COMPLETE");
        assert_eq!(entries[0]["registryId"], TARGET);
        assert_eq!(entries[0]["region"], "us-west-2");
    }

    #[test]
    fn put_image_skips_replication_when_no_rules_configured() {
        const SOURCE: &str = "111111111111";
        let (svc, state) = fixture(Vec::new());
        seed_image(&state, SOURCE, "app", "sha256:none");
        svc.replicate_image(SOURCE, "app", "sha256:none");

        let accounts = state.read();
        // No mirror account materialised.
        assert!(accounts.get("222222222222").is_none());
        let source_repo = accounts
            .get(SOURCE)
            .unwrap()
            .repositories
            .get("app")
            .unwrap();
        assert!(!source_repo.replication_statuses.contains_key("sha256:none"));
    }

    #[test]
    fn describe_image_replication_status_returns_empty_for_unreplicated_image() {
        const SOURCE: &str = "111111111111";
        const TARGET: &str = "222222222222";
        // Rule only fires for repos prefixed `prod/`; our image is in `app`.
        let rules = vec![ReplicationRule {
            destinations: vec![ReplicationDestination {
                region: "us-west-2".to_string(),
                registry_id: TARGET.to_string(),
            }],
            repository_filters: vec![RepositoryFilter {
                filter: "prod/".to_string(),
                filter_type: "PREFIX_MATCH".to_string(),
            }],
        }];
        let (svc, state) = fixture(rules);
        seed_image(&state, SOURCE, "app", "sha256:nofilter");
        svc.replicate_image(SOURCE, "app", "sha256:nofilter");

        let req = make_request(
            "DescribeImageReplicationStatus",
            json!({
                "repositoryName": "app",
                "imageId": {"imageDigest": "sha256:nofilter"},
            }),
        );
        let resp = svc.describe_image_replication_status(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let entries = body["replicationStatuses"].as_array().unwrap();
        assert!(entries.is_empty());

        // And the target account was never created.
        let accounts = state.read();
        assert!(accounts.get(TARGET).is_none());
    }
}

// ── Cross-account repo policy enforcement on data-plane ops ────
#[cfg(test)]
mod repo_policy_enforcement_tests {
    use super::super::EcrService;
    use crate::state::{EcrState, Image, Repository, SharedEcrState};
    use bytes::Bytes;
    use fakecloud_core::multi_account::MultiAccountState;
    use fakecloud_core::service::AwsRequest;
    use http::{HeaderMap, Method, StatusCode};
    use parking_lot::RwLock;
    use serde_json::{json, Value};
    use std::collections::HashMap;
    use std::sync::Arc;

    const OWNER: &str = "111111111111";
    const OTHER: &str = "222222222222";

    fn make_request(action: &str, caller_account: &str, body: Value) -> AwsRequest {
        AwsRequest {
            service: "ecr".into(),
            action: action.into(),
            region: "us-east-1".into(),
            account_id: caller_account.into(),
            request_id: "req-1".into(),
            headers: HeaderMap::new(),
            query_params: HashMap::new(),
            body: Bytes::from(serde_json::to_vec(&body).unwrap()),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: vec![],
            raw_path: "/".into(),
            raw_query: String::new(),
            method: Method::POST,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    fn fixture(policy: Option<&str>) -> (EcrService, SharedEcrState) {
        let mut mas: MultiAccountState<EcrState> =
            MultiAccountState::new(OWNER, "us-east-1", "http://fakecloud:4566");
        let owner = mas.get_or_create(OWNER);
        let arn = owner.repository_arn("us-east-1", "app");
        let mut repo = Repository::new("app", arn, OWNER, "fakecloud:4566");
        repo.policy = policy.map(|s| s.to_string());
        // Seed an image so BatchGetImage has something to look up after the
        // gate passes; the action under test should reach the body, not
        // bail early on missing data.
        repo.images.insert(
            "sha256:abc".to_string(),
            Image {
                image_digest: "sha256:abc".to_string(),
                image_manifest: "{\"mediaType\":\"x\"}".to_string(),
                image_manifest_media_type: "application/vnd.oci.image.manifest.v1+json".to_string(),
                artifact_media_type: None,
                image_size_in_bytes: 0,
                image_pushed_at: chrono::Utc::now(),
                last_recorded_pull_time: None,
                image_status: "ACTIVE".to_string(),
                last_archived_at: None,
                last_activated_at: None,
                last_in_use_at: None,
                in_use_count: 0,
            },
        );
        repo.image_tags
            .insert("v1".to_string(), "sha256:abc".to_string());
        owner.repositories.insert("app".to_string(), repo);
        let state: SharedEcrState = Arc::new(RwLock::new(mas));
        let svc = EcrService::new(state.clone());
        (svc, state)
    }

    fn cross_account_allow_policy(action: &str) -> String {
        format!(
            r#"{{
                "Version": "2012-10-17",
                "Statement": [{{
                    "Sid": "CrossAccount",
                    "Effect": "Allow",
                    "Principal": {{"AWS": "arn:aws:iam::{OTHER}:root"}},
                    "Action": ["{action}"],
                    "Resource": "*"
                }}]
            }}"#
        )
    }

    #[test]
    fn batch_get_image_cross_account_blocked_when_no_policy() {
        // Real ECR distinguishes "no resource policy at all" from "policy
        // exists but denies": the former returns
        // RepositoryPolicyNotFoundException so the cross-account caller can
        // tell the owner hasn't shared the repo, while the latter returns
        // AccessDeniedException.
        let (svc, _state) = fixture(None);
        let req = make_request(
            "BatchGetImage",
            OTHER,
            json!({
                "registryId": OWNER,
                "repositoryName": "app",
                "imageIds": [{"imageTag": "v1"}],
            }),
        );
        let err = match svc.batch_get_image(&req) {
            Err(e) => e,
            Ok(_) => panic!("expected denial"),
        };
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
        assert_eq!(err.code(), "RepositoryPolicyNotFoundException");
    }

    #[test]
    fn batch_get_image_cross_account_allowed_by_policy() {
        let policy = cross_account_allow_policy("ecr:BatchGetImage");
        let (svc, _state) = fixture(Some(&policy));
        let req = make_request(
            "BatchGetImage",
            OTHER,
            json!({
                "registryId": OWNER,
                "repositoryName": "app",
                "imageIds": [{"imageTag": "v1"}],
            }),
        );
        let resp = svc.batch_get_image(&req).expect("should succeed");
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["images"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn batch_get_image_same_account_skips_policy_check() {
        let (svc, _state) = fixture(None);
        let req = make_request(
            "BatchGetImage",
            OWNER,
            json!({
                "repositoryName": "app",
                "imageIds": [{"imageTag": "v1"}],
            }),
        );
        let resp = svc.batch_get_image(&req).expect("same-account allowed");
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["images"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn put_image_cross_account_blocked_when_policy_denies_action() {
        // Policy allows BatchGetImage but not PutImage.
        let policy = cross_account_allow_policy("ecr:BatchGetImage");
        let (svc, _state) = fixture(Some(&policy));
        let manifest = "{\"mediaType\":\"x\"}";
        let req = make_request(
            "PutImage",
            OTHER,
            json!({
                "registryId": OWNER,
                "repositoryName": "app",
                "imageManifest": manifest,
                "imageTag": "v2",
            }),
        );
        let err = match svc.put_image(&req) {
            Err(e) => e,
            Ok(_) => panic!("expected denial"),
        };
        assert_eq!(err.status(), StatusCode::FORBIDDEN);
        assert_eq!(err.code(), "AccessDeniedException");
    }

    #[test]
    fn get_download_url_for_layer_cross_account_blocked_when_no_policy() {
        let (svc, _state) = fixture(None);
        let req = make_request(
            "GetDownloadUrlForLayer",
            OTHER,
            json!({
                "registryId": OWNER,
                "repositoryName": "app",
                "layerDigest": "sha256:deadbeef",
            }),
        );
        let err = match svc.get_download_url_for_layer(&req) {
            Err(e) => e,
            Ok(_) => panic!("expected denial"),
        };
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
        assert_eq!(err.code(), "RepositoryPolicyNotFoundException");
    }

    #[test]
    fn put_image_cross_account_blocked_when_no_policy_returns_policy_not_found() {
        // PutImage with no repository policy at all on the cross-account
        // path: same RepositoryPolicyNotFoundException semantics as the
        // pull-side ops, since AWS ECR uses the same gate code path.
        let (svc, _state) = fixture(None);
        let manifest = "{\"mediaType\":\"x\"}";
        let req = make_request(
            "PutImage",
            OTHER,
            json!({
                "registryId": OWNER,
                "repositoryName": "app",
                "imageManifest": manifest,
                "imageTag": "v2",
            }),
        );
        let err = match svc.put_image(&req) {
            Err(e) => e,
            Ok(_) => panic!("expected denial"),
        };
        assert_eq!(err.code(), "RepositoryPolicyNotFoundException");
    }

    #[test]
    fn batch_delete_image_cross_account_gated_by_policy() {
        // BatchDeleteImage is the most destructive cross-account op and
        // gets the same policy gate as the pull/push ops. With no policy:
        // RepositoryPolicyNotFoundException; with explicit Allow on the
        // matching action: success.
        let (svc, _state) = fixture(None);
        let req_no_policy = make_request(
            "BatchDeleteImage",
            OTHER,
            json!({
                "registryId": OWNER,
                "repositoryName": "app",
                "imageIds": [{"imageTag": "v1"}],
            }),
        );
        let err = match svc.batch_delete_image(&req_no_policy) {
            Err(e) => e,
            Ok(_) => panic!("expected RepositoryPolicyNotFoundException"),
        };
        assert_eq!(err.code(), "RepositoryPolicyNotFoundException");

        let policy = cross_account_allow_policy("ecr:BatchDeleteImage");
        let (svc, _state) = fixture(Some(&policy));
        let req_allowed = make_request(
            "BatchDeleteImage",
            OTHER,
            json!({
                "registryId": OWNER,
                "repositoryName": "app",
                "imageIds": [{"imageTag": "v1"}],
            }),
        );
        if let Err(e) = svc.batch_delete_image(&req_allowed) {
            panic!(
                "BatchDeleteImage with allow policy should succeed, got: {}",
                e.code()
            );
        }
    }
}

// ── Scan-on-push auto-trigger from PutImage ────────────────────

#[cfg(test)]
mod scan_on_push_tests {
    use super::super::EcrService;
    use crate::state::{
        EcrState, ImageScanningConfiguration, RegistryScanningConfiguration, RegistryScanningRule,
        Repository, RepositoryFilter, SharedEcrState,
    };
    use bytes::Bytes;
    use fakecloud_core::multi_account::MultiAccountState;
    use fakecloud_core::service::AwsRequest;
    use http::{HeaderMap, Method};
    use parking_lot::RwLock;
    use serde_json::{json, Value};
    use std::collections::HashMap;
    use std::sync::Arc;

    const ACCOUNT: &str = "111111111111";

    fn make_request(action: &str, body: Value) -> AwsRequest {
        AwsRequest {
            service: "ecr".into(),
            action: action.into(),
            region: "us-east-1".into(),
            account_id: ACCOUNT.into(),
            request_id: "req-1".into(),
            headers: HeaderMap::new(),
            query_params: HashMap::new(),
            body: Bytes::from(serde_json::to_vec(&body).unwrap()),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: vec![],
            raw_path: "/".into(),
            raw_query: String::new(),
            method: Method::POST,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    /// Build a service with a single repo `repo_name`, the supplied
    /// repo-level scan-on-push flag, and the supplied registry-level
    /// scanning configuration.
    fn fixture(
        repo_name: &str,
        repo_scan_on_push: bool,
        registry_cfg: RegistryScanningConfiguration,
    ) -> (EcrService, SharedEcrState) {
        let mut mas: MultiAccountState<EcrState> =
            MultiAccountState::new(ACCOUNT, "us-east-1", "http://fakecloud:4566");
        let state = mas.get_or_create(ACCOUNT);
        state.registry_scanning_configuration = registry_cfg;
        let arn = state.repository_arn("us-east-1", repo_name);
        let mut repo = Repository::new(repo_name, arn, ACCOUNT, "fakecloud:4566");
        repo.image_scanning_configuration = ImageScanningConfiguration {
            scan_on_push: repo_scan_on_push,
        };
        state.repositories.insert(repo_name.to_string(), repo);
        let shared: SharedEcrState = Arc::new(RwLock::new(mas));
        let svc = EcrService::new(shared.clone());
        (svc, shared)
    }

    fn put_image(svc: &EcrService, repo_name: &str, manifest: &str, tag: &str) -> String {
        let req = make_request(
            "PutImage",
            json!({
                "repositoryName": repo_name,
                "imageManifest": manifest,
                "imageTag": tag,
            }),
        );
        let resp = svc.put_image(&req).expect("PutImage should succeed");
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        body["image"]["imageId"]["imageDigest"]
            .as_str()
            .unwrap()
            .to_string()
    }

    fn scan_findings_present(state: &SharedEcrState, repo_name: &str, digest: &str) -> bool {
        let accounts = state.read();
        accounts
            .get(ACCOUNT)
            .and_then(|s| s.repositories.get(repo_name))
            .map(|r| r.scan_findings.contains_key(digest))
            .unwrap_or(false)
    }

    #[tokio::test]
    async fn put_image_triggers_scan_when_repo_scan_on_push_enabled() {
        let (svc, state) = fixture("app", true, RegistryScanningConfiguration::default());
        let digest = put_image(&svc, "app", "{\"mediaType\":\"x\"}", "v1");

        // trigger_scan synchronously inserts an IN_PROGRESS findings
        // entry before the async scanner runs. That entry is the
        // observable signal that scan-on-push fired.
        assert!(
            scan_findings_present(&state, "app", &digest),
            "expected scan_findings entry for {digest} after PutImage with repo scan-on-push"
        );

        // DescribeImageScanFindings reflects the scan having occurred:
        // status is IN_PROGRESS or COMPLETE depending on scheduler
        // timing, never absent.
        let req = make_request(
            "DescribeImageScanFindings",
            json!({
                "repositoryName": "app",
                "imageId": {"imageDigest": digest},
            }),
        );
        let resp = svc.describe_image_scan_findings(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let status = body["imageScanStatus"]["status"].as_str().unwrap();
        assert!(
            status == "IN_PROGRESS" || status == "COMPLETE",
            "expected scan status IN_PROGRESS or COMPLETE, got {status}"
        );
    }

    #[tokio::test]
    async fn put_image_skips_scan_when_repo_scan_on_push_disabled() {
        let (svc, state) = fixture("app", false, RegistryScanningConfiguration::default());
        let digest = put_image(&svc, "app", "{\"mediaType\":\"x\"}", "v1");

        assert!(
            !scan_findings_present(&state, "app", &digest),
            "expected no scan_findings entry when scan-on-push is disabled"
        );
    }

    #[tokio::test]
    async fn describe_scan_findings_on_never_scanned_image_is_scan_not_found() {
        // A pushed-but-never-scanned image must return ScanNotFoundException,
        // NOT a fabricated COMPLETE result that would green-light a CI gate.
        let (svc, _state) = fixture("app", false, RegistryScanningConfiguration::default());
        let digest = put_image(&svc, "app", "{\"mediaType\":\"x\"}", "v1");
        let req = make_request(
            "DescribeImageScanFindings",
            json!({
                "repositoryName": "app",
                "imageId": {"imageDigest": digest},
            }),
        );
        let err = match svc.describe_image_scan_findings(&req) {
            Ok(_) => panic!("expected ScanNotFoundException for never-scanned image"),
            Err(e) => e,
        };
        assert_eq!(err.code(), "ScanNotFoundException");
    }

    #[tokio::test]
    async fn put_image_triggers_scan_via_registry_level_rule() {
        // Repo-level disabled, but registry rule says SCAN_ON_PUSH for
        // any repo whose name matches the wildcard.
        let registry_cfg = RegistryScanningConfiguration {
            scan_type: "BASIC".to_string(),
            rules: vec![RegistryScanningRule {
                scan_frequency: "SCAN_ON_PUSH".to_string(),
                repository_filters: vec![RepositoryFilter {
                    filter: "app*".to_string(),
                    filter_type: "WILDCARD".to_string(),
                }],
            }],
        };
        let (svc, state) = fixture("app", false, registry_cfg);
        let digest = put_image(&svc, "app", "{\"mediaType\":\"x\"}", "v1");

        assert!(
            scan_findings_present(&state, "app", &digest),
            "expected registry-level rule to trigger scan even with repo flag off"
        );
    }

    #[tokio::test]
    async fn put_image_skips_scan_when_registry_rule_filter_does_not_match() {
        // Registry rule scoped to a different prefix; pushed repo
        // should not trigger any scan path.
        let registry_cfg = RegistryScanningConfiguration {
            scan_type: "BASIC".to_string(),
            rules: vec![RegistryScanningRule {
                scan_frequency: "SCAN_ON_PUSH".to_string(),
                repository_filters: vec![RepositoryFilter {
                    filter: "other-*".to_string(),
                    filter_type: "WILDCARD".to_string(),
                }],
            }],
        };
        let (svc, state) = fixture("app", false, registry_cfg);
        let digest = put_image(&svc, "app", "{\"mediaType\":\"x\"}", "v1");

        assert!(
            !scan_findings_present(&state, "app", &digest),
            "expected non-matching registry rule to skip scan"
        );
    }
}

// ── Lifecycle policy timestamp + ticker integration ─────────────
#[cfg(test)]
mod lifecycle_timestamp_tests {
    use super::super::EcrService;
    use crate::lifecycle_ticker::tick_once;
    use crate::state::{EcrState, Image, Repository, SharedEcrState};
    use bytes::Bytes;
    use fakecloud_core::multi_account::MultiAccountState;
    use fakecloud_core::service::AwsRequest;
    use http::{HeaderMap, Method};
    use parking_lot::RwLock;
    use serde_json::{json, Value};
    use std::collections::HashMap;
    use std::sync::Arc;

    const ACCOUNT: &str = "111111111111";

    fn make_request(action: &str, body: Value) -> AwsRequest {
        AwsRequest {
            service: "ecr".into(),
            action: action.into(),
            region: "us-east-1".into(),
            account_id: ACCOUNT.into(),
            request_id: "req-1".into(),
            headers: HeaderMap::new(),
            query_params: HashMap::new(),
            body: Bytes::from(serde_json::to_vec(&body).unwrap()),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: vec![],
            raw_path: "/".into(),
            raw_query: String::new(),
            method: Method::POST,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    fn fixture() -> (EcrService, SharedEcrState) {
        let mut mas: MultiAccountState<EcrState> =
            MultiAccountState::new(ACCOUNT, "us-east-1", "http://fakecloud:4566");
        let s = mas.get_or_create(ACCOUNT);
        let arn = s.repository_arn("us-east-1", "app");
        let mut repo = Repository::new("app", arn, ACCOUNT, "fakecloud:4566");
        // Seed an image old enough to be eligible for `sinceImagePushed`.
        repo.images.insert(
            "sha256:old".to_string(),
            Image {
                image_digest: "sha256:old".to_string(),
                image_manifest: String::new(),
                image_manifest_media_type: String::new(),
                artifact_media_type: None,
                image_size_in_bytes: 0,
                image_pushed_at: chrono::Utc::now() - chrono::Duration::days(30),
                last_recorded_pull_time: None,
                image_status: "ACTIVE".to_string(),
                last_archived_at: None,
                last_activated_at: None,
                last_in_use_at: None,
                in_use_count: 0,
            },
        );
        repo.image_tags
            .insert("v1".to_string(), "sha256:old".to_string());
        s.repositories.insert("app".to_string(), repo);
        let state: SharedEcrState = Arc::new(RwLock::new(mas));
        let svc = EcrService::new(state.clone());
        (svc, state)
    }

    fn parse_body(resp: fakecloud_core::service::AwsResponse) -> Value {
        serde_json::from_slice(resp.body.expect_bytes()).expect("response body is JSON")
    }

    #[tokio::test]
    async fn put_lifecycle_policy_then_get_returns_last_evaluated_at() {
        let (svc, _state) = fixture();
        let policy = json!({
            "rules": [{
                "rulePriority": 1,
                "selection": {
                    "tagStatus": "any",
                    "countType": "imageCountMoreThan",
                    "countNumber": 100
                }
            }]
        })
        .to_string();
        let put_req = make_request(
            "PutLifecyclePolicy",
            json!({
                "repositoryName": "app",
                "lifecyclePolicyText": policy,
            }),
        );
        let put_resp = <EcrService as fakecloud_core::service::AwsService>::handle(&svc, put_req)
            .await
            .expect("PutLifecyclePolicy succeeds");
        assert!(put_resp.status.is_success());

        let get_req = make_request("GetLifecyclePolicy", json!({"repositoryName": "app"}));
        let get_resp = <EcrService as fakecloud_core::service::AwsService>::handle(&svc, get_req)
            .await
            .expect("GetLifecyclePolicy succeeds");
        let body = parse_body(get_resp);
        let ts = body
            .get("lastEvaluatedAt")
            .and_then(|v| v.as_i64())
            .expect("lastEvaluatedAt present");
        assert!(
            ts > 0,
            "lastEvaluatedAt should be a non-zero epoch second after PutLifecyclePolicy"
        );
        assert_eq!(
            body.get("repositoryName").and_then(|v| v.as_str()),
            Some("app")
        );
    }

    #[tokio::test]
    async fn ticker_tick_once_updates_last_evaluated_at_and_prunes() {
        let (svc, state) = fixture();
        // Policy: prune images older than 7 days (the seeded image is 30 days old).
        let policy = json!({
            "rules": [{
                "rulePriority": 1,
                "selection": {
                    "tagStatus": "any",
                    "countType": "sinceImagePushed",
                    "countUnit": "days",
                    "countNumber": 7
                }
            }]
        })
        .to_string();
        // Install policy with PutLifecyclePolicy (which already prunes
        // synchronously).
        let put_req = make_request(
            "PutLifecyclePolicy",
            json!({
                "repositoryName": "app",
                "lifecyclePolicyText": policy,
            }),
        );
        <EcrService as fakecloud_core::service::AwsService>::handle(&svc, put_req)
            .await
            .expect("PutLifecyclePolicy succeeds");

        // Capture the post-Put timestamp; the ticker should advance it.
        let first_ts = {
            let accounts = state.read();
            accounts
                .get(ACCOUNT)
                .unwrap()
                .repositories
                .get("app")
                .unwrap()
                .lifecycle_policy_last_evaluated_at
                .expect("Put stamped last_evaluated_at")
        };
        // Re-seed an old image to verify the ticker prunes it on the
        // next pass (Put already pruned the original).
        {
            let mut accounts = state.write();
            let s = accounts.get_mut(ACCOUNT).unwrap();
            let repo = s.repositories.get_mut("app").unwrap();
            repo.images.insert(
                "sha256:older".to_string(),
                Image {
                    image_digest: "sha256:older".to_string(),
                    image_manifest: String::new(),
                    image_manifest_media_type: String::new(),
                    artifact_media_type: None,
                    image_size_in_bytes: 0,
                    image_pushed_at: chrono::Utc::now() - chrono::Duration::days(60),
                    last_recorded_pull_time: None,
                    image_status: "ACTIVE".to_string(),
                    last_archived_at: None,
                    last_activated_at: None,
                    last_in_use_at: None,
                    in_use_count: 0,
                },
            );
        }
        // Ensure clock advances at least one second so the new
        // timestamp is strictly greater.
        tokio::time::sleep(std::time::Duration::from_millis(1100)).await;

        // Simulate a periodic tick.
        tick_once(&state);

        let accounts = state.read();
        let repo = accounts
            .get(ACCOUNT)
            .unwrap()
            .repositories
            .get("app")
            .unwrap();
        let later_ts = repo
            .lifecycle_policy_last_evaluated_at
            .expect("tick stamped last_evaluated_at");
        assert!(
            later_ts >= first_ts,
            "tick should not move the timestamp backwards"
        );
        assert!(
            !repo.images.contains_key("sha256:older"),
            "tick should have pruned the 60-day-old image"
        );
    }
}

// ── P5: pull-time exclusions, storage class, referrers, in-use tracking ──
#[cfg(test)]
mod p5_polish_tests {
    use super::super::EcrService;
    use crate::state::{EcrState, Image, Repository, SharedEcrState};
    use bytes::Bytes;
    use fakecloud_core::multi_account::MultiAccountState;
    use fakecloud_core::service::AwsRequest;
    use http::{HeaderMap, Method};
    use parking_lot::RwLock;
    use serde_json::{json, Value};
    use std::collections::HashMap;
    use std::sync::Arc;

    const ACCOUNT: &str = "111111111111";

    fn make_request(action: &str, body: Value) -> AwsRequest {
        AwsRequest {
            service: "ecr".into(),
            action: action.into(),
            region: "us-east-1".into(),
            account_id: ACCOUNT.into(),
            request_id: "req-1".into(),
            headers: HeaderMap::new(),
            query_params: HashMap::new(),
            body: Bytes::from(serde_json::to_vec(&body).unwrap()),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: vec![],
            raw_path: "/".into(),
            raw_query: String::new(),
            method: Method::POST,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    fn fixture() -> (EcrService, SharedEcrState) {
        let mut mas: MultiAccountState<EcrState> =
            MultiAccountState::new(ACCOUNT, "us-east-1", "http://fakecloud:4566");
        let state = mas.get_or_create(ACCOUNT);
        let arn = state.repository_arn("us-east-1", "app");
        let repo = Repository::new("app", arn, ACCOUNT, "fakecloud:4566");
        state.repositories.insert("app".to_string(), repo);
        let shared: SharedEcrState = Arc::new(RwLock::new(mas));
        let svc = EcrService::new(shared.clone());
        (svc, shared)
    }

    fn seed_image(state: &SharedEcrState, digest: &str, manifest: &str) {
        let mut accounts = state.write();
        let s = accounts.get_mut(ACCOUNT).unwrap();
        let repo = s.repositories.get_mut("app").unwrap();
        repo.images.insert(
            digest.to_string(),
            Image {
                image_digest: digest.to_string(),
                image_manifest: manifest.to_string(),
                image_manifest_media_type: "application/vnd.oci.image.manifest.v1+json".to_string(),
                artifact_media_type: None,
                image_size_in_bytes: manifest.len() as u64,
                image_pushed_at: chrono::Utc::now(),
                last_recorded_pull_time: None,
                image_status: "ACTIVE".to_string(),
                last_archived_at: None,
                last_activated_at: None,
                last_in_use_at: None,
                in_use_count: 0,
            },
        );
    }

    // ── PullTimeUpdateExclusion round-trip ────────────────────────
    #[test]
    fn pull_time_exclusion_register_list_deregister_round_trip() {
        let (svc, _state) = fixture();
        let arn = "arn:aws:iam::111111111111:role/ci-puller";

        let req = make_request(
            "RegisterPullTimeUpdateExclusion",
            json!({ "principalArn": arn }),
        );
        let resp = svc.register_pull_time_update_exclusion(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["principalArn"], arn);

        let req = make_request("ListPullTimeUpdateExclusions", json!({}));
        let resp = svc.list_pull_time_update_exclusions(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let list = body["pullTimeUpdateExclusions"].as_array().unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0]["principalArn"], arn);

        let req = make_request(
            "DeregisterPullTimeUpdateExclusion",
            json!({ "principalArn": arn }),
        );
        svc.deregister_pull_time_update_exclusion(&req).unwrap();

        let req = make_request("ListPullTimeUpdateExclusions", json!({}));
        let resp = svc.list_pull_time_update_exclusions(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert!(body["pullTimeUpdateExclusions"]
            .as_array()
            .unwrap()
            .is_empty());
    }

    // ── UpdateImageStorageClass: ARCHIVE then STANDARD round-trip ─
    #[test]
    fn update_image_storage_class_archive_then_restore_persists() {
        let (svc, state) = fixture();
        seed_image(&state, "sha256:abc", "{}");

        let req = make_request(
            "UpdateImageStorageClass",
            json!({
                "repositoryName": "app",
                "imageId": { "imageDigest": "sha256:abc" },
                "targetStorageClass": "ARCHIVE",
            }),
        );
        let resp = svc.update_image_storage_class(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["imageStatus"], "ARCHIVED");

        // DescribeImages reflects the archive.
        let req = make_request(
            "DescribeImages",
            json!({
                "repositoryName": "app",
                "imageIds": [{ "imageDigest": "sha256:abc" }],
            }),
        );
        let resp = svc.describe_images(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let detail = &body["imageDetails"][0];
        assert_eq!(detail["imageStatus"], "ARCHIVED");
        assert!(detail["lastArchivedAt"].is_i64());
        assert!(detail.get("lastActivatedAt").is_none());

        // Restore.
        let req = make_request(
            "UpdateImageStorageClass",
            json!({
                "repositoryName": "app",
                "imageId": { "imageDigest": "sha256:abc" },
                "targetStorageClass": "STANDARD",
            }),
        );
        let resp = svc.update_image_storage_class(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["imageStatus"], "ACTIVE");

        let req = make_request(
            "DescribeImages",
            json!({
                "repositoryName": "app",
                "imageIds": [{ "imageDigest": "sha256:abc" }],
            }),
        );
        let resp = svc.describe_images(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let detail = &body["imageDetails"][0];
        assert_eq!(detail["imageStatus"], "ACTIVE");
        assert!(detail["lastActivatedAt"].is_i64());
    }

    #[test]
    fn update_image_storage_class_rejects_unknown_class() {
        let (svc, state) = fixture();
        seed_image(&state, "sha256:abc", "{}");
        let req = make_request(
            "UpdateImageStorageClass",
            json!({
                "repositoryName": "app",
                "imageId": { "imageDigest": "sha256:abc" },
                "targetStorageClass": "GLACIER",
            }),
        );
        let err = svc
            .update_image_storage_class(&req)
            .err()
            .expect("GLACIER must be rejected");
        match err {
            fakecloud_core::service::AwsServiceError::AwsError { status, code, .. } => {
                assert_eq!(code, "InvalidParameterException");
                assert!(status.is_client_error());
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    // ── ListImageReferrers walks the manifest subject graph ───────
    #[test]
    fn list_image_referrers_returns_images_with_matching_subject() {
        let (svc, state) = fixture();
        // Subject image.
        seed_image(&state, "sha256:subject", "{}");
        // Referrer with subject pointing at sha256:subject (signature artifact).
        let referrer_manifest = serde_json::json!({
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "artifactType": "application/vnd.dev.cosign.artifact.sig.v1+json",
            "config": { "mediaType": "application/vnd.oci.empty.v1+json" },
            "subject": {
                "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "digest": "sha256:subject",
                "size": 2,
            },
            "annotations": { "org.opencontainers.image.created": "2026-05-03T00:00:00Z" },
            "layers": [],
        })
        .to_string();
        seed_image(&state, "sha256:sig", &referrer_manifest);
        // Unrelated image (different subject) — must not appear.
        let other_manifest = serde_json::json!({
            "subject": { "digest": "sha256:other" },
        })
        .to_string();
        seed_image(&state, "sha256:other-ref", &other_manifest);

        let req = make_request(
            "ListImageReferrers",
            json!({
                "repositoryName": "app",
                "subjectId": { "imageDigest": "sha256:subject" },
            }),
        );
        let resp = svc.list_image_referrers(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let referrers = body["referrers"].as_array().unwrap();
        assert_eq!(referrers.len(), 1, "exactly one matching referrer");
        assert_eq!(referrers[0]["digest"], "sha256:sig");
        assert_eq!(
            referrers[0]["artifactType"],
            "application/vnd.dev.cosign.artifact.sig.v1+json"
        );
        assert_eq!(referrers[0]["artifactStatus"], "ACTIVE");
        assert!(referrers[0]["annotations"]["org.opencontainers.image.created"].is_string());
    }

    #[test]
    fn list_image_referrers_filters_by_artifact_type() {
        let (svc, state) = fixture();
        seed_image(&state, "sha256:subject", "{}");
        let sig_manifest = serde_json::json!({
            "artifactType": "cosign.sig",
            "subject": { "digest": "sha256:subject" },
        })
        .to_string();
        seed_image(&state, "sha256:sig", &sig_manifest);
        let sbom_manifest = serde_json::json!({
            "artifactType": "cyclonedx.sbom",
            "subject": { "digest": "sha256:subject" },
        })
        .to_string();
        seed_image(&state, "sha256:sbom", &sbom_manifest);

        let req = make_request(
            "ListImageReferrers",
            json!({
                "repositoryName": "app",
                "subjectId": { "imageDigest": "sha256:subject" },
                "filter": { "artifactTypes": ["cosign.sig"] },
            }),
        );
        let resp = svc.list_image_referrers(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let referrers = body["referrers"].as_array().unwrap();
        assert_eq!(referrers.len(), 1);
        assert_eq!(referrers[0]["digest"], "sha256:sig");
    }

    #[test]
    fn list_image_referrers_rejects_missing_subject_image() {
        let (svc, _state) = fixture();
        let req = make_request(
            "ListImageReferrers",
            json!({
                "repositoryName": "app",
                "subjectId": { "imageDigest": "sha256:nope" },
            }),
        );
        let err = svc
            .list_image_referrers(&req)
            .err()
            .expect("missing subject image must error");
        match err {
            fakecloud_core::service::AwsServiceError::AwsError { code, .. } => {
                assert_eq!(code, "ImageNotFoundException");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    // ── DescribeImages: lastInUseAt + inUseCount track BatchGetImage ─
    #[test]
    fn describe_images_reflects_in_use_after_batch_get_image() {
        let (svc, state) = fixture();
        seed_image(&state, "sha256:hot", "{}");

        // Before any pull: counter is 0, lastInUseAt absent.
        let req = make_request(
            "DescribeImages",
            json!({
                "repositoryName": "app",
                "imageIds": [{ "imageDigest": "sha256:hot" }],
            }),
        );
        let resp = svc.describe_images(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let detail = &body["imageDetails"][0];
        assert_eq!(detail["inUseCount"], 0);
        assert!(detail.get("lastInUseAt").is_none());

        // Two BatchGetImage calls bump the counter to 2.
        for _ in 0..2 {
            let req = make_request(
                "BatchGetImage",
                json!({
                    "repositoryName": "app",
                    "imageIds": [{ "imageDigest": "sha256:hot" }],
                }),
            );
            svc.batch_get_image(&req).unwrap();
        }

        let req = make_request(
            "DescribeImages",
            json!({
                "repositoryName": "app",
                "imageIds": [{ "imageDigest": "sha256:hot" }],
            }),
        );
        let resp = svc.describe_images(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let detail = &body["imageDetails"][0];
        assert_eq!(detail["inUseCount"], 2);
        assert!(detail["lastInUseAt"].is_i64());
        // lastRecordedPullTime is touched in lockstep so existing
        // SDK callers see consistent timestamps.
        assert!(detail["lastRecordedPullTime"].is_i64());
    }

    // ── DescribeImages: subjectManifestDigest is surfaced for referrers ─
    #[test]
    fn describe_images_includes_subject_manifest_digest_for_referrers() {
        let (svc, state) = fixture();
        let manifest = serde_json::json!({
            "subject": { "digest": "sha256:parent" },
        })
        .to_string();
        seed_image(&state, "sha256:child", &manifest);

        let req = make_request(
            "DescribeImages",
            json!({
                "repositoryName": "app",
                "imageIds": [{ "imageDigest": "sha256:child" }],
            }),
        );
        let resp = svc.describe_images(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let detail = &body["imageDetails"][0];
        assert_eq!(detail["subjectManifestDigest"], "sha256:parent");
    }

    // ── Pull-time exclusion suppresses in-use bookkeeping ─────────
    #[test]
    fn batch_get_image_skips_pull_metadata_for_excluded_principal() {
        use fakecloud_core::auth::{Principal, PrincipalType};

        let (svc, state) = fixture();
        seed_image(&state, "sha256:hot", "{}");

        let role_arn = "arn:aws:iam::111111111111:role/ci-puller";
        let req = make_request(
            "RegisterPullTimeUpdateExclusion",
            json!({ "principalArn": role_arn }),
        );
        svc.register_pull_time_update_exclusion(&req).unwrap();

        let mut req = make_request(
            "BatchGetImage",
            json!({
                "repositoryName": "app",
                "imageIds": [{ "imageDigest": "sha256:hot" }],
            }),
        );
        req.principal = Some(Principal {
            arn: role_arn.to_string(),
            user_id: "AROAEXCLUDED".to_string(),
            account_id: ACCOUNT.to_string(),
            principal_type: PrincipalType::AssumedRole,
            source_identity: None,
            tags: None,
        });
        svc.batch_get_image(&req).unwrap();

        let req = make_request(
            "DescribeImages",
            json!({
                "repositoryName": "app",
                "imageIds": [{ "imageDigest": "sha256:hot" }],
            }),
        );
        let resp = svc.describe_images(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let detail = &body["imageDetails"][0];
        assert_eq!(
            detail["inUseCount"], 0,
            "excluded principal must not bump inUseCount"
        );
        assert!(
            detail.get("lastInUseAt").is_none(),
            "excluded principal must not set lastInUseAt"
        );
    }

    // ── Non-excluded principals still bump after a registration ───
    #[test]
    fn batch_get_image_still_tracks_other_principals_after_exclusion() {
        use fakecloud_core::auth::{Principal, PrincipalType};

        let (svc, state) = fixture();
        seed_image(&state, "sha256:hot", "{}");

        let excluded_arn = "arn:aws:iam::111111111111:role/excluded";
        let active_arn = "arn:aws:iam::111111111111:role/active";
        let req = make_request(
            "RegisterPullTimeUpdateExclusion",
            json!({ "principalArn": excluded_arn }),
        );
        svc.register_pull_time_update_exclusion(&req).unwrap();

        let mut req = make_request(
            "BatchGetImage",
            json!({
                "repositoryName": "app",
                "imageIds": [{ "imageDigest": "sha256:hot" }],
            }),
        );
        req.principal = Some(Principal {
            arn: active_arn.to_string(),
            user_id: "AROAACTIVE".to_string(),
            account_id: ACCOUNT.to_string(),
            principal_type: PrincipalType::AssumedRole,
            source_identity: None,
            tags: None,
        });
        svc.batch_get_image(&req).unwrap();

        let req = make_request(
            "DescribeImages",
            json!({
                "repositoryName": "app",
                "imageIds": [{ "imageDigest": "sha256:hot" }],
            }),
        );
        let resp = svc.describe_images(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let detail = &body["imageDetails"][0];
        assert_eq!(detail["inUseCount"], 1);
        assert!(detail["lastInUseAt"].is_i64());
    }
}

#[cfg(test)]
mod snapshot_hook_tests {
    use super::super::EcrService;
    use crate::state::SharedEcrState;
    use parking_lot::RwLock;
    use std::sync::Arc;

    fn make_state() -> SharedEcrState {
        Arc::new(RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new(
                "111111111111",
                "us-east-1",
                "http://fakecloud:4566",
            ),
        ))
    }

    /// No snapshot store (memory mode) -> no persist hook for the CFN provisioner.
    #[test]
    fn snapshot_hook_is_none_without_store() {
        let svc = EcrService::new(make_state());
        assert!(svc.snapshot_hook().is_none());
    }

    /// With a store, the hook is present and invoking it runs the whole-state
    /// persist path the CloudFormation provisioner uses after mutating ECR
    /// state directly.
    #[tokio::test]
    async fn snapshot_hook_fires_with_store() {
        let store: Arc<dyn fakecloud_persistence::SnapshotStore> =
            Arc::new(fakecloud_persistence::MemorySnapshotStore::new());
        let svc = EcrService::new(make_state()).with_snapshot_store(store);
        let hook = svc
            .snapshot_hook()
            .expect("hook present when a store is set");
        hook().await;
    }
}
