//! AWS CodeCommit awsJson1.1 dispatch + operation handlers.
//!
//! Implements the full CodeCommit git-repository control plane against a real
//! content-addressed object store (see [`crate::state`]): repositories, the
//! commit graph and materialized trees, branches, files, pull requests with
//! approvals/events/overrides, approval-rule templates and their repository
//! associations, per-revision pull-request approval rules, comments and
//! reactions, repository triggers, and resource tagging. Blob/tree/commit ids
//! are real 40-char SHA-1 hex; clone URLs, ARNs, and repository UUIDs are minted
//! in exact AWS form. Merges are computed on the stored graph (fast-forward and
//! non-conflicting three-way/squash resolve; genuinely divergent trees report
//! the declared `ManualMergeRequiredException`). Everything is real, persisted,
//! and account-partitioned; there is no live git transport.

use async_trait::async_trait;
use base64::Engine;
use chrono::{DateTime, Utc};
use http::StatusCode;
use serde_json::{json, Map, Value};
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::state::{CodeCommitState, FileEntry, Repo, SharedCodeCommitState};
use crate::validate;

pub const CODECOMMIT_ACTIONS: &[&str] = &[
    "AssociateApprovalRuleTemplateWithRepository",
    "BatchAssociateApprovalRuleTemplateWithRepositories",
    "BatchDescribeMergeConflicts",
    "BatchDisassociateApprovalRuleTemplateFromRepositories",
    "BatchGetCommits",
    "BatchGetRepositories",
    "CreateApprovalRuleTemplate",
    "CreateBranch",
    "CreateCommit",
    "CreatePullRequest",
    "CreatePullRequestApprovalRule",
    "CreateRepository",
    "CreateUnreferencedMergeCommit",
    "DeleteApprovalRuleTemplate",
    "DeleteBranch",
    "DeleteCommentContent",
    "DeleteFile",
    "DeletePullRequestApprovalRule",
    "DeleteRepository",
    "DescribeMergeConflicts",
    "DescribePullRequestEvents",
    "DisassociateApprovalRuleTemplateFromRepository",
    "EvaluatePullRequestApprovalRules",
    "GetApprovalRuleTemplate",
    "GetBlob",
    "GetBranch",
    "GetComment",
    "GetCommentReactions",
    "GetCommentsForComparedCommit",
    "GetCommentsForPullRequest",
    "GetCommit",
    "GetDifferences",
    "GetFile",
    "GetFolder",
    "GetMergeCommit",
    "GetMergeConflicts",
    "GetMergeOptions",
    "GetPullRequest",
    "GetPullRequestApprovalStates",
    "GetPullRequestOverrideState",
    "GetRepository",
    "GetRepositoryTriggers",
    "ListApprovalRuleTemplates",
    "ListAssociatedApprovalRuleTemplatesForRepository",
    "ListBranches",
    "ListFileCommitHistory",
    "ListPullRequests",
    "ListRepositories",
    "ListRepositoriesForApprovalRuleTemplate",
    "ListTagsForResource",
    "MergeBranchesByFastForward",
    "MergeBranchesBySquash",
    "MergeBranchesByThreeWay",
    "MergePullRequestByFastForward",
    "MergePullRequestBySquash",
    "MergePullRequestByThreeWay",
    "OverridePullRequestApprovalRules",
    "PostCommentForComparedCommit",
    "PostCommentForPullRequest",
    "PostCommentReply",
    "PutCommentReaction",
    "PutFile",
    "PutRepositoryTriggers",
    "TagResource",
    "TestRepositoryTriggers",
    "UntagResource",
    "UpdateApprovalRuleTemplateContent",
    "UpdateApprovalRuleTemplateDescription",
    "UpdateApprovalRuleTemplateName",
    "UpdateComment",
    "UpdateDefaultBranch",
    "UpdatePullRequestApprovalRuleContent",
    "UpdatePullRequestApprovalState",
    "UpdatePullRequestDescription",
    "UpdatePullRequestStatus",
    "UpdatePullRequestTitle",
    "UpdateRepositoryDescription",
    "UpdateRepositoryEncryptionKey",
    "UpdateRepositoryName",
];

pub struct CodeCommitService {
    state: SharedCodeCommitState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl CodeCommitService {
    pub fn new(state: SharedCodeCommitState) -> Self {
        Self {
            state,
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
        }
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    async fn save(&self) {
        crate::persistence::save_snapshot(
            &self.state,
            self.snapshot_store.clone(),
            &self.snapshot_lock,
        )
        .await;
    }

    /// Persist hook for the CloudFormation provisioner; `None` in memory mode.
    pub fn snapshot_hook(&self) -> Option<fakecloud_persistence::SnapshotHook> {
        let store = self.snapshot_store.clone()?;
        let state = self.state.clone();
        let lock = self.snapshot_lock.clone();
        Some(Arc::new(move || {
            let state = state.clone();
            let store = store.clone();
            let lock = lock.clone();
            Box::pin(async move {
                crate::persistence::save_snapshot(&state, Some(store), &lock).await;
            })
        }))
    }
}

/// Whether an action can mutate persisted state (so a snapshot is taken on a
/// successful response). The read/list/describe/get/evaluate/test operations
/// never mutate.
fn is_mutator(action: &str) -> bool {
    action.starts_with("Create")
        || action.starts_with("Delete")
        || action.starts_with("Update")
        || action.starts_with("Put")
        || action.starts_with("Merge")
        || action.starts_with("Post")
        || action.starts_with("Associate")
        || action.starts_with("Disassociate")
        || action.starts_with("BatchAssociate")
        || action.starts_with("BatchDisassociate")
        || matches!(
            action,
            "TagResource" | "UntagResource" | "OverridePullRequestApprovalRules"
        )
}

#[async_trait]
impl AwsService for CodeCommitService {
    fn service_name(&self) -> &str {
        "codecommit"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let action = req.action.clone();
        let result = self.dispatch(&action, &req);
        let should_save =
            matches!(result.as_ref(), Ok(resp) if resp.status.is_success()) && is_mutator(&action);
        if should_save {
            self.save().await;
        }
        result
    }

    fn supported_actions(&self) -> &[&str] {
        CODECOMMIT_ACTIONS
    }
}

impl CodeCommitService {
    #[allow(clippy::too_many_lines)]
    fn dispatch(&self, action: &str, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        match action {
            // Repositories
            "CreateRepository" => self.create_repository(req),
            "GetRepository" => self.get_repository(req),
            "DeleteRepository" => self.delete_repository(req),
            "ListRepositories" => self.list_repositories(req),
            "BatchGetRepositories" => self.batch_get_repositories(req),
            "UpdateRepositoryDescription" => self.update_repository_description(req),
            "UpdateRepositoryName" => self.update_repository_name(req),
            "UpdateRepositoryEncryptionKey" => self.update_repository_encryption_key(req),
            // Branches
            "CreateBranch" => self.create_branch(req),
            "DeleteBranch" => self.delete_branch(req),
            "GetBranch" => self.get_branch(req),
            "ListBranches" => self.list_branches(req),
            "UpdateDefaultBranch" => self.update_default_branch(req),
            // Files, blobs, commits
            "PutFile" => self.put_file(req),
            "DeleteFile" => self.delete_file(req),
            "GetFile" => self.get_file(req),
            "GetFolder" => self.get_folder(req),
            "GetBlob" => self.get_blob(req),
            "CreateCommit" => self.create_commit(req),
            "GetCommit" => self.get_commit(req),
            "BatchGetCommits" => self.batch_get_commits(req),
            "GetDifferences" => self.get_differences(req),
            "ListFileCommitHistory" => self.list_file_commit_history(req),
            // Merges
            "MergeBranchesByFastForward" => self.merge_branches_fast_forward(req),
            "MergeBranchesBySquash" => self.merge_branches_squash(req),
            "MergeBranchesByThreeWay" => self.merge_branches_three_way(req),
            "CreateUnreferencedMergeCommit" => self.create_unreferenced_merge_commit(req),
            "GetMergeCommit" => self.get_merge_commit(req),
            "GetMergeOptions" => self.get_merge_options(req),
            "GetMergeConflicts" => self.get_merge_conflicts(req),
            "DescribeMergeConflicts" => self.describe_merge_conflicts(req),
            "BatchDescribeMergeConflicts" => self.batch_describe_merge_conflicts(req),
            // Pull requests
            "CreatePullRequest" => self.create_pull_request(req),
            "GetPullRequest" => self.get_pull_request(req),
            "ListPullRequests" => self.list_pull_requests(req),
            "UpdatePullRequestTitle" => self.update_pull_request_title(req),
            "UpdatePullRequestDescription" => self.update_pull_request_description(req),
            "UpdatePullRequestStatus" => self.update_pull_request_status(req),
            "DescribePullRequestEvents" => self.describe_pull_request_events(req),
            "MergePullRequestByFastForward" => self.merge_pull_request_fast_forward(req),
            "MergePullRequestBySquash" => self.merge_pull_request_squash(req),
            "MergePullRequestByThreeWay" => self.merge_pull_request_three_way(req),
            // Pull-request approvals and rules
            "CreatePullRequestApprovalRule" => self.create_pull_request_approval_rule(req),
            "DeletePullRequestApprovalRule" => self.delete_pull_request_approval_rule(req),
            "UpdatePullRequestApprovalRuleContent" => {
                self.update_pull_request_approval_rule_content(req)
            }
            "UpdatePullRequestApprovalState" => self.update_pull_request_approval_state(req),
            "GetPullRequestApprovalStates" => self.get_pull_request_approval_states(req),
            "EvaluatePullRequestApprovalRules" => self.evaluate_pull_request_approval_rules(req),
            "OverridePullRequestApprovalRules" => self.override_pull_request_approval_rules(req),
            "GetPullRequestOverrideState" => self.get_pull_request_override_state(req),
            // Approval-rule templates
            "CreateApprovalRuleTemplate" => self.create_approval_rule_template(req),
            "GetApprovalRuleTemplate" => self.get_approval_rule_template(req),
            "DeleteApprovalRuleTemplate" => self.delete_approval_rule_template(req),
            "ListApprovalRuleTemplates" => self.list_approval_rule_templates(req),
            "UpdateApprovalRuleTemplateContent" => self.update_approval_rule_template_content(req),
            "UpdateApprovalRuleTemplateDescription" => {
                self.update_approval_rule_template_description(req)
            }
            "UpdateApprovalRuleTemplateName" => self.update_approval_rule_template_name(req),
            "AssociateApprovalRuleTemplateWithRepository" => {
                self.associate_template_with_repository(req)
            }
            "DisassociateApprovalRuleTemplateFromRepository" => {
                self.disassociate_template_from_repository(req)
            }
            "BatchAssociateApprovalRuleTemplateWithRepositories" => {
                self.batch_associate_template(req)
            }
            "BatchDisassociateApprovalRuleTemplateFromRepositories" => {
                self.batch_disassociate_template(req)
            }
            "ListAssociatedApprovalRuleTemplatesForRepository" => {
                self.list_associated_templates_for_repository(req)
            }
            "ListRepositoriesForApprovalRuleTemplate" => self.list_repositories_for_template(req),
            // Comments
            "PostCommentForComparedCommit" => self.post_comment_for_compared_commit(req),
            "PostCommentForPullRequest" => self.post_comment_for_pull_request(req),
            "PostCommentReply" => self.post_comment_reply(req),
            "GetComment" => self.get_comment(req),
            "GetCommentsForComparedCommit" => self.get_comments_for_compared_commit(req),
            "GetCommentsForPullRequest" => self.get_comments_for_pull_request(req),
            "UpdateComment" => self.update_comment(req),
            "DeleteCommentContent" => self.delete_comment_content(req),
            "PutCommentReaction" => self.put_comment_reaction(req),
            "GetCommentReactions" => self.get_comment_reactions(req),
            // Triggers
            "PutRepositoryTriggers" => self.put_repository_triggers(req),
            "GetRepositoryTriggers" => self.get_repository_triggers(req),
            "TestRepositoryTriggers" => self.test_repository_triggers(req),
            // Tagging
            "TagResource" => self.tag_resource(req),
            "UntagResource" => self.untag_resource(req),
            "ListTagsForResource" => self.list_tags_for_resource(req),
            _ => Err(AwsServiceError::action_not_implemented(
                self.service_name(),
                action,
            )),
        }
    }
}

// ---------------------------------------------------------------------------
// Free helpers
// ---------------------------------------------------------------------------

fn ok(v: Value) -> Result<AwsResponse, AwsServiceError> {
    Ok(AwsResponse::json_value(StatusCode::OK, v))
}

fn ts(dt: DateTime<Utc>) -> Value {
    json!(dt.timestamp_millis() as f64 / 1000.0)
}

fn err(code: &str, msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, code, msg)
}

fn body(req: &AwsRequest) -> Value {
    serde_json::from_slice(&req.body).unwrap_or(Value::Null)
}

fn str_field(b: &Value, key: &str) -> Option<String> {
    b.get(key).and_then(|v| v.as_str()).map(str::to_string)
}

fn caller_arn(req: &AwsRequest) -> String {
    req.principal
        .as_ref()
        .map(|p| p.arn.clone())
        .unwrap_or_else(|| format!("arn:aws:iam::{}:root", req.account_id))
}

/// SHA-1 hex of the given bytes.
fn sha1_hex(data: &[u8]) -> String {
    use sha1::{Digest, Sha1};
    let mut h = Sha1::new();
    h.update(data);
    format!("{:x}", h.finalize())
}

/// git-style blob id: SHA-1 over `blob <len>\0<content>`.
fn blob_id_for(content: &[u8]) -> String {
    let header = format!("blob {}\0", content.len());
    let mut buf = header.into_bytes();
    buf.extend_from_slice(content);
    sha1_hex(&buf)
}

/// git-style tree id: SHA-1 over the sorted `mode\tpath\tblob` entries.
fn tree_id_for(entries: &std::collections::BTreeMap<String, FileEntry>) -> String {
    let mut buf = String::new();
    for (path, e) in entries {
        buf.push_str(&format!("{}\t{}\t{}\n", e.mode, path, e.blob_id));
    }
    sha1_hex(buf.as_bytes())
}

fn new_uuid() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// A 40-char hex id derived from a fresh UUID (used for commit ids and
/// pull-request revision ids so each mint is unique like a real committer
/// timestamp would make it).
fn fresh_object_id() -> String {
    sha1_hex(uuid::Uuid::new_v4().as_bytes())
}

fn is_object_id(s: &str) -> bool {
    s.len() == 40 && s.chars().all(|c| c.is_ascii_hexdigit())
}

fn file_mode_to_git(mode: &str) -> &'static str {
    match mode {
        "EXECUTABLE" => "100755",
        "SYMLINK" => "120000",
        _ => "100644",
    }
}

fn git_mode_to_file(mode: &str) -> &'static str {
    match mode {
        "100755" => "EXECUTABLE",
        "120000" => "SYMLINK",
        _ => "NORMAL",
    }
}

/// Require a present, non-empty string member, erroring with `err_code` when
/// absent or empty.
fn require(b: &Value, key: &str, err_code: &str) -> Result<String, AwsServiceError> {
    match str_field(b, key) {
        Some(v) if !v.is_empty() => Ok(v),
        _ => Err(err(err_code, format!("{key} is required."))),
    }
}

/// Require and validate a `RepositoryName` member.
fn require_repository_name(b: &Value) -> Result<String, AwsServiceError> {
    let v = require(b, "repositoryName", "RepositoryNameRequiredException")?;
    if validate::valid_repository_name(&v) {
        Ok(v)
    } else {
        Err(err(
            "InvalidRepositoryNameException",
            "The repository name is not valid. Repository names must be between 1 and 100 characters and can only contain letters, numbers, periods, underscores, and dashes.",
        ))
    }
}

/// Validate an enum member if present, else return the declared `err_code`.
fn check_enum(b: &Value, key: &str, set: &[&str], err_code: &str) -> Result<(), AwsServiceError> {
    if let Some(v) = str_field(b, key) {
        if !validate::is_enum(set, &v) {
            return Err(err(
                err_code,
                format!("{v} is not a valid value for {key}."),
            ));
        }
    }
    Ok(())
}

/// A single ARN component: `arn:aws:codecommit:<region>:<account>:<name>`.
fn repo_arn(region: &str, account: &str, name: &str) -> String {
    format!("arn:aws:codecommit:{region}:{account}:{name}")
}

/// Validate a CodeCommit resource ARN (`arn:aws:codecommit:...`).
fn is_codecommit_arn(s: &str) -> bool {
    let parts: Vec<&str> = s.splitn(6, ':').collect();
    parts.len() == 6 && parts[0] == "arn" && parts[2] == "codecommit"
}

/// The repository name embedded in a CodeCommit resource ARN, if any.
fn repo_name_from_arn(s: &str) -> Option<String> {
    let parts: Vec<&str> = s.splitn(6, ':').collect();
    if parts.len() == 6 && parts[0] == "arn" && parts[2] == "codecommit" {
        Some(parts[5].to_string())
    } else {
        None
    }
}

/// Strip a comment's private `_ctx*` context members before returning it.
fn public_comment(stored: &Value) -> Value {
    let mut c = stored.clone();
    if let Some(obj) = c.as_object_mut() {
        obj.retain(|k, _| !k.starts_with("_ctx"));
    }
    c
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

impl CodeCommitService {
    fn account(&self, req: &AwsRequest) -> String {
        req.account_id.clone()
    }

    // ----- Repositories -----

    fn create_repository(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = require_repository_name(&b)?;
        if let Some(d) = str_field(&b, "repositoryDescription") {
            if d.chars().count() > 1000 {
                return Err(err(
                    "InvalidRepositoryDescriptionException",
                    "The repository description is not valid. Length must not exceed 1000 characters.",
                ));
            }
        }
        if let Some(k) = str_field(&b, "kmsKeyId") {
            if !validate::valid_kms_key_id(&k) {
                return Err(err(
                    "EncryptionKeyInvalidIdException",
                    "The Amazon Web Services KMS key is not valid.",
                ));
            }
        }
        let account = self.account(req);
        let arn = repo_arn(&req.region, &account, &name);
        let now = Utc::now();
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        if st.repositories.contains_key(&name) {
            return Err(err(
                "RepositoryNameExistsException",
                format!("Repository named {name} already exists."),
            ));
        }
        let kms = str_field(&b, "kmsKeyId").unwrap_or_else(|| {
            format!("arn:aws:kms:{}:{}:key/{}", req.region, account, new_uuid())
        });
        let mut metadata = Map::new();
        metadata.insert("accountId".into(), json!(account));
        metadata.insert("repositoryId".into(), json!(new_uuid()));
        metadata.insert("repositoryName".into(), json!(name));
        if let Some(d) = str_field(&b, "repositoryDescription") {
            metadata.insert("repositoryDescription".into(), json!(d));
        }
        metadata.insert("lastModifiedDate".into(), ts(now));
        metadata.insert("creationDate".into(), ts(now));
        metadata.insert(
            "cloneUrlHttp".into(),
            json!(format!(
                "https://git-codecommit.{}.amazonaws.com/v1/repos/{}",
                req.region, name
            )),
        );
        metadata.insert(
            "cloneUrlSsh".into(),
            json!(format!(
                "ssh://git-codecommit.{}.amazonaws.com/v1/repos/{}",
                req.region, name
            )),
        );
        metadata.insert("Arn".into(), json!(arn));
        metadata.insert("kmsKeyId".into(), json!(kms));
        let metadata = Value::Object(metadata);
        let repo = Repo {
            metadata: metadata.clone(),
            ..Repo::default()
        };
        st.repositories.insert(name.clone(), repo);
        st.repository_order.push(name.clone());
        if let Some(tags) = b.get("tags").and_then(Value::as_object) {
            let map = tags
                .iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect();
            st.tags.insert(arn, map);
        }
        ok(json!({ "repositoryMetadata": metadata }))
    }

    fn get_repository(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = require_repository_name(&b)?;
        let account = self.account(req);
        let guard = self.state.read();
        let st = guard.get(&account);
        match st.and_then(|s| s.repositories.get(&name)) {
            Some(repo) => ok(json!({ "repositoryMetadata": repo.metadata })),
            None => Err(repo_not_found(&name)),
        }
    }

    fn delete_repository(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = require_repository_name(&b)?;
        let account = self.account(req);
        let arn = repo_arn(&req.region, &account, &name);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        // Removing the repository drops its `associated_templates`, which is the
        // sole record of approval-rule-template associations, so no template
        // association can outlive the repository.
        let repository_id = st.repositories.remove(&name).and_then(|r| {
            r.metadata
                .get("repositoryId")
                .and_then(Value::as_str)
                .map(str::to_string)
        });
        st.repository_order.retain(|n| n != &name);
        st.tags.remove(&arn);
        match repository_id {
            Some(id) => ok(json!({ "repositoryId": id })),
            None => ok(json!({})),
        }
    }

    fn list_repositories(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        check_enum(&b, "sortBy", validate::SORT_BY, "InvalidSortByException")?;
        check_enum(&b, "order", validate::ORDER, "InvalidOrderException")?;
        let account = self.account(req);
        let guard = self.state.read();
        let st = guard.get(&account);
        let mut pairs = Vec::new();
        if let Some(s) = st {
            let mut names: Vec<String> = s.repository_order.clone();
            if str_field(&b, "sortBy").as_deref() == Some("lastModifiedDate") {
                names.sort_by_key(|n| {
                    s.repositories
                        .get(n)
                        .and_then(|r| r.metadata.get("lastModifiedDate"))
                        .and_then(Value::as_f64)
                        .map(|f| (f * 1000.0) as i64)
                        .unwrap_or(0)
                });
            } else {
                names.sort();
            }
            if str_field(&b, "order").as_deref() == Some("descending") {
                names.reverse();
            }
            for n in names {
                if let Some(r) = s.repositories.get(&n) {
                    pairs.push(json!({
                        "repositoryName": n,
                        "repositoryId": r.metadata.get("repositoryId").cloned().unwrap_or(Value::Null),
                    }));
                }
            }
        }
        ok(json!({ "repositories": pairs }))
    }

    fn batch_get_repositories(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let names = b
            .get("repositoryNames")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                err(
                    "RepositoryNamesRequiredException",
                    "A repository names object is required.",
                )
            })?;
        if names.len() > 100 {
            return Err(err(
                "MaximumRepositoryNamesExceededException",
                "The maximum number of repository names for a request (100) has been exceeded.",
            ));
        }
        let account = self.account(req);
        let guard = self.state.read();
        let st = guard.get(&account);
        let mut found = Vec::new();
        let mut not_found = Vec::new();
        for n in names {
            let Some(name) = n.as_str() else { continue };
            match st.and_then(|s| s.repositories.get(name)) {
                Some(r) => found.push(r.metadata.clone()),
                None => not_found.push(json!(name)),
            }
        }
        ok(json!({
            "repositories": found,
            "repositoriesNotFound": not_found,
            "errors": [],
        }))
    }

    fn update_repository_description(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = require_repository_name(&b)?;
        if let Some(d) = str_field(&b, "repositoryDescription") {
            if d.chars().count() > 1000 {
                return Err(err(
                    "InvalidRepositoryDescriptionException",
                    "The repository description is not valid.",
                ));
            }
        }
        let account = self.account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let repo = st
            .repositories
            .get_mut(&name)
            .ok_or_else(|| repo_not_found(&name))?;
        if let Some(obj) = repo.metadata.as_object_mut() {
            match str_field(&b, "repositoryDescription") {
                Some(d) => {
                    obj.insert("repositoryDescription".into(), json!(d));
                }
                None => {
                    obj.remove("repositoryDescription");
                }
            }
            obj.insert("lastModifiedDate".into(), ts(Utc::now()));
        }
        ok(json!({}))
    }

    fn update_repository_name(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let old = require(&b, "oldName", "RepositoryNameRequiredException")?;
        let new = require(&b, "newName", "RepositoryNameRequiredException")?;
        if !validate::valid_repository_name(&old) || !validate::valid_repository_name(&new) {
            return Err(err(
                "InvalidRepositoryNameException",
                "The repository name is not valid.",
            ));
        }
        let account = self.account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        if !st.repositories.contains_key(&old) {
            return Err(repo_not_found(&old));
        }
        if old != new && st.repositories.contains_key(&new) {
            return Err(err(
                "RepositoryNameExistsException",
                format!("Repository named {new} already exists."),
            ));
        }
        if old != new {
            let mut repo = st.repositories.remove(&old).unwrap();
            let arn = repo_arn(&req.region, &account, &new);
            if let Some(obj) = repo.metadata.as_object_mut() {
                obj.insert("repositoryName".into(), json!(new));
                obj.insert("Arn".into(), json!(arn.clone()));
                obj.insert(
                    "cloneUrlHttp".into(),
                    json!(format!(
                        "https://git-codecommit.{}.amazonaws.com/v1/repos/{}",
                        req.region, new
                    )),
                );
                obj.insert(
                    "cloneUrlSsh".into(),
                    json!(format!(
                        "ssh://git-codecommit.{}.amazonaws.com/v1/repos/{}",
                        req.region, new
                    )),
                );
                obj.insert("lastModifiedDate".into(), ts(Utc::now()));
            }
            let old_arn = repo_arn(&req.region, &account, &old);
            if let Some(tags) = st.tags.remove(&old_arn) {
                st.tags.insert(arn, tags);
            }
            st.repositories.insert(new.clone(), repo);
            for n in &mut st.repository_order {
                if n == &old {
                    *n = new.clone();
                }
            }
        }
        ok(json!({}))
    }

    fn update_repository_encryption_key(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = require_repository_name(&b)?;
        let kms = require(&b, "kmsKeyId", "EncryptionKeyRequiredException")?;
        if !validate::valid_kms_key_id(&kms) {
            return Err(err(
                "EncryptionKeyInvalidIdException",
                "The Amazon Web Services KMS key is not valid.",
            ));
        }
        let account = self.account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let repo = st
            .repositories
            .get_mut(&name)
            .ok_or_else(|| repo_not_found(&name))?;
        let repository_id = repo
            .metadata
            .get("repositoryId")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let original = repo
            .metadata
            .get("kmsKeyId")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        if let Some(obj) = repo.metadata.as_object_mut() {
            obj.insert("kmsKeyId".into(), json!(kms));
            obj.insert("lastModifiedDate".into(), ts(Utc::now()));
        }
        ok(json!({
            "repositoryId": repository_id,
            "kmsKeyId": kms,
            "originalKmsKeyId": original,
        }))
    }

    // ----- Branches -----

    fn create_branch(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = require_repository_name(&b)?;
        let branch = require(&b, "branchName", "BranchNameRequiredException")?;
        if !validate::valid_branch_name(&branch) {
            return Err(err(
                "InvalidBranchNameException",
                "The branch name is not valid.",
            ));
        }
        let commit = require(&b, "commitId", "CommitIdRequiredException")?;
        if !is_object_id(&commit) {
            return Err(err(
                "InvalidCommitIdException",
                "The commit ID is not valid.",
            ));
        }
        let account = self.account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let repo = st
            .repositories
            .get_mut(&name)
            .ok_or_else(|| repo_not_found(&name))?;
        if repo.branches.contains_key(&branch) {
            return Err(err(
                "BranchNameExistsException",
                format!("Branch name {branch} already exists."),
            ));
        }
        if !repo.commits.contains_key(&commit) {
            return Err(err(
                "CommitDoesNotExistException",
                "The specified commit does not exist.",
            ));
        }
        repo.branches.insert(branch, commit);
        ok(json!({}))
    }

    fn delete_branch(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = require_repository_name(&b)?;
        let branch = require(&b, "branchName", "BranchNameRequiredException")?;
        if !validate::valid_branch_name(&branch) {
            return Err(err(
                "InvalidBranchNameException",
                "The branch name is not valid.",
            ));
        }
        let account = self.account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let repo = st
            .repositories
            .get_mut(&name)
            .ok_or_else(|| repo_not_found(&name))?;
        let default = repo
            .metadata
            .get("defaultBranch")
            .and_then(Value::as_str)
            .map(str::to_string);
        if default.as_deref() == Some(branch.as_str()) {
            return Err(err(
                "DefaultBranchCannotBeDeletedException",
                "The default branch for a repository cannot be deleted.",
            ));
        }
        match repo.branches.remove(&branch) {
            Some(commit) => ok(json!({
                "deletedBranch": { "branchName": branch, "commitId": commit }
            })),
            None => ok(json!({})),
        }
    }

    fn get_branch(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = str_field(&b, "repositoryName").ok_or_else(|| {
            err(
                "RepositoryNameRequiredException",
                "A repository name is required.",
            )
        })?;
        if !validate::valid_repository_name(&name) {
            return Err(err(
                "InvalidRepositoryNameException",
                "The repository name is not valid.",
            ));
        }
        let branch = require(&b, "branchName", "BranchNameRequiredException")?;
        if !validate::valid_branch_name(&branch) {
            return Err(err(
                "InvalidBranchNameException",
                "The branch name is not valid.",
            ));
        }
        let account = self.account(req);
        let guard = self.state.read();
        let st = guard.get(&account);
        let repo = st
            .and_then(|s| s.repositories.get(&name))
            .ok_or_else(|| repo_not_found(&name))?;
        match repo.branches.get(&branch) {
            Some(commit) => ok(json!({
                "branch": { "branchName": branch, "commitId": commit }
            })),
            None => Err(err(
                "BranchDoesNotExistException",
                "The specified branch does not exist.",
            )),
        }
    }

    fn list_branches(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = require_repository_name(&b)?;
        let account = self.account(req);
        let guard = self.state.read();
        let st = guard.get(&account);
        let repo = st
            .and_then(|s| s.repositories.get(&name))
            .ok_or_else(|| repo_not_found(&name))?;
        let branches: Vec<Value> = repo.branches.keys().map(|k| json!(k)).collect();
        ok(json!({ "branches": branches }))
    }

    fn update_default_branch(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = require_repository_name(&b)?;
        let branch = require(&b, "defaultBranchName", "BranchNameRequiredException")?;
        if !validate::valid_branch_name(&branch) {
            return Err(err(
                "InvalidBranchNameException",
                "The branch name is not valid.",
            ));
        }
        let account = self.account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let repo = st
            .repositories
            .get_mut(&name)
            .ok_or_else(|| repo_not_found(&name))?;
        if !repo.branches.contains_key(&branch) {
            return Err(err(
                "BranchDoesNotExistException",
                "The specified branch does not exist.",
            ));
        }
        if let Some(obj) = repo.metadata.as_object_mut() {
            obj.insert("defaultBranch".into(), json!(branch));
            obj.insert("lastModifiedDate".into(), ts(Utc::now()));
        }
        ok(json!({}))
    }

    // ----- Files / blobs / commits -----

    fn put_file(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = require_repository_name(&b)?;
        let branch = require(&b, "branchName", "BranchNameRequiredException")?;
        if !validate::valid_branch_name(&branch) {
            return Err(err(
                "InvalidBranchNameException",
                "The branch name is not valid.",
            ));
        }
        let path = require(&b, "filePath", "PathRequiredException")?;
        let content_b64 = require(&b, "fileContent", "FileContentRequiredException")?;
        check_enum(
            &b,
            "fileMode",
            validate::FILE_MODE,
            "InvalidFileModeException",
        )?;
        let mode = str_field(&b, "fileMode").unwrap_or_else(|| "NORMAL".to_string());
        let content = base64::engine::general_purpose::STANDARD
            .decode(content_b64.as_bytes())
            .unwrap_or_else(|_| content_b64.clone().into_bytes());
        let account = self.account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let repo = st
            .repositories
            .get_mut(&name)
            .ok_or_else(|| repo_not_found(&name))?;

        let parent_commit = str_field(&b, "parentCommitId");
        let mut tree = if let Some(tip) = repo.branches.get(&branch).cloned() {
            // Existing branch: parent commit id must match the tip.
            match &parent_commit {
                None => {
                    return Err(err(
                        "ParentCommitIdRequiredException",
                        "A parent commit ID is required.",
                    ))
                }
                Some(p) if *p != tip => {
                    return Err(err(
                        "ParentCommitIdOutdatedException",
                        "The parent commit ID is not the tip of the branch.",
                    ))
                }
                _ => {}
            }
            repo.trees.get(&tip).cloned().unwrap_or_default()
        } else {
            std::collections::BTreeMap::new()
        };

        let bid = blob_id_for(&content);
        // SameFileContentException when the path already holds this exact blob.
        if tree.get(&path).map(|e| &e.blob_id) == Some(&bid)
            && tree.get(&path).map(|e| e.mode.as_str()) == Some(file_mode_to_git(&mode))
        {
            return Err(err(
                "SameFileContentException",
                "The file was not added or updated because the content of the file is exactly the same as the content of that file in the repository and branch that you specified.",
            ));
        }
        repo.blobs.insert(bid.clone(), content_b64.clone());
        tree.insert(
            path.clone(),
            FileEntry {
                blob_id: bid.clone(),
                mode: file_mode_to_git(&mode).to_string(),
            },
        );
        let tree_id = tree_id_for(&tree);
        let commit_id = fresh_object_id();
        let parents: Vec<String> = parent_commit.into_iter().collect();
        let commit = build_commit(&commit_id, &tree_id, &parents, &b, req);
        repo.commits.insert(commit_id.clone(), commit);
        repo.trees.insert(commit_id.clone(), tree);
        repo.branches.insert(branch.clone(), commit_id.clone());
        set_default_if_absent(repo);
        // Advancing this branch invalidates approvals on any open PR sourced from it.
        refresh_source_revisions(st, &name, &branch);
        ok(json!({ "commitId": commit_id, "blobId": bid, "treeId": tree_id }))
    }

    fn delete_file(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = require_repository_name(&b)?;
        let branch = require(&b, "branchName", "BranchNameRequiredException")?;
        if !validate::valid_branch_name(&branch) {
            return Err(err(
                "InvalidBranchNameException",
                "The branch name is not valid.",
            ));
        }
        let path = require(&b, "filePath", "PathRequiredException")?;
        let parent = require(&b, "parentCommitId", "ParentCommitIdRequiredException")?;
        let account = self.account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let repo = st
            .repositories
            .get_mut(&name)
            .ok_or_else(|| repo_not_found(&name))?;
        let tip = repo.branches.get(&branch).cloned().ok_or_else(|| {
            err(
                "BranchDoesNotExistException",
                "The specified branch does not exist.",
            )
        })?;
        if parent != tip {
            return Err(err(
                "ParentCommitIdOutdatedException",
                "The parent commit ID is not the tip of the branch.",
            ));
        }
        let mut tree = repo.trees.get(&tip).cloned().unwrap_or_default();
        if tree.remove(&path).is_none() {
            return Err(err(
                "FileDoesNotExistException",
                "The specified file does not exist.",
            ));
        }
        let tree_id = tree_id_for(&tree);
        let commit_id = fresh_object_id();
        let commit = build_commit(&commit_id, &tree_id, &[tip], &b, req);
        repo.commits.insert(commit_id.clone(), commit);
        repo.trees.insert(commit_id.clone(), tree);
        repo.branches.insert(branch.clone(), commit_id.clone());
        refresh_source_revisions(st, &name, &branch);
        ok(json!({
            "commitId": commit_id,
            "blobId": blob_id_for(&[]),
            "treeId": tree_id,
            "filePath": path,
        }))
    }

    fn create_commit(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = require_repository_name(&b)?;
        let branch = require(&b, "branchName", "BranchNameRequiredException")?;
        if !validate::valid_branch_name(&branch) {
            return Err(err(
                "InvalidBranchNameException",
                "The branch name is not valid.",
            ));
        }
        let account = self.account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let repo = st
            .repositories
            .get_mut(&name)
            .ok_or_else(|| repo_not_found(&name))?;

        let parent_commit = str_field(&b, "parentCommitId");
        let mut tree = if let Some(tip) = repo.branches.get(&branch).cloned() {
            match &parent_commit {
                None => {
                    return Err(err(
                        "ParentCommitIdRequiredException",
                        "A parent commit ID is required.",
                    ))
                }
                Some(p) if *p != tip => {
                    return Err(err(
                        "ParentCommitIdOutdatedException",
                        "The parent commit ID is not the tip of the branch.",
                    ))
                }
                _ => {}
            }
            repo.trees.get(&tip).cloned().unwrap_or_default()
        } else {
            std::collections::BTreeMap::new()
        };

        let mut files_added = Vec::new();
        let mut files_updated = Vec::new();
        let mut files_deleted = Vec::new();

        if let Some(puts) = b.get("putFiles").and_then(Value::as_array) {
            for pf in puts {
                let Some(path) = pf.get("filePath").and_then(Value::as_str) else {
                    return Err(err("PathRequiredException", "A file path is required."));
                };
                let mode = pf
                    .get("fileMode")
                    .and_then(Value::as_str)
                    .unwrap_or("NORMAL");
                let content_b64 = pf
                    .get("fileContent")
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .unwrap_or_default();
                let content = base64::engine::general_purpose::STANDARD
                    .decode(content_b64.as_bytes())
                    .unwrap_or_else(|_| content_b64.clone().into_bytes());
                let bid = blob_id_for(&content);
                repo.blobs.insert(bid.clone(), content_b64.clone());
                let existed = tree.contains_key(path);
                tree.insert(
                    path.to_string(),
                    FileEntry {
                        blob_id: bid.clone(),
                        mode: file_mode_to_git(mode).to_string(),
                    },
                );
                let meta = json!({ "absolutePath": path, "blobId": bid, "fileMode": git_mode_to_file(file_mode_to_git(mode)) });
                if existed {
                    files_updated.push(meta);
                } else {
                    files_added.push(meta);
                }
            }
        }
        if let Some(dels) = b.get("deleteFiles").and_then(Value::as_array) {
            for df in dels {
                if let Some(path) = df.get("filePath").and_then(Value::as_str) {
                    if tree.remove(path).is_some() {
                        files_deleted.push(json!({ "absolutePath": path }));
                    }
                }
            }
        }
        if let Some(modes) = b.get("setFileModes").and_then(Value::as_array) {
            for sm in modes {
                if let (Some(path), Some(mode)) = (
                    sm.get("filePath").and_then(Value::as_str),
                    sm.get("fileMode").and_then(Value::as_str),
                ) {
                    if let Some(e) = tree.get_mut(path) {
                        e.mode = file_mode_to_git(mode).to_string();
                    }
                }
            }
        }

        if files_added.is_empty()
            && files_updated.is_empty()
            && files_deleted.is_empty()
            && parent_commit.is_some()
        {
            return Err(err(
                "NoChangeException",
                "The commit cannot be created because no changes will be made to the repository as a result of this commit.",
            ));
        }

        let tree_id = tree_id_for(&tree);
        let commit_id = fresh_object_id();
        let parents: Vec<String> = parent_commit.into_iter().collect();
        let commit = build_commit(&commit_id, &tree_id, &parents, &b, req);
        repo.commits.insert(commit_id.clone(), commit);
        repo.trees.insert(commit_id.clone(), tree);
        repo.branches.insert(branch.clone(), commit_id.clone());
        set_default_if_absent(repo);
        refresh_source_revisions(st, &name, &branch);
        ok(json!({
            "commitId": commit_id,
            "treeId": tree_id,
            "filesAdded": files_added,
            "filesUpdated": files_updated,
            "filesDeleted": files_deleted,
        }))
    }

    fn get_commit(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = require_repository_name(&b)?;
        let commit = require(&b, "commitId", "CommitIdRequiredException")?;
        if !is_object_id(&commit) {
            return Err(err(
                "InvalidCommitIdException",
                "The commit ID is not valid.",
            ));
        }
        let account = self.account(req);
        let guard = self.state.read();
        let st = guard.get(&account);
        let repo = st
            .and_then(|s| s.repositories.get(&name))
            .ok_or_else(|| repo_not_found(&name))?;
        match repo.commits.get(&commit) {
            Some(c) => ok(json!({ "commit": c })),
            None => Err(err(
                "CommitIdDoesNotExistException",
                "The specified commit ID does not exist.",
            )),
        }
    }

    fn batch_get_commits(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = require_repository_name(&b)?;
        let ids = b
            .get("commitIds")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                err(
                    "CommitIdsListRequiredException",
                    "A list of commit IDs is required.",
                )
            })?;
        let account = self.account(req);
        let guard = self.state.read();
        let st = guard.get(&account);
        let repo = st
            .and_then(|s| s.repositories.get(&name))
            .ok_or_else(|| repo_not_found(&name))?;
        let mut commits = Vec::new();
        let mut errors = Vec::new();
        for id in ids {
            let Some(cid) = id.as_str() else { continue };
            match repo.commits.get(cid) {
                Some(c) => commits.push(c.clone()),
                None => errors.push(json!({
                    "commitId": cid,
                    "errorCode": "CommitDoesNotExist",
                    "errorMessage": "The specified commit does not exist.",
                })),
            }
        }
        ok(json!({ "commits": commits, "errors": errors }))
    }

    fn get_blob(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = require_repository_name(&b)?;
        let blob = require(&b, "blobId", "BlobIdRequiredException")?;
        if !is_object_id(&blob) {
            return Err(err("InvalidBlobIdException", "The blob ID is not valid."));
        }
        let account = self.account(req);
        let guard = self.state.read();
        let st = guard.get(&account);
        let repo = st
            .and_then(|s| s.repositories.get(&name))
            .ok_or_else(|| repo_not_found(&name))?;
        match repo.blobs.get(&blob) {
            Some(content_b64) => ok(json!({ "content": content_b64 })),
            None => Err(err(
                "BlobIdDoesNotExistException",
                "The specified blob does not exist.",
            )),
        }
    }

    fn get_file(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = require_repository_name(&b)?;
        let path = require(&b, "filePath", "PathRequiredException")?;
        let account = self.account(req);
        let guard = self.state.read();
        let st = guard.get(&account);
        let repo = st
            .and_then(|s| s.repositories.get(&name))
            .ok_or_else(|| repo_not_found(&name))?;
        let commit_id = resolve_commit(repo, &b, "commitSpecifier")?;
        let tree = repo.trees.get(&commit_id).cloned().unwrap_or_default();
        let entry = tree.get(&path).ok_or_else(|| {
            err(
                "FileDoesNotExistException",
                "The specified file does not exist.",
            )
        })?;
        let content_b64 = repo.blobs.get(&entry.blob_id).cloned().unwrap_or_default();
        let size = base64::engine::general_purpose::STANDARD
            .decode(content_b64.as_bytes())
            .map(|d| d.len())
            .unwrap_or(content_b64.len());
        ok(json!({
            "commitId": commit_id,
            "blobId": entry.blob_id,
            "filePath": path,
            "fileMode": git_mode_to_file(&entry.mode),
            "fileSize": size,
            "fileContent": content_b64,
        }))
    }

    fn get_folder(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = require_repository_name(&b)?;
        let folder = require(&b, "folderPath", "PathRequiredException")?;
        let account = self.account(req);
        let guard = self.state.read();
        let st = guard.get(&account);
        let repo = st
            .and_then(|s| s.repositories.get(&name))
            .ok_or_else(|| repo_not_found(&name))?;
        let commit_id = resolve_commit(repo, &b, "commitSpecifier")?;
        let tree = repo.trees.get(&commit_id).cloned().unwrap_or_default();
        let prefix = if folder == "/" || folder.is_empty() {
            String::new()
        } else {
            format!("{}/", folder.trim_end_matches('/'))
        };
        let tree_id = repo
            .commits
            .get(&commit_id)
            .and_then(|c| c.get("treeId"))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let mut files = Vec::new();
        let mut subfolders = std::collections::BTreeSet::new();
        let mut symlinks = Vec::new();
        for (path, entry) in &tree {
            if !path.starts_with(&prefix) {
                continue;
            }
            let rel = &path[prefix.len()..];
            if let Some(idx) = rel.find('/') {
                subfolders.insert(rel[..idx].to_string());
            } else {
                let file = json!({
                    "blobId": entry.blob_id,
                    "absolutePath": path,
                    "relativePath": rel,
                    "fileMode": git_mode_to_file(&entry.mode),
                });
                if entry.mode == "120000" {
                    symlinks.push(file);
                } else {
                    files.push(file);
                }
            }
        }
        let sub: Vec<Value> = subfolders
            .into_iter()
            .map(|s| {
                json!({
                    "treeId": tree_id,
                    "absolutePath": format!("{prefix}{s}"),
                    "relativePath": s,
                })
            })
            .collect();
        ok(json!({
            "commitId": commit_id,
            "folderPath": folder,
            "treeId": tree_id,
            "subFolders": sub,
            "files": files,
            "symbolicLinks": symlinks,
            "subModules": [],
        }))
    }

    fn get_differences(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = require_repository_name(&b)?;
        let account = self.account(req);
        let guard = self.state.read();
        let st = guard.get(&account);
        let repo = st
            .and_then(|s| s.repositories.get(&name))
            .ok_or_else(|| repo_not_found(&name))?;
        let after = resolve_commit(repo, &b, "afterCommitSpecifier")?;
        let after_tree = repo.trees.get(&after).cloned().unwrap_or_default();
        let before_tree = if b.get("beforeCommitSpecifier").is_some() {
            let before = resolve_commit(repo, &b, "beforeCommitSpecifier")?;
            repo.trees.get(&before).cloned().unwrap_or_default()
        } else {
            std::collections::BTreeMap::new()
        };
        let mut diffs = Vec::new();
        for (path, entry) in &after_tree {
            match before_tree.get(path) {
                None => diffs.push(json!({
                    "afterBlob": { "blobId": entry.blob_id, "path": path, "mode": entry.mode },
                    "changeType": "A",
                })),
                Some(bentry) if bentry.blob_id != entry.blob_id => diffs.push(json!({
                    "beforeBlob": { "blobId": bentry.blob_id, "path": path, "mode": bentry.mode },
                    "afterBlob": { "blobId": entry.blob_id, "path": path, "mode": entry.mode },
                    "changeType": "M",
                })),
                _ => {}
            }
        }
        for (path, bentry) in &before_tree {
            if !after_tree.contains_key(path) {
                diffs.push(json!({
                    "beforeBlob": { "blobId": bentry.blob_id, "path": path, "mode": bentry.mode },
                    "changeType": "D",
                }));
            }
        }
        ok(json!({ "differences": diffs }))
    }

    fn list_file_commit_history(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = require_repository_name(&b)?;
        let _path = require(&b, "filePath", "PathRequiredException")?;
        let account = self.account(req);
        let guard = self.state.read();
        let st = guard.get(&account);
        let repo = st
            .and_then(|s| s.repositories.get(&name))
            .ok_or_else(|| repo_not_found(&name))?;
        let commit_id = resolve_commit(repo, &b, "commitSpecifier")?;
        // Walk the first-parent chain from the resolved commit.
        let mut dag = Vec::new();
        let mut cur = Some(commit_id);
        while let Some(cid) = cur {
            let Some(c) = repo.commits.get(&cid) else {
                break;
            };
            let parents = c
                .get("parents")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            dag.push(json!({
                "commit": cid,
                "parents": parents.iter().filter_map(|p| p.as_str()).collect::<Vec<_>>(),
            }));
            cur = parents.first().and_then(Value::as_str).map(str::to_string);
        }
        ok(json!({ "revisionDag": dag }))
    }

    // ----- Merges -----

    fn merge_branches_fast_forward(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = require_repository_name(&b)?;
        require(&b, "sourceCommitSpecifier", "CommitRequiredException")?;
        require(&b, "destinationCommitSpecifier", "CommitRequiredException")?;
        let account = self.account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let repo = st
            .repositories
            .get_mut(&name)
            .ok_or_else(|| repo_not_found(&name))?;
        let source = resolve_commit(repo, &b, "sourceCommitSpecifier")?;
        let dest = resolve_commit(repo, &b, "destinationCommitSpecifier")?;
        if !is_ancestor(repo, &dest, &source) {
            return Err(err(
                "ManualMergeRequiredException",
                "The fast-forward merge cannot be performed because the destination is not an ancestor of the source.",
            ));
        }
        let tree_id = repo
            .commits
            .get(&source)
            .and_then(|c| c.get("treeId"))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        if let Some(target) = str_field(&b, "targetBranch") {
            repo.branches.insert(target.clone(), source.clone());
            refresh_source_revisions(st, &name, &target);
        }
        ok(json!({ "commitId": source, "treeId": tree_id }))
    }

    fn merge_branches_squash(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        self.merge_branches_create(req, "SQUASH_MERGE")
    }

    fn merge_branches_three_way(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        self.merge_branches_create(req, "THREE_WAY_MERGE")
    }

    fn merge_branches_create(
        &self,
        req: &AwsRequest,
        merge_option: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = require_repository_name(&b)?;
        require(&b, "sourceCommitSpecifier", "CommitRequiredException")?;
        require(&b, "destinationCommitSpecifier", "CommitRequiredException")?;
        validate_merge_conflict_enums(&b)?;
        let account = self.account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let repo = st
            .repositories
            .get_mut(&name)
            .ok_or_else(|| repo_not_found(&name))?;
        let source = resolve_commit(repo, &b, "sourceCommitSpecifier")?;
        let dest = resolve_commit(repo, &b, "destinationCommitSpecifier")?;
        let strategy = str_field(&b, "conflictResolutionStrategy").unwrap_or_default();
        let merged = merge_trees(repo, &source, &dest, &strategy).ok_or_else(|| {
            err(
                "ManualMergeRequiredException",
                "The merge cannot be completed because there are merge conflicts that must be resolved manually.",
            )
        })?;
        let tree_id = tree_id_for(&merged);
        let commit_id = fresh_object_id();
        let parents = merge_parents(merge_option, &dest, &source);
        let commit = build_commit(&commit_id, &tree_id, &parents, &b, req);
        repo.commits.insert(commit_id.clone(), commit);
        repo.trees.insert(commit_id.clone(), merged);
        if let Some(target) = str_field(&b, "targetBranch") {
            repo.branches.insert(target.clone(), commit_id.clone());
            refresh_source_revisions(st, &name, &target);
        }
        ok(json!({ "commitId": commit_id, "treeId": tree_id }))
    }

    fn create_unreferenced_merge_commit(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = require_repository_name(&b)?;
        require(&b, "sourceCommitSpecifier", "CommitRequiredException")?;
        require(&b, "destinationCommitSpecifier", "CommitRequiredException")?;
        let merge_option = require(&b, "mergeOption", "MergeOptionRequiredException")?;
        if !validate::is_enum(validate::MERGE_OPTION, &merge_option) {
            return Err(err(
                "InvalidMergeOptionException",
                "The merge option is not valid.",
            ));
        }
        validate_merge_conflict_enums(&b)?;
        let account = self.account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let repo = st
            .repositories
            .get_mut(&name)
            .ok_or_else(|| repo_not_found(&name))?;
        let source = resolve_commit(repo, &b, "sourceCommitSpecifier")?;
        let dest = resolve_commit(repo, &b, "destinationCommitSpecifier")?;
        let strategy = str_field(&b, "conflictResolutionStrategy").unwrap_or_default();
        let merged = merge_trees(repo, &source, &dest, &strategy).ok_or_else(|| {
            err(
                "ManualMergeRequiredException",
                "The merge cannot be completed because there are merge conflicts that must be resolved manually.",
            )
        })?;
        let tree_id = tree_id_for(&merged);
        let commit_id = fresh_object_id();
        let parents = merge_parents(&merge_option, &dest, &source);
        let commit = build_commit(&commit_id, &tree_id, &parents, &b, req);
        repo.commits.insert(commit_id.clone(), commit);
        repo.trees.insert(commit_id.clone(), merged);
        ok(json!({ "commitId": commit_id, "treeId": tree_id }))
    }

    fn get_merge_commit(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = require_repository_name(&b)?;
        require(&b, "sourceCommitSpecifier", "CommitRequiredException")?;
        require(&b, "destinationCommitSpecifier", "CommitRequiredException")?;
        let account = self.account(req);
        let guard = self.state.read();
        let st = guard.get(&account);
        let repo = st
            .and_then(|s| s.repositories.get(&name))
            .ok_or_else(|| repo_not_found(&name))?;
        let source = resolve_commit(repo, &b, "sourceCommitSpecifier")?;
        let dest = resolve_commit(repo, &b, "destinationCommitSpecifier")?;
        let base = merge_base(repo, &source, &dest);
        ok(json!({
            "sourceCommitId": source,
            "destinationCommitId": dest,
            "baseCommitId": base,
        }))
    }

    fn get_merge_options(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = require_repository_name(&b)?;
        require(&b, "sourceCommitSpecifier", "CommitRequiredException")?;
        require(&b, "destinationCommitSpecifier", "CommitRequiredException")?;
        let account = self.account(req);
        let guard = self.state.read();
        let st = guard.get(&account);
        let repo = st
            .and_then(|s| s.repositories.get(&name))
            .ok_or_else(|| repo_not_found(&name))?;
        let source = resolve_commit(repo, &b, "sourceCommitSpecifier")?;
        let dest = resolve_commit(repo, &b, "destinationCommitSpecifier")?;
        let base = merge_base(repo, &source, &dest);
        let mut options = vec![json!("SQUASH_MERGE"), json!("THREE_WAY_MERGE")];
        if is_ancestor(repo, &dest, &source) {
            options.insert(0, json!("FAST_FORWARD_MERGE"));
        }
        ok(json!({
            "mergeOptions": options,
            "sourceCommitId": source,
            "destinationCommitId": dest,
            "baseCommitId": base,
        }))
    }

    fn get_merge_conflicts(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = require_repository_name(&b)?;
        require(&b, "sourceCommitSpecifier", "CommitRequiredException")?;
        require(&b, "destinationCommitSpecifier", "CommitRequiredException")?;
        let merge_option = require(&b, "mergeOption", "MergeOptionRequiredException")?;
        if !validate::is_enum(validate::MERGE_OPTION, &merge_option) {
            return Err(err(
                "InvalidMergeOptionException",
                "The merge option is not valid.",
            ));
        }
        let account = self.account(req);
        let guard = self.state.read();
        let st = guard.get(&account);
        let repo = st
            .and_then(|s| s.repositories.get(&name))
            .ok_or_else(|| repo_not_found(&name))?;
        let source = resolve_commit(repo, &b, "sourceCommitSpecifier")?;
        let dest = resolve_commit(repo, &b, "destinationCommitSpecifier")?;
        let empty = std::collections::BTreeMap::new();
        let (base, base_tree, src_tree, dst_tree) = merge_inputs(repo, &source, &dest, &empty);
        let conflicts = conflicting_paths_in(base_tree, src_tree, dst_tree);
        let mergeable = conflicts.is_empty();
        let metadata: Vec<Value> = conflicts
            .iter()
            .map(|p| conflict_metadata(repo, &source, &dest, base_tree, src_tree, dst_tree, p).0)
            .collect();
        ok(json!({
            "mergeable": mergeable,
            "destinationCommitId": dest,
            "sourceCommitId": source,
            "baseCommitId": base,
            "conflictMetadataList": metadata,
        }))
    }

    fn describe_merge_conflicts(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = require_repository_name(&b)?;
        require(&b, "sourceCommitSpecifier", "CommitRequiredException")?;
        require(&b, "destinationCommitSpecifier", "CommitRequiredException")?;
        let merge_option = require(&b, "mergeOption", "MergeOptionRequiredException")?;
        if !validate::is_enum(validate::MERGE_OPTION, &merge_option) {
            return Err(err(
                "InvalidMergeOptionException",
                "The merge option is not valid.",
            ));
        }
        let path = require(&b, "filePath", "PathRequiredException")?;
        let account = self.account(req);
        let guard = self.state.read();
        let st = guard.get(&account);
        let repo = st
            .and_then(|s| s.repositories.get(&name))
            .ok_or_else(|| repo_not_found(&name))?;
        let source = resolve_commit(repo, &b, "sourceCommitSpecifier")?;
        let dest = resolve_commit(repo, &b, "destinationCommitSpecifier")?;
        let empty = std::collections::BTreeMap::new();
        let (base, base_tree, src_tree, dst_tree) = merge_inputs(repo, &source, &dest, &empty);
        if base_tree.get(&path).is_none()
            && src_tree.get(&path).is_none()
            && dst_tree.get(&path).is_none()
        {
            return Err(err(
                "FileDoesNotExistException",
                "The specified file does not exist.",
            ));
        }
        let (metadata, hunks) =
            conflict_metadata(repo, &source, &dest, base_tree, src_tree, dst_tree, &path);
        ok(json!({
            "conflictMetadata": metadata,
            "mergeHunks": hunks,
            "destinationCommitId": dest,
            "sourceCommitId": source,
            "baseCommitId": base,
        }))
    }

    fn batch_describe_merge_conflicts(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = require_repository_name(&b)?;
        require(&b, "sourceCommitSpecifier", "CommitRequiredException")?;
        require(&b, "destinationCommitSpecifier", "CommitRequiredException")?;
        let merge_option = require(&b, "mergeOption", "MergeOptionRequiredException")?;
        if !validate::is_enum(validate::MERGE_OPTION, &merge_option) {
            return Err(err(
                "InvalidMergeOptionException",
                "The merge option is not valid.",
            ));
        }
        let account = self.account(req);
        let guard = self.state.read();
        let st = guard.get(&account);
        let repo = st
            .and_then(|s| s.repositories.get(&name))
            .ok_or_else(|| repo_not_found(&name))?;
        let source = resolve_commit(repo, &b, "sourceCommitSpecifier")?;
        let dest = resolve_commit(repo, &b, "destinationCommitSpecifier")?;
        let max_files = b
            .get("maxConflictFiles")
            .and_then(Value::as_i64)
            .filter(|n| *n >= 0)
            .map_or(usize::MAX, |n| n as usize);
        let max_hunks = b
            .get("maxMergeHunks")
            .and_then(Value::as_i64)
            .filter(|n| *n >= 0)
            .map_or(usize::MAX, |n| n as usize);
        let empty = std::collections::BTreeMap::new();
        let (base, base_tree, src_tree, dst_tree) = merge_inputs(repo, &source, &dest, &empty);
        let mut conflicts = Vec::new();
        for path in conflicting_paths_in(base_tree, src_tree, dst_tree) {
            if conflicts.len() >= max_files {
                break;
            }
            let (metadata, mut hunks) =
                conflict_metadata(repo, &source, &dest, base_tree, src_tree, dst_tree, &path);
            hunks.truncate(max_hunks);
            conflicts.push(json!({
                "conflictMetadata": metadata,
                "mergeHunks": hunks,
            }));
        }
        ok(json!({
            "conflicts": conflicts,
            "errors": [],
            "destinationCommitId": dest,
            "sourceCommitId": source,
            "baseCommitId": base,
        }))
    }

    // ----- Pull requests -----

    fn create_pull_request(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let title = match str_field(&b, "title") {
            Some(t) => t,
            None => {
                return Err(err(
                    "TitleRequiredException",
                    "A pull request title is required.",
                ))
            }
        };
        if title.chars().count() > 150 {
            return Err(err("InvalidTitleException", "The title is not valid."));
        }
        let targets = b
            .get("targets")
            .and_then(Value::as_array)
            .filter(|t| !t.is_empty())
            .ok_or_else(|| {
                err(
                    "TargetsRequiredException",
                    "Pull request targets are required.",
                )
            })?
            .clone();
        if let Some(d) = str_field(&b, "description") {
            if d.chars().count() > 10240 {
                return Err(err(
                    "InvalidDescriptionException",
                    "The description is not valid.",
                ));
            }
        }
        let account = self.account(req);
        let author = caller_arn(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let now = Utc::now();
        let mut pr_targets = Vec::new();
        let mut approval_rules = Vec::new();
        for t in &targets {
            let repo_name = t
                .get("repositoryName")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    err(
                        "RepositoryNameRequiredException",
                        "A repository name is required.",
                    )
                })?;
            let repo = st
                .repositories
                .get(repo_name)
                .ok_or_else(|| repo_not_found(repo_name))?;
            let source_ref = t
                .get("sourceReference")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    err(
                        "ReferenceNameRequiredException",
                        "A source reference is required.",
                    )
                })?;
            let dest_ref = t
                .get("destinationReference")
                .and_then(Value::as_str)
                .map(str::to_string)
                .or_else(|| {
                    repo.metadata
                        .get("defaultBranch")
                        .and_then(Value::as_str)
                        .map(str::to_string)
                })
                .unwrap_or_else(|| "main".to_string());
            if source_ref == dest_ref {
                return Err(err(
                    "SourceAndDestinationAreSameException",
                    "The source reference and destination reference are the same. You must specify different references for the source and destination.",
                ));
            }
            let source_commit = repo.branches.get(source_ref).cloned().ok_or_else(|| {
                err(
                    "ReferenceDoesNotExistException",
                    "The specified reference does not exist.",
                )
            })?;
            // The destination reference must also resolve to a real branch.
            let dest_commit = repo.branches.get(&dest_ref).cloned().ok_or_else(|| {
                err(
                    "ReferenceDoesNotExistException",
                    "The specified reference does not exist.",
                )
            })?;
            let base = merge_base(repo, &source_commit, &dest_commit);
            pr_targets.push(json!({
                "repositoryName": repo_name,
                "sourceReference": source_ref,
                "destinationReference": dest_ref,
                "sourceCommit": source_commit,
                "destinationCommit": dest_commit,
                "mergeBase": base,
                "mergeMetadata": { "isMerged": false },
            }));
            // Auto-populate approval rules from every approval-rule template
            // associated with the target repository (exactly as CodeCommit does).
            for tmpl_name in &repo.associated_templates {
                if approval_rules.iter().any(|r: &Value| {
                    r.get("approvalRuleName").and_then(Value::as_str) == Some(tmpl_name.as_str())
                }) {
                    continue;
                }
                if let Some(tmpl) = st.templates.get(tmpl_name) {
                    let content = tmpl
                        .get("approvalRuleTemplateContent")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string();
                    approval_rules.push(json!({
                        "approvalRuleId": new_uuid(),
                        "approvalRuleName": tmpl_name,
                        "approvalRuleContent": content,
                        "ruleContentSha256": sha256_hex(content.as_bytes()),
                        "lastModifiedDate": ts(now),
                        "creationDate": ts(now),
                        "lastModifiedUser": author,
                        "originApprovalRuleTemplate": {
                            "approvalRuleTemplateId": tmpl
                                .get("approvalRuleTemplateId")
                                .cloned()
                                .unwrap_or(Value::Null),
                            "approvalRuleTemplateName": tmpl_name,
                        },
                    }));
                }
            }
        }
        st.pull_request_counter += 1;
        let pr_id = st.pull_request_counter.to_string();
        let revision_id = fresh_object_id();
        let pr = json!({
            "pullRequestId": pr_id,
            "title": title,
            "description": str_field(&b, "description").unwrap_or_default(),
            "lastActivityDate": ts(now),
            "creationDate": ts(now),
            "pullRequestStatus": "OPEN",
            "authorArn": author,
            "pullRequestTargets": pr_targets,
            "clientRequestToken": str_field(&b, "clientRequestToken").unwrap_or_default(),
            "revisionId": revision_id,
            "approvalRules": approval_rules,
        });
        st.pull_requests.insert(pr_id.clone(), pr.clone());
        st.pull_request_order.push(pr_id.clone());
        st.pull_request_events.insert(
            pr_id.clone(),
            vec![pr_event(
                &pr_id,
                "PULL_REQUEST_CREATED",
                &author,
                now,
                json!({}),
            )],
        );
        ok(json!({ "pullRequest": pr }))
    }

    fn get_pull_request(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let id = require(&b, "pullRequestId", "PullRequestIdRequiredException")?;
        let account = self.account(req);
        let guard = self.state.read();
        let st = guard.get(&account);
        // An open PR's target commits/mergeBase are recomputed from live branch
        // tips so a read always reflects what a merge would actually use.
        match st.and_then(|s| s.pull_requests.get(&id).map(|pr| refresh_pr_targets(s, pr))) {
            Some(pr) => ok(json!({ "pullRequest": pr })),
            None => Err(pr_not_found()),
        }
    }

    fn list_pull_requests(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = require_repository_name(&b)?;
        check_enum(
            &b,
            "pullRequestStatus",
            validate::PULL_REQUEST_STATUS,
            "InvalidPullRequestStatusException",
        )?;
        let author_filter = str_field(&b, "authorArn");
        let status_filter = str_field(&b, "pullRequestStatus");
        let account = self.account(req);
        let guard = self.state.read();
        let st = guard.get(&account);
        let s = st.ok_or_else(|| repo_not_found(&name))?;
        if !s.repositories.contains_key(&name) {
            return Err(repo_not_found(&name));
        }
        let mut ids = Vec::new();
        for pid in &s.pull_request_order {
            let Some(pr) = s.pull_requests.get(pid) else {
                continue;
            };
            let in_repo = pr
                .get("pullRequestTargets")
                .and_then(Value::as_array)
                .map(|ts| {
                    ts.iter().any(|t| {
                        t.get("repositoryName").and_then(Value::as_str) == Some(name.as_str())
                    })
                })
                .unwrap_or(false);
            if !in_repo {
                continue;
            }
            if let Some(a) = &author_filter {
                if pr.get("authorArn").and_then(Value::as_str) != Some(a.as_str()) {
                    continue;
                }
            }
            if let Some(s2) = &status_filter {
                if pr.get("pullRequestStatus").and_then(Value::as_str) != Some(s2.as_str()) {
                    continue;
                }
            }
            ids.push(json!(pid));
        }
        ok(json!({ "pullRequestIds": ids }))
    }

    fn update_pull_request_title(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let id = require(&b, "pullRequestId", "PullRequestIdRequiredException")?;
        let title = match str_field(&b, "title") {
            Some(t) => t,
            None => {
                return Err(err(
                    "TitleRequiredException",
                    "A pull request title is required.",
                ))
            }
        };
        if title.chars().count() > 150 {
            return Err(err("InvalidTitleException", "The title is not valid."));
        }
        self.mutate_pr(req, &id, |pr| {
            pr["title"] = json!(title);
            pr["lastActivityDate"] = ts(Utc::now());
            Ok(())
        })
    }

    fn update_pull_request_description(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let id = require(&b, "pullRequestId", "PullRequestIdRequiredException")?;
        let desc = str_field(&b, "description").unwrap_or_default();
        if desc.chars().count() > 10240 {
            return Err(err(
                "InvalidDescriptionException",
                "The description is not valid.",
            ));
        }
        self.mutate_pr(req, &id, |pr| {
            pr["description"] = json!(desc);
            pr["lastActivityDate"] = ts(Utc::now());
            Ok(())
        })
    }

    fn update_pull_request_status(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let id = require(&b, "pullRequestId", "PullRequestIdRequiredException")?;
        let status = require(
            &b,
            "pullRequestStatus",
            "PullRequestStatusRequiredException",
        )?;
        if !validate::is_enum(validate::PULL_REQUEST_STATUS, &status) {
            return Err(err(
                "InvalidPullRequestStatusException",
                "The pull request status is not valid.",
            ));
        }
        self.mutate_pr(req, &id, |pr| {
            let current = pr.get("pullRequestStatus").and_then(Value::as_str).unwrap_or("OPEN");
            // The only valid transition is OPEN -> CLOSED. Reopening a closed PR
            // (CLOSED -> *), re-closing (CLOSED -> CLOSED), and a no-op
            // OPEN -> OPEN are all rejected, matching CodeCommit.
            if !(current == "OPEN" && status == "CLOSED") {
                return Err(err(
                    "InvalidPullRequestStatusUpdateException",
                    "The pull request status update is not valid. The only valid update is from OPEN to CLOSED.",
                ));
            }
            pr["pullRequestStatus"] = json!(status);
            pr["lastActivityDate"] = ts(Utc::now());
            Ok(())
        })
    }

    fn describe_pull_request_events(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let id = require(&b, "pullRequestId", "PullRequestIdRequiredException")?;
        check_enum(
            &b,
            "pullRequestEventType",
            validate::PULL_REQUEST_EVENT_TYPE,
            "InvalidPullRequestEventTypeException",
        )?;
        let account = self.account(req);
        let guard = self.state.read();
        let st = guard.get(&account);
        let s = st.ok_or_else(pr_not_found)?;
        if !s.pull_requests.contains_key(&id) {
            return Err(pr_not_found());
        }
        let events = s.pull_request_events.get(&id).cloned().unwrap_or_default();
        ok(json!({ "pullRequestEvents": events }))
    }

    fn merge_pull_request_fast_forward(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.merge_pull_request(req, "FAST_FORWARD_MERGE")
    }

    fn merge_pull_request_squash(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        self.merge_pull_request(req, "SQUASH_MERGE")
    }

    fn merge_pull_request_three_way(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.merge_pull_request(req, "THREE_WAY_MERGE")
    }

    fn merge_pull_request(
        &self,
        req: &AwsRequest,
        merge_option: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let id = require(&b, "pullRequestId", "PullRequestIdRequiredException")?;
        let name = require_repository_name(&b)?;
        validate_merge_conflict_enums(&b)?;
        let author = caller_arn(req);
        let account = self.account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        if !st.repositories.contains_key(&name) {
            return Err(repo_not_found(&name));
        }
        let now = Utc::now();

        // Read the pull request's target and rules without holding a mutable
        // borrow across the object-store mutation below.
        let (source_ref, dest_ref, revision, rules) = {
            let pr = st.pull_requests.get(&id).ok_or_else(pr_not_found)?;
            if pr.get("pullRequestStatus").and_then(Value::as_str) == Some("CLOSED") {
                return Err(err(
                    "PullRequestAlreadyClosedException",
                    "The pull request status cannot be updated because it is already closed.",
                ));
            }
            let target = pr
                .get("pullRequestTargets")
                .and_then(Value::as_array)
                .and_then(|ts| {
                    ts.iter().find(|t| {
                        t.get("repositoryName").and_then(Value::as_str) == Some(name.as_str())
                    })
                })
                .ok_or_else(|| {
                    err(
                        "RepositoryNotAssociatedWithPullRequestException",
                        "The repository is not associated with the pull request.",
                    )
                })?;
            let source_ref = target
                .get("sourceReference")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let dest_ref = target
                .get("destinationReference")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let revision = pr
                .get("revisionId")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let rules = pr
                .get("approvalRules")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            (source_ref, dest_ref, revision, rules)
        };

        // Enforce approval rules (unless overridden), exactly as AWS does before
        // allowing a merge.
        let overridden = st
            .pull_request_overrides
            .get(&id)
            .and_then(|m| m.get(&revision))
            .is_some();
        if !overridden && !rules.is_empty() {
            let approvers: std::collections::BTreeSet<String> = st
                .pull_request_approvals
                .get(&id)
                .and_then(|m| m.get(&revision))
                .map(|m| {
                    m.iter()
                        .filter(|(_, v)| v.as_str() == "APPROVE")
                        .map(|(arn, _)| arn.clone())
                        .collect()
                })
                .unwrap_or_default();
            let all_satisfied = rules.iter().all(|r| {
                let content = r
                    .get("approvalRuleContent")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                rule_satisfied(content, &approvers)
            });
            if !all_satisfied {
                return Err(err(
                    "PullRequestApprovalRulesNotSatisfiedException",
                    "The pull request cannot be merged because one or more approval rules have not been satisfied.",
                ));
            }
        }

        // Resolve the current tips and perform the merge against the object
        // store, advancing the destination branch to the resulting commit.
        let strategy = str_field(&b, "conflictResolutionStrategy").unwrap_or_default();
        let repo = st.repositories.get_mut(&name).expect("repo checked above");
        let source_tip = repo.branches.get(&source_ref).cloned().ok_or_else(|| {
            err(
                "ReferenceDoesNotExistException",
                "The specified reference does not exist.",
            )
        })?;
        let dest_tip = repo.branches.get(&dest_ref).cloned().ok_or_else(|| {
            err(
                "ReferenceDoesNotExistException",
                "The specified reference does not exist.",
            )
        })?;
        // Concurrency check: if the caller pins a source tip it must still match.
        if let Some(pinned) = str_field(&b, "sourceCommitId") {
            if pinned != source_tip {
                return Err(err(
                    "TipOfSourceReferenceIsDifferentException",
                    "The tip of the source reference is different from the specified commit ID.",
                ));
            }
        }
        let merge_commit_id = match merge_option {
            "FAST_FORWARD_MERGE" => {
                if !is_ancestor(repo, &dest_tip, &source_tip) {
                    return Err(err(
                        "ManualMergeRequiredException",
                        "The fast-forward merge cannot be performed because the destination is not an ancestor of the source.",
                    ));
                }
                repo.branches.insert(dest_ref.clone(), source_tip.clone());
                source_tip.clone()
            }
            _ => {
                let merged =
                    merge_trees(repo, &source_tip, &dest_tip, &strategy).ok_or_else(|| {
                        err(
                            "ManualMergeRequiredException",
                            "The merge cannot be completed because there are merge conflicts that must be resolved manually.",
                        )
                    })?;
                let tree_id = tree_id_for(&merged);
                let commit_id = fresh_object_id();
                let parents = merge_parents(merge_option, &dest_tip, &source_tip);
                let commit = build_commit(&commit_id, &tree_id, &parents, &b, req);
                repo.commits.insert(commit_id.clone(), commit);
                repo.trees.insert(commit_id.clone(), merged);
                repo.branches.insert(dest_ref.clone(), commit_id.clone());
                commit_id
            }
        };

        // Advancing the destination branch invalidates approvals on any other
        // open PR sourced from it.
        refresh_source_revisions(st, &name, &dest_ref);

        // Close the pull request and record the real merge metadata.
        let pr = st.pull_requests.get_mut(&id).expect("pr checked above");
        pr["pullRequestStatus"] = json!("CLOSED");
        pr["lastActivityDate"] = ts(now);
        if let Some(targets) = pr
            .get_mut("pullRequestTargets")
            .and_then(Value::as_array_mut)
        {
            for t in targets.iter_mut() {
                if t.get("repositoryName").and_then(Value::as_str) == Some(name.as_str()) {
                    t["mergeMetadata"] = json!({
                        "isMerged": true,
                        "mergedBy": author,
                        "mergeCommitId": merge_commit_id,
                        "mergeOption": merge_option,
                    });
                }
            }
        }
        let pr = pr.clone();
        st.pull_request_events
            .entry(id.clone())
            .or_default()
            .push(pr_event(
                &id,
                "PULL_REQUEST_MERGE_STATE_CHANGED",
                &author,
                now,
                json!({}),
            ));
        ok(json!({ "pullRequest": pr }))
    }

    // ----- Pull-request approval rules / states -----

    fn create_pull_request_approval_rule(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let id = require(&b, "pullRequestId", "PullRequestIdRequiredException")?;
        let rule_name = require(&b, "approvalRuleName", "ApprovalRuleNameRequiredException")?;
        let content = require(
            &b,
            "approvalRuleContent",
            "ApprovalRuleContentRequiredException",
        )?;
        if rule_name.chars().count() > 100 {
            return Err(err(
                "InvalidApprovalRuleNameException",
                "The approval rule name is not valid.",
            ));
        }
        if content.chars().count() > 3000 {
            return Err(err(
                "InvalidApprovalRuleContentException",
                "The approval rule content is not valid.",
            ));
        }
        let content = normalize_rule_content(&content);
        let now = Utc::now();
        let user = caller_arn(req);
        let account = self.account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let pr = st.pull_requests.get_mut(&id).ok_or_else(pr_not_found)?;
        if pr.get("pullRequestStatus").and_then(Value::as_str) == Some("CLOSED") {
            return Err(err(
                "PullRequestAlreadyClosedException",
                "The pull request is already closed.",
            ));
        }
        let rules = pr
            .get_mut("approvalRules")
            .and_then(Value::as_array_mut)
            .ok_or_else(pr_not_found)?;
        if rules
            .iter()
            .any(|r| r.get("approvalRuleName").and_then(Value::as_str) == Some(rule_name.as_str()))
        {
            return Err(err(
                "ApprovalRuleNameAlreadyExistsException",
                "An approval rule with that name already exists.",
            ));
        }
        let rule = json!({
            "approvalRuleId": new_uuid(),
            "approvalRuleName": rule_name,
            "approvalRuleContent": content,
            "ruleContentSha256": sha256_hex(content.as_bytes()),
            "lastModifiedDate": ts(now),
            "creationDate": ts(now),
            "lastModifiedUser": user,
        });
        rules.push(rule.clone());
        ok(json!({ "approvalRule": rule }))
    }

    fn delete_pull_request_approval_rule(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let id = require(&b, "pullRequestId", "PullRequestIdRequiredException")?;
        let rule_name = require(&b, "approvalRuleName", "ApprovalRuleNameRequiredException")?;
        let account = self.account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let pr = st.pull_requests.get_mut(&id).ok_or_else(pr_not_found)?;
        let mut deleted_id = String::new();
        if let Some(rules) = pr.get_mut("approvalRules").and_then(Value::as_array_mut) {
            if let Some(pos) = rules.iter().position(|r| {
                r.get("approvalRuleName").and_then(Value::as_str) == Some(rule_name.as_str())
            }) {
                deleted_id = rules[pos]
                    .get("approvalRuleId")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                rules.remove(pos);
            }
        }
        ok(json!({ "approvalRuleId": deleted_id }))
    }

    fn update_pull_request_approval_rule_content(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let id = require(&b, "pullRequestId", "PullRequestIdRequiredException")?;
        let rule_name = require(&b, "approvalRuleName", "ApprovalRuleNameRequiredException")?;
        let content = require(&b, "newRuleContent", "ApprovalRuleContentRequiredException")?;
        if content.chars().count() > 3000 {
            return Err(err(
                "InvalidApprovalRuleContentException",
                "The approval rule content is not valid.",
            ));
        }
        let content = normalize_rule_content(&content);
        let now = Utc::now();
        let user = caller_arn(req);
        let account = self.account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let pr = st.pull_requests.get_mut(&id).ok_or_else(pr_not_found)?;
        let rules = pr
            .get_mut("approvalRules")
            .and_then(Value::as_array_mut)
            .ok_or_else(pr_not_found)?;
        let rule = rules
            .iter_mut()
            .find(|r| r.get("approvalRuleName").and_then(Value::as_str) == Some(rule_name.as_str()))
            .ok_or_else(|| {
                err(
                    "ApprovalRuleDoesNotExistException",
                    "The specified approval rule does not exist.",
                )
            })?;
        rule["approvalRuleContent"] = json!(content);
        rule["ruleContentSha256"] = json!(sha256_hex(content.as_bytes()));
        rule["lastModifiedDate"] = ts(now);
        rule["lastModifiedUser"] = json!(user);
        let rule = rule.clone();
        ok(json!({ "approvalRule": rule }))
    }

    fn update_pull_request_approval_state(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let id = require(&b, "pullRequestId", "PullRequestIdRequiredException")?;
        let revision = require(&b, "revisionId", "RevisionIdRequiredException")?;
        let state = require(&b, "approvalState", "ApprovalStateRequiredException")?;
        if !validate::is_enum(validate::APPROVAL_STATE, &state) {
            return Err(err(
                "InvalidApprovalStateException",
                "The approval state is not valid.",
            ));
        }
        let user = caller_arn(req);
        let account = self.account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let pr = st.pull_requests.get(&id).ok_or_else(pr_not_found)?;
        if pr.get("pullRequestStatus").and_then(Value::as_str) == Some("CLOSED") {
            return Err(err(
                "PullRequestAlreadyClosedException",
                "The pull request is already closed.",
            ));
        }
        let current_revision = pr
            .get("revisionId")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        if revision != current_revision {
            return Err(err(
                "RevisionNotCurrentException",
                "The revision ID is not the current revision of the pull request.",
            ));
        }
        if pr.get("authorArn").and_then(Value::as_str) == Some(user.as_str()) {
            return Err(err(
                "PullRequestCannotBeApprovedByAuthorException",
                "The pull request cannot be approved by the author of the pull request.",
            ));
        }
        st.pull_request_approvals
            .entry(id)
            .or_default()
            .entry(revision)
            .or_default()
            .insert(user, state);
        ok(json!({}))
    }

    fn get_pull_request_approval_states(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let id = require(&b, "pullRequestId", "PullRequestIdRequiredException")?;
        let revision = require(&b, "revisionId", "RevisionIdRequiredException")?;
        let account = self.account(req);
        let guard = self.state.read();
        let st = guard.get(&account);
        let s = st.ok_or_else(pr_not_found)?;
        if !s.pull_requests.contains_key(&id) {
            return Err(pr_not_found());
        }
        let approvals: Vec<Value> = s
            .pull_request_approvals
            .get(&id)
            .and_then(|m| m.get(&revision))
            .map(|m| {
                m.iter()
                    .filter(|(_, v)| v.as_str() == "APPROVE")
                    .map(|(arn, v)| json!({ "userArn": arn, "approvalState": v }))
                    .collect()
            })
            .unwrap_or_default();
        ok(json!({ "approvals": approvals }))
    }

    fn evaluate_pull_request_approval_rules(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let id = require(&b, "pullRequestId", "PullRequestIdRequiredException")?;
        let revision = require(&b, "revisionId", "RevisionIdRequiredException")?;
        let account = self.account(req);
        let guard = self.state.read();
        let st = guard.get(&account);
        let s = st.ok_or_else(pr_not_found)?;
        let pr = s.pull_requests.get(&id).ok_or_else(pr_not_found)?;
        let current_revision = pr
            .get("revisionId")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if revision != current_revision {
            return Err(err(
                "RevisionNotCurrentException",
                "The revision ID is not the current revision of the pull request.",
            ));
        }
        let overridden = s
            .pull_request_overrides
            .get(&id)
            .and_then(|m| m.get(&revision))
            .is_some();
        let rules = pr
            .get("approvalRules")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        // Distinct reviewers who approved the current revision.
        let approvers: std::collections::BTreeSet<String> = s
            .pull_request_approvals
            .get(&id)
            .and_then(|m| m.get(&revision))
            .map(|m| {
                m.iter()
                    .filter(|(_, v)| v.as_str() == "APPROVE")
                    .map(|(arn, _)| arn.clone())
                    .collect()
            })
            .unwrap_or_default();
        let mut satisfied_names = Vec::new();
        let mut not_satisfied_names = Vec::new();
        for r in &rules {
            let Some(name) = r.get("approvalRuleName").cloned() else {
                continue;
            };
            let content = r
                .get("approvalRuleContent")
                .and_then(Value::as_str)
                .unwrap_or_default();
            // An override satisfies every rule; otherwise honor the rule's
            // NumberOfApprovalsNeeded and approver pool.
            if overridden || rule_satisfied(content, &approvers) {
                satisfied_names.push(name);
            } else {
                not_satisfied_names.push(name);
            }
        }
        let approved = not_satisfied_names.is_empty();
        ok(json!({
            "evaluation": {
                "approved": approved,
                "overridden": overridden,
                "approvalRulesSatisfied": satisfied_names,
                "approvalRulesNotSatisfied": not_satisfied_names,
            }
        }))
    }

    fn override_pull_request_approval_rules(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let id = require(&b, "pullRequestId", "PullRequestIdRequiredException")?;
        let revision = require(&b, "revisionId", "RevisionIdRequiredException")?;
        let status = require(&b, "overrideStatus", "OverrideStatusRequiredException")?;
        if !validate::is_enum(validate::OVERRIDE_STATUS, &status) {
            return Err(err(
                "InvalidOverrideStatusException",
                "The override status is not valid.",
            ));
        }
        let user = caller_arn(req);
        let account = self.account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        {
            let pr = st.pull_requests.get(&id).ok_or_else(pr_not_found)?;
            let current_revision = pr
                .get("revisionId")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if revision != current_revision {
                return Err(err(
                    "RevisionNotCurrentException",
                    "The revision ID is not the current revision of the pull request.",
                ));
            }
        }
        let overrides = st.pull_request_overrides.entry(id).or_default();
        if status == "OVERRIDE" {
            if overrides.contains_key(&revision) {
                return Err(err(
                    "OverrideAlreadySetException",
                    "The override status is already set.",
                ));
            }
            overrides.insert(revision, user);
        } else {
            overrides.remove(&revision);
        }
        ok(json!({}))
    }

    fn get_pull_request_override_state(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let id = require(&b, "pullRequestId", "PullRequestIdRequiredException")?;
        let revision = require(&b, "revisionId", "RevisionIdRequiredException")?;
        let account = self.account(req);
        let guard = self.state.read();
        let st = guard.get(&account);
        let s = st.ok_or_else(pr_not_found)?;
        if !s.pull_requests.contains_key(&id) {
            return Err(pr_not_found());
        }
        match s
            .pull_request_overrides
            .get(&id)
            .and_then(|m| m.get(&revision))
        {
            Some(arn) => ok(json!({ "overridden": true, "overrider": arn })),
            None => ok(json!({ "overridden": false })),
        }
    }

    // ----- Approval-rule templates -----

    fn create_approval_rule_template(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = require(
            &b,
            "approvalRuleTemplateName",
            "ApprovalRuleTemplateNameRequiredException",
        )?;
        let content = require(
            &b,
            "approvalRuleTemplateContent",
            "ApprovalRuleTemplateContentRequiredException",
        )?;
        if name.chars().count() > 100 {
            return Err(err(
                "InvalidApprovalRuleTemplateNameException",
                "The approval rule template name is not valid.",
            ));
        }
        if content.chars().count() > 3000 {
            return Err(err(
                "InvalidApprovalRuleTemplateContentException",
                "The approval rule template content is not valid.",
            ));
        }
        let content = normalize_rule_content(&content);
        if let Some(d) = str_field(&b, "approvalRuleTemplateDescription") {
            if d.chars().count() > 1000 {
                return Err(err(
                    "InvalidApprovalRuleTemplateDescriptionException",
                    "The approval rule template description is not valid.",
                ));
            }
        }
        let now = Utc::now();
        let user = caller_arn(req);
        let account = self.account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        if st.templates.contains_key(&name) {
            return Err(err(
                "ApprovalRuleTemplateNameAlreadyExistsException",
                format!("An approval rule template with the name {name} already exists."),
            ));
        }
        let mut tmpl = Map::new();
        tmpl.insert("approvalRuleTemplateId".into(), json!(new_uuid()));
        tmpl.insert("approvalRuleTemplateName".into(), json!(name));
        if let Some(d) = str_field(&b, "approvalRuleTemplateDescription") {
            tmpl.insert("approvalRuleTemplateDescription".into(), json!(d));
        }
        tmpl.insert("approvalRuleTemplateContent".into(), json!(content));
        tmpl.insert(
            "ruleContentSha256".into(),
            json!(sha256_hex(content.as_bytes())),
        );
        tmpl.insert("lastModifiedDate".into(), ts(now));
        tmpl.insert("creationDate".into(), ts(now));
        tmpl.insert("lastModifiedUser".into(), json!(user));
        let tmpl = Value::Object(tmpl);
        st.templates.insert(name.clone(), tmpl.clone());
        st.template_order.push(name);
        ok(json!({ "approvalRuleTemplate": tmpl }))
    }

    fn get_approval_rule_template(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = require(
            &b,
            "approvalRuleTemplateName",
            "ApprovalRuleTemplateNameRequiredException",
        )?;
        let account = self.account(req);
        let guard = self.state.read();
        let st = guard.get(&account);
        match st.and_then(|s| s.templates.get(&name)) {
            Some(t) => ok(json!({ "approvalRuleTemplate": t })),
            None => Err(template_not_found()),
        }
    }

    fn delete_approval_rule_template(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = require(
            &b,
            "approvalRuleTemplateName",
            "ApprovalRuleTemplateNameRequiredException",
        )?;
        if name.chars().count() > 100 {
            return Err(err(
                "InvalidApprovalRuleTemplateNameException",
                "The approval rule template name is not valid.",
            ));
        }
        let account = self.account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let id = st
            .templates
            .remove(&name)
            .and_then(|t| {
                t.get("approvalRuleTemplateId")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            })
            .unwrap_or_default();
        st.template_order.retain(|n| n != &name);
        for repo in st.repositories.values_mut() {
            repo.associated_templates.retain(|t| t != &name);
        }
        ok(json!({ "approvalRuleTemplateId": id }))
    }

    fn list_approval_rule_templates(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let account = self.account(req);
        let guard = self.state.read();
        let st = guard.get(&account);
        let names: Vec<Value> = st
            .map(|s| s.template_order.iter().map(|n| json!(n)).collect())
            .unwrap_or_default();
        ok(json!({ "approvalRuleTemplateNames": names }))
    }

    fn update_approval_rule_template_content(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = require(
            &b,
            "approvalRuleTemplateName",
            "ApprovalRuleTemplateNameRequiredException",
        )?;
        let content = require(
            &b,
            "newRuleContent",
            "ApprovalRuleTemplateContentRequiredException",
        )?;
        if content.chars().count() > 3000 {
            return Err(err(
                "InvalidApprovalRuleTemplateContentException",
                "The approval rule template content is not valid.",
            ));
        }
        let content = normalize_rule_content(&content);
        self.mutate_template(req, &name, |t| {
            t["approvalRuleTemplateContent"] = json!(content);
            t["ruleContentSha256"] = json!(sha256_hex(content.as_bytes()));
            t["lastModifiedDate"] = ts(Utc::now());
            Ok(())
        })
    }

    fn update_approval_rule_template_description(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = require(
            &b,
            "approvalRuleTemplateName",
            "ApprovalRuleTemplateNameRequiredException",
        )?;
        let desc = require(
            &b,
            "approvalRuleTemplateDescription",
            "InvalidApprovalRuleTemplateDescriptionException",
        )?;
        if desc.chars().count() > 1000 {
            return Err(err(
                "InvalidApprovalRuleTemplateDescriptionException",
                "The approval rule template description is not valid.",
            ));
        }
        self.mutate_template(req, &name, |t| {
            t["approvalRuleTemplateDescription"] = json!(desc);
            t["lastModifiedDate"] = ts(Utc::now());
            Ok(())
        })
    }

    fn update_approval_rule_template_name(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let old = require(
            &b,
            "oldApprovalRuleTemplateName",
            "ApprovalRuleTemplateNameRequiredException",
        )?;
        let new = require(
            &b,
            "newApprovalRuleTemplateName",
            "ApprovalRuleTemplateNameRequiredException",
        )?;
        if new.chars().count() > 100 {
            return Err(err(
                "InvalidApprovalRuleTemplateNameException",
                "The approval rule template name is not valid.",
            ));
        }
        let account = self.account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        if !st.templates.contains_key(&old) {
            return Err(template_not_found());
        }
        if old != new && st.templates.contains_key(&new) {
            return Err(err(
                "ApprovalRuleTemplateNameAlreadyExistsException",
                format!("An approval rule template with the name {new} already exists."),
            ));
        }
        let mut tmpl = st.templates.remove(&old).unwrap();
        tmpl["approvalRuleTemplateName"] = json!(new);
        tmpl["lastModifiedDate"] = ts(Utc::now());
        for n in &mut st.template_order {
            if n == &old {
                *n = new.clone();
            }
        }
        for repo in st.repositories.values_mut() {
            for t in &mut repo.associated_templates {
                if t == &old {
                    *t = new.clone();
                }
            }
        }
        st.templates.insert(new, tmpl.clone());
        ok(json!({ "approvalRuleTemplate": tmpl }))
    }

    fn associate_template_with_repository(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let tmpl = require(
            &b,
            "approvalRuleTemplateName",
            "ApprovalRuleTemplateNameRequiredException",
        )?;
        let name = require_repository_name(&b)?;
        let account = self.account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        if !st.templates.contains_key(&tmpl) {
            return Err(template_not_found());
        }
        let repo = st
            .repositories
            .get_mut(&name)
            .ok_or_else(|| repo_not_found(&name))?;
        if !repo.associated_templates.contains(&tmpl) {
            repo.associated_templates.push(tmpl);
        }
        ok(json!({}))
    }

    fn disassociate_template_from_repository(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let tmpl = require(
            &b,
            "approvalRuleTemplateName",
            "ApprovalRuleTemplateNameRequiredException",
        )?;
        let name = require_repository_name(&b)?;
        let account = self.account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        if !st.templates.contains_key(&tmpl) {
            return Err(template_not_found());
        }
        let repo = st
            .repositories
            .get_mut(&name)
            .ok_or_else(|| repo_not_found(&name))?;
        repo.associated_templates.retain(|t| t != &tmpl);
        ok(json!({}))
    }

    fn batch_associate_template(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let tmpl = require(
            &b,
            "approvalRuleTemplateName",
            "ApprovalRuleTemplateNameRequiredException",
        )?;
        let names = b
            .get("repositoryNames")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                err(
                    "RepositoryNamesRequiredException",
                    "Repository names are required.",
                )
            })?
            .clone();
        let account = self.account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        if !st.templates.contains_key(&tmpl) {
            return Err(template_not_found());
        }
        let mut associated = Vec::new();
        let mut errors = Vec::new();
        for n in &names {
            let Some(rn) = n.as_str() else { continue };
            match st.repositories.get_mut(rn) {
                Some(repo) => {
                    if !repo.associated_templates.contains(&tmpl) {
                        repo.associated_templates.push(tmpl.clone());
                    }
                    associated.push(json!(rn));
                }
                None => errors.push(json!({
                    "repositoryName": rn,
                    "errorCode": "RepositoryDoesNotExist",
                    "errorMessage": format!("The repository {rn} does not exist."),
                })),
            }
        }
        ok(json!({ "associatedRepositoryNames": associated, "errors": errors }))
    }

    fn batch_disassociate_template(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let tmpl = require(
            &b,
            "approvalRuleTemplateName",
            "ApprovalRuleTemplateNameRequiredException",
        )?;
        let names = b
            .get("repositoryNames")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                err(
                    "RepositoryNamesRequiredException",
                    "Repository names are required.",
                )
            })?
            .clone();
        let account = self.account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        if !st.templates.contains_key(&tmpl) {
            return Err(template_not_found());
        }
        let mut disassociated = Vec::new();
        let mut errors = Vec::new();
        for n in &names {
            let Some(rn) = n.as_str() else { continue };
            match st.repositories.get_mut(rn) {
                Some(repo) => {
                    repo.associated_templates.retain(|t| t != &tmpl);
                    disassociated.push(json!(rn));
                }
                None => errors.push(json!({
                    "repositoryName": rn,
                    "errorCode": "RepositoryDoesNotExist",
                    "errorMessage": format!("The repository {rn} does not exist."),
                })),
            }
        }
        ok(json!({ "disassociatedRepositoryNames": disassociated, "errors": errors }))
    }

    fn list_associated_templates_for_repository(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = require_repository_name(&b)?;
        let account = self.account(req);
        let guard = self.state.read();
        let st = guard.get(&account);
        let repo = st
            .and_then(|s| s.repositories.get(&name))
            .ok_or_else(|| repo_not_found(&name))?;
        let names: Vec<Value> = repo.associated_templates.iter().map(|t| json!(t)).collect();
        ok(json!({ "approvalRuleTemplateNames": names }))
    }

    fn list_repositories_for_template(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let tmpl = require(
            &b,
            "approvalRuleTemplateName",
            "ApprovalRuleTemplateNameRequiredException",
        )?;
        let account = self.account(req);
        let guard = self.state.read();
        let st = guard.get(&account);
        let s = st.ok_or_else(template_not_found)?;
        if !s.templates.contains_key(&tmpl) {
            return Err(template_not_found());
        }
        let names: Vec<Value> = s
            .repository_order
            .iter()
            .filter(|n| {
                s.repositories
                    .get(*n)
                    .map(|r| r.associated_templates.contains(&tmpl))
                    .unwrap_or(false)
            })
            .map(|n| json!(n))
            .collect();
        ok(json!({ "repositoryNames": names }))
    }

    // ----- Comments -----

    fn post_comment_for_compared_commit(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = require_repository_name(&b)?;
        let after = require(&b, "afterCommitId", "CommitIdRequiredException")?;
        let content = require(&b, "content", "CommentContentRequiredException")?;
        if content.chars().count() > 10240 {
            return Err(err(
                "CommentContentSizeLimitExceededException",
                "The comment is too large. Comments are limited to 10,240 characters.",
            ));
        }
        let before = str_field(&b, "beforeCommitId");
        if before.as_deref() == Some(after.as_str()) {
            return Err(err(
                "BeforeCommitIdAndAfterCommitIdAreSameException",
                "The before commit ID and the after commit ID are the same.",
            ));
        }
        let account = self.account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        if !st.repositories.contains_key(&name) {
            return Err(repo_not_found(&name));
        }
        let now = Utc::now();
        let author = caller_arn(req);
        let comment_id = new_uuid();
        let location = b.get("location").cloned();
        let mut stored = comment_value(&comment_id, &content, &author, now, None);
        if let Some(obj) = stored.as_object_mut() {
            obj.insert("_ctxRepositoryName".into(), json!(name));
            obj.insert("_ctxAfterCommitId".into(), json!(after));
            if let Some(bc) = &before {
                obj.insert("_ctxBeforeCommitId".into(), json!(bc));
            }
            if let Some(loc) = &location {
                obj.insert("_ctxLocation".into(), loc.clone());
            }
        }
        st.comments.insert(comment_id.clone(), stored.clone());
        st.comment_order.push(comment_id);
        let public = public_comment(&stored);
        let mut out = Map::new();
        out.insert("repositoryName".into(), json!(name));
        if let Some(bc) = before {
            out.insert("beforeCommitId".into(), json!(bc));
        }
        out.insert("afterCommitId".into(), json!(after));
        if let Some(loc) = location {
            out.insert("location".into(), loc);
        }
        out.insert("comment".into(), public);
        ok(Value::Object(out))
    }

    fn post_comment_for_pull_request(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let pr_id = require(&b, "pullRequestId", "PullRequestIdRequiredException")?;
        let name = require_repository_name(&b)?;
        let before = require(&b, "beforeCommitId", "CommitIdRequiredException")?;
        let after = require(&b, "afterCommitId", "CommitIdRequiredException")?;
        let content = require(&b, "content", "CommentContentRequiredException")?;
        if content.chars().count() > 10240 {
            return Err(err(
                "CommentContentSizeLimitExceededException",
                "The comment is too large.",
            ));
        }
        let account = self.account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        if !st.repositories.contains_key(&name) {
            return Err(repo_not_found(&name));
        }
        if !st.pull_requests.contains_key(&pr_id) {
            return Err(pr_not_found());
        }
        let now = Utc::now();
        let author = caller_arn(req);
        let comment_id = new_uuid();
        let location = b.get("location").cloned();
        let mut stored = comment_value(&comment_id, &content, &author, now, None);
        if let Some(obj) = stored.as_object_mut() {
            obj.insert("_ctxRepositoryName".into(), json!(name));
            obj.insert("_ctxPullRequestId".into(), json!(pr_id));
            obj.insert("_ctxBeforeCommitId".into(), json!(before));
            obj.insert("_ctxAfterCommitId".into(), json!(after));
            if let Some(loc) = &location {
                obj.insert("_ctxLocation".into(), loc.clone());
            }
        }
        st.comments.insert(comment_id.clone(), stored.clone());
        st.comment_order.push(comment_id);
        let public = public_comment(&stored);
        let mut out = Map::new();
        out.insert("repositoryName".into(), json!(name));
        out.insert("pullRequestId".into(), json!(pr_id));
        out.insert("beforeCommitId".into(), json!(before));
        out.insert("afterCommitId".into(), json!(after));
        if let Some(loc) = location {
            out.insert("location".into(), loc);
        }
        out.insert("comment".into(), public);
        ok(Value::Object(out))
    }

    fn post_comment_reply(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let in_reply_to = require(&b, "inReplyTo", "CommentIdRequiredException")?;
        let content = require(&b, "content", "CommentContentRequiredException")?;
        if content.chars().count() > 10240 {
            return Err(err(
                "CommentContentSizeLimitExceededException",
                "The comment is too large.",
            ));
        }
        let account = self.account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let parent = st.comments.get(&in_reply_to).cloned().ok_or_else(|| {
            err(
                "CommentDoesNotExistException",
                "The specified comment does not exist.",
            )
        })?;
        let now = Utc::now();
        let author = caller_arn(req);
        let comment_id = new_uuid();
        let mut stored = comment_value(&comment_id, &content, &author, now, Some(&in_reply_to));
        // Inherit the parent's thread context.
        if let (Some(obj), Some(pobj)) = (stored.as_object_mut(), parent.as_object()) {
            for (k, v) in pobj {
                if k.starts_with("_ctx") {
                    obj.insert(k.clone(), v.clone());
                }
            }
        }
        st.comments.insert(comment_id.clone(), stored.clone());
        st.comment_order.push(comment_id);
        ok(json!({ "comment": public_comment(&stored) }))
    }

    fn get_comment(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let id = require(&b, "commentId", "CommentIdRequiredException")?;
        let account = self.account(req);
        let guard = self.state.read();
        let st = guard.get(&account);
        let comment = st.and_then(|s| s.comments.get(&id)).ok_or_else(|| {
            err(
                "CommentDoesNotExistException",
                "The specified comment does not exist.",
            )
        })?;
        if comment.get("deleted").and_then(Value::as_bool) == Some(true) {
            return Err(err(
                "CommentDeletedException",
                "This comment has already been deleted.",
            ));
        }
        ok(json!({ "comment": public_comment(comment) }))
    }

    fn get_comments_for_compared_commit(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = require_repository_name(&b)?;
        let after = require(&b, "afterCommitId", "CommitIdRequiredException")?;
        let before = str_field(&b, "beforeCommitId");
        let account = self.account(req);
        let guard = self.state.read();
        let st = guard.get(&account);
        let s = st.ok_or_else(|| repo_not_found(&name))?;
        if !s.repositories.contains_key(&name) {
            return Err(repo_not_found(&name));
        }
        let mut threads: Vec<Value> = Vec::new();
        for cid in &s.comment_order {
            let Some(c) = s.comments.get(cid) else {
                continue;
            };
            if c.get("_ctxPullRequestId").is_some() {
                continue;
            }
            if c.get("_ctxRepositoryName").and_then(Value::as_str) != Some(name.as_str()) {
                continue;
            }
            if c.get("_ctxAfterCommitId").and_then(Value::as_str) != Some(after.as_str()) {
                continue;
            }
            if let Some(bc) = &before {
                if c.get("_ctxBeforeCommitId").and_then(Value::as_str) != Some(bc.as_str()) {
                    continue;
                }
            }
            threads.push(json!({
                "repositoryName": name,
                "beforeCommitId": before,
                "afterCommitId": after,
                "location": c.get("_ctxLocation").cloned().unwrap_or(Value::Null),
                "comments": [public_comment(c)],
            }));
        }
        ok(json!({ "commentsForComparedCommitData": threads }))
    }

    fn get_comments_for_pull_request(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let pr_id = require(&b, "pullRequestId", "PullRequestIdRequiredException")?;
        let account = self.account(req);
        let guard = self.state.read();
        let st = guard.get(&account);
        let s = st.ok_or_else(pr_not_found)?;
        if !s.pull_requests.contains_key(&pr_id) {
            return Err(pr_not_found());
        }
        let mut threads: Vec<Value> = Vec::new();
        for cid in &s.comment_order {
            let Some(c) = s.comments.get(cid) else {
                continue;
            };
            if c.get("_ctxPullRequestId").and_then(Value::as_str) != Some(pr_id.as_str()) {
                continue;
            }
            threads.push(json!({
                "pullRequestId": pr_id,
                "repositoryName": c.get("_ctxRepositoryName").cloned().unwrap_or(Value::Null),
                "beforeCommitId": c.get("_ctxBeforeCommitId").cloned().unwrap_or(Value::Null),
                "afterCommitId": c.get("_ctxAfterCommitId").cloned().unwrap_or(Value::Null),
                "location": c.get("_ctxLocation").cloned().unwrap_or(Value::Null),
                "comments": [public_comment(c)],
            }));
        }
        ok(json!({ "commentsForPullRequestData": threads }))
    }

    fn update_comment(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let id = require(&b, "commentId", "CommentIdRequiredException")?;
        let content = require(&b, "content", "CommentContentRequiredException")?;
        if content.chars().count() > 10240 {
            return Err(err(
                "CommentContentSizeLimitExceededException",
                "The comment is too large.",
            ));
        }
        let caller = caller_arn(req);
        let account = self.account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let comment = st.comments.get_mut(&id).ok_or_else(|| {
            err(
                "CommentDoesNotExistException",
                "The specified comment does not exist.",
            )
        })?;
        if comment.get("deleted").and_then(Value::as_bool) == Some(true) {
            return Err(err(
                "CommentDeletedException",
                "This comment has already been deleted.",
            ));
        }
        if comment.get("authorArn").and_then(Value::as_str) != Some(caller.as_str()) {
            return Err(err(
                "CommentNotCreatedByCallerException",
                "You cannot modify a comment that was not created by you.",
            ));
        }
        comment["content"] = json!(content);
        comment["lastModifiedDate"] = ts(Utc::now());
        ok(json!({ "comment": public_comment(comment) }))
    }

    fn delete_comment_content(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let id = require(&b, "commentId", "CommentIdRequiredException")?;
        let account = self.account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let comment = st.comments.get_mut(&id).ok_or_else(|| {
            err(
                "CommentDoesNotExistException",
                "The specified comment does not exist.",
            )
        })?;
        comment["deleted"] = json!(true);
        comment["content"] = json!("");
        comment["lastModifiedDate"] = ts(Utc::now());
        ok(json!({ "comment": public_comment(comment) }))
    }

    fn put_comment_reaction(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let id = require(&b, "commentId", "CommentIdRequiredException")?;
        let reaction = require(&b, "reactionValue", "ReactionValueRequiredException")?;
        if !validate::REACTION_EMOJIS.contains(&reaction.as_str())
            && !reaction.starts_with(":")
            && !reaction.starts_with("\\u")
        {
            return Err(err(
                "InvalidReactionValueException",
                "The reaction value is not valid.",
            ));
        }
        let user = caller_arn(req);
        let account = self.account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let comment = st.comments.get_mut(&id).ok_or_else(|| {
            err(
                "CommentDoesNotExistException",
                "The specified comment does not exist.",
            )
        })?;
        if comment.get("deleted").and_then(Value::as_bool) == Some(true) {
            return Err(err(
                "CommentDeletedException",
                "This comment has already been deleted.",
            ));
        }
        let reactions = comment
            .as_object_mut()
            .unwrap()
            .entry("_ctxReactions".to_string())
            .or_insert_with(|| json!({}));
        if let Some(obj) = reactions.as_object_mut() {
            let users = obj.entry(reaction).or_insert_with(|| json!([]));
            if let Some(arr) = users.as_array_mut() {
                if !arr.iter().any(|u| u.as_str() == Some(user.as_str())) {
                    arr.push(json!(user));
                }
            }
        }
        ok(json!({}))
    }

    fn get_comment_reactions(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let id = require(&b, "commentId", "CommentIdRequiredException")?;
        let account = self.account(req);
        let guard = self.state.read();
        let st = guard.get(&account);
        let comment = st.and_then(|s| s.comments.get(&id)).ok_or_else(|| {
            err(
                "CommentDoesNotExistException",
                "The specified comment does not exist.",
            )
        })?;
        let mut reactions = Vec::new();
        if let Some(obj) = comment.get("_ctxReactions").and_then(Value::as_object) {
            for (emoji, users) in obj {
                let user_list: Vec<&str> = users
                    .as_array()
                    .map(|a| a.iter().filter_map(Value::as_str).collect())
                    .unwrap_or_default();
                reactions.push(json!({
                    "reaction": {
                        "emoji": emoji,
                        "shortCode": emoji,
                        "unicode": "",
                    },
                    "reactionUsers": user_list,
                    "reactionsFromDeletedUsersCount": 0,
                }));
            }
        }
        ok(json!({ "reactionsForComment": reactions }))
    }

    // ----- Triggers -----

    fn put_repository_triggers(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = require_repository_name(&b)?;
        let triggers = b
            .get("triggers")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                err(
                    "RepositoryTriggersListRequiredException",
                    "The list of triggers for the repository is required.",
                )
            })?
            .clone();
        for t in &triggers {
            let tname = t.get("name").and_then(Value::as_str);
            if tname.map(str::is_empty).unwrap_or(true) {
                return Err(err(
                    "RepositoryTriggerNameRequiredException",
                    "A name for the trigger is required.",
                ));
            }
            let dest = t.get("destinationArn").and_then(Value::as_str);
            if dest.map(str::is_empty).unwrap_or(true) {
                return Err(err(
                    "RepositoryTriggerDestinationArnRequiredException",
                    "A destination ARN for the target service for the trigger is required.",
                ));
            }
            let events = t.get("events").and_then(Value::as_array);
            match events {
                None => {
                    return Err(err(
                        "RepositoryTriggerEventsListRequiredException",
                        "At least one event for the trigger is required.",
                    ))
                }
                Some(evs) => {
                    for e in evs {
                        if let Some(ev) = e.as_str() {
                            if !validate::is_enum(validate::REPOSITORY_TRIGGER_EVENT, ev) {
                                return Err(err(
                                    "InvalidRepositoryTriggerEventsException",
                                    "One or more of the trigger events is not valid.",
                                ));
                            }
                        }
                    }
                }
            }
        }
        let account = self.account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let repo = st
            .repositories
            .get_mut(&name)
            .ok_or_else(|| repo_not_found(&name))?;
        let config_id = new_uuid();
        repo.triggers = triggers;
        repo.triggers_config_id = config_id.clone();
        ok(json!({ "configurationId": config_id }))
    }

    fn get_repository_triggers(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = require_repository_name(&b)?;
        let account = self.account(req);
        let guard = self.state.read();
        let st = guard.get(&account);
        let repo = st
            .and_then(|s| s.repositories.get(&name))
            .ok_or_else(|| repo_not_found(&name))?;
        let config_id = if repo.triggers_config_id.is_empty() {
            new_uuid()
        } else {
            repo.triggers_config_id.clone()
        };
        ok(json!({
            "configurationId": config_id,
            "triggers": repo.triggers,
        }))
    }

    fn test_repository_triggers(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = require_repository_name(&b)?;
        let triggers = b
            .get("triggers")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                err(
                    "RepositoryTriggersListRequiredException",
                    "The list of triggers is required.",
                )
            })?
            .clone();
        let account = self.account(req);
        let guard = self.state.read();
        let st = guard.get(&account);
        let repo = st
            .and_then(|s| s.repositories.get(&name))
            .ok_or_else(|| repo_not_found(&name))?;
        // Enforce the same structural requirements as PutRepositoryTriggers.
        for t in &triggers {
            if t.get("name")
                .and_then(Value::as_str)
                .map(str::is_empty)
                .unwrap_or(true)
            {
                return Err(err(
                    "RepositoryTriggerNameRequiredException",
                    "A name for the trigger is required.",
                ));
            }
            if t.get("destinationArn")
                .and_then(Value::as_str)
                .map(str::is_empty)
                .unwrap_or(true)
            {
                return Err(err(
                    "RepositoryTriggerDestinationArnRequiredException",
                    "A destination ARN for the target service for the trigger is required.",
                ));
            }
            if t.get("events")
                .and_then(Value::as_array)
                .map(|e| e.is_empty())
                .unwrap_or(true)
            {
                return Err(err(
                    "RepositoryTriggerEventsListRequiredException",
                    "At least one event for the trigger is required.",
                ));
            }
        }
        // A test "executes" each trigger: its destination must resolve to a
        // supported delivery target (SNS topic / Lambda function in this account
        // and region), its events must be valid, and any branch it is scoped to
        // must exist. Triggers that cannot be delivered are reported as failures
        // with a real message rather than a blanket success.
        let mut successful = Vec::new();
        let mut failed = Vec::new();
        for t in &triggers {
            let tname = t.get("name").and_then(Value::as_str).unwrap_or_default();
            let dest = t
                .get("destinationArn")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let mut failure: Option<String> = None;
            if !trigger_destination_ok(dest, &account, &req.region) {
                failure = Some(format!(
                    "Unable to deliver to destination {dest}. A repository trigger destination must be an Amazon SNS topic or an AWS Lambda function in the same account ({account}) and region ({}).",
                    req.region
                ));
            }
            if failure.is_none() {
                if let Some(evs) = t.get("events").and_then(Value::as_array) {
                    for e in evs {
                        if let Some(ev) = e.as_str() {
                            if !validate::is_enum(validate::REPOSITORY_TRIGGER_EVENT, ev) {
                                failure = Some(format!("The trigger event {ev} is not valid."));
                                break;
                            }
                        }
                    }
                }
            }
            if failure.is_none() {
                if let Some(branches) = t.get("branches").and_then(Value::as_array) {
                    for br in branches {
                        if let Some(bn) = br.as_str() {
                            if !repo.branches.contains_key(bn) {
                                failure = Some(format!(
                                    "The branch {bn} specified for the trigger does not exist in the repository."
                                ));
                                break;
                            }
                        }
                    }
                }
            }
            match failure {
                Some(msg) => {
                    failed.push(json!({ "trigger": tname, "failureMessage": msg }));
                }
                None => successful.push(json!(tname)),
            }
        }
        ok(json!({
            "successfulExecutions": successful,
            "failedExecutions": failed,
        }))
    }

    // ----- Tagging -----

    fn tag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let arn = require(&b, "resourceArn", "ResourceArnRequiredException")?;
        if !is_codecommit_arn(&arn) {
            return Err(err(
                "InvalidResourceArnException",
                "The resource ARN is not valid.",
            ));
        }
        let tags = b
            .get("tags")
            .and_then(Value::as_object)
            .ok_or_else(|| err("TagsMapRequiredException", "A map of tags is required."))?;
        let account = self.account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        if let Some(rn) = repo_name_from_arn(&arn) {
            if !st.repositories.contains_key(&rn) {
                return Err(repo_not_found(&rn));
            }
        }
        let entry = st.tags.entry(arn).or_default();
        for (k, v) in tags {
            if let Some(v) = v.as_str() {
                entry.insert(k.clone(), v.to_string());
            }
        }
        ok(json!({}))
    }

    fn untag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let arn = require(&b, "resourceArn", "ResourceArnRequiredException")?;
        if !is_codecommit_arn(&arn) {
            return Err(err(
                "InvalidResourceArnException",
                "The resource ARN is not valid.",
            ));
        }
        let keys = b
            .get("tagKeys")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                err(
                    "TagKeysListRequiredException",
                    "A list of tag keys is required.",
                )
            })?
            .clone();
        let account = self.account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        if let Some(rn) = repo_name_from_arn(&arn) {
            if !st.repositories.contains_key(&rn) {
                return Err(repo_not_found(&rn));
            }
        }
        if let Some(entry) = st.tags.get_mut(&arn) {
            for k in &keys {
                if let Some(k) = k.as_str() {
                    entry.remove(k);
                }
            }
        }
        ok(json!({}))
    }

    fn list_tags_for_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let arn = require(&b, "resourceArn", "ResourceArnRequiredException")?;
        if !is_codecommit_arn(&arn) {
            return Err(err(
                "InvalidResourceArnException",
                "The resource ARN is not valid.",
            ));
        }
        let account = self.account(req);
        let guard = self.state.read();
        let st = guard.get(&account);
        if let Some(rn) = repo_name_from_arn(&arn) {
            if !st
                .map(|s| s.repositories.contains_key(&rn))
                .unwrap_or(false)
            {
                return Err(repo_not_found(&rn));
            }
        }
        let tags = st
            .and_then(|s| s.tags.get(&arn))
            .map(|m| {
                let mut obj = Map::new();
                for (k, v) in m {
                    obj.insert(k.clone(), json!(v));
                }
                Value::Object(obj)
            })
            .unwrap_or_else(|| json!({}));
        ok(json!({ "tags": tags }))
    }

    // ----- Shared mutation helpers -----

    fn mutate_pr(
        &self,
        req: &AwsRequest,
        id: &str,
        f: impl FnOnce(&mut Value) -> Result<(), AwsServiceError>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let account = self.account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let pr = st.pull_requests.get_mut(id).ok_or_else(pr_not_found)?;
        f(pr)?;
        let pr = pr.clone();
        ok(json!({ "pullRequest": pr }))
    }

    fn mutate_template(
        &self,
        req: &AwsRequest,
        name: &str,
        f: impl FnOnce(&mut Value) -> Result<(), AwsServiceError>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let account = self.account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let t = st.templates.get_mut(name).ok_or_else(template_not_found)?;
        f(t)?;
        let t = t.clone();
        ok(json!({ "approvalRuleTemplate": t }))
    }
}

// ---------------------------------------------------------------------------
// Error constructors for the service-wide "does not exist" responses.
// ---------------------------------------------------------------------------

fn repo_not_found(name: &str) -> AwsServiceError {
    err(
        "RepositoryDoesNotExistException",
        format!("The repository {name} does not exist."),
    )
}

fn pr_not_found() -> AwsServiceError {
    err(
        "PullRequestDoesNotExistException",
        "The specified pull request does not exist.",
    )
}

fn template_not_found() -> AwsServiceError {
    err(
        "ApprovalRuleTemplateDoesNotExistException",
        "The specified approval rule template does not exist.",
    )
}

// ---------------------------------------------------------------------------
// Git / object-store helpers
// ---------------------------------------------------------------------------

/// AWS CodeCommit canonicalizes approval-rule (template) content to compact
/// JSON with no insignificant whitespace, preserving member order exactly (it
/// does not reorder or re-encode values). This strips whitespace that falls
/// outside string literals -- equivalent to AWS's normalization -- without
/// round-tripping through a map (which would reorder keys). A payload that does
/// not parse as JSON is returned unchanged.
fn normalize_rule_content(content: &str) -> String {
    if serde_json::from_str::<Value>(content).is_err() {
        return content.to_string();
    }
    let mut out = String::with_capacity(content.len());
    let mut in_string = false;
    let mut escaped = false;
    for c in content.chars() {
        if in_string {
            out.push(c);
            if escaped {
                escaped = false;
            } else if c == '\\' {
                escaped = true;
            } else if c == '"' {
                in_string = false;
            }
        } else if c == '"' {
            in_string = true;
            out.push(c);
        } else if !c.is_whitespace() {
            out.push(c);
        }
    }
    out
}

fn sha256_hex(data: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(data);
    format!("{:x}", h.finalize())
}

/// Whether an approval-pool member pattern matches an approver ARN. Pool members
/// may be exact ARNs or contain `*` wildcards (e.g. an assumed-role session
/// glob), matching CodeCommit's pool semantics.
fn pool_matches(pattern: &str, arn: &str) -> bool {
    if !pattern.contains('*') {
        return pattern == arn;
    }
    let parts: Vec<&str> = pattern.split('*').collect();
    let mut pos = 0usize;
    for (i, part) in parts.iter().enumerate() {
        if part.is_empty() {
            continue;
        }
        if i == 0 {
            if !arn[pos..].starts_with(part) {
                return false;
            }
            pos += part.len();
        } else if i == parts.len() - 1 {
            if !arn[pos..].ends_with(part) {
                return false;
            }
        } else if let Some(idx) = arn[pos..].find(part) {
            pos += idx + part.len();
        } else {
            return false;
        }
    }
    true
}

/// Whether an approval rule's content is satisfied by the given set of approving
/// reviewer ARNs. Each `Approvers` statement requires `NumberOfApprovalsNeeded`
/// distinct approvals drawn from its `ApprovalPoolMembers` (any approver when no
/// pool is specified); the rule is satisfied only when every statement is.
fn rule_satisfied(content: &str, approvers: &std::collections::BTreeSet<String>) -> bool {
    let Ok(v) = serde_json::from_str::<Value>(content) else {
        return !approvers.is_empty();
    };
    let Some(statements) = v.get("Statements").and_then(Value::as_array) else {
        return !approvers.is_empty();
    };
    if statements.is_empty() {
        return true;
    }
    for stmt in statements {
        let needed = stmt
            .get("NumberOfApprovalsNeeded")
            .and_then(Value::as_i64)
            .unwrap_or(1)
            .max(0) as usize;
        let pool = stmt.get("ApprovalPoolMembers").and_then(Value::as_array);
        let count = match pool {
            Some(pool) if !pool.is_empty() => approvers
                .iter()
                .filter(|a| {
                    pool.iter()
                        .filter_map(Value::as_str)
                        .any(|pat| pool_matches(pat, a))
                })
                .count(),
            _ => approvers.len(),
        };
        if count < needed {
            return false;
        }
    }
    true
}

/// Set the repository's default branch to its sole branch if none is set yet.
fn set_default_if_absent(repo: &mut Repo) {
    let has_default = repo
        .metadata
        .get("defaultBranch")
        .and_then(Value::as_str)
        .map(|s| !s.is_empty())
        .unwrap_or(false);
    if !has_default {
        if let Some(branch) = repo.branches.keys().next().cloned() {
            if let Some(obj) = repo.metadata.as_object_mut() {
                obj.insert("defaultBranch".into(), json!(branch));
            }
        }
    }
}

/// Build a `Commit`-shaped JSON value.
fn build_commit(
    commit_id: &str,
    tree_id: &str,
    parents: &[String],
    b: &Value,
    req: &AwsRequest,
) -> Value {
    let now = Utc::now();
    let date = format!("{} +0000", now.timestamp());
    let name = str_field(b, "authorName")
        .or_else(|| str_field(b, "name"))
        .unwrap_or_else(|| caller_arn(req));
    let email = str_field(b, "email").unwrap_or_default();
    let user = json!({ "name": name, "email": email, "date": date });
    json!({
        "commitId": commit_id,
        "treeId": tree_id,
        "parents": parents,
        "message": str_field(b, "commitMessage").unwrap_or_default(),
        "author": user,
        "committer": user,
        "additionalData": "",
    })
}

/// The parent list for a merge commit given the merge option. `SQUASH_MERGE`
/// collapses the source into a single-parent commit on the destination; every
/// other option (`THREE_WAY_MERGE`) records both tips. Shared by
/// `MergeBranchesBy*`, `MergePullRequestBy*`, and `CreateUnreferencedMergeCommit`
/// so the commit shape can never drift between them.
fn merge_parents(merge_option: &str, dest: &str, source: &str) -> Vec<String> {
    if merge_option == "SQUASH_MERGE" {
        vec![dest.to_string()]
    } else {
        vec![dest.to_string(), source.to_string()]
    }
}

/// Validate the optional `conflictDetailLevel` and `conflictResolutionStrategy`
/// members shared by every merge operation, returning the declared error when
/// either is not a modeled enum value.
fn validate_merge_conflict_enums(b: &Value) -> Result<(), AwsServiceError> {
    check_enum(
        b,
        "conflictDetailLevel",
        validate::CONFLICT_DETAIL_LEVEL,
        "InvalidConflictDetailLevelException",
    )?;
    check_enum(
        b,
        "conflictResolutionStrategy",
        validate::CONFLICT_RESOLUTION_STRATEGY,
        "InvalidConflictResolutionStrategyException",
    )?;
    Ok(())
}

/// After a branch tip moves, every OPEN pull request that uses that branch as
/// its source reference gets a fresh `revisionId`. Because approvals are stored
/// per-revision and only the current revision is ever counted, this invalidates
/// approvals cast against the prior source commit -- matching CodeCommit, where
/// a new source commit resets the review so unreviewed code can never be merged
/// on stale approvals.
fn refresh_source_revisions(st: &mut CodeCommitState, repo_name: &str, branch: &str) {
    let now = Utc::now();
    for pr in st.pull_requests.values_mut() {
        if pr.get("pullRequestStatus").and_then(Value::as_str) != Some("OPEN") {
            continue;
        }
        let is_source = pr
            .get("pullRequestTargets")
            .and_then(Value::as_array)
            .map(|ts| {
                ts.iter().any(|t| {
                    t.get("repositoryName").and_then(Value::as_str) == Some(repo_name)
                        && t.get("sourceReference").and_then(Value::as_str) == Some(branch)
                })
            })
            .unwrap_or(false);
        if is_source {
            pr["revisionId"] = json!(fresh_object_id());
            pr["lastActivityDate"] = ts(now);
        }
    }
}

/// Return a copy of a pull request whose OPEN targets have their
/// `sourceCommit`/`destinationCommit`/`mergeBase` recomputed from the current
/// branch tips, so a read never reports commits that diverge from what a merge
/// would actually use. Closed/merged pull requests are returned unchanged
/// (their target commits are frozen at merge time).
fn refresh_pr_targets(st: &CodeCommitState, pr: &Value) -> Value {
    let mut pr = pr.clone();
    if pr.get("pullRequestStatus").and_then(Value::as_str) != Some("OPEN") {
        return pr;
    }
    if let Some(targets) = pr
        .get_mut("pullRequestTargets")
        .and_then(Value::as_array_mut)
    {
        for t in targets.iter_mut() {
            let Some(repo_name) = t
                .get("repositoryName")
                .and_then(Value::as_str)
                .map(str::to_string)
            else {
                continue;
            };
            let src_ref = t
                .get("sourceReference")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let dst_ref = t
                .get("destinationReference")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let Some(repo) = st.repositories.get(&repo_name) else {
                continue;
            };
            let src = repo.branches.get(&src_ref).cloned();
            let dst = repo.branches.get(&dst_ref).cloned();
            if let Some(s) = &src {
                t["sourceCommit"] = json!(s);
            }
            if let Some(d) = &dst {
                t["destinationCommit"] = json!(d);
            }
            if let (Some(s), Some(d)) = (&src, &dst) {
                t["mergeBase"] = match merge_base(repo, s, d) {
                    Some(base) => json!(base),
                    None => Value::Null,
                };
            }
        }
    }
    pr
}

/// Whether a repository-trigger `destinationArn` resolves to a delivery target
/// CodeCommit supports: an Amazon SNS topic or an AWS Lambda function in the
/// caller's own account and region. (CodeCommit triggers publish only to SNS or
/// Lambda; a cross-account, cross-region, or other-service ARN is undeliverable
/// and is reported as a failed execution rather than a fake success.)
fn trigger_destination_ok(arn: &str, account: &str, region: &str) -> bool {
    // arn:aws:<service>:<region>:<account>:<resource...>
    let parts: Vec<&str> = arn.splitn(6, ':').collect();
    if parts.len() != 6 || parts[0] != "arn" {
        return false;
    }
    let service = parts[2];
    if service != "sns" && service != "lambda" {
        return false;
    }
    if parts[3] != region || parts[4] != account {
        return false;
    }
    !parts[5].is_empty()
}

/// Resolve a commit specifier member (branch name or 40-hex commit id) to a
/// concrete commit id, defaulting to the repository's default-branch tip when
/// the member is absent.
fn resolve_commit(repo: &Repo, b: &Value, key: &str) -> Result<String, AwsServiceError> {
    let spec = match str_field(b, key) {
        Some(s) => s,
        None => repo
            .metadata
            .get("defaultBranch")
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| {
                err(
                    "CommitDoesNotExistException",
                    "The specified commit does not exist or no default branch is set.",
                )
            })?,
    };
    if let Some(tip) = repo.branches.get(&spec) {
        return Ok(tip.clone());
    }
    if is_object_id(&spec) && repo.commits.contains_key(&spec) {
        return Ok(spec);
    }
    Err(err(
        "CommitDoesNotExistException",
        "The specified commit does not exist.",
    ))
}

/// The set of a commit's ancestors (inclusive of the commit itself).
fn ancestors(repo: &Repo, commit: &str) -> std::collections::BTreeSet<String> {
    let mut seen = std::collections::BTreeSet::new();
    let mut stack = vec![commit.to_string()];
    while let Some(c) = stack.pop() {
        if !seen.insert(c.clone()) {
            continue;
        }
        if let Some(commit) = repo.commits.get(&c) {
            if let Some(parents) = commit.get("parents").and_then(Value::as_array) {
                for p in parents {
                    if let Some(pid) = p.as_str() {
                        stack.push(pid.to_string());
                    }
                }
            }
        }
    }
    seen
}

/// Whether `ancestor` is an ancestor of (or equal to) `descendant`.
fn is_ancestor(repo: &Repo, ancestor: &str, descendant: &str) -> bool {
    ancestors(repo, descendant).contains(ancestor)
}

/// The merge base (lowest common ancestor) of two commits over the parent DAG,
/// if any. Handles merge commits (multiple parents) correctly.
///
/// Linear in the size of the ancestor graph: an ancestor of `a` is a *lowest*
/// common ancestor exactly when it is the common ancestor nearest `a` (fewest
/// hops). Any deeper common ancestor is necessarily an ancestor of a nearer one,
/// so the minimum-distance common ancestor is never above another candidate. A
/// criss-cross history that admits several bases resolves deterministically via
/// the lexical tie-break.
fn merge_base(repo: &Repo, a: &str, b: &str) -> Option<String> {
    let depth_a = bfs_depths(repo, a);
    let anc_b = ancestors(repo, b);
    depth_a
        .iter()
        .filter(|(c, _)| anc_b.contains(*c))
        .min_by(|(cx, dx), (cy, dy)| dx.cmp(dy).then_with(|| cx.cmp(cy)))
        .map(|(c, _)| c.clone())
}

/// Minimum hop distance from `start` to each of its ancestors over the parent
/// DAG (breadth-first by generation).
fn bfs_depths(repo: &Repo, start: &str) -> std::collections::BTreeMap<String, usize> {
    let mut depths = std::collections::BTreeMap::new();
    let mut frontier = vec![start.to_string()];
    let mut depth = 0usize;
    while !frontier.is_empty() {
        let mut next = Vec::new();
        for c in frontier {
            if depths.contains_key(&c) {
                continue;
            }
            depths.insert(c.clone(), depth);
            if let Some(commit) = repo.commits.get(&c) {
                if let Some(parents) = commit.get("parents").and_then(Value::as_array) {
                    for p in parents {
                        if let Some(pid) = p.as_str() {
                            if !depths.contains_key(pid) {
                                next.push(pid.to_string());
                            }
                        }
                    }
                }
            }
        }
        frontier = next;
        depth += 1;
    }
    depths
}

/// Whether two optional tree entries are identical (same blob id and file mode).
/// A missing entry (a deletion) is only equal to another missing entry.
fn entries_equal(x: Option<&FileEntry>, y: Option<&FileEntry>) -> bool {
    match (x, y) {
        (None, None) => true,
        (Some(a), Some(b)) => a.blob_id == b.blob_id && a.mode == b.mode,
        _ => false,
    }
}

/// The three-way resolution of a single path given its base, source, and
/// destination entries. `Take(None)` means the path is deleted in the result.
enum Resolved {
    Take(Option<FileEntry>),
    Conflict,
}

/// Real git/CodeCommit three-way resolution for one path. Additions,
/// modifications, and deletions on either side all resolve when only one side
/// changed relative to the base; a genuine two-sided divergence is a conflict.
fn merge_entry(
    base: Option<&FileEntry>,
    source: Option<&FileEntry>,
    dest: Option<&FileEntry>,
) -> Resolved {
    if entries_equal(source, dest) {
        // Both sides agree (including both deleting the file).
        Resolved::Take(source.cloned())
    } else if entries_equal(source, base) {
        // Source unchanged relative to base -> take destination's version
        // (which may itself be a deletion).
        Resolved::Take(dest.cloned())
    } else if entries_equal(dest, base) {
        // Destination unchanged relative to base -> take source's version
        // (which may itself be a deletion).
        Resolved::Take(source.cloned())
    } else {
        Resolved::Conflict
    }
}

/// A materialized working tree: path -> entry.
type Tree = std::collections::BTreeMap<String, FileEntry>;

/// The base/source/destination trees for a merge (empty maps when a commit has
/// no materialized tree), along with the computed base commit id.
fn merge_inputs<'a>(
    repo: &'a Repo,
    source: &str,
    dest: &str,
    empty: &'a Tree,
) -> (Option<String>, &'a Tree, &'a Tree, &'a Tree) {
    let base = merge_base(repo, source, dest);
    let base_tree = base
        .as_ref()
        .and_then(|b| repo.trees.get(b))
        .unwrap_or(empty);
    let src_tree = repo.trees.get(source).unwrap_or(empty);
    let dst_tree = repo.trees.get(dest).unwrap_or(empty);
    (base, base_tree, src_tree, dst_tree)
}

/// The union of all paths appearing in the base, source, and destination trees.
fn all_merge_paths(
    base_tree: &std::collections::BTreeMap<String, FileEntry>,
    src_tree: &std::collections::BTreeMap<String, FileEntry>,
    dst_tree: &std::collections::BTreeMap<String, FileEntry>,
) -> std::collections::BTreeSet<String> {
    let mut all: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    all.extend(base_tree.keys().cloned());
    all.extend(src_tree.keys().cloned());
    all.extend(dst_tree.keys().cloned());
    all
}

/// Paths that genuinely conflict between the given base/source/destination trees
/// (both sides changed the same path differently). Callers that already hold the
/// three trees pass them here to avoid recomputing the merge base.
fn conflicting_paths_in(base_tree: &Tree, src_tree: &Tree, dst_tree: &Tree) -> Vec<String> {
    let mut conflicts = Vec::new();
    for path in all_merge_paths(base_tree, src_tree, dst_tree) {
        if let Resolved::Conflict = merge_entry(
            base_tree.get(&path),
            src_tree.get(&path),
            dst_tree.get(&path),
        ) {
            conflicts.push(path);
        }
    }
    conflicts
}

/// Produce a merged working tree for merging source into destination using a
/// real three-way merge (per-path, relative to the merge base). Returns `None`
/// when there is an unresolved content conflict and no accept-source /
/// accept-destination strategy is chosen.
fn merge_trees(
    repo: &Repo,
    source: &str,
    dest: &str,
    strategy: &str,
) -> Option<std::collections::BTreeMap<String, FileEntry>> {
    let empty = std::collections::BTreeMap::new();
    let (_, base_tree, src_tree, dst_tree) = merge_inputs(repo, source, dest, &empty);
    let mut merged: std::collections::BTreeMap<String, FileEntry> =
        std::collections::BTreeMap::new();
    for path in all_merge_paths(base_tree, src_tree, dst_tree) {
        let s = src_tree.get(&path);
        let d = dst_tree.get(&path);
        let entry = match merge_entry(base_tree.get(&path), s, d) {
            Resolved::Take(e) => e,
            Resolved::Conflict => match strategy {
                "ACCEPT_SOURCE" => s.cloned(),
                "ACCEPT_DESTINATION" => d.cloned(),
                _ => return None,
            },
        };
        if let Some(e) = entry {
            merged.insert(path, e);
        }
    }
    Some(merged)
}

/// The number of lines in a byte buffer (for merge-hunk line ranges).
fn line_count(bytes: &[u8]) -> usize {
    if bytes.is_empty() {
        return 0;
    }
    let mut n = bytes.iter().filter(|&&b| b == b'\n').count();
    if *bytes.last().unwrap() != b'\n' {
        n += 1;
    }
    n
}

/// Decode a tree entry's stored (base64) blob content to raw bytes.
fn entry_bytes(repo: &Repo, entry: Option<&FileEntry>) -> Vec<u8> {
    let Some(e) = entry else { return Vec::new() };
    let Some(b64) = repo.blobs.get(&e.blob_id) else {
        return Vec::new();
    };
    base64::engine::general_purpose::STANDARD
        .decode(b64.as_bytes())
        .unwrap_or_else(|_| b64.clone().into_bytes())
}

/// One side of a merge hunk (`source`/`destination`/`base`), carrying the line
/// range and the base64 content for already-decoded bytes (decoded once by the
/// caller so a side's blob is never re-decoded).
fn hunk_side(bytes: &[u8]) -> Value {
    let lines = line_count(bytes);
    json!({
        "startLine": if lines == 0 { 0 } else { 1 },
        "endLine": lines,
        "hunkContent": base64::engine::general_purpose::STANDARD.encode(bytes),
    })
}

/// The change kind of one side relative to the base: `A`dded, `M`odified,
/// `D`eleted, or none when unchanged.
fn merge_operation(base: Option<&FileEntry>, side: Option<&FileEntry>) -> Option<&'static str> {
    match (base, side) {
        (None, Some(_)) => Some("A"),
        (Some(_), None) => Some("D"),
        (Some(_), Some(_)) if !entries_equal(base, side) => Some("M"),
        _ => None,
    }
}

/// Build the real `ConflictMetadata` + `mergeHunks` for one path between source
/// and destination relative to their base. Used by `DescribeMergeConflicts`,
/// `BatchDescribeMergeConflicts`, and (for the list form) `GetMergeConflicts`,
/// so every conflict-reporting operation agrees.
fn conflict_metadata(
    repo: &Repo,
    source: &str,
    dest: &str,
    base_tree: &std::collections::BTreeMap<String, FileEntry>,
    src_tree: &std::collections::BTreeMap<String, FileEntry>,
    dst_tree: &std::collections::BTreeMap<String, FileEntry>,
    path: &str,
) -> (Value, Vec<Value>) {
    let _ = (source, dest);
    let b = base_tree.get(path);
    let s = src_tree.get(path);
    let d = dst_tree.get(path);
    // Decode each side's blob exactly once and reuse for sizes, binary
    // detection, and hunk content.
    let sb = entry_bytes(repo, s);
    let db = entry_bytes(repo, d);
    let bb = entry_bytes(repo, b);
    let is_conflict = matches!(merge_entry(b, s, d), Resolved::Conflict);
    let content_conflict = is_conflict;
    let file_mode_conflict = match (b, s, d) {
        (base, Some(se), Some(de)) => {
            se.mode != de.mode
                && base.map(|e| e.mode.as_str()) != Some(se.mode.as_str())
                && base.map(|e| e.mode.as_str()) != Some(de.mode.as_str())
        }
        _ => false,
    };
    let mode_of = |e: Option<&FileEntry>| e.map(|x| git_mode_to_file(&x.mode)).unwrap_or("NORMAL");
    let mut merge_operations = Map::new();
    if let Some(o) = merge_operation(b, s) {
        merge_operations.insert("source".into(), json!(o));
    }
    if let Some(o) = merge_operation(b, d) {
        merge_operations.insert("destination".into(), json!(o));
    }
    let number_of_conflicts = i64::from(is_conflict);
    let metadata = json!({
        "filePath": path,
        "fileSizes": {
            "source": sb.len(),
            "destination": db.len(),
            "base": bb.len(),
        },
        "fileModes": {
            "source": mode_of(s),
            "destination": mode_of(d),
            "base": mode_of(b),
        },
        "objectTypes": {
            "source": if s.is_some() { "FILE" } else { "" },
            "destination": if d.is_some() { "FILE" } else { "" },
            "base": if b.is_some() { "FILE" } else { "" },
        },
        "numberOfConflicts": number_of_conflicts,
        "isBinaryFile": {
            "source": is_binary(&sb),
            "destination": is_binary(&db),
            "base": is_binary(&bb),
        },
        "contentConflict": content_conflict,
        "fileModeConflict": file_mode_conflict,
        "objectTypeConflict": false,
        "mergeOperations": Value::Object(merge_operations),
    });
    // Emit a merge hunk whenever the three sides are not all identical, marking
    // the hunk as a conflict only for a genuine two-sided divergence.
    let mut hunks = Vec::new();
    if !(entries_equal(s, d) && entries_equal(s, b)) {
        hunks.push(json!({
            "isConflict": is_conflict,
            "source": hunk_side(&sb),
            "destination": hunk_side(&db),
            "base": hunk_side(&bb),
        }));
    }
    (metadata, hunks)
}

/// Whether a blob looks binary (contains a NUL byte), as CodeCommit reports.
fn is_binary(bytes: &[u8]) -> bool {
    bytes.contains(&0)
}

/// Build a `PullRequestEvent`-shaped JSON value.
fn pr_event(
    pr_id: &str,
    event_type: &str,
    actor: &str,
    date: DateTime<Utc>,
    metadata: Value,
) -> Value {
    let mut e = Map::new();
    e.insert("pullRequestId".into(), json!(pr_id));
    e.insert("eventDate".into(), ts(date));
    e.insert("pullRequestEventType".into(), json!(event_type));
    e.insert("actorArn".into(), json!(actor));
    if let Some(obj) = metadata.as_object() {
        if !obj.is_empty() && event_type == "PULL_REQUEST_CREATED" {
            e.insert("pullRequestCreatedEventMetadata".into(), metadata);
        }
    }
    Value::Object(e)
}

/// Build a `Comment`-shaped JSON value.
fn comment_value(
    comment_id: &str,
    content: &str,
    author: &str,
    date: DateTime<Utc>,
    in_reply_to: Option<&str>,
) -> Value {
    let mut c = Map::new();
    c.insert("commentId".into(), json!(comment_id));
    c.insert("content".into(), json!(content));
    if let Some(r) = in_reply_to {
        c.insert("inReplyTo".into(), json!(r));
    }
    c.insert("creationDate".into(), ts(date));
    c.insert("lastModifiedDate".into(), ts(date));
    c.insert("authorArn".into(), json!(author));
    c.insert("deleted".into(), json!(false));
    c.insert("callerReactions".into(), json!([]));
    c.insert("reactionCounts".into(), json!({}));
    Value::Object(c)
}

#[cfg(test)]
mod normalize_tests {
    use super::normalize_rule_content;

    #[test]
    fn compacts_and_preserves_member_order() {
        let input = "  {\n\t  \"Version\": \"2018-11-08\",\n\t  \"DestinationReferences\": [\"refs/heads/master\"],\n\t  \"Statements\": []\n  }\n";
        let out = normalize_rule_content(input);
        assert_eq!(
            out,
            "{\"Version\":\"2018-11-08\",\"DestinationReferences\":[\"refs/heads/master\"],\"Statements\":[]}"
        );
    }
}

#[cfg(test)]
mod engine_tests {
    use super::*;

    fn entry(id: &str) -> FileEntry {
        FileEntry {
            blob_id: id.to_string(),
            mode: "100644".to_string(),
        }
    }

    fn commit(parents: &[&str]) -> Value {
        json!({ "parents": parents, "treeId": "t" })
    }

    // Defect 3: merge_base is the lowest common ancestor, not any ancestor.
    #[test]
    fn merge_base_is_lowest_common_ancestor() {
        // root <- X <- A ; and B is a merge commit whose parents are [X, root].
        // The common ancestors of A and B are {X, root}; the LCA is X. A
        // depth-first walk that returns the first common ancestor would wrongly
        // report root.
        let mut repo = Repo::default();
        repo.commits.insert("root".into(), commit(&[]));
        repo.commits.insert("X".into(), commit(&["root"]));
        repo.commits.insert("A".into(), commit(&["X"]));
        repo.commits.insert("B".into(), commit(&["X", "root"]));
        assert_eq!(merge_base(&repo, "A", "B"), Some("X".to_string()));
        assert_eq!(merge_base(&repo, "B", "A"), Some("X".to_string()));
    }

    #[test]
    fn merge_base_of_a_diamond_is_the_fork_point() {
        // root <- L <- A ; root <- R <- B. LCA(A, B) == root.
        let mut repo = Repo::default();
        repo.commits.insert("root".into(), commit(&[]));
        repo.commits.insert("L".into(), commit(&["root"]));
        repo.commits.insert("R".into(), commit(&["root"]));
        repo.commits.insert("A".into(), commit(&["L"]));
        repo.commits.insert("B".into(), commit(&["R"]));
        assert_eq!(merge_base(&repo, "A", "B"), Some("root".to_string()));
    }

    // Defect 2: a file deleted on the source branch is removed by the merge.
    #[test]
    fn three_way_merge_propagates_source_deletion() {
        let mut repo = Repo::default();
        repo.commits.insert("base".into(), commit(&[]));
        repo.commits.insert("src".into(), commit(&["base"]));
        repo.commits.insert("dst".into(), commit(&["base"]));
        // base has a.txt + b.txt.
        let mut base = Tree::new();
        base.insert("a.txt".into(), entry("a1"));
        base.insert("b.txt".into(), entry("b1"));
        repo.trees.insert("base".into(), base);
        // source deletes b.txt.
        let mut src = Tree::new();
        src.insert("a.txt".into(), entry("a1"));
        repo.trees.insert("src".into(), src);
        // destination is unchanged w.r.t. b.txt but adds c.txt.
        let mut dst = Tree::new();
        dst.insert("a.txt".into(), entry("a1"));
        dst.insert("b.txt".into(), entry("b1"));
        dst.insert("c.txt".into(), entry("c1"));
        repo.trees.insert("dst".into(), dst);

        let merged = merge_trees(&repo, "src", "dst", "").expect("mergeable");
        assert!(!merged.contains_key("b.txt"), "source deletion must win");
        assert!(merged.contains_key("a.txt"));
        assert!(merged.contains_key("c.txt"));
    }

    #[test]
    fn three_way_merge_flags_true_conflict() {
        let mut repo = Repo::default();
        repo.commits.insert("base".into(), commit(&[]));
        repo.commits.insert("src".into(), commit(&["base"]));
        repo.commits.insert("dst".into(), commit(&["base"]));
        let mut base = Tree::new();
        base.insert("a.txt".into(), entry("a1"));
        repo.trees.insert("base".into(), base);
        let mut src = Tree::new();
        src.insert("a.txt".into(), entry("a-src"));
        repo.trees.insert("src".into(), src);
        let mut dst = Tree::new();
        dst.insert("a.txt".into(), entry("a-dst"));
        repo.trees.insert("dst".into(), dst);
        // Both sides changed a.txt differently -> unresolved conflict.
        assert!(merge_trees(&repo, "src", "dst", "").is_none());
        let empty = Tree::new();
        let (_, bt, st, dt) = merge_inputs(&repo, "src", "dst", &empty);
        assert_eq!(conflicting_paths_in(bt, st, dt), vec!["a.txt"]);
        // A strategy resolves it deterministically.
        let merged = merge_trees(&repo, "src", "dst", "ACCEPT_SOURCE").expect("resolved");
        assert_eq!(merged.get("a.txt").unwrap().blob_id, "a-src");
    }
}

#[cfg(test)]
mod handler_tests {
    use super::*;
    use bytes::Bytes;
    use fakecloud_core::auth::{Principal, PrincipalType};
    use fakecloud_core::multi_account::MultiAccountState;
    use http::{HeaderMap, Method};
    use parking_lot::RwLock;
    use std::collections::HashMap;

    fn svc() -> CodeCommitService {
        CodeCommitService::new(Arc::new(RwLock::new(MultiAccountState::new(
            "000000000000",
            "us-east-1",
            "",
        ))))
    }

    fn req_as(action: &str, body: Value, arn: Option<&str>) -> AwsRequest {
        let principal = arn.map(|a| Principal {
            arn: a.to_string(),
            user_id: "AIDATEST".to_string(),
            account_id: "000000000000".to_string(),
            principal_type: PrincipalType::User,
            source_identity: None,
            tags: None,
        });
        AwsRequest {
            service: "codecommit".into(),
            action: action.into(),
            region: "us-east-1".into(),
            account_id: "000000000000".into(),
            request_id: "req".into(),
            headers: HeaderMap::new(),
            query_params: HashMap::new(),
            body: Bytes::from(serde_json::to_vec(&body).unwrap()),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: vec![],
            raw_path: String::new(),
            raw_query: String::new(),
            method: Method::POST,
            is_query_protocol: false,
            access_key_id: None,
            principal,
        }
    }

    fn call(s: &CodeCommitService, action: &str, body: Value) -> Value {
        let resp = s
            .dispatch(action, &req_as(action, body, None))
            .expect("op ok");
        serde_json::from_slice(resp.body.expect_bytes()).unwrap()
    }

    fn call_as(s: &CodeCommitService, action: &str, body: Value, arn: &str) -> Value {
        let resp = s
            .dispatch(action, &req_as(action, body, Some(arn)))
            .expect("op ok");
        serde_json::from_slice(resp.body.expect_bytes()).unwrap()
    }

    fn call_err(s: &CodeCommitService, action: &str, body: Value) -> AwsServiceError {
        s.dispatch(action, &req_as(action, body, None))
            .err()
            .expect("op err")
    }

    fn b64(s: &str) -> String {
        base64::engine::general_purpose::STANDARD.encode(s.as_bytes())
    }

    /// Create a repository and return nothing; the caller uses "repo".
    fn make_repo(s: &CodeCommitService) {
        call(s, "CreateRepository", json!({ "repositoryName": "repo" }));
    }

    /// Put a file on a branch, returning the new commit id.
    fn put(
        s: &CodeCommitService,
        branch: &str,
        path: &str,
        content: &str,
        parent: Option<&str>,
    ) -> String {
        let mut body = json!({
            "repositoryName": "repo",
            "branchName": branch,
            "filePath": path,
            "fileContent": b64(content),
        });
        if let Some(p) = parent {
            body["parentCommitId"] = json!(p);
        }
        call(s, "PutFile", body)["commitId"]
            .as_str()
            .unwrap()
            .to_string()
    }

    /// A repo with `main` and `feature` diverging non-conflictingly from a
    /// shared base commit. Returns (source_tip, dest_tip).
    fn diverged_repo(s: &CodeCommitService) -> (String, String) {
        make_repo(s);
        let c1 = put(s, "main", "a.txt", "base", None);
        call(
            s,
            "CreateBranch",
            json!({ "repositoryName": "repo", "branchName": "feature", "commitId": c1 }),
        );
        let feature_tip = put(s, "feature", "b.txt", "from-feature", Some(&c1));
        let main_tip = put(s, "main", "c.txt", "from-main", Some(&c1));
        (feature_tip, main_tip)
    }

    fn create_pr(s: &CodeCommitService, dest: &str) -> Value {
        call(
            s,
            "CreatePullRequest",
            json!({
                "title": "PR",
                "targets": [{
                    "repositoryName": "repo",
                    "sourceReference": "feature",
                    "destinationReference": dest,
                }],
            }),
        )["pullRequest"]
            .clone()
    }

    // Defect 1: MergePullRequestByThreeWay advances the destination branch and
    // the returned mergeCommitId is a real, retrievable commit.
    #[test]
    fn merge_pr_three_way_advances_branch_and_writes_commit() {
        let s = svc();
        diverged_repo(&s);
        let pr = create_pr(&s, "main");
        let pr_id = pr["pullRequestId"].as_str().unwrap().to_string();

        let out = call(
            &s,
            "MergePullRequestByThreeWay",
            json!({ "pullRequestId": pr_id, "repositoryName": "repo" }),
        );
        let target = &out["pullRequest"]["pullRequestTargets"][0];
        assert_eq!(target["mergeMetadata"]["isMerged"], json!(true));
        let merge_commit = target["mergeMetadata"]["mergeCommitId"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(is_object_id(&merge_commit));
        assert_eq!(out["pullRequest"]["pullRequestStatus"], json!("CLOSED"));

        // Destination branch now points at the merge commit.
        let branch = call(
            &s,
            "GetBranch",
            json!({ "repositoryName": "repo", "branchName": "main" }),
        );
        assert_eq!(branch["branch"]["commitId"], json!(merge_commit));

        // GetCommit on the merge commit succeeds and it has two parents.
        let got = call(
            &s,
            "GetCommit",
            json!({ "repositoryName": "repo", "commitId": merge_commit }),
        );
        assert_eq!(got["commit"]["parents"].as_array().unwrap().len(), 2);
    }

    // Defect 1/2: a squash merge that deletes a file on source removes it from
    // the merged destination tree, and the squash commit has a single parent.
    #[test]
    fn merge_pr_squash_deletes_file_and_has_one_parent() {
        let s = svc();
        make_repo(&s);
        let c1 = put(&s, "main", "a.txt", "a", None);
        let c2 = put(&s, "main", "b.txt", "b", Some(&c1));
        call(
            &s,
            "CreateBranch",
            json!({ "repositoryName": "repo", "branchName": "feature", "commitId": c2 }),
        );
        // feature deletes b.txt; main advances independently.
        call(
            &s,
            "DeleteFile",
            json!({ "repositoryName": "repo", "branchName": "feature", "filePath": "b.txt", "parentCommitId": c2 }),
        );
        put(&s, "main", "c.txt", "c", Some(&c2));

        let pr = create_pr(&s, "main");
        let pr_id = pr["pullRequestId"].as_str().unwrap().to_string();
        let out = call(
            &s,
            "MergePullRequestBySquash",
            json!({ "pullRequestId": pr_id, "repositoryName": "repo" }),
        );
        let merge_commit = out["pullRequest"]["pullRequestTargets"][0]["mergeMetadata"]
            ["mergeCommitId"]
            .as_str()
            .unwrap()
            .to_string();
        // Squash => single parent.
        let got = call(
            &s,
            "GetCommit",
            json!({ "repositoryName": "repo", "commitId": merge_commit }),
        );
        assert_eq!(got["commit"]["parents"].as_array().unwrap().len(), 1);
        // b.txt is gone on the destination branch after merge.
        let err = call_err(
            &s,
            "GetFile",
            json!({ "repositoryName": "repo", "commitSpecifier": "main", "filePath": "b.txt" }),
        );
        assert_eq!(err.code(), "FileDoesNotExistException");
    }

    // Defect 4: MergeBranchesBySquash yields a single-parent commit.
    #[test]
    fn merge_branches_squash_single_parent() {
        let s = svc();
        let (feature_tip, main_tip) = diverged_repo(&s);
        let out = call(
            &s,
            "MergeBranchesBySquash",
            json!({
                "repositoryName": "repo",
                "sourceCommitSpecifier": feature_tip,
                "destinationCommitSpecifier": main_tip,
                "targetBranch": "main",
            }),
        );
        let cid = out["commitId"].as_str().unwrap().to_string();
        let got = call(
            &s,
            "GetCommit",
            json!({ "repositoryName": "repo", "commitId": cid }),
        );
        assert_eq!(got["commit"]["parents"].as_array().unwrap().len(), 1);
    }

    // Defect 5 + 6: an associated 2-approval template is auto-applied on PR
    // create and is NOT satisfied by a single approval.
    #[test]
    fn template_applied_and_two_approvals_needed() {
        let s = svc();
        let content = "{\"Version\":\"2018-11-08\",\"Statements\":[{\"Type\":\"Approvers\",\"NumberOfApprovalsNeeded\":2,\"ApprovalPoolMembers\":[]}]}";
        call(
            &s,
            "CreateApprovalRuleTemplate",
            json!({ "approvalRuleTemplateName": "two", "approvalRuleTemplateContent": content }),
        );
        diverged_repo(&s);
        call(
            &s,
            "AssociateApprovalRuleTemplateWithRepository",
            json!({ "approvalRuleTemplateName": "two", "repositoryName": "repo" }),
        );
        let pr = create_pr(&s, "main");
        let pr_id = pr["pullRequestId"].as_str().unwrap().to_string();
        let revision = pr["revisionId"].as_str().unwrap().to_string();

        // The template's rule is materialized on the PR with template origin.
        let rules = pr["approvalRules"].as_array().unwrap();
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0]["approvalRuleName"], json!("two"));
        assert!(rules[0]["originApprovalRuleTemplate"].is_object());

        // One approval is not enough for a rule needing two.
        call_as(
            &s,
            "UpdatePullRequestApprovalState",
            json!({ "pullRequestId": pr_id, "revisionId": revision, "approvalState": "APPROVE" }),
            "arn:aws:iam::000000000000:user/reviewer",
        );
        let eval = call(
            &s,
            "EvaluatePullRequestApprovalRules",
            json!({ "pullRequestId": pr_id, "revisionId": revision }),
        );
        assert_eq!(eval["evaluation"]["approved"], json!(false));
        assert_eq!(
            eval["evaluation"]["approvalRulesNotSatisfied"],
            json!(["two"])
        );
    }

    // Defect 7: a closed pull request cannot be reopened.
    #[test]
    fn closed_pr_cannot_reopen() {
        let s = svc();
        diverged_repo(&s);
        let pr = create_pr(&s, "main");
        let pr_id = pr["pullRequestId"].as_str().unwrap().to_string();
        call(
            &s,
            "UpdatePullRequestStatus",
            json!({ "pullRequestId": pr_id, "pullRequestStatus": "CLOSED" }),
        );
        let err = call_err(
            &s,
            "UpdatePullRequestStatus",
            json!({ "pullRequestId": pr_id, "pullRequestStatus": "OPEN" }),
        );
        assert_eq!(err.code(), "InvalidPullRequestStatusUpdateException");
    }

    // Defect 8: a nonexistent destinationReference is rejected before create.
    #[test]
    fn create_pr_rejects_missing_destination() {
        let s = svc();
        diverged_repo(&s);
        let err = call_err(
            &s,
            "CreatePullRequest",
            json!({
                "title": "PR",
                "targets": [{
                    "repositoryName": "repo",
                    "sourceReference": "feature",
                    "destinationReference": "does-not-exist",
                }],
            }),
        );
        assert_eq!(err.code(), "ReferenceDoesNotExistException");
    }

    // Defect 9 + 10: Describe / BatchDescribe report a real content conflict.
    #[test]
    fn describe_and_batch_report_conflict() {
        let s = svc();
        make_repo(&s);
        let c1 = put(&s, "main", "a.txt", "base-content", None);
        call(
            &s,
            "CreateBranch",
            json!({ "repositoryName": "repo", "branchName": "feature", "commitId": c1 }),
        );
        put(&s, "feature", "a.txt", "feature-side", Some(&c1));
        put(&s, "main", "a.txt", "main-side", Some(&c1));

        let base_body = json!({
            "repositoryName": "repo",
            "sourceCommitSpecifier": "feature",
            "destinationCommitSpecifier": "main",
            "mergeOption": "THREE_WAY_MERGE",
        });

        let mut d = base_body.clone();
        d["filePath"] = json!("a.txt");
        let desc = call(&s, "DescribeMergeConflicts", d);
        assert_eq!(desc["conflictMetadata"]["numberOfConflicts"], json!(1));
        assert_eq!(desc["conflictMetadata"]["contentConflict"], json!(true));
        assert_eq!(desc["mergeHunks"][0]["isConflict"], json!(true));

        let batch = call(&s, "BatchDescribeMergeConflicts", base_body.clone());
        let conflicts = batch["conflicts"].as_array().unwrap();
        assert_eq!(conflicts.len(), 1);
        assert_eq!(conflicts[0]["conflictMetadata"]["filePath"], json!("a.txt"));

        // GetMergeConflicts agrees: not mergeable, a.txt listed.
        let gmc = call(&s, "GetMergeConflicts", base_body);
        assert_eq!(gmc["mergeable"], json!(false));
        assert_eq!(gmc["conflictMetadataList"][0]["filePath"], json!("a.txt"));
    }

    // Defect 1: a fast-forward PR merge that is not fast-forwardable is rejected
    // and leaves the PR open.
    #[test]
    fn merge_pr_fast_forward_non_ff_rejected() {
        let s = svc();
        diverged_repo(&s);
        let pr = create_pr(&s, "main");
        let pr_id = pr["pullRequestId"].as_str().unwrap().to_string();
        let err = call_err(
            &s,
            "MergePullRequestByFastForward",
            json!({ "pullRequestId": pr_id, "repositoryName": "repo" }),
        );
        assert_eq!(err.code(), "ManualMergeRequiredException");
        // PR remains open.
        let got = call(&s, "GetPullRequest", json!({ "pullRequestId": pr_id }));
        assert_eq!(got["pullRequest"]["pullRequestStatus"], json!("OPEN"));
    }

    fn branch_tip(s: &CodeCommitService, branch: &str) -> String {
        call(
            s,
            "GetBranch",
            json!({ "repositoryName": "repo", "branchName": branch }),
        )["branch"]["commitId"]
            .as_str()
            .unwrap()
            .to_string()
    }

    fn one_approval_rule() -> &'static str {
        "{\"Version\":\"2018-11-08\",\"Statements\":[{\"Type\":\"Approvers\",\"NumberOfApprovalsNeeded\":1,\"ApprovalPoolMembers\":[]}]}"
    }

    // Round-2 defect 1: a new commit on the PR's source branch mints a new
    // revisionId and invalidates approvals cast against the prior revision.
    #[test]
    fn new_source_commit_bumps_revision_and_invalidates_approvals() {
        let s = svc();
        let (feature_tip, _) = diverged_repo(&s);
        let pr = create_pr(&s, "main");
        let pr_id = pr["pullRequestId"].as_str().unwrap().to_string();
        let rev1 = pr["revisionId"].as_str().unwrap().to_string();
        call(
            &s,
            "CreatePullRequestApprovalRule",
            json!({
                "pullRequestId": pr_id,
                "approvalRuleName": "r",
                "approvalRuleContent": one_approval_rule(),
            }),
        );
        // One approval at rev1 satisfies the 1-approval rule.
        call_as(
            &s,
            "UpdatePullRequestApprovalState",
            json!({ "pullRequestId": pr_id, "revisionId": rev1, "approvalState": "APPROVE" }),
            "arn:aws:iam::000000000000:user/reviewer",
        );
        let eval1 = call(
            &s,
            "EvaluatePullRequestApprovalRules",
            json!({ "pullRequestId": pr_id, "revisionId": rev1 }),
        );
        assert_eq!(eval1["evaluation"]["approved"], json!(true));

        // A new commit on the source branch bumps the revisionId...
        put(&s, "feature", "d.txt", "more", Some(&feature_tip));
        let got = call(&s, "GetPullRequest", json!({ "pullRequestId": pr_id }));
        let rev2 = got["pullRequest"]["revisionId"]
            .as_str()
            .unwrap()
            .to_string();
        assert_ne!(rev1, rev2, "source advance must re-mint the revisionId");

        // ...and the old approval no longer counts against the new revision.
        let eval2 = call(
            &s,
            "EvaluatePullRequestApprovalRules",
            json!({ "pullRequestId": pr_id, "revisionId": rev2 }),
        );
        assert_eq!(eval2["evaluation"]["approved"], json!(false));
    }

    // Round-2 defect 4: GetPullRequest reflects the advanced source tip.
    #[test]
    fn get_pull_request_reflects_advanced_source_tip() {
        let s = svc();
        let (feature_tip, _) = diverged_repo(&s);
        let pr = create_pr(&s, "main");
        let pr_id = pr["pullRequestId"].as_str().unwrap().to_string();
        assert_eq!(
            pr["pullRequestTargets"][0]["sourceCommit"],
            json!(feature_tip)
        );
        put(&s, "feature", "e.txt", "x", Some(&feature_tip));
        let new_tip = branch_tip(&s, "feature");
        let got = call(&s, "GetPullRequest", json!({ "pullRequestId": pr_id }));
        assert_eq!(
            got["pullRequest"]["pullRequestTargets"][0]["sourceCommit"],
            json!(new_tip)
        );
    }

    // Round-2 defect 2: CreateUnreferencedMergeCommit SQUASH -> single parent.
    #[test]
    fn create_unreferenced_squash_single_parent() {
        let s = svc();
        diverged_repo(&s);
        let out = call(
            &s,
            "CreateUnreferencedMergeCommit",
            json!({
                "repositoryName": "repo",
                "sourceCommitSpecifier": "feature",
                "destinationCommitSpecifier": "main",
                "mergeOption": "SQUASH_MERGE",
            }),
        );
        let cid = out["commitId"].as_str().unwrap().to_string();
        let got = call(
            &s,
            "GetCommit",
            json!({ "repositoryName": "repo", "commitId": cid }),
        );
        assert_eq!(got["commit"]["parents"].as_array().unwrap().len(), 1);
    }

    // Round-2 defect 3: an invalid conflictResolutionStrategy is rejected on both
    // MergePullRequestBy* and CreateUnreferencedMergeCommit.
    #[test]
    fn invalid_conflict_strategy_rejected() {
        let s = svc();
        diverged_repo(&s);
        let pr = create_pr(&s, "main");
        let pr_id = pr["pullRequestId"].as_str().unwrap().to_string();
        let e1 = call_err(
            &s,
            "MergePullRequestByThreeWay",
            json!({
                "pullRequestId": pr_id,
                "repositoryName": "repo",
                "conflictResolutionStrategy": "BOGUS",
            }),
        );
        assert_eq!(e1.code(), "InvalidConflictResolutionStrategyException");
        let e2 = call_err(
            &s,
            "CreateUnreferencedMergeCommit",
            json!({
                "repositoryName": "repo",
                "sourceCommitSpecifier": "feature",
                "destinationCommitSpecifier": "main",
                "mergeOption": "THREE_WAY_MERGE",
                "conflictResolutionStrategy": "BOGUS",
            }),
        );
        assert_eq!(e2.code(), "InvalidConflictResolutionStrategyException");
    }

    // Round-2 defect 5: OPEN -> OPEN status update is rejected.
    #[test]
    fn open_to_open_status_rejected() {
        let s = svc();
        diverged_repo(&s);
        let pr = create_pr(&s, "main");
        let pr_id = pr["pullRequestId"].as_str().unwrap().to_string();
        let err = call_err(
            &s,
            "UpdatePullRequestStatus",
            json!({ "pullRequestId": pr_id, "pullRequestStatus": "OPEN" }),
        );
        assert_eq!(err.code(), "InvalidPullRequestStatusUpdateException");
    }

    // Round-2 defect 6: an unresolvable trigger destination is reported as a
    // failed execution, while a valid SNS destination succeeds.
    #[test]
    fn test_triggers_reports_unresolvable_destination() {
        let s = svc();
        make_repo(&s);
        let out = call(
            &s,
            "TestRepositoryTriggers",
            json!({
                "repositoryName": "repo",
                "triggers": [
                    {
                        "name": "ok",
                        "destinationArn": "arn:aws:sns:us-east-1:000000000000:topic",
                        "events": ["all"],
                    },
                    {
                        "name": "bad",
                        "destinationArn": "arn:aws:s3:::some-bucket",
                        "events": ["all"],
                    },
                ],
            }),
        );
        assert_eq!(out["successfulExecutions"], json!(["ok"]));
        let failed = out["failedExecutions"].as_array().unwrap();
        assert_eq!(failed.len(), 1);
        assert_eq!(failed[0]["trigger"], json!("bad"));
        assert!(failed[0]["failureMessage"].as_str().unwrap().contains("s3"));
    }

    // Round-2 defect 7: deleting a repository drops its template associations.
    #[test]
    fn delete_repository_clears_template_association() {
        let s = svc();
        make_repo(&s);
        call(
            &s,
            "CreateApprovalRuleTemplate",
            json!({
                "approvalRuleTemplateName": "t",
                "approvalRuleTemplateContent": one_approval_rule(),
            }),
        );
        call(
            &s,
            "AssociateApprovalRuleTemplateWithRepository",
            json!({ "approvalRuleTemplateName": "t", "repositoryName": "repo" }),
        );
        let before = call(
            &s,
            "ListRepositoriesForApprovalRuleTemplate",
            json!({ "approvalRuleTemplateName": "t" }),
        );
        assert_eq!(before["repositoryNames"], json!(["repo"]));
        call(&s, "DeleteRepository", json!({ "repositoryName": "repo" }));
        let after = call(
            &s,
            "ListRepositoriesForApprovalRuleTemplate",
            json!({ "approvalRuleTemplateName": "t" }),
        );
        assert_eq!(after["repositoryNames"], json!([]));
    }
}
