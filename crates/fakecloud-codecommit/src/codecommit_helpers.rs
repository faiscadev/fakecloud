//! Response helpers, validators, git-object hashing, and JSON serializers for
//! CodeCommit. Split out of `service.rs` (which retains the request handlers and
//! the `AwsService` impl) with no behavior change; all `pub(crate)`.

use base64::Engine;
use chrono::{DateTime, Utc};
use http::StatusCode;
use serde_json::{json, Map, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::*;
use crate::validate;

pub(crate) fn ok(v: Value) -> Result<AwsResponse, AwsServiceError> {
    Ok(AwsResponse::json_value(StatusCode::OK, v))
}

pub(crate) fn ts(dt: DateTime<Utc>) -> Value {
    json!(dt.timestamp_millis() as f64 / 1000.0)
}

pub(crate) fn err(code: &str, msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, code, msg)
}

pub(crate) fn body(req: &AwsRequest) -> Value {
    serde_json::from_slice(&req.body).unwrap_or(Value::Null)
}

pub(crate) fn str_field(b: &Value, key: &str) -> Option<String> {
    b.get(key).and_then(|v| v.as_str()).map(str::to_string)
}

pub(crate) fn caller_arn(req: &AwsRequest) -> String {
    req.principal
        .as_ref()
        .map(|p| p.arn.clone())
        .unwrap_or_else(|| format!("arn:aws:iam::{}:root", req.account_id))
}

/// SHA-1 hex of the given bytes.
pub(crate) fn sha1_hex(data: &[u8]) -> String {
    use sha1::{Digest, Sha1};
    let mut h = Sha1::new();
    h.update(data);
    format!("{:x}", h.finalize())
}

/// git-style blob id: SHA-1 over `blob <len>\0<content>`.
pub(crate) fn blob_id_for(content: &[u8]) -> String {
    let header = format!("blob {}\0", content.len());
    let mut buf = header.into_bytes();
    buf.extend_from_slice(content);
    sha1_hex(&buf)
}

/// git-style tree id: SHA-1 over the sorted `mode\tpath\tblob` entries.
pub(crate) fn tree_id_for(entries: &std::collections::BTreeMap<String, FileEntry>) -> String {
    let mut buf = String::new();
    for (path, e) in entries {
        buf.push_str(&format!("{}\t{}\t{}\n", e.mode, path, e.blob_id));
    }
    sha1_hex(buf.as_bytes())
}

pub(crate) fn new_uuid() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// A 40-char hex id derived from a fresh UUID (used for commit ids and
/// pull-request revision ids so each mint is unique like a real committer
/// timestamp would make it).
pub(crate) fn fresh_object_id() -> String {
    sha1_hex(uuid::Uuid::new_v4().as_bytes())
}

pub(crate) fn is_object_id(s: &str) -> bool {
    s.len() == 40 && s.chars().all(|c| c.is_ascii_hexdigit())
}

pub(crate) fn file_mode_to_git(mode: &str) -> &'static str {
    match mode {
        "EXECUTABLE" => "100755",
        "SYMLINK" => "120000",
        _ => "100644",
    }
}

pub(crate) fn git_mode_to_file(mode: &str) -> &'static str {
    match mode {
        "100755" => "EXECUTABLE",
        "120000" => "SYMLINK",
        _ => "NORMAL",
    }
}

/// Require a present, non-empty string member, erroring with `err_code` when
/// absent or empty.
pub(crate) fn require(b: &Value, key: &str, err_code: &str) -> Result<String, AwsServiceError> {
    match str_field(b, key) {
        Some(v) if !v.is_empty() => Ok(v),
        _ => Err(err(err_code, format!("{key} is required."))),
    }
}

/// Require and validate a `RepositoryName` member.
pub(crate) fn require_repository_name(b: &Value) -> Result<String, AwsServiceError> {
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
pub(crate) fn check_enum(
    b: &Value,
    key: &str,
    set: &[&str],
    err_code: &str,
) -> Result<(), AwsServiceError> {
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
pub(crate) fn repo_arn(region: &str, account: &str, name: &str) -> String {
    format!("arn:aws:codecommit:{region}:{account}:{name}")
}

/// Validate a CodeCommit resource ARN (`arn:aws:codecommit:...`).
pub(crate) fn is_codecommit_arn(s: &str) -> bool {
    let parts: Vec<&str> = s.splitn(6, ':').collect();
    parts.len() == 6 && parts[0] == "arn" && parts[2] == "codecommit"
}

/// The repository name embedded in a CodeCommit resource ARN, if any.
pub(crate) fn repo_name_from_arn(s: &str) -> Option<String> {
    let parts: Vec<&str> = s.splitn(6, ':').collect();
    if parts.len() == 6 && parts[0] == "arn" && parts[2] == "codecommit" {
        Some(parts[5].to_string())
    } else {
        None
    }
}

/// Strip a comment's private `_ctx*` context members before returning it.
pub(crate) fn public_comment(stored: &Value) -> Value {
    let mut c = stored.clone();
    if let Some(obj) = c.as_object_mut() {
        obj.retain(|k, _| !k.starts_with("_ctx"));
    }
    c
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

pub(crate) fn repo_not_found(name: &str) -> AwsServiceError {
    err(
        "RepositoryDoesNotExistException",
        format!("The repository {name} does not exist."),
    )
}

pub(crate) fn pr_not_found() -> AwsServiceError {
    err(
        "PullRequestDoesNotExistException",
        "The specified pull request does not exist.",
    )
}

pub(crate) fn template_not_found() -> AwsServiceError {
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
pub(crate) fn normalize_rule_content(content: &str) -> String {
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

pub(crate) fn sha256_hex(data: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(data);
    format!("{:x}", h.finalize())
}

/// Whether an approval-pool member pattern matches an approver ARN. Pool members
/// may be exact ARNs or contain `*` wildcards (e.g. an assumed-role session
/// glob), matching CodeCommit's pool semantics.
pub(crate) fn pool_matches(pattern: &str, arn: &str) -> bool {
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
pub(crate) fn rule_satisfied(
    content: &str,
    approvers: &std::collections::BTreeSet<String>,
) -> bool {
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
pub(crate) fn set_default_if_absent(repo: &mut Repo) {
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
pub(crate) fn build_commit(
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
pub(crate) fn merge_parents(merge_option: &str, dest: &str, source: &str) -> Vec<String> {
    if merge_option == "SQUASH_MERGE" {
        vec![dest.to_string()]
    } else {
        vec![dest.to_string(), source.to_string()]
    }
}

/// Validate the optional `conflictDetailLevel` and `conflictResolutionStrategy`
/// members shared by every merge operation, returning the declared error when
/// either is not a modeled enum value.
pub(crate) fn validate_merge_conflict_enums(b: &Value) -> Result<(), AwsServiceError> {
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
pub(crate) fn refresh_source_revisions(st: &mut CodeCommitState, repo_name: &str, branch: &str) {
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
pub(crate) fn refresh_pr_targets(st: &CodeCommitState, pr: &Value) -> Value {
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
pub(crate) fn trigger_destination_ok(arn: &str, account: &str, region: &str) -> bool {
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
pub(crate) fn resolve_commit(repo: &Repo, b: &Value, key: &str) -> Result<String, AwsServiceError> {
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
pub(crate) fn ancestors(repo: &Repo, commit: &str) -> std::collections::BTreeSet<String> {
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
pub(crate) fn is_ancestor(repo: &Repo, ancestor: &str, descendant: &str) -> bool {
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
pub(crate) fn merge_base(repo: &Repo, a: &str, b: &str) -> Option<String> {
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
pub(crate) fn bfs_depths(repo: &Repo, start: &str) -> std::collections::BTreeMap<String, usize> {
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
pub(crate) fn entries_equal(x: Option<&FileEntry>, y: Option<&FileEntry>) -> bool {
    match (x, y) {
        (None, None) => true,
        (Some(a), Some(b)) => a.blob_id == b.blob_id && a.mode == b.mode,
        _ => false,
    }
}

/// The three-way resolution of a single path given its base, source, and
/// destination entries. `Take(None)` means the path is deleted in the result.
pub(crate) enum Resolved {
    Take(Option<FileEntry>),
    Conflict,
}

/// Real git/CodeCommit three-way resolution for one path. Additions,
/// modifications, and deletions on either side all resolve when only one side
/// changed relative to the base; a genuine two-sided divergence is a conflict.
pub(crate) fn merge_entry(
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
pub(crate) type Tree = std::collections::BTreeMap<String, FileEntry>;

/// The base/source/destination trees for a merge (empty maps when a commit has
/// no materialized tree), along with the computed base commit id.
pub(crate) fn merge_inputs<'a>(
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
pub(crate) fn all_merge_paths(
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
pub(crate) fn conflicting_paths_in(
    base_tree: &Tree,
    src_tree: &Tree,
    dst_tree: &Tree,
) -> Vec<String> {
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
pub(crate) fn merge_trees(
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
pub(crate) fn line_count(bytes: &[u8]) -> usize {
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
pub(crate) fn entry_bytes(repo: &Repo, entry: Option<&FileEntry>) -> Vec<u8> {
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
pub(crate) fn hunk_side(bytes: &[u8]) -> Value {
    let lines = line_count(bytes);
    json!({
        "startLine": if lines == 0 { 0 } else { 1 },
        "endLine": lines,
        "hunkContent": base64::engine::general_purpose::STANDARD.encode(bytes),
    })
}

/// The change kind of one side relative to the base: `A`dded, `M`odified,
/// `D`eleted, or none when unchanged.
pub(crate) fn merge_operation(
    base: Option<&FileEntry>,
    side: Option<&FileEntry>,
) -> Option<&'static str> {
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
pub(crate) fn conflict_metadata(
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
pub(crate) fn is_binary(bytes: &[u8]) -> bool {
    bytes.contains(&0)
}

/// Split a text blob into logical lines, dropping the empty trailing element
/// produced by a final newline (so "a\nb\n" is two lines, not three).
fn split_lines(bytes: &[u8]) -> Vec<String> {
    let text = String::from_utf8_lossy(bytes);
    if text.is_empty() {
        return Vec::new();
    }
    let mut lines: Vec<String> = text.split('\n').map(|s| s.to_string()).collect();
    if lines.last().map(|s| s.is_empty()).unwrap_or(false) {
        lines.pop();
    }
    lines
}

/// The comparison key for a line: the raw line, or its whitespace stripped out
/// when the caller asked to ignore whitespace. The original content is always
/// preserved for the emitted change; only equality testing uses this key.
fn line_key(line: &str, ignore_ws: bool) -> String {
    if ignore_ws {
        line.chars().filter(|c| !c.is_whitespace()).collect()
    } else {
        line.to_string()
    }
}

/// One line-level edit produced by the diff before it is grouped into hunks.
enum Op {
    /// Unchanged line present on both sides: (before_idx, after_idx).
    Context(usize, usize),
    /// Line only in the before blob: before_idx.
    Delete(usize),
    /// Line only in the after blob: after_idx.
    Add(usize),
}

/// Compute the line-level edit script between two blobs using a classic LCS
/// dynamic program. Blob sizes here are test/repo scale, so the quadratic table
/// is fine and keeps the output deterministic.
fn edit_script(before: &[String], after: &[String], ignore_ws: bool) -> Vec<Op> {
    let bk: Vec<String> = before.iter().map(|l| line_key(l, ignore_ws)).collect();
    let ak: Vec<String> = after.iter().map(|l| line_key(l, ignore_ws)).collect();
    let (n, m) = (bk.len(), ak.len());
    // lcs[i][j] = length of LCS of before[i..] and after[j..].
    let mut lcs = vec![vec![0usize; m + 1]; n + 1];
    for i in (0..n).rev() {
        for j in (0..m).rev() {
            lcs[i][j] = if bk[i] == ak[j] {
                lcs[i + 1][j + 1] + 1
            } else {
                lcs[i + 1][j].max(lcs[i][j + 1])
            };
        }
    }
    let mut ops = Vec::new();
    let (mut i, mut j) = (0usize, 0usize);
    while i < n && j < m {
        if bk[i] == ak[j] {
            ops.push(Op::Context(i, j));
            i += 1;
            j += 1;
        } else if lcs[i + 1][j] >= lcs[i][j + 1] {
            ops.push(Op::Delete(i));
            i += 1;
        } else {
            ops.push(Op::Add(j));
            j += 1;
        }
    }
    while i < n {
        ops.push(Op::Delete(i));
        i += 1;
    }
    while j < m {
        ops.push(Op::Add(j));
        j += 1;
    }
    ops
}

/// Build the `DiffHunk` list for a before/after blob pair, honoring the
/// requested number of surrounding context lines. Runs of changes plus their
/// context are grouped into hunks; adjacent runs whose context windows touch
/// are merged, matching CodeCommit's documented behavior.
pub(crate) fn blob_diff_hunks(before: &[u8], after: &[u8], context: usize) -> Vec<Value> {
    let before_lines = split_lines(before);
    let after_lines = split_lines(after);
    let ops = edit_script(&before_lines, &after_lines, false);

    // Indices of the ops that represent an actual change (Add/Delete).
    let change_positions: Vec<usize> = ops
        .iter()
        .enumerate()
        .filter(|(_, op)| !matches!(op, Op::Context(_, _)))
        .map(|(idx, _)| idx)
        .collect();
    if change_positions.is_empty() {
        return Vec::new();
    }

    // Coalesce change positions into hunk ranges over the op list, expanding by
    // `context` on each side and merging ranges that overlap once expanded.
    let mut ranges: Vec<(usize, usize)> = Vec::new();
    for &pos in &change_positions {
        let lo = pos.saturating_sub(context);
        let hi = (pos + context).min(ops.len() - 1);
        match ranges.last_mut() {
            Some(last) if lo <= last.1 + 1 => last.1 = last.1.max(hi),
            _ => ranges.push((lo, hi)),
        }
    }

    ranges
        .into_iter()
        .map(|(lo, hi)| {
            let mut changes = Vec::new();
            let (mut before_start, mut after_start) = (0usize, 0usize);
            let (mut before_count, mut after_count) = (0usize, 0usize);
            let mut seen_before = false;
            let mut seen_after = false;
            for op in &ops[lo..=hi] {
                let mut change = Map::new();
                match op {
                    Op::Context(bi, ai) => {
                        change.insert("type".into(), json!("CONTEXT"));
                        change.insert("beforeLineNumber".into(), json!(bi + 1));
                        change.insert("afterLineNumber".into(), json!(ai + 1));
                        change.insert("content".into(), json!(before_lines[*bi]));
                        if !seen_before {
                            before_start = bi + 1;
                            seen_before = true;
                        }
                        if !seen_after {
                            after_start = ai + 1;
                            seen_after = true;
                        }
                        before_count += 1;
                        after_count += 1;
                    }
                    Op::Delete(bi) => {
                        change.insert("type".into(), json!("DELETE"));
                        change.insert("beforeLineNumber".into(), json!(bi + 1));
                        change.insert("content".into(), json!(before_lines[*bi]));
                        if !seen_before {
                            before_start = bi + 1;
                            seen_before = true;
                        }
                        before_count += 1;
                    }
                    Op::Add(ai) => {
                        change.insert("type".into(), json!("ADD"));
                        change.insert("afterLineNumber".into(), json!(ai + 1));
                        change.insert("content".into(), json!(after_lines[*ai]));
                        if !seen_after {
                            after_start = ai + 1;
                            seen_after = true;
                        }
                        after_count += 1;
                    }
                }
                changes.push(Value::Object(change));
            }
            json!({
                "beforeStartLine": before_start,
                "beforeLineCount": before_count,
                "afterStartLine": after_start,
                "afterLineCount": after_count,
                "changes": changes,
            })
        })
        .collect()
}

/// Build a `PullRequestEvent`-shaped JSON value.
pub(crate) fn pr_event(
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
pub(crate) fn comment_value(
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
