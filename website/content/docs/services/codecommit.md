+++
title = "AWS CodeCommit"
description = "AWS CodeCommit (codecommit) on fakecloud: a complete 79-operation implementation (100% conformance) covering repositories, branches, commits and files over a real content-addressed object store, pull requests with approvals and events, approval-rule templates and associations, comments and reactions, repository triggers, and tagging. awsJson1.1."
weight = 66
+++

fakecloud implements **AWS CodeCommit** as an awsJson1.1 service (sigv4 signing
name `codecommit`, target prefix `CodeCommit_20150413`). All **79 operations**
ship with **100% conformance** against AWS's own Smithy model, backed by
account-partitioned state that persists across restarts in persistent mode.

CodeCommit is a managed Git-repository control plane. fakecloud models the whole
control plane as real, persisted CRUD backed by a genuine content-addressed
object store: file contents become blobs keyed by a 40-char SHA-1 id, each
commit materializes a working tree, and branches point at commit tips. There is
no live Git smart-HTTP transport, so clone URLs are minted in exact AWS form but
no `git push`/`git pull` endpoint is served -- the same control-plane-only shape
the sibling `Code*` services use.

## Repositories

`CreateRepository` / `GetRepository` / `DeleteRepository` / `ListRepositories`
/ `BatchGetRepositories` manage repositories. A `CreateRepository` validates the
name against the model `@length` (1..100) and `@pattern` (`^[\w\.-]+$`), mints a
UUID `repositoryId`, the ARN
(`arn:aws:codecommit:<region>:<account>:<name>`), and the exact clone URLs
(`https://git-codecommit.<region>.amazonaws.com/v1/repos/<name>` and the
`ssh://` form), and records `creationDate`, `lastModifiedDate`, and a
`kmsKeyId`. `UpdateRepositoryDescription`, `UpdateRepositoryName`, and
`UpdateRepositoryEncryptionKey` round-trip their changes (a rename rewrites the
ARN, clone URLs, and any tags), and `ListRepositories` honors the `sortBy` /
`order` enums.

## Branches, files, and commits

`PutFile` and `DeleteFile` create commits on a branch, enforcing the
`parentCommitId` tip check (`ParentCommitIdRequiredException` /
`ParentCommitIdOutdatedException`) exactly as AWS does; `CreateCommit` applies a
batch of `putFiles` / `deleteFiles` / `setFileModes` in one commit and returns
the `filesAdded` / `filesUpdated` / `filesDeleted` metadata. `CreateBranch`,
`DeleteBranch` (guarding the default branch), `GetBranch`, `ListBranches`, and
`UpdateDefaultBranch` manage refs. Reads resolve a commit specifier (a branch
name or a 40-char commit id) against the stored graph: `GetFile`, `GetFolder`,
`GetBlob`, `GetCommit`, `BatchGetCommits`, `GetDifferences`, and
`ListFileCommitHistory` all return real data from the materialized trees, with
`fileMode` mapped to the `EXECUTABLE` / `NORMAL` / `SYMLINK` model enum.

## Merges

`MergeBranchesByFastForward` fast-forwards a branch when the destination is an
ancestor of the source; `MergeBranchesBySquash` / `MergeBranchesByThreeWay` and
`CreateUnreferencedMergeCommit` compute a three-way merge on the stored trees,
resolving non-conflicting changes and honoring `ACCEPT_SOURCE` /
`ACCEPT_DESTINATION` strategies. Genuinely divergent edits to the same file
report the declared `ManualMergeRequiredException`. `GetMergeCommit`,
`GetMergeOptions`, `GetMergeConflicts`, `DescribeMergeConflicts`, and
`BatchDescribeMergeConflicts` surface the merge base and per-path conflict
metadata.

## Pull requests

`CreatePullRequest` resolves each target's source/destination reference to a
commit and merge base, mints a numeric `pullRequestId` and a `revisionId`, and
records a `PULL_REQUEST_CREATED` event. `GetPullRequest`, `ListPullRequests`
(filtered by author and status), `UpdatePullRequestTitle` /
`UpdatePullRequestDescription` / `UpdatePullRequestStatus`, and
`DescribePullRequestEvents` round-trip pull-request state.
`MergePullRequestByFastForward` / `BySquash` / `ByThreeWay` close the pull
request and stamp its `mergeMetadata`.

Per-pull-request approval rules (`CreatePullRequestApprovalRule`,
`DeletePullRequestApprovalRule`, `UpdatePullRequestApprovalRuleContent`) and
approval state (`UpdatePullRequestApprovalState`,
`GetPullRequestApprovalStates`, `EvaluatePullRequestApprovalRules`) enforce the
current-revision and author-cannot-approve rules
(`RevisionNotCurrentException`, `PullRequestCannotBeApprovedByAuthorException`).
`OverridePullRequestApprovalRules` and `GetPullRequestOverrideState` manage the
approval override.

## Approval-rule templates

`CreateApprovalRuleTemplate` / `GetApprovalRuleTemplate` /
`DeleteApprovalRuleTemplate` / `ListApprovalRuleTemplates`, plus
`UpdateApprovalRuleTemplateContent` / `Description` / `Name`, manage templates.
The template content is canonicalized to compact JSON (preserving member order)
and a `ruleContentSha256` is recorded, matching AWS. Associations
(`AssociateApprovalRuleTemplateWithRepository`,
`DisassociateApprovalRuleTemplateFromRepository`, the `Batch*` variants,
`ListAssociatedApprovalRuleTemplatesForRepository`, and
`ListRepositoriesForApprovalRuleTemplate`) round-trip both directions of the
relationship.

## Comments, triggers, and tagging

`PostCommentForComparedCommit`, `PostCommentForPullRequest`, and
`PostCommentReply` create comments with their thread context;
`GetComment`, `GetCommentsForComparedCommit`, `GetCommentsForPullRequest`,
`UpdateComment` (author-only), and `DeleteCommentContent` (soft delete) manage
them, and `PutCommentReaction` / `GetCommentReactions` track reactions.
`PutRepositoryTriggers` / `GetRepositoryTriggers` persist triggers with a fresh
`configurationId` and validate the `RepositoryTriggerEventEnum`;
`TestRepositoryTriggers` reports which trigger names would fire. `TagResource` /
`UntagResource` / `ListTagsForResource` provide resource tagging keyed by ARN.
