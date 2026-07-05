mod helpers;

use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

#[test_action(
    "codecommit",
    "AssociateApprovalRuleTemplateWithRepository",
    checksum = "9e8aa160"
)]
#[test_action(
    "codecommit",
    "BatchAssociateApprovalRuleTemplateWithRepositories",
    checksum = "36d891cb"
)]
#[test_action("codecommit", "BatchDescribeMergeConflicts", checksum = "ab798bc2")]
#[test_action(
    "codecommit",
    "BatchDisassociateApprovalRuleTemplateFromRepositories",
    checksum = "013cde90"
)]
#[test_action("codecommit", "BatchGetCommits", checksum = "7461b057")]
#[test_action("codecommit", "BatchGetRepositories", checksum = "4a1991d9")]
#[test_action("codecommit", "CreateApprovalRuleTemplate", checksum = "b14264e7")]
#[test_action("codecommit", "CreateBranch", checksum = "3382a4f8")]
#[test_action("codecommit", "CreateCommit", checksum = "4f88e2da")]
#[test_action("codecommit", "CreatePullRequest", checksum = "8009aaad")]
#[test_action("codecommit", "CreatePullRequestApprovalRule", checksum = "af3bd53b")]
#[test_action("codecommit", "CreateRepository", checksum = "19c9e36b")]
#[test_action("codecommit", "CreateUnreferencedMergeCommit", checksum = "4223869d")]
#[test_action("codecommit", "DeleteApprovalRuleTemplate", checksum = "d64a0e04")]
#[test_action("codecommit", "DeleteBranch", checksum = "f4f32950")]
#[test_action("codecommit", "DeleteCommentContent", checksum = "f8e6eefd")]
#[test_action("codecommit", "DeleteFile", checksum = "c5c25ce2")]
#[test_action("codecommit", "DeletePullRequestApprovalRule", checksum = "5d6272b5")]
#[test_action("codecommit", "DeleteRepository", checksum = "3d860cbb")]
#[test_action("codecommit", "DescribeMergeConflicts", checksum = "c066f43f")]
#[test_action("codecommit", "DescribePullRequestEvents", checksum = "54363cb5")]
#[test_action(
    "codecommit",
    "DisassociateApprovalRuleTemplateFromRepository",
    checksum = "991eddbe"
)]
#[test_action(
    "codecommit",
    "EvaluatePullRequestApprovalRules",
    checksum = "c959f086"
)]
#[test_action("codecommit", "GetApprovalRuleTemplate", checksum = "1e984f9f")]
#[test_action("codecommit", "GetBlob", checksum = "a2705900")]
#[test_action("codecommit", "GetBranch", checksum = "c1096e18")]
#[test_action("codecommit", "GetComment", checksum = "75481012")]
#[test_action("codecommit", "GetCommentReactions", checksum = "89d65bec")]
#[test_action("codecommit", "GetCommentsForComparedCommit", checksum = "47a4790f")]
#[test_action("codecommit", "GetCommentsForPullRequest", checksum = "d42c46f7")]
#[test_action("codecommit", "GetCommit", checksum = "e80f073e")]
#[test_action("codecommit", "GetDifferences", checksum = "06b02994")]
#[test_action("codecommit", "GetFile", checksum = "e310725f")]
#[test_action("codecommit", "GetFolder", checksum = "7b27f144")]
#[test_action("codecommit", "GetMergeCommit", checksum = "5077100d")]
#[test_action("codecommit", "GetMergeConflicts", checksum = "bf5d8a2c")]
#[test_action("codecommit", "GetMergeOptions", checksum = "28b0f5e9")]
#[test_action("codecommit", "GetPullRequest", checksum = "14b3dc0b")]
#[test_action("codecommit", "GetPullRequestApprovalStates", checksum = "c0794785")]
#[test_action("codecommit", "GetPullRequestOverrideState", checksum = "83497bf6")]
#[test_action("codecommit", "GetRepository", checksum = "6ef1f007")]
#[test_action("codecommit", "GetRepositoryTriggers", checksum = "b7b6fd2a")]
#[test_action("codecommit", "ListApprovalRuleTemplates", checksum = "6e13c199")]
#[test_action(
    "codecommit",
    "ListAssociatedApprovalRuleTemplatesForRepository",
    checksum = "5404643c"
)]
#[test_action("codecommit", "ListBranches", checksum = "004d9c63")]
#[test_action("codecommit", "ListFileCommitHistory", checksum = "4a8e89a4")]
#[test_action("codecommit", "ListPullRequests", checksum = "480e328b")]
#[test_action("codecommit", "ListRepositories", checksum = "c5d47a23")]
#[test_action(
    "codecommit",
    "ListRepositoriesForApprovalRuleTemplate",
    checksum = "5e99897f"
)]
#[test_action("codecommit", "ListTagsForResource", checksum = "01522344")]
#[test_action("codecommit", "MergeBranchesByFastForward", checksum = "4bef9c0e")]
#[test_action("codecommit", "MergeBranchesBySquash", checksum = "cde0977a")]
#[test_action("codecommit", "MergeBranchesByThreeWay", checksum = "50a72a84")]
#[test_action("codecommit", "MergePullRequestByFastForward", checksum = "57b1ae62")]
#[test_action("codecommit", "MergePullRequestBySquash", checksum = "9f3e9ebc")]
#[test_action("codecommit", "MergePullRequestByThreeWay", checksum = "5d7d4048")]
#[test_action(
    "codecommit",
    "OverridePullRequestApprovalRules",
    checksum = "8dc307d9"
)]
#[test_action("codecommit", "PostCommentForComparedCommit", checksum = "d099f84a")]
#[test_action("codecommit", "PostCommentForPullRequest", checksum = "a87af735")]
#[test_action("codecommit", "PostCommentReply", checksum = "d1a61208")]
#[test_action("codecommit", "PutCommentReaction", checksum = "176eca44")]
#[test_action("codecommit", "PutFile", checksum = "793f2176")]
#[test_action("codecommit", "PutRepositoryTriggers", checksum = "990c1c99")]
#[test_action("codecommit", "TagResource", checksum = "4ecd584c")]
#[test_action("codecommit", "TestRepositoryTriggers", checksum = "9f4477ef")]
#[test_action("codecommit", "UntagResource", checksum = "7e1dd1c9")]
#[test_action(
    "codecommit",
    "UpdateApprovalRuleTemplateContent",
    checksum = "6acd5062"
)]
#[test_action(
    "codecommit",
    "UpdateApprovalRuleTemplateDescription",
    checksum = "7bada12c"
)]
#[test_action("codecommit", "UpdateApprovalRuleTemplateName", checksum = "3a7c7c6a")]
#[test_action("codecommit", "UpdateComment", checksum = "7557494c")]
#[test_action("codecommit", "UpdateDefaultBranch", checksum = "da6ee193")]
#[test_action(
    "codecommit",
    "UpdatePullRequestApprovalRuleContent",
    checksum = "3c75067a"
)]
#[test_action("codecommit", "UpdatePullRequestApprovalState", checksum = "a0ffb247")]
#[test_action("codecommit", "UpdatePullRequestDescription", checksum = "4bf8f201")]
#[test_action("codecommit", "UpdatePullRequestStatus", checksum = "126ff45e")]
#[test_action("codecommit", "UpdatePullRequestTitle", checksum = "a8d279dc")]
#[test_action("codecommit", "UpdateRepositoryDescription", checksum = "7e46f84a")]
#[test_action("codecommit", "UpdateRepositoryEncryptionKey", checksum = "e46a29c5")]
#[test_action("codecommit", "UpdateRepositoryName", checksum = "df35180c")]
#[tokio::test]
async fn codecommit_conformance() {
    let _server = TestServer::start().await;
}
