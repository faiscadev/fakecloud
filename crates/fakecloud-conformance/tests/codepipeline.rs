mod helpers;

use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

#[test_action("codepipeline", "AcknowledgeJob", checksum = "149205a2")]
#[test_action("codepipeline", "AcknowledgeThirdPartyJob", checksum = "2f96f6f5")]
#[test_action("codepipeline", "CreateCustomActionType", checksum = "2d333e59")]
#[test_action("codepipeline", "CreatePipeline", checksum = "e5c0ef10")]
#[test_action("codepipeline", "DeleteCustomActionType", checksum = "2c63eed3")]
#[test_action("codepipeline", "DeletePipeline", checksum = "e022d077")]
#[test_action("codepipeline", "DeleteWebhook", checksum = "66a9bdfc")]
#[test_action(
    "codepipeline",
    "DeregisterWebhookWithThirdParty",
    checksum = "4f487875"
)]
#[test_action("codepipeline", "DisableStageTransition", checksum = "ca5bfd0e")]
#[test_action("codepipeline", "EnableStageTransition", checksum = "31635b03")]
#[test_action("codepipeline", "GetActionType", checksum = "2690f660")]
#[test_action("codepipeline", "GetJobDetails", checksum = "a67650b5")]
#[test_action("codepipeline", "GetPipeline", checksum = "875a7aee")]
#[test_action("codepipeline", "GetPipelineExecution", checksum = "4c334079")]
#[test_action("codepipeline", "GetPipelineState", checksum = "733ce633")]
#[test_action("codepipeline", "GetThirdPartyJobDetails", checksum = "29c4ff13")]
#[test_action("codepipeline", "ListActionExecutions", checksum = "33e4a2d1")]
#[test_action("codepipeline", "ListActionTypes", checksum = "9972b418")]
#[test_action(
    "codepipeline",
    "ListDeployActionExecutionTargets",
    checksum = "08865ea5"
)]
#[test_action("codepipeline", "ListPipelineExecutions", checksum = "203e41a9")]
#[test_action("codepipeline", "ListPipelines", checksum = "e98cca76")]
#[test_action("codepipeline", "ListRuleExecutions", checksum = "fe381f20")]
#[test_action("codepipeline", "ListRuleTypes", checksum = "2602e0a3")]
#[test_action("codepipeline", "ListTagsForResource", checksum = "5c6de6ad")]
#[test_action("codepipeline", "ListWebhooks", checksum = "4374f2bb")]
#[test_action("codepipeline", "OverrideStageCondition", checksum = "cb37e9f3")]
#[test_action("codepipeline", "PollForJobs", checksum = "8bb9e4da")]
#[test_action("codepipeline", "PollForThirdPartyJobs", checksum = "f7ff9780")]
#[test_action("codepipeline", "PutActionRevision", checksum = "4c5e4aa5")]
#[test_action("codepipeline", "PutApprovalResult", checksum = "d743f9f1")]
#[test_action("codepipeline", "PutJobFailureResult", checksum = "02715b50")]
#[test_action("codepipeline", "PutJobSuccessResult", checksum = "284a612e")]
#[test_action("codepipeline", "PutThirdPartyJobFailureResult", checksum = "8855f3a4")]
#[test_action("codepipeline", "PutThirdPartyJobSuccessResult", checksum = "cbffddad")]
#[test_action("codepipeline", "PutWebhook", checksum = "90a33a73")]
#[test_action("codepipeline", "RegisterWebhookWithThirdParty", checksum = "db3b4fd9")]
#[test_action("codepipeline", "RetryStageExecution", checksum = "342a8e96")]
#[test_action("codepipeline", "RollbackStage", checksum = "e477dd9b")]
#[test_action("codepipeline", "StartPipelineExecution", checksum = "251a848f")]
#[test_action("codepipeline", "StopPipelineExecution", checksum = "f2474f4d")]
#[test_action("codepipeline", "TagResource", checksum = "6b3ddc0f")]
#[test_action("codepipeline", "UntagResource", checksum = "87d63e84")]
#[test_action("codepipeline", "UpdateActionType", checksum = "504e92bd")]
#[test_action("codepipeline", "UpdatePipeline", checksum = "b34ee356")]
#[tokio::test]
async fn codepipeline_conformance() {
    let _server = TestServer::start().await;
}
