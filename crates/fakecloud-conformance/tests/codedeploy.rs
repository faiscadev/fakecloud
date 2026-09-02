mod helpers;

use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

#[test_action("codedeploy", "AddTagsToOnPremisesInstances", checksum = "2fa61f4a")]
#[test_action("codedeploy", "BatchGetApplicationRevisions", checksum = "881be56a")]
#[test_action("codedeploy", "BatchGetApplications", checksum = "ced4a39b")]
#[test_action("codedeploy", "BatchGetDeploymentGroups", checksum = "28b3cd46")]
#[test_action("codedeploy", "BatchGetDeploymentInstances", checksum = "1a003697")]
#[test_action("codedeploy", "BatchGetDeployments", checksum = "723de778")]
#[test_action("codedeploy", "BatchGetDeploymentTargets", checksum = "ae884ccd")]
#[test_action("codedeploy", "BatchGetOnPremisesInstances", checksum = "18ddcc3e")]
#[test_action("codedeploy", "ContinueDeployment", checksum = "06774d5f")]
#[test_action("codedeploy", "CreateApplication", checksum = "86507d1e")]
#[test_action("codedeploy", "CreateDeployment", checksum = "094a4d3f")]
#[test_action("codedeploy", "CreateDeploymentConfig", checksum = "3f8ffaca")]
#[test_action("codedeploy", "CreateDeploymentGroup", checksum = "9e0441c8")]
#[test_action("codedeploy", "DeleteApplication", checksum = "e8ce4dd0")]
#[test_action("codedeploy", "DeleteDeploymentConfig", checksum = "67d47f86")]
#[test_action("codedeploy", "DeleteDeploymentGroup", checksum = "3e5b0312")]
#[test_action("codedeploy", "DeleteGitHubAccountToken", checksum = "544cbe9c")]
#[test_action("codedeploy", "DeleteResourcesByExternalId", checksum = "85408a24")]
#[test_action("codedeploy", "DeregisterOnPremisesInstance", checksum = "c200d120")]
#[test_action("codedeploy", "GetApplication", checksum = "fe4c0ef2")]
#[test_action("codedeploy", "GetApplicationRevision", checksum = "c764c1ac")]
#[test_action("codedeploy", "GetDeployment", checksum = "6bd84486")]
#[test_action("codedeploy", "GetDeploymentConfig", checksum = "4f01599a")]
#[test_action("codedeploy", "GetDeploymentGroup", checksum = "c9441587")]
#[test_action("codedeploy", "GetDeploymentInstance", checksum = "bc721610")]
#[test_action("codedeploy", "GetDeploymentTarget", checksum = "db0de697")]
#[test_action("codedeploy", "GetOnPremisesInstance", checksum = "70b3ea1c")]
#[test_action("codedeploy", "ListApplicationRevisions", checksum = "2b6eb82a")]
#[test_action("codedeploy", "ListApplications", checksum = "2d6fb52f")]
#[test_action("codedeploy", "ListDeploymentConfigs", checksum = "7d3865c5")]
#[test_action("codedeploy", "ListDeploymentGroups", checksum = "476566ad")]
#[test_action("codedeploy", "ListDeploymentInstances", checksum = "313c71da")]
#[test_action("codedeploy", "ListDeployments", checksum = "081bdb62")]
#[test_action("codedeploy", "ListDeploymentTargets", checksum = "e50e570b")]
#[test_action("codedeploy", "ListGitHubAccountTokenNames", checksum = "bfcc3b38")]
#[test_action("codedeploy", "ListOnPremisesInstances", checksum = "fe89fcde")]
#[test_action("codedeploy", "ListTagsForResource", checksum = "4970349f")]
#[test_action(
    "codedeploy",
    "PutLifecycleEventHookExecutionStatus",
    checksum = "1659591c"
)]
#[test_action("codedeploy", "RegisterApplicationRevision", checksum = "1b7ff76e")]
#[test_action("codedeploy", "RegisterOnPremisesInstance", checksum = "3816236f")]
#[test_action(
    "codedeploy",
    "RemoveTagsFromOnPremisesInstances",
    checksum = "a85fe953"
)]
#[test_action(
    "codedeploy",
    "SkipWaitTimeForInstanceTermination",
    checksum = "1e77039b"
)]
#[test_action("codedeploy", "StopDeployment", checksum = "dc6ac734")]
#[test_action("codedeploy", "TagResource", checksum = "e887cd24")]
#[test_action("codedeploy", "UntagResource", checksum = "856f66ab")]
#[test_action("codedeploy", "UpdateApplication", checksum = "3d152a99")]
#[test_action("codedeploy", "UpdateDeploymentGroup", checksum = "25796caa")]
#[tokio::test]
async fn codedeploy_conformance() {
    let _server = TestServer::start().await;
}
