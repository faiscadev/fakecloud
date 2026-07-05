mod helpers;

use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

#[test_action("codedeploy", "AddTagsToOnPremisesInstances", checksum = "2fa61f4a")]
#[test_action("codedeploy", "BatchGetApplicationRevisions", checksum = "ee0363fe")]
#[test_action("codedeploy", "BatchGetApplications", checksum = "d024a9b8")]
#[test_action("codedeploy", "BatchGetDeploymentGroups", checksum = "d7b61912")]
#[test_action("codedeploy", "BatchGetDeploymentInstances", checksum = "1a003697")]
#[test_action("codedeploy", "BatchGetDeployments", checksum = "7b0511fd")]
#[test_action("codedeploy", "BatchGetDeploymentTargets", checksum = "ae884ccd")]
#[test_action("codedeploy", "BatchGetOnPremisesInstances", checksum = "18ddcc3e")]
#[test_action("codedeploy", "ContinueDeployment", checksum = "06774d5f")]
#[test_action("codedeploy", "CreateApplication", checksum = "0f6e1a9b")]
#[test_action("codedeploy", "CreateDeployment", checksum = "9fd67d70")]
#[test_action("codedeploy", "CreateDeploymentConfig", checksum = "d13730f7")]
#[test_action("codedeploy", "CreateDeploymentGroup", checksum = "b245dc23")]
#[test_action("codedeploy", "DeleteApplication", checksum = "92b4d1bc")]
#[test_action("codedeploy", "DeleteDeploymentConfig", checksum = "32bb9ea0")]
#[test_action("codedeploy", "DeleteDeploymentGroup", checksum = "9f2fdff0")]
#[test_action("codedeploy", "DeleteGitHubAccountToken", checksum = "544cbe9c")]
#[test_action("codedeploy", "DeleteResourcesByExternalId", checksum = "85408a24")]
#[test_action("codedeploy", "DeregisterOnPremisesInstance", checksum = "c200d120")]
#[test_action("codedeploy", "GetApplication", checksum = "791b7ac5")]
#[test_action("codedeploy", "GetApplicationRevision", checksum = "7d667cd8")]
#[test_action("codedeploy", "GetDeployment", checksum = "47b3e2e2")]
#[test_action("codedeploy", "GetDeploymentConfig", checksum = "fe1a0804")]
#[test_action("codedeploy", "GetDeploymentGroup", checksum = "499e4a9b")]
#[test_action("codedeploy", "GetDeploymentInstance", checksum = "bc721610")]
#[test_action("codedeploy", "GetDeploymentTarget", checksum = "db0de697")]
#[test_action("codedeploy", "GetOnPremisesInstance", checksum = "70b3ea1c")]
#[test_action("codedeploy", "ListApplicationRevisions", checksum = "82c5b2e9")]
#[test_action("codedeploy", "ListApplications", checksum = "c8d7ba25")]
#[test_action("codedeploy", "ListDeploymentConfigs", checksum = "c2828887")]
#[test_action("codedeploy", "ListDeploymentGroups", checksum = "60b9d252")]
#[test_action("codedeploy", "ListDeploymentInstances", checksum = "457d447c")]
#[test_action("codedeploy", "ListDeployments", checksum = "fd884893")]
#[test_action("codedeploy", "ListDeploymentTargets", checksum = "771045c1")]
#[test_action("codedeploy", "ListGitHubAccountTokenNames", checksum = "bfcc3b38")]
#[test_action("codedeploy", "ListOnPremisesInstances", checksum = "fe89fcde")]
#[test_action("codedeploy", "ListTagsForResource", checksum = "4970349f")]
#[test_action(
    "codedeploy",
    "PutLifecycleEventHookExecutionStatus",
    checksum = "1659591c"
)]
#[test_action("codedeploy", "RegisterApplicationRevision", checksum = "fc529e98")]
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
#[test_action("codedeploy", "UpdateApplication", checksum = "38ac7bfc")]
#[test_action("codedeploy", "UpdateDeploymentGroup", checksum = "99664f43")]
#[tokio::test]
async fn codedeploy_conformance() {
    let _server = TestServer::start().await;
}
