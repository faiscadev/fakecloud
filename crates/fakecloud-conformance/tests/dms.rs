//! DMS (`dms`) awsJson1.1 conformance probe assertions.
//!
//! One `#[test_action]` per operation in the DMS Smithy model. Checksums
//! are harvested from `aws-models/dms.json`; the probe generates request
//! variants per operation and asserts fakecloud responds AWS-faithfully.

mod helpers;

use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

#[test_action("dms", "AddTagsToResource", checksum = "8943e120")]
#[test_action("dms", "ApplyPendingMaintenanceAction", checksum = "39e2c8d6")]
#[test_action("dms", "BatchStartRecommendations", checksum = "f79a4591")]
#[test_action("dms", "CancelMetadataModelConversion", checksum = "09fe95e6")]
#[test_action("dms", "CancelMetadataModelCreation", checksum = "f85d9a85")]
#[test_action("dms", "CancelReplicationTaskAssessmentRun", checksum = "e1a80834")]
#[test_action("dms", "CreateDataMigration", checksum = "a061d453")]
#[test_action("dms", "CreateDataProvider", checksum = "d75f9c8c")]
#[test_action("dms", "CreateEndpoint", checksum = "e9212701")]
#[test_action("dms", "CreateEventSubscription", checksum = "a0da27ec")]
#[test_action("dms", "CreateFleetAdvisorCollector", checksum = "b3c0e3d5")]
#[test_action("dms", "CreateInstanceProfile", checksum = "da561403")]
#[test_action("dms", "CreateMigrationProject", checksum = "6a752802")]
#[test_action("dms", "CreateReplicationConfig", checksum = "b4d3bb53")]
#[test_action("dms", "CreateReplicationInstance", checksum = "76319f4c")]
#[test_action("dms", "CreateReplicationSubnetGroup", checksum = "72dbcef2")]
#[test_action("dms", "CreateReplicationTask", checksum = "d1952f43")]
#[test_action("dms", "DeleteCertificate", checksum = "291f4005")]
#[test_action("dms", "DeleteConnection", checksum = "abb698be")]
#[test_action("dms", "DeleteDataMigration", checksum = "af5713bd")]
#[test_action("dms", "DeleteDataProvider", checksum = "021dcae3")]
#[test_action("dms", "DeleteEndpoint", checksum = "ca48788c")]
#[test_action("dms", "DeleteEventSubscription", checksum = "0206094c")]
#[test_action("dms", "DeleteFleetAdvisorCollector", checksum = "3c8c790f")]
#[test_action("dms", "DeleteFleetAdvisorDatabases", checksum = "296a9075")]
#[test_action("dms", "DeleteInstanceProfile", checksum = "6dfffb6a")]
#[test_action("dms", "DeleteMigrationProject", checksum = "b9e19041")]
#[test_action("dms", "DeleteReplicationConfig", checksum = "348cdb91")]
#[test_action("dms", "DeleteReplicationInstance", checksum = "2f8d47a8")]
#[test_action("dms", "DeleteReplicationSubnetGroup", checksum = "a14ff272")]
#[test_action("dms", "DeleteReplicationTask", checksum = "cfe0de70")]
#[test_action("dms", "DeleteReplicationTaskAssessmentRun", checksum = "57db6bc1")]
#[test_action("dms", "DescribeAccountAttributes", checksum = "b75b9e34")]
#[test_action(
    "dms",
    "DescribeApplicableIndividualAssessments",
    checksum = "503392f1"
)]
#[test_action("dms", "DescribeCertificates", checksum = "6c8b684e")]
#[test_action("dms", "DescribeConnections", checksum = "136ec040")]
#[test_action("dms", "DescribeConversionConfiguration", checksum = "1e2abb75")]
#[test_action("dms", "DescribeDataMigrations", checksum = "3db89280")]
#[test_action("dms", "DescribeDataProviders", checksum = "774d5d3c")]
#[test_action("dms", "DescribeEndpointSettings", checksum = "3a1bb229")]
#[test_action("dms", "DescribeEndpointTypes", checksum = "3ad173ab")]
#[test_action("dms", "DescribeEndpoints", checksum = "87b8fa15")]
#[test_action("dms", "DescribeEngineVersions", checksum = "dd2297fa")]
#[test_action("dms", "DescribeEventCategories", checksum = "509de359")]
#[test_action("dms", "DescribeEventSubscriptions", checksum = "debee493")]
#[test_action("dms", "DescribeEvents", checksum = "df33c996")]
#[test_action("dms", "DescribeExtensionPackAssociations", checksum = "25f7d6bf")]
#[test_action("dms", "DescribeFleetAdvisorCollectors", checksum = "98f9992f")]
#[test_action("dms", "DescribeFleetAdvisorDatabases", checksum = "af28ecf6")]
#[test_action("dms", "DescribeFleetAdvisorLsaAnalysis", checksum = "11a1df12")]
#[test_action(
    "dms",
    "DescribeFleetAdvisorSchemaObjectSummary",
    checksum = "507bbfc6"
)]
#[test_action("dms", "DescribeFleetAdvisorSchemas", checksum = "41a287b8")]
#[test_action("dms", "DescribeInstanceProfiles", checksum = "a20174c2")]
#[test_action("dms", "DescribeMetadataModel", checksum = "e5903048")]
#[test_action("dms", "DescribeMetadataModelAssessments", checksum = "8ccb9ec5")]
#[test_action("dms", "DescribeMetadataModelChildren", checksum = "ad6cbda0")]
#[test_action("dms", "DescribeMetadataModelConversions", checksum = "6f202d08")]
#[test_action("dms", "DescribeMetadataModelCreations", checksum = "6c7c5e98")]
#[test_action("dms", "DescribeMetadataModelExportsAsScript", checksum = "a58cc918")]
#[test_action("dms", "DescribeMetadataModelExportsToTarget", checksum = "42aec17b")]
#[test_action("dms", "DescribeMetadataModelImports", checksum = "3ad268ae")]
#[test_action("dms", "DescribeMigrationProjects", checksum = "e28146da")]
#[test_action("dms", "DescribeOrderableReplicationInstances", checksum = "7e934ba5")]
#[test_action("dms", "DescribePendingMaintenanceActions", checksum = "bb4d7065")]
#[test_action("dms", "DescribeRecommendationLimitations", checksum = "0043fead")]
#[test_action("dms", "DescribeRecommendations", checksum = "7c494486")]
#[test_action("dms", "DescribeRefreshSchemasStatus", checksum = "31d24944")]
#[test_action("dms", "DescribeReplicationConfigs", checksum = "5ed3241c")]
#[test_action("dms", "DescribeReplicationInstanceTaskLogs", checksum = "57438d75")]
#[test_action("dms", "DescribeReplicationInstances", checksum = "a381a8e8")]
#[test_action("dms", "DescribeReplicationSubnetGroups", checksum = "7eb010d9")]
#[test_action("dms", "DescribeReplicationTableStatistics", checksum = "581ffccd")]
#[test_action(
    "dms",
    "DescribeReplicationTaskAssessmentResults",
    checksum = "eb278b64"
)]
#[test_action("dms", "DescribeReplicationTaskAssessmentRuns", checksum = "6759c89c")]
#[test_action(
    "dms",
    "DescribeReplicationTaskIndividualAssessments",
    checksum = "4558b611"
)]
#[test_action("dms", "DescribeReplicationTasks", checksum = "ddccdef6")]
#[test_action("dms", "DescribeReplications", checksum = "ce083670")]
#[test_action("dms", "DescribeSchemas", checksum = "b995a7b1")]
#[test_action("dms", "DescribeTableStatistics", checksum = "091137fe")]
#[test_action("dms", "ExportMetadataModelAssessment", checksum = "9c5342f4")]
#[test_action("dms", "GetTargetSelectionRules", checksum = "e95b8c71")]
#[test_action("dms", "ImportCertificate", checksum = "d161ce60")]
#[test_action("dms", "ListTagsForResource", checksum = "a2ca4dde")]
#[test_action("dms", "ModifyConversionConfiguration", checksum = "a4726fa6")]
#[test_action("dms", "ModifyDataMigration", checksum = "f9781451")]
#[test_action("dms", "ModifyDataProvider", checksum = "98117d48")]
#[test_action("dms", "ModifyEndpoint", checksum = "8b38f214")]
#[test_action("dms", "ModifyEventSubscription", checksum = "4f901684")]
#[test_action("dms", "ModifyInstanceProfile", checksum = "e03a16db")]
#[test_action("dms", "ModifyMigrationProject", checksum = "8e76f7b1")]
#[test_action("dms", "ModifyReplicationConfig", checksum = "3b8fa554")]
#[test_action("dms", "ModifyReplicationInstance", checksum = "08c1261a")]
#[test_action("dms", "ModifyReplicationSubnetGroup", checksum = "a63870e7")]
#[test_action("dms", "ModifyReplicationTask", checksum = "2735657c")]
#[test_action("dms", "MoveReplicationTask", checksum = "9ac4787c")]
#[test_action("dms", "RebootReplicationInstance", checksum = "fe81e48c")]
#[test_action("dms", "RefreshSchemas", checksum = "8f5b221c")]
#[test_action("dms", "ReloadReplicationTables", checksum = "0250562d")]
#[test_action("dms", "ReloadTables", checksum = "6720a4f3")]
#[test_action("dms", "RemoveTagsFromResource", checksum = "016beccd")]
#[test_action("dms", "RunFleetAdvisorLsaAnalysis", checksum = "e594478e")]
#[test_action("dms", "StartDataMigration", checksum = "d96a85ae")]
#[test_action("dms", "StartExtensionPackAssociation", checksum = "ebc8e369")]
#[test_action("dms", "StartMetadataModelAssessment", checksum = "273da468")]
#[test_action("dms", "StartMetadataModelConversion", checksum = "abd62bfb")]
#[test_action("dms", "StartMetadataModelCreation", checksum = "00a348bd")]
#[test_action("dms", "StartMetadataModelExportAsScript", checksum = "94b51c87")]
#[test_action("dms", "StartMetadataModelExportToTarget", checksum = "c5014f79")]
#[test_action("dms", "StartMetadataModelImport", checksum = "81aeb9f6")]
#[test_action("dms", "StartRecommendations", checksum = "f8436b66")]
#[test_action("dms", "StartReplication", checksum = "3314d2b9")]
#[test_action("dms", "StartReplicationTask", checksum = "bc0ce3d3")]
#[test_action("dms", "StartReplicationTaskAssessment", checksum = "117a1b18")]
#[test_action("dms", "StartReplicationTaskAssessmentRun", checksum = "92e4e1b2")]
#[test_action("dms", "StopDataMigration", checksum = "c4db699c")]
#[test_action("dms", "StopReplication", checksum = "d2d12bb2")]
#[test_action("dms", "StopReplicationTask", checksum = "90bc56a0")]
#[test_action("dms", "TestConnection", checksum = "e3496328")]
#[test_action("dms", "UpdateSubscriptionsToEventBridge", checksum = "9b7577ce")]
#[tokio::test]
async fn dms_probe() {
    let _server = TestServer::start().await;
}
