mod helpers;

use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

#[test_action("lakeformation", "AddLFTagsToResource", checksum = "d4d8c51f")]
#[test_action("lakeformation", "AssumeDecoratedRoleWithSAML", checksum = "e82ee9b5")]
#[test_action("lakeformation", "BatchGrantPermissions", checksum = "1b50bec0")]
#[test_action("lakeformation", "BatchRevokePermissions", checksum = "a7382a9e")]
#[test_action("lakeformation", "CancelTransaction", checksum = "d1acd175")]
#[test_action("lakeformation", "CommitTransaction", checksum = "ae3045c8")]
#[test_action("lakeformation", "CreateDataCellsFilter", checksum = "78b2e203")]
#[test_action("lakeformation", "CreateLFTag", checksum = "1e3e3c76")]
#[test_action("lakeformation", "CreateLFTagExpression", checksum = "5e92fdcd")]
#[test_action(
    "lakeformation",
    "CreateLakeFormationIdentityCenterConfiguration",
    checksum = "2085b934"
)]
#[test_action("lakeformation", "CreateLakeFormationOptIn", checksum = "1fcff43e")]
#[test_action("lakeformation", "DeleteDataCellsFilter", checksum = "4712443c")]
#[test_action("lakeformation", "DeleteLFTag", checksum = "8850abc4")]
#[test_action("lakeformation", "DeleteLFTagExpression", checksum = "a7c95c45")]
#[test_action(
    "lakeformation",
    "DeleteLakeFormationIdentityCenterConfiguration",
    checksum = "c4b1b43c"
)]
#[test_action("lakeformation", "DeleteLakeFormationOptIn", checksum = "d15a0bf0")]
#[test_action("lakeformation", "DeleteObjectsOnCancel", checksum = "5ac55ab9")]
#[test_action("lakeformation", "DeregisterResource", checksum = "4b8c7ad9")]
#[test_action(
    "lakeformation",
    "DescribeLakeFormationIdentityCenterConfiguration",
    checksum = "63282eb0"
)]
#[test_action("lakeformation", "DescribeResource", checksum = "770306f8")]
#[test_action("lakeformation", "DescribeTransaction", checksum = "c79dd067")]
#[test_action("lakeformation", "ExtendTransaction", checksum = "17bdd1ee")]
#[test_action("lakeformation", "GetDataCellsFilter", checksum = "7c568b71")]
#[test_action("lakeformation", "GetDataLakePrincipal", checksum = "67ac1c5e")]
#[test_action("lakeformation", "GetDataLakeSettings", checksum = "4a8004a5")]
#[test_action(
    "lakeformation",
    "GetEffectivePermissionsForPath",
    checksum = "7de74c2b"
)]
#[test_action("lakeformation", "GetLFTag", checksum = "6e3e3d4e")]
#[test_action("lakeformation", "GetLFTagExpression", checksum = "a7dbb64c")]
#[test_action("lakeformation", "GetQueryState", checksum = "be5d50b0")]
#[test_action("lakeformation", "GetQueryStatistics", checksum = "c4fe161c")]
#[test_action("lakeformation", "GetResourceLFTags", checksum = "9f00c572")]
#[test_action("lakeformation", "GetTableObjects", checksum = "bf0ea4e4")]
#[test_action(
    "lakeformation",
    "GetTemporaryDataLocationCredentials",
    checksum = "73980393"
)]
#[test_action(
    "lakeformation",
    "GetTemporaryGluePartitionCredentials",
    checksum = "ea8a49e8"
)]
#[test_action(
    "lakeformation",
    "GetTemporaryGlueTableCredentials",
    checksum = "984c43ba"
)]
#[test_action("lakeformation", "GetWorkUnitResults", checksum = "85fd7e89")]
#[test_action("lakeformation", "GetWorkUnits", checksum = "df087a71")]
#[test_action("lakeformation", "GrantPermissions", checksum = "73d8d11b")]
#[test_action("lakeformation", "ListDataCellsFilter", checksum = "acee8b5e")]
#[test_action("lakeformation", "ListLFTagExpressions", checksum = "a0e866c0")]
#[test_action("lakeformation", "ListLFTags", checksum = "7cfab7c3")]
#[test_action("lakeformation", "ListLakeFormationOptIns", checksum = "887abd92")]
#[test_action("lakeformation", "ListPermissions", checksum = "abde1b98")]
#[test_action("lakeformation", "ListResources", checksum = "6eef415a")]
#[test_action("lakeformation", "ListTableStorageOptimizers", checksum = "2570c9a5")]
#[test_action("lakeformation", "ListTransactions", checksum = "4e99c819")]
#[test_action("lakeformation", "PutDataLakeSettings", checksum = "604ed3e6")]
#[test_action("lakeformation", "RegisterResource", checksum = "4b6369ff")]
#[test_action("lakeformation", "RemoveLFTagsFromResource", checksum = "268b68f5")]
#[test_action("lakeformation", "RevokePermissions", checksum = "7bb63b25")]
#[test_action("lakeformation", "SearchDatabasesByLFTags", checksum = "eaa46ab6")]
#[test_action("lakeformation", "SearchTablesByLFTags", checksum = "392895fa")]
#[test_action("lakeformation", "StartQueryPlanning", checksum = "96f727e2")]
#[test_action("lakeformation", "StartTransaction", checksum = "5bcacb3d")]
#[test_action("lakeformation", "UpdateDataCellsFilter", checksum = "9d89ef76")]
#[test_action("lakeformation", "UpdateLFTag", checksum = "d452386f")]
#[test_action("lakeformation", "UpdateLFTagExpression", checksum = "b26eee0d")]
#[test_action(
    "lakeformation",
    "UpdateLakeFormationIdentityCenterConfiguration",
    checksum = "aa327f5e"
)]
#[test_action("lakeformation", "UpdateResource", checksum = "127ee5ea")]
#[test_action("lakeformation", "UpdateTableObjects", checksum = "4a9546a3")]
#[test_action("lakeformation", "UpdateTableStorageOptimizer", checksum = "464a1329")]
#[tokio::test]
async fn lakeformation_conformance() {
    let _server = TestServer::start().await;
}
