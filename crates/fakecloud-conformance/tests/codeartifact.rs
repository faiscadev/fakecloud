mod helpers;

use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

#[test_action("codeartifact", "AssociateExternalConnection", checksum = "729defde")]
#[test_action("codeartifact", "CopyPackageVersions", checksum = "06e8a8d4")]
#[test_action("codeartifact", "CreateDomain", checksum = "4647c22b")]
#[test_action("codeartifact", "CreatePackageGroup", checksum = "319ce8b4")]
#[test_action("codeartifact", "CreateRepository", checksum = "b6715483")]
#[test_action("codeartifact", "DeleteDomain", checksum = "45821fd0")]
#[test_action("codeartifact", "DeleteDomainPermissionsPolicy", checksum = "7a4b6040")]
#[test_action("codeartifact", "DeletePackage", checksum = "d07d9520")]
#[test_action("codeartifact", "DeletePackageGroup", checksum = "b56b4b67")]
#[test_action("codeartifact", "DeletePackageVersions", checksum = "b410e722")]
#[test_action("codeartifact", "DeleteRepository", checksum = "414b749c")]
#[test_action(
    "codeartifact",
    "DeleteRepositoryPermissionsPolicy",
    checksum = "c8be4531"
)]
#[test_action("codeartifact", "DescribeDomain", checksum = "489848af")]
#[test_action("codeartifact", "DescribePackage", checksum = "2c3d6f8b")]
#[test_action("codeartifact", "DescribePackageGroup", checksum = "eb5d0cd1")]
#[test_action("codeartifact", "DescribePackageVersion", checksum = "7d2ba525")]
#[test_action("codeartifact", "DescribeRepository", checksum = "e4546d46")]
#[test_action(
    "codeartifact",
    "DisassociateExternalConnection",
    checksum = "fb90ee8e"
)]
#[test_action("codeartifact", "DisposePackageVersions", checksum = "7a6ce3ed")]
#[test_action("codeartifact", "GetAssociatedPackageGroup", checksum = "0e618088")]
#[test_action("codeartifact", "GetAuthorizationToken", checksum = "5f6d3f8f")]
#[test_action("codeartifact", "GetDomainPermissionsPolicy", checksum = "d0ad4bba")]
#[test_action("codeartifact", "GetPackageVersionAsset", checksum = "89398547")]
#[test_action("codeartifact", "GetPackageVersionReadme", checksum = "580c4da3")]
#[test_action("codeartifact", "GetRepositoryEndpoint", checksum = "386d8793")]
#[test_action(
    "codeartifact",
    "GetRepositoryPermissionsPolicy",
    checksum = "e2946a83"
)]
#[test_action(
    "codeartifact",
    "ListAllowedRepositoriesForGroup",
    checksum = "4e11a10d"
)]
#[test_action("codeartifact", "ListAssociatedPackages", checksum = "b91ccb8c")]
#[test_action("codeartifact", "ListDomains", checksum = "8d54e300")]
#[test_action("codeartifact", "ListPackageGroups", checksum = "28244028")]
#[test_action("codeartifact", "ListPackageVersionAssets", checksum = "b66fc604")]
#[test_action(
    "codeartifact",
    "ListPackageVersionDependencies",
    checksum = "5c49989a"
)]
#[test_action("codeartifact", "ListPackageVersions", checksum = "bab7fdd8")]
#[test_action("codeartifact", "ListPackages", checksum = "c704d787")]
#[test_action("codeartifact", "ListRepositories", checksum = "102daede")]
#[test_action("codeartifact", "ListRepositoriesInDomain", checksum = "22b3f19c")]
#[test_action("codeartifact", "ListSubPackageGroups", checksum = "40447346")]
#[test_action("codeartifact", "ListTagsForResource", checksum = "618b9f8c")]
#[test_action("codeartifact", "PublishPackageVersion", checksum = "a3117deb")]
#[test_action("codeartifact", "PutDomainPermissionsPolicy", checksum = "0dd0bf45")]
#[test_action("codeartifact", "PutPackageOriginConfiguration", checksum = "2ff26410")]
#[test_action(
    "codeartifact",
    "PutRepositoryPermissionsPolicy",
    checksum = "da3cd3b4"
)]
#[test_action("codeartifact", "TagResource", checksum = "7837fa30")]
#[test_action("codeartifact", "UntagResource", checksum = "927fae4f")]
#[test_action("codeartifact", "UpdatePackageGroup", checksum = "4f8277d3")]
#[test_action(
    "codeartifact",
    "UpdatePackageGroupOriginConfiguration",
    checksum = "80e443d7"
)]
#[test_action("codeartifact", "UpdatePackageVersionsStatus", checksum = "01140dcd")]
#[test_action("codeartifact", "UpdateRepository", checksum = "1a03e8ea")]
#[tokio::test]
async fn codeartifact_conformance() {
    let _server = TestServer::start().await;
}
