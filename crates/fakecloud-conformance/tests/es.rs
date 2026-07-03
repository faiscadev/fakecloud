mod helpers;

use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

#[test_action(
    "es",
    "AcceptInboundCrossClusterSearchConnection",
    checksum = "5632f7c3"
)]
#[test_action("es", "AddTags", checksum = "48b59226")]
#[test_action("es", "AssociatePackage", checksum = "4387de5c")]
#[test_action("es", "AuthorizeVpcEndpointAccess", checksum = "dfb66a35")]
#[test_action("es", "CancelDomainConfigChange", checksum = "ac9c5f6f")]
#[test_action(
    "es",
    "CancelElasticsearchServiceSoftwareUpdate",
    checksum = "dfdf94e7"
)]
#[test_action("es", "CreateElasticsearchDomain", checksum = "f80b7cbe")]
#[test_action(
    "es",
    "CreateOutboundCrossClusterSearchConnection",
    checksum = "45a30531"
)]
#[test_action("es", "CreatePackage", checksum = "c2dcccf9")]
#[test_action("es", "CreateVpcEndpoint", checksum = "c76d4594")]
#[test_action("es", "DeleteElasticsearchDomain", checksum = "179f8127")]
#[test_action("es", "DeleteElasticsearchServiceRole", checksum = "99948922")]
#[test_action(
    "es",
    "DeleteInboundCrossClusterSearchConnection",
    checksum = "d4367dba"
)]
#[test_action(
    "es",
    "DeleteOutboundCrossClusterSearchConnection",
    checksum = "8d236378"
)]
#[test_action("es", "DeletePackage", checksum = "f0565698")]
#[test_action("es", "DeleteVpcEndpoint", checksum = "1b75a90b")]
#[test_action("es", "DescribeDomainAutoTunes", checksum = "77b62a64")]
#[test_action("es", "DescribeDomainChangeProgress", checksum = "ae8b706e")]
#[test_action("es", "DescribeElasticsearchDomain", checksum = "30d0143b")]
#[test_action("es", "DescribeElasticsearchDomainConfig", checksum = "aaefd354")]
#[test_action("es", "DescribeElasticsearchDomains", checksum = "f1a2ae0f")]
#[test_action("es", "DescribeElasticsearchInstanceTypeLimits", checksum = "ce31afef")]
#[test_action(
    "es",
    "DescribeInboundCrossClusterSearchConnections",
    checksum = "ad919d59"
)]
#[test_action(
    "es",
    "DescribeOutboundCrossClusterSearchConnections",
    checksum = "a0421eb8"
)]
#[test_action("es", "DescribePackages", checksum = "b889687d")]
#[test_action(
    "es",
    "DescribeReservedElasticsearchInstanceOfferings",
    checksum = "86e1e8c1"
)]
#[test_action("es", "DescribeReservedElasticsearchInstances", checksum = "b8735fd9")]
#[test_action("es", "DescribeVpcEndpoints", checksum = "9d455e2d")]
#[test_action("es", "DissociatePackage", checksum = "4a152861")]
#[test_action("es", "GetCompatibleElasticsearchVersions", checksum = "f4eaf4ed")]
#[test_action("es", "GetPackageVersionHistory", checksum = "cd85cad6")]
#[test_action("es", "GetUpgradeHistory", checksum = "3efd27ab")]
#[test_action("es", "GetUpgradeStatus", checksum = "3e100554")]
#[test_action("es", "ListDomainNames", checksum = "eea987ea")]
#[test_action("es", "ListDomainsForPackage", checksum = "a8e68ba1")]
#[test_action("es", "ListElasticsearchInstanceTypes", checksum = "fe08c2e7")]
#[test_action("es", "ListElasticsearchVersions", checksum = "0354f59f")]
#[test_action("es", "ListPackagesForDomain", checksum = "dffb9002")]
#[test_action("es", "ListTags", checksum = "e902f5a0")]
#[test_action("es", "ListVpcEndpointAccess", checksum = "7d975764")]
#[test_action("es", "ListVpcEndpoints", checksum = "e9394d40")]
#[test_action("es", "ListVpcEndpointsForDomain", checksum = "4edc2cc0")]
#[test_action(
    "es",
    "PurchaseReservedElasticsearchInstanceOffering",
    checksum = "60d100c8"
)]
#[test_action(
    "es",
    "RejectInboundCrossClusterSearchConnection",
    checksum = "f37d6003"
)]
#[test_action("es", "RemoveTags", checksum = "278646a2")]
#[test_action("es", "RevokeVpcEndpointAccess", checksum = "f3d0c08f")]
#[test_action("es", "StartElasticsearchServiceSoftwareUpdate", checksum = "8ffd378b")]
#[test_action("es", "UpdateElasticsearchDomainConfig", checksum = "77a35f7f")]
#[test_action("es", "UpdatePackage", checksum = "8495978e")]
#[test_action("es", "UpdateVpcEndpoint", checksum = "5a8721bd")]
#[test_action("es", "UpgradeElasticsearchDomain", checksum = "97d0de6d")]
#[tokio::test]
async fn es_probe() {
    let _server = TestServer::start().await;
}
