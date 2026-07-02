mod helpers;

use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

#[test_action("servicediscovery", "CreateHttpNamespace", checksum = "3c5aeb2f")]
#[test_action("servicediscovery", "CreatePrivateDnsNamespace", checksum = "076b1899")]
#[test_action("servicediscovery", "CreatePublicDnsNamespace", checksum = "2fbd3194")]
#[test_action("servicediscovery", "DeleteNamespace", checksum = "22aa67f3")]
#[test_action("servicediscovery", "GetNamespace", checksum = "66bc34e2")]
#[test_action("servicediscovery", "GetOperation", checksum = "915a62d4")]
#[test_action("servicediscovery", "ListNamespaces", checksum = "49c86a92")]
#[test_action("servicediscovery", "ListOperations", checksum = "1208620c")]
#[test_action("servicediscovery", "UpdateHttpNamespace", checksum = "c261b30d")]
#[test_action("servicediscovery", "UpdatePrivateDnsNamespace", checksum = "6c03aca7")]
#[test_action("servicediscovery", "UpdatePublicDnsNamespace", checksum = "ffec59c5")]
#[test_action("servicediscovery", "CreateService", checksum = "80fb7bbc")]
#[test_action("servicediscovery", "GetService", checksum = "9d793b84")]
#[test_action("servicediscovery", "ListServices", checksum = "7a671b03")]
#[test_action("servicediscovery", "UpdateService", checksum = "4fc955ef")]
#[test_action("servicediscovery", "DeleteService", checksum = "b49d2bf7")]
#[test_action("servicediscovery", "GetServiceAttributes", checksum = "49cfab99")]
#[test_action("servicediscovery", "UpdateServiceAttributes", checksum = "f9ec4ca8")]
#[test_action("servicediscovery", "DeleteServiceAttributes", checksum = "f64d14f7")]
#[test_action("servicediscovery", "RegisterInstance", checksum = "32c1a456")]
#[test_action("servicediscovery", "DeregisterInstance", checksum = "6847c4d6")]
#[test_action("servicediscovery", "GetInstance", checksum = "ea10be2d")]
#[test_action("servicediscovery", "ListInstances", checksum = "00a8e1d4")]
#[test_action("servicediscovery", "GetInstancesHealthStatus", checksum = "d0c211a5")]
#[test_action("servicediscovery", "UpdateInstanceCustomHealthStatus", checksum = "c1e0b340")]
#[test_action("servicediscovery", "DiscoverInstances", checksum = "f0769d88")]
#[test_action("servicediscovery", "DiscoverInstancesRevision", checksum = "8968362e")]
#[test_action("servicediscovery", "TagResource", checksum = "6bf142a6")]
#[test_action("servicediscovery", "UntagResource", checksum = "d28c61d9")]
#[test_action("servicediscovery", "ListTagsForResource", checksum = "190fcb99")]
#[tokio::test]
async fn servicediscovery_probe() {
    let _server = TestServer::start().await;
}
