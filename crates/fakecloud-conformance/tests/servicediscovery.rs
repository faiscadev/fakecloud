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
#[tokio::test]
async fn servicediscovery_probe() {
    let _server = TestServer::start().await;
}
