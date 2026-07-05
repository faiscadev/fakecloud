mod helpers;

use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

#[test_action("mq", "CreateBroker", checksum = "5863b759")]
#[test_action("mq", "CreateConfiguration", checksum = "48ecbb9b")]
#[test_action("mq", "CreateTags", checksum = "0776e85f")]
#[test_action("mq", "CreateUser", checksum = "2429434e")]
#[test_action("mq", "DeleteBroker", checksum = "2edfa783")]
#[test_action("mq", "DeleteConfiguration", checksum = "72c0202f")]
#[test_action("mq", "DeleteTags", checksum = "7b1de8ef")]
#[test_action("mq", "DeleteUser", checksum = "98419498")]
#[test_action("mq", "DescribeBroker", checksum = "71fa32a9")]
#[test_action("mq", "DescribeBrokerEngineTypes", checksum = "2571c8f4")]
#[test_action("mq", "DescribeBrokerInstanceOptions", checksum = "6cbde8cc")]
#[test_action("mq", "DescribeConfiguration", checksum = "39367380")]
#[test_action("mq", "DescribeConfigurationRevision", checksum = "c75308c3")]
#[test_action("mq", "DescribeSharedResources", checksum = "de46186d")]
#[test_action("mq", "DescribeUser", checksum = "b1c62077")]
#[test_action("mq", "ListBrokers", checksum = "f800a6f7")]
#[test_action("mq", "ListConfigurationRevisions", checksum = "2dc3fab3")]
#[test_action("mq", "ListConfigurations", checksum = "67893bb6")]
#[test_action("mq", "ListTags", checksum = "2a2df519")]
#[test_action("mq", "ListUsers", checksum = "fffbfd63")]
#[test_action("mq", "Promote", checksum = "bc929ff5")]
#[test_action("mq", "RebootBroker", checksum = "ad2c894d")]
#[test_action("mq", "UpdateBroker", checksum = "f9f15222")]
#[test_action("mq", "UpdateConfiguration", checksum = "fe9a0aeb")]
#[test_action("mq", "UpdateUser", checksum = "3e8e7549")]
#[tokio::test]
async fn mq_conformance() {
    let _server = TestServer::start().await;
}
