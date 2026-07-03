mod helpers;

use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

#[test_action("identitystore", "CreateGroup", checksum = "6e221c01")]
#[test_action("identitystore", "CreateGroupMembership", checksum = "78ec5e14")]
#[test_action("identitystore", "CreateUser", checksum = "57ed0208")]
#[test_action("identitystore", "DeleteGroup", checksum = "3cdbb0fd")]
#[test_action("identitystore", "DeleteGroupMembership", checksum = "ba632b0b")]
#[test_action("identitystore", "DeleteUser", checksum = "a94d0b17")]
#[test_action("identitystore", "DescribeGroup", checksum = "e6cb62ae")]
#[test_action("identitystore", "DescribeGroupMembership", checksum = "890fb0b4")]
#[test_action("identitystore", "DescribeUser", checksum = "c5addd3c")]
#[test_action("identitystore", "GetGroupId", checksum = "6b2f623f")]
#[test_action("identitystore", "GetGroupMembershipId", checksum = "f880bc3f")]
#[test_action("identitystore", "GetUserId", checksum = "f19be109")]
#[test_action("identitystore", "IsMemberInGroups", checksum = "40b81a2c")]
#[test_action("identitystore", "ListGroupMemberships", checksum = "86da132e")]
#[test_action(
    "identitystore",
    "ListGroupMembershipsForMember",
    checksum = "81b78209"
)]
#[test_action("identitystore", "ListGroups", checksum = "634948d4")]
#[test_action("identitystore", "ListUsers", checksum = "e307ec47")]
#[test_action("identitystore", "UpdateGroup", checksum = "595f7a7f")]
#[test_action("identitystore", "UpdateUser", checksum = "95660264")]
#[tokio::test]
async fn identitystore_probe() {
    let _server = TestServer::start().await;
}
