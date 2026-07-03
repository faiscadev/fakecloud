mod helpers;

use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

#[test_action("verifiedpermissions", "BatchGetPolicy", checksum = "1e0dda86")]
#[test_action("verifiedpermissions", "BatchIsAuthorized", checksum = "f0494887")]
#[test_action(
    "verifiedpermissions",
    "BatchIsAuthorizedWithToken",
    checksum = "e9096306"
)]
#[test_action("verifiedpermissions", "CreateIdentitySource", checksum = "b2dcf782")]
#[test_action("verifiedpermissions", "CreatePolicy", checksum = "6ff9d411")]
#[test_action("verifiedpermissions", "CreatePolicyStore", checksum = "9d1a19e8")]
#[test_action("verifiedpermissions", "CreatePolicyStoreAlias", checksum = "9dde516e")]
#[test_action("verifiedpermissions", "CreatePolicyTemplate", checksum = "2d183f9d")]
#[test_action("verifiedpermissions", "DeleteIdentitySource", checksum = "6e9eb885")]
#[test_action("verifiedpermissions", "DeletePolicy", checksum = "bfe4f831")]
#[test_action("verifiedpermissions", "DeletePolicyStore", checksum = "1b97baf7")]
#[test_action("verifiedpermissions", "DeletePolicyStoreAlias", checksum = "3b8635ae")]
#[test_action("verifiedpermissions", "DeletePolicyTemplate", checksum = "9027315e")]
#[test_action("verifiedpermissions", "GetIdentitySource", checksum = "849f55c2")]
#[test_action("verifiedpermissions", "GetPolicy", checksum = "43611c58")]
#[test_action("verifiedpermissions", "GetPolicyStore", checksum = "991530ab")]
#[test_action("verifiedpermissions", "GetPolicyStoreAlias", checksum = "de1f0255")]
#[test_action("verifiedpermissions", "GetPolicyTemplate", checksum = "6073d362")]
#[test_action("verifiedpermissions", "GetSchema", checksum = "bda673b5")]
#[test_action("verifiedpermissions", "IsAuthorized", checksum = "b8a3e2a4")]
#[test_action("verifiedpermissions", "IsAuthorizedWithToken", checksum = "99468e73")]
#[test_action("verifiedpermissions", "ListIdentitySources", checksum = "d700f99d")]
#[test_action("verifiedpermissions", "ListPolicies", checksum = "30785a28")]
#[test_action("verifiedpermissions", "ListPolicyStoreAliases", checksum = "3072f7b0")]
#[test_action("verifiedpermissions", "ListPolicyStores", checksum = "f41bffe9")]
#[test_action("verifiedpermissions", "ListPolicyTemplates", checksum = "e91c068f")]
#[test_action("verifiedpermissions", "ListTagsForResource", checksum = "39f42759")]
#[test_action("verifiedpermissions", "PutSchema", checksum = "1ed7269a")]
#[test_action("verifiedpermissions", "TagResource", checksum = "533081a7")]
#[test_action("verifiedpermissions", "UntagResource", checksum = "d9f27794")]
#[test_action("verifiedpermissions", "UpdateIdentitySource", checksum = "ce050867")]
#[test_action("verifiedpermissions", "UpdatePolicy", checksum = "89333b1e")]
#[test_action("verifiedpermissions", "UpdatePolicyStore", checksum = "118a4cc8")]
#[test_action("verifiedpermissions", "UpdatePolicyTemplate", checksum = "30fe21eb")]
#[tokio::test]
async fn verifiedpermissions_probe() {
    let _server = TestServer::start().await;
}
